import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  AGENT_PROVIDERS,
  type AgentProviderId,
  type CentralAuthState,
  defaultProviderModel,
} from "@openbot/contracts/ipc";
import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";
import { ServerMode, type ServerModeOptions } from "../src/main/server-mode";

const OUT = resolve(import.meta.dirname, "../.openbot-build/session-size-cap-e2e");
const root = await mkdtemp(join(tmpdir(), "ob-cap-"));
const home = join(root, "home");
const logPath = join(root, "protocol.jsonl");
const controlPath = join(root, "control.json");
const fakeCodex = join(root, "codex");
const claudeExecutable = join(root, "claude");
const turnLog = join(root, "claude-turns.jsonl");
const modeFile = join(root, "mode");
process.env.HOME = home;
process.env.CODEX_HOME = join(home, ".codex");
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
process.env.OPENBOT_CODEX_PATH = fakeCodex;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.OPENBOT_FAKE_CODEX_LOG = logPath;
process.env.OPENBOT_FAKE_CODEX_CONTROL = controlPath;
mkdirSync(home, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { CodexAppServerClient } = await import("../src/backend/app-server-client");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { AgentStore } = await import("../src/backend/agent-store");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

const FAKE_CODEX = `#!/usr/bin/env node
const fs = require("node:fs");
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n");
  process.exit(0);
}
const log = (entry) => fs.appendFileSync(process.env.OPENBOT_FAKE_CODEX_LOG, JSON.stringify(entry) + "\\n");
const control = () => JSON.parse(fs.readFileSync(process.env.OPENBOT_FAKE_CODEX_CONTROL, "utf8"));
const config = { config: { mcp_servers: {} }, layers: [{ name: { type: "user", file: "/fake/.codex/config.toml", profile: null }, version: "1", config: { mcp_servers: {} } }] };
let buffer = "";
let threads = 0;
let turns = 0;

const compacting = new Map();
process.stdout.on("error", (error) => {
  if (error.code === "EPIPE") process.exit(0);
  throw error;
});
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const answer = (message) => {
  const params = message.params || {};
  switch (message.method) {
    case "initialize": return write({ id: message.id, result: {} });
    case "account/login/start": return write({ id: message.id, result: { type: "chatgptDeviceCode", loginId: "fixture-login", userCode: "fixture-code", verificationUrl: "https://example.com/login" } });
    case "account/login/cancel": return write({ id: message.id, result: {} });
    case "account/read": return write({ id: message.id, result: { account: { type: "chatgpt", email: "codex@example.com" } } });
    case "account/rateLimits/read": return write({ id: message.id, result: { rateLimits: { limitId: "codex", primary: null, secondary: null }, rateLimitsByLimitId: null } });
    case "model/list": return write({ id: message.id, result: { data: [{ model: "gpt-6-luna", displayName: "Primary" }] } });
    case "plugin/list": return write({ id: message.id, result: { marketplaces: [] } });
    case "config/read": return write({ id: message.id, result: config });
    case "config/batchWrite": return write({ id: message.id, result: {} });
    case "thread/start": {
      const threadId = "thread-" + process.pid + "-" + (++threads);
      const ready = () => { write({ id: message.id, result: { thread: { id: threadId, turns: [] } } }); log({ method: "fixture/thread-ready", threadId }); };
      return setTimeout(ready, control().startDelayMs ?? 0);
    }
    case "thread/resume": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "thread/unsubscribe": return write({ id: message.id, result: { status: "Unsubscribed" } });
    case "thread/archive": return write({ id: message.id, result: {} });
    case "thread/turns/list":
    case "thread/items/list": return write({ id: message.id, result: { data: [], nextCursor: null } });
    case "thread/read": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "turn/start": {
      if (compacting.has(params.threadId)) {
        log({ refused: params.threadId });
        return write({ id: message.id, error: { code: -32603, message: "failed to submit turn input: ActiveTurnNotSteerable { turn_kind: Compact }" } });
      }
      const turnId = "turn-" + (++turns);
      write({ id: message.id, result: { turn: { id: turnId, status: "inProgress", items: [] } } });
      write({ method: "turn/started", params: { threadId: params.threadId, turn: { id: turnId } } });
      write({ method: "thread/tokenUsage/updated", params: { threadId: params.threadId, turnId, tokenUsage: { total: { totalTokens: control().usedTokens }, last: { totalTokens: control().usedTokens }, modelContextWindow: control().windowTokens } } });
      setTimeout(() => {
        const item = { type: "agentMessage", id: "message-" + turnId, text: "Done.", phase: "final_answer" };
        write({ method: "item/completed", params: { threadId: params.threadId, turnId, item } });
        write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: turnId, status: "completed" } } });
        const input = (params.input || []).map((item) => item.text || "").join(" ");
        if (input.includes("NATIVE-HIDDEN-COMPACTION")) setTimeout(() => {
          const nativeId = "native-compact-" + turnId;
          const item = { type: "contextCompaction", id: nativeId };
          log({ method: "fixture/native-compaction-running", threadId: params.threadId });
          write({ method: "item/started", params: { threadId: params.threadId, turnId: nativeId, item } });
          if (input.includes("NATIVE-HIDDEN-COMPACTION-EXIT")) setTimeout(() => {
            log({ method: "fixture/native-compaction-provider-exit", threadId: params.threadId });
            process.exit(1);
          }, 500);
          else setTimeout(() => {
            write({ method: "item/completed", params: { threadId: params.threadId, turnId: nativeId, item } });
            log({ method: "fixture/native-compaction-completed", threadId: params.threadId });
          }, 1000);
        }, 25);
      }, control().turnMs ?? 30);
      return;
    }
    case "thread/compact/start": {
      const { compactionMs, ignoreInterrupt } = control();
      const turnId = "compact-turn-" + (++turns);
      const item = { type: "contextCompaction", id: "compact-item-" + turnId };
      write({ id: message.id, result: {} });
      write({ method: "turn/started", params: { threadId: params.threadId, turn: { id: turnId } } });
      write({ method: "item/started", params: { threadId: params.threadId, turnId, item } });
      const finish = (status) => {
        if (compacting.get(params.threadId)?.turnId !== turnId) return;
        compacting.delete(params.threadId);
        clearTimeout(timer);
        log({ compactionEnded: turnId, threadId: params.threadId, status });
        if (status === "completed") write({ method: "item/completed", params: { threadId: params.threadId, turnId, item } });
        write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: turnId, status } } });
      };
      const timer = setTimeout(() => finish("completed"), compactionMs);
      compacting.set(params.threadId, { turnId, finish, ignoreInterrupt });
      return;
    }
    case "turn/interrupt": {
      write({ id: message.id, result: {} });
      const compaction = compacting.get(params.threadId);
      if (compaction?.turnId === params.turnId) {
        if (!compaction.ignoreInterrupt) compaction.finish("interrupted");
        return;
      }
      return write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: params.turnId, status: "interrupted" } } });
    }
    default:
      return write({ id: message.id, error: { code: -32601, message: "Method not found: " + message.method } });
  }
};
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newline = buffer.indexOf("\\n");
  while (newline >= 0) {
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (line) {
      const message = JSON.parse(line);
      log({ method: message.method ?? null, id: message.id ?? null, params: message.params ?? null });
      if (message.method && message.id !== undefined) answer(message);
    }
    newline = buffer.indexOf("\\n");
  }
});
`;

const STAND_IN_CLAUDE = `#!/usr/bin/env node
const fs = require("node:fs");
const { randomUUID } = require("node:crypto");
const args = process.argv.slice(2);
if (args.includes("--version")) {
  process.stdout.write("2.1.302 (Claude Code)\\n");
  process.exit(0);
}
const option = (name) => {
  const index = args.findIndex((arg) => arg === name || arg.startsWith(name + "="));
  if (index < 0) return null;
  return args[index] === name ? (args[index + 1] ?? null) : args[index].slice(name.length + 1);
};
const sessionId = option("--resume") ?? option("--session-id") ?? randomUUID();
let model = option("--model");
const project = require("node:path").join(process.env.CLAUDE_CONFIG_DIR, "projects", process.cwd().replace(/[^a-zA-Z0-9]/g, "-"));
fs.mkdirSync(project, { recursive: true });
const transcript = require("node:path").join(project, sessionId + ".jsonl");
const previous = fs.existsSync(transcript) ? fs.readFileSync(transcript, "utf8").trim().split("\\n").filter(Boolean).map((line) => JSON.parse(line)) : [];
const recorded = new Set(previous.map((row) => row.uuid));
let parentUuid = previous.at(-1)?.uuid ?? null;
const record = (message) => {
  if (recorded.has(message.uuid)) return;
  const row = { ...message, parentUuid, sessionId, timestamp: new Date().toISOString(), isSidechain: false };
  fs.appendFileSync(transcript, JSON.stringify(row) + "\\n");
  recorded.add(row.uuid);
  parentUuid = row.uuid;
};
const write = (message) => {
  if (message.type === "assistant") record(message);
  process.stdout.write(JSON.stringify(message) + "\\n");
};
const result = (fields) =>
  write({ type: "result", subtype: "success", session_id: sessionId, uuid: randomUUID(), duration_ms: 1,
    duration_api_ms: 1, num_turns: 1, total_cost_usd: 0, usage: { input_tokens: 0, output_tokens: 0 },
    permission_denials: [], modelUsage: {}, ...fields });
const assistant = (text) =>
  write({ type: "assistant", message: { id: "msg_" + randomUUID(), type: "message", role: "assistant", model,
    content: [{ type: "text", text }], stop_reason: null, usage: { input_tokens: 1, output_tokens: 1 } },
    parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() });
const log = (row) => fs.appendFileSync(${JSON.stringify(turnLog)}, JSON.stringify(row) + "\\n");
let count = 0;
let budgetTimer = null;
let ignoreBudgetInterrupt = false;
const turn = (message) => {
  record(message);
  const input = JSON.stringify(message);
  const id = sessionId + "-" + (++count);
  const mode = fs.existsSync(${JSON.stringify(modeFile)}) ? fs.readFileSync(${JSON.stringify(modeFile)}, "utf8").trim() : "accept";
  const startedAt = Date.now();
  log({ id, phase: "start", pid: process.pid, model, mode, startedAt, input });
  ignoreBudgetInterrupt = input.includes("TURN-CALL-BUDGET-IGNORE") && !input.includes("Fresh bounded task turn.");
  const refuse = () => {
    const resetsAt = Math.floor(Date.now() / 1000) + 86400 + Math.floor(Math.random() * 3000) + 1;
    write({ type: "rate_limit_event", rate_limit_info: { status: "rejected", rateLimitType: "seven_day", resetsAt },
      uuid: randomUUID(), session_id: sessionId });
    result({ is_error: true, result: "You've hit your limit. Try again later." });
    log({ id, phase: "end", model, mode, startedAt, endedAt: Date.now(), refused: true, resetsAt, input });
  };
  const complete = () => {
    assistant("Done.");
    result({ is_error: false, result: "Done." });
    log({ id, phase: "end", model, mode, startedAt, endedAt: Date.now(), refused: false, input });
  };
  if (input.includes("BUDGET-SLOW-CONTROL") && !input.includes("Fresh bounded task turn.")) {
    let chunks = 0;
    const timer = setInterval(() => {
      write({ type: "stream_event", event: {type: "content_block_delta", index: 0, delta: {type: "text_delta", text: "Slow peer output."}}, parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() });
      if (++chunks === 30) { clearInterval(timer); complete(); }
    }, 200);
    return;
  }
  if (input.includes("TURN-CALL-BUDGET") && !input.includes("Fresh bounded task turn.")) {
    write({ type: "assistant", message: { id: "stale-child-request", type: "message", role: "assistant", model,
      content: [{ type: "text", text: "Previous turn child." }], usage: { input_tokens: 100000000, cache_read_input_tokens: 0, cache_creation_input_tokens: 0 } },
      parent_tool_use_id: "previous-turn-child", session_id: sessionId, uuid: randomUUID() });
    let calls = 0;
    const step = () => {
      calls++;
      const frame = { type: "assistant", message: { id: "call-budget-" + calls, type: "message", role: "assistant", model,
        content: [{ type: "text", text: "Working." }], stop_reason: null,
        usage: { input_tokens: 1, cache_read_input_tokens: 1, cache_creation_input_tokens: 0, output_tokens: 1 } },
        parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() };
      write(frame); write(frame);
      log({ phase: "model-call", calls });
      if (calls >= 60 && !ignoreBudgetInterrupt) { clearInterval(budgetTimer); complete(); }
    };
    budgetTimer = setInterval(step, 30);
    return;
  }
  if (input.includes("TURN-BUDGET-CHECK")) {
    const request = (requestId, child = false) => write({ type: "assistant", message: { id: requestId, type: "message", role: "assistant", model,
      content: [{ type: "text", text: "Working." }, ...(requestId === "budget-request-1" ? [{type: "tool_use", id: "child-call-0068", name: "Agent", input: {description: "Fixture child"}}] : [])], stop_reason: null,
      usage: { input_tokens: 300, cache_read_input_tokens: 100, cache_creation_input_tokens: 0, output_tokens: 999999 } },
      parent_tool_use_id: child ? "child-call-0068" : null, session_id: sessionId, uuid: randomUUID() });
    request("budget-request-1");
    request("budget-request-1");
    log({ phase: "budget-duplicate", at: Date.now() });
    setTimeout(() => { log({ phase: "budget-second", at: Date.now() }); request("budget-request-2"); }, 80);
    setTimeout(() => { log({ phase: "budget-child", at: Date.now() }); request("budget-request-child", true); }, 160);
    budgetTimer = setTimeout(complete, 700);
    return;
  }
  if (mode === "refuse") return refuse();
  if (mode === "slow-refuse") return setTimeout(refuse, 2000);
  if (mode === "slow-accept" && model === "claude-haiku-5-5") return setTimeout(complete, 2000);
  complete();
};
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newline = buffer.indexOf("\\n");
  while (newline >= 0) {
    const line = buffer.slice(0, newline).trim();
    buffer = buffer.slice(newline + 1);
    newline = buffer.indexOf("\\n");
    if (!line) continue;
    const message = JSON.parse(line);
    if (message.type === "control_request") {
      if (message.request?.subtype === "set_model") model = message.request.model ?? model;
      const state = JSON.parse(fs.readFileSync(${JSON.stringify(controlPath)}, "utf8"));
      const context = message.request?.subtype === "get_context_usage";
      const response = () => { write({ type: "control_response", response: { subtype: "success", request_id: message.request_id, response: context ? { totalTokens: state.usedTokens, rawMaxTokens: state.windowTokens, categories: [] } : {} } }); if (context) log({ phase: "context-probe-return", pid: process.pid }); };
      if (context) {
        log({ phase: "context-probe-start", pid: process.pid });
        if (state.unsolicitedProbe) {
          fs.writeFileSync(${JSON.stringify(controlPath)}, JSON.stringify({...state, unsolicitedProbe: false}));
          setTimeout(() => { assistant("Unsolicited followup."); log({ phase: "unsolicited-start", pid: process.pid }); }, 30);
          setTimeout(() => { result({ is_error: false, result: "Followup done." }); log({ phase: "unsolicited-end", pid: process.pid }); }, 900);
        }
        setTimeout(response, state.probeDelayMs ?? 0);
      } else response();
      if (message.request?.subtype === "interrupt" && !ignoreBudgetInterrupt) {
        clearTimeout(budgetTimer);
        log({ phase: "budget-interrupt", at: Date.now() });
        result({ is_error: true, result: "Interrupted.", terminal_reason: "aborted_streaming" });
      }
    } else if (message.type === "user") turn(message);
  }
});
process.stdin.on("end", () => process.exit(0));
`;

interface ClientEvents {
  notification: [notification: AppServerNotification];
  request: [request: AppServerRequest];
  exit: [error: Error];
  diagnostic: [message: string, origin?: DiagnosticOrigin];
}

class ClaudeStandIn extends EventEmitter<ClientEvents> implements AgentClient {
  readonly provider = "claude";
  readonly contextCompaction = "native";
  readonly #real: InstanceType<typeof ClaudeAgentClient>;
  readonly readHistory: ReadProviderHistory;

  constructor(cli: ClaudeCliInfo) {
    super();
    this.#real = new ClaudeAgentClient(cli);
    this.readHistory = this.#real.readHistory;
    this.#real.on("notification", (notification) => this.emit("notification", notification));
    this.#real.on("request", (request) => this.emit("request", request));
    this.#real.on("diagnostic", (message) => this.emit("diagnostic", message));
    this.#real.on("exit", (error) => this.emit("exit", error));
  }

  killTurnProcess(threadId: string, turnId: string) {
    return this.#real.killTurnProcess?.(threadId, turnId) ?? Effect.succeed(false);
  }

  readContextUsage(threadId: string) {
    return this.#real.readContextUsage?.(threadId) ?? Effect.succeed(null);
  }

  hasActiveTurn(threadId: string) {
    return this.#real.hasActiveTurn?.(threadId) ?? false;
  }

  get running(): boolean {
    return this.#real.running;
  }

  start(): void {
    this.#real.start();
  }

  stop() {
    return this.#real.stop();
  }

  canReleaseProcess = () => this.#real.canReleaseProcess();

  releaseThread(externalThreadId: string) {
    return this.#real.releaseThread(externalThreadId);
  }

  releaseIdleThreads() {
    return this.#real.releaseIdleThreads();
  }

  request<T>(method: string, params: unknown, decoder: ResponseDecoder<T>, timeoutMs?: number) {
    if (!this.#real.running) return this.#real.request(method, params, decoder, timeoutMs);
    switch (method) {
      case "account/read":
        return providerSync(() => decoder({ account: { type: "claude", email: "claude@example.com" } }));
      case "account/rateLimits/read":
        return providerSync(() => decoder({ rateLimits: null, rateLimitsByLimitId: null }));
      case "model/list":
        return providerSync(() =>
          decoder({
            data: [
              { model: defaultProviderModel("claude"), displayName: "Primary" },
              { model: "claude-sonnet-5-5", displayName: "Backup" },
            ],
          }),
        );
      default:
        return this.#real.request(method, params, decoder, timeoutMs);
    }
  }

  notify(): void {
    this.#real.notify();
  }

  respond(id: RequestId, result: unknown): void {
    this.#real.respond(id, result);
  }

  respondError(id: RequestId, error: Parameters<AgentClient["respondError"]>[1]): void {
    this.#real.respondError(id, error);
  }
}

const unused = () => Effect.die(new Error("Not used by this check."));
const browser: AgentBrowserHost = {
  listTabs: () => [],
  beginTakeover: unused,
  endTakeover: () => {},
  resolveUploadTarget: unused,
  handleDynamicTool: unused,
  userControlTabFor: () => null,
  awaitUserControlRelease: unused,
  onChanged: () => () => {},
  onControlChanged: () => () => {},
  onDocumentChanged: () => () => {},
  clearControls: () => {},
  endControl: () => {},
  close: () => Effect.void,
  clearAgentSession: () => Effect.void,
  shareTab: () => undefined,
  unshareThread: () => undefined,
  agentTab: () => null,
};

interface Row {
  method?: string;
  params?: { threadId?: string; input?: { text?: string }[] };
  threadId?: string;
}
function protocol(): Row[] {
  try {
    return readFileSync(logPath, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}
async function waitFor<T>(label: string, read: () => T | null | undefined | false, timeoutMs = 4000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise((done) => setTimeout(done, 20));
  }
}
const report: {
  passed: boolean;
  checks: { name: string; passed: boolean; detail?: unknown }[];
  error?: string;
  checkedAt?: string;
} = { passed: false, checks: [] };
async function check(name: string, run: () => Promise<unknown>) {
  try {
    report.checks.push({ name, passed: true, detail: await run() });
  } catch (error) {
    report.checks.push({ name, passed: false, detail: error instanceof Error ? error.message : String(error) });
  }
}
function controlUsage(usedTokens: number, windowTokens = 1_000_000, turnMs = 30) {
  writeFileSync(
    controlPath,
    JSON.stringify({ usedTokens, windowTokens, turnMs, compactionMs: 30, ignoreInterrupt: false }),
  );
}
await writeFile(fakeCodex, FAKE_CODEX, { mode: 0o755 });
await writeFile(claudeExecutable, STAND_IN_CLAUDE, { mode: 0o755 });
writeFileSync(modeFile, "accept");
let count = 0;
const resources: {
  service: InstanceType<typeof AgentService>;
  database: InstanceType<typeof OpenBotDatabase>;
  server?: InstanceType<typeof ServerMode>;
}[] = [];
async function fixture(provider: AgentProviderId = "codex", mode: AgentClient["contextCompaction"] = "events") {
  const userData = join(root, `data-${++count}`);
  const database = new OpenBotDatabase(userData);
  const store = new AgentStore(userData, home, database);
  const mailbox = new MailboxStore(userData, store.sharedRoot, database);
  const service = new AgentService({
    store,
    mailbox,
    browser,
    preferredProvider: provider,
    offProviders: AGENT_PROVIDERS.filter((item) => item !== provider),
    clientFactory: (current, cli) => {
      if (current === "claude") return new ClaudeStandIn(cli);
      const client = new CodexAppServerClient(cli.executable, 1000, {});
      Object.defineProperty(client, "contextCompaction", { value: mode });
      return client;
    },
  });
  const resource: (typeof resources)[number] = { service, database };
  resources.push(resource);
  await runCauseEffect(service.initialize());
  const selectedUsage = readFileSync(controlPath, "utf8");
  controlUsage(1000);
  const created = await runCauseEffect(
    service.createAgent({
      name: `Cap ${count}`,
      description: "Checks session bounds.",
      avatarSeed: `cap-${count}`,
      avatarHue: null,
      initialMessage: "Ready for the session cap check.",
      provider,
      model: defaultProviderModel(provider),
    }),
  );
  await waitFor("the initial fixture turn", () =>
    service.listQueue(created.id).deliveries.every((delivery) => delivery.status === "completed"),
  );
  const agent = service.listAgents().find((item) => item.id === created.id);
  assert.ok(agent?.threadId);
  const threadId = agent.threadId;
  writeFileSync(controlPath, selectedUsage);
  const send = async (text: string) => {
    const receipt = await runCauseEffect(service.sendMessage({ agentId: agent.id, text, attachmentDraftIds: [] }));
    const id = receipt.deliveries[0]?.id;
    assert.ok(id);
    await waitFor("the delivery to complete", () => mailbox.getDelivery(id)?.delivery.status === "completed");
    return id;
  };
  const session = () => database.activeProviderSession(threadId, provider)?.externalSessionId;
  const compactRows = () =>
    protocol().filter((row) => row.method === "thread/compact/start" && row.params?.threadId === session());
  const updateCap = async (sessionTokenCap: number | null) =>
    runCauseEffect(service.updateAgent({ agentId: agent.id, sessionTokenCap }));
  const serverOptions: ServerModeOptions = {
    environment: { controlSocketPath: join(root, `control-${count}.sock`) },
    version: "fixture",
    centralAuth: {
      getState: () => ({ status: "signed_out" }) satisfies CentralAuthState,
      requestEmailCode: unused,
      verifyEmailCode: unused,
      canPersistSession: () => true,
      redeemServerClaim: unused,
      logout: unused,
      createMobileConnect: unused,
      cancelCompanySignIn: unused,
    },
    host: {
      getStatus: () => ({
        phase: "idle",
        configured: false,
        enabledOnLaunch: false,
        serverId: null,
        serverName: null,
        apiUrl: null,
        logoUrl: null,
        apiOnline: false,
        remoteDesktopReady: false,
        remoteDesktopScreenRecordingDenied: false,
        remoteDesktopUnattended: false,
        remoteDesktopActiveSessions: 0,
        remoteDesktopMaxSessions: 0,
        message: null,
      }),
      configure: unused,
      start: unused,
      updateIdentity: unused,
      getMobileConnectHost: () => null,
    },
    memory: null,
    agents: service,
    onError: () => {},
  };
  const control = async (body: string) => {
    if (!resource.server) {
      resource.server = new ServerMode(serverOptions);
      await runCauseEffect(resource.server.listen());
    }
    return new Promise<{ status: number; text: string; lines: Record<string, string> }>((done, fail) => {
      const req = request(
        {
          socketPath: serverOptions.environment.controlSocketPath,
          path: "/v1/agents/compact",
          method: "POST",
          headers: { "content-type": "application/x-www-form-urlencoded", "content-length": Buffer.byteLength(body) },
        },
        (response) => {
          let text = "";
          response.setEncoding("utf8");
          response.on("data", (chunk) => {
            text += chunk;
          });
          response.on("end", () =>
            done({
              status: response.statusCode ?? 0,
              text,
              lines: Object.fromEntries(
                text
                  .trim()
                  .split("\n")
                  .filter(Boolean)
                  .map((line) => {
                    const i = line.indexOf("=");
                    return [line.slice(0, i), line.slice(i + 1)];
                  }),
              ),
            }),
          );
        },
      );
      req.on("error", fail);
      req.end(body);
    });
  };
  return { service, store, mailbox, database, agent, threadId, send, session, compactRows, updateCap, control };
}

try {
  delete process.env.OPENBOT_SESSION_TOKEN_CAP;
  await check("an existing agent without a cap compacts above the default 200000 token cap", async () => {
    controlUsage(201000);
    const f = await fixture();
    assert.equal(f.store.list().find((item) => item.id === f.agent.id)?.sessionTokenCap, undefined);
    await f.send("Default session cap.");
    await waitFor("default-cap compaction", () => f.compactRows().length > 0);
    return { cap: 200000, used: 201000, compactions: f.compactRows().length };
  });
  await check("an explicit agent cap persists and null removes the override", async () => {
    controlUsage(1000);
    const f = await fixture();
    const changed = await f.updateCap(12000);
    assert.equal(changed.sessionTokenCap, 12000);
    const restored = new AgentStore(f.database.userDataPath, home, f.database);
    await runCauseEffect(restored.initialize());
    assert.equal(restored.list().find((item) => item.id === f.agent.id)?.sessionTokenCap, 12000);
    await f.updateCap(null);
    assert.equal(f.store.list().find((item) => item.id === f.agent.id)?.sessionTokenCap, undefined);
  });
  await check("the environment cap applies when an agent has no override", async () => {
    process.env.OPENBOT_SESSION_TOKEN_CAP = "16000";
    controlUsage(17000);
    const f = await fixture();
    await f.send("Environment cap.");
    await waitFor("environment-cap compaction", () => f.compactRows().length > 0);
    delete process.env.OPENBOT_SESSION_TOKEN_CAP;
  });
  await check("an agent override takes priority over the environment cap", async () => {
    process.env.OPENBOT_SESSION_TOKEN_CAP = "16000";
    controlUsage(17000);
    const f = await fixture();
    await f.updateCap(24000);
    await f.send("Agent override above environment.");
    controlUsage(1000);
    await f.send("A second turn confirms that no compaction was pending.");
    assert.equal(f.compactRows().length, 0);
    delete process.env.OPENBOT_SESSION_TOKEN_CAP;
  });
  await check("the smaller provider window still compacts at eighty percent", async () => {
    controlUsage(41000, 50000);
    const f = await fixture();
    await f.updateCap(80000);
    await f.send("Smaller native window.");
    await waitFor("window-pressure compaction", () => f.compactRows().length > 0);
  });
  await check("large input does not compact before a turn and cap work waits for completion", async () => {
    controlUsage(1000);
    const f = await fixture();
    await f.updateCap(12000);
    await f.send("Small initial input.");
    controlUsage(13000, 1000000, 700);
    const before = protocol().length;
    const sending = f.send(`LARGE-NEXT-INPUT ${"x".repeat(50000)}`);
    await waitFor("the large input starts", () =>
      protocol()
        .slice(before)
        .some((row) => row.method === "turn/start"),
    );
    assert.equal(f.compactRows().length, 0);
    await sending;
    await waitFor("completion compaction", () => f.compactRows().length > 0);
    const rows = protocol().slice(before);
    const compact = rows.findIndex((row) => row.method === "thread/compact/start");
    const turn = rows.findIndex((row) => row.method === "turn/start");
    assert.ok(compact > turn);
    return { compact, turn };
  });
  await check("a handoff provider rotates at the cap and preserves the public thread and history", async () => {
    controlUsage(13000);
    const f = await fixture("codex", "handoff");
    await f.updateCap(12000);
    await f.send("HANDOFF-HISTORY-MARKER");
    await waitFor("a rotated handoff session", () => f.database.listProviderSessions(f.threadId).length > 1);
    controlUsage(1000);
    await f.send("After the automatic rotation.");
    assert.equal(f.service.listAgents().find((agent) => agent.id === f.agent.id)?.threadId, f.agent.threadId);
    const latest = protocol()
      .filter((row) => row.method === "turn/start")
      .at(-1);
    assert.ok(
      JSON.stringify(latest?.params).includes("HANDOFF-HISTORY-MARKER"),
      "The new session receives history from the same public thread.",
    );
  });
  await check("native Claude rotates at the cap without a model compaction turn", async () => {
    controlUsage(13000);
    const f = await fixture("claude", "native");
    await f.updateCap(12000);
    await f.send("NATIVE-CLAUDE-HISTORY-MARKER");
    await waitFor("native Claude rotation", () => f.database.listProviderSessions(f.threadId).length > 1);
    controlUsage(1000);
    await f.send("After native rotation.");
    assert.equal(f.service.listAgents().find((agent) => agent.id === f.agent.id)?.threadId, f.agent.threadId);
    const turns = readFileSync(turnLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
    assert.ok(
      turns.some(
        (row) =>
          row.phase === "start" &&
          row.input.includes("NATIVE-CLAUDE-HISTORY-MARKER") &&
          row.input.includes("After native rotation."),
      ),
    );
  });
  await check("a delayed native context probe holds the next delivery until rotation", async () => {
    controlUsage(1000);
    const f = await fixture("claude", "native");
    await f.updateCap(12000);
    const old = f.session();
    writeFileSync(controlPath, JSON.stringify({ usedTokens: 13000, windowTokens: 1000000, probeDelayMs: 500 }));
    const before = readFileSync(turnLog, "utf8").split("\n").filter(Boolean).length;
    const first = f.send("Delayed context boundary.");
    await waitFor("delayed probe starts", () =>
      readFileSync(turnLog, "utf8")
        .split("\n")
        .filter(Boolean)
        .slice(before)
        .some((line) => JSON.parse(line).phase === "context-probe-start"),
    );
    const next = f.send("Queued after delayed probe.");
    await first;
    controlUsage(1000);
    await next;
    assert.notEqual(f.session(), old);
    const rows = readFileSync(turnLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .slice(before)
      .map((line) => JSON.parse(line));
    const returned = rows.findIndex((row) => row.phase === "context-probe-return");
    const nextStart = rows.findIndex(
      (row) => row.phase === "start" && row.input?.includes("Queued after delayed probe."),
    );
    assert.ok(returned >= 0 && nextStart > returned);
    return { nextDeliveryWaitedForProbe: true, rotated: true };
  });
  await check("an unsolicited turn during a context probe keeps its active state until its own end", async () => {
    controlUsage(1000);
    const f = await fixture("claude", "native");
    await f.updateCap(12000);
    const old = f.session();
    const before = readFileSync(turnLog, "utf8").split("\n").filter(Boolean).length;
    writeFileSync(
      controlPath,
      JSON.stringify({ usedTokens: 13000, windowTokens: 1000000, probeDelayMs: 350, unsolicitedProbe: true }),
    );
    const first = f.send("Trigger probe followup.");
    await waitFor("unsolicited followup starts", () =>
      readFileSync(turnLog, "utf8")
        .split("\n")
        .filter(Boolean)
        .slice(before)
        .some((line) => JSON.parse(line).phase === "unsolicited-start"),
    );
    await first;
    const snapshot = await runCauseEffect(f.service.readConversation(f.agent.id));
    assert.ok(snapshot.activeTurnId);
    assert.equal(f.session(), old);
    await waitFor("followup completion rotation", () => f.database.listProviderSessions(f.threadId).length > 1);
    return { followupPreserved: true, rotatedAtFollowupEnd: true };
  });
  await check("a replacement session uses its own cap baseline and repeated ineffective rotations stop", async () => {
    controlUsage(800000);
    const f = await fixture("codex", "handoff");
    await f.send("Rotate the old 800k session.");
    await waitFor("the first 800k rotation", () => f.database.listProviderSessions(f.threadId).length === 2);
    controlUsage(400000);
    await f.send("The replacement still exceeds 200k.");
    await waitFor("the replacement cap rotation", () => f.database.listProviderSessions(f.threadId).length === 3);
    await f.send("A second ineffective replacement must warn and stop rotating.");
    await waitFor("ineffective rotation settlement", () => f.service.nativeWorkSnapshot().idle);
    await f.send("The same fixed context cannot rotate forever.");
    assert.equal(f.database.listProviderSessions(f.threadId).length, 3);
    return { oldContext: 800000, replacementContext: 400000, cap: 200000, rotations: 2 };
  });
  await check("the manual route rejects an unknown agent and closed-field validation errors", async () => {
    controlUsage(1000);
    const f = await fixture();
    const unknown = await f.control("agentId=unknown-agent");
    assert.equal(unknown.status, 404);
    for (const body of [
      "",
      `agentId=${f.agent.id}&mode=invalid`,
      `agentId=${f.agent.id}&mode=compact&extra=1`,
      `agentId=${f.agent.id}&agentId=${f.agent.id}`,
      `agentId=${"x".repeat(300)}`,
    ]) {
      const invalid = await f.control(body);
      assert.equal(invalid.status, 400, invalid.text);
    }
  });
  await check("the manual route refuses active work and rotates idle work without a model turn", async () => {
    controlUsage(1000, 1000000, 700);
    const f = await fixture();
    const sending = f.send("A slow active turn.");
    await waitFor("a native active turn", () => f.service.nativeWorkSnapshot().idle === false);
    const busy = await f.control(`agentId=${f.agent.id}&mode=rotate`);
    await sending;
    assert.equal(busy.status, 409, busy.text);
    await waitFor("idle after the active turn", () => f.service.nativeWorkSnapshot().idle);
    const oldSession = f.session();
    const turns = protocol().filter((row) => row.method === "turn/start").length;
    const rotated = await f.control(`agentId=${f.agent.id}&mode=rotate`);
    assert.equal(rotated.status, 202, rotated.text);
    assert.equal(rotated.lines.accepted, "rotate");
    await waitFor("the accepted session rotation", () => f.session() !== oldSession);
    await waitFor("idle after rotation", () => f.service.nativeWorkSnapshot().idle);
    assert.equal(protocol().filter((row) => row.method === "turn/start").length, turns);
    const compacted = await f.control(`agentId=${f.agent.id}`);
    assert.equal(compacted.status, 202, compacted.text);
    assert.equal(compacted.lines.accepted, "compact");
    assert.equal(protocol().filter((row) => row.method === "turn/start").length, turns);
  });
  await check("a manual rotation holds queued deliveries before its provider request yields", async () => {
    controlUsage(1000);
    const f = await fixture();
    const oldSession = f.session();
    const receipt = await runCauseEffect(
      f.mailbox.enqueue({
        sender: { kind: "user" },
        recipientAgentIds: [f.agent.id],
        text: "QUEUED-BEFORE-ROTATION",
        draftIds: [],
      }),
    );
    const queuedId = receipt.deliveries[0]?.id;
    assert.ok(queuedId);
    assert.equal(f.mailbox.getDelivery(queuedId)?.delivery.status, "queued");
    writeFileSync(
      controlPath,
      JSON.stringify({ usedTokens: 1000, windowTokens: 1000000, turnMs: 30, compactionMs: 30, startDelayMs: 400 }),
    );
    const before = protocol().length;
    const rotating = f.control(`agentId=${f.agent.id}&mode=rotate`);
    await waitFor("the rotation provider request", () =>
      protocol()
        .slice(before)
        .some((row) => row.method === "thread/start"),
    );
    const wake = f.send("A delivery that arrives while rotation runs.");
    const accepted = await rotating;
    await wake;
    assert.equal(accepted.status, 202, accepted.text);
    await waitFor(
      "the original queued delivery",
      () => f.mailbox.getDelivery(queuedId)?.delivery.status === "completed",
    );
    const rows = protocol().slice(before);
    const ready = rows.findIndex((row) => row.method === "fixture/thread-ready");
    const firstTurn = rows.findIndex((row) => row.method === "turn/start");
    assert.ok(ready >= 0 && firstTurn > ready, "Queued work waits for the new session to be ready.");
    assert.ok(rows.filter((row) => row.method === "turn/start").every((row) => row.params?.threadId !== oldSession));
  });
  await check(
    "rotation carries exact native task, signed dispatch and claim references into the new session",
    async () => {
      controlUsage(1000);
      const f = await fixture();
      const task = await runCauseEffect(
        f.service.startTask({ agentId: f.agent.id, text: "A task kept across a main session rotation." }),
      );
      await waitFor("the real native task turn", () =>
        f.service.readTask(f.agent.id, task.id).messages.some((message) => message.text === "Done."),
      );
      const { HostEventsService } = await import("../src/main/host-events-service");
      const { signWebhookBody } = await import("../src/main/webhook-security");
      const events = new HostEventsService({
        routines: f.service.routineRecords,
        cipher: { encrypt: (value: string) => Buffer.from(value), decrypt: (value: Buffer) => value.toString() },
        relay: {
          connected: () => true,
          setEnabled: () => {},
          setAccountActive: () => {},
          refresh: () => {},
          registerRoute: (routeId: string) => Effect.succeed(`https://signal.example.test/v1/webhooks/${routeId}`),
          revokeRoute: () => Effect.void,
        },
        accountPrincipal: () => "fixture-account",
      });
      const saved = await runCauseEffect(
        events.saveRoutine({
          owner: { kind: "agent", id: f.agent.id },
          name: "Carry exact refs",
          instruction: "Keep the structured event.",
          active: true,
          timezone: "UTC",
          trigger: { kind: "webhook", eventType: "pod.dispatch", filters: [] },
        }),
      );
      const route = f.service.routineRecords.routes.list().find((item) => item.routineId === saved.routine.id);
      assert.ok(route && saved.secret);
      const deliveryId = "control-delivery-exact-0068";
      const refs = ["delivery:control-delivery-exact-0068", "dispatch:dispatch-exact-0068", "pr:example/project#68"];
      const body = new TextEncoder().encode(
        JSON.stringify({
          type: "pod.dispatch",
          data: {
            bridgeWork: {
              kind: "work",
              deliveryIds: [deliveryId],
              dispatchId: "dispatch-exact-0068",
              pr: { repository: "example/project", number: 68 },
            },
            data: { eventId: "unrelated-nested-dispatch", claimKey: "unrelated nested claim" },
            claimKey: "pr:example/project#68 claim-exact-0068",
            pod: "pod-exact-0068",
            dispatchTaskId: "dispatch-task-exact-0068",
          },
        }),
      );
      const timestamp = String(Math.floor(Date.now() / 1000));
      await runCauseEffect(
        events.receive({
          routeId: route.routeId,
          deliveryId,
          timestamp,
          signature: signWebhookBody(saved.secret, timestamp, deliveryId, body),
          body,
        }),
      );
      const run = await waitFor("the signed routine receipt", () => {
        const row = f.database.connection
          .prepare(
            "SELECT r.* FROM projection_routine_runs r JOIN projection_webhook_receipts w ON w.run_id=r.run_id WHERE w.delivery_id=?",
          )
          .get(deliveryId);
        return isDynamicRecord(row) && typeof row.run_id === "string" && typeof row.delivery_id === "string"
          ? row
          : null;
      });
      assert.ok(isDynamicRecord(run) && typeof run.run_id === "string" && typeof run.delivery_id === "string");
      const nativeDeliveryId = run.delivery_id;
      await waitFor(
        "the signed routine delivery",
        () => f.mailbox.getDelivery(nativeDeliveryId)?.delivery.status === "completed",
      );
      const oldThreadId = f.agent.threadId;
      const rotated = await f.control(`agentId=${f.agent.id}&mode=rotate`);
      assert.equal(rotated.status, 202, rotated.text);
      await f.send("Read the structured carryover after rotation.");
      const sent = protocol()
        .filter((row) => row.method === "turn/start")
        .at(-1);
      const text = sent?.params?.input?.map((item) => item.text ?? "").join("\n") ?? "";
      const encoded = text.match(/<openbot_session_carryover>\s*([\s\S]*?)\s*<\/openbot_session_carryover>/u)?.[1];
      assert.ok(encoded, "The new native session receives a structured carryover packet.");
      const packet = JSON.parse(encoded);
      assert.ok(isDynamicRecord(packet) && Array.isArray(packet.openTasks) && Array.isArray(packet.references));
      assert.equal(packet.version, 1);
      assert.equal(packet.agentId, f.agent.id);
      assert.equal(packet.threadId, oldThreadId);
      assert.ok(
        packet.openTasks.some(
          (item) => isDynamicRecord(item) && item.taskId === task.id && item.threadId === task.threadId,
        ),
      );
      const reference = packet.references.find((item) => isDynamicRecord(item) && item.deliveryId === deliveryId);
      assert.ok(isDynamicRecord(reference));
      assert.deepEqual(reference.refs, refs);
      assert.equal(reference.routineId, saved.routine.id);
      assert.equal(reference.runId, run.run_id);
      assert.equal(reference.nativeDeliveryId, nativeDeliveryId);
      assert.equal(reference.nativeRunStatus, "succeeded");
      assert.equal(packet.referencesMissing, false);
      assert.ok(packet.openTasks.every((item) => isDynamicRecord(item) && item.state === "open"));
      assert.equal(reference.claimKey, "pr:example/project#68 claim-exact-0068");
      assert.equal(reference.pod, "pod-exact-0068");
      assert.equal(reference.dispatchTaskId, "dispatch-task-exact-0068");
      assert.ok(Buffer.byteLength(encoded) <= 48000);
      assert.equal(f.service.listTasks(f.agent.id).tasks.find((item) => item.id === task.id)?.threadId, task.threadId);
      return { taskId: task.id, runId: run.run_id, deliveryId, refs };
    },
  );
  await check(
    "a turn budget counts distinct input requests and cache once, ignores output, and stops only the active turn",
    async () => {
      controlUsage(1000);
      const f = await fixture("claude", "native");
      await runCauseEffect(f.service.updateAgent({ agentId: f.agent.id, turnTokenBudget: 1024 }));
      const failures: string[] = [];
      f.service.on("event", (event) => {
        if (event.type === "error") failures.push(event.code);
      });
      const before = readFileSync(turnLog, "utf8").split("\n").filter(Boolean).length;
      const receipt = await runCauseEffect(
        f.service.sendMessage({ agentId: f.agent.id, text: "TURN-BUDGET-CHECK", attachmentDraftIds: [] }),
      );
      const nativeRows = (): { phase: string; at?: number }[] =>
        readFileSync(turnLog, "utf8")
          .split("\n")
          .filter(Boolean)
          .slice(before)
          .map((line) => JSON.parse(line));
      await waitFor("the native budget interruption", () =>
        nativeRows().some((row) => row.phase === "budget-interrupt"),
      );
      await waitFor("the budget-stopped turn to release", () => f.service.nativeWorkSnapshot().idle);
      const rows = nativeRows();
      assert.equal(rows.filter((row) => row.phase === "budget-interrupt").length, 1);
      assert.equal(
        rows.filter((row) => row.phase === "start").length,
        1,
        "A budget stop does not start a replacement model turn.",
      );
      assert.ok(
        rows.findIndex((row) => row.phase === "budget-interrupt") >
          rows.findIndex((row) => row.phase === "budget-child"),
        "Distinct parent and child input requests spend the budget. A duplicate snapshot and output tokens do not.",
      );
      assert.equal(failures.filter((code) => code === "turn_input_budget_exceeded").length, 1);
      const deliveryId = receipt.deliveries[0]?.id;
      assert.ok(deliveryId);
      const delivery = f.mailbox.getDelivery(deliveryId)?.delivery;
      assert.equal(delivery?.status, "failed");
      assert.match(delivery?.error ?? "", /input token budget.*fresh turn/u);
      return { stops: 1, distinctInputRequests: 3, inputTokensIncludingCache: 1200 };
    },
  );
  await check("the default model call budget fails at forty distinct requests without retrying in place", async () => {
    delete process.env.OPENBOT_TURN_MODEL_CALL_BUDGET;
    controlUsage(1000);
    const f = await fixture("claude", "native");
    const before = readFileSync(turnLog, "utf8").split("\n").filter(Boolean).length;
    const receipt = await runCauseEffect(
      f.service.sendMessage({ agentId: f.agent.id, text: "TURN-CALL-BUDGET-CHECK", attachmentDraftIds: [] }),
    );
    const deliveryId = receipt.deliveries[0]?.id;
    assert.ok(deliveryId);
    await waitFor(
      "the call-budget terminal delivery",
      () => f.mailbox.getDelivery(deliveryId)?.delivery.status === "failed",
    );
    await waitFor("call-budget native idle", () => f.service.nativeWorkSnapshot().idle);
    const rows = readFileSync(turnLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .slice(before)
      .map((line) => JSON.parse(line));
    assert.equal(rows.filter((row) => row.phase === "start").length, 1);
    assert.equal(rows.filter((row) => row.phase === "model-call").length, 40);
    assert.equal(rows.filter((row) => row.phase === "budget-interrupt").length, 1);
    assert.match(f.mailbox.getDelivery(deliveryId)?.delivery.error ?? "", /model call budget.*fresh turn/u);
    return { modelCalls: 40, duplicateFrames: 40, inputTokens: 80, deliveryStatus: "failed", replacementTurns: 0 };
  });
  await check("an ignored budget interrupt kills only its exact query and fails the native delivery", async () => {
    process.env.OPENBOT_TURN_MODEL_CALL_BUDGET = "3";
    controlUsage(1000);
    const f = await fixture("claude", "native");
    const before = readFileSync(turnLog, "utf8").split("\n").filter(Boolean).length;
    const peer = await runCauseEffect(
      f.service.createAgent({
        name: "Budget slow peer",
        description: "Fixture",
        avatarSeed: "budget-peer",
        avatarHue: null,
        initialMessage: "Peer ready.",
        provider: "claude",
        model: f.agent.model,
      }),
    );
    await waitFor("the peer initial turn", () =>
      f.service.listQueue(peer.id).deliveries.every((row) => row.status === "completed"),
    );
    const peerReceipt = await runCauseEffect(
      f.service.sendMessage({ agentId: peer.id, text: "BUDGET-SLOW-CONTROL", attachmentDraftIds: [] }),
    );
    const peerDeliveryId = peerReceipt.deliveries[0]?.id;
    assert.ok(peerDeliveryId);
    await waitFor("the slow peer to start", () => f.mailbox.getDelivery(peerDeliveryId)?.delivery.status === "running");
    const receipt = await runCauseEffect(
      f.service.sendMessage({ agentId: f.agent.id, text: "TURN-CALL-BUDGET-IGNORE", attachmentDraftIds: [] }),
    );
    const deliveryId = receipt.deliveries[0]?.id;
    assert.ok(deliveryId);
    await waitFor(
      "the ignored interrupt to settle",
      () => f.mailbox.getDelivery(deliveryId)?.delivery.status === "failed",
    );
    const rows = readFileSync(turnLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .slice(before)
      .map((line) => JSON.parse(line));
    const pid = rows.find((row) => row.phase === "start" && row.input?.includes("TURN-CALL-BUDGET-IGNORE"))?.pid;
    assert.equal(typeof pid, "number");
    await waitFor("the over-budget query process to exit", () => {
      try {
        process.kill(pid, 0);
        return false;
      } catch {
        return true;
      }
    });
    assert.match(f.mailbox.getDelivery(deliveryId)?.delivery.error ?? "", /model call budget.*fresh turn/u);
    assert.equal(
      rows.filter((row) => row.phase === "start" && row.input?.includes("TURN-CALL-BUDGET-IGNORE")).length,
      1,
    );
    assert.equal(f.mailbox.getDelivery(peerDeliveryId)?.delivery.status, "running");
    const peerPid = rows.find((row) => row.phase === "start" && row.input?.includes("BUDGET-SLOW-CONTROL"))?.pid;
    assert.equal(typeof peerPid, "number");
    assert.doesNotThrow(() => process.kill(peerPid, 0));
    await waitFor(
      "the slow peer to complete",
      () => f.mailbox.getDelivery(peerDeliveryId)?.delivery.status === "completed",
      12000,
    );
    delete process.env.OPENBOT_TURN_MODEL_CALL_BUDGET;
    return {
      configuredCalls: 3,
      queryExited: true,
      deliveryStatus: "failed",
      replacementTurns: 0,
      slowPeerCompleted: true,
    };
  });
  await check("a task budget kill preserves the same agent own chat and starts its next task turn fresh", async () => {
    process.env.OPENBOT_TURN_MODEL_CALL_BUDGET = "3";
    controlUsage(1000);
    const f = await fixture("claude", "native");
    const own = await runCauseEffect(
      f.service.sendMessage({ agentId: f.agent.id, text: "BUDGET-SLOW-CONTROL", attachmentDraftIds: [] }),
    );
    const ownId = own.deliveries[0]?.id;
    assert.ok(ownId);
    await waitFor("own chat running", () => f.mailbox.getDelivery(ownId)?.delivery.status === "running");
    const task = await runCauseEffect(f.service.startTask({ agentId: f.agent.id, text: "TURN-CALL-BUDGET-IGNORE" }));
    const delivery = await waitFor("task delivery", () =>
      (() => {
        const row = f.database.connection
          .prepare(
            "SELECT d.delivery_id FROM projection_deliveries d JOIN projection_mailbox_messages m ON m.message_id=d.message_id WHERE d.recipient_agent_id=? AND m.text='TURN-CALL-BUDGET-IGNORE' ORDER BY d.rowid DESC LIMIT 1",
          )
          .get(f.agent.id);
        return isDynamicRecord(row) && typeof row.delivery_id === "string" ? { id: row.delivery_id } : null;
      })(),
    );
    await waitFor("task budget failure", () => f.mailbox.getDelivery(delivery.id)?.delivery.status === "failed");
    assert.equal(f.mailbox.getDelivery(ownId)?.delivery.status, "running");
    assert.match(f.mailbox.getDelivery(delivery.id)?.delivery.error ?? "", /model call budget.*fresh turn/u);
    assert.equal(f.service.readTask(f.agent.id, task.id).activeTurnId, null);
    const before = f.database.listProviderSessions(task.threadId).length;
    const next = await runCauseEffect(
      f.service.sendMessage({
        agentId: f.agent.id,
        taskId: task.id,
        text: "Fresh bounded task turn.",
        attachmentDraftIds: [],
      }),
    );
    const nextId = next.deliveries[0]?.id;
    assert.ok(nextId);
    await waitFor("fresh task turn completes", () => f.mailbox.getDelivery(nextId)?.delivery.status === "completed");
    assert.ok(f.database.listProviderSessions(task.threadId).length > before);
    await waitFor("own chat survives", () => f.mailbox.getDelivery(ownId)?.delivery.status === "completed", 12000);
    delete process.env.OPENBOT_TURN_MODEL_CALL_BUDGET;
    return { failedTask: true, ownChatCompleted: true, nextTaskSessionFresh: true };
  });
  await check("a channel budget kill settles its assignment and preserves the same agent own chat", async () => {
    process.env.OPENBOT_TURN_MODEL_CALL_BUDGET = "3";
    controlUsage(1000);
    const f = await fixture("claude", "native");
    const channelId = randomUUID();
    const person = { id: "local", name: "Fixture person" };
    await runCauseEffect(
      f.service.channels.command(
        {
          type: "save",
          operationId: randomUUID(),
          channelId,
          draft: {
            name: "Budget channel",
            title: "",
            instructions: "Answer the exact request.",
            members: [{ agentId: f.agent.id }],
            leadAgentId: f.agent.id,
          },
        },
        person,
      ),
    );
    await runCauseEffect(
      f.service.channels.command(
        {
          type: "send",
          operationId: randomUUID(),
          channelId,
          text: "TURN-CALL-BUDGET-IGNORE",
          recipientAgentId: f.agent.id,
          replyToMessageId: null,
          attachmentDraftIds: [],
        },
        person,
      ),
    );
    const assignment = await waitFor("channel budget assignment", () =>
      f.service.channels.store.assignments(channelId).find((row) => row.deliveryId),
    );
    const deliveryId = assignment.deliveryId;
    assert.ok(deliveryId);
    const own = await runCauseEffect(
      f.service.sendMessage({ agentId: f.agent.id, text: "BUDGET-SLOW-CONTROL", attachmentDraftIds: [] }),
    );
    const ownId = own.deliveries[0]?.id;
    assert.ok(ownId);
    await waitFor("own channel control running", () => f.mailbox.getDelivery(ownId)?.delivery.status === "running");
    await waitFor(
      "channel budget delivery failure",
      () => f.mailbox.getDelivery(deliveryId)?.delivery.status === "failed",
    );
    await waitFor(
      "channel assignment settles",
      () => f.service.channels.store.assignments(channelId).find((row) => row.id === assignment.id)?.state === "failed",
    );
    assert.equal(f.mailbox.getDelivery(ownId)?.delivery.status, "running");
    assert.match(f.mailbox.getDelivery(deliveryId)?.delivery.error ?? "", /model call budget.*fresh turn/u);
    await waitFor(
      "own chat survives channel kill",
      () => f.mailbox.getDelivery(ownId)?.delivery.status === "completed",
      12000,
    );
    delete process.env.OPENBOT_TURN_MODEL_CALL_BUDGET;
    return { deliveryFailed: true, assignmentFailed: true, ownChatCompleted: true };
  });
  report.passed = report.checks.every((item) => item.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  delete process.env.OPENBOT_SESSION_TOKEN_CAP;
  delete process.env.OPENBOT_TURN_MODEL_CALL_BUDGET;
  for (const resource of resources.reverse()) {
    if (resource.server) await runCauseEffect(resource.server.close()).catch(() => {});
    await runCauseEffect(resource.service.stop()).catch(() => {});
    resource.database.close();
  }
  report.checkedAt = new Date().toISOString();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  for (const item of report.checks)
    process.stdout.write(
      `${item.passed ? "PASS" : "FAIL"} ${item.name}${item.passed ? "" : `: ${String(item.detail)}`}\n`,
    );
  process.stdout.write(
    `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of ${report.checks.length} ${join(OUT, "report.json")}\n`,
  );
}
