import { EventEmitter } from "node:events";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  AGENT_PROVIDERS,
  type AgentModelId,
  type CentralAuthState,
  defaultProviderModel,
} from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";
import type { ServerModeOptions } from "../src/main/server-mode";

const OUT = resolve(import.meta.dirname, "../.openbot-build/turn-reaper-ceiling-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-reaper-ceiling-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const modeFile = join(root, "mode");
const claudeExecutable = join(root, "claude");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const SECOND = "claude-sonnet-5-5";

process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
process.env.OPENBOT_TURN_REAPER_SILENCE_MINUTES = String(4 / 60);
process.env.OPENBOT_TURN_REAPER_KILL_SECONDS = "2";
process.env.OPENBOT_TURN_REAPER_INTERVAL_SECONDS = "0.1";
mkdirSync(home, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const { ServerMode } = await import("../src/main/server-mode");

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
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const result = (fields) =>
  write({ type: "result", subtype: "success", session_id: sessionId, uuid: randomUUID(), duration_ms: 1,
    duration_api_ms: 1, num_turns: 1, total_cost_usd: 0, usage: { input_tokens: 0, output_tokens: 0 },
    permission_denials: [], modelUsage: {}, ...fields });
const assistant = (text) =>
  write({ type: "assistant", message: { id: "msg_" + randomUUID(), type: "message", role: "assistant", model,
    content: [{ type: "text", text }], stop_reason: null, usage: { input_tokens: 1, output_tokens: 1 } },
    parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() });
const log = (row) => fs.appendFileSync(${JSON.stringify(turnLog)}, JSON.stringify(row) + "\\n");
let held = null;
const turn = (message) => {
  const input = JSON.stringify(message);
  const mode = fs.existsSync(${JSON.stringify(modeFile)}) ? fs.readFileSync(${JSON.stringify(modeFile)}, "utf8").trim() : "accept";
  log({ phase: "start", pid: process.pid, sessionId, model, mode, at: Date.now(), input });
  if (mode === "hang429") {
    held = { input, mode };
    setInterval(() => write({ type: "system", subtype: "api_retry", attempt: 1, max_retries: 100,
      error_status: 429, retry_delay_ms: 100, session_id: sessionId, uuid: randomUUID() }), 100).unref();
    return;
  }
  if (mode === "orphan") {
    assistant("Working.");
    return setTimeout(() => process.exit(1), 100);
  }
  if (mode === "tool") {
    write({ type: "assistant", message: { id: "tool-message", type: "message", role: "assistant", model,
      content: [{ type: "tool_use", id: "slow-tool", name: "Bash", input: { command: "fixture" } }],
      stop_reason: null, usage: { input_tokens: 1, output_tokens: 1 } },
      parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() });
    const child = require("node:child_process").spawn(process.execPath, ["-e", 'let n=0;const t=setInterval(()=>{process.stdout.write(String.fromCharCode(46));if(++n===50){clearInterval(t)}},200)']);
    child.stderr.on("data", (chunk) => log({phase:"tool-stderr", input:String(chunk)}));
    let outputBytes = 0;
    log({ phase: "tool-start", pid: child.pid, parent: process.pid, at: Date.now() });
    child.stdout.on("data", (chunk) => { outputBytes += chunk.length; log({phase:"tool-output", outputBytes, at:Date.now()}); if(process.platform !== "linux") write({ type: "stream_event", event: { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "tool-output" } }, parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() }); });
    child.on("exit", (code) => { assistant("Done."); result({ is_error: code !== 0, result: "Done." }); log({ phase: "tool-end", pid: child.pid, at: Date.now(), code, outputBytes }); });
    return;
  }
  if (mode === "hold") {
    held = { input };
    assistant("Working.");
    return;
  }
  assistant("Done.");
  result({ is_error: false, result: "Done." });
  log({ phase: "end", model, mode, at: Date.now(), input });
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
      write({ type: "control_response", response: { subtype: "success", request_id: message.request_id, response: {} } });
      if (message.request?.subtype === "interrupt" && held?.mode === "hang429") {
        log({ phase: "ignored-interrupt", pid: process.pid, sessionId, at: Date.now() });
      }
      if (message.request?.subtype === "interrupt" && held && held.mode !== "hang429") {
        log({ phase: "interrupted", model, at: Date.now(), input: held.input });
        held = null;
        result({ subtype: "error_during_execution", is_error: true, errors: ["[Request interrupted by user]"] });
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
    this.#real.on("notification", (notification) => {
      if (readFileSync(modeFile, "utf8") === "orphan" && notification.method === "turn/completed") return;
      this.emit("notification", notification);
    });
    this.#real.on("request", (request) => this.emit("request", request));
    this.#real.on("diagnostic", (message) => this.emit("diagnostic", message));
    this.#real.on("exit", (error) => {
      if (readFileSync(modeFile, "utf8") !== "orphan") this.emit("exit", error);
    });
  }

  turnProcessState(threadId: string, turnId: string) {
    return (
      this.#real.turnProcessState?.(threadId, turnId) ?? Effect.succeed({ live: this.running, toolOutputAt: null })
    );
  }

  killTurnProcess(threadId: string, turnId: string) {
    return this.#real.killTurnProcess?.(threadId, turnId) ?? Effect.succeed(false);
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
              { model: PRIMARY, displayName: "Primary" },
              { model: SECOND, displayName: "Second" },
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

const report: {
  checks: { name: string; passed: boolean; detail?: unknown }[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
  fixtureRows?: unknown[];
} = { checks: [], passed: false };

const unused = () => Effect.die(new Error("Not used by this check."));
const browser: AgentBrowserHost = {
  listTabs: () => [],
  beginTakeover: unused,
  endTakeover: () => {},
  userControlTabFor: () => null,
  awaitUserControlRelease: unused,
  resolveUploadTarget: unused,
  handleDynamicTool: unused,
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

async function waitFor<T>(label: string, read: () => T | null | undefined | false, timeoutMs = 20_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise((done) => setTimeout(done, 25));
  }
}

function turnRows(): {
  phase: string;
  input: string;
  pid?: number;
  at?: number;
  code?: number;
  outputBytes?: number;
}[] {
  try {
    return readFileSync(turnLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}

function setMode(mode: "accept" | "hold" | "hang429" | "orphan" | "tool") {
  writeFileSync(modeFile, mode);
}

async function check(name: string, run: () => Promise<unknown>) {
  try {
    const detail = await run();
    report.checks.push({ name, passed: true, detail });
  } catch (error) {
    report.checks.push({ name, passed: false, detail: error instanceof Error ? error.message : String(error) });
  }
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

setMode("accept");
await writeFile(claudeExecutable, STAND_IN_CLAUDE, { mode: 0o755 });
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
const service = new AgentService({
  store,
  mailbox,
  browser,
  providerRetryDelayMs: 50,
  preferredProvider: "claude",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "claude"),
  clientFactory: (_provider, cli) => new ClaudeStandIn(cli),
});

async function send(agentId: string, text: string): Promise<string> {
  const receipt = await runCauseEffect(service.sendMessage({ agentId, text, attachmentDraftIds: [] }));
  const deliveryId = receipt.deliveries[0]?.id;
  assert(deliveryId, "The message has a delivery.");
  return deliveryId;
}

function deliveryOf(deliveryId: string) {
  return mailbox.getDelivery(deliveryId)?.delivery;
}

async function control(
  path: string,
  method: string,
  body = "",
): Promise<{ status: number; lines: Record<string, string> }> {
  return new Promise((done, fail) => {
    const req = request(
      {
        socketPath: join(runtime, "control.sock"),
        path,
        method,
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
            lines: Object.fromEntries(
              text
                .trim()
                .split("\n")
                .filter(Boolean)
                .map((line) => {
                  const index = line.indexOf("=");
                  return [line.slice(0, index), line.slice(index + 1)];
                }),
            ),
          }),
        );
      },
    );
    req.on("error", fail);
    req.end(body);
  });
}

const runtime = await mkdtemp(join("/tmp", "obh-"));
const authState: CentralAuthState = { status: "signed_out" };
const serverOptions = (): ServerModeOptions & { agents?: unknown } => ({
  environment: { controlSocketPath: join(runtime, "control.sock") },
  version: "fixture",
  centralAuth: {
    getState: () => authState,
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
  usageLimits: service,
  agents: service,
  onError: () => {},
});

const server = new ServerMode(serverOptions());

const live = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};
async function create(name: string) {
  setMode("accept");
  const agent = await runCauseEffect(
    service.createAgent({
      name,
      description: "Fixture",
      avatarSeed: name.toLowerCase(),
      avatarHue: null,
      initialMessage: "Hello.",
      provider: "claude",
      model: PRIMARY,
    }),
  );
  await waitFor("initial turn", () =>
    service.listQueue(agent.id).deliveries.every((row) => row.status === "completed"),
  );
  return agent;
}
try {
  await runCauseEffect(service.initialize());
  await runCauseEffect(server.listen());
  const slow = await create("Slow");
  const hanging = await create("Hanging");
  const orphan = await create("Orphan");
  setMode("tool");
  const toolDelivery = await send(slow.id, "Run a slow tool.");
  await waitFor("actual tool process", () => turnRows().find((row) => row.phase === "tool-start"));
  setMode("hang429");
  const hangingDelivery = await send(hanging.id, "Hang after429.");
  const turnId = await waitFor("hanging turn", () => deliveryOf(hangingDelivery)?.turnId);
  const processRow = await waitFor("hanging CLI pid", () =>
    turnRows().find((row) => row.phase === "start" && row.input.includes("Hang after429")),
  );
  const pid = processRow.pid;
  assert(pid, "Exact child pid exists.");
  await check("interrupt route distinguishes an acknowledged request from an ended turn", async () => {
    const answer = await control("/v1/turns/interrupt", "POST", `agentId=${hanging.id}&turnId=${turnId}`);
    assert(
      answer.status === 200 && answer.lines.ended === "false" && answer.lines.stopped === "false",
      JSON.stringify(answer),
    );
    assert(
      deliveryOf(hangingDelivery)?.status === "running" && live(pid),
      "An acknowledgement has not ended the turn.",
    );
    return answer.lines;
  });
  await check(
    "a provider hanging after429 is interrupted at the silence ceiling and its process is killed after grace",
    async () => {
      await waitFor("hard ceiling settlement", () => deliveryOf(hangingDelivery)?.status === "interrupted", 14000);
      await waitFor("the exact provider pid exits", () => !live(pid), 4000);
      const interrupted = turnRows().filter((row) => row.phase === "ignored-interrupt" && row.pid === pid);
      assert(interrupted.length >= 2, "Manual and reaper interrupts reach the hanging process.");
      const age = Date.now() - (processRow.at ?? Date.now());
      assert(age >= 5900 && age < 13000, `Ceiling plus grace: ${age}`);
      return { age, pid, interrupts: interrupted.length };
    },
  );
  await check("a slow live tool that produces output survives another session's process kill", async () => {
    await waitFor("slow tool completion", () => deliveryOf(toolDelivery)?.status === "completed", 16000);
    const started = turnRows().find((row) => row.phase === "tool-start");
    const ended = turnRows().find((row) => row.phase === "tool-end");
    assert(
      started &&
        ended &&
        ended.code === 0 &&
        (ended.outputBytes ?? 0) >= 50 &&
        (ended.at ?? 0) - (started.at ?? 0) >= 9900,
      JSON.stringify({ started, ended }),
    );
    return { status: deliveryOf(toolDelivery)?.status, processOutputSampling: process.platform === "linux" };
  });
  setMode("orphan");
  const orphanDelivery = await send(orphan.id, "Leave an orphan with a stale in-memory turn.");
  const orphanTurn = await waitFor("orphan turn", () => deliveryOf(orphanDelivery)?.turnId);
  const orphanRow = await waitFor("orphan pid", () =>
    turnRows().find((row) => row.phase === "start" && row.input.includes("Leave an orphan")),
  );
  assert(orphanRow.pid, "Orphan pid exists.");
  await waitFor("orphan process exit", () => !live(orphanRow.pid ?? 0));
  await check(
    "an orphan without a provider process settles on the first pass despite its stale running entry",
    async () => {
      await waitFor("first orphan pass", () => deliveryOf(orphanDelivery)?.status === "interrupted", 2500);
      const rows = database.connection
        .prepare("SELECT payload_json FROM orchestration_events WHERE event_type='turn.reaper-stopped'")
        .all();
      const matches = rows
        .map((row) => JSON.parse(String(row.payload_json)).detail)
        .filter((row) => row.turnId === orphanTurn);
      assert(
        matches.length === 1 && matches[0].settledOrphan === true && matches[0].ended === true,
        JSON.stringify(matches),
      );
      return matches;
    },
  );
  await check("a completed turn leaves no running native delivery or control queue entry", async () => {
    const queue = await control("/v1/agents/queue", "GET");
    const row = JSON.parse(queue.lines[`agent.${orphan.id}`] ?? "{}");
    assert(row.runningTurns === 0 && row.runningTurnId === null, JSON.stringify(row));
    return row;
  });
} catch (error) {
  report.error = error instanceof Error ? error.stack : String(error);
} finally {
  await runCauseEffect(server.close()).catch(() => {});
  await runCauseEffect(service.stop()).catch(() => {});
  database.close();
  report.passed = report.checks.length === 5 && report.checks.every((row) => row.passed) && !report.error;
  report.checkedAt = new Date().toISOString();
  const rows = turnRows();
  report.fixtureRows = rows;
  for (const row of rows) {
    if (row.phase === "tool-start" && row.pid && !rows.some((end) => end.phase === "tool-end" && end.pid === row.pid)) {
      try {
        process.kill(row.pid, "SIGKILL");
      } catch {}
    }
  }
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  await rm(runtime, { recursive: true, force: true });
}
for (const row of report.checks)
  process.stdout.write(`${row.passed ? "PASS" : "FAIL"} ${row.name} ${row.passed ? "" : JSON.stringify(row.detail)}\n`);
if (report.error) process.stdout.write(`${report.error}\n`);
process.stdout.write(`passed ${report.checks.filter((row) => row.passed).length} of ${report.checks.length}\n`);
if (!report.passed) process.exitCode = 1;
