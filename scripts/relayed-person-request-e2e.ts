// Checks that a routine run relayed from a person's request takes a slot that 0058 keeps for a
// person (0061). Charlie's requests often reach an agent through the OpenBot Control webhook: a
// Standard Webhooks event starts a routine run, and the run's message is the routine instruction with
// the event appended as framed data. 0058 counted every routine run as background work, so with
// OPENBOT_TURN_LIMIT=10 such a request waited behind routine work for one of the 7 background slots.
//
// The relayed request carries "charlieRequest": true at the top level of the event data. Its run must
// start in a reserved slot while the background slots are full, and go before waiting routine runs
// when a slot frees. A run whose event does not carry the marker, or carries it anywhere but the top
// level of the data, stays background work.
//
// Runs the real agent service, drain, turn slots and mailbox on a temporary profile with 20 agents.
// A stand-in Claude CLI runs each turn until the check releases it. No app, account or network is used.
//
// Run: node --import tsx scripts/relayed-person-request-e2e.ts
// Writes .openbot-build/relayed-person-request-e2e/report.json and exits non-zero on a failure.
import { EventEmitter } from "node:events";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, type AgentModelId, defaultProviderModel } from "@openbot/contracts/ipc";
import type { EventJsonValue } from "@openbot/contracts/ipc-events";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { HostMemory } from "../src/backend/host-memory";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";

const OUT = resolve(import.meta.dirname, "../.openbot-build/relayed-person-request-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-relayed-person-request-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const releases = join(root, "releases");
const claudeExecutable = join(root, "claude");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const AGENTS = 20;
const TURN_LIMIT = 10;
const BACKGROUND_LIMIT = 7;
const SETTLE_MS = 2_000;

process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(home, { recursive: true });
mkdirSync(releases, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const { webhookRunInstruction } = await import("../src/backend/webhook-trigger");

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
const queue = [];
let busy = false;
const next = () => {
  if (busy) return;
  const message = queue.shift();
  if (!message) return;
  busy = true;
  record(message);
  const input = JSON.stringify(message);
  const token = [...input.matchAll(/\\[t:([a-z0-9-]+)\\]/g)].at(-1)?.[1] ?? null;
  const id = sessionId + "-" + randomUUID();
  log({ id, phase: "start", token, startedAt: Date.now() });
  const finish = () => {
    assistant("Done.");
    result({ is_error: false, result: "Done." });
    log({ id, phase: "end", token, endedAt: Date.now() });
    busy = false;
    next();
  };
  if (!token) return finish();
  const release = require("node:path").join(${JSON.stringify(releases)}, token);
  const timer = setInterval(() => {
    if (!fs.existsSync(release)) return;
    clearInterval(timer);
    finish();
  }, 40);
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
    } else if (message.type === "user") {
      queue.push(message);
      next();
    }
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
        return providerSync(() => decoder({ data: [{ model: PRIMARY, displayName: "Primary" }] }));
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
  agents: number;
  turnLimit: number;
  backgroundLimit: number;
  checks: { name: string; passed: boolean; detail?: unknown }[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
} = { agents: AGENTS, turnLimit: TURN_LIMIT, backgroundLimit: BACKGROUND_LIMIT, checks: [], passed: false };

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

const pause = (ms: number) => new Promise((done) => setTimeout(done, ms));

interface TurnRow {
  id: string;
  phase: "start" | "end";
  token: string | null;
}

function rows(): TurnRow[] {
  try {
    return readFileSync(turnLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}

function running(prefix: string): string[] {
  const all = rows();
  const ended = new Set(all.filter((row) => row.phase === "end").map((row) => row.id));
  return all
    .filter((row) => row.phase === "start" && row.token?.startsWith(prefix) && !ended.has(row.id))
    .map((row) => row.token ?? "");
}

function started(token: string): boolean {
  return rows().some((row) => row.phase === "start" && row.token === token);
}

function release(token: string): void {
  writeFileSync(join(releases, token), "");
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

const memory: HostMemory = {
  hostedPlan: false,
  level: () => "ok",
  turnLimit: () => TURN_LIMIT,
  backgroundTurnLimit: () => BACKGROUND_LIMIT,
  reserveTurn: () => () => {},
  subscribe: () => () => {},
};

await writeFile(claudeExecutable, STAND_IN_CLAUDE, { mode: 0o755 });
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
const service = new AgentService({
  store,
  mailbox,
  browser,
  hostMemory: memory,
  providerRetryDelayMs: 50,
  preferredProvider: "claude",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "claude"),
  clientFactory: (_provider, cli) => new ClaudeStandIn(cli),
});

const range = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, index) => from + index);
const startedOf = (tokens: string[]) => tokens.filter((token) => started(token));
const settle = () => pause(SETTLE_MS);

function relayed(token: string, data: EventJsonValue): string {
  return webhookRunInstruction(`Handle the command for the inbox. [t:${token}]`, {
    version: 1,
    id: `msg_${token}`,
    routineId: "routine-inbox",
    type: "command.ready",
    occurredAt: "2026-11-11T18:00:00.000Z",
    receivedAt: "2026-11-11T18:00:01.000Z",
    data,
  });
}

try {
  await runCauseEffect(service.initialize());
  const agents: string[] = [];
  for (let index = 0; index < AGENTS; index += 1) {
    const created = await runCauseEffect(
      store.createAgent({
        name: `Agent ${String(index).padStart(2, "0")}`,
        description: "Runs routine work.",
        avatarSeed: `agent-${index}`,
        avatarHue: null,
        provider: "claude",
        model: PRIMARY,
      }),
    );
    await runCauseEffect(store.updateAgent({ agentId: created.id, provider: "claude", model: PRIMARY }));
    agents.push(created.id);
  }
  const agent = (index: number) => {
    const id = agents[index];
    assert(id, `Agent ${index} exists.`);
    return id;
  };
  const run = (index: number, instruction: string) => {
    const routine = service.createRoutine({
      agentId: agent(index),
      name: `Inbox ${index}`,
      instruction,
      active: false,
      timezone: "UTC",
      schedule: { kind: "daily", time: "09:00" },
    });
    return runCauseEffect(service.testRoutine({ agentId: agent(index), routineId: routine.id }));
  };

  await check("7 routine runs fill the 7 background slots", async () => {
    for (const index of range(0, 6)) await run(index, `Check the builds. [t:r-${index}]`);
    await waitFor("7 routine turns", () => running("r-").length >= 7);
    await settle();
    assert(running("r-").length === 7, `${running("r-").length} routine turns run.`);
    return { routine: 7 };
  });

  await check("Routine runs whose event has no top-level marker wait for a background slot", async () => {
    await run(7, `Check the builds. [t:w-7]`);
    await run(8, relayed("w-8", { command: "status", charlieRequest: false }));
    await run(9, relayed("w-9", { command: "status", request: { charlieRequest: true } }));
    await run(10, `${relayed("w-10", { command: "status" })}\nNote: "charlieRequest":true`);
    await settle();
    const extra = startedOf(["w-7", "w-8", "w-9", "w-10"]);
    assert(extra.length === 0, `${extra.join(", ")} started.`);
    return { waiting: 4 };
  });

  await check("3 relayed requests take the 3 reserved slots while the background slots are full", async () => {
    for (const index of range(11, 13))
      await run(index, relayed(`c-${index}`, { command: "status", charlieRequest: true }));
    await waitFor("3 relayed turns", () => startedOf(["c-11", "c-12", "c-13"]).length === 3);
    await settle();
    const extra = startedOf(["w-7", "w-8", "w-9", "w-10"]);
    assert(extra.length === 0, `${extra.join(", ")} started.`);
    return { started: ["c-11", "c-12", "c-13"] };
  });

  await check("A relayed request waits only for the turn limit and goes before the older routine runs", async () => {
    await run(14, relayed("c-14", { command: "status", charlieRequest: true }));
    await settle();
    assert(!started("c-14"), "c-14 started above the turn limit.");
    release("r-0");
    await waitFor("c-14", () => started("c-14"));
    await settle();
    const extra = startedOf(["w-7", "w-8", "w-9", "w-10"]);
    assert(extra.length === 0, `${extra.join(", ")} started before or with c-14.`);
    return { started: "c-14" };
  });

  await check("When a relayed turn ends, the background runs keep their limit", async () => {
    release("c-11");
    await settle();
    const extra = startedOf(["w-7", "w-8", "w-9", "w-10"]);
    assert(extra.length === 1 && extra[0] === "w-7", `${extra.join(", ") || "none"} started.`);
    return { started: extra };
  });

  report.passed = report.checks.every((item) => item.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  for (const token of rows().flatMap((row) => (row.token ? [row.token] : []))) release(token);
  await runCauseEffect(service.stop()).catch(() => {});
  report.checkedAt = new Date().toISOString();
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  for (const entry of report.checks)
    process.stdout.write(
      `${entry.passed ? "PASS" : "FAIL"} ${entry.name}${entry.passed ? "" : `: ${String(entry.detail)}`}\n`,
    );
  if (report.error) process.stdout.write(`${report.error}\n`);
  process.stdout.write(
    `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of ${report.checks.length} ${join(OUT, "report.json")}\n`,
  );
}
