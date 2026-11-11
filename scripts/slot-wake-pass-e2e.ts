// Checks the main-thread cost of a turn slot change on a server of live size (0065). The 11 November
// 20:03 UTC profile of the live server (queue 20261111d, 60 s, 99% busy) put 21.3% of the busy time in
// DrainScheduler.#scheduleLane and 19.2% in retrySlotWaiters: each scheduleDrain woke every lane that
// waited for a slot, and each woken lane asked every other lane whether it waits. Each of these asked
// ChannelService.mayDrain, and each mayDrain ran a SQLite query that reads every assignment row
// (hasAgentAssignmentInState, 15.8%; the native statement get was 19.4% of the self time). The
// statements were compiled again on each call (prepare 4.6%), and the channel pump's busy check
// built a whole queue entry for each queued task (12.4%).
//
// A slot change must take a bounded time and a bounded number of SQLite reads, whatever the number
// of waiting lanes, and the same turns must start in the same order: a person's message first, then
// the oldest, inside the turn limit of 10 and the background limit of 7 (0058).
//
// Runs the real agent service, drain, turn slots, mailbox and channel service on a temporary profile
// with 47 agents, open tasks (about 120 lanes), 18,000 settled deliveries, and 9 channels with
// 3,107 tasks and 5,400 assignments. A stand-in Claude CLI runs each turn until the check releases it.
// No app, account or network is used.
//
// Run: node --import tsx scripts/slot-wake-pass-e2e.ts
// OPENBOT_E2E_HISTORY sets the settled deliveries. Writes .openbot-build/slot-wake-pass-e2e/report.json
// and decisions.json, and exits non-zero on a failure.
import { createHash, randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { DatabaseSync, type SQLInputValue, StatementSync } from "node:sqlite";
import { AGENT_PROVIDERS, type AgentModelId, type ChannelTask, defaultProviderModel } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ChannelAssignment } from "../src/backend/channel-store";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { HostMemory } from "../src/backend/host-memory";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";

const OUT = resolve(import.meta.dirname, "../.openbot-build/slot-wake-pass-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-slot-wake-pass-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const releases = join(root, "releases");
const claudeExecutable = join(root, "claude");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const AGENTS = 47;
const OPEN_TASKS = 26;
const HISTORY = Number(process.env.OPENBOT_E2E_HISTORY ?? 18_000);
const TURN_LIMIT = 10;
const BACKGROUND_LIMIT = 7;
const SETTLE_MS = 2_000;
const LARGE = [
  { tasks: 1_094, assignments: 1_501 },
  { tasks: 1_088, assignments: 1_724 },
  { tasks: 721, assignments: 1_785 },
  { tasks: 204, assignments: 390 },
];
const SMALL = 5;
const MEMBER = { id: "local", name: "Example member" };
const MAX_SLOT_CHANGE_MS = 150;
const MAX_GETS_PER_SLOT_CHANGE = 1_000;
const MAX_PREPARES_PER_SLOT_CHANGE = 10;
const MAX_MAY_DRAIN_MS = 0.5;

process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(home, { recursive: true });
mkdirSync(releases, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { DrainScheduler } = await import("../src/backend/agent/drain-scheduler");
const { TurnSlots } = await import("../src/backend/agent/turn-slots");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

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

interface SlotChange {
  phase: string;
  changes: number;
  schedulerMs: number;
  perChangeMs: number;
  maxCallMs: number;
  calls: number;
  mayStart: number;
  gets: number;
  alls: number;
  prepares: number;
  getsPerChange: number;
}

const report: {
  agents: number;
  lanes: number;
  settledDeliveries: number;
  channels: { channels: number; tasks: number; assignments: number };
  turnLimit: number;
  backgroundLimit: number;
  seedSeconds: number;
  slotChanges: SlotChange[];
  busy?: { calls: number; totalMs: number; perCallMs: number; gets: number };
  mayDrain?: { calls: number; totalMs: number; perCallMs: number; gets: number };
  decisions: { count: number; allowed: number; digest: string };
  starts: string[];
  checks: { name: string; passed: boolean; detail?: unknown }[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
} = {
  agents: AGENTS,
  lanes: 0,
  settledDeliveries: 0,
  channels: { channels: 0, tasks: 0, assignments: 0 },
  turnLimit: TURN_LIMIT,
  backgroundLimit: BACKGROUND_LIMIT,
  seedSeconds: 0,
  slotChanges: [],
  decisions: { count: 0, allowed: 0, digest: "" },
  starts: [],
  checks: [],
  passed: false,
};

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

const sql = { get: 0, all: 0, prepare: 0 };
let depth = 0;
const statementGet = StatementSync.prototype.get;
const statementAll = StatementSync.prototype.all;
const databasePrepare = DatabaseSync.prototype.prepare;
Object.defineProperty(StatementSync.prototype, "get", {
  configurable: true,
  writable: true,
  value(this: StatementSync, ...args: SQLInputValue[]) {
    if (depth > 0) sql.get += 1;
    return statementGet.bind(this)(...args);
  },
});
Object.defineProperty(StatementSync.prototype, "all", {
  configurable: true,
  writable: true,
  value(this: StatementSync, ...args: SQLInputValue[]) {
    if (depth > 0) sql.all += 1;
    return statementAll.bind(this)(...args);
  },
});
Object.defineProperty(DatabaseSync.prototype, "prepare", {
  configurable: true,
  writable: true,
  value(this: DatabaseSync, text: string) {
    if (depth > 0) sql.prepare += 1;
    return databasePrepare.bind(this)(text);
  },
});

const window = { ms: 0, calls: 0, max: 0, mayStart: 0 };
function timed<T>(work: () => T): T {
  if (depth > 0) {
    depth += 1;
    try {
      return work();
    } finally {
      depth -= 1;
    }
  }
  depth = 1;
  const startedAt = performance.now();
  try {
    return work();
  } finally {
    const ms = performance.now() - startedAt;
    depth = 0;
    window.ms += ms;
    window.calls += 1;
    window.max = Math.max(window.max, ms);
  }
}

const agentNames = new Map<string, string>();
const taskNames = new Map<string, string>();
const laneName = (key: string) => {
  const [agentId = "", rest] = key.split("\u0000");
  const agent = agentNames.get(agentId) ?? agentId;
  if (rest === undefined) return agent;
  if (rest === "\u0001routine") return `${agent}/routine`;
  return `${agent}/${taskNames.get(rest) ?? rest}`;
};
const decisions: [string, boolean][] = [];
const mayStart = TurnSlots.prototype.mayStart;
TurnSlots.prototype.mayStart = function (lane: string): boolean {
  return timed(() => {
    window.mayStart += 1;
    const allowed = mayStart.call(this, lane);
    decisions.push([laneName(lane), allowed]);
    return allowed;
  });
};
const scheduleDrain = DrainScheduler.prototype.scheduleDrain;
DrainScheduler.prototype.scheduleDrain = function (agentId: string, followUsageLimit?: boolean): void {
  timed(() => scheduleDrain.call(this, agentId, followUsageLimit));
};
const retrySlotWaiters = DrainScheduler.prototype.retrySlotWaiters;
DrainScheduler.prototype.retrySlotWaiters = function (): void {
  timed(() => retrySlotWaiters.call(this));
};

const percentile = (values: number[], share: number) => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(share * sorted.length))] ?? 0;
};
const round = (value: number) => Math.round(value * 100) / 100;
const resetWindow = () => {
  window.ms = 0;
  window.calls = 0;
  window.max = 0;
  window.mayStart = 0;
  sql.get = 0;
  sql.all = 0;
  sql.prepare = 0;
};
interface Measured {
  ms: number;
  max: number;
  calls: number;
  mayStart: number;
  gets: number;
  alls: number;
  prepares: number;
}
const perChange: Measured[] = [];
const total = (measured: Measured[], field: keyof Measured) => measured.reduce((sum, item) => sum + item[field], 0);
function slotChanges(phase: string, changes: number, measured: Measured[]): SlotChange {
  const ms = total(measured, "ms");
  const gets = total(measured, "gets");
  const result = {
    phase,
    changes,
    schedulerMs: round(ms),
    perChangeMs: round(ms / Math.max(1, changes)),
    maxCallMs: round(Math.max(0, ...measured.map((item) => item.max))),
    calls: total(measured, "calls"),
    mayStart: total(measured, "mayStart"),
    gets,
    alls: total(measured, "alls"),
    prepares: total(measured, "prepares"),
    getsPerChange: round(gets / Math.max(1, changes)),
  };
  report.slotChanges.push(result);
  return result;
}

async function oneChange(token: string, next: string): Promise<Measured> {
  resetWindow();
  release(token);
  await waitFor(`${next} after ${token}`, () => started(next));
  await pause(800);
  const measured = { ...window, gets: sql.get, alls: sql.all, prepares: sql.prepare };
  perChange.push(measured);
  return measured;
}

const range = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, index) => from + index);
const tokens = (prefix: string, indexes: number[]) => indexes.map((index) => `${prefix}-${index}`);
const startedOf = (all: string[]) => all.filter((token) => started(token));

function task(channelId: string, ownerAgentId: string): ChannelTask {
  const id = randomUUID();
  return {
    id,
    channelId,
    parentTaskId: null,
    rootTaskId: id,
    ownerAgentId,
    requestMessageId: randomUUID(),
    instruction: "Earlier request: check the nightly build of the release branch and report each change.",
    attachmentDraftIds: [],
    expectedResult: "Complete the requested work and report the result.",
    sourceMessageIds: [],
    dependencies: [],
    resources: [],
    state: "completed",
    revision: 1,
    assignmentCount: 1,
    error: null,
  };
}

function assignment(item: ChannelTask, agentId: string): ChannelAssignment {
  return {
    id: randomUUID(),
    channelId: item.channelId,
    taskId: item.id,
    agentId,
    taskRevision: item.revision,
    requestMessageId: item.requestMessageId,
    resources: [],
    deliveryId: null,
    turnId: randomUUID(),
    state: "completed",
    throughSequence: 10,
    summaryVersion: 0,
    awaitedTaskIds: [],
    pendingRevision: null,
    pendingOutcome: null,
  };
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
    agentNames.set(created.id, `A${index}`);
  }
  const agent = (index: number) => {
    const id = agents[index];
    assert(id, `Agent ${index} exists.`);
    return id;
  };

  const seedStartedAt = performance.now();
  for (let index = 0; index < OPEN_TASKS; index += 1) {
    const opened = await runCauseEffect(service.startTask({ agentId: agent(index + 13), text: `Open task ${index}.` }));
    taskNames.set(opened.id, `task${index}`);
  }
  await waitFor("the open task turns", () => rows().filter((row) => row.phase === "end").length >= OPEN_TASKS, 60_000);
  report.lanes = agents.length * 2 + OPEN_TASKS;

  for (let index = 0; index < HISTORY; index += 1) {
    const kind = index % 3;
    const sender = agent((index * 7 + 3) % AGENTS);
    const receipt = await runCauseEffect(
      mailbox.enqueue({
        sender:
          kind === 0
            ? { kind: "user" }
            : kind === 1
              ? { kind: "agent", agentId: sender }
              : {
                  kind: "routine",
                  routineId: `history-${index % 40}`,
                  runId: `history-${index}`,
                  routineName: "History",
                  scheduledFor: "2026-11-11T08:00:00.000Z",
                },
        recipientAgentIds: [agent(index % AGENTS)],
        text: `${index}: The nightly build report of the release branch.`,
        ...(kind === 1 ? { idempotencyKey: `agent:${sender}:history-${index}:${index}` } : { draftIds: [] }),
      }),
    );
    const id = receipt.deliveries[0]?.id;
    assert(id, "The history message has a delivery.");
    await runCauseEffect(mailbox.markStarting(id));
    await runCauseEffect(mailbox.markTerminal(id, index % 50 === 3 ? "failed" : "completed"));
  }
  report.settledDeliveries = HISTORY;

  const channelIds: string[] = [];
  for (let index = 0; index < LARGE.length + SMALL; index += 1) {
    const channelId = randomUUID();
    await runCauseEffect(
      service.channels.command(
        {
          type: "save",
          operationId: randomUUID(),
          channelId,
          draft: {
            name: `Channel ${index}`,
            title: "",
            instructions: "Check the requested work.",
            members: agents.slice(index * 4, index * 4 + 8).map((agentId) => ({ agentId })),
            leadAgentId: agent(index * 4),
          },
        },
        MEMBER,
      ),
    );
    channelIds.push(channelId);
  }
  const channelStore = service.channels.store;
  for (const [index, scale] of LARGE.entries()) {
    const channelId = channelIds[index] ?? "";
    const members = agents.slice(index * 4, index * 4 + 8);
    const tasks = Array.from({ length: scale.tasks }, (_, n) => task(channelId, members[n % members.length] ?? ""));
    const assignments = Array.from({ length: scale.assignments }, (_, n) => {
      const item = tasks[n % tasks.length];
      assert(item, "The task exists.");
      return assignment(item, item.ownerAgentId ?? "");
    });
    for (let at = 0; at < tasks.length; at += 400)
      channelStore.update(channelStore.get(channelId), { tasks: tasks.slice(at, at + 400) });
    for (let at = 0; at < assignments.length; at += 400)
      channelStore.update(channelStore.get(channelId), { assignments: assignments.slice(at, at + 400) });
  }
  report.channels = {
    channels: channelIds.length,
    tasks: LARGE.reduce((sum, item) => sum + item.tasks, 0),
    assignments: LARGE.reduce((sum, item) => sum + item.assignments, 0),
  };
  report.seedSeconds = round((performance.now() - seedStartedAt) / 1000);

  const routines = new Map<number, string>();
  for (const index of [...range(0, 6), ...range(13, 46)]) {
    const routine = service.createRoutine({
      agentId: agent(index),
      name: `Builds ${index}`,
      instruction: `Check the builds. [t:r-${index}]`,
      active: false,
      timezone: "UTC",
      schedule: { kind: "daily", time: "09:00" },
    });
    routines.set(index, routine.id);
  }
  const runRoutine = async (index: number) => {
    const routineId = routines.get(index);
    assert(routineId, `Routine ${index} exists.`);
    await runCauseEffect(service.testRoutine({ agentId: agent(index), routineId }));
    await pause(5);
  };
  const send = async (index: number) => {
    await runCauseEffect(
      service.sendMessage({ agentId: agent(index), text: `Please look now. [t:p-${index}]`, attachmentDraftIds: [] }),
    );
    await pause(5);
  };
  decisions.length = 0;

  await check("7 routine runs and a person's message start: 8 of 10 slots, 7 of 7 background", async () => {
    for (const index of range(0, 6)) await runRoutine(index);
    await waitFor("7 routine turns", () => running("r-").length >= 7);
    await send(7);
    await waitFor("the person's turn", () => started("p-7"));
    await pause(SETTLE_MS);
    assert(running("r-").length === 7 && running("p-").length === 1, `${running("r-").length} routine turns run.`);
    return { routine: 7, person: 1 };
  });

  await check("34 more routine runs all wait: the background slots are full", async () => {
    for (const index of range(13, 46)) await runRoutine(index);
    await pause(SETTLE_MS);
    const extra = startedOf(tokens("r", range(13, 46)));
    assert(extra.length === 0, `${extra.join(", ")} started.`);
    return { waiting: 34 };
  });

  await check("5 messages of a person: the 2 free slots take the first 2, the others wait", async () => {
    for (const index of range(8, 12)) await send(index);
    await pause(SETTLE_MS);
    const people = startedOf(tokens("p", range(8, 12)));
    assert(people.join(",") === "p-8,p-9", `${people.join(", ") || "none"} started.`);
    return { started: people };
  });

  await check("3 person turns end one at a time: each slot goes to the next waiting message of a person", async () => {
    const measured: Measured[] = [];
    for (const [token, next] of [
      ["p-7", "p-10"],
      ["p-8", "p-11"],
      ["p-9", "p-12"],
    ] as const)
      measured.push(await oneChange(token, next));
    const extra = startedOf(tokens("r", range(13, 46)));
    assert(extra.length === 0, `${extra.join(", ")} started.`);
    return slotChanges("person turn ends", 3, measured);
  });

  await check("10 routine turns end one at a time: each slot goes to the oldest waiting routine run", async () => {
    const order = [...range(0, 6), 13, 14, 15];
    const measured: Measured[] = [];
    for (const [position, index] of order.entries()) {
      const next = `r-${13 + position}`;
      measured.push(await oneChange(`r-${index}`, next));
      const extra = startedOf(tokens("r", range(14 + position, 46)));
      assert(extra.length === 0, `After r-${index}: ${extra.join(", ")} started too.`);
    }
    assert(running("r-").length === 7, `${running("r-").length} routine turns run.`);
    return slotChanges("routine turn ends", order.length, measured);
  });

  await check("The channel pump's busy check of each agent", async () => {
    resetWindow();
    depth = 1;
    const startedAt = performance.now();
    let calls = 0;
    for (let pass = 0; pass < 10; pass += 1)
      for (const id of agents) {
        service.channels.hooks.busy(id);
        calls += 1;
      }
    const totalMs = performance.now() - startedAt;
    depth = 0;
    report.busy = { calls, totalMs: round(totalMs), perCallMs: round(totalMs / calls), gets: sql.get };
    resetWindow();
    depth = 1;
    const drainStartedAt = performance.now();
    for (let pass = 0; pass < 10; pass += 1) for (const id of agents) service.channels.mayDrain(id);
    const drainMs = performance.now() - drainStartedAt;
    depth = 0;
    report.mayDrain = { calls, totalMs: round(drainMs), perCallMs: round(drainMs / calls), gets: sql.get };
    return { busy: report.busy, mayDrain: report.mayDrain };
  });

  const decisionText = JSON.stringify(decisions);
  report.decisions = {
    count: decisions.length,
    allowed: decisions.filter(([, allowed]) => allowed).length,
    digest: createHash("sha256").update(decisionText).digest("hex"),
  };
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "decisions.json"), `${decisionText}\n`);
  report.starts = rows()
    .filter((row) => row.phase === "start" && row.token)
    .map((row) => row.token ?? "");

  await check(`A slot change takes under ${MAX_SLOT_CHANGE_MS} ms of scheduling on the main thread`, async () => {
    const worst = Math.max(...perChange.map((item) => item.ms));
    const median = percentile(
      perChange.map((item) => item.ms),
      0.5,
    );
    assert(worst < MAX_SLOT_CHANGE_MS, `The slowest slot change took ${round(worst)} ms (median ${round(median)} ms).`);
    return { median: round(median), max: round(worst) };
  });

  await check(`A slot change runs under ${MAX_GETS_PER_SLOT_CHANGE} SQLite statement reads`, async () => {
    const worst = Math.max(...perChange.map((item) => item.gets));
    assert(worst < MAX_GETS_PER_SLOT_CHANGE, `A slot change ran ${worst} statement reads.`);
    return {
      max: worst,
      median: percentile(
        perChange.map((item) => item.gets),
        0.5,
      ),
    };
  });

  await check(`A slot change compiles at most ${MAX_PREPARES_PER_SLOT_CHANGE} SQLite statements`, async () => {
    const worst = Math.max(...perChange.map((item) => item.prepares));
    assert(worst <= MAX_PREPARES_PER_SLOT_CHANGE, `A slot change compiled ${worst} statements.`);
    return { max: worst };
  });

  await check(`ChannelService.mayDrain takes under ${MAX_MAY_DRAIN_MS} ms and reads no row again`, async () => {
    assert(report.mayDrain, "The mayDrain check did not run.");
    assert(report.mayDrain.perCallMs < MAX_MAY_DRAIN_MS, `${report.mayDrain.perCallMs} ms for each call.`);
    assert(report.mayDrain.gets === 0, `${report.mayDrain.gets} statement reads after the first calls.`);
    return report.mayDrain;
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
