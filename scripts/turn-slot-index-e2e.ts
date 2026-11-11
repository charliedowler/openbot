// Checks the cost of a turn slot decision on a server with a long mailbox (0060). The 11 November
// profile of a copy of the server put 19.8% of the main-thread ticks, and 72% of a 16.4 s stall, in
// DrainScheduler.#heldByMachine -> TurnSlots.mayStart. A decision asked every lane of every agent
// whether it runs (MailboxStore.laneActive or besideActive) and for its head (nextQueued or
// nextBesideRoutine), and each of these reads every delivery. scheduleDrain asks again for each lane
// that waits for a slot, so the cost was lanes x deliveries for each waiting lane.
//
// A decision must read the whole mailbox a fixed number of times, whatever the number of lanes, and
// the same turns must start: a person's message first, then the oldest, inside the turn limit of 10
// and the background limit of 7 that OPENBOT_TURN_LIMIT=10 gives (0058).
//
// Runs the real agent service, drain, turn slots and mailbox on a temporary profile with 60 agents
// (120 lanes) and 3,000 settled deliveries. A stand-in Claude CLI runs each turn until the check
// releases it. No app, account or network is used.
//
// Run: node --import tsx scripts/turn-slot-index-e2e.ts
// Writes .openbot-build/turn-slot-index-e2e/report.json and exits non-zero on a failure.
import { EventEmitter } from "node:events";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, type AgentModelId, defaultProviderModel } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { HostMemory } from "../src/backend/host-memory";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";

const OUT = resolve(import.meta.dirname, "../.openbot-build/turn-slot-index-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-turn-slot-index-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const releases = join(root, "releases");
const claudeExecutable = join(root, "claude");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const AGENTS = 60;
const HISTORY = 3_000;
const TURN_LIMIT = 10;
const BACKGROUND_LIMIT = 7;
const SETTLE_MS = 2_000;
const MAX_READS_PER_DECISION = 12;
const MAX_DECISION_P95_MS = 5;
const MAX_SCHEDULE_DRAIN_MS = 150;

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

interface Decision {
  lane: string;
  allowed: boolean;
  ms: number;
  reads: number;
}

interface Cost {
  phase: string;
  decisions: number;
  allowed: number;
  decisionMs: { median: number; p95: number; max: number };
  readsPerDecision: { median: number; max: number };
  scheduleDrains: number;
  scheduleDrainMs: { median: number; max: number; total: number };
}

const report: {
  agents: number;
  lanes: number;
  settledDeliveries: number;
  turnLimit: number;
  backgroundLimit: number;
  laneIndex: boolean;
  costs: Cost[];
  checks: { name: string; passed: boolean; detail?: unknown }[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
} = {
  agents: AGENTS,
  lanes: AGENTS * 2,
  settledDeliveries: 0,
  turnLimit: TURN_LIMIT,
  backgroundLimit: BACKGROUND_LIMIT,
  laneIndex: false,
  costs: [],
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

let deciding = false;
let reads = 0;
let laneReads = 0;
const decisions: Decision[] = [];
const drainMs: number[] = [];
const countRead = () => {
  if (deciding) reads += 1;
};
const laneActive = mailbox.laneActive.bind(mailbox);
mailbox.laneActive = (agentId, taskId) => {
  countRead();
  return laneActive(agentId, taskId);
};
const besideActive = mailbox.besideActive.bind(mailbox);
mailbox.besideActive = (agentId) => {
  countRead();
  return besideActive(agentId);
};
const besideTurn = mailbox.besideTurn.bind(mailbox);
mailbox.besideTurn = (agentId, turnId) => {
  countRead();
  return besideTurn(agentId, turnId);
};
const personLanes = mailbox.personLanes.bind(mailbox);
mailbox.personLanes = () => {
  countRead();
  return personLanes();
};
const mainLaneRunsNonChannelWork = mailbox.mainLaneRunsNonChannelWork.bind(mailbox);
mailbox.mainLaneRunsNonChannelWork = (agentId) => {
  countRead();
  return mainLaneRunsNonChannelWork(agentId);
};
const queuedTaskIds = mailbox.queuedTaskIds.bind(mailbox);
mailbox.queuedTaskIds = (agentId) => {
  countRead();
  return queuedTaskIds(agentId);
};
const nextQueued = mailbox.nextQueued.bind(mailbox);
mailbox.nextQueued = (agentId, taskId = null, lanes) => {
  if (lanes === undefined) countRead();
  return nextQueued(agentId, taskId, lanes);
};
const nextBesideRoutine = mailbox.nextBesideRoutine.bind(mailbox);
mailbox.nextBesideRoutine = (agentId, lanes) => {
  if (lanes === undefined) countRead();
  return nextBesideRoutine(agentId, lanes);
};
report.laneIndex = "lanes" in mailbox;
if ("lanes" in mailbox) {
  const lanes = mailbox.lanes.bind(mailbox);
  mailbox.lanes = () => {
    if (deciding) reads += 1;
    else laneReads += 1;
    return lanes();
  };
}
const mayStart = TurnSlots.prototype.mayStart;
TurnSlots.prototype.mayStart = function (lane: string): boolean {
  deciding = true;
  reads = laneReads;
  laneReads = 0;
  const startedAt = performance.now();
  try {
    const allowed = mayStart.call(this, lane);
    decisions.push({ lane, allowed, ms: performance.now() - startedAt, reads });
    return allowed;
  } finally {
    deciding = false;
  }
};
const scheduleDrain = DrainScheduler.prototype.scheduleDrain;
DrainScheduler.prototype.scheduleDrain = function (agentId: string, followUsageLimit?: boolean): void {
  const startedAt = performance.now();
  try {
    scheduleDrain.call(this, agentId, followUsageLimit);
  } finally {
    drainMs.push(performance.now() - startedAt);
  }
};

const percentile = (values: number[], share: number) => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(share * sorted.length))] ?? 0;
};
const round = (value: number) => Math.round(value * 1000) / 1000;

function cost(phase: string): Cost {
  const ms = decisions.map((decision) => decision.ms);
  const counts = decisions.map((decision) => decision.reads);
  const result = {
    phase,
    decisions: decisions.length,
    allowed: decisions.filter((decision) => decision.allowed).length,
    decisionMs: {
      median: round(percentile(ms, 0.5)),
      p95: round(percentile(ms, 0.95)),
      max: round(Math.max(0, ...ms)),
    },
    readsPerDecision: { median: percentile(counts, 0.5), max: Math.max(0, ...counts) },
    scheduleDrains: drainMs.length,
    scheduleDrainMs: {
      median: round(percentile(drainMs, 0.5)),
      max: round(Math.max(0, ...drainMs)),
      total: round(drainMs.reduce((sum, value) => sum + value, 0)),
    },
  };
  report.costs.push(result);
  decisions.length = 0;
  drainMs.length = 0;
  return result;
}

const tokens = (prefix: string, indexes: number[]) => indexes.map((index) => `${prefix}-${index}`);
const range = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, index) => from + index);
const startedOf = (all: string[]) => all.filter((token) => started(token));

async function settle(): Promise<void> {
  await pause(SETTLE_MS);
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

  for (let index = 0; index < HISTORY; index += 1) {
    const kind = index % 3;
    const receipt = await runCauseEffect(
      mailbox.enqueue({
        sender:
          kind === 0
            ? { kind: "user" }
            : kind === 1
              ? { kind: "agent", agentId: "agent-helper" }
              : {
                  kind: "routine",
                  routineId: "history",
                  runId: `history-${index}`,
                  routineName: "History",
                  scheduledFor: "2026-11-11T08:00:00.000Z",
                },
        recipientAgentIds: [agent(index % AGENTS)],
        text: `${index}: The nightly build report of the release branch.`,
        ...(kind === 1 ? { idempotencyKey: `agent:agent-helper:history-${index}:${index}` } : { draftIds: [] }),
      }),
    );
    const id = receipt.deliveries[0]?.id;
    assert(id, "The history message has a delivery.");
    if (index % 50 === 7) {
      await runCauseEffect(mailbox.cancel(agent(index % AGENTS), id));
      continue;
    }
    await runCauseEffect(mailbox.markStarting(id));
    await runCauseEffect(
      mailbox.markTerminal(id, index % 50 === 3 ? "failed" : index % 50 === 11 ? "interrupted" : "completed"),
    );
  }
  report.settledDeliveries = agents.reduce(
    (sum, id) =>
      sum +
      service
        .listQueue(id)
        .deliveries.filter((delivery) => !["queued", "starting", "running"].includes(delivery.status)).length,
    0,
  );

  const routines = new Map<number, string>();
  for (const index of [...range(0, 6), ...range(10, 49)]) {
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
  const runRoutine = (index: number) => {
    const routineId = routines.get(index);
    assert(routineId, `Routine ${index} exists.`);
    return runCauseEffect(service.testRoutine({ agentId: agent(index), routineId }));
  };
  const send = (index: number) =>
    runCauseEffect(
      service.sendMessage({ agentId: agent(index), text: `Please look now. [t:p-${index}]`, attachmentDraftIds: [] }),
    );
  decisions.length = 0;
  drainMs.length = 0;

  await check("7 routine runs and a person's message start: 8 of 10 slots, 7 of 7 background", async () => {
    for (const index of range(0, 6)) await runRoutine(index);
    await waitFor("7 routine turns", () => running("r-").length >= 7);
    await send(7);
    await waitFor("the person's turn", () => started("p-7"));
    await settle();
    const routineTurns = running("r-");
    assert(
      routineTurns.length === 7 && running("p-").length === 1,
      `${routineTurns.length} routine, ${running("p-").length} person turns.`,
    );
    return { routine: routineTurns.length, person: 1, cost: cost("start 8 turns") };
  });

  await check("40 more routine runs all wait: the background slots are full", async () => {
    for (const index of range(10, 49)) await runRoutine(index);
    await settle();
    const extra = startedOf(tokens("r", range(10, 49)));
    assert(
      extra.length === 0 && running("r-").length === 7,
      `${extra.join(", ") || "none"} started; ${running("r-").length} routine turns run.`,
    );
    return { waiting: 40, cost: cost("queue 40 routine runs behind 7") };
  });

  await check("5 messages of a person: the 2 free slots take the first 2, the others wait", async () => {
    for (const index of range(50, 54)) await send(index);
    await settle();
    const people = startedOf(tokens("p", range(50, 54)));
    assert(people.join(",") === "p-50,p-51", `${people.join(", ") || "none"} started.`);
    assert(startedOf(tokens("r", range(10, 49))).length === 0, "A routine run started.");
    return { started: people, cost: cost("5 messages of a person, 2 slots") };
  });

  await check("3 person turns end: the 3 waiting messages of a person start before any routine run", async () => {
    for (const token of ["p-7", "p-50", "p-51"]) release(token);
    await waitFor("p-52 to p-54", () => startedOf(tokens("p", range(52, 54))).length === 3);
    await settle();
    const extra = startedOf(tokens("r", range(10, 49)));
    assert(extra.length === 0, `${extra.join(", ")} started.`);
    return { started: tokens("p", range(52, 54)), cost: cost("3 person turns end") };
  });

  await check("2 routine turns end: the 2 oldest waiting routine runs take the 2 background slots", async () => {
    for (const token of ["r-0", "r-1"]) release(token);
    await waitFor("2 routine runs", () => startedOf(tokens("r", range(10, 49))).length >= 2);
    await settle();
    const extra = startedOf(tokens("r", range(10, 49)));
    assert(extra.join(",") === "r-10,r-11", `${extra.join(", ")} started.`);
    assert(running("r-").length === 7, `${running("r-").length} routine turns run.`);
    return { started: extra, cost: cost("2 routine turns end") };
  });

  await check(
    `A decision reads the whole mailbox at most ${MAX_READS_PER_DECISION} times (${AGENTS * 2} lanes)`,
    async () => {
      const worst = Math.max(...report.costs.map((item) => item.readsPerDecision.max));
      const total = report.costs.reduce((sum, item) => sum + item.decisions, 0);
      assert(total > 0, "No decision ran.");
      assert(worst <= MAX_READS_PER_DECISION, `A decision read the whole mailbox ${worst} times.`);
      return { decisions: total, worst, medians: report.costs.map((item) => item.readsPerDecision.median) };
    },
  );

  await check(`A decision takes under ${MAX_DECISION_P95_MS} ms at p95`, async () => {
    const worst = Math.max(...report.costs.map((item) => item.decisionMs.p95));
    assert(worst < MAX_DECISION_P95_MS, `p95 ${worst} ms.`);
    return {
      p95: report.costs.map((item) => item.decisionMs.p95),
      max: report.costs.map((item) => item.decisionMs.max),
    };
  });

  await check(`scheduleDrain with every waiting lane takes under ${MAX_SCHEDULE_DRAIN_MS} ms`, async () => {
    const worst = Math.max(...report.costs.map((item) => item.scheduleDrainMs.max));
    assert(worst < MAX_SCHEDULE_DRAIN_MS, `max ${worst} ms.`);
    return {
      max: report.costs.map((item) => item.scheduleDrainMs.max),
      total: report.costs.map((item) => item.scheduleDrainMs.total),
    };
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
