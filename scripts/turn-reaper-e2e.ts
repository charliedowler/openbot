import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { AGENT_PROVIDERS } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";

const OUT = resolve(import.meta.dirname, "../.openbot-build/turn-reaper-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-turn-reaper-"));
const home = join(root, "home");
const fakeCodex = join(root, "codex");
const exitedFlag = join(root, "codex-exited");
const STALL_MINUTES = 0.05;
const STALL_MS = STALL_MINUTES * 60_000;
const PERSON = { id: "local", name: "Alex" };

process.env.HOME = home;
process.env.CODEX_HOME = join(home, ".codex");
process.env.OPENBOT_CODEX_PATH = fakeCodex;
process.env.OPENBOT_TURN_REAPER_MINUTES = String(STALL_MINUTES);
process.env.OPENBOT_TURN_REAPER_INTERVAL_SECONDS = "1";
mkdirSync(home, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

const FAKE_CODEX = `#!${process.execPath}
const { existsSync, writeFileSync } = require("node:fs");
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n");
  process.exit(0);
}
const EXITED = ${JSON.stringify(exitedFlag)};
if (existsSync(EXITED)) process.exit(1);
const config = { config: { mcp_servers: {} }, layers: [{ name: { type: "user", file: "/fake/.codex/config.toml", profile: null }, version: "1", config: { mcp_servers: {} } }] };
let buffer = "";
let threads = 0;
let turns = 0;
const waiting = new Map();
process.stdout.on("error", (error) => {
  if (error.code === "EPIPE") process.exit(0);
  throw error;
});
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const finish = (threadId, turnId, status = "completed") => {
  const item = { type: "agentMessage", id: "message-" + turnId, text: "Done.", phase: "final_answer" };
  write({ method: "item/completed", params: { threadId, turnId, item } });
  write({ method: "turn/completed", params: { threadId, turn: { id: turnId, status } } });
};
const live = new Map();
const answer = (message) => {
  const params = message.params || {};
  switch (message.method) {
    case "initialize": return write({ id: message.id, result: {} });
    case "account/read": return write({ id: message.id, result: { account: { type: "chatgpt", email: "codex@example.com" } } });
    case "account/rateLimits/read": return write({ id: message.id, result: { rateLimits: { limitId: "codex", primary: null, secondary: null }, rateLimitsByLimitId: null } });
    case "model/list": return write({ id: message.id, result: { data: [{ model: "gpt-6-luna", displayName: "GPT-6 Luna" }] } });
    case "plugin/list": return write({ id: message.id, result: { marketplaces: [] } });
    case "config/read": return write({ id: message.id, result: config });
    case "config/batchWrite": return write({ id: message.id, result: {} });
    case "thread/start": return write({ id: message.id, result: { thread: { id: "thread-" + (++threads), turns: [] } } });
    case "thread/resume": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "thread/unsubscribe": return write({ id: message.id, result: { status: "Unsubscribed" } });
    case "thread/turns/list":
    case "thread/items/list": return write({ id: message.id, result: { data: [], nextCursor: null } });
    case "thread/read": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "turn/start": {
      const turnId = "turn-" + (++turns);
      const text = (params.input || []).map((item) => item.text || "").join("\\n");
      write({ id: message.id, result: { turn: { id: turnId, status: "inProgress", items: [] } } });
      write({ method: "turn/started", params: { threadId: params.threadId, turn: { id: turnId } } });
      if (text.includes("PARK-ELICIT")) {
        const id = "elicit-" + turnId;
        waiting.set(id, { threadId: params.threadId, turnId });
        return write({ id, method: "mcpServer/elicitation/request", params: { threadId: params.threadId, turnId,
          serverName: "Vault", mode: "form", message: "Which token?",
          requestedSchema: { type: "object", properties: { token: { type: "string", title: "Token" } }, required: ["token"] } } });
      }
      if (text.includes("ORPHAN-EXIT")) {
        writeFileSync(EXITED, "1");
        return setTimeout(() => process.exit(1), 50);
      }
      if (text.includes("SILENT-LIVE")) {
        live.set(turnId, params.threadId);
        return;
      }
      if (text.includes("BUSY-LIVE")) {
        let ticks = 0;
        const timer = setInterval(() => {
          ticks += 1;
          write({ method: "item/agentMessage/delta", params: { threadId: params.threadId, turnId, itemId: "busy-" + turnId, delta: "." } });
          if (ticks >= 24) {
            clearInterval(timer);
            finish(params.threadId, turnId);
          }
        }, 500);
        return;
      }
      return setTimeout(() => finish(params.threadId, turnId), 20);
    }
    case "turn/interrupt": {
      write({ id: message.id, result: {} });
      live.delete(params.turnId);
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
      if (message.method && message.id !== undefined) answer(message);
      else if (!message.method && waiting.has(message.id)) {
        const turn = waiting.get(message.id);
        waiting.delete(message.id);
        finish(turn.threadId, turn.turnId);
      }
    }
    newline = buffer.indexOf("\\n");
  }
});
`;

const report: {
  checks: { name: string; passed: boolean; detail?: string }[];
  events: unknown[];
  passed: boolean;
  error?: string;
} = { checks: [], events: [], passed: false };

function check(name: string, condition: boolean, detail?: string) {
  report.checks.push({ name, passed: condition, ...(detail === undefined ? {} : { detail }) });
  process.stdout.write(
    `${condition ? "ok  " : "FAIL"} ${name}${condition || detail === undefined ? "" : `: ${detail}`}\n`,
  );
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

const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));

async function waitFor<T>(label: string, read: () => T | null | undefined | false, timeoutMs = 20_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await sleep(25);
  }
}

async function settlesWithin<T>(read: () => T | null | undefined | false, timeoutMs: number): Promise<T | null> {
  try {
    return await waitFor("a settled turn", read, timeoutMs);
  } catch {
    return null;
  }
}

function startService(name: string) {
  const userData = join(root, `user-data-${name}`);
  const database = new OpenBotDatabase(userData);
  const store = new AgentStore(userData, home, database);
  const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
  const agents = new AgentService({
    store,
    mailbox,
    browser,
    preferredProvider: "codex",
    offProviders: AGENT_PROVIDERS.filter((candidate) => candidate !== "codex"),
    timezone: () => "UTC",
  });
  let connection: DatabaseSync | null = null;
  const sql = () => {
    connection ??= new DatabaseSync(database.path);
    return connection;
  };
  const reaperEvents = () =>
    sql()
      .prepare(
        "SELECT aggregate_id, occurred_at, payload_json FROM orchestration_events WHERE event_type = 'turn.reaper-stopped' ORDER BY sequence",
      )
      .all()
      .map((row) => ({ threadId: String(row.aggregate_id), detail: JSON.parse(String(row.payload_json)).detail }));
  const turnStatus = (turnId: string) =>
    String(sql().prepare("SELECT status FROM projection_turns WHERE turn_id = ?").get(turnId)?.status ?? "missing");
  const active = (agentId: string) =>
    agents.listQueue(agentId).deliveries.find((delivery) => ["starting", "running"].includes(delivery.status));
  const idle = (agentId: string, timeoutMs = 20_000) =>
    waitFor(
      "the agent's turns to end",
      () =>
        agents
          .listQueue(agentId)
          .deliveries.every((delivery) => !["queued", "starting", "running"].includes(delivery.status)),
      timeoutMs,
    );
  const createAgent = async (agentName: string) => {
    const agent = await runCauseEffect(
      agents.createAgent({
        name: agentName,
        description: `${agentName} checks stuck turns.`,
        avatarSeed: agentName.toLowerCase(),
        avatarHue: null,
        initialMessage: "Hello.",
      }),
    );
    await idle(agent.id);
    return agent;
  };
  const routineRun = async (agentId: string, instruction: string) => {
    const routine = agents.createRoutine({
      agentId,
      name: `Relay ${Math.random().toString(36).slice(2, 6)}`,
      instruction,
      active: false,
      timezone: "UTC",
      schedule: { kind: "daily", time: "09:00" },
    });
    await runCauseEffect(agents.testRoutine({ agentId, routineId: routine.id }));
    const running = await waitFor("the routine run to start", () => active(agentId)?.turnId);
    return { routine, turnId: running };
  };
  const runStatus = (agentId: string, routineId: string) =>
    agents.listRoutineRuns({ agentId, routineId })[0]?.status ?? null;
  const stop = async () => {
    await runCauseEffect(agents.stop()).catch(() => {});
    connection?.close();
    database.close();
  };
  return { agents, reaperEvents, turnStatus, active, idle, createAgent, routineRun, runStatus, stop };
}

let first: ReturnType<typeof startService> | null = null;
let second: ReturnType<typeof startService> | null = null;
try {
  await writeFile(fakeCodex, FAKE_CODEX, { mode: 0o755 });
  first = startService("first");
  const service = first;
  await runCauseEffect(service.agents.initialize());
  const jeffrey = await service.createAgent("Jeffrey");
  const ada = await service.createAgent("Ada");
  const silas = await service.createAgent("Silas");
  const bea = await service.createAgent("Bea");

  const parked = await service.routineRun(jeffrey.id, "PARK-ELICIT Relay the command.");
  await waitFor("the routine run to need attention", () =>
    service.runStatus(jeffrey.id, parked.routine.id) === "needs-attention" ? true : null,
  );
  const parkedAt = Date.now();
  const parkedEnded = await settlesWithin(
    () => (service.active(jeffrey.id) === undefined ? Date.now() : null),
    STALL_MS + 10_000,
  );
  check(
    "A routine run parked on a question nobody answers is stopped after the idle limit",
    parkedEnded !== null && parkedEnded - parkedAt >= STALL_MS - 1_000,
    parkedEnded === null
      ? `still ${service.turnStatus(parked.turnId)} after ${Math.round((Date.now() - parkedAt) / 1000)} s; run ${service.runStatus(jeffrey.id, parked.routine.id)}`
      : `ended after ${parkedEnded - parkedAt} ms`,
  );
  check(
    "The parked routine run ends interrupted, not waiting",
    service.runStatus(jeffrey.id, parked.routine.id) === "interrupted",
    String(service.runStatus(jeffrey.id, parked.routine.id)),
  );
  const parkedEvent = service.reaperEvents().find((event) => event.detail?.turnId === parked.turnId);
  check(
    "The stop is stored as a turn.reaper-stopped event with the reason",
    parkedEvent?.detail?.kind === "parked" && parkedEvent.detail.agentId === jeffrey.id,
    JSON.stringify(service.reaperEvents()),
  );

  await runCauseEffect(service.agents.sendMessage({ agentId: ada.id, text: "PARK-ELICIT Pick a token." }, PERSON));
  const personTurn = await waitFor("the person's turn to start", () => service.active(ada.id)?.turnId);
  await sleep(STALL_MS + 4_000);
  check(
    "A person's own turn that waits on a question is not stopped",
    service.active(ada.id)?.turnId === personTurn,
    service.turnStatus(personTurn),
  );

  const silent = await service.routineRun(silas.id, "SILENT-LIVE Work quietly.");
  await sleep(STALL_MS + 4_000);
  check(
    "A quiet turn with a live provider and no question is not stopped",
    service.active(silas.id)?.turnId === silent.turnId,
    service.turnStatus(silent.turnId),
  );

  const busy = await service.routineRun(bea.id, "BUSY-LIVE Work with progress.");
  const busyDone = await settlesWithin(() => (service.active(bea.id) === undefined ? true : null), 25_000);
  check(
    "A turn that keeps sending progress runs to its own end",
    busyDone !== null && service.runStatus(bea.id, busy.routine.id) === "succeeded",
    String(service.runStatus(bea.id, busy.routine.id)),
  );

  const second1 = await service.routineRun(jeffrey.id, "PARK-ELICIT Relay again.");
  await settlesWithin(() => (service.active(jeffrey.id) === undefined ? true : null), STALL_MS + 10_000);
  const third = await service.routineRun(jeffrey.id, "PARK-ELICIT Relay a third time.");
  await sleep(STALL_MS + 4_000);
  check(
    "The second stop for the agent in the hour runs, the third is only logged",
    service.runStatus(jeffrey.id, second1.routine.id) === "interrupted" &&
      service.active(jeffrey.id)?.turnId === third.turnId,
    `second ${service.runStatus(jeffrey.id, second1.routine.id)}, third ${service.turnStatus(third.turnId)}`,
  );
  check("Each stop stores one event", service.reaperEvents().length === 2, JSON.stringify(service.reaperEvents()));
  report.events.push(...service.reaperEvents());
  await service.stop();
  first = null;

  second = startService("second");
  await runCauseEffect(second.agents.initialize());
  const otto = await second.createAgent("Otto");
  await runCauseEffect(second.agents.sendMessage({ agentId: otto.id, text: "ORPHAN-EXIT Build it." }, PERSON));
  const orphan = { turnId: await waitFor("the person's turn to start", () => second?.active(otto.id)?.turnId) };
  await waitFor("the provider process to exit", () => existsSync(exitedFlag));
  const orphanedAt = Date.now();
  const orphanEnded = await settlesWithin(
    () => (second?.active(otto.id) === undefined ? Date.now() : null),
    STALL_MS + 20_000,
  );
  check(
    "A turn whose provider process exited and did not come back is stopped",
    orphanEnded !== null,
    `still ${second.turnStatus(orphan.turnId)} after ${Math.round((Date.now() - orphanedAt) / 1000)} s`,
  );
  const orphanEvent = second.reaperEvents().find((event) => event.detail?.turnId === orphan.turnId);
  check(
    "The orphan stop is stored with kind orphaned",
    orphanEvent?.detail?.kind === "orphaned",
    JSON.stringify(second.reaperEvents()),
  );
  report.events.push(...second.reaperEvents());

  report.passed = report.checks.every((item) => item.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stdout.write(`${report.error}\n`);
  process.exitCode = 1;
} finally {
  await first?.stop();
  await second?.stop();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}
process.stdout.write(
  `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of ${report.checks.length}\n`,
);
