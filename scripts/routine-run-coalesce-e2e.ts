import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { AGENT_PROVIDERS } from "@openbot/contracts/ipc";
import type { EventRoutineOwner, SaveEventRoutineInput } from "@openbot/contracts/ipc-events";
import { teeLogLines } from "@openbot/logging";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";

const OUT = resolve(import.meta.dirname, "../.openbot-build/routine-run-coalesce-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-routine-coalesce-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const fakeCodex = join(root, "codex");
const holdFile = join(root, "hold");
process.env.HOME = home;
process.env.CODEX_HOME = join(home, ".codex");
process.env.OPENBOT_CODEX_PATH = fakeCodex;
mkdirSync(home, { recursive: true });

const FAKE_CODEX = `#!${process.execPath}
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n");
  process.exit(0);
}
const config = { config: { mcp_servers: {} }, layers: [{ name: { type: "user", file: "/fake/.codex/config.toml", profile: null }, version: "1", config: { mcp_servers: {} } }] };
let buffer = "";
let threads = 0;
let turns = 0;
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
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
      write({ id: message.id, result: { turn: { id: turnId, status: "inProgress", items: [] } } });
      write({ method: "turn/started", params: { threadId: params.threadId, turn: { id: turnId } } });
      const finish = () => {
        if (require("node:fs").existsSync(${JSON.stringify(holdFile)})) return setTimeout(finish, 20);
        write({ method: "item/completed", params: { threadId: params.threadId, turnId, item: { type: "agentMessage", id: "message-" + turnId, text: "Done.", phase: "final_answer" } } });
        write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: turnId, status: "completed" } } });
      };
      return setTimeout(finish, 20);
    }
    case "turn/interrupt": return write({ id: message.id, result: {} });
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
    }
    newline = buffer.indexOf("\\n");
  }
});
`;

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const { HostEventsService } = await import("../src/main/host-events-service");
const { signWebhookBody } = await import("../src/main/webhook-security");

const coalesceLines: string[] = [];
teeLogLines(["routine-scheduler"], (line) => coalesceLines.push(line));

const report: {
  checks: { name: string; passed: boolean; detail?: string }[];
  statuses: Record<string, number>;
  passed: boolean;
  error?: string;
} = { checks: [], statuses: {}, passed: false };

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

interface EventBody {
  type: string;
  data: { eventId: string };
}

const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));

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
const sqlite: { connection: DatabaseSync | null } = { connection: null };

try {
  await writeFile(fakeCodex, FAKE_CODEX, { mode: 0o755 });
  await runCauseEffect(agents.initialize());
  const create = (name: string) =>
    runCauseEffect(
      store.createAgent({
        name,
        description: `${name} for the coalesce check.`,
        avatarSeed: name.toLowerCase(),
        avatarHue: null,
      }),
    );
  const jeffrey = await create("Jeffrey");

  const cipher = {
    encrypt: (value: string) => Buffer.from(`sealed:${Buffer.from(value).toString("base64")}`),
    decrypt: (value: Buffer) => Buffer.from(value.toString().slice("sealed:".length), "base64").toString(),
  };
  const relay = {
    connected: () => true,
    setEnabled: () => {},
    setAccountActive: () => {},
    refresh: () => {},
    registerRoute: (routeId: string) => Effect.succeed(`https://signal.example.test/v1/webhooks/${routeId}`),
    revokeRoute: () => Effect.void,
  };
  const events = new HostEventsService({
    routines: agents.routineRecords,
    cipher,
    relay,
    accountPrincipal: () => "account-1",
  });
  const routineFor = async (agentId: string, name: string) => {
    const owner: EventRoutineOwner = { kind: "agent", id: agentId };
    const saved = await runCauseEffect(
      events.saveRoutine({
        owner,
        name,
        instruction: `${name}: handle the event.`,
        active: true,
        timezone: "UTC",
        trigger: { kind: "webhook", eventType: "inbox.event", filters: [] },
      } satisfies SaveEventRoutineInput),
    );
    const route = agents.routineRecords.routes.list().find((item) => item.routineId === saved.routine.id);
    if (!saved.secret || !route) throw new Error(`No webhook route for ${name}.`);
    return { id: saved.routine.id, routeId: route.routeId, secret: saved.secret };
  };
  const inbox = await routineFor(jeffrey.id, "Executor swarm inbox");
  const other = await routineFor(jeffrey.id, "Mail wake");

  const deliver = async (routine: { routeId: string; secret: string }, deliveryId: string, payload: EventBody) => {
    const body = new TextEncoder().encode(JSON.stringify(payload));
    const timestamp = String(Math.floor(Date.now() / 1000));
    const signature = signWebhookBody(routine.secret, timestamp, deliveryId, body);
    const { status } = await Effect.runPromise(
      events.receive({ routeId: routine.routeId, deliveryId, timestamp, signature, body }),
    );
    report.statuses[deliveryId] = status;
    await sleep(200);
    return status;
  };
  const event = (eventId: string): EventBody => ({ type: "inbox.event", data: { eventId } });
  const sql = () => {
    sqlite.connection ??= new DatabaseSync(database.path);
    return sqlite.connection;
  };
  const runRows = (routineId: string) =>
    sql()
      .prepare(
        "SELECT run_id, status, instruction, delivery_id FROM projection_routine_runs WHERE routine_id = ? ORDER BY created_at, run_id",
      )
      .all(routineId)
      .map((row) => ({
        runId: String(row.run_id),
        status: String(row.status),
        instruction: String(row.instruction),
        deliveryId: row.delivery_id === null ? null : String(row.delivery_id),
      }));
  const receiptRun = (deliveryId: string) => {
    const row = sql()
      .prepare("SELECT status, run_id FROM projection_webhook_receipts WHERE delivery_id = ?")
      .get(deliveryId);
    return row ? { status: String(row.status), runId: row.run_id === null ? null : String(row.run_id) } : null;
  };
  const deliveryStatus = (deliveryId: string | null) =>
    deliveryId === null ? null : (mailbox.getDelivery(deliveryId)?.delivery.status ?? null);
  const waitUntil = async (label: string, ready: () => boolean, timeoutMs = 15_000) => {
    const deadline = Date.now() + timeoutMs;
    while (!ready()) {
      if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
      await sleep(25);
    }
  };

  writeFileSync(holdFile, "hold");
  const firstStatus = await deliver(inbox, "event-1", event("e-1"));
  await waitUntil(
    "the first run to start",
    () => deliveryStatus(runRows(inbox.id)[0]?.deliveryId ?? null) === "running",
  );
  const secondStatus = await deliver(inbox, "event-2", event("e-2"));
  await waitUntil("the second run to queue", () => runRows(inbox.id)[1]?.deliveryId != null);
  const [first, second] = runRows(inbox.id);
  check(
    "A running run does not block one next run: both events get 202 and two runs",
    firstStatus === 202 &&
      secondStatus === 202 &&
      runRows(inbox.id).length === 2 &&
      deliveryStatus(first?.deliveryId ?? null) === "running" &&
      deliveryStatus(second?.deliveryId ?? null) === "queued",
    JSON.stringify({
      firstStatus,
      secondStatus,
      runs: runRows(inbox.id).map((row) => [row.status, deliveryStatus(row.deliveryId)]),
    }),
  );

  const linesBefore = coalesceLines.length;
  const more: number[] = [];
  for (const id of ["event-3", "event-4", "event-5", "event-6"]) more.push(await deliver(inbox, id, event(id)));
  const afterMore = runRows(inbox.id);
  check(
    "While the routine has a queued run, a new event queues no run and gets 202",
    more.every((status) => status === 202) && afterMore.length === 2,
    JSON.stringify({ more, runs: afterMore.length }),
  );
  check(
    "The receipt of each such event names the queued run",
    ["event-3", "event-4", "event-5", "event-6"].every((id) => {
      const receipt = receiptRun(id);
      return receipt?.status === "started" && receipt.runId === second?.runId;
    }),
    JSON.stringify(["event-3", "event-4", "event-5", "event-6"].map(receiptRun)),
  );
  const logged = coalesceLines.slice(linesBefore).filter((line) => line.includes("A routine run was not queued"));
  check(
    "OpenBot logs the skipped run once for the routine in the hour",
    logged.length === 1 && logged[0]?.includes(inbox.id) === true && logged[0].includes(second?.runId ?? "-"),
    JSON.stringify(logged),
  );
  const duplicate = await deliver(inbox, "event-3", event("event-3"));
  const ignored = await deliver(inbox, "event-ignored", { type: "other.event", data: { eventId: "x" } });
  check(
    "A repeated delivery and an event the routine ignores keep their answers",
    duplicate === 200 && ignored === 200 && runRows(inbox.id).length === 2,
    JSON.stringify({ duplicate, ignored, runs: runRows(inbox.id).length }),
  );
  const otherStatus = await deliver(other, "other-1", event("o-1"));
  await waitUntil("the other routine's run to queue", () => runRows(other.id)[0]?.deliveryId != null);
  check(
    "Another routine of the same agent still queues its run",
    otherStatus === 202 && runRows(other.id).length === 1,
    JSON.stringify({ otherStatus, runs: runRows(other.id).length }),
  );

  rmSync(holdFile);
  await waitUntil(
    "every run to finish",
    () => [...runRows(inbox.id), ...runRows(other.id)].every((row) => row.status === "succeeded"),
    30_000,
  );
  const lateStatus = await deliver(inbox, "event-late", event("e-late"));
  await waitUntil(
    "the late run to finish",
    () => runRows(inbox.id).length === 3 && runRows(inbox.id)[2]?.status === "succeeded",
  );
  check(
    "When no run of the routine waits, the next event queues a run again",
    lateStatus === 202 && runRows(inbox.id).length === 3,
    JSON.stringify({ lateStatus, runs: runRows(inbox.id).map((row) => row.status) }),
  );

  report.passed = report.checks.length === 7 && report.checks.every((item) => item.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stdout.write(`${report.error}\n`);
  process.exitCode = 1;
} finally {
  await runCauseEffect(agents.stop()).catch(() => {});
  sqlite.connection?.close();
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}
process.stdout.write(
  `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of 7\n`,
);
