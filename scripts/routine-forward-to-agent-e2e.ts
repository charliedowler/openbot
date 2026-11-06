import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { AGENT_PROVIDERS } from "@openbot/contracts/ipc";
import type { EventRoutineOwner, SaveEventRoutineInput } from "@openbot/contracts/ipc-events";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";

const OUT = resolve(import.meta.dirname, "../.openbot-build/routine-forward-to-agent-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-routine-forward-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const fakeCodex = join(root, "codex");
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
      return setTimeout(() => {
        write({ method: "item/completed", params: { threadId: params.threadId, turnId, item: { type: "agentMessage", id: "message-" + turnId, text: "Done.", phase: "final_answer" } } });
        write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: turnId, status: "completed" } } });
      }, 20);
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

interface ForwardBody {
  type: string;
  data: { command?: string; requestId?: string };
  forwardToAgentId?: string | number;
  instruction?: string;
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
        description: `${name} for the forward check.`,
        avatarSeed: name.toLowerCase(),
        avatarHue: null,
      }),
    );
  const helm = await create("Helm");
  const compass = await create("Compass");
  const scout = await create("Scout");
  const thunder = await create("Thunder");

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
  const setup = new HostEventsService({
    routines: agents.routineRecords,
    cipher,
    relay,
    accountPrincipal: () => "account-1",
  });
  const routineFor = async (agentId: string, name: string) => {
    const owner: EventRoutineOwner = { kind: "agent", id: agentId };
    const saved = await runCauseEffect(
      setup.saveRoutine({
        owner,
        name,
        instruction: `${name}: relay the event.`,
        active: true,
        timezone: "UTC",
        trigger: { kind: "webhook", eventType: "command.ready", filters: [] },
      } satisfies SaveEventRoutineInput),
    );
    const route = agents.routineRecords.routes.list().find((item) => item.routineId === saved.routine.id);
    if (!saved.secret || !route) throw new Error(`No webhook route for ${name}.`);
    return {
      id: saved.routine.id,
      routeId: route.routeId,
      secret: saved.secret,
      instruction: saved.routine.instruction,
    };
  };
  const helmRoutine = await routineFor(helm.id, "Executor inbox");
  const thunderRoutine = await routineFor(thunder.id, "Thunder inbox");
  const thunderOther = await routineFor(thunder.id, "Thunder other inbox");

  process.env.OPENBOT_ROUTINE_FORWARD_ALLOWLIST = `${helm.id}:${compass.id} ${thunderRoutine.id}:${compass.id}`;
  const events = new HostEventsService({
    routines: agents.routineRecords,
    cipher,
    relay,
    accountPrincipal: () => "account-1",
  });

  const deliver = async (
    routine: { routeId: string; secret: string },
    deliveryId: string,
    payload: ForwardBody,
    secret = routine.secret,
  ) => {
    const body = new TextEncoder().encode(JSON.stringify(payload));
    const timestamp = String(Math.floor(Date.now() / 1000));
    const signature = signWebhookBody(secret, timestamp, deliveryId, body);
    const { status } = await Effect.runPromise(
      events.receive({ routeId: routine.routeId, deliveryId, timestamp, signature, body }),
    );
    report.statuses[deliveryId] = status;
    await sleep(300);
    return status;
  };
  const routineDeliveries = (agentId: string) =>
    agents.listQueue(agentId).deliveries.filter((delivery) => delivery.sender.kind === "routine");
  const sql = () => {
    sqlite.connection ??= new DatabaseSync(database.path);
    return sqlite.connection;
  };
  const runRows = () =>
    sql()
      .prepare(
        "SELECT run_id, routine_id, agent_id, status, instruction, delivery_id FROM projection_routine_runs ORDER BY created_at",
      )
      .all()
      .map((row) => ({
        runId: String(row.run_id),
        routineId: String(row.routine_id),
        agentId: String(row.agent_id),
        status: String(row.status),
        instruction: String(row.instruction),
        deliveryId: row.delivery_id === null ? null : String(row.delivery_id),
      }));
  const turnThread = (turnId: string | null) =>
    turnId === null
      ? null
      : String(sql().prepare("SELECT thread_id FROM projection_turns WHERE turn_id = ?").get(turnId)?.thread_id ?? "");
  const settled = async (agentId: string) => {
    const deadline = Date.now() + 15_000;
    while (
      agents
        .listQueue(agentId)
        .deliveries.some((delivery) => ["queued", "starting", "running"].includes(delivery.status)) &&
      Date.now() < deadline
    )
      await sleep(50);
  };

  const forwardedStatus = await deliver(helmRoutine, "forward-1", {
    type: "command.ready",
    data: { command: "relay", requestId: "req-1" },
    forwardToAgentId: compass.id,
    instruction: "Compass: check the dispatch queue and report.",
  });
  await settled(compass.id);
  const compassRuns = routineDeliveries(compass.id);
  const forwardedRun = runRows().find((row) => row.agentId === compass.id);
  const compassThread = store.list().find((agent) => agent.id === compass.id)?.threadId ?? null;
  check("A signed forward to an allowed agent gets 202", forwardedStatus === 202, String(forwardedStatus));
  check(
    "The forwarded event is a routine run in the target agent's chat",
    compassRuns.length === 1 &&
      compassRuns[0]?.sender.kind === "routine" &&
      compassRuns[0].sender.routineId === helmRoutine.id &&
      forwardedRun?.routineId === helmRoutine.id &&
      forwardedRun.deliveryId === compassRuns[0].id &&
      mailbox.taskIdForDelivery(compassRuns[0].id) === null,
    JSON.stringify({ compassRuns: compassRuns.length, forwardedRun, helm: routineDeliveries(helm.id).length }),
  );
  check(
    "The forwarded run runs in the target's main chat and succeeds",
    forwardedRun?.status === "succeeded" &&
      compassRuns[0]?.status === "completed" &&
      compassThread !== null &&
      turnThread(compassRuns[0]?.turnId ?? null) === compassThread,
    JSON.stringify({
      run: forwardedRun?.status,
      delivery: compassRuns[0]?.status,
      thread: turnThread(compassRuns[0]?.turnId ?? null),
      compassThread,
    }),
  );
  check(
    "The run uses the body's instruction and carries the event",
    (compassRuns[0]?.text ?? "").includes("Compass: check the dispatch queue and report.") &&
      (compassRuns[0]?.text ?? "").includes("req-1") &&
      !(compassRuns[0]?.text ?? "").includes(helmRoutine.instruction),
    compassRuns[0]?.text.slice(0, 400),
  );
  check(
    "The receiving agent gets no run for the forwarded event",
    routineDeliveries(helm.id).length === 0 && !runRows().some((row) => row.agentId === helm.id),
    JSON.stringify(runRows()),
  );

  const noInstruction = await deliver(helmRoutine, "forward-2", {
    type: "command.ready",
    data: { requestId: "req-2" },
    forwardToAgentId: compass.id,
  });
  const second = routineDeliveries(compass.id).find((delivery) => delivery.text.includes("req-2"));
  check(
    "Without an instruction field the forwarded run uses the routine's instruction",
    noInstruction === 202 && (second?.text ?? "").includes(helmRoutine.instruction),
    `${noInstruction} ${second?.text.slice(0, 200)}`,
  );

  const runsBefore = runRows().length;
  const refused = await deliver(helmRoutine, "forward-3", {
    type: "command.ready",
    data: { requestId: "req-3" },
    forwardToAgentId: scout.id,
  });
  check(
    "A forward to an agent that is not on the allowlist is refused with 400 and starts nothing",
    refused === 400 &&
      runRows().length === runsBefore &&
      routineDeliveries(scout.id).length === 0 &&
      routineDeliveries(helm.id).length === 0,
    `${refused} runs ${runRows().length - runsBefore}`,
  );

  const malformed = await deliver(helmRoutine, "forward-4", {
    type: "command.ready",
    data: {},
    forwardToAgentId: 42,
  });
  check("A forward field that is not an agent ID is refused with 400", malformed === 400, String(malformed));

  const plain = await deliver(helmRoutine, "plain-1", { type: "command.ready", data: { requestId: "req-5" } });
  const plainRun = routineDeliveries(helm.id).find((delivery) => delivery.text.includes("req-5"));
  check(
    "A body without the field starts the receiving routine as before",
    plain === 202 && plainRun !== undefined && plainRun.text.includes(helmRoutine.instruction),
    `${plain} ${routineDeliveries(helm.id).length}`,
  );

  const runsBeforeBad = runRows().length;
  const badSignature = await deliver(
    helmRoutine,
    "forward-6",
    { type: "command.ready", data: { requestId: "req-6" }, forwardToAgentId: compass.id },
    "whsec_wrong_secret_for_this_route_0000",
  );
  check(
    "A forward with a wrong signature is refused with 401 and starts nothing",
    badSignature === 401 && runRows().length === runsBeforeBad,
    `${badSignature} runs ${runRows().length - runsBeforeBad}`,
  );

  const byRoutine = await deliver(thunderRoutine, "forward-7", {
    type: "command.ready",
    data: { requestId: "req-7" },
    forwardToAgentId: compass.id,
  });
  const byOtherRoutine = await deliver(thunderOther, "forward-8", {
    type: "command.ready",
    data: { requestId: "req-8" },
    forwardToAgentId: compass.id,
  });
  check(
    "An allowlist entry for one routine allows that routine only",
    byRoutine === 202 &&
      byOtherRoutine === 400 &&
      routineDeliveries(compass.id).some((delivery) => delivery.text.includes("req-7")) &&
      !routineDeliveries(compass.id).some((delivery) => delivery.text.includes("req-8")),
    `${byRoutine} ${byOtherRoutine}`,
  );

  const self = await deliver(helmRoutine, "self-9", {
    type: "command.ready",
    data: { requestId: "req-9" },
    forwardToAgentId: helm.id,
  });
  check(
    "A forward to the receiving agent itself runs in its own chat as before",
    self === 202 && routineDeliveries(helm.id).some((delivery) => delivery.text.includes("req-9")),
    String(self),
  );

  report.passed = report.checks.every((item) => item.passed);
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
  `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of ${report.checks.length}\n`,
);
