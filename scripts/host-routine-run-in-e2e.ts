// Checks the host control routes for where an agent routine runs. The host watchdog and the
// operator cannot open the app on this server, and a routine that runs in the agent's chat holds
// the chat behind each webhook run. GET /v1/routines lists the agent routines. POST
// /v1/routines/update takes routineId and runIn (chat or thread) and saves through
// HostEventsService.saveRoutine, the path of the app's routine editor. A routine set to run in a
// thread still coalesces its webhook events: one queued run for the routine.
//
// Runs the real agent service, the real HostEventsService with a stand-in relay, the real
// ServerMode on a temporary socket and a stand-in Codex CLI. No app, account or network is used.
//
// Run: node --import tsx scripts/host-routine-run-in-e2e.ts
// Writes .openbot-build/host-routine-run-in-e2e/report.json and exits non-zero on a failure.
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { AGENT_PROVIDERS, type CentralAuthState } from "@openbot/contracts/ipc";
import type { EventRoutineOwner, SaveEventRoutineInput } from "@openbot/contracts/ipc-events";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { ServerModeOptions } from "../src/main/server-mode";

const OUT = resolve(import.meta.dirname, "../.openbot-build/host-routine-run-in-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-routine-run-in-"));
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

const { ServerMode } = await import("../src/main/server-mode");

const CHECKS = 6;
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
  const routineFor = async (owner: EventRoutineOwner, name: string) => {
    const saved = await runCauseEffect(
      events.saveRoutine({
        owner,
        name,
        instruction: `${name}: handle the event.`,
        active: true,
        timezone: "UTC",
        trigger: { kind: "webhook", eventType: "inbox.event", filters: [{ pointer: "/kind", value: "mail" }] },
      } satisfies SaveEventRoutineInput),
    );
    const route = agents.routineRecords.routes.list().find((item) => item.routineId === saved.routine.id);
    if (!saved.secret || !route) throw new Error(`No webhook route for ${name}.`);
    return { id: saved.routine.id, routeId: route.routeId, secret: saved.secret };
  };
  const inbox = await routineFor({ kind: "agent", id: jeffrey.id }, "Executor swarm inbox");
  const channel = await runCauseEffect(
    agents.channels.command(
      {
        type: "save",
        operationId: "channel-1",
        channelId: "channel-run-in",
        draft: {
          name: "Run place",
          title: "",
          instructions: "",
          members: [{ agentId: jeffrey.id }],
          leadAgentId: jeffrey.id,
        },
      },
      { id: "local", name: "Example" },
    ),
  );
  const channelRoutine = await routineFor({ kind: "channel", id: channel.id }, "Channel inbox");

  const runtime = await mkdtemp(join("/tmp", "obr-"));
  const socket = join(runtime, "control.sock");
  const authState: CentralAuthState = { status: "signed_out" };
  const refused = () => Effect.die(new Error("Not used by this check."));
  const options: ServerModeOptions = {
    environment: { controlSocketPath: socket },
    version: "fixture",
    centralAuth: {
      getState: () => authState,
      requestEmailCode: refused,
      verifyEmailCode: refused,
      canPersistSession: () => true,
      redeemServerClaim: refused,
      logout: refused,
      createMobileConnect: refused,
      cancelCompanySignIn: refused,
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
      configure: refused,
      start: refused,
      updateIdentity: refused,
      getMobileConnectHost: () => null,
    },
    memory: null,
    usageLimits: agents,
    agents,
    routines: events,
    channelIds: () => agents.channels.store.ids(),
    onError: () => {},
  };
  const server = new ServerMode(options);
  await runCauseEffect(server.listen());
  const control = (
    path: string,
    method: string,
    body = "",
  ): Promise<{ status: number; lines: Record<string, string> }> =>
    new Promise((done, fail) => {
      const req = request(
        {
          socketPath: socket,
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

  const listed = await control("/v1/routines", "GET");
  const row = listed.lines[`routine.${inbox.id}`];
  check(
    "GET /v1/routines lists the agent routine with its agent, name, place and state, and no channel routine",
    listed.status === 200 &&
      listed.lines.routines === "1" &&
      row !== undefined &&
      JSON.stringify(JSON.parse(row)) ===
        JSON.stringify({
          agentId: jeffrey.id,
          name: "Executor swarm inbox",
          runIn: "chat",
          active: true,
          trigger: "webhook",
        }) &&
      listed.lines[`routine.${channelRoutine.id}`] === undefined,
    JSON.stringify(listed),
  );

  const before = agents.routineRecords.get({ kind: "agent", id: jeffrey.id }, inbox.id);
  const refusals = {
    noRoutine: await control("/v1/routines/update", "POST", "runIn=thread"),
    badPlace: await control("/v1/routines/update", "POST", `routineId=${inbox.id}&runIn=elsewhere`),
    otherField: await control("/v1/routines/update", "POST", `routineId=${inbox.id}&runIn=thread&name=x`),
    repeated: await control("/v1/routines/update", "POST", `routineId=${inbox.id}&runIn=thread&runIn=chat`),
    unknown: await control("/v1/routines/update", "POST", "routineId=routine-nobody&runIn=thread"),
    channel: await control("/v1/routines/update", "POST", `routineId=${channelRoutine.id}&runIn=thread`),
    wrongMethod: await control("/v1/routines/update", "GET"),
    listPost: await control("/v1/routines", "POST", "routineId=x"),
  };
  const unchanged = agents.routineRecords.get({ kind: "agent", id: jeffrey.id }, inbox.id);
  check(
    "Bad input gets 400, an unknown routine 404, a channel routine 409, another method 404, and nothing changes",
    refusals.noRoutine.status === 400 &&
      refusals.noRoutine.lines.error === "invalid_routine" &&
      refusals.badPlace.status === 400 &&
      refusals.badPlace.lines.error === "invalid_run_in" &&
      refusals.otherField.status === 400 &&
      refusals.otherField.lines.error === "unknown_field" &&
      refusals.repeated.status === 400 &&
      refusals.repeated.lines.error === "repeated_field" &&
      refusals.unknown.status === 404 &&
      refusals.unknown.lines.error === "unknown_routine" &&
      refusals.channel.status === 409 &&
      refusals.channel.lines.error === "channel_routine" &&
      refusals.wrongMethod.status === 404 &&
      refusals.listPost.status === 404 &&
      JSON.stringify(unchanged) === JSON.stringify(before),
    JSON.stringify(
      Object.fromEntries(Object.entries(refusals).map(([key, value]) => [key, [value.status, value.lines.error]])),
    ),
  );

  const moved = await control("/v1/routines/update", "POST", `routineId=${inbox.id}&runIn=thread`);
  const after = agents.routineRecords.get({ kind: "agent", id: jeffrey.id }, inbox.id);
  const routeAfter = agents.routineRecords.routes.list().find((item) => item.routineId === inbox.id);
  check(
    "runIn=thread saves through the routine editor's path: only the place changes, the route and secret stay",
    moved.status === 200 &&
      moved.lines.runIn === "thread" &&
      moved.lines.agentId === jeffrey.id &&
      after?.runIn === "thread" &&
      before !== null &&
      after !== null &&
      after.name === before.name &&
      after.instruction === before.instruction &&
      after.active === before.active &&
      JSON.stringify(after.trigger.kind === "webhook" ? [after.trigger.eventType, after.trigger.filters] : null) ===
        JSON.stringify(before.trigger.kind === "webhook" ? [before.trigger.eventType, before.trigger.filters] : null) &&
      routeAfter?.routeId === inbox.routeId,
    JSON.stringify({ moved, runIn: after?.runIn, routeId: routeAfter?.routeId }),
  );

  const deliver = async (deliveryId: string, eventId: string) => {
    const body = new TextEncoder().encode(JSON.stringify({ type: "inbox.event", data: { kind: "mail", eventId } }));
    const timestamp = String(Math.floor(Date.now() / 1000));
    const signature = signWebhookBody(inbox.secret, timestamp, deliveryId, body);
    const { status } = await Effect.runPromise(
      events.receive({ routeId: inbox.routeId, deliveryId, timestamp, signature, body }),
    );
    report.statuses[deliveryId] = status;
    await sleep(200);
    return status;
  };
  const sql = () => {
    sqlite.connection ??= new DatabaseSync(database.path);
    return sqlite.connection;
  };
  const runRows = () =>
    sql()
      .prepare(
        "SELECT run_id, status, delivery_id FROM projection_routine_runs WHERE routine_id = ? ORDER BY created_at, run_id",
      )
      .all(inbox.id)
      .map((item) => ({
        runId: String(item.run_id),
        status: String(item.status),
        deliveryId: item.delivery_id === null ? null : String(item.delivery_id),
      }));
  const receiptRun = (deliveryId: string) => {
    const receipt = sql()
      .prepare("SELECT status, run_id FROM projection_webhook_receipts WHERE delivery_id = ?")
      .get(deliveryId);
    return receipt
      ? { status: String(receipt.status), runId: receipt.run_id === null ? null : String(receipt.run_id) }
      : null;
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
  const first = await deliver("event-1", "e-1");
  await waitUntil("the first run to start", () => deliveryStatus(runRows()[0]?.deliveryId ?? null) === "running");
  const second = await deliver("event-2", "e-2");
  await waitUntil("the second run to queue", () => runRows()[1]?.deliveryId != null);
  const more: number[] = [];
  for (const id of ["event-3", "event-4", "event-5"]) more.push(await deliver(id, id));
  const rows = runRows();
  const threaded = rows.every(
    (item) => item.deliveryId !== null && mailbox.taskIdForDelivery(item.deliveryId) !== null,
  );
  check(
    "A routine that runs in a thread still queues one run for its webhook events: the others name the queued run",
    first === 202 &&
      second === 202 &&
      more.every((status) => status === 202) &&
      rows.length === 2 &&
      threaded &&
      ["event-3", "event-4", "event-5"].every((id) => receiptRun(id)?.runId === rows[1]?.runId),
    JSON.stringify({
      first,
      second,
      more,
      runs: rows.length,
      threaded,
      receipts: ["event-3", "event-4", "event-5"].map(receiptRun),
    }),
  );
  rmSync(holdFile);
  await waitUntil(
    "both runs to finish",
    () => runRows().length === 2 && runRows().every((item) => item.status === "succeeded"),
    30_000,
  );
  check(
    "Both thread runs finish",
    runRows().every((item) => item.status === "succeeded"),
    JSON.stringify(runRows()),
  );

  const back = await control("/v1/routines/update", "POST", `routineId=${inbox.id}&runIn=chat`);
  const listedBack = await control("/v1/routines", "GET");
  check(
    "runIn=chat moves the routine back to the chat, and the list shows it",
    back.status === 200 &&
      back.lines.runIn === "chat" &&
      agents.routineRecords.get({ kind: "agent", id: jeffrey.id }, inbox.id)?.runIn === "chat" &&
      JSON.parse(listedBack.lines[`routine.${inbox.id}`] ?? "{}").runIn === "chat",
    JSON.stringify({ back, listedBack }),
  );
  await runCauseEffect(server.close());
  await rm(runtime, { recursive: true, force: true });

  report.passed = report.checks.length === CHECKS && report.checks.every((item) => item.passed);
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
  `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of ${CHECKS}\n`,
);
