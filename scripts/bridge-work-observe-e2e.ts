import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS } from "@openbot/contracts/ipc";
import type { EventJsonValue } from "@openbot/contracts/ipc-events";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";

const OUT = resolve(import.meta.dirname, "../.openbot-build/bridge-work-observe-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-work-cancel-"));
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
        const input = JSON.stringify(params.input);
        const modelFailed = input.lastIndexOf("MODEL_FAIL") > Math.max(input.lastIndexOf("ACKED"), input.lastIndexOf("EXECUTOR_FAIL"));
        const executorFailed = input.lastIndexOf("EXECUTOR_FAIL") > Math.max(input.lastIndexOf("ACKED"), input.lastIndexOf("MODEL_FAIL"));
        if (!modelFailed) write({ method: "item/completed", params: { threadId: params.threadId, turnId, item: {
          type: "mcpToolCall", id: "tool-" + turnId, server: "executor", tool: "execute", status: "completed",
          result: { isError: executorFailed, content: [{ type: "text", text: "fixture-secret-never-return" }] }
        } } });
        write({ method: "item/completed", params: { threadId: params.threadId, turnId, item: { type: "agentMessage", id: "message-" + turnId, text: "Done.", phase: "final_answer" } } });
        write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: turnId, status: modelFailed ? "failed" : "completed", ...(modelFailed ? { error: { message: "Unrelated model failure." } } : {}) } } });
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

const observerModule = existsSync(resolve(import.meta.dirname, "../src/main/bridge-work-observer.ts"))
  ? await import("../src/main/bridge-work-observer")
  : null;
const report: { passed: boolean; checks: string[]; failed: string[] } = { passed: false, checks: [], failed: [] };
async function check(name: string, run: () => Promise<void>) {
  try {
    await run();
    report.checks.push(name);
    process.stdout.write(`PASS ${name}\n`);
  } catch (error) {
    report.failed.push(`${name}: ${String(error)}`);
    process.stdout.write(`FAIL ${name}: ${String(error)}\n`);
  }
}
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, database);
const agents = new AgentService({
  store,
  mailbox,
  browser,
  preferredProvider: "codex",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "codex"),
  timezone: () => "UTC",
});
const cipher = { encrypt: (value: string) => Buffer.from(value), decrypt: (value: Buffer) => value.toString() };
const relay = {
  connected: () => true,
  setEnabled: () => {},
  setAccountActive: () => {},
  refresh: () => {},
  registerRoute: (routeId: string) => Effect.succeed(`https://signal.example.test/v1/webhooks/${routeId}`),
  revokeRoute: () => Effect.void,
};
const observer = observerModule
  ? new observerModule.BridgeWorkObserver(
      database,
      {
        delivery: (deliveryId) => mailbox.getDelivery(deliveryId)?.delivery ?? null,
        turnAnswer: (agentId, turnId) => database.readTurnAssistantMessages(agentId, turnId).at(-1)?.text ?? null,
      },
      (agentId) => store.list().find((agent) => agent.id === agentId)?.name ?? null,
    )
  : undefined;
const events = new HostEventsService({
  routines: agents.routineRecords,
  cipher,
  relay,
  accountPrincipal: () => "test-account",
  ...(observer ? { bridgeWorkObserver: observer } : {}),
});
const until = async (condition: () => boolean) => {
  const deadline = Date.now() + 15000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for native turn.");
    await new Promise((done) => setTimeout(done, 10));
  }
};
try {
  await writeFile(fakeCodex, FAKE_CODEX, { mode: 0o755 });
  await runCauseEffect(agents.initialize());
  const owner = await runCauseEffect(
    store.createAgent({ name: "Canary", description: "Fixture", avatarSeed: "canary", avatarHue: null }),
  );
  const save = async (eventType: string | null) => {
    const saved = await runCauseEffect(
      events.saveRoutine({
        owner: { kind: "agent", id: owner.id },
        name: "Fixture canary receiver",
        instruction: "Handle this native canary event.",
        active: true,
        timezone: "UTC",
        trigger: { kind: "webhook", eventType, filters: [] },
      }),
    );
    const route = agents.routineRecords.routes.list().find((entry) => entry.routineId === saved.routine.id);
    assert.ok(route && saved.secret);
    return { id: saved.routine.id, routeId: route.routeId, secret: saved.secret };
  };
  const canary = await save("bridge.work.canary");
  const inbox = await save(null);
  process.env.OPENBOT_WORK_CANARY_RECEIVERS = JSON.stringify([
    {
      ownerAgentId: owner.id,
      routineId: canary.id,
      sources: [{ kind: "agent", ownerId: owner.id, routineId: inbox.id }],
    },
  ]);
  const deliver = (route: typeof inbox, deliveryId: string, type: string, data: Record<string, EventJsonValue>) => {
    const body = new TextEncoder().encode(JSON.stringify({ type, data }));
    const timestamp = String(Math.floor(Date.now() / 1000));
    return runCauseEffect(
      events.receive({
        routeId: route.routeId,
        deliveryId,
        timestamp,
        signature: signWebhookBody(route.secret, timestamp, deliveryId, body),
        body,
      }),
    );
  };
  const observe = async (deliveryIds: string[]) => {
    const response = await deliver(canary, `observe-${randomUUID()}`, "bridge.work.observe", { deliveryIds });
    assert.equal(response.status, 200);
    assert.ok(response.body);
    return JSON.parse(response.body);
  };
  const runFor = (deliveryId: string) =>
    database.connection
      .prepare(
        "SELECT r.* FROM projection_routine_runs r JOIN projection_webhook_receipts w ON w.run_id=r.run_id WHERE w.delivery_id=?",
      )
      .get(deliveryId);
  const probe = (deliveryId: string, marker: string, extra: Record<string, EventJsonValue> = {}) =>
    deliver(inbox, deliveryId, "bridge.canary.probe", { marker, bridgeWork: { kind: "canary", ...extra } });
  await check("queued is not a failure", async () => {
    const release = mailbox.releaseStart;
    mailbox.releaseStart = () => {};
    try {
      await probe("queued-probe", "QUEUED", { deliveryIds: ["queued-probe"] });
      await until(() => typeof runFor("queued-probe")?.delivery_id === "string");
      const observation = (await observe(["queued-probe"])).observations[0];
      assert.equal(observation.nativeStatus, "queued");
      assert.equal(observation.startedAt, null);
      assert.equal(observation.executor.classification, "not-observed");
    } finally {
      mailbox.releaseStart = release;
    }
    const deliveryId = runFor("queued-probe")?.delivery_id;
    if (typeof deliveryId === "string") await runCauseEffect(agents.cancelQueuedMessage(owner.id, deliveryId));
  });
  for (const [deliveryId, marker, expected] of [
    ["acked-probe", "ACKED", "none"],
    ["executor-error-probe", "EXECUTOR_FAIL", "tool-error"],
    ["model-error-probe", "MODEL_FAIL", "not-observed"],
  ]) {
    await check(`native turn ${marker}`, async () => {
      assert.ok(deliveryId && marker && expected);
      const sentAt = Date.now();
      await probe(deliveryId, marker, { deliveryIds: [deliveryId] });
      await until(() => ["succeeded", "failed"].includes(String(runFor(deliveryId)?.status)));
      const observation = (await observe([deliveryId])).observations[0];
      assert.equal(observation.recipient.agentId, owner.id);
      assert.equal(observation.deliveryId, deliveryId);
      assert.ok(Date.parse(observation.startedAt) >= sentAt - 1000);
      assert.ok(Date.parse(observation.terminalAt) >= Date.parse(observation.startedAt));
      assert.equal(observation.executor.classification, expected);
      if (marker === "ACKED") assert.equal(observation.hasAnswer, true);
      assert.equal(JSON.stringify(observation).includes("fixture-secret-never-return"), false);
    });
  }
  await check("unknown ID and aliases are not failures", async () => {
    const response = await observe(["unknown-probe"]);
    assert.equal(response.observations[0].nativeStatus, "missing-receipt");
    assert.equal(response.observations[0].executor.classification, "not-observed");
    await probe("real-envelope-probe", "ACKED", { deliveryIds: ["metadata-alias"] });
    await until(() => runFor("real-envelope-probe")?.status === "succeeded");
    const alias = (await observe(["metadata-alias"])).observations[0];
    assert.equal(alias.nativeStatus, "missing-receipt");
  });
  await check("bounded response and read-only metadata", async () => {
    const before = database.connection.prepare("SELECT COUNT(*) AS n FROM orchestration_events").get()?.n;
    const response = await observe(Array.from({ length: 20 }, (_, n) => `unknown-${n}-${"a".repeat(220)}`));
    assert.equal(response.observations.length, 20);
    assert.ok(Buffer.byteLength(JSON.stringify(response)) < 32768);
    const after = database.connection.prepare("SELECT COUNT(*) AS n FROM orchestration_events").get()?.n;
    assert.equal(after, before);
    assert.equal(
      (
        await deliver(canary, "oversized-observe", "bridge.work.observe", {
          deliveryIds: Array.from({ length: 21 }, (_, n) => `id-${n}`),
        })
      ).status,
      400,
    );
    await assert.rejects(runCauseEffect(events.testRoutine({ owner: { kind: "agent", id: owner.id }, id: canary.id })));
  });
} catch (error) {
  report.failed.push(String(error));
  process.stdout.write(`${String(error)}\n`);
} finally {
  await runCauseEffect(agents.stop()).catch(() => {});
  database.close();
  report.passed = report.failed.length === 0;
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}
process.stdout.write(`${report.checks.length} passed, ${report.failed.length} failed\n`);
if (!report.passed) process.exitCode = 1;
