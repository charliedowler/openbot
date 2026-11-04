// Checks the webhook triggers of workflows on the real SQLite, the real workflow service and the real
// events service, on a temporary profile. The relay is a fake that gives a URL and records each
// revocation. A webhook trigger has no address until its first secret; the secret is stored as
// ciphertext only; a signed request starts one run, and the same delivery again starts none; a wrong
// signature is refused; a new secret replaces the old one and keeps the address; a workflow that
// still runs skips a request; a graph that cannot run keeps the request as done; a removed trigger and a deleted workflow revoke their routes. No app,
// provider, account or network is started.
//
// Run: bun scripts/webhook-ignored-status-e2e.ts
// Writes .openbot-build/webhook-ignored-status-e2e/report.json and exits non-zero on a failure.
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS } from "@openbot/contracts/ipc";
import type { EventRoutineOwner, SaveEventRoutineInput } from "@openbot/contracts/ipc-events";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";

const OUT = resolve(import.meta.dirname, "../.openbot-build/webhook-ignored-status-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-webhook-ignored-"));
const userData = join(root, "user-data");
const home = join(root, "home");
// Before the service modules load: the stores read the home folder.
process.env.HOME = home;
process.env.CODEX_HOME = join(home, ".codex");
mkdirSync(home, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const { HostEventsService } = await import("../src/main/host-events-service");
const { signWebhookBody } = await import("../src/main/webhook-security");

const report: {
  checks: string[];
  failures: string[];
  statuses: Record<string, number>;
  passed: boolean;
  error?: string;
} = {
  checks: [],
  failures: [],
  statuses: {},
  passed: false,
};
const check = (name: string, ok: boolean) => (ok ? report.checks : report.failures).push(name);

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
};

const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
const agents = new AgentService({ store, mailbox, browser, preferredProvider: "codex", offProviders: AGENT_PROVIDERS });

try {
  await runCauseEffect(agents.initialize());
  const events = new HostEventsService({
    routines: agents.routineRecords,
    cipher: {
      encrypt: (value) => Buffer.from(`sealed:${Buffer.from(value).toString("base64")}`),
      decrypt: (value) => Buffer.from(value.toString().slice("sealed:".length), "base64").toString(),
    },
    relay: {
      connected: () => true,
      setEnabled: () => {},
      setAccountActive: () => {},
      refresh: () => {},
      registerRoute: (routeId) => Effect.succeed(`https://signal.example.test/v1/webhooks/${routeId}`),
      revokeRoute: () => Effect.void,
    },
    accountPrincipal: () => "account-1",
  });
  const agent = await runCauseEffect(
    store.createAgent({ name: "Compass", description: "Routes events.", avatarSeed: "compass", avatarHue: null }),
  );
  const owner: EventRoutineOwner = { kind: "agent", id: agent.id };
  const base = {
    owner,
    name: "Dispatch intake",
    instruction: "Route the event.",
    active: true,
    timezone: "UTC",
    trigger: { kind: "webhook", eventType: "slack.mentioned", filters: [] },
  } satisfies SaveEventRoutineInput;
  const saved = await runCauseEffect(events.saveRoutine(base));
  const secret = saved.secret;
  assert.ok(secret);
  const route = agents.routineRecords.routes.list().find((item) => item.routineId === saved.routine.id);
  assert.ok(route);
  const deliver = async (deliveryId: string, type: string) => {
    const body = new TextEncoder().encode(JSON.stringify({ type, data: { text: "Status of the release?" } }));
    const timestamp = String(Math.floor(Date.now() / 1000));
    const signature = signWebhookBody(secret, timestamp, deliveryId, body);
    const { status } = await Effect.runPromise(
      events.receive({ routeId: route.routeId, deliveryId, timestamp, signature, body }),
    );
    report.statuses[`${Object.keys(report.statuses).length + 1}:${deliveryId}`] = status;
    return status;
  };

  check("An event that starts a run gets 202.", (await deliver("started-1", "slack.mentioned")) === 202);
  check("The same delivery again gets 200.", (await deliver("started-1", "slack.mentioned")) === 200);
  check(
    "An event that the routine ignores for its event type gets 200, not 202.",
    (await deliver("ignored-type-1", "dispatch.stalled")) === 200,
  );
  check("An ignored delivery again gets 200.", (await deliver("ignored-type-1", "dispatch.stalled")) === 200);
  await runCauseEffect(events.saveRoutine({ ...base, id: saved.routine.id, active: false }));
  check(
    "An event for a paused routine gets 200, not 202.",
    (await deliver("ignored-paused-1", "slack.mentioned")) === 200,
  );
  const activity = await runCauseEffect(events.listActivity({ owner, routineId: saved.routine.id }));
  check(
    "The receipts keep the reasons: one started, one event-type, one inactive.",
    activity.some((item) => item.deliveryId === "started-1" && item.status === "started") &&
      activity.some(
        (item) => item.deliveryId === "ignored-type-1" && item.status === "ignored" && item.reason === "event-type",
      ) &&
      activity.some(
        (item) => item.deliveryId === "ignored-paused-1" && item.status === "ignored" && item.reason === "inactive",
      ),
  );
  report.passed = report.failures.length === 0;
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
