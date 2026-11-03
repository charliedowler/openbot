// @vitest-environment node
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect } from "effect";
import { afterEach, describe, expect, it } from "vitest";
import { decodeWebhookBody } from "../../main/host-events-service";
import { runCauseEffect } from "../effect-boundary";
import { MailboxStore } from "../mailbox-store";
import { OpenBotDatabase } from "../openbot-database";
import { AgentWaitStore } from "./agent-wait-store";
import { AgentWaits, type AgentWaitsHooks } from "./agent-waits";

const AGENT = "agent-compass";
const RESULTS = "routine-pod-results";
const INBOX = "routine-direct-inbox";
const roots: string[] = [];
const databases: OpenBotDatabase[] = [];

afterEach(async () => {
  for (const database of databases.splice(0)) database.close();
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function harness(waits: Array<[id: string, routineId: string | null]>) {
  const root = await mkdtemp(join(tmpdir(), "openbot-webhook-wait-"));
  roots.push(root);
  const database = new OpenBotDatabase(root);
  databases.push(database);
  await runCauseEffect(database.initialize());
  const db = database.connection;
  db.exec("PRAGMA foreign_keys = OFF");
  for (const routineId of [RESULTS, INBOX])
    db.prepare(
      `INSERT INTO projection_agent_routines
        (routine_id, agent_id, name, instruction, active, timezone, created_at, updated_at, last_event_sequence)
        VALUES (?, ?, ?, 'Read the event.', 1, 'UTC', '2026-11-03', '2026-11-03', 1)`,
    ).run(routineId, AGENT, routineId);
  for (const [id, routineId] of waits)
    db.prepare(
      `INSERT INTO agent_waits (wait_id, agent_id, request_delivery_id, request_text, note, routine_id, created_at)
        VALUES (?, ?, ?, 'Dispatch work to a pod.', 'Waiting for the pod.', ?, '2026-11-03T00:00:00.000Z')`,
    ).run(id, AGENT, `request-${id}`, routineId);
  const resumed: string[] = [];
  const hooks: AgentWaitsHooks = {
    emit: () => {},
    ownsRoutine: () => true,
    hasRoutine: () => true,
    requireKnownAgent: () => {},
    channelWaitState: () => "waiting",
    channelWaitEventHandled: () => false,
    resumeChannelWait: (item) =>
      Effect.sync(() => {
        resumed.push(item.id);
        return true;
      }),
  };
  const store = new AgentWaitStore({ database });
  const service = new AgentWaits({ store, mailbox: new MailboxStore(root, root, database), hooks });
  const wake = (routineId: string, waitId: string | null, fallback: boolean) =>
    Effect.runSync(service.wakeForRun({ agentId: AGENT, routineId }, "delivery-result", waitId, fallback));
  const woken = () =>
    store
      .list(AGENT)
      .filter((item) => item.wakeDeliveryId === "delivery-result")
      .map((item) => item.id)
      .sort();
  return { wake, woken, resumed };
}

const dispatchWaits = (): Array<[string, string | null]> => [
  ["channel:task-a", RESULTS],
  ["channel:task-b", RESULTS],
  ["channel:task-c", RESULTS],
  ["own-any", null],
  ["channel:inbox", INBOX],
];

describe("a signed webhook request that names a wait", () => {
  it("wakes every wait of the routine when it names none, as before", async () => {
    const run = await harness(dispatchWaits());
    run.wake(RESULTS, null, true);
    expect(run.woken()).toEqual(["channel:task-a", "channel:task-b", "channel:task-c", "own-any"]);
  });

  it("wakes only the named channel wait of this routine", async () => {
    const run = await harness(dispatchWaits());
    run.wake(RESULTS, "channel:task-b", true);
    expect(run.woken()).toEqual(["channel:task-b"]);
    expect(run.resumed).toEqual(["channel:task-b"]);
  });

  it("wakes only a named wait that has no routine", async () => {
    const run = await harness(dispatchWaits());
    run.wake(RESULTS, "own-any", true);
    expect(run.woken()).toEqual(["own-any"]);
  });

  it("wakes as if unnamed when the named wait is gone", async () => {
    const run = await harness(dispatchWaits());
    run.wake(RESULTS, "channel:closed", true);
    expect(run.woken()).toEqual(["channel:task-a", "channel:task-b", "channel:task-c", "own-any"]);
  });

  it("never wakes a named wait that another routine owns", async () => {
    const run = await harness(dispatchWaits());
    run.wake(RESULTS, "channel:inbox", true);
    expect(run.woken()).toEqual(["channel:task-a", "channel:task-b", "channel:task-c", "own-any"]);
  });

  it("keeps the local script rule: a missing named wait wakes nothing", async () => {
    const run = await harness(dispatchWaits());
    run.wake(RESULTS, "channel:closed", false);
    expect(run.woken()).toEqual([]);
  });
});

describe("decodeWebhookBody", () => {
  const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));

  it("reads a top-level waitId next to type and data", () => {
    expect(decodeWebhookBody(encode({ type: "pod.result", waitId: "channel:task-b", data: { ok: true } }))).toEqual({
      type: "pod.result",
      occurredAt: null,
      data: { ok: true },
      waitId: "channel:task-b",
    });
  });

  it("leaves waitId out when the body has none", () => {
    expect(decodeWebhookBody(encode({ type: "pod.result", data: null }))).toEqual({
      type: "pod.result",
      occurredAt: null,
      data: null,
    });
  });

  it("refuses a waitId that is not a wait id", () => {
    expect(decodeWebhookBody(encode({ type: "pod.result", waitId: "", data: null }))).toBeNull();
    expect(decodeWebhookBody(encode({ type: "pod.result", waitId: 7, data: null }))).toBeNull();
    expect(decodeWebhookBody(encode({ type: "pod.result", waitId: "x".repeat(129), data: null }))).toBeNull();
  });
});
