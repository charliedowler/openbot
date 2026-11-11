import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentEvent, QueueDelivery } from "@openbot/contracts/ipc";
import { Effect, Scope } from "effect";
import { afterEach, describe, expect, it } from "vitest";
import { runCauseEffect } from "../effect-boundary";
import { MailboxStore, QUEUE_WINDOW_COMPLETED_LIMIT, QUEUE_WINDOW_ENDED_LIMIT } from "../mailbox-store";
import { OpenBotDatabase } from "../openbot-database";
import { MailboxSync, type MailboxSyncConversation } from "./mailbox-sync";

const AGENT = "agent";
const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), "openbot-queue-window-"));
  const database = new OpenBotDatabase(join(root, "data"));
  const mailbox = new MailboxStore(join(root, "data"), join(root, "shared"), database);
  await runCauseEffect(mailbox.initialize());
  cleanups.push(async () => {
    database.close();
    await rm(root, { recursive: true, force: true });
  });
  return { database, mailbox };
}

async function deliver(
  mailbox: MailboxStore,
  index: number,
  outcome: "completed" | "failed" | "cancelled" | "queued" | "running",
  kind: "user" | "routine" = "user",
): Promise<string> {
  const receipt = await runCauseEffect(
    mailbox.enqueue({
      sender:
        kind === "user"
          ? { kind: "user" }
          : {
              kind: "routine",
              routineId: "routine",
              runId: `run-${index}`,
              routineName: "Check",
              scheduledFor: "2026-11-11T08:00:00.000Z",
            },
      recipientAgentIds: [AGENT],
      text: `Message ${index}`,
      draftIds: [],
    }),
  );
  const id = receipt.deliveries[0]?.id;
  if (!id) throw new Error("No delivery.");
  if (outcome === "queued") return id;
  if (outcome === "cancelled") {
    await runCauseEffect(mailbox.cancel(AGENT, id));
    return id;
  }
  await runCauseEffect(mailbox.markStarting(id));
  if (outcome === "running") {
    await runCauseEffect(mailbox.markRunning(id, `turn-${index}`));
    return id;
  }
  await runCauseEffect(mailbox.markTerminal(id, outcome, outcome === "failed" ? "The provider failed." : null));
  return id;
}

const active = (deliveries: readonly QueueDelivery[]) =>
  deliveries.filter((delivery) => ["queued", "starting", "running"].includes(delivery.status));

describe("MailboxStore.listQueueWindow", () => {
  it("keeps every active delivery and the newest settled ones, in queue order", async () => {
    const { mailbox } = await fixture();
    for (let index = 0; index < 150; index += 1) await deliver(mailbox, index, "completed");
    for (let index = 150; index < 280; index += 1)
      await deliver(mailbox, index, index % 2 === 0 ? "failed" : "cancelled");
    await deliver(mailbox, 280, "running");
    await deliver(mailbox, 281, "queued");
    await deliver(mailbox, 282, "queued");

    const full = mailbox.listQueue(AGENT);
    const window = mailbox.listQueueWindow(AGENT);
    expect(full.deliveries).toHaveLength(283);
    expect(active(window.deliveries)).toEqual(active(full.deliveries));
    const completed = window.deliveries.filter((delivery) => delivery.status === "completed");
    expect(completed).toHaveLength(QUEUE_WINDOW_COMPLETED_LIMIT);
    expect(completed.map((delivery) => delivery.text)).toEqual(
      full.deliveries
        .filter((delivery) => delivery.status === "completed")
        .slice(-QUEUE_WINDOW_COMPLETED_LIMIT)
        .map((delivery) => delivery.text),
    );
    const ended = window.deliveries.filter(
      (delivery) => delivery.status === "failed" || delivery.status === "cancelled",
    );
    expect(ended).toHaveLength(QUEUE_WINDOW_ENDED_LIMIT);
    expect(ended.at(-1)?.text).toBe("Message 279");
    const order = full.deliveries.map((delivery) => delivery.id);
    const positions = window.deliveries.map((delivery) => order.indexOf(delivery.id));
    expect(positions).toEqual([...positions].sort((left, right) => left - right));
    for (const delivery of window.deliveries)
      expect(delivery).toEqual(full.deliveries.find((candidate) => candidate.id === delivery.id));
  });

  it("is the full queue while the history is small", async () => {
    const { mailbox } = await fixture();
    for (let index = 0; index < 20; index += 1) await deliver(mailbox, index, index % 5 === 0 ? "failed" : "completed");
    await deliver(mailbox, 20, "queued");
    expect(mailbox.listQueueWindow(AGENT)).toEqual(mailbox.listQueue(AGENT));
  });
});

describe("MailboxSync.emitQueue", () => {
  async function sync() {
    const { database, mailbox } = await fixture();
    const scope = Effect.runSync(Scope.make());
    const reconciled: QueueDelivery[] = [];
    const events: AgentEvent[] = [];
    const conversation: MailboxSyncConversation = {
      isExecutionThread: () => false,
      snapshot: () => undefined,
      snapshotWithTurn: () => undefined,
      snapshotToUpdate: () => undefined,
      hasPublishedConversation: () => false,
      publishConversation: () => {},
      emitConversation: () => {},
    };
    const mailboxSync = new MailboxSync({
      database,
      mailbox,
      conversation,
      routines: {
        reconcileDelivery: (delivery) => {
          reconciled.push(delivery);
          return false;
        },
        stateChanged: () => {},
      },
      scope: () => scope,
      hooks: { emit: (event) => events.push(event), emitError: () => {}, queueHold: () => null },
    });
    return { mailbox, mailboxSync, reconciled, events };
  }

  it("emits the window and reconciles each settled routine delivery once", async () => {
    const { mailbox, mailboxSync, reconciled, events } = await sync();
    for (let index = 0; index < 120; index += 1)
      await deliver(mailbox, index, "completed", index % 2 === 0 ? "routine" : "user");
    const running = await deliver(mailbox, 120, "running", "routine");
    await deliver(mailbox, 121, "queued", "routine");

    mailboxSync.emitQueue(AGENT);
    expect(reconciled).toHaveLength(62);
    const first = events.at(-1);
    expect(first?.type === "queue-changed" && first.snapshot.deliveries).toEqual(
      mailbox.listQueueWindow(AGENT).deliveries,
    );

    reconciled.length = 0;
    mailboxSync.emitQueue(AGENT);
    expect(reconciled.map((delivery) => delivery.status).sort()).toEqual(["queued", "running"]);

    await runCauseEffect(mailbox.markTerminal(running, "completed"));
    reconciled.length = 0;
    mailboxSync.emitQueue(AGENT);
    expect(reconciled.map((delivery) => [delivery.id === running, delivery.status]).sort()).toEqual([
      [false, "queued"],
      [true, "completed"],
    ]);

    reconciled.length = 0;
    mailboxSync.emitQueue(AGENT);
    expect(reconciled.map((delivery) => delivery.status)).toEqual(["queued"]);
  });

  it("keeps the full queue for listQueue", async () => {
    const { mailbox, mailboxSync, events } = await sync();
    for (let index = 0; index < 130; index += 1) await deliver(mailbox, index, "completed");
    expect(mailboxSync.queueSnapshot(AGENT).deliveries).toHaveLength(130);
    mailboxSync.emitQueue(AGENT);
    const event = events.at(-1);
    expect(event?.type === "queue-changed" && event.snapshot.deliveries.length).toBe(QUEUE_WINDOW_COMPLETED_LIMIT);
  });

  it("sends a delivery that settles in the next snapshot, even when it is older than the window", async () => {
    const { mailbox, mailboxSync, events } = await sync();
    const old = await deliver(mailbox, 0, "queued");
    for (let index = 1; index < 130; index += 1) await deliver(mailbox, index, "cancelled");
    const snapshot = () => {
      const event = events.at(-1);
      return event?.type === "queue-changed" ? event.snapshot : null;
    };
    mailboxSync.emitQueue(AGENT);
    expect(snapshot()?.deliveries.find((delivery) => delivery.id === old)?.status).toBe("queued");
    await runCauseEffect(mailbox.cancel(AGENT, old));
    mailboxSync.emitQueue(AGENT);
    expect(snapshot()?.deliveries.find((delivery) => delivery.id === old)?.status).toBe("cancelled");
    expect(snapshot()?.deliveries).toHaveLength(QUEUE_WINDOW_ENDED_LIMIT + 1);
    mailboxSync.emitQueue(AGENT);
    expect(snapshot()?.deliveries.some((delivery) => delivery.id === old)).toBe(false);
    expect(snapshot()?.deliveries).toHaveLength(QUEUE_WINDOW_ENDED_LIMIT);
  });
});
