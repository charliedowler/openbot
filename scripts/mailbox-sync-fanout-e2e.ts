// Checks the conversation syncs of queue changes for an agent that many teammates write to. On 11
// November the mailbox held 17,249 messages and 18,006 deliveries, and each queue change synced the
// chat of the recipient and of every agent that ever sent it a message: each sync read the whole
// mailbox. A sync of a teammate is needed only when a delivery of a message that the teammate sent
// or got changed, when the queue around its queued message moved, or when its reactions changed.
//
// After each change, every chat is compared with a new sync of a copy of it: the chats must be the
// same as a sync of every agent gives. The syncs are counted as mailbox reads.
//
// Runs the real mailbox store, SQLite and `MailboxSync` on a temporary profile. The chats in memory
// and the routine scheduler are injected. No app, provider, account or network is started.
//
// Run: node --import tsx scripts/mailbox-sync-fanout-e2e.ts
// Writes .openbot-build/mailbox-sync-fanout-e2e/report.json and exits non-zero on a failure.
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { ConversationMessage, ConversationSnapshot } from "@openbot/contracts/ipc";
import { Effect, Scope } from "effect";
import { MailboxSync, type MailboxSyncConversation, type MailboxSyncOptions } from "../src/backend/agent/mailbox-sync";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MAILBOX_CONVERSATION_MESSAGE_LIMIT, MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const OUT = resolve(import.meta.dirname, "../.openbot-build/mailbox-sync-fanout-e2e");
const RECIPIENT = "recipient";
const SENDERS = Array.from({ length: 24 }, (_, index) => `sender-${index}`);
const MESSAGES_PER_SENDER = 5;

interface Step {
  name: string;
  senderSyncs: number;
  recipientSyncs: number;
}

interface Report {
  checks: string[];
  steps: Step[];
  passed: boolean;
  error?: string;
}

const report: Report = { checks: [], steps: [], passed: false };
const root = await mkdtemp(join(tmpdir(), "openbot-mailbox-sync-fanout-"));
const scope = Effect.runSync(Scope.make());
const snapshots = new Map<string, ConversationSnapshot>();
const published = new Set<string>();
const syncs = new Map<string, number>();

function snapshotFor(agentId: string): ConversationSnapshot {
  return {
    agentId,
    threadId: `thread-${agentId}`,
    activeTurnId: null,
    revision: 0,
    messages: [
      {
        id: `answer-${agentId}`,
        author: "assistant",
        text: "An earlier answer.",
        createdAt: "2026-01-01T00:00:00.000Z",
        status: "completed",
      },
    ],
  };
}

const conversation: MailboxSyncConversation = {
  isExecutionThread: () => false,
  snapshot: (agentId: string) => snapshots.get(agentId),
  snapshotWithTurn: () => undefined,
  snapshotToUpdate: (agentId: string) => snapshots.get(agentId),
  hasPublishedConversation: (agentId: string) => published.has(agentId),
  publishConversation: (snapshot: ConversationSnapshot) => {
    published.add(snapshot.agentId);
  },
  emitConversation: (snapshot: ConversationSnapshot) => {
    published.add(snapshot.agentId);
  },
};

const routines: MailboxSyncOptions["routines"] = { reconcileDelivery: () => false, stateChanged: () => {} };

const database = new OpenBotDatabase(join(root, "data"));
const mailbox = new MailboxStore(join(root, "data"), join(root, "shared"), database);
const sync = new MailboxSync({
  database,
  mailbox,
  conversation,
  routines,
  scope: () => scope,
  hooks: { emit: () => {}, emitError: () => {}, queueHold: () => null },
});
const read = mailbox.conversationMessages.bind(mailbox);
let counting = true;
mailbox.conversationMessages = (...args: Parameters<typeof read>) => {
  if (counting) syncs.set(args[0], (syncs.get(args[0]) ?? 0) + 1);
  return read(...args);
};

function step(name: string): Step {
  let senderSyncs = 0;
  for (const sender of SENDERS) senderSyncs += syncs.get(sender) ?? 0;
  const result = { name, senderSyncs, recipientSyncs: syncs.get(RECIPIENT) ?? 0 };
  report.steps.push(result);
  syncs.clear();
  return result;
}

function sameAsFullSync(label: string): void {
  counting = false;
  try {
    for (const [agentId, snapshot] of snapshots) {
      const copy = structuredClone(snapshot);
      const changed = sync.syncMailboxMessages(copy);
      assert.deepEqual(copy.messages, snapshot.messages, `${label}: the chat of ${agentId} differs from a full sync.`);
      assert.equal(changed, false, `${label}: a full sync changes the chat of ${agentId}.`);
    }
  } finally {
    counting = true;
  }
}

const card = (agentId: string, messageId: string, recipient = RECIPIENT) =>
  snapshots
    .get(agentId)
    ?.messages.find((message: ConversationMessage) => message.exchange?.messageId === messageId)
    ?.exchange?.deliveries?.find((delivery) => delivery.recipientAgentId === recipient)?.status;

try {
  await runCauseEffect(mailbox.initialize());
  for (const sender of SENDERS) {
    for (let index = 0; index < MESSAGES_PER_SENDER; index += 1) {
      const receipt = await runCauseEffect(
        mailbox.enqueue({
          sender: { kind: "agent", agentId: sender },
          recipientAgentIds: [RECIPIENT],
          text: `Report ${index} from ${sender}`,
          idempotencyKey: `agent:${sender}:turn-${index}:${index}`,
        }),
      );
      const delivery = receipt.deliveries[0];
      assert.ok(delivery);
      await runCauseEffect(mailbox.markStarting(delivery.id));
      await runCauseEffect(mailbox.markTerminal(delivery.id, "completed"));
    }
    snapshots.set(sender, snapshotFor(sender));
  }
  snapshots.set(RECIPIENT, snapshotFor(RECIPIENT));
  syncs.clear();
  sync.emitQueue(RECIPIENT);
  sameAsFullSync("first queue read");
  step("first queue read");
  report.checks.push("The first queue read syncs every chat, and each chat is the same as a full sync.");

  const routine = await runCauseEffect(
    mailbox.enqueue({
      sender: {
        kind: "routine",
        routineId: "routine-1",
        runId: "run-1",
        routineName: "Morning check",
        scheduledFor: "2026-01-01T08:00:00.000Z",
      },
      recipientAgentIds: [RECIPIENT],
      text: "Check the overnight builds.",
      draftIds: [],
    }),
  );
  sync.emitQueue(RECIPIENT);
  const routineDelivery = routine.deliveries[0];
  assert.ok(routineDelivery);
  await runCauseEffect(mailbox.markStarting(routineDelivery.id));
  await runCauseEffect(mailbox.markRunning(routineDelivery.id, "turn-routine"));
  sync.emitQueue(RECIPIENT);
  await runCauseEffect(mailbox.markTerminal(routineDelivery.id, "completed"));
  sync.emitQueue(RECIPIENT);
  await Promise.resolve();
  sameAsFullSync("routine run");
  const routineStep = step("routine run, 3 queue changes");
  assert.ok(routineStep.recipientSyncs >= 1, "The recipient's chat is not synced for its own queue.");

  const update = await runCauseEffect(
    mailbox.enqueue({
      sender: { kind: "agent", agentId: "sender-3" },
      recipientAgentIds: [RECIPIENT],
      text: "Status update from sender-3",
      idempotencyKey: "agent:sender-3:turn-update:update",
    }),
  );
  const updateDelivery = update.deliveries[0];
  assert.ok(updateDelivery);
  sync.emitQueue(RECIPIENT);
  assert.equal(card("sender-3", update.messageId), "queued");
  await runCauseEffect(mailbox.markStarting(updateDelivery.id));
  await runCauseEffect(mailbox.markRunning(updateDelivery.id, "turn-update"));
  sync.emitQueue(RECIPIENT);
  assert.equal(card("sender-3", update.messageId), "running");
  await runCauseEffect(mailbox.markTerminal(updateDelivery.id, "completed"));
  sync.emitQueue(RECIPIENT);
  assert.equal(card("sender-3", update.messageId), "completed");
  await Promise.resolve();
  sameAsFullSync("teammate message");
  const teammateStep = step("one teammate message, 3 queue changes");

  const shared = await runCauseEffect(
    mailbox.enqueue({
      sender: { kind: "agent", agentId: "sender-5" },
      recipientAgentIds: [RECIPIENT, "sender-7"],
      text: "A note for both of you.",
      idempotencyKey: "agent:sender-5:turn-shared:shared",
    }),
  );
  sync.emitQueue(RECIPIENT);
  sync.emitQueue("sender-7");
  const sharedDelivery = shared.deliveries.find((delivery) => delivery.recipientAgentId === RECIPIENT);
  assert.ok(sharedDelivery);
  await runCauseEffect(mailbox.markStarting(sharedDelivery.id));
  await runCauseEffect(mailbox.markTerminal(sharedDelivery.id, "completed"));
  sync.emitQueue(RECIPIENT);
  const incoming = snapshots
    .get("sender-7")
    ?.messages.find(
      (message) => message.exchange?.direction === "incoming" && message.exchange.messageId === shared.messageId,
    );
  assert.equal(
    incoming?.exchange?.deliveries?.find((delivery) => delivery.recipientAgentId === RECIPIENT)?.status,
    "completed",
    "A co-recipient's copy does not show the recipient's delivery.",
  );
  await Promise.resolve();
  sameAsFullSync("shared message");
  const sharedStep = step("one message to the recipient and a teammate");
  report.checks.push(
    "A teammate's own message shows queued, running and completed, and a teammate that got the same message sees the recipient's delivery end.",
  );

  const queued: string[] = [];
  for (const sender of ["sender-10", "sender-11"]) {
    const receipt = await runCauseEffect(
      mailbox.enqueue({
        sender: { kind: "agent", agentId: sender },
        recipientAgentIds: [RECIPIENT],
        text: `A queued report from ${sender}`,
        idempotencyKey: `agent:${sender}:turn-queued:queued`,
      }),
    );
    queued.push(receipt.messageId);
  }
  const blocker = await runCauseEffect(
    mailbox.enqueue({
      sender: { kind: "user" },
      recipientAgentIds: [RECIPIENT],
      text: "The person's own request.",
    }),
  );
  sync.emitQueue(RECIPIENT);
  for (let index = 0; index < 5; index += 1) sync.emitQueue(RECIPIENT);
  await Promise.resolve();
  sameAsFullSync("burst");
  const burstStep = step("a queue of three, then 5 queue changes in one tick");
  assert.ok(blocker.deliveries[0]);
  await runCauseEffect(mailbox.setReaction("sender-9", `answer-sender-9`, { kind: "user" }, "👍"));
  sync.emitQueue(RECIPIENT);
  assert.equal(
    snapshots.get("sender-9")?.messages.find((message) => message.id === "answer-sender-9")?.reaction,
    "👍",
    "A teammate's reaction does not reach its chat.",
  );
  await Promise.resolve();
  sameAsFullSync("reaction");
  const reactionStep = step("a reaction in a teammate's chat");
  report.checks.push("Queued positions, a burst in one tick and a teammate's reaction give the chats of a full sync.");

  const older = await runCauseEffect(
    mailbox.enqueue({
      sender: { kind: "user" },
      recipientAgentIds: ["reader"],
      text: "An old request that still runs.",
    }),
  );
  const olderDelivery = older.deliveries[0];
  assert.ok(olderDelivery);
  await runCauseEffect(mailbox.markStarting(olderDelivery.id));
  await runCauseEffect(mailbox.markRunning(olderDelivery.id, "turn-old"));
  for (let index = 0; index < MAILBOX_CONVERSATION_MESSAGE_LIMIT + 20; index += 1) {
    const receipt = await runCauseEffect(
      mailbox.enqueue({ sender: { kind: "user" }, recipientAgentIds: ["reader"], text: `Request ${index}` }),
    );
    const delivery = receipt.deliveries[0];
    assert.ok(delivery);
    await runCauseEffect(mailbox.markStarting(delivery.id));
    await runCauseEffect(mailbox.markTerminal(delivery.id, "completed"));
  }
  const readerMessages = mailbox.conversationMessages("reader");
  assert.equal(readerMessages.length, MAILBOX_CONVERSATION_MESSAGE_LIMIT + 1);
  assert.equal(readerMessages[0]?.id, olderDelivery.id, "The old running request is not the first message.");
  assert.equal(readerMessages.at(-1)?.text, `Request ${MAILBOX_CONVERSATION_MESSAGE_LIMIT + 19}`);
  report.checks.push(
    `A chat read gives the newest ${MAILBOX_CONVERSATION_MESSAGE_LIMIT} completed requests and an older request that still runs, in order.`,
  );

  const quiet = [routineStep, burstStep, reactionStep];
  for (const item of quiet)
    assert.ok(item.senderSyncs <= 3, `${item.name}: ${item.senderSyncs} teammate syncs (limit 3).`);
  assert.ok(
    teammateStep.senderSyncs <= 3,
    `${teammateStep.name}: ${teammateStep.senderSyncs} teammate syncs (limit 3).`,
  );
  assert.ok(sharedStep.senderSyncs <= 6, `${sharedStep.name}: ${sharedStep.senderSyncs} teammate syncs (limit 6).`);
  report.checks.push(
    `Teammate syncs after the first read: ${report.steps
      .slice(1)
      .map((item) => `${item.name} ${item.senderSyncs}`)
      .join(", ")}.`,
  );
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
} finally {
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}

process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (!report.passed) process.exit(1);
