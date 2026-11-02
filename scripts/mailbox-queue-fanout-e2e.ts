// Checks the work of one queue change for an agent that many teammates write to. `emitQueue` syncs
// the chat of the recipient and of each agent that ever sent it a message. Each of those chats is
// a large snapshot in memory. A queue change that does not touch a teammate's message must not
// serialize that teammate's whole chat. A change that does must still update its outgoing card,
// and a reaction must still reach the recipient's chat.
//
// Runs the real mailbox store, SQLite and `MailboxSync` on a temporary profile. The chats in memory
// and the routine scheduler are injected. No app, provider, account or network is started.
//
// Run: node --import tsx scripts/mailbox-queue-fanout-e2e.ts
// Writes .openbot-build/mailbox-queue-fanout-e2e/report.json and exits non-zero on a failure.
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentEvent, ConversationMessage, ConversationSnapshot } from "@openbot/contracts/ipc";
import { Effect, Scope } from "effect";
import { MailboxSync, type MailboxSyncConversation, type MailboxSyncOptions } from "../src/backend/agent/mailbox-sync";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const OUT = resolve(import.meta.dirname, "../.openbot-build/mailbox-queue-fanout-e2e");
const RECIPIENT = "recipient";
const SENDERS = Array.from({ length: 24 }, (_, index) => `sender-${index}`);
const MESSAGES_PER_SENDER = 5;
/** Whole-chat serializations of the teammates that one unrelated queue change may make. */
const MAX_UNRELATED_SERIALIZATIONS = 0;
const LARGE_TOOL_OUTPUT = "x".repeat(200_000);

interface Report {
  senders: number;
  checks: string[];
  unrelatedChange?: { senderSerializations: number; senderEmits: number };
  relatedChange?: { senderSerializations: number; emittedAgents: string[] };
  passed: boolean;
  error?: string;
}

const report: Report = { senders: SENDERS.length, checks: [], passed: false };
const root = await mkdtemp(join(tmpdir(), "openbot-mailbox-fanout-"));
const scope = Effect.runSync(Scope.make());

const serializations = new Map<string, number>();
const snapshots = new Map<string, ConversationSnapshot>();
const published = new Set<string>();
const emitted: string[] = [];

/** A large earlier answer in the chat. JSON.stringify counts each whole-chat serialization through it. */
function largeAnswer(agentId: string): ConversationMessage {
  const message: ConversationMessage = {
    id: `answer-${agentId}`,
    author: "assistant",
    text: LARGE_TOOL_OUTPUT,
    createdAt: "2026-01-01T00:00:00.000Z",
    status: "completed",
  };
  Object.defineProperty(message, "toJSON", {
    enumerable: false,
    value: () => {
      serializations.set(agentId, (serializations.get(agentId) ?? 0) + 1);
      return { ...message };
    },
  });
  return message;
}

function snapshotFor(agentId: string): ConversationSnapshot {
  return { agentId, threadId: `thread-${agentId}`, activeTurnId: null, revision: 0, messages: [largeAnswer(agentId)] };
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
    emitted.push(snapshot.agentId);
    published.add(snapshot.agentId);
  },
};

const routines: MailboxSyncOptions["routines"] = {
  reconcileDelivery: () => false,
  stateChanged: () => {},
};

function senderSerializations(): number {
  let total = 0;
  for (const sender of SENDERS) total += serializations.get(sender) ?? 0;
  return total;
}

function outgoingDeliveryStatus(agentId: string, messageId: string): string | undefined {
  const card = snapshots.get(agentId)?.messages.find((message) => message.id === `outbox-${messageId}`);
  return card?.exchange?.deliveries?.find((delivery) => delivery.recipientAgentId === RECIPIENT)?.status;
}

try {
  const database = new OpenBotDatabase(join(root, "data"));
  const mailbox = new MailboxStore(join(root, "data"), join(root, "shared"), database);
  await runCauseEffect(mailbox.initialize());
  const events: AgentEvent[] = [];
  const sync = new MailboxSync({
    database,
    mailbox,
    conversation,
    routines,
    scope: () => scope,
    hooks: { emit: (event) => events.push(event), emitError: () => {}, queueHold: () => null },
  });

  const sent = new Map<string, string[]>();
  for (const sender of SENDERS) {
    const ids: string[] = [];
    for (let index = 0; index < MESSAGES_PER_SENDER; index += 1) {
      const receipt = await runCauseEffect(
        mailbox.enqueue({
          sender: { kind: "agent", agentId: sender },
          recipientAgentIds: [RECIPIENT],
          text: `Report ${index} from ${sender}`,
          idempotencyKey: `agent:${sender}:turn-${index}:${index}`,
        }),
      );
      ids.push(receipt.messageId);
      const delivery = receipt.deliveries.find((item) => item.recipientAgentId === RECIPIENT);
      assert.ok(delivery);
      await runCauseEffect(mailbox.markStarting(delivery.id));
      await runCauseEffect(mailbox.markTerminal(delivery.id, "completed"));
    }
    sent.set(sender, ids);
    snapshots.set(sender, snapshotFor(sender));
  }
  snapshots.set(RECIPIENT, snapshotFor(RECIPIENT));
  assert.equal(mailbox.senderAgentIdsForRecipient(RECIPIENT).length, SENDERS.length);
  report.checks.push("Every teammate is a sender of the recipient.");

  sync.emitQueue(RECIPIENT);
  for (const sender of SENDERS) {
    const firstId = sent.get(sender)?.[0];
    assert.ok(firstId);
    assert.equal(outgoingDeliveryStatus(sender, firstId), "completed", `${sender} shows its completed message.`);
  }
  assert.equal(published.size, SENDERS.length + 1, "The first queue read publishes every chat.");
  report.checks.push("The first queue read shows each teammate's outgoing cards and publishes every chat.");

  // A routine run starts for the recipient: a queue change with no teammate message in it.
  serializations.clear();
  emitted.length = 0;
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
  report.unrelatedChange = {
    senderSerializations: senderSerializations(),
    senderEmits: emitted.filter((agentId) => agentId !== RECIPIENT).length,
  };
  assert.equal(report.unrelatedChange.senderEmits, 0, "No teammate chat changes for a routine delivery.");
  assert.ok(
    report.unrelatedChange.senderSerializations <= MAX_UNRELATED_SERIALIZATIONS,
    `Two routine queue changes serialized teammate chats ${report.unrelatedChange.senderSerializations} times (limit ${MAX_UNRELATED_SERIALIZATIONS}).`,
  );
  report.checks.push("A routine queue change serializes and emits no teammate chat.");

  // A teammate's new message starts: only that teammate's card changes.
  serializations.clear();
  emitted.length = 0;
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
  assert.equal(outgoingDeliveryStatus("sender-3", update.messageId), "queued");
  await runCauseEffect(mailbox.markStarting(updateDelivery.id));
  await runCauseEffect(mailbox.markRunning(updateDelivery.id, "turn-update"));
  sync.emitQueue(RECIPIENT);
  assert.equal(outgoingDeliveryStatus("sender-3", update.messageId), "running");
  await runCauseEffect(mailbox.markTerminal(updateDelivery.id, "completed"));
  sync.emitQueue(RECIPIENT);
  assert.equal(outgoingDeliveryStatus("sender-3", update.messageId), "completed");
  const emittedSenders = [...new Set(emitted.filter((agentId) => agentId !== RECIPIENT))];
  assert.deepEqual(emittedSenders, ["sender-3"], "Only the teammate whose message moved is emitted.");
  report.relatedChange = { senderSerializations: senderSerializations(), emittedAgents: [...new Set(emitted)] };
  report.checks.push("A teammate's message shows queued, running and completed on its own card.");

  const incoming = snapshots
    .get(RECIPIENT)
    ?.messages.find((message) => message.exchange?.direction === "incoming" && message.text.includes("sender-3"));
  assert.ok(incoming, "The recipient's chat shows the teammate's message.");
  emitted.length = 0;
  await runCauseEffect(mailbox.setReaction(RECIPIENT, incoming.id, { kind: "user" }, "👍"));
  sync.emitQueue(RECIPIENT);
  const reacted = snapshots.get(RECIPIENT)?.messages.find((message) => message.id === incoming.id);
  assert.equal(reacted?.reaction, "👍", "The reaction shows in the recipient's chat.");
  assert.ok(emitted.includes(RECIPIENT), "A reaction change emits the recipient's chat.");
  report.checks.push("A reaction reaches the recipient's chat and emits it.");

  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? error.message : String(error);
} finally {
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}

process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (!report.passed) process.exit(1);
