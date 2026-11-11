import { sortConversationMessages } from "@openbot/contracts/conversation-order";
import type {
  AgentEvent,
  AgentSummary,
  ConversationMessage,
  ConversationSnapshot,
  QueueHold,
  QueueSnapshot,
} from "@openbot/contracts/ipc";
import { Effect, type Scope } from "effect";
import { isMailboxMessageCopy } from "../conversation-snapshots";
import type { MailboxStore, QueueDeliveryState } from "../mailbox-store";
import type { OpenBotDatabase } from "../openbot-database";
import { noteCallTime } from "../slow-calls";
import type { ConversationRuntime } from "./conversation-runtime";
import { conversationContentSignature } from "./delivery-content";
import type { RoutineScheduler } from "./routine-scheduler";

export interface MailboxSyncHooks {
  emit(event: AgentEvent): void;
  emitError(code: string, error: unknown, agentId?: string): void;
  /** What holds the queue back, when the reason is not this agent's own running turn. */
  queueHold(agentId: string): QueueHold | null;
}

/** The conversation cache calls of the sync, so a check can give its own cache. */
export type MailboxSyncConversation = Pick<
  ConversationRuntime,
  | "emitConversation"
  | "hasPublishedConversation"
  | "isExecutionThread"
  | "publishConversation"
  | "snapshot"
  | "snapshotToUpdate"
  | "snapshotWithTurn"
>;

export interface MailboxSyncOptions {
  database: OpenBotDatabase;
  mailbox: MailboxStore;
  conversation: MailboxSyncConversation;
  routines: Pick<RoutineScheduler, "reconcileDelivery" | "stateChanged">;
  scope: () => Scope.Scope;
  hooks: MailboxSyncHooks;
}

/**
 * Keeps conversation snapshots merged with mailbox rows, and fans queue
 * state out to the renderer.
 *
 * Owns no durable state — every method reads the mailbox/database and writes
 * the in-memory snapshot. The facade keeps no wrappers: call sites use this
 * directly, and later extractions (drain, turn lifecycle) take it as a dep
 * instead of reaching into the mailbox themselves.
 */
export class MailboxSync {
  readonly #database: OpenBotDatabase;
  readonly #mailbox: MailboxStore;
  readonly #conversation: MailboxSyncConversation;
  readonly #routines: Pick<RoutineScheduler, "reconcileDelivery" | "stateChanged">;
  readonly #scope: () => Scope.Scope;
  readonly #hooks: MailboxSyncHooks;
  readonly #synced = new WeakMap<
    ConversationSnapshot,
    { version: number; messages: ConversationMessage[]; first: string | undefined }
  >();
  readonly #syncedThisTick = new Set<string>();
  readonly #trailing = new Set<string>();
  readonly #reconciledEnded = new Map<string, string>();
  readonly #windowActive = new Map<string, ReadonlySet<string>>();
  #tickScheduled = false;

  constructor(options: MailboxSyncOptions) {
    this.#database = options.database;
    this.#mailbox = options.mailbox;
    this.#conversation = options.conversation;
    this.#routines = options.routines;
    this.#scope = options.scope;
    this.#hooks = options.hooks;
  }

  syncDeliveryMessage(snapshot: ConversationSnapshot, deliveryId: string): void {
    const context = this.#mailbox.getDelivery(deliveryId);
    const message = snapshot.messages.find((candidate) => candidate.id === deliveryId);
    if (!context || !message) return;
    // A plan continuation keeps the request in its first turn, so that turn's plan and answer stay
    // between the request and the continuation's answer.
    const turnId = context.planContinuation?.turnIds[0] ?? context.delivery.turnId;
    if (turnId === null) delete message.turnId;
    else message.turnId = turnId;
    message.delivery = {
      id: context.delivery.id,
      status: context.delivery.status,
      position: context.delivery.position,
    };
  }

  /** Answers whether the messages of the snapshot changed, so a caller does not serialize the whole chat to find out. */
  syncMailboxMessages(snapshot: ConversationSnapshot, mailboxMessages?: readonly ConversationMessage[]): boolean {
    if (this.#conversation.isExecutionThread(snapshot.threadId)) return false;
    const before = messagesState(snapshot.messages);
    const fromCreatedAt = snapshot.messages[0]?.createdAt;
    const mailboxCopies =
      mailboxMessages ??
      this.#mailbox.conversationMessages(snapshot.agentId, fromCreatedAt === undefined ? {} : { fromCreatedAt });
    // A teammate's or a routine's message stays in the mailbox after a person deletes it from this
    // chat, so the record of the delete keeps it out.
    const deleted = snapshot.threadId
      ? this.#database.deletedConversationMessageIds(snapshot.threadId)
      : new Set<string>();
    const incomingMailboxMessages =
      deleted.size === 0 ? mailboxCopies : mailboxCopies.filter((message) => !deleted.has(message.id));
    // A deleted message keeps its entry here, so its provider copy also leaves the chat.
    const incomingMessages = new Map(
      mailboxCopies.flatMap((message) =>
        message.exchange?.direction === "incoming" ? [[message.exchange.messageId, message] as const] : [],
      ),
    );
    for (let index = snapshot.messages.length - 1; index >= 0; index--) {
      const message = snapshot.messages[index];
      if (message && isMailboxMessageCopy(message, incomingMessages)) snapshot.messages.splice(index, 1);
    }
    const indexes = new Map(snapshot.messages.map((message, index) => [message.id, index]));
    for (const mailboxMessage of incomingMailboxMessages) {
      const index = indexes.get(mailboxMessage.id);
      if (index !== undefined) snapshot.messages[index] = mailboxMessage;
      else {
        indexes.set(mailboxMessage.id, snapshot.messages.length);
        snapshot.messages.push(mailboxMessage);
      }
    }
    const reactions = this.#mailbox.reactionsFor(snapshot.agentId);
    for (const message of snapshot.messages) {
      message.reactions = reactions.get(message.id) ?? [];
      message.reaction = message.reactions.find((reaction) => reaction.actor.kind === "user")?.emoji ?? null;
    }
    sortConversationMessages(snapshot.messages);
    return messagesChanged(before, snapshot.messages);
  }

  reconcilePersistedMailboxMessages(agent: AgentSummary): void {
    if (!agent.threadId) return;
    const page = this.#database.readConversationPage(agent.id, agent.threadId, { type: "latest" }, 100);
    const persisted: ConversationSnapshot = {
      agentId: page.agentId,
      threadId: page.threadId,
      activeTurnId: page.activeTurnId,
      revision: page.revision,
      messages: page.messages,
    };
    const previousMessageIds = new Set(persisted.messages.map((message) => message.id));
    const previousSignature = conversationContentSignature(persisted);
    const oldest = persisted.messages[0]?.createdAt;
    this.syncMailboxMessages(
      persisted,
      this.#mailbox.conversationMessages(agent.id, oldest === undefined ? {} : { fromCreatedAt: oldest }),
    );
    if (conversationContentSignature(persisted) === previousSignature) return;
    // The signature also changes with key order, which the stored rows keep. Write only the
    // messages that store differently, so a read with no real change writes nothing (#2224).
    const changedMessages = this.#database.changedConversationMessages(agent.threadId, persisted.messages);
    const removedMessageIds = [...previousMessageIds].filter(
      (messageId) => !persisted.messages.some((message) => message.id === messageId),
    );
    if (changedMessages.length > 0 || removedMessageIds.length > 0) {
      this.#database.persistConversationChanges({
        agentId: agent.id,
        threadId: agent.threadId,
        activeTurnId: persisted.activeTurnId,
        changedMessages,
        removedMessageIds,
        eventType: "conversation.mailbox-reconciled",
        detail: {
          messageCount: changedMessages.length,
        },
      });
    }
    const live = this.#conversation.snapshot(agent.id);
    if (live) {
      const liveOldest = live.messages[0]?.createdAt;
      this.syncMailboxMessages(
        live,
        this.#mailbox.conversationMessages(agent.id, liveOldest === undefined ? {} : { fromCreatedAt: liveOldest }),
      );
    }
  }

  /**
   * The queue the user reads, carrying the reason it waits when something outside this agent holds
   * it. `AgentService.listQueue` returns this as well, so a reload and a reconnect report the same
   * wait as the event below.
   */
  queueSnapshot(agentId: string): QueueSnapshot {
    const queue = this.#mailbox.listQueue(agentId);
    if (!queue.deliveries.some((delivery) => delivery.status === "queued")) return queue;
    const hold = this.#hooks.queueHold(agentId);
    return hold ? { ...queue, hold } : queue;
  }

  #queueWindow(agentId: string): QueueSnapshot {
    const queue = this.#mailbox.listQueueWindow(agentId, this.#windowActive.get(agentId));
    this.#windowActive.set(
      agentId,
      new Set(queue.deliveries.filter((delivery) => ACTIVE_STATUSES.has(delivery.status)).map(({ id }) => id)),
    );
    if (!queue.deliveries.some((delivery) => delivery.status === "queued")) return queue;
    const hold = this.#hooks.queueHold(agentId);
    return hold ? { ...queue, hold } : queue;
  }

  emitQueue(agentId: string): void {
    const startedAt = performance.now();
    const queue = this.#queueWindow(agentId);
    let routinesChanged = false;
    // A run in a routine's side thread is not in the queue of the chat, but its run still follows it.
    const reconciling = this.#mailbox.routineQueueDeliveries(agentId, (delivery) => this.#needsReconcile(delivery));
    for (const delivery of reconciling) {
      if (this.#routines.reconcileDelivery(delivery)) routinesChanged = true;
      if (!ACTIVE_STATUSES.has(delivery.status)) this.#reconciledEnded.set(delivery.id, endedKey(delivery));
    }
    this.#hooks.emit({ type: "queue-changed", snapshot: queue });
    if (routinesChanged) this.#routines.stateChanged(agentId);
    const affectedAgents = new Set([agentId, ...this.#mailbox.senderAgentIdsForRecipient(agentId)]);
    let synced = 0;
    let deferred = 0;
    let snapshotMessages = 0;
    for (const affectedAgentId of affectedAgents) {
      if (affectedAgentId !== agentId && this.#syncedThisTick.has(affectedAgentId)) {
        this.#trailing.add(affectedAgentId);
        deferred += 1;
        continue;
      }
      this.#syncedThisTick.add(affectedAgentId);
      const messages = this.#syncAgent(affectedAgentId);
      if (messages !== null) {
        synced += 1;
        snapshotMessages += messages;
      }
    }
    if (!this.#tickScheduled) {
      this.#tickScheduled = true;
      queueMicrotask(() => this.#endTick());
    }
    noteCallTime("MailboxSync.emitQueue", startedAt, () => ({
      queuedDeliveries: queue.deliveries.length,
      reconciled: reconciling.length,
      agents: affectedAgents.size,
      synced,
      deferred,
      snapshotMessages,
    }));
  }

  #needsReconcile(delivery: QueueDeliveryState): boolean {
    return ACTIVE_STATUSES.has(delivery.status) || this.#reconciledEnded.get(delivery.id) !== endedKey(delivery);
  }

  #endTick(): void {
    this.#tickScheduled = false;
    this.#syncedThisTick.clear();
    const trailing = [...this.#trailing];
    this.#trailing.clear();
    for (const agentId of trailing) {
      try {
        this.#syncAgent(agentId);
      } catch (error) {
        this.#hooks.emitError("mailbox_sync_failed", error, agentId);
      }
    }
  }

  #syncAgent(agentId: string): number | null {
    const snapshot = this.#conversation.snapshotToUpdate(agentId);
    if (!snapshot) return null;
    const record = this.#synced.get(snapshot);
    if (
      record &&
      record.messages === snapshot.messages &&
      record.first === snapshot.messages[0]?.createdAt &&
      this.#conversation.hasPublishedConversation(agentId) &&
      !this.#mailbox.changedForAgentSince(agentId, record.version)
    )
      return null;
    const version = this.#mailbox.syncVersion();
    if (this.syncMailboxMessages(snapshot)) this.#conversation.emitConversation(snapshot);
    else if (!this.#conversation.hasPublishedConversation(agentId)) this.#conversation.publishConversation(snapshot);
    this.#synced.set(snapshot, { version, messages: snapshot.messages, first: snapshot.messages[0]?.createdAt });
    return snapshot.messages.length;
  }

  retryDeliveryReconciliation(agentId: string, turnId?: string, deliveryIds: readonly string[] = []): void {
    queueMicrotask(() => {
      Effect.runFork(
        Effect.gen({ self: this }, function* () {
          // A provider response can arrive before the mailbox writes complete. Associate every
          // still-starting row with the confirmed turn before publishing the retry. This is safe:
          // the turn id came from the provider, and it prevents a second drain from replaying it.
          // Check the active marker again so a delayed retry cannot claim a new turn's rows.
          if (turnId && this.#conversation.snapshotWithTurn(agentId, turnId)) {
            const accepted = new Set(deliveryIds);
            for (const deliveryId of accepted) {
              const current = this.#mailbox.getDelivery(deliveryId)?.delivery;
              if (current?.recipientAgentId !== agentId || current.status !== "starting" || current.turnId !== null)
                continue;
              yield* this.#mailbox.markRunning(deliveryId, turnId);
            }
          }
          yield* Effect.sync(() => {
            this.emitQueue(agentId);
            const snapshot = this.#conversation.snapshotToUpdate(agentId);
            if (snapshot) this.#conversation.emitConversation(snapshot);
          });
        })
          .pipe(
            Effect.catch((failure) =>
              Effect.sync(() => {
                this.#hooks.emitError("delivery_reconciliation_pending", failure, agentId);
              }),
            ),
          )
          .pipe(Effect.forkIn(this.#scope(), { startImmediately: true })),
      );
    });
  }
}

interface MessagesState {
  ids: string[];
  byId: Map<string, ConversationMessage>;
  reactions: Map<ConversationMessage, string>;
}

/** What a mailbox sync can change: the order, the mailbox rows it replaces, and the reactions of every message. */
function messagesState(messages: readonly ConversationMessage[]): MessagesState {
  return {
    ids: messages.map((message) => message.id),
    byId: new Map(messages.map((message) => [message.id, message])),
    reactions: new Map(messages.map((message) => [message, reactionKey(message)])),
  };
}

function reactionKey(message: ConversationMessage): string {
  return JSON.stringify({ reactions: message.reactions, reaction: message.reaction });
}

/**
 * The same answer as comparing the serialized chat before and after the sync. A message the sync
 * kept is compared by its reactions; a message it replaced is compared whole.
 */
function messagesChanged(before: MessagesState, after: readonly ConversationMessage[]): boolean {
  if (before.byId.size !== before.ids.length || after.length !== before.ids.length) return true;
  for (const [index, message] of after.entries()) {
    if (message.id !== before.ids[index]) return true;
    const previous = before.byId.get(message.id);
    if (previous === message) {
      if (before.reactions.get(message) !== reactionKey(message)) return true;
    } else if (!previous || JSON.stringify(previous) !== JSON.stringify(message)) return true;
  }
  return false;
}

const ACTIVE_STATUSES: ReadonlySet<QueueDeliveryState["status"]> = new Set(["queued", "starting", "running"]);

function endedKey(delivery: QueueDeliveryState): string {
  return `${delivery.status}\u0000${delivery.error ?? ""}`;
}
