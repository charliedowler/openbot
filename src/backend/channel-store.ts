import { randomUUID } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";
import {
  CHANNEL_PREVIEW_LIMIT,
  CHANNEL_ROUTING_EVENT_ITEM_TYPE_PREFIX,
  type Channel,
  type ChannelDraft,
  type ChannelMessage,
  type ChannelPage,
  type ChannelSummary,
  type ChannelTask,
  type ChannelThread,
  decodeChannel,
  isChannelMessage,
  isChannelTask,
  SIGNED_OUT_CHANNEL_MEMBER_ID,
} from "@openbot/contracts/ipc";
import { markdownPreviewText } from "@openbot/contracts/markdown-preview-text";
import { type DynamicRecord, isDynamicRecord, isString } from "@openbot/contracts/runtime-values";
import { sourceText } from "@openbot/i18n/source";
import { changesModelContent } from "./channel-history";
import { CHANNEL_REPLY_ID_PREFIX } from "./channel-replies";
import { deleteAggregateHistory, deleteThreadAttachments } from "./database/database-core";
import {
  databaseRow,
  databaseRows,
  optionalStringColumn,
  requiredNumberColumn,
  requiredStringColumn,
} from "./database/database-rows";
import type { OpenBotDatabase } from "./openbot-database";
import { noteCallTime } from "./slow-calls";
import { revokeRoutineWebhooks } from "./webhook-route-store";

/**
 * A message that a person deleted keeps its row, with its id, sequence and author but no content,
 * so that read, summary and context cursors stay correct. Every read skips it.
 */
const SHOWN_MESSAGE_SQL = "json_extract(message_json, '$.deleted') IS NOT 1";
const LISTED_MESSAGE_SQL = `${SHOWN_MESSAGE_SQL}
  AND COALESCE(json_extract(message_json, '$.message.itemType'), '') != 'plan'
  AND COALESCE(json_extract(message_json, '$.message.itemType'), '') != 'subagents'`;
const UNREAD_MESSAGE_SQL = `${LISTED_MESSAGE_SQL}
  AND COALESCE(json_extract(message_json, '$.message.itemType'), '') != 'channel-change'
  AND COALESCE(json_extract(message_json, '$.message.itemType'), '') != 'commentary'
  AND COALESCE(json_extract(message_json, '$.message.itemType'), '') NOT LIKE ?1`;
const MESSAGE_FLAGS_SQL = `SELECT message_id, sequence, json_extract(message_json, '$.author.id') AS author_id,
  (${LISTED_MESSAGE_SQL}) AS listed, (${UNREAD_MESSAGE_SQL}) AS counted
  FROM projection_channel_messages WHERE channel_id = ?2`;
const ROUTING_EVENT_PATTERN = `${CHANNEL_ROUTING_EVENT_ITEM_TYPE_PREFIX}%`;

interface MessageFlags {
  messageId: string;
  sequence: number;
  authorId: unknown;
  latest: boolean;
  counted: boolean;
}

function messageFlags(row: DynamicRecord): MessageFlags {
  return {
    messageId: requiredStringColumn(row, "message_id"),
    sequence: requiredNumberColumn(row, "sequence"),
    authorId: row.author_id ?? null,
    latest: Number(row.listed) === 1,
    counted: Number(row.counted) === 1,
  };
}

function laterMessage(candidate: MessageFlags, current: MessageFlags): boolean {
  if (candidate.sequence !== current.sequence) return candidate.sequence > current.sequence;
  return Buffer.compare(Buffer.from(candidate.messageId), Buffer.from(current.messageId)) > 0;
}

/** The row that replaces a deleted channel message. `deleted` is internal and never sent. */
function deletedChannelMessage(entry: ChannelMessage): ChannelMessage & { deleted: true } {
  return {
    id: entry.id,
    channelId: entry.channelId,
    sequence: entry.sequence,
    author: entry.author,
    taskId: entry.taskId,
    superseded: entry.superseded,
    deleted: true,
    message: {
      id: entry.message.id,
      author: entry.message.author,
      text: "",
      createdAt: entry.message.createdAt,
      status: "completed",
    },
  };
}

function isDeletedChannelMessage(entry: ChannelMessage): boolean {
  return isDynamicRecord(entry) && entry.deleted === true;
}

export interface ChannelAssignment {
  id: string;
  channelId: string;
  taskId: string;
  agentId: string;
  taskRevision: number;
  /**
   * The request the task served when this assignment started. A task can be reused for a newer
   * request, so its id alone does not tie an assignment to the request it worked on. Null for an
   * assignment written before this was stored.
   */
  requestMessageId: string | null;
  /**
   * The resources this assignment holds, copied from the task when the assignment starts. A task
   * record can change owner and resources while its previous owner still runs, so the task is not
   * a safe place to read a live reservation from.
   */
  resources: string[];
  deliveryId: string | null;
  turnId: string | null;
  state: "queued" | "starting" | "running" | "completed" | "failed" | "interrupted";
  throughSequence: number;
  summaryVersion: number;
  awaitedTaskIds: string[];
  pendingRevision: number | null;
  pendingOutcome: string | null;
}

export interface ChannelContext {
  threadId: string;
  sessionId: string | null;
  throughSequence: number;
  summaryVersion: number;
}

export interface ChannelHistorySummary {
  version: number;
  throughSequence: number;
  text: string;
}

interface ChannelChange {
  channel: Channel;
  messages: ChannelMessage[];
  tasks: ChannelTask[];
  assignments: ChannelAssignment[];
}

/**
 * Owns durable channel records. Every change and its retry receipt commit together.
 */
export class ChannelStore {
  /** By channel: how many times a message changed in place after it was complete, in this process. */
  readonly #completedMessageChanges = new Map<string, number>();
  readonly #tasks = new Map<string, readonly ChannelTask[]>();
  readonly #assignments = new Map<string, readonly ChannelAssignment[]>();
  readonly #messageFlags = new Map<string, Map<string, MessageFlags>>();
  readonly #agentAssignments = new Map<string, { assignment: ChannelAssignment; channelId: string } | null>();

  constructor(
    readonly database: OpenBotDatabase,
    readonly onTasksStopped?: (channel: Channel, tasks: ChannelTask[]) => void,
  ) {}

  /** A packet built before this count changed can show a message as it was before. */
  completedMessageChanges(channelId: string): number {
    return this.#completedMessageChanges.get(channelId) ?? 0;
  }

  get(channelId: string): Channel {
    const row = databaseRow(
      this.database.connection
        .prepare("SELECT channel_json FROM projection_channels WHERE channel_id = ?")
        .get(channelId),
    );
    if (!row) throw new Error(sourceText("error.backend.channelNotFound"));
    return decodeChannel(JSON.parse(requiredStringColumn(row, "channel_json")));
  }

  /**
   * Only the ids, in one query. Placing channels in the sidebar layout needs the set of ids that
   * exist - archived ones included, so an archived channel keeps its section - and the scheduler
   * works by id as well. `list` would read every message of every channel to answer either. The order
   * is the order of `list`: the layout places the channels it has no order for in it, as the sidebar does.
   */
  ids(): string[] {
    return databaseRows(
      this.database.connection.prepare("SELECT channel_id FROM projection_channels ORDER BY rowid").all(),
    ).map((row) => requiredStringColumn(row, "channel_id"));
  }

  /**
   * The archived ids, in one query. An archived channel must not fire a routine and must not wake
   * the shared timer, and `list` would read every message of every channel to answer that.
   */
  archivedIds(): ReadonlySet<string> {
    return new Set(
      databaseRows(
        this.database.connection
          .prepare("SELECT channel_id FROM projection_channels WHERE json_extract(channel_json, '$.archived') = 1")
          .all(),
      ).map((row) => requiredStringColumn(row, "channel_id")),
    );
  }

  /**
   * @param signedOutMessagesAreTheirs true only when `memberId` is the host user of this computer.
   * Their messages from before they signed in carry `SIGNED_OUT_CHANNEL_MEMBER_ID`, so those
   * messages are their own and are never unread for them. A remote member is a different person:
   * the host's signed-out messages are unread for them until they read them.
   */
  list(memberId: string, signedOutMessagesAreTheirs = false): ChannelSummary[] {
    // The sidebar is rebuilt on every `channels-changed`, and a streaming reply emits those while
    // it arrives. One query returns every summary: the previous per-channel latest-message, unread,
    // and task queries made this O(channels x queries) per stream frame. A member's thinking is not
    // unread, as in an agent chat: it folds into the steps of the member's answer.
    const startedAt = performance.now();
    const excludedId = signedOutMessagesAreTheirs ? SIGNED_OUT_CHANNEL_MEMBER_ID : memberId;
    const cursors = new Map<string, number>();
    for (const row of databaseRows(
      this.database.connection
        .prepare("SELECT channel_id, through_sequence FROM projection_channel_reads WHERE member_id = ?")
        .all(memberId),
    ))
      cursors.set(requiredStringColumn(row, "channel_id"), Number(row.through_sequence ?? 0));
    const rows = databaseRows(
      this.database.connection.prepare("SELECT channel_id, channel_json FROM projection_channels ORDER BY rowid").all(),
    ).map((row) => {
      const channelId = requiredStringColumn(row, "channel_id");
      const cursor = cursors.get(channelId) ?? 0;
      let unread = 0;
      let latest: MessageFlags | undefined;
      for (const flags of this.#flagsOf(channelId).values()) {
        if (flags.latest && (!latest || laterMessage(flags, latest))) latest = flags;
        if (flags.counted && flags.sequence > cursor && flags.authorId !== memberId && flags.authorId !== excludedId)
          unread += 1;
      }
      let running = 0;
      let open = 0;
      for (const task of this.#tasksOf(channelId)) {
        if (task.state === "running") running += 1;
        if (task.state === "queued" || task.state === "waiting" || task.state === "running") open += 1;
      }
      const latestRow = latest
        ? databaseRow(
            this.database.connection
              .prepare("SELECT message_json FROM projection_channel_messages WHERE channel_id = ? AND message_id = ?")
              .get(channelId, latest.messageId),
          )
        : undefined;
      return {
        channel_json: row.channel_json,
        latest_json: latestRow?.message_json ?? null,
        unread,
        running,
        open,
      };
    });
    const threads = new Map<string, ChannelThread[]>();
    for (const row of databaseRows(
      this.database.connection
        .prepare(
          "SELECT channel_id, agent_id, thread_id FROM projection_channel_contexts ORDER BY channel_id, agent_id",
        )
        .all(),
    )) {
      const channelId = requiredStringColumn(row, "channel_id");
      const thread = {
        agentId: requiredStringColumn(row, "agent_id"),
        threadId: requiredStringColumn(row, "thread_id"),
      };
      threads.set(channelId, [...(threads.get(channelId) ?? []), thread]);
    }
    const summaries = rows.map((row) => {
      const channel = decodeChannel(JSON.parse(requiredStringColumn(row, "channel_json")));
      const latestJson = row.latest_json;
      const latest =
        typeof latestJson === "string" && latestJson.length > 0
          ? (() => {
              const value = JSON.parse(latestJson);
              if (!isChannelMessage(value)) throw new Error("Invalid stored channel message.");
              return value;
            })()
          : undefined;
      return {
        ...channel,
        unreadCount: Number(row.unread ?? 0),
        activeTasks: Number(row.running ?? 0),
        openTasks: Number(row.open ?? 0),
        lastMessage: latest
          ? {
              authorName: latest.author.name,
              author: { kind: latest.author.kind, id: latest.author.id },
              status: latest.message.status,
              text: previewText(latest),
              at: latest.message.createdAt,
            }
          : null,
        // A former member keeps its thread, but its prompts are no longer the channel's. The members
        // are at most 100, which is the bound of channel-attention-v1.
        threads: (threads.get(channel.id) ?? []).filter((thread) =>
          channel.members.some((item) => item.agentId === thread.agentId),
        ),
      };
    });
    noteCallTime("ChannelStore.list", startedAt, () => ({
      channels: rows.length,
      threads: [...threads.values()].reduce((total, list) => total + list.length, 0),
      unreadMessages: summaries.reduce((total, summary) => total + summary.unreadCount, 0),
      bytes: rows.reduce(
        (total, row) =>
          total +
          (typeof row.channel_json === "string" ? row.channel_json.length : 0) +
          (typeof row.latest_json === "string" ? row.latest_json.length : 0),
        0,
      ),
    }));
    return summaries;
  }

  create(channelId: string, draft: ChannelDraft): Channel {
    return { ...draft, id: channelId, archived: false, revision: 0, createdAt: new Date().toISOString() };
  }

  exists(channelId: string): boolean {
    return (
      this.database.connection.prepare("SELECT 1 FROM projection_channels WHERE channel_id = ?").get(channelId) !==
      undefined
    );
  }

  messages(
    channelId: string,
    before = Number.MAX_SAFE_INTEGER,
    limit?: number,
    after = Number.MIN_SAFE_INTEGER,
  ): ChannelMessage[] {
    const rows = databaseRows(
      this.database.connection
        .prepare(
          `SELECT message_json FROM projection_channel_messages WHERE channel_id = ? AND sequence < ? AND sequence > ? AND ${SHOWN_MESSAGE_SQL} ORDER BY sequence DESC, message_id DESC LIMIT ?`,
        )
        .all(channelId, before, after, limit ?? -1),
    );
    return rows.reverse().map((row) => storedChannelMessage(row));
  }

  messagesById(channelId: string, messageIds: readonly string[]): ChannelMessage[] {
    const unique = [...new Set(messageIds)];
    const found: ChannelMessage[] = [];
    for (let start = 0; start < unique.length; start += ID_BATCH) {
      const batch = unique.slice(start, start + ID_BATCH);
      found.push(
        ...databaseRows(
          this.database.connection
            .prepare(
              `SELECT message_json FROM projection_channel_messages WHERE channel_id = ? AND message_id IN (${batch.map(() => "?").join(", ")}) AND ${SHOWN_MESSAGE_SQL}`,
            )
            .all(channelId, ...batch),
        ).map((row) => storedChannelMessage(row)),
      );
    }
    return found.sort(compareChannelMessages);
  }

  resultTurns(channelId: string, turnIds: readonly string[]): Set<string> {
    const unique = [...new Set(turnIds)].filter((turnId) => turnId.length > 0);
    const found = new Set<string>();
    for (let start = 0; start < unique.length; start += ID_BATCH) {
      const batch = unique.slice(start, start + ID_BATCH);
      for (const row of databaseRows(
        this.database.connection
          .prepare(
            `SELECT json_extract(message_json, '$.message.turnId') AS turn_id FROM projection_channel_messages
              WHERE channel_id = ?
                AND ((message_id >= ? AND message_id < ?) OR (message_id >= ? AND message_id < ?))
                AND ${SHOWN_MESSAGE_SQL}
                AND json_extract(message_json, '$.message.turnId') IN (${batch.map(() => "?").join(", ")})`,
          )
          .all(
            channelId,
            CHANNEL_RESULT_ID_PREFIX,
            prefixEnd(CHANNEL_RESULT_ID_PREFIX),
            CHANNEL_REPLY_ID_PREFIX,
            prefixEnd(CHANNEL_REPLY_ID_PREFIX),
            ...batch,
          ),
      )) {
        const turnId = row.turn_id;
        if (typeof turnId === "string") found.add(turnId);
      }
    }
    return found;
  }

  taskMessages(channelId: string, taskId: string, through: number): ChannelMessage[] {
    return databaseRows(
      this.database.connection
        .prepare(
          `SELECT message_json FROM projection_channel_messages
            WHERE channel_id = ? AND sequence <= ? AND ${SHOWN_MESSAGE_SQL}
              AND json_extract(message_json, '$.taskId') = ?
              AND json_extract(message_json, '$.author.kind') = 'agent'
            ORDER BY sequence, message_id`,
        )
        .all(channelId, through, taskId),
    ).map((row) => storedChannelMessage(row));
  }

  messagesHolding(channelId: string, fragment: string, through: number): ChannelMessage[] {
    return databaseRows(
      this.database.connection
        .prepare(
          `SELECT message_json FROM projection_channel_messages
            WHERE channel_id = ? AND sequence <= ? AND ${SHOWN_MESSAGE_SQL} AND instr(message_json, ?) > 0
            ORDER BY sequence DESC, message_id DESC`,
        )
        .all(channelId, through, fragment),
    ).map((row) => storedChannelMessage(row));
  }

  /** One message by id, read through the primary key rather than the whole history. A deleted message is null. */
  message(channelId: string, messageId: string): ChannelMessage | null {
    const row = databaseRow(
      this.database.connection
        .prepare(
          `SELECT message_json FROM projection_channel_messages WHERE channel_id = ? AND message_id = ? AND ${SHOWN_MESSAGE_SQL}`,
        )
        .get(channelId, messageId),
    );
    if (!row) return null;
    const value = JSON.parse(requiredStringColumn(row, "message_json"));
    if (!isChannelMessage(value)) throw new Error("Invalid stored channel message.");
    return value;
  }

  /** One task by id, read through the primary key rather than every task of the channel. */
  task(channelId: string, taskId: string): ChannelTask | null {
    const row = databaseRow(
      this.database.connection
        .prepare("SELECT task_json FROM projection_channel_tasks WHERE channel_id = ? AND task_id = ?")
        .get(channelId, taskId),
    );
    if (!row) return null;
    const value = JSON.parse(requiredStringColumn(row, "task_json"));
    if (!isChannelTask(value)) throw new Error("Invalid stored channel task.");
    return value;
  }

  tasks(channelId: string): ChannelTask[] {
    return [...this.#tasksOf(channelId)];
  }

  #tasksOf(channelId: string): readonly ChannelTask[] {
    const cached = this.#tasks.get(channelId);
    if (cached) return cached;
    const tasks = this.#readTasks(channelId);
    this.#tasks.set(channelId, tasks);
    return tasks;
  }

  #readTasks(channelId: string): ChannelTask[] {
    return databaseRows(
      this.database.connection
        .prepare("SELECT task_json FROM projection_channel_tasks WHERE channel_id = ? ORDER BY rowid")
        .all(channelId),
    ).map((row) => {
      const value = JSON.parse(requiredStringColumn(row, "task_json"));
      if (!isChannelTask(value)) throw new Error("Invalid stored channel task.");
      return value;
    });
  }

  assignments(channelId: string): ChannelAssignment[] {
    const cached = this.#assignments.get(channelId);
    if (cached) return [...cached];
    const assignments = this.#readAssignments(channelId);
    this.#assignments.set(channelId, assignments);
    return [...assignments];
  }

  #readAssignments(channelId: string): ChannelAssignment[] {
    return databaseRows(
      this.database.connection
        .prepare("SELECT assignment_json FROM projection_channel_assignments WHERE channel_id = ? ORDER BY rowid")
        .all(channelId),
    ).map((row) => decodeAssignment(JSON.parse(requiredStringColumn(row, "assignment_json"))));
  }

  /**
   * Does any channel hold an assignment in one of these states?
   *
   * Walking the channel list to answer this parses every stored message of every channel to build
   * summaries that nothing here reads, so the question is answered by one query over the
   * assignment rows.
   */
  hasAssignmentInState(states: readonly ChannelAssignment["state"][]): boolean {
    if (!states.length) return false;
    const placeholders = states.map(() => "?").join(", ");
    return (
      this.database.connection
        .prepare(
          `SELECT 1 FROM projection_channel_assignments WHERE json_extract(assignment_json, '$.state') IN (${placeholders}) LIMIT 1`,
        )
        .get(...states) !== undefined
    );
  }

  /** `hasAssignmentInState` for the assignments of one agent. `mayDrain` asks this on every queue check. */
  hasAgentAssignmentInState(agentId: string, states: readonly ChannelAssignment["state"][]): boolean {
    if (!states.length) return false;
    const placeholders = states.map(() => "?").join(", ");
    return (
      this.database.connection
        .prepare(
          `SELECT 1 FROM projection_channel_assignments
            WHERE json_extract(assignment_json, '$.agentId') = ?
              AND json_extract(assignment_json, '$.state') IN (${placeholders}) LIMIT 1`,
        )
        .get(agentId, ...states) !== undefined
    );
  }

  /**
   * The assignment of this agent that holds its queue right now, with the channel it belongs to.
   *
   * `hasAgentAssignmentInState` answers the drain question with a yes or a no; the queue the user
   * reads has to name the cause as well. The same reason keeps this to one query: reaching a
   * channel through `list` parses every stored message of every channel. States are tried in the
   * order given, so a running assignment is reported before one that is only queued.
   */
  agentAssignment(
    agentId: string,
    states: readonly ChannelAssignment["state"][],
  ): { assignment: ChannelAssignment; channel: Channel } | null {
    if (!states.length) return null;
    const key = JSON.stringify([agentId, states]);
    let found = this.#agentAssignments.get(key);
    if (found === undefined) {
      found = this.#readAgentAssignment(agentId, states);
      this.#agentAssignments.set(key, found);
    }
    return found ? { assignment: { ...found.assignment }, channel: this.get(found.channelId) } : null;
  }

  #readAgentAssignment(
    agentId: string,
    states: readonly ChannelAssignment["state"][],
  ): { assignment: ChannelAssignment; channelId: string } | null {
    const placeholders = states.map(() => "?").join(", ");
    const ranking = states.map((_, index) => `WHEN ? THEN ${index}`).join(" ");
    const row = databaseRow(
      this.database.connection
        .prepare(
          `SELECT assignments.assignment_json AS assignment_json, assignments.channel_id AS channel_id
             FROM projection_channel_assignments AS assignments
             JOIN projection_channels AS channels ON channels.channel_id = assignments.channel_id
            WHERE json_extract(assignments.assignment_json, '$.agentId') = ?
              AND json_extract(assignments.assignment_json, '$.state') IN (${placeholders})
            ORDER BY CASE json_extract(assignments.assignment_json, '$.state') ${ranking} END,
                     assignments.rowid
            LIMIT 1`,
        )
        .get(agentId, ...states, ...states),
    );
    if (!row) return null;
    return {
      assignment: decodeAssignment(JSON.parse(requiredStringColumn(row, "assignment_json"))),
      channelId: requiredStringColumn(row, "channel_id"),
    };
  }

  assignmentForDelivery(deliveryId: string): ChannelAssignment | null {
    const row = databaseRow(
      this.database.connection
        .prepare("SELECT assignment_json FROM projection_channel_assignments WHERE delivery_id = ?")
        .get(deliveryId),
    );
    return row ? decodeAssignment(JSON.parse(requiredStringColumn(row, "assignment_json"))) : null;
  }

  page(channelId: string, before?: number): ChannelPage {
    const messages = this.messages(channelId, before, 101);
    const hasOlder = messages.length > 100;
    if (hasOlder) messages.shift();
    const olderCursor = hasOlder ? (messages[0]?.sequence ?? null) : null;
    return {
      channel: this.get(channelId),
      tasks: this.tasks(channelId),
      messages,
      olderCursor,
      throughSequence: this.messages(channelId, undefined, 1)[0]?.sequence ?? 0,
      ...(olderCursor === null ? {} : this.olderExtent(channelId, olderCursor)),
    };
  }

  /**
   * How many messages are older than a page, and when the first one was written: the day rail draws
   * the unloaded part from it. The time is only in the message JSON, so only the oldest row is read.
   */
  private olderExtent(channelId: string, before: number): { olderCount: number; oldestAt?: string } {
    const row = databaseRow(
      this.database.connection
        .prepare(
          `SELECT COUNT(*) AS older_count,
             (SELECT json_extract(message_json, '$.message.createdAt') FROM projection_channel_messages
              WHERE channel_id = ? AND ${SHOWN_MESSAGE_SQL} ORDER BY sequence LIMIT 1) AS oldest_at
           FROM projection_channel_messages WHERE channel_id = ? AND sequence < ? AND ${SHOWN_MESSAGE_SQL}`,
        )
        .get(channelId, channelId, before),
    );
    const oldestAt = row ? optionalStringColumn(row, "oldest_at") : null;
    return { olderCount: row ? requiredNumberColumn(row, "older_count") : 0, ...(oldestAt ? { oldestAt } : {}) };
  }

  commit(operationId: string, change: ChannelChange): Channel {
    const commandId = `channels:${operationId}`;
    const previous = this.database.commandResult(commandId);
    if (previous !== undefined) return decodeChannel(previous);
    if (!change.tasks.every(isChannelTask)) throw new Error("Invalid channel task.");
    const channel = { ...change.channel, revision: change.channel.revision + 1 };
    // A late write of a deleted message must not keep its text in the event history either.
    const deleted = this.database.connection.prepare(
      "SELECT 1 FROM projection_channel_messages WHERE channel_id = ? AND message_id = ? AND json_extract(message_json, '$.deleted') = 1",
    );
    const messages = change.messages.filter(
      (message) => isDeletedChannelMessage(message) || deleted.get(channel.id, message.id) === undefined,
    );
    const payload = { ...change, channel, messages };
    const stopping = this.onTasksStopped ? change.tasks.filter(stoppedState) : [];
    const before = new Map(stopping.map((task) => [task.id, this.task(channel.id, task.id)?.state]));
    let committed: Channel;
    try {
      committed = this.database.dispatch(
        commandId,
        [{ aggregateType: "channel", aggregateId: channel.id, eventType: "channel.changed", payload }],
        (db) => {
          this.project(db, payload);
          return channel;
        },
      );
    } finally {
      this.#afterChange(payload);
    }
    const stopped = stopping.filter((task) => {
      const state = before.get(task.id);
      return state !== "paused" && state !== "failed";
    });
    if (stopped.length) this.onTasksStopped?.(committed, stopped);
    return committed;
  }

  /**
   * Replaces one message with its deleted row. The earlier `channel.changed` events of the channel get
   * the same row, so the text leaves those events too, and a rebuild gives the same projection. A
   * summary that an agent wrote before the delete keeps what it says.
   */
  deleteMessage(channel: Channel, entry: ChannelMessage): Channel {
    const deleted = deletedChannelMessage(entry);
    const updated = { ...channel, revision: channel.revision + 1 };
    const payload: ChannelChange = { channel: updated, messages: [deleted], tasks: [], assignments: [] };
    try {
      return this.#deleteMessage(channel, entry, deleted, updated, payload);
    } finally {
      this.#afterChange(payload);
    }
  }

  #deleteMessage(
    channel: Channel,
    entry: ChannelMessage,
    deleted: ChannelMessage,
    updated: Channel,
    payload: ChannelChange,
  ): Channel {
    return this.database.dispatch(
      `channels:message-deleted:${channel.id}:${entry.id}:${randomUUID()}`,
      [{ aggregateType: "channel", aggregateId: channel.id, eventType: "channel.changed", payload }],
      (db) => {
        this.project(db, payload);
        const events = databaseRows(
          db
            .prepare(
              "SELECT sequence, payload_json FROM orchestration_events WHERE aggregate_type = 'channel' AND aggregate_id = ? AND event_type = 'channel.changed' AND instr(payload_json, ?) > 0",
            )
            .all(channel.id, JSON.stringify(entry.id)),
        );
        const update = db.prepare("UPDATE orchestration_events SET payload_json = ? WHERE sequence = ?");
        for (const event of events) {
          const value = JSON.parse(requiredStringColumn(event, "payload_json"));
          if (!isDynamicRecord(value) || !Array.isArray(value.messages)) continue;
          let changed = false;
          const messages = value.messages.map((item: unknown) => {
            if (!isChannelMessage(item) || item.id !== entry.id || isDeletedChannelMessage(item)) return item;
            changed = true;
            return { ...deleted, sequence: item.sequence };
          });
          if (changed) update.run(JSON.stringify({ ...value, messages }), requiredNumberColumn(event, "sequence"));
        }
        return updated;
      },
    );
  }

  private project(db: DatabaseSync, change: ChannelChange): void {
    db.prepare(
      "INSERT INTO projection_channels VALUES (?, ?) ON CONFLICT(channel_id) DO UPDATE SET channel_json = excluded.channel_json",
    ).run(change.channel.id, JSON.stringify(change.channel));
    for (const message of change.messages) {
      const existing = databaseRow(
        db
          .prepare(
            "SELECT sequence, message_json, json_extract(message_json, '$.message.status') AS status, json_extract(message_json, '$.deleted') AS deleted FROM projection_channel_messages WHERE channel_id = ? AND message_id = ?",
          )
          .get(change.channel.id, message.id),
      );
      // A person deleted this message. A late capture, a repeated result or a replay of an older
      // event must not bring its content back.
      if (existing?.deleted === 1 && !isDeletedChannelMessage(message)) continue;
      const last = databaseRow(
        db
          .prepare(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS next FROM projection_channel_messages WHERE channel_id = ?",
          )
          .get(change.channel.id),
      );
      const assigned = existing
        ? requiredNumberColumn(existing, "sequence")
        : last
          ? requiredNumberColumn(last, "next")
          : 1;
      db.prepare(
        "INSERT INTO projection_channel_messages VALUES (?, ?, ?, ?) ON CONFLICT(channel_id, message_id) DO UPDATE SET message_json = excluded.message_json",
      ).run(change.channel.id, message.id, assigned, JSON.stringify({ ...message, sequence: assigned }));
      const before = existing ? JSON.parse(requiredStringColumn(existing, "message_json")) : null;
      // Only a change to what a model reads makes a summary or a cursor that covers the message
      // wrong. A continued task marks its earlier turn superseded, and in a long channel that reset
      // the summary to the first message on almost every turn of the lead (#3761).
      if (existing && (!isChannelMessage(before) || changesModelContent(before, message))) {
        // A streaming message changes on each part of its text; a packet already leaves it unread.
        if (existing.status !== "streaming")
          this.#completedMessageChanges.set(change.channel.id, this.completedMessageChanges(change.channel.id) + 1);
        db.prepare(
          "UPDATE projection_channel_summaries SET version = version + 1, through_sequence = 0, text = '' WHERE channel_id = ? AND through_sequence >= ?",
        ).run(change.channel.id, assigned);
        // A member read this message before it changed, as when a correction supersedes a result. A
        // packet with only later messages would not show the change, so the next one is complete.
        db.prepare(
          "UPDATE projection_channel_contexts SET through_sequence = 0, summary_version = 0 WHERE channel_id = ? AND through_sequence >= ?",
        ).run(change.channel.id, assigned);
      }
    }
    for (const task of change.tasks)
      db.prepare(
        "INSERT INTO projection_channel_tasks VALUES (?, ?, ?) ON CONFLICT(task_id) DO UPDATE SET task_json = excluded.task_json",
      ).run(task.id, change.channel.id, JSON.stringify(task));
    for (const assignment of change.assignments)
      db.prepare(
        "INSERT INTO projection_channel_assignments VALUES (?, ?, ?, ?, ?) ON CONFLICT(assignment_id) DO UPDATE SET delivery_id = excluded.delivery_id, assignment_json = excluded.assignment_json",
      ).run(assignment.id, change.channel.id, assignment.taskId, assignment.deliveryId, JSON.stringify(assignment));
  }

  update(
    channel: Channel,
    changes: Partial<Omit<ChannelChange, "channel">> = {},
    operationId: string = randomUUID(),
  ): Channel {
    return this.commit(operationId, { channel, messages: [], tasks: [], assignments: [], ...changes });
  }

  context(channelId: string, agentId: string): ChannelContext {
    const row = databaseRow(
      this.database.connection
        .prepare("SELECT * FROM projection_channel_contexts WHERE channel_id = ? AND agent_id = ?")
        .get(channelId, agentId),
    );
    if (row)
      return {
        threadId: requiredStringColumn(row, "thread_id"),
        sessionId: isString(row.session_id) ? row.session_id : null,
        throughSequence: requiredNumberColumn(row, "through_sequence"),
        summaryVersion: requiredNumberColumn(row, "summary_version"),
      };
    const threadId = `openbot-thread-${randomUUID()}`;
    return this.database.dispatch(
      `channel-context:${channelId}:${agentId}`,
      [
        {
          aggregateType: "channel",
          aggregateId: channelId,
          eventType: "channel.context-created",
          payload: { agentId, threadId },
        },
      ],
      (db, sequences) => {
        const now = new Date().toISOString();
        db.prepare("INSERT INTO projection_threads VALUES (?, ?, ?, NULL, ?, ?, ?)").run(
          threadId,
          agentId,
          this.get(channelId).name,
          now,
          now,
          sequences[0] ?? 0,
        );
        db.prepare("INSERT INTO projection_channel_contexts(channel_id, agent_id, thread_id) VALUES (?, ?, ?)").run(
          channelId,
          agentId,
          threadId,
        );
        return { threadId, sessionId: null, throughSequence: 0, summaryVersion: 0 };
      },
    );
  }

  executionThreads(): Array<{ id: string; threadId: string }> {
    return databaseRows(
      this.database.connection.prepare("SELECT agent_id, thread_id FROM projection_channel_contexts").all(),
    ).map((row) => ({ id: requiredStringColumn(row, "agent_id"), threadId: requiredStringColumn(row, "thread_id") }));
  }

  contextThreads(channelId: string): string[] {
    return databaseRows(
      this.database.connection
        .prepare("SELECT thread_id FROM projection_channel_contexts WHERE channel_id = ? ORDER BY agent_id")
        .all(channelId),
    ).map((row) => requiredStringColumn(row, "thread_id"));
  }

  /** The execution threads of one agent, one for each channel it works in. */
  contextThreadsForAgent(agentId: string): string[] {
    return databaseRows(
      this.database.connection
        .prepare("SELECT thread_id FROM projection_channel_contexts WHERE agent_id = ? ORDER BY channel_id")
        .all(agentId),
    ).map((row) => requiredStringColumn(row, "thread_id"));
  }

  /**
   * The execution threads of every channel, in one query. Deleting an agent asks for this: a file
   * the agent generated in a channel thread belongs to the shared transcript, so it stays with the
   * channel rather than leaving with the agent.
   */
  allContextThreads(): string[] {
    return databaseRows(
      this.database.connection.prepare("SELECT thread_id FROM projection_channel_contexts").all(),
    ).map((row) => requiredStringColumn(row, "thread_id"));
  }

  /** Permanently removes a channel and its execution threads from every local projection. */
  delete(channelId: string, operationId: string = randomUUID()): void {
    if (!this.exists(channelId)) throw new Error(sourceText("error.backend.channelNotFound"));
    const threadIds = this.contextThreads(channelId);
    try {
      this.#deleteChannel(channelId, operationId, threadIds);
    } finally {
      this.#forget(channelId);
    }
  }

  #deleteChannel(channelId: string, operationId: string, threadIds: readonly string[]): void {
    this.database.dispatch(
      `channel-delete:${operationId}`,
      [
        {
          aggregateType: "channel",
          aggregateId: channelId,
          eventType: "channel.deleted",
          payload: { threadIds },
        },
      ],
      (db) => {
        const memoryIds = databaseRows(
          db.prepare("SELECT memory_id FROM projection_channel_memories WHERE channel_id = ?").all(channelId),
        ).map((row) => requiredStringColumn(row, "memory_id"));
        const routineIds = channelRoutineIds(db, channelId);
        revokeRoutineWebhooks(db, "channel", routineIds, { forget: true });
        deleteAggregateHistory(db, "channel", [channelId]);
        deleteAggregateHistory(db, "channel-memory", memoryIds);
        deleteAggregateHistory(db, "channel-routine", routineIds);
        // Routine run events use the routine id as their aggregate id; one routine can own many runs.
        deleteAggregateHistory(db, "channel-routine-run", routineIds);
        if (routineIds.length) {
          const placeholders = routineIds.map(() => "?").join(", ");
          db.prepare(`DELETE FROM projection_channel_routine_triggers WHERE routine_id IN (${placeholders})`).run(
            ...routineIds,
          );
        }

        for (const table of [
          "projection_channel_contexts",
          "projection_channel_summaries",
          "projection_channel_reads",
          "projection_channel_assignments",
          "projection_channel_tasks",
          "projection_channel_messages",
          "projection_channel_routine_runs",
          "projection_channel_routines",
          "projection_channel_memories",
        ]) {
          db.prepare(`DELETE FROM ${table} WHERE channel_id = ?`).run(channelId);
        }
        db.prepare("DELETE FROM projection_channels WHERE channel_id = ?").run(channelId);

        for (const threadId of threadIds) {
          deleteAggregateHistory(db, "thread", [threadId]);
          deleteThreadAttachments(db, threadId);
          db.prepare("DELETE FROM projection_threads WHERE thread_id = ?").run(threadId);
        }
        return null;
      },
    );
  }

  channelForThread(threadId: string): string | null {
    const row = databaseRow(
      this.database.connection
        .prepare("SELECT channel_id FROM projection_channel_contexts WHERE thread_id = ?")
        .get(threadId),
    );
    return row ? requiredStringColumn(row, "channel_id") : null;
  }

  acceptContext(
    channelId: string,
    agentId: string,
    sessionId: string,
    throughSequence: number,
    summaryVersion: number,
  ): void {
    const current = databaseRow(
      this.database.connection
        .prepare(
          "SELECT session_id, through_sequence, summary_version FROM projection_channel_contexts WHERE channel_id = ? AND agent_id = ?",
        )
        .get(channelId, agentId),
    );
    if (
      current &&
      current.session_id === sessionId &&
      current.through_sequence === throughSequence &&
      current.summary_version === summaryVersion
    )
      return;
    // Each change has its own operation: after a compaction reset, the cursor can come back to a value
    // that an earlier operation set, and a key made from the values would skip it as a repeat.
    this.database.dispatch(
      `channel-context-accepted:${randomUUID()}`,
      [
        {
          aggregateType: "channel",
          aggregateId: channelId,
          eventType: "channel.context-accepted",
          payload: { agentId, sessionId, throughSequence, summaryVersion },
        },
      ],
      (db) => {
        db.prepare(
          "UPDATE projection_channel_contexts SET session_id = ?, through_sequence = ?, summary_version = ? WHERE channel_id = ? AND agent_id = ?",
        ).run(sessionId, throughSequence, summaryVersion, channelId, agentId);
      },
    );
  }

  /**
   * The provider compacted this session, so its summary can leave out channel messages that it
   * read. A cursor at zero makes the next packet complete. It is the acceptance event, not a new
   * event type, so an earlier version still rebuilds the channel.
   */
  resetContextsForSession(sessionId: string): void {
    const rows = databaseRows(
      this.database.connection
        .prepare(
          "SELECT channel_id, agent_id FROM projection_channel_contexts WHERE session_id = ? AND through_sequence > 0",
        )
        .all(sessionId),
    );
    for (const row of rows)
      this.acceptContext(
        requiredStringColumn(row, "channel_id"),
        requiredStringColumn(row, "agent_id"),
        sessionId,
        0,
        0,
      );
  }

  summary(channelId: string): ChannelHistorySummary {
    const row = databaseRow(
      this.database.connection
        .prepare("SELECT * FROM projection_channel_summaries WHERE channel_id = ?")
        .get(channelId),
    );
    return row
      ? {
          version: requiredNumberColumn(row, "version"),
          throughSequence: requiredNumberColumn(row, "through_sequence"),
          text: requiredStringColumn(row, "text"),
        }
      : { version: 0, throughSequence: 0, text: "" };
  }

  saveSummary(channelId: string, summary: ChannelHistorySummary): void {
    this.database.dispatch(
      `channel-summary:${channelId}:${summary.version}`,
      [{ aggregateType: "channel", aggregateId: channelId, eventType: "channel.summarized", payload: summary }],
      (db) => {
        db.prepare(
          "INSERT INTO projection_channel_summaries VALUES (?, ?, ?, ?) ON CONFLICT(channel_id) DO UPDATE SET version = excluded.version, through_sequence = excluded.through_sequence, text = excluded.text",
        ).run(channelId, summary.version, summary.throughSequence, summary.text);
      },
    );
  }

  /**
   * Channel read cursors follow the reader when an account signs in.
   *
   * `HostService.channelActor` answers `local` while no account is signed in and the team member
   * id after it, so without this every channel the reader already read turns unread on sign-in.
   * Direct threads already adopt their cursors this way. A target that read a channel under its
   * own id keeps that position, and the shared operation id makes a repeated adoption a no-op.
   */
  adoptReads(sourceMemberId: string, targetMemberId: string): void {
    if (sourceMemberId === targetMemberId) return;
    const rows = databaseRows(
      this.database.connection
        .prepare("SELECT channel_id, through_sequence FROM projection_channel_reads WHERE member_id = ?")
        .all(sourceMemberId),
    );
    for (const row of rows) {
      const channelId = requiredStringColumn(row, "channel_id");
      if (this.#hasReadCursor(channelId, targetMemberId)) continue;
      const throughSequence = requiredNumberColumn(row, "through_sequence");
      // The command key holds the target member and this operation id, not the channel. Without the
      // channel here, every channel after the first would answer with the first channel's receipt.
      this.markRead(channelId, targetMemberId, throughSequence, `adopt:${sourceMemberId}:${channelId}`);
    }
  }

  markRead(channelId: string, memberId: string, throughSequence: number, operationId: string): Channel {
    const maximum = this.page(channelId).throughSequence;
    if (throughSequence > maximum) throw new Error("The read position exceeds the channel history.");
    return this.database.dispatch(
      `channel-read:${memberId}:${operationId}`,
      [
        {
          aggregateType: "channel",
          aggregateId: channelId,
          eventType: "channel.read",
          payload: { memberId, throughSequence },
        },
      ],
      (db) => {
        db.prepare(
          "INSERT INTO projection_channel_reads VALUES (?, ?, ?) ON CONFLICT(channel_id, member_id) DO UPDATE SET through_sequence = MAX(through_sequence, excluded.through_sequence)",
        ).run(channelId, memberId, throughSequence);
        return this.get(channelId);
      },
    );
  }

  /** Rebuilds channel projections from committed events without changing agent threads. */
  rebuild(channelId: string): void {
    try {
      this.#rebuild(channelId);
    } finally {
      this.#forget(channelId);
    }
  }

  #rebuild(channelId: string): void {
    const db = this.database.connection;
    db.exec("BEGIN IMMEDIATE");
    try {
      const events = databaseRows(
        db
          .prepare(
            "SELECT event_type, payload_json, occurred_at, sequence FROM orchestration_events WHERE aggregate_type = 'channel' AND aggregate_id = ? ORDER BY sequence",
          )
          .all(channelId),
      );
      for (const table of ["assignments", "tasks", "messages", "summaries", "reads", "contexts", "memories"])
        db.prepare(`DELETE FROM projection_channel_${table} WHERE channel_id = ?`).run(channelId);
      db.prepare("DELETE FROM projection_channels WHERE channel_id = ?").run(channelId);
      for (const event of events) {
        const value = JSON.parse(requiredStringColumn(event, "payload_json"));
        if (!isDynamicRecord(value)) throw new Error("Invalid channel event.");
        switch (event.event_type) {
          case "channel.changed": {
            if (
              !Array.isArray(value.messages) ||
              !value.messages.every(isChannelMessage) ||
              !Array.isArray(value.tasks) ||
              !value.tasks.every(isChannelTask) ||
              !Array.isArray(value.assignments)
            )
              throw new Error("Invalid channel change event.");
            this.project(db, {
              channel: decodeChannel(value.channel),
              messages: value.messages,
              tasks: value.tasks,
              assignments: value.assignments.map(decodeAssignment),
            });
            break;
          }
          case "channel.context-created": {
            if (!isString(value.agentId) || !isString(value.threadId))
              throw new Error("Invalid channel context event.");
            const now = requiredStringColumn(event, "occurred_at");
            db.prepare("INSERT OR IGNORE INTO projection_threads VALUES (?, ?, ?, NULL, ?, ?, ?)").run(
              value.threadId,
              value.agentId,
              this.get(channelId).name,
              now,
              now,
              requiredNumberColumn(event, "sequence"),
            );
            db.prepare("INSERT INTO projection_channel_contexts(channel_id, agent_id, thread_id) VALUES (?, ?, ?)").run(
              channelId,
              value.agentId,
              value.threadId,
            );
            break;
          }
          case "channel.context-accepted": {
            if (
              !isString(value.agentId) ||
              !isString(value.sessionId) ||
              typeof value.throughSequence !== "number" ||
              typeof value.summaryVersion !== "number"
            )
              throw new Error("Invalid context acceptance event.");
            db.prepare(
              "UPDATE projection_channel_contexts SET session_id = ?, through_sequence = ?, summary_version = ? WHERE channel_id = ? AND agent_id = ?",
            ).run(value.sessionId, value.throughSequence, value.summaryVersion, channelId, value.agentId);
            break;
          }
          case "channel.summarized": {
            if (!isString(value.text) || typeof value.version !== "number" || typeof value.throughSequence !== "number")
              throw new Error("Invalid channel summary event.");
            db.prepare("INSERT OR REPLACE INTO projection_channel_summaries VALUES (?, ?, ?, ?)").run(
              channelId,
              value.version,
              value.throughSequence,
              value.text,
            );
            break;
          }
          case "channel.read": {
            if (!isString(value.memberId) || typeof value.throughSequence !== "number")
              throw new Error("Invalid channel read event.");
            db.prepare(
              "INSERT INTO projection_channel_reads VALUES (?, ?, ?) ON CONFLICT(channel_id, member_id) DO UPDATE SET through_sequence = MAX(through_sequence, excluded.through_sequence)",
            ).run(channelId, value.memberId, value.throughSequence);
            break;
          }
          default:
            throw new Error("Unknown channel event.");
        }
      }
      db.exec("COMMIT");
    } catch (error) {
      if (db.isTransaction) db.exec("ROLLBACK");
      throw error;
    }
  }

  #afterChange(change: ChannelChange): void {
    const channelId = change.channel.id;
    if (change.tasks.length > 0)
      for (const owner of this.#ownerChannels("projection_channel_tasks", "task_id", change.tasks, channelId))
        this.#tasks.delete(owner);
    if (change.assignments.length > 0) this.#agentAssignments.clear();
    if (change.assignments.length > 0)
      for (const owner of this.#ownerChannels(
        "projection_channel_assignments",
        "assignment_id",
        change.assignments,
        channelId,
      ))
        this.#assignments.delete(owner);
    const flags = this.#messageFlags.get(channelId);
    if (!flags || change.messages.length === 0) return;
    const ids = change.messages.map((message) => message.id);
    for (const id of ids) flags.delete(id);
    for (const row of databaseRows(
      this.database.connection
        .prepare(`${MESSAGE_FLAGS_SQL} AND message_id IN (SELECT value FROM json_each(?))`)
        .all(ROUTING_EVENT_PATTERN, channelId, JSON.stringify(ids)),
    ))
      flags.set(requiredStringColumn(row, "message_id"), messageFlags(row));
  }

  #ownerChannels(
    table: "projection_channel_tasks" | "projection_channel_assignments",
    column: "task_id" | "assignment_id",
    rows: readonly { id: string }[],
    channelId: string,
  ): Set<string> {
    const owners = new Set([channelId]);
    for (const row of databaseRows(
      this.database.connection
        .prepare(`SELECT DISTINCT channel_id FROM ${table} WHERE ${column} IN (SELECT value FROM json_each(?))`)
        .all(JSON.stringify(rows.map((row) => row.id))),
    ))
      owners.add(requiredStringColumn(row, "channel_id"));
    return owners;
  }

  #forget(channelId: string): void {
    this.#agentAssignments.clear();
    this.#tasks.delete(channelId);
    this.#assignments.delete(channelId);
    this.#messageFlags.delete(channelId);
  }

  #flagsOf(channelId: string): ReadonlyMap<string, MessageFlags> {
    const cached = this.#messageFlags.get(channelId);
    if (cached) return cached;
    const flags = new Map<string, MessageFlags>();
    for (const row of databaseRows(
      this.database.connection.prepare(MESSAGE_FLAGS_SQL).all(ROUTING_EVENT_PATTERN, channelId),
    ))
      flags.set(requiredStringColumn(row, "message_id"), messageFlags(row));
    this.#messageFlags.set(channelId, flags);
    return flags;
  }

  /** A stored cursor of zero is a reader that read nothing, not a reader with no cursor at all. */
  #hasReadCursor(channelId: string, memberId: string): boolean {
    return (
      this.database.connection
        .prepare("SELECT 1 FROM projection_channel_reads WHERE channel_id = ? AND member_id = ?")
        .get(channelId, memberId) !== undefined
    );
  }
}

/**
 * Every routine this channel ever owned, not only the ones it owns now.
 *
 * `RoutineStore.delete()` drops the projection row and keeps the event log, so a routine the user
 * deleted before the channel is invisible to `projection_channel_routines`. Its instruction text
 * lives on in the `channel-routine.created` payload, and permanent deletion promises that no such
 * text stays behind. The event log names its owner, and the run rows carry the channel, so the two
 * together cover a routine whichever of them outlived it.
 */
function channelRoutineIds(db: DatabaseSync, channelId: string): string[] {
  const ids = new Set<string>();
  for (const row of databaseRows(
    db
      .prepare(
        `SELECT DISTINCT aggregate_id FROM orchestration_events
         WHERE aggregate_type = 'channel-routine' AND json_extract(payload_json, '$.ownerId') = ?`,
      )
      .all(channelId),
  )) {
    ids.add(requiredStringColumn(row, "aggregate_id"));
  }
  for (const table of ["projection_channel_routines", "projection_channel_routine_runs"]) {
    for (const row of databaseRows(
      db.prepare(`SELECT DISTINCT routine_id FROM ${table} WHERE channel_id = ?`).all(channelId),
    )) {
      ids.add(requiredStringColumn(row, "routine_id"));
    }
  }
  return [...ids];
}

const CHANNEL_PREVIEW_SOURCE_LIMIT = CHANNEL_PREVIEW_LIMIT * 16;

/**
 * One line for a sidebar row. A message can carry no text at all (an attachment, or a question
 * the agent asked), so fall back to a description of what arrived instead of showing an empty row.
 */
function previewText(entry: ChannelMessage): string {
  // The text is Markdown with mention markup (`@[Chief](agent:chief)`): show what a reader sees.
  // Only the start is parsed, so a very long message does not slow down every channel list.
  const text = markdownPreviewText(entry.message.text.slice(0, CHANNEL_PREVIEW_SOURCE_LIMIT));
  if (text.length > 0) return text.slice(0, CHANNEL_PREVIEW_LIMIT);
  if (entry.message.questionPrompt) return "Asked a question";
  if (entry.message.attachments?.length) return "Sent an attachment";
  return "Sent a message";
}

const ID_BATCH = 500;
const CHANNEL_RESULT_ID_PREFIX = "channel-result-";

function prefixEnd(prefix: string): string {
  return `${prefix.slice(0, -1)}${String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1)}`;
}

function storedChannelMessage(row: DynamicRecord): ChannelMessage {
  const value = JSON.parse(requiredStringColumn(row, "message_json"));
  if (!isChannelMessage(value)) throw new Error("Invalid stored channel message.");
  return value;
}

function compareChannelMessages(a: ChannelMessage, b: ChannelMessage): number {
  return a.sequence - b.sequence || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
}

function stoppedState(task: ChannelTask): boolean {
  return task.state === "paused" || task.state === "failed";
}

function decodeAssignment(value: unknown): ChannelAssignment {
  if (
    !isDynamicRecord(value) ||
    !isString(value.id) ||
    !isString(value.channelId) ||
    !isString(value.taskId) ||
    !isString(value.agentId) ||
    !(value.pendingRevision === null || typeof value.pendingRevision === "number") ||
    !(value.pendingOutcome === null || isString(value.pendingOutcome)) ||
    !Array.isArray(value.awaitedTaskIds) ||
    !value.awaitedTaskIds.every(isString) ||
    typeof value.taskRevision !== "number" ||
    typeof value.throughSequence !== "number" ||
    typeof value.summaryVersion !== "number" ||
    !(value.deliveryId === null || isString(value.deliveryId)) ||
    !(value.turnId === null || isString(value.turnId)) ||
    !(
      value.state === "queued" ||
      value.state === "starting" ||
      value.state === "running" ||
      value.state === "completed" ||
      value.state === "failed" ||
      value.state === "interrupted"
    )
  )
    throw new Error("Invalid stored channel assignment.");
  return {
    id: value.id,
    channelId: value.channelId,
    taskId: value.taskId,
    agentId: value.agentId,
    taskRevision: value.taskRevision,
    requestMessageId: isString(value.requestMessageId) ? value.requestMessageId : null,
    // An assignment written before resources were stored reads as a host reservation, which
    // conflicts with every other task. That holds the channel until the assignment ends, rather
    // than letting a second agent take a resource this one may still use.
    resources: Array.isArray(value.resources) && value.resources.every(isString) ? value.resources : ["host"],
    awaitedTaskIds: value.awaitedTaskIds,
    pendingRevision: value.pendingRevision,
    pendingOutcome: value.pendingOutcome,
    throughSequence: value.throughSequence,
    summaryVersion: value.summaryVersion,
    deliveryId: value.deliveryId,
    turnId: value.turnId,
    state: value.state,
  };
}
