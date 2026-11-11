// Checks the cost of the channel reads on a server with long channels (0061). On 11 November the
// live server had 9 channels; the four large ones held 204 to 1,094 tasks, 390 to 1,785 assignments
// and 1,451 to 6,028 messages. The 11 November profile of a copy of the server put 12.6% of the
// main-thread ticks in ChannelStore.assignments (each call reads and parses every assignment row of
// the channel) and 7.5% in ChannelStore.list. The channel pump asked for the assignments once for
// each queued task (#freshPersonRequest) and read every task and assignment of every channel for
// each candidate (#scheduling). The live slow-call log shows ChannelStore.list at 300 to 420 ms on
// about 6 calls a minute.
//
// A pump wake must read each channel's rows a bounded number of times, and ChannelStore.list must
// not count the unread messages of every channel again on each call. The reads must give the same
// answer as a store with no cache (a second ChannelStore on the same database) after every change:
// new tasks, task state changes, new assignments, a task row that moves channel through its id,
// new messages, a message changed in place, a deleted message, a read cursor and a deleted channel.
//
// Runs the real channel service, channel store and SQLite on a temporary profile. No app,
// provider, account or network is started.
//
// Run: node --import tsx scripts/channel-read-cache-e2e.ts
// Writes .openbot-build/channel-read-cache-e2e/report.json and exits non-zero on a failure.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { ChannelMessage, ChannelTask } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelImages } from "../src/backend/channel-images";
import { ChannelService } from "../src/backend/channel-service";
import { type ChannelAssignment, ChannelStore } from "../src/backend/channel-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const LARGE = [
  { tasks: 1_094, assignments: 1_501, messages: 6_028, read: 400 },
  { tasks: 1_088, assignments: 1_724, messages: 2_983, read: 300 },
  { tasks: 721, assignments: 1_785, messages: 4_483, read: 300 },
  { tasks: 204, assignments: 390, messages: 1_451, read: 100 },
];
const ITEM_TYPES = [
  { itemType: "commentary" },
  { itemType: "plan" },
  { itemType: "subagents" },
  { itemType: "channel-change" },
  { itemType: "channel-routing-event:assigned" },
];
const SMALL = 5;
const AGENTS = 47;
const QUEUED_PERSON_TASKS = 24;
const MEMBER = { id: "local", name: "Example member" };
const TEXT = "The nightly build of the release branch passed. Check the notes and report each change. ".repeat(16);
const MAX_WHOLE_CHANNEL_READS_PER_WAKE = 2;
const MAX_LIST_MS = 10;

const output = resolve(import.meta.dirname, "../.openbot-build/channel-read-cache-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-read-cache-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const errors: unknown[] = [];
const report: {
  passed: boolean;
  scale: Record<string, number>;
  pump?: { wakes: number; rowReads: number; rowsDecoded: number; readMs: number; channels: number };
  list?: { calls: number; medianMs: number; maxMs: number; afterChangeMs: number[] };
  checks: string[];
  failedLimits?: string[];
  error?: string;
} = { passed: false, scale: {}, checks: [] };
let busy = true;
const channels = new ChannelService(database, mailbox, new ChannelImages(join(root, "channel-images")), {
  agents: () => agents.list(),
  generate: () => Effect.die(new Error("No routing model is used by this check.")),
  schedule: () => {},
  changed: () => {},
  busy: () => busy,
  turnRunning: () => busy,
  interrupt: () => Effect.void,
  error: (error) => {
    errors.push(error);
  },
});
const store = channels.store;

let rowReads = 0;
let rowsDecoded = 0;
let readMs = 0;
let counting = false;
const firstRows = new Map<string, unknown>();
function noteRead(key: string, rows: readonly unknown[], startedAt: number) {
  if (!counting) return;
  readMs += performance.now() - startedAt;
  if (rows.length > 0 && firstRows.get(key) !== rows[0]) {
    rowReads += 1;
    rowsDecoded += rows.length;
  }
  firstRows.set(key, rows[0]);
}
const readTasks = store.tasks.bind(store);
store.tasks = (channelId: string) => {
  const startedAt = performance.now();
  const rows = readTasks(channelId);
  noteRead(`tasks:${channelId}`, rows, startedAt);
  return rows;
};
const readAssignments = store.assignments.bind(store);
store.assignments = (channelId: string) => {
  const startedAt = performance.now();
  const rows = readAssignments(channelId);
  noteRead(`assignments:${channelId}`, rows, startedAt);
  return rows;
};

const truth = () => new ChannelStore(database);
const ACTIVE_STATES: ChannelAssignment["state"][][] = [["running", "starting", "queued"], ["completed"]];
let sampleAgents: string[] = [];

function sameReads(label: string, channelIds: readonly string[]) {
  const fresh = truth();
  for (const channelId of channelIds) {
    assert.deepEqual(store.tasks(channelId), fresh.tasks(channelId), `${label}: tasks of ${channelId}`);
    assert.deepEqual(
      store.assignments(channelId),
      fresh.assignments(channelId),
      `${label}: assignments of ${channelId}`,
    );
  }
  for (const signedOut of [false, true])
    assert.deepEqual(store.list(MEMBER.id, signedOut), fresh.list(MEMBER.id, signedOut), `${label}: list`);
  assert.deepEqual(store.list("other-member"), fresh.list("other-member"), `${label}: list of another member`);
  for (const agentId of sampleAgents)
    for (const states of ACTIVE_STATES)
      assert.deepEqual(
        store.agentAssignment(agentId, states),
        fresh.agentAssignment(agentId, states),
        `${label}: active assignment of ${agentId}`,
      );
}

function task(channelId: string, ownerAgentId: string, state: ChannelTask["state"], requestMessageId = randomUUID()) {
  const id = randomUUID();
  return {
    id,
    channelId,
    parentTaskId: null,
    rootTaskId: id,
    ownerAgentId,
    requestMessageId,
    instruction: `Earlier request: ${TEXT.slice(0, 600)}`,
    attachmentDraftIds: [],
    expectedResult: "Complete the requested work and report the result.",
    sourceMessageIds: [],
    dependencies: [],
    resources: [],
    state,
    revision: 1,
    assignmentCount: 1,
    error: null,
  } satisfies ChannelTask;
}

function assignment(item: ChannelTask, state: ChannelAssignment["state"] = "completed"): ChannelAssignment {
  return {
    id: randomUUID(),
    channelId: item.channelId,
    taskId: item.id,
    agentId: item.ownerAgentId ?? "none",
    taskRevision: item.revision,
    requestMessageId: item.requestMessageId,
    resources: [],
    deliveryId: null,
    turnId: randomUUID(),
    state,
    throughSequence: 10,
    summaryVersion: 0,
    awaitedTaskIds: [],
    pendingRevision: null,
    pendingOutcome: null,
  };
}

function message(
  channelId: string,
  author: ChannelMessage["author"],
  index: number,
  fields: Partial<ChannelMessage["message"]> = {},
): ChannelMessage {
  const id = randomUUID();
  return {
    id,
    channelId,
    sequence: 0,
    author,
    taskId: null,
    superseded: false,
    message: {
      id,
      author: author.kind === "member" ? "user" : "assistant",
      text: `${index}: ${TEXT}`,
      createdAt: new Date(Date.UTC(2026, 10, 1) + index * 1000).toISOString(),
      status: "completed",
      ...fields,
    },
  };
}

async function chunks<T>(items: T[], size: number, write: (part: T[]) => void) {
  for (let index = 0; index < items.length; index += size) write(items.slice(index, index + size));
}

async function settle(label: string, ready: () => boolean) {
  const deadline = Date.now() + 60_000;
  let stableSince = Date.now();
  let last = rowReads;
  for (;;) {
    if (errors.length) throw errors[0];
    if (rowReads !== last) {
      last = rowReads;
      stableSince = Date.now();
    }
    if (ready() && Date.now() - stableSince >= 400) return;
    if (Date.now() > deadline) throw new Error(`No state change: ${label}`);
    await new Promise((done) => setTimeout(done, 20));
  }
}

const failedLimits: string[] = [];
function limit(passed: boolean, message: string) {
  if (!passed) failedLimits.push(message);
}
const median = (values: number[]) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)] ?? 0;
const round = (value: number) => Math.round(value * 10) / 10;

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  const members: string[] = [];
  for (let index = 0; index < AGENTS; index += 1) {
    const created = await runCauseEffect(
      agents.createAgent({
        name: `Agent ${index}`,
        description: "Synthetic member",
        avatarSeed: `agent-${index}`,
        avatarHue: null,
      }),
    );
    members.push(created.id);
  }
  sampleAgents = members.slice(0, 6);
  const channelIds: string[] = [];
  for (let index = 0; index < LARGE.length + SMALL; index += 1) {
    const channelId = randomUUID();
    const lead = members[index] ?? "";
    await runCauseEffect(
      channels.command(
        {
          type: "save",
          operationId: randomUUID(),
          channelId,
          draft: {
            name: `Channel ${index}`,
            title: "",
            instructions: "Check the requested work.",
            members: members.slice(index, index + 8).map((agentId) => ({ agentId })),
            leadAgentId: lead,
          },
        },
        MEMBER,
      ),
    );
    channelIds.push(channelId);
  }

  for (const [index, scale] of LARGE.entries()) {
    const channelId = channelIds[index] ?? "";
    const owner = members[index] ?? "";
    const tasks = Array.from({ length: scale.tasks }, () => task(channelId, owner, "completed"));
    const assignments: ChannelAssignment[] = [];
    for (let n = 0; n < scale.assignments; n += 1) {
      const item = tasks[n % tasks.length];
      if (item) assignments.push(assignment(item));
    }
    const messages = Array.from({ length: scale.messages }, (_, n) =>
      message(
        channelId,
        n % 11 === 2
          ? { kind: "member", id: MEMBER.id, name: MEMBER.name }
          : { kind: "agent", id: members[((index + n) % 8) + index] ?? owner, name: "Agent" },
        n,
        n % 11 < 3 ? {} : ITEM_TYPES[n % ITEM_TYPES.length],
      ),
    );
    await chunks(tasks, 400, (part) => store.update(store.get(channelId), { tasks: part }));
    await chunks(assignments, 400, (part) => store.update(store.get(channelId), { assignments: part }));
    await chunks(messages, 400, (part) => store.update(store.get(channelId), { messages: part }));
    store.markRead(channelId, MEMBER.id, scale.read, randomUUID());
  }
  report.scale = {
    channels: channelIds.length,
    agents: AGENTS,
    tasks: LARGE.reduce((sum, item) => sum + item.tasks, 0),
    assignments: LARGE.reduce((sum, item) => sum + item.assignments, 0),
    messages: LARGE.reduce((sum, item) => sum + item.messages, 0),
    queuedPersonTasks: QUEUED_PERSON_TASKS,
  };
  sameReads("seeded", channelIds);
  report.checks.push("The seeded channels read the same as a store with no cache.");

  const busyChannel = channelIds[0] ?? "";
  for (let index = 0; index < QUEUED_PERSON_TASKS; index += 1)
    await runCauseEffect(
      channels.command(
        {
          type: "send",
          operationId: randomUUID(),
          channelId: busyChannel,
          text: `Check release ${index}.`,
          recipientAgentId: members[0] ?? "",
          replyToMessageId: null,
          attachmentDraftIds: [],
        },
        MEMBER,
      ),
    );
  await settle(
    "queued person requests",
    () =>
      truth()
        .tasks(busyChannel)
        .filter((item) => item.state === "queued").length >= QUEUED_PERSON_TASKS,
  );
  sameReads("queued person requests", channelIds);
  report.checks.push(`${QUEUED_PERSON_TASKS} person requests wait in one channel while their owner is busy.`);

  rowReads = 0;
  rowsDecoded = 0;
  readMs = 0;
  counting = true;
  const wakes = 5;
  for (let wake = 0; wake < wakes; wake += 1) {
    channels.event({
      type: "turn-completed",
      agentId: members[0] ?? "",
      threadId: randomUUID(),
      turnId: randomUUID(),
      status: "completed",
    });
    await settle(`wake ${wake}`, () => true);
  }
  counting = false;
  report.pump = {
    wakes,
    rowReads,
    rowsDecoded,
    readMs: round(readMs),
    channels: channelIds.length,
  };
  const readsPerWake = rowReads / wakes;
  limit(
    readsPerWake <= MAX_WHOLE_CHANNEL_READS_PER_WAKE,
    `A pump wake read the whole task or assignment rows of a channel ${round(readsPerWake)} times (${Math.round(rowsDecoded / wakes)} rows); the limit is ${MAX_WHOLE_CHANNEL_READS_PER_WAKE}.`,
  );
  report.checks.push(
    `A pump wake reads the whole task or assignment rows of a channel ${round(readsPerWake)} times, ${Math.round(rowsDecoded / wakes)} rows (limit ${MAX_WHOLE_CHANNEL_READS_PER_WAKE} reads); ${round(readMs / wakes)} ms in the task and assignment reads for each wake.`,
  );

  const listMs: number[] = [];
  for (let call = 0; call < 12; call += 1) {
    const startedAt = performance.now();
    store.list(MEMBER.id, true);
    listMs.push(performance.now() - startedAt);
  }
  const afterChangeMs: number[] = [];
  const timedList = () => {
    const startedAt = performance.now();
    store.list(MEMBER.id, true);
    afterChangeMs.push(round(performance.now() - startedAt));
  };

  const streaming = message(busyChannel, { kind: "agent", id: members[1] ?? "", name: "Agent" }, 90_000, {
    status: "streaming",
  });
  store.update(store.get(busyChannel), { messages: [streaming] });
  timedList();
  sameReads("a new streaming message", channelIds);
  store.update(store.get(busyChannel), {
    messages: [{ ...streaming, message: { ...streaming.message, text: `${streaming.message.text} More.` } }],
  });
  timedList();
  store.update(store.get(busyChannel), {
    messages: [{ ...streaming, message: { ...streaming.message, status: "completed", itemType: "commentary" } }],
  });
  timedList();
  sameReads("a message changed in place", channelIds);
  report.checks.push("A message added and changed in place reads the same as a store with no cache.");

  const unreadTarget = truth()
    .page(channelIds[1] ?? "")
    .messages.at(-1);
  assert.ok(unreadTarget);
  store.deleteMessage(store.get(channelIds[1] ?? ""), unreadTarget);
  timedList();
  sameReads("a deleted message", channelIds);
  report.checks.push("A deleted message reads the same as a store with no cache.");

  store.markRead(channelIds[2] ?? "", MEMBER.id, 4_400, randomUUID());
  timedList();
  sameReads("a read cursor", channelIds);
  report.checks.push("A moved read cursor reads the same as a store with no cache.");

  const queued = truth()
    .tasks(busyChannel)
    .filter((item) => item.state === "queued");
  const first = queued[0];
  assert.ok(first);
  store.update(store.get(busyChannel), {
    tasks: [{ ...first, state: "running", revision: first.revision + 1 }],
    assignments: [assignment(first, "running")],
  });
  timedList();
  sameReads("a task state change and an assignment", channelIds);
  const moved = truth().tasks(channelIds[3] ?? "")[0];
  assert.ok(moved);
  store.update(store.get(channelIds[4] ?? ""), {
    tasks: [{ ...moved, channelId: channelIds[4] ?? "", state: "queued" }],
  });
  sameReads("a task row written through another channel", channelIds);
  report.checks.push(
    "Task and assignment writes, also one through another channel, read the same as a store with no cache.",
  );

  const second = queued[1];
  assert.ok(second);
  const orphan = { ...assignment(second, "running"), taskId: randomUUID() };
  assert.throws(() =>
    store.update(store.get(busyChannel), {
      tasks: [{ ...second, state: "failed", revision: second.revision + 1 }],
      assignments: [orphan],
    }),
  );
  sameReads("a write that rolls back", channelIds);
  assert.equal(store.tasks(busyChannel).find((item) => item.id === second.id)?.state, "queued");
  report.checks.push("A write that rolls back after its task row leaves no stale rows.");

  const removed = channelIds[LARGE.length] ?? "";
  store.delete(removed);
  const kept = channelIds.filter((id) => id !== removed);
  timedList();
  sameReads("a deleted channel", kept);
  assert.deepEqual(store.tasks(removed), []);
  assert.deepEqual(store.assignments(removed), []);
  report.checks.push("A deleted channel reads the same as a store with no cache.");

  store.rebuild(channelIds[3] ?? "");
  sameReads("a rebuilt channel", kept);
  report.checks.push("A rebuilt channel reads the same as a store with no cache.");

  report.list = {
    calls: listMs.length,
    medianMs: round(median(listMs)),
    maxMs: round(Math.max(...listMs)),
    afterChangeMs,
  };
  limit(
    median(listMs) <= MAX_LIST_MS,
    `ChannelStore.list took a median ${round(median(listMs))} ms; the limit is ${MAX_LIST_MS} ms.`,
  );
  report.checks.push(`ChannelStore.list takes a median ${round(median(listMs))} ms (limit ${MAX_LIST_MS} ms).`);
  assert.equal(errors.length, 0);
  assert.deepEqual(failedLimits, []);
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  busy = false;
  await runCauseEffect(channels.stop()).catch(() => undefined);
  database.close();
  report.failedLimits = failedLimits;
  mkdirSync(output, { recursive: true });
  writeFileSync(join(output, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}

process.stdout.write(`${JSON.stringify(report)}\n`);
