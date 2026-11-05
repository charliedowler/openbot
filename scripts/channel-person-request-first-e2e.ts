import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { ChannelMessage, ChannelTask } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelService } from "../src/backend/channel-service";
import type { ChannelAssignment } from "../src/backend/channel-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

// A channel lead with a queue of routine-made tasks (intake events, pod wakes) started them in the
// order they were made, so a new message of the person waited behind every one of them, one turn
// each. The lead was also "busy" for the pump while its own chat only had queued messages. These
// checks drive the real ChannelService on a temporary database: a fresh request of the person is
// assigned before older routine requests and before woken requests of the person, and queued
// messages of the owner's own chat do not hold it, while a running turn still does.

const output = resolve(import.meta.dirname, "../.openbot-build/channel-person-request-first-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-person-first-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const errors: unknown[] = [];
const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };
let busy = true;
let turnRunning = true;

const channels = new ChannelService(database, mailbox, {
  agents: () => agents.list(),
  generate: () => Effect.die(new Error("Every request names its member, so no routing turn runs.")),
  schedule: () => {},
  changed: () => {},
  busy: () => busy,
  turnRunning: () => turnRunning,
  interrupt: () => Effect.void,
  error: (error) => {
    errors.push(error);
  },
});

const person = { id: "local", name: "Example member" };
const intake = { kind: "member", id: "routine:intake", name: "Dispatch intake" } as const;

async function settle(label: string, ready: () => boolean) {
  const deadline = Date.now() + 30_000;
  for (;;) {
    if (errors.length) throw errors[0];
    if (ready()) return;
    if (Date.now() > deadline) throw new Error(`No state change: ${label}`);
    await new Promise((done) => setTimeout(done, 10));
  }
}

async function quiet(ms = 400) {
  await new Promise((done) => setTimeout(done, ms));
  if (errors.length) throw errors[0];
}

async function channelWith(name: string, leadAgentId: string) {
  const channelId = randomUUID();
  await runCauseEffect(
    channels.command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId,
        draft: {
          name,
          title: "",
          instructions: "Answer requests about the release.",
          members: [{ agentId: leadAgentId }],
          leadAgentId,
        },
      },
      person,
    ),
  );
  return channelId;
}

function request(channelId: string, taskId: string, author: ChannelMessage["author"], text: string): ChannelMessage {
  const id = randomUUID();
  return {
    id,
    channelId,
    taskId,
    author,
    sequence: 0,
    superseded: false,
    message: { id, text, author: "user", createdAt: new Date(Date.now() - 60_000).toISOString(), status: "completed" },
  };
}

function queuedTask(
  channelId: string,
  ownerAgentId: string,
  requestMessageId: string,
  instruction: string,
): ChannelTask {
  const id = randomUUID();
  return {
    id,
    channelId,
    parentTaskId: null,
    rootTaskId: id,
    ownerAgentId,
    requestMessageId,
    instruction,
    attachmentDraftIds: [],
    expectedResult: "Complete the requested work and report the result.",
    sourceMessageIds: [requestMessageId],
    dependencies: [],
    resources: [],
    state: "queued",
    revision: 0,
    assignmentCount: 0,
    error: null,
  };
}

/** A queued task that a routine run requested, as the Dispatch intake routine makes for each event. */
function seedRoutineTask(channelId: string, ownerAgentId: string, label: string): ChannelTask {
  const taskId = randomUUID();
  const message = request(channelId, taskId, intake, `${label}: handle the OpenBot Control event.`);
  const task = {
    ...queuedTask(channelId, ownerAgentId, message.id, message.message.text),
    id: taskId,
    rootTaskId: taskId,
  };
  channels.store.update(channels.store.get(channelId), { messages: [message], tasks: [task] });
  return task;
}

/** A queued task of the person whose request an earlier assignment already carried: a woken wait. */
function seedWokenPersonTask(channelId: string, ownerAgentId: string): ChannelTask {
  const taskId = randomUUID();
  const author = { kind: "member", id: person.id, name: person.name } as const;
  const message = request(channelId, taskId, author, "Earlier request: ship the fix and report.");
  const task: ChannelTask = {
    ...queuedTask(channelId, ownerAgentId, message.id, message.message.text),
    id: taskId,
    rootTaskId: taskId,
    assignmentCount: 1,
  };
  const earlier: ChannelAssignment = {
    id: randomUUID(),
    channelId,
    taskId,
    agentId: ownerAgentId,
    taskRevision: 0,
    requestMessageId: message.id,
    resources: [],
    deliveryId: null,
    turnId: null,
    state: "completed",
    throughSequence: 0,
    summaryVersion: 0,
    awaitedTaskIds: [],
    pendingRevision: null,
    pendingOutcome: null,
  };
  channels.store.update(channels.store.get(channelId), { messages: [message], tasks: [task], assignments: [earlier] });
  return task;
}

async function send(channelId: string, text: string, recipientAgentId: string) {
  await runCauseEffect(
    channels.command(
      {
        type: "send",
        operationId: randomUUID(),
        channelId,
        text,
        recipientAgentId,
        replyToMessageId: null,
        attachmentDraftIds: [],
      },
      person,
    ),
  );
}

function personTask(channelId: string): ChannelTask {
  const task = channels.store
    .tasks(channelId)
    .find(
      (item) =>
        channels.store.message(channelId, item.requestMessageId)?.author.id === person.id && item.assignmentCount === 0,
    );
  assert.ok(task, "The person's request has a task.");
  return task;
}

function started(channelId: string): ChannelAssignment[] {
  return channels.store.assignments(channelId).filter((assignment) => assignment.deliveryId);
}

async function wake() {
  await runCauseEffect(channels.wake());
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  // One lead per scenario: an active assignment of a lead holds its other tasks in every channel.
  const create = async (name: string) =>
    (
      await runCauseEffect(
        agents.createAgent({
          name,
          description: `Synthetic ${name}`,
          avatarSeed: name.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
          avatarHue: null,
        }),
      )
    ).id;

  // 1. Routine requests queued before the person's message do not start first.
  busy = true;
  turnRunning = true;
  const dispatchLead = await create("Compass");
  const dispatch = await channelWith("Dispatch", dispatchLead);
  const intakeTasks = [1, 2, 3].map((index) => seedRoutineTask(dispatch, dispatchLead, `Event ${index}`));
  await send(dispatch, "What PRs are older than this week?", dispatchLead);
  await quiet();
  assert.equal(started(dispatch).length, 0, "A busy owner starts nothing.");
  const fresh = personTask(dispatch);
  busy = false;
  turnRunning = false;
  await wake();
  await settle("first assignment in Dispatch", () => started(dispatch).length > 0);
  await quiet();
  assert.deepEqual(
    started(dispatch).map((assignment) => assignment.taskId),
    [fresh.id],
    "The person's request is assigned before three older routine requests.",
  );
  assert.ok(intakeTasks.every((task) => channels.store.task(dispatch, task.id)?.state === "queued"));
  report.checks.push("A fresh request of the person starts before older routine requests of the same owner.");

  // 2. Without a request of the person, the routine requests keep their order.
  busy = true;
  turnRunning = true;
  const intakeLead = await create("Intake lead");
  const intakeOnly = await channelWith("Intake only", intakeLead);
  const ordered = [1, 2, 3].map((index) => seedRoutineTask(intakeOnly, intakeLead, `Event ${index}`));
  busy = false;
  turnRunning = false;
  await wake();
  await settle("first assignment in the intake channel", () => started(intakeOnly).length > 0);
  await quiet();
  assert.deepEqual(
    started(intakeOnly).map((assignment) => assignment.taskId),
    [ordered[0]?.id],
    "Routine requests start oldest first.",
  );
  report.checks.push("Routine requests keep their order when no request of the person waits.");

  // 3. A woken request of the person does not pass a fresh one.
  busy = true;
  turnRunning = true;
  const wakesLead = await create("Wakes lead");
  const wakes = await channelWith("Wakes", wakesLead);
  const woken = seedWokenPersonTask(wakes, wakesLead);
  await send(wakes, "Where is the iOS work at?", wakesLead);
  await quiet();
  const freshAfterWake = personTask(wakes);
  busy = false;
  turnRunning = false;
  await wake();
  await settle("first assignment in the wakes channel", () => started(wakes).length > 0);
  await quiet();
  assert.deepEqual(
    started(wakes).map((assignment) => assignment.taskId),
    [freshAfterWake.id],
    "The fresh request starts before the woken one.",
  );
  assert.equal(channels.store.task(wakes, woken.id)?.state, "queued");
  report.checks.push("A fresh request of the person starts before a woken request of the person.");

  // 4. Queued messages of the owner's own chat do not hold the person's request; a running turn does.
  busy = true;
  turnRunning = false;
  const queueLead = await create("Queue lead");
  const heldByQueue = await channelWith("Queued own chat", queueLead);
  const intakeHeld = seedRoutineTask(heldByQueue, queueLead, "Event held");
  await send(heldByQueue, "How is the Wise fix going?", queueLead);
  const freshHeld = personTask(heldByQueue);
  await wake();
  await settle("assignment while own chat has queued messages", () => started(heldByQueue).length > 0);
  await quiet();
  assert.deepEqual(
    started(heldByQueue).map((assignment) => assignment.taskId),
    [freshHeld.id],
    "The person's request starts while the owner only has queued messages.",
  );
  assert.equal(channels.store.task(heldByQueue, intakeHeld.id)?.state, "queued", "The routine request still waits.");
  report.checks.push("Queued own-chat messages of the owner do not hold a fresh request of the person.");

  busy = true;
  turnRunning = true;
  const turnLead = await create("Turn lead");
  const heldByTurn = await channelWith("Running turn", turnLead);
  await send(heldByTurn, "Progress?", turnLead);
  await wake();
  await quiet();
  assert.equal(started(heldByTurn).length, 0, "A running turn of the owner still holds the person's request.");
  report.checks.push("A running turn of the owner holds a fresh request of the person.");

  assert.deepEqual(errors, []);
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? error.message : String(error);
  throw error;
} finally {
  await runCauseEffect(channels.stop()).catch(() => undefined);
  database.close();
  mkdirSync(output, { recursive: true });
  writeFileSync(join(output, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}

process.stdout.write(`${JSON.stringify(report)}\n`);
