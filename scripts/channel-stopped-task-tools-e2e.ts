import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary, ChannelMessage, ChannelTask } from "@openbot/contracts/ipc";
import { Effect, Result } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelToolInputError } from "../src/backend/channel-effects";
import { ChannelHistory } from "../src/backend/channel-history";
import { ChannelImages } from "../src/backend/channel-images";
import { ChannelMemoryStore } from "../src/backend/channel-memory-store";
import { ChannelService } from "../src/backend/channel-service";
import { CHANNEL_TOOL_DEFINITIONS } from "../src/backend/channel-tools";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const output = resolve(import.meta.dirname, "../.openbot-build/channel-stopped-task-tools-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-stopped-tasks-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const errors: unknown[] = [];
const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };
const channels = new ChannelService(database, mailbox, new ChannelImages(join(root, "channel-images")), {
  agents: () => agents.list(),
  generate: () => Effect.die(new Error("No routing model is used by this check.")),
  schedule: () => {},
  changed: () => {},
  busy: () => true,
  interrupt: () => Effect.void,
  error: (error) => {
    errors.push(error);
  },
});
const pass = (check: string) => report.checks.push(check);
const person = { id: "local", name: "Alex" };
const DAY = 86_400_000;

function task(
  channelId: string,
  ownerAgentId: string,
  state: ChannelTask["state"],
  options: Partial<ChannelTask> = {},
) {
  const id = options.id ?? randomUUID();
  const requestMessageId = options.requestMessageId ?? randomUUID();
  return {
    id,
    channelId,
    parentTaskId: null,
    rootTaskId: id,
    ownerAgentId,
    requestMessageId,
    instruction: "Check the release notes.\nList each change.",
    attachmentDraftIds: [],
    expectedResult: "Complete the requested work and report the result.",
    sourceMessageIds: [requestMessageId],
    dependencies: [],
    resources: [],
    state,
    revision: 1,
    assignmentCount: 1,
    error: null,
    ...options,
  } satisfies ChannelTask;
}

function request(channelId: string, taskId: string, id: string, ageMs: number): ChannelMessage {
  return {
    id,
    channelId,
    taskId,
    author: { kind: "member", ...person },
    sequence: 0,
    superseded: false,
    message: {
      id,
      text: "Check the release notes.",
      author: "user",
      createdAt: new Date(Date.now() - ageMs).toISOString(),
      status: "completed",
    },
  };
}

interface Turn {
  agentId: string;
  turnId: string;
  taskId: string;
}

function working(channelId: string, owner: AgentSummary, options: Partial<ChannelTask> = {}): Turn {
  const current = task(channelId, owner.id, "running", options);
  const turnId = randomUUID();
  channels.store.update(channels.store.get(channelId), {
    tasks: [current],
    assignments: [
      {
        id: randomUUID(),
        channelId,
        taskId: current.id,
        agentId: owner.id,
        taskRevision: current.revision,
        requestMessageId: current.requestMessageId,
        resources: [],
        deliveryId: null,
        turnId,
        state: "running",
        throughSequence: 0,
        summaryVersion: 0,
        awaitedTaskIds: [],
        pendingRevision: null,
        pendingOutcome: null,
      },
    ],
  });
  return { agentId: owner.id, turnId, taskId: current.id };
}

async function call(turn: Turn, channelId: string, tool: string, args: unknown, callId: string = randomUUID()) {
  const result = await runCauseEffect(
    Effect.result(channels.tool(channelId, turn.agentId, turn.turnId, callId, tool, args)),
  );
  if (Result.isFailure(result)) {
    const cause = result.failure.cause;
    assert.ok(cause instanceof ChannelToolInputError, `Unexpected failure: ${String(cause)}`);
    return { refused: cause.message };
  }
  return { value: result.success };
}

const stored = (channelId: string, id: string) => channels.store.task(channelId, id);
const required = (channelId: string, id: string): ChannelTask => {
  const value = stored(channelId, id);
  assert.ok(value);
  return value;
};
const banners = (channelId: string) =>
  channels.store
    .page(channelId)
    .tasks.filter((item) => (item.state === "paused" || item.state === "failed") && item.error)
    .map((item) => item.id);

interface ListedTask {
  id: string;
  ownerAgentId: string | null;
  request: string;
  error: string | null;
  ageHours: number | null;
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  const create = (name: string) =>
    runCauseEffect(agents.createAgent({ name, description: name, avatarSeed: name.toLowerCase(), avatarHue: null }));
  const lead = await create("Lead");
  const parentOwner = await create("Planner");
  const worker = await create("Worker");
  const channelId = randomUUID();
  await runCauseEffect(
    channels.command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId,
        draft: {
          name: "Stopped work",
          title: "",
          instructions: "Check the requested work.",
          members: [{ agentId: lead.id }, { agentId: parentOwner.id }, { agentId: worker.id }],
          leadAgentId: lead.id,
        },
      },
      person,
    ),
  );

  const names = CHANNEL_TOOL_DEFINITIONS.map((definition) => definition.name);
  assert.ok(names.includes("channel_resume_task") && names.includes("channel_close_task"));
  pass("The channel tool list offers channel_resume_task and channel_close_task.");

  const planner = working(channelId, parentOwner);
  const changedChild = task(channelId, worker.id, "paused", {
    parentTaskId: planner.taskId,
    rootTaskId: planner.taskId,
    error: "The parent request changed.",
  });
  const staleRoot = task(channelId, worker.id, "paused", { error: "Stopped. Continue or reassign this task." });
  const staleChild = task(channelId, worker.id, "paused", {
    parentTaskId: staleRoot.id,
    rootTaskId: staleRoot.id,
    error: "Stopped. Continue or reassign this task.",
  });
  const failedRoot = task(channelId, worker.id, "failed", { error: "The provider failed." });
  const blockedRoot = task(channelId, worker.id, "paused", { error: "Stopped. Continue or reassign this task." });
  const activeChild = task(channelId, worker.id, "queued", {
    parentTaskId: blockedRoot.id,
    rootTaskId: blockedRoot.id,
  });
  channels.store.update(channels.store.get(channelId), {
    tasks: [changedChild, staleRoot, staleChild, failedRoot, blockedRoot, activeChild],
    messages: [
      request(channelId, staleRoot.id, staleRoot.requestMessageId, 3 * DAY),
      request(channelId, failedRoot.id, failedRoot.requestMessageId, DAY / 2),
    ],
  });

  const workerTurn = working(channelId, worker);
  const resumeByWorker = await call(workerTurn, channelId, "channel_resume_task", { taskId: changedChild.id });
  assert.match(resumeByWorker.refused ?? "", /Only the channel lead or the owner of the parent task/);
  const closeByWorker = await call(workerTurn, channelId, "channel_close_task", {
    taskId: staleRoot.id,
    reason: "No longer needed.",
  });
  assert.match(closeByWorker.refused ?? "", /Only the channel lead can close a task/);
  assert.equal(stored(channelId, changedChild.id)?.state, "paused");
  assert.equal(stored(channelId, staleRoot.id)?.state, "paused");
  pass("A member that is not the lead or the parent owner cannot resume or close a stopped task.");

  const closeByParentOwner = await call(planner, channelId, "channel_close_task", {
    taskId: changedChild.id,
    reason: "No longer needed.",
  });
  assert.match(closeByParentOwner.refused ?? "", /Only the channel lead can close a task/);
  pass("The owner of a parent task cannot close a task; only the lead can.");

  const resumed = await call(planner, channelId, "channel_resume_task", { taskId: changedChild.id });
  assert.deepEqual(resumed.value, { accepted: true, state: "queued" });
  const child = stored(channelId, changedChild.id);
  assert.equal(child?.state, "queued");
  assert.equal(child?.error, null);
  assert.equal(child?.revision, changedChild.revision + 1);
  assert.ok(!banners(channelId).includes(changedChild.id));
  pass("The parent owner resumes a child that stopped because the parent request changed, as Continue does.");

  const leadTurn = working(channelId, lead);
  const packet = await runCauseEffect(
    new ChannelHistory(
      channels.store,
      () => Effect.die(new Error("No summary")),
      new ChannelMemoryStore(database),
    ).prepare(required(channelId, leadTurn.taskId), lead, lead, null, []),
  );
  const listed = packet.text.split("Stopped tasks in this channel")[1] ?? "";
  const entries: ListedTask[] = JSON.parse(listed.split("\n")[1] ?? "[]");
  assert.deepEqual(
    entries.map((entry) => entry.id),
    [failedRoot.id, staleRoot.id, staleChild.id, blockedRoot.id],
  );
  const staleEntry = entries.find((entry) => entry.id === staleRoot.id);
  assert.equal(staleEntry?.ownerAgentId, worker.id);
  assert.equal(staleEntry?.request, "Check the release notes.");
  assert.equal(staleEntry?.error, "Stopped. Continue or reassign this task.");
  assert.equal(staleEntry?.ageHours, 72);
  const workerPacket = await runCauseEffect(
    new ChannelHistory(
      channels.store,
      () => Effect.die(new Error("No summary")),
      new ChannelMemoryStore(database),
    ).prepare(required(channelId, workerTurn.taskId), worker, lead, null, []),
  );
  assert.ok(!workerPacket.text.includes("Stopped tasks in this channel"));
  pass(
    "The lead's task packet lists stopped and failed tasks with owner, age, request line and error; a member's does not.",
  );

  const closeRunning = await call(leadTurn, channelId, "channel_close_task", {
    taskId: planner.taskId,
    reason: "No longer needed.",
  });
  assert.match(closeRunning.refused ?? "", /The task is running/);
  assert.equal(stored(channelId, planner.taskId)?.state, "running");
  const closeWithActiveChild = await call(leadTurn, channelId, "channel_close_task", {
    taskId: blockedRoot.id,
    reason: "No longer needed.",
  });
  assert.match(closeWithActiveChild.refused ?? "", /A task under it is still active/);
  assert.equal(stored(channelId, blockedRoot.id)?.state, "paused");
  assert.equal(stored(channelId, activeChild.id)?.state, "queued");
  pass("The lead cannot close a running task or a stopped task with active work under it.");

  assert.ok(banners(channelId).includes(staleRoot.id));
  const callId = randomUUID();
  const closed = await call(
    leadTurn,
    channelId,
    "channel_close_task",
    { taskId: staleRoot.id, reason: "The release shipped last week." },
    callId,
  );
  assert.deepEqual(closed.value, { accepted: true, state: "cancelled" });
  assert.equal(stored(channelId, staleRoot.id)?.state, "cancelled");
  assert.equal(stored(channelId, staleChild.id)?.state, "cancelled");
  assert.equal(stored(channelId, staleRoot.id)?.error, null);
  assert.ok(!banners(channelId).includes(staleRoot.id));
  const notes = () =>
    channels.store
      .messages(channelId)
      .filter((message) => message.message.text === "Closed by Lead: The release shipped last week.");
  assert.equal(notes().length, 1);
  assert.equal(notes()[0]?.taskId, staleRoot.id);
  const retried = await call(
    leadTurn,
    channelId,
    "channel_close_task",
    { taskId: staleRoot.id, reason: "The release shipped last week." },
    callId,
  );
  assert.deepEqual(retried.value, { accepted: true });
  assert.equal(notes().length, 1);
  const resumeClosed = await call(leadTurn, channelId, "channel_resume_task", { taskId: staleRoot.id });
  assert.match(resumeClosed.refused ?? "", /The task is cancelled/);
  pass("The lead closes a stopped task and its stopped child as cancelled, clears the banner and posts one note.");

  const leadResume = await call(leadTurn, channelId, "channel_resume_task", { taskId: failedRoot.id });
  assert.deepEqual(leadResume.value, { accepted: true, state: "queued" });
  assert.equal(stored(channelId, failedRoot.id)?.state, "queued");
  pass("The lead resumes a failed task.");

  const unknownTask = await call(leadTurn, channelId, "channel_close_task", {
    taskId: randomUUID(),
    reason: "No longer needed.",
  });
  assert.match(unknownTask.refused ?? "", /No task of this channel has that taskId/);
  const noReason = await call(leadTurn, channelId, "channel_close_task", { taskId: blockedRoot.id, reason: " " });
  assert.match(noReason.refused ?? "", /Provide a reason/);
  pass("An unknown task or an empty reason returns tool feedback.");

  assert.equal(errors.length, 0, String(errors[0]));
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
