import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { ChannelTask } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const COMPLETED_TASKS = 700;
const MAX_FULL_READS_PER_WAKE = 4;

const output = resolve(import.meta.dirname, "../.openbot-build/channel-pump-scan-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-pump-scan-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const errors: unknown[] = [];
const report: {
  passed: boolean;
  checks: string[];
  completedTasks: number;
  fullReadsPerWake?: number;
  idleFullReadsPerWake?: number;
  error?: string;
} = { passed: false, checks: [], completedTasks: COMPLETED_TASKS };
let busy = true;
const channels = new ChannelService(database, mailbox, {
  agents: () => agents.list(),
  generate: () => Effect.die(new Error("No routing model is used by this check.")),
  schedule: () => {},
  changed: () => {},
  busy: () => busy,
  interrupt: () => Effect.void,
  error: (error) => {
    errors.push(error);
  },
});

let fullReads = 0;
const readTasks = channels.store.tasks.bind(channels.store);
channels.store.tasks = (channelId: string) => {
  fullReads++;
  return readTasks(channelId);
};

async function settle(label: string, ready: () => boolean) {
  const deadline = Date.now() + 30_000;
  let stableSince = Date.now();
  let lastReads = fullReads;
  for (;;) {
    if (errors.length) throw errors[0];
    if (fullReads !== lastReads) {
      lastReads = fullReads;
      stableSince = Date.now();
    }
    if (ready() && Date.now() - stableSince >= 250) return;
    if (Date.now() > deadline) throw new Error(`No state change: ${label}`);
    await new Promise((done) => setTimeout(done, 10));
  }
}

function seeded(channelId: string, ownerAgentId: string, state: ChannelTask["state"], index: number): ChannelTask {
  const id = randomUUID();
  return {
    id,
    channelId,
    parentTaskId: null,
    rootTaskId: id,
    ownerAgentId,
    requestMessageId: randomUUID(),
    instruction: `Earlier request ${index}: ${"Check the release notes and report each change. ".repeat(12)}`,
    attachmentDraftIds: [],
    expectedResult: "Complete the requested work and report the result.",
    sourceMessageIds: [],
    dependencies: [],
    resources: [],
    state,
    revision: 1,
    assignmentCount: 1,
    error: null,
  };
}

function nonChannelTurnEnded(agentId: string, threadId: string) {
  channels.event({ type: "turn-completed", agentId, threadId, turnId: randomUUID(), status: "completed" });
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  const worker = await runCauseEffect(
    agents.createAgent({ name: "Worker", description: "Synthetic worker", avatarSeed: "worker", avatarHue: null }),
  );
  const ownChatThreadId = randomUUID();
  const person = { id: "local", name: "Example member" };
  const channelId = randomUUID();
  await runCauseEffect(
    channels.command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId,
        draft: {
          name: "Large history",
          title: "",
          instructions: "Check the requested work.",
          members: [{ agentId: worker.id }],
          leadAgentId: worker.id,
        },
      },
      person,
    ),
  );

  const history = Array.from({ length: COMPLETED_TASKS }, (_, index) =>
    seeded(channelId, worker.id, "completed", index),
  );
  const pausedRoot = seeded(channelId, worker.id, "paused", COMPLETED_TASKS);
  const heldChild: ChannelTask = {
    ...seeded(channelId, worker.id, "queued", COMPLETED_TASKS + 1),
    parentTaskId: pausedRoot.id,
    rootTaskId: pausedRoot.id,
  };
  channels.store.update(channels.store.get(channelId), { tasks: [...history, pausedRoot, heldChild] });

  await runCauseEffect(
    channels.command(
      {
        type: "send",
        operationId: randomUUID(),
        channelId,
        text: "Check the new release.",
        recipientAgentId: worker.id,
        replyToMessageId: null,
        attachmentDraftIds: [],
      },
      person,
    ),
  );
  await settle("queued request while the owner is busy", () =>
    readTasks(channelId).some((task) => task.state === "queued" && task.id !== heldChild.id),
  );
  const queued = readTasks(channelId).find((task) => task.state === "queued" && task.id !== heldChild.id);
  assert.ok(queued);
  assert.equal(queued.ownerAgentId, worker.id);
  assert.equal(channels.store.assignments(channelId).length, 0);
  report.checks.push(
    `A channel with ${COMPLETED_TASKS} completed tasks keeps a new request queued while its owner is busy.`,
  );

  busy = false;
  fullReads = 0;
  nonChannelTurnEnded(worker.id, ownChatThreadId);
  await settle("queued request starts after a non-channel turn ends", () =>
    channels.store
      .assignments(channelId)
      .some((assignment) => assignment.taskId === queued.id && assignment.deliveryId),
  );
  const assignments = channels.store.assignments(channelId);
  const assignment = assignments.find((item) => item.taskId === queued.id);
  assert.ok(assignment?.deliveryId);
  assert.equal(assignment.agentId, worker.id);
  assert.equal(mailbox.getDelivery(assignment.deliveryId)?.channelId, channelId);
  report.checks.push("The queued request starts with a channel delivery after a non-channel turn ends.");

  assert.equal(readTasks(channelId).find((task) => task.id === heldChild.id)?.state, "queued");
  assert.equal(assignments.filter((item) => item.taskId === heldChild.id).length, 0);
  report.checks.push("A queued task whose root is paused stays queued and gets no assignment.");

  report.fullReadsPerWake = fullReads;
  assert.ok(
    fullReads <= MAX_FULL_READS_PER_WAKE,
    `One wake read the channel task list ${fullReads} times; the limit is ${MAX_FULL_READS_PER_WAKE}.`,
  );
  report.checks.push(`One wake reads the channel task list ${fullReads} times (limit ${MAX_FULL_READS_PER_WAKE}).`);

  fullReads = 0;
  nonChannelTurnEnded(worker.id, ownChatThreadId);
  await settle("idle wake", () => fullReads > 0);
  report.idleFullReadsPerWake = fullReads;
  assert.ok(
    fullReads <= MAX_FULL_READS_PER_WAKE,
    `A wake with no startable task read the channel task list ${fullReads} times.`,
  );
  report.checks.push(`A wake with no startable task reads the channel task list ${fullReads} times.`);
  assert.equal(errors.length, 0);
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
