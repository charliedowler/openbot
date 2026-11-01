import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary, ChannelTask } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const output = resolve(import.meta.dirname, "../.openbot-build/channel-stop-wake-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-stop-wake-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const errors: unknown[] = [];
const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };
const channels = new ChannelService(database, mailbox, {
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
const SETTLE_MS = 3_000;

function task(
  channelId: string,
  ownerAgentId: string,
  state: ChannelTask["state"],
  options: Partial<ChannelTask> = {},
) {
  const id = options.id ?? randomUUID();
  const requestMessageId = randomUUID();
  return {
    id,
    channelId,
    parentTaskId: null,
    rootTaskId: id,
    ownerAgentId,
    requestMessageId,
    instruction: "Check the release notes.",
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

async function channel(name: string, lead: AgentSummary, worker: AgentSummary) {
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
          instructions: "Check the requested work.",
          members: [{ agentId: lead.id }, { agentId: worker.id }],
          leadAgentId: lead.id,
        },
      },
      person,
    ),
  );
  return channelId;
}

const notices = (channelId: string) =>
  channels.store.tasks(channelId).filter((item) => item.requestMessageId.startsWith("channel-stop-notice-"));
const settle = () => new Promise((done) => setTimeout(done, SETTLE_MS));
const stop = (channelId: string, taskId: string) =>
  runCauseEffect(
    channels.command({ type: "stop", operationId: randomUUID(), channelId, taskId, recipientAgentId: null }, person),
  );

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  const create = (name: string) =>
    runCauseEffect(agents.createAgent({ name, description: name, avatarSeed: name.toLowerCase(), avatarHue: null }));
  const lead = await create("Lead");
  const worker = await create("Worker");

  const channelId = await channel("Work", lead, worker);
  const running = task(channelId, worker.id, "running");
  const other = task(channelId, worker.id, "queued");
  channels.store.update(channels.store.get(channelId), { tasks: [running, other] });
  await stop(channelId, running.id);
  await stop(channelId, other.id);
  await settle();
  const [notice, ...extra] = notices(channelId);
  assert.ok(notice);
  assert.equal(extra.length, 0);
  assert.equal(notice.ownerAgentId, lead.id);
  assert.equal(notice.state, "queued");
  assert.ok(notice.instruction.includes(running.id) && notice.instruction.includes(other.id));
  assert.ok(notice.instruction.includes("Stopped. Continue or reassign this task."));
  const message = channels.store.message(channelId, notice.requestMessageId);
  assert.equal(message?.author.kind, "coordinator");
  assert.equal(message?.message.text, "2 tasks stopped. Lead will resume or close them.");
  pass("Two person stops in one burst wake the lead once with one queued task that lists both stopped tasks.");

  await stop(channelId, running.id);
  await settle();
  assert.equal(notices(channelId).length, 1);
  pass("A stop of a task that is already stopped does not wake the lead again.");

  channels.store.update(channels.store.get(channelId), {
    tasks: [{ ...notice, state: "completed", revision: notice.revision + 1 }],
  });
  const failed = task(channelId, worker.id, "running");
  channels.store.update(channels.store.get(channelId), { tasks: [failed] });
  channels.store.update(channels.store.get(channelId), {
    tasks: [{ ...failed, state: "failed", revision: failed.revision + 1, error: "The provider could not start." }],
  });
  await settle();
  const afterFailure = notices(channelId).filter((item) => item.state === "queued");
  assert.equal(afterFailure.length, 1);
  assert.ok(afterFailure[0]?.instruction.includes("The provider could not start."));
  pass("A task that fails wakes the lead once.");

  const queuedNotice = afterFailure[0];
  assert.ok(queuedNotice);
  const absorbed = task(channelId, worker.id, "running");
  channels.store.update(channels.store.get(channelId), { tasks: [absorbed] });
  await stop(channelId, absorbed.id);
  await settle();
  assert.equal(notices(channelId).filter((item) => item.state === "queued").length, 1);
  pass("A stop while the lead's wake is still queued adds no second wake; the lead packet lists every stopped task.");

  channels.store.update(channels.store.get(channelId), {
    tasks: [{ ...queuedNotice, state: "failed", revision: queuedNotice.revision + 1, error: "The provider failed." }],
  });
  await settle();
  assert.equal(notices(channelId).filter((item) => item.state === "queued").length, 0);
  pass("A failed wake task does not wake the lead again.");

  const parent = task(channelId, lead.id, "queued");
  const child = task(channelId, worker.id, "running", { parentTaskId: parent.id, rootTaskId: parent.id });
  channels.store.update(channels.store.get(channelId), { tasks: [parent, child] });
  channels.store.update(channels.store.get(channelId), {
    tasks: [{ ...child, state: "paused", revision: child.revision + 1, error: "The parent request changed." }],
  });
  await settle();
  assert.equal(notices(channelId).filter((item) => item.state === "queued").length, 0);
  pass("A child that stops under a parent the lead is about to run does not wake the lead.");

  const archivedId = await channel("Archived", lead, worker);
  const archivedTask = task(archivedId, worker.id, "running");
  channels.store.update(channels.store.get(archivedId), { tasks: [archivedTask] });
  await runCauseEffect(channels.command({ type: "archive", operationId: randomUUID(), channelId: archivedId }, person));
  await settle();
  assert.equal(channels.store.task(archivedId, archivedTask.id)?.state, "paused");
  assert.equal(notices(archivedId).length, 0);
  pass("Archiving a channel stops its tasks without a lead wake.");

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
