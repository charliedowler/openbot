import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary, ChannelMessage, ConversationMessage } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { type DeliveryContext, MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const output = resolve(import.meta.dirname, "../.openbot-build/agent-channel-manage-e2e/summary-continuation");
const root = await mkdtemp(join(tmpdir(), "openbot-summary-continuation-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const waiters = new Set<() => void>();
const errors: unknown[] = [];
const report: { passed: boolean; checks: string[]; summaryCalls: number; error?: string } = {
  passed: false,
  checks: [],
  summaryCalls: 0,
};
const notify = () => {
  for (const wake of waiters) wake();
};
const channels = new ChannelService(database, mailbox, {
  agents: () => agents.list(),
  generate: () =>
    Effect.sync(() => {
      report.summaryCalls++;
      return "The earlier synthetic discussion is complete. Keep the current request and delegated findings.";
    }),
  schedule: notify,
  interrupt: () => Effect.void,
  busy: () => false,
  reportsCompactions: () => true,
  changed: notify,
  error: (error) => {
    errors.push(error);
    notify();
  },
});

async function waitForState(label: string, ready: () => boolean): Promise<void> {
  if (ready()) return;
  await new Promise<void>((done, reject) => {
    const timeout = setTimeout(() => {
      waiters.delete(check);
      reject(new Error(`No state change: ${label}`));
    }, 10_000);
    const check = () => {
      if (!ready() && errors.length === 0) return;
      clearTimeout(timeout);
      waiters.delete(check);
      if (errors.length) reject(errors[0]);
      else done();
    };
    waiters.add(check);
    check();
  });
}

function historyMessage(channelId: string): ChannelMessage {
  const id = randomUUID();
  return {
    id,
    channelId,
    sequence: 0,
    author: { kind: "member", id: "local", name: "Example member" },
    taskId: null,
    superseded: false,
    message: {
      id,
      author: "user",
      text: "Synthetic completed discussion. ".repeat(800),
      createdAt: new Date().toISOString(),
      status: "completed",
    },
  };
}

const conversations = new Map<string, ConversationMessage[]>();

function capture(agent: AgentSummary, threadId: string, activeTurnId: string | null): void {
  channels.event({
    type: "conversation",
    snapshot: {
      agentId: agent.id,
      threadId,
      activeTurnId,
      revision: 1,
      messages: conversations.get(agent.id) ?? [],
    },
  });
}

async function start(channelId: string, agent: AgentSummary) {
  await waitForState(`queued delivery for ${agent.name}`, () => mailbox.nextQueued(agent.id) !== null);
  const delivery = mailbox.nextQueued(agent.id);
  assert.ok(delivery);
  await runCauseEffect(mailbox.markStarting(delivery.delivery.id));
  const packet = await runCauseEffect(channels.prepare(delivery));
  assert.ok(packet);
  capture(agent, packet.threadId, null);
  const turnId = randomUUID();
  await runCauseEffect(mailbox.markRunning(delivery.delivery.id, turnId));
  await runCauseEffect(channels.accepted(delivery.delivery.id, `session-${agent.id}`, turnId));
  assert.equal(channels.store.context(channelId, agent.id).threadId, packet.threadId);
  return { delivery, threadId: packet.threadId, turnId };
}

async function finish(
  agent: AgentSummary,
  turn: { delivery: DeliveryContext; threadId: string; turnId: string },
): Promise<void> {
  await runCauseEffect(mailbox.markTerminal(turn.delivery.delivery.id, "completed"));
  channels.event({
    type: "turn-completed",
    agentId: agent.id,
    threadId: turn.threadId,
    turnId: turn.turnId,
    status: "completed",
  });
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  const lead = await runCauseEffect(
    agents.createAgent({ name: "Planner", description: "Synthetic planner", avatarSeed: "planner", avatarHue: null }),
  );
  const reviewer = await runCauseEffect(
    agents.createAgent({
      name: "Reviewer",
      description: "Synthetic reviewer",
      avatarSeed: "reviewer",
      avatarHue: null,
    }),
  );
  const person = { id: "local", name: "Example member" };
  const channel = await runCauseEffect(
    channels.command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId: randomUUID(),
        draft: {
          name: "Summary continuation check",
          title: "",
          instructions: "Complete the synthetic review.",
          members: [{ agentId: lead.id }, { agentId: reviewer.id }],
          leadAgentId: lead.id,
        },
      },
      person,
    ),
  );
  channels.store.update(channels.store.get(channel.id), {
    messages: Array.from({ length: 3 }, () => historyMessage(channel.id)),
  });
  await runCauseEffect(
    channels.command(
      {
        type: "send",
        operationId: randomUUID(),
        channelId: channel.id,
        text: "Prepare a plan and ask for a review.",
        recipientAgentId: lead.id,
        replyToMessageId: null,
        attachmentDraftIds: [],
      },
      person,
    ),
  );
  const parent = channels.store.tasks(channel.id).find((task) => task.parentTaskId === null);
  assert.ok(parent);
  const firstTurn = await start(channel.id, lead);
  const firstMessages: ConversationMessage[] = [
    {
      id: `rs_${randomUUID()}`,
      turnId: firstTurn.turnId,
      author: "assistant",
      itemType: "commentary",
      text: "",
      createdAt: new Date().toISOString(),
      status: "completed",
    },
    {
      id: randomUUID(),
      turnId: firstTurn.turnId,
      author: "assistant",
      itemType: "agentMessage",
      text: "The plan is ready for review.",
      createdAt: new Date().toISOString(),
      status: "completed",
    },
  ];
  conversations.set(lead.id, firstMessages);
  capture(lead, firstTurn.threadId, firstTurn.turnId);
  await runCauseEffect(
    channels.tool(channel.id, lead.id, firstTurn.turnId, randomUUID(), "channel_assign", {
      recipientAgentId: reviewer.id,
      task: "Review the plan.",
      expectedResult: "A short review.",
      sourceMessageIds: [parent.requestMessageId],
      resources: [],
    }),
  );
  await finish(lead, firstTurn);
  await waitForState(
    "parent waits for its review",
    () => channels.store.tasks(channel.id).find((task) => task.id === parent.id)?.state === "waiting",
  );
  channels.store.update(channels.store.get(channel.id), {
    messages: Array.from({ length: 3 }, () => historyMessage(channel.id)),
  });
  const childTurn = await start(channel.id, reviewer);
  const summary = channels.store.summary(channel.id);
  assert.ok(summary.version > 0);
  assert.ok(
    firstMessages.every(
      (message) => (channels.store.message(channel.id, message.id)?.sequence ?? Infinity) <= summary.throughSequence,
    ),
  );
  await runCauseEffect(
    channels.tool(channel.id, reviewer.id, childTurn.turnId, randomUUID(), "channel_result", {
      text: "The review is complete.",
    }),
  );
  await finish(reviewer, childTurn);
  await waitForState(
    "parent resumes after the review",
    () => (channels.store.tasks(channel.id).find((task) => task.id === parent.id)?.revision ?? 0) > parent.revision,
  );
  const resumedTask = channels.store.tasks(channel.id).find((task) => task.id === parent.id);
  assert.ok(resumedTask);
  assert.equal(resumedTask.requestMessageId, parent.requestMessageId);
  assert.equal(resumedTask.instruction, parent.instruction);
  const summaryCallsBeforeResume = report.summaryCalls;
  const resumedTurn = await start(channel.id, lead);
  const resumedSummary = channels.store.summary(channel.id);
  assert.deepEqual(resumedSummary, summary, "A normal parent continuation resets the completed channel summary.");
  assert.ok(
    firstMessages.every((message) => channels.store.message(channel.id, message.id)?.superseded === false),
    "A normal continuation marks valid earlier messages superseded.",
  );
  assert.equal(report.summaryCalls, summaryCallsBeforeResume, "A continuation repeats history summarisation.");
  assert.ok(
    channels.store.context(channel.id, lead.id).throughSequence > 0,
    "The continuation loses its accepted context cursor.",
  );
  capture(lead, resumedTurn.threadId, resumedTurn.turnId);
  assert.deepEqual(channels.store.summary(channel.id), summary, "Repeated capture changes the saved summary.");
  report.checks.push(
    "Delegation and parent continuation preserve the summary, earlier messages and accepted cursor without summary calls.",
  );
  await finish(lead, resumedTurn);
  await waitForState(
    "parent completes",
    () => channels.store.tasks(channel.id).find((task) => task.id === parent.id)?.state === "completed",
  );
  await runCauseEffect(
    channels.command(
      {
        type: "send",
        operationId: randomUUID(),
        channelId: channel.id,
        text: "Correction: replace the plan with a different approach.",
        recipientAgentId: lead.id,
        replyToMessageId: parent.requestMessageId,
        attachmentDraftIds: [],
      },
      person,
    ),
  );
  const corrected = channels.store.tasks(channel.id).find((task) => task.id === parent.id);
  assert.ok(corrected);
  assert.notEqual(corrected.requestMessageId, parent.requestMessageId);
  assert.equal(channels.store.summary(channel.id).throughSequence, 0);
  assert.ok(firstMessages.every((message) => channels.store.message(channel.id, message.id)?.superseded === true));
  await assert.rejects(
    runCauseEffect(
      channels.tool(channel.id, lead.id, resumedTurn.turnId, randomUUID(), "channel_result", {
        text: "An obsolete result.",
      }),
    ),
    /no longer active|task has changed/,
  );
  const lateMessage: ConversationMessage = {
    id: randomUUID(),
    turnId: resumedTurn.turnId,
    author: "assistant",
    text: "A delayed answer to the previous request.",
    createdAt: new Date().toISOString(),
    status: "completed",
  };
  conversations.set(lead.id, [...firstMessages, lateMessage]);
  capture(lead, resumedTurn.threadId, null);
  assert.ok(firstMessages.every((message) => channels.store.message(channel.id, message.id)?.superseded === true));
  assert.equal(channels.store.message(channel.id, lateMessage.id)?.superseded, true);
  assert.equal(channels.store.context(channel.id, lead.id).throughSequence, 0);
  report.checks.push(
    "A real request correction invalidates the summary and cursor, supersedes delayed old messages and rejects a stale assignment result.",
  );
  assert.deepEqual(errors, []);
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  await runCauseEffect(channels.stop());
  database.close();
  await rm(root, { recursive: true, force: true });
  mkdirSync(output, { recursive: true });
  writeFileSync(join(output, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
