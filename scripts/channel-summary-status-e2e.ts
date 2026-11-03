import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary, ChannelMessage, ConversationMessage } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelImages } from "../src/backend/channel-images";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const output = resolve(import.meta.dirname, "../.openbot-build/channel-summary-status-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-summary-status-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const waiters = new Set<() => void>();
const errors: unknown[] = [];
const report: { passed: boolean; checks: string[]; failures: string[]; summaryCalls: number; error?: string } = {
  passed: false,
  checks: [],
  failures: [],
  summaryCalls: 0,
};
const notify = () => {
  for (const wake of waiters) wake();
};
const channels = new ChannelService(database, mailbox, new ChannelImages(join(root, "channel-images")), {
  agents: () => agents.list(),
  generate: () =>
    Effect.sync(() => {
      report.summaryCalls++;
      return "The earlier synthetic discussion is complete. Keep the current request.";
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

function check(passed: boolean, label: string): void {
  (passed ? report.checks : report.failures).push(label);
}

async function waitForState(label: string, ready: () => boolean): Promise<void> {
  if (ready()) return;
  await new Promise<void>((done, reject) => {
    const timeout = setTimeout(() => {
      waiters.delete(wake);
      reject(new Error(`No state change: ${label}`));
    }, 10_000);
    const wake = () => {
      if (!ready() && errors.length === 0) return;
      clearTimeout(timeout);
      waiters.delete(wake);
      if (errors.length) reject(errors[0]);
      else done();
    };
    waiters.add(wake);
    wake();
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

function capture(agent: AgentSummary, threadId: string, messages: ConversationMessage[]): void {
  channels.event({
    type: "conversation",
    snapshot: { agentId: agent.id, threadId, activeTurnId: null, revision: 1, messages },
  });
}

const person = { id: "local", name: "Example member" };

async function send(channelId: string, agent: AgentSummary, text: string): Promise<void> {
  await runCauseEffect(
    channels.command(
      {
        type: "send",
        operationId: randomUUID(),
        channelId,
        text,
        recipientAgentId: agent.id,
        replyToMessageId: null,
        attachmentDraftIds: [],
      },
      person,
    ),
  );
  await waitForState(`queued delivery for ${agent.name}`, () => mailbox.nextQueued(agent.id) !== null);
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  const lead = await runCauseEffect(
    agents.createAgent({ name: "Builder", description: "Synthetic builder", avatarSeed: "builder", avatarHue: null }),
  );
  const channel = await runCauseEffect(
    channels.command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId: randomUUID(),
        draft: {
          name: "Summary status check",
          title: "",
          instructions: "Complete the synthetic work.",
          members: [{ agentId: lead.id }],
          leadAgentId: lead.id,
        },
      },
      person,
    ),
  );

  await send(channel.id, lead, "Build the first part.");
  const first = mailbox.nextQueued(lead.id);
  assert.ok(first);
  await runCauseEffect(mailbox.markStarting(first.delivery.id));
  const firstPacket = await runCauseEffect(channels.prepare(first));
  assert.ok(firstPacket);
  const firstTurnId = randomUUID();
  await runCauseEffect(mailbox.markRunning(first.delivery.id, firstTurnId));
  await runCauseEffect(channels.accepted(first.delivery.id, `session-${lead.id}`, firstTurnId));
  const reply: ConversationMessage = {
    id: randomUUID(),
    turnId: firstTurnId,
    author: "assistant",
    itemType: "agentMessage",
    text: "The first part is built.",
    createdAt: new Date().toISOString(),
    status: "completed",
  };
  capture(lead, firstPacket.threadId, [reply]);
  await runCauseEffect(mailbox.markTerminal(first.delivery.id, "completed"));
  channels.event({
    type: "turn-completed",
    agentId: lead.id,
    threadId: firstPacket.threadId,
    turnId: firstTurnId,
    status: "completed",
  });
  await waitForState("the first task completes", () =>
    channels.store.tasks(channel.id).every((task) => task.state === "completed"),
  );
  const replyRow = channels.store.message(channel.id, reply.id);
  assert.ok(replyRow, "The agent reply is in the channel.");
  assert.equal(replyRow.message.status, "completed");

  channels.store.update(channels.store.get(channel.id), {
    messages: Array.from({ length: 4 }, () => historyMessage(channel.id)),
  });
  await send(channel.id, lead, "Build the second part.");
  const second = mailbox.nextQueued(lead.id);
  assert.ok(second);
  await runCauseEffect(mailbox.markStarting(second.delivery.id));
  assert.ok(await runCauseEffect(channels.prepare(second)));
  const summary = channels.store.summary(channel.id);
  assert.ok(report.summaryCalls > 0, "The long history is summarised.");
  assert.ok(summary.throughSequence >= replyRow.sequence, "The summary covers the agent reply.");

  capture(lead, firstPacket.threadId, [{ ...reply, status: "failed" }]);
  check(
    channels.store.message(channel.id, reply.id)?.message.status === "failed",
    "The recovery capture records the new status of the earlier reply.",
  );
  check(
    JSON.stringify(channels.store.summary(channel.id)) === JSON.stringify(summary),
    "A status change of a summarised message keeps the channel summary.",
  );
  const callsBefore = report.summaryCalls;
  assert.ok(await runCauseEffect(channels.prepare(second)));
  check(
    report.summaryCalls === callsBefore,
    `The next packet after a status change makes no summary calls (made ${report.summaryCalls - callsBefore}).`,
  );

  capture(lead, firstPacket.threadId, [{ ...reply, status: "failed", text: "The first part is built again." }]);
  const corrected = channels.store.summary(channel.id);
  check(
    corrected.throughSequence === 0 && corrected.text === "" && corrected.version > summary.version,
    "A text change of a summarised message still resets the channel summary.",
  );
  assert.deepEqual(errors, []);
  report.passed = report.failures.length === 0;
  if (!report.passed) process.exitCode = 1;
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
