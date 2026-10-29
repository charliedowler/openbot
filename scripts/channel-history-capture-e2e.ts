import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary, ConversationMessage } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { importProviderHistory } from "../src/backend/provider-history-import";

const output = resolve(import.meta.dirname, "../.openbot-build/agent-channel-manage-e2e/history-capture");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-history-capture-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const errors: unknown[] = [];
const listeners = new Set<() => void>();
const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };
const changed = () => {
  for (const listener of listeners) listener();
};
const channels = new ChannelService(database, mailbox, {
  agents: () => agents.list(),
  generate: () => Effect.succeed('{"idle":true}'),
  schedule: changed,
  changed,
  busy: () => false,
  interrupt: () => Effect.void,
  error: (error) => {
    errors.push(error);
    changed();
  },
});

async function waitForState(label: string, ready: () => boolean) {
  if (ready()) return;
  await new Promise<void>((done, fail) => {
    const timer = setTimeout(() => {
      listeners.delete(check);
      fail(new Error(`No state change: ${label}`));
    }, 10_000);
    const check = () => {
      if (!ready() && errors.length === 0) return;
      clearTimeout(timer);
      listeners.delete(check);
      if (errors.length) fail(errors[0]);
      else done();
    };
    listeners.add(check);
    check();
  });
}

function capture(agent: AgentSummary, threadId: string, activeTurnId: string | null, messages: ConversationMessage[]) {
  channels.event({
    type: "conversation",
    snapshot: { agentId: agent.id, threadId, activeTurnId, revision: 1, messages },
  });
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  const agent = await runCauseEffect(
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
          name: "History capture check",
          title: "",
          instructions: "Answer each separate request.",
          members: [{ agentId: agent.id }],
          leadAgentId: agent.id,
        },
      },
      person,
    ),
  );
  const send = async () => {
    await runCauseEffect(
      channels.command(
        {
          type: "send",
          operationId: randomUUID(),
          channelId: channel.id,
          text: "Check the synthetic plan.",
          recipientAgentId: agent.id,
          replyToMessageId: null,
          attachmentDraftIds: [],
        },
        person,
      ),
    );
    await waitForState("queued delivery", () => mailbox.nextQueued(agent.id) !== null);
    const delivery = mailbox.nextQueued(agent.id);
    assert.ok(delivery);
    await runCauseEffect(mailbox.markStarting(delivery.delivery.id));
    const packet = await runCauseEffect(channels.prepare(delivery));
    assert.ok(packet);
    return { delivery, threadId: packet.threadId, turnId: randomUUID() };
  };
  const finish = async (turn: Awaited<ReturnType<typeof send>>) => {
    await runCauseEffect(mailbox.markTerminal(turn.delivery.delivery.id, "completed"));
    channels.event({
      type: "turn-completed",
      agentId: agent.id,
      threadId: turn.threadId,
      turnId: turn.turnId,
      status: "completed",
    });
    await waitForState("assignment completed", () =>
      channels.store
        .assignments(channel.id)
        .some((assignment) => assignment.turnId === turn.turnId && assignment.state === "completed"),
    );
  };
  const text = "The synthetic plan is ready.";
  const first = await send();
  await runCauseEffect(mailbox.markRunning(first.delivery.delivery.id, first.turnId));
  await runCauseEffect(channels.accepted(first.delivery.delivery.id, "synthetic-session", first.turnId));
  const live: ConversationMessage = {
    id: `${first.turnId}:assistant`,
    turnId: first.turnId,
    author: "assistant",
    itemType: "agentMessage",
    text,
    createdAt: new Date().toISOString(),
    status: "completed",
  };
  database.persistConversationChanges({
    agentId: agent.id,
    threadId: first.threadId,
    activeTurnId: first.turnId,
    changedMessages: [live],
    eventType: "turn.message",
    detail: {},
  });
  capture(agent, first.threadId, first.turnId, [live]);
  await finish(first);
  const second = await send();
  assert.equal(second.threadId, first.threadId);
  const importedTurnId = randomUUID();
  const importedMessageId = randomUUID();
  const session = database.bindProviderSession({
    threadId: second.threadId,
    provider: "claude",
    externalSessionId: "synthetic-session",
    model: "claude-fable-5-1",
    effort: "high",
  });
  await runCauseEffect(
    importProviderHistory({
      database,
      sessionId: session.id,
      provider: "claude",
      externalSessionId: session.externalSessionId,
      agentId: agent.id,
      publicThreadId: second.threadId,
      findDelivery: () => null,
      findMessageDelivery: () => null,
      quietRoutineDelivery: () => false,
      readHistory: (_request, consume) =>
        consume({
          turnId: importedTurnId,
          status: "completed",
          itemOffset: 0,
          items: [{ id: importedMessageId, type: "agentMessage", text }],
          complete: true,
        }).pipe(Effect.asVoid),
    }),
  );
  const imported = database.readConversation(agent.id, second.threadId);
  assert.ok(imported.messages.some((message) => message.id === importedMessageId));
  capture(agent, second.threadId, null, imported.messages);
  const replies = () => channels.store.messages(channel.id).filter((entry) => entry.message.text === text);
  assert.equal(replies().length, 1, "A restored history reply is copied into a different pending task.");
  report.checks.push(
    "A bounded real history import keeps its source row without copying an unknown turn into the active channel task.",
  );
  const secondLive: ConversationMessage = {
    ...live,
    id: `${second.turnId}:assistant`,
    turnId: second.turnId,
    createdAt: new Date().toISOString(),
    status: "streaming",
  };
  capture(agent, second.threadId, second.turnId, [...imported.messages, secondLive]);
  assert.equal(
    replies().length,
    2,
    "A distinct live turn with identical text must remain visible before its acceptance is saved.",
  );
  await runCauseEffect(mailbox.markRunning(second.delivery.delivery.id, second.turnId));
  await runCauseEffect(channels.accepted(second.delivery.delivery.id, session.externalSessionId, second.turnId));
  secondLive.status = "completed";
  capture(agent, second.threadId, second.turnId, [...imported.messages, secondLive]);
  capture(agent, second.threadId, second.turnId, [...imported.messages, secondLive]);
  assert.equal(replies().length, 2, "Repeated snapshots must update the same live reply.");
  assert.equal(new Set(replies().map((entry) => entry.taskId)).size, 2);
  report.checks.push(
    "Identical replies for separate requests remain, and repeated snapshots update their original identities.",
  );
  await finish(second);
  capture(agent, second.threadId, null, [...imported.messages, { ...live, text: `${text} Verified.` }, secondLive]);
  assert.equal(channels.store.message(channel.id, live.id)?.message.text, `${text} Verified.`);
  assert.equal(channels.store.message(channel.id, importedMessageId), null);
  report.checks.push("A delayed update for a known completed assignment still updates its original message.");
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
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
