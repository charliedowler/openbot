// Checks the slow-call warnings of the main-thread work that grows with the history: a channel
// packet and its summary check, the channel list, a queue change of the mailbox sync, a mailbox
// chat read, and the emit and publish of a chat. Each call over 250 ms logs one warning line with
// the function, the time and the counts that drive the cost, at most one line for each function in
// 10 s. A line carries no message text.
//
// The clock of the timers (performance.now) moves 300 ms on each read, so each call takes over
// 250 ms without a large profile. Runs the real ChannelStore, ChannelHistory, MailboxStore,
// MailboxSync and ConversationRuntime on a temporary profile. No app, provider, account or network
// is started.
//
// Run: node --import tsx scripts/slow-call-timers-e2e.ts
// Writes .openbot-build/slow-call-timers-e2e/report.json and exits non-zero on a failure.
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { ChannelMessage, ChannelTask, ConversationSnapshot } from "@openbot/contracts/ipc";
import { type DynamicRecord, isDynamicRecord } from "@openbot/contracts/runtime-values";
import { teeLogLines } from "@openbot/logging";
import { Effect, Result, Scope } from "effect";
import { ConversationRuntime } from "../src/backend/agent/conversation-runtime";
import { MailboxSync } from "../src/backend/agent/mailbox-sync";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelHistory } from "../src/backend/channel-history";
import { ChannelMemoryStore } from "../src/backend/channel-memory-store";
import { ChannelStore } from "../src/backend/channel-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const OUT = resolve(import.meta.dirname, "../.openbot-build/slow-call-timers-e2e");
const SECRET = "private-text-7f3c";
const FUNCTIONS = [
  "ChannelHistory.needsSummary",
  "ChannelHistory.prepare",
  "ChannelStore.list",
  "MailboxSync.emitQueue",
  "MailboxStore.conversationMessages",
  "ConversationRuntime.emitConversation",
  "ConversationRuntime.publishConversation",
] as const;
const COUNTS: Record<(typeof FUNCTIONS)[number], string[]> = {
  "ChannelHistory.needsSummary": ["messagesRead", "characters"],
  "ChannelHistory.prepare": ["recentMessages", "messagesReadById", "packetCharacters", "summaryPasses"],
  "ChannelStore.list": ["channels", "unreadMessages", "bytes"],
  "MailboxSync.emitQueue": ["agents", "synced", "snapshotMessages"],
  "MailboxStore.conversationMessages": ["mailboxMessages", "deliveries", "messagesRead", "returned"],
  "ConversationRuntime.emitConversation": ["messages", "bytes"],
  "ConversationRuntime.publishConversation": ["messages", "bytes"],
};

interface Report {
  checks: string[];
  lines: string[];
  passed: boolean;
  error?: string;
}

const report: Report = { checks: [], lines: [], passed: false };
const lines: string[] = [];
teeLogLines(["slow-call"], (line) => lines.push(line));
const root = await mkdtemp(join(tmpdir(), "openbot-slow-calls-"));
const realPerformanceNow = performance.now.bind(performance);
const realDateNow = Date.now;
let slow = false;
let reads = 0;
let dateShift = 0;
performance.now = () => realPerformanceNow() + (slow ? 300 * ++reads : 0);
Date.now = () => realDateNow() + dateShift;

function fields(line: string): DynamicRecord {
  const start = line.indexOf("{");
  assert.ok(start > 0, `No fields in ${line}`);
  const value = JSON.parse(line.slice(start));
  assert.ok(isDynamicRecord(value));
  return value;
}

function linesFor(name: string): DynamicRecord[] {
  return lines.filter((line) => line.includes(`"function":"${name}"`)).map(fields);
}

const database = new OpenBotDatabase(join(root, "data"));
const scope = Scope.makeUnsafe();
try {
  const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
  await runCauseEffect(agents.initialize());
  const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
  await runCauseEffect(mailbox.initialize());
  const agent = await runCauseEffect(
    agents.createAgent({ name: "Writer", description: "Synthetic", avatarSeed: "writer", avatarHue: null }),
  );
  const teammate = await runCauseEffect(
    agents.createAgent({ name: "Reviewer", description: "Synthetic", avatarSeed: "reviewer", avatarHue: null }),
  );
  const store = new ChannelStore(database);
  const channel = store.create("channel-slow", {
    name: "Slow calls",
    title: "",
    instructions: "",
    members: [{ agentId: agent.id }],
    leadAgentId: agent.id,
  });
  const messages: ChannelMessage[] = Array.from({ length: 30 }, (_, index) => ({
    id: `m-${index}`,
    channelId: channel.id,
    sequence: 0,
    author: { kind: "member", id: "local", name: "Example" },
    taskId: null,
    superseded: false,
    message: {
      id: `m-${index}`,
      author: "user",
      text: `Message ${index} with ${SECRET}.`,
      createdAt: new Date(Date.parse("2026-11-01T00:00:00.000Z") + index * 60_000).toISOString(),
      status: "completed",
    },
  }));
  const task: ChannelTask = {
    id: "task-slow",
    channelId: channel.id,
    parentTaskId: null,
    rootTaskId: "task-slow",
    ownerAgentId: agent.id,
    requestMessageId: "m-29",
    instruction: `Answer ${SECRET}.`,
    attachmentDraftIds: [],
    expectedResult: "A result.",
    sourceMessageIds: ["m-29"],
    dependencies: [],
    resources: [],
    state: "queued",
    revision: 0,
    assignmentCount: 0,
    error: null,
  };
  store.update(channel, { messages, tasks: [task] });
  const history = new ChannelHistory(store, () => Effect.succeed("Summary."), new ChannelMemoryStore(database));
  const conversation = new ConversationRuntime(
    agents,
    () => {},
    () => agents.list(),
  );
  const sync = new MailboxSync({
    database,
    mailbox,
    conversation,
    routines: { reconcileDelivery: () => false, stateChanged: () => {} },
    scope: () => scope,
    hooks: { emit: () => {}, emitError: () => {}, queueHold: () => null },
  });
  const sent = await runCauseEffect(
    mailbox.enqueue({
      sender: { kind: "agent", agentId: teammate.id },
      recipientAgentIds: [agent.id],
      text: `A request with ${SECRET}.`,
      idempotencyKey: `agent:${teammate.id}:turn-1:1`,
    }),
  );
  assert.ok(sent.deliveries[0]);
  const threadId = agents.ensureThreadIdNow(agent.id);
  const snapshot: ConversationSnapshot = {
    agentId: agent.id,
    threadId,
    activeTurnId: null,
    revision: 0,
    messages: [
      {
        id: "answer-1",
        turnId: "turn-1",
        author: "assistant",
        text: `An answer with ${SECRET}.`,
        createdAt: "2026-11-01T00:00:00.000Z",
        status: "completed",
      },
    ],
  };
  conversation.emitConversation(snapshot, "e2e.seeded", {});
  assert.equal(lines.length, 0, "A fast call logged a slow-call warning.");
  report.checks.push("Fast calls log no slow-call warning.");

  slow = true;
  const runAll = async () => {
    history.needsSummary(channel.id);
    const prepared = await Effect.runPromise(history.prepare(task, agent, agent, null, []).pipe(Effect.result));
    assert.ok(Result.isSuccess(prepared));
    store.list("local");
    mailbox.conversationMessages(agent.id);
    sync.emitQueue(agent.id);
    const answer = snapshot.messages[0];
    assert.ok(answer);
    answer.text = `${answer.text} Edited.`;
    conversation.emitConversation(snapshot, "e2e.changed", {});
    conversation.publishConversation(snapshot);
  };
  await runAll();
  await runAll();
  slow = false;
  for (const name of FUNCTIONS) {
    const logged = linesFor(name);
    assert.equal(logged.length, 1, `${name} logged ${logged.length} lines in 10 s (one allowed).`);
    const [line] = logged;
    assert.ok(line);
    assert.ok(typeof line.durationMs === "number" && line.durationMs > 250, `${name} logged no duration.`);
    for (const key of COUNTS[name]) assert.ok(key in line, `${name} logged no ${key}.`);
  }
  report.checks.push(
    "Each of the 7 functions logs one warning with its time and its counts for two slow calls in 10 s.",
  );
  assert.ok(!lines.some((line) => line.includes(SECRET)), "A slow-call line carries message text.");
  report.checks.push("No slow-call line carries message text.");

  dateShift = 11_000;
  slow = true;
  store.list("local");
  slow = false;
  const later = linesFor("ChannelStore.list");
  assert.equal(later.length, 2, "A slow call 11 s later logs no new line.");
  assert.equal(later[1]?.slowCallsNotLogged, 1, "The new line does not count the slow call that was not logged.");
  report.checks.push("11 s later a slow call logs again and counts the slow call that was not logged.");
  report.lines = lines;
  report.passed = true;
} catch (error) {
  report.lines = lines;
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
} finally {
  performance.now = realPerformanceNow;
  Date.now = realDateNow;
  database.close();
  await rm(root, { recursive: true, force: true });
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}

process.stdout.write(`${JSON.stringify({ ...report, lines: report.lines.length }, null, 2)}\n`);
if (!report.passed) process.exit(1);
