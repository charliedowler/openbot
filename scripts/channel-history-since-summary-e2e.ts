// Checks that a channel packet reads only the messages after the channel's summary. A channel of
// 4,101 messages and 8.1 MB held the main thread for 5 to 14 seconds under load: each packet, each
// summary check and each whole capture parsed the whole history. The packet must stay the same:
// its text is compared with the sha256 of the packet that the unpatched source (9859956a) builds
// for the same synthetic channel. The read is counted as channel messages that the store returns.
//
// Runs the real ChannelStore, ChannelHistory and ChannelService on a temporary profile with a
// stand-in summary model. No app, provider, account or network is started.
//
// Run: node --import tsx scripts/channel-history-since-summary-e2e.ts
// Writes .openbot-build/channel-history-since-summary-e2e/report.json and exits non-zero on a failure.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary, ChannelMessage, ChannelTask, ConversationMessage } from "@openbot/contracts/ipc";
import { Effect, Result } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelHistory } from "../src/backend/channel-history";
import { ChannelImages } from "../src/backend/channel-images";
import { ChannelMemoryStore } from "../src/backend/channel-memory-store";
import { ChannelService } from "../src/backend/channel-service";
import { ChannelStore } from "../src/backend/channel-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const OUT = resolve(import.meta.dirname, "../.openbot-build/channel-history-since-summary-e2e");
const NOW = Date.parse("2026-11-11T12:00:00.000Z");
const MESSAGES = 600;
const SUMMARY_THROUGH = 560;
const RECENT = MESSAGES - SUMMARY_THROUGH;
/** The sha256 of the packet that 9859956a builds for the first channel below. */
const EXPECTED_PACKET_SHA256 = "e841b5950b97c575fc50608b511c1159cc6b75d3bfb591b9dcfbe335dd730313";
const EXPECTED_LONG_PACKET_SHA256 = "f0c60f2c6783145bdce6516a94bac7e11423eb151e17ec47a59f9b806daf334b";

interface Report {
  checks: string[];
  packetSha256?: string;
  longPacketSha256?: string;
  rows?: Record<string, number>;
  passed: boolean;
  error?: string;
}

const report: Report = { checks: [], passed: false };
const root = await mkdtemp(join(tmpdir(), "openbot-channel-since-summary-"));
const realNow = Date.now;
Date.now = () => NOW;

let messageRows = 0;
function counted(rows: ChannelMessage[]): ChannelMessage[] {
  messageRows += rows.length;
  return rows;
}

function countReads(store: ChannelStore) {
  const messages = store.messages.bind(store);
  store.messages = (...args: Parameters<typeof messages>) => counted(messages(...args));
  const message = store.message.bind(store);
  store.message = (...args: Parameters<typeof message>) => {
    const row = message(...args);
    if (row) messageRows += 1;
    return row;
  };
  if ("messagesById" in store) {
    const byId = store.messagesById.bind(store);
    store.messagesById = (...args: Parameters<typeof byId>) => counted(byId(...args));
  }
  if ("taskMessages" in store) {
    const ofTask = store.taskMessages.bind(store);
    store.taskMessages = (...args: Parameters<typeof ofTask>) => counted(ofTask(...args));
  }
  if ("messagesHolding" in store) {
    const holding = store.messagesHolding.bind(store);
    store.messagesHolding = (...args: Parameters<typeof holding>) => counted(holding(...args));
  }
}

function rowsOf<T>(work: () => T): { value: T; rows: number } {
  const before = messageRows;
  const value = work();
  return { value, rows: messageRows - before };
}

async function rowsOfAsync<T>(work: () => Promise<T>): Promise<{ value: T; rows: number }> {
  const before = messageRows;
  const value = await work();
  return { value, rows: messageRows - before };
}

const at = (index: number) => new Date(Date.parse("2026-11-01T00:00:00.000Z") + index * 60_000).toISOString();

function person(channelId: string, index: number, text: string, replyToMessageId?: string): ChannelMessage {
  const id = `m-${String(index).padStart(4, "0")}`;
  return {
    id,
    channelId,
    sequence: 0,
    author: { kind: "member", id: "local", name: "Example" },
    taskId: null,
    superseded: false,
    message: {
      id,
      author: "user",
      text,
      createdAt: at(index),
      status: "completed",
      ...(replyToMessageId ? { replyToMessageId } : {}),
    },
  };
}

function agentRow(
  channelId: string,
  id: string,
  index: number,
  agent: AgentSummary,
  taskId: string,
  text: string,
  extra: Partial<ChannelMessage["message"]> = {},
): ChannelMessage {
  return {
    id,
    channelId,
    sequence: 0,
    author: { kind: "agent", id: agent.id, name: agent.name },
    taskId,
    superseded: false,
    message: { id, author: "assistant", text, createdAt: at(index), status: "completed", ...extra },
  };
}

function task(channelId: string, id: string, fields: Partial<ChannelTask>): ChannelTask {
  return {
    id,
    channelId,
    parentTaskId: null,
    rootTaskId: id,
    ownerAgentId: "agent-lead",
    requestMessageId: "",
    instruction: "Continue the work.",
    attachmentDraftIds: [],
    expectedResult: "A result.",
    sourceMessageIds: [],
    dependencies: [],
    resources: [],
    state: "queued",
    revision: 0,
    assignmentCount: 0,
    error: null,
    ...fields,
  };
}

const filler = (index: number, size: number) =>
  `Message ${index}. ${"Synthetic channel history line. ".repeat(Math.ceil(size / 32))}`.slice(0, size);

function scenario(store: ChannelStore, lead: AgentSummary, member: AgentSummary, channelId: string, size: number) {
  const channel = store.create(channelId, {
    name: "Since summary",
    title: "",
    instructions: "Answer each request.",
    members: [{ agentId: lead.id }, { agentId: member.id }],
    leadAgentId: lead.id,
  });
  const instruction = `Rebuild the report from the notes. ${"Keep every figure and its source. ".repeat(800)}`;
  assert.ok(instruction.length > 24_000);
  const rows: ChannelMessage[] = [];
  for (let index = 1; index <= MESSAGES; index += 1) {
    if (index === 20) rows.push(person(channelId, index, "Start the report work."));
    else if (index === 50) rows.push(person(channelId, index, "The notes are in the shared folder."));
    else if (index === 100) rows.push(person(channelId, index, "Use the notes from last week.", "m-0050"));
    else if (index === 300)
      rows.push(agentRow(channelId, "brief-old", index, lead, `${channelId}:task-main`, `Builder: ${instruction}`));
    else if (index === 590) rows.push(person(channelId, index, "Continue with the report now.", "m-0100"));
    else rows.push(person(channelId, index, filler(index, index > SUMMARY_THROUGH ? size : 200)));
  }
  rows.splice(
    150,
    0,
    agentRow(channelId, "answer-old", 150, lead, `${channelId}:task-main`, "The first draft of the report is ready."),
    agentRow(channelId, "channel-result-old", 151, member, `${channelId}:task-side`, "The side figures.", {
      turnId: "turn-old",
    }),
  );
  rows.splice(
    595,
    0,
    agentRow(
      channelId,
      "commentary-late",
      595,
      member,
      `${channelId}:task-side`,
      "Result submitted to the shared chat.",
      {
        turnId: "turn-old",
        itemType: "commentary",
      },
    ),
    agentRow(channelId, "sibling-late", 596, member, `${channelId}:task-sibling`, "My view on the report."),
  );
  const tasks = [
    task(channelId, `${channelId}:task-main`, {
      requestMessageId: "m-0590",
      sourceMessageIds: ["m-0020", "m-0590"],
      instruction,
    }),
    task(channelId, `${channelId}:task-sibling`, {
      ownerAgentId: member.id,
      requestMessageId: "m-0020",
      sourceMessageIds: ["m-0020"],
      state: "running",
    }),
    task(channelId, `${channelId}:task-side`, {
      ownerAgentId: member.id,
      requestMessageId: "m-0050",
      sourceMessageIds: ["m-0050"],
    }),
    task(channelId, `${channelId}:task-stopped`, {
      requestMessageId: "m-0010",
      sourceMessageIds: ["m-0010"],
      state: "paused",
      error: "Stopped by the user.",
    }),
  ].map((item) => (item.id === `${channelId}:task-side` ? { ...item, state: "completed" as const } : item));
  store.update(channel, { messages: rows, tasks });
  const through = store
    .messages(channelId)
    .find((message) => message.id === `m-${String(SUMMARY_THROUGH).padStart(4, "0")}`)?.sequence;
  assert.ok(through);
  store.saveSummary(channelId, { version: 1, throughSequence: through, text: "Earlier: the team planned the report." });
  const main = store.task(channelId, `${channelId}:task-main`);
  assert.ok(main);
  return { channel, task: main, through, total: store.messages(channelId).length };
}

function packetSha(text: string, throughSequence: number, summaryVersion: number): string {
  return createHash("sha256").update(JSON.stringify({ text, throughSequence, summaryVersion })).digest("hex");
}

const database = new OpenBotDatabase(join(root, "data"));
try {
  const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
  await runCauseEffect(agents.initialize());
  const created = await runCauseEffect(
    agents.createAgent({ name: "Planner", description: "Synthetic", avatarSeed: "planner", avatarHue: null }),
  );
  const lead: AgentSummary = { ...created, id: "agent-lead", name: "Planner" };
  const member: AgentSummary = { ...created, id: "agent-member", name: "Builder" };
  const store = new ChannelStore(database);
  countReads(store);
  const memories = new ChannelMemoryStore(database);
  const prompts: string[] = [];
  const history = new ChannelHistory(
    store,
    (_lead, prompt) => {
      prompts.push(prompt);
      return Effect.succeed(`Summary pass ${prompts.length}: the team kept working on the report.`);
    },
    memories,
  );

  const first = scenario(store, lead, member, "channel-short", 200);
  assert.equal(first.total, MESSAGES + 4);
  const needs = rowsOf(() => history.needsSummary(first.channel.id));
  assert.equal(needs.value, false);
  const prepared = await rowsOfAsync(() =>
    Effect.runPromise(history.prepare(first.task, lead, lead, null, []).pipe(Effect.result)),
  );
  assert.ok(Result.isSuccess(prepared.value), "The packet failed.");
  const packet = prepared.value.success;
  assert.equal(prompts.length, 0, "A channel under the limit was summarized.");
  report.packetSha256 = packetSha(packet.text, packet.throughSequence, packet.summaryVersion);
  assert.ok(packet.text.includes("Use the notes from last week."), "The quoted older source is not in the packet.");
  assert.ok(
    packet.text.includes("The notes are in the shared folder."),
    "The source that the quote replies to is missing.",
  );
  assert.ok(
    packet.text.includes('messageId \\"brief-old\\"') || packet.text.includes('messageId "brief-old"'),
    "The long brief does not name the older message that holds it.",
  );
  assert.ok(
    !packet.text.includes("Result submitted to the shared chat."),
    "The commentary of a result turn is in the packet.",
  );
  assert.ok(packet.text.includes("My view on the report."), "An answered task hides the sibling's reply.");
  assert.ok(packet.text.includes("task-stopped"), "The lead's stopped task is missing.");
  assert.equal(packet.throughSequence, first.total);
  report.checks.push(
    "The packet of a 604-message channel gives the older quoted sources, the brief by reference, the stopped task, and leaves out the commentary of a result turn.",
  );
  assert.equal(report.packetSha256, EXPECTED_PACKET_SHA256, "The packet differs from the one that 9859956a builds.");
  report.checks.push("The packet text, cursor and summary version are the ones that 9859956a builds (sha256).");

  const second = scenario(store, lead, member, "channel-long", 1_500);
  const needsLong = rowsOf(() => history.needsSummary(second.channel.id));
  assert.equal(needsLong.value, true);
  const preparedLong = await rowsOfAsync(() =>
    Effect.runPromise(history.prepare(second.task, lead, lead, null, []).pipe(Effect.result)),
  );
  assert.ok(Result.isSuccess(preparedLong.value), "The long packet failed.");
  const longPacket = preparedLong.value.success;
  assert.ok(prompts.length >= 1, "A channel over the limit was not summarized.");
  const passes = prompts.length;
  report.longPacketSha256 = packetSha(longPacket.text, longPacket.throughSequence, longPacket.summaryVersion);
  assert.match(longPacket.text, /History window:/u);
  assert.equal(store.messages(second.channel.id).length, second.total);
  assert.ok(
    prompts.every((prompt) => !prompt.includes("Message 561.")),
    "Summary input excludes messages outside the recent window.",
  );
  assert.equal(
    report.longPacketSha256,
    EXPECTED_LONG_PACKET_SHA256,
    "The summarized packet differs from the bounded-window fixture.",
  );
  report.checks.push(
    `A channel over the limit uses the bounded summary window (${passes} pass), preserves source rows and gives the saved packet (sha256).`,
  );

  const services = new ChannelService(
    database,
    new MailboxStore(join(root, "data"), join(root, "shared"), database),
    new ChannelImages(join(root, "channel-images")),
    {
      agents: () => [lead, member],
      generate: () => Effect.succeed('{"idle":true}'),
      schedule: () => {},
      changed: () => {},
      busy: () => false,
      interrupt: () => Effect.void,
      error: (error) => {
        throw error;
      },
    },
  );
  countReads(services.store);
  const contextChannel = scenario(services.store, lead, member, "channel-capture", 200);
  const threadId = services.store.context(contextChannel.channel.id, member.id).threadId;
  assert.equal(services.store.channelForThread(threadId), contextChannel.channel.id);
  const snapshotMessages: ConversationMessage[] = [
    {
      id: "answer-old",
      author: "assistant",
      text: "The first draft of the report is ready. Updated.",
      createdAt: at(150),
      status: "completed",
      turnId: "turn-unknown",
    },
    { id: "unknown-row", author: "assistant", text: "Not a channel row.", createdAt: at(601), status: "completed" },
  ];
  const capture = rowsOf(() =>
    services.event({
      type: "conversation",
      snapshot: { agentId: member.id, threadId, activeTurnId: null, revision: 1, messages: snapshotMessages },
    }),
  );
  assert.equal(
    services.store.message(contextChannel.channel.id, "answer-old")?.message.text,
    "The first draft of the report is ready.",
    "A whole capture changed a row of an ended turn.",
  );
  assert.equal(services.store.message(contextChannel.channel.id, "unknown-row"), null);
  report.checks.push("A whole capture with an older row and an unknown row writes nothing for them, as before.");

  report.rows = {
    channelMessages: first.total,
    recentMessages: RECENT,
    needsSummary: needs.rows,
    needsSummaryLong: needsLong.rows,
    prepare: prepared.rows,
    prepareLong: preparedLong.rows,
    summaryPasses: passes,
    capture: capture.rows,
  };
  assert.ok(needs.rows <= RECENT + 10, `needsSummary read ${needs.rows} rows (limit ${RECENT + 10}).`);
  assert.ok(needsLong.rows <= RECENT + 10, `needsSummary read ${needsLong.rows} rows (limit ${RECENT + 10}).`);
  assert.ok(prepared.rows <= 2 * RECENT + 40, `A packet read ${prepared.rows} rows (limit ${2 * RECENT + 40}).`);
  assert.ok(
    preparedLong.rows <= (passes + 2) * (RECENT + 10) + 40,
    `A summarized packet read ${preparedLong.rows} rows (limit ${(passes + 2) * (RECENT + 10) + 40}).`,
  );
  assert.ok(capture.rows <= RECENT + 20, `A whole capture read ${capture.rows} rows (limit ${RECENT + 20}).`);
  report.checks.push(
    `needsSummary, the packet, the summary passes and a whole capture read only the messages after the summary and a few by id (${prepared.rows} rows for a packet of a ${first.total}-message channel).`,
  );
  await runCauseEffect(services.stop());
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
} finally {
  Date.now = realNow;
  database.close();
  await rm(root, { recursive: true, force: true });
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}

process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (!report.passed) process.exit(1);
