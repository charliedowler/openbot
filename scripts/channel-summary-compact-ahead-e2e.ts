import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { ChannelMessage } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

// A channel summary was written only while a delivery started (`ChannelHistory.prepare`), one model
// pass of the lead per half budget of uncovered messages. A channel that grew by 1,800 messages
// between assignments held its member's lane for 22 passes. The summary is now compacted after the
// channel stays quiet, so the next assignment finds at most the newest messages uncovered. These
// checks drive the real ChannelService on a temporary database with a stub lead model; no assignment
// starts, because every owner is busy.

const output = resolve(import.meta.dirname, "../.openbot-build/channel-summary-compact-ahead-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-compact-ahead-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const errors: unknown[] = [];
const report: { passed: boolean; checks: string[]; summaryPasses: number; error?: string } = {
  passed: false,
  checks: [],
  summaryPasses: 0,
};
const purposes = new Set<string | undefined>();
const summarizedBy = new Set<string>();
let limited = false;

const channels = new ChannelService(database, mailbox, {
  agents: () => agents.list(),
  generate: (lead, prompt, purpose) => {
    purposes.add(purpose);
    summarizedBy.add(lead.id);
    report.summaryPasses++;
    return Effect.succeed(`Summary pass ${report.summaryPasses}: ${prompt.length} characters read.`);
  },
  schedule: () => {},
  changed: () => {},
  busy: () => true,
  usageLimited: () => limited,
  summaryCompactionDelayMs: 50,
  interrupt: () => Effect.void,
  error: (error) => {
    errors.push(error);
  },
});

const person = { id: "local", name: "Example member" };

async function settle(label: string, ready: () => boolean, ms = 10_000) {
  const deadline = Date.now() + ms;
  for (;;) {
    if (errors.length) throw errors[0];
    if (ready()) return;
    if (Date.now() > deadline) throw new Error(`No state change: ${label}`);
    await new Promise((done) => setTimeout(done, 10));
  }
}

async function quiet(ms = 600) {
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

/** Earlier conversation: `count` completed replies of about 500 characters each, no summary yet. */
function seedHistory(channelId: string, agentId: string, count: number) {
  const taskId = randomUUID();
  const messages: ChannelMessage[] = Array.from({ length: count }, (_, index) => {
    const id = randomUUID();
    return {
      id,
      channelId,
      taskId,
      author: { kind: "agent", id: agentId, name: "Lead" },
      sequence: 0,
      superseded: false,
      message: {
        id,
        text: `Report ${index}: ${"The release check found one more detail to record. ".repeat(9)}`,
        author: "assistant",
        createdAt: new Date(Date.now() - (count - index) * 1_000).toISOString(),
        status: "completed",
      },
    };
  });
  for (let start = 0; start < messages.length; start += 50)
    channels.store.update(channels.store.get(channelId), { messages: messages.slice(start, start + 50) });
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

function uncoveredCharacters(channelId: string): number {
  const summary = channels.store.summary(channelId);
  return channels.store
    .messages(channelId)
    .filter((message) => message.sequence > summary.throughSequence)
    .reduce((total, message) => total + JSON.stringify(message).length, 0);
}

async function create(name: string) {
  return (
    await runCauseEffect(
      agents.createAgent({
        name,
        description: `Synthetic ${name}`,
        avatarSeed: name.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
        avatarHue: null,
      }),
    )
  ).id;
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());

  // 1. A channel with 80,000 uncovered characters is compacted after it stays quiet, with no assignment.
  const lead = await create("Lightdash iOS");
  const busyChannel = await channelWith("Pod Mobile", lead);
  seedHistory(busyChannel, lead, 160);
  assert.ok(uncoveredCharacters(busyChannel) > 60_000, "The seeded history is over half the budget.");
  await send(busyChannel, "Where is the pull-to-refresh work at?", lead);
  await settle("compaction of the busy channel", () => channels.store.summary(busyChannel).throughSequence > 0);
  await quiet();
  const summary = channels.store.summary(busyChannel);
  assert.ok(summary.version >= 1, "The summary has a version.");
  assert.ok(summary.text.startsWith("Summary pass"), "The summary is the lead model's text.");
  assert.ok(uncoveredCharacters(busyChannel) <= 30_000, "At most a quarter of the budget stays uncovered.");
  assert.equal(channels.store.assignments(busyChannel).length, 0, "No assignment started.");
  assert.deepEqual([...purposes], ["channel-summary"], "Only summary generations ran.");
  assert.deepEqual([...summarizedBy], [lead], "The lead's model wrote the summary.");
  report.checks.push(
    "A quiet channel with uncovered messages over a quarter of the budget is compacted without an assignment.",
  );

  // 2. A compaction covers everything but the newest messages, in as many passes as it takes.
  const passesSoFar = report.summaryPasses;
  seedHistory(busyChannel, lead, 240);
  await send(busyChannel, "And the Android side?", lead);
  await settle(
    "second compaction",
    () => uncoveredCharacters(busyChannel) <= 30_000 && report.summaryPasses > passesSoFar,
  );
  await quiet();
  assert.ok(report.summaryPasses - passesSoFar >= 2, "A larger backlog takes more than one pass.");
  report.checks.push("A backlog larger than one pass is compacted in several passes in one run.");

  // 3. A small channel is not summarized at all.
  const small = await create("Compass");
  const smallChannel = await channelWith("Dispatch", small);
  seedHistory(smallChannel, small, 20);
  const before = report.summaryPasses;
  await send(smallChannel, "Progress?", small);
  await quiet(800);
  assert.equal(report.summaryPasses, before, "A channel under a quarter of the budget runs no pass.");
  assert.equal(channels.store.summary(smallChannel).throughSequence, 0);
  report.checks.push("A channel with few uncovered messages runs no summary pass.");

  // 4. A lead at its usage limit runs no pass; the next change after the reset does.
  limited = true;
  const held = await create("Thunder");
  const heldChannel = await channelWith("Pod Web A", held);
  seedHistory(heldChannel, held, 160);
  const beforeHeld = report.summaryPasses;
  await send(heldChannel, "Status of the CLI work?", held);
  await quiet(800);
  assert.equal(report.summaryPasses, beforeHeld, "A limited lead runs no pass.");
  limited = false;
  await send(heldChannel, "And now?", held);
  await settle("compaction after the limit", () => channels.store.summary(heldChannel).throughSequence > 0);
  report.checks.push("A lead at its usage limit runs no pass; the next change after the reset compacts.");

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
