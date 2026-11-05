import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { ChannelTask } from "@openbot/contracts/ipc";
import { sourceText } from "@openbot/i18n/source";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelOperationError } from "../src/backend/channel-effects";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

type Routing = "timeout" | "provider" | "unreadable" | "question" | "unused";

const output = resolve(import.meta.dirname, "../.openbot-build/channel-routing-lead-fallback-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-routing-fallback-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const errors: unknown[] = [];
const removed = new Set<string>();
const report: { passed: boolean; checks: string[]; routingTurns: number; error?: string } = {
  passed: false,
  checks: [],
  routingTurns: 0,
};
let routing: Routing = "unused";

function routingAnswer(): Effect.Effect<string, ChannelOperationError> {
  report.routingTurns++;
  if (routing === "timeout")
    return Effect.fail(new ChannelOperationError({ cause: new Error(sourceText("error.agent.profileTimedOut")) }));
  if (routing === "provider")
    return Effect.fail(new ChannelOperationError({ cause: new Error("Provider said: upstream connection reset") }));
  if (routing === "unreadable") return Effect.succeed("I think the lead should take this one.");
  if (routing === "question") return Effect.succeed(JSON.stringify({ question: "Which release do you mean?" }));
  return Effect.die(new Error("This request names its member, so no routing turn runs."));
}

const channels = new ChannelService(database, mailbox, {
  agents: () => agents.list().filter((agent) => !removed.has(agent.id)),
  generate: () => routingAnswer(),
  schedule: () => {},
  changed: () => {},
  busy: () => false,
  interrupt: () => Effect.void,
  error: (error) => {
    errors.push(error);
  },
});

const person = { id: "local", name: "Example member" };

async function settle(label: string, ready: () => boolean) {
  const deadline = Date.now() + 30_000;
  for (;;) {
    if (ready()) return;
    if (Date.now() > deadline) throw new Error(`No state change: ${label}`);
    await new Promise((done) => setTimeout(done, 10));
  }
}

async function channelWith(name: string, leadAgentId: string, memberIds: string[]) {
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
          members: memberIds.map((agentId) => ({ agentId })),
          leadAgentId,
        },
      },
      person,
    ),
  );
  return channelId;
}

async function send(channelId: string, text: string, recipientAgentId: string | null = null) {
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

function requestTask(channelId: string): ChannelTask | undefined {
  return channels.store.tasks(channelId).find((task) => task.parentTaskId === null);
}

function coordinatorMessages(channelId: string): string[] {
  return channels.store
    .messages(channelId, undefined, 50)
    .filter((message) => message.author.kind === "coordinator")
    .map((message) => message.message.text);
}

function create(name: string) {
  return runCauseEffect(
    agents.createAgent({
      name,
      description: `Synthetic ${name}`,
      avatarSeed: name.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
      avatarHue: null,
    }),
  );
}

async function expectLeadTakes(routingCase: Routing, memberId: string, label: string) {
  routing = routingCase;
  const leadId = (await create(`Lead ${routingCase}`)).id;
  const channelId = await channelWith(`Fallback ${routingCase}`, leadId, [leadId, memberId]);
  const before = report.routingTurns;
  await send(channelId, "What changed in the release?");
  await settle(`${routingCase}: lead assignment`, () =>
    channels.store.assignments(channelId).some((assignment) => assignment.deliveryId),
  );
  assert.equal(report.routingTurns, before + 1, `${routingCase}: one routing turn runs.`);
  const task = requestTask(channelId);
  assert.ok(task);
  assert.equal(task.ownerAgentId, leadId, `${routingCase}: the lead owns the request.`);
  assert.notEqual(task.state, "paused", `${routingCase}: the request is not paused.`);
  assert.equal(task.error, null, `${routingCase}: the request has no error.`);
  assert.deepEqual(coordinatorMessages(channelId), [], `${routingCase}: nothing is posted to the person.`);
  const assignment = channels.store.assignments(channelId).find((item) => item.taskId === task.id);
  assert.equal(assignment?.agentId, leadId);
  report.checks.push(label);
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  const lead = await create("Compass");
  const member = await create("Scout");
  const gone = await create("Gone");
  const other = await create("Other");

  await expectLeadTakes(
    "timeout",
    member.id,
    "A routing turn that times out gives the request to the lead, with no post to the person.",
  );
  await expectLeadTakes("provider", member.id, "A routing turn that the provider fails gives the request to the lead.");
  await expectLeadTakes(
    "unreadable",
    member.id,
    "A routing answer that is not a decision gives the request to the lead.",
  );

  routing = "question";
  const asked = await channelWith("Lead question", lead.id, [lead.id, member.id]);
  await send(asked, "What changed?");
  await settle("question pauses", () => requestTask(asked)?.state === "paused");
  assert.equal(requestTask(asked)?.ownerAgentId, null);
  assert.deepEqual(coordinatorMessages(asked), ["Which release do you mean?"]);
  report.checks.push("A question from the lead still pauses the request and posts the question.");

  routing = "timeout";
  const leaderless = await channelWith("Lead gone", gone.id, [gone.id, member.id, other.id]);
  removed.add(gone.id);
  await send(leaderless, "What changed in the release?");
  await settle("unavailable lead pauses", () => requestTask(leaderless)?.state === "paused");
  assert.equal(requestTask(leaderless)?.ownerAgentId, null);
  assert.deepEqual(coordinatorMessages(leaderless), [sourceText("error.backend.channelLeadRequired")]);
  assert.equal(channels.store.assignments(leaderless).length, 0);
  report.checks.push("A channel whose lead is unavailable still pauses the request and tells the person.");

  routing = "unused";
  const named = await channelWith("Named member", lead.id, [lead.id, member.id]);
  const turns = report.routingTurns;
  await send(named, "Scout, check the release.", member.id);
  await settle("named member assignment", () =>
    channels.store.assignments(named).some((assignment) => assignment.deliveryId),
  );
  assert.equal(report.routingTurns, turns, "A request with a named member runs no routing turn.");
  assert.equal(requestTask(named)?.ownerAgentId, member.id);
  report.checks.push("A request with a named member goes to that member without a routing turn.");

  const unexpected = errors.filter(
    (error) => !(error instanceof Error && error.message.startsWith("The provider returned no JSON object.")),
  );
  assert.deepEqual(unexpected, []);
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
