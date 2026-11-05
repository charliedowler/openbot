import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentModelOption, AgentSummary, ChannelMessage, ChannelTask } from "@openbot/contracts/ipc";
import { sourceText } from "@openbot/i18n/source";
import { Effect, Result } from "effect";
import { generateTextWithoutTools } from "../src/backend/agent/profile-generation";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import { AgentStore } from "../src/backend/agent-store";
import { channelFailure } from "../src/backend/channel-effects";
import { ChannelHistory } from "../src/backend/channel-history";
import { ChannelMemoryStore } from "../src/backend/channel-memory-store";
import { ChannelStore } from "../src/backend/channel-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import type { AppServerNotification, AppServerRequest, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";

interface ClientEvents {
  notification: [notification: AppServerNotification];
  request: [request: AppServerRequest];
  exit: [error: Error];
  diagnostic: [message: string, origin?: DiagnosticOrigin];
}

const model: AgentModelOption = {
  provider: "claude",
  id: "claude-sonnet-5-5",
  name: "Synthetic Claude model",
  description: "Synthetic Claude model",
  defaultReasoningEffort: "high",
  supportedReasoningEfforts: ["low", "medium", "high"],
};

class ClaudeShapedClient extends EventEmitter<ClientEvents> implements AgentClient {
  readonly provider = "claude";
  running = false;
  constructor(readonly answer: string) {
    super();
  }
  start() {
    this.running = true;
  }
  stop() {
    return Effect.sync(() => {
      this.running = false;
    });
  }
  request<T>(method: string, _params: unknown, decoder: ResponseDecoder<T>) {
    if (method === "turn/start")
      setImmediate(() => {
        this.emit("notification", { method: "turn/started", params: { turn: { id: "turn", status: "inProgress" } } });
        this.emit("notification", {
          method: "item/completed",
          params: { turnId: "turn", item: { id: "turn:assistant", type: "agentMessage", text: this.answer } },
        });
        this.emit("notification", { method: "turn/completed", params: { turn: { id: "turn", status: "completed" } } });
      });
    return providerSync(() => decoder(method === "thread/start" ? { thread: { id: "summary" } } : {}));
  }
  notify() {}
  respond() {}
  respondError() {}
}

const lines = (count: number) =>
  Array.from({ length: count }, (_, index) => `- Decision ${index}: keep message ${randomUUID()} as the source.`).join(
    "\n",
  );
const longSummary = lines(200);
const longerSummary = lines(190);
const stillLong = lines(185);
const shortSummary = lines(100);
assert.ok(longSummary.length > 12_000 && longerSummary.length > 12_000 && stillLong.length > 12_000);
assert.ok(shortSummary.length < 12_000);

const output = resolve(import.meta.dirname, "../.openbot-build/channel-summary-fit-e2e");
const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };
const root = await mkdtemp(join(tmpdir(), "openbot-channel-summary-fit-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const store = new ChannelStore(database);
const memories = new ChannelMemoryStore(database);

function scenario(lead: AgentSummary, sizes: number[]) {
  const channel = store.create(randomUUID(), {
    name: "Summary fit",
    title: "",
    instructions: "",
    members: [{ agentId: lead.id }],
    leadAgentId: lead.id,
  });
  const messages: ChannelMessage[] = sizes.map((size, index) => {
    const id = randomUUID();
    return {
      id,
      channelId: channel.id,
      sequence: 0,
      author: { kind: "member", id: "local", name: "Example" },
      taskId: null,
      superseded: false,
      message: {
        id,
        author: "user",
        text: `Message ${index} marker-${index}. ${"Synthetic history. ".repeat(Math.ceil(size / 19))}`.slice(0, size),
        createdAt: new Date().toISOString(),
        status: "completed",
      },
    };
  });
  const request = messages.at(-1);
  assert.ok(request);
  const taskId = randomUUID();
  const task: ChannelTask = {
    id: taskId,
    channelId: channel.id,
    parentTaskId: null,
    rootTaskId: taskId,
    ownerAgentId: lead.id,
    requestMessageId: request.id,
    instruction: "Continue the work.",
    attachmentDraftIds: [],
    expectedResult: "A result.",
    sourceMessageIds: [request.id],
    dependencies: [],
    resources: [],
    state: "queued",
    revision: 0,
    assignmentCount: 0,
    error: null,
  };
  store.update(channel, { messages, tasks: [task] });
  return { channel, task };
}

async function prepare(lead: AgentSummary, sizes: number[], answers: string[]) {
  const { channel, task } = scenario(lead, sizes);
  const prompts: string[] = [];
  const history = new ChannelHistory(
    store,
    (_lead, prompt, purpose) => {
      assert.equal(purpose, "channel-summary");
      const answer = answers[prompts.length];
      prompts.push(prompt);
      if (answer === undefined) return Effect.die(new Error(`Unexpected summary call ${prompts.length}.`));
      return generateTextWithoutTools(
        new ClaudeShapedClient(answer),
        model,
        prompt,
        undefined,
        undefined,
        purpose,
      ).pipe(Effect.mapError(channelFailure));
    },
    memories,
  );
  const result = await Effect.runPromise(history.prepare(task, lead, lead, null, []).pipe(Effect.result));
  return { channel, result, prompts, summary: store.summary(channel.id) };
}

try {
  await Effect.runPromise(agents.initialize());
  const lead = await Effect.runPromise(
    agents.createAgent({ name: "Planner", description: "Synthetic", avatarSeed: "planner", avatarHue: null }),
  );
  const history = [26_000, 26_000, 26_000];

  const trimmed = await prepare(lead, history, [longSummary, longerSummary, stillLong]);
  assert.ok(Result.isSuccess(trimmed.result), "A summary that stays too long after shortening fails the assignment.");
  assert.equal(trimmed.prompts.length, 3);
  assert.ok(trimmed.prompts[1]?.startsWith("Shorten"));
  assert.ok(trimmed.prompts[2]?.startsWith("The summary is still too long."));
  assert.ok(trimmed.prompts[2]?.includes("under 4000 characters"));
  assert.ok(trimmed.summary.text.length <= 12_000);
  assert.ok(stillLong.startsWith(`${trimmed.summary.text}\n`), "The cut summary does not end at a line end.");
  assert.equal(trimmed.summary.version, 1);
  assert.ok(trimmed.summary.throughSequence > 0);
  assert.ok(trimmed.result.success.text.includes(trimmed.summary.text));
  report.checks.push(
    "A summary that stays over 12000 characters after two shorten requests is cut at a line end and saved.",
  );

  const shortened = await prepare(lead, history, [longSummary, shortSummary]);
  assert.ok(Result.isSuccess(shortened.result));
  assert.equal(shortened.prompts.length, 2);
  assert.equal(shortened.summary.text, shortSummary);
  assert.equal(shortened.summary.version, 1);
  report.checks.push("A summary that one shorten request fits is saved as the model wrote it.");

  const emptyShorten = await prepare(lead, history, [longSummary, "", " \n"]);
  assert.ok(Result.isSuccess(emptyShorten.result), "An empty shorten answer fails the assignment.");
  assert.equal(emptyShorten.prompts.length, 3);
  assert.ok(emptyShorten.summary.text.length <= 12_000 && emptyShorten.summary.text.length > 6_000);
  assert.ok(longSummary.startsWith(`${emptyShorten.summary.text}\n`));
  report.checks.push("Empty shorten answers keep the long summary, which is cut at a line end.");

  const empty = await prepare(lead, history, [""]);
  assert.ok(Result.isSuccess(empty.result), "An empty summary fails the assignment.");
  assert.equal(empty.prompts.length, 1);
  assert.deepEqual(
    { version: empty.summary.version, throughSequence: empty.summary.throughSequence },
    {
      version: 0,
      throughSequence: 0,
    },
  );
  const packet = empty.result.success.text;
  assert.ok(packet.length <= 120_000);
  assert.ok(packet.includes("marker-2"), "The request is not in the packet.");
  assert.ok(packet.includes("marker-1"), "The newest messages that fit are not in the packet.");
  assert.ok(!packet.includes("marker-0"), "The packet keeps a message that does not fit.");
  assert.ok(packet.includes("Messages 1 to 1 are not in this packet or the summary"));
  assert.ok(packet.includes("channel_history with beforeSequence 2"));
  assert.equal(empty.result.success.throughSequence, 3);
  report.checks.push(
    "An empty summary keeps the earlier summary and cursor, and the packet keeps the newest messages that fit and names the gap.",
  );

  const tooLarge = await prepare(lead, [1_000, 70_000], [""]);
  assert.ok(Result.isFailure(tooLarge.result));
  assert.ok(tooLarge.result.failure.cause instanceof Error);
  assert.equal(tooLarge.result.failure.cause.message, sourceText("error.backend.channelHistoryInvalid"));
  assert.equal(tooLarge.summary.version, 0);
  report.checks.push("An empty summary still fails when the newest message alone does not fit.");

  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? error.message : String(error);
  throw error;
} finally {
  database.close();
  await rm(root, { recursive: true, force: true });
  mkdirSync(output, { recursive: true });
  writeFileSync(resolve(output, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify(report)}\n`);
}
