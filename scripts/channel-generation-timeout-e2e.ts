import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentModelOption, ChannelMessage, ChannelTask } from "@openbot/contracts/ipc";
import { sourceText } from "@openbot/i18n/source";
import { Deferred, Effect, Fiber, Result } from "effect";
import { TestClock } from "effect/testing";
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
  provider: "codex",
  id: "gpt-6-astra",
  name: "Synthetic model",
  description: "Synthetic model",
  defaultReasoningEffort: "high",
  supportedReasoningEfforts: ["low", "high"],
};

class SummaryClient extends EventEmitter<ClientEvents> implements AgentClient {
  readonly provider = "codex";
  running = false;
  stopped = false;
  cwd: string | null = null;
  constructor(
    readonly started: Deferred.Deferred<void>,
    readonly delay: number,
    readonly text = "Keep the shared decisions and source IDs.",
  ) {
    super();
  }
  start() {
    this.running = true;
  }
  stop() {
    return Effect.sync(() => {
      this.running = false;
      this.stopped = true;
    });
  }
  request<T>(method: string, _params: unknown, decoder: ResponseDecoder<T>) {
    return Effect.gen({ self: this }, function* () {
      if (method === "thread/start") {
        assert.ok(typeof _params === "object" && _params !== null && "cwd" in _params);
        assert.equal(typeof _params.cwd, "string");
        if (typeof _params.cwd === "string") this.cwd = _params.cwd;
        assert.ok("effort" in _params);
        assert.equal(_params.effort, "high");
      }
      if (method === "turn/start") {
        yield* Effect.forkChild(
          Effect.sleep(this.delay).pipe(
            Effect.andThen(
              Effect.sync(() => {
                this.emit("notification", {
                  method: "item/agentMessage/delta",
                  params: { delta: this.text },
                });
                this.emit("notification", { method: "turn/completed", params: { turn: { status: "completed" } } });
              }),
            ),
          ),
        );
        yield* Deferred.succeed(this.started, undefined);
      }
      return yield* providerSync(() => decoder(method === "thread/start" ? { thread: { id: "summary" } } : {}));
    });
  }
  notify() {}
  respond() {}
  respondError() {}
}

const output = resolve(import.meta.dirname, "../.openbot-build/agent-channel-manage-e2e/generation-timeout");
const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };
const root = await mkdtemp(join(tmpdir(), "openbot-summary-timeout-"));
const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);

function clean(client: SummaryClient) {
  assert.equal(client.stopped, true);
  assert.equal(client.running, false);
  assert.ok(client.cwd);
  assert.equal(existsSync(client.cwd), false);
  for (const event of ["notification", "request", "exit"]) assert.equal(client.listenerCount(event), 0);
}

try {
  await Effect.runPromise(
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const client = new SummaryClient(started, 121_000);
      const fiber = yield* Effect.forkChild(
        generateTextWithoutTools(
          client,
          model,
          "Summarize the synthetic channel history.",
          undefined,
          undefined,
          "channel-summary",
        ).pipe(Effect.result),
      );
      yield* Deferred.await(started);
      yield* TestClock.adjust(121_000);
      const result = yield* Fiber.join(fiber);
      assert.ok(Result.isSuccess(result), "A valid channel summary is killed by the profile generation deadline.");
      assert.equal(result.success, "Keep the shared decisions and source IDs.");
      clean(client);
      report.checks.push("A summary that takes 121 seconds completes and releases its client.");

      const profileStarted = yield* Deferred.make<void>();
      const profile = new SummaryClient(profileStarted, 121_000);
      const profileFiber = yield* Effect.forkChild(
        generateTextWithoutTools(profile, model, "A profile.").pipe(Effect.result),
      );
      yield* Deferred.await(profileStarted);
      yield* TestClock.adjust(120_000);
      const profileResult = yield* Fiber.join(profileFiber);
      assert.ok(Result.isFailure(profileResult));
      assert.ok(profileResult.failure.cause instanceof Error);
      assert.equal(profileResult.failure.cause.message, sourceText("error.agent.profileTimedOut"));
      clean(profile);
      report.checks.push("Profile generation keeps its two-minute deadline and error.");

      const stalledStarted = yield* Deferred.make<void>();
      const stalled = new SummaryClient(stalledStarted, 601_000);
      const stalledFiber = yield* Effect.forkChild(
        generateTextWithoutTools(stalled, model, "A summary.", undefined, undefined, "channel-summary").pipe(
          Effect.result,
        ),
      );
      yield* Deferred.await(stalledStarted);
      yield* TestClock.adjust(600_000);
      const stalledResult = yield* Fiber.join(stalledFiber);
      assert.ok(Result.isFailure(stalledResult));
      assert.ok(stalledResult.failure.cause instanceof Error);
      assert.equal(stalledResult.failure.cause.message, sourceText("error.backend.channelSummaryTimedOut"));
      clean(stalled);
      report.checks.push("A stalled summary stops after ten minutes and names the failed operation.");

      const interruptedStarted = yield* Deferred.make<void>();
      const interrupted = new SummaryClient(interruptedStarted, 601_000);
      const interruptedFiber = yield* Effect.forkChild(
        generateTextWithoutTools(interrupted, model, "A summary.", undefined, undefined, "channel-summary"),
      );
      yield* Deferred.await(interruptedStarted);
      yield* Fiber.interrupt(interruptedFiber);
      clean(interrupted);
      report.checks.push("Cancellation releases the summary client before its deadline.");
    }).pipe(Effect.provide(TestClock.layer())),
  );

  await Effect.runPromise(agents.initialize());
  const lead = await Effect.runPromise(
    agents.createAgent({ name: "Planner", description: "Synthetic", avatarSeed: "planner", avatarHue: null }),
  );
  const store = new ChannelStore(database);
  const channel = store.create(randomUUID(), {
    name: "Summary timeout",
    title: "",
    instructions: "",
    members: [{ agentId: lead.id }],
    leadAgentId: lead.id,
  });
  const messages: ChannelMessage[] = Array.from({ length: 3 }, () => {
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
        text: "Synthetic history. ".repeat(1400),
        createdAt: new Date().toISOString(),
        status: "completed",
      },
    };
  });
  const taskId = randomUUID();
  const requestMessage = messages.at(-1);
  assert.ok(requestMessage);
  const task: ChannelTask = {
    id: taskId,
    channelId: channel.id,
    parentTaskId: null,
    rootTaskId: taskId,
    ownerAgentId: lead.id,
    requestMessageId: requestMessage.id,
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
  };
  store.update(channel, { messages, tasks: [task] });
  await Effect.runPromise(
    Effect.gen(function* () {
      const starts = [yield* Deferred.make<void>(), yield* Deferred.make<void>()];
      const clients: SummaryClient[] = [];
      const history = new ChannelHistory(
        store,
        (_lead, prompt, purpose) => {
          assert.equal(purpose, "channel-summary");
          const call = clients.length;
          const started = starts[call];
          assert.ok(started);
          const client = new SummaryClient(
            started,
            121_000,
            call === 0 ? "x".repeat(13_000) : "Keep decisions and source IDs.",
          );
          clients.push(client);
          return generateTextWithoutTools(client, model, prompt, undefined, undefined, purpose).pipe(
            Effect.mapError(channelFailure),
          );
        },
        new ChannelMemoryStore(database),
      );
      const fiber = yield* Effect.forkChild(history.prepare(task, lead, lead, null, []));
      for (const started of starts) {
        yield* Deferred.await(started);
        yield* TestClock.adjust(121_000);
      }
      const packet = yield* Fiber.join(fiber);
      assert.equal(clients.length, 2);
      for (const client of clients) clean(client);
      const saved = store.summary(channel.id);
      assert.equal(saved.version, 1);
      assert.ok(saved.throughSequence > 0);
      assert.equal(saved.text, "Keep decisions and source IDs.");
      assert.ok(packet.text.includes(saved.text));
      assert.equal(packet.summaryVersion, 1);
      report.checks.push("Channel preparation and summary shortening use the summary deadline and persist the result.");
    }).pipe(Effect.provide(TestClock.layer())),
  );
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
