import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary } from "@openbot/contracts/ipc";
import { Effect, Exit, Scope } from "effect";
import { ConversationRuntime } from "../src/backend/agent/conversation-runtime";
import { RoutineScheduler } from "../src/backend/agent/routine-scheduler";
import { AgentStore } from "../src/backend/agent-store";
import { AgentWaitError, AgentWaitStore } from "../src/backend/agent-waits/agent-wait-store";
import { AgentWaits } from "../src/backend/agent-waits/agent-waits";
import { channelFailure } from "../src/backend/channel-effects";
import { ChannelImages } from "../src/backend/channel-images";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import type { ReceivedWebhookEvent } from "../src/backend/routine-store";
import { RoutineTimer } from "../src/backend/routine-timer";
import { StoredStateFailure } from "../src/backend/stored-state-effects";

process.env.OPENBOT_ROUTINE_COALESCE = "0";

/** Signal answers the sender 503 when the host has not answered a delivery in this time. */
const SIGNAL_DELIVERY_TIMEOUT_MS = 2_500;

class Harness {
  readonly database: OpenBotDatabase;
  readonly store: AgentStore;
  readonly mailbox: MailboxStore;
  readonly waitStore: AgentWaitStore;
  readonly channels: ChannelService;
  readonly waits: AgentWaits;
  readonly conversation: ConversationRuntime;
  readonly scheduler: RoutineScheduler;
  readonly errors: Array<{ code: string; error: unknown }> = [];
  /** How long queueing a delivery holds the main loop, as on a busy host. */
  stallMs = 0;
  closed = false;

  constructor(root: string, scope?: Scope.Scope) {
    this.database = new OpenBotDatabase(join(root, "state"));
    this.store = new AgentStore(join(root, "state"), join(root, "home"), this.database);
    this.mailbox = new MailboxStore(join(root, "state"), this.store.sharedRoot, this.database);
    this.waitStore = new AgentWaitStore({ database: this.database });
    this.channels = new ChannelService(this.database, this.mailbox, new ChannelImages(join(root, "channel-images")), {
      agents: () => this.store.list(),
      generate: () => Effect.die(new Error("No routing model is used by this check.")),
      schedule: () => {},
      interrupt: () => Effect.void,
      busy: () => false,
      eventWaitInput: (assignment) => this.waits.channelWakeInput(assignment),
      eventWaitQueued: (agentId, waitIds, deliveryId) => this.waits.channelWakeQueued(agentId, waitIds, deliveryId),
      waitingForEvent: (deliveryId) => this.waits.hasWaitForDelivery(deliveryId),
      eventWaitPrompt: (deliveryId) => this.waits.channelWaitsPrompt(deliveryId),
      resumeEventWaits: (deliveryId) =>
        this.waits.resumePendingChannelWaits(deliveryId).pipe(Effect.mapError(channelFailure)),
      changed: () => {},
      error: (error) => {
        this.errors.push({ code: "channel", error });
      },
    });
    this.waits = new AgentWaits({
      store: this.waitStore,
      mailbox: this.mailbox,
      hooks: {
        emit: () => {},
        channelEventDispatched: (deliveryId) => this.channels.hasEventWaitDispatch(deliveryId),
        channelWaitDeliveryMatches: (wait, deliveryId) => this.channels.channelWaitDeliveryMatches(wait, deliveryId),
        channelWaitRequestText: (deliveryId) => this.channels.channelWaitRequestText(deliveryId),
        channelWaitAllowed: (deliveryId, agentId, turnId, threadId) =>
          this.channels.channelWaitAllowed(deliveryId, agentId, turnId, threadId),
        cancelChannelWait: (wait) => this.channels.cancelEventWait(wait),
        channelWaitEventHandled: (wait, deliveryId) => this.channels.eventWaitHandled(wait, deliveryId),
        channelWaitState: (wait) => this.channels.channelWaitState(wait),
        resumeChannelWait: (wait, deliveryId) =>
          this.channels
            .resumeEventWait(wait, deliveryId)
            .pipe(Effect.mapError((failure) => new AgentWaitError(String(failure.cause)))),
        channelWaitMatches: (wait, assignment) => this.channels.channelWaitMatches(wait, assignment),
        ownsRoutine: () => true,
        hasRoutine: () => true,
        requireKnownAgent: () => {},
      },
    });
    this.conversation = new ConversationRuntime(
      this.store,
      () => {},
      () => this.store.list(),
    );
    this.scheduler = this.newScheduler(scope);
  }

  newScheduler(scope?: Scope.Scope) {
    return new RoutineScheduler({
      store: this.store,
      mailbox: this.mailbox,
      conversation: this.conversation,
      waits: this.waits,
      ...(scope ? { scope: () => scope } : {}),
      timer: new RoutineTimer(
        () => [],
        () => false,
        () => {},
      ),
      hooks: {
        emit: () => {},
        emitError: (code, error) => {
          this.errors.push({ code, error });
        },
        emitQueue: () => {},
        scheduleDrain: () => {},
        interrupt: () => Effect.void,
        awaitDrain: () => undefined,
        syncMailboxMessages: () => {
          const end = Date.now() + this.stallMs;
          while (Date.now() < end) {}
        },
        listAgents: () => this.store.list(),
        excludedAgents: () => new Set(),
        isRunning: () => true,
        usageLimited: () => false,
        routineThread: () => null,
        routineDeleted: () => Effect.void,
      },
    });
  }

  async initialize() {
    await runCauseEffect(this.store.initialize());
    await runCauseEffect(this.mailbox.initialize());
  }

  async agent(name: string): Promise<AgentSummary> {
    return runCauseEffect(
      this.store.createAgent({
        name,
        description: "Coordinates pod work.",
        avatarSeed: name.toLowerCase().replaceAll(" ", "-"),
        avatarHue: null,
      }),
    );
  }

  inbox(agentId: string) {
    return this.scheduler.saveRecord(agentId, undefined, {
      name: "Pod results",
      instruction: "Read this pod.result delivery and acknowledge its exact event ID.",
      active: true,
      timezone: "UTC",
      trigger: { kind: "webhook", eventType: null, filters: [], secretCiphertext: "fixture-ciphertext" },
    });
  }

  event(eventId: string): ReceivedWebhookEvent {
    return {
      deliveryId: `webhook-${eventId}`,
      eventType: "pod.result",
      data: { eventId, summary: `Pod result ${eventId}` },
      occurredAt: "2026-11-03T15:00:00Z",
      receivedAt: "2026-11-03T15:00:01Z",
    };
  }

  run(agentId: string, routineId: string, runId: string) {
    return this.scheduler.listRuns({ agentId, routineId, limit: 50 }).find((item) => item.id === runId);
  }

  routineDeliveries(agentId: string) {
    return this.mailbox
      .queuedDeliveryIds(agentId)
      .map((id) => this.mailbox.getDelivery(id)?.delivery)
      .filter((delivery) => delivery?.sender.kind === "routine");
  }

  async until(label: string, condition: () => boolean, timeoutMs = 10_000) {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
      if (Date.now() > deadline) throw new Error(`Timed out: ${label}`);
      await new Promise((done) => setTimeout(done, 10));
    }
  }

  async close() {
    if (this.closed) return;
    this.conversation.dispose();
    await runCauseEffect(this.channels.stop());
    this.database.close();
    this.closed = true;
  }
}

const root = await mkdtemp(join(tmpdir(), "openbot-webhook-ack-"));
const out = resolve(import.meta.dirname, "../.openbot-build/webhook-ack-before-enqueue-e2e");
const report: { passed: boolean; checks: string[]; failed: string[] } = { passed: false, checks: [], failed: [] };
let index = 0;

async function check(name: string, body: (harness: Harness) => Promise<void>, scope?: Scope.Scope) {
  const harness = new Harness(join(root, `case-${index++}`), scope);
  try {
    await harness.initialize();
    await body(harness);
    report.checks.push(name);
    process.stdout.write(`PASS ${name}\n`);
  } catch (error) {
    report.failed.push(`${name}: ${error instanceof Error ? error.message : String(error)}`);
    process.stdout.write(`FAIL ${name}\n  ${error instanceof Error ? error.message : String(error)}\n`);
  } finally {
    await harness.close();
  }
}

try {
  await check(
    "The sender gets its answer within Signal's 2.5 s while queueing the delivery holds the main loop for 3 s.",
    async (harness) => {
      const compass = await harness.agent("Compass");
      const routine = harness.inbox(compass.id);
      harness.stallMs = 3_000;
      const startedAt = Date.now();
      const result = await runCauseEffect(
        harness.scheduler.receiveWebhook(compass.id, routine.id, harness.event("evt-slow")),
      );
      const answeredMs = Date.now() - startedAt;
      assert.ok(
        answeredMs < SIGNAL_DELIVERY_TIMEOUT_MS,
        `the answer took ${answeredMs} ms; Signal answers the sender 503 after ${SIGNAL_DELIVERY_TIMEOUT_MS} ms`,
      );
      assert.equal(result.kind, "started");
      assert.ok(result.kind === "started");
      assert.equal(harness.run(compass.id, routine.id, result.run.id)?.status, "queued");
      assert.equal(harness.scheduler.pendingWebhookRuns().length, 1, "a stop can wait for the run");
      await runCauseEffect(Effect.all(harness.scheduler.pendingWebhookRuns()));
      assert.equal(harness.scheduler.pendingWebhookRuns().length, 0);
      const run = harness.run(compass.id, routine.id, result.run.id);
      assert.ok(run?.deliveryId, "the delivery is queued after the answer");
      assert.equal(harness.mailbox.getDelivery(run.deliveryId)?.delivery.status, "queued");
      assert.deepEqual(harness.errors, []);
    },
  );

  await check(
    "Two webhooks for one routine that arrive together each queue one delivery, in the order they came.",
    async (harness) => {
      const compass = await harness.agent("Compass");
      const routine = harness.inbox(compass.id);
      const getOrCreate = harness.store.getOrCreate;
      let calls = 0;
      harness.store.getOrCreate = (id, name, title) =>
        calls++ === 0
          ? Effect.sleep("300 millis").pipe(Effect.andThen(getOrCreate(id, name, title)))
          : getOrCreate(id, name, title);
      const [first, second] = await Promise.all([
        runCauseEffect(harness.scheduler.receiveWebhook(compass.id, routine.id, harness.event("evt-1"))),
        runCauseEffect(harness.scheduler.receiveWebhook(compass.id, routine.id, harness.event("evt-2"))),
      ]);
      assert.ok(first.kind === "started" && second.kind === "started");
      assert.notEqual(first.run.id, second.run.id);
      await harness.until(
        "both deliveries",
        () =>
          Boolean(harness.run(compass.id, routine.id, first.run.id)?.deliveryId) &&
          Boolean(harness.run(compass.id, routine.id, second.run.id)?.deliveryId),
      );
      const firstDelivery = harness.run(compass.id, routine.id, first.run.id)?.deliveryId;
      const secondDelivery = harness.run(compass.id, routine.id, second.run.id)?.deliveryId;
      assert.notEqual(firstDelivery, secondDelivery);
      const queued = harness.routineDeliveries(compass.id);
      assert.deepEqual(
        queued.map((delivery) => delivery?.id),
        [firstDelivery, secondDelivery],
        "the deliveries queue in the order the requests came",
      );
      assert.deepEqual(
        queued.map((delivery) => (delivery?.sender.kind === "routine" ? delivery.sender.runId : null)),
        [first.run.id, second.run.id],
      );
      const repeat = await runCauseEffect(
        harness.scheduler.receiveWebhook(compass.id, routine.id, harness.event("evt-1")),
      );
      assert.equal(repeat.kind, "duplicate", "Signal's retry of a delivery queues nothing");
      await new Promise((done) => setTimeout(done, 50));
      assert.equal(harness.routineDeliveries(compass.id).length, 2);
      assert.deepEqual(harness.errors, []);
    },
  );

  await check(
    "Ten webhooks for one routine received at once queue ten deliveries, one per run, in order.",
    async (harness) => {
      const compass = await harness.agent("Compass");
      const routine = harness.inbox(compass.id);
      const results = await Promise.all(
        Array.from({ length: 10 }, (_, item) =>
          runCauseEffect(harness.scheduler.receiveWebhook(compass.id, routine.id, harness.event(`evt-burst-${item}`))),
        ),
      );
      const runIds = results.map((result) => (result.kind === "started" ? result.run.id : null));
      assert.equal(new Set(runIds).size, 10);
      await harness.until("ten deliveries", () => harness.routineDeliveries(compass.id).length === 10);
      await new Promise((done) => setTimeout(done, 50));
      const queued = harness.routineDeliveries(compass.id);
      assert.equal(queued.length, 10);
      assert.deepEqual(
        queued.map((delivery) => (delivery?.sender.kind === "routine" ? delivery.sender.runId : null)),
        runIds,
      );
      assert.deepEqual(harness.errors, []);
    },
  );

  await check(
    "A delivery that cannot be queued is still reported after the answer, and its run fails.",
    async (harness) => {
      const compass = await harness.agent("Compass");
      const routine = harness.inbox(compass.id);
      harness.mailbox.enqueue = () => Effect.fail(new StoredStateFailure({ cause: new Error("disk is full") }));
      const result = await runCauseEffect(
        harness.scheduler.receiveWebhook(compass.id, routine.id, harness.event("evt-fail")),
      );
      assert.equal(result.kind, "started");
      assert.ok(result.kind === "started");
      await harness.until("failure report", () => harness.errors.length > 0);
      assert.deepEqual(
        harness.errors.map((entry) => entry.code),
        ["routine_delivery_failed"],
      );
      await harness.until("failed run", () => harness.run(compass.id, routine.id, result.run.id)?.status === "failed");
    },
  );

  const stopScope = Scope.makeUnsafe();
  await check(
    "A stop before the delivery is queued leaves the run queued, and resumePendingRuns queues it once.",
    async (harness) => {
      const compass = await harness.agent("Compass");
      const routine = harness.inbox(compass.id);
      const result = await runCauseEffect(
        harness.scheduler.receiveWebhook(compass.id, routine.id, harness.event("evt-stop")),
      );
      await Effect.runPromise(Scope.close(stopScope, Exit.void));
      assert.ok(result.kind === "started");
      await new Promise((done) => setTimeout(done, 50));
      const saved = harness.run(compass.id, routine.id, result.run.id);
      assert.equal(saved?.status, "queued");
      assert.equal(saved?.deliveryId, null);
      const restarted = harness.newScheduler();
      await runCauseEffect(restarted.resumePendingRuns());
      const resumed = harness.run(compass.id, routine.id, result.run.id);
      assert.ok(resumed?.deliveryId);
      assert.equal(harness.routineDeliveries(compass.id).length, 1);
      assert.deepEqual(harness.errors, []);
    },
    stopScope,
  );
  report.passed = report.failed.length === 0;
} finally {
  await rm(root, { recursive: true, force: true });
  mkdirSync(out, { recursive: true });
  writeFileSync(join(out, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}
process.stdout.write(`${report.checks.length} of ${report.checks.length + report.failed.length} checks pass\n`);
if (!report.passed) process.exitCode = 1;
