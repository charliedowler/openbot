import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { ConversationRuntime } from "../src/backend/agent/conversation-runtime";
import { RoutineScheduler } from "../src/backend/agent/routine-scheduler";
import { AgentStore } from "../src/backend/agent-store";
import { AgentWaitError, AgentWaitStore } from "../src/backend/agent-waits/agent-wait-store";
import { AgentWaits } from "../src/backend/agent-waits/agent-waits";
import { channelFailure } from "../src/backend/channel-effects";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import type { ReceivedWebhookEvent } from "../src/backend/routine-store";
import { RoutineTimer } from "../src/backend/routine-timer";

interface ChannelWait {
  channelId: string;
  taskId: string;
  deliveryId: string;
  agentId: string;
  waitId: string;
  turnId: string;
  threadId: string;
}

class Harness {
  readonly database: OpenBotDatabase;
  readonly store: AgentStore;
  readonly mailbox: MailboxStore;
  readonly waitStore: AgentWaitStore;
  readonly channels: ChannelService;
  readonly waits: AgentWaits;
  readonly conversation: ConversationRuntime;
  readonly scheduler: RoutineScheduler;
  readonly listeners = new Set<() => void>();
  busy = false;
  /** What each lane of the agent would start next at the moment a channel wait resumes. */
  readonly atResume: Array<{ main: string | null; beside: string | null }> = [];
  /** The routine run that each drain the scheduler asks for could start. */
  readonly atSchedule: Array<string | null> = [];
  failure: Error | null = null;
  closed = false;

  constructor(root: string) {
    this.database = new OpenBotDatabase(join(root, "state"));
    this.store = new AgentStore(join(root, "state"), join(root, "home"), this.database);
    this.mailbox = new MailboxStore(join(root, "state"), this.store.sharedRoot, this.database);
    this.waitStore = new AgentWaitStore({ database: this.database });
    this.channels = new ChannelService(this.database, this.mailbox, {
      agents: () => this.store.list(),
      generate: () => Effect.die(new Error("No routing model is used by this check.")),
      schedule: () => this.changed(),
      interrupt: () => Effect.void,
      busy: () => {
        this.changed();
        return this.busy;
      },
      eventWaitInput: (assignment) => this.waits.channelWakeInput(assignment),
      eventWaitQueued: (agentId, waitIds, deliveryId) => this.waits.channelWakeQueued(agentId, waitIds, deliveryId),
      waitingForEvent: (deliveryId) => this.waits.hasWaitForDelivery(deliveryId),
      eventWaitPrompt: (deliveryId) => this.waits.channelWaitsPrompt(deliveryId),
      resumeEventWaits: (deliveryId) =>
        this.waits.resumePendingChannelWaits(deliveryId).pipe(Effect.mapError(channelFailure)),
      changed: () => this.changed(),
      error: (error) => {
        this.failure = error instanceof Error ? error : new Error(String(error));
        this.changed();
      },
    });
    this.waits = new AgentWaits({
      store: this.waitStore,
      mailbox: this.mailbox,
      hooks: {
        emit: () => this.changed(),
        channelEventDispatched: (deliveryId) => this.channels.hasEventWaitDispatch(deliveryId),
        channelWaitDeliveryMatches: (wait, deliveryId) => this.channels.channelWaitDeliveryMatches(wait, deliveryId),
        channelWaitRequestText: (deliveryId) => this.channels.channelWaitRequestText(deliveryId),
        channelWaitAllowed: (deliveryId, agentId, turnId, threadId) =>
          this.channels.channelWaitAllowed(deliveryId, agentId, turnId, threadId),
        cancelChannelWait: (wait) => this.channels.cancelEventWait(wait),
        channelWaitEventHandled: (wait, deliveryId) => this.channels.eventWaitHandled(wait, deliveryId),
        channelWaitState: (wait) => this.channels.channelWaitState(wait),
        resumeChannelWait: (wait, deliveryId) =>
          Effect.sync(() => {
            this.atResume.push({
              main: this.mailbox.nextQueued(wait.agentId)?.delivery.id ?? null,
              beside: this.mailbox.nextBesideRoutine(wait.agentId)?.delivery.id ?? null,
            });
          })
            .pipe(Effect.andThen(this.channels.resumeEventWait(wait, deliveryId)))
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
    this.scheduler = new RoutineScheduler({
      store: this.store,
      mailbox: this.mailbox,
      conversation: this.conversation,
      waits: this.waits,
      timer: new RoutineTimer(
        () => [],
        () => false,
        () => {},
      ),
      hooks: {
        emit: () => {},
        emitError: (_code, error) => {
          throw error;
        },
        emitQueue: () => {},
        scheduleDrain: (agentId) => {
          this.atSchedule.push(this.mailbox.nextBesideRoutine(agentId)?.delivery.id ?? null);
        },
        interrupt: () => Effect.void,
        awaitDrain: () => undefined,
        syncMailboxMessages: () => {},
        listAgents: () => this.store.list(),
        excludedAgents: () => new Set(),
        isRunning: () => true,
        usageLimited: () => false,
      },
    });
  }

  async initialize() {
    await runCauseEffect(this.store.initialize());
    await runCauseEffect(this.mailbox.initialize());
  }

  changed() {
    for (const listener of this.listeners) listener();
  }

  async until(label: string, condition: () => boolean) {
    if (condition()) return;
    await new Promise<void>((done, fail) => {
      const timeout = setTimeout(() => {
        this.listeners.delete(check);
        fail(new Error(`Timed out: ${label}`));
      }, 5000);
      const check = () => {
        if (this.failure || condition()) {
          clearTimeout(timeout);
          this.listeners.delete(check);
          if (this.failure) fail(this.failure);
          else done();
        }
      };
      this.listeners.add(check);
      check();
    });
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

  inbox(agentId: string, name: string) {
    return this.scheduler.saveRecord(agentId, undefined, {
      name,
      instruction: "Read this pod.result delivery and acknowledge its exact event ID.",
      active: true,
      timezone: "UTC",
      trigger: { kind: "webhook", eventType: null, filters: [], secretCiphertext: "fixture-ciphertext" },
    });
  }

  async open(agentId: string, routineId: string, finish = true): Promise<ChannelWait> {
    const channelId = randomUUID();
    const actor = { id: "local", name: "Alex" };
    await runCauseEffect(
      this.channels.command(
        {
          type: "save",
          operationId: randomUUID(),
          channelId,
          draft: {
            name: "Dispatch",
            title: "",
            instructions: "Dispatch work to pods.",
            members: [{ agentId }],
            leadAgentId: agentId,
          },
        },
        actor,
      ),
    );
    await runCauseEffect(
      this.channels.command(
        {
          type: "send",
          operationId: randomUUID(),
          channelId,
          text: "Send the fix to a pod and report its result.",
          recipientAgentId: agentId,
          replyToMessageId: null,
          attachmentDraftIds: [],
        },
        actor,
      ),
    );
    await this.until("channel assignment delivery", () =>
      Boolean(this.channels.store.assignments(channelId)[0]?.deliveryId),
    );
    const assignment = this.channels.store.assignments(channelId)[0];
    assert.ok(assignment?.deliveryId);
    const wait: ChannelWait = {
      channelId,
      taskId: assignment.taskId,
      deliveryId: assignment.deliveryId,
      agentId,
      waitId: "",
      turnId: randomUUID(),
      threadId: this.channels.store.context(channelId, agentId).threadId,
    };
    await runCauseEffect(this.mailbox.markStarting(wait.deliveryId));
    await runCauseEffect(this.mailbox.markRunning(wait.deliveryId, wait.turnId));
    await runCauseEffect(this.channels.accepted(wait.deliveryId, randomUUID(), wait.turnId));
    const result = this.waits.handleTool(
      {
        threadId: wait.threadId,
        turnId: wait.turnId,
        callId: randomUUID(),
        namespace: "openbot",
        tool: "await_event",
        arguments: { note: "Waiting for the pod result.", routineId },
      },
      { agentId, inExecutionThread: true, threadId: wait.threadId },
    );
    assert.equal(result?.success, true, JSON.stringify(result));
    const stored = this.waitStore.list(agentId).find((item) => item.requestMessageId === wait.deliveryId);
    assert.ok(stored);
    wait.waitId = stored.id;
    if (finish) await this.finish(wait);
    return wait;
  }

  async finish(wait: ChannelWait) {
    await runCauseEffect(this.mailbox.markTerminal(wait.deliveryId, "completed"));
    this.channels.event({
      type: "turn-completed",
      agentId: wait.agentId,
      threadId: wait.threadId,
      turnId: wait.turnId,
      status: "completed",
    });
    await this.until(
      "channel assignment completion",
      () => this.channels.store.assignmentForDelivery(wait.deliveryId)?.state === "completed",
    );
    assert.equal(this.taskState(wait), "waiting");
  }

  taskState(wait: ChannelWait) {
    return this.channels.store.tasks(wait.channelId).find((task) => task.id === wait.taskId)?.state;
  }

  async receive(agentId: string, routineId: string, eventId: string, waitId?: string) {
    const event: ReceivedWebhookEvent = {
      deliveryId: `webhook-${eventId}`,
      eventType: "pod.result",
      data: { eventId, summary: `Pod result ${eventId}` },
      occurredAt: "2026-11-03T15:00:00Z",
      receivedAt: "2026-11-03T15:00:01Z",
      ...(waitId === undefined ? {} : { waitId }),
    };
    const result = await runCauseEffect(this.scheduler.receiveWebhook(agentId, routineId, event));
    assert.equal(result.kind, "started");
    assert.ok(result.kind === "started" && result.run);
    const run = this.scheduler.listRuns({ agentId, routineId, limit: 50 }).find((item) => item.id === result.run.id);
    assert.ok(run?.deliveryId);
    return run.deliveryId;
  }

  context(deliveryId: string) {
    const context = this.mailbox.getDelivery(deliveryId);
    assert.ok(context);
    return context;
  }

  delivery(deliveryId: string) {
    return this.context(deliveryId).delivery;
  }

  async close() {
    if (this.closed) return;
    this.conversation.dispose();
    await runCauseEffect(this.channels.stop());
    this.database.close();
    this.closed = true;
  }
}

const root = await mkdtemp(join(tmpdir(), "openbot-webhook-skip-own-chat-"));
const out = resolve(import.meta.dirname, "../.openbot-build/webhook-wait-skip-own-chat-e2e");
const report: { passed: boolean; checks: string[]; failed: string[] } = { passed: false, checks: [], failed: [] };
let index = 0;

async function check(name: string, body: (harness: Harness) => Promise<void>) {
  const harness = new Harness(join(root, `case-${index++}`));
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
    "A pod.result that names a waiting channel wait wakes that task and settles the own-chat run with no turn.",
    async (harness) => {
      const compass = await harness.agent("Compass");
      const results = harness.inbox(compass.id, "Pod results (Compass)");
      const named = await harness.open(compass.id, results.id);
      const other = await harness.open(compass.id, results.id);
      const previewBefore = harness.store.list().find((agent) => agent.id === compass.id)?.preview;
      const deliveryId = await harness.receive(compass.id, results.id, "evt-named", named.waitId);
      await harness.until("named continuation", () => harness.channels.store.assignments(named.channelId).length === 2);
      assert.equal(harness.taskState(other), "waiting");
      assert.equal(harness.channels.store.assignments(other.channelId).length, 1);
      const own = harness.delivery(deliveryId);
      assert.equal(own.status, "completed", "the own-chat run delivery must not wait for a turn");
      assert.equal(own.turnId, null);
      assert.equal(own.error, null);
      assert.equal(harness.scheduler.runForDelivery(deliveryId)?.status, "succeeded");
      assert.equal(harness.store.list().find((agent) => agent.id === compass.id)?.preview, previewBefore);
      const continuation = harness.channels.store.assignments(named.channelId).at(-1);
      assert.ok(continuation?.deliveryId);
      const prepared = await runCauseEffect(harness.channels.prepare(harness.context(continuation.deliveryId)));
      assert.match(prepared?.text ?? "", /evt-named/);
      assert.match(prepared?.text ?? "", new RegExp(named.waitId));
    },
  );

  await check(
    "No lane can start a named pod.result in the own chat while its channel wait resumes.",
    async (harness) => {
      const compass = await harness.agent("Compass");
      const results = harness.inbox(compass.id, "Pod results (Compass)");
      const named = await harness.open(compass.id, results.id);
      const deliveryId = await harness.receive(compass.id, results.id, "evt-held", named.waitId);
      assert.ok(harness.atResume.length > 0, "the channel wait must resume");
      for (const next of harness.atResume) {
        assert.notEqual(next.main, deliveryId, "the main lane must not start the delivery");
        assert.notEqual(next.beside, deliveryId, "the routine lane must not start the delivery");
      }
      assert.equal(harness.delivery(deliveryId).status, "completed");
    },
  );

  await check(
    "A second pod.result for the same wait before the task starts keeps the first event and runs in the own chat.",
    async (harness) => {
      const compass = await harness.agent("Compass");
      const results = harness.inbox(compass.id, "Pod results (Compass)");
      const named = await harness.open(compass.id, results.id);
      harness.busy = true;
      const first = await harness.receive(compass.id, results.id, "evt-first", named.waitId);
      assert.equal(harness.taskState(named), "queued");
      const second = await harness.receive(compass.id, results.id, "evt-second", named.waitId);
      assert.equal(harness.waitStore.get(compass.id, named.waitId)?.wakeDeliveryId, first);
      assert.equal(harness.delivery(second).status, "queued");
      harness.busy = false;
      await runCauseEffect(harness.channels.wake(named.channelId));
      await harness.until(
        "queued continuation",
        () => harness.channels.store.assignments(named.channelId).length === 2,
      );
      const continuation = harness.channels.store.assignments(named.channelId).at(-1);
      assert.ok(continuation?.deliveryId);
      const prepared = await runCauseEffect(harness.channels.prepare(harness.context(continuation.deliveryId)));
      assert.match(prepared?.text ?? "", /evt-first/);
    },
  );

  await check("A pod.result with no waitId wakes every wait and keeps its own-chat turn.", async (harness) => {
    const compass = await harness.agent("Compass");
    const results = harness.inbox(compass.id, "Pod results (Compass)");
    const first = await harness.open(compass.id, results.id);
    const second = await harness.open(compass.id, results.id);
    const deliveryId = await harness.receive(compass.id, results.id, "evt-unnamed");
    await harness.until(
      "both tasks resumed",
      () => harness.taskState(first) !== "waiting" && harness.taskState(second) !== "waiting",
    );
    assert.equal(harness.delivery(deliveryId).status, "queued");
    assert.equal(harness.waits.hasForwardedChannelWaits(deliveryId), true);
  });

  await check("A waitId that names no wait wakes as before and keeps its own-chat turn.", async (harness) => {
    const compass = await harness.agent("Compass");
    const results = harness.inbox(compass.id, "Pod results (Compass)");
    const wait = await harness.open(compass.id, results.id);
    const deliveryId = await harness.receive(compass.id, results.id, "evt-unknown", "channel:closed-long-ago");
    await harness.until("fallback continuation", () => harness.channels.store.assignments(wait.channelId).length === 2);
    assert.equal(harness.delivery(deliveryId).status, "queued");
  });

  await check("A waitId that names an own-chat wait keeps its own-chat turn.", async (harness) => {
    const compass = await harness.agent("Compass");
    const results = harness.inbox(compass.id, "Pod results (Compass)");
    const request = await runCauseEffect(
      harness.mailbox.enqueue({
        sender: { kind: "user" },
        recipientAgentIds: [compass.id],
        text: "Wait in your own chat.",
        draftIds: [],
      }),
    );
    const requestId = request.deliveries[0]?.id;
    assert.ok(requestId);
    const turnId = randomUUID();
    await runCauseEffect(harness.mailbox.markStarting(requestId));
    await runCauseEffect(harness.mailbox.markRunning(requestId, turnId));
    const opened = harness.waits.handleTool(
      {
        threadId: "main-thread",
        turnId,
        callId: randomUUID(),
        namespace: "openbot",
        tool: "await_event",
        arguments: { note: "Own-chat pod result." },
      },
      { agentId: compass.id, inExecutionThread: false },
    );
    assert.equal(opened?.success, true);
    await runCauseEffect(harness.mailbox.markTerminal(requestId, "completed"));
    const ownWait = harness.waitStore.list(compass.id).find((wait) => !wait.id.startsWith("channel:"));
    assert.ok(ownWait);
    const deliveryId = await harness.receive(compass.id, results.id, "evt-own", ownWait.id);
    assert.equal(harness.delivery(deliveryId).status, "queued");
    assert.deepEqual(
      harness.waits.wokenBy(deliveryId).map((wait) => wait.id),
      [ownWait.id],
    );
  });

  await check("A local script run that names a channel wait keeps its own-chat turn.", async (harness) => {
    const compass = await harness.agent("Compass");
    const results = harness.inbox(compass.id, "Pod results (Compass)");
    const wait = await harness.open(compass.id, results.id);
    const run = await runCauseEffect(
      harness.scheduler.runWithPayload({
        agentId: compass.id,
        routineId: results.id,
        payload: "pod.result evt-script",
        source: "script",
        waitId: wait.waitId,
      }),
    );
    await harness.until("script continuation", () => harness.channels.store.assignments(wait.channelId).length === 2);
    assert.ok(run.deliveryId);
    assert.equal(harness.delivery(run.deliveryId).status, "queued");
  });

  await check("A pod.result for a channel wait whose task still runs keeps its own-chat turn.", async (harness) => {
    const compass = await harness.agent("Compass");
    const results = harness.inbox(compass.id, "Pod results (Compass)");
    const wait = await harness.open(compass.id, results.id, false);
    const deliveryId = await harness.receive(compass.id, results.id, "evt-early", wait.waitId);
    assert.equal(harness.channels.store.assignments(wait.channelId).length, 1);
    assert.equal(harness.delivery(deliveryId).status, "queued");
    assert.equal(harness.mailbox.nextBesideRoutine(compass.id)?.delivery.id, deliveryId, "the delivery is not held");
    assert.ok(harness.atSchedule.includes(deliveryId), "the drain that the run asks for can start the delivery");
  });
  report.passed = report.failed.length === 0;
} finally {
  await rm(root, { recursive: true, force: true });
  mkdirSync(out, { recursive: true });
  writeFileSync(join(out, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}
process.stdout.write(`${report.checks.length} of ${report.checks.length + report.failed.length} checks pass\n`);
if (!report.passed) process.exitCode = 1;
