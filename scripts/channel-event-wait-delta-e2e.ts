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
import { ChannelImages } from "../src/backend/channel-images";
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
  failure: Error | null = null;
  closed = false;

  constructor(root: string) {
    this.database = new OpenBotDatabase(join(root, "state"));
    this.store = new AgentStore(join(root, "state"), join(root, "home"), this.database);
    this.mailbox = new MailboxStore(join(root, "state"), this.store.sharedRoot, this.database);
    this.waitStore = new AgentWaitStore({ database: this.database });
    this.channels = new ChannelService(this.database, this.mailbox, new ChannelImages(join(root, "channel-images")), {
      agents: () => this.store.list(),
      generate: () => Effect.die(new Error("No routing model is used by this check.")),
      schedule: () => this.changed(),
      interrupt: () => Effect.void,
      busy: () => {
        this.changed();
        return this.busy;
      },
      reportsCompactions: () => true,
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
        scheduleDrain: () => {},
        interrupt: () => Effect.void,
        awaitDrain: () => undefined,
        syncMailboxMessages: () => {},
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
    await this.run(wait.deliveryId, wait.turnId);
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

  readonly sessionId = `session-${randomUUID()}`;

  async run(deliveryId: string, turnId: string) {
    await runCauseEffect(this.mailbox.markStarting(deliveryId));
    const packet = await runCauseEffect(this.channels.prepare(this.context(deliveryId)));
    assert.ok(packet);
    const agent = this.store.list().find((item) => item.id === this.delivery(deliveryId).recipientAgentId);
    assert.ok(agent);
    if (this.database.activeProviderSession(packet.threadId, agent.provider)?.externalSessionId !== this.sessionId)
      this.database.bindProviderSession({
        threadId: packet.threadId,
        provider: agent.provider,
        externalSessionId: this.sessionId,
        model: agent.model,
        effort: "medium",
      });
    await runCauseEffect(this.mailbox.markRunning(deliveryId, turnId));
    await runCauseEffect(this.channels.accepted(deliveryId, this.sessionId, turnId));
    return packet;
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

const root = await mkdtemp(join(tmpdir(), "openbot-event-wait-delta-"));
const out = resolve(import.meta.dirname, "../.openbot-build/channel-event-wait-delta-e2e");
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

async function wakeContinuation(harness: Harness, named: boolean) {
  const compass = await harness.agent("Compass");
  const results = harness.inbox(compass.id, "Pod results (Compass)");
  const wait = await harness.open(compass.id, results.id);
  await harness.receive(compass.id, results.id, "evt-delta-42", named ? wait.waitId : undefined);
  await harness.until("event continuation", () => harness.channels.store.assignments(wait.channelId).length === 2);
  const continuation = harness.channels.store.assignments(wait.channelId).at(-1);
  assert.ok(continuation?.deliveryId);
  await runCauseEffect(harness.mailbox.markStarting(continuation.deliveryId));
  const packet = await runCauseEffect(harness.channels.prepare(harness.context(continuation.deliveryId)));
  assert.ok(packet);
  return { wait, deliveryId: continuation.deliveryId, packet };
}

try {
  await check("A session that already read the channel gets the event in its delta packet.", async (harness) => {
    const { wait, deliveryId } = await wakeContinuation(harness, true);
    const delta = harness.channels.delta(deliveryId);
    assert.ok(delta, "the continuation goes to the session that read the channel, as a delta");
    assert.equal(delta.sessionId, harness.sessionId);
    assert.match(delta.text, /evt-delta-42/, "the delta packet must carry the event source");
    assert.match(delta.text, new RegExp(wait.waitId));
    assert.match(delta.text, /--- event source/);
  });

  await check("An unnamed event also reaches the delta packet.", async (harness) => {
    const { deliveryId } = await wakeContinuation(harness, false);
    const delta = harness.channels.delta(deliveryId);
    assert.ok(delta);
    assert.match(delta.text, /evt-delta-42/);
  });

  await check("The complete packet still carries the event.", async (harness) => {
    const { packet, wait } = await wakeContinuation(harness, true);
    assert.match(packet.text, /evt-delta-42/);
    assert.match(packet.text, new RegExp(wait.waitId));
  });

  await check("A continuation without an event has no event section in its delta.", async (harness) => {
    const compass = await harness.agent("Compass");
    const results = harness.inbox(compass.id, "Pod results (Compass)");
    const wait = await harness.open(compass.id, results.id, false);
    await harness.finish(wait);
    await runCauseEffect(
      harness.channels.command(
        {
          type: "send",
          operationId: randomUUID(),
          channelId: wait.channelId,
          text: "Any news on the release?",
          recipientAgentId: compass.id,
          replyToMessageId: null,
          attachmentDraftIds: [],
        },
        { id: "local", name: "Alex" },
      ),
    );
    await harness.until("reply assignment", () =>
      harness.channels.store
        .assignments(wait.channelId)
        .some((item) => item.deliveryId && item.deliveryId !== wait.deliveryId),
    );
    const next = harness.channels.store.assignments(wait.channelId).find((item) => item.deliveryId !== wait.deliveryId);
    assert.ok(next?.deliveryId);
    await runCauseEffect(harness.mailbox.markStarting(next.deliveryId));
    await runCauseEffect(harness.channels.prepare(harness.context(next.deliveryId)));
    const delta = harness.channels.delta(next.deliveryId);
    if (delta) assert.doesNotMatch(delta.text, /--- event source/);
  });
  report.passed = report.failed.length === 0;
} finally {
  await rm(root, { recursive: true, force: true });
  mkdirSync(out, { recursive: true });
  writeFileSync(join(out, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}
process.stdout.write(`${report.checks.length} of ${report.checks.length + report.failed.length} checks pass\n`);
if (!report.passed) process.exitCode = 1;
