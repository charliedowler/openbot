import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { ConversationRuntime } from "../src/backend/agent/conversation-runtime";
import { deliveryPromptInput } from "../src/backend/agent/delivery-content";
import { RoutineScheduler } from "../src/backend/agent/routine-scheduler";
import { AgentStore } from "../src/backend/agent-store";
import { AgentWaitError, AgentWaitStore } from "../src/backend/agent-waits/agent-wait-store";
import { AgentWaits } from "../src/backend/agent-waits/agent-waits";
import { channelFailure } from "../src/backend/channel-effects";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { RoutineTimer } from "../src/backend/routine-timer";

interface ChannelWait {
  channelId: string;
  taskId: string;
  deliveryId: string;
  revision: number;
  requestMessageId: string;
  waitId: string;
  agentId: string;
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
  readonly listeners = new Set<() => void>();
  scheduled = 0;
  busy = false;
  busyChecks = 0;
  failResume = false;
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
      schedule: () => {
        this.scheduled++;
        this.changed();
      },
      interrupt: () => Effect.void,
      busy: () => {
        this.busyChecks++;
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
          this.failResume
            ? Effect.fail(new AgentWaitError("Injected wake interruption."))
            : this.channels
                .resumeEventWait(wait, deliveryId)
                .pipe(Effect.mapError((failure) => new AgentWaitError(String(failure.cause)))),
        channelWaitMatches: (wait, assignment) => this.channels.channelWaitMatches(wait, assignment),
        ownsRoutine: () => true,
        hasRoutine: () => true,
        requireKnownAgent: () => {},
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
        description: "Checks existing authorised work.",
        avatarSeed: name.toLowerCase().replaceAll(" ", "-"),
        avatarHue: null,
      }),
    );
  }

  call(
    wait: ChannelWait,
    tool: string,
    args: { note?: string; waitId?: string; routineId?: string },
    threadId = wait.threadId,
  ) {
    return this.waits.handleTool(
      { threadId, turnId: wait.turnId, callId: randomUUID(), namespace: "openbot", tool, arguments: args },
      { agentId: wait.agentId, inExecutionThread: true, threadId },
    );
  }

  async open(agentId: string, routineId?: string): Promise<ChannelWait> {
    const channelId = randomUUID();
    const actor = { id: "local", name: "Alex" };
    await runCauseEffect(
      this.channels.command(
        {
          type: "save",
          operationId: randomUUID(),
          channelId,
          draft: {
            name: "Release",
            title: "",
            instructions: "Check the requested release.",
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
          text: "Check the already requested release when its event arrives.",
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
    assert.ok(assignment?.deliveryId && assignment.requestMessageId);
    const wait: ChannelWait = {
      channelId,
      taskId: assignment.taskId,
      deliveryId: assignment.deliveryId,
      revision: assignment.taskRevision,
      requestMessageId: assignment.requestMessageId,
      agentId,
      waitId: "",
      turnId: randomUUID(),
      threadId: this.channels.store.context(channelId, agentId).threadId,
    };
    await runCauseEffect(this.mailbox.markStarting(wait.deliveryId));
    await runCauseEffect(this.mailbox.markRunning(wait.deliveryId, wait.turnId));
    await runCauseEffect(this.channels.accepted(wait.deliveryId, randomUUID(), wait.turnId));
    const result = this.call(wait, "await_event", {
      note: "The existing release event.",
      ...(routineId ? { routineId } : {}),
    });
    assert.equal(result?.success, true, JSON.stringify(result));
    const stored = this.waitStore.list(agentId).find((item) => item.requestMessageId === wait.deliveryId);
    assert.ok(stored);
    wait.waitId = stored.id;
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
  }

  async event(wait: ChannelWait) {
    const receipt = await runCauseEffect(
      this.mailbox.enqueue({
        sender: {
          kind: "routine",
          routineId: "existing-event-inbox",
          runId: randomUUID(),
          routineName: "Existing event inbox",
          scheduledFor: new Date().toISOString(),
        },
        recipientAgentIds: [wait.agentId],
        text: "Verified event reference: existing-ci-run-42; read its current source before acting.",
        draftIds: [],
      }),
    );
    const delivery = receipt.deliveries[0];
    assert.ok(delivery);
    return delivery.id;
  }

  async wake(wait: ChannelWait, eventDeliveryId: string) {
    await runCauseEffect(
      this.waits
        .wakeForRun({ agentId: wait.agentId, routineId: "existing-event-inbox" }, eventDeliveryId, wait.waitId)
        .pipe(Effect.mapError(channelFailure)),
    );
  }

  async close() {
    if (this.closed) return;
    await runCauseEffect(this.channels.stop());
    this.database.close();
    this.closed = true;
  }
}

const root = await mkdtemp(join(tmpdir(), "openbot-channel-waits-"));
const out = resolve(import.meta.dirname, "../.openbot-build/channel-event-waits-e2e");
const report: { passed: boolean; checks: string[]; error: string } = { passed: false, checks: [], error: "" };
let harness = new Harness(root);
const pass = (name: string) => report.checks.push(name);
try {
  await harness.initialize();
  const worker = await harness.agent("Worker");
  const other = await harness.agent("Other");
  const wait = await harness.open(worker.id);
  assert.equal(harness.call(wait, "await_event", { note: "Wrong thread" }, "unrelated-thread")?.success, false);
  assert.equal(harness.call(wait, "await_event", { note: "The same request again." })?.success, true);
  assert.equal(harness.waitStore.list(worker.id).length, 1);
  pass("A real channel assignment opens one wait; repeated calls reuse it and an unrelated thread cannot open it.");
  await harness.finish(wait);
  assert.equal(harness.channels.store.tasks(wait.channelId)[0]?.state, "waiting");
  await harness.close();
  harness = new Harness(root);
  await harness.initialize();
  const restored = harness.waitStore.get(worker.id, wait.waitId);
  assert.ok(restored);
  const origin = harness.channels.store.assignmentForDelivery(restored.requestMessageId);
  assert.equal(origin?.taskId, wait.taskId);
  assert.equal(origin?.taskRevision, wait.revision);
  assert.equal(origin?.requestMessageId, wait.requestMessageId);
  pass("The channel, task, revision and request survive reopening the real SQLite and mailbox stores.");
  const eventId = await harness.event(wait);
  await harness.wake(wait, eventId);
  await harness.until(
    "event channel assignment",
    () => harness.channels.store.assignments(wait.channelId).length === 2,
  );
  const resumed = harness.channels.store.assignments(wait.channelId).at(-1);
  assert.ok(resumed?.deliveryId);
  assert.equal(resumed.taskId, wait.taskId);
  assert.equal(resumed.agentId, worker.id);
  assert.equal(resumed.taskRevision, wait.revision);
  assert.equal(resumed.requestMessageId, wait.requestMessageId);
  assert.equal(harness.channels.store.tasks(wait.channelId).length, 1);
  assert.equal(harness.mailbox.getDelivery(resumed.deliveryId)?.channelId, wait.channelId);
  assert.deepEqual(harness.waits.wokenBy(eventId), []);
  assert.equal(harness.waits.hasForwardedChannelWaits(eventId), true);
  const routineContext = harness.mailbox.getDelivery(eventId);
  assert.ok(routineContext);
  const routinePrompt = deliveryPromptInput(routineContext, {
    agentNames: new Map([[worker.id, worker.name]]),
    snapshot: harness.database.readConversation(worker.id, worker.threadId),
    routineRun: null,
    wokenWaits: harness.waits.wokenBy(eventId),
    forwardedChannelWaits: harness.waits.hasForwardedChannelWaits(eventId),
  });
  const routineText = routinePrompt.flatMap((item) => (item.type === "text" ? [item.text] : [])).join("\n");
  assert.match(routineText, /Their requests continue in their original channel assignments/);
  assert.doesNotMatch(routineText, /Check the already requested release/);
  const resumedContext = harness.mailbox.getDelivery(resumed.deliveryId);
  assert.ok(resumedContext);
  const prepared = await runCauseEffect(harness.channels.prepare(resumedContext));
  assert.equal(prepared?.threadId, wait.threadId);
  assert.match(prepared?.text ?? "", /existing-ci-run-42/);
  assert.match(prepared?.text ?? "", /Check the already requested release/);
  pass("An event continues only the same existing task and channel; the routine main chat gets no channel request.");
  await harness.wake(wait, eventId);
  assert.equal(harness.channels.store.assignments(wait.channelId).length, 2);
  pass("Duplicate pending event delivery creates no extra assignment.");
  const resumedWait = { ...wait, deliveryId: resumed.deliveryId, turnId: randomUUID() };
  await runCauseEffect(harness.mailbox.markStarting(resumedWait.deliveryId));
  await runCauseEffect(harness.mailbox.markRunning(resumedWait.deliveryId, resumedWait.turnId));
  await runCauseEffect(harness.channels.accepted(resumedWait.deliveryId, randomUUID(), resumedWait.turnId));
  assert.equal(harness.call(resumedWait, "close_wait", { waitId: wait.waitId })?.success, true);
  await harness.finish(resumedWait);
  assert.equal(harness.channels.store.tasks(wait.channelId)[0]?.state, "completed");
  assert.equal(harness.waits.hasForwardedChannelWaits(eventId), true);
  await harness.wake(wait, eventId);
  assert.equal(harness.channels.store.assignments(wait.channelId).length, 2);
  pass("Closing the wait completes the existing task; a later duplicate does not resurrect it.");

  const repeated = await harness.open(worker.id);
  await harness.finish(repeated);
  const repeatedEvent = await harness.event(repeated);
  await harness.wake(repeated, repeatedEvent);
  await harness.until(
    "first repeated-event continuation",
    () => harness.channels.store.assignments(repeated.channelId).length === 2,
  );
  const repeatedAssignment = harness.channels.store.assignments(repeated.channelId).at(-1);
  assert.ok(repeatedAssignment?.deliveryId);
  const repeatedTurn = { ...repeated, deliveryId: repeatedAssignment.deliveryId, turnId: randomUUID() };
  await runCauseEffect(harness.mailbox.markStarting(repeatedTurn.deliveryId));
  await runCauseEffect(harness.mailbox.markRunning(repeatedTurn.deliveryId, repeatedTurn.turnId));
  await runCauseEffect(harness.channels.accepted(repeatedTurn.deliveryId, randomUUID(), repeatedTurn.turnId));
  await harness.finish(repeatedTurn);
  assert.equal(harness.channels.store.tasks(repeated.channelId)[0]?.state, "waiting");
  await harness.wake(repeated, repeatedEvent);
  assert.equal(harness.channels.store.assignments(repeated.channelId).length, 2);
  assert.equal(harness.waitStore.get(worker.id, repeated.waitId)?.wakeDeliveryId, repeatedTurn.deliveryId);
  const followingEvent = await harness.event(repeated);
  await harness.wake(repeated, followingEvent);
  await harness.until(
    "next distinct event continuation",
    () => harness.channels.store.assignments(repeated.channelId).length === 3,
  );
  const followingAssignment = harness.channels.store.assignments(repeated.channelId).at(-1);
  assert.ok(followingAssignment?.deliveryId);
  const followingTurn = { ...repeated, deliveryId: followingAssignment.deliveryId, turnId: randomUUID() };
  await runCauseEffect(harness.mailbox.markStarting(followingTurn.deliveryId));
  await runCauseEffect(harness.mailbox.markRunning(followingTurn.deliveryId, followingTurn.turnId));
  await runCauseEffect(harness.channels.accepted(followingTurn.deliveryId, randomUUID(), followingTurn.turnId));
  assert.equal(harness.call(followingTurn, "close_wait", { waitId: repeated.waitId })?.success, true);
  await harness.finish(followingTurn);
  pass("A replay after a waiting continuation ends creates no turn and does not block the next distinct event.");

  const early = await harness.open(worker.id);
  const earlyEvent = await harness.event(early);
  await harness.wake(early, earlyEvent);
  assert.equal(harness.channels.store.assignments(early.channelId).length, 1);
  await harness.finish(early);
  await harness.until(
    "early event continuation",
    () => harness.channels.store.assignments(early.channelId).length === 2,
  );
  pass("An event received before the waiting turn ends resumes after that turn releases its assignment.");
  const earlyResumed = harness.channels.store.assignments(early.channelId).at(-1);
  assert.ok(earlyResumed?.deliveryId);
  const earlyContinuation = { ...early, deliveryId: earlyResumed.deliveryId, turnId: randomUUID() };
  await runCauseEffect(harness.mailbox.markStarting(earlyContinuation.deliveryId));
  await runCauseEffect(harness.mailbox.markRunning(earlyContinuation.deliveryId, earlyContinuation.turnId));
  await runCauseEffect(harness.channels.accepted(earlyContinuation.deliveryId, randomUUID(), earlyContinuation.turnId));
  harness.waits.cancel(worker.id, early.waitId);
  await harness.finish(earlyContinuation);

  for (const changed of [
    "cancelled",
    "completed",
    "paused",
    "revision",
    "request",
    "owner",
    "membership",
    "archived",
    "deleted",
  ] as const) {
    const stale = await harness.open(worker.id);
    await harness.finish(stale);
    const task = harness.channels.store.tasks(stale.channelId)[0];
    assert.ok(task);
    const channel = harness.channels.store.get(stale.channelId);
    if (changed === "deleted") await runCauseEffect(harness.channels.deleteChannel(stale.channelId));
    else if (changed === "archived") harness.channels.store.update({ ...channel, archived: true });
    else if (changed === "membership")
      harness.channels.store.update({ ...channel, members: [{ agentId: other.id }], leadAgentId: other.id });
    else
      harness.channels.store.update(channel, {
        tasks: [
          {
            ...task,
            ...(changed === "owner"
              ? { ownerAgentId: other.id }
              : changed === "revision"
                ? { revision: task.revision + 1 }
                : changed === "request"
                  ? { requestMessageId: randomUUID() }
                  : { state: changed }),
          },
        ],
      });
    const staleEvent = await harness.event(stale);
    const starts = harness.scheduled;
    await harness.wake(stale, staleEvent);
    assert.equal(harness.scheduled, starts);
    assert.equal(harness.waitStore.get(worker.id, stale.waitId), null);
    assert.deepEqual(harness.waits.wokenBy(staleEvent), []);
    if (changed !== "deleted") assert.equal(harness.channels.store.assignments(stale.channelId).length, 1);
    pass(`A ${changed} channel task never resumes from an old event and never falls back to the main chat.`);
  }
  const ownReceipt = await runCauseEffect(
    harness.mailbox.enqueue({
      sender: { kind: "user" },
      recipientAgentIds: [worker.id],
      text: "Wait in your own chat.",
      draftIds: [],
    }),
  );
  const ownDelivery = ownReceipt.deliveries[0];
  assert.ok(ownDelivery);
  const ownTurn = randomUUID();
  await runCauseEffect(harness.mailbox.markStarting(ownDelivery.id));
  await runCauseEffect(harness.mailbox.markRunning(ownDelivery.id, ownTurn));
  const ownParams = {
    threadId: "main-thread",
    turnId: ownTurn,
    callId: randomUUID(),
    namespace: "openbot",
    tool: "await_event",
    arguments: { note: "The existing routine event." },
  };
  assert.equal(
    harness.waits.handleTool(ownParams, { agentId: worker.id, inExecutionThread: true, threadId: "messaging-thread" })
      ?.success,
    false,
  );
  assert.equal(harness.waits.handleTool(ownParams, { agentId: worker.id, inExecutionThread: false })?.success, true);
  const ownWait = harness.waitStore.list(worker.id)[0];
  assert.ok(ownWait && !ownWait.id.startsWith("channel:"));
  const ownEvent = await harness.event({ ...wait, waitId: ownWait.id });
  await runCauseEffect(
    harness.waits
      .wakeForRun({ agentId: worker.id, routineId: "existing-event-inbox" }, ownEvent, ownWait.id)
      .pipe(Effect.mapError(channelFailure)),
  );
  assert.equal(harness.waits.wokenBy(ownEvent)[0]?.id, ownWait.id);
  pass(
    "Own-chat waits retain their IDs and wake behavior; messaging and unverified execution contexts remain refused.",
  );
  const cancelledWait = await harness.open(worker.id);
  await harness.finish(cancelledWait);
  const cancelledEvent = await harness.event(cancelledWait);
  await harness.wake(cancelledWait, cancelledEvent);
  await harness.until(
    "queued wake before wait cancellation",
    () => harness.channels.store.assignments(cancelledWait.channelId).length === 2,
  );
  const cancelledAssignment = harness.channels.store.assignments(cancelledWait.channelId).at(-1);
  assert.ok(cancelledAssignment?.deliveryId);
  harness.waits.cancel(worker.id, cancelledWait.waitId);
  assert.equal(harness.channels.store.tasks(cancelledWait.channelId)[0]?.state, "paused");
  const cancelledContext = harness.mailbox.getDelivery(cancelledAssignment.deliveryId);
  assert.ok(cancelledContext);
  await assert.rejects(() => runCauseEffect(harness.channels.prepare(cancelledContext)));
  pass("Cancelling a wait holds an already queued channel continuation before it can start.");
  const routines = harness.database.connection.prepare("SELECT COUNT(*) count FROM projection_agent_routines").get();
  assert.equal(routines?.count, 0);
  pass("No recurring routine or new event source is created.");
  const resourceHarness = new Harness(join(root, "resource-gate"));
  try {
    await resourceHarness.initialize();
    const gatedWorker = await resourceHarness.agent("Gated worker");
    const gated = await resourceHarness.open(gatedWorker.id);
    await resourceHarness.finish(gated);
    resourceHarness.busy = true;
    const gatedEvent = await resourceHarness.event(gated);
    const gateReads = resourceHarness.busyChecks;
    await resourceHarness.wake(gated, gatedEvent);
    await resourceHarness.until("existing busy-agent gate read", () => resourceHarness.busyChecks > gateReads);
    await runCauseEffect(resourceHarness.channels.stop());
    assert.equal(resourceHarness.channels.store.tasks(gated.channelId)[0]?.state, "queued");
    assert.equal(resourceHarness.channels.store.assignments(gated.channelId).length, 1);
    pass("Event continuation uses the existing channel pump and preserves its busy-agent gate.");
  } finally {
    await resourceHarness.close();
  }
  const pendingRoot = join(root, "pending-wake-recovery");
  let pendingHarness = new Harness(pendingRoot);
  try {
    await pendingHarness.initialize();
    const pendingWorker = await pendingHarness.agent("Recovery worker");
    const pending = await pendingHarness.open(pendingWorker.id);
    await pendingHarness.finish(pending);
    const pendingEvent = await pendingHarness.event(pending);
    pendingHarness.failResume = true;
    await assert.rejects(() => pendingHarness.wake(pending, pendingEvent), /Injected wake interruption/);
    assert.equal(pendingHarness.waitStore.get(pending.agentId, pending.waitId)?.wakeDeliveryId, pendingEvent);
    await pendingHarness.close();
    pendingHarness = new Harness(pendingRoot);
    await pendingHarness.initialize();
    await runCauseEffect(pendingHarness.waits.recoverChannelWaits().pipe(Effect.mapError(channelFailure)));
    await pendingHarness.until(
      "interrupted event wake recovery",
      () => pendingHarness.channels.store.assignments(pending.channelId).length === 2,
    );
    assert.equal(pendingHarness.channels.store.tasks(pending.channelId)[0]?.id, pending.taskId);
    pass("A wake interrupted after its event link is stored resumes the same task after reopening the real stores.");
  } finally {
    await pendingHarness.close();
  }
  const parentHarness = new Harness(join(root, "parent-wait"));
  try {
    await parentHarness.initialize();
    const lead = await parentHarness.agent("Parent owner");
    const childOwner = await parentHarness.agent("Child owner");
    const parent = await parentHarness.open(lead.id);
    assert.equal(parentHarness.call(parent, "close_wait", { waitId: parent.waitId })?.success, true);
    const parentChannel = parentHarness.channels.store.get(parent.channelId);
    parentHarness.channels.store.update({
      ...parentChannel,
      members: [...parentChannel.members, { agentId: childOwner.id }],
    });
    await runCauseEffect(
      parentHarness.channels.tool(parent.channelId, lead.id, parent.turnId, randomUUID(), "channel_assign", {
        recipientAgentId: childOwner.id,
        task: "Check the already requested dependency.",
        expectedResult: "Return the existing dependency result.",
        sourceMessageIds: [parent.requestMessageId],
      }),
    );
    await parentHarness.finish(parent);
    await parentHarness.until("child dependency assignment", () =>
      parentHarness.channels.store
        .assignments(parent.channelId)
        .some((item) => item.agentId === childOwner.id && item.deliveryId !== null),
    );
    const childAssignment = parentHarness.channels.store
      .assignments(parent.channelId)
      .find((item) => item.agentId === childOwner.id);
    assert.ok(childAssignment?.deliveryId && childAssignment.requestMessageId);
    const child: ChannelWait = {
      channelId: parent.channelId,
      taskId: childAssignment.taskId,
      deliveryId: childAssignment.deliveryId,
      revision: childAssignment.taskRevision,
      requestMessageId: childAssignment.requestMessageId,
      agentId: childOwner.id,
      turnId: randomUUID(),
      threadId: parentHarness.channels.store.context(parent.channelId, childOwner.id).threadId,
      waitId: "",
    };
    await runCauseEffect(parentHarness.mailbox.markStarting(child.deliveryId));
    await runCauseEffect(parentHarness.mailbox.markRunning(child.deliveryId, child.turnId));
    await runCauseEffect(parentHarness.channels.accepted(child.deliveryId, randomUUID(), child.turnId));
    assert.equal(parentHarness.call(child, "await_event", { note: "The existing dependency event." })?.success, true);
    await parentHarness.finish(child);
    assert.equal(
      parentHarness.channels.store.tasks(parent.channelId).find((task) => task.id === child.taskId)?.state,
      "waiting",
    );
    assert.equal(
      parentHarness.channels.store.tasks(parent.channelId).find((task) => task.id === parent.taskId)?.state,
      "waiting",
    );
    pass("A child that opens an event wait does not report dependency completion or resume its parent.");
  } finally {
    await parentHarness.close();
  }
  const webhookHarness = new Harness(join(root, "native-webhook"));
  const conversation = new ConversationRuntime(
    webhookHarness.store,
    () => {},
    () => webhookHarness.store.list(),
  );
  const routineTimer = new RoutineTimer(
    () => [],
    () => false,
    () => {},
  );
  const scheduler = new RoutineScheduler({
    store: webhookHarness.store,
    mailbox: webhookHarness.mailbox,
    conversation,
    waits: webhookHarness.waits,
    timer: routineTimer,
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
      listAgents: () => webhookHarness.store.list(),
      excludedAgents: () => new Set(),
      isRunning: () => true,
      usageLimited: () => false,
    },
  });
  try {
    await webhookHarness.initialize();
    const eventWorker = await webhookHarness.agent("Event worker");
    const createInbox = (name: string) =>
      scheduler.saveRecord(eventWorker.id, undefined, {
        name,
        instruction: "Read this Event Bridge delivery and acknowledge its exact event ID.",
        active: true,
        timezone: "UTC",
        trigger: { kind: "webhook", eventType: null, filters: [], secretCiphertext: "fixture-ciphertext" },
      });
    const selectedInbox = createInbox("Selected existing inbox");
    const otherInbox = createInbox("Other existing inbox");
    const selectedWait = await webhookHarness.open(eventWorker.id, selectedInbox.id);
    await webhookHarness.finish(selectedWait);
    const nativeEvent = {
      deliveryId: "eventbridge-delivery-one",
      eventType: "event-bridge.task.changed",
      data: {
        nativeEventBridge: true,
        eventId: "eventbridge-event-one",
        watchId: "a-different-watch",
        targetAgentId: eventWorker.id,
        sourceEvent: { threadId: "mcp:existing-work", runOrdinal: 1, status: "completed" },
      },
      occurredAt: "2026-10-29T20:00:00Z",
      receivedAt: "2026-10-29T20:00:01Z",
    };
    const otherRun = await runCauseEffect(scheduler.receiveWebhook(eventWorker.id, otherInbox.id, nativeEvent));
    assert.equal(otherRun.kind, "started");
    assert.equal(webhookHarness.channels.store.assignments(selectedWait.channelId).length, 1);
    const selectedRun = await runCauseEffect(scheduler.receiveWebhook(eventWorker.id, selectedInbox.id, nativeEvent));
    assert.equal(selectedRun.kind, "started");
    await webhookHarness.until(
      "native webhook channel continuation",
      () => webhookHarness.channels.store.assignments(selectedWait.channelId).length === 2,
    );
    const accepted = webhookHarness.channels.store.assignments(selectedWait.channelId).at(-1);
    assert.ok(accepted?.deliveryId);
    assert.match(webhookHarness.mailbox.getDelivery(accepted.deliveryId)?.delivery.text ?? "", /a-different-watch/);
    const replay = await runCauseEffect(scheduler.receiveWebhook(eventWorker.id, selectedInbox.id, nativeEvent));
    assert.equal(replay.kind, "duplicate");
    assert.equal(webhookHarness.channels.store.assignments(selectedWait.channelId).length, 2);
    pass(
      "A real native webhook envelope with watchId and no waitId respects routineId; watchId does not filter waits, and exact delivery replay creates no extra assignment.",
    );
    const acceptedTurn = { ...selectedWait, deliveryId: accepted.deliveryId, turnId: randomUUID() };
    await runCauseEffect(webhookHarness.mailbox.markStarting(acceptedTurn.deliveryId));
    await runCauseEffect(webhookHarness.mailbox.markRunning(acceptedTurn.deliveryId, acceptedTurn.turnId));
    await runCauseEffect(webhookHarness.channels.accepted(acceptedTurn.deliveryId, randomUUID(), acceptedTurn.turnId));
    assert.equal(webhookHarness.call(acceptedTurn, "close_wait", { waitId: selectedWait.waitId })?.success, true);
    await webhookHarness.finish(acceptedTurn);
    const unfiltered = await webhookHarness.open(eventWorker.id);
    await webhookHarness.finish(unfiltered);
    await runCauseEffect(
      scheduler.receiveWebhook(eventWorker.id, otherInbox.id, {
        ...nativeEvent,
        deliveryId: "eventbridge-delivery-two",
      }),
    );
    await webhookHarness.until(
      "unfiltered existing-inbox continuation",
      () => webhookHarness.channels.store.assignments(unfiltered.channelId).length === 2,
    );
    pass(
      "With routineId omitted, a delivery from any existing owner inbox wakes the channel wait, as with own-chat waits.",
    );
  } finally {
    conversation.dispose();
    await webhookHarness.close();
  }
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? error.message : String(error);
  throw error;
} finally {
  await harness.close();
  await rm(root, { recursive: true, force: true });
  mkdirSync(out, { recursive: true });
  writeFileSync(join(out, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}
