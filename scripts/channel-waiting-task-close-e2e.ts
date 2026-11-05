import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentSummary, ChannelTask } from "@openbot/contracts/ipc";
import { Effect, Result } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { AgentWaitError, AgentWaitStore } from "../src/backend/agent-waits/agent-wait-store";
import { AgentWaits } from "../src/backend/agent-waits/agent-waits";
import { ChannelToolInputError, channelFailure } from "../src/backend/channel-effects";
import { ChannelImages } from "../src/backend/channel-images";
import { ChannelService } from "../src/backend/channel-service";
import { CHANNEL_TOOL_DEFINITIONS } from "../src/backend/channel-tools";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

interface Turn {
  channelId: string;
  taskId: string;
  deliveryId: string;
  agentId: string;
  turnId: string;
  threadId: string;
  waitId: string;
}

class Harness {
  readonly database: OpenBotDatabase;
  readonly store: AgentStore;
  readonly mailbox: MailboxStore;
  readonly waitStore: AgentWaitStore;
  readonly channels: ChannelService;
  readonly waits: AgentWaits;
  readonly listeners = new Set<() => void>();
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
      busy: () => false,
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
        closedChannelWait: (wait) => this.channels.eventWaitClosed(wait),
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

  agent(name: string): Promise<AgentSummary> {
    return runCauseEffect(
      this.store.createAgent({ name, description: name, avatarSeed: name.toLowerCase(), avatarHue: null }),
    );
  }

  async channel(lead: AgentSummary, members: AgentSummary[]): Promise<string> {
    const channelId = randomUUID();
    await runCauseEffect(
      this.channels.command(
        {
          type: "save",
          operationId: randomUUID(),
          channelId,
          draft: {
            name: "Dispatch",
            title: "",
            instructions: "Route the requested work.",
            members: [lead, ...members].map((member) => ({ agentId: member.id })),
            leadAgentId: lead.id,
          },
        },
        { id: "local", name: "Alex" },
      ),
    );
    return channelId;
  }

  async waitingTask(channelId: string, agent: AgentSummary): Promise<Turn> {
    const before = this.channels.store.assignments(channelId).filter((item) => item.agentId === agent.id).length;
    await runCauseEffect(
      this.channels.command(
        {
          type: "send",
          operationId: randomUUID(),
          channelId,
          text: "Follow the release until its event arrives.",
          recipientAgentId: agent.id,
          replyToMessageId: null,
          attachmentDraftIds: [],
        },
        { id: "local", name: "Alex" },
      ),
    );
    const mine = () => this.channels.store.assignments(channelId).filter((item) => item.agentId === agent.id);
    await this.until("channel assignment delivery", () => mine().length > before && Boolean(mine().at(-1)?.deliveryId));
    const assignment = mine().at(-1);
    assert.ok(assignment?.deliveryId);
    const turn: Turn = {
      channelId,
      taskId: assignment.taskId,
      deliveryId: assignment.deliveryId,
      agentId: agent.id,
      turnId: randomUUID(),
      threadId: this.channels.store.context(channelId, agent.id).threadId,
      waitId: "",
    };
    await runCauseEffect(this.mailbox.markStarting(turn.deliveryId));
    await runCauseEffect(this.mailbox.markRunning(turn.deliveryId, turn.turnId));
    await runCauseEffect(this.channels.accepted(turn.deliveryId, randomUUID(), turn.turnId));
    const opened = this.waits.handleTool(
      {
        threadId: turn.threadId,
        turnId: turn.turnId,
        callId: randomUUID(),
        namespace: "openbot",
        tool: "await_event",
        arguments: { note: "The release event." },
      },
      { agentId: agent.id, inExecutionThread: true, threadId: turn.threadId },
    );
    assert.equal(opened?.success, true, JSON.stringify(opened));
    const stored = this.waitStore.list(agent.id).find((item) => item.requestMessageId === turn.deliveryId);
    assert.ok(stored);
    turn.waitId = stored.id;
    await runCauseEffect(this.mailbox.markTerminal(turn.deliveryId, "completed"));
    this.channels.event({
      type: "turn-completed",
      agentId: agent.id,
      threadId: turn.threadId,
      turnId: turn.turnId,
      status: "completed",
    });
    await this.until("the task waits", () => this.task(channelId, turn.taskId).state === "waiting");
    return turn;
  }

  closeWait(agentId: string, waitId: string) {
    return this.waits.handleTool(
      {
        threadId: "another-task-thread",
        turnId: randomUUID(),
        callId: randomUUID(),
        namespace: "openbot",
        tool: "close_wait",
        arguments: { waitId },
      },
      { agentId, inExecutionThread: true, threadId: "another-task-thread" },
    );
  }

  task(channelId: string, taskId: string): ChannelTask {
    const task = this.channels.store.task(channelId, taskId);
    assert.ok(task);
    return task;
  }

  running(channelId: string, owner: AgentSummary, options: Partial<ChannelTask> = {}) {
    const id = options.id ?? randomUUID();
    const requestMessageId = randomUUID();
    const task: ChannelTask = {
      id,
      channelId,
      parentTaskId: null,
      rootTaskId: id,
      ownerAgentId: owner.id,
      requestMessageId,
      instruction: "Coordinate the requested work.",
      attachmentDraftIds: [],
      expectedResult: "Report the result.",
      sourceMessageIds: [requestMessageId],
      dependencies: [],
      resources: [],
      state: "running",
      revision: 1,
      assignmentCount: 1,
      error: null,
      ...options,
    };
    const turnId = randomUUID();
    this.channels.store.update(this.channels.store.get(channelId), {
      tasks: [task],
      assignments: [
        {
          id: randomUUID(),
          channelId,
          taskId: task.id,
          agentId: owner.id,
          taskRevision: task.revision,
          requestMessageId: task.requestMessageId,
          resources: [],
          deliveryId: null,
          turnId,
          state: task.state === "running" ? "running" : "completed",
          throughSequence: 0,
          summaryVersion: 0,
          awaitedTaskIds: [],
          pendingRevision: null,
          pendingOutcome: null,
        },
      ],
    });
    return { agentId: owner.id, turnId, taskId: task.id, task };
  }

  async tool(turn: { agentId: string; turnId: string }, channelId: string, tool: string, args: unknown) {
    const result = await runCauseEffect(
      Effect.result(this.channels.tool(channelId, turn.agentId, turn.turnId, randomUUID(), tool, args)),
    );
    if (Result.isFailure(result)) {
      const cause = result.failure.cause;
      assert.ok(cause instanceof ChannelToolInputError, `Unexpected failure: ${String(cause)}`);
      return { refused: cause.message };
    }
    return { value: result.success };
  }

  async close() {
    if (this.closed) return;
    await runCauseEffect(this.channels.stop());
    this.database.close();
    this.closed = true;
  }
}

const root = await mkdtemp(join(tmpdir(), "openbot-channel-waiting-close-"));
const out = resolve(import.meta.dirname, "../.openbot-build/channel-waiting-task-close-e2e");
const report: { passed: boolean; checks: string[]; failures: string[]; error: string } = {
  passed: false,
  checks: [],
  failures: [],
  error: "",
};
const check = (passed: boolean, label: string) => (passed ? report.checks : report.failures).push(label);
let harness = new Harness(root);
try {
  await harness.initialize();
  const lead = await harness.agent("Compass");
  const owner = await harness.agent("Monty");
  const worker = await harness.agent("Worker");
  const channelId = await harness.channel(lead, [owner, worker]);

  const description = CHANNEL_TOOL_DEFINITIONS.find((item) => item.name === "channel_close_task")?.description ?? "";
  check(
    /waiting task that has no open wait/.test(description) && /reads it as cancelled/.test(description),
    "The channel_close_task description names waiting tasks with no open wait and the dependent's view.",
  );

  const closedElsewhere = await harness.waitingTask(channelId, worker);
  const response = harness.closeWait(worker.id, closedElsewhere.waitId);
  assert.equal(response?.success, true, JSON.stringify(response));
  const afterClose = harness.task(channelId, closedElsewhere.taskId);
  check(
    afterClose.state === "paused" && afterClose.error !== null,
    `A waiting task whose wait another task closed stops with a reason (state ${afterClose.state}).`,
  );

  const openWait = await harness.waitingTask(channelId, worker);
  const lost = await harness.waitingTask(channelId, worker);
  assert.ok(harness.waitStore.delete(worker.id, lost.waitId));
  await harness.close();
  harness = new Harness(root);
  await harness.initialize();
  await runCauseEffect(harness.channels.recover());
  check(
    harness.task(channelId, lost.taskId).state === "paused",
    `The restart recovery stops a waiting task that has lost its wait (state ${harness.task(channelId, lost.taskId).state}).`,
  );
  check(
    harness.task(channelId, openWait.taskId).state === "waiting",
    "The restart recovery keeps a waiting task with an open wait.",
  );

  const leadTurn = harness.running(channelId, lead);
  const refusedOpen = await harness.tool(leadTurn, channelId, "channel_close_task", {
    taskId: openWait.taskId,
    reason: "Not needed.",
  });
  check(
    /open wait/.test(refusedOpen.refused ?? "") && harness.task(channelId, openWait.taskId).state === "waiting",
    "The lead cannot close a waiting task that has an open wait.",
  );

  const stranded = await harness.waitingTask(channelId, worker);
  assert.ok(harness.waitStore.delete(worker.id, stranded.waitId));
  const closedWaiting = await harness.tool(leadTurn, channelId, "channel_close_task", {
    taskId: stranded.taskId,
    reason: "The work was cancelled.",
  });
  check(
    harness.task(channelId, stranded.taskId).state === "cancelled",
    `The lead closes a waiting task that has no open wait (${closedWaiting.refused ?? "accepted"}).`,
  );

  const parentId = randomUUID();
  const childId = randomUUID();
  const parent = harness.running(channelId, owner, {
    id: parentId,
    state: "waiting",
    dependencies: [childId],
  }).task;
  const child: ChannelTask = {
    ...parent,
    id: childId,
    parentTaskId: parentId,
    rootTaskId: parentId,
    ownerAgentId: worker.id,
    dependencies: [],
    state: "paused",
    error: "Stopped. Continue or reassign this task.",
  };
  harness.channels.store.update(harness.channels.store.get(channelId), { tasks: [child] });
  const closedChild = await harness.tool(leadTurn, channelId, "channel_close_task", {
    taskId: childId,
    reason: "Charlie cancelled the work.",
  });
  const woken = harness.task(channelId, parentId);
  check(
    harness.task(channelId, childId).state === "cancelled" &&
      woken.state === "queued" &&
      woken.revision === parent.revision + 1,
    `Closing a dependency wakes its waiting parent (${closedChild.refused ?? `parent ${woken.state}`}).`,
  );

  const runningParentId = randomUUID();
  const runningChildId = randomUUID();
  const runningParent = harness.running(channelId, owner, { id: runningParentId, dependencies: [runningChildId] });
  harness.channels.store.update(harness.channels.store.get(channelId), {
    tasks: [{ ...child, id: runningChildId, parentTaskId: runningParentId, rootTaskId: runningParentId }],
  });
  const closedUnderRunning = await harness.tool(leadTurn, channelId, "channel_close_task", {
    taskId: runningChildId,
    reason: "Charlie cancelled the work.",
  });
  harness.channels.event({
    type: "turn-completed",
    agentId: owner.id,
    threadId: harness.channels.store.context(channelId, owner.id).threadId,
    turnId: runningParent.turnId,
    status: "completed",
  });
  await harness
    .until("the running parent ends its turn", () => harness.task(channelId, runningParentId).state !== "running")
    .catch(() => undefined);
  check(
    closedUnderRunning.value !== undefined && harness.task(channelId, runningParentId).state === "queued",
    `A parent that delegated a closed task continues after its turn instead of waiting for ever (${closedUnderRunning.refused ?? harness.task(channelId, runningParentId).state}).`,
  );

  const queuedTask = harness.running(channelId, owner, { state: "queued" });
  const closeRunning = await harness.tool(leadTurn, channelId, "channel_close_task", {
    taskId: queuedTask.taskId,
    reason: "Not needed.",
  });
  const busyParent = harness.running(channelId, owner);
  const closeBusy = await harness.tool(leadTurn, channelId, "channel_close_task", {
    taskId: busyParent.taskId,
    reason: "Not needed.",
  });
  check(
    /The task is queued/.test(closeRunning.refused ?? "") && /The task is running/.test(closeBusy.refused ?? ""),
    "The lead still cannot close a queued or running task.",
  );

  assert.equal(harness.failure, null, String(harness.failure));
  report.passed = report.failures.length === 0;
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  await harness.close().catch(() => undefined);
  mkdirSync(out, { recursive: true });
  writeFileSync(join(out, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
