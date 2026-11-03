// @vitest-environment node
// A channel lead with constant channel work held every queued routine run of its own chat: the main
// lane runs channel work and the own chat one turn at a time. The routine lane starts an own-chat
// routine run beside the channel work. These checks drive the real `DrainScheduler` and the real
// `MailboxStore` on a temporary database; only the provider, the channels, the conversation and the
// limits are stubs. A started delivery is marked starting and running in the real mailbox, so the
// lanes read the same rows as in the app.
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { QueueDelivery } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { afterEach, describe, expect, it } from "vitest";
import { runCauseEffect } from "../effect-boundary";
import { type DeliveryContext, MailboxStore } from "../mailbox-store";
import { OpenBotDatabase } from "../openbot-database";
import { DrainScheduler, type DrainSchedulerOptions } from "./drain-scheduler";

const AGENT = "agent-lead";
const TEAMMATE = "agent-helper";
const OWN_THREAD = "thread-own-chat";
const CHANNEL_THREAD = "thread-channel";
const CHANNEL = "channel-pod";

interface Start {
  deliveryId: string;
  beside: boolean;
}

interface World {
  /** An assignment of the agent is starting, running or queued: channel work holds its own chat. */
  assignmentActive: boolean;
  /** The public threads whose provider session compacts. */
  compacting: string[];
  oneTurnAtATime: boolean;
  usageLimited: boolean;
  clientCurrent: boolean;
  hidden: boolean;
  memoryLow: boolean;
}

interface Snapshot {
  agentId: string;
  threadId: string;
  activeTurnId: string | null;
}

const roots: string[] = [];
const databases: OpenBotDatabase[] = [];

afterEach(async () => {
  for (const database of databases.splice(0)) database.close();
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function realMailbox(prefix: string): Promise<MailboxStore> {
  const root = await mkdtemp(join(tmpdir(), prefix));
  roots.push(root);
  const database = new OpenBotDatabase(root);
  databases.push(database);
  const mailbox = new MailboxStore(root, root, database);
  await runCauseEffect(mailbox.initialize());
  return mailbox;
}

async function harness(change: Partial<World> = {}) {
  const mailbox = await realMailbox("openbot-routine-lane-");
  const world: World = {
    assignmentActive: false,
    compacting: [],
    oneTurnAtATime: false,
    usageLimited: false,
    clientCurrent: true,
    hidden: false,
    memoryLow: false,
    ...change,
  };
  const starts: Start[] = [];
  const channelDeliveries = new Set<string>();
  const ownChat: Snapshot = { agentId: AGENT, threadId: OWN_THREAD, activeTurnId: null };
  const channelThread: Snapshot = { agentId: AGENT, threadId: CHANNEL_THREAD, activeTurnId: null };
  const snapshotFor = (deliveryId: string) => (channelDeliveries.has(deliveryId) ? channelThread : ownChat);
  const agent: { id: string; provider: string; model: string; threadId: string; hiddenAt?: string } = {
    id: AGENT,
    provider: "codex",
    model: "gpt-6-luna",
    threadId: OWN_THREAD,
  };
  const yes = () => true;
  const options = {
    store: { list: () => [agent], database: { activeProviderSession: () => null } },
    mailbox,
    mailboxSync: { emitQueue: () => {} },
    conversation: {
      activeSnapshots: () =>
        [ownChat, channelThread]
          .filter((snapshot) => snapshot.activeTurnId)
          .map((snapshot) => [AGENT, snapshot] as const)
          .values(),
      loadedSnapshot: () => ownChat,
      workingSnapshot: () => [ownChat, channelThread].find((snapshot) => snapshot.activeTurnId),
      workingTaskSnapshots: () => [],
      isTaskThread: () => false,
      publicThreadId: (_agentId: string, threadId: string) => threadId,
    },
    providers: { isReady: yes, isReplacingCli: () => false, agentClientCurrent: () => world.clientCurrent },
    duplication: { mayDrain: yes },
    profileSave: { mayDrain: yes },
    compaction: { heldThreads: () => world.compacting },
    routines: { mayDrain: yes },
    memory: { mayDrain: () => !world.memoryLow, turnLimit: () => null, held: () => {} },
    usageLimits: {
      mayDrain: () => !world.usageLimited,
      mayTry: yes,
      retryHeld: () => false,
      tryStarted: () => {},
    },
    tasks: { threadId: () => null, openTaskIds: () => [], limit: () => 2, oneTurnAtATime: () => world.oneTurnAtATime },
    hooks: {
      isStopping: () => false,
      followUsageLimit: () => false,
      dropRequestedNewChat: () => {},
      startRequestedNewChat: () => Effect.void,
    },
    channels: {
      // As ChannelService.mayDrain: a queued channel delivery may start, other work waits for it.
      mayDrain: (agentId: string) => {
        const next = mailbox.nextQueued(agentId);
        if (next && channelDeliveries.has(next.delivery.id)) return true;
        return !world.assignmentActive;
      },
      queueHold: () =>
        world.assignmentActive
          ? { reason: "channel-task", channelId: CHANNEL, channelName: "Pod", agentId: AGENT }
          : null,
      store: {
        context: () => ({ threadId: CHANNEL_THREAD }),
        assignmentForDelivery: (deliveryId: string) =>
          channelDeliveries.has(deliveryId) ? { channelId: CHANNEL, agentId: AGENT } : null,
        channelForThread: (threadId: string) => (threadId === CHANNEL_THREAD ? CHANNEL : null),
      },
    },
    // biome-ignore lint/nursery/noUnsafeTypeAssertion: a stub of only the collaborators that the drain reads in these checks.
  } as unknown as DrainSchedulerOptions;
  const drain = new DrainScheduler(options);
  Object.defineProperty(drain, "startDelivery", {
    value: (started: DeliveryContext, _planNote?: string, beside = false) =>
      Effect.gen(function* () {
        const id = started.delivery.id;
        starts.push({ deliveryId: id, beside });
        yield* mailbox.markStarting(id, beside);
        yield* mailbox.markRunning(id, `turn-${id}`);
        snapshotFor(id).activeTurnId = `turn-${id}`;
      }).pipe(Effect.orDie),
  });

  const enqueue = async (sender: QueueDelivery["sender"], text: string, channelId?: string) => {
    const receipt = await runCauseEffect(
      mailbox.enqueue({
        sender,
        recipientAgentIds: [AGENT],
        text,
        draftIds: [],
        ...(channelId ? { channelId } : {}),
        ...(sender.kind === "agent" ? { expectsReply: false } : {}),
      }),
    );
    const id = receipt.deliveries[0]?.id;
    if (!id) throw new Error("No delivery.");
    return id;
  };
  let runs = 0;
  const settle = async () => {
    for (let round = 0; round < 6; round += 1) {
      await new Promise((done) => setTimeout(done, 0));
      await Effect.runPromise(Effect.all(drain.pendingTasks(), { discard: true }));
    }
  };
  return {
    mailbox,
    world,
    agent,
    starts,
    besideStarts: () => starts.filter((start) => start.beside).map((start) => start.deliveryId),
    started: (id: string) => starts.some((start) => start.deliveryId === id),
    startIndex: (id: string) => starts.findIndex((start) => start.deliveryId === id),
    schedule: async () => {
      drain.scheduleDrain(AGENT);
      await settle();
    },
    routine: () => {
      runs += 1;
      return enqueue(
        {
          kind: "routine",
          routineId: "routine-inbox",
          runId: `run-${runs}`,
          routineName: "Event inbox",
          scheduledFor: "2026-11-03T10:00:00.000Z",
        },
        `Read the watch event ${runs}.`,
      );
    },
    person: () => enqueue({ kind: "user" }, "Status please."),
    teammate: () => enqueue({ kind: "agent", agentId: TEAMMATE }, "The build is green."),
    channel: async () => {
      const id = await enqueue({ kind: "user" }, "Check the release.", CHANNEL);
      channelDeliveries.add(id);
      return id;
    },
    status: (id: string) => mailbox.getDelivery(id)?.delivery.status,
    finish: async (id: string) => {
      await runCauseEffect(mailbox.markTerminal(id, "completed"));
      snapshotFor(id).activeTurnId = null;
    },
  };
}

type Harness = Awaited<ReturnType<typeof harness>>;

/** A channel turn of the agent runs, as when its assignment started. */
async function channelTurnRuns(test: Harness): Promise<string> {
  test.world.assignmentActive = true;
  const channel = await test.channel();
  await test.schedule();
  expect(test.starts).toEqual([{ deliveryId: channel, beside: false }]);
  return channel;
}

describe("routine lane beside channel work", () => {
  it("starts a routine run beside a running channel turn of its agent", async () => {
    const test = await harness();
    await channelTurnRuns(test);
    const routine = await test.routine();
    await test.schedule();
    expect(test.besideStarts()).toEqual([routine]);
    expect(test.mailbox.besideActive(test.agent.id)).toBe(true);
    expect(test.status(routine)).toBe("running");
  });

  it("starts one routine run at a time beside the channel work", async () => {
    const test = await harness();
    await channelTurnRuns(test);
    const first = await test.routine();
    const second = await test.routine();
    await test.schedule();
    expect(test.besideStarts()).toEqual([first]);
    await test.finish(first);
    await test.schedule();
    expect(test.besideStarts()).toEqual([first, second]);
  });

  it("keeps the routine run in the main lane when the agent has no channel work", async () => {
    const test = await harness();
    const routine = await test.routine();
    await test.schedule();
    expect(test.starts).toEqual([{ deliveryId: routine, beside: false }]);
  });

  it("does not pass a message of the person", async () => {
    const test = await harness();
    await channelTurnRuns(test);
    await test.person();
    await test.routine();
    await test.schedule();
    expect(test.besideStarts()).toEqual([]);
  });

  it("passes a teammate's message, which keeps its place before later messages", async () => {
    const test = await harness();
    const channel = await channelTurnRuns(test);
    const teammate = await test.teammate();
    const routine = await test.routine();
    await test.schedule();
    expect(test.besideStarts()).toEqual([routine]);
    expect(test.status(teammate)).toBe("queued");
    const person = await test.person();
    await test.finish(routine);
    await test.finish(channel);
    test.world.assignmentActive = false;
    await test.schedule();
    expect(test.started(teammate)).toBe(true);
    expect(test.started(person)).toBe(false);
    await test.finish(teammate);
    await test.schedule();
    expect(test.startIndex(teammate)).toBeLessThan(test.startIndex(person));
  });

  it("does not pass a routine run that continues a plan or a turn stopped at a usage limit", async () => {
    const test = await harness();
    const plan = await test.routine();
    const limited = await test.routine();
    const later = await test.routine();
    for (const id of [plan, limited]) {
      await runCauseEffect(test.mailbox.markStarting(id));
      await runCauseEffect(test.mailbox.markRunning(id, `turn-${id}`));
    }
    await runCauseEffect(test.mailbox.requeueForPlan(plan, `turn-${plan}`));
    await runCauseEffect(
      test.mailbox.requeueRefused(limited, { turnId: `turn-${limited}`, workSteps: "Step 1", previousTurnIds: [] }),
    );
    expect(test.mailbox.nextBesideRoutine(AGENT)).toBeNull();
    await runCauseEffect(test.mailbox.cancel(AGENT, plan));
    expect(test.mailbox.nextBesideRoutine(AGENT)).toBeNull();
    await runCauseEffect(test.mailbox.cancel(AGENT, limited));
    expect(test.mailbox.nextBesideRoutine(AGENT)?.delivery.id).toBe(later);
  });

  it("keeps a routine run beside the channel work out of the main lane", async () => {
    const test = await harness();
    const routine = await test.routine();
    await runCauseEffect(test.mailbox.markStarting(routine, true));
    expect(test.mailbox.besideActive(AGENT)).toBe(true);
    expect(test.mailbox.laneActive(AGENT, null)).toBe(false);
    await runCauseEffect(test.mailbox.markRunning(routine, "turn-routine"));
    expect(test.mailbox.besideTurn(AGENT, "turn-routine")).toBe(true);
    expect(test.mailbox.mainLaneRunsNonChannelWork(AGENT)).toBe(false);
    const other = await test.routine();
    await runCauseEffect(test.mailbox.markStarting(other));
    expect(test.mailbox.laneActive(AGENT, null)).toBe(true);
    expect(test.mailbox.mainLaneRunsNonChannelWork(AGENT)).toBe(true);
  });

  it("starts the queued channel delivery while a routine run runs beside it", async () => {
    // The main lane chose the channel delivery and compacts its channel thread first. The routine
    // lane starts beside that compaction; the channel turn then starts beside the routine run.
    const test = await harness({ assignmentActive: true, compacting: [CHANNEL_THREAD] });
    const channel = await test.channel();
    const routine = await test.routine();
    await test.schedule();
    expect(test.starts).toEqual([{ deliveryId: routine, beside: true }]);
    test.world.compacting = [];
    await test.schedule();
    expect(test.starts).toEqual([
      { deliveryId: routine, beside: true },
      { deliveryId: channel, beside: false },
    ]);
  });

  it("holds a message of the person while a routine run beside the channel work runs", async () => {
    const test = await harness();
    const channel = await channelTurnRuns(test);
    const routine = await test.routine();
    await test.schedule();
    expect(test.besideStarts()).toEqual([routine]);
    await test.finish(channel);
    test.world.assignmentActive = false;
    const person = await test.person();
    await test.schedule();
    expect(test.started(person)).toBe(false);
    await test.finish(routine);
    await test.schedule();
    expect(test.started(person)).toBe(true);
  });

  it("starts a routine run beside a compaction of a channel thread with no channel work", async () => {
    const test = await harness({ compacting: [CHANNEL_THREAD] });
    const routine = await test.routine();
    await test.schedule();
    expect(test.starts).toEqual([{ deliveryId: routine, beside: true }]);
  });

  it("holds the routine run for a compaction of the own chat", async () => {
    const test = await harness({ assignmentActive: true, compacting: [OWN_THREAD] });
    await test.routine();
    await test.schedule();
    expect(test.starts).toEqual([]);
  });

  it("does not start a routine run beside a main-lane run that is still starting", async () => {
    const test = await harness();
    const first = await test.routine();
    await runCauseEffect(test.mailbox.markStarting(first));
    test.world.assignmentActive = true;
    await test.routine();
    await test.schedule();
    expect(test.starts).toEqual([]);
  });

  it.each([
    ["the provider runs one turn of the agent at a time", { oneTurnAtATime: true }],
    ["the agent is at its usage limit", { usageLimited: true }],
    ["the agent process restarts for a changed client", { clientCurrent: false }],
    ["the machine is low on memory", { memoryLow: true }],
  ] as const)("does not start a routine run beside channel work when %s", async (_label, change) => {
    const test = await harness();
    await channelTurnRuns(test);
    Object.assign(test.world, change);
    await test.routine();
    await test.schedule();
    expect(test.besideStarts()).toEqual([]);
  });

  it("does not start a routine run beside channel work when the agent is hidden", async () => {
    const test = await harness();
    await channelTurnRuns(test);
    test.agent.hiddenAt = "2026-11-03T10:00:00.000Z";
    await test.routine();
    await test.schedule();
    expect(test.besideStarts()).toEqual([]);
  });

  it("does not start a routine run beside a turn of the own chat", async () => {
    const test = await harness();
    const person = await test.person();
    await test.schedule();
    expect(test.started(person)).toBe(true);
    test.world.assignmentActive = true;
    await test.routine();
    await test.schedule();
    expect(test.besideStarts()).toEqual([]);
  });
});

describe("a routine run in its own session", () => {
  it("starts no session while a turn of the own chat runs", async () => {
    const mailbox = await realMailbox("openbot-routine-session-");
    const receipt = await runCauseEffect(
      mailbox.enqueue({
        sender: {
          kind: "routine",
          routineId: "routine-inbox",
          runId: "run-1",
          routineName: "Event inbox",
          scheduledFor: "2026-11-03T10:00:00.000Z",
        },
        recipientAgentIds: [AGENT],
        text: "Read the watch event.",
        draftIds: [],
      }),
    );
    const routine = mailbox.getDelivery(receipt.deliveries[0]?.id ?? "");
    if (!routine) throw new Error("No delivery.");
    const ownChat: Snapshot = { agentId: AGENT, threadId: OWN_THREAD, activeTurnId: "turn-person" };
    const agent = { id: AGENT, provider: "codex", model: "gpt-6-luna", threadId: OWN_THREAD };
    const sessions: string[] = [];
    const options = {
      store: {
        list: () => [agent],
        getOrCreate: () => Effect.succeed(agent),
        ensureThreadId: () => Effect.succeed(OWN_THREAD),
        database: { activeProviderSession: () => null },
      },
      mailbox,
      mailboxSync: { emitQueue: () => {} },
      conversation: { ensureSnapshot: () => ownChat },
      memory: { reserveTurn: () => () => {} },
      hooks: { servesModel: () => true },
      threads: {
        applyPendingRuntimeRefresh: () => Effect.void,
        holdRuntimeRefresh: () => () => {},
        startRoutineThread: () =>
          Effect.sync(() => {
            sessions.push("routine");
            return "thread-routine";
          }),
        closeRoutineThread: () => Effect.void,
        deletePendingHandoff: () => Effect.void,
      },
      providers: { ensureAgentClient: () => Effect.succeed({}) },
      channels: { store: { assignmentForDelivery: () => null }, prepare: () => Effect.succeed(null) },
      // biome-ignore lint/nursery/noUnsafeTypeAssertion: a stub of only the collaborators that this start reads.
    } as unknown as DrainSchedulerOptions;
    const drain = new DrainScheduler(options);
    await Effect.runPromise(drain.startDelivery(routine, undefined, true));
    expect(sessions).toEqual([]);
    expect(mailbox.getDelivery(routine.delivery.id)?.delivery.status).toBe("queued");
    expect(mailbox.besideActive(AGENT)).toBe(false);
  });
});
