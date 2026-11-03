// A channel lead with constant channel work held every queued routine run of its own chat: the main
// lane runs channel work and the own chat one turn at a time. The routine lane starts the oldest own
// chat message beside the channel work when it is a routine run. These checks drive the real
// `DrainScheduler` with stub collaborators and record which lane starts which delivery. Each limit
// that holds an agent still holds the routine lane.
import type { QueueDelivery } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { describe, expect, it } from "vitest";
import type { DeliveryContext } from "../mailbox-store";
import { DrainScheduler, type DrainSchedulerOptions } from "./drain-scheduler";

const AGENT = "agent-lead";
const ROUTINE_DELIVERY = "delivery-routine";
const CHANNEL_DELIVERY = "delivery-channel";
const PUBLIC_THREAD = "thread-own-chat";

interface Start {
  deliveryId: string;
  beside: boolean;
}

interface World {
  /** The channel work of the agent holds its own chat (an active assignment). */
  channelWork: boolean;
  /** The next delivery of the main lane: the routine run, or a channel delivery. */
  mainHead: "routine" | "channel" | null;
  /** The oldest message of the own chat is the routine run. */
  routineAtHead: boolean;
  oneTurnAtATime: boolean;
  usageLimited: boolean;
  compacting: boolean;
  clientCurrent: boolean;
  hidden: boolean;
  memoryLow: boolean;
  /** A turn of the own chat runs (a message of the person). */
  ownChatTurn: boolean;
  /** The main lane starts or runs a delivery that is not channel work. */
  mainRunsNonChannelWork: boolean;
  /** A channel turn runs on the channel thread. */
  channelTurn: boolean;
}

const calm = (): World => ({
  channelWork: true,
  mainHead: "channel",
  routineAtHead: true,
  oneTurnAtATime: false,
  usageLimited: false,
  compacting: false,
  clientCurrent: true,
  hidden: false,
  memoryLow: false,
  ownChatTurn: false,
  mainRunsNonChannelWork: false,
  channelTurn: false,
});

function context(deliveryId: string, kind: "routine" | "user"): DeliveryContext {
  const sender: QueueDelivery["sender"] =
    kind === "routine"
      ? {
          kind: "routine",
          routineId: "routine-1",
          runId: "run-1",
          routineName: "Watch events",
          scheduledFor: "2026-11-03T10:00:00.000Z",
        }
      : { kind: "user" };
  return {
    delivery: {
      id: deliveryId,
      messageId: `message-${deliveryId}`,
      recipientAgentId: AGENT,
      sender,
      text: "Read the watch event.",
      attachments: [],
      replyToMessageId: null,
      createdAt: "2026-11-03T10:00:00.000Z",
      status: "queued",
      turnId: null,
      error: null,
      position: 0,
    },
    managedAttachments: [],
  };
}

function scheduler(world: World): { drain: DrainScheduler; starts: Start[] } {
  const starts: Start[] = [];
  const started = (deliveryId: string, beside?: boolean) =>
    starts.some((start) => start.deliveryId === deliveryId && (beside === undefined || start.beside === beside));
  const agent = { id: AGENT, provider: "codex", model: "gpt-6-luna", threadId: PUBLIC_THREAD };
  const ownChat = { agentId: AGENT, threadId: PUBLIC_THREAD, activeTurnId: world.ownChatTurn ? "turn-chat" : null };
  const channelThread = { agentId: AGENT, threadId: "thread-channel", activeTurnId: "turn-channel" };
  const routine = context(ROUTINE_DELIVERY, "routine");
  const channel = context(CHANNEL_DELIVERY, "user");
  const yes = () => true;
  const options = {
    store: { list: () => [agent], database: { activeProviderSession: () => null } },
    mailbox: {
      nextQueued: (_agentId: string, taskId: string | null = null) => {
        if (taskId !== null) return null;
        if (world.mainHead === "channel" && !started(CHANNEL_DELIVERY)) return channel;
        if ((world.mainHead === "routine" || world.mainHead === "channel") && !started(ROUTINE_DELIVERY))
          return routine;
        return null;
      },
      nextBesideRoutine: () => (world.routineAtHead && !started(ROUTINE_DELIVERY) ? routine : null),
      laneActive: (_agentId: string, taskId: string | null) =>
        taskId === null &&
        (world.mainRunsNonChannelWork ||
          world.channelTurn ||
          started(CHANNEL_DELIVERY) ||
          started(ROUTINE_DELIVERY, false)),
      besideActive: () => started(ROUTINE_DELIVERY, true),
      besideTurn: () => false,
      mainLaneRunsNonChannelWork: () => world.mainRunsNonChannelWork,
      queuedTaskIds: () => [],
      taskIdForDelivery: () => null,
    },
    conversation: {
      activeSnapshots: () =>
        [
          ...(world.ownChatTurn ? [[AGENT, ownChat]] : []),
          ...(world.channelTurn ? [[AGENT, channelThread]] : []),
        ].values(),
      loadedSnapshot: () => ownChat,
      workingSnapshot: () => (world.ownChatTurn ? ownChat : world.channelTurn ? channelThread : undefined),
      isTaskThread: () => false,
      publicThreadId: (_agentId: string, threadId: string) => threadId,
    },
    providers: {
      isReady: yes,
      isReplacingCli: () => false,
      agentClientCurrent: () => world.clientCurrent,
    },
    duplication: { mayDrain: yes },
    profileSave: { mayDrain: yes },
    compaction: { heldThreads: () => (world.compacting ? ["thread-channel"] : []) },
    routines: { mayDrain: yes },
    memory: { mayDrain: () => !world.memoryLow, turnLimit: () => null, held: () => {} },
    usageLimits: {
      mayDrain: () => !world.usageLimited,
      mayTry: yes,
      retryHeld: () => false,
      tryStarted: () => {},
    },
    tasks: {
      threadId: () => null,
      openTaskIds: () => [],
      limit: () => 2,
      oneTurnAtATime: () => world.oneTurnAtATime,
    },
    hooks: {
      isStopping: () => false,
      followUsageLimit: () => false,
      dropRequestedNewChat: () => {},
      startRequestedNewChat: () => Effect.void,
    },
    channels: {
      mayDrain: () => !world.channelWork || world.mainHead === "channel",
      queueHold: () =>
        world.channelWork
          ? { reason: "channel-task", channelId: "channel-1", channelName: "Pod", agentId: AGENT }
          : null,
      store: {
        context: () => ({ threadId: "thread-channel" }),
        assignmentForDelivery: (deliveryId: string) =>
          deliveryId === CHANNEL_DELIVERY ? { channelId: "channel-1" } : null,
      },
    },
    // biome-ignore lint/nursery/noUnsafeTypeAssertion: a stub of only the collaborators that the drain reads in these checks.
  } as unknown as DrainSchedulerOptions;
  if (world.hidden) Object.assign(agent, { hiddenAt: "2026-11-03T10:00:00.000Z" });
  const drain = new DrainScheduler(options);
  Object.defineProperty(drain, "startDelivery", {
    value: (started: DeliveryContext, _planNote?: string, beside = false) =>
      Effect.sync(() => starts.push({ deliveryId: started.delivery.id, beside })),
  });
  return { drain, starts };
}

async function startsAfterSchedule(world: World): Promise<Start[]> {
  const { drain, starts } = scheduler(world);
  drain.scheduleDrain(AGENT);
  for (let round = 0; round < 5; round += 1) {
    await new Promise((done) => setTimeout(done, 0));
    await Effect.runPromise(Effect.all(drain.pendingTasks(), { discard: true }));
  }
  return starts;
}

const besideStarts = (starts: Start[]) => starts.filter((start) => start.beside);

describe("routine lane beside channel work", () => {
  it("starts the routine run beside the channel work of its agent", async () => {
    const starts = await startsAfterSchedule(calm());
    expect(besideStarts(starts)).toEqual([{ deliveryId: ROUTINE_DELIVERY, beside: true }]);
  });

  it("starts the routine run beside a running channel turn", async () => {
    const starts = await startsAfterSchedule({ ...calm(), mainHead: null, channelTurn: true });
    expect(besideStarts(starts)).toEqual([{ deliveryId: ROUTINE_DELIVERY, beside: true }]);
  });

  it("keeps the routine run in the main lane when the agent has no channel work", async () => {
    const starts = await startsAfterSchedule({ ...calm(), channelWork: false, mainHead: "routine" });
    expect(starts).toEqual([{ deliveryId: ROUTINE_DELIVERY, beside: false }]);
  });

  it("does not start a routine run that waits behind another own-chat message", async () => {
    expect(besideStarts(await startsAfterSchedule({ ...calm(), routineAtHead: false }))).toEqual([]);
  });

  it.each([
    ["the provider runs one turn of the agent at a time", { oneTurnAtATime: true }],
    ["the agent is at its usage limit", { usageLimited: true }],
    ["a thread of the agent compacts", { compacting: true }],
    ["the agent process restarts for a changed client", { clientCurrent: false, channelTurn: true, mainHead: null }],
    ["the agent is hidden", { hidden: true }],
    ["the machine is low on memory", { memoryLow: true }],
    ["a turn of the own chat runs", { ownChatTurn: true }],
    ["the main lane runs work that is not channel work", { mainRunsNonChannelWork: true, mainHead: null }],
  ] as const)("does not start a routine run beside channel work when %s", async (_label, change) => {
    expect(besideStarts(await startsAfterSchedule({ ...calm(), ...change }))).toEqual([]);
  });
});
