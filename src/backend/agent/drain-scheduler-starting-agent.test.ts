// @vitest-environment node
// A channel delivery can take minutes between its claim and its turn: `channels.prepare` builds the
// channel history, and a catch-up of the channel summary runs many model passes. The agent's own
// provider process ("Workspace only") was released as idle during that time, because the idle check
// counted only running turns, and the turn then failed with "Claude Agent SDK is not running." These
// checks drive the real `DrainScheduler.startDelivery` on a real `MailboxStore` with a channel
// prepare that waits, and read the per-agent starting state that the idle check now consults.
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Deferred, Effect } from "effect";
import { afterEach, describe, expect, it } from "vitest";
import { runCauseEffect } from "../effect-boundary";
import { MailboxStore } from "../mailbox-store";
import { OpenBotDatabase } from "../openbot-database";
import { DrainScheduler, type DrainSchedulerOptions } from "./drain-scheduler";

const AGENT = "agent-lead";
const OTHER = "agent-other";
const CHANNEL = "channel-pod";
const CHANNEL_THREAD = "thread-channel";

const roots: string[] = [];
const databases: OpenBotDatabase[] = [];

afterEach(async () => {
  for (const database of databases.splice(0)) database.close();
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function harness() {
  const root = await mkdtemp(join(tmpdir(), "openbot-starting-agent-"));
  roots.push(root);
  const database = new OpenBotDatabase(root);
  databases.push(database);
  const mailbox = new MailboxStore(root, root, database);
  await runCauseEffect(mailbox.initialize());
  const prepared = Deferred.makeUnsafe<{ threadId: string; text: string } | null>();
  const channelDeliveries = new Set<string>();
  const agent = {
    id: AGENT,
    provider: "claude",
    model: "claude-sonnet-5-5",
    threadId: "thread-own-chat",
    additionalFolders: [],
  };
  const yes = () => true;
  const options = {
    store: {
      list: () => [agent],
      getOrCreate: () => Effect.succeed(agent),
      database: { activeProviderSession: () => null },
    },
    mailbox,
    mailboxSync: { emitQueue: () => {} },
    conversation: {
      activeSnapshots: () => [].values(),
      loadedSnapshot: () => undefined,
      workingSnapshot: () => undefined,
      workingTaskSnapshots: () => [],
      isTaskThread: () => false,
      publicThreadId: (_agentId: string, threadId: string) => threadId,
      registerTaskThread: () => {},
    },
    providers: {
      isReady: yes,
      isReplacingCli: () => false,
      agentClientCurrent: yes,
      ensureAgentClient: () => Effect.succeed({ provider: "claude" }),
    },
    duplication: { mayDrain: yes },
    profileSave: { mayDrain: yes },
    compaction: { heldThreads: () => [] },
    routines: { mayDrain: yes },
    threads: { applyPendingRuntimeRefresh: () => Effect.void, holdRuntimeRefresh: () => () => {} },
    memory: { mayDrain: yes, turnLimit: () => null, held: () => {}, reserveTurn: () => () => {} },
    usageLimits: { mayDrain: yes, mayTry: yes, retryHeld: () => false, tryStarted: () => {} },
    planContinuation: {},
    tasks: { threadId: () => null, openTaskIds: () => [], limit: () => 2, oneTurnAtATime: () => false },
    hooks: {
      isStopping: () => false,
      followUsageLimit: () => false,
      dropRequestedNewChat: () => {},
      startRequestedNewChat: () => Effect.void,
      servesModel: yes,
      redactMcp: (text: string) => text,
      emitError: () => {},
    },
    channels: {
      mayDrain: yes,
      queueHold: () => null,
      prepare: () => Deferred.await(prepared),
      store: {
        context: () => ({ threadId: CHANNEL_THREAD }),
        assignmentForDelivery: (deliveryId: string) =>
          channelDeliveries.has(deliveryId) ? { channelId: CHANNEL, agentId: AGENT } : null,
        channelForThread: (threadId: string) => (threadId === CHANNEL_THREAD ? CHANNEL : null),
      },
    },
    // biome-ignore lint/nursery/noUnsafeTypeAssertion: a stub of only the collaborators that the start reads before its turn.
  } as unknown as DrainSchedulerOptions;
  const drain = new DrainScheduler(options);
  const settle = async () => {
    for (let round = 0; round < 6; round += 1) await new Promise((done) => setTimeout(done, 0));
  };
  const channel = async () => {
    const receipt = await runCauseEffect(
      mailbox.enqueue({
        sender: { kind: "user" },
        recipientAgentIds: [AGENT],
        text: "Check the release.",
        draftIds: [],
        channelId: CHANNEL,
      }),
    );
    const id = receipt.deliveries[0]?.id;
    if (!id) throw new Error("No delivery.");
    channelDeliveries.add(id);
    return id;
  };
  return {
    drain,
    mailbox,
    channel,
    settle,
    schedule: async () => {
      drain.scheduleDrain(AGENT);
      await settle();
    },
    // The channel was removed before the packet was ready: the start ends with no turn.
    endPrepare: async () => {
      await Effect.runPromise(Deferred.succeed(prepared, null));
      await Effect.runPromise(Effect.all(drain.pendingTasks(), { discard: true }));
      await settle();
    },
    status: (id: string) => mailbox.getDelivery(id)?.delivery.status,
  };
}

describe("a starting delivery keeps its agent busy", () => {
  it("counts the agent as starting while its channel packet is prepared", async () => {
    const test = await harness();
    expect(test.drain.hasStartingDeliveriesFor(AGENT)).toBe(false);
    const delivery = await test.channel();
    await test.schedule();
    expect(test.status(delivery)).toBe("starting");
    expect(test.drain.hasStartingDeliveriesFor(AGENT)).toBe(true);
    expect(test.drain.hasStartingDeliveriesFor(OTHER)).toBe(false);
    await test.endPrepare();
    expect(test.status(delivery)).toBe("interrupted");
    expect(test.drain.hasStartingDeliveriesFor(AGENT)).toBe(false);
  });
});
