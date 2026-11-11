import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { QueueDelivery } from "@openbot/contracts/ipc";
import { afterEach, describe, expect, it } from "vitest";
import { runCauseEffect } from "../effect-boundary";
import { type MailboxLanes, MailboxStore } from "../mailbox-store";
import { OpenBotDatabase } from "../openbot-database";
import { TurnSlots, type TurnSlotsOptions } from "./turn-slots";

const AGENTS = ["agent-a", "agent-b", "agent-c", "agent-d", "agent-e", "agent-f"];
const TASKS: Record<string, string[]> = { "agent-a": ["task-a1", "task-a2"], "agent-c": ["task-c1"] };
const ROUTINE = "\u0000routine";
const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), "openbot-turn-slot-index-"));
  const database = new OpenBotDatabase(join(root, "data"));
  const mailbox = new MailboxStore(join(root, "data"), join(root, "shared"), database);
  await runCauseEffect(mailbox.initialize());
  cleanups.push(async () => {
    database.close();
    await rm(root, { recursive: true, force: true });
  });
  return mailbox;
}

type Sender = "user" | "agent" | "routine";
let sequence = 0;

async function send(
  mailbox: MailboxStore,
  recipients: string[],
  sender: Sender,
  options: { taskId?: string; replyTo?: string; expectsReply?: boolean; holdStart?: boolean; from?: string } = {},
) {
  sequence += 1;
  const receipt = await runCauseEffect(
    mailbox.enqueue({
      sender:
        sender === "user"
          ? { kind: "user" }
          : sender === "agent"
            ? { kind: "agent", agentId: options.from ?? "agent-helper" }
            : {
                kind: "routine",
                routineId: "builds",
                runId: `run-${sequence}`,
                routineName: "Builds",
                scheduledFor: "2026-11-11T08:00:00.000Z",
              },
      recipientAgentIds: recipients,
      text: `${sequence}: Check the release.`,
      ...(options.taskId ? { taskId: options.taskId } : {}),
      ...(options.replyTo ? { replyToMessageId: options.replyTo } : {}),
      ...(options.expectsReply === false ? { expectsReply: false } : {}),
      ...(options.holdStart ? { holdStart: true } : {}),
      ...(sender === "agent"
        ? { idempotencyKey: `agent:${options.from ?? "agent-helper"}:turn-${sequence}:${sequence}` }
        : { draftIds: [] }),
    }),
  );
  await new Promise((done) => setTimeout(done, 2));
  return receipt;
}

async function one(mailbox: MailboxStore, recipient: string, sender: Sender, options: Parameters<typeof send>[3] = {}) {
  const id = (await send(mailbox, [recipient], sender, options)).deliveries[0]?.id;
  if (!id) throw new Error("The message has no delivery.");
  return id;
}

const lanesOf = (agentId: string) => [
  agentId,
  `${agentId}${ROUTINE}`,
  ...(TASKS[agentId] ?? []).map((task) => `${agentId}\u0000${task}`),
];
const allLanes = () => AGENTS.flatMap(lanesOf);

function parse(key: string): { agentId: string; taskId: string | null; routine: boolean } {
  const separator = key.indexOf("\u0000");
  if (separator < 0) return { agentId: key, taskId: null, routine: false };
  const agentId = key.slice(0, separator);
  if (key.slice(separator) === ROUTINE) return { agentId, taskId: null, routine: true };
  return { agentId, taskId: key.slice(separator + 1), routine: false };
}

function reads(mailbox: MailboxStore, lanes?: MailboxLanes) {
  const activity = lanes ?? mailbox;
  const running = (key: string) => {
    const lane = parse(key);
    return lane.routine ? activity.besideActive(lane.agentId) : activity.laneActive(lane.agentId, lane.taskId);
  };
  const head = (key: string) => {
    const lane = parse(key);
    const next = lane.routine
      ? mailbox.nextBesideRoutine(lane.agentId, lanes)
      : mailbox.nextQueued(lane.agentId, lane.taskId, lanes);
    return next?.delivery ?? null;
  };
  return { running, head, personLanes: () => activity.personLanes() };
}

function options(
  mailbox: MailboxStore,
  limit: number | null,
  backgroundLimit: number | null,
  current: () => MailboxLanes | undefined,
): TurnSlotsOptions {
  const read = () => reads(mailbox, current());
  return {
    limit: () => limit,
    lanes: allLanes,
    isRunning: (key) => read().running(key),
    isWaiting: (key) => !read().running(key) && read().head(key) !== null,
    head: (key) => read().head(key),
    backgroundLimit: () => backgroundLimit,
    personLanes: () =>
      new Set(
        read()
          .personLanes()
          .map((lane) => (lane.taskId === null ? lane.agentId : `${lane.agentId}\u0000${lane.taskId}`)),
      ),
  };
}

function startsBefore(left: QueueDelivery, leftLane: string, right: QueueDelivery, rightLane: string): boolean {
  const leftPerson = left.sender.kind === "user";
  const rightPerson = right.sender.kind === "user";
  if (leftPerson !== rightPerson) return leftPerson;
  if (left.createdAt !== right.createdAt) return left.createdAt < right.createdAt;
  return leftLane < rightLane;
}

function scanDecision(slots: TurnSlotsOptions, lane: string): boolean {
  const limit = slots.limit();
  if (limit === null) return true;
  const lanes = slots.lanes();
  const running = lanes.filter((id) => slots.isRunning(id));
  const free = limit - running.length;
  if (free <= 0) return false;
  const head = slots.head(lane);
  if (!head) return true;
  const backgroundLimit = head.sender.kind === "user" ? null : (slots.backgroundLimit?.(limit) ?? null);
  const personLanes = slots.personLanes?.() ?? new Set<string>();
  const backgroundFree =
    backgroundLimit === null ? null : backgroundLimit - running.filter((id) => !personLanes.has(id)).length;
  if (backgroundFree !== null && backgroundFree <= 0) return false;
  let ahead = 0;
  let backgroundAhead = 0;
  for (const other of lanes) {
    if (other === lane) continue;
    const otherHead = slots.head(other);
    if (!otherHead || !startsBefore(otherHead, other, head, lane)) continue;
    if (!slots.isRunning(other) && slots.isWaiting(other)) {
      ahead += 1;
      if (otherHead.sender.kind !== "user") backgroundAhead += 1;
    }
  }
  return ahead < free && (backgroundFree === null || backgroundAhead < backgroundFree);
}

function compare(mailbox: MailboxStore, turnIds: string[]) {
  const lanes = mailbox.lanes();
  for (const agentId of AGENTS) {
    expect(lanes.besideActive(agentId), `besideActive ${agentId}`).toBe(mailbox.besideActive(agentId));
    expect(mailbox.nextBesideRoutine(agentId, lanes), `nextBesideRoutine ${agentId}`).toEqual(
      mailbox.nextBesideRoutine(agentId),
    );
    for (const taskId of [null, ...(TASKS[agentId] ?? []), "task-unknown"]) {
      expect(lanes.laneActive(agentId, taskId), `laneActive ${agentId} ${taskId}`).toBe(
        mailbox.laneActive(agentId, taskId),
      );
      expect(mailbox.nextQueued(agentId, taskId, lanes), `nextQueued ${agentId} ${taskId}`).toEqual(
        mailbox.nextQueued(agentId, taskId),
      );
    }
    for (const turnId of [...turnIds, "turn-unknown"])
      expect(lanes.besideTurn(agentId, turnId), `besideTurn ${agentId} ${turnId}`).toBe(
        mailbox.besideTurn(agentId, turnId),
      );
  }
  expect(lanes.personLanes()).toEqual(mailbox.personLanes());

  let current: MailboxLanes | undefined;
  const limits: Array<[number | null, number | null]> = [[null, null]];
  for (let limit = 1; limit <= 12; limit += 1)
    for (const backgroundLimit of [null, 1, 2, 3, 5]) limits.push([limit, backgroundLimit]);
  for (const [limit, backgroundLimit] of limits) {
    const scan = options(mailbox, limit, backgroundLimit, () => undefined);
    const indexed = new TurnSlots(options(mailbox, limit, backgroundLimit, () => current));
    for (const lane of allLanes()) {
      current = mailbox.lanes();
      expect(indexed.mayStart(lane), `mayStart ${lane} limit ${limit} background ${backgroundLimit}`).toBe(
        scanDecision(scan, lane),
      );
    }
  }
}

describe("MailboxStore.lanes", () => {
  it("gives the answers and the slot decisions of the whole-mailbox scan in each delivery state", async () => {
    const mailbox = await fixture();
    const turnIds: string[] = [];
    const start = async (id: string, beside = false) => {
      await runCauseEffect(mailbox.markStarting(id, beside));
      return id;
    };
    const run = async (id: string, beside = false) => {
      await start(id, beside);
      const turnId = `turn-${id}`;
      turnIds.push(turnId);
      await runCauseEffect(mailbox.markRunning(id, turnId));
      return id;
    };
    compare(mailbox, turnIds);

    for (const [index, agentId] of AGENTS.entries()) {
      const done = await run(await one(mailbox, agentId, index % 2 ? "agent" : "user"));
      await runCauseEffect(mailbox.markTerminal(done, "completed"));
      const failed = await start(await one(mailbox, agentId, "routine"));
      await runCauseEffect(mailbox.markTerminal(failed, "failed", "The provider failed."));
      const interrupted = await run(await one(mailbox, agentId, "agent"));
      await runCauseEffect(mailbox.markTerminal(interrupted, "interrupted"));
      await runCauseEffect(mailbox.cancel(agentId, await one(mailbox, agentId, "user")));
    }
    compare(mailbox, turnIds);

    const personRunning = await run(await one(mailbox, "agent-a", "user"));
    await one(mailbox, "agent-a", "routine");
    await one(mailbox, "agent-a", "user");
    await run(await one(mailbox, "agent-a", "agent", { taskId: "task-a1" }));
    await one(mailbox, "agent-a", "user", { taskId: "task-a1" });
    await one(mailbox, "agent-a", "agent", { taskId: "task-a2" });
    await start(await one(mailbox, "agent-b", "routine"));
    await one(mailbox, "agent-b", "user");
    const beside = await run(await one(mailbox, "agent-c", "routine"), true);
    await one(mailbox, "agent-c", "routine");
    await one(mailbox, "agent-c", "agent");
    await start(await one(mailbox, "agent-c", "user", { taskId: "task-c1" }));
    await start(await one(mailbox, "agent-d", "routine"), true);
    await one(mailbox, "agent-d", "user");
    await one(mailbox, "agent-e", "routine", { holdStart: true });
    await one(mailbox, "agent-e", "agent");
    const request = await send(mailbox, ["agent-e", "agent-f"], "agent", { from: "agent-b" });
    await one(mailbox, "agent-b", "agent", { from: "agent-e", replyTo: request.messageId, expectsReply: false });
    await one(mailbox, "agent-b", "routine");
    compare(mailbox, turnIds);

    await runCauseEffect(mailbox.markTerminal(personRunning, "completed"));
    await runCauseEffect(mailbox.markTerminal(beside, "completed"));
    const fDelivery = request.deliveries.find((delivery) => delivery.recipientAgentId === "agent-f")?.id;
    if (!fDelivery) throw new Error("The request has no delivery to agent-f.");
    await run(fDelivery);
    await runCauseEffect(mailbox.markTerminal(fDelivery, "completed"));
    compare(mailbox, turnIds);
  });

  it("does not follow a change after the read", async () => {
    const mailbox = await fixture();
    const id = await one(mailbox, "agent-a", "user");
    const before = mailbox.lanes();
    expect(before.laneActive("agent-a", null)).toBe(false);
    await runCauseEffect(mailbox.markStarting(id));
    expect(before.laneActive("agent-a", null)).toBe(false);
    expect(mailbox.lanes().laneActive("agent-a", null)).toBe(true);
    expect(mailbox.laneActive("agent-a", null)).toBe(true);
  });
});
