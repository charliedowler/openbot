import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChannelTask } from "@openbot/contracts/ipc";
import { afterEach, describe, expect, it } from "vitest";
import { AgentStore } from "../agent-store";
import { type ChannelAssignment, ChannelStore } from "../channel-store";
import { preparedForGetOrAll } from "../database/prepared-statements";
import { runCauseEffect } from "../effect-boundary";
import { MailboxStore } from "../mailbox-store";
import { OpenBotDatabase } from "../openbot-database";
import { withDatabaseTransaction } from "./conversation-runtime";

const STATES: ChannelAssignment["state"][][] = [["starting", "running", "queued"], ["completed"], ["running"]];
const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), "openbot-slot-wake-pass-"));
  const database = new OpenBotDatabase(join(root, "data"));
  const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
  const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  cleanups.push(async () => {
    database.close();
    await rm(root, { recursive: true, force: true });
  });
  const ids: string[] = [];
  for (let index = 0; index < 4; index += 1)
    ids.push(
      (
        await runCauseEffect(
          agents.createAgent({
            name: `Agent ${index}`,
            description: "Member",
            avatarSeed: `a-${index}`,
            avatarHue: null,
          }),
        )
      ).id,
    );
  return { database, mailbox, ids };
}

function task(channelId: string, ownerAgentId: string, state: ChannelTask["state"]): ChannelTask {
  const id = randomUUID();
  return {
    id,
    channelId,
    parentTaskId: null,
    rootTaskId: id,
    ownerAgentId,
    requestMessageId: randomUUID(),
    instruction: "Check the release.",
    attachmentDraftIds: [],
    expectedResult: "A report.",
    sourceMessageIds: [],
    dependencies: [],
    resources: [],
    state,
    revision: 1,
    assignmentCount: 1,
    error: null,
  };
}

function assignment(item: ChannelTask, state: ChannelAssignment["state"]): ChannelAssignment {
  return {
    id: randomUUID(),
    channelId: item.channelId,
    taskId: item.id,
    agentId: item.ownerAgentId ?? "none",
    taskRevision: item.revision,
    requestMessageId: item.requestMessageId,
    resources: [],
    deliveryId: null,
    turnId: randomUUID(),
    state,
    throughSequence: 0,
    summaryVersion: 0,
    awaitedTaskIds: [],
    pendingRevision: null,
    pendingOutcome: null,
  };
}

describe("ChannelStore agent assignment reads", () => {
  it("answers as a store with no cache after each kind of write", async () => {
    const { database, ids } = await fixture();
    const store = new ChannelStore(database);
    const truth = () => new ChannelStore(database);
    const same = (label: string) => {
      const fresh = truth();
      for (const agentId of [...ids, "nobody"])
        for (const states of STATES) {
          expect(store.hasAgentAssignmentInState(agentId, states), `${label}: ${agentId} ${states}`).toBe(
            fresh.hasAgentAssignmentInState(agentId, states),
          );
          expect(store.agentAssignment(agentId, states), `${label}: ${agentId} ${states}`).toEqual(
            fresh.agentAssignment(agentId, states),
          );
        }
    };
    const channels = ["channel-1", "channel-2"].map((channelId) => {
      const channel = store.create(channelId, {
        name: channelId,
        title: "",
        instructions: "Ship the release.",
        members: ids.map((agentId) => ({ agentId })),
        leadAgentId: ids[0] ?? "",
      });
      store.commit(randomUUID(), { channel, messages: [], tasks: [], assignments: [] });
      return channelId;
    });
    const [first = "", second = ""] = channels;
    const [a = "", b = "", c = ""] = ids;
    same("empty");

    const queued = task(first, a, "queued");
    store.update(store.get(first), { tasks: [queued] });
    same("a task");
    const running = assignment(queued, "running");
    store.update(store.get(first), { tasks: [{ ...queued, state: "running" }], assignments: [running] });
    same("a running assignment");
    expect(store.hasAgentAssignmentInState(a, ["running"])).toBe(true);

    store.update(store.get(first), { assignments: [{ ...running, state: "completed" }] });
    same("the assignment completed");
    expect(store.hasAgentAssignmentInState(a, ["running"])).toBe(false);

    const other = task(second, b, "queued");
    store.update(store.get(second), { tasks: [other], assignments: [assignment(other, "queued")] });
    same("a queued assignment in another channel");

    const rolledBack = task(first, c, "queued");
    store.update(store.get(first), { tasks: [rolledBack] });
    same("a task of a third agent");
    expect(() =>
      withDatabaseTransaction(database, () => {
        store.update(store.get(first), { assignments: [assignment(rolledBack, "running")] });
        expect(store.hasAgentAssignmentInState(c, ["running"])).toBe(true);
        expect(store.agentAssignment(c, ["running"])).not.toBeNull();
        throw new Error("The outer work fails.");
      }),
    ).toThrow("The outer work fails.");
    same("a write inside an outer transaction that rolled back");
    expect(store.hasAgentAssignmentInState(c, ["running"])).toBe(false);

    expect(() =>
      store.update(store.get(first), {
        tasks: [{ ...rolledBack, state: "failed" }],
        assignments: [{ ...assignment(rolledBack, "running"), taskId: randomUUID() }],
      }),
    ).toThrow();
    same("a write that rolled back");

    store.rebuild(first);
    same("a rebuilt channel");
    store.delete(second);
    same("a deleted channel");
    expect(store.hasAgentAssignmentInState(b, ["queued"])).toBe(false);
    expect(store.hasAgentAssignmentInState(a, [])).toBe(false);
  });
});

describe("preparedForGetOrAll", () => {
  it("compiles a statement once for each connection", async () => {
    const { database } = await fixture();
    const db = database.connection;
    const sql = "SELECT COUNT(*) AS count FROM projection_channels WHERE channel_id = ?";
    const statement = preparedForGetOrAll(db, sql);
    expect(preparedForGetOrAll(db, sql)).toBe(statement);
    expect(preparedForGetOrAll(db, `${sql} `)).not.toBe(statement);
    expect(statement.get("missing")).toEqual({ count: 0 });
    expect(statement.all("missing")).toEqual([{ count: 0 }]);
  });
});

describe("MailboxStore.hasQueuedHead", () => {
  it("is true exactly when nextQueued has a delivery, for each lane and state", async () => {
    const { mailbox, ids } = await fixture();
    const [a = "", b = ""] = ids;
    const lanes: [string, string | null][] = [
      [a, null],
      [a, "task-1"],
      [b, null],
      [b, "task-2"],
    ];
    const same = (label: string) => {
      for (const [agentId, taskId] of lanes) {
        const expected = mailbox.nextQueued(agentId, taskId) !== null;
        const active = mailbox.laneActive(agentId, taskId);
        expect(!active && mailbox.hasQueuedHead(agentId, taskId), `${label}: ${agentId} ${taskId}`).toBe(expected);
        if (!active) expect(mailbox.hasQueuedHead(agentId, taskId), `${label}: ${agentId} ${taskId}`).toBe(expected);
      }
    };
    const send = async (recipient: string, options: { taskId?: string; holdStart?: boolean; from?: string } = {}) => {
      const receipt = await runCauseEffect(
        mailbox.enqueue({
          sender: options.from ? { kind: "agent", agentId: options.from } : { kind: "user" },
          recipientAgentIds: [recipient],
          text: "Check the release.",
          ...(options.taskId ? { taskId: options.taskId } : {}),
          ...(options.holdStart ? { holdStart: true } : {}),
          ...(options.from ? { idempotencyKey: `agent:${options.from}:${randomUUID()}:1` } : { draftIds: [] }),
        }),
      );
      const id = receipt.deliveries[0]?.id;
      if (!id) throw new Error("No delivery.");
      return id;
    };
    same("empty");
    const held = await send(a, { holdStart: true });
    same("a held start");
    mailbox.releaseStart(held);
    same("a released start");
    const taskMessage = await send(a, { taskId: "task-1" });
    same("a task message");
    await runCauseEffect(mailbox.markStarting(held));
    same("a starting delivery");
    await send(b, { from: a });
    same("a teammate message");
    await runCauseEffect(mailbox.markStarting(taskMessage));
    await runCauseEffect(mailbox.markTerminal(taskMessage, "completed"));
    await runCauseEffect(mailbox.markTerminal(held, "completed"));
    same("settled deliveries");
  });
});

describe("MailboxStore per-agent reads", () => {
  it("answer as the stored rows after each kind of change", async () => {
    const { database, mailbox, ids } = await fixture();
    const [a = "", b = "", c = ""] = ids;
    const lanes: [string, string | null][] = [
      [a, null],
      [a, "task-1"],
      [b, null],
      [b, "task-2"],
      [c, null],
    ];
    const stored = () => {
      const rows = database.connection
        .prepare(
          `SELECT d.recipient_agent_id AS agent, d.status AS status, json_extract(m.message_json, '$.taskId') AS task
             FROM projection_deliveries AS d JOIN projection_mailbox_messages AS m ON m.message_id = d.message_id`,
        )
        .all();
      return rows.map((row) => ({
        agent: String(row.agent),
        status: String(row.status),
        task: row.task === null ? null : String(row.task),
      }));
    };
    const same = (label: string) => {
      const rows = stored();
      for (const [agentId, taskId] of lanes) {
        const active = rows.some(
          (row) =>
            row.agent === agentId && (row.status === "starting" || row.status === "running") && row.task === taskId,
        );
        expect(mailbox.laneActive(agentId, taskId), `${label}: active ${agentId} ${taskId}`).toBe(active);
        expect(mailbox.hasQueuedHead(agentId, taskId) && !active, `${label}: head ${agentId} ${taskId}`).toBe(
          mailbox.nextQueued(agentId, taskId) !== null,
        );
      }
      for (const agentId of ids) {
        const queuedTasks = new Set(
          rows
            .filter((row) => row.agent === agentId && row.status === "queued" && row.task !== null)
            .map((row) => row.task),
        );
        expect(new Set(mailbox.queuedTaskIds(agentId)), `${label}: queued tasks of ${agentId}`).toEqual(queuedTasks);
        expect(mailbox.hasUnfinishedDelivery(agentId), `${label}: unfinished ${agentId}`).toBe(
          rows.some((row) => row.agent === agentId && ["queued", "starting", "running"].includes(row.status)),
        );
      }
    };
    const send = async (recipients: string[], taskId?: string) => {
      const receipt = await runCauseEffect(
        mailbox.enqueue({
          sender: { kind: "user" },
          recipientAgentIds: recipients,
          text: "Check the release.",
          ...(taskId ? { taskId } : {}),
          draftIds: [],
        }),
      );
      return receipt.deliveries.map((delivery) => delivery.id);
    };
    same("empty");
    const [first = ""] = await send([a]);
    same("one queued");
    const [toB = "", toC = ""] = await send([b, c]);
    same("two recipients");
    const [taskA = ""] = await send([a], "task-1");
    const [taskB = ""] = await send([b], "task-2");
    same("task messages");
    await runCauseEffect(mailbox.markStarting(first));
    await runCauseEffect(mailbox.markStarting(taskB));
    same("starting");
    await runCauseEffect(mailbox.markTerminal(first, "completed"));
    same("completed");
    await runCauseEffect(mailbox.cancel(c, toC));
    same("cancelled");
    await runCauseEffect(mailbox.cancel(a, taskA));
    same("a task message cancelled");
    await runCauseEffect(mailbox.deleteTaskMessages(a, "task-1"));
    same("task messages deleted");
    await runCauseEffect(mailbox.deleteAgentData(b));
    same("agent data deleted");
    expect(toB).not.toBe("");
    await send([a, c]);
    same("a new message after the deletes");
  });
});
