import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, type ChannelMessage, type ChannelTask } from "@openbot/contracts/ipc";
import type { EventJsonValue } from "@openbot/contracts/ipc-events";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { ChannelAssignment } from "../src/backend/channel-store";

const OUT = resolve(import.meta.dirname, "../.openbot-build/bridge-work-cancel-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-work-cancel-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const fakeCodex = join(root, "codex");
process.env.HOME = home;
process.env.CODEX_HOME = join(home, ".codex");
process.env.OPENBOT_CODEX_PATH = fakeCodex;
mkdirSync(home, { recursive: true });

const FAKE_CODEX = `#!${process.execPath}
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n");
  process.exit(0);
}
const config = { config: { mcp_servers: {} }, layers: [{ name: { type: "user", file: "/fake/.codex/config.toml", profile: null }, version: "1", config: { mcp_servers: {} } }] };
let buffer = "";
let threads = 0;
let turns = 0;
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const answer = (message) => {
  const params = message.params || {};
  switch (message.method) {
    case "initialize": return write({ id: message.id, result: {} });
    case "account/read": return write({ id: message.id, result: { account: { type: "chatgpt", email: "codex@example.com" } } });
    case "account/rateLimits/read": return write({ id: message.id, result: { rateLimits: { limitId: "codex", primary: null, secondary: null }, rateLimitsByLimitId: null } });
    case "model/list": return write({ id: message.id, result: { data: [{ model: "gpt-6-luna", displayName: "GPT-6 Luna" }] } });
    case "plugin/list": return write({ id: message.id, result: { marketplaces: [] } });
    case "config/read": return write({ id: message.id, result: config });
    case "config/batchWrite": return write({ id: message.id, result: {} });
    case "thread/start": return write({ id: message.id, result: { thread: { id: "thread-" + (++threads), turns: [] } } });
    case "thread/resume": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "thread/unsubscribe": return write({ id: message.id, result: { status: "Unsubscribed" } });
    case "thread/turns/list":
    case "thread/items/list": return write({ id: message.id, result: { data: [], nextCursor: null } });
    case "thread/read": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "turn/start": {
      const turnId = "turn-" + (++turns);
      write({ id: message.id, result: { turn: { id: turnId, status: "inProgress", items: [] } } });
      write({ method: "turn/started", params: { threadId: params.threadId, turn: { id: turnId } } });
      return setTimeout(() => {
        write({ method: "item/completed", params: { threadId: params.threadId, turnId, item: { type: "agentMessage", id: "message-" + turnId, text: "Done.", phase: "final_answer" } } });
        write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: turnId, status: "completed" } } });
      }, 20);
    }
    case "turn/interrupt": return write({ id: message.id, result: {} });
    default:
      return write({ id: message.id, error: { code: -32601, message: "Method not found: " + message.method } });
  }
};
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newline = buffer.indexOf("\\n");
  while (newline >= 0) {
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (line) {
      const message = JSON.parse(line);
      if (message.method && message.id !== undefined) answer(message);
    }
    newline = buffer.indexOf("\\n");
  }
});
`;

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const { HostEventsService } = await import("../src/main/host-events-service");
const { signWebhookBody } = await import("../src/main/webhook-security");

const unused = () => Effect.die(new Error("Not used by this check."));
const browser: AgentBrowserHost = {
  listTabs: () => [],
  beginTakeover: unused,
  endTakeover: () => {},
  resolveUploadTarget: unused,
  handleDynamicTool: unused,
  userControlTabFor: () => null,
  awaitUserControlRelease: unused,
  onChanged: () => () => {},
  onControlChanged: () => () => {},
  onDocumentChanged: () => () => {},
  clearControls: () => {},
  endControl: () => {},
  close: () => Effect.void,
  clearAgentSession: () => Effect.void,
  shareTab: () => undefined,
  unshareThread: () => undefined,
  agentTab: () => null,
};

const bridgeModule = existsSync(resolve(import.meta.dirname, "../src/backend/bridge-work-service.ts"))
  ? await import("../src/backend/bridge-work-service")
  : null;
const report: { passed: boolean; checks: string[]; failed: string[] } = { passed: false, checks: [], failed: [] };
async function check(name: string, run: () => Promise<void>) {
  try {
    await run();
    report.checks.push(name);
    process.stdout.write(`PASS ${name}\n`);
  } catch (error) {
    report.failed.push(`${name}: ${String(error)}`);
    process.stdout.write(`FAIL ${name}: ${String(error)}\n`);
  }
}
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, database);
const agents = new AgentService({
  store,
  mailbox,
  browser,
  preferredProvider: "codex",
  offProviders: [...AGENT_PROVIDERS],
  timezone: () => "UTC",
});
const cipher = { encrypt: (value: string) => Buffer.from(value), decrypt: (value: Buffer) => value.toString() };
const relay = {
  connected: () => true,
  setEnabled: () => {},
  setAccountActive: () => {},
  refresh: () => {},
  registerRoute: (routeId: string) => Effect.succeed(`https://signal.example.test/v1/webhooks/${routeId}`),
  revokeRoute: () => Effect.void,
};
const bridge = bridgeModule
  ? new bridgeModule.BridgeWorkService(database, {
      channels: agents.channels,
      mailbox,
      cancelQueuedMessage: (agentId, deliveryId) => agents.cancelQueuedMessage(agentId, deliveryId),
    })
  : undefined;
const events = new HostEventsService({
  routines: agents.routineRecords,
  cipher,
  relay,
  accountPrincipal: () => "test-account",
  ...(bridge ? { bridgeWork: bridge } : {}),
});
const until = async (condition: () => boolean) => {
  const deadline = Date.now() + 5000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for durable work.");
    await new Promise((done) => setTimeout(done, 10));
  }
};
try {
  await writeFile(fakeCodex, FAKE_CODEX, { mode: 0o755 });
  await runCauseEffect(agents.initialize());
  const owner = await runCauseEffect(
    store.createAgent({ name: "Lifecycle", description: "Fixture", avatarSeed: "fixture", avatarHue: null }),
  );
  const save = async (eventType: string | null, channelId?: string) => {
    const saved = await runCauseEffect(
      events.saveRoutine({
        owner: channelId ? { kind: "channel", id: channelId } : { kind: "agent", id: owner.id },
        name: "Fixture receiver",
        instruction: "Handle the structured event.",
        active: true,
        timezone: "UTC",
        trigger: { kind: "webhook", eventType, filters: [] },
      }),
    );
    const route = agents.routineRecords.routes.list().find((item) => item.routineId === saved.routine.id);
    assert.ok(saved.secret && route);
    return { id: saved.routine.id, routeId: route.routeId, secret: saved.secret };
  };
  const lifecycle = await save("bridge.work.lifecycle");
  const inbox = await save(null);
  const replyRecipient = await runCauseEffect(
    store.createAgent({ name: "Reply recipient", description: "Fixture", avatarSeed: "reply", avatarHue: null }),
  );
  const channelId = randomUUID();
  await runCauseEffect(
    agents.channels.command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId,
        draft: {
          name: "Fixture channel",
          title: "",
          instructions: "Fixture",
          members: [{ agentId: owner.id }, { agentId: replyRecipient.id }],
          leadAgentId: owner.id,
        },
      },
      { id: "fixture", name: "Fixture" },
    ),
  );
  const channelInbox = await save(null, channelId);
  process.env.OPENBOT_WORK_LIFECYCLE_RECEIVERS = JSON.stringify([
    {
      ownerAgentId: owner.id,
      routineId: lifecycle.id,
      sources: [
        { kind: "agent", ownerId: owner.id, routineId: inbox.id },
        { kind: "channel", ownerId: channelId, routineId: channelInbox.id },
      ],
    },
  ]);
  const deliver = (
    route: typeof inbox,
    deliveryId: string,
    type: string,
    data: Record<string, EventJsonValue>,
    secret = route.secret,
  ) => {
    const body = new TextEncoder().encode(JSON.stringify({ type, data }));
    const timestamp = String(Math.floor(Date.now() / 1000));
    return runCauseEffect(
      events.receive({
        routeId: route.routeId,
        deliveryId,
        timestamp,
        signature: signWebhookBody(secret, timestamp, deliveryId, body),
        body,
      }),
    );
  };
  const cancel = (idempotencyKey: string, refs: Record<string, EventJsonValue>) =>
    deliver(lifecycle, `cancel-${randomUUID()}`, "bridge.work.cancel", {
      idempotencyKey,
      reason: "Finished fixture work.",
      ...refs,
    });
  const work = (deliveryId: string, dispatchId: string, route = inbox, extra: Record<string, EventJsonValue> = {}) =>
    deliver(route, deliveryId, "pod.dispatch", { bridgeWork: { kind: "work", dispatchId, ...extra } });
  const runFor = (deliveryId: string) =>
    database.connection
      .prepare(
        "SELECT r.* FROM projection_routine_runs r JOIN projection_webhook_receipts w ON w.run_id=r.run_id WHERE w.delivery_id=?",
      )
      .get(deliveryId);
  const queued = (deliveryId: string) => {
    const run = runFor(deliveryId);
    return run && typeof run.delivery_id === "string" ? mailbox.getDelivery(run.delivery_id)?.delivery.status : null;
  };
  await check("duplicate cancellation", async () => {
    await work("duplicate-work", "duplicate-dispatch");
    await until(() => queued("duplicate-work") === "queued");
    const first = await cancel("duplicate-key", { deliveryIds: ["duplicate-work"] });
    assert.equal(queued("duplicate-work"), "cancelled");
    const second = await cancel("duplicate-key", { deliveryIds: ["duplicate-work"] });
    assert.equal(JSON.parse(second.body ?? "null")?.duplicate, true);
    assert.equal(JSON.parse(first.body ?? "null")?.results[0]?.cancelledDeliveries, 1);
    assert.equal(JSON.parse(second.body ?? "null")?.results[0]?.cancelledDeliveries, 0);
  });
  await check("closure before delivery", async () => {
    await cancel("before-key", { dispatchId: "before-dispatch" });
    await work("before-work", "before-dispatch");
    assert.equal(runFor("before-work"), undefined);
  });
  await check("delivery racing closure", async () => {
    const original = store.getOrCreate;
    let release: (() => void) | undefined;
    let entered = false;
    store.getOrCreate = (...args) =>
      Effect.gen(function* () {
        entered = true;
        yield* Effect.promise(
          () =>
            new Promise<void>((done) => {
              release = done;
            }),
        );
        return yield* original.apply(store, args);
      });
    try {
      await work("race-work", "race-dispatch");
      await until(() => entered);
      await cancel("race-key", { dispatchId: "race-dispatch" });
    } finally {
      store.getOrCreate = original;
      release?.();
    }
    await until(() => runFor("race-work")?.status === "cancelled");
    await new Promise((done) => setTimeout(done, 30));
    assert.notEqual(queued("race-work"), "queued");
  });
  await check("retried old event after the tombstone", async () => {
    const original = await cancel("retry-key", { deliveryIds: ["old-event"] });
    assert.equal(original.status, 200);
    for (let attempt = 0; attempt < 3; attempt++) await work("old-event", "retry-dispatch");
    assert.equal(runFor("old-event"), undefined);
  });
  await check("exact PR references and final result notices", async () => {
    await cancel("pr-key", { pr: { repository: "fixture/repo", number: 10 } });
    await work("closed-pr-work", "pr-dispatch", inbox, { pr: { repository: "fixture/repo", number: 10 } });
    await work("other-pr-work", "other-pr-dispatch", inbox, { pr: { repository: "fixture/repo", number: 11 } });
    await deliver(inbox, "final-result", "pod.result", {
      bridgeWork: { kind: "work", dispatchId: "pr-dispatch", pr: { repository: "fixture/repo", number: 10 } },
    });
    assert.equal(runFor("closed-pr-work"), undefined);
    await until(() => queued("other-pr-work") === "queued" && queued("final-result") === "queued");
  });
  await check("signed receiver restriction and no model turn", async () => {
    assert.equal(
      (
        await deliver(
          lifecycle,
          "bad-signature",
          "bridge.work.cancel",
          { idempotencyKey: "bad", reason: "Fixture", dispatchId: "x" },
          "whsec_wrong_secret_fixture_12345",
        )
      ).status,
      401,
    );
    assert.equal(
      (
        await deliver(inbox, "wrong-receiver", "bridge.work.cancel", {
          idempotencyKey: "bad",
          reason: "Fixture",
          dispatchId: "x",
        })
      ).status,
      400,
    );
    await assert.rejects(
      runCauseEffect(events.testRoutine({ owner: { kind: "agent", id: owner.id }, id: lifecycle.id })),
    );
    assert.equal(database.connection.prepare("SELECT COUNT(*) AS n FROM projection_turns").get()?.n, 0);
  });
  const channelRun = (deliveryId: string) =>
    database.connection
      .prepare(
        "SELECT r.* FROM projection_channel_routine_runs r JOIN projection_webhook_receipts w ON w.run_id=r.run_id WHERE w.delivery_id=?",
      )
      .get(deliveryId);
  const taskFor = (deliveryId: string) =>
    agents.channels.store
      .tasks(channelId)
      .find((task) => task.requestMessageId === channelRun(deliveryId)?.request_message_id);
  const assignmentFor = async (
    task: ChannelTask,
    key: string,
    state: ChannelAssignment["state"] = "completed",
    requestMessageId: string | null = task.requestMessageId,
  ): Promise<ChannelAssignment> => {
    const receipt = await runCauseEffect(
      mailbox.enqueue({
        sender: { kind: "user" },
        recipientAgentIds: [task.ownerAgentId ?? owner.id],
        text: "Fixture channel assignment",
        draftIds: [],
        channelId,
        idempotencyKey: key,
        holdStart: true,
      }),
    );
    const deliveryId = receipt.deliveries[0]?.id;
    assert.ok(deliveryId);
    if (state === "completed") {
      await runCauseEffect(mailbox.markStarting(deliveryId));
      await runCauseEffect(mailbox.markTerminal(deliveryId, "completed"));
    }
    const assignment: ChannelAssignment = {
      id: randomUUID(),
      channelId,
      taskId: task.id,
      agentId: task.ownerAgentId ?? owner.id,
      taskRevision: task.revision,
      requestMessageId,
      resources: [],
      deliveryId,
      turnId: null,
      state,
      throughSequence: 0,
      summaryVersion: 0,
      awaitedTaskIds: [],
      pendingRevision: null,
      pendingOutcome: null,
    };
    agents.channels.store.update(agents.channels.store.get(channelId), { assignments: [assignment] });
    return assignment;
  };
  const openWait = (assignment: ChannelAssignment, id: string) => {
    assert.ok(assignment.deliveryId);
    database.connection
      .prepare(
        "INSERT INTO agent_waits(wait_id,agent_id,request_delivery_id,request_text,note,routine_id,created_at) VALUES(?,?,?,?,?,?,?)",
      )
      .run(id, assignment.agentId, assignment.deliveryId, "Fixture", "Fixture", null, new Date().toISOString());
  };
  const hasWait = (id: string, agentId = owner.id) => agents.listWaits(agentId).waits.some((wait) => wait.id === id);
  await check("open waits and child branches", async () => {
    await work("channel-work", "channel-dispatch", channelInbox);
    await until(() => Boolean(taskFor("channel-work")));
    const rootTask = taskFor("channel-work");
    assert.ok(rootTask);
    const assignment = await assignmentFor(rootTask, "wait-assignment");
    const childId = randomUUID();
    agents.channels.store.update(agents.channels.store.get(channelId), {
      tasks: [
        { ...rootTask, state: "waiting", dependencies: [childId] },
        { ...rootTask, id: childId, parentTaskId: rootTask.id, rootTaskId: rootTask.id, state: "paused" },
      ],
    });
    openWait(assignment, "channel:fixture-wait");
    const response = await cancel("channel-key", { dispatchId: "channel-dispatch" });
    assert.equal(taskFor("channel-work")?.state, "cancelled");
    assert.equal(agents.channels.store.task(channelId, childId)?.state, "cancelled");
    assert.equal(hasWait("channel:fixture-wait"), false);
    assert.equal(JSON.parse(response.body ?? "null")?.results[0]?.cancelledTasks, 2);
    assert.equal(JSON.parse(response.body ?? "null")?.results[0]?.cancelledWaits, 1);
  });
  const replyBranch = async (key: string) => {
    await work(`${key}-work`, `${key}-dispatch`, channelInbox);
    await until(() => Boolean(taskFor(`${key}-work`)));
    const rootTask = taskFor(`${key}-work`);
    assert.ok(rootTask);
    const parentAssignment = await assignmentFor(rootTask, `${key}-parent`);
    const turnId = `${key}-native-turn`;
    const targetId = `${key}-target-message`;
    const target: ChannelMessage = {
      id: targetId,
      channelId,
      taskId: rootTask.id,
      author: { kind: "agent", id: replyRecipient.id, name: replyRecipient.name },
      sequence: 0,
      superseded: false,
      message: {
        id: targetId,
        text: "Fixture recipient question.",
        author: "assistant",
        createdAt: new Date().toISOString(),
        status: "completed",
      },
    };
    agents.channels.store.update(agents.channels.store.get(channelId), {
      tasks: [{ ...rootTask, ownerAgentId: owner.id, state: "running" }],
      assignments: [{ ...parentAssignment, state: "running", turnId }],
      messages: [target],
    });
    const response = await runCauseEffect(
      agents.channels.tool(channelId, owner.id, turnId, `${key}-reply-call`, "channel_reply", {
        messageId: targetId,
        text: "Fixture reply needs an answer.",
      }),
    );
    assert.equal(JSON.stringify(response).includes('"accepted":true'), true);
    const parent = agents.channels.store.task(channelId, rootTask.id);
    const child = agents.channels.store
      .tasks(channelId)
      .find((task) => task.parentTaskId === rootTask.id && task.requestMessageId.startsWith("channel-reply-"));
    assert.ok(parent && child);
    agents.channels.store.update(agents.channels.store.get(channelId), {
      tasks: [{ ...parent, state: "waiting" }],
      assignments: [{ ...parentAssignment, state: "completed", turnId }],
    });
    return { parent, child, parentAssignment };
  };
  await check("queued native reply child closes with its original branch", async () => {
    const { parent, child, parentAssignment } = await replyBranch("reply-child");
    const childAssignment = await assignmentFor(child, "reply-child-queued", "queued");
    const historical = await assignmentFor(child, "reply-child-history", "completed", "older-reply-request");
    openWait(parentAssignment, "channel:reply-parent-wait");
    openWait(childAssignment, "channel:reply-child-wait");
    openWait(historical, "channel:reply-history-wait");
    const response = await cancel("reply-child-key", { dispatchId: "reply-child-dispatch" });
    assert.equal(JSON.parse(response.body ?? "null")?.results[0]?.refusals.length, 0);
    assert.equal(agents.channels.store.task(channelId, parent.id)?.state, "cancelled");
    assert.equal(agents.channels.store.task(channelId, child.id)?.state, "cancelled");
    assert.equal(mailbox.getDelivery(childAssignment.deliveryId ?? "")?.delivery.status, "cancelled");
    assert.equal(
      agents.channels.store.assignments(channelId).find((item) => item.id === childAssignment.id)?.state,
      "interrupted",
    );
    assert.equal(hasWait("channel:reply-parent-wait"), false);
    assert.equal(hasWait("channel:reply-child-wait", childAssignment.agentId), false);
    assert.equal(hasWait("channel:reply-history-wait", historical.agentId), true);
    assert.equal(JSON.parse(response.body ?? "null")?.results[0]?.cancelledTasks, 2);
  });
  await check("reply from a historical parent assignment is refused", async () => {
    const { parent, child, parentAssignment } = await replyBranch("historical-origin");
    const assignment = await assignmentFor(child, "historical-origin-queued", "queued");
    openWait(assignment, "channel:historical-origin-wait");
    const saved = agents.channels.store.assignments(channelId).find((item) => item.id === parentAssignment.id);
    assert.ok(saved);
    agents.channels.store.update(agents.channels.store.get(channelId), {
      assignments: [{ ...saved, requestMessageId: "older-parent-request" }],
    });
    const response = await cancel("historical-origin-key", { dispatchId: "historical-origin-dispatch" });
    assert.equal(JSON.parse(response.body ?? "null")?.results[0]?.refusals[0]?.reason, "unrelated-branch");
    assert.equal(agents.channels.store.task(channelId, parent.id)?.state, "waiting");
    assert.equal(agents.channels.store.task(channelId, child.id)?.state, child.state);
    assert.equal(mailbox.getDelivery(assignment.deliveryId ?? "")?.delivery.status, "queued");
    assert.equal(hasWait("channel:historical-origin-wait", assignment.agentId), true);
  });
  await check("reused parent refuses historical native reply child", async () => {
    const { parent, child } = await replyBranch("historical-reply");
    const assignment = await assignmentFor(child, "historical-reply-queued", "queued");
    openWait(assignment, "channel:historical-reply-wait");
    agents.channels.store.update(agents.channels.store.get(channelId), {
      tasks: [{ ...parent, requestMessageId: "newer-parent-request", state: "waiting" }],
    });
    const response = await cancel("historical-reply-key", { dispatchId: "historical-reply-dispatch" });
    assert.equal(JSON.parse(response.body ?? "null")?.results[0]?.refusals[0]?.reason, "unrelated-branch");
    assert.equal(agents.channels.store.task(channelId, parent.id)?.requestMessageId, "newer-parent-request");
    assert.equal(agents.channels.store.task(channelId, child.id)?.state, child.state);
    assert.equal(mailbox.getDelivery(assignment.deliveryId ?? "")?.delivery.status, "queued");
    assert.equal(hasWait("channel:historical-reply-wait", assignment.agentId), true);
  });
  await check("failed cancellation retries terminal descendants", async () => {
    await work("retry-channel-work", "retry-channel-dispatch", channelInbox);
    await until(() => Boolean(taskFor("retry-channel-work")));
    const rootTask = taskFor("retry-channel-work");
    assert.ok(rootTask);
    const assignment = await assignmentFor(rootTask, "retry-channel-assignment", "queued");
    openWait(assignment, "channel:retry-wait");
    const original = mailbox.cancelNow.bind(mailbox);
    let failed = false;
    mailbox.cancelNow = (agentId, deliveryId) => {
      if (deliveryId === assignment.deliveryId && !failed) {
        failed = true;
        throw new Error("Fixture cancellation failed once.");
      }
      original(agentId, deliveryId);
    };
    try {
      await assert.rejects(cancel("retry-channel-key", { dispatchId: "retry-channel-dispatch" }));
    } finally {
      mailbox.cancelNow = original;
    }
    assert.equal(failed, true);
    assert.equal(taskFor("retry-channel-work")?.state, "cancelled");
    assert.equal(mailbox.getDelivery(assignment.deliveryId ?? "")?.delivery.status, "queued");
    const response = await cancel("retry-channel-key", { dispatchId: "retry-channel-dispatch" });
    assert.equal(JSON.parse(response.body ?? "null")?.duplicate, true);
    assert.equal(mailbox.getDelivery(assignment.deliveryId ?? "")?.delivery.status, "cancelled");
    assert.equal(
      agents.channels.store.assignments(channelId).find((item) => item.id === assignment.id)?.state,
      "interrupted",
    );
    assert.equal(hasWait("channel:retry-wait"), false);
    assert.equal(JSON.parse(response.body ?? "null")?.results[0]?.cancelledDeliveries, 1);
  });
  await check("retry closes assignment after mailbox cancellation commits", async () => {
    await work("committed-cancel-work", "committed-cancel-dispatch", channelInbox);
    await until(() => Boolean(taskFor("committed-cancel-work")));
    const rootTask = taskFor("committed-cancel-work");
    assert.ok(rootTask);
    const assignment = await assignmentFor(rootTask, "committed-cancel-assignment", "queued");
    openWait(assignment, "channel:committed-cancel-wait");
    const store = agents.channels.store;
    const original = store.update.bind(store);
    let failed = false;
    store.update = (channel, changes) => {
      if (changes?.assignments?.some((item) => item.id === assignment.id && item.state === "interrupted") && !failed) {
        failed = true;
        throw new Error("Fixture assignment update failed after mailbox cancellation.");
      }
      return original(channel, changes);
    };
    try {
      await assert.rejects(cancel("committed-cancel-key", { dispatchId: "committed-cancel-dispatch" }));
    } finally {
      store.update = original;
    }
    assert.equal(failed, true);
    assert.equal(mailbox.getDelivery(assignment.deliveryId ?? "")?.delivery.status, "cancelled");
    assert.equal(store.assignments(channelId).find((item) => item.id === assignment.id)?.state, "queued");
    const response = await cancel("committed-cancel-key", { dispatchId: "committed-cancel-dispatch" });
    assert.equal(JSON.parse(response.body ?? "null")?.duplicate, true);
    assert.equal(store.assignments(channelId).find((item) => item.id === assignment.id)?.state, "interrupted");
    assert.equal(hasWait("channel:committed-cancel-wait"), false);
  });
  await check("historical assignments and legacy ambiguity", async () => {
    await work("history-work", "history-dispatch", channelInbox);
    await until(() => Boolean(taskFor("history-work")));
    const rootTask = taskFor("history-work");
    assert.ok(rootTask);
    const current = await assignmentFor(rootTask, "current-assignment");
    const historical = await assignmentFor(rootTask, "historical-assignment", "completed", "older-request");
    agents.channels.store.update(agents.channels.store.get(channelId), { tasks: [{ ...rootTask, state: "waiting" }] });
    openWait(current, "channel:current-wait");
    openWait(historical, "channel:historical-wait");
    await cancel("history-key", { dispatchId: "history-dispatch" });
    assert.equal(taskFor("history-work")?.state, "cancelled");
    assert.equal(hasWait("channel:current-wait"), false);
    assert.equal(hasWait("channel:historical-wait"), true);
    await work("legacy-work", "legacy-dispatch", channelInbox);
    await until(() => Boolean(taskFor("legacy-work")));
    const legacyTask = taskFor("legacy-work");
    assert.ok(legacyTask);
    const legacy = await assignmentFor(legacyTask, "legacy-assignment", "completed", null);
    openWait(legacy, "channel:legacy-wait");
    const refused = await cancel("legacy-key", { dispatchId: "legacy-dispatch" });
    assert.equal(JSON.parse(refused.body ?? "null")?.results[0]?.refusals[0]?.reason, "unrelated-branch");
    assert.equal(taskFor("legacy-work")?.state, legacyTask.state);
    assert.equal(hasWait("channel:legacy-wait"), true);
  });
  await check("running child preserves its whole branch and waits", async () => {
    await work("running-child-work", "running-child-dispatch", channelInbox);
    await until(() => Boolean(taskFor("running-child-work")));
    const rootTask = taskFor("running-child-work");
    assert.ok(rootTask);
    const assignment = await assignmentFor(rootTask, "running-child-parent");
    const childId = randomUUID();
    agents.channels.store.update(agents.channels.store.get(channelId), {
      tasks: [
        { ...rootTask, state: "waiting", dependencies: [childId] },
        { ...rootTask, id: childId, parentTaskId: rootTask.id, rootTaskId: rootTask.id, state: "running" },
      ],
    });
    openWait(assignment, "channel:running-child-wait");
    const refused = await cancel("running-child-key", { dispatchId: "running-child-dispatch" });
    assert.equal(JSON.parse(refused.body ?? "null")?.results[0]?.refusals[0]?.reason, "running-branch");
    assert.equal(taskFor("running-child-work")?.state, "waiting");
    assert.equal(agents.channels.store.task(channelId, childId)?.state, "running");
    assert.equal(hasWait("channel:running-child-wait"), true);
  });
  await check("unrelated dependent refuses cancellation", async () => {
    await work("dependency-work", "dependency-dispatch", channelInbox);
    await work("dependent-work", "dependent-dispatch", channelInbox);
    await until(() => Boolean(taskFor("dependency-work") && taskFor("dependent-work")));
    const dependency = taskFor("dependency-work");
    const dependent = taskFor("dependent-work");
    assert.ok(dependency && dependent);
    agents.channels.store.update(agents.channels.store.get(channelId), {
      tasks: [{ ...dependent, dependencies: [dependency.id] }],
    });
    const refused = await cancel("dependency-key", { dispatchId: "dependency-dispatch" });
    assert.equal(JSON.parse(refused.body ?? "null")?.results[0]?.refusals[0]?.reason, "unrelated-branch");
    assert.equal(taskFor("dependency-work")?.state, dependency.state);
    assert.deepEqual(taskFor("dependent-work")?.dependencies, [dependency.id]);
  });
  await check("reused root refuses orphaned request children", async () => {
    await work("reused-root-work", "reused-root-dispatch", channelInbox);
    await until(() => Boolean(taskFor("reused-root-work")));
    const rootTask = taskFor("reused-root-work");
    assert.ok(rootTask);
    const childId = randomUUID();
    agents.channels.store.update(agents.channels.store.get(channelId), {
      tasks: [
        { ...rootTask, requestMessageId: "newer-request", state: "waiting", dependencies: [childId] },
        { ...rootTask, id: childId, parentTaskId: rootTask.id, rootTaskId: rootTask.id, state: "paused" },
      ],
    });
    const refused = await cancel("reused-root-key", { dispatchId: "reused-root-dispatch" });
    assert.equal(JSON.parse(refused.body ?? "null")?.results[0]?.refusals[0]?.reason, "unrelated-branch");
    assert.equal(agents.channels.store.task(channelId, childId)?.state, "paused");
    assert.equal(agents.channels.store.task(channelId, rootTask.id)?.requestMessageId, "newer-request");
  });
  await check("unrelated running task and running child refusal", async () => {
    await work("running-channel-work", "running-dispatch", channelInbox);
    await until(() => Boolean(taskFor("running-channel-work")));
    const running = taskFor("running-channel-work");
    assert.ok(running);
    agents.channels.store.update(agents.channels.store.get(channelId), { tasks: [{ ...running, state: "running" }] });
    await work("cancel-channel-work", "cancel-dispatch", channelInbox);
    await until(() => Boolean(taskFor("cancel-channel-work")));
    const response = await cancel("unrelated-key", { dispatchId: "cancel-dispatch" });
    assert.equal(taskFor("cancel-channel-work")?.state, "cancelled");
    assert.equal(agents.channels.store.task(channelId, running.id)?.state, "running");
    assert.equal(JSON.parse(response.body ?? "null")?.results[0]?.refusals.length, 0);
    const refused = await cancel("running-key", { dispatchId: "running-dispatch" });
    assert.equal(JSON.parse(refused.body ?? "null")?.results[0]?.refusals[0]?.reason, "running-branch");
    assert.equal(agents.channels.store.task(channelId, running.id)?.state, "running");
  });
  await check("startup replays committed closure before queues drain", async () => {
    await work("startup-work", "startup-dispatch");
    await until(() => queued("startup-work") === "queued");
    assert.ok(bridge);
    bridge.store.close(`${owner.id}:${lifecycle.id}`, {
      idempotencyKey: "startup-key",
      reason: "Fixture committed closure before process loss.",
      deliveryIds: ["startup-work"],
    });
    assert.equal(queued("startup-work"), "queued");
    await runCauseEffect(agents.stop());
    const restoredStore = new AgentStore(userData, home, database);
    const restoredMailbox = new MailboxStore(userData, store.sharedRoot, database);
    const restored = new AgentService({
      store: restoredStore,
      mailbox: restoredMailbox,
      browser,
      preferredProvider: "codex",
      offProviders: [...AGENT_PROVIDERS],
      timezone: () => "UTC",
    });
    try {
      await runCauseEffect(restored.initialize());
      const deliveryId = runFor("startup-work")?.delivery_id;
      assert.equal(typeof deliveryId, "string");
      assert.equal(restoredMailbox.getDelivery(String(deliveryId))?.delivery.status, "cancelled");
      assert.equal(database.connection.prepare("SELECT COUNT(*) AS n FROM projection_turns").get()?.n, 0);
    } finally {
      await runCauseEffect(restored.stop());
    }
  });
} catch (error) {
  report.failed.push(String(error));
  process.stdout.write(`${String(error)}\n`);
} finally {
  await runCauseEffect(agents.stop()).catch(() => {});
  database.close();
  report.passed = report.failed.length === 0;
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}
process.stdout.write(`${report.checks.length} passed, ${report.failed.length} failed\n`);
if (!report.passed) process.exitCode = 1;
