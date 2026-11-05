import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, type AgentEvent } from "@openbot/contracts/ipc";
import {
  TEAM_CURRENT_CAPABILITIES,
  TEAM_PROTOCOL_V9_ONLY_CAPABILITIES,
} from "@openbot/contracts/team-protocol/current";
import { WATCH_REGISTRY_CODECS, WATCH_REGISTRY_ROUTES } from "@openbot/contracts/team-protocol/watch-registry-v1";
import {
  WATCH_REGISTRY_V2_CAPABILITY,
  WATCH_REGISTRY_V2_CODECS,
  WATCH_REGISTRY_V2_ROUTES,
} from "@openbot/contracts/team-protocol/watch-registry-v2";
import {
  decodeWatchingSnapshot,
  WATCH_REGISTRY_EVENT_TYPE,
  WATCH_REGISTRY_ROUTINE_EVENT_TYPE,
  WATCH_REGISTRY_TASKS_PUBLISHER_ID,
  type WatchingTask,
  type WatchRegistryPublication,
} from "@openbot/contracts/watch-registry";
import { registerSecretValue } from "@openbot/logging";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { AgentService } from "../src/backend/agent-service";
import { AgentStore } from "../src/backend/agent-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { AgentAccessStore } from "../src/main/agent-access-store";
import { HostEventsService } from "../src/main/host-events-service";
import { TeamApiServer } from "../src/main/team-api-server";
import { TeamStore } from "../src/main/team-store";
import { WatchRegistryStore } from "../src/main/watch-registry-store";
import { signWebhookBody } from "../src/main/webhook-security";

const OUT = resolve(import.meta.dirname, "../.openbot-build/watch-registry-v2-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-watch-registry-v2-"));
const userData = join(root, "state");
const legacyPath = join(root, "registry-v1.json");
const path = join(root, "registry-v2.json");
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, join(root, "home"), database);
const mailbox = new MailboxStore(userData, store.sharedRoot, database);
const unused = () => Effect.die(new Error("No provider or browser work is permitted."));
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
const changes: AgentEvent[] = [];
const registry = new WatchRegistryStore(path, {
  legacyPath,
  knownAgents: () => new Set(store.list().map((agent) => agent.id)),
  changed: () => agents.notifyWatchingChanged(),
});
const agents = new AgentService({
  store,
  mailbox,
  browser,
  offProviders: AGENT_PROVIDERS,
  watchRegistry: () => registry.snapshot(),
  watchRegistryV2: () => registry.snapshotV2(),
  watchTasks: () => registry.tasks(),
});
agents.on("event", (event) => changes.push(event));
const events = new HostEventsService({
  routines: agents.routineRecords,
  cipher: { encrypt: (value: string) => Buffer.from(value), decrypt: (value: Buffer) => value.toString() },
  relay: {
    connected: () => true,
    setEnabled: () => {},
    setAccountActive: () => {},
    refresh: () => {},
    registerRoute: (routeId: string) => Effect.succeed(`https://signal.example.test/v1/webhooks/${routeId}`),
    revokeRoute: () => Effect.void,
  },
  accountPrincipal: () => "synthetic-account",
  watchRegistry: registry,
});
const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };
async function check(name: string, run: () => void | Promise<void>) {
  await run();
  report.checks.push(name);
  process.stdout.write(`PASS ${name}\n`);
}
const watchingChanges = () => changes.filter((event) => event.type === "watching-changed").length;
const countWork = () => ({
  runs: database.connection.prepare("SELECT COUNT(*) AS n FROM projection_routine_runs").get(),
  messages: database.connection.prepare("SELECT COUNT(*) AS n FROM projection_mailbox_messages").get(),
});
try {
  await runCauseEffect(store.initialize());
  await runCauseEffect(mailbox.initialize());
  const create = (name: string) =>
    runCauseEffect(
      store.createAgent({
        name,
        description: "Synthetic watch owner",
        avatarSeed: name.toLowerCase(),
        avatarHue: null,
      }),
    );
  const owner = await create("Owner");
  const helper = await create("Helper");
  const hidden = await create("Hidden");
  const save = async (name: string) => {
    const saved = await Effect.runPromise(
      events.saveRoutine({
        owner: { kind: "agent", id: owner.id },
        name,
        instruction: "Synthetic metadata receiver.",
        active: true,
        timezone: "UTC",
        trigger: { kind: "webhook", eventType: WATCH_REGISTRY_ROUTINE_EVENT_TYPE, filters: [] },
      }),
    );
    assert.ok(saved.secret);
    const route = agents.routineRecords.routes.list().find((route) => route.routineId === saved.routine.id);
    assert.ok(route);
    return { routeId: route.routeId, secret: saved.secret };
  };
  const primary = await save("Management registry");
  const restricted = await save("Owner registry");
  const legacy = {
    version: 1,
    managementPublisherRouteId: primary.routeId,
    revision: 7,
    publishers: [
      {
        publisherId: primary.routeId,
        ownerAgentId: owner.id,
        snapshot: {
          version: 1,
          sequence: 40,
          generatedAt: "2026-11-05T10:00:00.000Z",
          watches: [
            {
              id: "legacy",
              ownerAgentId: owner.id,
              sourceKind: "github",
              title: "Legacy watch",
              status: "active",
              condition: "Until merged",
            },
          ],
        },
      },
    ],
  };
  const legacyBytes = JSON.stringify(legacy);
  await writeFile(legacyPath, legacyBytes);
  await Effect.runPromise(registry.load());
  const receive = async (route: { routeId: string; secret: string }, data: unknown) => {
    const timestamp = String(Math.floor(Date.now() / 1000));
    const deliveryId = randomUUID();
    const body = new TextEncoder().encode(JSON.stringify({ type: WATCH_REGISTRY_EVENT_TYPE, data }));
    return (
      await Effect.runPromise(
        events.receive({
          routeId: route.routeId,
          deliveryId,
          timestamp,
          body,
          signature: signWebhookBody(route.secret, timestamp, deliveryId, body),
        }),
      )
    ).status;
  };
  const watches: WatchRegistryPublication = {
    version: 2,
    sequence: 41,
    generatedAt: new Date().toISOString(),
    watches: [
      {
        id: "pr-review",
        ownerAgentId: owner.id,
        sourceKind: "github.pr",
        title: "lightdash/lightdash#32090",
        targetUrl: "https://github.com/lightdash/lightdash/pull/32090",
        status: "active",
        summary: "PR #32090 review thread",
        purpose: "Tell Owner when the review asks for changes",
        receiverAgentIds: [owner.id, helper.id, hidden.id],
        expiresAt: new Date(Date.now() + 24 * 3600_000).toISOString(),
      },
    ],
  };
  const tasks: WatchingTask[] = [
    {
      id: "decision:1",
      kind: "decision",
      state: "waiting-on-charlie",
      title: "Pick the rollout date",
      detail: "Recommend Monday",
      ownerAgentId: owner.id,
      link: "https://linear.app/lightdash/issue/SPK-1",
      updatedAt: "2026-11-05T19:00:00Z",
      waitingOn: "Charlie",
      waitingSince: "2026-11-03T09:00:00.000Z",
      urgency: "high",
      urgencyReason: "Decision waiting more than 24 hours",
    },
    {
      id: "follow-up:2",
      kind: "follow-up",
      state: "waiting-on-event",
      title: "Reply to the customer",
      updatedAt: "2026-11-05T18:00:00.000Z",
      waitingOn: "Sam at Example Retail",
      waitingSince: "2026-11-02T09:00:00.000Z",
      urgency: "normal",
    },
    {
      id: "dispatch:3",
      kind: "dispatch",
      state: "in-progress",
      title: "Hidden agent work",
      ownerAgentId: hidden.id,
      updatedAt: "2026-11-05T18:30:00.000Z",
    },
  ];
  const taskPublication: WatchRegistryPublication = {
    version: 2,
    sequence: 1,
    generatedAt: new Date().toISOString(),
    publisherId: WATCH_REGISTRY_TASKS_PUBLISHER_ID,
    watches: [],
    tasks,
    health: {
      wip: 2,
      wipLimit: 5,
      oldest: { taskId: "follow-up:2", since: "2026-11-02T09:00:00.000Z" },
      longestWaiting: {
        taskId: "follow-up:2",
        waitingOn: "Sam at Example Retail",
        since: "2026-11-02T09:00:00.000Z",
      },
      urgent: 1,
      score: 85,
      reasons: ["1 urgent task", "1 task waiting more than 3 working days"],
    },
  };
  const work = countWork();
  await check("The host advertises watch-registry-v2 to protocol 9 peers", () => {
    assert.ok(TEAM_CURRENT_CAPABILITIES.some((capability) => capability === WATCH_REGISTRY_V2_CAPABILITY));
    assert.ok(TEAM_PROTOCOL_V9_ONLY_CAPABILITIES.has(WATCH_REGISTRY_V2_CAPABILITY));
  });
  await check("The v1 file is read until the first v2 publication", () => {
    assert.equal(registry.snapshot()?.watches[0]?.id, "legacy");
    assert.equal(registry.snapshot()?.sequence, 7);
    assert.equal(existsSync(path), false);
  });
  await check("A v2 publication keeps summary, purpose and receivers and needs no condition", async () => {
    assert.equal(await receive(primary, watches), 202);
    const watch = agents.getWatchingSnapshotV2().registry?.watches[0];
    assert.equal(watch?.summary, "PR #32090 review thread");
    assert.equal(watch?.purpose, "Tell Owner when the review asks for changes");
    assert.deepEqual(watch?.receiverAgentIds, [owner.id, helper.id, hidden.id]);
    assert.equal(watch?.condition, undefined);
    assert.equal(readFileSync(legacyPath, "utf8"), legacyBytes);
    assert.equal(JSON.parse(readFileSync(path, "utf8")).version, 2);
    assert.equal(watchingChanges(), 1);
  });
  await check("The v1 snapshot keeps its exact shape for older readers", () => {
    const v1 = agents.getWatchingSnapshot();
    assert.equal(v1.registry?.version, 1);
    assert.equal("tasks" in v1, false);
    assert.ok(v1.registry?.watches.every((watch) => !("summary" in watch) && !("receiverAgentIds" in watch)));
    const codec = WATCH_REGISTRY_CODECS.get(WATCH_REGISTRY_ROUTES.snapshot);
    assert.ok(codec);
    assert.deepEqual(decodeWatchingSnapshot(codec.response(200, v1)), v1);
  });
  await check("A tasks publication has its own slot, sequence and change event", async () => {
    assert.equal(await receive(primary, taskPublication), 202);
    assert.equal(watchingChanges(), 2);
    const snapshot = agents.getWatchingSnapshotV2();
    assert.equal(snapshot.registry?.watches.length, 1);
    assert.equal(snapshot.registry?.watches[0]?.id, "pr-review");
    assert.equal(snapshot.tasks?.sequence, 1);
    assert.deepEqual(
      snapshot.tasks?.items.map((task) => task.id),
      ["decision:1", "follow-up:2", "dispatch:3"],
    );
    assert.equal(snapshot.tasks?.health?.score, 85);
    assert.equal(snapshot.tasks?.items[1]?.waitingOn, "Sam at Example Retail");
    assert.equal(snapshot.tasks?.items[0]?.urgency, "high");
    const codec = WATCH_REGISTRY_V2_CODECS.get(WATCH_REGISTRY_V2_ROUTES.snapshot);
    assert.ok(codec);
    assert.deepEqual(decodeWatchingSnapshot(codec.response(200, snapshot)), snapshot);
    assert.equal(await receive(primary, { ...watches, sequence: 42 }), 202);
    assert.equal(agents.getWatchingSnapshotV2().tasks?.items.length, 3);
    assert.equal(await receive(primary, taskPublication), 200);
    assert.equal(await receive(primary, { ...taskPublication, sequence: 2, tasks: tasks.slice(0, 1) }), 202);
    assert.equal(agents.getWatchingSnapshotV2().tasks?.items.length, 1);
    assert.equal(agents.getWatchingSnapshotV2().registry?.watches.length, 1);
    assert.equal(watchingChanges(), 4);
    const { publisherId: _publisherId, ...implicit } = taskPublication;
    assert.equal(await receive(primary, { ...implicit, sequence: 3 }), 202);
    assert.equal(agents.getWatchingSnapshotV2().tasks?.items.length, 3);
    assert.deepEqual(countWork(), work);
  });
  await check("Only the management route can publish tasks", async () => {
    assert.equal(await receive(restricted, { ...taskPublication, sequence: 9 }), 400);
    assert.equal(agents.getWatchingSnapshotV2().tasks?.sequence, 3);
  });
  await check("Strict task and watch bounds refuse unknown or oversized fields", async () => {
    const [task] = tasks;
    assert.ok(task);
    for (const bad of [
      { ...taskPublication, sequence: 10, watches: watches.watches },
      { ...taskPublication, sequence: 10, publisherId: "other" },
      { ...taskPublication, sequence: 10, tasks: [{ ...task, extra: true }] },
      { ...taskPublication, sequence: 10, tasks: [{ ...task, urgency: "urgent" }] },
      { ...taskPublication, sequence: 10, tasks: [{ ...task, title: "x".repeat(201) }] },
      { ...taskPublication, sequence: 10, tasks: [{ ...task, waitingOn: "x".repeat(121) }] },
      { ...taskPublication, sequence: 10, tasks: [{ ...task, link: "https://example.test/a?token=1" }] },
      { ...taskPublication, sequence: 10, tasks: [task, task] },
      {
        ...taskPublication,
        sequence: 10,
        tasks: Array.from({ length: 301 }, (_, index) => ({
          id: `d:${index}`,
          kind: "decision",
          state: "done",
          title: "t",
          updatedAt: "2026-11-05T19:00:00Z",
        })),
      },
      { ...taskPublication, sequence: 10, health: { ...taskPublication.health, score: 101 } },
      { ...taskPublication, sequence: 10, health: { ...taskPublication.health, reasons: Array(9).fill("x") } },
      { ...watches, sequence: 50, watches: [{ ...watches.watches[0], summary: "x".repeat(121) }] },
      { ...watches, sequence: 50, watches: [{ ...watches.watches[0], purpose: "x".repeat(241) }] },
      {
        ...watches,
        sequence: 50,
        watches: [{ ...watches.watches[0], receiverAgentIds: Array.from({ length: 9 }, (_, i) => `a${i}`) }],
      },
      { ...watches, version: 1, sequence: 50 },
    ])
      assert.equal(await receive(primary, bad), 400, JSON.stringify(bad).slice(0, 200));
    assert.equal(agents.getWatchingSnapshotV2().tasks?.sequence, 3);
  });
  await check("A v1 publication is still accepted after v2", async () => {
    const [first] = watches.watches;
    assert.ok(first);
    const { summary: _s, purpose: _p, receiverAgentIds: _r, ...plain } = first;
    assert.equal(
      await receive(primary, {
        ...watches,
        version: 1,
        sequence: 60,
        watches: [{ ...plain, condition: "Until merged" }],
      }),
      202,
    );
    assert.equal(agents.getWatchingSnapshotV2().registry?.watches[0]?.condition, "Until merged");
    assert.equal(agents.getWatchingSnapshotV2().tasks?.sequence, 3);
    assert.equal(await receive(primary, { ...watches, sequence: 61 }), 202);
  });
  await check("Known secrets are removed from v2 display text", async () => {
    const secret = "synthetic-secret-for-watch-v2";
    registerSecretValue(secret);
    assert.equal(
      await receive(primary, {
        ...taskPublication,
        sequence: 20,
        tasks: [{ ...tasks[0], title: `Rotate ${secret}`, detail: secret, urgencyReason: secret }],
        health: { ...taskPublication.health, reasons: [`Reason ${secret}`] },
      }),
      202,
    );
    assert.equal(
      await receive(primary, {
        ...watches,
        sequence: 70,
        watches: [{ ...watches.watches[0], summary: `Summary ${secret}`, purpose: `Purpose ${secret}` }],
      }),
      202,
    );
    assert.ok(!readFileSync(path, "utf8").includes(secret));
    assert.ok(!JSON.stringify(agents.getWatchingSnapshotV2()).includes(secret));
    assert.equal(
      await receive(primary, { ...taskPublication, sequence: 21, tasks: [{ ...tasks[0], id: `decision:${secret}` }] }),
      400,
    );
    assert.equal(await receive(primary, { ...taskPublication, sequence: 22 }), 202);
  });
  await check("The stored v2 file reloads to the same watches and tasks", async () => {
    const reloaded = new WatchRegistryStore(path, {
      legacyPath,
      knownAgents: () => new Set([owner.id, helper.id, hidden.id]),
      changed: () => {
        throw new Error("A read cannot notify.");
      },
    });
    await Effect.runPromise(reloaded.load());
    assert.deepEqual(reloaded.snapshot(), registry.snapshot());
    assert.deepEqual(reloaded.tasks(), registry.tasks());
  });
  await check("Team API v2 reads filter tasks and receivers by access", async () => {
    const team = new TeamStore(join(root, "team.json"));
    await runCauseEffect(team.initialize());
    await runCauseEffect(team.configure("Synthetic host", "owner", "synthetic-password"));
    const admin = await runCauseEffect(team.login("owner", "synthetic-password"));
    const invite = await runCauseEffect(team.createInvite("member"));
    const member = await runCauseEffect(team.acceptInvite(invite.token, "member", "synthetic-password"));
    const serverId = team.getIdentity()?.serverId;
    assert.ok(serverId);
    const accessPath = join(root, "access.json");
    await writeFile(
      accessPath,
      JSON.stringify({
        version: 1,
        servers: { [serverId]: { revision: 1, everyone: { [hidden.id]: "none" }, roles: [] } },
      }),
    );
    const access = new AgentAccessStore(accessPath);
    await Effect.runPromise(access.load());
    const unexpected = (): never => {
      throw new Error("No browser work is permitted.");
    };
    const api = new TeamApiServer({
      store: team,
      events,
      agents,
      mailbox,
      agentAccess: access,
      appVersion: "0.62.0",
      browser: {
        listTabs: () => [],
        getControlState: unexpected,
        open: unexpected,
        activate: unexpected,
        navigate: unexpected,
        reload: unexpected,
        close: unexpected,
        capturePreview: unexpected,
        setVisible: unexpected,
        getDisplayState: unexpected,
        loadUrl: unexpected,
        isPrivate: unexpected,
        takeUserControl: unexpected,
        releaseUserControl: unexpected,
        releaseUserControlHeldBy: unexpected,
        stop: unexpected,
        importSignIn: unexpected,
        startView: unexpected,
        dispatchViewInput: unexpected,
        copyViewSelection: unexpected,
      },
    });
    const port = await Effect.runPromise(api.start());
    try {
      const get = (route: string, token: string, capabilities: string) =>
        fetch(`http://127.0.0.1:${port}${route}`, {
          headers: {
            "OpenBot-Protocol-Version": "7",
            "OpenBot-App-Version": "0.62.0",
            "OpenBot-Capabilities": capabilities,
            Authorization: `Bearer ${token}`,
          },
        });
      assert.equal(
        (await get(WATCH_REGISTRY_V2_ROUTES.snapshot, admin.sessionToken, "watch-registry-v1,agent-hidden-v1")).status,
        400,
      );
      const read = async (token: string) => {
        const response = await get(
          WATCH_REGISTRY_V2_ROUTES.snapshot,
          token,
          "watch-registry-v1,watch-registry-v2,agent-hidden-v1",
        );
        assert.equal(response.status, 200);
        const codec = WATCH_REGISTRY_V2_CODECS.get(WATCH_REGISTRY_V2_ROUTES.snapshot);
        assert.ok(codec);
        return decodeWatchingSnapshot(codec.response(200, await response.json()));
      };
      const ownerSnapshot = await read(admin.sessionToken);
      assert.deepEqual(
        ownerSnapshot.tasks?.items.map((task) => task.id),
        ["decision:1", "follow-up:2", "dispatch:3"],
      );
      assert.deepEqual(ownerSnapshot.registry?.watches[0]?.receiverAgentIds, [owner.id, helper.id, hidden.id]);
      assert.equal(ownerSnapshot.registry?.version, 2);
      const memberSnapshot = await read(member.sessionToken);
      assert.deepEqual(
        memberSnapshot.tasks?.items.map((task) => task.id),
        ["decision:1"],
      );
      assert.deepEqual(memberSnapshot.registry?.watches[0]?.receiverAgentIds, [owner.id, helper.id]);
      const v1 = await get(WATCH_REGISTRY_ROUTES.snapshot, admin.sessionToken, "watch-registry-v1,agent-hidden-v1");
      assert.equal(v1.status, 200);
      const v1Body = await v1.json();
      assert.equal(v1Body.registry.version, 1);
      assert.equal("tasks" in v1Body, false);
      assert.ok(!JSON.stringify(v1Body).includes("PR #32090 review thread"));
      decodeWatchingSnapshot(v1Body);
    } finally {
      await Effect.runPromise(api.stop());
    }
  });
  await check("Paged v2 watches merge into one list, each page in its own slot", async () => {
    const before = watchingChanges();
    const [first] = watches.watches;
    assert.ok(first);
    const page = (index: number, count: number, sequence: number, generatedAt: string, ids: string[]) => ({
      version: 2,
      sequence,
      generatedAt,
      publisherId: `openbot-control-watches-${index}`,
      page: { index, count },
      watches: ids.map((id) => ({ ...first, id, receiverAgentIds: undefined })),
    });
    const older = "2026-11-05T19:00:00.000Z";
    const newer = "2026-11-05T19:00:05.000Z";
    assert.equal(await receive(restricted, page(1, 2, 100, older, ["a"])), 400);
    assert.equal(await receive(primary, page(1, 2, 100, older, ["a"])), 202);
    assert.equal(await receive(primary, page(2, 2, 100, newer, ["b"])), 202);
    assert.equal(watchingChanges(), before + 2);
    const merged = agents.getWatchingSnapshotV2().registry;
    assert.deepEqual(merged?.watches.map((watch) => watch.id).sort(), ["a", "b"]);
    assert.ok(merged?.watches.every((watch) => watch.receiverAgentIds === undefined));
    assert.equal(merged?.sequence, 100);
    assert.equal(merged?.generatedAt, older);
    assert.equal(agents.getWatchingSnapshotV2().tasks?.items.length, 3);
    assert.equal(await receive(primary, page(2, 2, 101, newer, ["b", "c"])), 202);
    assert.equal(agents.getWatchingSnapshotV2().registry?.sequence, 101);
    assert.deepEqual(
      agents
        .getWatchingSnapshotV2()
        .registry?.watches.map((watch) => watch.id)
        .sort(),
      ["a", "b", "c"],
    );
    assert.equal(await receive(primary, page(2, 2, 100, newer, ["b"])), 400);
    for (const bad of [
      page(5, 5, 102, newer, ["a"]),
      { ...page(1, 2, 102, newer, ["a"]), page: { index: 2, count: 2 } },
      { ...page(2, 1, 102, newer, ["a"]) },
      { ...page(1, 2, 102, newer, ["a"]), publisherId: undefined },
      { ...page(1, 2, 102, newer, []), tasks },
      { ...page(1, 2, 102, newer, ["a"]), page: { index: 1, count: 2, extra: 1 } },
    ])
      assert.equal(await receive(primary, bad), 400, JSON.stringify(bad).slice(0, 160));
    assert.equal(await receive(primary, page(1, 1, 102, newer, ["a"])), 202);
    assert.deepEqual(
      agents.getWatchingSnapshotV2().registry?.watches.map((watch) => watch.id),
      ["a"],
    );
    assert.ok(
      !JSON.parse(readFileSync(path, "utf8")).publishers.some(
        (item: { publisherId: string }) => item.publisherId === "openbot-control-watches-2",
      ),
    );
    const v1 = agents.getWatchingSnapshot();
    assert.equal(v1.registry?.version, 1);
    assert.deepEqual(
      v1.registry?.watches.map((watch) => watch.id),
      ["a"],
    );
    const reloaded = new WatchRegistryStore(path, {
      knownAgents: () => new Set([owner.id, helper.id, hidden.id]),
      changed: () => {},
    });
    await Effect.runPromise(reloaded.load());
    assert.deepEqual(reloaded.snapshotV2(), registry.snapshotV2());
  });
  await check("An unpaged publication from the management route replaces the pages", async () => {
    assert.equal(await receive(primary, { ...watches, sequence: 200 }), 202);
    assert.deepEqual(
      agents.getWatchingSnapshotV2().registry?.watches.map((watch) => watch.id),
      ["pr-review"],
    );
    assert.equal(agents.getWatchingSnapshotV2().tasks?.items.length, 3);
  });
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? error.stack : String(error);
  process.exitCode = 1;
} finally {
  await runCauseEffect(agents.stop()).catch(() => {});
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  if (report.error) process.stderr.write(`${report.error}\n`);
}
