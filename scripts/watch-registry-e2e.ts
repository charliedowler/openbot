import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { AGENT_PROVIDERS, type AgentEvent } from "@openbot/contracts/ipc";
import { WATCH_REGISTRY_CODECS, WATCH_REGISTRY_ROUTES } from "@openbot/contracts/team-protocol/watch-registry-v1";
import {
  decodeWatchingSnapshot,
  WATCH_REGISTRY_EVENT_TYPE,
  WATCH_REGISTRY_ROUTINE_EVENT_TYPE,
  type WatchRegistryPublication,
} from "@openbot/contracts/watch-registry";
import { registerSecretValue } from "@openbot/logging";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { AgentService } from "../src/backend/agent-service";
import { AgentStore } from "../src/backend/agent-store";
import { AgentWaitStore } from "../src/backend/agent-waits/agent-wait-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { AgentAccessStore } from "../src/main/agent-access-store";
import { HostEventsService } from "../src/main/host-events-service";
import { TeamApiServer } from "../src/main/team-api-server";
import { TeamStore } from "../src/main/team-store";
import { WatchRegistryStore } from "../src/main/watch-registry-store";
import { signWebhookBody } from "../src/main/webhook-security";

const OUT = resolve(import.meta.dirname, "../.openbot-build/watch-registry-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-watch-registry-"));
const userData = join(root, "state");
const path = join(root, "registry.json");
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
};
const changes: AgentEvent[] = [];
const registry = new WatchRegistryStore(path, {
  knownAgents: () => new Set(store.list().map((agent) => agent.id)),
  changed: () => agents.notifyWatchingChanged(),
});
const agents = new AgentService({
  store,
  mailbox,
  browser,
  offProviders: AGENT_PROVIDERS,
  watchRegistry: () => registry.snapshot(),
});
agents.on("event", (event) => changes.push(event));
const options = {
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
};
const events = new HostEventsService({ ...options, watchRegistry: registry });
const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };
async function check(name: string, run: () => void | Promise<void>) {
  await run();
  report.checks.push(name);
  process.stdout.write(`PASS ${name}\n`);
}
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
  const other = await create("Other");
  const save = async (name: string, eventType: string | null = WATCH_REGISTRY_ROUTINE_EVENT_TYPE, active = true) => {
    const saved = await Effect.runPromise(
      events.saveRoutine({
        owner: { kind: "agent", id: owner.id },
        name,
        instruction: "Synthetic metadata receiver.",
        active,
        timezone: "UTC",
        trigger: { kind: "webhook", eventType, filters: [] },
      }),
    );
    assert.ok(saved.secret);
    const route = agents.routineRecords.routes.list().find((route) => route.routineId === saved.routine.id);
    assert.ok(route);
    return { routeId: route.routeId, secret: saved.secret, routineId: saved.routine.id };
  };
  const primary = await save("Management registry");
  const restricted = await save("Owner registry");
  const ordinary = await save("Ordinary receiver", null);
  const inactive = await save("Inactive registry", WATCH_REGISTRY_ROUTINE_EVENT_TYPE, false);
  await writeFile(
    path,
    JSON.stringify({ version: 1, managementPublisherRouteId: primary.routeId, revision: 0, publishers: [] }),
  );
  await Effect.runPromise(registry.load());
  const publication: WatchRegistryPublication = {
    version: 1,
    sequence: 1,
    generatedAt: new Date().toISOString(),
    watches: [
      {
        id: "watch-one",
        ownerAgentId: owner.id,
        sourceKind: "github",
        title: "Pull request activity",
        targetUrl: "https://example.test/pulls/1",
        status: "active",
      },
    ],
  };
  const ingress = (
    route = primary,
    data: unknown = publication,
    overrides: { secret?: string; timestamp?: string; type?: string; raw?: Uint8Array } = {},
  ) => {
    const timestamp = overrides.timestamp ?? String(Math.floor(Date.now() / 1000));
    const deliveryId = randomUUID();
    const body =
      overrides.raw ??
      new TextEncoder().encode(JSON.stringify({ type: overrides.type ?? WATCH_REGISTRY_EVENT_TYPE, data }));
    return {
      routeId: route.routeId,
      deliveryId,
      timestamp,
      body,
      signature: signWebhookBody(overrides.secret ?? route.secret, timestamp, deliveryId, body),
    };
  };
  const receive = async (route = primary, data: unknown = publication, overrides: Parameters<typeof ingress>[2] = {}) =>
    (await Effect.runPromise(events.receive(ingress(route, data, overrides)))).status;
  const work = countWork();
  await check("Signed metadata persists once without queued AI work", async () => {
    assert.equal(await receive(), 202);
    assert.equal(registry.snapshot()?.watches[0]?.publisherId, primary.routeId);
    assert.equal(changes.filter((event) => event.type === "watching-changed").length, 1);
    assert.deepEqual(countWork(), work);
    assert.equal(statSync(path).mode & 0o777, 0o600);
  });
  await check("Exact retry is unchanged and stale or conflicting sequences are refused", async () => {
    assert.equal(await receive(), 200);
    assert.equal(await receive(primary, { ...publication, generatedAt: new Date(0).toISOString() }), 400);
    assert.equal(await receive(primary, { ...publication, sequence: 2 }), 202);
    assert.equal(await receive(), 400);
    assert.equal(registry.snapshot()?.sequence, 2);
    assert.equal(changes.filter((event) => event.type === "watching-changed").length, 2);
  });
  await check("HMAC and timestamp failures cannot change metadata", async () => {
    assert.equal(await receive(primary, { ...publication, sequence: 3 }, { secret: "wrong-secret" }), 401);
    assert.equal(await receive(primary, { ...publication, sequence: 3 }, { timestamp: "1" }), 401);
    assert.equal(registry.snapshot()?.sequence, 2);
  });
  await check("Strict bounded metadata rejects unknown fields, credentials and oversized publications", async () => {
    for (const bad of [
      { ...publication, extra: "secret" },
      { ...publication, generatedAt: "2026-02-30T00:00:00.000Z" },
      { ...publication, watches: [{ ...publication.watches[0], title: "bad\nmetadata" }] },
      { ...publication, watches: [{ ...publication.watches[0], targetUrl: "https://example.test/pull?token=secret" }] },
      { ...publication, watches: [{ ...publication.watches[0], targetUrl: "https://user:pass@example.test/pull" }] },
      {
        ...publication,
        watches: Array.from({ length: 201 }, (_, i) => ({ ...publication.watches[0], id: `watch-${i}` })),
      },
    ])
      assert.equal(await receive(primary, bad), 400);
    assert.equal(await receive(primary, null, { raw: new Uint8Array(65_537) }), 413);
  });
  await check("Only an active dedicated sink can receive registry metadata", async () => {
    assert.equal(await receive(ordinary), 400);
    assert.equal(await receive(inactive), 404);
    assert.equal((await Effect.runPromise(new HostEventsService(options).receive(ingress()))).status, 400);
  });
  const legacyPath = process.argv
    .find((argument) => argument.startsWith("--legacy-host-events="))
    ?.slice("--legacy-host-events=".length);
  if (legacyPath)
    await check("Previous signed handler ignores the dedicated sink without queueing AI", async () => {
      const legacyModule: typeof import("../src/main/host-events-service") = await import(
        pathToFileURL(resolve(legacyPath)).href
      );
      const legacy = new legacyModule.HostEventsService(options);
      const before = countWork();
      assert.equal((await Effect.runPromise(legacy.receive(ingress()))).status, 202);
      assert.deepEqual(countWork(), before);
      const activity = await Effect.runPromise(
        legacy.listActivity({ owner: { kind: "agent", id: owner.id }, routineId: primary.routineId }),
      );
      assert.equal(activity[0]?.status, "ignored");
      assert.equal(activity[0]?.reason, "event-type");
    });
  await check("Publisher ownership and management authority follow authenticated routes", async () => {
    assert.equal(
      await receive(restricted, { ...publication, watches: [{ ...publication.watches[0], ownerAgentId: other.id }] }),
      400,
    );
    assert.equal(
      await receive(restricted, {
        ...publication,
        watches: [{ ...publication.watches[0], ownerAgentId: "unknown-agent" }],
      }),
      400,
    );
    assert.equal(await receive(restricted), 202);
    assert.equal(
      await receive(primary, {
        ...publication,
        sequence: 3,
        watches: [{ ...publication.watches[0], ownerAgentId: other.id }],
      }),
      202,
    );
    assert.equal(agents.getWatchingSnapshot(new Set([owner.id])).registry?.watches.length, 1);
    assert.equal(agents.getWatchingSnapshot(new Set()).registry?.watches.length, 0);
  });
  await check("Known signing secrets are removed from persisted display fields", async () => {
    const secret = "synthetic-secret-value-for-registry";
    registerSecretValue(secret);
    assert.equal(
      await receive(primary, {
        ...publication,
        sequence: 4,
        watches: [
          {
            ...publication.watches[0],
            title: `Watch ${secret}`,
            lastError: `Check failed ${secret}`,
            condition: `Until ${secret}`,
          },
        ],
      }),
      202,
    );
    assert.ok(!readFileSync(path, "utf8").includes(secret));
    assert.ok(!JSON.stringify(agents.getWatchingSnapshot()).includes(secret));
    for (const field of ["id", "sourceKind", "targetUrl"] as const) {
      const value = field === "targetUrl" ? `https://example.test/${secret}` : secret;
      assert.equal(
        await receive(primary, {
          ...publication,
          sequence: 5,
          watches: [{ ...publication.watches[0], [field]: value }],
        }),
        400,
      );
    }
  });
  const waits = new AgentWaitStore({ database });
  const native = waits.open({
    agentId: owner.id,
    requestMessageId: "native-request",
    requestText: "private complete request",
    note: "Watch line one\nWatch line two\tmore\0",
    routineId: null,
  });
  const channel = agents.channels.store.create("synthetic-channel", {
    name: "Group",
    title: "Group",
    instructions: "",
    members: [{ agentId: owner.id }, { agentId: other.id }],
    leadAgentId: owner.id,
  });
  agents.channels.store.update(channel, {
    tasks: [
      {
        id: "task",
        channelId: channel.id,
        parentTaskId: null,
        rootTaskId: "task",
        ownerAgentId: owner.id,
        requestMessageId: "request",
        instruction: "Synthetic task",
        attachmentDraftIds: [],
        expectedResult: "Wait",
        sourceMessageIds: [],
        dependencies: [],
        resources: [],
        state: "waiting",
        revision: 1,
        assignmentCount: 1,
        error: null,
      },
    ],
    assignments: [
      {
        id: "assignment",
        channelId: channel.id,
        taskId: "task",
        agentId: owner.id,
        taskRevision: 1,
        requestMessageId: "request",
        resources: [],
        deliveryId: "channel-request",
        turnId: null,
        state: "completed",
        throughSequence: 0,
        summaryVersion: 0,
        awaitedTaskIds: [],
        pendingRevision: null,
        pendingOutcome: null,
      },
    ],
  });
  new AgentWaitStore({ database, createId: () => "channel:synthetic-wait" }).open({
    agentId: owner.id,
    requestMessageId: "channel-request",
    requestText: "private group request",
    note: "Private group note",
    routineId: null,
  });
  new AgentWaitStore({ database, createId: () => "channel:unresolved-wait" }).open({
    agentId: owner.id,
    requestMessageId: "deleted-channel-request",
    requestText: "deleted group request",
    note: "Deleted private group note",
    routineId: null,
  });
  await check("Multiline native notes decode while full requests stay private", () => {
    const snapshot = decodeWatchingSnapshot(agents.getWatchingSnapshot());
    assert.equal(snapshot.waits.find((wait) => wait.id === native.id)?.note, native.note.replaceAll("\0", "\uFFFD"));
    assert.ok(!JSON.stringify(snapshot).includes("private complete request"));
    const codec = WATCH_REGISTRY_CODECS.get(WATCH_REGISTRY_ROUTES.snapshot);
    assert.ok(codec);
    assert.deepEqual(decodeWatchingSnapshot(codec.response(200, snapshot)), snapshot);
  });
  await check("Channel waits require a resolvable origin visible to every member", () => {
    const all = agents.getWatchingSnapshot();
    assert.equal(all.waits.find((wait) => wait.id === "channel:synthetic-wait")?.channelId, channel.id);
    assert.ok(!all.waits.some((wait) => wait.id === "channel:unresolved-wait"));
    const restrictedSnapshot = agents.getWatchingSnapshot(new Set([owner.id]));
    assert.ok(!restrictedSnapshot.waits.some((wait) => wait.id.startsWith("channel:")));
    assert.ok(!JSON.stringify(restrictedSnapshot).includes("Private group note"));
  });
  await check("Authenticated Team API requires capability and filters private group origins", async () => {
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
        servers: { [serverId]: { revision: 1, everyone: { [other.id]: "none" }, roles: [] } },
      }),
    );
    const access = new AgentAccessStore(accessPath);
    await Effect.runPromise(access.load());
    const unexpected = (): never => {
      throw new Error("No browser work is permitted.");
    };
    const api = new TeamApiServer({
      store: team,
      agents,
      mailbox,
      agentAccess: access,
      appVersion: "0.53.2",
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
      const get = (token?: string, capability = true) =>
        fetch(`http://127.0.0.1:${port}/v1/watch-registry`, {
          headers: {
            "OpenBot-Protocol-Version": "7",
            "OpenBot-App-Version": "0.53.2",
            "OpenBot-Capabilities": capability ? "watch-registry-v1,agent-hidden-v1" : "agent-hidden-v1",
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
        });
      assert.equal((await get()).status, 401);
      assert.equal((await get(admin.sessionToken, false)).status, 400);
      const ownerResponse = await get(admin.sessionToken);
      assert.equal(ownerResponse.status, 200);
      assert.ok(decodeWatchingSnapshot(await ownerResponse.json()).waits.some((wait) => wait.channelId === channel.id));
      const memberResponse = await get(member.sessionToken);
      assert.equal(memberResponse.status, 200);
      const memberSnapshot = decodeWatchingSnapshot(await memberResponse.json());
      assert.ok(!memberSnapshot.waits.some((wait) => wait.id.startsWith("channel:")));
      assert.ok(memberSnapshot.registry?.watches.every((watch) => watch.ownerAgentId === owner.id));
      assert.ok(!JSON.stringify(memberSnapshot).includes("Private group note"));
    } finally {
      await Effect.runPromise(api.stop());
    }
  });
  await check("Deleting the origin cannot relabel a group wait as an own-chat wait", () => {
    agents.channels.store.delete(channel.id);
    const snapshot = agents.getWatchingSnapshot();
    assert.ok(!snapshot.waits.some((wait) => wait.id.startsWith("channel:")));
    assert.ok(!JSON.stringify(snapshot).includes("Private group note"));
    assert.equal(snapshot.waits.length, 1);
  });
  await check("Native wait changes invalidate the Watching snapshot without AI work", () => {
    const before = changes.filter((event) => event.type === "watching-changed").length;
    agents.cancelWait(owner.id, native.id);
    assert.equal(agents.getWatchingSnapshot().waits.length, 0);
    assert.equal(changes.filter((event) => event.type === "watching-changed").length, before + 1);
    assert.deepEqual(countWork(), work);
  });
  await check("Persisted publication reloads and future settings stay untouched", async () => {
    const reloaded = new WatchRegistryStore(path, {
      knownAgents: () => new Set([owner.id, other.id]),
      changed: () => {
        throw new Error("A read cannot notify.");
      },
    });
    await Effect.runPromise(reloaded.load());
    assert.deepEqual(reloaded.snapshot(), registry.snapshot());
    const futurePath = join(root, "future.json");
    const bytes = '{"version":2,"future":"preserve"}';
    await writeFile(futurePath, bytes);
    const future = new WatchRegistryStore(futurePath, { knownAgents: () => new Set([owner.id]), changed: () => {} });
    await Effect.runPromise(future.load());
    assert.equal(
      await Effect.runPromise(future.accept({ publisherId: primary.routeId, ownerAgentId: owner.id }, publication)),
      503,
    );
    assert.equal(readFileSync(futurePath, "utf8"), bytes);
  });
  await check("Legacy exact event-type filtering ignores metadata without a routine run", async () => {
    const before = countWork();
    const result = await runCauseEffect(
      agents.routineRecords.receiveWebhook({ kind: "agent", id: owner.id }, primary.routineId, {
        deliveryId: randomUUID(),
        eventType: WATCH_REGISTRY_EVENT_TYPE,
        data: JSON.parse(JSON.stringify(publication)),
        occurredAt: new Date().toISOString(),
        receivedAt: new Date().toISOString(),
      }),
    );
    assert.equal(result.kind, "ignored");
    assert.deepEqual(countWork(), before);
  });
  await check("Ordinary webhooks still enqueue their matching routine", async () => {
    assert.deepEqual(countWork(), work);
    assert.equal(await receive(ordinary, { value: "ordinary" }, { type: "bridge.event" }), 202);
    assert.notDeepEqual(countWork(), work);
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
