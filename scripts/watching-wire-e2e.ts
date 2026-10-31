import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS } from "@openbot/contracts/ipc";
import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { CONTEXT_PRS_ROUTES } from "@openbot/contracts/team-protocol/context-prs-v1";
import { teamCapabilitiesForProtocol } from "@openbot/contracts/team-protocol/current";
import { teamSideRouteCodec } from "@openbot/contracts/team-protocol/side-routes";
import { decodeTeamProtocolV2Json } from "@openbot/contracts/team-protocol/v2";
import { WATCH_REGISTRY_ROUTES } from "@openbot/contracts/team-protocol/watch-registry-v1";
import { plugin } from "bun";
import { Effect } from "effect";
import { empty } from "../packages/contracts/src/team-protocol/admin-wire";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { AgentService } from "../src/backend/agent-service";
import { AgentStore } from "../src/backend/agent-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import type { TeamApiBrowser } from "../src/main/team-api/dependencies";
import type { TeamWebRtcChannel } from "../src/main/team-webrtc-bridge";

plugin({
  name: "watching-wire-electron-boundary",
  setup(build) {
    class Inert {}
    build.module("electron", () => ({
      loader: "object",
      exports: {
        app: { getPath: () => tmpdir(), getVersion: () => "0.54.0", isPackaged: false, on: () => {} },
        BrowserWindow: Object.assign(class extends Inert {}, { getAllWindows: () => [] }),
        MessageChannelMain: class extends Inert {},
        Notification: Object.assign(class extends Inert {}, { isSupported: () => false }),
        clipboard: {},
        dialog: {},
        ipcMain: { handle: () => {}, removeHandler: () => {}, on: () => {} },
        nativeImage: {},
        net: {},
        powerMonitor: { on: () => {} },
        safeStorage: { isEncryptionAvailable: () => false },
        screen: {},
        session: {},
        shell: { openExternal: async () => {} },
        systemPreferences: {},
        webContents: { getAllWebContents: () => [] },
      },
    }));
  },
});
const { TeamApiServer } = await import("../src/main/team-api-server");
const { TeamStore } = await import("../src/main/team-store");
const { TeamWebRtcBridge } = await import("../src/main/team-webrtc-bridge");
const { TeamWebRtcHostPeer } = await import("../src/main/team-webrtc-host-peer");
const { TeamWebRtcClientTransport } = await import("../src/main/team-webrtc-client-transport");
const { requestJson } = await import("../src/main/remote-server-http");
const { decodeWatchingSnapshotFromHost } = await import("../src/main/remote-agent-decoding");

const OUT = resolve(import.meta.dirname, "../.openbot-build/watching-wire-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-watching-wire-"));
const database = new OpenBotDatabase(join(root, "state"));
const store = new AgentStore(join(root, "state"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "state"), store.sharedRoot, database);
const team = new TeamStore(join(root, "team.json"));
const unused = () => Effect.die(new Error("This check must not start provider or browser work."));
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
const teamBrowser: TeamApiBrowser = {
  listTabs: () => [],
  getControlState: () => ({ sessions: [] }),
  open: unused,
  activate: unused,
  navigate: unused,
  reload: unused,
  stop: unused,
  takeUserControl: unused,
  releaseUserControl: () => {},
  releaseUserControlHeldBy: () => {},
  close: unused,
  capturePreview: unused,
  setVisible: unused,
  getDisplayState: () => ({ tabs: [], activeTabId: null }),
  loadUrl: unused,
  isPrivate: () => false,
  startView: unused,
  dispatchViewInput: unused,
  copyViewSelection: unused,
  importSignIn: unused,
};
class LoopbackBridge extends TeamWebRtcBridge {
  other: LoopbackBridge | null = null;
  connected: () => Effect.Effect<void> = () => Effect.void;
  readonly requests: unknown[] = [];
  override readonly start = () => Effect.void;
  override readonly prepareSignal = () => Effect.void;
  override readonly connect = () => this.connected();
  override readonly disconnect = (peerId: string) =>
    Effect.sync(() => {
      this.emit("disconnected", peerId);
      this.other?.emit("disconnected", peerId);
    });
  override readonly disconnectPeer = this.disconnect;
  override readonly send = (peerId: string, channel: TeamWebRtcChannel, data: string | ArrayBuffer) =>
    Effect.sync(() => {
      if (channel === "rpc" && typeof data === "string") {
        const frame = decodeTeamProtocolV2Json(JSON.parse(data));
        if (isDynamicRecord(frame) && frame.type === "request") this.requests.push(frame.payload);
      }
      queueMicrotask(() => this.other?.emit("data", peerId, channel, data));
    });
}
const agents = new AgentService({ store, mailbox, browser, offProviders: AGENT_PROVIDERS });
const server = new TeamApiServer({ store: team, agents, mailbox, browser: teamBrowser, appVersion: "0.53.2" });
const clientBridge = new LoopbackBridge();
const hostBridge = new LoopbackBridge();
clientBridge.other = hostBridge;
hostBridge.other = clientBridge;
const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };
let client: InstanceType<typeof TeamWebRtcClientTransport> | null = null;
let peer: InstanceType<typeof TeamWebRtcHostPeer> | null = null;
async function check(name: string, run: () => Promise<void> | void) {
  await run();
  report.checks.push(name);
  process.stdout.write(`PASS ${name}\n`);
}
try {
  await runCauseEffect(store.initialize());
  await runCauseEffect(mailbox.initialize());
  await runCauseEffect(team.initialize());
  const identity = await runCauseEffect(team.configure("Fixture", "owner", "fixture-password"));
  const invite = await runCauseEffect(team.createInvite("member"));
  const member = await runCauseEffect(team.acceptInvite(invite.token, "member", "fixture-password"));
  const port = await Effect.runPromise(server.start());
  const sessionId = randomUUID();
  const expiresAt = Date.now() + 3_600_000;
  const sessionExpiresAt = Math.floor(expiresAt / 1_000);
  let clientPublicKey = "";
  peer = new TeamWebRtcHostPeer(
    {
      bridge: hostBridge,
      store: team,
      appVersion: "0.53.2",
      transferDirectory: join(root, "host-transfers"),
      verifyClientTicket: () =>
        Effect.succeed({
          sessionId,
          hostId: identity.serverId,
          userId: `account-${member.member.id}`,
          membershipId: member.member.id,
          authEpoch: 1,
          sessionExpiresAt,
          role: "member" as const,
          clientPublicKey,
        }),
    },
    { peerId: identity.serverId, hostId: identity.serverId, localApiPort: port },
  );
  const hostPeer = peer;
  clientBridge.connected = () =>
    Effect.gen(function* () {
      yield* hostPeer.incoming({
        hostId: identity.serverId,
        connectionId: randomUUID(),
        sessionId,
        userId: `account-${member.member.id}`,
        membershipId: member.member.id,
        role: "member",
        sessionExpiresAt,
      });
      hostBridge.emit("connected", identity.serverId, {
        localFingerprint: "host-fingerprint",
        remoteFingerprint: "client-fingerprint",
      });
      clientBridge.emit("connected", identity.serverId, {
        localFingerprint: "client-fingerprint",
        remoteFingerprint: "host-fingerprint",
      });
    });
  client = new TeamWebRtcClientTransport({
    bridge: clientBridge,
    listHosts: () => Effect.succeed([]),
    startSession: () => Effect.succeed({ sessionId, hostId: identity.serverId, expiresAt }),
    issueTicket: (_session, key) => {
      clientPublicKey = key;
      return Effect.succeed({ ticket: "fixture-ticket", expiresAt, signalUrl: "wss://signal.example.test" });
    },
    endSession: () => Effect.void,
    createInvite: unused,
    listInvites: unused,
    previewInvite: unused,
    acceptInvite: unused,
    revokeInvite: unused,
    listMembers: unused,
    updateMember: unused,
    removeMember: unused,
    removeOwnedHost: unused,
    getPrincipalId: () => "fixture-principal",
    controlPlaneUrl: "https://control.example.test",
    downloadHostLogo: unused,
    transferDirectory: join(root, "client-transfers"),
  });
  client.pinHostKey(identity.serverId, identity.publicKey);
  const transport = client;
  const path = WATCH_REGISTRY_ROUTES.snapshot;
  const snapshot = { registry: null, waits: [] };
  await check("Real HTTP service and desktop decoder accept the empty snapshot", async () => {
    const actual = await Effect.runPromise(
      Effect.scoped(
        requestJson(`http://127.0.0.1:${port}`, path, decodeWatchingSnapshotFromHost, {
          token: member.sessionToken,
          protocol: 9,
          appVersion: "0.56.0",
          capabilities: teamCapabilitiesForProtocol(9),
        }),
      ),
    );
    assert.deepEqual(actual, snapshot);
  });
  await check("Native RTC bodyless GET reaches the real HTTP service and decodes its snapshot", async () => {
    assert.deepEqual(
      await Effect.runPromise(transport.request(identity.serverId, path, { timeoutMs: 5_000 })),
      snapshot,
    );
    assert.ok(
      clientBridge.requests.some((value) => {
        assert.ok(typeof value === "object" && value !== null);
        return "path" in value && value.path === path && "body" in value && JSON.stringify(value.body) === "{}";
      }),
    );
  });
  await check("Native RTC null and empty bodies use the same canonical GET", async () => {
    for (const body of [null, {}]) {
      assert.deepEqual(
        await Effect.runPromise(transport.request(identity.serverId, path, { method: "GET", body })),
        snapshot,
      );
    }
  });
  await check("Malformed or nonempty watch requests fail before a frame is sent", async () => {
    const count = clientBridge.requests.length;
    for (const body of [[], "", 0, false, { unexpected: true }]) {
      await assert.rejects(Effect.runPromise(transport.request(identity.serverId, path, { method: "GET", body })));
    }
    assert.equal(clientBridge.requests.length, count);
  });
  await check("Context PR POST and released empty-record request validation stay strict", () => {
    const prs = teamSideRouteCodec(CONTEXT_PRS_ROUTES.read);
    assert.ok(prs);
    const body = { scope: { kind: "agent", id: "fixture-agent" }, refresh: false };
    assert.deepEqual(prs.request(CONTEXT_PRS_ROUTES.read, body), body);
    for (const invalid of [undefined, null, {}, []]) assert.throws(() => prs.request(CONTEXT_PRS_ROUTES.read, invalid));
    assert.deepEqual(empty({}), {});
    for (const invalid of [undefined, null, []]) assert.throws(() => empty(invalid));
  });
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? error.message : String(error);
  throw error;
} finally {
  await Effect.runPromise(client?.stop() ?? Effect.void).catch(() => {});
  await Effect.runPromise(peer?.dispose() ?? Effect.void).catch(() => {});
  await Effect.runPromise(server.stop()).catch(() => {});
  await runCauseEffect(agents.stop()).catch(() => {});
  database.close();
  await mkdir(OUT, { recursive: true });
  await writeFile(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}
