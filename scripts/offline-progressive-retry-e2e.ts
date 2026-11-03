import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { mock } from "node:test";
import { Effect } from "effect";
import { RemoteEventStream, type RemoteEventStreamOptions } from "../src/main/remote-server-event-stream";
import type { StoredRemoteServerView } from "../src/main/remote-server-store";

const realSetTimeout = globalThis.setTimeout;
const realClearTimeout = globalThis.clearTimeout;
const pending = new Map<number, { delay: number; run: () => void }>();
let nextTimer = 1;
mock.method(globalThis, "setTimeout", (run: (...args: unknown[]) => void, delay = 0, ...args: unknown[]) => {
  if (delay < 1_000) return realSetTimeout(run, delay, ...args);
  const id = nextTimer++;
  pending.set(id, { delay, run });
  return id;
});
mock.method(globalThis, "clearTimeout", (id: Parameters<typeof clearTimeout>[0]) => {
  if (typeof id === "number" && pending.delete(id)) return;
  realClearTimeout(id);
});

let random = 0.5;
mock.method(Math, "random", () => random);

const hostId = "host";
const server: StoredRemoteServerView = {
  id: hostId,
  name: "Host",
  apiUrl: "https://host.invalid",
  fingerprint: "fingerprint",
  username: "user",
  encryptedToken: "",
  remoteDesktopAvailable: false,
  role: "owner",
  transport: "webrtc-v2",
};
const unused = () => Effect.die("The offline retry fixture does not use this dependency");
let listed = false;
const options: RemoteEventStreamOptions = {
  appVersion: "0.58.0",
  servers: {
    activeServerId: hostId,
    get servers() {
      return listed ? [server] : [];
    },
    find: (id) => (id === hostId ? server : null),
    has: (id) => id === hostId,
    require: () => server,
    token: () => "token",
  },
  client: { ensureCompatibility: unused, requestProtocol: () => ({}) },
  connections: {
    markConnected() {},
    setState() {},
    setCompatibility() {},
    compatibilityFor: () => null,
    reportError() {},
    reportUnreachable() {},
  },
  agents: { refreshAgentState: unused, forward: unused },
  transport: { connect: () => Effect.void, requestRuntimeSnapshot: () => Effect.void },
  onServerIdentity() {},
  onPresence() {},
  onDirectMessage() {},
  onDirectTyping() {},
  onHostRestart() {},
  onMachineAlert() {},
  onOffline() {},
  onChanged() {},
};

function onlyTimer(): { id: number; delay: number; run: () => void } {
  assert.equal(pending.size, 1, `expected one reconnect timer, found ${pending.size}`);
  const [entry] = pending.entries();
  assert.ok(entry);
  return { id: entry[0], ...entry[1] };
}

function fire(): void {
  const timer = onlyTimer();
  pending.delete(timer.id);
  timer.run();
}

function offlineCycle(stream: RemoteEventStream): number {
  stream.markHostOffline(hostId);
  stream.scheduleReconnect(hostId);
  const { delay } = onlyTimer();
  fire();
  return delay;
}

async function fresh(): Promise<RemoteEventStream> {
  pending.clear();
  listed = false;
  const stream = new RemoteEventStream(options);
  await Effect.runPromise(stream.start());
  listed = true;
  return stream;
}

const minute = 60_000;

const focusedStream = await fresh();
const focused = Array.from({ length: 7 }, () => offlineCycle(focusedStream));
assert.deepEqual(focused, [15_000, 30_000, 60_000, 2 * minute, 4 * minute, 5 * minute, 5 * minute]);
focusedStream.clearReconnectBackoff(hostId);
const afterConnect = offlineCycle(focusedStream);
assert.equal(afterConnect, 15_000, "a successful connect starts the offline delay again at fifteen seconds");
await Effect.runPromise(focusedStream.stop());

const unfocusedStream = await fresh();
await Effect.runPromise(unfocusedStream.setAppFocused(false));
const unfocused = Array.from({ length: 8 }, () => offlineCycle(unfocusedStream));
assert.deepEqual(unfocused, [15_000, 30_000, 60_000, 2 * minute, 4 * minute, 8 * minute, 15 * minute, 15 * minute]);
await Effect.runPromise(unfocusedStream.stop());

const jitterStream = await fresh();
random = 1;
const high = offlineCycle(jitterStream);
random = 0;
const low = offlineCycle(jitterStream);
random = 1;
for (let attempt = 0; attempt < 4; attempt++) offlineCycle(jitterStream);
const capped = offlineCycle(jitterStream);
random = 0.5;
assert.equal(high, 18_000, "jitter adds up to a fifth of the offline delay");
assert.equal(low, 24_000, "jitter takes up to a fifth off the offline delay");
assert.equal(capped, 5 * minute, "jitter never passes the offline cap");
await Effect.runPromise(jitterStream.stop());

const onlineStream = await fresh();
const online: number[] = [];
for (let attempt = 0; attempt < 8; attempt++) {
  onlineStream.scheduleReconnect(hostId);
  online.push(onlyTimer().delay);
  fire();
}
assert.deepEqual(online, [1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000]);
onlineStream.clearReconnectBackoff(hostId);
onlineStream.scheduleReconnect(hostId);
assert.equal(onlyTimer().delay, 1_000);
onlineStream.markHostOffline(hostId);
const offlineOverPending = onlyTimer().delay;
assert.equal(offlineOverPending, 30_000, "an offline mark replaces a shorter pending retry");
await Effect.runPromise(onlineStream.stop());

const report = { focused, afterConnect, unfocused, jitter: { high, low, capped }, online, offlineOverPending };
const out = resolve(import.meta.dirname, "../.openbot-build/offline-progressive-retry-e2e");
mkdirSync(out, { recursive: true });
writeFileSync(resolve(out, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
process.stdout.write(`${JSON.stringify(report)}\n`);
