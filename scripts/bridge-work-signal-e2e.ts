import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { SIGNAL_MESSAGE_BYTES_LIMIT } from "@openbot/contracts/signal-protocol/messages";
import {
  WEBHOOK_DELIVERY_ID_HEADER,
  WEBHOOK_EVENTS_PATH,
  WEBHOOK_SIGNATURE_HEADER,
  WEBHOOK_TIMESTAMP_HEADER,
} from "@openbot/contracts/signal-protocol/webhook-route";
import { Effect } from "effect";
import { SignalIngress } from "../src/main/signal-ingress";
import { signWebhookBody, verifyWebhookSignature } from "../src/main/webhook-security";

const ROOT = resolve(import.meta.dirname, "..");
const OUT = join(ROOT, ".openbot-build/bridge-work-signal-e2e");
const ROUTE = "bridge-work-signal-fixture";
const SECRET = `whsec_${randomBytes(32).toString("base64url")}`;
const report: { passed: boolean; checks: string[]; failed: string[]; frames: Frame[] } = {
  passed: false,
  checks: [],
  failed: [],
  frames: [],
};
interface Frame {
  deliveryId: string;
  status: number;
  bytes: number;
  hasBody: boolean;
}

const REMOTE_FIXTURE = `
import { Effect, ManagedRuntime } from "effect";
import { createRemoteApiApp } from "./remote/api/src/app.ts";
import { readRemoteApiConfig } from "./remote/api/src/config.ts";
import { SignalEntry } from "./remote/api/src/entry.ts";
import { SignalService } from "./remote/api/src/signal-service.ts";
import { TurnRelays } from "./remote/api/src/turn-relays.ts";
const config = readRemoteApiConfig({
  REMOTE_TLS_DISABLED: "true",
  REMOTE_TICKET_PUBLIC_KEYS: '{"keys":[]}',
  REMOTE_CONTROL_PLANE_URL: "http://127.0.0.1:1",
  REMOTE_SESSION_SECRET: "fixture-session-secret-000000000000000000",
  REMOTE_AUTH_WEBHOOK_SECRET: "fixture-auth-secret-00000000000000000000",
  TURN_SHARED_SECRET: "fixture-turn-secret-00000000000000000000",
  TURN_HOST: "127.0.0.1",
});
const now = Math.floor(Date.now() / 1000);
const claims = {
  aud: "openbot-remote", jti: "fixture-ticket", sessionId: "fixture-session", hostId: "fixture-host",
  userId: "fixture-user", membershipId: "fixture-member", role: "host", authEpoch: 1,
  protocolMinimum: 2, protocolMaximum: 2, sessionExpiresAt: now + 3600, iat: now, exp: now + 3600,
};
const tokens = {
  verifyTicket: () => Effect.succeed(claims),
  verifyResumeToken: () => Effect.succeed(claims),
  validateClaims: () => Effect.succeed(true),
  issueResumeToken: () => Effect.succeed("fixture-resume"),
  iceServers: () => [],
  verifyWebhookRoute: () => Effect.succeed({ routes: [{ id: ${JSON.stringify(ROUTE)}, linkedAt: 0 }] }),
  validateWebhookRoute: () => Effect.succeed([${JSON.stringify(ROUTE)}]),
};
const signal = new SignalService(tokens, 32);
const deliveries = new Map();
const connect = signal.connect.bind(signal);
const observedSocket = (socket) => ({
  ...socket,
  send: (text) => {
    const frame = JSON.parse(text);
    if (frame.type === "webhook-delivery") deliveries.set(frame.requestId, frame.deliveryId);
    socket.send(text);
  },
});
signal.connect = (socket) => connect(observedSocket(socket));
const receive = signal.receive;
signal.receive = (socket, input) => {
  const text = typeof input === "string" ? input : new TextDecoder().decode(input);
  const frame = JSON.parse(text);
  if (frame.type === "webhook-delivery-result") process.stdout.write(JSON.stringify({
    type: "frame", deliveryId: deliveries.get(frame.requestId), status: frame.status,
    bytes: new TextEncoder().encode(text).byteLength, hasBody: frame.body !== undefined,
  }) + "\\n");
  return receive(observedSocket(socket), input);
};
const runtime = ManagedRuntime.make(signal.dependencies);
const app = createRemoteApiApp(config, signal, runtime, new TurnRelays(config), new SignalEntry(config));
app.listen({ hostname: "127.0.0.1", port: 0 });
process.stdout.write(JSON.stringify({ type: "ready", port: app.server.port }) + "\\n");
process.on("SIGTERM", async () => {
  signal.close();
  await app.stop();
  await runtime.dispose();
  process.exit(0);
});
`;

let stderr = "";
let port: number | null = null;
let ready: (() => void) | null = null;
const child = spawn("bun", ["--eval", REMOTE_FIXTURE], {
  cwd: ROOT,
  env: { PATH: process.env.PATH, TZ: "UTC" },
  stdio: ["ignore", "pipe", "pipe"],
});
const childErrors: string[] = [];
child.on("error", (error) => childErrors.push(error.message));
child.stderr.on("data", (chunk) => {
  stderr = (stderr + chunk.toString()).slice(-8192);
});
const lines = createInterface({ input: child.stdout });
lines.on("line", (line) => {
  let value: unknown;
  try {
    value = JSON.parse(line);
  } catch {
    return;
  }
  if (!isDynamicRecord(value)) return;
  if (value.type === "ready" && typeof value.port === "number") {
    port = value.port;
    ready?.();
  }
  if (
    value.type === "frame" &&
    typeof value.deliveryId === "string" &&
    typeof value.status === "number" &&
    typeof value.bytes === "number" &&
    typeof value.hasBody === "boolean"
  )
    report.frames.push({
      deliveryId: value.deliveryId,
      status: value.status,
      bytes: value.bytes,
      hasBody: value.hasBody,
    });
});

let ingress: SignalIngress | null = null;
let release: (() => void) | null = null;
const observation = {
  version: 1,
  generatedAt: "2026-11-11T20:00:00.000Z",
  observations: [
    {
      deliveryId: "probe-signed-id",
      recipient: { agentId: "fixture-agent", name: 'Fixture "agent"\n界' },
      nativeStatus: "completed",
      turnId: "fixture-turn",
      startedAt: "2026-11-11T19:59:58.000Z",
      terminalAt: "2026-11-11T19:59:59.000Z",
      executor: { classification: "none", completedCalls: 1, failedCalls: 0 },
    },
  ],
};
const bodies = new Map([
  ["observation", JSON.stringify(observation)],
  ["unicode", JSON.stringify({ value: "界".repeat(1000) })],
  ["quotes", '"'.repeat(32768)],
  ["controls", "\u0000".repeat(12000)],
]);

async function until(label: string, condition: () => boolean) {
  const deadline = Date.now() + 5000;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error(`Timed out: ${label}. ${stderr} ${childErrors.join(" ")}`);
    await new Promise((done) => setTimeout(done, 10));
  }
}

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

async function deliver(deliveryId: string) {
  assert.ok(port);
  const body = new TextEncoder().encode(
    JSON.stringify({ type: "bridge.work.observe", data: { deliveryIds: [deliveryId] } }),
  );
  const timestamp = String(Math.floor(Date.now() / 1000));
  const response = await fetch(`http://127.0.0.1:${port}${WEBHOOK_EVENTS_PATH}/${ROUTE}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      [WEBHOOK_DELIVERY_ID_HEADER]: deliveryId,
      [WEBHOOK_TIMESTAMP_HEADER]: timestamp,
      [WEBHOOK_SIGNATURE_HEADER]: signWebhookBody(SECRET, timestamp, deliveryId, body),
    },
    body,
    signal: AbortSignal.timeout(5000),
  });
  const text = await response.text();
  await until("captured ingress result", () => report.frames.some((frame) => frame.deliveryId === deliveryId));
  const frame = report.frames.find((item) => item.deliveryId === deliveryId);
  assert.ok(frame);
  assert.ok(frame.bytes <= SIGNAL_MESSAGE_BYTES_LIMIT);
  return { response, text, frame };
}

try {
  await new Promise<void>((done, reject) => {
    const timeout = setTimeout(() => reject(new Error(`Signal fixture did not start. ${stderr}`)), 10000);
    ready = () => {
      clearTimeout(timeout);
      done();
    };
    if (port !== null) ready();
  });
  ingress = new SignalIngress({
    hostId: () => "fixture-host",
    signedIn: () => true,
    issueTicket: () => Effect.succeed({ ticket: "fixture-ticket", signalUrl: `ws://127.0.0.1:${port}/v1/signal` }),
    issueSlackRoute: () => Effect.succeed("fixture-unused"),
    issueDiscordRoute: () => Effect.succeed("fixture-unused"),
    issueWebhookRoute: () => Effect.succeed("fixture-webhook-route"),
    issueGitHubRoute: () => Effect.succeed("fixture-unused"),
  });
  ingress.handleWebhooks((delivery) =>
    Effect.sync(() => {
      verifyWebhookSignature(SECRET, delivery, delivery.signature);
      const body = bodies.get(delivery.deliveryId);
      return {
        status: delivery.deliveryId === "legacy-202" ? (202 as const) : (200 as const),
        ...(body === undefined ? {} : { body }),
      };
    }),
  );
  release = ingress.acquireWebhooks();
  await until("ingress online", () => ingress?.state() === "online");
  await check("observation survives host ingress, remote Signal and HTTP JSON", async () => {
    const { response, text, frame } = await deliver("observation");
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "application/json");
    assert.equal(response.headers.get("cache-control"), "no-store");
    assert.deepEqual(JSON.parse(text), observation);
    assert.equal(frame.hasBody, true);
  });
  await check("multibyte response survives Signal unchanged", async () => {
    const { response, text } = await deliver("unicode");
    assert.equal(response.status, 200);
    assert.ok(text === bodies.get("unicode"), "The HTTP body differs from the host result.");
  });
  for (const deliveryId of ["quotes", "controls"])
    await check(`${deliveryId} overflow the full frame and return 413`, async () => {
      const body = bodies.get(deliveryId);
      assert.ok(body);
      assert.ok(Buffer.byteLength(body) <= 32768);
      assert.ok(Buffer.byteLength(JSON.stringify({ body })) > SIGNAL_MESSAGE_BYTES_LIMIT);
      const { response, text, frame } = await deliver(deliveryId);
      assert.equal(response.status, 413);
      assert.equal(text, "");
      assert.equal(frame.status, 413);
      assert.equal(frame.hasBody, false);
      assert.equal(ingress?.state(), "online");
    });
  for (const status of [200, 202])
    await check(`status-only ${status} host remains compatible`, async () => {
      const { response, text, frame } = await deliver(`legacy-${status}`);
      assert.equal(response.status, status);
      assert.equal(text, "");
      assert.equal(frame.hasBody, false);
    });
} catch (error) {
  report.failed.push(String(error));
} finally {
  release?.();
  if (ingress) await Effect.runPromise(ingress.dispose());
  child.kill("SIGTERM");
  await until("Signal fixture exits", () => child.exitCode !== null || child.signalCode !== null).catch(() => {
    child.kill("SIGKILL");
  });
  lines.close();
  report.passed = report.failed.length === 0;
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}
process.stdout.write(`${report.checks.length} passed, ${report.failed.length} failed\n`);
if (!report.passed) process.exitCode = 1;
