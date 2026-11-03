import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { isDynamicRecord, isNumber, isString } from "@openbot/contracts/runtime-values";
import { SIGNAL_PROTOCOL_VERSION, type SignalServerMessage } from "@openbot/contracts/signal-protocol/messages";
import { teeLogLines } from "@openbot/logging";
import { Effect } from "effect";
import { type WebSocket, WebSocketServer } from "ws";
import { SignalIngress } from "../src/main/signal-ingress";

const out = resolve(import.meta.dirname, "../.openbot-build/signal-ingress-error-log-e2e");
const report: { passed: boolean; checks: string[]; failed: string[] } = { passed: false, checks: [], failed: [] };

const lines: string[] = [];
const removeTee = teeLogLines(["signal-ingress"], (line) => lines.push(line));

const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
await new Promise<void>((done) => server.once("listening", () => done()));
const address = server.address();
assert.ok(address !== null && typeof address === "object");
const port = address.port;

interface ClientFrame {
  type: string;
  requestId: string | null;
  status: number | null;
}

function clientFrame(text: string): ClientFrame {
  const value = JSON.parse(text);
  assert.ok(isDynamicRecord(value) && isString(value.type));
  return {
    type: value.type,
    requestId: isString(value.requestId) ? value.requestId : null,
    status: isNumber(value.status) ? value.status : null,
  };
}

const frames: ClientFrame[] = [];
let peer: WebSocket | null = null;
let peerReady: (() => void) | null = null;
server.on("connection", (socket) => {
  peer = socket;
  socket.on("message", (data) => {
    const frame = clientFrame(data.toString());
    frames.push(frame);
    if (frame.type !== "hello") return;
    socket.send(
      JSON.stringify({
        type: "ready",
        version: SIGNAL_PROTOCOL_VERSION,
        connectionId: null,
        resumeToken: "resume-token",
        iceServers: [],
      }),
    );
    socket.send(JSON.stringify({ type: "webhook-ready", version: SIGNAL_PROTOCOL_VERSION }));
    peerReady?.();
  });
});

const ingress = new SignalIngress({
  hostId: () => "host-1",
  signedIn: () => true,
  issueTicket: () => Effect.succeed({ ticket: "ticket", signalUrl: `ws://127.0.0.1:${port}` }),
  issueSlackRoute: () => Effect.succeed("slack-route"),
  issueDiscordRoute: () => Effect.succeed("discord-route"),
  issueWebhookRoute: () => Effect.succeed("webhook-route"),
  issueGitHubRoute: () => Effect.succeed("github-route"),
});
ingress.handleWebhooks(() => Effect.succeed({ status: 202 as const }));

async function until(label: string, condition: () => boolean, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out: ${label}`);
    await new Promise((done) => setTimeout(done, 10));
  }
}

async function connect() {
  const ready = new Promise<void>((done) => {
    peerReady = done;
  });
  const release = ingress.acquireWebhooks();
  await ready;
  await until("ingress online", () => ingress.state() === "online");
  return release;
}

async function check(name: string, body: () => Promise<void>) {
  lines.length = 0;
  try {
    await body();
    report.checks.push(name);
    process.stdout.write(`PASS ${name}\n`);
  } catch (error) {
    report.failed.push(`${name}: ${error instanceof Error ? error.message : String(error)}`);
    process.stdout.write(`FAIL ${name}\n  ${error instanceof Error ? error.message : String(error)}\n`);
  }
}

function send(frame: SignalServerMessage) {
  assert.ok(peer, "Signal has no ingress socket");
  peer.send(JSON.stringify(frame));
}

try {
  const release = await connect();

  await check(
    "A permission_denied error is logged at info as a message Signal did not accept, and the socket stays open.",
    async () => {
      send({
        type: "error",
        version: SIGNAL_PROTOCOL_VERSION,
        code: "permission_denied",
        message: "The delivery does not belong to this peer.",
      });
      await until("error line", () => lines.some((line) => line.includes("[signal-ingress]")));
      await new Promise((done) => setTimeout(done, 100));
      const logged = lines.filter((line) => line.includes("[signal-ingress]"));
      assert.equal(logged.length, 1, logged.join("\n"));
      assert.match(
        logged[0] ?? "",
        / INFO \[signal-ingress\] Signal did not accept a message on the ingress socket\. \{"code":"permission_denied"\}$/,
      );
      assert.doesNotMatch(logged[0] ?? "", /refused/);
      assert.equal(ingress.state(), "online");
      assert.equal(peer?.readyState, 1);
    },
  );

  await check("The socket still answers a webhook delivery after a permission_denied error.", async () => {
    const before = frames.length;
    send({
      type: "webhook-delivery",
      version: SIGNAL_PROTOCOL_VERSION,
      requestId: "request-1",
      routeId: "route-1",
      timestamp: "1762200000",
      deliveryId: "delivery-1",
      signature: `sha256=${"0".repeat(64)}`,
      bodyBase64: Buffer.from("{}").toString("base64"),
    });
    await until("webhook result", () => frames.slice(before).some((frame) => frame.type === "webhook-delivery-result"));
    const result = frames.slice(before).find((frame) => frame.type === "webhook-delivery-result");
    assert.equal(result?.requestId, "request-1");
    assert.equal(result?.status, 202);
  });

  await check("An error that Signal follows with a close is still logged as a refusal at warn.", async () => {
    send({
      type: "error",
      version: SIGNAL_PROTOCOL_VERSION,
      code: "rate_limited",
      message: "Too many signal messages.",
    });
    peer?.close(1008, "Too many signal messages.");
    await until("refusal line", () => lines.some((line) => line.includes("[signal-ingress]")));
    const logged = lines.filter((line) => line.includes("[signal-ingress]"));
    assert.match(
      logged[0] ?? "",
      / WARN \[signal-ingress\] Signal refused the ingress socket\. \{"code":"rate_limited"\}$/,
    );
    await until("ingress offline", () => ingress.state() !== "online");
  });

  release();
  report.passed = report.failed.length === 0;
} finally {
  await Effect.runPromise(ingress.dispose());
  removeTee();
  for (const client of server.clients) client.terminate();
  await new Promise<void>((done) => server.close(() => done()));
  mkdirSync(out, { recursive: true });
  writeFileSync(join(out, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}
process.stdout.write(`${report.checks.length} of ${report.checks.length + report.failed.length} checks pass\n`);
if (!report.passed) process.exitCode = 1;
