import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { CentralAuthState } from "@openbot/contracts/ipc";
import { AGENT_PROVIDERS } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { getString, isRecord } from "../src/backend/protocol";
import { ServerMode, type ServerModeOptions } from "../src/main/server-mode";

const OUT = resolve(import.meta.dirname, "../.openbot-build/native-idle-compaction-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-compaction-queue-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const logPath = join(root, "protocol.jsonl");
const controlPath = join(root, "control.json");
const fakeCodex = join(root, "codex");
const COMPACTION_TIMEOUT_MS = 1_000;
process.env.HOME = home;
process.env.CODEX_HOME = join(home, ".codex");
process.env.OPENBOT_CODEX_PATH = fakeCodex;
process.env.OPENBOT_FAKE_CODEX_LOG = logPath;
process.env.OPENBOT_FAKE_CODEX_CONTROL = controlPath;
mkdirSync(home, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

const FAKE_CODEX = `#!/usr/bin/env node
const fs = require("node:fs");
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n");
  process.exit(0);
}
const log = (entry) => fs.appendFileSync(process.env.OPENBOT_FAKE_CODEX_LOG, JSON.stringify(entry) + "\\n");
const control = () => JSON.parse(fs.readFileSync(process.env.OPENBOT_FAKE_CODEX_CONTROL, "utf8"));
const config = { config: { mcp_servers: {} }, layers: [{ name: { type: "user", file: "/fake/.codex/config.toml", profile: null }, version: "1", config: { mcp_servers: {} } }] };
let buffer = "";
let threads = 0;
let turns = 0;

const compacting = new Map();
process.stdout.on("error", (error) => {
  if (error.code === "EPIPE") process.exit(0);
  throw error;
});
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const answer = (message) => {
  const params = message.params || {};
  switch (message.method) {
    case "initialize": return write({ id: message.id, result: {} });
    case "account/login/start": return write({ id: message.id, result: { type: "chatgptDeviceCode", loginId: "fixture-login", userCode: "fixture-code", verificationUrl: "https://example.com/login" } });
    case "account/login/cancel": return write({ id: message.id, result: {} });
    case "account/read": return write({ id: message.id, result: { account: { type: "chatgpt", email: "codex@example.com" } } });
    case "account/rateLimits/read": return write({ id: message.id, result: { rateLimits: { limitId: "codex", primary: null, secondary: null }, rateLimitsByLimitId: null } });
    case "model/list": return write({ id: message.id, result: { data: [{ model: "gpt-5.5", displayName: "GPT-5.5" }] } });
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
      if (compacting.has(params.threadId)) {
        log({ refused: params.threadId });
        return write({ id: message.id, error: { code: -32603, message: "failed to submit turn input: ActiveTurnNotSteerable { turn_kind: Compact }" } });
      }
      const turnId = "turn-" + (++turns);
      write({ id: message.id, result: { turn: { id: turnId, status: "inProgress", items: [] } } });
      write({ method: "turn/started", params: { threadId: params.threadId, turn: { id: turnId } } });
      write({ method: "thread/tokenUsage/updated", params: { threadId: params.threadId, turnId, tokenUsage: { total: { totalTokens: 1000 }, last: { totalTokens: 1000 }, modelContextWindow: 100000 } } });
      setTimeout(() => {
        const item = { type: "agentMessage", id: "message-" + turnId, text: "Done.", phase: "final_answer" };
        write({ method: "item/completed", params: { threadId: params.threadId, turnId, item } });
        write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: turnId, status: "completed" } } });
        const input = (params.input || []).map((item) => item.text || "").join(" ");
        if (input.includes("NATIVE-HIDDEN-COMPACTION")) setTimeout(() => {
          const nativeId = "native-compact-" + turnId;
          const item = { type: "contextCompaction", id: nativeId };
          log({ method: "fixture/native-compaction-running", threadId: params.threadId });
          write({ method: "item/started", params: { threadId: params.threadId, turnId: nativeId, item } });
          setTimeout(() => {
            write({ method: "item/completed", params: { threadId: params.threadId, turnId: nativeId, item } });
            log({ method: "fixture/native-compaction-completed", threadId: params.threadId });
          }, 1000);
        }, 25);
      }, 50);
      return;
    }
    case "thread/compact/start": {
      const { compactionMs, ignoreInterrupt } = control();
      const turnId = "compact-turn-" + (++turns);
      const item = { type: "contextCompaction", id: "compact-item-" + turnId };
      write({ id: message.id, result: {} });
      write({ method: "turn/started", params: { threadId: params.threadId, turn: { id: turnId } } });
      write({ method: "item/started", params: { threadId: params.threadId, turnId, item } });
      const finish = (status) => {
        if (compacting.get(params.threadId)?.turnId !== turnId) return;
        compacting.delete(params.threadId);
        clearTimeout(timer);
        log({ compactionEnded: turnId, threadId: params.threadId, status });
        if (status === "completed") write({ method: "item/completed", params: { threadId: params.threadId, turnId, item } });
        write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: turnId, status } } });
      };
      const timer = setTimeout(() => finish("completed"), compactionMs);
      compacting.set(params.threadId, { turnId, finish, ignoreInterrupt });
      return;
    }
    case "turn/interrupt": {
      write({ id: message.id, result: {} });
      const compaction = compacting.get(params.threadId);
      if (compaction?.turnId === params.turnId) {
        if (!compaction.ignoreInterrupt) compaction.finish("interrupted");
        return;
      }
      return write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: params.turnId, status: "interrupted" } } });
    }
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
      log({ method: message.method ?? null, id: message.id ?? null, params: message.params ?? null });
      if (message.method && message.id !== undefined) answer(message);
    }
    newline = buffer.indexOf("\\n");
  }
});
`;

interface ProtocolEntry {
  method: string | null;
  threadId: string | null;
  turnId: string | null;
  refused: string | null;
  compactionEnded: string | null;
  status: string | null;
}

const report: { passed: boolean; checks: string[]; error?: string } = { passed: false, checks: [] };

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
};

function protocol(): ProtocolEntry[] {
  try {
    return readFileSync(logPath, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => protocolEntry(JSON.parse(line)));
  } catch {
    return [];
  }
}

function protocolEntry(value: unknown): ProtocolEntry {
  const entry = isRecord(value) ? value : {};
  const params = isRecord(entry.params) ? entry.params : {};
  return {
    method: getString(entry, "method"),
    threadId: getString(params, "threadId") ?? getString(entry, "threadId"),
    turnId: getString(params, "turnId"),
    refused: getString(entry, "refused"),
    compactionEnded: getString(entry, "compactionEnded"),
    status: getString(entry, "status"),
  };
}

async function waitFor<T>(label: string, read: () => T | null | undefined | false, timeoutMs = 20_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise((done) => setTimeout(done, 25));
  }
}

await writeFile(fakeCodex, FAKE_CODEX, { mode: 0o755 });
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
const agents = new AgentService({
  store,
  mailbox,
  browser,
  preferredProvider: "codex",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "codex"),
  contextCompactionTimeoutMs: COMPACTION_TIMEOUT_MS,
});

const runtime = join(root, "runtime");
mkdirSync(runtime, { mode: 0o700 });
let authState: CentralAuthState = { status: "signed_out" };
const options: ServerModeOptions = {
  environment: { controlSocketPath: join(runtime, "control.sock") },
  version: "fixture",
  centralAuth: {
    getState: () => authState,
    requestEmailCode: unused,
    verifyEmailCode: unused,
    canPersistSession: () => true,
    redeemServerClaim: unused,
    logout: unused,
    createMobileConnect: unused,
  },
  host: {
    getStatus: () => ({
      phase: "idle",
      configured: false,
      enabledOnLaunch: false,
      serverId: null,
      serverName: null,
      apiUrl: null,
      logoUrl: null,
      apiOnline: false,
      remoteDesktopReady: false,
      remoteDesktopScreenRecordingDenied: false,
      remoteDesktopUnattended: false,
      remoteDesktopActiveSessions: 0,
      remoteDesktopMaxSessions: 0,
      message: null,
    }),
    configure: unused,
    start: unused,
    updateIdentity: unused,
    getMobileConnectHost: () => null,
  },
  memory: null,
  nativeWork: () => agents.nativeWorkSnapshot(),
  onError: () => {
    throw new Error("A control request failed.");
  },
};
const server = new ServerMode(options);
const unknownServer = new ServerMode({
  ...options,
  environment: { controlSocketPath: join(runtime, "unknown.sock") },
  nativeWork: undefined,
});
async function status(socketPath = options.environment.controlSocketPath): Promise<Record<string, string>> {
  return new Promise((resolve, reject) => {
    const req = request({ socketPath, path: "/v1/status", method: "GET" }, (response) => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => {
        body += chunk;
      });
      response.on("end", () =>
        resolve(
          Object.fromEntries(
            body
              .trim()
              .split("\n")
              .map((line) => {
                const i = line.indexOf("=");
                return [line.slice(0, i), line.slice(i + 1)];
              }),
          ),
        ),
      );
    });
    req.on("error", reject);
    req.end();
  });
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(server.listen());
  await runCauseEffect(unknownServer.listen());
  const owner = await runCauseEffect(
    agents.createAgent({
      name: "Native idle",
      description: "Checks hidden native work.",
      avatarSeed: "native-idle",
      avatarHue: null,
      initialMessage: "NATIVE-HIDDEN-COMPACTION",
    }),
  );
  const native = await waitFor("native compaction without a projected turn", () =>
    protocol().find((entry) => entry.method === "fixture/native-compaction-running"),
  );
  assert.ok(native.threadId);
  const projected = database.connection
    .prepare("SELECT COUNT(*) count FROM projection_threads WHERE active_turn_id IS NOT NULL")
    .get();
  assert.equal(projected?.count, 0);
  assert.ok(
    agents.hasActiveWork().includes("context-compaction"),
    "A native compaction must block restart even when SQL has no active turn.",
  );
  const hiddenStatus = await status();
  assert.equal(hiddenStatus.nativeIdle, "false");
  assert.equal(hiddenStatus.nativeWorkVersion, "1");
  assert.equal(JSON.parse(hiddenStatus.nativeWork ?? "null").compactions, 1);
  assert.deepEqual(Object.keys(JSON.parse(hiddenStatus.nativeWork ?? "null")).sort(), [
    "compactions",
    "idle",
    "providerOperations",
    "reasons",
  ]);
  report.checks.push("A provider-native compaction blocks restart while all SQL active-turn fields are empty.");
  await waitFor("native compaction completion", () =>
    protocol().find((entry) => entry.method === "fixture/native-compaction-completed"),
  );
  await waitFor("native work released", () => !agents.hasActiveWork().includes("context-compaction"));
  const channelId = crypto.randomUUID();
  const actor = { id: "local", name: "Alex" };
  await runCauseEffect(
    agents.channels.command(
      {
        type: "save",
        operationId: crypto.randomUUID(),
        channelId,
        draft: {
          name: "Native context",
          title: "",
          instructions: "Check existing work.",
          members: [{ agentId: owner.id }],
          leadAgentId: owner.id,
        },
      },
      actor,
    ),
  );
  await runCauseEffect(
    agents.channels.command(
      {
        type: "send",
        operationId: crypto.randomUUID(),
        channelId,
        text: "NATIVE-HIDDEN-COMPACTION",
        recipientAgentId: owner.id,
        replyToMessageId: null,
        attachmentDraftIds: [],
      },
      actor,
    ),
  );
  await waitFor("native compaction in a channel context", () =>
    protocol().find(
      (entry) => entry.method === "fixture/native-compaction-running" && entry.threadId !== native.threadId,
    ),
  );
  const projectedChannel = database.connection
    .prepare("SELECT COUNT(*) count FROM projection_threads WHERE active_turn_id IS NOT NULL")
    .get();
  assert.equal(projectedChannel?.count, 0);
  assert.ok(
    agents.hasActiveWork().includes("context-compaction"),
    "A channel compaction must block restart although channel contexts are absent from public context snapshots.",
  );
  report.checks.push("A hidden channel-context compaction blocks restart without a normal SQL turn.");
  await waitFor("channel native compaction completion", () =>
    protocol().find(
      (entry) => entry.method === "fixture/native-compaction-completed" && entry.threadId !== native.threadId,
    ),
  );
  await waitFor("channel native work released", () => !agents.hasActiveWork().includes("context-compaction"));
  await waitFor("all native work released", () => agents.nativeWorkSnapshot().idle);
  assert.equal((await status()).nativeIdle, "true");
  for (const state of [
    { status: "loading" },
    { status: "signing_in" },
    {
      status: "code_sent",
      challengeId: "fixture",
      email: "fixture@example.com",
      expiresAt: Date.now() + 1000,
      resendAvailableAt: 0,
    },
  ] satisfies CentralAuthState[]) {
    authState = state;
    const pending = await status();
    assert.equal(pending.nativeIdle, "false");
    assert.ok(JSON.parse(pending.nativeWork ?? "null").reasons.includes("server-auth"));
  }
  authState = { status: "signed_out" };
  const login = await runCauseEffect(agents.startProviderCodeLogin("codex"));
  assert.equal(login.kind, "code");
  const pendingProvider = await status();
  assert.equal(pendingProvider.nativeIdle, "false");
  assert.ok(JSON.parse(pendingProvider.nativeWork ?? "null").providerOperations > 0);
  assert.ok(JSON.parse(pendingProvider.nativeWork ?? "null").reasons.includes("provider-process"));
  await runCauseEffect(agents.cancelProviderCodeLogin("codex"));
  await waitFor("provider login released", () => agents.nativeWorkSnapshot().idle);
  assert.equal((await status()).nativeIdle, "true");
  const unknown = await status(join(runtime, "unknown.sock"));
  assert.equal(unknown.nativeIdle, "unknown");
  assert.equal(unknown.nativeWork, "unavailable");
  report.checks.push(
    "The private control socket reports hidden work, blocks pending provider and server sign-in, and returns unknown when native capability is absent.",
  );
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? error.message : String(error);
  process.exitCode = 1;
} finally {
  await runCauseEffect(server.close());
  await runCauseEffect(unknownServer.close());
  await runCauseEffect(agents.stop());
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
