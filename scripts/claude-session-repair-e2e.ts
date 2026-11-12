import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, type CentralAuthState, defaultProviderModel } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { ProviderAccountStore } from "../src/backend/agent/provider-account-store";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { AgentService } from "../src/backend/agent-service";
import { AgentStore } from "../src/backend/agent-store";
import { AgentThreadStore } from "../src/backend/agent-threads/agent-thread-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { ServerMode, type ServerModeOptions } from "../src/main/server-mode";

const root = await mkdtemp(join(tmpdir(), "ob-repair-"));
process.env.HOME = join(root, "home");
process.env.CLAUDE_CONFIG_DIR = join(root, "claude");
mkdirSync(process.env.HOME, { recursive: true });
const OUT = resolve(import.meta.dirname, "../.openbot-build/claude-session-repair-e2e");
const checks: { name: string; passed: boolean; detail?: unknown }[] = [];
let starts = 0;
const unused = () => Effect.die(new Error("Not used by this check."));
const browser: AgentBrowserHost = {
  listTabs: () => [],
  beginTakeover: unused,
  endTakeover: () => {},
  userControlTabFor: () => null,
  awaitUserControlRelease: unused,
  resolveUploadTarget: unused,
  handleDynamicTool: unused,
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

const authState: CentralAuthState = { status: "signed_out" };
const serverOptions = (service: AgentService, runtime: string): ServerModeOptions => ({
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
    cancelCompanySignIn: unused,
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
  usageLimits: service,
  agents: service,
  onError: () => {},
});

async function fixture() {
  const data = await mkdtemp(join(root, "data-"));
  const runtime = await mkdtemp(join("/tmp", "obr-"));
  const database = new OpenBotDatabase(data);
  const store = new AgentStore(data, join(root, "home"), database);
  const mailbox = new MailboxStore(data, store.sharedRoot, database);
  const accounts = new ProviderAccountStore(join(data, "accounts.json"), join(data, "accounts"));
  await runCauseEffect(accounts.load());
  const service = new AgentService({
    store,
    mailbox,
    browser,
    providerAccounts: accounts,
    offProviders: AGENT_PROVIDERS,
    clientFactory: () => {
      starts++;
      throw new Error("No provider must start during repair.");
    },
  });
  await runCauseEffect(service.initialize());
  const server = new ServerMode(serverOptions(service, runtime));
  await runCauseEffect(server.listen());
  const create = async (provider: "claude" | "codex" = "claude") => {
    const agent = await runCauseEffect(
      store.createAgent({
        name: "Repair fixture",
        description: "Keeps public history.",
        avatarSeed: "repair",
        avatarHue: null,
        provider,
        model: defaultProviderModel(provider),
      }),
    );
    const threadId = store.ensureThreadIdNow(agent.id);
    return { ...agent, threadId };
  };
  const bind = (threadId: string, provider: "claude" | "codex" = "claude") =>
    database.bindProviderSession({
      threadId,
      provider,
      externalSessionId: randomUUID(),
      model: defaultProviderModel(provider),
      effort: "medium",
    });
  const post = (body = "provider=claude") =>
    new Promise<{ status: number; lines: Record<string, string> }>((done, fail) => {
      const req = request(
        {
          socketPath: join(runtime, "control.sock"),
          method: "POST",
          path: "/v1/provider-sessions/repair",
          headers: { "content-type": "application/x-www-form-urlencoded", "content-length": Buffer.byteLength(body) },
        },
        (response) => {
          let text = "";
          response.setEncoding("utf8");
          response.on("data", (chunk) => (text += chunk));
          response.on("end", () =>
            done({
              status: response.statusCode ?? 0,
              lines: Object.fromEntries(
                text
                  .trim()
                  .split("\n")
                  .filter(Boolean)
                  .map((line) => {
                    const i = line.indexOf("=");
                    return [line.slice(0, i), line.slice(i + 1)];
                  }),
              ),
            }),
          );
        },
      );
      req.on("error", fail);
      req.end(body);
    });
  const close = async () => {
    await runCauseEffect(server.close());
    await runCauseEffect(service.stop());
    database.close();
    await rm(runtime, { recursive: true, force: true });
  };
  return { data, database, store, mailbox, accounts, service, create, bind, post, close };
}
interface Row {
  id: string;
  agentId: string;
  threadId: string;
  externalSessionId: string;
  reason?: string;
}
interface Report {
  applied: boolean;
  count: number;
  repaired: Row[];
  missing: Row[];
  skipped: Row[];
  errors: Row[];
  busy: boolean;
}
async function check(name: string, run: (f: Awaited<ReturnType<typeof fixture>>) => Promise<unknown>) {
  const f = await fixture();
  try {
    checks.push({ name, passed: true, detail: await run(f) });
  } catch (error) {
    checks.push({ name, passed: false, detail: error instanceof Error ? error.stack : String(error) });
  } finally {
    await f.close();
  }
}
function result(answer: { status: number; lines: Record<string, string> }, status = 200): Report {
  assert.equal(answer.status, status, JSON.stringify(answer));
  return JSON.parse(answer.lines.result ?? "null");
}
const apply = "provider=claude&apply=true&reason=Host+repair+of+missing+Claude+transcripts";
try {
  await check("dry-run finds missing main sessions without a write or model start", async (f) => {
    const agent = await f.create();
    const session = f.bind(agent.threadId);
    const before = f.database.connection.prepare("SELECT COUNT(*) AS n FROM orchestration_events").get();
    const report = result(await f.post());
    assert.equal(report.applied, false);
    assert.equal(report.count, 0);
    assert.deepEqual(
      report.missing.map((row) => row.id),
      [session.id],
    );
    assert.equal(f.database.activeProviderSession(agent.threadId, "claude")?.id, session.id);
    assert.deepEqual(f.database.connection.prepare("SELECT COUNT(*) AS n FROM orchestration_events").get(), before);
    assert.equal(starts, 0);
  });
  await check("the route refuses missing provider, unknown/repeated fields and non-explicit apply", async (f) => {
    for (const body of [
      "",
      "provider=codex",
      "provider=claude&apply=1",
      "provider=claude&apply=true",
      "provider=claude&apply=true&reason=",
      "provider=claude&wat=x",
      "provider=claude&provider=claude",
    ]) {
      const answer = await f.post(body);
      assert.equal(answer.status, 400, JSON.stringify({ body, answer }));
    }
  });
  await check(
    "apply repairs stale main, task and channel bindings while the agent uses Codex and Claude is stopped",
    async (f) => {
      const agent = await f.create("codex");
      const main = f.bind(agent.threadId);
      const task = new AgentThreadStore(f.database).create({
        agentId: agent.id,
        origin: "user",
        title: "Keep task history.",
        titleSource: "auto",
      });
      const taskSession = f.bind(task.threadId);
      const channelId = randomUUID();
      await runCauseEffect(
        f.service.channels.command(
          {
            type: "save",
            operationId: randomUUID(),
            channelId,
            draft: {
              name: "Repair channel",
              title: "",
              instructions: "Keep channel history.",
              members: [{ agentId: agent.id }],
              leadAgentId: agent.id,
            },
          },
          { id: "local", name: "Fixture" },
        ),
      );
      const channel = f.service.channels.store.context(channelId, agent.id);
      const channelSession = f.bind(channel.threadId);
      const other = await f.create("codex");
      const codex = f.bind(other.threadId, "codex");
      const profiles = JSON.stringify(f.store.list());
      const taskBefore = JSON.stringify(f.service.readTask(agent.id, task.taskId));
      const report = result(await f.post(apply));
      assert.equal(report.applied, true);
      assert.equal(report.count, 3);
      assert.deepEqual(
        report.repaired.map((row) => row.id).sort(),
        [main.id, taskSession.id, channelSession.id].sort(),
      );
      for (const session of [main, taskSession, channelSession])
        assert.equal(
          f.database.listProviderSessions(session.threadId).find((row) => row.id === session.id)?.state,
          "inactive",
        );
      assert.equal(f.database.activeProviderSession(other.threadId, "codex")?.id, codex.id);
      assert.equal(JSON.stringify(f.store.list()), profiles);
      assert.equal(JSON.stringify(f.service.readTask(agent.id, task.taskId)), taskBefore);
      assert.equal(starts, 0);
      const events = f.database.connection
        .prepare("SELECT payload_json FROM orchestration_events WHERE event_type='provider-session.deactivated'")
        .all();
      assert.equal(events.length, 3);
      assert.ok(
        events.every((row) => typeof row.payload_json === "string" && row.payload_json.includes("Host repair")),
      );
      return report;
    },
  );
  await check("healthy transcripts in any known Claude account/project remain active", async (f) => {
    const agent = await f.create();
    const session = f.bind(agent.threadId);
    const account = await runCauseEffect(f.accounts.create("claude"));
    assert.ok(account);
    const directory = join(f.accounts.home(account), "projects", "another-workspace");
    mkdirSync(directory, { recursive: true });
    writeFileSync(join(directory, `${session.externalSessionId}.jsonl`), "{}\n");
    const report = result(await f.post(apply));
    assert.equal(report.count, 0);
    assert.equal(report.missing.length, 0);
    assert.ok(report.skipped.some((row) => row.id === session.id && row.reason === "transcript-present"));
    assert.equal(f.database.activeProviderSession(agent.threadId, "claude")?.id, session.id);
  });
  await check("a transcript path I/O error fails the complete apply closed", async (f) => {
    const agent = await f.create();
    const session = f.bind(agent.threadId);
    const configured = process.env.CLAUDE_CONFIG_DIR;
    const bad = join(f.data, "not-directory");
    writeFileSync(bad, "file");
    process.env.CLAUDE_CONFIG_DIR = bad;
    try {
      const report = result(await f.post(apply), 409);
      assert.equal(report.count, 0);
      assert.ok(report.errors.length > 0);
      assert.equal(f.database.activeProviderSession(agent.threadId, "claude")?.id, session.id);
    } finally {
      process.env.CLAUDE_CONFIG_DIR = configured;
    }
  });
  await check("starting native delivery refuses apply before any session retirement", async (f) => {
    const agent = await f.create();
    const session = f.bind(agent.threadId);
    const sent = await runCauseEffect(
      f.mailbox.enqueue({ sender: { kind: "user" }, recipientAgentIds: [agent.id], text: "Keep queued work." }),
    );
    const id = sent.deliveries[0]?.id;
    assert.ok(id);
    await runCauseEffect(f.mailbox.markStarting(id));
    const report = result(await f.post(apply), 409);
    assert.equal(report.busy, true);
    assert.equal(report.count, 0);
    assert.equal(f.database.activeProviderSession(agent.threadId, "claude")?.id, session.id);
    await runCauseEffect(f.mailbox.markTerminal(id, "interrupted"));
  });
  await check(
    "repeated apply reports historical sessions as already retired and changes no further state",
    async (f) => {
      const agent = await f.create();
      const session = f.bind(agent.threadId);
      assert.equal(result(await f.post(apply)).count, 1);
      const before = f.database.connection.prepare("SELECT COUNT(*) AS n FROM orchestration_events").get();
      const report = result(await f.post(apply));
      assert.equal(report.count, 0);
      assert.equal(report.missing.length, 0);
      assert.ok(report.skipped.some((row) => row.id === session.id && row.reason === "already-retired"));
      assert.deepEqual(f.database.connection.prepare("SELECT COUNT(*) AS n FROM orchestration_events").get(), before);
      assert.equal(starts, 0);
    },
  );
  await check("an active side-thread turn refuses the complete batch", async (f) => {
    const agent = await f.create("codex");
    const main = f.bind(agent.threadId);
    const task = new AgentThreadStore(f.database).create({
      agentId: agent.id,
      origin: "user",
      title: "Active task",
      titleSource: "auto",
    });
    const session = f.bind(task.threadId);
    const snapshot = f.database.readConversation(agent.id, task.threadId);
    f.database.persistConversation({ ...snapshot, activeTurnId: "fixture-active-turn" }, "fixture.active-turn");
    const report = result(await f.post(apply), 409);
    assert.equal(report.busy, true);
    assert.equal(report.count, 0);
    for (const one of [main, session])
      assert.equal(f.database.activeProviderSession(one.threadId, "claude")?.id, one.id);
    f.database.persistConversation({ ...snapshot, activeTurnId: null }, "fixture.active-turn-ended");
  });
  await check("an unreadable account root refuses apply even if another root is missing", async (f) => {
    const agent = await f.create();
    const session = f.bind(agent.threadId);
    const account = await runCauseEffect(f.accounts.create("claude"));
    assert.ok(account);
    const directory = join(f.accounts.home(account), "projects");
    mkdirSync(directory, { recursive: true });
    chmodSync(directory, 0);
    try {
      const report = result(await f.post(apply), 409);
      assert.equal(report.count, 0);
      assert.ok(report.errors.some((row) => row.reason === "transcript-check-failed:EACCES"));
      assert.equal(f.database.activeProviderSession(agent.threadId, "claude")?.id, session.id);
    } finally {
      chmodSync(directory, 0o700);
    }
  });
  await check("a later invalid binding prevents any earlier missing candidate from being retired", async (f) => {
    const agent = await f.create();
    const main = f.bind(agent.threadId);
    const task = new AgentThreadStore(f.database).create({
      agentId: agent.id,
      origin: "user",
      title: "Invalid session",
      titleSource: "auto",
    });
    const invalid = f.database.bindProviderSession({
      threadId: task.threadId,
      provider: "claude",
      externalSessionId: "not-a-Claude-session",
      model: defaultProviderModel("claude"),
      effort: "medium",
    });
    const report = result(await f.post(apply), 409);
    assert.equal(report.count, 0);
    assert.ok(report.missing.some((row) => row.id === main.id));
    assert.ok(report.errors.some((row) => row.id === invalid.id));
    assert.equal(f.database.activeProviderSession(agent.threadId, "claude")?.id, main.id);
  });
  await check("native retirement rolls back all events when a selected binding changed", async (f) => {
    result(await f.post());
    const agent = await f.create();
    const main = f.bind(agent.threadId);
    const task = new AgentThreadStore(f.database).create({
      agentId: agent.id,
      origin: "user",
      title: "Changed session",
      titleSource: "auto",
    });
    const session = f.bind(task.threadId);
    f.database.deactivateProviderSessions(task.threadId);
    const before = f.database.connection.prepare("SELECT COUNT(*) AS n FROM orchestration_events").get();
    assert.throws(() => f.database.retireSelectedProviderSessions([main, session], "Host repair with stale selection"));
    assert.equal(f.database.activeProviderSession(agent.threadId, "claude")?.id, main.id);
    assert.deepEqual(f.database.connection.prepare("SELECT COUNT(*) AS n FROM orchestration_events").get(), before);
  });
} finally {
  mkdirSync(OUT, { recursive: true });
  const report = { passed: checks.every((row) => row.passed), total: checks.length, starts, checks };
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  for (const row of checks)
    process.stdout.write(
      `${row.passed ? "PASS" : "FAIL"} ${row.name} ${row.detail ? JSON.stringify(row.detail) : ""}\n`,
    );
  await rm(root, { recursive: true, force: true });
  if (!report.passed) process.exitCode = 1;
}
