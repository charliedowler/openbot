import { EventEmitter } from "node:events";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  AGENT_PROVIDERS,
  type AgentModelId,
  type CentralAuthState,
  defaultProviderModel,
} from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { getRecord, getString, isRecord } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";
import type { ServerModeOptions } from "../src/main/server-mode";

const OUT = resolve(import.meta.dirname, "../.openbot-build/review-fresh-session-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-review-session-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const modeFile = join(root, "mode");
const claudeExecutable = join(root, "claude");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const SECOND = "claude-sonnet-5-5";

process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(home, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const { ServerMode } = await import("../src/main/server-mode");
const { parseSendMessage } = await import("../src/main/ipc/agent-inputs");

const STAND_IN_CLAUDE = `#!/usr/bin/env node
const fs = require("node:fs");
const { randomUUID } = require("node:crypto");
const args = process.argv.slice(2);
if (args.includes("--version")) {
  process.stdout.write("2.1.302 (Claude Code)\\n");
  process.exit(0);
}
const option = (name) => {
  const index = args.findIndex((arg) => arg === name || arg.startsWith(name + "="));
  if (index < 0) return null;
  return args[index] === name ? (args[index + 1] ?? null) : args[index].slice(name.length + 1);
};
const sessionId = option("--resume") ?? option("--session-id") ?? randomUUID();
let model = option("--model");
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const result = (fields) =>
  write({ type: "result", subtype: "success", session_id: sessionId, uuid: randomUUID(), duration_ms: 1,
    duration_api_ms: 1, num_turns: 1, total_cost_usd: 0, usage: { input_tokens: 0, output_tokens: 0 },
    permission_denials: [], modelUsage: {}, ...fields });
const assistant = (text) =>
  write({ type: "assistant", message: { id: "msg_" + randomUUID(), type: "message", role: "assistant", model,
    content: [{ type: "text", text }], stop_reason: null, usage: { input_tokens: 1, output_tokens: 1 } },
    parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() });
const log = (row) => fs.appendFileSync(${JSON.stringify(turnLog)}, JSON.stringify(row) + "\\n");
let held = null;
const turn = (message) => {
  const input = JSON.stringify(message);
  const mode = fs.existsSync(${JSON.stringify(modeFile)}) ? fs.readFileSync(${JSON.stringify(modeFile)}, "utf8").trim() : "accept";
  log({ phase: "start", sessionId, model, mode, at: Date.now(), input });
  if (mode === "hold") {
    held = { input };
    assistant("Working.");
    return;
  }
  assistant("Done.");
  result({ is_error: false, result: "Done." });
  log({ phase: "end", sessionId, model, mode, at: Date.now(), input });
};
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newline = buffer.indexOf("\\n");
  while (newline >= 0) {
    const line = buffer.slice(0, newline).trim();
    buffer = buffer.slice(newline + 1);
    newline = buffer.indexOf("\\n");
    if (!line) continue;
    const message = JSON.parse(line);
    if (message.type === "control_request") {
      if (message.request?.subtype === "set_model") model = message.request.model ?? model;
      write({ type: "control_response", response: { subtype: "success", request_id: message.request_id, response: {} } });
      if (message.request?.subtype === "interrupt" && held) {
        log({ phase: "interrupted", model, at: Date.now(), input: held.input });
        held = null;
        result({ subtype: "error_during_execution", is_error: true, errors: ["[Request interrupted by user]"] });
      }
    } else if (message.type === "user") turn(message);
  }
});
process.on("exit", () => log({ phase: "close", sessionId }));
process.on("SIGTERM", () => process.exit(0));
process.stdin.on("end", () => process.exit(0));
`;

interface ClientEvents {
  notification: [notification: AppServerNotification];
  request: [request: AppServerRequest];
  exit: [error: Error];
  diagnostic: [message: string, origin?: DiagnosticOrigin];
}

class ClaudeStandIn extends EventEmitter<ClientEvents> implements AgentClient {
  readonly provider = "claude";
  readonly contextCompaction = "native";
  readonly #real: InstanceType<typeof ClaudeAgentClient>;
  readonly readHistory: ReadProviderHistory;

  constructor(cli: ClaudeCliInfo) {
    super();
    this.#real = new ClaudeAgentClient(cli);
    this.readHistory = this.#real.readHistory;
    this.#real.on("notification", (notification) => this.emit("notification", notification));
    this.#real.on("request", (request) => this.emit("request", request));
    this.#real.on("diagnostic", (message) => this.emit("diagnostic", message));
    this.#real.on("exit", (error) => this.emit("exit", error));
  }

  get running(): boolean {
    return this.#real.running;
  }

  start(): void {
    this.#real.start();
  }

  stop() {
    return this.#real.stop();
  }

  canReleaseProcess = () => this.#real.canReleaseProcess();

  releaseThread(externalThreadId: string) {
    return this.#real.releaseThread(externalThreadId);
  }

  releaseIdleThreads() {
    return this.#real.releaseIdleThreads();
  }

  request<T>(method: string, params: unknown, decoder: ResponseDecoder<T>, timeoutMs?: number) {
    if (!this.#real.running) return this.#real.request(method, params, decoder, timeoutMs);
    if (method === "thread/start")
      return this.#real.request(method, params, decoder, timeoutMs).pipe(
        Effect.tap((response) =>
          providerSync(() =>
            writeFileSync(
              turnLog,
              `${JSON.stringify({
                phase: "thread-start",
                sessionId: getString(getRecord(response, "thread"), "id"),
                ephemeral: isRecord(params) ? params.ephemeral : null,
                persistSession: isRecord(params) ? params.persistSession : null,
              })}\n`,
              { flag: "a" },
            ),
          ),
        ),
      );
    switch (method) {
      case "account/read":
        return providerSync(() => decoder({ account: { type: "claude", email: "claude@example.com" } }));
      case "account/rateLimits/read":
        return providerSync(() => decoder({ rateLimits: null, rateLimitsByLimitId: null }));
      case "model/list":
        return providerSync(() =>
          decoder({
            data: [
              { model: PRIMARY, displayName: "Primary" },
              { model: SECOND, displayName: "Second" },
            ],
          }),
        );
      default:
        return this.#real.request(method, params, decoder, timeoutMs);
    }
  }

  notify(): void {
    this.#real.notify();
  }

  respond(id: RequestId, result: unknown): void {
    this.#real.respond(id, result);
  }

  respondError(id: RequestId, error: Parameters<AgentClient["respondError"]>[1]): void {
    this.#real.respondError(id, error);
  }
}

const report: {
  checks: { name: string; passed: boolean; detail?: unknown }[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
} = { checks: [], passed: false };

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

async function waitFor<T>(label: string, read: () => T | null | undefined | false, timeoutMs = 20_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise((done) => setTimeout(done, 25));
  }
}

async function check(name: string, run: () => Promise<unknown>) {
  try {
    const detail = await run();
    report.checks.push({ name, passed: true, detail });
  } catch (error) {
    report.checks.push({ name, passed: false, detail: error instanceof Error ? error.message : String(error) });
  }
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

await writeFile(claudeExecutable, STAND_IN_CLAUDE, { mode: 0o755 });
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
const service = new AgentService({
  store,
  mailbox,
  browser,
  providerRetryDelayMs: 50,
  preferredProvider: "claude",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "claude"),
  clientFactory: (_provider, cli) => new ClaudeStandIn(cli),
});

function deliveryOf(deliveryId: string) {
  return mailbox.getDelivery(deliveryId)?.delivery;
}

async function control(
  path: string,
  method: string,
  body = "",
): Promise<{ status: number; lines: Record<string, string> }> {
  return new Promise((done, fail) => {
    const req = request(
      {
        socketPath: join(runtime, "control.sock"),
        path,
        method,
        headers: { "content-type": "application/x-www-form-urlencoded", "content-length": Buffer.byteLength(body) },
      },
      (response) => {
        let text = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          text += chunk;
        });
        response.on("end", () =>
          done({
            status: response.statusCode ?? 0,
            lines: Object.fromEntries(
              text
                .trim()
                .split("\n")
                .filter(Boolean)
                .map((line) => {
                  const index = line.indexOf("=");
                  return [line.slice(0, index), line.slice(index + 1)];
                }),
            ),
          }),
        );
      },
    );
    req.on("error", fail);
    req.end(body);
  });
}

const runtime = await mkdtemp(join("/tmp", "obh-"));
const authState: CentralAuthState = { status: "signed_out" };
const serverOptions = (): ServerModeOptions & { agents?: unknown } => ({
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

const server = new ServerMode(serverOptions());

interface TurnEntry {
  phase: string;
  sessionId: string;
  input?: string;
  ephemeral?: boolean;
  persistSession?: boolean;
}
function turns(): TurnEntry[] {
  try {
    return readFileSync(turnLog, "utf8")
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}
async function settled(id: string) {
  await waitFor("delivery completion", () => deliveryOf(id)?.status === "completed");
  await waitFor("native work idle", () => service.nativeWorkSnapshot().idle);
}
try {
  await runCauseEffect(service.initialize());
  await runCauseEffect(server.listen());
  const agent = await runCauseEffect(
    service.createAgent({
      name: "Ada",
      description: "Checks explicit review sessions.",
      avatarSeed: "review",
      avatarHue: null,
      initialMessage: "Initial chat.",
      provider: "claude",
      model: PRIMARY,
    }),
  );
  await waitFor("initial turn completion", () =>
    service.listQueue(agent.id).deliveries.every((item) => item.status === "completed"),
  );
  await waitFor("initial native idle", () => service.nativeWorkSnapshot().idle);
  const publicId = store.list().find((item) => item.id === agent.id)?.threadId;
  assert(publicId, "The public thread exists.");
  const main = database.activeProviderSession(publicId, "claude")?.externalSessionId;
  assert(main, "The ordinary session exists.");
  const send = async (text: string, review = false) => {
    const input = { agentId: agent.id, text, ...(review ? { executionPurpose: "review" as const } : {}) };
    const receipt = await runCauseEffect(service.sendMessage(input));
    const id = receipt.deliveries[0]?.id;
    assert(id, "The delivery exists.");
    await settled(id);
    return id;
  };
  await send(`HISTORY-START ${"Long earlier context. ".repeat(2500)} HISTORY-END`);
  const reviewSessions: string[] = [];
  await check("the local parser accepts only the explicit review purpose", async () => {
    const parsed = parseSendMessage({ agentId: agent.id, text: "Check the change.", executionPurpose: "review" });
    assert(parsed.executionPurpose === "review", "The local parser retains the purpose.");
    assert(
      parseSendMessage({ agentId: agent.id, text: "Review ordinary chat." }).executionPurpose === undefined,
      "Ordinary chat has no marker.",
    );
    let refused = false;
    try {
      parseSendMessage({ agentId: agent.id, text: "Check.", executionPurpose: "routine" });
    } catch {
      refused = true;
    }
    assert(refused, "An unknown purpose is refused.");
    return { explicit: true, inferred: false, invalidRefused: true };
  });
  await check("the native review marker survives persisted mailbox recovery", async () => {
    const input = {
      sender: { kind: "user" as const },
      recipientAgentIds: [agent.id],
      text: "Stored review marker.",
      executionPurpose: "review" as const,
      holdStart: true,
    };
    const receipt = await runCauseEffect(mailbox.enqueue(input));
    const id = receipt.deliveries[0]?.id;
    assert(id, "The stored delivery exists.");
    try {
      mailbox.restorePersistedState();
      const context = mailbox.getDelivery(id);
      assert(context && context.executionPurpose === "review", "The persisted marker is restored.");
      assert(!Object.hasOwn(context.delivery, "executionPurpose"), "The marker stays out of the public queue payload.");
      return { restored: true, publicPayloadUnchanged: true };
    } finally {
      await runCauseEffect(mailbox.cancel(agent.id, id));
    }
  });
  await check("review words in ordinary chat do not create a fresh session", async () => {
    await send("Review ordinary chat wording. This has no execution-purpose marker.");
    const row = turns().findLast(
      (item) => item.phase === "start" && item.input?.includes("Review ordinary chat wording."),
    );
    assert(row?.sessionId === main, "Ordinary chat retains its provider session.");
    return { sameSession: true };
  });
  for (const marker of ["EXPLICIT-REVIEW-ONE", "EXPLICIT-REVIEW-TWO"]) {
    await check(`${marker} uses a fresh bounded session and preserves its answer`, async () => {
      const id = await send(`${marker}: Check the prepared change and report the result.`, true);
      const row = turns().findLast((item) => item.phase === "start" && item.input?.includes(marker));
      assert(row, "The provider receives the review.");
      assert(row.sessionId !== main && !reviewSessions.includes(row.sessionId), "Every review has a new session.");
      reviewSessions.push(row.sessionId);
      const start = turns().find((entry) => entry.phase === "thread-start" && entry.sessionId === row.sessionId);
      assert(
        start?.ephemeral === true && start.persistSession === false,
        "The review session is ephemeral and unsaved.",
      );
      assert((row.input?.length ?? 0) < 16_000, "The handoff stays bounded after a long chat.");
      assert(
        row.input?.includes("--- recent messages ---") &&
          row.input.includes("Review ordinary chat wording.") &&
          row.input.includes("openbot.history_search"),
        "The short handoff carries recent context and tells the review how to read omitted history.",
      );
      assert(
        !row.input?.includes("This routine run has its own session."),
        "The review handoff names the correct purpose.",
      );
      const history = await runCauseEffect(service.readConversation(agent.id));
      assert(
        history.messages.some(
          (message) =>
            message.turnId === deliveryOf(id)?.turnId && message.author === "assistant" && message.text === "Done.",
        ),
        "The ordinary review answer remains in public history.",
      );
      assert(
        database.activeProviderSession(publicId, "claude")?.externalSessionId === main,
        "The ordinary chat session remains active.",
      );
      return { deliveryId: id, sessionId: row.sessionId, inputCharacters: row.input?.length, historyPreserved: true };
    });
  }
  await check("the host review route refuses invalid fields and oversized input before enqueue", async () => {
    const before = service.listQueue(agent.id).deliveries.length;
    for (const body of [
      new URLSearchParams({ text: "Check." }).toString(),
      new URLSearchParams({ agentId: agent.id }).toString(),
      new URLSearchParams({ agentId: agent.id, text: " " }).toString(),
      new URLSearchParams({ agentId: agent.id, text: "Check.", extra: "1" }).toString(),
      `agentId=${agent.id}&text=Check&text=Again`,
    ]) {
      const response = await control("/v1/agents/review", "POST", body);
      assert(response.status === 400, `Invalid fields return 400: ${JSON.stringify(response)}`);
    }
    const unknown = await control("/v1/agents/review", "POST", "agentId=unknown&text=Check");
    assert(unknown.status === 404, "The route refuses an unknown agent.");
    const oversized = await control(
      "/v1/agents/review",
      "POST",
      new URLSearchParams({ agentId: agent.id, text: "x".repeat(4097) }).toString(),
    );
    assert(oversized.status === 413, "The host refuses an oversized body before service input.");
    assert(service.listQueue(agent.id).deliveries.length === before, "Refused requests never enqueue work.");
    return { refused: 7, enqueued: 0 };
  });
  await check("the host review route queues real native review work", async () => {
    const marker = "HOST-REVIEW-THREE";
    const answer = await control(
      "/v1/agents/review",
      "POST",
      new URLSearchParams({ agentId: agent.id, text: marker }).toString(),
    );
    assert(answer.status === 200, JSON.stringify(answer));
    const id = answer.lines.deliveryId;
    assert(id, "The host returns the native delivery id.");
    await settled(id);
    const context = mailbox.getDelivery(id);
    assert(context && context.executionPurpose === "review", "The host persists an explicit native marker.");
    const row = turns().findLast((item) => item.phase === "start" && item.input?.includes(marker));
    assert(
      row && row.sessionId !== main && !reviewSessions.includes(row.sessionId),
      "The host uses another new session.",
    );
    reviewSessions.push(row.sessionId);
    return { answer, sessionId: row.sessionId };
  });
  await check("review sessions close and later chat still uses the original session", async () => {
    assert(reviewSessions.length === 3, "All three review turns have unique sessions.");
    await waitFor("review sessions to close", () =>
      reviewSessions.every((sessionId) =>
        turns().some((item) => item.phase === "close" && item.sessionId === sessionId),
      ),
    );
    await send("Final ordinary chat.");
    const row = turns().findLast((item) => item.phase === "start" && item.input?.includes("Final ordinary chat."));
    assert(row?.sessionId === main, "The ordinary session is reused after all reviews.");
    const sessions = database.listProviderSessions(publicId);
    assert(
      sessions.length === 1 && sessions[0]?.externalSessionId === main,
      "Review sessions are not saved as durable public-thread sessions.",
    );
    return { reviewSessions, mainSession: main, durableSessions: sessions.length };
  });
  report.passed = report.checks.length === 8 && report.checks.every((entry) => entry.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  await runCauseEffect(server.close()).catch(() => {});
  await runCauseEffect(service.stop()).catch(() => {});
  report.checkedAt = new Date().toISOString();
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  await rm(runtime, { recursive: true, force: true });
  for (const entry of report.checks)
    process.stdout.write(
      `${entry.passed ? "PASS" : "FAIL"} ${entry.name}${entry.passed ? "" : `: ${String(entry.detail)}`}\n`,
    );
  if (report.error) process.stdout.write(`${report.error}\n`);
  process.stdout.write(
    `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of ${report.checks.length} ${join(OUT, "report.json")}\n`,
  );
}
