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
import { teeLogLines } from "@openbot/logging";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";
import type { ServerModeOptions } from "../src/main/server-mode";

const OUT = resolve(import.meta.dirname, "../.openbot-build/usage-reset-releases-failures-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-usage-reset-releases-failures-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const modeFile = join(root, "mode");
const claudeExecutable = join(root, "claude");
const savedLimits = join(userData, "usage-limits.json");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const RESET_AHEAD_S = 86_400;
const BACKUP: AgentModelId = PRIMARY === "claude-sonnet-5-5" ? "claude-opus-5-5" : "claude-sonnet-5-5";
const PROBE_MS = 700;
const SLOW_MS = PROBE_MS * 3;

process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(home, { recursive: true });

const gateLines: { at: number; line: string }[] = [];
teeLogLines(["usage-limit-gate"], (line) => gateLines.push({ at: Date.now(), line }));

const gateModule = await import("../src/backend/agent/usage-limit-gate");
const { UsageLimitGate, USAGE_LIMIT_PROBE_MS } = gateModule;
const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const { ServerMode } = await import("../src/main/server-mode");

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
const project = require("node:path").join(process.env.CLAUDE_CONFIG_DIR, "projects", process.cwd().replace(/[^a-zA-Z0-9]/g, "-"));
fs.mkdirSync(project, { recursive: true });
const transcript = require("node:path").join(project, sessionId + ".jsonl");
const previous = fs.existsSync(transcript) ? fs.readFileSync(transcript, "utf8").trim().split("\\n").filter(Boolean).map((line) => JSON.parse(line)) : [];
const recorded = new Set(previous.map((row) => row.uuid));
let parentUuid = previous.at(-1)?.uuid ?? null;
const record = (message) => {
  if (recorded.has(message.uuid)) return;
  const row = { ...message, parentUuid, sessionId, timestamp: new Date().toISOString(), isSidechain: false };
  fs.appendFileSync(transcript, JSON.stringify(row) + "\\n");
  recorded.add(row.uuid);
  parentUuid = row.uuid;
};
const write = (message) => {
  if (message.type === "assistant") record(message);
  process.stdout.write(JSON.stringify(message) + "\\n");
};
const result = (fields) =>
  write({ type: "result", subtype: "success", session_id: sessionId, uuid: randomUUID(), duration_ms: 1,
    duration_api_ms: 1, num_turns: 1, total_cost_usd: 0, usage: { input_tokens: 0, output_tokens: 0 },
    permission_denials: [], modelUsage: {}, ...fields });
const assistant = (text) =>
  write({ type: "assistant", message: { id: "msg_" + randomUUID(), type: "message", role: "assistant", model,
    content: [{ type: "text", text }], stop_reason: null, usage: { input_tokens: 1, output_tokens: 1 } },
    parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() });
const log = (row) => fs.appendFileSync(${JSON.stringify(turnLog)}, JSON.stringify(row) + "\\n");
let count = 0;
const turn = (message) => {
  record(message);
  const input = JSON.stringify(message);
  const id = sessionId + "-" + (++count);
  const mode = fs.existsSync(${JSON.stringify(modeFile)}) ? fs.readFileSync(${JSON.stringify(modeFile)}, "utf8").trim() : "accept";
  const startedAt = Date.now();
  log({ id, phase: "start", model, mode, startedAt, input });
  const refuse = () => {
    const resetsAt = Math.floor(Date.now() / 1000) + ${RESET_AHEAD_S} + Math.floor(Math.random() * 3000) + 1;
    write({ type: "rate_limit_event", rate_limit_info: { status: "rejected", rateLimitType: "seven_day", resetsAt },
      uuid: randomUUID(), session_id: sessionId });
    result({ is_error: true, result: "You've hit your limit. Try again later." });
    log({ id, phase: "end", model, mode, startedAt, endedAt: Date.now(), refused: true, resetsAt, input });
  };
  const complete = () => {
    assistant("Done.");
    result({ is_error: false, result: "Done." });
    log({ id, phase: "end", model, mode, startedAt, endedAt: Date.now(), refused: false, input });
  };
  if (mode === "refuse") return refuse();
  if (mode === "slow-refuse") return setTimeout(refuse, ${SLOW_MS});
  if (mode === "slow-accept" && model === ${JSON.stringify(PRIMARY)}) return setTimeout(complete, ${SLOW_MS});
  complete();
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
    } else if (message.type === "user") turn(message);
  }
});
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
              { model: BACKUP, displayName: "Backup" },
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
  primary: string;
  probeMs: number;
  checks: { name: string; passed: boolean; detail?: unknown }[];
  gateLog: string[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
} = { primary: PRIMARY, probeMs: PROBE_MS, checks: [], gateLog: [], passed: false };

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

interface TurnRow {
  id: string;
  phase: "start" | "end";
  model: string;
  mode: string;
  startedAt: number;
  endedAt?: number;
  refused?: boolean;
  resetsAt?: number;
  input: string;
}

function rows(): TurnRow[] {
  try {
    return readFileSync(turnLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}

function starts(since: number): TurnRow[] {
  return rows().filter((row) => row.phase === "start" && row.startedAt >= since);
}

function ends(since: number): TurnRow[] {
  return rows().filter((row) => row.phase === "end" && (row.endedAt ?? 0) >= since);
}

function linesSince(since: number, ...parts: string[]): string[] {
  return gateLines
    .filter(
      (entry) => entry.at >= since && entry.line.includes("INFO") && parts.every((part) => entry.line.includes(part)),
    )
    .map((entry) => entry.line);
}

function setMode(mode: "accept" | "refuse" | "slow-refuse" | "slow-accept") {
  writeFileSync(modeFile, mode);
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

function startService(usageLimitProbeMs?: number, providersOn = true) {
  const store = new AgentStore(userData, home, database);
  const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
  const service = new AgentService({
    store,
    mailbox,
    browser,
    providerRetryDelayMs: 50,
    preferredProvider: "claude",
    offProviders: providersOn ? AGENT_PROVIDERS.filter((provider) => provider !== "claude") : [...AGENT_PROVIDERS],
    clientFactory: (_provider, cli) => new ClaudeStandIn(cli),
    ...(usageLimitProbeMs === undefined ? {} : { usageLimitProbeMs }),
  });
  return { store, mailbox, service };
}

let { store, mailbox, service } = startService(PROBE_MS);

async function send(agentId: string, text: string): Promise<string> {
  const receipt = await runCauseEffect(service.sendMessage({ agentId, text, attachmentDraftIds: [] }));
  const deliveryId = receipt.deliveries[0]?.id;
  assert(deliveryId, "The message has a delivery.");
  return deliveryId;
}

function statusOf(deliveryId: string) {
  return mailbox.getDelivery(deliveryId)?.delivery.status;
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

const runtime = await mkdtemp(join("/tmp", "obu-"));
const authState: CentralAuthState = { status: "signed_out" };
const serverOptions = (): ServerModeOptions & { usageLimits?: unknown } => ({
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
  onError: () => {},
});
const servers: InstanceType<typeof ServerMode>[] = [];

interface SavedFailure {
  provider: string;
  accountId: string | null;
  model: string;
  until: number;
}
function savedState(path = savedLimits): { limits: unknown[]; failures: SavedFailure[] } {
  return JSON.parse(readFileSync(path, "utf8"));
}
const gateOptions = (statePath: string) => ({
  store,
  statePath,
  probeIntervalMs: PROBE_MS,
  hooks: {
    emit: () => {},
    emitRuntimeSnapshot: () => {},
    scheduleDrain: () => {},
    readUsage: () => Effect.succeed(null),
    held: () => Effect.void,
    released: () => Effect.void,
  },
});

try {
  await runCauseEffect(service.initialize());
  const agents: { id: string; name: string }[] = [];
  for (const name of ["Ada", "Ben"]) {
    const agent = await runCauseEffect(
      service.createAgent({
        name,
        description: "Checks recovery from provider failures.",
        avatarSeed: name.toLowerCase(),
        avatarHue: null,
        initialMessage: `Hello from ${name}.`,
        provider: "claude",
        model: PRIMARY,
      }),
    );
    agents.push({ id: agent.id, name });
    await waitFor(`${name}'s initial turn`, () =>
      service.listQueue(agent.id).deliveries.every((delivery) => delivery.status === "completed"),
    );
  }
  const [ada, ben] = agents;
  assert(ada && ben, "Two agents exist.");
  const fixtures: Record<string, string> = {};
  for (const [name, provider, accountId] of [
    ["Named A", "claude", "a1111111111111111"],
    ["Named B", "claude", "a2222222222222222"],
    ["Other provider", "codex", null],
  ] as const) {
    const agent = await runCauseEffect(
      store.createAgent({
        name,
        description: "Account scope fixture.",
        avatarSeed: name.toLowerCase().replaceAll(" ", "-"),
        avatarHue: null,
      }),
    );
    await runCauseEffect(
      store.updateAgent({ agentId: agent.id, provider, model: PRIMARY, providerAccountId: accountId }),
    );
    fixtures[name] = agent.id;
  }
  const gatePath = join(userData, "focused-failures.json");
  let gate = new UsageLimitGate(gateOptions(gatePath));
  const defaultKey = { provider: "claude" as const, accountId: null, model: PRIMARY };
  const namedKey = { provider: "claude" as const, accountId: "a1111111111111111", model: PRIMARY };
  const otherKey = { provider: "codex" as const, accountId: null, model: PRIMARY };
  try {
    await check("account reset releases failure-only keys with an exact account and reason", async () => {
      gate.failed(ada.id, PRIMARY, 3 * 86_400_000);
      gate.failed(fixtures["Named A"] ?? "", PRIMARY, 3 * 86_400_000);
      gate.failed(fixtures["Named B"] ?? "", PRIMARY, 3 * 86_400_000);
      gate.failed(fixtures["Other provider"] ?? "", PRIMARY, 3 * 86_400_000);
      const since = Date.now();
      const released = await runCauseEffect(gate.accountReset("claude", "a1111111111111111"));
      assert(
        released.length === 1 && released[0]?.accountId === "a1111111111111111",
        `One named account releases: ${JSON.stringify(released)}`,
      );
      assert(!gate.isFailed(namedKey), "The exact named failure ends.");
      assert(gate.isFailed(defaultKey) && gate.isFailed(otherKey), "Default and other provider remain held.");
      const logged = linesSince(since, "provider failure", "account-reset", "a1111111111111111");
      assert(logged.length === 1, `One failure release names account-reset: ${logged.length}`);
      return { released, logged };
    });
    await check("default reset deduplicates a key with both a limit and a failure", async () => {
      await runCauseEffect(gate.reached(ada.id, Math.floor(Date.now() / 1000) + RESET_AHEAD_S, PRIMARY));
      const since = Date.now();
      const released = await runCauseEffect(gate.accountReset("claude", "default"));
      assert(
        released.length === 1 && released[0]?.accountId === null,
        `The default key is returned once: ${JSON.stringify(released)}`,
      );
      assert(!gate.isFailed(defaultKey) && !gate.isLimited(defaultKey), "Both default holds end.");
      assert(gate.isFailed({ ...namedKey, accountId: "a2222222222222222" }), "The other account remains held.");
      assert(
        linesSince(since, "provider failure", "account-reset").length === 1,
        "The failure release is logged once.",
      );
      return released;
    });
    await check("null reset releases all provider accounts and persists account isolation", async () => {
      gate.failed(ada.id, PRIMARY, 3 * 86_400_000);
      gate.failed(fixtures["Named A"] ?? "", PRIMARY, 3 * 86_400_000);
      const released = await runCauseEffect(gate.accountReset("claude", null));
      assert(
        new Set(released.map((entry) => entry.accountId)).size === 3,
        `Default and both named accounts release: ${JSON.stringify(released)}`,
      );
      assert(gate.isFailed(otherKey), "Another provider stays held.");
      await runCauseEffect(gate.dispose());
      gate = new UsageLimitGate(gateOptions(gatePath));
      gate.restore();
      assert(!gate.isFailed(defaultKey) && !gate.isFailed(namedKey), "Released accounts stay released after restore.");
      assert(gate.isFailed(otherKey), "The untouched provider failure survives restore.");
      return { released, saved: savedState(gatePath) };
    });
    await check("completed turns release only their own provider account and model", async () => {
      gate.failed(ada.id, PRIMARY, 3 * 86_400_000);
      gate.failed(ada.id, BACKUP, 3 * 86_400_000);
      gate.failed(fixtures["Named A"] ?? "", PRIMARY, 3 * 86_400_000);
      const since = Date.now();
      await runCauseEffect(gate.completed(ada.id, PRIMARY));
      assert(!gate.isFailed(defaultKey), "The completed model releases.");
      assert(
        gate.isFailed({ ...defaultKey, model: BACKUP }) && gate.isFailed(namedKey),
        "Other models and accounts stay held.",
      );
      const logged = linesSince(since, "provider failure", "turn-completed", PRIMARY);
      assert(logged.length === 1, `One completion release is logged: ${logged.length}`);
      return logged;
    });
  } finally {
    await runCauseEffect(gate.dispose());
  }

  const seedFailure = async (withLimit = false) => {
    const seeded = new UsageLimitGate(gateOptions(savedLimits));
    if (withLimit) await runCauseEffect(seeded.reached(ada.id, Math.floor(Date.now() / 1000) + RESET_AHEAD_S, PRIMARY));
    seeded.failed(ada.id, PRIMARY, 3 * 86_400_000);
    seeded.failed(fixtures["Named A"] ?? "", PRIMARY, 3 * 86_400_000);
    const saved = savedState();
    assert(
      saved.failures.every((failure) => failure.until > Date.now() + 2 * 86_400_000),
      "The fixture uses real far-reset failure holds.",
    );
    await runCauseEffect(seeded.dispose());
    for (const agent of agents)
      await runCauseEffect(
        store.updateAgent({
          agentId: agent.id,
          provider: "claude",
          model: BACKUP,
          backupModels: {
            automatic: true,
            order: [
              { provider: "claude", model: PRIMARY },
              { provider: "claude", model: BACKUP },
            ],
          },
          usageLimitReturn: { provider: "claude", model: PRIMARY, reasoningEffort: "low", cause: "provider-error" },
        }),
      );
  };
  await check("POST /v1/usage/reset releases failure-only holds and returns fallback work to the primary", async () => {
    await seedFailure();
    await runCauseEffect(service.stop());
    ({ store, mailbox, service } = startService(60_000));
    await runCauseEffect(service.initialize());
    const server = new ServerMode(serverOptions());
    servers.push(server);
    await runCauseEffect(server.listen());
    const since = Date.now();
    const answer = await control("/v1/usage/reset", "POST", "provider=claude&accountId=default");
    assert(
      answer.status === 200 && answer.lines.released === "1",
      `One failure-only default key releases: ${JSON.stringify(answer)}`,
    );
    assert(
      savedState().failures.length === 1 && savedState().failures[0]?.accountId === "a1111111111111111",
      "The named account stays held.",
    );
    const deliveryId = await send(ada.id, "Ada: real CLI work after the host reset.");
    await waitFor("the reset work to complete", () => statusOf(deliveryId) === "completed");
    assert(
      starts(since).some((row) => row.model === PRIMARY && row.input.includes("real CLI work")),
      "The real primary CLI runs the work after fallback.",
    );
    const logged = linesSince(since, "provider failure", "host-reset", PRIMARY);
    assert(logged.length === 1, `The host failure release has its reason: ${logged.length}`);
    const all = await control("/v1/usage/reset", "POST", "provider=claude");
    assert(
      all.lines.released === "1" && savedState().failures.length === 0,
      `An all-account host reset releases named failure-only keys: ${JSON.stringify(all)}`,
    );
    await runCauseEffect(server.close());
    return { answer, all, logged };
  });

  for (const { withLimit, extraLanes } of [
    { withLimit: false, extraLanes: false },
    { withLimit: true, extraLanes: false },
    { withLimit: false, extraLanes: true },
  ]) {
    await check(
      extraLanes
        ? "one failure probe holds other task lanes of its own agent until completion"
        : withLimit
          ? "one combined usage-limit and failure probe returns from fallback without concurrent primary turns"
          : "one far-reset failure probe returns from fallback and completion releases the key",
      async () => {
        assert(USAGE_LIMIT_PROBE_MS === 15 * 60_000, "The production probe interval is fifteen minutes.");
        await seedFailure(withLimit);
        await runCauseEffect(service.stop());
        ({ store, mailbox, service } = startService(PROBE_MS, false));
        setMode("slow-accept");
        const since = Date.now();
        await runCauseEffect(service.initialize());
        const deliveries: string[] = [];
        for (const agent of agents) {
          const receipt = await runCauseEffect(
            mailbox.enqueue({
              sender: { kind: "user" },
              recipientAgentIds: [agent.id],
              text: `${agent.name}: queued recovery probe.`,
              draftIds: [],
            }),
          );
          const deliveryId = receipt.deliveries[0]?.id;
          assert(deliveryId, "The recovery work has a delivery.");
          deliveries.push(deliveryId);
        }
        await waitFor(
          "one failure probe permit",
          () => linesSince(since, withLimit ? "usage limit" : "provider failure", "waiting turn try")[0],
          PROBE_MS * 5,
        );
        const permitAt = Date.now();
        assert(permitAt - since >= PROBE_MS - 100, "The probe waits for its interval.");
        await runCauseEffect(service.setProviderOn("claude", true));
        const primary = await waitFor(
          "the real primary recovery turn",
          () => starts(since).find((row) => row.model === PRIMARY),
          PROBE_MS * 5,
        );
        const tasks: string[] = [];
        if (extraLanes) {
          const probingAgent = service
            .listAgents()
            .find((agent) => agents.some((candidate) => candidate.id === agent.id) && agent.model === PRIMARY);
          assert(probingAgent, "The primary probe has an agent.");
          for (const lane of [1, 2]) {
            const task = await runCauseEffect(
              service.startTask({
                agentId: probingAgent.id,
                text: `A separate task lane ${lane} beside the recovery probe.`,
              }),
            );
            tasks.push(task.id);
          }
        }
        const completed = await waitFor(
          "the primary recovery turn to complete",
          () => ends(since).find((row) => row.id === primary.id),
          SLOW_MS + 5000,
        );
        const concurrent = starts(since).filter(
          (row) => row.model === PRIMARY && row.id !== primary.id && row.startedAt <= (completed.endedAt ?? 0),
        );
        assert(concurrent.length === 0, `Only one primary turn probes the failed key: ${JSON.stringify(concurrent)}`);
        if (extraLanes) {
          const probingAgent = service
            .listAgents()
            .find((agent) => agents.some((candidate) => candidate.id === agent.id) && agent.model === PRIMARY);
          assert(probingAgent, "The completed primary probe keeps its agent.");
          for (const taskId of tasks)
            await waitFor("the held task lane to run after the probe", () =>
              service.readTask(probingAgent.id, taskId).messages.some((message) => message.text === "Done."),
            );
          const taskStarts = starts(since).filter((row) => row.input.includes("A separate task lane"));
          assert(taskStarts.length === 2, "Both native task lanes run after recovery.");
          assert(
            taskStarts.every((row) => row.startedAt >= (completed.endedAt ?? 0)),
            "Both task lanes wait for the probe to complete.",
          );
          const firstTask = taskStarts[0];
          const secondTask = taskStarts[1];
          const firstEnd = ends(since).find((row) => row.id === firstTask?.id);
          assert(
            firstEnd && secondTask && secondTask.startedAt < (firstEnd.endedAt ?? 0),
            "Native task lanes can run concurrently after the failure ends.",
          );
        }
        await waitFor(
          "the failed primary key to release",
          () =>
            !savedState().failures.some(
              (failure) => failure.provider === "claude" && failure.accountId === null && failure.model === PRIMARY,
            ),
        );
        for (const deliveryId of deliveries)
          await waitFor("queued recovery work to finish", () => statusOf(deliveryId) === "completed");
        assert(
          savedState().failures.some((failure) => failure.accountId === "a1111111111111111"),
          "A different account remains held.",
        );
        const released = linesSince(since, "provider failure", "probe-completed", PRIMARY);
        assert(released.length === 1, `One completed probe releases the failure: ${released.length}`);
        setMode("accept");
        const followup = await send(ben.id, "Ben: primary after successful failure probe.");
        await waitFor("work after probe success", () => statusOf(followup) === "completed");
        assert(
          service.listAgents().find((agent) => agent.id === ben.id)?.model === PRIMARY,
          "Another fallback agent returns after probe success.",
        );
        return { primary, released, simultaneousPrimaryProbes: concurrent.length };
      },
    );
  }
  report.passed = report.checks.every((item) => item.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  for (const server of servers) await runCauseEffect(server.close()).catch(() => {});
  await runCauseEffect(service.stop()).catch(() => {});
  report.gateLog = gateLines.map((entry) => entry.line);
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
