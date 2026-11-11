// Checks the turn buffer of a server (0058). On 11 November the server ran up to 16 turns at once on
// its 12 GiB container. Background turns (routine runs and teammate messages) filled every slot, so a
// person's message waited behind them, and the main thread stalled while they ran.
//
// OPENBOT_TURN_LIMIT caps the turn limit that the memory gives. OPENBOT_PERSON_RESERVED_SLOTS (3 when
// the limit is set) keeps slots that only a person's message may use. While the main loop p99 of two
// samples is over OPENBOT_SHED_P99_MS (2000), background turns get half of their slots, until three
// samples are under it. The control socket shows the effective limits.
//
// Runs the real ServerMemory with a fixed 12 GiB sample and the real ServerMode on a temporary socket
// for the limits. The drain runs in the real agent service with a stand-in Claude CLI whose turns run
// until the check releases them; its memory has the limit of 10 that the cap gives, and the
// background limit of the turn buffer, as ServerMemory has. The samples of the resource monitor need
// Electron, so the check gives the buffer its main-loop samples. No app, account or network is used.
//
// Run: node --import tsx scripts/turn-buffer-e2e.ts
// Writes .openbot-build/turn-buffer-e2e/report.json and exits non-zero on a failure.
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
import type { HostMemory } from "../src/backend/host-memory";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";

const OUT = resolve(import.meta.dirname, "../.openbot-build/turn-buffer-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-turn-buffer-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const releases = join(root, "releases");
const claudeExecutable = join(root, "claude");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const GIB = 1024 * 1024 * 1024;
const SAMPLE = { total: 12 * GIB, available: 10 * GIB };
const AGENTS = 12;
const SETTLE_MS = 2_000;

process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(home, { recursive: true });
mkdirSync(releases, { recursive: true });

const bufferLines: string[] = [];
teeLogLines(["turn-buffer"], (line) => bufferLines.push(line));

type BufferModule = typeof import("../src/backend/agent/turn-buffer");
const bufferModule: BufferModule | null = await import("../src/backend/agent/turn-buffer").catch(() => null);
const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const { ServerMemory } = await import("../src/main/server-memory");
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
const queue = [];
let busy = false;
const next = () => {
  if (busy) return;
  const message = queue.shift();
  if (!message) return;
  busy = true;
  record(message);
  const input = JSON.stringify(message);
  const token = [...input.matchAll(/\\[t:([a-z0-9-]+)\\]/g)].at(-1)?.[1] ?? null;
  const id = sessionId + "-" + randomUUID();
  log({ id, phase: "start", token, startedAt: Date.now() });
  const finish = () => {
    assistant("Done.");
    result({ is_error: false, result: "Done." });
    log({ id, phase: "end", token, endedAt: Date.now() });
    busy = false;
    next();
  };
  if (!token) return finish();
  const release = require("node:path").join(${JSON.stringify(releases)}, token);
  const timer = setInterval(() => {
    if (!fs.existsSync(release)) return;
    clearInterval(timer);
    finish();
  }, 40);
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
    } else if (message.type === "user") {
      queue.push(message);
      next();
    }
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
        return providerSync(() => decoder({ data: [{ model: PRIMARY, displayName: "Primary" }] }));
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
  bufferModule: boolean;
  checks: { name: string; passed: boolean; detail?: unknown }[];
  bufferLog: string[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
} = { bufferModule: bufferModule !== null, checks: [], bufferLog: [], passed: false };

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

const pause = (ms: number) => new Promise((done) => setTimeout(done, ms));

interface TurnRow {
  id: string;
  phase: "start" | "end";
  token: string | null;
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

function running(prefix: string): string[] {
  const all = rows();
  const ended = new Set(all.filter((row) => row.phase === "end").map((row) => row.id));
  return all
    .filter((row) => row.phase === "start" && row.token?.startsWith(prefix) && !ended.has(row.id))
    .map((row) => row.token ?? "");
}

function started(token: string): boolean {
  return rows().some((row) => row.phase === "start" && row.token === token);
}

function release(token: string): void {
  writeFileSync(join(releases, token), "");
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

const turnBuffer = bufferModule
  ? new bufferModule.TurnBuffer(bufferModule.turnBufferSettings({ OPENBOT_TURN_LIMIT: "10" }))
  : null;
const serverMemory = (environment: NodeJS.ProcessEnv) => {
  const buffer = bufferModule ? new bufferModule.TurnBuffer(bufferModule.turnBufferSettings(environment)) : null;
  return new ServerMemory({
    hostedPlan: false,
    onError: () => {},
    readSample: () => SAMPLE,
    ...(buffer ? { turnBuffer: buffer } : {}),
  });
};
const configured = new ServerMemory({
  hostedPlan: false,
  onError: () => {},
  readSample: () => SAMPLE,
  ...(turnBuffer ? { turnBuffer } : {}),
});
await Effect.runPromise(configured.tick());

const sampleListeners = new Set<() => void>();
const drainMemory: HostMemory = {
  hostedPlan: false,
  level: () => "ok",
  turnLimit: () => 10,
  reserveTurn: () => () => {},
  subscribe: (listener) => {
    sampleListeners.add(listener);
    return () => sampleListeners.delete(listener);
  },
  ...(turnBuffer ? { backgroundTurnLimit: (limit: number) => turnBuffer.backgroundLimit(limit) } : {}),
};
const sample = () => {
  for (const listener of sampleListeners) listener();
};

await writeFile(claudeExecutable, STAND_IN_CLAUDE, { mode: 0o755 });
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
const service = new AgentService({
  store,
  mailbox,
  browser,
  hostMemory: drainMemory,
  providerRetryDelayMs: 50,
  preferredProvider: "claude",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "claude"),
  clientFactory: (_provider, cli) => new ClaudeStandIn(cli),
});

async function control(path: string, method = "GET"): Promise<{ status: number; lines: Record<string, string> }> {
  return new Promise((done, fail) => {
    const req = request({ socketPath: join(runtime, "control.sock"), path, method }, (response) => {
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
    });
    req.on("error", fail);
    req.end();
  });
}

const runtime = await mkdtemp(join("/tmp", "obtb-"));
const authState: CentralAuthState = { status: "signed_out" };
const serverMode = (memory: InstanceType<typeof ServerMemory>, socket: string) =>
  new ServerMode({
    environment: { controlSocketPath: join(runtime, socket) },
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
    memory,
    agents: service,
    onError: () => {},
  });
const servers: InstanceType<typeof ServerMode>[] = [];

try {
  await runCauseEffect(service.initialize());
  const agents: { id: string; name: string }[] = [];
  for (let index = 0; index < AGENTS; index += 1) {
    const name = `Agent ${String.fromCharCode(65 + index)}`;
    const created = await runCauseEffect(
      service.createAgent({
        name,
        description: "Runs routine work.",
        avatarSeed: `agent-${index}`,
        avatarHue: null,
        initialMessage: `Hello from ${name}.`,
        provider: "claude",
        model: PRIMARY,
      }),
    );
    agents.push({ id: created.id, name });
  }
  for (const agent of agents)
    await waitFor(`${agent.name}'s first turn`, () =>
      service.listQueue(agent.id).deliveries.every((delivery) => delivery.status === "completed"),
    );
  const routineIds = new Map<string, string>();
  for (const [index, agent] of agents.entries())
    for (const round of [1, 2]) {
      const routine = service.createRoutine({
        agentId: agent.id,
        name: `Round ${round}`,
        instruction: `Check the builds. [t:r${round}-${index}]`,
        active: false,
        timezone: "UTC",
        schedule: { kind: "daily", time: "09:00" },
      });
      routineIds.set(`${round}-${index}`, routine.id);
    }
  const runRoutine = (round: number, index: number) => {
    const agent = agents[index];
    const routineId = routineIds.get(`${round}-${index}`);
    assert(agent && routineId, "The routine exists.");
    return runCauseEffect(service.testRoutine({ agentId: agent.id, routineId }));
  };

  await check("OPENBOT_TURN_LIMIT=10 caps the memory limit of a 12 GiB server at 10", async () => {
    const limit = configured.turnLimit();
    assert(limit === 10, `The turn limit is ${limit}.`);
    const background = "backgroundTurnLimit" in configured ? configured.backgroundTurnLimit(limit) : undefined;
    assert(background === 7, `The background limit is ${String(background)}.`);
    const unset = serverMemory({});
    await Effect.runPromise(unset.tick());
    assert(unset.turnLimit() === 16, `With no setting the limit is ${unset.turnLimit()}.`);
    const unsetBackground = "backgroundTurnLimit" in unset ? unset.backgroundTurnLimit(16) : undefined;
    assert(unsetBackground === null, `With no setting the background limit is ${String(unsetBackground)}.`);
    return { limit, background, unset: unset.turnLimit() };
  });

  await check("10 routine runs start 7 turns, and a person's message starts at once beside them", async () => {
    for (let index = 0; index < 10; index += 1) await runRoutine(1, index);
    await waitFor("7 routine turns", () => running("r1-").length >= 7);
    await pause(SETTLE_MS);
    const background = running("r1-").length;
    const person = agents[10];
    assert(person, "The person's agent exists.");
    await runCauseEffect(
      service.sendMessage({ agentId: person.id, text: "Please look now. [t:p1-10]", attachmentDraftIds: [] }),
    );
    const personStarted = await waitFor("the person's turn", () => started("p1-10"), 5_000).catch(() => false);
    assert(
      background === 7 && personStarted,
      `${background} routine turns run at once, and the person's message ${personStarted ? "starts" : "waits 5 s behind them"}.`,
    );
    return { background, person: "started" };
  });

  await check("a background turn that ends lets the next routine run start", async () => {
    const before = running("r1-");
    const first = before[0];
    assert(first, "A routine turn runs.");
    release(first);
    await waitFor("the next routine turn", () => running("r1-").length === 7 && !running("r1-").includes(first));
    await pause(SETTLE_MS);
    assert(running("r1-").length === 7, `${running("r1-").length} routine turns run.`);
    return { released: first, running: running("r1-").length };
  });

  for (let index = 0; index < 10; index += 1) release(`r1-${index}`);
  release("p1-10");
  await waitFor("round 1 to end", () => running("r1-").length === 0 && running("p1-").length === 0, 30_000);
  for (const agent of agents)
    await waitFor(`${agent.name}'s queue to settle`, () =>
      service.listQueue(agent.id).deliveries.every((delivery) => delivery.status === "completed"),
    );

  await check("two slow main-loop samples halve the background slots, with one log line", async () => {
    assert(turnBuffer, "No turn buffer module.");
    turnBuffer.observeMainLoop(3_100);
    turnBuffer.observeMainLoop(2_900);
    assert(turnBuffer.shedding(), "The buffer does not shed.");
    const lines = bufferLines.filter((line) => line.includes("Background turns get half"));
    assert(lines.length === 1, `${lines.length} shed lines.`);
    for (let index = 0; index < 10; index += 1) await runRoutine(2, index);
    await waitFor("3 routine turns", () => running("r2-").length >= 3);
    await pause(SETTLE_MS);
    assert(running("r2-").length === 3, `${running("r2-").length} routine turns run while the buffer sheds.`);
    return { running: running("r2-").length, line: lines[0] };
  });

  await check("the control socket shows the effective limits and the shed state", async () => {
    const server = serverMode(configured, "control.sock");
    servers.push(server);
    await runCauseEffect(server.listen());
    const status = await control("/v1/status");
    assert(status.status === 200, `status ${status.status}`);
    const expected = {
      turn_limit: "10",
      turn_limit_memory: "16",
      turn_limit_cap: "10",
      person_reserved_slots: "3",
      background_turn_limit: "3",
      shed: "on",
      shed_p99_ms: "2000",
      main_loop_p99_ms: "2900",
    };
    for (const [key, value] of Object.entries(expected))
      assert(status.lines[key] === value, `status ${key}=${String(status.lines[key])}, expected ${value}`);
    const queue = await control("/v1/agents/queue");
    assert(queue.status === 200, `queue ${queue.status}`);
    for (const [key, value] of Object.entries(expected))
      assert(queue.lines[key] === value, `queue ${key}=${String(queue.lines[key])}, expected ${value}`);
    assert(queue.lines.agents === String(AGENTS), `agents=${String(queue.lines.agents)}`);
    const rowsListed = Object.keys(queue.lines).filter((key) => key.startsWith("agent.")).length;
    assert(rowsListed === AGENTS, `${rowsListed} agent rows.`);
    const unset = serverMemory({});
    await Effect.runPromise(unset.tick());
    const plain = serverMode(unset, "plain.sock");
    servers.push(plain);
    await runCauseEffect(plain.listen());
    const plainStatus = await new Promise<Record<string, string>>((done, fail) => {
      const req = request(
        { socketPath: join(runtime, "plain.sock"), path: "/v1/status", method: "GET" },
        (response) => {
          let text = "";
          response.setEncoding("utf8");
          response.on("data", (chunk) => {
            text += chunk;
          });
          response.on("end", () =>
            done(
              Object.fromEntries(
                text
                  .trim()
                  .split("\n")
                  .map((line) => [line.slice(0, line.indexOf("=")), line.slice(line.indexOf("=") + 1)]),
              ),
            ),
          );
        },
      );
      req.on("error", fail);
      req.end();
    });
    const plainExpected = {
      turn_limit: "16",
      turn_limit_cap: "none",
      person_reserved_slots: "0",
      background_turn_limit: "none",
      shed: "disabled",
    };
    for (const [key, value] of Object.entries(plainExpected))
      assert(plainStatus[key] === value, `unset ${key}=${String(plainStatus[key])}, expected ${value}`);
    return { status: expected, unset: plainExpected };
  });

  await check("three fast samples restore the background slots, with one log line", async () => {
    assert(turnBuffer, "No turn buffer module.");
    for (const p99 of [120, 90, 150]) turnBuffer.observeMainLoop(p99);
    assert(!turnBuffer.shedding(), "The buffer still sheds.");
    const lines = bufferLines.filter((line) => line.includes("Background turns get all"));
    assert(lines.length === 1, `${lines.length} restore lines.`);
    sample();
    await waitFor("7 routine turns", () => running("r2-").length >= 7);
    await pause(SETTLE_MS);
    assert(running("r2-").length === 7, `${running("r2-").length} routine turns run after the restore.`);
    const changes = bufferLines.filter((line) => line.includes("Background turns get"));
    assert(changes.length === 2, `${changes.length} change lines in all.`);
    return { running: running("r2-").length, line: lines[0] };
  });

  await check("OPENBOT_SHED=0 keeps every background slot while the main loop is slow", async () => {
    assert(bufferModule, "No turn buffer module.");
    const off = new bufferModule.TurnBuffer(
      bufferModule.turnBufferSettings({ OPENBOT_TURN_LIMIT: "10", OPENBOT_SHED: "0" }),
    );
    for (let index = 0; index < 4; index += 1) off.observeMainLoop(5_000);
    assert(!off.shedding() && off.backgroundLimit(10) === 7, `background ${off.backgroundLimit(10)}`);
    return { background: off.backgroundLimit(10) };
  });

  for (let index = 0; index < 10; index += 1) release(`r2-${index}`);
  await waitFor("round 2 to end", () => running("r2-").length === 0, 30_000);

  report.passed = report.checks.every((item) => item.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  for (const token of rows().flatMap((row) => (row.token ? [row.token] : []))) release(token);
  for (const server of servers) await runCauseEffect(server.close()).catch(() => {});
  await runCauseEffect(service.stop()).catch(() => {});
  report.bufferLog = bufferLines;
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
