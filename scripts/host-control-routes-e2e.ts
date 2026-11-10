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
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";
import type { ServerModeOptions } from "../src/main/server-mode";

const OUT = resolve(import.meta.dirname, "../.openbot-build/host-control-routes-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-host-control-routes-"));
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
  log({ phase: "start", model, mode, at: Date.now(), input });
  if (mode === "hold") {
    held = { input };
    assistant("Working.");
    return;
  }
  assistant("Done.");
  result({ is_error: false, result: "Done." });
  log({ phase: "end", model, mode, at: Date.now(), input });
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

function turnRows(): { phase: string; input: string }[] {
  try {
    return readFileSync(turnLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}

function setMode(mode: "accept" | "hold") {
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

async function send(agentId: string, text: string): Promise<string> {
  const receipt = await runCauseEffect(service.sendMessage({ agentId, text, attachmentDraftIds: [] }));
  const deliveryId = receipt.deliveries[0]?.id;
  assert(deliveryId, "The message has a delivery.");
  return deliveryId;
}

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

interface QueueRow {
  name: string;
  provider: string;
  model: string;
  providerAccountId: string | null;
  backupModels: unknown;
  queued: number;
  oldestQueuedAt: string | null;
  runningTurnId: string | null;
  runningTurnStartedAt: string | null;
  runningTurns: number;
}

function queueRows(lines: { [key: string]: string }): Map<string, QueueRow> {
  return new Map(
    Object.entries(lines)
      .filter(([key]) => key.startsWith("agent."))
      .map(([key, value]) => [key.slice("agent.".length), JSON.parse(value)]),
  );
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

try {
  await runCauseEffect(service.initialize());
  await runCauseEffect(server.listen());
  const agents: { id: string; name: string }[] = [];
  for (const name of ["Ada", "Ben"]) {
    const created = await runCauseEffect(
      service.createAgent({
        name,
        description: "Works on host control checks.",
        avatarSeed: name.toLowerCase(),
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
  const [ada, ben] = agents;
  assert(ada && ben, "Two agents exist.");

  setMode("hold");
  const held = await send(ada.id, "Ada: a long task.");
  const heldTurn = await waitFor("Ada's held turn", () => {
    const delivery = deliveryOf(held);
    return delivery?.status === "running" && delivery.turnId ? delivery.turnId : null;
  });
  const waiting = await send(ada.id, "Ada: the next task.");
  const waitingCreatedAt = deliveryOf(waiting)?.createdAt;

  await check("GET /v1/agents/queue lists each agent's model, queue and running turn", async () => {
    const answer = await control("/v1/agents/queue", "GET");
    assert(answer.status === 200, `The queue answers 200: ${answer.status} ${JSON.stringify(answer.lines)}`);
    assert(answer.lines.agents === "2", `It counts 2 agents: ${answer.lines.agents}`);
    const rows = queueRows(answer.lines);
    const adaRow = rows.get(ada.id);
    const benRow = rows.get(ben.id);
    assert(adaRow && benRow, `It has a line for each agent: ${[...rows.keys()].join(", ")}`);
    assert(
      adaRow.name === "Ada" && adaRow.provider === "claude" && adaRow.model === PRIMARY,
      `Ada's model: ${JSON.stringify(adaRow)}`,
    );
    assert(
      adaRow.providerAccountId === null && adaRow.backupModels === null,
      `Ada's account and backups: ${JSON.stringify(adaRow)}`,
    );
    assert(adaRow.queued === 1 && adaRow.oldestQueuedAt === waitingCreatedAt, `Ada's queue: ${JSON.stringify(adaRow)}`);
    assert(
      adaRow.runningTurnId === heldTurn && typeof adaRow.runningTurnStartedAt === "string",
      `Ada's running turn: ${JSON.stringify(adaRow)} (expected ${heldTurn})`,
    );
    assert(benRow.queued === 0 && benRow.runningTurnId === null, `Ben is idle: ${JSON.stringify(benRow)}`);
    return { ada: adaRow, ben: benRow };
  });

  await check("POST /v1/turns/interrupt refuses bad input, an unknown agent and a turn that does not run", async () => {
    const empty = await control("/v1/turns/interrupt", "POST", "agentId=");
    assert(empty.status === 400, `No agent gets 400: ${empty.status} ${JSON.stringify(empty.lines)}`);
    const unknown = await control("/v1/turns/interrupt", "POST", "agentId=agent-nobody");
    assert(unknown.status === 404, `An unknown agent gets 404: ${unknown.status}`);
    const other = await control("/v1/turns/interrupt", "POST", `agentId=${ada.id}&turnId=turn-not-this-one`);
    assert(
      other.status === 409 && other.lines.error === "turn_mismatch" && other.lines.running === heldTurn,
      `Another turn id gets 409: ${other.status} ${JSON.stringify(other.lines)}`,
    );
    const idle = await control("/v1/turns/interrupt", "POST", `agentId=${ben.id}`);
    assert(idle.status === 409 && idle.lines.error === "no_running_turn", `An idle agent gets 409: ${idle.status}`);
    const wrongMethod = await control("/v1/turns/interrupt", "GET");
    assert(wrongMethod.status === 404, `GET gets 404: ${wrongMethod.status}`);
    await new Promise((done) => setTimeout(done, 300));
    assert(deliveryOf(held)?.status === "running", `The held turn still runs: ${deliveryOf(held)?.status}`);
    assert(!turnRows().some((row) => row.phase === "interrupted"), "No stop reached the CLI.");
    return { empty: empty.lines, unknown: unknown.status, other: other.lines, idle: idle.lines };
  });

  await check(
    "POST /v1/turns/interrupt stops the running turn as the app's Stop does, and the queue moves",
    async () => {
      setMode("accept");
      const answer = await control("/v1/turns/interrupt", "POST", `agentId=${ada.id}&turnId=${heldTurn}`);
      assert(
        answer.status === 200 && answer.lines.stopped === heldTurn,
        `The stop answers 200: ${answer.status} ${JSON.stringify(answer.lines)}`,
      );
      assert(answer.lines.ownChat === "true", `The turn runs in Ada's chat: ${JSON.stringify(answer.lines)}`);
      await waitFor("the CLI to get the stop", () => turnRows().some((row) => row.phase === "interrupted"));
      const ended = await waitFor("the held delivery to end", () => {
        const status = deliveryOf(held)?.status;
        return status && status !== "running" && status !== "starting" ? status : null;
      });
      assert(ended === "interrupted", `The held delivery ends interrupted: ${ended}`);
      await waitFor("the next message to complete", () => deliveryOf(waiting)?.status === "completed");
      setMode("hold");
      const benHeld = await send(ben.id, "Ben: a long task.");
      await waitFor("Ben's held turn", () => deliveryOf(benHeld)?.status === "running" && deliveryOf(benHeld)?.turnId);
      setMode("accept");
      const noTurnId = await control("/v1/turns/interrupt", "POST", `agentId=${ben.id}`);
      assert(
        noTurnId.status === 200,
        `Without a turn id the chat's turn stops: ${noTurnId.status} ${JSON.stringify(noTurnId.lines)}`,
      );
      const benEnded = await waitFor("Ben's held delivery to end", () => {
        const status = deliveryOf(benHeld)?.status;
        return status && status !== "running" && status !== "starting" ? status : null;
      });
      assert(benEnded === "interrupted", `Ben's delivery ends interrupted: ${benEnded}`);
      const after = queueRows((await control("/v1/agents/queue", "GET")).lines);
      assert(
        after.get(ada.id)?.runningTurnId === null && after.get(ada.id)?.queued === 0,
        `Ada is idle: ${JSON.stringify(after.get(ada.id))}`,
      );
      return { ada: answer.lines, ben: noTurnId.lines, adaEnded: ended, benEnded };
    },
  );

  await check(
    "POST /v1/agents/update sets backup models, account and model through the update_profile checks",
    async () => {
      const backups = { automatic: true, order: [{ provider: "claude", model: SECOND }] };
      const set = await control(
        "/v1/agents/update",
        "POST",
        `agentId=${ada.id}&backupModels=${encodeURIComponent(JSON.stringify(backups))}`,
      );
      assert(set.status === 200, `The update answers 200: ${set.status} ${JSON.stringify(set.lines)}`);
      const saved = service.listAgents().find((agent) => agent.id === ada.id);
      assert(
        JSON.stringify(saved?.backupModels) === JSON.stringify(backups),
        `Ada keeps the backup models: ${JSON.stringify(saved?.backupModels)}`,
      );
      assert(set.lines.backupModels === JSON.stringify(backups), `The answer names them: ${set.lines.backupModels}`);
      const listed = queueRows((await control("/v1/agents/queue", "GET")).lines).get(ada.id);
      assert(
        JSON.stringify(listed?.backupModels) === JSON.stringify(backups),
        `The queue shows them: ${JSON.stringify(listed)}`,
      );
      const model = await control("/v1/agents/update", "POST", `agentId=${ada.id}&model=${SECOND}`);
      assert(
        model.status === 200 && model.lines.model === SECOND,
        `The model changes: ${model.status} ${JSON.stringify(model.lines)}`,
      );
      const account = await control("/v1/agents/update", "POST", `agentId=${ada.id}&providerAccountId=default`);
      assert(
        account.status === 200 && account.lines.providerAccountId === "default",
        `The default account is accepted: ${account.status} ${JSON.stringify(account.lines)}`,
      );
      const cleared = await control("/v1/agents/update", "POST", `agentId=${ada.id}&backupModels=null`);
      assert(
        cleared.status === 200 && service.listAgents().find((agent) => agent.id === ada.id)?.backupModels === undefined,
        `null removes the backup models: ${cleared.status} ${JSON.stringify(cleared.lines)}`,
      );
      return { set: set.lines, model: model.lines, account: account.lines };
    },
  );

  await check("POST /v1/agents/update refuses other fields and input that update_profile refuses", async () => {
    const before = JSON.stringify(service.listAgents().find((agent) => agent.id === ben.id));
    const refusals: Record<string, { status: number; error?: string; message?: string }> = {};
    const tryUpdate = async (label: string, body: string, status: number) => {
      const answer = await control("/v1/agents/update", "POST", body);
      refusals[label] = { status: answer.status, error: answer.lines.error, message: answer.lines.message };
      assert(answer.status === status, `${label} gets ${status}: ${answer.status} ${JSON.stringify(answer.lines)}`);
    };
    await tryUpdate("no agent", "agentId=", 400);
    await tryUpdate("an unknown agent", `agentId=agent-nobody&model=${PRIMARY}`, 404);
    await tryUpdate("no field", `agentId=${ben.id}`, 400);
    await tryUpdate("another field", `agentId=${ben.id}&name=Renamed`, 400);
    await tryUpdate("a repeated field", `agentId=${ben.id}&model=${PRIMARY}&model=${SECOND}`, 400);
    await tryUpdate("unreadable JSON", `agentId=${ben.id}&backupModels=%7Bnot-json`, 400);
    await tryUpdate(
      "an unlisted backup model",
      `agentId=${ben.id}&backupModels=${encodeURIComponent(JSON.stringify({ automatic: true, order: [{ provider: "claude", model: "claude-nothing-9" }] }))}`,
      400,
    );
    await tryUpdate(
      "a backup with an extra key",
      `agentId=${ben.id}&backupModels=${encodeURIComponent(JSON.stringify({ automatic: true, order: [], extra: 1 }))}`,
      400,
    );
    await tryUpdate("an unlisted model", `agentId=${ben.id}&model=claude-nothing-9`, 400);
    await tryUpdate("an unknown provider", `agentId=${ben.id}&provider=nobody`, 400);
    await tryUpdate("a malformed account", `agentId=${ben.id}&providerAccountId=nope`, 400);
    await tryUpdate("an account that does not exist", `agentId=${ben.id}&providerAccountId=a0123456789abcdef`, 400);
    const after = JSON.stringify(service.listAgents().find((agent) => agent.id === ben.id));
    assert(before === after, `Ben is unchanged: ${after}`);
    assert(refusals["an unlisted backup model"]?.message?.includes("claude-nothing-9"), "The refusal names the model.");
    return refusals;
  });

  report.passed = report.checks.length === 5 && report.checks.every((item) => item.passed);
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
    `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of 5 ${join(OUT, "report.json")}\n`,
  );
}
