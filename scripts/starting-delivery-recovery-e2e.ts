import { EventEmitter } from "node:events";
import { mkdirSync, writeFileSync } from "node:fs";
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

const OUT = resolve(import.meta.dirname, "../.openbot-build/starting-delivery-recovery-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-starting-delivery-"));
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
process.env.OPENBOT_STARTING_DELIVERY_MINUTES = "0.02";
process.env.OPENBOT_TURN_REAPER_INTERVAL_SECONDS = "0.1";
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

  const enqueue = async (agentId: string, kind: "routine" | "agent" | "user", text: string) => {
    const sender =
      kind === "routine"
        ? {
            kind: "routine" as const,
            routineId: "fixture-routine",
            runId: text,
            routineName: "Fixture",
            scheduledFor: new Date().toISOString(),
          }
        : kind === "agent"
          ? { kind: "agent" as const, agentId: ben.id }
          : { kind: "user" as const };
    const receipt = await runCauseEffect(mailbox.enqueue({ sender, recipientAgentIds: [agentId], text }));
    const id = receipt.deliveries[0]?.id;
    assert(id, "Delivery exists.");
    return id;
  };
  const post = (fields: Record<string, string>) =>
    control("/v1/deliveries/cancel", "POST", new URLSearchParams(fields).toString());
  const fields = { agentId: ada.id, senderKind: "routine", createdBefore: new Date(Date.now()).toISOString() };
  const pending = await enqueue(ada.id, "routine", "cancel queued");
  const starting = await enqueue(ada.id, "routine", "cancel starting");
  await runCauseEffect(mailbox.markStarting(starting));
  const otherKind = await enqueue(ada.id, "agent", "keep agent");
  const otherAgent = await enqueue(ben.id, "routine", "keep other recipient");
  for (const key of ["agentId", "senderKind", "createdBefore"]) {
    await check(`cancel refuses missing ${key}`, async () => {
      const body: Record<string, string> = { ...fields };
      delete body[key];
      const answer = await post(body);
      assert(answer.status === 400, JSON.stringify(answer));
      assert(deliveryOf(pending)?.status === "queued", "Refusal changes no delivery.");
      return answer;
    });
  }
  for (const [name, extra] of Object.entries<Record<string, string>>({
    badDate: { createdBefore: "yesterday" },
    future: { createdBefore: "2099-01-01T00:00:00.000Z" },
    badKind: { senderKind: "all" },
    running: { statuses: "running" },
    emptyStatuses: { statuses: "" },
    unknown: { agentId: "agent-unknown" },
  }))
    await check(`cancel refuses ${name}`, async () => {
      const answer = await post({ ...fields, ...extra });
      assert(answer.status === (name === "unknown" ? 404 : 400), JSON.stringify(answer));
      return answer;
    });
  await check("cancel refuses repeated and unknown fields", async () => {
    for (const suffix of ["&agentId=other", "&surprise=1"]) {
      const answer = await control("/v1/deliveries/cancel", "POST", new URLSearchParams(fields).toString() + suffix);
      assert(answer.status === 400, JSON.stringify(answer));
    }
  });
  await check("cancel uses native delivery state and exact recipient, sender and cutoff", async () => {
    const cutoff = new Date().toISOString();
    await new Promise((done) => setTimeout(done, 5));
    const newer = await enqueue(ada.id, "routine", "keep newer");
    const answer = await post({ ...fields, createdBefore: cutoff });
    const ids = JSON.parse(answer.lines.ids ?? "[]");
    assert(Array.isArray(ids), "The ids are an array.");
    assert(
      answer.status === 200 && answer.lines.count === "2" && ids.includes(pending) && ids.includes(starting),
      JSON.stringify(answer),
    );
    for (const id of [pending, starting]) {
      assert(
        deliveryOf(id)?.status === "cancelled" && deliveryOf(id)?.error?.includes("Host control"),
        "Cancellation and reason persist.",
      );
      const row = database.connection
        .prepare("SELECT status, delivery_json FROM projection_deliveries WHERE delivery_id = ?")
        .get(id);
      assert(row && typeof row.delivery_json === "string", "Stored delivery exists.");
      assert(row.status === "cancelled" && JSON.parse(row.delivery_json).error, "Native projection agrees.");
    }
    for (const id of [newer, otherKind, otherAgent])
      assert(deliveryOf(id)?.status !== "cancelled" && !ids.includes(id), "Unselected work remains.");
    const retry = await post({ ...fields, createdBefore: cutoff });
    assert(retry.status === 200 && retry.lines.count === "0", "Retry cancels no extra work.");
    return answer;
  });
  await check("statuses restrict cancellation and never cancel a delivery with a turn", async () => {
    const queued = await enqueue(ada.id, "routine", "status queued");
    const withTurn = await enqueue(ada.id, "routine", "status running");
    await runCauseEffect(mailbox.markStarting(withTurn));
    await runCauseEffect(mailbox.markRunning(withTurn, "fixture-running-turn"));
    const answer = await post({ ...fields, createdBefore: new Date().toISOString(), statuses: "queued" });
    assert(answer.status === 200 && deliveryOf(queued)?.status === "cancelled", JSON.stringify(answer));
    assert(deliveryOf(withTurn)?.status === "running", "Running work remains.");
    await runCauseEffect(mailbox.markTerminal(withTurn, "interrupted"));
    return answer;
  });
  await check("reaper settles a starting delivery without a turn and drains the lane", async () => {
    const stuck = await enqueue(ada.id, "routine", "stuck start");
    await runCauseEffect(mailbox.markStarting(stuck));
    const next = await enqueue(ada.id, "user", "next after stuck");
    assert(deliveryOf(stuck)?.status === "starting" && !deliveryOf(stuck)?.turnId, "Fixture is a turnless start.");
    await waitFor("stale start settled", () => deliveryOf(stuck)?.status === "interrupted", 6000);
    assert(deliveryOf(stuck)?.error?.includes("starting"), "Recovery records why.");
    await waitFor("next delivery completed", () => deliveryOf(next)?.status === "completed", 6000);
    return { stuck, next, status: deliveryOf(stuck)?.status };
  });
  await check("reaper cancels stalled preparation, releases reservations and runs the next delivery", async () => {
    const verify = mailbox.verifyDeliveryAttachments;
    let release: () => void = () => {};
    mailbox.verifyDeliveryAttachments = (id) =>
      mailbox.getDelivery(id)?.delivery.text === "blocked preparation"
        ? Effect.promise(
            () =>
              new Promise<void>((done) => {
                release = done;
              }),
          )
        : verify(id);
    try {
      const first = await runCauseEffect(
        service.sendMessage({ agentId: ada.id, text: "blocked preparation", attachmentDraftIds: [] }),
      );
      const id = first.deliveries[0]?.id;
      assert(id, "The blocked delivery exists.");
      await waitFor("preparation started", () => deliveryOf(id)?.status === "starting");
      const second = await runCauseEffect(
        service.sendMessage({ agentId: ada.id, text: "after blocked preparation", attachmentDraftIds: [] }),
      );
      const next = second.deliveries[0]?.id;
      assert(next, "The next delivery exists.");
      await waitFor("blocked preparation interrupted", () => deliveryOf(id)?.status === "interrupted", 6000);
      await waitFor("next after blocked preparation completed", () => deliveryOf(next)?.status === "completed", 6000);
      assert(!deliveryOf(id)?.turnId, "The cancelled preparation starts no provider turn.");
      return { id, next };
    } finally {
      mailbox.verifyDeliveryAttachments = verify;
      release();
    }
  });
  await check("the ceiling measures starting time and keeps the exact boundary and confirmed turns", async () => {
    const id = await enqueue(ada.id, "routine", "old queued fresh start");
    const row = database.connection
      .prepare("SELECT delivery_json FROM projection_deliveries WHERE delivery_id = ?")
      .get(id);
    assert(row && typeof row.delivery_json === "string", "Stored delivery exists.");
    const saved = JSON.parse(row.delivery_json);
    saved.createdAt = "2026-01-01T00:00:00.000Z";
    database.connection
      .prepare("UPDATE projection_deliveries SET delivery_json = ?, created_at = ? WHERE delivery_id = ?")
      .run(JSON.stringify(saved), saved.createdAt, id);
    mailbox.restorePersistedState();
    await runCauseEffect(mailbox.markStarting(id));
    const current = database.connection
      .prepare("SELECT delivery_json FROM projection_deliveries WHERE delivery_id = ?")
      .get(id);
    assert(current && typeof current.delivery_json === "string", "Starting delivery exists.");
    const startedAt = Date.parse(JSON.parse(current.delivery_json).startingAt);
    assert(Number.isFinite(startedAt), "Starting time persists.");
    assert(
      !mailbox.staleStartingDeliveries(startedAt + 600_000, 600_000).some(({ delivery }) => delivery.id === id),
      "Exact ceiling stays starting.",
    );
    assert(
      mailbox.staleStartingDeliveries(startedAt + 600_001, 600_000).some(({ delivery }) => delivery.id === id),
      "Only older starts expire.",
    );
    await runCauseEffect(mailbox.markRunning(id, "confirmed-fixture"));
    assert(
      !mailbox.staleStartingDeliveries(startedAt + 900_000, 600_000).some(({ delivery }) => delivery.id === id),
      "A confirmed turn is excluded.",
    );
    await runCauseEffect(mailbox.markTerminal(id, "completed"));
  });
  await check("boot settles old turnless starts before provider history recovery", async () => {
    const stuck = await enqueue(ben.id, "routine", "boot stale");
    await runCauseEffect(mailbox.markStarting(stuck));
    await runCauseEffect(service.stop());
    const row = database.connection
      .prepare("SELECT delivery_json FROM projection_deliveries WHERE delivery_id = ?")
      .get(stuck);
    assert(row && typeof row.delivery_json === "string", "Stored delivery exists.");
    const saved = JSON.parse(row.delivery_json);
    saved.createdAt = "2026-01-01T00:00:00.000Z";
    delete saved.startingAt;
    database.connection
      .prepare("UPDATE projection_deliveries SET delivery_json = ?, created_at = ? WHERE delivery_id = ?")
      .run(JSON.stringify(saved), saved.createdAt, stuck);
    mailbox.restorePersistedState();
    process.env.OPENBOT_STARTUP_DELIVERY_HOLD_AGENT = ben.id;
    const bootCutoff = new Date().toISOString();
    await runCauseEffect(service.initialize());
    await waitFor("boot stale start settled", () => deliveryOf(stuck)?.status === "interrupted", 6000);
    assert(deliveryOf(stuck)?.error?.includes("starting ceiling"), "Boot uses the starting ceiling path.");
    const queued = await enqueue(ben.id, "routine", "held stale routine");
    await new Promise((done) => setTimeout(done, 150));
    assert(deliveryOf(queued)?.status === "queued", "The startup hold prevents a model turn.");
    const answer = await post({ agentId: ben.id, senderKind: "routine", createdBefore: bootCutoff });
    assert(answer.status === 200, JSON.stringify(answer));
    await waitFor("held lane released", () => deliveryOf(queued)?.status === "completed", 6000);
    delete process.env.OPENBOT_STARTUP_DELIVERY_HOLD_AGENT;
    return { stuck: deliveryOf(stuck), queued: deliveryOf(queued), answer };
  });
  report.passed = report.checks.length === 16 && report.checks.every((item) => item.passed);
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
