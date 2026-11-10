import { EventEmitter } from "node:events";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
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
import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { teeLogLines } from "@openbot/logging";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";
import type { ServerModeOptions } from "../src/main/server-mode";

const OUT = resolve(import.meta.dirname, "../.openbot-build/usage-limit-recovery-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-usage-limit-recovery-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const modeFile = join(root, "mode");
const claudeExecutable = join(root, "claude");
const savedLimits = join(userData, "usage-limits.json");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const RESET_AHEAD_S = 86_400;
const PROBE_MS = 2_000;
const SLOW_MS = PROBE_MS * 3;
const EARLY_MS = 150;

process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(home, { recursive: true });

const gateLines: { at: number; line: string }[] = [];
teeLogLines(["usage-limit-gate"], (line) => gateLines.push({ at: Date.now(), line }));

const gateModule = await import("../src/backend/agent/usage-limit-gate");
const probeConstant = "USAGE_LIMIT_PROBE_MS" in gateModule ? gateModule.USAGE_LIMIT_PROBE_MS : undefined;
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
  if (mode === "refuse") return refuse();
  if (mode === "slow-refuse") return setTimeout(refuse, ${SLOW_MS});
  assistant("Done.");
  result({ is_error: false, result: "Done." });
  log({ id, phase: "end", model, mode, startedAt, endedAt: Date.now(), refused: false, input });
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

interface SavedLimit {
  provider: string;
  accountId: string | null;
  model: string;
  resetsAt: number | null;
}

function savedLimitList(): SavedLimit[] | null {
  if (!existsSync(savedLimits)) return null;
  const value = JSON.parse(readFileSync(savedLimits, "utf8"));
  if (!isDynamicRecord(value) || !Array.isArray(value.limits)) return null;
  return value.limits.flatMap((entry: unknown) =>
    isDynamicRecord(entry) && typeof entry.provider === "string" && typeof entry.model === "string"
      ? [
          {
            provider: entry.provider,
            accountId: typeof entry.accountId === "string" ? entry.accountId : null,
            model: entry.model,
            resetsAt: typeof entry.resetsAt === "number" ? entry.resetsAt : null,
          },
        ]
      : [],
  );
}

function primaryLimit(): SavedLimit | undefined {
  return savedLimitList()?.find((entry) => entry.provider === "claude" && entry.model === PRIMARY);
}

function linesSince(since: number, ...parts: string[]): string[] {
  return gateLines
    .filter(
      (entry) => entry.at >= since && entry.line.includes("INFO") && parts.every((part) => entry.line.includes(part)),
    )
    .map((entry) => entry.line);
}

function setMode(mode: "accept" | "refuse" | "slow-refuse") {
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

function startService(usageLimitProbeMs?: number) {
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
    ...(usageLimitProbeMs === undefined ? {} : { usageLimitProbeMs }),
  });
  return { store, mailbox, service };
}

let { mailbox, service } = startService(PROBE_MS);

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

try {
  await runCauseEffect(service.initialize());
  const names = ["Ada", "Ben", "Cy"];
  const agents: { id: string; name: string }[] = [];
  for (const name of names) {
    const created = await runCauseEffect(
      service.createAgent({
        name,
        description: "Waits behind a usage limit.",
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
  const nameOf = (row: TurnRow) => agents.find((agent) => row.input.includes(agent.name))?.name ?? "unknown";
  const [ada, ben, cy] = agents;
  assert(ada && ben && cy, "Three agents exist.");

  await check("USAGE_LIMIT_PROBE_MS is exported as 15 minutes", async () => {
    assert(probeConstant === 15 * 60_000, `USAGE_LIMIT_PROBE_MS is ${String(probeConstant)}`);
    return probeConstant;
  });

  setMode("refuse");
  const holdStart = Date.now();
  const deliveries = new Map<string, string>();
  deliveries.set(ada.id, await send(ada.id, "Ada: first request at the limit."));
  const held = await waitFor("the limit to be saved", () => primaryLimit());
  const heldAt = Date.now();

  await check("a refused turn holds the model and logs the hold with its cause", async () => {
    assert(
      held.resetsAt !== null && held.resetsAt * 1_000 > Date.now() + 23 * 3_600_000,
      "The hold keeps the named reset.",
    );
    const lines = await waitFor(
      "the hold log line",
      () => {
        const found = linesSince(holdStart, "claude", PRIMARY, ada.id, String(held.resetsAt));
        return found.length > 0 ? found : null;
      },
      3_000,
    );
    return { resetsAt: held.resetsAt, lines };
  });

  deliveries.set(ben.id, await send(ben.id, "Ben: waits behind the limit."));
  deliveries.set(cy.id, await send(cy.id, "Cy: waits behind the limit."));
  await waitFor(
    "the tries of Ben and Cy to be refused",
    () => ends(holdStart).filter((row) => row.refused).length >= 3,
  );
  const waitingSince = Date.now();
  for (const [agentId, deliveryId] of deliveries)
    assert(statusOf(deliveryId) === "queued", `${agentId}'s message waits: ${statusOf(deliveryId)}`);

  let lastRefusalEnd = waitingSince;
  await check("after the probe interval one waiting turn tries, and its refusal holds the model again", async () => {
    const probe = await waitFor("a probe turn", () => starts(waitingSince)[0], PROBE_MS * 4);
    const early = probe.startedAt - heldAt;
    assert(early >= PROBE_MS - EARLY_MS, `The probe started ${early} ms after the hold, before the interval.`);
    const ended = await waitFor("the probe's refusal", () => ends(probe.startedAt).find((row) => row.id === probe.id));
    lastRefusalEnd = ended.endedAt ?? Date.now();
    const again = await waitFor("the new reset to be saved", () => {
      const limit = primaryLimit();
      return limit && limit.resetsAt === ended.resetsAt ? limit : null;
    });
    const logged = await waitFor(
      "the re-hold log line",
      () => {
        const found = linesSince(probe.startedAt, "probe-refused", String(ended.resetsAt));
        return found.length > 0 ? found : null;
      },
      3_000,
    );
    for (const deliveryId of deliveries.values())
      assert(statusOf(deliveryId) === "queued", "Every message still waits after the refused probe.");
    return { probeAgent: nameOf(probe), afterHoldMs: early, newResetsAt: again.resetsAt, logged };
  });

  await check("only one probe per limit runs at a time, and the next waits a full interval after it ends", async () => {
    setMode("slow-refuse");
    const since = lastRefusalEnd;
    const slow = await waitFor(
      "a slow probe turn",
      () => starts(since).find((row) => row.mode === "slow-refuse"),
      PROBE_MS * 4,
    );
    const slowEnd = await waitFor(
      "the slow probe to end",
      () => ends(slow.startedAt).find((row) => row.id === slow.id),
      SLOW_MS + 10_000,
    );
    setMode("accept");
    const during = starts(slow.startedAt).filter(
      (row) => row.id !== slow.id && row.startedAt <= (slowEnd.endedAt ?? 0),
    );
    assert(during.length === 0, `Another turn started during the probe: ${during.map(nameOf).join(", ")}`);
    lastRefusalEnd = slowEnd.endedAt ?? Date.now();
    return { probeAgent: nameOf(slow), ranMs: (slowEnd.endedAt ?? 0) - slow.startedAt, otherTurns: during.length };
  });

  await check(
    "a probe that completes after the interval releases the limit, logs it, and every held agent runs",
    async () => {
      const since = lastRefusalEnd;
      const success = await waitFor(
        "a probe that completes",
        () => ends(since).find((row) => row.refused === false),
        PROBE_MS * 4,
      );
      const gap = success.startedAt - since;
      assert(gap >= PROBE_MS - EARLY_MS, `The probe after a refusal started ${gap} ms later, before the interval.`);
      await waitFor("the limit to end", () => savedLimitList()?.length === 0);
      for (const [agentId, deliveryId] of deliveries)
        await waitFor(`${agentId}'s waiting message to complete`, () => statusOf(deliveryId) === "completed");
      const released = linesSince(since, "A usage limit ended", "probe-completed", PRIMARY);
      assert(released.length === 1, `One release line names the probe: ${released.length}`);
      return { probeAgent: nameOf(success), afterRefusalMs: gap, released };
    },
  );

  await runCauseEffect(service.stop());
  ({ mailbox, service } = startService());
  await runCauseEffect(service.initialize());
  await check("POST /v1/usage/reset on the control socket releases a hold", async () => {
    const server = new ServerMode(serverOptions());
    servers.push(server);
    await runCauseEffect(server.listen());
    setMode("refuse");
    const since = Date.now();
    const deliveryId = await send(ada.id, "Ada: second request at the limit.");
    await waitFor("the second limit to be saved", () => primaryLimit());
    await new Promise((done) => setTimeout(done, PROBE_MS * 2));
    assert(statusOf(deliveryId) === "queued", "With the default interval nothing tries in this time.");
    setMode("accept");
    const invalid = await control("/v1/usage/reset", "POST", "provider=nobody");
    assert(invalid.status === 400, `An unknown provider gets 400: ${invalid.status}`);
    const answer = await control("/v1/usage/reset", "POST", "provider=claude");
    assert(answer.status === 200, `The reset answers 200: ${answer.status} ${JSON.stringify(answer.lines)}`);
    assert(answer.lines.released === "1", `The reset names one released limit: ${JSON.stringify(answer.lines)}`);
    await waitFor("the limit to end", () => savedLimitList()?.length === 0, 3_000);
    await waitFor("Ada's message to complete", () => statusOf(deliveryId) === "completed");
    const logged = linesSince(since, "A usage limit ended", "host-reset", PRIMARY);
    assert(logged.length === 1, `One release line names the host reset: ${logged.length}`);
    const again = await control("/v1/usage/reset", "POST", "provider=claude&accountId=default");
    assert(
      again.status === 200 && again.lines.released === "0",
      `A second reset releases nothing: ${JSON.stringify(again.lines)}`,
    );
    return { answer: answer.lines, logged };
  });

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
