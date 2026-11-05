// Checks that a usage limit and a provider-error hold survive a server restart. Before, the usage limit
// gate kept them in memory only: after a restart each agent on a backup went back to its limited model
// at its first drain, and its next turn failed there once before it moved to the backup again. Runs the
// real agent service on a temporary profile. Claude is the real `ClaudeAgentClient`, which the Claude
// Agent SDK connects to a stand-in `claude` executable. The stand-in refuses a message that holds
// `usage-limit` on the primary model with a rejected rate limit event that names a reset one hour
// ahead, and fails a message that holds `outage` on the primary model with an Anthropic overload (529).
// Checks:
//   1. A usage limit moves the agent to its backup model.
//   2. After a restart the agent stays on its backup until the reset; its message runs there.
//   3. A limit whose reset passed while the server was down is dropped: the agent goes back.
//   4. A provider-error hold survives a restart, and an ended hold lets the agent go back.
//   5. An unreadable saved file starts the gate with no limits and logs a warning.
// `node --import tsx scripts/usage-limit-restart-e2e.ts`. Writes .openbot-build/usage-limit-restart-e2e/report.json.
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, type AgentModelId, defaultProviderModel } from "@openbot/contracts/ipc";
import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { teeLogLines } from "@openbot/logging";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";

const OUT = resolve(import.meta.dirname, "../.openbot-build/usage-limit-restart-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-usage-limit-restart-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const claudeExecutable = join(root, "claude");
const savedLimits = join(userData, "usage-limits.json");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const BACKUP: AgentModelId = PRIMARY === "claude-sonnet-5-5" ? "claude-opus-5-5" : "claude-sonnet-5-5";
const RESET_AHEAD_S = 3_600;
const PROVIDER_ERROR =
  'API Error: 529 {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"},"request_id":"req_e2e"}';

process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(home, { recursive: true });

const warnings: string[] = [];
teeLogLines(["usage-limit-gate"], (line) => {
  if (line.includes("WARN")) warnings.push(line);
});

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

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
const turn = (message) => {
  record(message);
  const input = JSON.stringify(message);
  const primary = model === ${JSON.stringify(PRIMARY)};
  const limited = primary && input.includes("usage-limit");
  const failed = primary && input.includes("outage");
  fs.appendFileSync(${JSON.stringify(turnLog)}, JSON.stringify({ model, limited, failed, input }) + "\\n");
  if (limited) {
    write({ type: "rate_limit_event", rate_limit_info: { status: "rejected", rateLimitType: "five_hour",
      resetsAt: Math.floor(Date.now() / 1000) + ${RESET_AHEAD_S} }, uuid: randomUUID(), session_id: sessionId });
    result({ is_error: true, result: "You've hit your limit. Try again later." });
    return;
  }
  if (failed) {
    result({ is_error: true, result: ${JSON.stringify(PROVIDER_ERROR)} });
    return;
  }
  assistant("Done.");
  result({ is_error: false, result: "Done." });
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

/** The real Claude client. Only the account, usage and model reads are answered here. */
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
  backup: string;
  checks: { name: string; passed: boolean; detail?: unknown }[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
} = { primary: PRIMARY, backup: BACKUP, checks: [], passed: false };

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

function turns(): { model: string; limited: boolean; failed: boolean; input: string }[] {
  try {
    return readFileSync(turnLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}

interface SavedEntry {
  provider: string;
  accountId: string | null;
  model: string;
  resetsAt?: number | null;
  resetFromUsage?: boolean;
  until?: number;
}

interface SavedLimits {
  limits: SavedEntry[];
  failures: SavedEntry[];
}

function savedEntry(value: unknown): SavedEntry {
  assert.ok(isDynamicRecord(value), "A saved entry is an object.");
  const { provider, accountId, model, resetsAt, resetFromUsage, until } = value;
  assert.ok(typeof provider === "string" && typeof model === "string", "A saved entry names its provider and model.");
  assert.ok(accountId === null || typeof accountId === "string", "A saved entry names its account.");
  return {
    provider,
    accountId,
    model,
    ...(resetsAt === null || typeof resetsAt === "number" ? { resetsAt } : {}),
    ...(typeof resetFromUsage === "boolean" ? { resetFromUsage } : {}),
    ...(typeof until === "number" ? { until } : {}),
  };
}

function saved(): SavedLimits | null {
  if (!existsSync(savedLimits)) return null;
  const value = JSON.parse(readFileSync(savedLimits, "utf8"));
  assert.ok(isDynamicRecord(value) && Array.isArray(value.limits) && Array.isArray(value.failures));
  return { limits: value.limits.map(savedEntry), failures: value.failures.map(savedEntry) };
}

function rewriteSaved(change: (state: SavedLimits) => void): void {
  const state = saved();
  assert.ok(state, "The limits are saved.");
  change(state);
  writeFileSync(savedLimits, `${JSON.stringify({ version: 1, ...state })}\n`);
}

await writeFile(claudeExecutable, STAND_IN_CLAUDE, { mode: 0o755 });
const database = new OpenBotDatabase(userData);

function startService() {
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
  return { store, mailbox, service };
}

let { store, mailbox, service } = startService();

async function restart(beforeStart?: () => void) {
  await runCauseEffect(service.stop());
  beforeStart?.();
  ({ store, mailbox, service } = startService());
  await runCauseEffect(service.initialize());
}

function agentOf(agentId: string) {
  const agent = store.list().find((candidate) => candidate.id === agentId);
  assert.ok(agent, `Agent ${agentId} exists.`);
  return agent;
}

async function send(agentId: string, text: string) {
  const receipt = await runCauseEffect(service.sendMessage({ agentId, text, attachmentDraftIds: [] }));
  const deliveryId = receipt.deliveries[0]?.id;
  assert.ok(deliveryId, "The message has a delivery.");
  const status = await waitFor(`"${text}" to end`, () => {
    const value = mailbox.getDelivery(deliveryId)?.delivery.status;
    return value === "completed" || value === "failed" ? value : null;
  });
  return { deliveryId, status, error: mailbox.getDelivery(deliveryId)?.delivery.error ?? null };
}

async function ranOn(agentId: string, text: string) {
  const before = turns().length;
  const outcome = await send(agentId, text);
  assert.equal(outcome.status, "completed", `"${text}" completes: ${outcome.error}`);
  return turns()
    .slice(before)
    .map((turn) => turn.model);
}

async function check(name: string, run: () => Promise<unknown>) {
  const detail = await run();
  report.checks.push({ name, passed: true, detail });
}

try {
  await runCauseEffect(service.initialize());
  const created = await runCauseEffect(
    service.createAgent({
      name: "Limited",
      description: "Checks usage limits across restarts.",
      avatarSeed: "limited",
      avatarHue: null,
      initialMessage: "Hello.",
      provider: "claude",
      model: PRIMARY,
    }),
  );
  const agentId = created.id;
  await waitFor("the first turn to end", () =>
    service.listQueue(agentId).deliveries.every((delivery) => delivery.status === "completed"),
  );
  await runCauseEffect(
    service.updateAgent({ agentId, backupModels: { automatic: true, order: [{ provider: "claude", model: BACKUP }] } }),
  );

  await check("a usage limit moves the agent to its backup model", async () => {
    const ran = await ranOn(agentId, "usage-limit: first request");
    assert.deepEqual(ran, [PRIMARY, BACKUP], "The refused message runs again on the backup.");
    const agent = agentOf(agentId);
    assert.equal(agent.model, BACKUP, "The agent is on its backup.");
    assert.equal(agent.usageLimitReturn?.model, PRIMARY, "The agent keeps its own model to go back to.");
    return { ran, usageLimitReturn: agent.usageLimitReturn };
  });

  await check("after a restart the agent stays on its backup until the reset", async () => {
    await restart();
    await new Promise((done) => setTimeout(done, 500));
    assert.equal(agentOf(agentId).model, BACKUP, "The restart does not move the agent back.");
    const ran = await ranOn(agentId, "after the restart");
    assert.deepEqual(ran, [BACKUP], "The message runs on the backup, not on the limited model.");
    assert.equal(agentOf(agentId).model, BACKUP, "The agent stays on its backup.");
    assert.equal(agentOf(agentId).usageLimitReturn?.model, PRIMARY, "The agent still goes back later.");
    const state = saved();
    assert.ok(state, "The limit is saved in the data folder.");
    const limit = state.limits.find((entry) => entry.provider === "claude" && entry.model === PRIMARY);
    assert.ok(limit, "The saved limit names the provider and model.");
    assert.equal(limit.accountId, null, "The saved limit is of the provider's own sign-in.");
    assert.ok(
      typeof limit.resetsAt === "number" && limit.resetsAt * 1_000 > Date.now(),
      "The saved limit keeps the reset.",
    );
    assert.equal(limit.resetFromUsage, false, "The reset came from the refusal.");
    return { ran, limit };
  });

  await check("a limit whose reset passed while the server was down is dropped", async () => {
    await restart(() =>
      rewriteSaved((state) => {
        state.limits = state.limits.map((entry) => ({ ...entry, resetsAt: Math.floor(Date.now() / 1_000) - 120 }));
      }),
    );
    const ran = await ranOn(agentId, "after the reset");
    assert.deepEqual(ran, [PRIMARY], "The message runs on the agent's own model.");
    const agent = agentOf(agentId);
    assert.equal(agent.model, PRIMARY, "The agent went back.");
    assert.equal(agent.usageLimitReturn, undefined, "Nothing is left to go back to.");
    assert.deepEqual(saved()?.limits, [], "The passed limit is no longer saved.");
    return { ran };
  });

  await check("a provider-error hold survives a restart and ends with its time", async () => {
    const moved = await ranOn(agentId, "outage: second request");
    assert.deepEqual(moved, [PRIMARY, BACKUP], "The failed message runs again on the backup.");
    assert.equal(agentOf(agentId).usageLimitReturn?.cause, "provider-error");
    const hold = saved()?.failures.find((entry) => entry.provider === "claude" && entry.model === PRIMARY);
    assert.ok(hold && typeof hold.until === "number" && hold.until > Date.now(), "The hold is saved with its end.");
    await restart();
    await new Promise((done) => setTimeout(done, 500));
    const held = await ranOn(agentId, "while the hold lasts");
    assert.deepEqual(held, [BACKUP], "The message runs on the backup while the hold lasts.");
    assert.equal(agentOf(agentId).model, BACKUP);
    await restart(() =>
      rewriteSaved((state) => {
        state.failures = state.failures.map((entry) => ({ ...entry, until: Date.now() - 1_000 }));
      }),
    );
    const back = await ranOn(agentId, "after the hold");
    assert.deepEqual(back, [PRIMARY], "After the hold the message runs on the agent's own model.");
    assert.equal(agentOf(agentId).model, PRIMARY);
    assert.deepEqual(saved()?.failures, [], "The ended hold is no longer saved.");
    return { moved, held, back };
  });

  await check("an unreadable saved file starts with no limits", async () => {
    const moved = await ranOn(agentId, "usage-limit: third request");
    assert.deepEqual(moved, [PRIMARY, BACKUP]);
    const since = warnings.length;
    await restart(() => writeFileSync(savedLimits, "{not json"));
    const warned = warnings.slice(since).filter((line) => line.includes("unreadable"));
    assert.equal(warned.length, 1, "One warning names the unreadable file.");
    const ran = await ranOn(agentId, "after the unreadable file");
    assert.deepEqual(ran, [PRIMARY], "With no saved limit the agent goes back, as before this change.");
    return { warned, ran };
  });

  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  await runCauseEffect(service.stop()).catch(() => {});
  report.checkedAt = new Date().toISOString();
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  for (const entry of report.checks) process.stdout.write(`PASS ${entry.name}\n`);
  process.stdout.write(`${report.passed ? "PASS" : "FAIL"} ${join(OUT, "report.json")}\n`);
  if (report.error) process.stdout.write(`${report.error}\n`);
}
