// Checks that an agent reads its queued teammate messages in one turn (0061). On 11 November at
// 18:45 Helm had a long queue of teammate messages, mostly notes that need no answer, and each one
// waited for a turn of its own; Pocket had 7 queued since 18:05 and the Scouts 2 to 7 each. A
// coordinator that reads one message for each turn cannot keep up.
//
// When the first queued delivery of a lane is a teammate's message, the turn takes the other queued
// teammate messages of that lane too: oldest first, at most 20 messages and about 24 KB of text, the
// rest stay queued. A person's message never joins the batch, and the batch never passes it. A
// routine run stays queued as its own turn. Each message keeps its own delivery, and all of them end
// with the turn.
//
// Runs the real agent service, drain, turn slots and mailbox on a temporary profile with 4 agents. A
// stand-in Claude CLI logs the input of each turn and runs a turn with a token until the check
// releases it. No app, account or network is used.
//
// Run: node --import tsx scripts/inbox-coalesce-e2e.ts
// Writes .openbot-build/inbox-coalesce-e2e/report.json and exits non-zero on a failure.
import { EventEmitter } from "node:events";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, type AgentModelId, defaultProviderModel } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { HostMemory } from "../src/backend/host-memory";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";

const OUT = resolve(import.meta.dirname, "../.openbot-build/inbox-coalesce-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-inbox-coalesce-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const turnLog = join(root, "claude-turns.jsonl");
const releases = join(root, "releases");
const claudeExecutable = join(root, "claude");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const AGENTS = 4;
const TEAMMATE_MESSAGES = 15;
const TURN_LIMIT = 10;
const BACKGROUND_LIMIT = 7;
const SETTLE_MS = 2_000;

process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(home, { recursive: true });
mkdirSync(releases, { recursive: true });

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
  log({ id, phase: "start", token, input, startedAt: Date.now() });
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
  agents: number;
  turnLimit: number;
  backgroundLimit: number;
  checks: { name: string; passed: boolean; detail?: unknown }[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
} = { agents: AGENTS, turnLimit: TURN_LIMIT, backgroundLimit: BACKGROUND_LIMIT, checks: [], passed: false };

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
  input?: string;
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

const memory: HostMemory = {
  hostedPlan: false,
  level: () => "ok",
  turnLimit: () => TURN_LIMIT,
  backgroundTurnLimit: () => BACKGROUND_LIMIT,
  reserveTurn: () => () => {},
  subscribe: () => () => {},
};

await writeFile(claudeExecutable, STAND_IN_CLAUDE, { mode: 0o755 });
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
const service = new AgentService({
  store,
  mailbox,
  browser,
  hostMemory: memory,
  providerRetryDelayMs: 50,
  preferredProvider: "claude",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "claude"),
  clientFactory: (_provider, cli) => new ClaudeStandIn(cli),
});

const settle = () => pause(SETTLE_MS);

try {
  await runCauseEffect(service.initialize());
  const agents: string[] = [];
  for (let index = 0; index < AGENTS; index += 1) {
    const created = await runCauseEffect(
      store.createAgent({
        name: `Agent ${String(index).padStart(2, "0")}`,
        description: "Coordinates the team.",
        avatarSeed: `agent-${index}`,
        avatarHue: null,
        provider: "claude",
        model: PRIMARY,
      }),
    );
    await runCauseEffect(store.updateAgent({ agentId: created.id, provider: "claude", model: PRIMARY }));
    agents.push(created.id);
  }
  const [helm, pocket, scout, thunder] = agents;
  assert(helm && pocket && scout && thunder, "Four agents exist.");
  const teammates = [pocket, scout, thunder];
  let sent = 0;
  const sendFromTeammate = async (index: number, text: string) => {
    const sender = teammates[index % teammates.length] ?? pocket;
    sent += 1;
    const receipt = await runCauseEffect(
      mailbox.enqueue({
        sender: { kind: "agent", agentId: sender },
        recipientAgentIds: [helm],
        text,
        idempotencyKey: `agent:${sender}:turn-${sent}:${sent}`,
        expectsReply: false,
      }),
    );
    const id = receipt.deliveries[0]?.id;
    assert(id, "The teammate message has a delivery.");
    return id;
  };
  const sendFromPerson = (text: string) =>
    runCauseEffect(service.sendMessage({ agentId: helm, text, attachmentDraftIds: [] }));
  const turnsSince = (count: number) =>
    rows()
      .filter((row) => row.phase === "start")
      .slice(count);

  const note = (index: number) => `Note ${index} for Helm: PR ${190 + index} has two QA comments; nothing to do.`;
  let deliveries: string[] = [];
  let before = 0;

  await check(`${TEAMMATE_MESSAGES} teammate messages queue while Helm's turn runs`, async () => {
    await sendFromPerson("Plan the release. [t:p-0]");
    await waitFor("the person's turn", () => started("p-0"));
    for (let index = 0; index < TEAMMATE_MESSAGES; index += 1)
      deliveries.push(await sendFromTeammate(index, note(index)));
    await settle();
    const queued = service.listQueue(helm).deliveries.filter((item) => item.status === "queued").length;
    assert(queued === TEAMMATE_MESSAGES, `${queued} deliveries are queued.`);
    before = rows().filter((row) => row.phase === "start").length;
    return { queued };
  });

  await check(`The ${TEAMMATE_MESSAGES} teammate messages start in one turn that holds each of them`, async () => {
    release("p-0");
    await waitFor(
      "every teammate delivery to end",
      () => deliveries.every((id) => mailbox.getDelivery(id)?.delivery.status === "completed"),
      60_000,
    );
    await settle();
    const turns = turnsSince(before);
    const input = turns.map((row) => row.input ?? "").join("\n");
    const missing = Array.from({ length: TEAMMATE_MESSAGES }, (_, index) => index).filter(
      (index) => !input.includes(note(index)),
    );
    const turnIds = new Set(deliveries.map((id) => mailbox.getDelivery(id)?.delivery.turnId));
    assert(missing.length === 0, `Notes ${missing.join(", ")} are not in a turn input.`);
    assert(turns.length === 1, `${turns.length} turns started for ${TEAMMATE_MESSAGES} teammate messages.`);
    assert(turnIds.size === 1, `The deliveries ended in ${turnIds.size} turns.`);
    const first = input.indexOf(note(0));
    const last = input.indexOf(note(TEAMMATE_MESSAGES - 1));
    assert(first >= 0 && first < last, "The messages are not oldest first.");
    assert(
      input.includes(`Message 1 of ${TEAMMATE_MESSAGES}, from Agent 01 at `),
      "The first message has no label with its sender and time.",
    );
    return { turns: turns.length, deliveriesCompleted: deliveries.length };
  });

  await check("A person's message is never in a batch, and a batch does not pass it", async () => {
    await sendFromPerson("Hold the release. [t:p-1]");
    await waitFor("the person's turn", () => started("p-1"));
    deliveries = [];
    for (let index = 0; index < 3; index += 1) deliveries.push(await sendFromTeammate(index, `${note(index)} (early)`));
    await sendFromPerson("Then ship it. [t:p-2]");
    const late: string[] = [];
    for (let index = 0; index < 3; index += 1) late.push(await sendFromTeammate(index, `${note(index)} (late)`));
    await settle();
    before = rows().filter((row) => row.phase === "start").length;
    release("p-1");
    await waitFor("the second person's turn", () => started("p-2"));
    await settle();
    release("p-2");
    await waitFor(
      "every teammate delivery to end",
      () => [...deliveries, ...late].every((id) => mailbox.getDelivery(id)?.delivery.status === "completed"),
      60_000,
    );
    await settle();
    const turns = turnsSince(before);
    const person = turns.find((row) => row.token === "p-2");
    assert(person, "The second person's turn started.");
    const personInput = person.input ?? "";
    assert(
      !personInput.includes("(early)") && !personInput.includes("(late)"),
      "A teammate message joined the person's turn.",
    );
    const earlyTurns = new Set(deliveries.map((id) => mailbox.getDelivery(id)?.delivery.turnId));
    const lateTurns = new Set(late.map((id) => mailbox.getDelivery(id)?.delivery.turnId));
    assert(earlyTurns.size === 1 && lateTurns.size === 1, `Early: ${earlyTurns.size} turns, late: ${lateTurns.size}.`);
    const earlyAt = turns.findIndex((row) => (row.input ?? "").includes("(early)"));
    const lateAt = turns.findIndex((row) => (row.input ?? "").includes("(late)"));
    const personAt = turns.indexOf(person);
    assert(
      earlyAt >= 0 && earlyAt < personAt && personAt < lateAt,
      `Order: early ${earlyAt}, person ${personAt}, late ${lateAt}.`,
    );
    assert(
      !(turns[earlyAt]?.input ?? "").includes("(late)"),
      "A teammate message queued after the person's message passed it.",
    );
    return { turns: turns.length, order: ["early batch", "person", "late batch"] };
  });

  await check("A batch holds at most 20 messages; the rest stay queued and start next", async () => {
    await sendFromPerson("Review the queue. [t:p-3]");
    await waitFor("the person's turn", () => started("p-3"));
    deliveries = [];
    for (let index = 0; index < 25; index += 1) deliveries.push(await sendFromTeammate(index, `${note(index)} (cap)`));
    await settle();
    before = rows().filter((row) => row.phase === "start").length;
    release("p-3");
    await waitFor(
      "every capped delivery to end",
      () => deliveries.every((id) => mailbox.getDelivery(id)?.delivery.status === "completed"),
      60_000,
    );
    const counts = [
      ...deliveries
        .reduce((map, id) => {
          const turnId = mailbox.getDelivery(id)?.delivery.turnId ?? "";
          return map.set(turnId, (map.get(turnId) ?? 0) + 1);
        }, new Map<string, number>())
        .values(),
    ];
    assert(counts.length === 2 && counts[0] === 20 && counts[1] === 5, `Batches: ${counts.join(", ")}.`);
    return { batches: counts };
  });

  report.passed = report.checks.every((item) => item.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  for (const token of rows().flatMap((row) => (row.token ? [row.token] : []))) release(token);
  await runCauseEffect(service.stop()).catch(() => {});
  report.checkedAt = new Date().toISOString();
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  for (const entry of report.checks)
    process.stdout.write(
      `${entry.passed ? "PASS" : "FAIL"} ${entry.name}${entry.passed ? "" : `: ${String(entry.detail)}`}\n`,
    );
  if (report.error) process.stdout.write(`${report.error}\n`);
  process.stdout.write(
    `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of ${report.checks.length} ${join(OUT, "report.json")}\n`,
  );
}
