import { randomUUID } from "node:crypto";
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
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";

const OUT = resolve(import.meta.dirname, "../.openbot-build/helper-turn-proxy-env-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-helper-proxy-env-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const accountFolder = join(root, "claude-proxy-account");
const processLog = join(root, "claude-processes.jsonl");
const routeFile = join(root, "route-to");
const claudeExecutable = join(root, "claude");
const PRIMARY: AgentModelId = defaultProviderModel("claude");
const SECOND = "claude-sonnet-5-5";
const PROXY_URL = "http://127.0.0.1:9/proxy";
const PROXY_TOKEN = "proxy-token-for-the-check";

for (const key of ["ANTHROPIC_BASE_URL", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY"]) delete process.env[key];
process.env.HOME = home;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(join(home, ".claude"), { recursive: true });
mkdirSync(accountFolder, { recursive: true });
writeFileSync(
  join(accountFolder, "settings.json"),
  `${JSON.stringify(
    {
      env: { ANTHROPIC_BASE_URL: PROXY_URL, ANTHROPIC_AUTH_TOKEN: PROXY_TOKEN },
      hooks: { SessionStart: [{ hooks: [{ type: "command", command: "true" }] }] },
    },
    null,
    2,
  )}\n`,
);

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

const STAND_IN_CLAUDE = `#!/usr/bin/env node
const fs = require("node:fs");
const path = require("node:path");
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
const sources = (option("--setting-sources") ?? "user,project,local").split(",").filter(Boolean);
const folder = process.env.CLAUDE_CONFIG_DIR;
const env = { ...process.env };
let hooksLoaded = false;
if (sources.includes("user")) {
  try {
    const settings = JSON.parse(fs.readFileSync(path.join(folder, "settings.json"), "utf8"));
    Object.assign(env, settings.env ?? {});
    hooksLoaded = Boolean(settings.hooks);
  } catch {}
}
const credentials = fs.existsSync(path.join(folder, ".credentials.json"));
const proxy = env.ANTHROPIC_BASE_URL === ${JSON.stringify(PROXY_URL)} && env.ANTHROPIC_AUTH_TOKEN === ${JSON.stringify(PROXY_TOKEN)};
const helper = sources.length === 0;
const log = (row) => fs.appendFileSync(${JSON.stringify(processLog)}, JSON.stringify({ pid: process.pid, folder, helper, sources, hooksLoaded, credentials, proxy, ...row }) + "\\n");
log({ phase: "process" });
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const result = (fields) =>
  write({ type: "result", subtype: "success", session_id: sessionId, uuid: randomUUID(), duration_ms: 1,
    duration_api_ms: 1, num_turns: 1, total_cost_usd: 0, usage: { input_tokens: 0, output_tokens: 0 },
    permission_denials: [], modelUsage: {}, ...fields });
const assistant = (text) =>
  write({ type: "assistant", message: { id: "msg_" + randomUUID(), type: "message", role: "assistant", model,
    content: [{ type: "text", text }], stop_reason: null, usage: { input_tokens: 1, output_tokens: 1 } },
    parent_tool_use_id: null, session_id: sessionId, uuid: randomUUID() });
const turn = () => {
  if (!credentials && !proxy) {
    log({ phase: "turn", outcome: "not-logged-in" });
    return result({ is_error: true, result: "Not logged in \\u00b7 Please run /login" });
  }
  const text = helper ? JSON.stringify({ agentId: fs.readFileSync(${JSON.stringify(routeFile)}, "utf8").trim() }) : "Done.";
  log({ phase: "turn", outcome: "answered", text });
  assistant(text);
  result({ is_error: false, result: text });
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
    } else if (message.type === "user") turn();
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
    this.#real = new ClaudeAgentClient(
      cli,
      undefined,
      undefined,
      30_000,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      () => ({ CLAUDE_CONFIG_DIR: accountFolder }),
    );
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
  processes: unknown[];
  passed: boolean;
  checkedAt?: string;
  error?: string;
} = { checks: [], processes: [], passed: false };

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

interface ProcessRow {
  phase: string;
  helper: boolean;
  sources: string[];
  hooksLoaded: boolean;
  credentials: boolean;
  proxy: boolean;
  folder: string;
  outcome?: string;
  text?: string;
}

function processRows(): ProcessRow[] {
  try {
    return readFileSync(processLog, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
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
const person = { id: "local", name: "Example member" };

try {
  await runCauseEffect(service.initialize());
  const agents: { id: string; name: string }[] = [];
  for (const name of ["Ada", "Ben"]) {
    const created = await runCauseEffect(
      service.createAgent({
        name,
        description: "Works in a channel.",
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
  writeFileSync(routeFile, ben.id);

  const channelId = randomUUID();
  await runCauseEffect(
    service.channels.command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId,
        draft: {
          name: "Release",
          title: "",
          instructions: "Answer requests about the release.",
          members: [{ agentId: ada.id }, { agentId: ben.id }],
          leadAgentId: ada.id,
        },
      },
      person,
    ),
  );
  const helpersBefore = processRows().filter((row) => row.helper && row.phase === "turn").length;
  await runCauseEffect(
    service.channels.command(
      {
        type: "send",
        operationId: randomUUID(),
        channelId,
        text: "What changed in the release?",
        recipientAgentId: null,
        replyToMessageId: null,
        attachmentDraftIds: [],
      },
      person,
    ),
  );
  const task = await waitFor(
    "the request to get an owner",
    () => service.channels.store.tasks(channelId).find((item) => item.parentTaskId === null && item.ownerAgentId),
    30_000,
  );

  await check("the account folder signs in only through the proxy env of its settings.json", async () => {
    const own = processRows().filter((row) => !row.helper && row.phase === "process");
    assert(own.length > 0, "An agent CLI process ran.");
    assert(
      own.every((row) => row.folder === accountFolder && !row.credentials && row.proxy && row.hooksLoaded),
      `Agent processes use the folder's settings: ${JSON.stringify(own)}`,
    );
    return { agentProcesses: own.length };
  });

  await check("a channel routing turn on that folder completes and routes the request", async () => {
    const helpers = processRows()
      .filter((row) => row.helper && row.phase === "turn")
      .slice(helpersBefore);
    assert(helpers.length >= 1, "A routing turn ran.");
    assert(
      helpers.every((row) => row.outcome === "answered"),
      `Every helper turn is answered: ${JSON.stringify(helpers.map((row) => row.outcome))}`,
    );
    assert(
      task.ownerAgentId === ben.id,
      `The routing answer gives the request to Ben, not the lead: ${task.ownerAgentId}`,
    );
    return { helperTurns: helpers.length, owner: "Ben" };
  });

  await check("the helper turn loads no setting source, so the folder's hooks do not run", async () => {
    const helpers = processRows().filter((row) => row.helper && row.phase === "process");
    assert(helpers.length >= 1, "A helper process ran.");
    assert(
      helpers.every((row) => row.sources.length === 0 && !row.hooksLoaded && row.folder === accountFolder),
      `Helper processes: ${JSON.stringify(helpers)}`,
    );
    return { helperProcesses: helpers.length };
  });

  await check("the member's turn for the routed request completes", async () => {
    const assignment = await waitFor(
      "Ben's assignment",
      () => service.channels.store.assignments(channelId).find((item) => item.taskId === task.id && item.deliveryId),
      30_000,
    );
    const deliveryId = assignment.deliveryId;
    assert(deliveryId, "The assignment has a delivery.");
    await waitFor("Ben's channel turn", () => mailbox.getDelivery(deliveryId)?.delivery.status === "completed", 30_000);
    assert(!processRows().some((row) => row.outcome === "not-logged-in"), "No turn got Not logged in.");
    return { agentId: assignment.agentId };
  });

  report.passed = report.checks.length === 4 && report.checks.every((item) => item.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  await runCauseEffect(service.stop()).catch(() => {});
  report.processes = processRows();
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
    `${report.passed ? "passed" : "failed"} ${report.checks.filter((item) => item.passed).length} of 4 ${join(OUT, "report.json")}\n`,
  );
}
