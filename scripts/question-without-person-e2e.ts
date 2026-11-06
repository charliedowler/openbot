import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { AGENT_PROVIDERS, type AgentPromptQuestion, defaultProviderModel } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";

const OUT = resolve(import.meta.dirname, "../.openbot-build/question-without-person-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-question-without-person-"));
const home = join(root, "home");
const claudeExecutable = join(root, "claude");
const claudeLog = join(root, "claude.jsonl");
const fakeCodex = join(root, "codex");
const codexLog = join(root, "codex.jsonl");
const CLAUDE_MODEL = defaultProviderModel("claude");
const ASK_MARK = "ASK-QUESTION";
const CALL_MARK = "QUESTION-CALL";
const PERSON = { id: "local", name: "Alex" };
const REFUSAL = "No person can answer a question in this turn";

process.env.HOME = home;
process.env.CODEX_HOME = join(home, ".codex");
process.env.OPENBOT_CODEX_PATH = fakeCodex;
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
mkdirSync(join(home, ".claude"), { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

const STAND_IN_CLAUDE = `#!${process.execPath}
const { appendFileSync } = require("node:fs");
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
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const frame = (message) => ({ ...message, parent_tool_use_id: null, session_id: sessionId });
const begin = (message) => {
  write(frame({ type: "stream_event", uuid: randomUUID(), user_message_uuid: message.uuid, user_message_uuids: [message.uuid],
    event: { type: "message_start", message: { id: "msg_" + message.uuid, type: "message", role: "assistant", content: [] } } }));
};
const finish = (message, text) => {
  const reply = { id: "msg_" + message.uuid, type: "message", role: "assistant", model: ${JSON.stringify(CLAUDE_MODEL)},
    content: [{ type: "text", text }], stop_reason: "end_turn", usage: { input_tokens: 1, output_tokens: 1 } };
  write(frame({ type: "assistant", uuid: randomUUID(), message: reply, user_message_uuid: message.uuid }));
  write(frame({ type: "result", subtype: "success", is_error: false, result: text, uuid: randomUUID(),
    duration_ms: 1, duration_api_ms: 1, num_turns: 1, total_cost_usd: 0, usage: { input_tokens: 1, output_tokens: 1 },
    permission_denials: [] }));
};
const asking = new Map();
const turn = (message) => {
  begin(message);
  if (!JSON.stringify(message.message).includes(${JSON.stringify(ASK_MARK)})) return finish(message, "Done.");
  const requestId = "ask-" + randomUUID();
  asking.set(requestId, message);
  write({ type: "control_request", request_id: requestId, request: {
    subtype: "can_use_tool",
    tool_name: "AskUserQuestion",
    tool_use_id: "toolu_" + randomUUID().replaceAll("-", ""),
    input: { questions: [{ header: "Color", question: "Which color?", multiSelect: false,
      options: [{ label: "Red", description: "Red" }, { label: "Blue", description: "Blue" }] }] },
  } });
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
    if (message.type === "control_request")
      write({ type: "control_response", response: { subtype: "success", request_id: message.request_id, response: {} } });
    else if (message.type === "control_response" && asking.has(message.response?.request_id)) {
      const asked = asking.get(message.response.request_id);
      asking.delete(message.response.request_id);
      appendFileSync(${JSON.stringify(claudeLog)}, JSON.stringify({ sessionId, response: message.response }) + "\\n");
      finish(asked, "Done.");
    } else if (message.type === "user") turn(message);
  }
});
process.stdin.on("end", () => process.exit(0));
`;

const FAKE_CODEX = `#!/usr/bin/env node
const { appendFileSync } = require("node:fs");
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n");
  process.exit(0);
}
const LOG = ${JSON.stringify(codexLog)};
const config = { config: { mcp_servers: {} }, layers: [{ name: { type: "user", file: "/fake/.codex/config.toml", profile: null }, version: "1", config: { mcp_servers: {} } }] };
let buffer = "";
let threads = 0;
let turns = 0;
const waiting = new Map();
const seenCalls = new Set();
process.stdout.on("error", (error) => {
  if (error.code === "EPIPE") process.exit(0);
  throw error;
});
const log = (entry) => appendFileSync(LOG, JSON.stringify(entry) + "\\n");
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const finish = (threadId, turnId) => {
  const item = { type: "agentMessage", id: "message-" + turnId, text: "Done.", phase: "final_answer" };
  write({ method: "item/completed", params: { threadId, turnId, item } });
  write({ method: "turn/completed", params: { threadId, turn: { id: turnId, status: "completed" } } });
};
const QUESTIONS = [{ id: "color", header: "Color", question: "Which color?", options: [{ label: "Red", description: "Red" }, { label: "Blue", description: "Blue" }] }];
const callFor = (scope, threadId) => {
  const calls = scope.includes(${JSON.stringify(CALL_MARK)}) ? [...scope.matchAll(/<<ARGS([\\s\\S]*?)ARGS>>/g)] : [];
  const match = calls.at(-1);
  const marks = [
    ["args", match ? match.index + match[0].length : -1],
    ["ask_user", scope.lastIndexOf("ASK-USER")],
    ["ask_ui", scope.lastIndexOf("ASK-UI")],
    ["requestUserInput", scope.lastIndexOf("ASK-INPUT")],
  ].filter(([, at]) => at >= 0).sort((a, b) => b[1] - a[1]);
  const latest = marks[0]?.[0];
  if (latest === "args") {
    if (seenCalls.has(threadId + match[1])) return null;
    seenCalls.add(threadId + match[1]);
    const { __tool, ...args } = JSON.parse(match[1].replace(/\\\\"/g, '"'));
    return { kind: __tool, method: "item/tool/call", tool: "tool_call", arguments: { name: "openbot." + __tool, arguments: args } };
  }
  if (latest === "ask_user") return { kind: "ask_user", method: "item/tool/call", tool: "ask_user", arguments: { questions: QUESTIONS } };
  if (latest === "ask_ui") return { kind: "ask_ui", method: "item/tool/call", tool: "ask_ui", arguments: { block: { type: "quick_replies", title: "Which color?", options: [{ id: "red", label: "Red" }, { id: "blue", label: "Blue" }] } } };
  if (latest === "requestUserInput") return { kind: "requestUserInput", method: "item/tool/requestUserInput" };
  return null;
};
const answer = (message) => {
  const params = message.params || {};
  switch (message.method) {
    case "initialize": return write({ id: message.id, result: {} });
    case "account/read": return write({ id: message.id, result: { account: { type: "chatgpt", email: "codex@example.com" } } });
    case "account/rateLimits/read": return write({ id: message.id, result: { rateLimits: { limitId: "codex", primary: null, secondary: null }, rateLimitsByLimitId: null } });
    case "model/list": return write({ id: message.id, result: { data: [{ model: "gpt-6-luna", displayName: "GPT-6 Luna" }] } });
    case "plugin/list": return write({ id: message.id, result: { marketplaces: [] } });
    case "config/read": return write({ id: message.id, result: config });
    case "config/batchWrite": return write({ id: message.id, result: {} });
    case "thread/start": return write({ id: message.id, result: { thread: { id: "thread-" + (++threads), turns: [] } } });
    case "thread/resume": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "thread/unsubscribe": return write({ id: message.id, result: { status: "Unsubscribed" } });
    case "thread/turns/list":
    case "thread/items/list": return write({ id: message.id, result: { data: [], nextCursor: null } });
    case "thread/read": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "turn/start": {
      const turnId = "turn-" + (++turns);
      const text = (params.input || []).map((item) => item.text || "").join("\\n");
      write({ id: message.id, result: { turn: { id: turnId, status: "inProgress", items: [] } } });
      write({ method: "turn/started", params: { threadId: params.threadId, turn: { id: turnId } } });
      const scope = text.includes("Current assignment:") ? text.split("Current assignment:").at(-1).split("Expected result:")[0] : text;
      const call = callFor(scope, params.threadId);
      if (!call) return setTimeout(() => finish(params.threadId, turnId), 20);
      const id = "tool-" + turnId;
      waiting.set(id, { threadId: params.threadId, turnId, kind: call.kind });
      if (call.method === "item/tool/requestUserInput")
        return write({ id, method: call.method, params: { threadId: params.threadId, turnId, itemId: "item-" + turnId, questions: QUESTIONS } });
      return write({ id, method: call.method, params: { threadId: params.threadId, turnId, callId: "call-" + turnId, namespace: "openbot", tool: call.tool, arguments: call.arguments } });
    }
    case "turn/interrupt": {
      write({ id: message.id, result: {} });
      return write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: params.turnId, status: "interrupted" } } });
    }
    default:
      return write({ id: message.id, error: { code: -32601, message: "Method not found: " + message.method } });
  }
};
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newline = buffer.indexOf("\\n");
  while (newline >= 0) {
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (line) {
      const message = JSON.parse(line);
      if (message.method && message.id !== undefined) answer(message);
      else if (!message.method && waiting.has(message.id)) {
        const turn = waiting.get(message.id);
        waiting.delete(message.id);
        log({ turnId: turn.turnId, kind: turn.kind, result: message.result ?? null, error: message.error ?? null });
        finish(turn.threadId, turn.turnId);
      }
    }
    newline = buffer.indexOf("\\n");
  }
});
`;

interface ClientEvents {
  notification: [notification: AppServerNotification];
  request: [request: AppServerRequest];
  exit: [error?: Error];
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
        return providerSync(() => decoder({ data: [{ model: CLAUDE_MODEL, displayName: "Claude" }] }));
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

interface SeenPrompt {
  requestId: string | number;
  agentId: string;
  questions: AgentPromptQuestion[];
}

const report: {
  steps: { name: string; outcome: string; detail: string }[];
  checks: { name: string; passed: boolean; detail?: string }[];
  providerLogs?: { claude: unknown[]; codex: unknown[] };
  passed: boolean;
  error?: string;
} = { steps: [], checks: [], passed: false };

function check(name: string, condition: boolean, detail?: string) {
  report.checks.push({ name, passed: condition, ...(detail === undefined ? {} : { detail }) });
  process.stdout.write(
    `${condition ? "ok  " : "FAIL"} ${name}${condition || detail === undefined ? "" : `: ${detail}`}\n`,
  );
}

const unused = () => Effect.die(new Error("Not used by this check."));
const browser: AgentBrowserHost = {
  listTabs: () => [],
  beginTakeover: unused,
  endTakeover: () => {},
  resolveUploadTarget: unused,
  handleDynamicTool: unused,
  userControlTabFor: () => null,
  awaitUserControlRelease: unused,
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

function jsonLines<T>(path: string): T[] {
  let text = "";
  try {
    text = readFileSync(path, "utf8");
  } catch {
    return [];
  }
  return text
    .split("\n")
    .filter(Boolean)
    .map((line): T => JSON.parse(line));
}

function startService(name: string, provider: "claude" | "codex") {
  const userData = join(root, `user-data-${name}`);
  const database = new OpenBotDatabase(userData);
  const store = new AgentStore(userData, home, database);
  const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
  const agents = new AgentService({
    store,
    mailbox,
    browser,
    preferredProvider: provider,
    offProviders: AGENT_PROVIDERS.filter((candidate) => candidate !== provider),
    timezone: () => "UTC",
    ...(provider === "claude" ? { clientFactory: (_provider, cli) => new ClaudeStandIn(cli) } : {}),
  });
  const prompts: SeenPrompt[] = [];
  agents.on("event", (event) => {
    if (event.type === "prompt")
      prompts.push({ requestId: event.requestId, agentId: event.agentId, questions: event.questions });
  });
  let connection: DatabaseSync | null = null;
  const questionCards = () => {
    connection ??= new DatabaseSync(database.path);
    const row = connection
      .prepare(
        "SELECT COUNT(*) AS count FROM projection_thread_messages WHERE json_extract(message_json, '$.itemType') = 'question_prompt'",
      )
      .get();
    return Number(row?.count ?? 0);
  };
  const idle = (agentId: string) =>
    waitFor("the agent's turns to end", () =>
      agents
        .listQueue(agentId)
        .deliveries.every((delivery) => !["queued", "starting", "running"].includes(delivery.status)),
    );
  const createAgent = async (agentName: string) => {
    const agent = await runCauseEffect(
      agents.createAgent({
        name: agentName,
        description: `${agentName} checks questions.`,
        avatarSeed: agentName.toLowerCase(),
        avatarHue: null,
        initialMessage: "Hello.",
        ...(provider === "claude" ? { provider: "claude" as const, model: CLAUDE_MODEL } : {}),
      }),
    );
    await idle(agent.id);
    return agent;
  };
  const answerPrompt = (prompt: SeenPrompt) =>
    runCauseEffect(
      agents.respondToPrompt({
        requestId: prompt.requestId,
        answers: Object.fromEntries(
          prompt.questions.map((question) => [question.id, [question.options?.at(-1)?.label ?? "Blue"]]),
        ),
      }),
    );
  const stop = async () => {
    await runCauseEffect(agents.stop()).catch(() => {});
    connection?.close();
    database.close();
  };
  return { agents, mailbox, prompts, questionCards, idle, createAgent, answerPrompt, stop };
}

type Service = ReturnType<typeof startService>;

async function outcome<T>(
  service: Service,
  agentId: string,
  promptsBefore: number,
  result: () => T | undefined,
  whilePrompted?: () => void,
) {
  const first = await waitFor<{ answered: T } | { prompt: SeenPrompt }>("the question to be answered or shown", () => {
    const answered = result();
    if (answered) return { answered };
    const prompt = service.prompts.slice(promptsBefore).find((candidate) => candidate.agentId === agentId);
    return prompt ? { prompt } : null;
  });
  if ("answered" in first) return { answered: first.answered, prompt: null };
  whilePrompted?.();
  await service.answerPrompt(first.prompt);
  return { answered: await waitFor("the answer to reach the provider", result), prompt: first.prompt };
}

interface ClaudeLog {
  sessionId: string;
  response: { response?: { behavior?: string; message?: string; updatedInput?: { answers?: Record<string, string> } } };
}

interface CodexLog {
  turnId: string;
  kind: string;
  result: {
    success?: boolean;
    contentItems?: { type: string; text: string }[];
    answers?: Record<string, { answers?: string[] }>;
    refusal?: string;
  } | null;
  error: unknown;
}

const codexText = (entry: CodexLog) =>
  entry.result?.contentItems?.map((item) => item.text).join("\n") ?? JSON.stringify(entry.result ?? entry.error);

let claude: Service | null = null;
let codex: Service | null = null;
try {
  await writeFile(claudeExecutable, STAND_IN_CLAUDE, { mode: 0o755 });
  await writeFile(fakeCodex, FAKE_CODEX, { mode: 0o755 });

  claude = startService("claude", "claude");
  await runCauseEffect(claude.agents.initialize());
  const claudeRoutineAgent = await claude.createAgent("Jeffrey");
  const claudePersonAgent = await claude.createAgent("Ada");

  {
    const service = claude;
    const before = jsonLines<ClaudeLog>(claudeLog).length;
    const promptsBefore = service.prompts.length;
    const cardsBefore = service.questionCards();
    const routine = service.agents.createRoutine({
      agentId: claudeRoutineAgent.id,
      name: "Executor swarm inbox",
      instruction: `${ASK_MARK} Relay the command.`,
      active: false,
      timezone: "UTC",
      schedule: { kind: "daily", time: "09:00" },
    });
    await runCauseEffect(service.agents.testRoutine({ agentId: claudeRoutineAgent.id, routineId: routine.id }));
    let statusWhileAsked: string | null = null;
    const seen = await outcome(
      service,
      claudeRoutineAgent.id,
      promptsBefore,
      () => jsonLines<ClaudeLog>(claudeLog)[before],
      () => {
        statusWhileAsked =
          service.agents.listRoutineRuns({ agentId: claudeRoutineAgent.id, routineId: routine.id })[0]?.status ?? null;
      },
    );
    await service.idle(claudeRoutineAgent.id);
    const run = service.agents.listRoutineRuns({ agentId: claudeRoutineAgent.id, routineId: routine.id })[0];
    const response = seen.answered.response.response;
    report.steps.push({
      name: "Claude AskUserQuestion in a routine run",
      outcome: seen.prompt ? "question card shown" : (response?.behavior ?? "none"),
      detail: JSON.stringify({ response, runStatus: run?.status, statusWhileAsked }),
    });
    check(
      "A routine run's AskUserQuestion shows no question card",
      seen.prompt === null && service.questionCards() === cardsBefore,
      `card shown; routine run status while it waited: ${statusWhileAsked}`,
    );
    check(
      "A routine run's AskUserQuestion is denied with the reason",
      response?.behavior === "deny" && (response.message ?? "").includes(REFUSAL),
      JSON.stringify(response),
    );
    check(
      "The routine run goes on and ends without waiting for attention",
      seen.prompt === null && run?.status === "succeeded",
      `run status while asked ${statusWhileAsked}, at the end ${run?.status}`,
    );
  }

  {
    const service = claude;
    const before = jsonLines<ClaudeLog>(claudeLog).length;
    const promptsBefore = service.prompts.length;
    await runCauseEffect(
      service.agents.sendMessage({ agentId: claudePersonAgent.id, text: `${ASK_MARK} Pick one.` }, PERSON),
    );
    const seen = await outcome(
      service,
      claudePersonAgent.id,
      promptsBefore,
      () => jsonLines<ClaudeLog>(claudeLog)[before],
    );
    await service.idle(claudePersonAgent.id);
    const response = seen.answered.response.response;
    report.steps.push({
      name: "Claude AskUserQuestion in the person's chat",
      outcome: seen.prompt ? "question card shown and answered" : (response?.behavior ?? "none"),
      detail: JSON.stringify(response),
    });
    check("A person's message still gets the AskUserQuestion card", seen.prompt !== null);
    check(
      "The person's answer reaches Claude",
      response?.behavior === "allow" && response.updatedInput?.answers?.["Which color?"] === "Blue",
      JSON.stringify(response),
    );
  }
  await claude.stop();
  claude = null;

  codex = startService("codex", "codex");
  const codexService = codex;
  await runCauseEffect(codexService.agents.initialize());
  const codexRoutineAgent = await codexService.createAgent("Router");
  const codexPersonAgent = await codexService.createAgent("Builder");
  const codexSender = await codexService.createAgent("Sender");
  const codexRecipient = await codexService.createAgent("Recipient");
  const lead = await codexService.createAgent("Lead");
  const tester = await codexService.createAgent("Tester");

  const codexStep = async (
    name: string,
    agentId: string,
    start: (service: Service) => Promise<unknown>,
    kind: string,
  ) => {
    const service = codexService;
    const before = jsonLines<CodexLog>(codexLog).length;
    const promptsBefore = service.prompts.length;
    const cardsBefore = service.questionCards();
    await start(service);
    const seen = await outcome(service, agentId, promptsBefore, () =>
      jsonLines<CodexLog>(codexLog)
        .slice(before)
        .find((entry) => entry.kind === kind),
    );
    await service.idle(agentId);
    report.steps.push({
      name,
      outcome: seen.prompt ? "question card shown" : "answered at once",
      detail: codexText(seen.answered),
    });
    return { ...seen, newCards: service.questionCards() - cardsBefore, text: codexText(seen.answered) };
  };

  const routineStep = (kind: string, mark: string) =>
    codexStep(
      `Codex ${kind} in a routine run`,
      codexRoutineAgent.id,
      async (service) => {
        const routine = service.agents.createRoutine({
          agentId: codexRoutineAgent.id,
          name: `Routine ${kind}`,
          instruction: `${mark} Relay the command.`,
          active: false,
          timezone: "UTC",
          schedule: { kind: "daily", time: "09:00" },
        });
        await runCauseEffect(service.agents.testRoutine({ agentId: codexRoutineAgent.id, routineId: routine.id }));
        return routine;
      },
      kind,
    );

  const routinePrompts: boolean[] = [];
  for (const [kind, mark] of [
    ["ask_user", "ASK-USER"],
    ["ask_ui", "ASK-UI"],
  ] as const) {
    const step = await routineStep(kind, mark);
    routinePrompts.push(step.prompt !== null);
    check(
      `A routine run's ${kind} is refused at once with the reason`,
      step.prompt === null &&
        step.newCards === 0 &&
        step.answered.result?.success === false &&
        step.text.includes(REFUSAL),
      step.prompt ? "question card shown" : step.text,
    );
  }
  {
    const step = await routineStep("requestUserInput", "ASK-INPUT");
    routinePrompts.push(step.prompt !== null);
    check(
      "A routine run's user input request gets no answers and the reason at once",
      step.prompt === null &&
        step.newCards === 0 &&
        Object.keys(step.answered.result?.answers ?? {}).length === 0 &&
        (step.answered.result?.refusal ?? "").includes(REFUSAL),
      step.prompt ? "question card shown" : step.text,
    );
  }
  const codexRuns = codexService.agents
    .listRoutines(codexRoutineAgent.id)
    .flatMap(
      (routine) => codexService.agents.listRoutineRuns({ agentId: codexRoutineAgent.id, routineId: routine.id }) ?? [],
    );
  check(
    "The Codex routine runs end without waiting for attention",
    codexRuns.length === 3 && codexRuns.every((run) => run.status === "succeeded") && !routinePrompts.includes(true),
    JSON.stringify({ statuses: codexRuns.map((run) => run.status), questionCards: routinePrompts }),
  );

  {
    const step = await codexStep(
      "Codex ask_user in another agent's message",
      codexRecipient.id,
      async (service) =>
        runCauseEffect(
          service.agents.sendMessage(
            {
              agentId: codexSender.id,
              text: `${CALL_MARK} <<ARGS${JSON.stringify({
                __tool: "send_message",
                recipientAgentIds: [codexRecipient.id],
                text: "ASK-USER about the release.",
                expectsReply: false,
              })}ARGS>>`,
            },
            PERSON,
          ),
        ),
      "ask_user",
    );
    check(
      "Another agent's message cannot make ask_user wait",
      step.prompt === null &&
        step.newCards === 0 &&
        step.answered.result?.success === false &&
        step.text.includes(REFUSAL),
      step.prompt ? "question card shown" : step.text,
    );
  }

  for (const [kind, mark] of [
    ["ask_user", "ASK-USER"],
    ["ask_ui", "ASK-UI"],
    ["requestUserInput", "ASK-INPUT"],
  ] as const) {
    const step = await codexStep(
      `Codex ${kind} in the person's chat`,
      codexPersonAgent.id,
      async (service) =>
        runCauseEffect(service.agents.sendMessage({ agentId: codexPersonAgent.id, text: `${mark} Pick one.` }, PERSON)),
      kind,
    );
    const answered =
      kind === "requestUserInput"
        ? step.answered.result?.answers?.color?.answers?.[0] === "Blue"
        : step.answered.result?.success === true && /blue/i.test(step.text);
    check(`A person's message still gets the ${kind} card and its answer`, step.prompt !== null && answered, step.text);
  }

  const channelId = randomUUID();
  await runCauseEffect(
    codexService.agents.channels.command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId,
        draft: {
          name: "team",
          title: "",
          instructions: "Ship the release.",
          members: [{ agentId: lead.id }, { agentId: tester.id }],
          leadAgentId: lead.id,
        },
      },
      PERSON,
    ),
  );
  const settled = () =>
    waitFor("the channel tasks to end", () =>
      codexService.agents.channels.store
        .tasks(channelId)
        .every((task) => !["queued", "running", "waiting"].includes(task.state)),
    );
  {
    const step = await codexStep(
      "Codex ask_ui in a person's channel request",
      lead.id,
      async (service) =>
        runCauseEffect(
          service.agents.channels.command(
            {
              type: "send",
              operationId: randomUUID(),
              channelId,
              text: "ASK-UI Which color for the release?",
              recipientAgentId: lead.id,
              replyToMessageId: null,
              attachmentDraftIds: [],
            },
            PERSON,
          ),
        ),
      "ask_ui",
    );
    await settled();
    check(
      "A person's channel request still gets the ask_ui card and its answer",
      step.prompt !== null && step.answered.result?.success === true && /blue/i.test(step.text),
      step.text,
    );
  }
  const sendToChannel = (text: string, recipientAgentId: string) =>
    runCauseEffect(
      codexService.agents.channels.command(
        {
          type: "send",
          operationId: randomUUID(),
          channelId,
          text,
          recipientAgentId,
          replyToMessageId: null,
          attachmentDraftIds: [],
        },
        PERSON,
      ),
    );
  await sendToChannel("The release build is ready.", lead.id);
  await settled();
  const sourceId = codexService.agents.channels.store
    .messages(channelId)
    .find((message) => message.author.kind === "member" && message.message.text === "The release build is ready.")?.id;
  assert.ok(sourceId, "The person's channel message is stored.");
  {
    const step = await codexStep(
      "Codex ask_user in a channel task that the lead assigned",
      tester.id,
      async (service) =>
        runCauseEffect(
          service.agents.channels.command(
            {
              type: "send",
              operationId: randomUUID(),
              channelId,
              text: `${CALL_MARK} <<ARGS${JSON.stringify({
                __tool: "channel_assign",
                recipientAgentId: tester.id,
                task: "ASK-USER which build to test.",
                expectedResult: "The tested build.",
                sourceMessageIds: [sourceId],
              })}ARGS>>`,
              recipientAgentId: lead.id,
              replyToMessageId: null,
              attachmentDraftIds: [],
            },
            PERSON,
          ),
        ),
      "ask_user",
    );
    await settled();
    check(
      "An agent's channel assignment cannot make ask_user wait",
      step.prompt === null &&
        step.newCards === 0 &&
        step.answered.result?.success === false &&
        step.text.includes(REFUSAL),
      step.prompt ? "question card shown" : step.text,
    );
  }

  report.passed = report.checks.length > 0 && report.checks.every((item) => item.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  if (claude) await claude.stop();
  if (codex) await codex.stop();
  report.providerLogs = { claude: jsonLines(claudeLog), codex: jsonLines(codexLog) };
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  const passed = report.checks.filter((item) => item.passed).length;
  process.stdout.write(
    `${report.passed ? "PASS" : "FAIL"} ${passed} of ${report.checks.length} checks ${join(OUT, "report.json")}\n`,
  );
  if (report.error) process.stdout.write(`${report.error}\n`);
}
