import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { CodexAppServerClient } from "../src/backend/app-server-client";

const OUT = resolve(import.meta.dirname, "../.openbot-build/channel-handoff-validation-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-handoff-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const fakeCodex = join(root, "codex");
const fakeLog = join(root, "codex.jsonl");
process.env.CODEX_HOME = join(home, ".codex");
process.env.OPENBOT_CODEX_PATH = fakeCodex;
mkdirSync(home, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

const CALL_MARK = "CHANNEL-CALL";
const PERSON = { id: "local", name: "Alex" };

const FAKE_CODEX = `#!/usr/bin/env node
const { appendFileSync } = require("node:fs");
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n");
  process.exit(0);
}
const LOG = ${JSON.stringify(fakeLog)};
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
      const currentAssignment = text.split("Current assignment:").at(-1).split("Expected result:")[0];
      if (!currentAssignment.includes(${JSON.stringify(CALL_MARK)})) return setTimeout(() => finish(params.threadId, turnId), 20);
      const calls = text.includes(${JSON.stringify(CALL_MARK)}) ? [...text.matchAll(/<<ARGS([\\s\\S]*?)ARGS>>/g)] : [];
      const match = calls.at(-1);
      if (!match || seenCalls.has(params.threadId + match[1])) return setTimeout(() => finish(params.threadId, turnId), 20);
      seenCalls.add(params.threadId + match[1]);
      const id = "tool-" + turnId;
      waiting.set(id, { threadId: params.threadId, turnId });
      const { __tool, __retry, ...args } = JSON.parse(match[1].replace(/\\\\"/g, '"'));
      waiting.set(id, { threadId: params.threadId, turnId, retry: __retry });
      return write({
        id,
        method: "item/tool/call",
        params: {
          threadId: params.threadId,
          turnId,
          callId: "call-" + turnId,
          namespace: "openbot",
          tool: "tool_call",
          arguments: { name: "openbot." + __tool, arguments: args },
        },
      });
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
        if (turn.retry && message.result?.success === false) {
          const id = "retry-" + turn.turnId;
          waiting.set(id, { threadId: turn.threadId, turnId: turn.turnId });
          write({ id, method: "item/tool/call", params: { threadId: turn.threadId, turnId: turn.turnId, callId: id, namespace: "openbot", tool: "channel_assign", arguments: turn.retry } });
        } else {
          log({ turnId: turn.turnId, result: message.result ?? null, error: message.error ?? null });
          finish(turn.threadId, turn.turnId);
        }
      }
    }
    newline = buffer.indexOf("\\n");
  }
});
`;

interface ChannelToolArgs {
  recipientAgentId?: string;
  task?: string;
  expectedResult?: string;
  sourceMessageIds?: readonly string[];
  __retry?: { recipientAgentId: string; task: string; expectedResult: string; sourceMessageIds: string[] };
  agentId?: string;
  instructions?: string;
  routineId?: string;
  name?: string;
  instruction?: string;
  schedule?: { kind: "daily"; time: string } | { kind: "once"; date: string; time: string };
  active?: boolean;
}

interface ToolLog {
  turnId: string;
  result: { success: boolean; contentItems: { type: string; text: string }[] } | null;
  error: unknown;
}

const report: {
  calls: { name: string; turnId: string; success: boolean | null; text: string }[];
  notes: string[];
  checks: { name: string; passed: boolean }[];
  passed: boolean;
  error?: string;
} = { calls: [], notes: [], checks: [], passed: false };

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

function toolLogs(): ToolLog[] {
  let text = "";
  try {
    text = readFileSync(fakeLog, "utf8");
  } catch {
    return [];
  }
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function pass(name: string, condition: boolean, detail?: string) {
  report.checks.push({ name, passed: condition });
  assert.ok(condition, detail ? `${name}: ${detail}` : name);
}

await writeFile(fakeCodex, FAKE_CODEX, { mode: 0o755 });
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
let failStorageRead = false;
const agents = new AgentService({
  store,
  mailbox,
  browser,
  preferredProvider: "codex",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "codex"),
  timezone: () => "UTC",
  clientFactory: (provider, cli) => {
    assert.equal(provider, "codex");
    const client = new CodexAppServerClient(cli.executable);
    client.on("request", (request) => {
      if (!failStorageRead || request.method !== "item/tool/call") return;
      failStorageRead = false;
      const channelStore = agents.channels.store;
      const original = channelStore.message;
      channelStore.message = () => {
        channelStore.message = original;
        throw new Error("Fixture storage read failure.");
      };
    });
    return client;
  },
});

const channels = () => agents.channels;

function settled(channelId: string) {
  return waitFor("the channel tasks to end", () =>
    channels()
      .store.tasks(channelId)
      .every((task) => !["queued", "running", "waiting"].includes(task.state)),
  );
}

async function callTool(
  name: string,
  channelId: string,
  recipientAgentId: string,
  tool: string,
  args: ChannelToolArgs,
  person = PERSON,
) {
  const before = toolLogs().length;
  await runCauseEffect(
    channels().command(
      {
        type: "send",
        operationId: randomUUID(),
        channelId,
        text: `${CALL_MARK} <<ARGS${JSON.stringify({ __tool: tool, ...args })}ARGS>>`,
        recipientAgentId,
        replyToMessageId: null,
        attachmentDraftIds: [],
      },
      person,
    ),
  );
  const entry = await waitFor(`${name} tool result`, () => toolLogs()[before]);
  await settled(channelId);
  const text = entry.result?.contentItems.map((item) => item.text).join("\n") ?? JSON.stringify(entry.error);
  report.calls.push({ name, turnId: entry.turnId, success: entry.result?.success ?? null, text });
  return { success: entry.result?.success === true, text };
}

try {
  await runCauseEffect(agents.initialize());
  const create = async (name: string) => {
    const agent = await runCauseEffect(
      agents.createAgent({
        name,
        description: `${name} works in the team channel.`,
        avatarSeed: name.toLowerCase(),
        avatarHue: null,
        initialMessage: "Hello.",
      }),
    );
    await waitFor(`the first chat turn of ${name} to end`, () =>
      agents
        .listQueue(agent.id)
        .deliveries.every((delivery) => !["queued", "starting", "running"].includes(delivery.status)),
    );
    return agent;
  };
  const lead = await create("Lead");
  const tester = await create("Tester");
  const channelId = randomUUID();
  await runCauseEffect(
    channels().command(
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

  const errorEvents: { code: string; message: string }[] = [];
  agents.on("event", (event) => {
    if (event.type === "error") errorEvents.push(event);
  });
  const beforeTasks = channels().store.tasks(channelId).length;
  const invalid = await callTool("incomplete handoff", channelId, lead.id, "channel_assign", {
    task: "Review the synthetic evidence.",
  });
  pass("Invalid handoffs return a normal failed tool result", report.calls.at(-1)?.success === false, invalid.text);
  pass(
    "The model receives the handoff requirements",
    invalid.text.includes("recipient") && invalid.text.includes("source"),
    invalid.text,
  );
  pass("Invalid arguments do not raise a global provider error", errorEvents.length === 0, JSON.stringify(errorEvents));
  pass(
    "An incomplete handoff does not create a child task",
    channels().store.tasks(channelId).length === beforeTasks + 1,
  );
  const sourceId = channels()
    .store.messages(channelId)
    .find((message) => message.author.kind === "member")?.id;
  assert.ok(sourceId);
  const correction = {
    recipientAgentId: tester.id,
    task: "Review this synthetic evidence.",
    expectedResult: "Return one concise finding.",
    sourceMessageIds: [sourceId],
  };
  for (const [name, tool, args] of [
    ["empty source list", "channel_assign", { ...correction, sourceMessageIds: [] }],
    ["missing transfer task", "channel_transfer", { recipientAgentId: tester.id }],
    ["unavailable source", "channel_assign", { ...correction, sourceMessageIds: [randomUUID()] }],
    ["handoff to self", "channel_assign", { ...correction, recipientAgentId: lead.id }],
  ] as const) {
    const refused = await callTool(name, channelId, lead.id, tool, args);
    pass(`${name} returns corrective tool feedback`, report.calls.at(-1)?.success === false, refused.text);
    pass(`${name} does not raise a global provider error`, errorEvents.length === 0, JSON.stringify(errorEvents));
  }
  const count = report.calls.length;
  const retry = await callTool("corrected handoff", channelId, lead.id, "channel_assign", {
    task: "Review this synthetic evidence.",
    __retry: correction,
  });
  pass("The same provider turn can correct its failed handoff", retry.success, retry.text);
  pass(
    "The corrected handoff creates exactly one child",
    channels()
      .store.tasks(channelId)
      .filter((task) => task.parentTaskId !== null).length === 1,
  );
  pass(
    "The correction keeps the channel free of provider errors",
    errorEvents.length === 0,
    JSON.stringify(errorEvents),
  );
  pass("Every call reaches a terminal result", report.calls.length === count + 1);
  failStorageRead = true;
  const fatal = await callTool("unexpected storage failure", channelId, lead.id, "channel_assign", correction);
  pass(
    "Unexpected storage failures still reach the global error path",
    !fatal.success && errorEvents.some((event) => event.code === "server_request_failed"),
  );
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  await runCauseEffect(agents.stop()).catch(() => {});
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  process.stdout.write(`${report.passed ? "PASS" : "FAIL"} ${join(OUT, "report.json")}\n`);
  if (report.error) process.stdout.write(`${report.error}\n`);
}
