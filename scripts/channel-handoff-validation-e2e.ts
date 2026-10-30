import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { INPUT_LIMITS } from "@openbot/contracts/input-limits";
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
const readyPath = join(root, "ready.json");
const releasePath = join(root, "release");
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
const { appendFileSync, readFileSync, writeFileSync, watchFile, unwatchFile } = require("node:fs");
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n");
  process.exit(0);
}
const LOG = ${JSON.stringify(fakeLog)};
const READY = ${JSON.stringify(readyPath)};
const RELEASE = ${JSON.stringify(releasePath)};
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
      const { __tool, __retry, __followup, __deferCall, ...args } = JSON.parse(match[1].replace(/\\\\"/g, '"'));
      waiting.set(id, { threadId: params.threadId, turnId, tool: __tool, retry: __retry, followup: __followup, stale: Boolean(__deferCall), ended: Boolean(__deferCall) });
      const sendCall = () => write({
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
      if (__deferCall) {
        finish(params.threadId, turnId);
        writeFileSync(READY, JSON.stringify({ token: __deferCall, turnId }));
        watchFile(RELEASE, { interval: 10 }, () => {
          try { if (readFileSync(RELEASE, "utf8") !== __deferCall) return; } catch { return; }
          unwatchFile(RELEASE);
          sendCall();
        });
        return;
      }
      return sendCall();
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
        if (turn.followup) {
          log({ turnId: turn.turnId, stage: "transfer", result: message.result ?? null, error: message.error ?? null });
          const id = "stale-" + turn.turnId;
          waiting.set(id, { threadId: turn.threadId, turnId: turn.turnId, stale: true });
          write({ id, method: "item/tool/call", params: { threadId: turn.threadId, turnId: turn.turnId, callId: id, namespace: "openbot", tool: turn.followup.tool, arguments: turn.followup.arguments } });
        } else if (turn.stale) {
          log({ turnId: turn.turnId, stage: "stale", result: message.result ?? null, error: message.error ?? null });
          const id = "recover-" + turn.turnId;
          waiting.set(id, { threadId: turn.threadId, turnId: turn.turnId, recovered: true, ended: turn.ended });
          write({ id, method: "item/tool/call", params: { threadId: turn.threadId, turnId: turn.turnId, callId: id, namespace: "openbot", tool: "list_agents", arguments: {} } });
        } else if (turn.retry && message.result?.success === false) {
          const id = "retry-" + turn.turnId;
          waiting.set(id, { threadId: turn.threadId, turnId: turn.turnId });
          write({ id, method: "item/tool/call", params: { threadId: turn.threadId, turnId: turn.turnId, callId: id, namespace: "openbot", tool: turn.tool, arguments: turn.retry } });
        } else {
          log({ turnId: turn.turnId, ...(turn.recovered ? { stage: "recovery" } : {}), result: message.result ?? null, error: message.error ?? null });
          if (!turn.ended) finish(turn.threadId, turn.turnId);
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
  __retry?:
    | { recipientAgentId: string; task: string; expectedResult: string; sourceMessageIds: string[] }
    | { text: string };
  __followup?: {
    tool: "channel_assign";
    arguments: { recipientAgentId: string; task: string; expectedResult: string; sourceMessageIds: string[] };
  };
  __deferCall?: string;
  agentId?: string;
  instructions?: string;
  routineId?: string;
  name?: string;
  instruction?: string;
  schedule?: { kind: "daily"; time: string } | { kind: "once"; date: string; time: string };
  active?: boolean;
  text?: unknown;
}

interface ToolLog {
  turnId: string;
  stage?: string;
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
let failStorageRead: string | null = null;
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
      const failure = failStorageRead;
      failStorageRead = null;
      const channelStore = agents.channels.store;
      const original = channelStore.message;
      channelStore.message = () => {
        channelStore.message = original;
        throw new Error(failure);
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
  beforeTool?: () => Promise<void>,
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
  await beforeTool?.();
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
  for (const tool of ["channel_remember", "channel_forget_memory"]) {
    for (const [label, args] of [
      ["missing text", {}],
      ["empty text", { text: "" }],
      ["blank text", { text: "   " }],
      ["non-string text", { text: 42 }],
      ["oversized text", { text: "x".repeat(INPUT_LIMITS.agentMemoryText + 1) }],
    ] as const) {
      const memoriesBefore = channels().memories.list(channelId);
      const refused = await callTool(`${tool}: ${label}`, channelId, lead.id, tool, args);
      pass(`${tool}: ${label} returns failed tool feedback`, report.calls.at(-1)?.success === false, refused.text);
      pass(`${tool}: ${label} explains the text requirement`, refused.text.includes("text"), refused.text);
      pass(`${tool}: ${label} does not raise a provider error`, errorEvents.length === 0, JSON.stringify(errorEvents));
      pass(
        `${tool}: ${label} preserves saved memories`,
        JSON.stringify(channels().memories.list(channelId)) === JSON.stringify(memoriesBefore),
      );
    }
  }
  const memoryText = "The synthetic release requires an independent review.";
  const memoryRetry = await callTool("corrected memory", channelId, lead.id, "channel_remember", {
    __retry: { text: memoryText },
  });
  pass("The same provider turn can correct its memory arguments", memoryRetry.success, memoryRetry.text);
  pass(
    "The corrected memory is saved exactly once",
    channels()
      .memories.list(channelId)
      .filter((entry) => entry.text === memoryText).length === 1,
  );
  const forgotten = await callTool("forget corrected memory", channelId, lead.id, "channel_forget_memory", {
    text: memoryText,
  });
  pass(
    "A valid forget call still removes its matching memory",
    forgotten.success && channels().memories.list(channelId).length === 0,
    forgotten.text,
  );
  pass("Memory corrections do not raise provider errors", errorEvents.length === 0, JSON.stringify(errorEvents));
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
  const providerChoice = () => {
    const agent = agents.listAgents().find((candidate) => candidate.id === lead.id);
    assert.ok(agent);
    return {
      provider: agent.provider,
      model: agent.model,
      account: agent.providerAccountId,
      backups: agent.backupModels,
      returnTo: agent.usageLimitReturn,
      pending: agent.pendingRuntime,
    };
  };
  const originalChoice = providerChoice();
  let archivedState = "";
  const archivedId = randomUUID();
  await runCauseEffect(
    channels().command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId: archivedId,
        draft: {
          name: "Archived work",
          title: "",
          instructions: "Check one request.",
          members: [{ agentId: lead.id }, { agentId: tester.id }],
          leadAgentId: lead.id,
        },
      },
      PERSON,
    ),
  );
  const deferred = randomUUID();
  const archivedCallStart = toolLogs().length;
  const archived = await callTool(
    "late handoff after archive",
    archivedId,
    lead.id,
    "channel_assign",
    { ...correction, __deferCall: deferred },
    PERSON,
    async () => {
      await waitFor("the provider to prepare the late call", () => {
        try {
          return JSON.parse(readFileSync(readyPath, "utf8")).token === deferred;
        } catch {
          return false;
        }
      });
      await settled(archivedId);
      await runCauseEffect(
        channels().command({ type: "archive", channelId: archivedId, operationId: randomUUID() }, PERSON),
      );
      archivedState = JSON.stringify({
        tasks: channels().store.tasks(archivedId),
        assignments: channels().store.assignments(archivedId),
      });
      await writeFile(releasePath, deferred);
    },
  );
  pass(
    "An inactive assignment returns a normal failed tool result",
    report.calls.at(-1)?.success === false,
    archived.text,
  );
  pass(
    "Inactive assignment feedback instructs the old turn to end",
    archived.text.includes("End this turn"),
    archived.text,
  );
  pass(
    "An inactive assignment does not raise a global provider error",
    errorEvents.length === 0,
    JSON.stringify(errorEvents),
  );
  await waitFor("the stale turn to finish its correction", () =>
    toolLogs()
      .slice(archivedCallStart)
      .some((entry) => entry.stage === "recovery" && entry.result?.success === true),
  );
  pass(
    "An archived task is not resumed or reassigned",
    channels().store.get(archivedId).archived &&
      channels().store.tasks(archivedId).length === 1 &&
      channels()
        .store.tasks(archivedId)
        .every((task) => task.state === "completed" && task.ownerAgentId === lead.id),
  );
  pass(
    "A late tool call leaves the archived task and assignment unchanged",
    archivedState ===
      JSON.stringify({
        tasks: channels().store.tasks(archivedId),
        assignments: channels().store.assignments(archivedId),
      }),
  );
  const staleStart = toolLogs().length;
  const originalTasks = new Set(
    channels()
      .store.tasks(channelId)
      .map((task) => task.id),
  );
  const beforeStale = channels().store.tasks(channelId).length;
  await callTool("handoff followed by a stale assignment", channelId, lead.id, "channel_transfer", {
    ...correction,
    __followup: { tool: "channel_assign", arguments: correction },
  });
  const staleCalls = toolLogs().slice(staleStart);
  const stale = staleCalls.find((entry) => entry.stage === "stale");
  const staleText = stale?.result?.contentItems.map((item) => item.text).join("\n") ?? JSON.stringify(stale?.error);
  report.calls.push({
    name: "stale assignment",
    turnId: stale?.turnId ?? "missing",
    success: stale?.result?.success ?? null,
    text: staleText,
  });
  pass("A superseded task returns a normal failed tool result", stale?.result?.success === false, staleText);
  pass(
    "A superseded task tells the old turn to end rather than retry",
    staleText.includes("End this turn") && staleText.includes("Do not continue the old task"),
    staleText,
  );
  pass(
    "Stale assignment feedback does not raise a global provider error",
    errorEvents.length === 0,
    JSON.stringify(errorEvents),
  );
  pass(
    "The same turn can finish after a stale call",
    staleCalls.some((entry) => entry.stage === "recovery" && entry.result?.success === true),
  );
  pass(
    "A stale handoff creates no child or replacement task",
    channels().store.tasks(channelId).length === beforeStale + 1,
  );
  const transferred = channels()
    .store.tasks(channelId)
    .find((task) => !originalTasks.has(task.id));
  pass(
    "A stale call preserves the transferred task's current owner and revision",
    transferred?.ownerAgentId === tester.id &&
      transferred.revision === 1 &&
      transferred.assignmentCount === 1 &&
      transferred.state === "completed",
  );
  const oldAssignment = channels()
    .store.assignments(channelId)
    .find((assignment) => assignment.turnId === stale?.turnId);
  pass("A stale call does not revive the old assignment", oldAssignment?.state === "completed");
  pass(
    "Expected stale calls do not switch provider, model, account or fallbacks",
    JSON.stringify(providerChoice()) === JSON.stringify(originalChoice),
  );
  for (const failure of [
    "Fixture storage read failure.",
    "The channel assignment is no longer active.",
    "The channel task has changed.",
  ]) {
    const beforeErrors = errorEvents.length;
    failStorageRead = failure;
    const fatal = await callTool(`unexpected storage failure: ${failure}`, channelId, lead.id, "channel_assign", {
      ...correction,
      task: `Review the synthetic evidence: ${failure}`,
    });
    pass(
      `Unexpected storage failure stays fatal: ${failure}`,
      !fatal.success &&
        report.calls.at(-1)?.success === null &&
        errorEvents.slice(beforeErrors).some((event) => event.code === "server_request_failed"),
      fatal.text,
    );
  }
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
