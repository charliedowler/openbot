// Checks that a routine run of an agent's own chat starts beside the agent's channel work, and that
// the other rules of the own chat do not change. Runs the real agent service on a temporary profile
// with a fake Codex app-server. A turn whose current text names a `HOLD-<token>` runs until the
// script releases that token, so a channel turn and a routine run can stay open at the same time.
// `bun scripts/own-chat-routine-beside-channel-e2e.ts`.
// Writes .openbot-build/own-chat-routine-beside-channel-e2e/report.json.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { CodexAppServerClient } from "../src/backend/app-server-client";

const OUT = resolve(import.meta.dirname, "../.openbot-build/own-chat-routine-beside-channel-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-routine-beside-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const fakeCodex = join(root, "codex");
const fakeLog = join(root, "codex.jsonl");
const releaseDir = join(root, "release");
process.env.CODEX_HOME = join(home, ".codex");
process.env.OPENBOT_CODEX_PATH = fakeCodex;
mkdirSync(home, { recursive: true });
mkdirSync(releaseDir, { recursive: true });

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

const PERSON = { id: "local", name: "Alex" };
const OBSERVE_MS = 2_000;

/**
 * A Codex app-server with the methods that the service calls. Each turn writes `start` and `end` to
 * the log, with the hold token of its current text and whether its thread is ephemeral (a routine
 * session). A turn with a token ends when the file `<release>/<token>` exists.
 */
const FAKE_CODEX = `#!/usr/bin/env node
const { appendFileSync, existsSync } = require("node:fs");
const { join } = require("node:path");
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n");
  process.exit(0);
}
const LOG = ${JSON.stringify(fakeLog)};
const RELEASE = ${JSON.stringify(releaseDir)};
const config = { config: { mcp_servers: {} }, layers: [{ name: { type: "user", file: "/fake/.codex/config.toml", profile: null }, version: "1", config: { mcp_servers: {} } }] };
let buffer = "";
let threads = 0;
let turns = 0;
const ephemeral = new Set();
const open = new Map();
process.stdout.on("error", (error) => {
  if (error.code === "EPIPE") process.exit(0);
  throw error;
});
const log = (entry) => appendFileSync(LOG, JSON.stringify({ at: Date.now(), ...entry }) + "\\n");
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const finish = (threadId, turnId, token, status = "completed") => {
  if (!open.delete(turnId)) return;
  log({ event: "end", token, threadId, turnId, status });
  if (status === "completed") {
    const item = { type: "agentMessage", id: "message-" + turnId, text: "Done.", phase: "final_answer" };
    write({ method: "item/completed", params: { threadId, turnId, item } });
  }
  write({ method: "turn/completed", params: { threadId, turn: { id: turnId, status } } });
};
const currentToken = (text) => {
  const current = text.includes("Current assignment:") ? text.split("Current assignment:").at(-1) : text;
  return [...current.matchAll(/HOLD-[A-Za-z0-9]+/g)].at(-1)?.[0] ?? null;
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
    case "thread/start": {
      const id = "thread-" + (++threads);
      if (params.ephemeral) ephemeral.add(id);
      return write({ id: message.id, result: { thread: { id, turns: [] } } });
    }
    case "thread/resume": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "thread/unsubscribe": return write({ id: message.id, result: { status: "Unsubscribed" } });
    case "thread/turns/list":
    case "thread/items/list": return write({ id: message.id, result: { data: [], nextCursor: null } });
    case "thread/read": return write({ id: message.id, result: { thread: { id: params.threadId, turns: [] } } });
    case "turn/start": {
      const turnId = "turn-" + (++turns);
      const text = (params.input || []).map((item) => item.text || "").join("\\n");
      const token = currentToken(text);
      open.set(turnId, params.threadId);
      write({ id: message.id, result: { turn: { id: turnId, status: "inProgress", items: [] } } });
      write({ method: "turn/started", params: { threadId: params.threadId, turn: { id: turnId } } });
      log({ event: "start", token, threadId: params.threadId, turnId, ephemeral: ephemeral.has(params.threadId) });
      if (!token) return setTimeout(() => finish(params.threadId, turnId, token), 20);
      const timer = setInterval(() => {
        if (!open.has(turnId)) return clearInterval(timer);
        if (!existsSync(join(RELEASE, token))) return;
        clearInterval(timer);
        finish(params.threadId, turnId, token);
      }, 20);
      return;
    }
    case "turn/interrupt": {
      write({ id: message.id, result: {} });
      return finish(params.threadId, params.turnId, null, "interrupted");
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
    }
    newline = buffer.indexOf("\\n");
  }
});
`;

interface TurnLog {
  at: number;
  event: "start" | "end";
  token: string | null;
  threadId: string;
  turnId: string;
  ephemeral?: boolean;
  status?: string;
}

const report: {
  checks: { name: string; passed: boolean; detail?: string }[];
  passed: boolean;
  error?: string;
} = { checks: [], passed: false };

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

const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));

async function waitFor<T>(label: string, read: () => T | null | undefined | false, timeoutMs = 20_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await sleep(25);
  }
}

function turns(): TurnLog[] {
  if (!existsSync(fakeLog)) return [];
  return readFileSync(fakeLog, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

const started = (token: string) => turns().find((entry) => entry.event === "start" && entry.token === token);
const ended = (token: string) => turns().find((entry) => entry.event === "end" && entry.token === token);
const startIndex = (token: string) => turns().findIndex((entry) => entry.event === "start" && entry.token === token);

function release(token: string): void {
  writeFileSync(join(releaseDir, token), "");
}

/** Records one check. A failed check is kept in the report and the scenario goes on. */
function check(name: string, condition: boolean, detail?: string): boolean {
  report.checks.push({ name, passed: condition, ...(detail ? { detail } : {}) });
  return condition;
}

await writeFile(fakeCodex, FAKE_CODEX, { mode: 0o755 });
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
const agents = new AgentService({
  store,
  mailbox,
  browser,
  preferredProvider: "codex",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "codex"),
  timezone: () => "UTC",
  clientFactory: (provider, cli) => {
    assert.equal(provider, "codex");
    return new CodexAppServerClient(cli.executable);
  },
});
const channels = () => agents.channels;

const idle = (agentId: string) =>
  agents
    .listQueue(agentId)
    .deliveries.every((delivery) => !["queued", "starting", "running"].includes(delivery.status));

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
    await waitFor(`the first chat turn of ${name} to end`, () => idle(agent.id));
    return agent;
  };
  const lead = await create("Lead");
  const helper = await create("Helper");
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
          members: [{ agentId: lead.id }, { agentId: helper.id }],
          leadAgentId: lead.id,
        },
      },
      PERSON,
    ),
  );

  const channelWorkActive = () =>
    channels()
      .store.tasks(channelId)
      .some((task) => ["queued", "running", "waiting"].includes(task.state));
  const settle = (label: string) =>
    waitFor(`${label}: the channel and the own chat to settle`, () => !channelWorkActive() && idle(lead.id), 30_000);

  const sendChannel = (token: string) =>
    runCauseEffect(
      channels().command(
        {
          type: "send",
          operationId: randomUUID(),
          channelId,
          text: `Check the release. ${token}`,
          recipientAgentId: lead.id,
          replyToMessageId: null,
          attachmentDraftIds: [],
        },
        PERSON,
      ),
    );
  const sendOwnChat = (text: string) =>
    runCauseEffect(agents.sendMessage({ agentId: lead.id, text, clientMessageId: randomUUID() }));
  const runRoutine = async (token: string) => {
    const routine = agents.createRoutine({
      agentId: lead.id,
      name: `Watch ${token}`,
      instruction: `Read the watch event. ${token}`,
      active: false,
      timezone: "UTC",
      schedule: { kind: "daily", time: "09:00" },
    });
    await runCauseEffect(agents.testRoutine({ agentId: lead.id, routineId: routine.id }));
  };
  const scenario = async (name: string, tokens: string[], body: () => Promise<void>) => {
    try {
      await body();
    } catch (error) {
      check(`${name}: scenario ran to the end`, false, error instanceof Error ? error.message : String(error));
    } finally {
      for (const token of tokens) release(token);
      await settle(name);
    }
  };

  // 1. New: a routine run of the own chat starts while a channel turn of the same agent runs.
  await scenario("routine beside channel", ["HOLD-C1", "HOLD-R1"], async () => {
    await sendChannel("HOLD-C1");
    await waitFor("the channel turn C1 to start", () => started("HOLD-C1"));
    await runRoutine("HOLD-R1");
    const routine = await waitFor("the routine turn R1 to start", () => started("HOLD-R1"), 8_000).catch(
      () => undefined,
    );
    check("A routine run starts while the channel turn of its agent runs", Boolean(routine) && !ended("HOLD-C1"));
    check("The routine run beside the channel turn uses its own ephemeral session", routine?.ephemeral === true);
    if (!routine) return;
    release("HOLD-R1");
    await waitFor("the routine turn R1 to end", () => ended("HOLD-R1"));
    check("The routine run ends while the channel turn still runs", !ended("HOLD-C1"));
  });

  // 2. Unchanged: a message from the person to the own chat still waits for the channel work.
  await scenario("user message waits for channel work", ["HOLD-C2", "HOLD-U2"], async () => {
    await sendChannel("HOLD-C2");
    await waitFor("the channel turn C2 to start", () => started("HOLD-C2"));
    release("HOLD-U2");
    await sendOwnChat("Status please. HOLD-U2");
    await sleep(OBSERVE_MS);
    check("A message from the person does not start while the channel turn runs", !started("HOLD-U2"));
    check(
      "The own chat names the channel that holds it",
      agents.listQueue(lead.id).hold?.channelId === channelId,
      JSON.stringify(agents.listQueue(lead.id).hold ?? null),
    );
    release("HOLD-C2");
    await waitFor("the person's message U2 to start", () => started("HOLD-U2"));
    check(
      "The message from the person starts after the channel turn ends",
      turns().findIndex((entry) => entry.event === "end" && entry.token === "HOLD-C2") < startIndex("HOLD-U2"),
    );
  });

  // 3. Unchanged: a routine run does not pass a message of the person that waits before it.
  await scenario("routine keeps queue order", ["HOLD-C3", "HOLD-R3"], async () => {
    await sendChannel("HOLD-C3");
    await waitFor("the channel turn C3 to start", () => started("HOLD-C3"));
    await sendOwnChat("Status please. HOLD-U3");
    await runRoutine("HOLD-R3");
    await sleep(OBSERVE_MS);
    check(
      "A routine run behind a waiting message of the person does not start beside the channel turn",
      !started("HOLD-R3"),
    );
    release("HOLD-C3");
    await waitFor("the person's message U3 to start", () => started("HOLD-U3"));
    check(
      "The message of the person starts before the routine run",
      startIndex("HOLD-R3") < 0 || startIndex("HOLD-U3") < startIndex("HOLD-R3"),
    );
    await sleep(OBSERVE_MS / 2);
    check("The routine run does not start while the message of the person runs", !started("HOLD-R3"));
    release("HOLD-U3");
    await waitFor("the routine turn R3 to start", () => started("HOLD-R3"));
  });

  // 4. Unchanged: the own chat runs one turn at a time. A message of the person waits for a routine run.
  await scenario("person waits for routine run", ["HOLD-C4", "HOLD-R4", "HOLD-U4"], async () => {
    await sendChannel("HOLD-C4");
    await waitFor("the channel turn C4 to start", () => started("HOLD-C4"));
    await runRoutine("HOLD-R4");
    await sleep(OBSERVE_MS / 2);
    release("HOLD-C4");
    await waitFor("the channel work C4 to end", () => ended("HOLD-C4") && !channelWorkActive());
    await waitFor("the routine turn R4 to start", () => started("HOLD-R4"));
    await sendOwnChat("Status please. HOLD-U4");
    await sleep(OBSERVE_MS);
    check("A message of the person does not start while a routine run of the own chat runs", !started("HOLD-U4"));
    release("HOLD-R4");
    await waitFor("the person's message U4 to start", () => started("HOLD-U4"));
    check("The message of the person starts after the routine run ends", Boolean(ended("HOLD-R4")));
  });

  // 5. Unchanged: a new channel task of the agent does not start while a routine run of its own chat runs.
  await scenario("channel task waits for routine run", ["HOLD-C5", "HOLD-R5", "HOLD-C5B"], async () => {
    await sendChannel("HOLD-C5");
    await waitFor("the channel turn C5 to start", () => started("HOLD-C5"));
    await runRoutine("HOLD-R5");
    await sleep(OBSERVE_MS / 2);
    release("HOLD-C5");
    await waitFor("the channel work C5 to end", () => ended("HOLD-C5") && !channelWorkActive());
    await waitFor("the routine turn R5 to start", () => started("HOLD-R5"));
    await sendChannel("HOLD-C5B");
    await sleep(OBSERVE_MS);
    check("A new channel task does not start while a routine run of the own chat runs", !started("HOLD-C5B"));
    release("HOLD-R5");
    await waitFor("the channel turn C5B to start", () => started("HOLD-C5B"));
  });

  // 6. No two turns ever ran on the own chat at the same time. Own-chat turns carry no channel token.
  const open = new Set<string>();
  let overlap = false;
  for (const entry of turns()) {
    if (entry.token?.startsWith("HOLD-C")) continue;
    if (entry.event === "start") {
      if (open.size > 0) overlap = true;
      open.add(entry.turnId);
    } else open.delete(entry.turnId);
  }
  check("No two turns of the own chat ran at the same time", !overlap);

  report.passed = report.checks.every((item) => item.passed);
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
} finally {
  await runCauseEffect(agents.stop()).catch(() => {});
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  for (const item of report.checks) process.stdout.write(`${item.passed ? "PASS" : "FAIL"} ${item.name}\n`);
  process.stdout.write(`${report.passed ? "PASS" : "FAIL"} ${join(OUT, "report.json")}\n`);
  if (report.error) process.stdout.write(`${report.error}\n`);
  if (!report.passed) process.exitCode = 1;
}
