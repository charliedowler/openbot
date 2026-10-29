import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, decodeUsageTokens } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";

const output = resolve(import.meta.dirname, "../.openbot-build/agent-channel-manage-e2e/helper-usage");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-helper-usage-"));
const userData = join(root, "data");
const home = join(root, "home");
const fakeCodex = join(root, "codex");
const fakeLog = join(root, "codex.jsonl");
process.env.HOME = home;
process.env.CODEX_HOME = join(home, ".codex");
process.env.OPENBOT_CODEX_PATH = fakeCodex;
mkdirSync(home, { recursive: true });
const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const { runCauseEffect } = await import("../src/backend/effect-boundary");

const fake = `#!/usr/bin/env node
const { appendFileSync } = require("node:fs");
if (process.argv.includes("--version")) {
  process.stdout.write("codex-cli 0.160.1\\n"); process.exit(0);
}
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const log = (message) => appendFileSync(${JSON.stringify(fakeLog)}, JSON.stringify(message) + "\\n");
const handle = (message) => {
  const params = message.params || {};
  if (message.id === undefined) return;
  switch (message.method) {
    case "initialize": return write({ id: message.id, result: {} });
    case "account/read": return write({ id: message.id, result: { account: { type: "chatgpt", email: "synthetic@example.com" } } });
    case "account/rateLimits/read": return write({ id: message.id, result: { rateLimits: null, rateLimitsByLimitId: null } });
    case "model/list": return write({ id: message.id, result: { data: [{ model: "gpt-6-luna", displayName: "GPT-6 Luna" }] } });
    case "plugin/list": return write({ id: message.id, result: { marketplaces: [] } });
    case "config/read": return write({ id: message.id, result: { config: { mcp_servers: {} }, layers: [] } });
    case "thread/start":
      log({ method: message.method, model: params.model, effort: params.effort, profileGeneration: params.profileGeneration,
        dynamicTools: params.dynamicTools, persistSession: params.persistSession });
      return write({ id: message.id, result: { thread: { id: "synthetic-thread", turns: [] } } });
    case "turn/start": {
      const threadId = params.threadId;
      const turnId = "synthetic-turn";
      const text = (params.input || []).map((item) => item.text || "").join("");
      write({ id: message.id, result: { turn: { id: turnId, status: "inProgress", items: [] } } });
      write({ method: "turn/started", params: { threadId, turn: { id: turnId } } });
      const tokens = { inputTokens: 120, cachedInputTokens: 40, cacheWriteInputTokens: 10, outputTokens: 15 };
      if (text !== "UNKNOWN") {
        const usage = { method: "thread/tokenUsage/updated", params: { threadId, turnId, tokenUsage: { total: tokens, last: tokens } } };
        write(usage); write(usage);
      }
      if (text === "FAIL") {
        write({ method: "turn/completed", params: { threadId, turn: { id: turnId, status: "failed", error: { message: "Synthetic helper failure." } } } });
        return;
      }
      write({ method: "item/completed", params: { threadId, turnId, item: { id: "answer", type: "agentMessage", text: "ok", phase: "final_answer" } } });
      write({ method: "turn/completed", params: { threadId, turn: { id: turnId, status: "completed" } } });
      return;
    }
    default: return write({ id: message.id, result: {} });
  }
};
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let end;
  while ((end = buffer.indexOf("\\n")) >= 0) {
    const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
    if (line.trim()) handle(JSON.parse(line));
  }
});
`;

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
await writeFile(fakeCodex, fake, { mode: 0o755 });
const database = new OpenBotDatabase(userData);
const store = new AgentStore(userData, home, database);
const mailbox = new MailboxStore(userData, store.sharedRoot, database);
const service = new AgentService({
  store,
  mailbox,
  browser,
  preferredProvider: "codex",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "codex"),
});
const report: { passed: boolean; checks: string[]; records?: unknown; error?: string } = { passed: false, checks: [] };
try {
  await runCauseEffect(service.initialize());
  const created = await runCauseEffect(
    store.createAgent({ name: "Planner", description: "Synthetic planner", avatarSeed: "planner", avatarHue: null }),
  );
  const lead = await runCauseEffect(
    store.updateAgent({ agentId: created.id, provider: "codex", model: "gpt-6-luna", reasoningEffort: "high" }),
  );
  assert.equal(await runCauseEffect(service.channels.hooks.generate(lead, "ROUTE")), "ok");
  assert.equal(await runCauseEffect(service.channels.hooks.generate(lead, "SUMMARY", "channel-summary")), "ok");
  await assert.rejects(runCauseEffect(service.channels.hooks.generate(lead, "FAIL", "channel-summary")));
  assert.equal(await runCauseEffect(service.channels.hooks.generate(lead, "UNKNOWN")), "ok");
  const rows = database.connection
    .prepare("SELECT agent_id,session_id,model,tokens_json FROM agent_usage_records ORDER BY recorded_at,rowid")
    .all();
  report.records = rows;
  assert.equal(rows.length, 3, "Each reported helper usage is recorded once, including the failed helper.");
  assert.equal(
    new Set(rows.map((row) => row.session_id)).size,
    3,
    "Helpers on disposable processes have separate usage counters.",
  );
  assert.ok(rows.every((row) => row.agent_id === lead.id && row.model === lead.model));
  assert.equal(rows.filter((row) => String(row.session_id).startsWith("channel-routing:")).length, 1);
  assert.equal(rows.filter((row) => String(row.session_id).startsWith("channel-summary:")).length, 2);
  for (const row of rows) {
    assert.equal(typeof row.tokens_json, "string");
    assert.deepEqual(decodeUsageTokens(JSON.parse(String(row.tokens_json))), {
      uncachedInput: 70,
      cachedInput: 40,
      cacheCreation: 10,
      output: 15,
    });
  }
  report.checks.push(
    "Routing, summary and failed helper usage reaches the lead agent with separate purpose identities and exact provider counters.",
  );
  report.checks.push(
    "Repeated provider notifications do not double count, and missing counters do not become invented zeros.",
  );
  const starts = readFileSync(fakeLog, "utf8")
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  assert.equal(starts.length, 4);
  assert.ok(
    starts.every(
      (start) =>
        start.model === lead.model &&
        start.effort === "high" &&
        start.profileGeneration === true &&
        start.persistSession === false &&
        start.dynamicTools.length === 0,
    ),
  );
  report.checks.push(
    "Every helper keeps the lead model and effort, creates no persistent session and has no dynamic tools.",
  );
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  await runCauseEffect(service.stop());
  database.close();
  await rm(root, { recursive: true, force: true });
  mkdirSync(output, { recursive: true });
  writeFileSync(join(output, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
