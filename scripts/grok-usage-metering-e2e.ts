import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { decodeUsageTokens, type UsageTokens } from "@openbot/contracts/ipc";
import { collectProviderUsage } from "../src/backend/agent/usage-collection";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { NO_PROVIDER_CREDENTIALS, requireProviderDriver } from "../src/backend/provider-drivers";
import { runTurn, startThread } from "./provider-fakes/host";

const output = resolve(import.meta.dirname, "../.openbot-build/grok-steer-e2e/usage-metering");
const root = await mkdtemp(join(tmpdir(), "openbot-grok-usage-"));
const executable = join(root, "grok");
const database = new OpenBotDatabase(join(root, "data"));
const fake = `#!/usr/bin/env node
const write = (message) => process.stdout.write(JSON.stringify({ jsonrpc: "2.0", ...message }) + "\\n");
const setup = (sessionId) => ({ sessionId, models: { availableModels: [{ modelId: "grok-4.7", name: "Grok" }], currentModelId: "grok-4.7" } });
const handle = (message) => {
  if (message.id === undefined) return;
  const params = message.params || {};
  if (message.method === "initialize") return write({ id: message.id, result: { protocolVersion: 1, authMethods: [], agentCapabilities: { loadSession: true } } });
  if (message.method === "session/new") return write({ id: message.id, result: setup("synthetic-grok-session") });
  if (message.method === "session/load") return write({ id: message.id, result: setup(params.sessionId) });
  if (message.method === "session/prompt") {
    const text = params.prompt.map((block) => block.text || "").join("");
    const name = /CASE=([A-Z]+)/.exec(text)?.[1] || "FIRST";
    const usage = name === "SMALLER"
      ? { inputTokens: 60, cachedReadTokens: 10, cacheCreationTokens: 5, outputTokens: 12, totalTokens: 72, reasoningTokens: 4 }
      : { inputTokens: 120, cachedReadTokens: 40, cacheCreationTokens: 10, outputTokens: 30, totalTokens: 150, reasoningTokens: 8 };
    if (name === "INCOMPLETE") usage.usageIsIncomplete = true;
    if (name === "MALFORMED") usage.inputTokens = "120";
    const meta = { inputTokens: 1, outputTokens: 1, cachedReadTokens: 0, totalTokens: 999999 };
    if (name !== "MISSING") meta.usage = usage;
    write({ method: "session/update", params: { sessionId: params.sessionId,
      update: { sessionUpdate: "usage_update", used: 999999, size: 1000000 } } });
    write({ method: "session/update", params: { sessionId: params.sessionId,
      update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text: "ok" } } } });
    const standard = { inputTokens: 200, cachedReadTokens: 60, cachedWriteTokens: 10, outputTokens: 50, totalTokens: 250 };
    const result = name === "STANDARD" ? { stopReason: "end_turn", usage: standard, _meta: meta }
      : { stopReason: "end_turn", _meta: meta };
    return write({ id: message.id, result });
  }
  write({ id: message.id, result: {} });
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
await writeFile(executable, fake, { mode: 0o755 });
const client = requireProviderDriver("grok").createClient(
  { executable, version: "1.0.70", source: "managed" },
  10_000,
  NO_PROVIDER_CREDENTIALS,
);
const report: { passed: boolean; checks: string[]; tokens: UsageTokens[]; error?: string } = {
  passed: false,
  checks: [],
  tokens: [],
};
try {
  await runCauseEffect(database.initialize());
  const session = { id: "synthetic-session", provider: "grok" as const, createdAt: new Date().toISOString() };
  client.on("notification", ({ method, params }) => {
    collectProviderUsage(database.usage, { id: "synthetic-agent", model: "grok-4.7" }, session, method, params);
    collectProviderUsage(database.usage, { id: "synthetic-agent", model: "grok-4.7" }, session, method, params);
  });
  client.start();
  const thread = await startThread(client, { cwd: root, model: "grok-4.7" });
  for (const name of ["FIRST", "SMALLER", "INCOMPLETE", "MALFORMED", "MISSING", "STANDARD"]) {
    const result = await runTurn(client, thread, { text: `CASE=${name}`, timeoutMs: 10_000 });
    assert.equal(result.status, "completed");
    assert.equal(result.reply, "ok");
  }
  const rows = database.connection.prepare("SELECT tokens_json FROM agent_usage_records ORDER BY rowid").all();
  report.tokens = rows.map((row) => decodeUsageTokens(JSON.parse(String(row.tokens_json))));
  assert.deepEqual(
    report.tokens,
    [
      { uncachedInput: 70, cachedInput: 40, cacheCreation: 10, output: 30 },
      { uncachedInput: 45, cachedInput: 10, cacheCreation: 5, output: 12 },
      { uncachedInput: 130, cachedInput: 60, cacheCreation: 10, output: 50 },
    ],
    "Grok whole-prompt metadata is metered once per prompt, without last-call or context-fill counts.",
  );
  report.checks.push(
    "Whole-prompt metadata counts cache reads and writes correctly and includes reasoning once in output.",
  );
  report.checks.push("A smaller later prompt is recorded in full; duplicate notifications do not double count.");
  report.checks.push(
    "Missing, malformed and incomplete metadata stays unknown; context fill and last-call sibling fields are ignored.",
  );
  report.checks.push("A standard ACP usage response takes precedence and retains its session-counter interpretation.");
  await runCauseEffect(client.stop());
  client.start();
  await runCauseEffect(
    client.request("thread/resume", { threadId: thread, cwd: root, model: "grok-4.7" }, (value) => value),
  );
  assert.equal((await runTurn(client, thread, { text: "CASE=SMALLER", timeoutMs: 10_000 })).status, "completed");
  const resumed = database.connection
    .prepare("SELECT tokens_json FROM agent_usage_records ORDER BY rowid DESC LIMIT 1")
    .get();
  assert.deepEqual(decodeUsageTokens(JSON.parse(String(resumed?.tokens_json))), report.tokens[1]);
  assert.equal(database.connection.prepare("SELECT COUNT(*) AS count FROM agent_usage_records").get()?.count, 4);
  report.checks.push("The prompt counter remains separate after a provider process restart and session resume.");
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  await runCauseEffect(client.stop());
  database.close();
  await rm(root, { recursive: true, force: true });
  mkdirSync(output, { recursive: true });
  writeFileSync(join(output, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
