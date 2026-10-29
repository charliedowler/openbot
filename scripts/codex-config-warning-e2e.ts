import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, type AgentEvent } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { CodexAppServerClient } from "../src/backend/app-server-client";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { decodeRecordResponse } from "../src/backend/protocol";

const out = resolve(import.meta.dirname, "../.openbot-build/codex-config-warning-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-config-warning-"));
const executable = join(root, "codex");
const codexHome = join(root, "codex-home");
const configPath = join(codexHome, "config.toml");
const header =
  "Project-local config, hooks, and exec policies are disabled in the following folders until the project is trusted, but skills still load.";
const folder = "/test/untrusted-project";
const secret = "sk-config-warning-fixture-secret";
const config = '[projects."/test/untrusted-project"]\ntrust_level = "untrusted"\n';
await mkdir(codexHome);
await writeFile(configPath, config);
await writeFile(
  executable,
  `#!/usr/bin/env node
const readline = require("node:readline");
if (process.argv.includes("--version")) { console.log("codex-cli 0.165.0"); process.exit(0); }
const write = (value) => console.log(JSON.stringify(value));
readline.createInterface({ input: process.stdin }).on("line", (line) => {
  const message = JSON.parse(line);
  if (message.id === undefined) return;
  let result = {};
  switch (message.method) {
    case "account/read": result = { account: { type: "chatgpt", email: "fixture@example.invalid" } }; break;
    case "account/rateLimits/read": result = { rateLimits: { limitId: "codex", primary: null, secondary: null }, rateLimitsByLimitId: null }; break;
    case "model/list": result = { data: [{ model: "gpt-5.5", displayName: "GPT-5.5" }] }; break;
    case "plugin/list": result = { marketplaces: [] }; break;
    case "config/read": result = { config: { mcp_servers: {} }, layers: [] }; break;
    case "test/emit":
      if (message.params.stderr) process.stderr.write(message.params.stderr + "\\n");
      if (message.params.warning) write({ method: "configWarning", params: message.params.warning });
      break;
  }
  write({ id: message.id, result });
});
`,
  { mode: 0o755 },
);
process.env.OPENBOT_CODEX_PATH = executable;
process.env.CODEX_HOME = codexHome;

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");
const database = new OpenBotDatabase(join(root, "profile"));
const store = new AgentStore(database.userDataPath, join(root, "workspace-home"), database);
const mailbox = new MailboxStore(database.userDataPath, store.sharedRoot, database);
const unused = () => Effect.die(new Error("The warning test does not use a browser."));
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
};
const clients: CodexAppServerClient[] = [];
const events: AgentEvent[] = [];
const service = new AgentService({
  store,
  mailbox,
  browser,
  preferredProvider: "codex",
  offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "codex"),
  clientFactory: (provider) => {
    assert.equal(provider, "codex");
    const client = new CodexAppServerClient(executable, 5_000, { CODEX_HOME: codexHome });
    clients.push(client);
    return client;
  },
});
service.on("event", (event) => events.push(event));
const checks: string[] = [];
let failure: string | undefined;
async function waitFor(read: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!read()) {
    assert.ok(Date.now() < deadline, "The provider events arrive before the deadline.");
    await new Promise((done) => setTimeout(done, 10));
  }
}
try {
  await runCauseEffect(service.initialize());
  const client = clients.find((candidate) => candidate.running);
  assert.ok(client, "The real Codex client starts.");
  const emit = (params: { stderr?: string; warning?: { summary: string; details: string | null } }) =>
    runCauseEffect(client.request("test/emit", params, decodeRecordResponse));
  const warning = {
    summary: `${header}\n    1. ${folder}\n       Review project trust before enabling local settings.`,
    details: `api_key=${secret}`,
  };
  await emit({ stderr: `2026-10-29T19:00:00Z ERROR codex_app_server: ${header}`, warning });
  await emit({ warning });
  await runCauseEffect(client.stop());
  client.start();
  await emit({ stderr: `2026-10-29T19:01:00Z ERROR codex_app_server: ${header}`, warning });
  await emit({ warning: { summary: `${header}\n    1. /test/another-project`, details: null } });
  await emit({ warning: { summary: header, details: null } });
  await emit({
    warning: { summary: "Codex is ignoring 1 unrecognized configuration setting: `old_setting`", details: null },
  });
  await emit({ stderr: "ERROR codex_app_server: Authentication failed: invalid token" });
  await emit({ stderr: `ERROR codex_app_server: ${header} Authentication failed.` });
  await emit({ stderr: "ERROR codex_app_server: Protocol failure: invalid response" });
  await waitFor(() => events.some((event) => event.type === "error" && event.message.includes("Protocol failure")));
  const errors = events.filter((event) => event.type === "error");
  assert.equal(errors.filter((event) => event.code === "codex_diagnostic" && event.message.endsWith(header)).length, 0);
  checks.push("The trust notice does not become a provider failure.");
  const warnings = errors.filter((event) => event.code === "codex_config_untrusted");
  assert.equal(warnings.length, 3);
  checks.push("Duplicate notices are suppressed across a provider process restart; changed folders are reported.");
  assert.ok(warnings[0]?.message.includes(folder));
  assert.ok(warnings[0]?.message.includes("config, hooks, and command policies"));
  checks.push("The warning names the folder and the disabled project settings.");
  assert.ok(warnings.some((event) => event.message.includes("in untrusted folders")));
  checks.push("A notice without folder details still gives an actionable warning.");
  assert.ok(!JSON.stringify(errors).includes(secret));
  checks.push("Warning details do not expose credentials.");
  assert.equal(errors.filter((event) => event.code === "codex_config_ignored").length, 1);
  assert.ok(errors.some((event) => event.message.includes("Authentication failed")));
  assert.ok(
    errors.some(
      (event) => event.code === "codex_diagnostic" && event.message.includes(`${header} Authentication failed.`),
    ),
  );
  assert.ok(errors.some((event) => event.message.includes("Protocol failure")));
  checks.push("Ignored settings, authentication failures, and protocol failures remain visible.");
  assert.equal(await readFile(configPath, "utf8"), config);
  checks.push("The project stays untrusted and its configuration stays unchanged.");
} catch (error) {
  failure = error instanceof Error ? error.stack : String(error);
} finally {
  await runCauseEffect(service.stop());
  database.close();
  await mkdir(out, { recursive: true });
  await writeFile(
    join(out, "report.json"),
    JSON.stringify({ passed: !failure, checks, failure, fixture: root }, null, 2),
  );
}
for (const check of checks) process.stdout.write(`PASS ${check}\n`);
if (failure) process.stderr.write(`${failure}\n`);
process.stdout.write(`Report: ${join(out, "report.json")}\n`);
if (failure) process.exitCode = 1;
