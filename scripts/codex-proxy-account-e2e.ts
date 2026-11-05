// Checks that a Codex sign-in can run through an OpenAI-compatible proxy, such as CLIProxyAPI, with
// `openbot-proxy.json` in its home folder. Runs the real agent service and the real Codex CLI on a
// temporary profile. A local server stands in for the proxy, for OpenAI and for ChatGPT, so no
// account or key leaves this computer. The proxy accepts only the test key.
// Phases: an agent's chat starts on the own sign-in; the proxy file is added and the service starts
// again, so the saved chat resumes on the `openai` provider through the proxy with the key, not the
// ChatGPT token; a new chat runs; usage reading gives no ring and no error; a 429 from the proxy holds
// the agent; the proxy file is removed and Codex uses the ChatGPT sign-in again. auth.json does not
// change, and the key is in no log, status, event or file of the profile.
// `node --import tsx scripts/codex-proxy-account-e2e.ts`. Writes .openbot-build/codex-proxy-account-e2e/report.json.
import assert from "node:assert/strict";
import { mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, type AgentEvent } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";

const OUT = resolve(import.meta.dirname, "../.openbot-build/codex-proxy-account-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-codex-proxy-"));
const userData = join(root, "user-data");
const home = join(root, "home");
const codexHome = join(home, ".codex");
const keyFile = join(root, "proxy-key");
const PROXY_KEY = "sk-openbot-proxy-e2e-0d4f7a29c1";
const OWN_KEY = "sk-openbot-own-e2e-5b8e1c3f77";
const NL = String.fromCharCode(10);

process.env.HOME = home;
process.env.CODEX_HOME = codexHome;
mkdirSync(codexHome, { recursive: true });

const { resolveCodexCli } = await import("../src/backend/cli");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const cli = await runCauseEffect(resolveCodexCli());
process.env.OPENBOT_CODEX_PATH = cli.executable;

const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { ProviderAccountStore } = await import("../src/backend/agent/provider-account-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

const b64 = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
const plan = { chatgpt_account_id: "acct-e2e", chatgpt_plan_type: "pro", chatgpt_user_id: "user-e2e" };
interface Claims {
  exp: number;
  email?: string;
  "https://api.openai.com/auth": typeof plan;
}
const jwt = (claims: Claims) => `${b64({ alg: "none", typ: "JWT" })}.${b64(claims)}.sig`;
const expires = Math.floor(Date.now() / 1000) + 30 * 86_400;
const CHATGPT_TOKEN = jwt({ exp: expires, "https://api.openai.com/auth": plan });
const CHATGPT_AUTH = `${JSON.stringify(
  {
    auth_mode: "chatgpt",
    OPENAI_API_KEY: null,
    tokens: {
      id_token: jwt({ exp: expires, email: "codex@example.com", "https://api.openai.com/auth": plan }),
      access_token: CHATGPT_TOKEN,
      refresh_token: "refresh-e2e",
      account_id: "acct-e2e",
    },
    last_refresh: new Date().toISOString(),
  },
  null,
  2,
)}${NL}`;

type Caller = "proxy-key" | "own-key" | "chatgpt-token" | "other" | "none";
interface Seen {
  path: string;
  caller: Caller;
  body: string;
}

const seen: Seen[] = [];
let proxyAnswer: "ok" | "cooldown" = "ok";

function caller(authorization: string | undefined): Caller {
  if (!authorization) return "none";
  if (authorization === `Bearer ${PROXY_KEY}`) return "proxy-key";
  if (authorization === `Bearer ${OWN_KEY}`) return "own-key";
  if (authorization === `Bearer ${CHATGPT_TOKEN}`) return "chatgpt-token";
  return "other";
}

interface OutputItem {
  type: "message";
  role: "assistant";
  id: string;
  content: Array<{ type: "output_text"; text: string }>;
}

interface StreamEvent {
  type: string;
  output_index?: number;
  item?: OutputItem;
  response?: { id: string; status: string; output?: OutputItem[]; usage?: Record<string, number> };
}

function streamAnswer(response: import("node:http").ServerResponse): void {
  const item: OutputItem = {
    type: "message",
    role: "assistant",
    id: "m",
    content: [{ type: "output_text", text: "done" }],
  };
  const send = (event: StreamEvent) =>
    response.write(`event: ${event.type}${NL}data: ${JSON.stringify(event)}${NL}${NL}`);
  response.writeHead(200, { "content-type": "text/event-stream" });
  send({ type: "response.created", response: { id: "r", status: "in_progress" } });
  send({ type: "response.output_item.done", output_index: 0, item });
  send({
    type: "response.completed",
    response: {
      id: "r",
      status: "completed",
      output: [item],
      usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
    },
  });
  response.end();
}

function json(response: import("node:http").ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

const server = createServer((request, response) => {
  let body = "";
  request.on("data", (chunk) => {
    body += chunk;
  });
  request.on("end", () => {
    const path = (request.url ?? "").split("?")[0] ?? "";
    const who = caller(request.headers.authorization);
    seen.push({ path, caller: who, body });
    if (path === "/proxy/v1/responses") {
      if (who !== "proxy-key")
        return json(response, 401, { error: { message: "Invalid API key", code: "invalid_api_key" } });
      if (proxyAnswer === "cooldown")
        return json(response, 429, {
          error: {
            code: "model_cooldown",
            message: "All credentials for model gpt-6-luna are cooling down via provider codex",
            reset_seconds: 3600,
          },
        });
      return streamAnswer(response);
    }
    if (path === "/direct/v1/responses") {
      if (who !== "own-key") return json(response, 401, { error: { message: "Invalid API key" } });
      return streamAnswer(response);
    }
    return json(response, 401, { error: { message: "Unauthorized" } });
  });
});
server.on("upgrade", (request, socket) => {
  seen.push({
    path: `upgrade ${(request.url ?? "").split("?")[0]}`,
    caller: caller(request.headers.authorization),
    body: "",
  });
  socket.end(`HTTP/1.1 426 Upgrade Required\r${NL}Content-Length: 0\r${NL}\r${NL}`);
});
await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
const address = server.address();
assert(address && typeof address === "object");
const base = `http://127.0.0.1:${address.port}`;
process.env.CODEX_REFRESH_TOKEN_URL_OVERRIDE = `${base}/oauth/token`;

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

interface Check {
  name: string;
  passed: boolean;
  details?: unknown;
  error?: string;
}
const report: {
  codexVersion: string;
  checks: Check[];
  passed: boolean;
  error?: string;
  requests?: string[];
  errors?: string[];
} = {
  codexVersion: cli.version,
  checks: [],
  passed: false,
};

async function check(name: string, run: () => Promise<unknown> | unknown): Promise<boolean> {
  try {
    const details = await run();
    report.checks.push({ name, passed: true, ...(details === undefined ? {} : { details }) });
    return true;
  } catch (error) {
    report.checks.push({ name, passed: false, error: error instanceof Error ? error.message : String(error) });
    return false;
  }
}

async function waitFor<T>(label: string, read: () => T | null | undefined | false, timeoutMs = 60_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise((done) => setTimeout(done, 50));
  }
}

const output: string[] = [];
const events: AgentEvent[] = [];
const write = process.stdout.write.bind(process.stdout);
function capture(
  chunk: string | Uint8Array,
  encoding?: BufferEncoding | ((error?: Error | null) => void),
  done?: (error?: Error | null) => void,
): boolean {
  output.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"));
  return typeof encoding === "function" ? write(chunk, encoding) : write(chunk, encoding, done);
}
process.stdout.write = capture;

async function startService() {
  const database = new OpenBotDatabase(userData);
  const store = new AgentStore(userData, home, database);
  const mailbox = new MailboxStore(userData, store.sharedRoot, store.database);
  const providerAccounts = new ProviderAccountStore(
    join(userData, "openbot-provider-accounts-v1.json"),
    join(userData, "provider-accounts"),
  );
  await runCauseEffect(providerAccounts.load());
  const agents = new AgentService({
    store,
    mailbox,
    browser,
    providerAccounts,
    preferredProvider: "codex",
    offProviders: AGENT_PROVIDERS.filter((provider) => provider !== "codex"),
  });
  agents.on("event", (event: AgentEvent) => events.push(event));
  return {
    agents,
    store,
    providerAccounts,
    close: async () => {
      await runCauseEffect(agents.stop()).catch(() => {});
      database.close();
    },
  };
}

function codexRow(agents: InstanceType<typeof AgentService>) {
  return agents.getStatus().providers?.find((row) => row.id === "codex");
}

function done(agents: InstanceType<typeof AgentService>, agentId: string): boolean {
  const deliveries = agents.listQueue(agentId).deliveries;
  return deliveries.length > 0 && deliveries.every((delivery) => delivery.status === "completed");
}

function accountRow(agents: InstanceType<typeof AgentService>, id: string) {
  return codexRow(agents)?.accounts?.find((row) => row.id === id);
}

function rolloutProviders(): string[] {
  const providers: string[] = [];
  const visit = (folder: string) => {
    for (const entry of readdirSync(folder, { withFileTypes: true })) {
      const path = join(folder, entry.name);
      if (entry.isDirectory()) visit(path);
      else if (entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) {
        const first = JSON.parse(readFileSync(path, "utf8").split(NL)[0] ?? "{}");
        providers.push(String(first.payload?.model_provider));
      }
    }
  };
  visit(join(codexHome, "sessions"));
  return providers;
}

function filesHolding(folder: string, needle: string): string[] {
  const found: string[] = [];
  const visit = (current: string) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const path = join(current, entry.name);
      if (entry.isDirectory()) visit(path);
      else if (entry.isFile() && statSync(path).size < 64 * 1024 * 1024 && readFileSync(path).includes(needle))
        found.push(path);
    }
  };
  visit(folder);
  return found;
}

let agentId = "";
let session: string | null = null;
let accountId = "";
let accountHome = "";

try {
  // Phase A: the own sign-in. An API key at a local address stands in for it, so the chat is saved on
  // the `openai` provider as an OpenBot chat on a ChatGPT sign-in is.
  writeFileSync(
    join(codexHome, "auth.json"),
    `${JSON.stringify({ auth_mode: "apikey", OPENAI_API_KEY: OWN_KEY })}${NL}`,
  );
  writeFileSync(join(codexHome, "config.toml"), `openai_base_url = "${base}/direct/v1"${NL}`);
  const first = await startService();
  try {
    await runCauseEffect(first.agents.initialize());
    const agent = await runCauseEffect(
      first.agents.createAgent({
        name: "Proxy",
        description: "Checks the Codex proxy.",
        avatarSeed: "proxy",
        avatarHue: null,
        initialMessage: "marker-alpha-first",
      }),
    );
    agentId = agent.id;
    await waitFor("the first chat turn", () => done(first.agents, agentId));
    session = first.store.activeProviderSession(agentId)?.externalSessionId ?? null;
    assert.ok(session, "The first turn saved a Codex session.");
    const providers = rolloutProviders();
    assert.deepEqual([...new Set(providers)], ["openai"], `The saved chat is on the openai provider: ${providers}`);
    report.checks.push({ name: "an own sign-in chat is saved on the openai provider", passed: true });
    const account = await runCauseEffect(first.providerAccounts.create("codex"));
    assert.ok(account, "The second Codex account is saved.");
    accountId = account.id;
    accountHome = first.providerAccounts.home(account);
  } finally {
    await first.close();
  }

  // The sign-in is now ChatGPT, and the proxy file sends it through the proxy.
  writeFileSync(keyFile, `${PROXY_KEY}${NL}`, { mode: 0o600 });
  for (const folder of [codexHome, accountHome]) {
    writeFileSync(join(folder, "auth.json"), CHATGPT_AUTH, { mode: 0o600 });
    writeFileSync(join(folder, "config.toml"), `chatgpt_base_url = "${base}/backend-api/"${NL}`);
    writeFileSync(
      join(folder, "openbot-proxy.json"),
      `${JSON.stringify({ baseUrl: `${base}/proxy/v1`, apiKeyFile: keyFile })}${NL}`,
    );
  }
  seen.length = 0;

  // Phase B: the proxy.
  const second = await startService();
  try {
    await runCauseEffect(second.agents.initialize());
    await check("the saved chat resumes through the proxy", async () => {
      await runCauseEffect(second.agents.sendMessage({ agentId, text: "marker-beta-second" }));
      await waitFor("the resumed turn", () => done(second.agents, agentId));
      const resumed = second.store.activeProviderSession(agentId)?.externalSessionId;
      assert.equal(resumed, session, "The chat keeps its Codex session.");
      const turn = seen.find(
        (request) => request.path === "/proxy/v1/responses" && request.body.includes("marker-beta-second"),
      );
      assert.ok(turn, "The proxy got the turn.");
      assert.ok(turn.body.includes("marker-alpha-first"), "The turn carries the saved chat.");
      return { session };
    });
    await check("the proxy gets the key, never the ChatGPT token", () => {
      const callers = [...new Set(seen.map((request) => `${request.path} ${request.caller}`))].sort();
      const proxied = seen.filter((request) => request.path.includes("/proxy/"));
      assert.ok(proxied.length > 0, "The proxy got requests.");
      assert.ok(
        proxied.every((request) => request.caller === "proxy-key"),
        `Each proxy request has the key: ${callers.join(", ")}`,
      );
      // Codex still reads the public plugin list from ChatGPT with no sign-in.
      assert.ok(
        seen.every((request) => request.caller === "proxy-key" || request.caller === "none"),
        `No request has the ChatGPT token or another credential: ${callers.join(", ")}`,
      );
      return callers;
    });
    await check("a new chat runs through the proxy", async () => {
      const fresh = await runCauseEffect(
        second.agents.createAgent({
          name: "Fresh",
          description: "Starts a new chat.",
          avatarSeed: "fresh",
          avatarHue: null,
          initialMessage: "marker-gamma-fresh",
        }),
      );
      await waitFor("the new chat turn", () => done(second.agents, fresh.id));
      assert.ok(
        seen.some(
          (request) =>
            request.path === "/proxy/v1/responses" &&
            request.caller === "proxy-key" &&
            request.body.includes("marker-gamma-fresh"),
        ),
        "The proxy got the new chat.",
      );
      assert.deepEqual([...new Set(rolloutProviders())], ["openai"], "Every saved chat is on the openai provider.");
    });
    await check("an account after the first runs through the proxy", async () => {
      const connected = await waitFor("the account check", () => {
        const row = accountRow(second.agents, accountId);
        return row && row.state !== "checking" && row.state !== "connecting" ? row : null;
      });
      assert.equal(
        connected.state,
        "connected",
        `The account is connected: ${connected.state} ${connected.message ?? ""}`,
      );
      const onAccount = await runCauseEffect(
        second.agents.createAgent(
          {
            name: "Work",
            description: "Runs on the second Codex account.",
            avatarSeed: "work",
            avatarHue: null,
            initialMessage: "marker-zeta-account",
          },
          (agent) => second.agents.updateAgent({ agentId: agent.id, providerAccountId: accountId }),
        ),
      );
      await waitFor("the account turn", () => done(second.agents, onAccount.id));
      assert.ok(
        seen.some(
          (request) =>
            request.path === "/proxy/v1/responses" &&
            request.caller === "proxy-key" &&
            request.body.includes("marker-zeta-account"),
        ),
        "The proxy got the account turn.",
      );
      await runCauseEffect(second.agents.getUsage());
      const after = accountRow(second.agents, accountId);
      assert.equal(after?.state, "connected", `The account stays connected: ${after?.state} ${after?.message ?? ""}`);
      return { state: after?.state };
    });
    await check("usage reading gives no ring and no error", async () => {
      const usage = await runCauseEffect(second.agents.getUsage());
      const row = codexRow(second.agents);
      assert.ok(!usage.limits.some((limit) => limit.id === "codex"), "No Codex reading.");
      assert.equal(row?.state, "available", `Codex stays available: ${row?.state} ${row?.message ?? ""}`);
      return { limits: usage.limits.length, state: row?.state };
    });
    await check("a 429 from the proxy holds the agent", async () => {
      proxyAnswer = "cooldown";
      await runCauseEffect(second.agents.sendMessage({ agentId, text: "marker-delta-held" }));
      const held = await waitFor("the usage limit hold", () =>
        second.agents.getRuntimeSnapshot().usageLimits?.find((limit) => limit.agentId === agentId),
      );
      const statuses = second.agents.listQueue(agentId).deliveries.map((delivery) => delivery.status);
      assert.ok(statuses.includes("queued"), `The message waits: ${statuses.join(", ")}`);
      const row = codexRow(second.agents);
      assert.equal(row?.state, "available", `Codex stays available: ${row?.state} ${row?.message ?? ""}`);
      proxyAnswer = "ok";
      await runCauseEffect(second.agents.sendMessage({ agentId, text: "marker-epsilon-after" }));
      await waitFor("the held messages to run", () => done(second.agents, agentId));
      assert.ok(
        !second.agents.getRuntimeSnapshot().usageLimits?.some((limit) => limit.agentId === agentId),
        "A completed turn ends the hold.",
      );
      return { held, statuses };
    });
  } finally {
    await second.close();
  }

  // Phase C: the proxy file is removed, so Codex uses the ChatGPT sign-in again.
  rmSync(join(codexHome, "openbot-proxy.json"));
  rmSync(join(accountHome, "openbot-proxy.json"));
  seen.length = 0;
  const third = await startService();
  try {
    await runCauseEffect(third.agents.initialize());
    await check("without the proxy file Codex uses the ChatGPT sign-in", async () => {
      await runCauseEffect(third.agents.getUsage()).catch(() => null);
      await waitFor("a ChatGPT request", () => seen.some((request) => request.caller === "chatgpt-token"), 30_000);
      assert.ok(!seen.some((request) => request.caller === "proxy-key"), "The key is not sent.");
      return [...new Set(seen.map((request) => `${request.path} ${request.caller}`))].sort();
    });
  } finally {
    await third.close();
  }

  await check("auth.json keeps the ChatGPT sign-in", () => {
    assert.equal(readFileSync(join(codexHome, "auth.json"), "utf8"), CHATGPT_AUTH, "The own sign-in is unchanged.");
    assert.equal(readFileSync(join(accountHome, "auth.json"), "utf8"), CHATGPT_AUTH, "The account is unchanged.");
  });
  await check("the key is in no log, status, event or profile file", () => {
    const logged = output.join("");
    assert.ok(!logged.includes(PROXY_KEY), "No log line holds the key.");
    assert.ok(!JSON.stringify(events).includes(PROXY_KEY), "No event holds the key.");
    assert.deepEqual(filesHolding(userData, PROXY_KEY), [], "No profile file holds the key.");
    assert.deepEqual(
      [...filesHolding(codexHome, PROXY_KEY), ...filesHolding(accountHome, PROXY_KEY)],
      [],
      "No file in the Codex home holds the key, the saved chats included.",
    );
  });
  report.passed = report.checks.every((item) => item.passed);
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  report.requests = seen.map((request) => `${request.path} ${request.caller}`);
  report.errors = events.filter((event) => event.type === "error").map((event) => JSON.stringify(event).slice(0, 400));
} finally {
  process.stdout.write = write;
  server.closeAllConnections();
  server.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}${NL}`);
  await rm(root, { recursive: true, force: true });
  for (const item of report.checks) process.stdout.write(`${item.passed ? "ok  " : "FAIL"} ${item.name}${NL}`);
  process.stdout.write(`${report.passed ? "PASS" : "FAIL"} ${join(OUT, "report.json")}${NL}`);
  if (report.error) process.stdout.write(`${report.error}${NL}`);
  process.exitCode = report.passed ? 0 : 1;
}
