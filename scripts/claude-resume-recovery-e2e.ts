import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { AGENT_PROVIDERS, defaultProviderModel } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import type { AgentClient, DiagnosticOrigin } from "../src/backend/agent-client";
import type { ClaudeCliInfo } from "../src/backend/cli";
import type { AppServerNotification, AppServerRequest, RequestId, ResponseDecoder } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import type { ReadProviderHistory } from "../src/backend/provider-history";

const OUT = resolve(import.meta.dirname, "../.openbot-build/claude-resume-recovery-e2e");
const root = await mkdtemp(join(tmpdir(), "ob-resume-"));
const home = join(root, "home");
const vanishPath = join(root, "vanish.json");
const protocolPath = join(root, "protocol.jsonl");
const claudeExecutable = join(root, "claude");
const CLAUDE_MODEL = defaultProviderModel("claude");
process.env.HOME = home;
process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
process.env.OPENBOT_CLAUDE_PATH = claudeExecutable;
mkdirSync(home, { recursive: true });
writeFileSync(vanishPath, "[]");
const { AgentService } = await import("../src/backend/agent-service");
const { AgentStore } = await import("../src/backend/agent-store");
const { ClaudeAgentClient } = await import("../src/backend/claude-client");
const { runCauseEffect } = await import("../src/backend/effect-boundary");
const { MailboxStore } = await import("../src/backend/mailbox-store");
const { OpenBotDatabase } = await import("../src/backend/openbot-database");

const STAND_IN_CLAUDE = `#!${process.execPath}
const fs = require("node:fs");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const args = process.argv.slice(2);
if (args.includes("--version")) { process.stdout.write("2.1.318 (Claude Code)\\n"); process.exit(0); }
const option = (name) => {
 const index = args.findIndex((arg) => arg === name || arg.startsWith(name + "="));
 return index < 0 ? null : args[index] === name ? args[index + 1] : args[index].slice(name.length + 1);
};
const resume = option("--resume");
const sessionId = resume ?? option("--session-id") ?? randomUUID();
const project=path.join(process.env.CLAUDE_CONFIG_DIR,"projects",process.cwd().replace(/[^a-zA-Z0-9]/g,"-"));
fs.mkdirSync(project,{recursive:true});
const transcript=path.join(project,sessionId+".jsonl");
if(resume && JSON.parse(fs.readFileSync(${JSON.stringify(vanishPath)},"utf8")).includes(resume) && fs.existsSync(transcript))fs.unlinkSync(transcript);
const log = (row) => fs.appendFileSync(${JSON.stringify(protocolPath)}, JSON.stringify({sessionId, pid:process.pid, ...row}) + "\\n");
log({phase:"spawn",resume,session:option("--session-id")});
const write = (row) => process.stdout.write(JSON.stringify(row) + "\\n");
const result = (fields) => write({type:"result",subtype:"success",session_id:sessionId,uuid:randomUUID(),duration_ms:1,duration_api_ms:1,num_turns:1,total_cost_usd:0,usage:{input_tokens:1,output_tokens:1},permission_denials:[],modelUsage:{},...fields});
const assistant = (text) => write({type:"assistant",message:{id:"msg_"+randomUUID(),type:"message",role:"assistant",model:${JSON.stringify(CLAUDE_MODEL)},content:[{type:"text",text}],stop_reason:null,usage:{input_tokens:1,output_tokens:1}},parent_tool_use_id:null,session_id:sessionId,uuid:randomUUID()});
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => {
 buffer += chunk;
 for (let newline; (newline=buffer.indexOf("\\n"))>=0;) {
  const line=buffer.slice(0,newline).trim();buffer=buffer.slice(newline+1);if(!line)continue;
  const message=JSON.parse(line);
  if(message.type === "control_request") {
   write({type:"control_response",response:{subtype:"success",request_id:message.request_id,response:{}}});
  } else if(message.type === "user") {
   const text=JSON.stringify(message.message.content);
   log({phase:"input",text,resume,exists:fs.existsSync(transcript)});
   const channelMissing = text.includes("CHANNEL-MISSING-ONCE") && !fs.readFileSync(${JSON.stringify(protocolPath)},"utf8").trim().split("\\n").filter(Boolean).map(line=>JSON.parse(line)).some(row=>row.phase==="missing" && row.text?.includes("CHANNEL-MISSING-ONCE"));
   if((resume && !fs.existsSync(transcript)) || channelMissing || text.includes("MISSING-FOREVER") || text.includes("AFTER-WORK-ERROR")) {
    if(text.includes("AFTER-WORK-ERROR")) assistant("Visible work before the error.");
    const error="No conversation found with session ID: " + sessionId;
    result({subtype:"error_during_execution",is_error:true,errors:[error]});log({phase:"missing",text});setTimeout(()=>process.exit(1),10);
   } else if(text.includes("MODEL-REFUSAL")) {
    result({subtype:"error_during_execution",is_error:true,errors:["The requested model refused this turn."]});log({phase:"refusal"});setTimeout(()=>process.exit(1),10);
   } else {
    fs.appendFileSync(transcript,JSON.stringify(message)+"\\n");
    const complete=()=>{assistant("Done.");result({is_error:false,result:"Done."});log({phase:"completed",text});};
    if(text.includes("SLOW-PEER"))setTimeout(complete,750);else complete();
   }
  }
 }
});
process.stdin.on("end",()=>process.exit(0));
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
  exitCount = 0;
  readonly readHistory: ReadProviderHistory;

  constructor(cli: ClaudeCliInfo) {
    super();
    this.#real = new ClaudeAgentClient(cli);
    this.readHistory = this.#real.readHistory;
    this.#real.on("notification", (notification) => this.emit("notification", notification));
    this.#real.on("request", (request) => this.emit("request", request));
    this.#real.on("diagnostic", (message) => this.emit("diagnostic", message));
    this.#real.on("exit", (error) => {
      this.exitCount++;
      this.emit("exit", error);
    });
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

function transcriptPath(workspace: string, sessionId: string) {
  return join(home, ".claude", "projects", workspace.replace(/[^a-zA-Z0-9]/g, "-"), `${sessionId}.jsonl`);
}
function rows(): { sessionId: string; phase: string; text?: string; resume?: string | null; exists?: boolean }[] {
  if (!existsSync(protocolPath)) return [];
  return readFileSync(protocolPath, "utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}
async function waitFor<T>(label: string, read: () => T | null | undefined | false, timeoutMs = 7000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise((done) => setTimeout(done, 20));
  }
}
const checks: { name: string; passed: boolean; detail?: unknown }[] = [];
async function check(name: string, run: () => Promise<unknown>) {
  try {
    checks.push({ name, passed: true, detail: await run() });
  } catch (error) {
    checks.push({ name, passed: false, detail: error instanceof Error ? error.stack : String(error) });
  }
}
const resources: { service: InstanceType<typeof AgentService>; database: InstanceType<typeof OpenBotDatabase> }[] = [];
let index = 0;
async function openFixture(userData = join(root, `data-${++index}`)) {
  const database = new OpenBotDatabase(userData);
  const store = new AgentStore(userData, home, database);
  const mailbox = new MailboxStore(userData, store.sharedRoot, database);
  const clients: ClaudeStandIn[] = [];
  const service = new AgentService({
    store,
    mailbox,
    browser,
    preferredProvider: "claude",
    offProviders: AGENT_PROVIDERS.filter((one) => one !== "claude"),
    clientFactory: (_, cli) => {
      const client = new ClaudeStandIn(cli);
      clients.push(client);
      return client;
    },
  });
  resources.push({ service, database });
  await runCauseEffect(service.initialize());
  const create = async (name: string) => {
    const agent = await runCauseEffect(
      service.createAgent({
        name,
        description: "Checks missing Claude sessions.",
        avatarSeed: name.toLowerCase().replaceAll(" ", "-"),
        avatarHue: null,
        initialMessage: "First durable message.",
        provider: "claude",
        model: CLAUDE_MODEL,
      }),
    );
    await waitFor("initial completion", () =>
      service.listQueue(agent.id).deliveries.every((one) => one.status === "completed"),
    );
    const current = store.list().find((one) => one.id === agent.id);
    assert.ok(current?.threadId);
    return current;
  };
  const send = async (agentId: string, text: string) => {
    const receipt = await runCauseEffect(service.sendMessage({ agentId, text }));
    const id = receipt.deliveries[0]?.id;
    assert.ok(id);
    return id;
  };
  const end = (id: string) =>
    waitFor("delivery settlement", () => {
      const row = mailbox.getDelivery(id)?.delivery;
      return row && ["completed", "failed", "interrupted"].includes(row.status) ? row : null;
    });
  const close = async () => {
    await runCauseEffect(service.stop());
    database.close();
  };
  return {
    userData,
    database,
    store,
    mailbox,
    service,
    create,
    send,
    end,
    close,
    clients,
    exitCount: () => clients.reduce((total, client) => total + client.exitCount, 0),
  };
}
await writeFile(claudeExecutable, STAND_IN_CLAUDE, { mode: 0o755 });
try {
  await check(
    "a missing stored resume retries once on a fresh persisted session with the bounded handoff",
    async () => {
      const f = await openFixture();
      const agent = await f.create("Missing resume");
      assert.ok(agent.threadId);
      const missing = randomUUID();
      const transcript = transcriptPath(agent.workspacePath, missing);
      mkdirSync(join(transcript, ".."), { recursive: true });
      writeFileSync(
        transcript,
        `${JSON.stringify({
          type: "user",
          sessionId: missing,
          uuid: randomUUID(),
          message: { role: "user", content: "Earlier saved input." },
        })}\n`,
      );
      writeFileSync(vanishPath, JSON.stringify([missing]));
      f.database.bindProviderSession({
        threadId: agent.threadId,
        provider: "claude",
        externalSessionId: missing,
        model: CLAUDE_MODEL,
        effort: agent.reasoningEffort,
      });
      const id = await f.send(agent.id, "Recover this one delivery.");
      const ended = await f.end(id);
      assert.equal(ended.status, "completed");
      const sessions = f.database.listProviderSessions(agent.threadId);
      const active = f.database.activeProviderSession(agent.threadId, "claude");
      assert.ok(active);
      assert.notEqual(active.externalSessionId, missing);
      assert.equal(sessions.find((one) => one.externalSessionId === missing)?.state, "inactive");
      const input =
        rows().find((one) => one.phase === "input" && one.sessionId === active.externalSessionId)?.text ?? "";
      assert.ok(input.includes("First durable message."));
      assert.ok(input.includes("Recover this one delivery."));
      assert.ok(input.length < 40000);
      assert.equal(rows().filter((one) => one.phase === "missing" && one.sessionId === missing).length, 1);
      return { deliveryId: id, oldSession: missing, newSession: active.externalSessionId, handoffLength: input.length };
    },
  );
  await check("a missing side-thread resume replaces only that task session", async () => {
    const f = await openFixture();
    const agent = await f.create("Side resume");
    assert.ok(agent.threadId);
    const mainSession = f.database.activeProviderSession(agent.threadId, "claude")?.externalSessionId;
    assert.ok(mainSession);
    const task = await runCauseEffect(f.service.startTask({ agentId: agent.id, text: "First durable task input." }));
    await waitFor("task completion", () =>
      f.service.readTask(agent.id, task.id).messages.some((message) => message.text === "Done."),
    );
    const missing = randomUUID();
    const transcript = transcriptPath(agent.workspacePath, missing);
    mkdirSync(join(transcript, ".."), { recursive: true });
    writeFileSync(
      transcript,
      `${JSON.stringify({ type: "user", sessionId: missing, uuid: randomUUID(), message: { role: "user", content: "Earlier task input." } })}\n`,
    );
    writeFileSync(vanishPath, JSON.stringify([missing]));
    f.database.bindProviderSession({
      threadId: task.threadId,
      provider: "claude",
      externalSessionId: missing,
      model: CLAUDE_MODEL,
      effort: agent.reasoningEffort,
    });
    const receipt = await runCauseEffect(
      f.service.sendMessage({ agentId: agent.id, taskId: task.id, text: "Recover this task delivery." }),
    );
    const id = receipt.deliveries[0]?.id;
    assert.ok(id);
    assert.equal((await f.end(id)).status, "completed");
    const active = f.database.activeProviderSession(task.threadId, "claude");
    assert.ok(active);
    assert.notEqual(active.externalSessionId, missing);
    assert.equal(
      f.database.listProviderSessions(task.threadId).find((session) => session.externalSessionId === missing)?.state,
      "inactive",
    );
    assert.equal(f.database.activeProviderSession(agent.threadId, "claude")?.externalSessionId, mainSession);
    assert.equal(f.service.readTask(agent.id, task.id).activeTurnId, null);
    return { taskThread: task.threadId, oldSession: missing, newSession: active.externalSessionId, mainSession };
  });
  await check(
    "a rotated but unwritten Claude session survives a cold service restart without resuming an absent transcript",
    async () => {
      let f = await openFixture();
      const agent = await f.create("Virgin rotation");
      assert.ok(agent.threadId);
      const publicThread = agent.threadId;
      const old = f.database.activeProviderSession(publicThread, "claude")?.externalSessionId;
      assert.ok(old);
      const rotation = await runCauseEffect(f.service.controlAgentContext(agent.id, "rotate"));
      assert.equal(rotation.kind, "accepted");
      const virgin = await waitFor("rotation binding", () => {
        const id = f.database.activeProviderSession(publicThread, "claude")?.externalSessionId;
        return id && id !== old ? id : null;
      });
      assert.equal(existsSync(transcriptPath(agent.workspacePath, virgin)), false);
      await f.close();
      f = await openFixture(f.userData);
      const boundary = rows().length;
      const delivery = await f.send(agent.id, "First turn after rotated restart.");
      assert.equal((await f.end(delivery)).status, "completed");
      assert.equal(f.store.list().find((one) => one.id === agent.id)?.threadId, agent.threadId);
      assert.equal(
        rows()
          .slice(boundary)
          .some((one) => one.phase === "missing" && one.sessionId === virgin),
        false,
      );
      return {
        oldSession: old,
        rotatedSession: virgin,
        active: f.database.activeProviderSession(agent.threadId, "claude")?.externalSessionId,
      };
    },
  );
  await check("a fresh session that is also refused stops after the one delivery retry", async () => {
    const f = await openFixture();
    const agent = await f.create("Retry ceiling");
    const boundary = rows().length;
    const delivery = await f.send(agent.id, "MISSING-FOREVER");
    assert.equal((await f.end(delivery)).status, "failed");
    const attempts = rows()
      .slice(boundary)
      .filter((one) => one.phase === "input" && one.text?.includes("MISSING-FOREVER"));
    assert.equal(attempts.length, 2);
    assert.notEqual(attempts[0]?.sessionId, attempts[1]?.sessionId);
    await waitFor("native quiet", () => f.service.nativeWorkSnapshot().idle);
    assert.equal(
      rows()
        .slice(boundary)
        .filter((one) => one.phase === "input" && one.text?.includes("MISSING-FOREVER")).length,
      2,
    );
    return { attempts: attempts.length };
  });
  await check("a missing-session error after visible work does not replay the delivery", async () => {
    const f = await openFixture();
    const agent = await f.create("No work replay");
    const boundary = rows().length;
    const delivery = await f.send(agent.id, "AFTER-WORK-ERROR");
    assert.equal((await f.end(delivery)).status, "failed");
    assert.equal(
      rows()
        .slice(boundary)
        .filter((one) => one.phase === "input" && one.text?.includes("AFTER-WORK-ERROR")).length,
      1,
    );
  });
  await check("a model refusal does not stop a peer Claude turn or the shared provider", async () => {
    const f = await openFixture();
    const bad = await f.create("Refusing agent");
    const peer = await f.create("Working peer");
    const slow = await f.send(peer.id, "SLOW-PEER");
    await waitFor("peer running", () => f.mailbox.getDelivery(slow)?.delivery.status === "running");
    const refused = await f.send(bad.id, "MODEL-REFUSAL");
    assert.equal((await f.end(refused)).status, "failed");
    assert.equal((await f.end(slow)).status, "completed");
    assert.equal((await f.end(await f.send(peer.id, "A later healthy turn."))).status, "completed");
    assert.equal(f.exitCount(), 0);
  });
  await check("a missing Claude session retries the same native channel assignment once", async () => {
    const f = await openFixture();
    const agent = await f.create("Channel resume");
    assert.ok(agent.threadId);
    const mainSession = f.database.activeProviderSession(agent.threadId, "claude")?.externalSessionId;
    const channelId = randomUUID();
    const person = { id: "local", name: "Fixture person" };
    await runCauseEffect(
      f.service.channels.command(
        {
          type: "save",
          operationId: randomUUID(),
          channelId,
          draft: {
            name: "Resume recovery",
            title: "",
            instructions: "Answer the exact request.",
            members: [{ agentId: agent.id }],
            leadAgentId: agent.id,
          },
        },
        person,
      ),
    );
    const boundary = rows().length;
    await runCauseEffect(
      f.service.channels.command(
        {
          type: "send",
          operationId: randomUUID(),
          channelId,
          text: "CHANNEL-MISSING-ONCE",
          recipientAgentId: agent.id,
          replyToMessageId: null,
          attachmentDraftIds: [],
        },
        person,
      ),
    );
    const assignment = await waitFor("native channel assignment", () =>
      f.service.channels.store.assignments(channelId).find((one) => one.deliveryId),
    );
    const id = assignment.deliveryId;
    assert.ok(id);
    const ended = await f.end(id);
    assert.equal(ended.status, "completed", ended.error ?? undefined);
    await waitFor(
      "channel assignment completion",
      () =>
        f.service.channels.store.assignments(channelId).find((one) => one.id === assignment.id)?.state === "completed",
    );
    assert.equal(f.service.channels.store.assignments(channelId).length, 1);
    const attempts = rows()
      .slice(boundary)
      .filter((one) => one.phase === "input" && one.text?.includes("CHANNEL-MISSING-ONCE"));
    assert.equal(attempts.length, 2);
    assert.notEqual(attempts[0]?.sessionId, attempts[1]?.sessionId);
    assert.equal(f.database.activeProviderSession(agent.threadId, "claude")?.externalSessionId, mainSession);
    return { deliveryId: id, assignmentId: assignment.id, attempts: attempts.length };
  });
  await check("bulk repair releases an idle native binding and wakes work queued during its start hold", async () => {
    const f = await openFixture();
    const agent = await f.create("Repair native binding");
    assert.ok(agent.threadId);
    const old = f.database.activeProviderSession(agent.threadId, "claude");
    assert.ok(old);
    for (const project of readdirSync(join(home, ".claude", "projects")))
      await rm(join(home, ".claude", "projects", project, `${old.externalSessionId}.jsonl`), { force: true });
    await waitFor("native idle before repair", () => f.service.nativeWorkSnapshot().idle);
    const client = f.clients[0];
    assert.ok(client);
    const release = client.releaseThread.bind(client);
    let releaseReached: () => void = () => {};
    const reached = new Promise<void>((done) => {
      releaseReached = done;
    });
    let continueRelease: () => void = () => {};
    const held = new Promise<void>((done) => {
      continueRelease = done;
    });
    client.releaseThread = (id) =>
      id === old.externalSessionId
        ? Effect.gen(function* () {
            releaseReached();
            yield* Effect.promise(() => held);
            yield* release(id);
          })
        : release(id);
    const boundary = rows().length;
    const repair = runCauseEffect(f.service.repairClaudeProviderSessions(true, "Host repair of missing transcript"));
    let queued: string | undefined;
    try {
      const early = await Promise.race([reached.then(() => null), repair]);
      assert.equal(early, null, JSON.stringify(early));
      assert.equal((await runCauseEffect(f.service.controlAgentContext(agent.id, "rotate"))).kind, "busy");
      await assert.rejects(runCauseEffect(f.service.refreshProviders({ automatic: false })));
      queued = await f.send(agent.id, "Queued during session repair.");
      assert.equal(f.mailbox.getDelivery(queued)?.delivery.status, "queued");
      assert.equal(rows().length, boundary);
    } finally {
      continueRelease();
    }
    const report = await repair;
    assert.equal(report.count, 1);
    assert.equal(report.repaired[0]?.id, old.id);
    assert.ok(queued);
    assert.equal((await f.end(queued)).status, "completed");
    const active = f.database.activeProviderSession(agent.threadId, "claude");
    assert.ok(active);
    assert.notEqual(active.externalSessionId, old.externalSessionId);
    assert.equal(f.exitCount(), 0);
    return { repaired: old.id, nextSession: active.externalSessionId, queuedDelivery: queued };
  });
} finally {
  for (const resource of resources) {
    await runCauseEffect(resource.service.stop()).catch(() => {});
    try {
      resource.database.close();
    } catch {}
  }
  mkdirSync(OUT, { recursive: true });
  const report = { passed: checks.every((one) => one.passed), total: checks.length, checks };
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  for (const one of checks)
    process.stdout.write(
      `${one.passed ? "PASS" : "FAIL"} ${one.name} ${one.passed ? "" : JSON.stringify(one.detail)}\n`,
    );
  await rm(root, { recursive: true, force: true });
  if (!report.passed) process.exitCode = 1;
}
