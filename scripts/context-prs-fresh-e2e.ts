import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  type ContextPullRequest,
  type ContextPullRequestScope,
  isContextPullRequestsSnapshot,
} from "@openbot/contracts/context-prs";
import { AGENT_PROVIDERS, DISCONNECTED_GITHUB_CONNECTOR, type GitHubConnectorStatus } from "@openbot/contracts/ipc";
import { CONTEXT_PRS_CODECS, CONTEXT_PRS_ROUTES } from "@openbot/contracts/team-protocol/context-prs-v1";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { AgentService } from "../src/backend/agent-service";
import { AgentStore } from "../src/backend/agent-store";
import { ContextPrReferences } from "../src/backend/context-pr-references";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import type { PrIdentity } from "../src/main/context-pr-reader";
import { ContextPrService } from "../src/main/context-pr-service";

const output = resolve(import.meta.dirname, "../.openbot-build/context-prs-fresh-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-context-prs-fresh-"));
const report: { passed: boolean; checks: string[]; error: string } = { passed: false, checks: [], error: "" };
async function check(name: string, run: () => void | Promise<void>) {
  await run();
  report.checks.push(name);
  process.stdout.write(`PASS ${name}\n`);
}
const database = new OpenBotDatabase(join(root, "state"));
const store = new AgentStore(join(root, "state"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "state"), store.sharedRoot, database);
const unused = () => Effect.die(new Error("No model or browser calls are permitted."));
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
const agents = new AgentService({ store, mailbox, browser, offProviders: AGENT_PROVIDERS });
class CountingReferences extends ContextPrReferences {
  fullScans = 0;
  override read(scope: ContextPullRequestScope, includeWatches?: boolean) {
    this.fullScans++;
    return super.read(scope, includeWatches);
  }
}
const references = new CountingReferences(database, () => null);
const calls: number[] = [];
const titles = new Map<number, string>();
let active = 0,
  maximum = 0;
let gate: Promise<void> | null = null;
const merged = new Set([20]);
const connection: GitHubConnectorStatus = {
  ...DISCONNECTED_GITHUB_CONNECTOR,
  available: true,
  state: "connected",
  login: "synthetic",
};
let clock = Date.parse("2026-11-05T12:00:00.000Z");
const row = (identity: PrIdentity): ContextPullRequest => ({
  url: identity.url,
  repo: identity.repo,
  number: identity.number,
  title: titles.get(identity.number) ?? `Change ${identity.number}`,
  state: merged.has(identity.number) ? "merged" : "open",
  draft: false,
  mergedAt: merged.has(identity.number) ? "2026-11-01T12:00:00.000Z" : null,
  headSha: "c".repeat(40),
  checks: "passed",
  checkedAt: new Date().toISOString(),
});
const prs = new ContextPrService(
  references,
  {
    status: () => connection,
    readContextPullRequest: (identity) =>
      Effect.promise(async () => {
        calls.push(identity.number);
        active++;
        maximum = Math.max(maximum, active);
        if (gate) await gate;
        await new Promise((resolve) => setTimeout(resolve, 5));
        active--;
        return row(identity);
      }),
  },
  undefined,
  undefined,
  () => clock,
);
const pr = (number: number) => `https://github.com/example/repo/pull/${number}`;
try {
  await runCauseEffect(store.initialize());
  await runCauseEffect(mailbox.initialize());
  const create = (name: string) =>
    runCauseEffect(
      store.createAgent({ name, description: "Synthetic", avatarSeed: name.toLowerCase(), avatarHue: null }),
    );
  const owner = await create("Owner"),
    other = await create("Other"),
    idle = await create("Idle");
  const scope = { kind: "agent" as const, id: owner.id };
  let messageTime = Date.parse("2026-11-05T11:00:00.000Z");
  const insertMessage = (text: string, agent = owner, status = "completed") => {
    const createdAt = new Date(++messageTime).toISOString();
    const id = `message-${messageTime}`;
    database.connection
      .prepare(
        "INSERT INTO projection_thread_messages(thread_id,message_id,author,status,created_at,ordinal,message_json,last_event_sequence) VALUES(?,?,'assistant',?,?,0,?,0)",
      )
      .run(
        store.ensureThreadIdNow(agent.id),
        id,
        status,
        createdAt,
        JSON.stringify({ id, text, author: "assistant", status, createdAt }),
      );
    return id;
  };
  insertMessage(`Work ${pr(1)} and ${pr(2)} and merged ${pr(20)}`);
  insertMessage(`Other ${pr(3)}`, other);
  insertMessage(`Idle ${pr(30)}`, idle);
  await check("The first read builds a chat's index once and later reads append only new rows", async () => {
    const first = await runCauseEffect(prs.read({ scope }));
    assert.equal(references.fullScans, 1);
    assert.deepEqual(
      first.open.map((row) => row.number),
      [1, 2],
    );
    assert.deepEqual(
      first.merged.map((row) => row.number),
      [20],
    );
    assert.equal(first.stale, false);
    assert.equal(first.refreshedAt, first.checkedAt);
    insertMessage(`Later ${pr(4)}`);
    assert.ok(references.index(scope).urls.includes(pr(4)));
    await runCauseEffect(prs.read({ scope }));
    await prs.whenIdle();
    assert.equal(references.fullScans, 1);
  });
  await check("A streaming message is read again when it completes", () => {
    const id = insertMessage("Typing", owner, "streaming");
    assert.ok(!references.index(scope).urls.includes(pr(5)));
    database.connection
      .prepare(
        "UPDATE projection_thread_messages SET status='completed',message_json=json_set(message_json,'$.text',?) WHERE message_id=?",
      )
      .run(`Done ${pr(5)}`, id);
    assert.ok(references.index(scope).urls.includes(pr(5)));
    assert.equal(references.fullScans, 1);
  });
  await check("Channel messages and changed channel tasks update the channel index", () => {
    const channel = agents.channels.store.create("fresh-channel", {
      name: "Group",
      title: "Group",
      instructions: "",
      members: [{ agentId: owner.id }, { agentId: other.id }],
      leadAgentId: owner.id,
    });
    const task = {
      id: "fresh-task",
      channelId: channel.id,
      parentTaskId: null,
      rootTaskId: "fresh-task",
      ownerAgentId: owner.id,
      requestMessageId: "request",
      instruction: `Review ${pr(40)}`,
      attachmentDraftIds: [],
      expectedResult: "Done",
      sourceMessageIds: [],
      dependencies: [],
      resources: [],
      state: "queued" as const,
      revision: 1,
      assignmentCount: 0,
      error: null,
    };
    agents.channels.store.update(channel, { tasks: [task], assignments: [] });
    const channelScope = { kind: "channel" as const, id: channel.id };
    assert.deepEqual(references.index(channelScope).urls, [pr(40)]);
    agents.channels.store.update(channel, {
      tasks: [{ ...task, revision: 2, expectedResult: `Merged ${pr(41)}` }],
      assignments: [],
    });
    database.connection
      .prepare("INSERT INTO projection_channel_messages(channel_id,message_id,sequence,message_json) VALUES(?,?,9,?)")
      .run(
        channel.id,
        "fresh-message",
        JSON.stringify({ id: "fresh-message", message: { text: `See ${pr(42)}`, status: "completed" } }),
      );
    assert.deepEqual(references.index(channelScope).urls.sort(), [pr(40), pr(41), pr(42)]);
    assert.equal(references.fullScans, 2);
  });
  await check("A cached snapshot returns at once and marks itself stale while it refreshes", async () => {
    const before = calls.length;
    let release = () => {};
    gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const started = performance.now();
    const cached = await runCauseEffect(prs.read({ scope }));
    assert.ok(performance.now() - started < 1000);
    assert.equal(cached.stale, true);
    assert.ok(cached.refreshedAt);
    assert.ok(!cached.open.some((row) => row.number === 5));
    await new Promise((resolve) => setTimeout(resolve, 20));
    const during = await runCauseEffect(prs.read({ scope }));
    assert.equal(during.stale, true);
    assert.ok(!during.open.some((row) => row.number === 5));
    assert.deepEqual(calls.slice(before), [5]);
    release();
    gate = null;
    await prs.whenIdle();
    const fresh = await runCauseEffect(prs.read({ scope }));
    assert.equal(fresh.stale, false);
    assert.deepEqual(
      fresh.open.map((row) => row.number),
      [1, 2, 4, 5],
    );
    assert.ok(!calls.slice(before).includes(20));
    assert.ok(!calls.slice(before).includes(1));
    assert.ok(isContextPullRequestsSnapshot(fresh));
  });
  await check("A change to a chat that was read refreshes it in the background without a read", async () => {
    insertMessage(`Background ${pr(6)}`);
    const before = calls.length;
    prs.noteChanged(scope);
    prs.flushChanges();
    await prs.whenIdle();
    assert.deepEqual(calls.slice(before), [6]);
    const after = calls.length;
    const result = await runCauseEffect(prs.read({ scope }));
    assert.equal(calls.length, after);
    assert.equal(result.stale, false);
    assert.ok(result.open.some((row) => row.number === 6));
    insertMessage(`Unread ${pr(31)}`, idle);
    prs.noteChanged({ kind: "agent", id: idle.id });
    prs.flushChanges();
    await prs.whenIdle();
    assert.equal(calls.length, after);
  });
  await check("Open PRs of chats read in the last 24 hours refresh every 5 minutes", async () => {
    const otherScope = { kind: "agent" as const, id: other.id };
    await runCauseEffect(prs.read({ scope: otherScope }));
    clock += 25 * 3_600_000;
    await runCauseEffect(prs.read({ scope }));
    await prs.whenIdle();
    titles.set(1, "Updated title");
    clock += 300_000;
    const before = calls.length;
    prs.refreshRecent();
    await prs.whenIdle();
    const refreshed = calls.slice(before).sort((a, b) => a - b);
    assert.deepEqual(refreshed, [1, 2, 4, 5, 6]);
    const result = await runCauseEffect(prs.read({ scope }));
    assert.equal(result.stale, false);
    assert.equal(result.open[0]?.title, "Updated title");
    clock += 60_000;
    const quiet = calls.length;
    prs.refreshRecent();
    await prs.whenIdle();
    assert.equal(calls.length, quiet);
  });
  await check("Explicit refresh bypasses the snapshot and the PR cache", async () => {
    const before = calls.length;
    const result = await runCauseEffect(prs.read({ scope, refresh: true }));
    assert.deepEqual(
      calls.slice(before).sort((a, b) => a - b),
      [1, 2, 4, 5, 6, 20],
    );
    assert.equal(result.stale, false);
  });
  await check("Background and explicit reads share three request permits", async () => {
    insertMessage([7, 8, 9, 10, 11, 12].map(pr).join(" "));
    maximum = 0;
    let release = () => {};
    gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    prs.noteChanged(scope);
    prs.flushChanges();
    const explicit = runCauseEffect(prs.read({ scope, refresh: true }));
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(maximum, 3);
    release();
    gate = null;
    await explicit;
    await prs.whenIdle();
    assert.ok(maximum <= 3);
  });
  await check("The wire codec keeps stale and refreshedAt", async () => {
    clock += 300_000;
    const result = await runCauseEffect(prs.read({ scope }));
    assert.equal(result.stale, true);
    const codec = CONTEXT_PRS_CODECS.get(CONTEXT_PRS_ROUTES.read);
    assert.ok(codec);
    assert.deepEqual(codec.response(200, result), result);
    await prs.whenIdle();
  });
  await check("Authentication changes drop cached snapshots", async () => {
    prs.invalidate();
    const before = calls.length;
    const result = await runCauseEffect(prs.read({ scope }));
    assert.equal(result.stale, false);
    assert.ok(calls.length > before);
  });
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  try {
    prs.stop();
  } catch {}
  await runCauseEffect(agents.stop()).catch(() => {});
  database.close();
  mkdirSync(output, { recursive: true });
  writeFileSync(join(output, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
  if (report.error) process.stderr.write(`${report.error}\n`);
}
