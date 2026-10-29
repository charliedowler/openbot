import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { isContextPullRequestsSnapshot, parseContextPullRequestsInput } from "@openbot/contracts/context-prs";
import { AGENT_PROVIDERS, DISCONNECTED_GITHUB_CONNECTOR, type GitHubConnectorStatus } from "@openbot/contracts/ipc";
import { CONTEXT_PRS_CODECS, CONTEXT_PRS_ROUTES } from "@openbot/contracts/team-protocol/context-prs-v1";
import type { WatchingSnapshot } from "@openbot/contracts/watch-registry";
import { registerSecretValue } from "@openbot/logging";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { AgentService } from "../src/backend/agent-service";
import { AgentStore } from "../src/backend/agent-store";
import { ContextPrReferences } from "../src/backend/context-pr-references";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { AgentAccessStore } from "../src/main/agent-access-store";
import { contextPrCliWithEnvironment } from "../src/main/context-pr-cli";
import { decodeContextPullRequestsFromHost } from "../src/main/context-pr-decoding";
import { contextPrChecks, readCliContextPr, readConnectedContextPr } from "../src/main/context-pr-reader";
import { ContextPrService } from "../src/main/context-pr-service";
import { TeamApiServer } from "../src/main/team-api-server";
import { TeamStore } from "../src/main/team-store";
import { decodeContextPullRequestsFromMain } from "../src/preload/context-pr-decoding";

const output = resolve(import.meta.dirname, "../.openbot-build/context-prs-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-context-prs-"));
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
};
let registry: WatchingSnapshot["registry"] = null;
const agents = new AgentService({
  store,
  mailbox,
  browser,
  offProviders: AGENT_PROVIDERS,
  watchRegistry: () => registry,
});
const requests: string[] = [];
let payloadMode = "normal";
let httpStatus = 200,
  active = 0,
  maximum = 0;
let gate: Promise<void> | null = null,
  arrived: (() => void) | null = null,
  arrivals = 0;
const fixture = createServer(async (request, response) => {
  requests.push(`${request.method} ${request.url}`);
  assert.equal(request.method, "GET");
  assert.equal(request.headers.authorization, "Bearer synthetic-only");
  active++;
  maximum = Math.max(maximum, active);
  if (request.url?.includes("/pulls/") && gate) {
    arrivals++;
    if (arrivals >= 3) arrived?.();
    await gate;
  }
  response.setHeader("content-type", "application/json");
  response.statusCode = httpStatus;
  if (httpStatus !== 200) response.end(JSON.stringify({ message: "synthetic-provider-secret" }));
  else if (request.url?.includes("/check-runs"))
    response.end(JSON.stringify({ total_count: 1, check_runs: [{ conclusion: "success", status: "completed" }] }));
  else if (request.url?.includes("/status")) response.end(JSON.stringify({ total_count: 0, statuses: [] }));
  else {
    const number = Number(request.url?.split("/").at(-1));
    response.end(
      JSON.stringify({
        number,
        html_url:
          payloadMode === "wrong-host"
            ? `https://github.com.evil.test/example/repo/pull/${number}`
            : `https://github.com/example/repo/pull/${number}`,
        title: payloadMode === "secret" ? "Change synthetic-sensitive-value-12345" : `Change ${number}`,
        state: payloadMode === "closed" ? "closed" : number >= 10 ? "closed" : "open",
        draft: number === 2,
        merged_at:
          payloadMode === "contradiction"
            ? "2026-10-10T12:00:00Z"
            : payloadMode === "invalid-date"
              ? "yesterday"
              : payloadMode === "closed"
                ? null
                : number >= 10
                  ? `2026-10-${String(number).padStart(2, "0")}T12:00:00Z`
                  : null,
        head: { sha: "a".repeat(40) },
      }),
    );
  }
  active--;
});
await new Promise<void>((resolve) => fixture.listen(0, "127.0.0.1", resolve));
const address = fixture.address();
assert.ok(address && typeof address !== "string");
const fetcher = (url: string, init: RequestInit) =>
  fetch(`http://127.0.0.1:${address.port}${new URL(url).pathname}${new URL(url).search}`, init);
let connection: GitHubConnectorStatus = {
  ...DISCONNECTED_GITHUB_CONNECTOR,
  available: true,
  state: "connected",
  login: "synthetic",
};
const references = new ContextPrReferences(database, () => registry);
const prs = new ContextPrService(references, {
  status: () => connection,
  readContextPullRequest: (identity, signal) => readConnectedContextPr(identity, "synthetic-only", fetcher, signal),
});
const originalPath = process.env.PATH,
  originalGhHost = process.env.GH_HOST;
try {
  await runCauseEffect(store.initialize());
  await runCauseEffect(mailbox.initialize());
  const create = (name: string) =>
    runCauseEffect(
      store.createAgent({ name, description: "Synthetic", avatarSeed: name.toLowerCase(), avatarHue: null }),
    );
  const owner = await create("Owner"),
    other = await create("Other");
  const scope = { kind: "agent" as const, id: owner.id };
  const insertMessage = (text: string, agent = owner) => {
    const id = `message-${Math.random()}`;
    database.connection
      .prepare(
        "INSERT INTO projection_thread_messages(thread_id,message_id,author,status,created_at,ordinal,message_json,last_event_sequence) VALUES(?,?,'assistant','completed',?,0,?,0)",
      )
      .run(
        store.ensureThreadIdNow(agent.id),
        id,
        new Date().toISOString(),
        JSON.stringify({ id, text, author: "assistant", status: "completed", createdAt: new Date().toISOString() }),
      );
  };
  insertMessage("Work https://github.com/example/repo/pull/1 and https://github.com/example/repo/pull/2");
  insertMessage("Other scope https://github.com/example/repo/pull/3", other);
  registry = {
    version: 1,
    sequence: 1,
    generatedAt: new Date().toISOString(),
    watches: [
      {
        id: "watch",
        publisherId: "synthetic",
        ownerAgentId: owner.id,
        sourceKind: "github_ci",
        title: "Watch",
        targetUrl: "https://github.com/example/repo/pull/4",
        status: "active",
      },
    ],
  };
  const channel = agents.channels.store.create("channel", {
    name: "Group",
    title: "Group",
    instructions: "",
    members: [{ agentId: owner.id }, { agentId: other.id }],
    leadAgentId: owner.id,
  });
  agents.channels.store.update(channel, { tasks: [], assignments: [] });
  database.connection
    .prepare("INSERT INTO projection_channel_messages(channel_id,message_id,sequence,message_json) VALUES(?,?,1,?)")
    .run(
      channel.id,
      "channel-message",
      JSON.stringify({
        id: "channel-message",
        channelId: channel.id,
        sequence: 1,
        author: { kind: "agent", id: owner.id, name: "Owner" },
        taskId: null,
        superseded: false,
        message: {
          id: "channel-message",
          text: "Group https://github.com/example/repo/pull/5",
          createdAt: new Date().toISOString(),
          status: "completed",
          author: "assistant",
        },
      }),
    );
  const workBefore = database.connection.prepare("SELECT COUNT(*) AS n FROM projection_mailbox_messages").get();
  await check(
    "Persisted scope links and owned watches select exact PRs without cross-agent or channel inference",
    async () => {
      assert.deepEqual(
        references.read(scope).urls.sort(),
        [1, 2, 4].map((number) => `https://github.com/example/repo/pull/${number}`),
      );
      assert.deepEqual(references.read({ kind: "channel", id: channel.id }).urls, [
        "https://github.com/example/repo/pull/5",
      ]);
      const result = await runCauseEffect(prs.read({ scope }));
      assert.deepEqual(
        result.open.map((row) => row.number),
        [1, 2, 4],
      );
      assert.equal(result.open[1]?.draft, true);
      assert.equal(result.open[0]?.checks, "passed");
      assert.ok(isContextPullRequestsSnapshot(result));
      assert.equal(requests.length, 9);
    },
  );
  await check(
    "Cache reuses verified times, explicit refresh fetches and new references appear on the next read",
    async () => {
      const before = requests.length,
        first = await runCauseEffect(prs.read({ scope }));
      assert.equal(requests.length, before);
      assert.equal((await runCauseEffect(prs.read({ scope }))).open[0]?.checkedAt, first.open[0]?.checkedAt);
      insertMessage("New https://github.com/example/repo/pull/6");
      await runCauseEffect(prs.read({ scope }));
      assert.equal(requests.length, before + 3);
      await runCauseEffect(prs.read({ scope, refresh: true }));
      assert.equal(requests.length, before + 15);
      prs.invalidate();
      await runCauseEffect(prs.read({ scope }));
      assert.equal(requests.length, before + 27);
    },
  );
  await check("Merged PRs sort by actual merge time and keep only the newest three", async () => {
    insertMessage([10, 11, 12, 13].map((n) => `https://github.com/example/repo/pull/${n}`).join(" "));
    const result = await runCauseEffect(prs.read({ scope }));
    assert.deepEqual(
      result.merged.map((row) => row.number),
      [13, 12, 11],
    );
    assert.equal(result.merged[0]?.mergedAt, "2026-10-13T12:00:00.000Z");
  });
  await check(
    "A global concurrency bound applies across reads and no request starts a model or mailbox task",
    async () => {
      maximum = 0;
      let release = () => {};
      gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const ready = new Promise<void>((resolve) => {
        arrived = resolve;
      });
      const first = runCauseEffect(prs.read({ scope, refresh: true }));
      const second = runCauseEffect(prs.read({ scope, refresh: true }));
      await ready;
      assert.equal(maximum, 3);
      release();
      gate = null;
      await Promise.all([first, second]);
      assert.ok(maximum <= 3);
      assert.deepEqual(
        database.connection.prepare("SELECT COUNT(*) AS n FROM projection_mailbox_messages").get(),
        workBefore,
      );
    },
  );
  await check("Connector access errors never change identity and expose no provider body", async () => {
    httpStatus = 403;
    const result = await runCauseEffect(prs.read({ scope, refresh: true }));
    assert.equal(result.open.length, 0);
    assert.ok(result.failures.every((row) => row.reason === "forbidden"));
    assert.ok(!JSON.stringify(result).includes("synthetic-provider-secret"));
    httpStatus = 404;
    assert.ok(
      (await runCauseEffect(prs.read({ scope, refresh: true }))).failures.every((row) => row.reason === "not-found"),
    );
    httpStatus = 200;
  });
  await check(
    "Wrong GitHub response hosts, invalid merge times and contradictory states never become live facts",
    async () => {
      for (const mode of ["wrong-host", "invalid-date"]) {
        payloadMode = mode;
        const result = await runCauseEffect(prs.read({ scope, refresh: true }));
        assert.equal(result.open.length + result.merged.length, 0);
        assert.ok(result.failures.length > 0 && result.failures.every((row) => row.reason === "invalid-response"));
      }
      payloadMode = "contradiction";
      const contradictory = await runCauseEffect(prs.read({ scope, refresh: true }));
      assert.ok(contradictory.failures.some((row) => row.url.endsWith("/1") && row.reason === "invalid-response"));
      payloadMode = "closed";
      const closed = await runCauseEffect(prs.read({ scope, refresh: true }));
      assert.equal(closed.open.length + closed.merged.length + closed.failures.length, 0);
      payloadMode = "normal";
    },
  );
  await check(
    "Real subprocess fallback uses only the exact read-only gh argv and handles missing sign-in",
    async () => {
      const bin = join(root, "bin"),
        log = join(root, "cli-log");
      await mkdir(bin);
      await writeFile(
        join(bin, "gh"),
        `#!${process.execPath}\nif(process.env.GH_HOST!=='github.com')process.exit(2);process.stdin.resume();process.stdin.once('end',()=>{const fs=require('node:fs');fs.appendFileSync(${JSON.stringify(log)},JSON.stringify(process.argv.slice(2))+'\\n');if(process.argv.includes('4'))process.exit(4);console.log(JSON.stringify({url:'https://github.com/example/repo/pull/'+process.argv[4],title:'CLI',state:'OPEN',isDraft:false,mergedAt:null,headRefOid:'b'.repeat(40),statusCheckRollup:[]}));});\n`,
        { mode: 0o700 },
      );
      process.env.PATH = `${bin}:${originalPath}`;
      process.env.GH_HOST = "enterprise.example.test";
      connection = { ...DISCONNECTED_GITHUB_CONNECTOR };
      prs.invalidate();
      const cliReader = new ContextPrService(
        references,
        {
          status: () => connection,
          readContextPullRequest: (identity, signal) =>
            readConnectedContextPr(identity, "synthetic-only", fetcher, signal),
        },
        contextPrCliWithEnvironment(
          () => ({ ...process.env, PATH: "" }),
          () => [bin],
          () => Effect.succeed(null),
        ),
      );
      const before = requests.length,
        result = await runCauseEffect(cliReader.read({ scope, refresh: true }));
      assert.equal(requests.length, before);
      assert.ok(result.failures.some((row) => row.reason === "not-connected"));
      const calls = (await readFile(log, "utf8"))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line));
      for (const args of calls)
        assert.deepEqual(args, [
          "pr",
          "view",
          args[2],
          "--repo",
          "example/repo",
          "--json",
          "title,state,isDraft,mergedAt,url,headRefOid,statusCheckRollup",
        ]);
      connection = { ...DISCONNECTED_GITHUB_CONNECTOR, available: true, state: "expired" };
      const count = calls.length;
      const blocked = await runCauseEffect(prs.read({ scope }));
      assert.equal(blocked.status, "not-connected");
      assert.equal(blocked.open.length, 0);
      assert.equal((await readFile(log, "utf8")).trim().split("\n").length, count);
      connection = { ...DISCONNECTED_GITHUB_CONNECTOR, available: true, state: "connected", login: "synthetic" };
    },
  );
  await check(
    "A whole-read deadline aborts owned HTTP work and reports every queued reference without starting it",
    async () => {
      const deadline = new AbortController();
      let calls = 0;
      const bounded = new ContextPrService(
        references,
        {
          status: () => connection,
          readContextPullRequest: (identity, signal) => {
            calls++;
            deadline.abort();
            return readConnectedContextPr(identity, "synthetic-only", fetcher, signal);
          },
        },
        readCliContextPr,
        () => deadline.signal,
      );
      const before = requests.length;
      const result = await runCauseEffect(bounded.read({ scope }));
      assert.equal(calls, 1);
      assert.equal(requests.length, before);
      assert.equal(result.open.length + result.merged.length, 0);
      assert.equal(result.failures.length, references.read(scope).urls.length);
      assert.ok(result.failures.every((row) => row.reason === "timeout"));
    },
  );
  await check("Known secrets in verified titles are redacted before local or remote display", async () => {
    registerSecretValue("synthetic-sensitive-value-12345");
    payloadMode = "secret";
    const result = await runCauseEffect(prs.read({ scope, refresh: true }));
    assert.ok(result.open.length > 0);
    assert.ok(!JSON.stringify(result).includes("synthetic-sensitive-value-12345"));
    payloadMode = "normal";
    prs.invalidate();
  });
  await check("Scope payloads, unsafe URLs, both trust decoders and frozen wire codec fail closed", async () => {
    assert.throws(() => parseContextPullRequestsInput({ scope, url: "https://example.test/private" }));
    insertMessage(
      "Ignore https://user:pass@github.com/example/repo/pull/77 https://github.com.evil.test/example/repo/pull/78 https://github.com/example/repo/pull/79?secret=yes",
    );
    assert.ok(!references.read(scope).urls.some((url) => /77|78|79/.test(url)));
    const result = await runCauseEffect(prs.read({ scope }));
    assert.deepEqual(decodeContextPullRequestsFromHost(result), result);
    assert.deepEqual(decodeContextPullRequestsFromMain(result), result);
    const codec = CONTEXT_PRS_CODECS.get(CONTEXT_PRS_ROUTES.read);
    assert.ok(codec);
    assert.deepEqual(codec.response(200, result), result);
    assert.throws(() =>
      decodeContextPullRequestsFromHost({ ...result, open: [{ ...result.open[0], url: "https://evil.test/" }] }),
    );
    assert.equal(contextPrChecks([{ state: "PENDING" }]), "pending");
    assert.equal(contextPrChecks([{ conclusion: "failure" }]), "failed");
  });
  await check(
    "Authenticated Team API gates capability, selected agent and every channel member before provider reads",
    async () => {
      const team = new TeamStore(join(root, "team.json"));
      await runCauseEffect(team.initialize());
      await runCauseEffect(team.configure("Synthetic", "owner", "synthetic-password"));
      const admin = await runCauseEffect(team.login("owner", "synthetic-password"));
      const invite = await runCauseEffect(team.createInvite("member"));
      const member = await runCauseEffect(team.acceptInvite(invite.token, "member", "synthetic-password"));
      const serverId = team.getIdentity()?.serverId;
      assert.ok(serverId);
      const accessPath = join(root, "access.json");
      await writeFile(
        accessPath,
        JSON.stringify({
          version: 1,
          servers: { [serverId]: { revision: 1, everyone: { [other.id]: "none" }, roles: [] } },
        }),
      );
      const access = new AgentAccessStore(accessPath);
      await Effect.runPromise(access.load());
      const unexpected = (): never => {
        throw new Error("No browser work is permitted.");
      };
      const api = new TeamApiServer({
        store: team,
        agents,
        mailbox,
        channels: agents.channels,
        contextPrs: prs,
        agentAccess: access,
        appVersion: "0.54.0",
        browser: {
          listTabs: () => [],
          getControlState: unexpected,
          open: unexpected,
          activate: unexpected,
          navigate: unexpected,
          reload: unexpected,
          close: unexpected,
          capturePreview: unexpected,
          setVisible: unexpected,
          getDisplayState: unexpected,
          loadUrl: unexpected,
          isPrivate: unexpected,
          takeUserControl: unexpected,
          releaseUserControl: unexpected,
          releaseUserControlHeldBy: unexpected,
          stop: unexpected,
          importSignIn: unexpected,
          startView: unexpected,
          dispatchViewInput: unexpected,
          copyViewSelection: unexpected,
        },
      });
      const port = await Effect.runPromise(api.start());
      try {
        const post = (
          token?: string,
          scopeInput: import("@openbot/contracts/context-prs").ContextPullRequestScope = scope,
          capability = true,
        ) =>
          fetch(`http://127.0.0.1:${port}${CONTEXT_PRS_ROUTES.read}`, {
            method: "POST",
            headers: {
              "OpenBot-Protocol-Version": "7",
              "OpenBot-App-Version": "0.54.0",
              "OpenBot-Capabilities": capability
                ? "context-prs-v1,agent-hidden-v1,agent-threads-v1,channel-chats-v1"
                : "agent-hidden-v1",
              "Content-Type": "application/json",
              ...(token ? { Authorization: `Bearer ${token}` } : {}),
            },
            body: JSON.stringify({ scope: scopeInput, refresh: true }),
          });
        const before = requests.length;
        assert.equal((await post()).status, 401);
        assert.equal((await post(admin.sessionToken, scope, false)).status, 400);
        assert.equal((await post(member.sessionToken, { kind: "agent", id: other.id })).status, 404);
        const channelScope = { kind: "channel" as const, id: channel.id };
        const memberResponse = await fetch(`http://127.0.0.1:${port}${CONTEXT_PRS_ROUTES.read}`, {
          method: "POST",
          headers: {
            "OpenBot-Protocol-Version": "7",
            "OpenBot-App-Version": "0.54.0",
            "OpenBot-Capabilities": "context-prs-v1,agent-hidden-v1,channel-chats-v1",
            "Content-Type": "application/json",
            Authorization: `Bearer ${member.sessionToken}`,
          },
          body: JSON.stringify({ scope: channelScope }),
        });
        assert.equal(memberResponse.status, 404);
        assert.equal(requests.length, before);
        const response = await post(admin.sessionToken);
        assert.equal(response.status, 200);
        const snapshot = decodeContextPullRequestsFromHost(await response.json());
        assert.equal(snapshot.scope.id, owner.id);
        assert.ok(snapshot.open.every((row) => row.number !== 3 && row.number !== 5));
        const group = await fetch(`http://127.0.0.1:${port}${CONTEXT_PRS_ROUTES.read}`, {
          method: "POST",
          headers: {
            "OpenBot-Protocol-Version": "7",
            "OpenBot-App-Version": "0.54.0",
            "OpenBot-Capabilities": "context-prs-v1,agent-hidden-v1,channel-chats-v1",
            "Content-Type": "application/json",
            Authorization: `Bearer ${admin.sessionToken}`,
          },
          body: JSON.stringify({ scope: channelScope }),
        });
        assert.equal(group.status, 200);
        assert.deepEqual(
          decodeContextPullRequestsFromHost(await group.json()).open.map((row) => row.number),
          [5],
        );
        arrivals = 0;
        let release = () => {};
        gate = new Promise<void>((resolve) => {
          release = resolve;
        });
        const arrivedAtProvider = new Promise<void>((resolve) => {
          arrived = resolve;
        });
        const pending = post(member.sessionToken);
        await arrivedAtProvider;
        await writeFile(
          accessPath,
          JSON.stringify({
            version: 1,
            servers: { [serverId]: { revision: 2, everyone: { [owner.id]: "none", [other.id]: "none" }, roles: [] } },
          }),
        );
        await Effect.runPromise(access.load());
        release();
        gate = null;
        const revoked = await pending;
        assert.equal(revoked.status, 404);
        assert.ok(!JSON.stringify(await revoked.json()).includes("https://github.com"));
      } finally {
        await Effect.runPromise(api.stop());
      }
    },
  );
  await check("Reference bounds are explicit and never start a repository-wide query", () => {
    insertMessage(
      Array.from({ length: 201 }, (_, index) => `https://github.com/example/repo/pull/${100 + index}`).join(" "),
      other,
    );
    const result = references.read({ kind: "agent", id: other.id });
    assert.equal(result.urls.length, 200);
    assert.equal(result.limited, true);
    assert.ok(
      requests.every((path) =>
        /\/repos\/example\/repo\/(pulls\/[0-9]+|commits\/[a-f0-9]{40}\/(check-runs|status)\?per_page=100)$/.test(
          path.replace("GET ", ""),
        ),
      ),
    );
  });
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.exitCode = 1;
} finally {
  process.env.PATH = originalPath;
  if (originalGhHost === undefined) delete process.env.GH_HOST;
  else process.env.GH_HOST = originalGhHost;
  await runCauseEffect(agents.stop()).catch(() => {});
  database.close();
  await new Promise<void>((resolve, reject) => fixture.close((error) => (error ? reject(error) : resolve())));
  await mkdir(output, { recursive: true });
  await writeFile(join(output, "report.json"), JSON.stringify(report, null, 2));
  await rm(root, { recursive: true, force: true });
  process.stdout.write(`${JSON.stringify(report)}\n`);
}
