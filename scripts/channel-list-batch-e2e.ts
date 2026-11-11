// Checks that a burst of channel changes reads the channel list at most once a second, and that a
// client that reads the list again on each channels-changed event still gets the last state. Each
// publish of a channel sends channels-changed, and each client then reads the list. The list
// query counts the unread messages of every channel, so a streaming reply read it on each frame.
//
// Runs the real ChannelService, ChannelStore, SQLite and the Team API channel route on a
// temporary profile. No app, provider, account or network is started.
//
// Run: node --import tsx scripts/channel-list-batch-e2e.ts
// Writes .openbot-build/channel-list-batch-e2e/report.json and exits non-zero on a failure.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { IncomingMessage, ServerResponse } from "node:http";
import { Socket } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { CHANNEL_CHATS_CAPABILITY, type ChannelSummary, type TeamMemberSummary } from "@openbot/contracts/ipc";
import { CHANNEL_ROUTES } from "@openbot/contracts/team-protocol/channels-v1";
import { Effect } from "effect";
import { AgentStore } from "../src/backend/agent-store";
import { ChannelImages } from "../src/backend/channel-images";
import { ChannelService } from "../src/backend/channel-service";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { type ChannelSettingsAgents, routeChannels } from "../src/main/team-api/route-channels";

const OUT = resolve(import.meta.dirname, "../.openbot-build/channel-list-batch-e2e");
const BURST = 30;
const BURST_GAP_MS = 20;

interface Report {
  checks: string[];
  burst?: { changes: number; elapsedMs: number; listReads: number };
  settle?: { listReads: number; relistedEvents: number; lastRevision: number; clientRevision: number; afterMs: number };
  passed: boolean;
  error?: string;
}

const report: Report = { checks: [], passed: false };
const root = await mkdtemp(join(tmpdir(), "openbot-channel-list-batch-"));
const events: { channelId: string; revision: number; relisted: boolean }[] = [];
const unused = (): never => {
  throw new Error("Not used by the list route.");
};
const settingsAgents: ChannelSettingsAgents = {
  listChannelMemories: unused,
  createChannelMemory: unused,
  updateChannelMemory: unused,
  deleteChannelMemory: unused,
  clearChannelMemories: unused,
  listChannelRoutines: unused,
  createChannelRoutine: unused,
  updateChannelRoutine: unused,
  deleteChannelRoutine: unused,
  testChannelRoutine: unused,
  listChannelRoutineRuns: unused,
};
const member: TeamMemberSummary = {
  id: "member-remote",
  username: "remote",
  email: null,
  name: "Remote member",
  role: "owner",
  createdAt: "2026-11-01T00:00:00.000Z",
  disabled: false,
};

const pause = (ms: number) => new Promise((done) => setTimeout(done, ms));

const database = new OpenBotDatabase(join(root, "data"));
const agents = new AgentStore(join(root, "data"), join(root, "home"), database);
const mailbox = new MailboxStore(join(root, "data"), agents.sharedRoot, database);
const channels = new ChannelService(database, mailbox, new ChannelImages(join(root, "channel-images")), {
  agents: () => agents.list(),
  generate: () => Effect.succeed('{"idle":true}'),
  schedule: () => {},
  changed: (channelId, revision) => events.push({ channelId, revision, relisted: false }),
  relisted: (channelId, revision) => events.push({ channelId, revision, relisted: true }),
  busy: () => false,
  interrupt: () => Effect.void,
  error: (error) => {
    throw error;
  },
});

let listReads = 0;
const list = channels.store.list.bind(channels.store);
channels.store.list = (...args: Parameters<typeof list>) => {
  listReads += 1;
  return list(...args);
};

async function clientRead(): Promise<ChannelSummary[]> {
  const request = new IncomingMessage(new Socket());
  let body: unknown = null;
  const outcome = await routeChannels(
    {
      request,
      response: new ServerResponse(request),
      method: "GET",
      url: new URL(`http://openbot${CHANNEL_ROUTES.list}`),
      protocol: 7,
      capabilities: new Set([CHANNEL_CHATS_CAPABILITY]),
      member,
      token: "token",
      sessionId: "session",
      sessionExpiresAt: "2026-12-01T00:00:00.000Z",
      agentLevel: () => "manage",
      json: (_status, value) => {
        body = value;
        return "handled";
      },
      empty: () => "handled",
    },
    channels,
    settingsAgents,
  );
  assert.equal(outcome, "handled");
  assert.ok(Array.isArray(body));
  return body;
}

try {
  await runCauseEffect(agents.initialize());
  await runCauseEffect(mailbox.initialize());
  const agent = await runCauseEffect(
    agents.createAgent({ name: "Reviewer", description: "Synthetic", avatarSeed: "reviewer", avatarHue: null }),
  );
  const person = { id: member.id, name: "Remote member" };
  const channel = await runCauseEffect(
    channels.command(
      {
        type: "save",
        operationId: randomUUID(),
        channelId: randomUUID(),
        draft: {
          name: "List batch",
          title: "",
          instructions: "",
          members: [{ agentId: agent.id }],
          leadAgentId: agent.id,
        },
      },
      person,
    ),
  );
  const first = await clientRead();
  assert.equal(first.length, 1);
  await pause(1_100);
  listReads = 0;
  let seen = events.length;
  let client = first;
  const start = Date.now();
  for (let index = 0; index < BURST; index += 1) {
    await runCauseEffect(
      channels.command(
        {
          type: "save",
          operationId: randomUUID(),
          channelId: channel.id,
          draft: {
            name: `List batch ${index}`,
            title: "",
            instructions: "",
            members: [{ agentId: agent.id }],
            leadAgentId: agent.id,
          },
        },
        person,
      ),
    );
    if (events.length > seen) {
      seen = events.length;
      client = await clientRead();
    }
    await pause(BURST_GAP_MS);
  }
  const elapsedMs = Date.now() - start;
  report.burst = { changes: BURST, elapsedMs, listReads };
  const allowed = Math.ceil(elapsedMs / 1_000) + 1;
  assert.ok(
    listReads <= allowed,
    `${BURST} changes in ${elapsedMs} ms read the channel list ${listReads} times (limit ${allowed}).`,
  );
  report.checks.push(
    `${BURST} channel changes in ${elapsedMs} ms, each read by a client, read the channel list ${listReads} times.`,
  );

  const lastRevision = channels.store.get(channel.id).revision;
  const settleStart = Date.now();
  while (Date.now() - settleStart < 2_500) {
    if (events.length > seen) {
      seen = events.length;
      client = await clientRead();
    }
    if (client[0]?.revision === lastRevision && client[0]?.name === `List batch ${BURST - 1}`) break;
    await pause(20);
  }
  const clientRevision = client[0]?.revision ?? -1;
  report.settle = {
    listReads,
    relistedEvents: events.filter((event) => event.relisted).length,
    lastRevision,
    clientRevision,
    afterMs: Date.now() - settleStart,
  };
  assert.equal(clientRevision, lastRevision, "A client that reads on each event does not get the last channel state.");
  assert.equal(client[0]?.name, `List batch ${BURST - 1}`);
  report.checks.push(
    `A client that reads the list on each channels-changed event gets the last revision (${lastRevision}) ${report.settle.afterMs} ms after the burst.`,
  );
  assert.ok(listReads <= allowed + 2, `The settle read the list ${listReads} times in all.`);
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
} finally {
  await runCauseEffect(channels.stop());
  database.close();
  await rm(root, { recursive: true, force: true });
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}

process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (!report.passed) process.exit(1);
