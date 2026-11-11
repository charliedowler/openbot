// Checks the queue snapshot of an agent with a long history (0059). On 11 November Helm had 3,390
// deliveries in its queue snapshot (about 8 MB of message text), and Jeffrey about 11.8 MB. Each
// MailboxSync.emitQueue built that whole snapshot, reconciled every delivery with its routine run
// (one runForDelivery read for each routine delivery), and sent it in queue-changed, which the main
// process structured-clones to the window. The profile of a copy of the server put emitQueue at the
// top of the main thread.
//
// The queue-changed snapshot must hold every queued, starting and running delivery, as the full
// queue does, and only a bounded window of the settled ones. emitQueue must reconcile the unsettled
// routine deliveries and a settled one only once. listQueue (Team API, IPC and the clients without
// queue-window-v1) still gives the full queue.
//
// Runs the real mailbox store, SQLite and MailboxSync on a temporary profile with 3,000 settled
// deliveries of about 2.4 KB each. The routine scheduler counts the deliveries it gets. No app,
// provider, account or network is started.
//
// Run: node --import tsx scripts/mailbox-queue-window-e2e.ts
// Writes .openbot-build/mailbox-queue-window-e2e/report.json and exits non-zero on a failure.
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { serialize } from "node:v8";
import type { AgentEvent, QueueDelivery, QueueSnapshot } from "@openbot/contracts/ipc";
import { Effect, Scope } from "effect";
import { MailboxSync, type MailboxSyncConversation } from "../src/backend/agent/mailbox-sync";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const OUT = resolve(import.meta.dirname, "../.openbot-build/mailbox-queue-window-e2e");
const AGENT = "helm";
const HISTORY = 3_000;
const TEXT = "The nightly build report for the release branch. ".repeat(48);
const MAX_PAYLOAD_BYTES = 1024 * 1024;

interface Emit {
  name: string;
  deliveries: number;
  jsonBytes: number;
  cloneBytes: number;
  reconciled: number;
  routineReconciled: number;
  ms: number;
}

const report: {
  history: number;
  textBytes: number;
  full?: { deliveries: number; jsonBytes: number };
  emits: Emit[];
  checks: string[];
  results: { name: string; passed: boolean; detail: string }[];
  passed: boolean;
  error?: string;
} = { history: HISTORY, textBytes: Buffer.byteLength(TEXT), emits: [], checks: [], results: [], passed: false };

const root = await mkdtemp(join(tmpdir(), "openbot-queue-window-"));
const scope = Effect.runSync(Scope.make());
const events: AgentEvent[] = [];
const reconciled: QueueDelivery[] = [];
const conversation: MailboxSyncConversation = {
  isExecutionThread: () => false,
  snapshot: () => undefined,
  snapshotWithTurn: () => undefined,
  snapshotToUpdate: () => undefined,
  hasPublishedConversation: () => false,
  publishConversation: () => {},
  emitConversation: () => {},
};
const database = new OpenBotDatabase(join(root, "data"));
const mailbox = new MailboxStore(join(root, "data"), join(root, "shared"), database);
const sync = new MailboxSync({
  database,
  mailbox,
  conversation,
  routines: {
    reconcileDelivery: (delivery) => {
      reconciled.push(delivery);
      return false;
    },
    stateChanged: () => {},
  },
  scope: () => scope,
  hooks: {
    emit: (event) => events.push(event),
    emitError: () => {},
    queueHold: () => ({ reason: "channel-task", channelId: "pod-web", channelName: "Pod Web", agentId: AGENT }),
  },
});

const senders: QueueDelivery["sender"][] = [
  { kind: "user" },
  { kind: "agent", agentId: "jeffrey" },
  {
    kind: "routine",
    routineId: "inbox",
    runId: "run",
    routineName: "Inbox",
    scheduledFor: "2026-11-11T08:00:00.000Z",
  },
];

async function enqueue(index: number, sender: QueueDelivery["sender"]): Promise<string> {
  const receipt = await runCauseEffect(
    mailbox.enqueue({
      sender: sender.kind === "routine" ? { ...sender, runId: `run-${index}` } : sender,
      recipientAgentIds: [AGENT],
      text: `${index}: ${TEXT}`,
      ...(sender.kind === "agent" ? { idempotencyKey: `agent:jeffrey:turn-${index}:${index}` } : { draftIds: [] }),
    }),
  );
  const id = receipt.deliveries[0]?.id;
  assert.ok(id, "The message has a delivery.");
  return id;
}

function emit(name: string): Emit {
  events.length = 0;
  reconciled.length = 0;
  const startedAt = performance.now();
  sync.emitQueue(AGENT);
  const ms = performance.now() - startedAt;
  const event = events.find((item) => item.type === "queue-changed");
  assert.ok(event && event.type === "queue-changed", "emitQueue emits queue-changed.");
  const result = {
    name,
    deliveries: event.snapshot.deliveries.length,
    jsonBytes: Buffer.byteLength(JSON.stringify(event)),
    cloneBytes: serialize(event).length,
    reconciled: reconciled.length,
    routineReconciled: reconciled.filter((delivery) => delivery.sender.kind === "routine").length,
    ms: Math.round(ms),
  };
  report.emits.push(result);
  return result;
}

function lastSnapshot(): QueueSnapshot {
  const event = events.find((item) => item.type === "queue-changed");
  assert.ok(event && event.type === "queue-changed");
  return event.snapshot;
}

function check(name: string, run: () => string): void {
  try {
    report.results.push({ name, passed: true, detail: run() });
  } catch (error) {
    report.results.push({ name, passed: false, detail: error instanceof Error ? error.message : String(error) });
  }
}

const active = (snapshot: QueueSnapshot) =>
  snapshot.deliveries.filter((delivery) => ["queued", "starting", "running"].includes(delivery.status));

try {
  await runCauseEffect(mailbox.initialize());
  for (let index = 0; index < HISTORY; index += 1) {
    const id = await enqueue(index, senders[index % 3] ?? { kind: "user" });
    if (index % 50 === 7) {
      await runCauseEffect(mailbox.cancel(AGENT, id));
      continue;
    }
    await runCauseEffect(mailbox.markStarting(id));
    await runCauseEffect(
      index % 50 === 3
        ? mailbox.markTerminal(id, "failed", "The provider failed.")
        : mailbox.markTerminal(id, "completed"),
    );
  }
  const running = await enqueue(HISTORY, senders[2] ?? { kind: "user" });
  await runCauseEffect(mailbox.markStarting(running));
  await runCauseEffect(mailbox.markRunning(running, "turn-running"));
  for (const [offset, sender] of senders.entries()) await enqueue(HISTORY + 1 + offset, sender);

  const full = sync.queueSnapshot(AGENT);
  report.full = { deliveries: full.deliveries.length, jsonBytes: Buffer.byteLength(JSON.stringify(full)) };
  assert.equal(full.deliveries.length, HISTORY + 4, "listQueue gives the full queue.");
  assert.equal(active(full).length, 4);
  report.checks.push(
    `listQueue (AgentService.listQueue, the Team API and IPC read) gives all ${full.deliveries.length} deliveries, ${report.full.jsonBytes} bytes.`,
  );

  const first = emit("first emit");
  const firstSnapshot = lastSnapshot();
  const second = emit("second emit, same queue");
  const secondSnapshot = lastSnapshot();

  check("The queued, starting and running deliveries and the hold are those of the full queue", () => {
    assert.equal(JSON.stringify(active(firstSnapshot)), JSON.stringify(active(full)));
    assert.ok(firstSnapshot.hold !== undefined, "The snapshot has no hold.");
    assert.equal(JSON.stringify(firstSnapshot.hold), JSON.stringify(full.hold));
    return `${active(firstSnapshot).length} active deliveries, hold ${firstSnapshot.hold?.reason}`;
  });

  check("Each kept delivery is the one of the full queue, in queue order, with the newest completed ones", () => {
    for (const delivery of firstSnapshot.deliveries)
      assert.equal(
        JSON.stringify(delivery),
        JSON.stringify(full.deliveries.find((candidate) => candidate.id === delivery.id)),
        `Delivery ${delivery.id} differs from the full queue.`,
      );
    const fullOrder = full.deliveries.map((delivery) => delivery.id);
    const positions = firstSnapshot.deliveries.map((delivery) => fullOrder.indexOf(delivery.id));
    assert.deepEqual(
      positions,
      [...positions].sort((left, right) => left - right),
      "The order differs.",
    );
    const kept = new Set(firstSnapshot.deliveries.map((delivery) => delivery.id));
    const newest = full.deliveries.filter((delivery) => delivery.status === "completed").slice(-50);
    assert.ok(
      newest.every((delivery) => kept.has(delivery.id)),
      "The newest completed deliveries are not kept.",
    );
    assert.equal(JSON.stringify(secondSnapshot), JSON.stringify(firstSnapshot), "Two emits differ.");
    return `${firstSnapshot.deliveries.length} of ${full.deliveries.length} deliveries`;
  });

  check(`queue-changed is at most ${MAX_PAYLOAD_BYTES} bytes as JSON and as a structured clone`, () => {
    assert.ok(
      first.jsonBytes <= MAX_PAYLOAD_BYTES && first.cloneBytes <= MAX_PAYLOAD_BYTES,
      `${first.jsonBytes} JSON bytes and ${first.cloneBytes} clone bytes for ${first.deliveries} deliveries`,
    );
    return `${first.jsonBytes} JSON bytes, ${first.cloneBytes} clone bytes, ${first.deliveries} deliveries (full ${report.full?.jsonBytes})`;
  });

  check("A second emit with the same queue reconciles only the 2 unsettled routine deliveries", () => {
    assert.ok(
      second.reconciled <= 2,
      `${second.reconciled} deliveries reconciled (${second.routineReconciled} routine); first emit ${first.reconciled}`,
    );
    return `first emit ${first.reconciled} (${first.routineReconciled} routine), second ${second.reconciled}`;
  });

  await runCauseEffect(mailbox.markTerminal(running, "completed"));
  const settled = emit("the running routine delivery completes");
  const settledRunning = reconciled.filter((delivery) => delivery.id === running).map((delivery) => delivery.status);
  const after = emit("next emit");
  const againRunning = reconciled.filter((delivery) => delivery.id === running).length;
  check("A routine delivery that completes is reconciled once with its new status, and not again", () => {
    assert.deepEqual(settledRunning, ["completed"], "The completed routine delivery is not reconciled.");
    assert.ok(settled.reconciled <= 2, `${settled.reconciled} deliveries reconciled after the completion`);
    assert.equal(againRunning, 0, "The completed routine delivery is reconciled again.");
    return `after the completion ${settled.reconciled}, next emit ${after.reconciled}`;
  });

  report.passed = report.results.every((result) => result.passed);
} catch (error) {
  report.error = error instanceof Error ? (error.stack ?? error.message) : String(error);
} finally {
  database.close();
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  await rm(root, { recursive: true, force: true });
}

for (const result of report.results)
  process.stdout.write(`${result.passed ? "PASS" : "FAIL"} ${result.name}: ${result.detail}\n`);
if (report.error) process.stdout.write(`${report.error}\n`);
process.stdout.write(
  `${report.passed ? "passed" : "failed"} ${report.results.filter((result) => result.passed).length} of ${report.results.length} ${join(OUT, "report.json")}\n`,
);
if (!report.passed) process.exit(1);
