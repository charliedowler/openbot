import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";

const output = resolve(import.meta.dirname, "../.openbot-build/channel-queue-reservation-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-channel-queue-"));
const profile = join(root, "profile");
const agentId = "agent-queue-check";
const checks: string[] = [];
let database = new OpenBotDatabase(profile);
let mailbox = new MailboxStore(profile, join(root, "shared"), database);
let passed = false;

async function enqueue(
  text: string,
  options: { channelId?: string; taskId?: string; routine?: boolean } = {},
): Promise<string> {
  const receipt = await runCauseEffect(
    mailbox.enqueue({
      sender: options.routine
        ? {
            kind: "routine",
            routineId: "routine-check",
            runId: "run-check",
            routineName: "Event receiver",
            scheduledFor: new Date().toISOString(),
          }
        : { kind: "user" },
      recipientAgentIds: [agentId],
      text,
      ...(options.channelId ? { channelId: options.channelId } : {}),
      ...(options.taskId ? { taskId: options.taskId } : {}),
    }),
  );
  const delivery = receipt.deliveries[0];
  assert.ok(delivery);
  return delivery.id;
}

function nextIs(expected: string, label: string, taskId: string | null = null): void {
  assert.equal(mailbox.nextQueued(agentId, taskId)?.delivery.id, expected, label);
  checks.push(label);
}

try {
  await runCauseEffect(mailbox.initialize());
  const routine = await enqueue("Handle the existing event", { routine: true });
  await runCauseEffect(mailbox.markStarting(routine));
  await runCauseEffect(mailbox.markRunning(routine, "interrupted-turn"));
  const channel = await enqueue("Run the assigned QA", { channelId: "channel-check" });
  await runCauseEffect(mailbox.requeueRefused(routine));
  assert.deepEqual(mailbox.queuedDeliveryIds(agentId), [routine, channel]);
  nextIs(channel, "Reserved channel work runs before an interrupted routine retry");

  database.close();
  database = new OpenBotDatabase(profile);
  mailbox = new MailboxStore(profile, join(root, "shared"), database);
  await runCauseEffect(mailbox.initialize());
  nextIs(channel, "Reserved work keeps its priority after a database reload");

  const independent = await enqueue("Work in the independent task", { taskId: "task-check" });
  nextIs(independent, "An independent task keeps its own queue", "task-check");
  nextIs(channel, "Independent task work does not replace the main queue assignment");

  await runCauseEffect(mailbox.markStarting(channel));
  await runCauseEffect(mailbox.markRunning(channel, "channel-turn"));
  await runCauseEffect(mailbox.markTerminal(channel, "completed"));
  nextIs(routine, "The original routine continues after the assigned work completes");

  const firstChannel = await enqueue("First assigned request", { channelId: "channel-first" });
  const secondChannel = await enqueue("Second assigned request", { channelId: "channel-second" });
  nextIs(firstChannel, "Assigned requests keep their existing relative order");
  await runCauseEffect(mailbox.cancel(agentId, firstChannel));
  nextIs(secondChannel, "Cancelling one assignment releases the next assignment");
  await runCauseEffect(mailbox.cancel(agentId, secondChannel));
  nextIs(routine, "Cancelling assigned work preserves the original routine");
  passed = true;
} finally {
  mkdirSync(output, { recursive: true });
  writeFileSync(join(output, "report.json"), `${JSON.stringify({ passed, checks }, null, 2)}\n`);
  database.close();
  await rm(root, { recursive: true, force: true });
}

process.stdout.write(`${JSON.stringify({ passed, checks: checks.length })}\n`);
