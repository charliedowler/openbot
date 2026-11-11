import { createHash } from "node:crypto";
import type { EventRoutineOwner } from "@openbot/contracts/ipc-events";
import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { WEBHOOK_DELIVERY_RESPONSE_BYTES_LIMIT } from "@openbot/contracts/signal-protocol/messages";
import { databaseRows, requiredStringColumn } from "./database/database-rows";
import type { OpenBotDatabase } from "./openbot-database";

export const BRIDGE_WORK_OBSERVE_EVENT = "bridge.work.observe";
export const BRIDGE_WORK_CANARY_RECEIVER = "bridge.work.canary";
export const BRIDGE_WORK_CANARY_RECEIVERS_ENV = "OPENBOT_WORK_CANARY_RECEIVERS";
export const BRIDGE_WORK_OBSERVE_IDS_LIMIT = 20;
export const BRIDGE_WORK_RESPONSE_BYTES_LIMIT = WEBHOOK_DELIVERY_RESPONSE_BYTES_LIMIT;
export const BRIDGE_WORK_EVENT = "bridge.work.cancel";
export const BRIDGE_WORK_RECEIVER = "bridge.work.lifecycle";
export const BRIDGE_WORK_RECEIVERS_ENV = "OPENBOT_WORK_LIFECYCLE_RECEIVERS";

export interface BridgeWorkReferences {
  deliveryIds?: string[];
  dispatchId?: string;
  pr?: { repository: string; number: number };
}

export interface BridgeWorkCancellation extends BridgeWorkReferences {
  idempotencyKey: string;
  reason: string;
}

export interface BridgeWorkLink {
  authority: string;
  refs: string[];
  owner: EventRoutineOwner;
  routineId: string;
  runId: string;
  deliveryId: string;
}

export interface BridgeWorkReceiver {
  ownerAgentId: string;
  routineId: string;
  sources: Array<{ kind: "agent" | "channel"; ownerId: string; routineId: string }>;
}

const id = (value: unknown): value is string =>
  typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$/u.test(value);

export function bridgeWorkReceivers(env = BRIDGE_WORK_RECEIVERS_ENV): BridgeWorkReceiver[] {
  const raw = process.env[env];
  if (!raw) return [];
  const values = JSON.parse(raw);
  if (!Array.isArray(values) || values.length > 64) throw new Error("Invalid lifecycle receivers.");
  return values.map((value) => {
    if (!isDynamicRecord(value) || !id(value.ownerAgentId) || !id(value.routineId) || !Array.isArray(value.sources))
      throw new Error("Invalid lifecycle receiver.");
    const sources: BridgeWorkReceiver["sources"] = value.sources.map((source: unknown) => {
      if (
        !isDynamicRecord(source) ||
        (source.kind !== "agent" && source.kind !== "channel") ||
        !id(source.ownerId) ||
        !id(source.routineId)
      )
        throw new Error("Invalid lifecycle source.");
      return { kind: source.kind, ownerId: source.ownerId, routineId: source.routineId };
    });
    return { ownerAgentId: value.ownerAgentId, routineId: value.routineId, sources };
  });
}

export function bridgeWorkAuthority(receiver: BridgeWorkReceiver): string {
  return `${receiver.ownerAgentId}:${receiver.routineId}`;
}

export function bridgeWorkReferences(value: unknown): BridgeWorkReferences | null {
  if (!isDynamicRecord(value)) return null;
  const result: BridgeWorkReferences = {};
  if (value.deliveryIds !== undefined) {
    if (
      !Array.isArray(value.deliveryIds) ||
      value.deliveryIds.length < 1 ||
      value.deliveryIds.length > 100 ||
      !value.deliveryIds.every(id)
    )
      return null;
    result.deliveryIds = [...new Set(value.deliveryIds)];
  }
  if (value.dispatchId !== undefined) {
    if (!id(value.dispatchId)) return null;
    result.dispatchId = value.dispatchId;
  }
  if (value.pr !== undefined) {
    const pr = value.pr;
    if (
      !isDynamicRecord(pr) ||
      typeof pr.repository !== "string" ||
      !/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/u.test(pr.repository) ||
      !Number.isSafeInteger(pr.number) ||
      typeof pr.number !== "number" ||
      pr.number < 1
    )
      return null;
    result.pr = { repository: pr.repository.toLowerCase(), number: pr.number };
  }
  return Object.keys(result).length ? result : null;
}

export function bridgeWorkCancellation(value: unknown): BridgeWorkCancellation | null {
  const refs = bridgeWorkReferences(value);
  if (
    !refs ||
    !isDynamicRecord(value) ||
    !id(value.idempotencyKey) ||
    typeof value.reason !== "string" ||
    !value.reason.trim() ||
    value.reason.length > 512
  )
    return null;
  return { ...refs, idempotencyKey: value.idempotencyKey, reason: value.reason };
}

export function bridgeWorkKeys(refs: BridgeWorkReferences): string[] {
  return [
    ...(refs.deliveryIds ?? []).map((value) => `delivery:${value}`),
    ...(refs.dispatchId ? [`dispatch:${refs.dispatchId}`] : []),
    ...(refs.pr ? [`pr:${refs.pr.repository.toLowerCase()}#${refs.pr.number}`] : []),
  ].sort();
}

function key(...values: string[]): string {
  return createHash("sha256").update(JSON.stringify(values)).digest("hex");
}

export class BridgeWorkStore {
  constructor(readonly database: OpenBotDatabase) {}

  source(owner: EventRoutineOwner, routineId: string, purpose = "work"): BridgeWorkReceiver | null {
    const receivers = bridgeWorkReceivers(
      purpose === "canary" ? BRIDGE_WORK_CANARY_RECEIVERS_ENV : BRIDGE_WORK_RECEIVERS_ENV,
    ).filter((receiver) =>
      receiver.sources.some(
        (source) => source.kind === owner.kind && source.ownerId === owner.id && source.routineId === routineId,
      ),
    );
    if (receivers.length > 1) throw new Error("Ambiguous lifecycle source.");
    return receivers[0] ?? null;
  }

  purpose(data: unknown): "work" | "canary" | null {
    if (!isDynamicRecord(data)) return null;
    const body = isDynamicRecord(data.data) ? data.data : data;
    return isDynamicRecord(body.bridgeWork) && (body.bridgeWork.kind === "work" || body.bridgeWork.kind === "canary")
      ? body.bridgeWork.kind
      : null;
  }

  metadata(data: unknown): BridgeWorkReferences | null {
    if (!isDynamicRecord(data)) return null;
    const body = isDynamicRecord(data.data) ? data.data : data;
    const metadata = body.bridgeWork;
    if (!isDynamicRecord(metadata) || (metadata.kind !== "work" && metadata.kind !== "canary")) return null;
    return bridgeWorkReferences(metadata);
  }

  closed(authority: string, refs: string[]): boolean {
    return refs.some((ref) => this.database.commandResult(`bridge-work:closed:${key(authority, ref)}`) !== undefined);
  }

  requestClosed(requestMessageId: string): boolean {
    const row = this.database.connection
      .prepare("SELECT run_id FROM projection_channel_routine_runs WHERE request_message_id = ?")
      .get(requestMessageId);
    return isDynamicRecord(row) && typeof row.run_id === "string" && this.runClosed(row.run_id);
  }

  record(link: BridgeWorkLink): void {
    const runKey = `bridge-work:run:${link.runId}`;
    this.database.dispatch(
      runKey,
      [
        { aggregateType: "bridge-work-run", aggregateId: link.runId, eventType: "bridge.work.linked", payload: link },
        ...link.refs.map((ref) => ({
          aggregateType: "bridge-work",
          aggregateId: key(link.authority, ref),
          eventType: "bridge.work.linked",
          payload: link,
        })),
      ],
      () => link,
    );
  }

  run(runId: string): BridgeWorkLink | null {
    const value = this.database.commandResult(`bridge-work:run:${runId}`);
    if (value === undefined) return null;
    return decodeLink(value);
  }

  recordExecutorOutcome(agentId: string, turnId: string, itemId: string, failed: boolean): void {
    this.database.dispatch(
      `bridge-work:tool:${key(agentId, turnId, itemId)}`,
      [
        {
          aggregateType: "bridge-work-tool",
          aggregateId: key(agentId, turnId),
          eventType: "bridge.work.executor-outcome",
          payload: { failed },
        },
      ],
      () => null,
    );
  }

  executorOutcomes(agentId: string, turnId: string): Array<{ failed: boolean }> {
    return databaseRows(
      this.database.connection
        .prepare(
          "SELECT json_extract(payload_json, '$.failed') AS failed FROM orchestration_events WHERE aggregate_type = 'bridge-work-tool' AND aggregate_id = ? ORDER BY sequence",
        )
        .all(key(agentId, turnId)),
    ).map((row) => ({ failed: row.failed === 1 }));
  }

  runClosed(runId: string): boolean {
    const link = this.run(runId);
    return link !== null && this.closed(link.authority, link.refs);
  }

  links(authority: string, ref: string): BridgeWorkLink[] {
    return databaseRows(
      this.database.connection
        .prepare(
          "SELECT payload_json FROM orchestration_events WHERE aggregate_type = 'bridge-work' AND aggregate_id = ? AND event_type = 'bridge.work.linked' ORDER BY sequence",
        )
        .all(key(authority, ref)),
    ).map((row) => decodeLink(JSON.parse(requiredStringColumn(row, "payload_json"))));
  }

  closures(): Array<{ authority: string; input: BridgeWorkCancellation }> {
    return databaseRows(
      this.database.connection
        .prepare(
          "SELECT payload_json FROM orchestration_events WHERE aggregate_type = 'bridge-work-cancel' AND event_type = 'bridge.work.closure' ORDER BY sequence",
        )
        .all(),
    ).map((row) => {
      const value = JSON.parse(requiredStringColumn(row, "payload_json"));
      const input = bridgeWorkCancellation(value);
      if (!isDynamicRecord(value) || typeof value.authority !== "string" || !input)
        throw new Error("Invalid lifecycle closure.");
      return { authority: value.authority, input };
    });
  }

  close(authority: string, input: BridgeWorkCancellation): boolean {
    const commandId = `bridge-work:cancel:${key(authority, input.idempotencyKey)}`;
    const canonical = JSON.stringify({ refs: bridgeWorkKeys(input), reason: input.reason });
    const previous = this.database.commandResult(commandId);
    if (previous !== undefined) {
      if (previous !== canonical) throw new Error("Lifecycle idempotency key conflict.");
      return false;
    }
    this.database.dispatch(
      commandId,
      [
        {
          aggregateType: "bridge-work-cancel",
          aggregateId: key(authority, input.idempotencyKey),
          eventType: "bridge.work.closure",
          payload: { authority, ...input },
        },
      ],
      () => {
        for (const ref of bridgeWorkKeys(input))
          this.database.dispatch(
            `bridge-work:closed:${key(authority, ref)}`,
            [
              {
                aggregateType: "bridge-work",
                aggregateId: key(authority, ref),
                eventType: "bridge.work.closed",
                payload: { authority, ref, reason: input.reason },
              },
            ],
            () => true,
          );
        return canonical;
      },
    );
    return true;
  }
}

function decodeLink(value: unknown): BridgeWorkLink {
  if (
    !isDynamicRecord(value) ||
    typeof value.authority !== "string" ||
    !Array.isArray(value.refs) ||
    !value.refs.every((ref): ref is string => typeof ref === "string") ||
    !isDynamicRecord(value.owner) ||
    (value.owner.kind !== "agent" && value.owner.kind !== "channel") ||
    typeof value.owner.id !== "string" ||
    typeof value.routineId !== "string" ||
    typeof value.runId !== "string" ||
    typeof value.deliveryId !== "string"
  )
    throw new Error("Invalid lifecycle link.");
  return {
    authority: value.authority,
    refs: value.refs,
    owner: { kind: value.owner.kind, id: value.owner.id },
    routineId: value.routineId,
    runId: value.runId,
    deliveryId: value.deliveryId,
  };
}
