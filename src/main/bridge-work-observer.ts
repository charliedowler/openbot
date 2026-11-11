import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { BRIDGE_WORK_OBSERVE_IDS_LIMIT, BridgeWorkStore } from "../backend/bridge-work-store";
import type { OpenBotDatabase } from "../backend/openbot-database";
import { type AutomationDeliveryReader, readAutomationDelivery } from "./automation-delivery-reader";

export interface BridgeWorkObservation {
  deliveryId: string;
  runId: string | null;
  nativeDeliveryId: string | null;
  recipient: { agentId: string; name: string } | null;
  nativeStatus: string;
  turnId: string | null;
  startedAt: string | null;
  terminalAt: string | null;
  hasAnswer: boolean;
  executor: { classification: "not-observed" | "none" | "tool-error"; completedCalls: number; failedCalls: number };
}

export function decodeObservationIds(value: unknown): string[] | null {
  if (
    !isDynamicRecord(value) ||
    !Array.isArray(value.deliveryIds) ||
    value.deliveryIds.length < 1 ||
    value.deliveryIds.length > BRIDGE_WORK_OBSERVE_IDS_LIMIT ||
    !value.deliveryIds.every(
      (id): id is string => typeof id === "string" && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$/u.test(id),
    )
  )
    return null;
  return [...new Set(value.deliveryIds)];
}

export class BridgeWorkObserver {
  readonly #store: BridgeWorkStore;
  constructor(
    readonly database: OpenBotDatabase,
    readonly reader: AutomationDeliveryReader,
    readonly recipientName: (agentId: string) => string | null,
  ) {
    this.#store = new BridgeWorkStore(database);
  }

  observe(authority: string, deliveryIds: string[]) {
    const observations = deliveryIds.map((deliveryId): BridgeWorkObservation => {
      const result: BridgeWorkObservation = {
        deliveryId,
        runId: null,
        nativeDeliveryId: null,
        recipient: null,
        nativeStatus: "missing-receipt",
        turnId: null,
        startedAt: null,
        terminalAt: null,
        hasAnswer: false,
        executor: { classification: "not-observed", completedCalls: 0, failedCalls: 0 },
      };
      const links = this.#store.links(authority, `delivery:${deliveryId}`);
      if (links.length !== 1) {
        if (links.length > 1) result.nativeStatus = "ambiguous-receipt";
        return result;
      }
      const link = links[0];
      if (!link || link.deliveryId !== deliveryId || link.owner.kind !== "agent") return result;
      const run = this.database.connection
        .prepare("SELECT agent_id, delivery_id FROM projection_routine_runs WHERE run_id = ?")
        .get(link.runId);
      if (!isDynamicRecord(run) || typeof run.agent_id !== "string") return result;
      result.runId = link.runId;
      result.recipient = { agentId: run.agent_id, name: (this.recipientName(run.agent_id) ?? "").slice(0, 120) };
      result.nativeStatus = "pending-delivery";
      if (typeof run.delivery_id !== "string") return result;
      result.nativeDeliveryId = run.delivery_id;
      const delivery = readAutomationDelivery(this.reader, run.agent_id, run.delivery_id);
      if (!delivery) {
        result.nativeStatus = "missing-delivery";
        return result;
      }
      result.nativeStatus = delivery.status;
      result.turnId = delivery.turnId;
      result.hasAnswer = delivery.answer !== null;
      if (!delivery.turnId) return result;
      const turn = this.database.connection
        .prepare(
          "SELECT turn.started_at, turn.completed_at FROM projection_turns turn JOIN projection_threads thread ON thread.thread_id = turn.thread_id WHERE turn.turn_id = ? AND thread.agent_id = ?",
        )
        .get(delivery.turnId, run.agent_id);
      if (!isDynamicRecord(turn) || typeof turn.started_at !== "string") return result;
      result.startedAt = turn.started_at;
      result.terminalAt = typeof turn.completed_at === "string" ? turn.completed_at : null;
      const outcomes = this.#store.executorOutcomes(run.agent_id, delivery.turnId);
      const failedCalls = outcomes.filter((outcome) => outcome.failed).length;
      result.executor = {
        classification: failedCalls ? "tool-error" : outcomes.length ? "none" : "not-observed",
        completedCalls: outcomes.length,
        failedCalls,
      };
      return result;
    });
    return { version: 1, generatedAt: new Date().toISOString(), observations };
  }
}
