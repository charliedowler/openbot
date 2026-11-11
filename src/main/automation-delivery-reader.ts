import type { QueueDelivery } from "@openbot/contracts/ipc";

export interface AutomationDeliveryReader {
  delivery(deliveryId: string): Pick<QueueDelivery, "recipientAgentId" | "status" | "turnId" | "error"> | null;
  turnAnswer(agentId: string, turnId: string): string | null;
}

export function readAutomationDelivery(reader: AutomationDeliveryReader, agentId: string, deliveryId: string) {
  const delivery = reader.delivery(deliveryId);
  if (!delivery || delivery.recipientAgentId !== agentId) return null;
  const ended = ["completed", "failed", "interrupted", "cancelled"].includes(delivery.status);
  return {
    id: deliveryId,
    status: delivery.status,
    turnId: delivery.turnId,
    error: delivery.error,
    answer: ended && delivery.turnId ? reader.turnAnswer(agentId, delivery.turnId) : null,
  };
}
