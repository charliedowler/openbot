import { INPUT_LIMITS } from "@openbot/contracts/input-limits";
import { type DynamicRecord, isString } from "@openbot/contracts/runtime-values";

export const ROUTINE_FORWARD_ALLOWLIST_ENV = "OPENBOT_ROUTINE_FORWARD_ALLOWLIST";

const AGENT_ID_MAX_LENGTH = 200;

export type RoutineForwardAllowlist = ReadonlyMap<string, ReadonlySet<string>>;

export interface WebhookForward {
  agentId: string;
  instruction: string | null;
}

export function parseRoutineForwardAllowlist(text: string | undefined): RoutineForwardAllowlist {
  const allowlist = new Map<string, Set<string>>();
  for (const entry of (text ?? "").split(/[\s;]+/u)) {
    const separator = entry.indexOf(":");
    if (separator <= 0) continue;
    const source = entry.slice(0, separator).trim();
    const targets = entry
      .slice(separator + 1)
      .split(",")
      .map((target) => target.trim())
      .filter(Boolean);
    if (!source || targets.length === 0) continue;
    const allowed = allowlist.get(source) ?? new Set<string>();
    for (const target of targets) allowed.add(target);
    allowlist.set(source, allowed);
  }
  return allowlist;
}

export function forwardAllowed(
  allowlist: RoutineForwardAllowlist,
  source: { routineId: string; ownerAgentId: string },
  targetAgentId: string,
): boolean {
  return (
    allowlist.get(source.routineId)?.has(targetAgentId) === true ||
    allowlist.get(source.ownerAgentId)?.has(targetAgentId) === true
  );
}

export function readWebhookForward(value: DynamicRecord): WebhookForward | "invalid" | undefined {
  const { forwardToAgentId, instruction } = value;
  if (forwardToAgentId === undefined) return undefined;
  if (!isString(forwardToAgentId) || !forwardToAgentId.trim() || forwardToAgentId.length > AGENT_ID_MAX_LENGTH)
    return "invalid";
  if (
    instruction !== undefined &&
    (!isString(instruction) || !instruction.trim() || instruction.length > INPUT_LIMITS.routineInstruction)
  )
    return "invalid";
  return { agentId: forwardToAgentId.trim(), instruction: instruction === undefined ? null : instruction };
}
