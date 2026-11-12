import { type AgentSummary, isAgentTokenBudget } from "@openbot/contracts/ipc";

export const DEFAULT_SESSION_TOKEN_CAP = 300_000;
export const DEFAULT_TURN_TOKEN_BUDGET = 20_000_000;
export const DEFAULT_TURN_MODEL_CALL_BUDGET = 40;

export function sessionTokenCap(agent?: Pick<AgentSummary, "sessionTokenCap">): number {
  const configured = Number(process.env.OPENBOT_SESSION_TOKEN_CAP);
  return agent?.sessionTokenCap ?? (isAgentTokenBudget(configured) ? configured : DEFAULT_SESSION_TOKEN_CAP);
}

export function turnTokenBudget(agent?: Pick<AgentSummary, "turnTokenBudget">): number {
  const configured = Number(process.env.OPENBOT_TURN_TOKEN_BUDGET);
  return agent?.turnTokenBudget ?? (isAgentTokenBudget(configured) ? configured : DEFAULT_TURN_TOKEN_BUDGET);
}

export function turnModelCallBudget(): number {
  const configured = Number(process.env.OPENBOT_TURN_MODEL_CALL_BUDGET);
  return Number.isSafeInteger(configured) && configured >= 1 && configured <= 10_000
    ? configured
    : DEFAULT_TURN_MODEL_CALL_BUDGET;
}
