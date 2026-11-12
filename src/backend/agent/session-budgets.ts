import { type AgentSummary, isAgentTokenBudget } from "@openbot/contracts/ipc";

export const DEFAULT_SESSION_TOKEN_CAP = 200_000;
export const DEFAULT_TURN_TOKEN_BUDGET = 1_000_000;

export function sessionTokenCap(agent?: Pick<AgentSummary, "sessionTokenCap">): number {
  const configured = Number(process.env.OPENBOT_SESSION_TOKEN_CAP);
  return agent?.sessionTokenCap ?? (isAgentTokenBudget(configured) ? configured : DEFAULT_SESSION_TOKEN_CAP);
}

export function turnTokenBudget(agent?: Pick<AgentSummary, "turnTokenBudget">): number {
  const configured = Number(process.env.OPENBOT_TURN_TOKEN_BUDGET);
  return agent?.turnTokenBudget ?? (isAgentTokenBudget(configured) ? configured : DEFAULT_TURN_TOKEN_BUDGET);
}
