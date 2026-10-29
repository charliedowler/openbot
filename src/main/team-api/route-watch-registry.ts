import { WATCH_REGISTRY_CAPABILITY, WATCH_REGISTRY_ROUTES } from "@openbot/contracts/team-protocol/watch-registry-v1";
import type { TeamApiAgents } from "./dependencies";
import { HttpError } from "./http-error";
import type { RouteOutcome, TeamApiRequestContext } from "./request-context";

export async function routeWatchRegistry(
  context: TeamApiRequestContext,
  agents: Pick<TeamApiAgents, "listAgents" | "getWatchingSnapshot">,
  hidden: ReadonlySet<string>,
): Promise<RouteOutcome> {
  if (context.method !== "GET" || context.url.pathname !== WATCH_REGISTRY_ROUTES.snapshot) return "unmatched";
  if (!context.capabilities.has(WATCH_REGISTRY_CAPABILITY) || !agents.getWatchingSnapshot)
    throw new HttpError(400, "Watch registry capability is required.");
  const visible = new Set(
    agents
      .listAgents()
      .filter((agent) => !hidden.has(agent.id) && context.agentLevel(agent.id) !== "none")
      .map((agent) => agent.id),
  );
  return context.json(200, agents.getWatchingSnapshot(visible));
}
