import { WATCH_REGISTRY_CAPABILITY, WATCH_REGISTRY_ROUTES } from "@openbot/contracts/team-protocol/watch-registry-v1";
import {
  WATCH_REGISTRY_V2_CAPABILITY,
  WATCH_REGISTRY_V2_ROUTES,
} from "@openbot/contracts/team-protocol/watch-registry-v2";
import type { TeamApiAgents } from "./dependencies";
import { HttpError } from "./http-error";
import type { RouteOutcome, TeamApiRequestContext } from "./request-context";

export async function routeWatchRegistry(
  context: TeamApiRequestContext,
  agents: Pick<TeamApiAgents, "listAgents" | "getWatchingSnapshot" | "getWatchingSnapshotV2">,
  hidden: ReadonlySet<string>,
): Promise<RouteOutcome> {
  if (context.method !== "GET") return "unmatched";
  const v2 = context.url.pathname === WATCH_REGISTRY_V2_ROUTES.snapshot;
  if (!v2 && context.url.pathname !== WATCH_REGISTRY_ROUTES.snapshot) return "unmatched";
  const visible = () =>
    new Set(
      agents
        .listAgents()
        .filter((agent) => !hidden.has(agent.id) && context.agentLevel(agent.id) !== "none")
        .map((agent) => agent.id),
    );
  if (v2) {
    if (!context.capabilities.has(WATCH_REGISTRY_V2_CAPABILITY) || !agents.getWatchingSnapshotV2)
      throw new HttpError(400, "Watch registry v2 capability is required.");
    return context.json(
      200,
      agents.getWatchingSnapshotV2(visible(), { includeUnowned: context.member.role !== "member" }),
    );
  }
  if (!context.capabilities.has(WATCH_REGISTRY_CAPABILITY) || !agents.getWatchingSnapshot)
    throw new HttpError(400, "Watch registry capability is required.");
  return context.json(200, agents.getWatchingSnapshot(visible()));
}
