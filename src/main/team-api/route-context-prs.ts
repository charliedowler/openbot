import { parseContextPullRequestsInput } from "@openbot/contracts/context-prs";
import { CONTEXT_PRS_CAPABILITY, CONTEXT_PRS_ROUTES } from "@openbot/contracts/team-protocol/context-prs-v1";
import { sourceText } from "@openbot/i18n/source";
import type { ChannelService } from "../../backend/channel-service";
import { runCauseEffect } from "../../backend/effect-boundary";
import type { TeamApiAgents, TeamApiOptions } from "./dependencies";
import { HttpError } from "./http-error";
import type { RouteOutcome, TeamApiRequestContext } from "./request-context";
import { readJson } from "./request-helpers";
export async function routeContextPrs(
  context: TeamApiRequestContext,
  service: TeamApiOptions["contextPrs"],
  channels: ChannelService | undefined,
  agents: Pick<TeamApiAgents, "listAgents">,
  hidden: () => ReadonlySet<string>,
): Promise<RouteOutcome> {
  if (context.method !== "POST" || context.url.pathname !== CONTEXT_PRS_ROUTES.read) return "unmatched";
  if (!context.capabilities.has(CONTEXT_PRS_CAPABILITY) || !service)
    throw new HttpError(400, "Context PR capability is required.");
  const input = parseContextPullRequestsInput(await readJson(context.request));
  const authorize = () => {
    const known = new Set(agents.listAgents().map((agent) => agent.id));
    const allowed = (id: string) => known.has(id) && !hidden().has(id) && context.agentLevel(id) !== "none";
    if (input.scope.kind === "agent") {
      if (!allowed(input.scope.id)) throw new HttpError(404, sourceText("error.team.agentNotFound"));
    } else {
      if (!channels?.store.exists(input.scope.id)) throw new HttpError(404, sourceText("error.team.agentNotFound"));
      if (channels.store.get(input.scope.id).members.some((member) => !allowed(member.agentId)))
        throw new HttpError(404, sourceText("error.team.agentNotFound"));
    }
  };
  authorize();
  const result = await runCauseEffect(service.read(input));
  authorize();
  return context.json(200, result);
}
