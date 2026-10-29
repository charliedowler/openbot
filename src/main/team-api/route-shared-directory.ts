import { INPUT_LIMITS } from "@openbot/contracts/input-limits";
import {
  SHARED_DIRECTORY_CAPABILITY,
  SHARED_DIRECTORY_ROUTES,
} from "@openbot/contracts/team-protocol/shared-directory-v1";
import { sourceText } from "@openbot/i18n/source";
import { runCauseEffect } from "../../backend/effect-boundary";
import { SharedPathRefused } from "../../backend/workspace-paths";
import type { TeamApiAgents } from "./dependencies";
import { HttpError } from "./http-error";
import type { RouteOutcome, TeamApiRequestContext } from "./request-context";
import { readJson, stringField } from "./request-helpers";

export async function routeSharedDirectory(
  context: TeamApiRequestContext,
  agents: Pick<TeamApiAgents, "listSharedDirectory">,
): Promise<RouteOutcome> {
  const { method, url, capabilities, request, json } = context;
  if (method !== "POST" || url.pathname !== SHARED_DIRECTORY_ROUTES.list) return "unmatched";
  if (!capabilities.has(SHARED_DIRECTORY_CAPABILITY)) {
    throw new HttpError(400, sourceText("error.team.sharedDirectoryUnsupported"));
  }
  const path = stringField(await readJson(request), "path", false, INPUT_LIMITS.path);
  try {
    return json(200, await runCauseEffect(agents.listSharedDirectory(path)));
  } catch (error) {
    if (error instanceof SharedPathRefused) {
      throw new HttpError(error.reason === "not-directory" ? 400 : 404, error.message);
    }
    throw error;
  }
}
