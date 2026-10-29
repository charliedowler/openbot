import { parseContextPullRequestsInput, unsupportedContextPullRequests } from "@openbot/contracts/context-prs";
import { CONTEXT_PRS_CAPABILITY, CONTEXT_PRS_ROUTES } from "@openbot/contracts/team-protocol/context-prs-v1";
import { runCauseEffect } from "../../backend/effect-boundary";
import { decodeContextPullRequestsFromHost } from "../context-pr-decoding";
import type { ContextPrService } from "../context-pr-service";
import type { RemoteServerManager } from "../remote-server-manager";
import type { IpcGroupHandlers } from "./define-ipc-group";
import { scopedHandler } from "./scoped-handler";
export interface ContextPrsIpcDependencies {
  contextPrs: Pick<ContextPrService, "read">;
  remoteServers: Pick<RemoteServerManager, "supportsCapability" | "request">;
}
export function contextPrsIpcHandlers({
  contextPrs,
  remoteServers,
}: ContextPrsIpcDependencies): Pick<IpcGroupHandlers, "contextPrs"> {
  return {
    contextPrs: {
      read: scopedHandler(parseContextPullRequestsInput, {
        local: (input) => runCauseEffect(contextPrs.read(input)),
        remote: (input, serverId) =>
          remoteServers.supportsCapability(serverId, CONTEXT_PRS_CAPABILITY)
            ? runCauseEffect(
                remoteServers.request(serverId, CONTEXT_PRS_ROUTES.read, decodeContextPullRequestsFromHost, {
                  method: "POST",
                  body: input,
                }),
              )
            : Promise.resolve(unsupportedContextPullRequests(input.scope)),
      }),
    },
  };
}
