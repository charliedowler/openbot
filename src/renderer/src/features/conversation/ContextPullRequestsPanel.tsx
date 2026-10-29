import { PullRequestList } from "@openbot/ui/features/conversation/SidePanel";
import { useText } from "@openbot/ui/text";
import { createMemo } from "solid-js";
import { useAgents } from "../agents/agents-context";
import { useChannels } from "../channels/channels-context";
import { serverSupportsCapability } from "../servers/server-capabilities";
import { useSidebar } from "../sidebar/sidebar-context";
import { createContextPrsView } from "./context-prs-view";
import { conversationRuntime } from "./conversation-runtime";
import { useConversationViewScope } from "./conversation-scope";

export function ContextPullRequestsPanel() {
  const { props, channel } = useConversationViewScope();
  const { agentList } = useAgents();
  const channels = useChannels();
  const sidebar = useSidebar();
  const { t } = useText();
  const supported = () =>
    props.runtime ? props.runtime.contextPrs !== undefined : serverSupportsCapability(props.server, "context-prs-v1");
  const target = createMemo(() => {
    const agents = new Set(agentList().map((agent) => agent.id));
    if (channel) {
      const current = channels.state.channels.find((entry) => entry.id === channel.id);
      if (
        !current ||
        current.archived ||
        channel.readOnly() ||
        current.members.some((member) => !agents.has(member.agentId))
      )
        return null;
      return {
        scope: { kind: "channel" as const, id: channel.id },
        serverId: props.server?.id ?? "local",
        authorization: JSON.stringify(current.members.map((member) => member.agentId).sort()),
      };
    }
    const agentId = props.agent?.id;
    return agentId && agents.has(agentId)
      ? {
          scope: { kind: "agent" as const, id: agentId },
          serverId: props.server?.id ?? "local",
          authorization: agentId,
        }
      : null;
  });
  const view = createContextPrsView({
    target,
    supported,
    revision: () => `${sidebar.watchingRevision()}:${props.messages.length}:${props.activeTurnId ?? "idle"}`,
    read: (refresh) => {
      const current = target();
      if (!current) return Promise.reject(new Error(t("conversation.pullRequests.failed")));
      const api = props.runtime?.contextPrs ?? window.openbot.contextPrs;
      return api.read({ scope: current.scope, refresh }, current.serverId);
    },
  });
  return (
    <aside class="browser-panel context-pr-panel" aria-label={t("conversation.sidePanel.pane.pullRequests")}>
      <PullRequestList
        data={view.data()}
        loading={view.state.loading}
        supported={supported()}
        error={view.state.error}
        onRefresh={view.refresh}
        onOpen={(url) => void conversationRuntime(props).openUrl(url)}
      />
    </aside>
  );
}
