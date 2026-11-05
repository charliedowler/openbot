import { PullRequestList } from "@openbot/ui/features/conversation/SidePanel";
import { useText } from "@openbot/ui/text";
import { createMemo } from "solid-js";
import { useAgents } from "../agents/agents-context";
import { useChannels } from "../channels/channels-context";
import { serverSupportsCapability } from "../servers/server-capabilities";
import { useSidebar } from "../sidebar/sidebar-context";
import { createContextPrsView, prefetchContextPrs } from "./context-prs-view";
import { conversationRuntime } from "./conversation-runtime";
import { useConversationViewScope } from "./conversation-scope";

function useContextPrSource() {
  const { props, channel } = useConversationViewScope();
  const { agentList } = useAgents();
  const channels = useChannels();
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
  const read = (refresh: boolean) => {
    const current = target();
    if (!current) return Promise.reject(new Error(t("conversation.pullRequests.failed")));
    const api = props.runtime?.contextPrs ?? window.openbot.contextPrs;
    return api.read({ scope: current.scope, refresh }, current.serverId);
  };
  return { props, target, supported, read };
}

export function ContextPullRequestsPrefetch() {
  const { props, channel } = useConversationViewScope();
  const supported = () =>
    props.runtime ? props.runtime.contextPrs !== undefined : serverSupportsCapability(props.server, "context-prs-v1");
  const target = () => {
    const serverId = props.server?.id ?? "local";
    if (channel) return channel.readOnly() ? null : { scope: { kind: "channel" as const, id: channel.id }, serverId };
    const agentId = props.agent?.id;
    return agentId ? { scope: { kind: "agent" as const, id: agentId }, serverId } : null;
  };
  prefetchContextPrs({
    target,
    supported,
    read: (refresh) => {
      const current = target();
      const api = props.runtime?.contextPrs ?? window.openbot?.contextPrs;
      if (!current || !api) return Promise.reject(new Error("Context PRs are unavailable."));
      return api.read({ scope: current.scope, refresh }, current.serverId);
    },
  });
  return null;
}

export function ContextPullRequestsPanel() {
  const { props, target, supported, read } = useContextPrSource();
  const sidebar = useSidebar();
  const { t } = useText();
  const view = createContextPrsView({
    target,
    supported,
    revision: () => `${sidebar.watchingRevision()}:${props.messages.length}:${props.activeTurnId ?? "idle"}`,
    read,
  });
  return (
    <aside class="browser-panel context-pr-panel" aria-label={t("conversation.sidePanel.pane.pullRequests")}>
      <PullRequestList
        data={view.data()}
        loading={view.loading()}
        updating={view.updating()}
        supported={supported()}
        error={view.state.error}
        onRefresh={view.refresh}
        onOpen={(url) => void conversationRuntime(props).openUrl(url)}
      />
    </aside>
  );
}
