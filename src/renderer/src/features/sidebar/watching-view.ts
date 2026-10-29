import type { ChannelSummary, ServerSummary } from "@openbot/contracts/ipc";
import type { AgentProfile } from "@openbot/ui/data";
import type { SidebarWatchingData } from "@openbot/ui/features/sidebar/Sidebar";
import { useText } from "@openbot/ui/text";
import { createEffect, createMemo, createStore, onSettled, untrack } from "solid-js";
import { serverSupportsCapability } from "../servers/server-capabilities";
import { sidebarPort } from "./sidebar-port";

export function createSidebarWatching(deps: {
  server: () => ServerSummary | undefined;
  revision: () => number;
  agents: () => AgentProfile[];
  channels: () => ChannelSummary[];
}) {
  const { t, errorMessage } = useText();
  type Snapshot = Awaited<ReturnType<ReturnType<typeof sidebarPort>["agent"]["getWatchingSnapshot"]>>;
  const [state, setState] = createStore<{
    open: boolean;
    loading: boolean;
    error: string | null;
    snapshot: Snapshot | null;
    snapshotServerId: string | null;
  }>({ open: false, loading: false, error: null, snapshot: null, snapshotServerId: null });
  const supported = () => serverSupportsCapability(deps.server(), "watch-registry-v1");
  let request = 0;
  const target = createMemo(() => ({
    id: deps.server()?.id ?? "local",
    connection: deps.server()?.connectionSequence ?? 0,
    status: deps.server()?.state,
    supported: supported(),
    revision: deps.revision(),
    open: state.open,
  }));

  async function refresh(): Promise<void> {
    const current = untrack(target);
    if (!current.open || !current.supported) return;
    const reading = ++request;
    setState((state) => {
      state.loading = true;
      state.error = null;
    });
    try {
      const snapshot = await sidebarPort().agent.getWatchingSnapshot(current.id);
      if (reading !== request || untrack(target).id !== current.id) return;
      setState((state) => {
        state.snapshot = snapshot;
        state.snapshotServerId = current.id;
        state.loading = false;
      });
    } catch (error) {
      if (reading !== request || untrack(target).id !== current.id) return;
      setState((state) => {
        state.loading = false;
        state.error = errorMessage(error, t("sidebar.watching.failed"));
      });
    }
  }

  createEffect(target, (current, previous) => {
    request += 1;
    if (!previous || current.id !== previous.id || !current.supported) {
      setState((state) => {
        state.snapshot = null;
        state.snapshotServerId = null;
        state.error = null;
        state.loading = false;
      });
    }
    if (current.open && current.supported) void refresh();
  });
  onSettled(() => () => {
    request += 1;
  });

  const data = createMemo<SidebarWatchingData | null>(() => {
    const snapshot = state.snapshot;
    if (!snapshot || state.snapshotServerId !== (deps.server()?.id ?? "local")) return null;
    const owners = new Map(deps.agents().map((agent) => [agent.id, agent]));
    const channels = new Map(deps.channels().map((channel) => [channel.id, channel]));
    const visible = (item: { ownerAgentId: string; channelId?: string }) => {
      if (!owners.has(item.ownerAgentId)) return false;
      if (item.channelId === undefined) return true;
      const origin = channels.get(item.channelId);
      if (!origin) return false;
      return (
        origin.members.some((member) => member.agentId === item.ownerAgentId) &&
        origin.members.every((member) => owners.has(member.agentId))
      );
    };
    const owner = (id: string) => owners.get(id)?.name ?? t("sidebar.watching.unknownOwner");
    const channel = (id: string | undefined) => (id ? channels.get(id)?.name : undefined);
    return {
      generatedAt: snapshot.registry?.generatedAt ?? null,
      watches: (snapshot.registry?.watches ?? []).filter(visible).map((watch) => ({
        id: `${watch.publisherId}:${watch.id}`,
        ownerId: watch.ownerAgentId,
        ownerName: owner(watch.ownerAgentId),
        channelId: watch.channelId,
        channelName: channel(watch.channelId),
        title: watch.title,
        sourceKind: watch.sourceKind,
        status: watch.status,
        condition: watch.condition,
        targetUrl: watch.targetUrl,
        checkedAt: watch.lastCheckedAt,
        expiresAt: watch.expiresAt,
        error: watch.lastError,
      })),
      waits: snapshot.waits.filter(visible).map((wait) => ({
        id: wait.id,
        ownerId: wait.ownerAgentId,
        ownerName: owner(wait.ownerAgentId),
        channelId: wait.channelId,
        channelName: channel(wait.channelId),
        title: wait.note,
        sourceKind: t("sidebar.watching.nativeWait"),
        status: wait.wokenAt ? "received" : "waiting",
        startedAt: wait.createdAt,
      })),
    };
  });

  return {
    state,
    data,
    supported,
    open: () =>
      setState((state) => {
        state.open = true;
      }),
    close: () =>
      setState((state) => {
        state.open = false;
      }),
    refresh: () => void refresh(),
  };
}
