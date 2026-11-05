import type { ChannelSummary, ServerSummary } from "@openbot/contracts/ipc";
import type { AgentProfile } from "@openbot/ui/data";
import { type SidebarWatchingData, watchConditionKey } from "@openbot/ui/features/sidebar/Sidebar";
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
    tasksOpen: boolean;
    loading: boolean;
    error: string | null;
    snapshot: Snapshot | null;
    snapshotServerId: string | null;
  }>({ open: false, tasksOpen: false, loading: false, error: null, snapshot: null, snapshotServerId: null });
  const supported = () => serverSupportsCapability(deps.server(), "watch-registry-v1");
  const tasksSupported = () => serverSupportsCapability(deps.server(), "watch-registry-v2");
  let request = 0;
  const target = createMemo(() => ({
    id: deps.server()?.id ?? "local",
    connection: deps.server()?.connectionSequence ?? 0,
    status: deps.server()?.state,
    supported: supported(),
    revision: deps.revision(),
    open: state.open || state.tasksOpen,
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
        condition: watch.condition ?? t(watchConditionKey(watch.sourceKind, watch.lastCheckedAt !== undefined)),
        targetUrl: watch.targetUrl,
        checkedAt: watch.lastCheckedAt,
        expiresAt: watch.expiresAt,
        error: watch.lastError,
        summary: watch.summary,
        purpose: watch.purpose,
        receivers: watch.receiverAgentIds?.filter((id) => owners.has(id)).map((id) => ({ id, name: owner(id) })),
      })),
      tasks: snapshot.tasks
        ? {
            generatedAt: snapshot.tasks.generatedAt,
            items: snapshot.tasks.items
              .filter((task) => task.ownerAgentId === undefined || owners.has(task.ownerAgentId))
              .map((task) => ({
                ...task,
                ownerName: task.ownerAgentId === undefined ? undefined : owner(task.ownerAgentId),
              })),
            health: snapshot.tasks.health
              ? {
                  score: snapshot.tasks.health.score,
                  wip: snapshot.tasks.health.wip,
                  wipLimit: snapshot.tasks.health.wipLimit,
                  urgent: snapshot.tasks.health.urgent,
                  reasons: snapshot.tasks.health.reasons,
                  oldest: snapshot.tasks.health.oldest
                    ? {
                        title:
                          snapshot.tasks.items.find((task) => task.id === snapshot.tasks?.health?.oldest?.taskId)
                            ?.title ?? snapshot.tasks.health.oldest.taskId,
                        since: snapshot.tasks.health.oldest.since,
                      }
                    : undefined,
                  longestWaiting: snapshot.tasks.health.longestWaiting
                    ? {
                        who: snapshot.tasks.health.longestWaiting.waitingOn,
                        since: snapshot.tasks.health.longestWaiting.since,
                      }
                    : undefined,
                }
              : undefined,
          }
        : null,
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
    tasksSupported,
    open: () =>
      setState((state) => {
        state.open = true;
      }),
    close: () =>
      setState((state) => {
        state.open = false;
      }),
    openTasks: () =>
      setState((state) => {
        state.tasksOpen = true;
      }),
    closeTasks: () =>
      setState((state) => {
        state.tasksOpen = false;
      }),
    refresh: () => void refresh(),
  };
}
