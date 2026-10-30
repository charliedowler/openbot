import {
  Button,
  Checkbox,
  ChevronDown,
  Dialog,
  ExternalLink,
  Eye,
  IconButton,
  Input,
  RefreshCw,
  Tabs,
  X,
} from "@openbot/ui";
import { createMemo, createSignal, createStore, createUniqueId, For, Show } from "solid-js";
import { useText } from "../../text";

export interface SidebarWatchingItem {
  id: string;
  ownerId: string;
  ownerName: string;
  channelId?: string | undefined;
  channelName?: string | undefined;
  title: string;
  sourceKind: string;
  status: "active" | "paused" | "completed" | "expired" | "error" | "waiting" | "received";
  condition?: string | undefined;
  targetUrl?: string | undefined;
  checkedAt?: string | undefined;
  startedAt?: string | undefined;
  expiresAt?: string | undefined;
  error?: string | undefined;
}

export interface SidebarWatchingData {
  generatedAt: string | null;
  watches: SidebarWatchingItem[];
  waits: SidebarWatchingItem[];
}

const STATUS_KEYS = {
  active: "sidebar.watching.status.active",
  paused: "sidebar.watching.status.paused",
  completed: "sidebar.watching.status.completed",
  expired: "sidebar.watching.status.expired",
  error: "sidebar.watching.status.error",
  waiting: "sidebar.watching.status.waiting",
  received: "sidebar.watching.status.received",
} as const;

function current(item: SidebarWatchingItem) {
  return item.status !== "expired" && item.status !== "completed" && item.status !== "paused";
}

function WatchingRow(props: { item: SidebarWatchingItem; onOpenSource: (url: string) => void }) {
  const { t, format } = useText();
  const [expanded, setExpanded] = createSignal(false);
  const detailsId = createUniqueId();
  const date = (value: string) =>
    format.date(new Date(value), { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  return (
    <li class="sidebar-watching-item" data-watch-status={props.item.status}>
      <Button
        variant="ghost"
        class="sidebar-watching-row"
        aria-expanded={expanded() ? "true" : "false"}
        aria-controls={detailsId}
        onClick={() => setExpanded((value) => !value)}
      >
        <ChevronDown class="sidebar-watching-chevron" aria-hidden="true" />
        <span class="sidebar-watching-item-title">{props.item.title}</span>
        <span class="sidebar-watching-state">{t(STATUS_KEYS[props.item.status])}</span>
      </Button>
      <Show when={current(props.item) && props.item.error}>
        <p class="sidebar-watching-error sidebar-watching-row-error">{props.item.error}</p>
      </Show>
      <Show when={expanded()}>
        <div id={detailsId} class="sidebar-watching-details">
          <Show when={props.item.condition}>
            <p class="sidebar-watching-condition">{props.item.condition}</p>
          </Show>
          <div class="sidebar-watching-meta">
            <span>{props.item.sourceKind}</span>
            <Show when={props.item.checkedAt}>
              {(value) => <span>{t("sidebar.watching.checked", { date: date(value()) })}</span>}
            </Show>
            <Show when={props.item.startedAt}>
              {(value) => <span>{t("sidebar.watching.started", { date: date(value()) })}</span>}
            </Show>
            <Show when={props.item.expiresAt}>
              {(value) => <span>{t("sidebar.watching.expires", { date: date(value()) })}</span>}
            </Show>
            <Show when={props.item.targetUrl}>
              {(url) => (
                <Button variant="ghost" class="sidebar-watching-source" onClick={() => props.onOpenSource(url())}>
                  {t("sidebar.watching.openSource")}
                  <ExternalLink class="size-3" aria-hidden="true" />
                </Button>
              )}
            </Show>
          </div>
        </div>
      </Show>
    </li>
  );
}

export function SidebarWatchingDialog(props: {
  open: boolean;
  loading: boolean;
  supported: boolean;
  error: string | null;
  data: SidebarWatchingData | null;
  initialOwnerId?: string | undefined;
  onClose: () => void;
  onRefresh: () => void;
  onOpenChat: (ownerId: string, channelId?: string) => void;
  onOpenSource: (url: string) => void;
}) {
  const { t, format } = useText();
  const [view, setView] = createStore<{ query: string; ownerId: string | null; includeInactive: boolean }>({
    query: "",
    ownerId: null,
    includeInactive: false,
  });
  const inactiveId = createUniqueId();
  const available = createMemo(() => [...(props.data?.watches ?? []), ...(props.data?.waits ?? [])]);
  const owners = createMemo(() => {
    const grouped = new Map<string, { id: string; name: string; count: number }>();
    for (const item of available()) {
      if (!view.includeInactive && !current(item)) continue;
      const group = grouped.get(item.ownerId) ?? { id: item.ownerId, name: item.ownerName, count: 0 };
      group.count += 1;
      grouped.set(group.id, group);
    }
    return [...grouped.values()].sort((a, b) => a.name.localeCompare(b.name));
  });
  const tabs = createMemo(() => [
    { id: "all", name: t("sidebar.watching.allAgents"), count: owners().reduce((sum, owner) => sum + owner.count, 0) },
    ...owners(),
  ]);
  const selected = () => {
    const requested = view.ownerId ?? props.initialOwnerId ?? "all";
    return tabs().some((owner) => owner.id === requested) ? requested : "all";
  };
  const sections = createMemo(() => {
    const search = view.query.trim().toLowerCase();
    return [
      { kind: "sources", title: t("sidebar.watching.sources"), items: props.data?.watches ?? [] },
      { kind: "waits", title: t("sidebar.watching.waits"), items: props.data?.waits ?? [] },
    ].map((section) => ({
      ...section,
      groups: Array.from(
        section.items.reduce((groups, item) => {
          if (!view.includeInactive && !current(item)) return groups;
          if (selected() !== "all" && item.ownerId !== selected()) return groups;
          if (
            search &&
            ![item.title, item.ownerName, item.channelName, item.sourceKind, item.condition, item.error]
              .filter(Boolean)
              .join(" ")
              .toLowerCase()
              .includes(search)
          )
            return groups;
          const key = item.channelId ?? item.ownerId;
          const group = groups.get(key) ?? { id: key, name: item.channelName ?? item.ownerName, items: [] };
          group.items.push(item);
          groups.set(key, group);
          return groups;
        }, new Map<string, { id: string; name: string; items: SidebarWatchingItem[] }>()),
      ).map(([, group]) => ({
        ...group,
        items: group.items.sort(
          (a, b) => Number(b.status === "error") - Number(a.status === "error") || a.title.localeCompare(b.title),
        ),
      })),
    }));
  });
  const inactiveCount = () => available().filter((item) => !current(item)).length;
  const stale = () => !!props.data?.generatedAt && Date.now() - Date.parse(props.data.generatedAt) > 10 * 60_000;
  const date = (value: string) =>
    format.date(new Date(value), { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  return (
    <Dialog.Root open={props.open} onOpenChange={(open) => !open && props.onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay class="sidebar-watching-backdrop">
          <Dialog.Content
            as="section"
            class="sidebar-watching-dialog"
            aria-busy={props.loading ? "true" : undefined}
            onEscapeKeyDown={(event) => {
              event.preventDefault();
              props.onClose();
            }}
          >
            <header class="sidebar-watching-header">
              <div class="sidebar-watching-heading">
                <Eye class="size-5" aria-hidden="true" />
                <Dialog.Title as="h2">{t("sidebar.watching.title")}</Dialog.Title>
              </div>
              <div class="sidebar-watching-actions">
                <IconButton
                  variant="ghost"
                  label={t("sidebar.watching.refresh")}
                  disabled={props.loading || !props.supported}
                  onClick={props.onRefresh}
                >
                  <RefreshCw />
                </IconButton>
                <IconButton variant="ghost" label={t("common.close")} onClick={props.onClose}>
                  <X />
                </IconButton>
              </div>
            </header>
            <Dialog.Description class="sidebar-watching-description">
              {t("sidebar.watching.description")}
            </Dialog.Description>
            <Show
              when={props.supported}
              fallback={<p class="sidebar-watching-notice">{t("sidebar.watching.unsupported")}</p>}
            >
              <div class="sidebar-watching-tools">
                <Input
                  type="search"
                  aria-label={t("sidebar.watching.filter")}
                  placeholder={t("sidebar.watching.filter")}
                  value={view.query}
                  onValueChange={(query) =>
                    setView((state) => {
                      state.query = query;
                    })
                  }
                />
                <label class="sidebar-watching-history" for={inactiveId}>
                  <Checkbox
                    id={inactiveId}
                    checked={view.includeInactive}
                    onChange={(event) =>
                      setView((state) => {
                        state.includeInactive = event.currentTarget.checked;
                      })
                    }
                  />
                  <span>{t("sidebar.watching.includeInactive", { count: inactiveCount() })}</span>
                </label>
              </div>
              <p class="sidebar-watching-updated" role="status" data-stale={stale() ? "true" : undefined}>
                {props.loading
                  ? t("sidebar.watching.loading")
                  : props.data?.generatedAt
                    ? t(stale() ? "sidebar.watching.staleSnapshot" : "sidebar.watching.updated", {
                        date: date(props.data.generatedAt),
                      })
                    : t("sidebar.watching.noSnapshot")}
              </p>
              <Show when={props.error}>
                {(error) => (
                  <p class="sidebar-watching-error" role="alert">
                    {error()}
                  </p>
                )}
              </Show>
              <Show when={props.data}>
                <Tabs.Root
                  class="sidebar-watching-layout"
                  orientation="vertical"
                  value={selected()}
                  onChange={(ownerId) =>
                    setView((state) => {
                      state.ownerId = ownerId;
                    })
                  }
                >
                  <Tabs.List class="sidebar-watching-agents" aria-label={t("sidebar.watching.agents")}>
                    <For each={tabs()}>
                      {(owner) => (
                        <Tabs.Trigger class="sidebar-watching-agent" value={owner.id}>
                          <span>{owner.name}</span>
                          <span class="sidebar-watching-count">{owner.count}</span>
                        </Tabs.Trigger>
                      )}
                    </For>
                  </Tabs.List>
                  <For each={tabs()}>
                    {(owner) => (
                      <Tabs.Content value={owner.id} class="sidebar-watching-scroll">
                        <For each={sections()}>
                          {(section) => (
                            <section class="sidebar-watching-section" aria-label={section.title}>
                              <h3>{section.title}</h3>
                              <Show
                                when={section.groups.length > 0}
                                fallback={
                                  <p class="sidebar-watching-empty">
                                    {view.query.trim()
                                      ? t("sidebar.watching.noMatches")
                                      : section.kind === "sources"
                                        ? t("sidebar.watching.noSources")
                                        : t("sidebar.watching.noWaits")}
                                  </p>
                                }
                              >
                                <For each={section.groups}>
                                  {(group) => (
                                    <div class="sidebar-watching-group">
                                      <Button
                                        variant="ghost"
                                        class="sidebar-watching-owner"
                                        onClick={() => {
                                          const first = group.items[0];
                                          if (first) props.onOpenChat(first.ownerId, first.channelId);
                                        }}
                                      >
                                        {group.name}
                                      </Button>
                                      <ul>
                                        <For each={group.items}>
                                          {(item) => <WatchingRow item={item} onOpenSource={props.onOpenSource} />}
                                        </For>
                                      </ul>
                                    </div>
                                  )}
                                </For>
                              </Show>
                            </section>
                          )}
                        </For>
                      </Tabs.Content>
                    )}
                  </For>
                </Tabs.Root>
              </Show>
            </Show>
          </Dialog.Content>
        </Dialog.Overlay>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
