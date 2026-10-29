import { Button, Dialog, ExternalLink, Eye, IconButton, Input, RefreshCw, X } from "@openbot/ui";
import { createMemo, createSignal, For, Show } from "solid-js";
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

export function SidebarWatchingDialog(props: {
  open: boolean;
  loading: boolean;
  supported: boolean;
  error: string | null;
  data: SidebarWatchingData | null;
  onClose: () => void;
  onRefresh: () => void;
  onOpenChat: (ownerId: string, channelId?: string) => void;
  onOpenSource: (url: string) => void;
}) {
  const { t, format } = useText();
  const [query, setQuery] = createSignal("");
  const sections = createMemo(() => {
    const search = query().trim().toLowerCase();
    return [
      { kind: "sources", title: t("sidebar.watching.sources"), items: props.data?.watches ?? [] },
      { kind: "waits", title: t("sidebar.watching.waits"), items: props.data?.waits ?? [] },
    ].map((section) => ({
      ...section,
      groups: Array.from(
        section.items.reduce((groups, item) => {
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
      ).map(([, group]) => group),
    }));
  });
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
                  value={query()}
                  onValueChange={setQuery}
                />
                <p class="sidebar-watching-updated" role="status">
                  {props.loading
                    ? t("sidebar.watching.loading")
                    : props.data?.generatedAt
                      ? t("sidebar.watching.updated", { date: date(props.data.generatedAt) })
                      : t("sidebar.watching.noSnapshot")}
                </p>
              </div>
              <Show when={props.error}>
                {(error) => (
                  <p class="sidebar-watching-error" role="alert">
                    {error()}
                  </p>
                )}
              </Show>
              <Show when={props.data}>
                <div class="sidebar-watching-scroll">
                  <For each={sections()}>
                    {(section) => (
                      <section class="sidebar-watching-section" aria-label={section.title}>
                        <h3>{section.title}</h3>
                        <Show
                          when={section.groups.length > 0}
                          fallback={
                            <p class="sidebar-watching-empty">
                              {query().trim()
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
                                    {(item) => (
                                      <li class="sidebar-watching-item" data-watch-status={item.status}>
                                        <div class="sidebar-watching-item-heading">
                                          <strong>{item.title}</strong>
                                          <span class="sidebar-watching-state">{t(STATUS_KEYS[item.status])}</span>
                                        </div>
                                        <Show when={item.condition}>
                                          <p class="sidebar-watching-condition">{item.condition}</p>
                                        </Show>
                                        <div class="sidebar-watching-meta">
                                          <span>{item.sourceKind}</span>
                                          <Show when={item.channelName}>
                                            <span>{item.ownerName}</span>
                                          </Show>
                                          <Show when={item.checkedAt}>
                                            {(checkedAt) => (
                                              <span>{t("sidebar.watching.checked", { date: date(checkedAt()) })}</span>
                                            )}
                                          </Show>
                                          <Show when={item.startedAt}>
                                            {(startedAt) => (
                                              <span>{t("sidebar.watching.started", { date: date(startedAt()) })}</span>
                                            )}
                                          </Show>
                                          <Show when={item.expiresAt}>
                                            {(expiresAt) => (
                                              <span>{t("sidebar.watching.expires", { date: date(expiresAt()) })}</span>
                                            )}
                                          </Show>
                                          <Show when={item.targetUrl}>
                                            {(url) => (
                                              <Button
                                                variant="ghost"
                                                class="sidebar-watching-source"
                                                onClick={() => props.onOpenSource(url())}
                                              >
                                                {t("sidebar.watching.openSource")}
                                                <ExternalLink class="size-3" aria-hidden="true" />
                                              </Button>
                                            )}
                                          </Show>
                                        </div>
                                        <Show when={item.error}>
                                          <p class="sidebar-watching-error">{item.error}</p>
                                        </Show>
                                      </li>
                                    )}
                                  </For>
                                </ul>
                              </div>
                            )}
                          </For>
                        </Show>
                      </section>
                    )}
                  </For>
                </div>
              </Show>
            </Show>
          </Dialog.Content>
        </Dialog.Overlay>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
