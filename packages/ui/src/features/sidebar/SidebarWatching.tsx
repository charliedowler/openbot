import type { AppMessages, AppTextKey } from "@openbot/i18n";
import {
  Button,
  Checkbox,
  ChevronDown,
  Dialog,
  ExternalLink,
  Eye,
  IconButton,
  Input,
  ListChecks,
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
  summary?: string | undefined;
  purpose?: string | undefined;
  receivers?: { id: string; name: string }[] | undefined;
}

export interface SidebarTaskItem {
  id: string;
  kind: "decision" | "follow-up" | "dispatch";
  state: "waiting-on-charlie" | "in-progress" | "waiting-on-event" | "done";
  title: string;
  detail?: string | undefined;
  ownerName?: string | undefined;
  link?: string | undefined;
  updatedAt: string;
  dueAt?: string | undefined;
  waitingOn?: string | undefined;
  waitingSince?: string | undefined;
  urgency?: "low" | "normal" | "high" | undefined;
  urgencyReason?: string | undefined;
}

export interface SidebarTaskHealth {
  score: number;
  wip: number;
  wipLimit: number;
  urgent: number;
  reasons: string[];
  oldest?: { title: string; since: string } | undefined;
  longestWaiting?: { who: string; since: string } | undefined;
}

export interface SidebarTasksData {
  generatedAt: string;
  items: SidebarTaskItem[];
  health?: SidebarTaskHealth | undefined;
}

export interface SidebarWatchingData {
  generatedAt: string | null;
  watches: SidebarWatchingItem[];
  waits: SidebarWatchingItem[];
  tasks?: SidebarTasksData | null | undefined;
}

const KIND_KEYS = {
  t3: "sidebar.watching.kind.t3",
  githubThread: "sidebar.watching.kind.githubThread",
  githubCi: "sidebar.watching.kind.githubCi",
  slack: "sidebar.watching.kind.slack",
  sentry: "sidebar.watching.kind.sentry",
  gcp: "sidebar.watching.kind.gcp",
  release: "sidebar.watching.kind.release",
  assigned: "sidebar.watching.kind.assigned",
  health: "sidebar.watching.kind.health",
  other: "sidebar.watching.kind.other",
} as const satisfies Record<string, keyof AppMessages>;

function watchKind(sourceKind: string): keyof typeof KIND_KEYS {
  if (sourceKind === "t3") return "t3";
  if (sourceKind === "github_thread") return "githubThread";
  if (sourceKind === "github_ci") return "githubCi";
  if (sourceKind.startsWith("slack")) return "slack";
  if (sourceKind === "sentry") return "sentry";
  if (sourceKind.startsWith("gcp")) return "gcp";
  if (["lightdash_release", "version", "asc_reviews", "asc_review_state"].includes(sourceKind)) return "release";
  if (sourceKind.endsWith("_assigned")) return "assigned";
  if (sourceKind.includes("health") || ["t3_capacity", "qa_checkpoint"].includes(sourceKind)) return "health";
  return "other";
}

const PUSH_KINDS = (sourceKind: string) =>
  sourceKind === "sentry" || sourceKind.startsWith("slack") || sourceKind.startsWith("asc");

export function watchConditionKey(sourceKind: string, checked: boolean): AppTextKey {
  if (!checked) return "sidebar.watching.condition.pending";
  if (sourceKind === "t3") return "sidebar.watching.condition.t3";
  if (sourceKind === "github_ci") return "sidebar.watching.condition.githubCi";
  if (sourceKind === "github_thread") return "sidebar.watching.condition.githubThread";
  if (sourceKind === "version") return "sidebar.watching.condition.version";
  if (sourceKind === "health_sweep") return "sidebar.watching.condition.healthSweep";
  if (sourceKind === "linear_assigned") return "sidebar.watching.condition.linearAssigned";
  if (sourceKind === "pylon_assigned") return "sidebar.watching.condition.pylonAssigned";
  if (sourceKind === "slack_keyword") return "sidebar.watching.condition.slackKeyword";
  if (sourceKind === "sentry" || sourceKind.startsWith("slack") || sourceKind === "asc")
    return "sidebar.watching.condition.push";
  return "sidebar.watching.condition.default";
}

const DAY_MS = 86_400_000;

export function watchIsStale(item: SidebarWatchingItem, now: number): boolean {
  return (
    item.status === "active" &&
    !PUSH_KINDS(item.sourceKind) &&
    item.checkedAt !== undefined &&
    now - Date.parse(item.checkedAt) > DAY_MS
  );
}

function receiverIds(item: SidebarWatchingItem): { id: string; name: string }[] {
  return item.receivers?.length ? item.receivers : [{ id: item.ownerId, name: item.ownerName }];
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
        <span class="sidebar-watching-item-copy">
          <span class="sidebar-watching-item-title">{props.item.summary ?? props.item.title}</span>
          <Show when={props.item.purpose}>
            {(purpose) => <span class="sidebar-watching-purpose">{purpose()}</span>}
          </Show>
        </span>
        <span class="sidebar-watching-state">
          {watchIsStale(props.item, Date.now()) ? t("sidebar.watching.stale") : t(STATUS_KEYS[props.item.status])}
        </span>
      </Button>
      <Show when={current(props.item) && props.item.error}>
        <p class="sidebar-watching-error sidebar-watching-row-error">{props.item.error}</p>
      </Show>
      <Show when={expanded()}>
        <div id={detailsId} class="sidebar-watching-details">
          <Show when={props.item.condition}>
            <p class="sidebar-watching-condition">{props.item.condition}</p>
          </Show>
          <Show when={props.item.summary && props.item.summary !== props.item.title}>
            <p class="sidebar-watching-condition">{t("sidebar.watching.watchTitle", { title: props.item.title })}</p>
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
      for (const receiver of receiverIds(item)) {
        const group = grouped.get(receiver.id) ?? { id: receiver.id, name: receiver.name, count: 0 };
        group.count += 1;
        grouped.set(group.id, group);
      }
    }
    return [...grouped.values()].sort((a, b) => a.name.localeCompare(b.name));
  });
  const tabs = createMemo(() => [
    {
      id: "all",
      name: t("sidebar.watching.allReceivers"),
      count: available().filter((item) => view.includeInactive || current(item)).length,
    },
    ...owners(),
  ]);
  const summary = createMemo(() => {
    const now = Date.now();
    const live = (props.data?.watches ?? []).filter(current);
    return {
      active: live.filter((item) => item.status === "active").length,
      receivers: new Set(live.flatMap((item) => receiverIds(item).map((receiver) => receiver.id))).size,
      expiring: live.filter((item) => item.expiresAt !== undefined && Date.parse(item.expiresAt) - now <= 2 * DAY_MS)
        .length,
      attention: live.filter((item) => item.status === "error" || watchIsStale(item, now)).length,
    };
  });
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
          const receivers = receiverIds(item);
          if (selected() !== "all" && !receivers.some((receiver) => receiver.id === selected())) return groups;
          if (
            search &&
            ![
              item.title,
              item.summary,
              item.purpose,
              item.ownerName,
              item.channelName,
              item.sourceKind,
              item.condition,
              item.error,
              ...receivers.map((receiver) => receiver.name),
            ]
              .filter(Boolean)
              .join(" ")
              .toLowerCase()
              .includes(search)
          )
            return groups;
          const targets =
            section.kind === "sources"
              ? receivers
                  .filter((receiver) => selected() === "all" || receiver.id === selected())
                  .map((receiver) => ({ key: receiver.id, name: receiver.name, chat: receiver.id }))
              : [{ key: item.channelId ?? item.ownerId, name: item.channelName ?? item.ownerName, chat: item.ownerId }];
          for (const target of targets) {
            const group = groups.get(target.key) ?? {
              id: target.key,
              name: target.name,
              chatId: target.chat,
              channelId: section.kind === "sources" ? undefined : item.channelId,
              items: [],
            };
            group.items.push(item);
            groups.set(target.key, group);
          }
          return groups;
        }, new Map<
          string,
          { id: string; name: string; chatId: string; channelId: string | undefined; items: SidebarWatchingItem[] }
        >()),
      )
        .map(([, group]) => {
          const kinds = new Map<keyof typeof KIND_KEYS, number>();
          for (const item of group.items)
            kinds.set(watchKind(item.sourceKind), (kinds.get(watchKind(item.sourceKind)) ?? 0) + 1);
          return {
            ...group,
            rollup:
              section.kind === "sources"
                ? [...kinds.entries()]
                    .sort((a, b) => b[1] - a[1])
                    .map(([kind, count]) => t(KIND_KEYS[kind], { count }))
                    .join(" · ")
                : "",
            items: group.items.sort(
              (a, b) =>
                Number(b.status === "error") - Number(a.status === "error") ||
                (a.summary ?? a.title).localeCompare(b.summary ?? b.title),
            ),
          };
        })
        .sort((a, b) => a.name.localeCompare(b.name)),
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
              <Show when={props.data}>
                <ul class="sidebar-watching-summary" aria-label={t("sidebar.watching.title")}>
                  <li>{t("sidebar.watching.summary.active", { count: summary().active })}</li>
                  <li>{t("sidebar.watching.summary.receivers", { count: summary().receivers })}</li>
                  <li data-warning={summary().expiring > 0 ? "true" : undefined}>
                    {t("sidebar.watching.summary.expiring", { count: summary().expiring })}
                  </li>
                  <li data-danger={summary().attention > 0 ? "true" : undefined}>
                    {t("sidebar.watching.summary.attention", { count: summary().attention })}
                  </li>
                </ul>
              </Show>
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
                  <Tabs.List class="sidebar-watching-agents" aria-label={t("sidebar.watching.receivers")}>
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
                                        onClick={() => props.onOpenChat(group.chatId, group.channelId)}
                                      >
                                        <span>{group.name}</span>
                                        <Show when={group.rollup}>
                                          <span class="sidebar-watching-rollup">{group.rollup}</span>
                                        </Show>
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

const TASK_GROUPS = [
  { state: "waiting-on-charlie", title: "sidebar.tasks.group.waitingOnCharlie" },
  { state: "in-progress", title: "sidebar.tasks.group.inProgress" },
  { state: "waiting-on-event", title: "sidebar.tasks.group.waitingOnEvent" },
  { state: "done", title: "sidebar.tasks.group.doneToday" },
] as const satisfies readonly { state: SidebarTaskItem["state"]; title: AppTextKey }[];

const TASK_KINDS = {
  decision: "sidebar.tasks.kind.decision",
  "follow-up": "sidebar.tasks.kind.followUp",
  dispatch: "sidebar.tasks.kind.dispatch",
} as const satisfies Record<SidebarTaskItem["kind"], AppTextKey>;

const URGENCY_RANK = { high: 0, normal: 1, low: 2 } as const;

export function sortTasks(items: readonly SidebarTaskItem[]): SidebarTaskItem[] {
  const since = (item: SidebarTaskItem) => Date.parse(item.waitingSince ?? item.updatedAt);
  return [...items].sort(
    (a, b) =>
      URGENCY_RANK[a.urgency ?? "normal"] - URGENCY_RANK[b.urgency ?? "normal"] ||
      since(a) - since(b) ||
      a.title.localeCompare(b.title),
  );
}

export function tasksDoneToday(item: SidebarTaskItem, now: Date): boolean {
  const updated = new Date(item.updatedAt);
  return (
    updated.getFullYear() === now.getFullYear() &&
    updated.getMonth() === now.getMonth() &&
    updated.getDate() === now.getDate()
  );
}

function useDuration() {
  const { t } = useText();
  return (since: string, now = Date.now()) => {
    const minutes = Math.max(0, Math.floor((now - Date.parse(since)) / 60_000));
    if (minutes < 60) return t("sidebar.tasks.duration.minutes", { count: minutes });
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return t("sidebar.tasks.duration.hours", { count: hours });
    return t("sidebar.tasks.duration.days", { count: Math.floor(hours / 24) });
  };
}

function TaskRow(props: { item: SidebarTaskItem; onOpenLink: (url: string) => void }) {
  const { t, format } = useText();
  const duration = useDuration();
  const date = (value: string) =>
    format.date(new Date(value), { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  return (
    <li class="sidebar-task" data-urgency={props.item.urgency}>
      <div class="sidebar-task-heading">
        <span class="sidebar-task-kind">{t(TASK_KINDS[props.item.kind])}</span>
        <span class="sidebar-task-title">{props.item.title}</span>
        <Show when={props.item.urgency === "high"}>
          <span class="sidebar-task-urgent" title={props.item.urgencyReason}>
            {t("sidebar.tasks.urgent")}
          </span>
        </Show>
      </div>
      <Show when={props.item.detail}>{(detail) => <p class="sidebar-task-detail">{detail()}</p>}</Show>
      <div class="sidebar-watching-meta">
        <Show when={props.item.waitingOn && props.item.waitingSince ? props.item : undefined}>
          {(item) => (
            <span class="sidebar-task-waiting">
              {t("sidebar.tasks.waitingFor", {
                duration: duration(item().waitingSince ?? item().updatedAt),
                who: item().waitingOn ?? "",
              })}
            </span>
          )}
        </Show>
        <Show when={props.item.ownerName}>{(name) => <span>{t("sidebar.tasks.owner", { name: name() })}</span>}</Show>
        <Show when={props.item.dueAt}>{(due) => <span>{t("sidebar.tasks.due", { date: date(due()) })}</span>}</Show>
        <Show when={props.item.urgency !== "high" && props.item.urgencyReason}>
          {(reason) => <span>{reason()}</span>}
        </Show>
        <Show when={props.item.link}>
          {(url) => (
            <Button variant="ghost" class="sidebar-watching-source" onClick={() => props.onOpenLink(url())}>
              {t("sidebar.tasks.open")}
              <ExternalLink class="size-3" aria-hidden="true" />
            </Button>
          )}
        </Show>
      </div>
    </li>
  );
}

function TaskHealthHeader(props: { health: SidebarTaskHealth }) {
  const { t } = useText();
  const duration = useDuration();
  const [open, setOpen] = createSignal(false);
  const reasonsId = createUniqueId();
  return (
    <section class="sidebar-task-health" aria-label={t("sidebar.tasks.health.reasons")}>
      <div class="sidebar-task-health-line">
        <strong
          class="sidebar-task-score"
          data-score={props.health.score >= 80 ? "good" : props.health.score >= 50 ? "fair" : "poor"}
        >
          {t("sidebar.tasks.health.score", { score: props.health.score })}
        </strong>
        <span data-warning={props.health.wip > props.health.wipLimit ? "true" : undefined}>
          {t("sidebar.tasks.health.wip", { wip: props.health.wip, limit: props.health.wipLimit })}
        </span>
        <span data-danger={props.health.urgent > 0 ? "true" : undefined}>
          {t("sidebar.tasks.health.urgent", { count: props.health.urgent })}
        </span>
        <Show when={props.health.oldest}>
          {(oldest) => (
            <span>
              {t("sidebar.tasks.health.oldest", { title: oldest().title, duration: duration(oldest().since) })}
            </span>
          )}
        </Show>
        <Show when={props.health.longestWaiting}>
          {(longest) => (
            <span>
              {t("sidebar.tasks.health.longestWaiting", {
                who: longest().who,
                duration: duration(longest().since),
              })}
            </span>
          )}
        </Show>
        <Button
          variant="ghost"
          class="sidebar-task-reasons-toggle"
          aria-expanded={open() ? "true" : "false"}
          aria-controls={reasonsId}
          title={props.health.reasons.join("\n")}
          onClick={() => setOpen((value) => !value)}
        >
          <ChevronDown class="sidebar-watching-chevron" aria-hidden="true" />
          {t("sidebar.tasks.health.reasons")}
        </Button>
      </div>
      <Show when={open()}>
        <ul id={reasonsId} class="sidebar-task-reasons">
          <For each={props.health.reasons} fallback={<li>{t("sidebar.tasks.health.noReasons")}</li>}>
            {(reason) => <li>{reason}</li>}
          </For>
        </ul>
      </Show>
    </section>
  );
}

export function SidebarTasksDialog(props: {
  open: boolean;
  loading: boolean;
  supported: boolean;
  error: string | null;
  data: SidebarTasksData | null;
  onClose: () => void;
  onRefresh: () => void;
  onOpenLink: (url: string) => void;
}) {
  const { t, format } = useText();
  const groups = createMemo(() => {
    const now = new Date();
    const items = props.data?.items ?? [];
    return TASK_GROUPS.map((group) => ({
      ...group,
      items: sortTasks(
        items.filter((item) => item.state === group.state && (item.state !== "done" || tasksDoneToday(item, now))),
      ),
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
            class="sidebar-watching-dialog sidebar-tasks-dialog"
            aria-busy={props.loading ? "true" : undefined}
            onEscapeKeyDown={(event) => {
              event.preventDefault();
              props.onClose();
            }}
          >
            <header class="sidebar-watching-header">
              <div class="sidebar-watching-heading">
                <ListChecks class="size-5" aria-hidden="true" />
                <Dialog.Title as="h2">{t("sidebar.tasks.title")}</Dialog.Title>
              </div>
              <div class="sidebar-watching-actions">
                <IconButton
                  variant="ghost"
                  label={t("sidebar.tasks.refresh")}
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
              {t("sidebar.tasks.description")}
            </Dialog.Description>
            <Show
              when={props.supported}
              fallback={<p class="sidebar-watching-notice">{t("sidebar.tasks.unsupported")}</p>}
            >
              <Show when={props.data?.health}>{(health) => <TaskHealthHeader health={health()} />}</Show>
              <p class="sidebar-watching-updated" role="status">
                {props.loading && !props.data
                  ? t("sidebar.tasks.loading")
                  : props.data
                    ? t("sidebar.tasks.updated", { date: date(props.data.generatedAt) })
                    : t("sidebar.tasks.noSnapshot")}
              </p>
              <Show when={props.error}>
                {(error) => (
                  <p class="sidebar-watching-error" role="alert">
                    {error()}
                  </p>
                )}
              </Show>
              <Show when={props.data}>
                <div class="sidebar-watching-scroll sidebar-tasks-scroll">
                  <For each={groups()}>
                    {(group) => (
                      <section class="sidebar-watching-section" aria-label={t(group.title)}>
                        <h3>
                          {t(group.title)} <span class="sidebar-watching-count">{group.items.length}</span>
                        </h3>
                        <ul class="sidebar-tasks-list">
                          <For
                            each={group.items}
                            fallback={<li class="sidebar-watching-empty">{t("sidebar.tasks.empty")}</li>}
                          >
                            {(item) => <TaskRow item={item} onOpenLink={props.onOpenLink} />}
                          </For>
                        </ul>
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
