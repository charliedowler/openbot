import type { ContextPullRequest, ContextPullRequestsSnapshot } from "@openbot/contracts/context-prs";
import { Button, ExternalLink, GitMerge, GitPullRequest, IconButton, RefreshCw } from "@openbot/ui";
import { For, Show } from "solid-js";
import { useText } from "../../text";

const CHECK_LABELS = {
  passed: "conversation.pullRequests.checks.passed",
  pending: "conversation.pullRequests.checks.pending",
  failed: "conversation.pullRequests.checks.failed",
  unknown: "conversation.pullRequests.checks.unknown",
} as const;

function PullRequestRow(props: { item: ContextPullRequest; onOpen: (url: string) => void }) {
  const { t, format } = useText();
  const date = (value: string) =>
    format.date(new Date(value), { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  return (
    <li>
      <Button
        variant="ghost"
        class="context-pr-row"
        onClick={() => props.onOpen(props.item.url)}
        title={t("conversation.pullRequests.verified", { date: date(props.item.checkedAt) })}
      >
        <span class="context-pr-icon" data-pr-state={props.item.state}>
          <Show when={props.item.state === "merged"} fallback={<GitPullRequest aria-hidden="true" />}>
            <GitMerge aria-hidden="true" />
          </Show>
        </span>
        <span class="context-pr-copy">
          <strong>{props.item.title}</strong>
          <span class="context-pr-reference">
            {props.item.repo}#{props.item.number}
          </span>
          <span class="context-pr-meta">
            <Show when={props.item.state === "open"}>
              <Show when={props.item.draft}>
                <span>{t("conversation.pullRequests.draft")}</span>
              </Show>
              <span data-check-state={props.item.checks}>{t(CHECK_LABELS[props.item.checks])}</span>
            </Show>
            <Show when={props.item.mergedAt}>
              {(mergedAt) => <span>{t("conversation.pullRequests.mergedAt", { date: date(mergedAt()) })}</span>}
            </Show>
          </span>
        </span>
        <ExternalLink class="context-pr-open-icon size-3" aria-hidden="true" />
      </Button>
    </li>
  );
}

export function PullRequestList(props: {
  data: ContextPullRequestsSnapshot | null;
  loading: boolean;
  supported: boolean;
  error: string | null;
  onRefresh: () => void;
  onOpen: (url: string) => void;
}) {
  const { t, format } = useText();
  const sections = () => [
    {
      id: "open",
      title: t("conversation.pullRequests.open"),
      items: props.data?.open ?? [],
      empty: t("conversation.pullRequests.noOpen"),
    },
    {
      id: "merged",
      title: t("conversation.pullRequests.recentMerged"),
      items: props.data?.merged ?? [],
      empty: t("conversation.pullRequests.noMerged"),
    },
  ];
  return (
    <div class="context-pr-list">
      <header class="context-pr-header">
        <div>
          <h2>{t("conversation.sidePanel.pane.pullRequests")}</h2>
          <p>{t("conversation.pullRequests.description")}</p>
        </div>
        <IconButton
          variant="ghost"
          label={t("conversation.pullRequests.refresh")}
          disabled={props.loading || !props.supported}
          onClick={props.onRefresh}
        >
          <RefreshCw />
        </IconButton>
      </header>
      <Show
        when={props.supported && props.data?.status !== "unsupported"}
        fallback={<p class="context-pr-notice">{t("conversation.pullRequests.unsupported")}</p>}
      >
        <Show when={props.loading}>
          <p class="context-pr-notice" role="status">
            {t("conversation.pullRequests.loading")}
          </p>
        </Show>
        <Show when={props.error}>
          {(error) => (
            <p class="context-pr-error" role="alert">
              {error()}
            </p>
          )}
        </Show>
        <Show when={props.data}>
          {(data) => (
            <div class="context-pr-scroll">
              <Show when={data().status === "not-connected"}>
                <p class="context-pr-notice">{t("conversation.pullRequests.notConnected")}</p>
              </Show>
              <Show when={data().referencesLimited}>
                <p class="context-pr-notice">{t("conversation.pullRequests.limited")}</p>
              </Show>
              <For each={sections()}>
                {(section) => (
                  <section class="context-pr-section" aria-label={section.title}>
                    <h3>{section.title}</h3>
                    <ul>
                      <For each={section.items} fallback={<li class="context-pr-empty">{section.empty}</li>}>
                        {(item) => <PullRequestRow item={item} onOpen={props.onOpen} />}
                      </For>
                    </ul>
                  </section>
                )}
              </For>
              <Show when={data().failures.length > 0}>
                <section class="context-pr-section" aria-label={t("conversation.pullRequests.unverified")}>
                  <h3>{t("conversation.pullRequests.unverified")}</h3>
                  <p class="context-pr-notice">{t("conversation.pullRequests.partial")}</p>
                  <ul>
                    <For each={data().failures}>
                      {(failure) => (
                        <li>
                          <Button variant="ghost" class="context-pr-failure" onClick={() => props.onOpen(failure.url)}>
                            {failure.url}
                          </Button>
                        </li>
                      )}
                    </For>
                  </ul>
                </section>
              </Show>
              <p class="context-pr-read-time">
                {t("conversation.pullRequests.readAt", {
                  date: format.date(new Date(data().checkedAt), { hour: "2-digit", minute: "2-digit" }),
                })}
              </p>
            </div>
          )}
        </Show>
      </Show>
    </div>
  );
}
