/** The search field, and the compact-mode button in its place. Both open the global search. */

import { Button, Eye } from "@openbot/ui";
import { Show } from "solid-js";
import { useText } from "../../text";
import { SearchIcon } from "./SidebarIcons";
import { useSidebarScope } from "./sidebar-scope";

export function SidebarSearch() {
  const { props } = useSidebarScope();
  const { t } = useText();
  return (
    <>
      <div class="sidebar-search-wrap">
        <Button
          variant="ghost"
          type="button"
          class="search-field sidebar-search-trigger"
          aria-label={t("conversation.globalSearch.title")}
          aria-haspopup="dialog"
          aria-hidden={props.compact ? "true" : undefined}
          tabindex={props.compact ? -1 : 0}
          onClick={props.onOpenSearch}
        >
          <SearchIcon />
          <span class="sidebar-search-placeholder">{t("common.search")}</span>
        </Button>
        <Button
          variant="ghost"
          type="button"
          class="sidebar-compact-search"
          aria-label={t("conversation.globalSearch.title")}
          aria-haspopup="dialog"
          aria-hidden={props.compact ? undefined : "true"}
          tabindex={props.compact ? 0 : -1}
          onClick={props.onOpenSearch}
        >
          <SearchIcon />
        </Button>
      </div>
      <Show when={props.onOpenWatching}>
        <div class="sidebar-watching-trigger-wrap">
          <Button
            variant="ghost"
            type="button"
            class="sidebar-watching-trigger"
            aria-label={t("sidebar.watching.title")}
            aria-haspopup="dialog"
            onClick={() => props.onOpenWatching?.()}
          >
            <Eye class="size-4" aria-hidden="true" />
            <Show when={!props.compact}>
              <span>{t("sidebar.watching.title")}</span>
            </Show>
          </Button>
        </div>
      </Show>
    </>
  );
}
