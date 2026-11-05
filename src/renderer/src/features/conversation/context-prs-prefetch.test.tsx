import type { ContextPullRequestsSnapshot } from "@openbot/contracts/context-prs";
import { PullRequestList } from "@openbot/ui/features/conversation/SidePanel";
import { render, screen, waitFor } from "@solidjs/testing-library";
import { createRoot } from "solid-js";
import { describe, expect, it, vi } from "vitest";
import { cachedContextPrs, contextPrCacheKey, createContextPrsView, readContextPrs } from "./context-prs-view";

const NOW = Date.now();
const hoursAgo = (hours: number) => new Date(NOW - hours * 3_600_000).toISOString();

describe("Context PRs", () => {
  const snapshot: ContextPullRequestsSnapshot = {
    scope: { kind: "agent", id: "compass" },
    status: "ready",
    checkedAt: hoursAgo(0.2),
    refreshedAt: hoursAgo(0.2),
    stale: true,
    open: [
      {
        url: "https://github.com/lightdash/lightdash/pull/32090",
        repo: "lightdash/lightdash",
        number: 32090,
        title: "Fix the chart legend",
        state: "open",
        draft: false,
        mergedAt: null,
        headSha: "a".repeat(40),
        checks: "passed",
        checkedAt: hoursAgo(0.2),
      },
    ],
    merged: [],
    failures: [],
    referencesLimited: false,
  };

  it("renders a cached snapshot with a small updating state and no loading-only state", () => {
    render(() => (
      <PullRequestList
        data={snapshot}
        loading={false}
        updating
        supported
        error={null}
        onRefresh={() => {}}
        onOpen={() => {}}
      />
    ));
    expect(screen.getByText("Updating…")).toBeTruthy();
    expect(screen.getByText("Fix the chart legend")).toBeTruthy();
    expect(screen.queryByText("Reading PR status…")).toBeNull();
  });

  it("uses the snapshot prefetched when the chat opened at once when the panel opens", async () => {
    const target = { scope: { kind: "agent" as const, id: "compass" }, serverId: "local", authorization: "compass" };
    const key = contextPrCacheKey(target);
    if (!key) throw new Error("Missing key");
    const read = vi.fn(async () => snapshot);
    await readContextPrs(key, read);
    expect(cachedContextPrs(key)).toEqual(snapshot);
    const pending = vi.fn(() => new Promise<ContextPullRequestsSnapshot>(() => {}));
    let view: ReturnType<typeof createContextPrsView> | undefined;
    const dispose = createRoot((dispose) => {
      view = createContextPrsView({ target: () => target, revision: () => "0", supported: () => true, read: pending });
      return dispose;
    });
    await waitFor(() => expect(pending).toHaveBeenCalledTimes(1));
    expect(view?.data()?.open[0]?.number).toBe(32090);
    expect(view?.updating()).toBe(true);
    expect(view?.loading()).toBe(false);
    dispose();
  });
});
