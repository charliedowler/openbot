import {
  type SidebarTasksData,
  SidebarTasksDialog,
  type SidebarWatchingData,
  SidebarWatchingDialog,
  watchConditionKey,
} from "@openbot/ui/features/sidebar/Sidebar";
import { fireEvent, render, screen, waitFor, within } from "@solidjs/testing-library";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const NOW = new Date("2026-11-05T15:00:00.000Z");
const hoursAgo = (hours: number) => new Date(NOW.getTime() - hours * 3_600_000).toISOString();
const hoursAhead = (hours: number) => new Date(NOW.getTime() + hours * 3_600_000).toISOString();

const compass = { id: "compass", name: "Compass" };
const helm = { id: "helm", name: "Helm" };
const watching: SidebarWatchingData = {
  generatedAt: hoursAgo(0.1),
  watches: [
    {
      id: "p:pr",
      ownerId: "compass",
      ownerName: "Compass",
      title: "lightdash/lightdash#32090",
      summary: "PR #32090 review thread",
      purpose: "Tell Helm when the review asks for changes",
      sourceKind: "github_thread",
      status: "active",
      condition: "Comments, reviews and thread state changes.",
      checkedAt: hoursAgo(1),
      expiresAt: hoursAhead(30),
      receivers: [compass, helm],
    },
    {
      id: "p:t3-a",
      ownerId: "compass",
      ownerName: "Compass",
      title: "Thread a",
      summary: "Coding thread: Fix the chart legend",
      sourceKind: "t3",
      status: "active",
      checkedAt: hoursAgo(30),
      receivers: [helm],
    },
    {
      id: "p:t3-b",
      ownerId: "compass",
      ownerName: "Compass",
      title: "Thread b",
      sourceKind: "t3",
      status: "error",
      error: "Source read failed",
      checkedAt: hoursAgo(1),
      receivers: [helm],
    },
    {
      id: "p:ci",
      ownerId: "compass",
      ownerName: "Compass",
      title: "CI",
      sourceKind: "github_ci",
      status: "active",
      checkedAt: hoursAgo(1),
    },
  ],
  waits: [],
};

const tasks: SidebarTasksData = {
  generatedAt: hoursAgo(0.05),
  health: {
    score: 70,
    wip: 6,
    wipLimit: 5,
    urgent: 1,
    reasons: ["1 task over the limit of 5", "1 urgent task", "1 task waiting more than 3 working days"],
    oldest: { title: "Reply to Sam", since: hoursAgo(80) },
    longestWaiting: { who: "Sam at Example Retail", since: hoursAgo(80) },
  },
  items: [
    {
      id: "decision:late",
      kind: "decision",
      state: "waiting-on-charlie",
      title: "Pick the rollout date",
      detail: "Recommend Monday",
      ownerName: "Compass",
      updatedAt: hoursAgo(2),
      waitingOn: "Charlie",
      waitingSince: hoursAgo(2),
    },
    {
      id: "decision:urgent",
      kind: "decision",
      state: "waiting-on-charlie",
      title: "Approve the cutover",
      updatedAt: hoursAgo(1),
      waitingOn: "Charlie",
      waitingSince: hoursAgo(26),
      urgency: "high",
      urgencyReason: "Decision waiting more than 24 hours",
      link: "https://linear.app/lightdash/issue/SPK-1",
    },
    {
      id: "decision:older",
      kind: "decision",
      state: "waiting-on-charlie",
      title: "Name the release",
      updatedAt: hoursAgo(1),
      waitingOn: "Charlie",
      waitingSince: hoursAgo(10),
    },
    {
      id: "follow-up:tim",
      kind: "follow-up",
      state: "waiting-on-event",
      title: "Reply to Sam",
      updatedAt: hoursAgo(3),
      waitingOn: "Sam at Example Retail",
      waitingSince: hoursAgo(80),
    },
    { id: "dispatch:1", kind: "dispatch", state: "in-progress", title: "Fix the legend", updatedAt: hoursAgo(1) },
    { id: "dispatch:2", kind: "dispatch", state: "done", title: "Shipped today", updatedAt: hoursAgo(1) },
    { id: "dispatch:3", kind: "dispatch", state: "done", title: "Shipped last week", updatedAt: hoursAgo(170) },
  ],
};

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(NOW);
});
afterEach(() => {
  vi.useRealTimers();
});

describe("Watching v2", () => {
  it("shows the header counts, receiving agent groups with a kind roll-up, and summary with purpose", () => {
    render(() => (
      <SidebarWatchingDialog
        open
        loading={false}
        supported
        error={null}
        data={watching}
        onClose={() => {}}
        onRefresh={() => {}}
        onOpenChat={() => {}}
        onOpenSource={() => {}}
      />
    ));
    expect(screen.getByText("3 active watches")).toBeTruthy();
    expect(screen.getByText("2 receiving agents")).toBeTruthy();
    expect(screen.getByText("1 expires within 48 hours")).toBeTruthy();
    expect(screen.getByText("2 stale or failing")).toBeTruthy();
    expect(screen.getByRole("tab", { name: /Helm/ }).textContent).toContain("3");
    expect(screen.getByText("2 coding threads · 1 GitHub thread")).toBeTruthy();
    expect(screen.getByText("1 GitHub thread · 1 CI check")).toBeTruthy();
    expect(screen.getAllByText("PR #32090 review thread")).toHaveLength(2);
    expect(screen.getAllByText("Tell Helm when the review asks for changes")).toHaveLength(2);
    expect(screen.getByText("Coding thread: Fix the chart legend")).toBeTruthy();
    expect(screen.getByText("Not checked for a day")).toBeTruthy();
  });

  it("maps a source kind to the bridge's condition text when the publication has none", () => {
    expect(watchConditionKey("github_ci", true)).toBe("sidebar.watching.condition.githubCi");
    expect(watchConditionKey("slack_mentions", true)).toBe("sidebar.watching.condition.push");
    expect(watchConditionKey("slack_keyword", true)).toBe("sidebar.watching.condition.slackKeyword");
    expect(watchConditionKey("t3", false)).toBe("sidebar.watching.condition.pending");
    expect(watchConditionKey("gcp_log", true)).toBe("sidebar.watching.condition.default");
  });
});

describe("Tasks", () => {
  it("groups open tasks by state in order, sorts by urgency then wait, and shows no done group", () => {
    render(() => (
      <SidebarTasksDialog
        open
        loading={false}
        supported
        error={null}
        data={tasks}
        onClose={() => {}}
        onRefresh={() => {}}
        onOpenLink={() => {}}
      />
    ));
    const sections = screen.getAllByRole("region").filter((section) => section.tagName === "SECTION");
    const titles = sections.map((section) => section.getAttribute("aria-label"));
    expect(titles.filter((title) => title !== "Why this score")).toEqual([
      "Waiting on Charlie",
      "In progress",
      "Waiting on an event",
    ]);
    const charlie = sections.find((section) => section.getAttribute("aria-label") === "Waiting on Charlie");
    if (!charlie) throw new Error("Missing group");
    expect(
      within(charlie)
        .getAllByText(/Approve the cutover|Name the release|Pick the rollout date/)
        .map((node) => node.textContent),
    ).toEqual(["Approve the cutover", "Name the release", "Pick the rollout date"]);
    expect(screen.getByText("Waiting 3 days on Sam at Example Retail")).toBeTruthy();
    expect(screen.getByText("Waiting 1 day on Charlie")).toBeTruthy();
    expect(screen.getByText("Owner: Compass")).toBeTruthy();
    expect(screen.getByText("Urgent").getAttribute("title")).toBe("Decision waiting more than 24 hours");
    expect(screen.queryByText("Done today")).toBeNull();
    expect(screen.queryByText("Shipped today")).toBeNull();
    expect(screen.queryByText("Shipped last week")).toBeNull();
    expect(screen.queryByText("No open tasks.")).toBeNull();
  });

  it("hides a group with no tasks", () => {
    render(() => (
      <SidebarTasksDialog
        open
        loading={false}
        supported
        error={null}
        data={{ ...tasks, items: tasks.items.filter((item) => item.state === "in-progress" || item.state === "done") }}
        onClose={() => {}}
        onRefresh={() => {}}
        onOpenLink={() => {}}
      />
    ));
    const titles = screen
      .getAllByRole("region")
      .filter((section) => section.tagName === "SECTION")
      .map((section) => section.getAttribute("aria-label"))
      .filter((title) => title !== "Why this score");
    expect(titles).toEqual(["In progress"]);
    expect(screen.getByText("Fix the legend")).toBeTruthy();
    expect(screen.queryByText("Nothing here.")).toBeNull();
    expect(screen.queryByText("No open tasks.")).toBeNull();
  });

  it("shows one empty line for the panel when no task is open", () => {
    render(() => (
      <SidebarTasksDialog
        open
        loading={false}
        supported
        error={null}
        data={{ ...tasks, items: tasks.items.filter((item) => item.state === "done") }}
        onClose={() => {}}
        onRefresh={() => {}}
        onOpenLink={() => {}}
      />
    ));
    const titles = screen
      .getAllByRole("region")
      .filter((section) => section.tagName === "SECTION")
      .map((section) => section.getAttribute("aria-label"))
      .filter((title) => title !== "Why this score");
    expect(titles).toEqual([]);
    expect(screen.getAllByText("No open tasks.")).toHaveLength(1);
    expect(screen.queryByText("Nothing here.")).toBeNull();
  });

  it("shows the health header with the score, limit, oldest, longest wait and reasons on expand", async () => {
    render(() => (
      <SidebarTasksDialog
        open
        loading={false}
        supported
        error={null}
        data={tasks}
        onClose={() => {}}
        onRefresh={() => {}}
        onOpenLink={() => {}}
      />
    ));
    expect(screen.getByText("Health 70")).toBeTruthy();
    expect(screen.getByText("Work in progress 6 of 5")).toBeTruthy();
    expect(screen.getByText("1 urgent")).toBeTruthy();
    expect(screen.getByText("Oldest: Reply to Sam, 3 days")).toBeTruthy();
    expect(screen.getByText("Longest wait: Sam at Example Retail, 3 days")).toBeTruthy();
    expect(screen.queryByText("1 urgent task")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Why this score" }));
    await waitFor(() => expect(screen.getByText("1 urgent task")).toBeTruthy());
    expect(screen.getByText("1 task waiting more than 3 working days")).toBeTruthy();
  });
});
