import { type SidebarTasksData, SidebarTasksDialog } from "@openbot/ui/features/sidebar/Sidebar";
import { fireEvent, render, screen, waitFor, within } from "@solidjs/testing-library";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createTaskPickUp } from "./task-pick-up";

const NOW = new Date("2026-11-05T15:00:00.000Z");
const hoursAgo = (hours: number) => new Date(NOW.getTime() - hours * 3_600_000).toISOString();

const compass = { id: "agent-compass", name: "Compass" };
const helm = { id: "agent-helm", name: "Helm" };

const tasks: SidebarTasksData = {
  generatedAt: hoursAgo(0.05),
  items: [
    {
      id: "decision:rollout",
      kind: "decision",
      state: "waiting-on-charlie",
      title: "Pick the rollout date",
      detail: "Recommend Monday",
      ownerId: "agent-helm",
      ownerName: "Helm",
      link: "https://linear.app/lightdash/issue/SPK-1",
      updatedAt: hoursAgo(2),
      dueAt: "2026-11-06T17:00:00.000Z",
      waitingOn: "Charlie",
      waitingSince: "2026-11-04T09:30:00.000Z",
    },
    {
      id: "follow-up:sam",
      kind: "follow-up",
      state: "waiting-on-event",
      title: "Reply to Sam",
      updatedAt: hoursAgo(3),
    },
    { id: "dispatch:1", kind: "dispatch", state: "in-progress", title: "Fix the legend", updatedAt: hoursAgo(1) },
    { id: "dispatch:2", kind: "dispatch", state: "done", title: "Shipped today", updatedAt: hoursAgo(1) },
  ],
};

function renderTasks(agents: { id: string; name: string }[], serverId = "server-1") {
  const pickUp = vi.fn((_agentId: string, _message: string) => true);
  const state = createTaskPickUp({ serverId: () => serverId, agents: () => agents, pickUp });
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
      pickUp={state()}
    />
  ));
  return pickUp;
}

const row = (title: string) => {
  const item = screen.getByText(title).closest("li");
  if (!item) throw new Error(`Missing row ${title}`);
  return item;
};

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(NOW);
  window.localStorage.clear();
});
afterEach(() => {
  vi.useRealTimers();
  window.localStorage.clear();
});

describe("Tasks: Pick this up", () => {
  it("shows the button on open tasks only", () => {
    renderTasks([helm, compass]);
    expect(within(row("Pick the rollout date")).getByRole("button", { name: "Pick this up" })).toBeTruthy();
    expect(within(row("Reply to Sam")).getByRole("button", { name: "Pick this up" })).toBeTruthy();
    expect(within(row("Fix the legend")).getByRole("button", { name: "Pick this up" })).toBeTruthy();
    expect(within(row("Shipped today")).queryByRole("button", { name: "Pick this up" })).toBeNull();
    expect(screen.getAllByRole("button", { name: "Pick this up" })).toHaveLength(3);
  });

  it("sends one message with every field of a decision to Compass by default, also on a double click", async () => {
    const pickUp = renderTasks([helm, compass]);
    expect(screen.getByRole("button", { name: /Pick up with: Compass/ })).toBeTruthy();
    const button = within(row("Pick the rollout date")).getByRole("button", { name: "Pick this up" });
    fireEvent.click(button);
    fireEvent.click(button);
    await waitFor(() => expect(pickUp).toHaveBeenCalledTimes(1));
    await Promise.resolve();
    expect(pickUp).toHaveBeenCalledTimes(1);
    expect(pickUp).toHaveBeenCalledWith(
      "agent-compass",
      [
        "Pick this up: Decision: Pick the rollout date",
        "Recommendation/trigger: Recommend Monday",
        "Waiting on: Charlie since Wed, Nov 4, 09:30 AM UTC",
        "Owner: Helm",
        "Due: Fri, Nov 6, 05:00 PM UTC",
        "Link: https://linear.app/lightdash/issue/SPK-1",
        "Task: decision:rollout",
        "Brief me on the context and what I need to decide.",
      ].join("\n"),
    );
    await waitFor(() => expect(button.hasAttribute("disabled")).toBe(true));
  });

  it("skips missing fields and asks per kind", async () => {
    const pickUp = renderTasks([compass]);
    fireEvent.click(within(row("Reply to Sam")).getByRole("button", { name: "Pick this up" }));
    fireEvent.click(within(row("Fix the legend")).getByRole("button", { name: "Pick this up" }));
    await waitFor(() => expect(pickUp).toHaveBeenCalledTimes(2));
    expect(pickUp.mock.calls.map((call) => call[1])).toEqual([
      "Pick this up: Follow-up: Reply to Sam\nTask: follow-up:sam\nBrief me on the context and what I need to do.",
      "Pick this up: Dispatch: Fix the legend\nTask: dispatch:1\nBrief me on where this stands.",
    ]);
  });

  it("asks for an agent when no Compass exists, then remembers the choice for the server", async () => {
    const other = { id: "agent-other", name: "Other" };
    const pickUp = renderTasks([helm, other]);
    expect(screen.getByRole("button", { name: /Pick up with: choose an agent/ })).toBeTruthy();
    const trigger = within(row("Fix the legend")).getByRole("button", { name: "Pick this up" });
    fireEvent.keyDown(trigger, { key: "Enter" });
    const helmItem = await screen.findByRole("menuitem", { name: "Helm" });
    fireEvent.keyDown(helmItem, { key: "Enter" });
    await waitFor(() => expect(pickUp).toHaveBeenCalledTimes(1));
    expect(pickUp.mock.calls[0]?.[0]).toBe("agent-helm");
    expect(window.localStorage.getItem("openbot:task-pick-up-agent:server-1")).toBe("agent-helm");
    await waitFor(() => expect(screen.getByRole("button", { name: /Pick up with: Helm/ })).toBeTruthy());
  });

  it("uses the saved agent of the server over Compass", () => {
    window.localStorage.setItem("openbot:task-pick-up-agent:server-2", "agent-helm");
    renderTasks([compass, helm], "server-2");
    expect(screen.getByRole("button", { name: /Pick up with: Helm/ })).toBeTruthy();
  });
});
