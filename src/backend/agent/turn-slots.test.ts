import type { QueueDelivery } from "@openbot/contracts/ipc";
import { describe, expect, it } from "vitest";
import { TurnSlots } from "./turn-slots";

function delivery(createdAt: string, kind: "user" | "routine" = "routine"): QueueDelivery {
  return {
    id: `delivery-${createdAt}`,
    messageId: `message-${createdAt}`,
    recipientAgentId: "agent",
    sender:
      kind === "user"
        ? { kind: "user" }
        : { kind: "routine", routineId: "r", runId: "run", routineName: "R", scheduledFor: createdAt },
    text: "Check the release.",
    attachments: [],
    replyToMessageId: null,
    status: "queued",
    position: 0,
    turnId: null,
    error: null,
    createdAt,
  };
}

function slots(options: { limit: number; running: string[]; heads: Record<string, QueueDelivery>; lanes: string[] }) {
  const waitingChecks: string[] = [];
  const turnSlots = new TurnSlots({
    limit: () => options.limit,
    lanes: () => options.lanes,
    isRunning: (lane) => options.running.includes(lane),
    isWaiting: (lane) => {
      waitingChecks.push(lane);
      return options.heads[lane] !== undefined;
    },
    head: (lane) => options.heads[lane] ?? null,
  });
  return { turnSlots, waitingChecks };
}

describe("TurnSlots.mayStart", () => {
  const lanes = Array.from({ length: 200 }, (_, index) => `agent-${String(index).padStart(3, "0")}`);

  it("checks only the lanes whose next delivery would start first", () => {
    const { turnSlots, waitingChecks } = slots({
      limit: 4,
      running: ["agent-000"],
      lanes,
      heads: {
        "agent-010": delivery("2026-11-04T10:00:00.000Z"),
        "agent-020": delivery("2026-11-04T10:05:00.000Z"),
        "agent-150": delivery("2026-11-04T11:00:00.000Z"),
      },
    });
    expect(turnSlots.mayStart("agent-020")).toBe(true);
    expect(waitingChecks).toEqual(["agent-010"]);
  });

  it("keeps the order of the waiting lanes when the slots are short", () => {
    const heads = {
      "agent-010": delivery("2026-11-04T10:00:00.000Z"),
      "agent-020": delivery("2026-11-04T10:05:00.000Z"),
      "agent-030": delivery("2026-11-04T10:10:00.000Z", "user"),
    };
    const { turnSlots } = slots({ limit: 3, running: ["agent-000"], lanes, heads });
    expect(turnSlots.mayStart("agent-030")).toBe(true);
    expect(turnSlots.mayStart("agent-010")).toBe(true);
    expect(turnSlots.mayStart("agent-020")).toBe(false);
  });

  it("does not count a lane that runs or cannot start", () => {
    const heads: Record<string, QueueDelivery> = {
      "agent-010": delivery("2026-11-04T10:00:00.000Z"),
      "agent-020": delivery("2026-11-04T10:05:00.000Z"),
    };
    const running = slots({ limit: 2, running: ["agent-000"], lanes, heads });
    expect(running.turnSlots.mayStart("agent-020")).toBe(false);
    const blocked = new TurnSlots({
      limit: () => 2,
      lanes: () => lanes,
      isRunning: (lane) => lane === "agent-000",
      isWaiting: (lane) => lane !== "agent-010",
      head: (lane) => heads[lane] ?? null,
    });
    expect(blocked.mayStart("agent-020")).toBe(true);
  });
});

describe("TurnSlots.mayStart with reserved person slots", () => {
  const lanes = Array.from({ length: 20 }, (_, index) => `agent-${String(index).padStart(2, "0")}`);

  function reserved(options: {
    running: string[];
    person: string[];
    heads: Record<string, QueueDelivery>;
    backgroundLimit: number | null;
  }) {
    return new TurnSlots({
      limit: () => 10,
      lanes: () => lanes,
      isRunning: (lane) => options.running.includes(lane),
      isWaiting: (lane) => !options.running.includes(lane) && options.heads[lane] !== undefined,
      head: (lane) => options.heads[lane] ?? null,
      backgroundLimit: () => options.backgroundLimit,
      personLanes: () => new Set(options.person),
    });
  }

  it("keeps the reserved slots for a person's message when background turns fill their share", () => {
    const running = lanes.slice(0, 7);
    const heads = {
      "agent-10": delivery("2026-11-11T10:00:00.000Z"),
      "agent-11": delivery("2026-11-11T10:05:00.000Z", "user"),
    };
    const slots = reserved({ running, person: [], heads, backgroundLimit: 7 });
    expect(slots.mayStart("agent-10")).toBe(false);
    expect(slots.mayStart("agent-11")).toBe(true);
  });

  it("does not count a person's running turn against the background share", () => {
    const running = [...lanes.slice(0, 6), "agent-15", "agent-16"];
    const heads = { "agent-10": delivery("2026-11-11T10:00:00.000Z") };
    const slots = reserved({ running, person: ["agent-15", "agent-16"], heads, backgroundLimit: 7 });
    expect(slots.mayStart("agent-10")).toBe(true);
  });

  it("lets only the oldest waiting background lanes into the free background slots", () => {
    const running = lanes.slice(0, 5);
    const heads = {
      "agent-10": delivery("2026-11-11T10:00:00.000Z"),
      "agent-11": delivery("2026-11-11T10:01:00.000Z"),
      "agent-12": delivery("2026-11-11T10:02:00.000Z"),
      "agent-13": delivery("2026-11-11T10:03:00.000Z", "user"),
    };
    const slots = reserved({ running, person: [], heads, backgroundLimit: 7 });
    expect(slots.mayStart("agent-10")).toBe(true);
    expect(slots.mayStart("agent-11")).toBe(true);
    expect(slots.mayStart("agent-12")).toBe(false);
    expect(slots.mayStart("agent-13")).toBe(true);
  });

  it("still stops a person's message when every slot runs", () => {
    const running = lanes.slice(0, 10);
    const heads = { "agent-11": delivery("2026-11-11T10:05:00.000Z", "user") };
    expect(reserved({ running, person: [], heads, backgroundLimit: 7 }).mayStart("agent-11")).toBe(false);
  });

  it("starts background turns in every free slot when there is no background limit", () => {
    const running = lanes.slice(0, 9);
    const heads = { "agent-10": delivery("2026-11-11T10:00:00.000Z") };
    expect(reserved({ running, person: [], heads, backgroundLimit: null }).mayStart("agent-10")).toBe(true);
  });
});
