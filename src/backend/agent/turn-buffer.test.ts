import { describe, expect, it } from "vitest";
import { TurnBuffer, type TurnBufferLogFields, turnBufferSettings } from "./turn-buffer";

function buffer(environment: NodeJS.ProcessEnv) {
  const lines: { message: string; fields: TurnBufferLogFields }[] = [];
  const turnBuffer = new TurnBuffer(turnBufferSettings(environment), {
    log: (message, fields) => lines.push({ message, fields }),
    now: () => new Date("2026-11-11T15:00:00.000Z"),
  });
  return { turnBuffer, lines };
}

describe("turnBufferSettings", () => {
  it("keeps the memory limit and no reservation or shedding when nothing is set", () => {
    expect(turnBufferSettings({})).toEqual({ limit: null, personReservedSlots: 0, shed: false, shedP99Ms: 2_000 });
    const { turnBuffer } = buffer({});
    expect(turnBuffer.limit(16)).toBe(16);
    expect(turnBuffer.backgroundLimit(16)).toBeNull();
    turnBuffer.observeMainLoop(9_000);
    turnBuffer.observeMainLoop(9_000);
    expect(turnBuffer.shedding()).toBe(false);
    expect(turnBuffer.status(16).shed).toBe("disabled");
  });

  it("reserves 3 person slots and sheds by default when the limit is set", () => {
    expect(turnBufferSettings({ OPENBOT_TURN_LIMIT: "10" })).toEqual({
      limit: 10,
      personReservedSlots: 3,
      shed: true,
      shedP99Ms: 2_000,
    });
    expect(
      turnBufferSettings({
        OPENBOT_TURN_LIMIT: "10",
        OPENBOT_PERSON_RESERVED_SLOTS: "0",
        OPENBOT_SHED: "0",
        OPENBOT_SHED_P99_MS: "1500",
      }),
    ).toEqual({ limit: 10, personReservedSlots: 0, shed: false, shedP99Ms: 1_500 });
  });

  it("ignores values that are not whole numbers", () => {
    expect(turnBufferSettings({ OPENBOT_TURN_LIMIT: "ten", OPENBOT_SHED_P99_MS: "-4" })).toEqual({
      limit: null,
      personReservedSlots: 0,
      shed: false,
      shedP99Ms: 2_000,
    });
    expect(turnBufferSettings({ OPENBOT_TURN_LIMIT: "0" }).limit).toBeNull();
    expect(turnBufferSettings({ OPENBOT_TURN_LIMIT: "2.5" }).limit).toBeNull();
  });
});

describe("TurnBuffer", () => {
  it("caps the memory limit and never raises it", () => {
    const { turnBuffer } = buffer({ OPENBOT_TURN_LIMIT: "10" });
    expect(turnBuffer.limit(16)).toBe(10);
    expect(turnBuffer.limit(8)).toBe(8);
    expect(turnBuffer.limit(2)).toBe(2);
  });

  it("keeps at least one background slot when the reservation is larger than the limit", () => {
    const { turnBuffer } = buffer({ OPENBOT_TURN_LIMIT: "10" });
    expect(turnBuffer.backgroundLimit(10)).toBe(7);
    expect(turnBuffer.backgroundLimit(2)).toBe(1);
  });

  it("halves the background slots after two slow samples and restores them after three fast ones", () => {
    const { turnBuffer, lines } = buffer({ OPENBOT_TURN_LIMIT: "10" });
    turnBuffer.observeMainLoop(2_500);
    expect(turnBuffer.shedding()).toBe(false);
    turnBuffer.observeMainLoop(800);
    turnBuffer.observeMainLoop(2_500);
    expect(turnBuffer.shedding()).toBe(false);
    turnBuffer.observeMainLoop(3_000);
    expect(turnBuffer.shedding()).toBe(true);
    expect(turnBuffer.backgroundLimit(10)).toBe(3);
    turnBuffer.observeMainLoop(4_000);
    turnBuffer.observeMainLoop(100);
    turnBuffer.observeMainLoop(100);
    expect(turnBuffer.shedding()).toBe(true);
    turnBuffer.observeMainLoop(2_100);
    turnBuffer.observeMainLoop(100);
    turnBuffer.observeMainLoop(100);
    expect(turnBuffer.shedding()).toBe(true);
    turnBuffer.observeMainLoop(100);
    expect(turnBuffer.shedding()).toBe(false);
    expect(turnBuffer.backgroundLimit(10)).toBe(7);
    expect(lines.map((line) => line.fields.shedding)).toEqual([true, false]);
    expect(lines[0]?.fields).toMatchObject({ mainLoopP99Ms: 3_000, shedP99Ms: 2_000 });
  });

  it("sheds with no reservation down to one slot", () => {
    const { turnBuffer } = buffer({ OPENBOT_TURN_LIMIT: "2", OPENBOT_PERSON_RESERVED_SLOTS: "0" });
    expect(turnBuffer.backgroundLimit(2)).toBeNull();
    turnBuffer.observeMainLoop(5_000);
    turnBuffer.observeMainLoop(5_000);
    expect(turnBuffer.backgroundLimit(2)).toBe(1);
    expect(turnBuffer.backgroundLimit(1)).toBe(1);
  });

  it("does not shed when OPENBOT_SHED is 0", () => {
    const { turnBuffer, lines } = buffer({ OPENBOT_TURN_LIMIT: "10", OPENBOT_SHED: "0" });
    for (let index = 0; index < 5; index += 1) turnBuffer.observeMainLoop(10_000);
    expect(turnBuffer.shedding()).toBe(false);
    expect(lines).toEqual([]);
    expect(turnBuffer.status(16)).toMatchObject({ shed: "disabled", mainLoopP99Ms: 10_000, backgroundLimit: 7 });
  });

  it("reports the effective limits", () => {
    const { turnBuffer } = buffer({ OPENBOT_TURN_LIMIT: "10" });
    expect(turnBuffer.status(16)).toEqual({
      memoryLimit: 16,
      limit: 10,
      limitCap: 10,
      personReservedSlots: 3,
      backgroundLimit: 7,
      shed: "off",
      shedP99Ms: 2_000,
      mainLoopP99Ms: null,
      shedChangedAt: null,
    });
    turnBuffer.observeMainLoop(2_400);
    turnBuffer.observeMainLoop(2_600);
    expect(turnBuffer.status(16)).toMatchObject({
      shed: "on",
      backgroundLimit: 3,
      mainLoopP99Ms: 2_600,
      shedChangedAt: "2026-11-11T15:00:00.000Z",
    });
  });
});
