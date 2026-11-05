import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CHANNEL_LIST_INTERVAL_MS, createChannelListRefresh } from "./channel-list-refresh";

describe("channel list refresh", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  function recorder() {
    const reads: { serverId: string; at: number }[] = [];
    const refresh = createChannelListRefresh(async (serverId) => {
      reads.push({ serverId, at: Date.now() });
    });
    return { reads, refresh };
  }

  it("reads at once after a quiet interval", async () => {
    const { reads, refresh } = recorder();
    refresh("local");
    expect(reads).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(CHANNEL_LIST_INTERVAL_MS * 2);
    refresh("local");
    expect(reads).toHaveLength(2);
  });

  it("reads once at the end of the interval for a stream of changes, after the last one", async () => {
    const { reads, refresh } = recorder();
    const start = Date.now();
    refresh("local");
    let last = start;
    for (let index = 0; index < 40; index++) {
      await vi.advanceTimersByTimeAsync(10);
      refresh("local");
      last = Date.now();
    }
    expect(reads).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(CHANNEL_LIST_INTERVAL_MS);
    expect(reads).toHaveLength(2);
    expect(reads[1]?.at).toBe(start + CHANNEL_LIST_INTERVAL_MS);
    expect(reads[1]?.at).toBeGreaterThanOrEqual(last);
    await vi.advanceTimersByTimeAsync(CHANNEL_LIST_INTERVAL_MS * 3);
    expect(reads).toHaveLength(2);
  });

  it("reads again after a read during which a change came in", async () => {
    let finish: () => void = () => undefined;
    const reads: number[] = [];
    const refresh = createChannelListRefresh(
      (_serverId) =>
        new Promise<void>((done) => {
          reads.push(Date.now());
          finish = done;
        }),
    );
    refresh("local");
    await vi.advanceTimersByTimeAsync(CHANNEL_LIST_INTERVAL_MS * 2);
    refresh("local");
    expect(reads).toHaveLength(1);
    finish();
    await vi.advanceTimersByTimeAsync(0);
    expect(reads).toHaveLength(2);
  });

  it("keeps the servers apart", () => {
    const { reads, refresh } = recorder();
    refresh("local");
    refresh("remote");
    expect(reads.map((read) => read.serverId)).toEqual(["local", "remote"]);
  });

  it("keeps reading after a read fails", async () => {
    let calls = 0;
    const refresh = createChannelListRefresh(async () => {
      calls++;
      throw new Error("The list could not be read.");
    });
    refresh("local");
    await vi.advanceTimersByTimeAsync(CHANNEL_LIST_INTERVAL_MS * 2);
    refresh("local");
    expect(calls).toBe(2);
  });
});
