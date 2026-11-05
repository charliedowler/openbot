// @vitest-environment node
// The usage limit gate saves its limits, so a restart does not send agents back to a limited model.
// A saved limit must still end as a limit in memory does: at a fresh plan reading that shows quota,
// and when the user spends a usage reset credit. A stop must keep the saved limits for the next start.
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AccountUsage } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AgentStore } from "../agent-store";
import { UsageLimitGate, type UsageLimitKey } from "./usage-limit-gate";

const MODEL = "gpt-6.1-sol";
const agent = { id: "agent-codex", provider: "codex", model: MODEL };
const roots: string[] = [];

afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

function reading(usedPercent: number, resetsAt: number): AccountUsage {
  return { limits: [{ id: "codex", primary: { usedPercent, windowDurationMins: 300, resetsAt }, secondary: null }] };
}

async function harness() {
  const root = await mkdtemp(join(tmpdir(), "openbot-usage-limit-restore-"));
  roots.push(root);
  const statePath = join(root, "usage-limits.json");
  let usage: AccountUsage | null = null;
  const released: UsageLimitKey[] = [];
  const make = () =>
    new UsageLimitGate({
      // biome-ignore lint/nursery/noUnsafeTypeAssertion: the gate reads only the agent list of its store.
      store: { list: () => [agent] } as unknown as AgentStore,
      statePath,
      hooks: {
        emit: () => {},
        emitRuntimeSnapshot: () => {},
        scheduleDrain: () => {},
        readUsage: () => Effect.succeed(usage),
        held: () => Effect.void,
        released: (limit) => Effect.sync(() => released.push(limit)),
      },
    });
  return {
    make,
    released,
    setUsage: (next: AccountUsage | null) => {
      usage = next;
    },
    saved: async () => JSON.parse(await readFile(statePath, "utf8")),
  };
}

const key: UsageLimitKey = { provider: "codex", accountId: null, model: MODEL };

describe("usage limits saved across a restart", () => {
  it("a stop keeps the saved limit, and a new gate holds the agent", async () => {
    const test = await harness();
    const resetsAt = Math.floor(Date.now() / 1_000) + 3_600;
    const before = test.make();
    await Effect.runPromise(before.reached(agent.id, resetsAt));
    await Effect.runPromise(before.dispose());
    expect((await test.saved()).limits).toEqual([{ ...key, resetsAt, resetFromUsage: false }]);
    const after = test.make();
    after.restore();
    expect(after.mayDrain(agent.id)).toBe(false);
    expect(after.isLimited(key)).toBe(true);
    await Effect.runPromise(after.dispose());
  });

  it("a fresh plan reading that shows quota ends a restored limit", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const test = await harness();
    test.setUsage(reading(100, Math.floor(Date.now() / 1_000) + 5 * 3_600));
    const before = test.make();
    await Effect.runPromise(before.reached(agent.id, null));
    await vi.advanceTimersByTimeAsync(0);
    expect((await test.saved()).limits[0]).toMatchObject({ ...key, resetFromUsage: true });
    await Effect.runPromise(before.dispose());
    const after = test.make();
    after.restore();
    expect(after.isLimited(key)).toBe(true);
    test.setUsage(reading(20, Math.floor(Date.now() / 1_000) + 5 * 3_600));
    await vi.advanceTimersByTimeAsync(15 * 60_000 + 1_000);
    expect(after.isLimited(key)).toBe(false);
    expect(test.released).toEqual([key]);
    expect((await test.saved()).limits).toEqual([]);
    await Effect.runPromise(after.dispose());
  });

  it("a usage reset credit ends a restored limit", async () => {
    const test = await harness();
    const before = test.make();
    await Effect.runPromise(before.reached(agent.id, Math.floor(Date.now() / 1_000) + 3_600));
    await Effect.runPromise(before.dispose());
    const after = test.make();
    after.restore();
    await Effect.runPromise(after.accountReset("codex", null));
    expect(after.isLimited(key)).toBe(false);
    expect((await test.saved()).limits).toEqual([]);
    await Effect.runPromise(after.dispose());
  });
});
