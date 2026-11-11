import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { CpuProfileBusy, CpuProfiler, cpuProfileRequest } from "./cpu-profile";

const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

describe("cpuProfileRequest", () => {
  it("takes 1 to 120 seconds and an interval of 100 to 100,000 microseconds", () => {
    expect(cpuProfileRequest(null, null)).toEqual({ seconds: 30, intervalUs: 1000 });
    expect(cpuProfileRequest("120", "100")).toEqual({ seconds: 120, intervalUs: 100 });
    for (const [seconds, interval] of [
      ["0", null],
      ["121", null],
      ["2.5", null],
      ["ten", null],
      [null, "99"],
      [null, "100001"],
    ] as const)
      expect(cpuProfileRequest(seconds, interval)).toBeNull();
  });
});

describe("CpuProfiler", () => {
  it("writes one private profile of this process and refuses a second one while it runs", async () => {
    const root = await mkdtemp(join(tmpdir(), "openbot-cpu-profile-"));
    roots.push(root);
    const profiler = new CpuProfiler(join(root, "logs"), () => new Date("2026-11-11T19:00:00.000Z"));
    const first = profiler.take(1, 1000);
    await expect(profiler.take(1, 1000)).rejects.toBeInstanceOf(CpuProfileBusy);
    const result = await first;
    expect(result.path).toBe(join(root, "logs", "cpu-profile-2026-11-11T190000.000Z.cpuprofile"));
    const profile = JSON.parse(await readFile(result.path, "utf8"));
    expect(Array.isArray(profile.nodes)).toBe(true);
    expect(result.samples).toBe(profile.samples.length);
    expect((await stat(result.path)).mode & 0o777).toBe(0o600);
    expect(profiler.running).toBe(false);
  });
});
