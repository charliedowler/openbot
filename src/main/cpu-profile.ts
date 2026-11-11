import { mkdir, writeFile } from "node:fs/promises";
import { Session } from "node:inspector/promises";
import { join } from "node:path";

export const CPU_PROFILE_MAX_SECONDS = 120;
export const CPU_PROFILE_DEFAULT_SECONDS = 30;
export const CPU_PROFILE_MIN_INTERVAL_US = 100;
export const CPU_PROFILE_DEFAULT_INTERVAL_US = 1000;

export interface CpuProfileResult {
  path: string;
  seconds: number;
  intervalUs: number;
  samples: number;
  bytes: number;
}

export class CpuProfiler {
  readonly #directory: string;
  readonly #now: () => Date;
  #running = false;

  constructor(directory: string, now: () => Date = () => new Date()) {
    this.#directory = directory;
    this.#now = now;
  }

  get running(): boolean {
    return this.#running;
  }

  async take(seconds: number, intervalUs: number): Promise<CpuProfileResult> {
    if (this.#running) throw new CpuProfileBusy();
    this.#running = true;
    const session = new Session();
    try {
      session.connect();
      await session.post("Profiler.enable");
      await session.post("Profiler.setSamplingInterval", { interval: intervalUs });
      await session.post("Profiler.start");
      await new Promise((done) => setTimeout(done, seconds * 1000));
      const { profile } = await session.post("Profiler.stop");
      await mkdir(this.#directory, { recursive: true, mode: 0o700 });
      const path = join(this.#directory, `cpu-profile-${this.#now().toISOString().replaceAll(":", "")}.cpuprofile`);
      const text = JSON.stringify(profile);
      await writeFile(path, text, { mode: 0o600 });
      return { path, seconds, intervalUs, samples: profile.samples?.length ?? 0, bytes: Buffer.byteLength(text) };
    } finally {
      await session.post("Profiler.disable").catch(() => undefined);
      session.disconnect();
      this.#running = false;
    }
  }
}

export class CpuProfileBusy extends Error {
  constructor() {
    super("A CPU profile is already running.");
  }
}

export function cpuProfileRequest(
  seconds: string | null,
  intervalUs: string | null,
): { seconds: number; intervalUs: number } | null {
  const parsedSeconds = seconds === null ? CPU_PROFILE_DEFAULT_SECONDS : Number(seconds);
  const parsedInterval = intervalUs === null ? CPU_PROFILE_DEFAULT_INTERVAL_US : Number(intervalUs);
  if (!Number.isInteger(parsedSeconds) || parsedSeconds < 1 || parsedSeconds > CPU_PROFILE_MAX_SECONDS) return null;
  if (!Number.isInteger(parsedInterval) || parsedInterval < CPU_PROFILE_MIN_INTERVAL_US || parsedInterval > 100_000)
    return null;
  return { seconds: parsedSeconds, intervalUs: parsedInterval };
}
