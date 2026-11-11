import { createOpenBotLogger } from "@openbot/logging";

const logger = createOpenBotLogger("slow-call");

export const SLOW_CALL_MS = 250;
export const SLOW_CALL_LOG_INTERVAL_MS = 10_000;

export type SlowCallCounts = Record<string, number | boolean | null>;

const loggedAt = new Map<string, number>();
const notLogged = new Map<string, number>();

export function noteCallTime(name: string, startedAt: number, counts: () => SlowCallCounts): void {
  const durationMs = performance.now() - startedAt;
  if (durationMs <= SLOW_CALL_MS) return;
  const now = Date.now();
  const last = loggedAt.get(name);
  if (last !== undefined && now - last < SLOW_CALL_LOG_INTERVAL_MS) {
    notLogged.set(name, (notLogged.get(name) ?? 0) + 1);
    return;
  }
  loggedAt.set(name, now);
  const slowCallsNotLogged = notLogged.get(name) ?? 0;
  notLogged.delete(name);
  logger.warn("A call took more than 250 ms.", {
    function: name,
    durationMs: Math.round(durationMs),
    slowCallsNotLogged,
    ...counts(),
  });
}

export function timedCall<T>(name: string, work: () => T, counts: (result: T) => SlowCallCounts): T {
  const startedAt = performance.now();
  const result = work();
  noteCallTime(name, startedAt, () => counts(result));
  return result;
}
