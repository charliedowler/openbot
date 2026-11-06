import { createOpenBotLogger } from "@openbot/logging";
import { Effect } from "effect";

const logger = createOpenBotLogger("turn-reaper");

const HOUR_MS = 60 * 60 * 1000;
const MAX_STOPS_PER_AGENT_PER_HOUR = 2;
const DEFAULT_STALL_MINUTES = 8;
const DEFAULT_INTERVAL_SECONDS = 60;

export type TurnReaperKind = "orphaned" | "parked";

export interface TurnReaperTurn {
  agentId: string;
  threadId: string;
  turnId: string;
  startedAt: number;
  lastThreadEventAt: number | null;
}

export interface TurnReaperStop {
  turn: TurnReaperTurn;
  kind: TurnReaperKind;
  idleMs: number;
  reason: string;
}

export interface TurnReaperHost {
  runningTurns(): TurnReaperTurn[];
  lastProviderEventAt(agentId: string): number | null;
  providerTurnLive(agentId: string, turnId: string): boolean;
  pendingAttention(agentId: string, turnId: string): boolean;
  nobodyToAnswer(agentId: string, turnId: string, threadId: string): boolean;
  stop(stop: TurnReaperStop): Effect.Effect<void, unknown>;
}

export interface TurnReaperConfig {
  enabled: boolean;
  stallMs: number;
  intervalMs: number;
}

function positiveNumber(value: string | undefined, fallback: number): number {
  const parsed = value === undefined || value.trim() === "" ? Number.NaN : Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function turnReaperConfig(env: NodeJS.ProcessEnv = process.env): TurnReaperConfig {
  const flag = (env.OPENBOT_TURN_REAPER ?? "").trim().toLowerCase();
  return {
    enabled: !["0", "false", "off", "no", "disabled"].includes(flag),
    stallMs: positiveNumber(env.OPENBOT_TURN_REAPER_MINUTES, DEFAULT_STALL_MINUTES) * 60_000,
    intervalMs: positiveNumber(env.OPENBOT_TURN_REAPER_INTERVAL_SECONDS, DEFAULT_INTERVAL_SECONDS) * 1000,
  };
}

export class TurnReaper {
  readonly #host: TurnReaperHost;
  readonly #config: TurnReaperConfig;
  readonly #firstSeen = new Map<string, number>();
  readonly #attempted = new Map<string, number>();
  readonly #stops = new Map<string, number[]>();
  readonly #limitLogged = new Set<string>();
  #timer: ReturnType<typeof setInterval> | null = null;
  #sweeping = false;

  constructor(host: TurnReaperHost, config: TurnReaperConfig = turnReaperConfig()) {
    this.#host = host;
    this.#config = config;
  }

  get config(): TurnReaperConfig {
    return this.#config;
  }

  start(): void {
    if (!this.#config.enabled || this.#timer) return;
    logger.info("Turn reaper started.", {
      stallMinutes: this.#config.stallMs / 60_000,
      intervalSeconds: this.#config.intervalMs / 1000,
      maxStopsPerAgentPerHour: MAX_STOPS_PER_AGENT_PER_HOUR,
    });
    this.#timer = setInterval(() => {
      void Effect.runPromise(this.sweep()).catch((error: unknown) => {
        logger.warn("Turn reaper sweep failed.", { error: error instanceof Error ? error.message : String(error) });
      });
    }, this.#config.intervalMs);
    this.#timer.unref?.();
  }

  dispose(): void {
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = null;
    this.#firstSeen.clear();
    this.#attempted.clear();
    this.#limitLogged.clear();
  }

  readonly sweep = Effect.fn("TurnReaper.sweep")(function* (this: TurnReaper, now: number = Date.now()) {
    if (this.#sweeping) return;
    this.#sweeping = true;
    try {
      const turns = this.#host.runningTurns();
      const running = new Set(turns.map((turn) => turn.turnId));
      for (const turnId of this.#firstSeen.keys()) if (!running.has(turnId)) this.#firstSeen.delete(turnId);
      for (const turnId of this.#attempted.keys()) if (!running.has(turnId)) this.#attempted.delete(turnId);
      for (const turnId of this.#limitLogged) if (!running.has(turnId)) this.#limitLogged.delete(turnId);
      for (const turn of turns) {
        const decision = this.#classify(turn, now);
        if (!decision) continue;
        const attemptedAt = this.#attempted.get(turn.turnId);
        if (attemptedAt !== undefined && now - attemptedAt < this.#config.stallMs) continue;
        const recent = (this.#stops.get(turn.agentId) ?? []).filter((at) => now - at < HOUR_MS);
        this.#stops.set(turn.agentId, recent);
        if (recent.length >= MAX_STOPS_PER_AGENT_PER_HOUR) {
          if (!this.#limitLogged.has(turn.turnId)) {
            this.#limitLogged.add(turn.turnId);
            logger.warn("Turn reaper left a stuck turn running: the agent reached its stop limit for this hour.", {
              ...this.#describe(decision),
              stopsThisHour: recent.length,
            });
          }
          continue;
        }
        recent.push(now);
        this.#attempted.set(turn.turnId, now);
        logger.warn("Turn reaper is stopping a stuck turn.", this.#describe(decision));
        const outcome = yield* Effect.result(this.#host.stop(decision));
        if (outcome._tag === "Failure")
          logger.warn("Turn reaper could not stop a stuck turn.", {
            ...this.#describe(decision),
            error: outcome.failure instanceof Error ? outcome.failure.message : String(outcome.failure),
          });
      }
    } finally {
      this.#sweeping = false;
    }
  });

  #classify(turn: TurnReaperTurn, now: number): TurnReaperStop | null {
    const firstSeen = this.#firstSeen.get(turn.turnId) ?? now;
    this.#firstSeen.set(turn.turnId, firstSeen);
    const lastProgress = Math.max(
      turn.startedAt,
      firstSeen,
      turn.lastThreadEventAt ?? 0,
      this.#host.lastProviderEventAt(turn.agentId) ?? 0,
    );
    const idleMs = now - lastProgress;
    if (idleMs < this.#config.stallMs) return null;
    const minutes = Math.round(idleMs / 60_000);
    if (!this.#host.providerTurnLive(turn.agentId, turn.turnId))
      return {
        turn,
        kind: "orphaned",
        idleMs,
        reason: `No provider process runs this turn and it made no progress for ${minutes} minutes.`,
      };
    if (
      this.#host.pendingAttention(turn.agentId, turn.turnId) &&
      this.#host.nobodyToAnswer(turn.agentId, turn.turnId, turn.threadId)
    )
      return {
        turn,
        kind: "parked",
        idleMs,
        reason: `No person started this turn, and it waited ${minutes} minutes on a request that nobody answers.`,
      };
    return null;
  }

  #describe(stop: TurnReaperStop) {
    return {
      kind: stop.kind,
      agentId: stop.turn.agentId,
      threadId: stop.turn.threadId,
      turnId: stop.turn.turnId,
      idleSeconds: Math.round(stop.idleMs / 1000),
      reason: stop.reason,
    };
  }
}
