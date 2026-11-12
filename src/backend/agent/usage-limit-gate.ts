import { closeSync, fsyncSync, openSync, readFileSync, renameSync, rmSync, writeSync } from "node:fs";
import { INPUT_LIMITS } from "@openbot/contracts/input-limits";
import {
  type AccountUsage,
  type AgentEvent,
  type AgentModelChoice,
  type AgentRuntimeSnapshot,
  isAgentProvider,
} from "@openbot/contracts/ipc";
import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { createOpenBotLogger } from "@openbot/logging";
import { Effect, Exit, Schema, Scope } from "effect";
import type { AgentProvider } from "../agent-client";
import type { AgentStore } from "../agent-store";
import { isEmptyStoredFile, parseStoredJson } from "../atomic-json-file";
import { causeHelpers } from "../effect-boundary";
import { agentProviderAccount as accountOf, providerForAgent } from "./thread-items";

/** A provider client's report that the plan refused this turn, with the reset in epoch seconds when it gave one. */
export const USAGE_LIMIT_METHOD = "openbot/usageLimit";

/**
 * How often a limit with no reported reset, or with a reset from a usage reading, is read again. A
 * usage read is one request, not a turn.
 */
const USAGE_LIMIT_RECHECK_MS = 15 * 60_000;

/** Time after the reported reset before work starts again, so the provider's clock has passed it too. */
const RESET_GRACE_MS = 30_000;

/**
 * How long a model that failed a turn with a provider error is passed over, so an agent on a backup
 * does not go back to a model that still fails at once. An error that names a later reset, such as
 * a spent Gemini quota, holds the model until that reset (#4583).
 */
const PROVIDER_FAILURE_HOLD_MS = 10 * 60_000;

const PROVIDER_RETRY_DELAY_MS = 30_000;

export const USAGE_LIMIT_PROBE_MS = 15 * 60_000;

/** The longest delay `setTimeout` keeps. A longer wait is armed again when it fires. */
const MAX_TIMER_MS = 2_147_483_647;

const STATE_VERSION = 1;

const logger = createOpenBotLogger("usage-limit-gate");

export interface UsageLimitHooks {
  emit(event: AgentEvent): void;
  emitRuntimeSnapshot(): void;
  scheduleDrain(agentId: string): void;
  /** The account's usage reading for this model, or null when it gives none. A null account is the provider's own sign-in. */
  readUsage(
    provider: AgentProvider,
    model: string,
    accountId: string | null,
  ): Effect.Effect<AccountUsage | null, UsageReadFailed>;
  /** The agents a limit now holds, each time a refused turn reports it. Must not wait for their queues. */
  held(agentIds: readonly string[]): Effect.Effect<void>;
  /** A limit ended, after its agents' drains were scheduled. Must not wait for the work it starts. */
  released(limit: UsageLimitKey): Effect.Effect<void>;
  waiting?(agentId: string): boolean;
  busy?(agentId: string): boolean;
}

export interface UsageLimitGateOptions {
  store: AgentStore;
  hooks: UsageLimitHooks;
  retryDelayMs?: number;
  /** The file that keeps the limits and provider-error holds across a restart. Omitted, they stay in memory. */
  statePath?: string | null;
  probeIntervalMs?: number;
}

export interface UsageLimitKey {
  provider: AgentProvider;
  /** The provider account after the first, or null for the provider's own sign-in. */
  accountId: string | null;
  model: string;
}

interface ProviderFailure extends UsageLimitKey {
  /** Epoch milliseconds when the hold ends. */
  endsAt: number;
  timer: ReturnType<typeof setTimeout>;
}

interface UsageLimit extends UsageLimitKey {
  /** Epoch seconds, or null while the provider has not said. */
  resetsAt: number | null;
  /**
   * The reset came from a usage reading, not from the refusal. The provider can end such a limit
   * before it, as when a reset credit is spent in the Codex app, so it is read again.
   */
  resetFromUsage: boolean;
  timer: ReturnType<typeof setTimeout> | null;
  probeTimer: ReturnType<typeof setTimeout> | null;
  agentId: string | null;
  lastProbeAgentId: string | null;
}

interface UsageLimitProbe {
  agentId: string;
  started: boolean;
}

type UsageLimitReleaseReason =
  | "turn-completed"
  | "probe-completed"
  | "reset-passed"
  | "usage-reading"
  | "account-reset"
  | "host-reset";

interface UsageLimitRelease {
  reason: UsageLimitReleaseReason;
  agentId?: string;
}

/**
 * The plan limits of the provider accounts, and the queues they hold.
 *
 * Owns one entry per provider account and model whose last turn the plan refused. While an entry exists, no
 * agent on that provider and model starts a turn: its messages and routine runs stay queued instead
 * of failing one by one. A model is part of the key because a weekly window can cover one model
 * family only; an account-wide window costs each other model one refused, requeued turn.
 *
 * The entry ends at the reported reset, after a usage read that finds no spent window, or when a
 * turn on that provider and model completes. The entries and the provider-error holds are also saved
 * to `statePath` and read again by `restore`, so a restart does not send agents back to a model that
 * is still limited. Entries whose reset or hold passed while the server was down are dropped. A
 * message from the user lets its agent try one turn before
 * that: the user can have signed the provider in to another account, or bought more usage, and no
 * reading shows that before the reset.
 *
 * It also owns the models that failed a turn with a provider error. They hold no queue: they only
 * move agents set to switch without asking to a backup, and keep them there for a while.
 */
export class UsageLimitGate {
  readonly #store: AgentStore;
  readonly #hooks: UsageLimitHooks;
  readonly #limits = new Map<string, UsageLimit>();
  /** Owns the usage reads and the timer runs, so `dispose` stops them. */
  #scope = Scope.makeUnsafe();
  /** Providers the user was told about in this limit, so a limit that closes again is not announced twice. */
  readonly #announced = new Set<AgentProvider>();
  readonly #failures = new Map<string, ProviderFailure>();
  /** The agents that may start one turn while their limit holds, with that limit's key. */
  readonly #tries = new Map<string, string>();
  readonly #retries = new Map<string, ReturnType<typeof setTimeout>>();
  readonly #retryDelayMs: number;
  readonly #statePath: string | null;
  readonly #probes = new Map<string, UsageLimitProbe>();
  readonly #probeMs: number;

  constructor(options: UsageLimitGateOptions) {
    this.#store = options.store;
    this.#hooks = options.hooks;
    this.#retryDelayMs = options.retryDelayMs ?? PROVIDER_RETRY_DELAY_MS;
    this.#statePath = options.statePath ?? null;
    this.#probeMs = options.probeIntervalMs ?? USAGE_LIMIT_PROBE_MS;
  }

  /**
   * Reads the saved limits and provider-error holds, before any drain runs. A limit whose reset has
   * passed, or a hold that has ended, is dropped. A missing or unreadable file starts with none.
   */
  restore(): void {
    const path = this.#statePath;
    if (!path) return;
    let saved: SavedState;
    try {
      const contents = readFileSync(path, "utf8");
      saved = isEmptyStoredFile(contents) ? emptyState() : parseStoredJson(path, contents, decodeState);
    } catch (error) {
      const missing = isDynamicRecord(error) && error.code === "ENOENT";
      logger.warn(
        missing
          ? "No saved usage limits. The gate starts with none."
          : "The saved usage limits are unreadable. The gate starts with none.",
        { path, ...(missing ? {} : { error: error instanceof Error ? error.message : String(error) }) },
      );
      return;
    }
    const now = Date.now();
    let dropped = 0;
    for (const entry of saved.limits) {
      if (entry.resetsAt !== null && entry.resetsAt * 1_000 + RESET_GRACE_MS <= now) {
        dropped += 1;
        continue;
      }
      const key = limitKey(entry.provider, entry.accountId, entry.model);
      const previous = this.#limits.get(key);
      if (previous?.timer) clearTimeout(previous.timer);
      const limit: UsageLimit = { ...entry, timer: null, probeTimer: null, agentId: null, lastProbeAgentId: null };
      this.#limits.set(key, limit);
      this.#announced.add(entry.provider);
      this.#arm(key, limit);
      this.#armProbe(key, limit);
      logger.info("A usage limit holds a model.", { ...describeLimit(limit), cause: "restored" });
    }
    for (const entry of saved.failures) {
      if (entry.until <= now) {
        dropped += 1;
        continue;
      }
      const key = limitKey(entry.provider, entry.accountId, entry.model);
      this.#holdFailure(key, entry, entry.until);
    }
    logger.info("Restored the saved usage limits.", {
      limits: this.#limits.size,
      failures: this.#failures.size,
      dropped,
    });
    if (dropped > 0) this.#save();
  }

  holdForRetry(agentId: string): void {
    const previous = this.#retries.get(agentId);
    if (previous) clearTimeout(previous);
    this.#retries.set(
      agentId,
      setTimeout(() => {
        this.#retries.delete(agentId);
        this.#hooks.scheduleDrain(agentId);
      }, this.#retryDelayMs),
    );
  }

  retryHeld(agentId: string): boolean {
    return this.#retries.has(agentId);
  }

  mayDrain(agentId: string, execution?: AgentModelChoice): boolean {
    const agent = this.#agent(agentId, execution);
    return !agent || !this.#limits.has(limitKey(providerForAgent(agent), accountOf(agent), agent.model));
  }

  /**
   * The user sent this agent a message while a limit holds it, so the agent may start one turn. The
   * try ends when that turn starts: a refused turn queues its messages again, and a completed one
   * ends the limit.
   */
  retry(agentId: string): void {
    const agent = this.#agent(agentId);
    if (!agent) return;
    const key = limitKey(providerForAgent(agent), accountOf(agent), agent.model);
    if (this.#limits.has(key)) this.#tries.set(agentId, key);
  }

  /** Whether the agent may start a turn that its limit holds, because the user sent it a message. */
  mayTry(agentId: string, execution?: AgentModelChoice): boolean {
    const agent = this.#agent(agentId, execution);
    return (
      agent !== undefined &&
      this.#tries.get(agentId) === limitKey(providerForAgent(agent), accountOf(agent), agent.model)
    );
  }

  /** A turn of this agent starts, so a try that let it start ends. */
  tryStarted(agentId: string): void {
    const key = this.#tries.get(agentId);
    this.#tries.delete(agentId);
    const probe = key === undefined ? undefined : this.#probes.get(key);
    if (probe?.agentId === agentId) probe.started = true;
  }

  /** Whether a limit holds this provider account and model. */
  isLimited(limit: UsageLimitKey): boolean {
    return this.#limits.has(limitKey(limit.provider, limit.accountId, limit.model));
  }

  /**
   * The provider account and model that `failed` would hold for this agent's turn on `model`, as one
   * key. Null when the agent no longer exists.
   */
  failureKey(agentId: string, model: string | null = null, execution?: AgentModelChoice): string | null {
    const agent = this.#agent(agentId, execution);
    return agent ? limitKey(providerForAgent(agent), accountOf(agent), model ?? agent.model) : null;
  }

  /** Whether a provider error passes over this provider account and model. */
  isFailed(limit: UsageLimitKey): boolean {
    return this.#failures.has(limitKey(limit.provider, limit.accountId, limit.model));
  }

  /**
   * A provider error failed a turn of this agent on `model`, the agent's current model when null.
   * The model is passed over until the hold ends or a turn on it completes. Then the agents that
   * left it go back. `resetDelayMs` is the wait until the reset that the error names, when it names one.
   */
  failed(
    agentId: string,
    model: string | null = null,
    resetDelayMs: number | null = null,
    execution?: AgentModelChoice,
  ): void {
    const agent = this.#agent(agentId, execution);
    if (!agent) return;
    const provider = providerForAgent(agent);
    const accountId = accountOf(agent);
    const turnModel = model ?? agent.model;
    const key = limitKey(provider, accountId, turnModel);
    const previous = this.#failures.get(key);
    // A later error with no reset, such as from another agent on the same model, keeps the named reset.
    const hold = Math.max(PROVIDER_FAILURE_HOLD_MS, (resetDelayMs ?? 0) + RESET_GRACE_MS);
    const endsAt = Math.max(previous?.endsAt ?? 0, Date.now() + hold);
    this.#holdFailure(key, { provider, accountId, model: turnModel }, endsAt);
    this.#save();
  }

  #holdFailure(key: string, failure: UsageLimitKey, endsAt: number): void {
    const previous = this.#failures.get(key);
    if (previous) clearTimeout(previous.timer);
    this.#failures.set(key, {
      provider: failure.provider,
      accountId: failure.accountId,
      model: failure.model,
      endsAt,
      timer: this.#armFailure(key, endsAt),
    });
  }

  /** Releases the failure at `endsAt`. A wait longer than `MAX_TIMER_MS` is armed again when it fires. */
  #armFailure(key: string, endsAt: number): ReturnType<typeof setTimeout> {
    return setTimeout(
      () => {
        const failure = this.#failures.get(key);
        if (failure && failure.endsAt > Date.now()) failure.timer = this.#armFailure(key, failure.endsAt);
        else Effect.runFork(this.#releaseFailure(key).pipe(Effect.forkIn(this.#scope)));
      },
      Math.min(MAX_TIMER_MS, endsAt - Date.now()),
    );
  }

  /** The agents whose queue a limit holds, for the runtime snapshot. */
  limitedAgents(): NonNullable<AgentRuntimeSnapshot["usageLimits"]> {
    return this.#store.list().flatMap((agent) => {
      const limit = this.#limits.get(limitKey(providerForAgent(agent), accountOf(agent), agent.model));
      return limit ? [{ agentId: agent.id, resetsAt: limit.resetsAt }] : [];
    });
  }

  /** Each provider account and model that a limit holds, for the runtime snapshot. */
  limitedModels(): NonNullable<AgentRuntimeSnapshot["usageLimitedModels"]> {
    return [...this.#limits.values()].slice(0, INPUT_LIMITS.agents).map((limit) => ({
      provider: limit.provider,
      model: limit.model,
      ...(limit.accountId ? { providerAccountId: limit.accountId } : {}),
    }));
  }

  /**
   * The plan refused a turn of this agent. `resetsAt` is in epoch seconds. `model` is the one the
   * turn ran on; the agent's current model when null.
   */
  readonly reached = Effect.fn("UsageLimitGate.reached")(function* (
    this: UsageLimitGate,
    agentId: string,
    resetsAt: number | null,
    model: string | null = null,
    execution?: AgentModelChoice,
  ) {
    const agent = this.#agent(agentId, execution);
    if (!agent) return;
    const provider = providerForAgent(agent);
    const turnModel = model ?? agent.model;
    const accountId = accountOf(agent);
    const key = limitKey(provider, accountId, turnModel);
    const existing = this.#limits.get(key);
    const limit = existing ?? {
      provider,
      accountId,
      model: turnModel,
      resetsAt: null,
      resetFromUsage: false,
      timer: null,
      probeTimer: null,
      agentId: null,
      lastProbeAgentId: null,
    };
    const probeRefused = this.#probes.get(key)?.agentId === agentId;
    if (probeRefused) this.#probes.delete(key);
    limit.agentId = agentId;
    // A reset already past would release the queue at once into the same refusal.
    const future = (value: number | null) => (value !== null && value * 1_000 > Date.now() ? value : null);
    const named = future(resetsAt);
    limit.resetsAt = named ?? future(limit.resetsAt);
    if (named !== null) limit.resetFromUsage = false;
    this.#limits.set(key, limit);
    this.#arm(key, limit);
    const probeArmed = !existing || probeRefused;
    if (probeArmed) this.#armProbe(key, limit);
    this.#save();
    const heldAgents = this.#heldAgents(limit);
    logger.info(
      probeRefused
        ? "A usage limit probe was refused. The limit holds the model again."
        : "A usage limit holds a model.",
      {
        ...describeLimit(limit),
        agentId,
        cause: probeRefused ? "probe-refused" : "refused",
        heldAgents: heldAgents.length,
        ...(probeArmed ? { nextProbeInSeconds: Math.round(this.#probeMs / 1_000) } : {}),
      },
    );
    yield* this.#hooks.held(heldAgents.map((held) => held.id));
    this.#hooks.emitRuntimeSnapshot();
    if (limit.resetsAt !== null) {
      this.#announce(agentId, limit);
      return;
    }
    // A provider that names no reset in its refusal usually reports it in its usage reading.
    yield* this.#readReset(key, limit).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          if (this.#limits.get(key) === limit) this.#announce(agentId, limit);
        }),
      ),
      Effect.forkIn(this.#scope),
    );
  }).bind(this);

  /** A turn of this agent completed on `model`, so its provider and model take turns again. */
  readonly completed = Effect.fn("UsageLimitGate.completed")(function* (
    this: UsageLimitGate,
    agentId: string,
    model: string | null = null,
    execution?: AgentModelChoice,
  ) {
    const agent = this.#agent(agentId, execution);
    if (!agent) return;
    const provider = providerForAgent(agent);
    const key = limitKey(provider, accountOf(agent), model ?? agent.model);
    if (this.#limits.has(key))
      yield* this.#release(key, false, {
        reason: this.#probes.get(key)?.agentId === agentId ? "probe-completed" : "turn-completed",
        agentId,
      });
    if (this.#failures.has(key)) yield* this.#releaseFailure(key);
    if (![...this.#limits.values()].some((limit) => limit.provider === provider)) this.#announced.delete(provider);
  }).bind(this);

  /**
   * The user spent a usage reset credit of a provider account, so each limit of that account ends now
   * and its agents take turns again. A null account is the provider's own sign-in.
   */
  readonly accountReset = Effect.fn("UsageLimitGate.accountReset")(function* (
    this: UsageLimitGate,
    provider: AgentProvider,
    accountId: string | null,
    reason: "account-reset" | "host-reset" = "account-reset",
  ) {
    const released: UsageLimitKey[] = [];
    for (const [key, limit] of [...this.#limits]) {
      if (limit.provider !== provider || limit.accountId !== accountId) continue;
      released.push({ provider: limit.provider, accountId: limit.accountId, model: limit.model });
      yield* this.#release(key, true, { reason });
    }
    return released;
  }).bind(this);

  readonly dispose = Effect.fn("UsageLimitGate.dispose")(function* (this: UsageLimitGate) {
    for (const limit of this.#limits.values()) {
      if (limit.timer) clearTimeout(limit.timer);
      if (limit.probeTimer) clearTimeout(limit.probeTimer);
    }
    this.#limits.clear();
    this.#probes.clear();
    for (const failure of this.#failures.values()) clearTimeout(failure.timer);
    this.#failures.clear();
    for (const timer of this.#retries.values()) clearTimeout(timer);
    this.#retries.clear();
    this.#tries.clear();
    this.#announced.clear();
    yield* Scope.close(this.#scope, Exit.void);
    this.#scope = Scope.makeUnsafe();
  }).bind(this);

  #agent(agentId: string, execution?: AgentModelChoice) {
    const agent = this.#store.list().find((candidate) => candidate.id === agentId);
    return agent && execution ? { ...agent, ...execution, providerAccountId: execution.providerAccountId } : agent;
  }

  #announce(agentId: string, limit: UsageLimit): void {
    if (this.#announced.has(limit.provider)) return;
    this.#announced.add(limit.provider);
    this.#hooks.emit({
      type: "usage-limit-reached",
      agentId,
      provider: limit.provider,
      resetsAt: limit.resetsAt,
      agentCount: this.#heldAgents(limit).length,
    });
  }

  #arm(key: string, limit: UsageLimit): void {
    if (limit.timer) clearTimeout(limit.timer);
    const untilReset =
      limit.resetsAt === null ? USAGE_LIMIT_RECHECK_MS : limit.resetsAt * 1_000 - Date.now() + RESET_GRACE_MS;
    const delay = limit.resetFromUsage ? Math.min(untilReset, USAGE_LIMIT_RECHECK_MS) : untilReset;
    limit.timer = setTimeout(
      () => Effect.runFork(this.#fire(key, limit).pipe(Effect.forkIn(this.#scope))),
      Math.max(0, Math.min(delay, MAX_TIMER_MS)),
    );
  }

  #armProbe(key: string, limit: UsageLimit): void {
    if (limit.probeTimer) clearTimeout(limit.probeTimer);
    limit.probeTimer = setTimeout(() => this.#probe(key, limit), Math.max(0, Math.min(this.#probeMs, MAX_TIMER_MS)));
  }

  #probe(key: string, limit: UsageLimit): void {
    if (this.#limits.get(key) !== limit) return;
    this.#armProbe(key, limit);
    const current = this.#probes.get(key);
    if (current) {
      if (current.started && (this.#hooks.busy?.(current.agentId) ?? false)) return;
      this.#probes.delete(key);
      if (this.#tries.get(current.agentId) === key) this.#tries.delete(current.agentId);
    }
    const waiting = this.#heldAgents(limit).filter((agent) => this.#hooks.waiting?.(agent.id) ?? false);
    if (waiting.length === 0) return;
    const after = waiting.findIndex((agent) => agent.id === limit.lastProbeAgentId);
    const agent = waiting[(after + 1) % waiting.length];
    if (!agent) return;
    limit.lastProbeAgentId = agent.id;
    this.#probes.set(key, { agentId: agent.id, started: false });
    this.#tries.set(agent.id, key);
    logger.info("A usage limit lets one waiting turn try.", {
      ...describeLimit(limit),
      agentId: agent.id,
      waitingAgents: waiting.length,
    });
    this.#hooks.scheduleDrain(agent.id);
  }

  readonly #fire = Effect.fn("UsageLimitGate.fire")(function* (this: UsageLimitGate, key: string, limit: UsageLimit) {
    if (this.#limits.get(key) !== limit) return;
    limit.timer = null;
    if (limit.resetsAt !== null && limit.resetsAt * 1_000 + RESET_GRACE_MS <= Date.now()) {
      yield* this.#release(key, true, { reason: "reset-passed" });
      return;
    }
    if (limit.resetsAt !== null && !limit.resetFromUsage) {
      this.#arm(key, limit);
      return;
    }
    // A reading that still shows a spent window gives the reset, and one that shows none lets one turn
    // try. With no reset known, no reading lets one turn try too; a reset from a reading waits for it.
    const known = limit.resetsAt !== null;
    const resetsAt = yield* this.#readReset(key, limit);
    if (this.#limits.get(key) !== limit || typeof resetsAt === "number") return;
    if (resetsAt === null || !known) yield* this.#release(key, false, { reason: "usage-reading" });
    else this.#arm(key, limit);
  });

  /**
   * Reads the reset of the spent window into `limit` and arms it. Null when the reading shows none,
   * undefined when there is no reading, it fails, or it has no window.
   */
  readonly #readReset = Effect.fn("UsageLimitGate.readReset")(function* (
    this: UsageLimitGate,
    key: string,
    limit: UsageLimit,
  ) {
    const usage = yield* this.#hooks
      .readUsage(limit.provider, limit.model, limit.accountId)
      .pipe(Effect.catch(() => Effect.succeed(null)));
    if (this.#limits.get(key) !== limit) return null;
    // A reset that a refusal named while the reading ran stays.
    if (limit.resetsAt !== null && !limit.resetFromUsage) return limit.resetsAt;
    // A provider that does not run, or a reading that does not cover the model, has no window.
    if (!usage?.limits.some((reading) => reading.primary || reading.secondary)) return undefined;
    const resetsAt = spentWindowReset(usage);
    if (resetsAt === null) return null;
    const changed = limit.resetsAt !== resetsAt;
    limit.resetsAt = resetsAt;
    limit.resetFromUsage = true;
    this.#arm(key, limit);
    this.#save();
    if (changed) this.#hooks.emitRuntimeSnapshot();
    return resetsAt;
  });

  /**
   * Ends a limit. At the reset the provider reported, the limit is over, so the next one is announced
   * again. A probe of a limit with no known reset keeps the announcement: the next refused turn may
   * only close the same limit again.
   */
  readonly #release = Effect.fn("UsageLimitGate.release")(function* (
    this: UsageLimitGate,
    key: string,
    reset: boolean,
    cause: UsageLimitRelease,
  ) {
    const limit = this.#limits.get(key);
    if (!limit) return;
    if (limit.timer) clearTimeout(limit.timer);
    if (limit.probeTimer) clearTimeout(limit.probeTimer);
    this.#limits.delete(key);
    this.#probes.delete(key);
    this.#save();
    logger.info("A usage limit ended.", {
      ...describeLimit(limit),
      reason: cause.reason,
      ...(cause.agentId ? { agentId: cause.agentId } : {}),
      heldBy: limit.agentId,
      heldAgents: this.#heldAgents(limit).length,
    });
    for (const [agentId, tryKey] of this.#tries) if (tryKey === key) this.#tries.delete(agentId);
    if (reset && ![...this.#limits.values()].some((other) => other.provider === limit.provider))
      this.#announced.delete(limit.provider);
    this.#hooks.emitRuntimeSnapshot();
    for (const agent of this.#affectedAgents(limit)) this.#hooks.scheduleDrain(agent.id);
    yield* this.#hooks.released({ provider: limit.provider, accountId: limit.accountId, model: limit.model });
  });

  readonly #releaseFailure = Effect.fn("UsageLimitGate.releaseFailure")(function* (this: UsageLimitGate, key: string) {
    const failure = this.#failures.get(key);
    if (!failure) return;
    clearTimeout(failure.timer);
    this.#failures.delete(key);
    this.#save();
    yield* this.#hooks.released({ provider: failure.provider, accountId: failure.accountId, model: failure.model });
  });

  /**
   * Replaces the saved state with the current limits and holds: a temporary file, flushed, then renamed
   * over the old one. `dispose` does not call it, so a stop keeps the saved state for the next start.
   */
  #save(): void {
    const path = this.#statePath;
    if (!path) return;
    const state: SavedState = {
      version: STATE_VERSION,
      limits: [...this.#limits.values()].map(({ provider, accountId, model, resetsAt, resetFromUsage }) => ({
        provider,
        accountId,
        model,
        resetsAt,
        resetFromUsage,
      })),
      failures: [...this.#failures.values()].map(({ provider, accountId, model, endsAt }) => ({
        provider,
        accountId,
        model,
        until: endsAt,
      })),
    };
    const temporaryPath = `${path}.${process.pid}.tmp`;
    try {
      const handle = openSync(temporaryPath, "w", 0o600);
      try {
        writeSync(handle, `${JSON.stringify(state)}\n`);
        fsyncSync(handle);
      } finally {
        closeSync(handle);
      }
      renameSync(temporaryPath, path);
    } catch (error) {
      rmSync(temporaryPath, { force: true });
      logger.warn("The usage limits were not saved.", {
        path,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  #heldAgents(limit: UsageLimit) {
    return this.#store
      .list()
      .filter(
        (agent) =>
          providerForAgent(agent) === limit.provider &&
          accountOf(agent) === limit.accountId &&
          agent.model === limit.model,
      );
  }

  #affectedAgents(limit: UsageLimit) {
    return this.#store
      .list()
      .filter((agent) =>
        [agent, agent.routineModel, agent.channelSummaryModel].some(
          (choice) =>
            choice &&
            providerForAgent(choice) === limit.provider &&
            accountOf(choice) === limit.accountId &&
            choice.model === limit.model,
        ),
      );
  }
}

function describeLimit(limit: UsageLimit) {
  return {
    provider: limit.provider,
    accountId: limit.accountId,
    model: limit.model,
    resetsAt: limit.resetsAt,
    resetsAtIso: limit.resetsAt === null ? null : new Date(limit.resetsAt * 1_000).toISOString(),
  };
}

function limitKey(provider: AgentProvider, accountId: string | null, model: string): string {
  return `${provider}\u0000${accountId ?? ""}\u0000${model}`;
}

interface SavedLimit extends UsageLimitKey {
  resetsAt: number | null;
  resetFromUsage: boolean;
}

interface SavedFailure extends UsageLimitKey {
  until: number;
}

interface SavedState {
  version: number;
  limits: SavedLimit[];
  failures: SavedFailure[];
}

function emptyState(): SavedState {
  return { version: STATE_VERSION, limits: [], failures: [] };
}

function decodeKey(value: unknown): UsageLimitKey | null {
  if (!isDynamicRecord(value) || !isAgentProvider(value.provider) || typeof value.model !== "string" || !value.model)
    return null;
  if (value.accountId !== null && (typeof value.accountId !== "string" || !value.accountId)) return null;
  return { provider: value.provider, accountId: value.accountId, model: value.model };
}

function decodeState(value: unknown): SavedState {
  if (
    !isDynamicRecord(value) ||
    value.version !== STATE_VERSION ||
    !Array.isArray(value.limits) ||
    !Array.isArray(value.failures)
  )
    throw new Error("The usage limit file has an unknown shape.");
  const limits = value.limits.map((entry: unknown): SavedLimit => {
    const key = decodeKey(entry);
    if (
      !key ||
      !isDynamicRecord(entry) ||
      !(entry.resetsAt === null || (typeof entry.resetsAt === "number" && Number.isFinite(entry.resetsAt))) ||
      typeof entry.resetFromUsage !== "boolean"
    )
      throw new Error("The usage limit file has an invalid limit.");
    return { ...key, resetsAt: entry.resetsAt, resetFromUsage: entry.resetFromUsage };
  });
  const failures = value.failures.map((entry: unknown): SavedFailure => {
    const key = decodeKey(entry);
    if (!key || !isDynamicRecord(entry) || typeof entry.until !== "number" || !Number.isFinite(entry.until))
      throw new Error("The usage limit file has an invalid hold.");
    return { ...key, until: entry.until };
  });
  return { version: STATE_VERSION, limits, failures };
}

/** The latest future reset among the spent windows of a reading, in epoch seconds. */
function spentWindowReset(usage: AccountUsage): number | null {
  const now = Date.now() / 1_000;
  let latest: number | null = null;
  for (const limit of usage.limits) {
    for (const window of [limit.primary, limit.secondary]) {
      if (!window || window.usedPercent < 100 || window.resetsAt === null || window.resetsAt <= now) continue;
      latest = Math.max(latest ?? window.resetsAt, window.resetsAt);
    }
  }
  return latest;
}

export class UsageReadFailed extends Schema.TaggedError<UsageReadFailed>()("UsageReadFailed", {
  cause: Schema.Defect(),
}) {}

export const { rewrap: toUsageReadFailed } = causeHelpers(UsageReadFailed);
