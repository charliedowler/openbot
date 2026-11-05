import {
  type ContextPullRequest,
  type ContextPullRequestScope,
  type ContextPullRequestsInput,
  type ContextPullRequestsSnapshot,
  contextPullRequestIdentity,
} from "@openbot/contracts/context-prs";
import { Effect, Result, Semaphore } from "effect";
import type { ContextPrReferences } from "../backend/context-pr-references";
import { type StoredStateFailure, storedSync } from "../backend/stored-state-effects";
import { ContextPrFailure, readCliContextPr } from "./context-pr-reader";
import type { GitHubConnectorService } from "./github-connector-service";

const REFRESH_MS = 300_000;
const READ_WINDOW_MS = 86_400_000;
const CHANGE_DEBOUNCE_MS = 2_000;
const SNAPSHOT_LIMIT = 200;
interface StoredSnapshot {
  urls: string[];
  builtAt: number;
  merged: ContextPullRequest[];
  snapshot: ContextPullRequestsSnapshot;
}
const scopeKey = (scope: ContextPullRequestScope) => `${scope.kind}:${scope.id}`;
const sameUrls = (a: readonly string[], b: readonly string[]) => {
  if (a.length !== b.length) return false;
  const keys = new Set(a.map((url) => url.toLowerCase()));
  return b.every((url) => keys.has(url.toLowerCase()));
};

export class ContextPrService {
  #generation = 0;
  readonly #permits = Semaphore.makeUnsafe(3);
  readonly #cache = new Map<string, { expiresAt: number; row: ContextPullRequest | null }>();
  readonly #snapshots = new Map<string, StoredSnapshot>();
  readonly #reads = new Map<string, { scope: ContextPullRequestScope; at: number }>();
  readonly #queued = new Map<string, ContextPullRequestScope>();
  readonly #changed = new Map<string, ContextPullRequestScope>();
  #changeTimer: ReturnType<typeof setTimeout> | null = null;
  #sweepTimer: ReturnType<typeof setInterval> | null = null;
  #draining: Promise<void> | null = null;
  constructor(
    readonly references: Pick<ContextPrReferences, "index">,
    readonly github: Pick<GitHubConnectorService, "status" | "readContextPullRequest">,
    readonly cli = readCliContextPr,
    readonly requestSignal = () => AbortSignal.timeout(12_000),
    readonly now = () => Date.now(),
  ) {}
  invalidate(): void {
    this.#generation++;
    this.#cache.clear();
    this.#snapshots.clear();
  }
  start(): void {
    if (this.#sweepTimer) return;
    this.#sweepTimer = setInterval(() => this.refreshRecent(), REFRESH_MS);
    this.#sweepTimer.unref?.();
  }
  stop(): void {
    if (this.#sweepTimer) clearInterval(this.#sweepTimer);
    if (this.#changeTimer) clearTimeout(this.#changeTimer);
    this.#sweepTimer = null;
    this.#changeTimer = null;
    this.#queued.clear();
    this.#changed.clear();
  }
  refreshRecent(): void {
    const now = this.now();
    for (const [key, read] of this.#reads) {
      if (now - read.at > READ_WINDOW_MS) {
        this.#reads.delete(key);
        for (const stored of this.#snapshots.keys()) if (stored.endsWith(`|${key}`)) this.#snapshots.delete(stored);
        continue;
      }
      const stored = this.#snapshots.get(this.#snapshotKey(read.scope));
      if (!stored || now - stored.builtAt >= REFRESH_MS - CHANGE_DEBOUNCE_MS) this.#queue(read.scope);
    }
  }
  noteChanged(scope: ContextPullRequestScope): void {
    const key = scopeKey(scope);
    const read = this.#reads.get(key);
    if (!read || this.now() - read.at > READ_WINDOW_MS) return;
    this.#changed.set(key, scope);
    if (this.#changeTimer) return;
    this.#changeTimer = setTimeout(() => {
      this.#changeTimer = null;
      this.flushChanges();
    }, CHANGE_DEBOUNCE_MS);
    this.#changeTimer.unref?.();
  }
  flushChanges(): void {
    const changed = [...this.#changed.values()];
    this.#changed.clear();
    for (const scope of changed) {
      const stored = this.#snapshots.get(this.#snapshotKey(scope));
      try {
        const reference = this.references.index(scope);
        if (!stored || !sameUrls(stored.urls, reference.urls)) this.#queue(scope);
      } catch {
        this.#reads.delete(scopeKey(scope));
      }
    }
  }
  whenIdle(): Promise<void> {
    return this.#draining ?? Promise.resolve();
  }
  #source() {
    const status = this.github.status();
    const connected = status.state === "connected";
    const mayUseCli = !status.available || status.state === "disconnected";
    return {
      source: connected ? `connector:${status.login}` : mayUseCli ? "cli" : "unavailable",
      connected,
      mayUseCli,
      status,
    };
  }
  #snapshotKey(scope: ContextPullRequestScope): string {
    return `${this.#source().source}|${scopeKey(scope)}`;
  }
  #queue(scope: ContextPullRequestScope): void {
    this.#queued.set(scopeKey(scope), scope);
    if (this.#draining) return;
    this.#draining = (async () => {
      try {
        for (let next = this.#queued.entries().next(); !next.done; next = this.#queued.entries().next()) {
          const [key, scope] = next.value;
          this.#queued.delete(key);
          await Effect.runPromise(this.#build({ scope }, true).pipe(Effect.result));
        }
      } finally {
        this.#draining = null;
      }
    })();
  }
  #store(key: string, value: StoredSnapshot): void {
    this.#snapshots.delete(key);
    if (this.#snapshots.size >= SNAPSHOT_LIMIT) this.#snapshots.delete(this.#snapshots.keys().next().value ?? "");
    this.#snapshots.set(key, value);
  }
  readonly read = Effect.fn("ContextPrService.read")(function* (
    this: ContextPrService,
    input: ContextPullRequestsInput,
  ): Effect.fn.Return<ContextPullRequestsSnapshot, StoredStateFailure> {
    const reference = yield* storedSync(() => this.references.index(input.scope));
    const key = scopeKey(input.scope);
    this.#reads.delete(key);
    this.#reads.set(key, { scope: input.scope, at: this.now() });
    if (this.#reads.size > SNAPSHOT_LIMIT) this.#reads.delete(this.#reads.keys().next().value ?? "");
    const stored = input.refresh ? undefined : this.#snapshots.get(this.#snapshotKey(input.scope));
    if (stored) {
      const stale =
        !sameUrls(stored.urls, reference.urls) || this.now() - stored.builtAt >= REFRESH_MS || this.#queued.has(key);
      if (stale) this.#queue(input.scope);
      return { ...stored.snapshot, stale, refreshedAt: new Date(stored.builtAt).toISOString() };
    }
    return yield* this.#build(input, false);
  }).bind(this);
  readonly #build = Effect.fn("ContextPrService.build")(function* (
    this: ContextPrService,
    input: ContextPullRequestsInput,
    background: boolean,
  ): Effect.fn.Return<ContextPullRequestsSnapshot, StoredStateFailure> {
    const reference = yield* storedSync(() => this.references.index(input.scope));
    const { source, connected, mayUseCli, status } = this.#source();
    const storeKey = `${source}|${scopeKey(input.scope)}`;
    const previous = background ? this.#snapshots.get(storeKey) : undefined;
    const merged = new Map((previous?.merged ?? []).map((row) => [row.url.toLowerCase(), row]));
    const generation = this.#generation;
    const now = Date.now(),
      cacheNow = this.now(),
      signal = this.requestSignal();
    const results = yield* Effect.forEach(
      reference.urls,
      (url) =>
        this.#permits
          .withPermit(
            Effect.gen({ self: this }, function* () {
              const identity = contextPullRequestIdentity(url);
              if (!identity) return { url, result: Result.fail(new ContextPrFailure({ reason: "invalid-response" })) };
              const known = merged.get(identity.url.toLowerCase());
              if (known) return { url, result: Result.succeed(known) };
              const key = `${source}:${identity.url.toLowerCase()}`;
              const cached = this.#cache.get(key);
              if (!input.refresh && cached && cached.expiresAt > cacheNow)
                return { url, result: Result.succeed(cached.row) };
              if (signal.aborted) return { url, result: Result.fail(new ContextPrFailure({ reason: "timeout" })) };
              const result = yield* (
                connected
                  ? this.github.readContextPullRequest(identity, signal)
                  : mayUseCli
                    ? this.cli(identity, undefined, signal)
                    : Effect.fail(new ContextPrFailure({ reason: "not-connected" }))
              ).pipe(Effect.result);
              if (Result.isFailure(result)) this.#cache.delete(key);
              if (Result.isSuccess(result) && generation === this.#generation) {
                if (this.#cache.size >= 400) this.#cache.delete(this.#cache.keys().next().value ?? "");
                this.#cache.set(key, { expiresAt: this.now() + 120_000, row: result.success });
              }
              return { url, result };
            }),
          )
          .pipe(
            Effect.timeoutOrElse({
              duration: Math.max(0, 12_000 - (Date.now() - now)),
              orElse: () =>
                Effect.sync(() => {
                  this.#cache.delete(`${source}:${url.toLowerCase()}`);
                  return { url, result: Result.fail(new ContextPrFailure({ reason: "timeout" })) };
                }),
            }),
          ),
      { concurrency: 3 },
    );
    const current = this.github.status();
    if (generation !== this.#generation || current.state !== status.state || current.login !== status.login) {
      const checkedAt = new Date().toISOString();
      return {
        scope: input.scope,
        checkedAt,
        status: "not-connected",
        open: [],
        merged: [],
        failures: reference.urls.map((url) => ({ url, reason: "not-connected" })),
        referencesLimited: reference.limited,
        stale: false,
        refreshedAt: checkedAt,
      };
    }
    const rows: ContextPullRequest[] = [],
      failures: ContextPullRequestsSnapshot["failures"] = [];
    for (const entry of results) {
      if (Result.isFailure(entry.result)) failures.push({ url: entry.url, reason: entry.result.failure.reason });
      else if (entry.result.success) rows.push(entry.result.success);
    }
    const checkedAt = new Date().toISOString();
    const mergedRows = rows
      .filter((row) => row.state === "merged")
      .sort((a, b) => (b.mergedAt ?? "").localeCompare(a.mergedAt ?? "") || a.url.localeCompare(b.url));
    const snapshot: ContextPullRequestsSnapshot = {
      scope: input.scope,
      checkedAt,
      status:
        failures.length > 0 && failures.every((failure) => failure.reason === "not-connected") && !rows.length
          ? "not-connected"
          : "ready",
      open: rows
        .filter((row) => row.state === "open")
        .sort((a, b) => a.repo.localeCompare(b.repo) || a.number - b.number),
      merged: mergedRows.slice(0, 3),
      failures,
      referencesLimited: reference.limited,
      stale: false,
      refreshedAt: checkedAt,
    };
    if (snapshot.status === "ready")
      this.#store(storeKey, { urls: reference.urls, builtAt: this.now(), merged: mergedRows, snapshot });
    return snapshot;
  }).bind(this);
}
