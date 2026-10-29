import {
  type ContextPullRequest,
  type ContextPullRequestsInput,
  type ContextPullRequestsSnapshot,
  contextPullRequestIdentity,
} from "@openbot/contracts/context-prs";
import { Effect, Result, Semaphore } from "effect";
import type { ContextPrReferences } from "../backend/context-pr-references";
import { storedSync } from "../backend/stored-state-effects";
import { ContextPrFailure, readCliContextPr } from "./context-pr-reader";
import type { GitHubConnectorService } from "./github-connector-service";

export class ContextPrService {
  #generation = 0;
  readonly #permits = Semaphore.makeUnsafe(3);
  readonly #cache = new Map<string, { expiresAt: number; row: ContextPullRequest | null }>();
  constructor(
    readonly references: Pick<ContextPrReferences, "read">,
    readonly github: Pick<GitHubConnectorService, "status" | "readContextPullRequest">,
    readonly cli = readCliContextPr,
    readonly requestSignal = () => AbortSignal.timeout(12_000),
  ) {}
  invalidate(): void {
    this.#generation++;
    this.#cache.clear();
  }
  readonly read = Effect.fn("ContextPrService.read")(function* (
    this: ContextPrService,
    input: ContextPullRequestsInput,
  ): Effect.fn.Return<ContextPullRequestsSnapshot, import("../backend/stored-state-effects").StoredStateFailure> {
    const reference = yield* storedSync(() => this.references.read(input.scope));
    const status = this.github.status();
    const connected = status.state === "connected";
    const mayUseCli = !status.available || status.state === "disconnected";
    const source = connected ? `connector:${status.login}` : mayUseCli ? "cli" : "unavailable";
    const generation = this.#generation;
    const now = Date.now(),
      signal = this.requestSignal();
    const results = yield* Effect.forEach(
      reference.urls,
      (url) =>
        this.#permits
          .withPermit(
            Effect.gen({ self: this }, function* () {
              const identity = contextPullRequestIdentity(url);
              if (!identity) return { url, result: Result.fail(new ContextPrFailure({ reason: "invalid-response" })) };
              const key = `${source}:${identity.url.toLowerCase()}`;
              const cached = this.#cache.get(key);
              if (!input.refresh && cached && cached.expiresAt > now)
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
                this.#cache.set(key, { expiresAt: Date.now() + 120_000, row: result.success });
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
    if (generation !== this.#generation || current.state !== status.state || current.login !== status.login)
      return {
        scope: input.scope,
        checkedAt: new Date().toISOString(),
        status: "not-connected",
        open: [],
        merged: [],
        failures: reference.urls.map((url) => ({ url, reason: "not-connected" })),
        referencesLimited: reference.limited,
      };
    const rows: ContextPullRequest[] = [],
      failures: ContextPullRequestsSnapshot["failures"] = [];
    for (const entry of results) {
      if (Result.isFailure(entry.result)) failures.push({ url: entry.url, reason: entry.result.failure.reason });
      else if (entry.result.success) rows.push(entry.result.success);
    }
    return {
      scope: input.scope,
      checkedAt: new Date().toISOString(),
      status:
        failures.length > 0 && failures.every((failure) => failure.reason === "not-connected") && !rows.length
          ? "not-connected"
          : "ready",
      open: rows
        .filter((row) => row.state === "open")
        .sort((a, b) => a.repo.localeCompare(b.repo) || a.number - b.number),
      merged: rows
        .filter((row) => row.state === "merged")
        .sort((a, b) => (b.mergedAt ?? "").localeCompare(a.mergedAt ?? "") || a.url.localeCompare(b.url))
        .slice(0, 3),
      failures,
      referencesLimited: reference.limited,
    };
  }).bind(this);
}
