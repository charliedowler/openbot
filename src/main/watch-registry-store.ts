import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import type { WatchingSnapshot, WatchRegistryPublication } from "@openbot/contracts/watch-registry";
import { isWatchRegistryPublication, WATCHING_MAX_WATCHES } from "@openbot/contracts/watch-registry";
import { redactText } from "@openbot/logging";
import { Effect, Result, Semaphore } from "effect";
import { isMissingFileError } from "../backend/file-errors";
import { readPreferenceFile, writePreferenceFile } from "./preference-file";

export const WATCH_REGISTRY_PREFERENCE_FILE = "openbot-watch-registry-v1.json";
interface Publisher {
  publisherId: string;
  ownerAgentId: string;
  snapshot: WatchRegistryPublication;
}
interface RegistryFile {
  version: 1;
  managementPublisherRouteId: string | null;
  revision: number;
  publishers: Publisher[];
}
function identifier(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 160;
}
function decodeFile(value: unknown): RegistryFile | null {
  if (
    !isDynamicRecord(value) ||
    Object.keys(value).some(
      (key) => !["version", "managementPublisherRouteId", "revision", "publishers"].includes(key),
    ) ||
    value.version !== 1 ||
    (value.managementPublisherRouteId !== null && !identifier(value.managementPublisherRouteId)) ||
    typeof value.revision !== "number" ||
    !Number.isSafeInteger(value.revision) ||
    value.revision < 0 ||
    !Array.isArray(value.publishers) ||
    value.publishers.length > 200 ||
    !value.publishers.every(
      (publisher): publisher is Publisher =>
        isDynamicRecord(publisher) &&
        Object.keys(publisher).every((key) => ["publisherId", "ownerAgentId", "snapshot"].includes(key)) &&
        identifier(publisher.publisherId) &&
        identifier(publisher.ownerAgentId) &&
        isWatchRegistryPublication(publisher.snapshot),
    )
  )
    return null;
  if (
    new Set(value.publishers.map((publisher) => publisher.publisherId)).size !== value.publishers.length ||
    value.publishers.reduce((sum, publisher) => sum + publisher.snapshot.watches.length, 0) > WATCHING_MAX_WATCHES
  )
    return null;
  return {
    version: 1,
    managementPublisherRouteId: value.managementPublisherRouteId,
    revision: value.revision,
    publishers: value.publishers,
  };
}
function cleanPublication(snapshot: WatchRegistryPublication): WatchRegistryPublication {
  return {
    ...snapshot,
    watches: snapshot.watches.map((watch) => ({
      ...watch,
      title: redactText(watch.title),
      ...(watch.lastError === undefined ? {} : { lastError: redactText(watch.lastError) }),
      ...(watch.condition === undefined ? {} : { condition: redactText(watch.condition) }),
    })),
  };
}
export class WatchRegistryStore {
  readonly #path: string;
  readonly #knownAgents: () => ReadonlySet<string>;
  readonly #changed: () => void;
  readonly #writes = Semaphore.makeUnsafe(1);
  #file: RegistryFile = { version: 1, managementPublisherRouteId: null, revision: 0, publishers: [] };
  #readOnly = false;
  constructor(path: string, options: { knownAgents(): ReadonlySet<string>; changed(): void }) {
    this.#path = path;
    this.#knownAgents = options.knownAgents;
    this.#changed = options.changed;
  }
  readonly load = Effect.fn("WatchRegistry.load")(function* (this: WatchRegistryStore) {
    const result = yield* Effect.result(readPreferenceFile(this.#path, decodeFile));
    if (Result.isSuccess(result) && result.success) this.#file = result.success;
    else if (Result.isFailure(result) && isMissingFileError(result.failure.cause)) return;
    else this.#readOnly = true;
  }).bind(this);
  snapshot(): WatchingSnapshot["registry"] {
    if (!this.#file.publishers.length || this.#file.revision === 0) return null;
    return {
      version: 1,
      sequence: this.#file.revision,
      generatedAt:
        this.#file.publishers
          .map((publisher) => publisher.snapshot.generatedAt)
          .sort()
          .at(-1) ?? new Date(0).toISOString(),
      watches: this.#file.publishers.flatMap((publisher) =>
        publisher.snapshot.watches.map((watch) => ({ ...watch, publisherId: publisher.publisherId })),
      ),
    };
  }
  readonly accept = Effect.fn("WatchRegistry.accept")(function* (
    this: WatchRegistryStore,
    publisher: { publisherId: string; ownerAgentId: string },
    snapshot: WatchRegistryPublication,
  ) {
    return yield* this.#writes.withPermit(
      Effect.gen({ self: this }, function* () {
        if (this.#readOnly) return 503 as const;
        const known = this.#knownAgents();
        if (
          snapshot.watches.some(
            (watch) =>
              !known.has(watch.ownerAgentId) ||
              (publisher.publisherId !== this.#file.managementPublisherRouteId &&
                watch.ownerAgentId !== publisher.ownerAgentId),
          )
        )
          return 400 as const;
        const previous = this.#file.publishers.find((item) => item.publisherId === publisher.publisherId);
        if (
          previous &&
          (previous.ownerAgentId !== publisher.ownerAgentId || snapshot.sequence < previous.snapshot.sequence)
        )
          return 400 as const;
        const clean = cleanPublication(snapshot);
        if (previous && snapshot.sequence === previous.snapshot.sequence)
          return JSON.stringify(clean) === JSON.stringify(previous.snapshot) ? (200 as const) : (400 as const);
        const publishers = [
          ...this.#file.publishers.filter((item) => item.publisherId !== publisher.publisherId),
          { ...publisher, snapshot: clean },
        ];
        if (
          publishers.length > 200 ||
          publishers.reduce((sum, item) => sum + item.snapshot.watches.length, 0) > WATCHING_MAX_WATCHES
        )
          return 413 as const;
        const file: RegistryFile = { ...this.#file, revision: this.#file.revision + 1, publishers };
        yield* writePreferenceFile(this.#path, file, { createDirectory: true });
        this.#file = file;
        this.#changed();
        return 202 as const;
      }).pipe(Effect.uninterruptible),
    );
  }).bind(this);
}
