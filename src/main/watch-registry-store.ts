import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import type {
  WatchingSnapshot,
  WatchingTask,
  WatchingTaskHealth,
  WatchingTasks,
  WatchRegistryEntry,
  WatchRegistryPublication,
} from "@openbot/contracts/watch-registry";
import {
  isTasksPublication,
  isWatchRegistryPublication,
  WATCH_REGISTRY_PAGE_PREFIX,
  WATCH_REGISTRY_TASKS_PUBLISHER_ID,
  WATCHING_MAX_WATCHES,
  watchRegistryPage,
  watchRegistryPageIndex,
} from "@openbot/contracts/watch-registry";
import { redactText } from "@openbot/logging";
import { Effect, Result, Semaphore } from "effect";
import { isMissingFileError } from "../backend/file-errors";
import { readPreferenceFile, writePreferenceFile } from "./preference-file";

export const WATCH_REGISTRY_PREFERENCE_FILE = "openbot-watch-registry-v1.json";
export const WATCH_REGISTRY_V2_PREFERENCE_FILE = "openbot-watch-registry-v2.json";
interface Publisher {
  publisherId: string;
  ownerAgentId: string;
  snapshot: WatchRegistryPublication;
}
interface RegistryFile {
  version: 1 | 2;
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
    (value.version !== 1 && value.version !== 2) ||
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
    value.publishers.reduce((sum, publisher) => sum + publisher.snapshot.watches.length, 0) > WATCHING_MAX_WATCHES ||
    value.publishers.some(
      (publisher) =>
        isTasksPublication(publisher.snapshot) !== (publisher.publisherId === WATCH_REGISTRY_TASKS_PUBLISHER_ID) ||
        (watchRegistryPage(publisher.snapshot)?.index ?? null) !== watchRegistryPageIndex(publisher.publisherId),
    ) ||
    (value.version === 1 && value.publishers.some((publisher) => publisher.snapshot.version !== 1))
  )
    return null;
  return {
    version: value.version,
    managementPublisherRouteId: value.managementPublisherRouteId,
    revision: value.revision,
    publishers: value.publishers,
  };
}
function cleanWatch(watch: WatchRegistryEntry, known: ReadonlySet<string>): WatchRegistryEntry {
  const receivers = watch.receiverAgentIds?.filter((id) => known.has(id));
  const { receiverAgentIds: _receivers, ...rest } = watch;
  return {
    ...rest,
    title: redactText(watch.title),
    ...(watch.lastError === undefined ? {} : { lastError: redactText(watch.lastError) }),
    ...(watch.condition === undefined ? {} : { condition: redactText(watch.condition) }),
    ...(watch.summary === undefined ? {} : { summary: redactText(watch.summary) }),
    ...(watch.purpose === undefined ? {} : { purpose: redactText(watch.purpose) }),
    ...(receivers?.length ? { receiverAgentIds: receivers } : {}),
  };
}
function cleanTask(task: WatchingTask, known: ReadonlySet<string>): WatchingTask {
  const { ownerAgentId, ...rest } = task;
  return {
    ...rest,
    title: redactText(task.title),
    ...(task.detail === undefined ? {} : { detail: redactText(task.detail) }),
    ...(task.waitingOn === undefined ? {} : { waitingOn: redactText(task.waitingOn) }),
    ...(task.urgencyReason === undefined ? {} : { urgencyReason: redactText(task.urgencyReason) }),
    ...(ownerAgentId !== undefined && known.has(ownerAgentId) ? { ownerAgentId } : {}),
  };
}
function cleanHealth(health: WatchingTaskHealth): WatchingTaskHealth {
  return {
    ...health,
    reasons: health.reasons.map((reason) => redactText(reason)),
    ...(health.longestWaiting === undefined
      ? {}
      : { longestWaiting: { ...health.longestWaiting, waitingOn: redactText(health.longestWaiting.waitingOn) } }),
  };
}
function cleanPublication(snapshot: WatchRegistryPublication, known: ReadonlySet<string>): WatchRegistryPublication {
  const watches = snapshot.watches.map((watch) => cleanWatch(watch, known));
  if (snapshot.version === 1) return { ...snapshot, watches };
  return {
    ...snapshot,
    watches,
    ...(snapshot.tasks === undefined ? {} : { tasks: snapshot.tasks.map((task) => cleanTask(task, known)) }),
    ...(snapshot.health === undefined ? {} : { health: cleanHealth(snapshot.health) }),
  };
}
function unsafeIdentifiers(snapshot: WatchRegistryPublication): boolean {
  const values: (string | undefined)[] = snapshot.watches.flatMap((watch) => [
    watch.id,
    watch.ownerAgentId,
    watch.sourceKind,
    watch.targetUrl,
    watch.channelId,
    ...(watch.receiverAgentIds ?? []),
  ]);
  if (snapshot.version === 2)
    for (const task of snapshot.tasks ?? []) values.push(task.id, task.ownerAgentId, task.link);
  return values.some((value) => value !== undefined && redactText(value) !== value);
}
export class WatchRegistryStore {
  readonly #path: string;
  readonly #legacyPath: string | undefined;
  readonly #knownAgents: () => ReadonlySet<string>;
  readonly #changed: () => void;
  readonly #writes = Semaphore.makeUnsafe(1);
  #file: RegistryFile = { version: 1, managementPublisherRouteId: null, revision: 0, publishers: [] };
  #readOnly = false;
  constructor(
    path: string,
    options: { knownAgents(): ReadonlySet<string>; changed(): void; legacyPath?: string | undefined },
  ) {
    this.#path = path;
    this.#legacyPath = options.legacyPath;
    this.#knownAgents = options.knownAgents;
    this.#changed = options.changed;
  }
  readonly load = Effect.fn("WatchRegistry.load")(function* (this: WatchRegistryStore) {
    const result = yield* Effect.result(readPreferenceFile(this.#path, decodeFile));
    if (Result.isSuccess(result) && result.success) {
      this.#file = result.success;
      return;
    }
    if (Result.isSuccess(result) || !isMissingFileError(result.failure.cause)) {
      this.#readOnly = true;
      return;
    }
    if (this.#legacyPath === undefined) return;
    const legacy = yield* Effect.result(readPreferenceFile(this.#legacyPath, decodeFile));
    if (Result.isSuccess(legacy) && legacy.success) this.#file = legacy.success;
    else if (Result.isFailure(legacy) && isMissingFileError(legacy.failure.cause)) return;
    else this.#readOnly = true;
  }).bind(this);
  #watchPublishers(): Publisher[] {
    return this.#file.publishers.filter((publisher) => publisher.publisherId !== WATCH_REGISTRY_TASKS_PUBLISHER_ID);
  }
  snapshot(): WatchingSnapshot["registry"] {
    const publishers = this.#watchPublishers();
    if (!publishers.length || this.#file.revision === 0) return null;
    return {
      version: 1,
      sequence: this.#file.revision,
      generatedAt:
        publishers
          .map((publisher) => publisher.snapshot.generatedAt)
          .sort()
          .at(-1) ?? new Date(0).toISOString(),
      watches: publishers.flatMap((publisher) =>
        publisher.snapshot.watches.map((watch) => ({ ...watch, publisherId: publisher.publisherId })),
      ),
    };
  }
  snapshotV2(): WatchingSnapshot["registry"] {
    const registry = this.snapshot();
    if (!registry) return null;
    const publishers = this.#watchPublishers();
    return {
      ...registry,
      version: 2,
      sequence: Math.max(...publishers.map((publisher) => publisher.snapshot.sequence)),
      generatedAt:
        publishers
          .map((publisher) => publisher.snapshot.generatedAt)
          .sort()
          .at(0) ?? registry.generatedAt,
    };
  }
  tasks(): WatchingTasks | null {
    const publication = this.#file.publishers.find(
      (publisher) => publisher.publisherId === WATCH_REGISTRY_TASKS_PUBLISHER_ID,
    )?.snapshot;
    if (publication?.version !== 2 || publication.tasks === undefined) return null;
    return {
      sequence: publication.sequence,
      generatedAt: publication.generatedAt,
      ...(publication.health === undefined ? {} : { health: publication.health }),
      items: publication.tasks,
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
        const tasks = isTasksPublication(snapshot);
        const page = watchRegistryPage(snapshot);
        const management = publisher.publisherId === this.#file.managementPublisherRouteId;
        if ((tasks || page) && !management) return 400 as const;
        const slot = tasks
          ? WATCH_REGISTRY_TASKS_PUBLISHER_ID
          : page
            ? `${WATCH_REGISTRY_PAGE_PREFIX}${page.index}`
            : publisher.publisherId;
        if (
          snapshot.watches.some(
            (watch) =>
              !known.has(watch.ownerAgentId) ||
              (publisher.publisherId !== this.#file.managementPublisherRouteId &&
                watch.ownerAgentId !== publisher.ownerAgentId),
          )
        )
          return 400 as const;
        const previous = this.#file.publishers.find((item) => item.publisherId === slot);
        if (
          previous &&
          (previous.ownerAgentId !== publisher.ownerAgentId || snapshot.sequence < previous.snapshot.sequence)
        )
          return 400 as const;
        if (unsafeIdentifiers(snapshot)) return 400 as const;
        const clean = cleanPublication(snapshot, known);
        if (!isWatchRegistryPublication(clean)) return 400 as const;
        if (previous && snapshot.sequence === previous.snapshot.sequence)
          return JSON.stringify(clean) === JSON.stringify(previous.snapshot) ? (200 as const) : (400 as const);
        const replaced = (item: Publisher) => {
          if (item.publisherId === slot) return true;
          const index = watchRegistryPageIndex(item.publisherId);
          if (page) return item.publisherId === publisher.publisherId || (index !== null && index > page.count);
          return !tasks && management && index !== null;
        };
        const publishers = [
          ...this.#file.publishers.filter((item) => !replaced(item)),
          { publisherId: slot, ownerAgentId: publisher.ownerAgentId, snapshot: clean },
        ];
        if (
          publishers.length > 200 ||
          publishers.reduce((sum, item) => sum + item.snapshot.watches.length, 0) > WATCHING_MAX_WATCHES
        )
          return 413 as const;
        const file: RegistryFile = {
          ...this.#file,
          version: publishers.some((item) => item.snapshot.version === 2) ? 2 : this.#file.version,
          revision: this.#file.revision + 1,
          publishers,
        };
        yield* writePreferenceFile(this.#path, file, { createDirectory: true });
        this.#file = file;
        this.#changed();
        return 202 as const;
      }).pipe(Effect.uninterruptible),
    );
  }).bind(this);
}
