import { type DynamicRecord, isDynamicRecord } from "./runtime-values";

export const WATCH_REGISTRY_EVENT_TYPE = "bridge.watch.registry";
export const WATCH_REGISTRY_ROUTINE_EVENT_TYPE = "bridge.watch.registry.sink";
export const WATCH_REGISTRY_MAX_WATCHES = 200;
export const WATCHING_MAX_WATCHES = 2000;
export const WATCH_STATUSES = ["active", "paused", "completed", "expired", "error"] as const;
export type WatchStatus = (typeof WATCH_STATUSES)[number];
export interface WatchRegistryEntry {
  id: string;
  ownerAgentId: string;
  sourceKind: string;
  title: string;
  targetUrl?: string;
  status: WatchStatus;
  lastCheckedAt?: string;
  expiresAt?: string;
  channelId?: string;
  lastError?: string;
  condition?: string;
}
export interface WatchRegistryPublication {
  version: 1;
  sequence: number;
  generatedAt: string;
  watches: WatchRegistryEntry[];
}
export interface WatchingWatch extends WatchRegistryEntry {
  publisherId: string;
}
export interface WatchingWait {
  id: string;
  ownerAgentId: string;
  note: string;
  routineId: string | null;
  createdAt: string;
  wokenAt: string | null;
  channelId?: string;
}
export interface WatchingSnapshot {
  registry: { version: 1; sequence: number; generatedAt: string; watches: WatchingWatch[] } | null;
  waits: WatchingWait[];
}
function keys(value: DynamicRecord, allowed: readonly string[]): boolean {
  return Object.keys(value).every((key) => allowed.includes(key));
}
function text(value: unknown, maximum: number): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= maximum &&
    [...value].every((character) => {
      const point = character.codePointAt(0) ?? 0;
      return point >= 32 && point !== 127;
    })
  );
}
function timestamp(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(value) &&
    !Number.isNaN(Date.parse(value)) &&
    new Date(value).toISOString() === value
  );
}
function targetUrl(value: unknown): value is string {
  if (!text(value, 2048)) return false;
  try {
    const url = new URL(value);
    return url.protocol === "https:" && !url.username && !url.password && !url.search && !url.hash;
  } catch {
    return false;
  }
}
export function isWatchRegistryEntry(value: unknown): value is WatchRegistryEntry {
  return (
    isDynamicRecord(value) &&
    keys(value, [
      "id",
      "ownerAgentId",
      "sourceKind",
      "title",
      "targetUrl",
      "status",
      "lastCheckedAt",
      "expiresAt",
      "channelId",
      "lastError",
      "condition",
    ]) &&
    text(value.id, 160) &&
    text(value.ownerAgentId, 160) &&
    text(value.sourceKind, 64) &&
    text(value.title, 256) &&
    WATCH_STATUSES.some((status) => status === value.status) &&
    (value.targetUrl === undefined || targetUrl(value.targetUrl)) &&
    (value.lastCheckedAt === undefined || timestamp(value.lastCheckedAt)) &&
    (value.expiresAt === undefined || timestamp(value.expiresAt)) &&
    (value.channelId === undefined || text(value.channelId, 160)) &&
    (value.lastError === undefined || text(value.lastError, 512)) &&
    (value.condition === undefined || text(value.condition, 256))
  );
}
export function isWatchRegistryPublication(value: unknown): value is WatchRegistryPublication {
  return (
    isDynamicRecord(value) &&
    keys(value, ["version", "sequence", "generatedAt", "watches"]) &&
    value.version === 1 &&
    typeof value.sequence === "number" &&
    Number.isSafeInteger(value.sequence) &&
    value.sequence >= 1 &&
    timestamp(value.generatedAt) &&
    Array.isArray(value.watches) &&
    value.watches.length <= WATCH_REGISTRY_MAX_WATCHES &&
    value.watches.every(isWatchRegistryEntry) &&
    new Set(value.watches.map((watch) => watch.id)).size === value.watches.length
  );
}
export function isWatchingSnapshot(value: unknown): value is WatchingSnapshot {
  if (
    !isDynamicRecord(value) ||
    !keys(value, ["registry", "waits"]) ||
    !Array.isArray(value.waits) ||
    value.waits.length > 2000
  )
    return false;
  const registry = value.registry;
  if (
    registry !== null &&
    (!isDynamicRecord(registry) ||
      !keys(registry, ["version", "sequence", "generatedAt", "watches"]) ||
      registry.version !== 1 ||
      typeof registry.sequence !== "number" ||
      !Number.isSafeInteger(registry.sequence) ||
      registry.sequence < 1 ||
      !timestamp(registry.generatedAt) ||
      !Array.isArray(registry.watches) ||
      registry.watches.length > WATCHING_MAX_WATCHES ||
      !registry.watches.every((watch) => {
        if (!isDynamicRecord(watch)) return false;
        const { publisherId, ...entry } = watch;
        return text(publisherId, 160) && isWatchRegistryEntry(entry);
      }))
  )
    return false;
  return value.waits.every(
    (wait) =>
      isDynamicRecord(wait) &&
      keys(wait, ["id", "ownerAgentId", "note", "routineId", "createdAt", "wokenAt", "channelId"]) &&
      text(wait.id, 160) &&
      text(wait.ownerAgentId, 160) &&
      typeof wait.note === "string" &&
      wait.note.length > 0 &&
      wait.note.length <= 500 &&
      !wait.note.includes("\0") &&
      (wait.routineId === null || text(wait.routineId, 160)) &&
      timestamp(wait.createdAt) &&
      (wait.wokenAt === null || timestamp(wait.wokenAt)) &&
      (wait.channelId === undefined || text(wait.channelId, 160)),
  );
}
export function decodeWatchingSnapshot(value: unknown): WatchingSnapshot {
  if (!isWatchingSnapshot(value)) throw new Error("Invalid watching snapshot.");
  return value;
}
