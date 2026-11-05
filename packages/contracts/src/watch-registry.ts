import { type DynamicRecord, isDynamicRecord } from "./runtime-values";

export const WATCH_REGISTRY_EVENT_TYPE = "bridge.watch.registry";
export const WATCH_REGISTRY_ROUTINE_EVENT_TYPE = "bridge.watch.registry.sink";
export const WATCH_REGISTRY_MAX_WATCHES = 200;
export const WATCHING_MAX_WATCHES = 2000;
export const WATCH_REGISTRY_TASKS_PUBLISHER_ID = "openbot-control-tasks";
export const WATCH_REGISTRY_MAX_PAGES = 4;
export const WATCH_REGISTRY_PAGE_PREFIX = "openbot-control-watches-";
export function watchRegistryPageIndex(publisherId: unknown): number | null {
  if (typeof publisherId !== "string" || !publisherId.startsWith(WATCH_REGISTRY_PAGE_PREFIX)) return null;
  const index = Number(publisherId.slice(WATCH_REGISTRY_PAGE_PREFIX.length));
  return Number.isSafeInteger(index) &&
    index >= 1 &&
    index <= WATCH_REGISTRY_MAX_PAGES &&
    `${index}` === publisherId.slice(WATCH_REGISTRY_PAGE_PREFIX.length)
    ? index
    : null;
}
export const WATCH_REGISTRY_MAX_TASKS = 300;
export const WATCH_RECEIVERS_MAX = 8;
export const WATCH_STATUSES = ["active", "paused", "completed", "expired", "error"] as const;
export type WatchStatus = (typeof WATCH_STATUSES)[number];
export const WATCHING_TASK_KINDS = ["decision", "follow-up", "dispatch"] as const;
export type WatchingTaskKind = (typeof WATCHING_TASK_KINDS)[number];
export const WATCHING_TASK_STATES = ["waiting-on-charlie", "in-progress", "waiting-on-event", "done"] as const;
export type WatchingTaskState = (typeof WATCHING_TASK_STATES)[number];
export const WATCHING_TASK_URGENCIES = ["low", "normal", "high"] as const;
export type WatchingTaskUrgency = (typeof WATCHING_TASK_URGENCIES)[number];
export interface WatchingTask {
  id: string;
  kind: WatchingTaskKind;
  state: WatchingTaskState;
  title: string;
  detail?: string;
  ownerAgentId?: string;
  link?: string;
  updatedAt: string;
  dueAt?: string;
  waitingOn?: string;
  waitingSince?: string;
  urgency?: WatchingTaskUrgency;
  urgencyReason?: string;
}
export interface WatchingTaskHealth {
  wip: number;
  wipLimit: number;
  oldest?: { taskId: string; since: string };
  longestWaiting?: { taskId: string; waitingOn: string; since: string };
  urgent: number;
  score: number;
  reasons: string[];
}
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
  summary?: string;
  purpose?: string;
  receiverAgentIds?: string[];
}
export interface WatchRegistryPublicationV1 {
  version: 1;
  sequence: number;
  generatedAt: string;
  watches: WatchRegistryEntry[];
}
export interface WatchRegistryPublicationV2 {
  version: 2;
  sequence: number;
  generatedAt: string;
  watches: WatchRegistryEntry[];
  publisherId?: string;
  page?: { index: number; count: number };
  tasks?: WatchingTask[];
  health?: WatchingTaskHealth;
}
export type WatchRegistryPublication = WatchRegistryPublicationV1 | WatchRegistryPublicationV2;
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
export interface WatchingTasks {
  sequence: number;
  generatedAt: string;
  health?: WatchingTaskHealth;
  items: WatchingTask[];
}
export interface WatchingSnapshot {
  registry: { version: 1 | 2; sequence: number; generatedAt: string; watches: WatchingWatch[] } | null;
  tasks?: WatchingTasks | null;
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
function isoText(value: unknown): value is string {
  return text(value, 160) && Number.isFinite(Date.parse(value));
}
function count(value: unknown, maximum: number): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= maximum;
}
const V1_WATCH_KEYS = [
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
];
const V2_WATCH_KEYS = [...V1_WATCH_KEYS, "summary", "purpose", "receiverAgentIds"];
export function isWatchingTask(value: unknown): value is WatchingTask {
  return (
    isDynamicRecord(value) &&
    keys(value, [
      "id",
      "kind",
      "state",
      "title",
      "detail",
      "ownerAgentId",
      "link",
      "updatedAt",
      "dueAt",
      "waitingOn",
      "waitingSince",
      "urgency",
      "urgencyReason",
    ]) &&
    text(value.id, 160) &&
    WATCHING_TASK_KINDS.some((kind) => kind === value.kind) &&
    WATCHING_TASK_STATES.some((state) => state === value.state) &&
    text(value.title, 200) &&
    (value.detail === undefined || text(value.detail, 300)) &&
    (value.ownerAgentId === undefined || text(value.ownerAgentId, 160)) &&
    (value.link === undefined || targetUrl(value.link)) &&
    isoText(value.updatedAt) &&
    (value.dueAt === undefined || isoText(value.dueAt)) &&
    (value.waitingOn === undefined || text(value.waitingOn, 120)) &&
    (value.waitingSince === undefined || isoText(value.waitingSince)) &&
    (value.urgency === undefined || WATCHING_TASK_URGENCIES.some((urgency) => urgency === value.urgency)) &&
    (value.urgencyReason === undefined || text(value.urgencyReason, 160))
  );
}
export function isWatchingTaskHealth(value: unknown): value is WatchingTaskHealth {
  if (
    !isDynamicRecord(value) ||
    !keys(value, ["wip", "wipLimit", "oldest", "longestWaiting", "urgent", "score", "reasons"]) ||
    !count(value.wip, 10_000) ||
    !count(value.wipLimit, 10_000) ||
    !count(value.urgent, 10_000) ||
    !count(value.score, 100) ||
    !Array.isArray(value.reasons) ||
    value.reasons.length > 8 ||
    !value.reasons.every((reason) => text(reason, 160))
  )
    return false;
  const oldest = value.oldest;
  if (
    oldest !== undefined &&
    !(isDynamicRecord(oldest) && keys(oldest, ["taskId", "since"]) && text(oldest.taskId, 160) && isoText(oldest.since))
  )
    return false;
  const longest = value.longestWaiting;
  return (
    longest === undefined ||
    (isDynamicRecord(longest) &&
      keys(longest, ["taskId", "waitingOn", "since"]) &&
      text(longest.taskId, 160) &&
      text(longest.waitingOn, 120) &&
      isoText(longest.since))
  );
}
export function isWatchRegistryEntry(value: unknown, version: 1 | 2 = 1): value is WatchRegistryEntry {
  return (
    isDynamicRecord(value) &&
    keys(value, version === 1 ? V1_WATCH_KEYS : V2_WATCH_KEYS) &&
    (value.summary === undefined || text(value.summary, 120)) &&
    (value.purpose === undefined || text(value.purpose, 240)) &&
    (value.receiverAgentIds === undefined ||
      (Array.isArray(value.receiverAgentIds) &&
        value.receiverAgentIds.length <= WATCH_RECEIVERS_MAX &&
        value.receiverAgentIds.every((id) => text(id, 160)) &&
        new Set(value.receiverAgentIds).size === value.receiverAgentIds.length)) &&
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
  if (!isDynamicRecord(value) || (value.version !== 1 && value.version !== 2)) return false;
  const version = value.version;
  const base =
    keys(
      value,
      version === 1
        ? ["version", "sequence", "generatedAt", "watches"]
        : ["version", "sequence", "generatedAt", "watches", "publisherId", "page", "tasks", "health"],
    ) &&
    typeof value.sequence === "number" &&
    Number.isSafeInteger(value.sequence) &&
    value.sequence >= 1 &&
    timestamp(value.generatedAt) &&
    Array.isArray(value.watches) &&
    value.watches.length <= WATCH_REGISTRY_MAX_WATCHES &&
    value.watches.every((watch) => isWatchRegistryEntry(watch, version)) &&
    new Set(value.watches.map((watch) => (isDynamicRecord(watch) ? watch.id : null))).size === value.watches.length;
  if (!base || version === 1) return base;
  const tasks = value.tasks;
  const pageIndex = watchRegistryPageIndex(value.publisherId);
  if (pageIndex !== null || value.page !== undefined) {
    const page = value.page;
    return (
      pageIndex !== null &&
      tasks === undefined &&
      value.health === undefined &&
      isDynamicRecord(page) &&
      keys(page, ["index", "count"]) &&
      page.index === pageIndex &&
      typeof page.count === "number" &&
      Number.isSafeInteger(page.count) &&
      page.count >= pageIndex &&
      page.count <= WATCH_REGISTRY_MAX_PAGES
    );
  }
  if (tasks === undefined) return value.publisherId === undefined && value.health === undefined;
  return (
    (value.publisherId === undefined || value.publisherId === WATCH_REGISTRY_TASKS_PUBLISHER_ID) &&
    Array.isArray(value.watches) &&
    value.watches.length === 0 &&
    Array.isArray(tasks) &&
    tasks.length <= WATCH_REGISTRY_MAX_TASKS &&
    tasks.every(isWatchingTask) &&
    new Set(tasks.map((task) => (isDynamicRecord(task) ? task.id : null))).size === tasks.length &&
    (value.health === undefined || isWatchingTaskHealth(value.health))
  );
}
export function isTasksPublication(publication: WatchRegistryPublication): boolean {
  return publication.version === 2 && publication.tasks !== undefined;
}
export function watchRegistryPage(publication: WatchRegistryPublication): { index: number; count: number } | null {
  return publication.version === 2 && publication.page !== undefined ? publication.page : null;
}
function isWatchingTasks(value: unknown): value is WatchingTasks {
  return (
    isDynamicRecord(value) &&
    keys(value, ["sequence", "generatedAt", "health", "items"]) &&
    typeof value.sequence === "number" &&
    Number.isSafeInteger(value.sequence) &&
    value.sequence >= 1 &&
    timestamp(value.generatedAt) &&
    (value.health === undefined || isWatchingTaskHealth(value.health)) &&
    Array.isArray(value.items) &&
    value.items.length <= WATCH_REGISTRY_MAX_TASKS &&
    value.items.every(isWatchingTask)
  );
}
export function isWatchingSnapshot(value: unknown): value is WatchingSnapshot {
  if (
    !isDynamicRecord(value) ||
    !keys(value, ["registry", "tasks", "waits"]) ||
    (value.tasks !== undefined && value.tasks !== null && !isWatchingTasks(value.tasks)) ||
    !Array.isArray(value.waits) ||
    value.waits.length > 2000
  )
    return false;
  const registry = value.registry;
  if (
    registry !== null &&
    (!isDynamicRecord(registry) ||
      !keys(registry, ["version", "sequence", "generatedAt", "watches"]) ||
      (registry.version !== 1 && registry.version !== 2) ||
      typeof registry.sequence !== "number" ||
      !Number.isSafeInteger(registry.sequence) ||
      registry.sequence < 1 ||
      !timestamp(registry.generatedAt) ||
      !Array.isArray(registry.watches) ||
      registry.watches.length > WATCHING_MAX_WATCHES ||
      !registry.watches.every((watch) => {
        if (!isDynamicRecord(watch)) return false;
        const { publisherId, ...entry } = watch;
        return text(publisherId, 160) && isWatchRegistryEntry(entry, registry.version === 1 ? 1 : 2);
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
