import { isDynamicRecord } from "../runtime-values";
import {
  type AdminDecoder,
  adminRoute,
  count,
  fields,
  list,
  nullable,
  type OptionalRouteCodec,
  oneOf,
  string,
} from "./admin-wire";

export const WATCH_REGISTRY_V2_CAPABILITY = "watch-registry-v2";
export const WATCH_REGISTRY_V2_ROUTES = { snapshot: "/v2/watch-registry" } as const;
const watch = fields(
  {
    id: string(160),
    ownerAgentId: string(160),
    publisherId: string(160),
    sourceKind: string(64),
    title: string(256),
    status: oneOf("active", "paused", "completed", "expired", "error"),
  },
  {
    targetUrl: string(2048),
    lastCheckedAt: string(160),
    expiresAt: string(160),
    channelId: string(160),
    lastError: string(512),
    condition: string(256),
    summary: string(120),
    purpose: string(240),
    receiverAgentIds: list(string(160), 8),
  },
);
const task = fields(
  {
    id: string(160),
    kind: oneOf("decision", "follow-up", "dispatch"),
    state: oneOf("waiting-on-charlie", "in-progress", "waiting-on-event", "done"),
    title: string(200),
    updatedAt: string(160),
  },
  {
    detail: string(300),
    ownerAgentId: string(160),
    link: string(2048),
    dueAt: string(160),
    waitingOn: string(120),
    waitingSince: string(160),
    urgency: oneOf("low", "normal", "high"),
    urgencyReason: string(160),
  },
);
const health = fields(
  { wip: count, wipLimit: count, urgent: count, score: count, reasons: list(string(160), 8) },
  {
    oldest: fields({ taskId: string(160), since: string(160) }),
    longestWaiting: fields({ taskId: string(160), waitingOn: string(120), since: string(160) }),
  },
);
const wait = fields(
  {
    id: string(160),
    ownerAgentId: string(160),
    note: string(500),
    routineId: nullable(string(160)),
    createdAt: string(160),
    wokenAt: nullable(string(160)),
  },
  { channelId: string(160) },
);
const snapshot = fields({
  registry: nullable(fields({ version: count, sequence: count, generatedAt: string(160), watches: list(watch, 2000) })),
  tasks: nullable(fields({ sequence: count, generatedAt: string(160), items: list(task, 300) }, { health })),
  waits: list(wait, 2000),
});
const snapshotRequest: AdminDecoder = (value) => {
  if (value === undefined || value === null) return {};
  if (!isDynamicRecord(value) || Object.keys(value).length !== 0) throw new Error("Invalid watch registry request.");
  return {};
};
export const WATCH_REGISTRY_V2_CODECS: ReadonlyMap<string, OptionalRouteCodec> = new Map([
  [WATCH_REGISTRY_V2_ROUTES.snapshot, adminRoute(snapshotRequest, snapshot)],
]);
