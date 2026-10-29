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

export const WATCH_REGISTRY_CAPABILITY = "watch-registry-v1";
export const WATCH_REGISTRY_ROUTES = { snapshot: "/v1/watch-registry" } as const;
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
  waits: list(wait, 2000),
});
const snapshotRequest: AdminDecoder = (value) => {
  if (value === undefined || value === null) return {};
  if (!isDynamicRecord(value) || Object.keys(value).length !== 0) throw new Error("Invalid watch registry request.");
  return {};
};
export const WATCH_REGISTRY_CODECS: ReadonlyMap<string, OptionalRouteCodec> = new Map([
  [WATCH_REGISTRY_ROUTES.snapshot, adminRoute(snapshotRequest, snapshot)],
]);
export interface WatchingEvent {
  type: "watching-changed";
}
export function watchingEvent(value: unknown): WatchingEvent | null {
  if (!isDynamicRecord(value) || value.type !== "watching-changed") return null;
  if (Object.keys(value).some((key) => key !== "type")) throw new Error("Invalid watching event.");
  return { type: "watching-changed" };
}
