import { isDynamicRecord } from "./runtime-values";

export type ContextPullRequestScope = { kind: "agent" | "channel"; id: string };
export type ContextPullRequestsInput = { scope: ContextPullRequestScope; refresh?: boolean };
export type ContextPullRequestFailureReason =
  | "not-connected"
  | "not-found"
  | "forbidden"
  | "unavailable"
  | "invalid-response"
  | "timeout";
export interface ContextPullRequest {
  url: string;
  repo: string;
  number: number;
  title: string;
  state: "open" | "merged";
  draft: boolean;
  mergedAt: string | null;
  headSha: string;
  checks: "passed" | "pending" | "failed" | "unknown";
  checkedAt: string;
}
export interface ContextPullRequestsSnapshot {
  scope: ContextPullRequestScope;
  status: "ready" | "not-connected" | "unsupported";
  checkedAt: string;
  open: ContextPullRequest[];
  merged: ContextPullRequest[];
  failures: Array<{ url: string; reason: ContextPullRequestFailureReason }>;
  referencesLimited: boolean;
}
const text = (value: unknown, limit: number): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= limit;
const instant = (value: unknown): value is string =>
  text(value, 40) && Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value;
export function contextPullRequestIdentity(value: unknown): { url: string; repo: string; number: number } | null {
  if (!text(value, 2048)) return null;
  const match =
    /^https:\/\/github\.com\/([A-Za-z0-9_.-]{1,100})\/([A-Za-z0-9_.-]{1,100})\/pull\/([1-9][0-9]{0,9})\/?$/.exec(value);
  if (!match || [match[1], match[2]].some((part) => part === "." || part === "..")) return null;
  const number = Number(match[3]);
  if (!Number.isSafeInteger(number)) return null;
  return { url: `https://github.com/${match[1]}/${match[2]}/pull/${number}`, repo: `${match[1]}/${match[2]}`, number };
}
export function parseContextPullRequestsInput(value: unknown): ContextPullRequestsInput {
  if (
    !isDynamicRecord(value) ||
    !isDynamicRecord(value.scope) ||
    !["agent", "channel"].includes(String(value.scope.kind)) ||
    !text(value.scope.id, 128) ||
    [...value.scope.id].some((character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127) ||
    Object.keys(value.scope).some((key) => !["kind", "id"].includes(key)) ||
    Object.keys(value).some((key) => !["scope", "refresh"].includes(key)) ||
    (value.refresh !== undefined && typeof value.refresh !== "boolean")
  )
    throw new Error("Invalid context PR scope.");
  return {
    scope: { kind: value.scope.kind === "agent" ? "agent" : "channel", id: value.scope.id },
    ...(value.refresh === undefined ? {} : { refresh: value.refresh }),
  };
}
export function isContextPullRequest(value: unknown): value is ContextPullRequest {
  if (!isDynamicRecord(value)) return false;
  const identity = contextPullRequestIdentity(value.url);
  return (
    identity !== null &&
    value.url === identity.url &&
    value.repo === identity.repo &&
    value.number === identity.number &&
    text(value.title, 1024) &&
    ["open", "merged"].includes(String(value.state)) &&
    typeof value.draft === "boolean" &&
    (value.mergedAt === null || instant(value.mergedAt)) &&
    (value.state !== "merged" || instant(value.mergedAt)) &&
    typeof value.headSha === "string" &&
    /^[a-f0-9]{40}$/.test(value.headSha) &&
    ["passed", "pending", "failed", "unknown"].includes(String(value.checks)) &&
    instant(value.checkedAt)
  );
}
export function isContextPullRequestsSnapshot(value: unknown): value is ContextPullRequestsSnapshot {
  if (!isDynamicRecord(value)) return false;
  try {
    parseContextPullRequestsInput({ scope: value.scope });
  } catch {
    return false;
  }
  return (
    ["ready", "not-connected", "unsupported"].includes(String(value.status)) &&
    instant(value.checkedAt) &&
    typeof value.referencesLimited === "boolean" &&
    Array.isArray(value.open) &&
    value.open.length <= 200 &&
    value.open.every((row) => isContextPullRequest(row) && row.state === "open") &&
    Array.isArray(value.merged) &&
    value.merged.length <= 3 &&
    value.merged.every((row) => isContextPullRequest(row) && row.state === "merged") &&
    Array.isArray(value.failures) &&
    value.failures.length <= 200 &&
    value.failures.every(
      (row) =>
        isDynamicRecord(row) &&
        contextPullRequestIdentity(row.url) !== null &&
        ["not-connected", "not-found", "forbidden", "unavailable", "invalid-response", "timeout"].includes(
          String(row.reason),
        ),
    )
  );
}
export function unsupportedContextPullRequests(scope: ContextPullRequestScope): ContextPullRequestsSnapshot {
  return {
    scope,
    status: "unsupported",
    checkedAt: new Date().toISOString(),
    open: [],
    merged: [],
    failures: [],
    referencesLimited: false,
  };
}
