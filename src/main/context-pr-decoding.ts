import { type ContextPullRequestsSnapshot, isContextPullRequestsSnapshot } from "@openbot/contracts/context-prs";
export function decodeContextPullRequestsFromHost(value: unknown): ContextPullRequestsSnapshot {
  if (!isContextPullRequestsSnapshot(value)) throw new Error("Invalid context PR snapshot.");
  return value;
}
