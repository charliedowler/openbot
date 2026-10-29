import { type ContextPullRequestScope, contextPullRequestIdentity } from "@openbot/contracts/context-prs";
import { isDynamicRecord, isString } from "@openbot/contracts/runtime-values";
import type { WatchingSnapshot } from "@openbot/contracts/watch-registry";
import type { OpenBotDatabase } from "./openbot-database";

const rowLimit = 5000;
const referenceLimit = 200;
const textLimit = 200_000;
export class ContextPrReferences {
  constructor(
    readonly database: OpenBotDatabase,
    readonly watches: () => WatchingSnapshot["registry"],
  ) {}
  read(scope: ContextPullRequestScope): { urls: string[]; limited: boolean } {
    const sql = this.database.connection;
    const exists =
      scope.kind === "agent"
        ? sql.prepare("SELECT 1 FROM projection_agents WHERE agent_id=?").get(scope.id)
        : sql.prepare("SELECT 1 FROM projection_channels WHERE channel_id=?").get(scope.id);
    if (!exists) throw new Error("Unknown context PR scope.");
    const rows =
      scope.kind === "agent"
        ? sql
            .prepare(
              "SELECT json_extract(m.message_json,'$.text') AS text FROM projection_thread_messages m WHERE m.thread_id IN (SELECT thread_id FROM projection_agents WHERE agent_id=?1 UNION SELECT thread_id FROM projection_agent_threads WHERE agent_id=?1) ORDER BY m.created_at DESC,m.message_id DESC LIMIT ?2",
            )
            .all(scope.id, rowLimit + 1)
        : sql
            .prepare(
              "SELECT json_extract(message_json,'$.message.text') AS text FROM projection_channel_messages WHERE channel_id=? ORDER BY sequence DESC LIMIT ?",
            )
            .all(scope.id, rowLimit + 1);
    const urls = new Map<string, string>();
    let limited = rows.length > rowLimit;
    const add = (candidate: string) => {
      const identity = contextPullRequestIdentity(candidate);
      if (!identity) return;
      const key = identity.url.toLowerCase();
      if (urls.has(key)) return;
      if (urls.size >= referenceLimit) {
        limited = true;
        return;
      }
      urls.set(key, identity.url);
    };
    const scan = (value: unknown) => {
      if (!isString(value)) return;
      if (value.length > textLimit) limited = true;
      for (const match of value
        .slice(0, textLimit)
        .matchAll(
          /https:\/\/github\.com\/[A-Za-z0-9_.-]{1,100}\/[A-Za-z0-9_.-]{1,100}\/pull\/[1-9][0-9]{0,9}(?![A-Za-z0-9_/?#.-])/g,
        ))
        add(match[0]);
    };
    if (scope.kind === "agent") {
      for (const watch of this.watches()?.watches ?? [])
        if (watch.ownerAgentId === scope.id && watch.targetUrl) add(watch.targetUrl);
    }
    for (const row of rows.slice(0, rowLimit)) if (isDynamicRecord(row)) scan(row.text);
    if (scope.kind === "channel") {
      const tasks = sql
        .prepare("SELECT task_json FROM projection_channel_tasks WHERE channel_id=? LIMIT ?")
        .all(scope.id, rowLimit + 1);
      if (tasks.length > rowLimit) limited = true;
      for (const row of tasks.slice(0, rowLimit)) {
        if (!isDynamicRecord(row) || !isString(row.task_json)) continue;
        const task = JSON.parse(row.task_json);
        if (isDynamicRecord(task)) {
          scan(task.instruction);
          scan(task.expectedResult);
        }
      }
    }
    return { urls: [...urls.values()], limited };
  }
}
