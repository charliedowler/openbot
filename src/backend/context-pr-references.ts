import { type ContextPullRequestScope, contextPullRequestIdentity } from "@openbot/contracts/context-prs";
import { isDynamicRecord, isString } from "@openbot/contracts/runtime-values";
import type { WatchingSnapshot } from "@openbot/contracts/watch-registry";
import type { OpenBotDatabase } from "./openbot-database";

const rowLimit = 5000;
const referenceLimit = 200;
const textLimit = 200_000;
const scanLimit = 4_000_000;
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
    const urls = new Map<string, string>();
    let limited = false,
      scannedText = 0;
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
      const remaining = Math.min(textLimit, scanLimit - scannedText);
      const truncated = value.length > remaining;
      if (truncated) limited = true;
      const part = value.slice(0, remaining);
      scannedText += part.length;
      for (const match of part.matchAll(
        /https:\/\/github\.com\/[A-Za-z0-9_.-]{1,100}\/[A-Za-z0-9_.-]{1,100}\/pull\/[1-9][0-9]{0,9}(?![A-Za-z0-9_/?#.-])/g,
      ))
        if (!truncated || match.index + match[0].length < part.length) add(match[0]);
    };
    if (scope.kind === "agent") {
      for (const watch of this.watches()?.watches ?? [])
        if (watch.ownerAgentId === scope.id && !watch.channelId && watch.targetUrl) add(watch.targetUrl);
      const text = sql.prepare(
        "SELECT substr(json_extract(message_json,'$.text'),1,?) AS text FROM projection_thread_messages WHERE thread_id=? AND message_id=?",
      );
      const rows = sql
        .prepare(
          "SELECT thread_id,message_id FROM projection_thread_messages WHERE thread_id IN (SELECT thread_id FROM projection_agents WHERE agent_id=?1 UNION SELECT thread_id FROM projection_agent_threads WHERE agent_id=?1) ORDER BY created_at DESC,message_id DESC LIMIT ?2",
        )
        .iterate(scope.id, rowLimit + 1);
      let count = 0;
      for (const row of rows) {
        if (count++ >= rowLimit || scannedText >= scanLimit) {
          limited = true;
          break;
        }
        if (!isDynamicRecord(row) || !isString(row.thread_id) || !isString(row.message_id)) continue;
        const value = text.get(textLimit + 1, row.thread_id, row.message_id);
        if (isDynamicRecord(value)) scan(value.text);
      }
    } else {
      const text = sql.prepare(
        "SELECT substr(json_extract(message_json,'$.message.text'),1,?) AS text FROM projection_channel_messages WHERE channel_id=? AND message_id=?",
      );
      const rows = sql
        .prepare("SELECT message_id FROM projection_channel_messages WHERE channel_id=? ORDER BY sequence DESC LIMIT ?")
        .iterate(scope.id, rowLimit + 1);
      let count = 0;
      for (const row of rows) {
        if (count++ >= rowLimit || scannedText >= scanLimit) {
          limited = true;
          break;
        }
        if (!isDynamicRecord(row) || !isString(row.message_id)) continue;
        const value = text.get(textLimit + 1, scope.id, row.message_id);
        if (isDynamicRecord(value)) scan(value.text);
      }
      const tasks = sql
        .prepare(
          "SELECT substr(json_extract(task_json,'$.instruction'),1,?1) AS instruction,substr(json_extract(task_json,'$.expectedResult'),1,?1) AS expected FROM projection_channel_tasks WHERE channel_id=?2 LIMIT ?3",
        )
        .iterate(textLimit + 1, scope.id, rowLimit + 1);
      count = 0;
      for (const row of tasks) {
        if (count++ >= rowLimit || scannedText >= scanLimit) {
          limited = true;
          break;
        }
        if (isDynamicRecord(row)) {
          scan(row.instruction);
          scan(row.expected);
        }
      }
    }
    return { urls: [...urls.values()], limited };
  }
}
