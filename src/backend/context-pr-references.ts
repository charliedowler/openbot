import { type ContextPullRequestScope, contextPullRequestIdentity } from "@openbot/contracts/context-prs";
import { isDynamicRecord, isString } from "@openbot/contracts/runtime-values";
import type { WatchingSnapshot } from "@openbot/contracts/watch-registry";
import type { OpenBotDatabase } from "./openbot-database";

const rowLimit = 5000;
const referenceLimit = 200;
const textLimit = 200_000;
const scanLimit = 4_000_000;
const indexLimit = 400;
const indexRebuildMs = 3_600_000;
const pullRequestPattern =
  /https:\/\/github\.com\/[A-Za-z0-9_.-]{1,100}\/[A-Za-z0-9_.-]{1,100}\/pull\/[1-9][0-9]{0,9}(?![A-Za-z0-9_/?#.-])/g;
interface ScopeIndex {
  urls: Map<string, { url: string; rank: number }>;
  limited: boolean;
  rowid: number;
  streaming: Set<string>;
  tasks: Map<string, number>;
  rank: number;
  builtAt: number;
}
export class ContextPrReferences {
  readonly #indexes = new Map<string, ScopeIndex>();
  constructor(
    readonly database: OpenBotDatabase,
    readonly watches: () => WatchingSnapshot["registry"],
  ) {}
  index(scope: ContextPullRequestScope): { urls: string[]; limited: boolean } {
    const key = `${scope.kind}:${scope.id}`;
    let state = this.#indexes.get(key);
    if (state && Date.now() - state.builtAt > indexRebuildMs) {
      this.#indexes.delete(key);
      state = undefined;
    }
    if (state) this.#require(scope);
    if (!state) {
      const sql = this.database.connection;
      const watermark =
        scope.kind === "agent"
          ? sql
              .prepare(
                "SELECT COALESCE(MAX(rowid),0) AS rowid FROM projection_thread_messages WHERE thread_id IN (SELECT thread_id FROM projection_agents WHERE agent_id=?1 UNION SELECT thread_id FROM projection_agent_threads WHERE agent_id=?1)",
              )
              .get(scope.id)
          : sql
              .prepare("SELECT COALESCE(MAX(rowid),0) AS rowid FROM projection_channel_messages WHERE channel_id=?")
              .get(scope.id);
      const full = this.read(scope, false);
      state = {
        urls: new Map(full.urls.map((url, index) => [url.toLowerCase(), { url, rank: -index }])),
        limited: full.limited,
        rowid: isDynamicRecord(watermark) && typeof watermark.rowid === "number" ? watermark.rowid : 0,
        streaming: this.#streaming(scope),
        tasks: scope.kind === "channel" ? this.#taskSizes(scope.id) : new Map(),
        rank: 0,
        builtAt: Date.now(),
      };
      if (this.#indexes.size >= indexLimit) this.#indexes.delete(this.#indexes.keys().next().value ?? "");
      this.#indexes.set(key, state);
    } else this.#append(scope, state);
    const urls = new Map([...state.urls.entries()].map(([key, entry]) => [key, entry.url]));
    let limited = state.limited;
    if (scope.kind === "agent")
      for (const watch of this.watches()?.watches ?? [])
        if (watch.ownerAgentId === scope.id && !watch.channelId && watch.targetUrl) {
          const identity = contextPullRequestIdentity(watch.targetUrl);
          if (!identity || urls.has(identity.url.toLowerCase())) continue;
          if (urls.size >= referenceLimit) limited = true;
          else urls.set(identity.url.toLowerCase(), identity.url);
        }
    return { urls: [...urls.values()], limited };
  }
  #require(scope: ContextPullRequestScope): void {
    const sql = this.database.connection;
    const exists =
      scope.kind === "agent"
        ? sql.prepare("SELECT 1 FROM projection_agents WHERE agent_id=?").get(scope.id)
        : sql.prepare("SELECT 1 FROM projection_channels WHERE channel_id=?").get(scope.id);
    if (!exists) {
      this.forget(scope);
      throw new Error("Unknown context PR scope.");
    }
  }
  forget(scope: ContextPullRequestScope): void {
    this.#indexes.delete(`${scope.kind}:${scope.id}`);
  }
  #streaming(scope: ContextPullRequestScope): Set<string> {
    const sql = this.database.connection;
    const rows =
      scope.kind === "agent"
        ? sql
            .prepare(
              "SELECT message_id FROM projection_thread_messages WHERE thread_id IN (SELECT thread_id FROM projection_agents WHERE agent_id=?1 UNION SELECT thread_id FROM projection_agent_threads WHERE agent_id=?1) AND status='streaming'",
            )
            .all(scope.id)
        : sql
            .prepare(
              "SELECT message_id FROM projection_channel_messages WHERE channel_id=? AND json_extract(message_json,'$.message.status')='streaming'",
            )
            .all(scope.id);
    return new Set(rows.flatMap((row) => (isDynamicRecord(row) && isString(row.message_id) ? [row.message_id] : [])));
  }
  #taskSizes(channelId: string): Map<string, number> {
    return new Map(
      this.database.connection
        .prepare("SELECT task_id,length(task_json) AS size FROM projection_channel_tasks WHERE channel_id=?")
        .all(channelId)
        .flatMap((row) =>
          isDynamicRecord(row) && isString(row.task_id) && typeof row.size === "number"
            ? [[row.task_id, row.size] as const]
            : [],
        ),
    );
  }
  #append(scope: ContextPullRequestScope, state: ScopeIndex): void {
    const sql = this.database.connection;
    const found: string[] = [];
    const scan = (value: unknown) => {
      if (!isString(value)) return;
      const part = value.slice(0, textLimit);
      for (const match of part.matchAll(pullRequestPattern))
        if (part.length < textLimit || match.index + match[0].length < part.length) found.push(match[0]);
    };
    const streaming = new Set<string>();
    const rows =
      scope.kind === "agent"
        ? sql
            .prepare(
              `SELECT rowid,message_id,status,substr(json_extract(message_json,'$.text'),1,${textLimit}) AS text FROM projection_thread_messages WHERE thread_id IN (SELECT thread_id FROM projection_agents WHERE agent_id=?1 UNION SELECT thread_id FROM projection_agent_threads WHERE agent_id=?1) AND (rowid>?2 OR message_id IN (SELECT value FROM json_each(?3))) ORDER BY rowid LIMIT ${rowLimit}`,
            )
            .all(scope.id, state.rowid, JSON.stringify([...state.streaming]))
        : sql
            .prepare(
              `SELECT rowid,message_id,json_extract(message_json,'$.message.status') AS status,substr(json_extract(message_json,'$.message.text'),1,${textLimit}) AS text FROM projection_channel_messages WHERE channel_id=?1 AND (rowid>?2 OR message_id IN (SELECT value FROM json_each(?3))) ORDER BY rowid LIMIT ${rowLimit}`,
            )
            .all(scope.id, state.rowid, JSON.stringify([...state.streaming]));
    if (rows.length >= rowLimit) state.limited = true;
    for (const row of rows) {
      if (!isDynamicRecord(row)) continue;
      if (typeof row.rowid === "number") state.rowid = Math.max(state.rowid, row.rowid);
      if (row.status === "streaming" && isString(row.message_id)) streaming.add(row.message_id);
      scan(row.text);
    }
    state.streaming = streaming;
    if (scope.kind === "channel") {
      const sizes = this.#taskSizes(scope.id);
      const changed = [...sizes].filter(([id, size]) => state.tasks.get(id) !== size).map(([id]) => id);
      state.tasks = sizes;
      if (changed.length)
        for (const row of sql
          .prepare(
            `SELECT substr(json_extract(task_json,'$.instruction'),1,${textLimit}) AS instruction,substr(json_extract(task_json,'$.expectedResult'),1,${textLimit}) AS expected FROM projection_channel_tasks WHERE task_id IN (SELECT value FROM json_each(?)) LIMIT ${rowLimit}`,
          )
          .all(JSON.stringify(changed))) {
          if (!isDynamicRecord(row)) continue;
          scan(row.instruction);
          scan(row.expected);
        }
    }
    for (const candidate of found) {
      const identity = contextPullRequestIdentity(candidate);
      if (!identity) continue;
      const key = identity.url.toLowerCase();
      const existing = state.urls.get(key);
      state.rank += 1;
      if (existing) {
        existing.rank = state.rank;
        continue;
      }
      if (state.urls.size >= referenceLimit) {
        state.limited = true;
        let oldest: string | undefined;
        let lowest = Number.POSITIVE_INFINITY;
        for (const [entryKey, entry] of state.urls)
          if (entry.rank < lowest) {
            lowest = entry.rank;
            oldest = entryKey;
          }
        if (oldest !== undefined) state.urls.delete(oldest);
      }
      state.urls.set(key, { url: identity.url, rank: state.rank });
    }
  }
  read(scope: ContextPullRequestScope, includeWatches = true): { urls: string[]; limited: boolean } {
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
      if (includeWatches)
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
