import type { ChannelMessage } from "@openbot/contracts/ipc";
import { type DynamicRecord, isDynamicRecord, isString } from "@openbot/contracts/runtime-values";

const EVENT_START = "--- external event input ---";
const EVENT_END = "--- end of external event input ---";
const TITLE_KEYS = ["title", "summary", "subject", "text", "name"] as const;
const TITLE_LIMIT = 80;

export interface ChannelRoutineEvent {
  summary: string;
}

export function channelRoutineEvent(entry: ChannelMessage): ChannelRoutineEvent | null {
  if (entry.author.kind !== "member" || !entry.author.id.startsWith("routine:")) return null;
  const text = entry.message.text;
  const start = text.indexOf(EVENT_START);
  const end = text.lastIndexOf(EVENT_END);
  if (start < 0 || end < start) return null;
  const envelope = parseEnvelope(text.slice(start + EVENT_START.length, end));
  const parts = [entry.author.name.trim() || entry.author.id.slice("routine:".length)];
  if (envelope) {
    if (isString(envelope.type) && envelope.type) parts.push(envelope.type);
    const data = isDynamicRecord(envelope.data) ? envelope.data : null;
    if (data && isString(data.signalKind) && data.signalKind) parts.push(data.signalKind);
    const title = data ? titleOf(data) : null;
    if (title) parts.push(title);
  }
  return { summary: parts.join(" · ") };
}

function parseEnvelope(block: string): DynamicRecord | null {
  const json = block.slice(block.indexOf("{"), block.lastIndexOf("}") + 1);
  if (!json) return null;
  try {
    const value = JSON.parse(json);
    return isDynamicRecord(value) ? value : null;
  } catch {
    return null;
  }
}

function titleOf(data: DynamicRecord): string | null {
  const nested = isDynamicRecord(data.data) ? data.data : null;
  for (const source of nested ? [data, nested] : [data])
    for (const key of TITLE_KEYS) {
      const value = source[key];
      if (!isString(value)) continue;
      const line = value.replace(/\s+/gu, " ").trim();
      if (line) return line.length > TITLE_LIMIT ? `${line.slice(0, TITLE_LIMIT - 1)}…` : line;
    }
  return null;
}
