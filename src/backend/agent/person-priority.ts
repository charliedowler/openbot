import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { EXTERNAL_EVENT_END, EXTERNAL_EVENT_START } from "../webhook-trigger";

export const RELAYED_PERSON_REQUEST_FIELD = "charlieRequest";

const MARKER = `"${RELAYED_PERSON_REQUEST_FIELD}":true`;
const CACHE_LIMIT = 512;
const cache = new Map<string, boolean>();

export function relayedPersonRequest(text: string): boolean {
  if (!text.endsWith(EXTERNAL_EVENT_END) || !text.includes(MARKER)) return false;
  const cached = cache.get(text);
  if (cached !== undefined) return cached;
  const result = readRelayedPersonRequest(text);
  if (cache.size >= CACHE_LIMIT) cache.delete(cache.keys().next().value ?? "");
  cache.set(text, result);
  return result;
}

function readRelayedPersonRequest(text: string): boolean {
  const start = text.lastIndexOf(`\n${EXTERNAL_EVENT_START}\n`);
  if (start < 0) return false;
  const lines = text.slice(start + 1).split("\n");
  if (lines.length !== 4 || lines[0] !== EXTERNAL_EVENT_START || lines[3] !== EXTERNAL_EVENT_END) return false;
  try {
    const envelope = JSON.parse(lines[2] ?? "");
    return (
      isDynamicRecord(envelope) &&
      isDynamicRecord(envelope.data) &&
      envelope.data[RELAYED_PERSON_REQUEST_FIELD] === true
    );
  } catch {
    return false;
  }
}

export function personWork(work: { sender: { kind: string }; text: string }): boolean {
  return work.sender.kind === "user" || (work.sender.kind === "routine" && relayedPersonRequest(work.text));
}
