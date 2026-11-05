import { ROUTINE_QUIET_ANSWER_ITEM_TYPE, ROUTINE_RUN_EVENT_ITEM_TYPE_PREFIX } from "@openbot/contracts/ipc";
import type { AgentMessage } from "@openbot/ui/data";
import { createSignal } from "solid-js";

const SHOW_BACKGROUND_STORAGE_PREFIX = "openbot:direct-chat-background:";

/**
 * The turns of an agent's own chat that no person started: a routine run or a message from another
 * agent started them. A turn that also runs a person's message is the person's turn. A turn whose
 * first message is not loaded is not in the set, so it shows as before.
 */
export function backgroundTurnIds(messages: readonly AgentMessage[]): ReadonlySet<string> {
  const started = new Set<string>();
  const personTurns = new Set<string>();
  for (const message of messages) {
    if (!message.turnId) continue;
    if (message.routine || message.exchange?.direction === "incoming") started.add(message.turnId);
    else if (message.author === "you") personTurns.add(message.turnId);
  }
  for (const turnId of personTurns) started.delete(turnId);
  return started;
}

/**
 * Whether the agent's own chat hides a message while the background is hidden. The chat then shows
 * the person's messages and the agent's answers only: a routine run's request and its state rows,
 * a message from another agent, the other rows of a turn that no person started, and an empty
 * answer have no row. A failed empty answer in a person's turn keeps its row, because it tells the
 * person that the request failed.
 */
export function hiddenInDirectChat(message: AgentMessage, backgroundTurns: ReadonlySet<string>): boolean {
  if (message.routine || message.actionMarker?.kind === "routine-run") return true;
  if (message.itemType?.startsWith(ROUTINE_RUN_EVENT_ITEM_TYPE_PREFIX)) return true;
  if (message.exchange?.direction === "incoming") return true;
  const background = message.turnId !== undefined && backgroundTurns.has(message.turnId);
  if (emptyAnswer(message)) return background || message.status !== "Failed";
  return background && !answerToReader(message);
}

function emptyAnswer(message: AgentMessage): boolean {
  return (
    message.author === "agent" &&
    (message.kind ?? "text") === "text" &&
    !message.streaming &&
    !message.actionMarker &&
    !message.questionPrompt &&
    !message.imageGeneration &&
    !message.attachments?.length &&
    !message.body.trim()
  );
}

/** A message of the agent that says something to the reader: text, a file, an image or a question. */
function answerToReader(message: AgentMessage): boolean {
  if (message.author !== "agent" || message.actionMarker) return false;
  if (message.kind === "question") return true;
  if ((message.kind ?? "text") !== "text" || message.itemType === ROUTINE_QUIET_ANSWER_ITEM_TYPE) return false;
  return Boolean(message.body.trim() || message.attachments?.length || message.imageGeneration);
}

/** The key of one agent's own chat, on one server, for the background choice. */
export function directChatBackgroundKey(serverId: string, agentId: string): string {
  return `${serverId}:${agentId}`;
}

/** The choices that storage did not save, for this page only. They win over an older saved value. */
const [unsavedChoices, setUnsavedChoices] = createSignal<ReadonlyMap<string, boolean>>(new Map());
const [savedVersion, setSavedVersion] = createSignal(0);

/**
 * Whether the agent's own chat shows its background, on this device and browser. It is hidden unless
 * the person chose otherwise. Reading `window.localStorage` throws when the browser blocks storage,
 * so it is read inside the guard.
 */
export function directChatBackgroundShown(key: string): boolean {
  savedVersion();
  const unsaved = unsavedChoices().get(key);
  if (unsaved !== undefined) return unsaved;
  try {
    return window.localStorage.getItem(SHOW_BACKGROUND_STORAGE_PREFIX + key) === "true";
  } catch {
    return false;
  }
}

/** Saves the choice for one agent's own chat. Blocked or full storage keeps it for this page only. */
export function setDirectChatBackgroundShown(key: string, shown: boolean): void {
  try {
    window.localStorage.setItem(SHOW_BACKGROUND_STORAGE_PREFIX + key, String(shown));
    if (unsavedChoices().has(key))
      setUnsavedChoices((choices) => {
        const next = new Map(choices);
        next.delete(key);
        return next;
      });
    setSavedVersion((version) => version + 1);
  } catch {
    setUnsavedChoices((choices) => new Map(choices).set(key, shown));
  }
}
