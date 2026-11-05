import type { ConversationMessage } from "@openbot/contracts/ipc";
import { fireEvent, render, screen } from "@solidjs/testing-library";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { StoryConversation } from "../../../stories/conversation-story-support";
import { toAgentMessages } from "../../app-message-projection";
import { STORY_AGENTS } from "../../preview/fixtures";

const AGENT_ID = "chief";
const ROUTINE = {
  routineId: "routine-inbox",
  runId: "run-1",
  name: "Inbox sweep",
  scheduledFor: "2026-11-05T09:00:00.000Z",
};

function message(
  id: string,
  author: ConversationMessage["author"],
  text: string,
  minute: number,
  fields: Partial<ConversationMessage> = {},
): ConversationMessage {
  return {
    id,
    author,
    text,
    createdAt: `2026-11-05T09:${String(minute).padStart(2, "0")}:00.000Z`,
    status: "completed",
    ...fields,
  };
}

function incomingExchange(messageId: string) {
  return {
    direction: "incoming" as const,
    messageId,
    senderAgentId: "research",
    recipientAgentIds: [AGENT_ID],
    replyToMessageId: null,
    deliveries: [
      {
        id: `delivery-${messageId}`,
        recipientAgentId: AGENT_ID,
        status: "completed" as const,
        position: null,
        error: null,
      },
    ],
  };
}

const stored: ConversationMessage[] = [
  message("person-ask", "user", "Can you check the release?", 0, { turnId: "turn-person", source: "user" }),
  message("person-step", "assistant", "Reading the release notes", 1, {
    turnId: "turn-person",
    itemType: "commentary",
  }),
  message("person-answer", "assistant", "The release is ready.", 2, {
    turnId: "turn-person",
    itemType: "final_answer",
  }),
  message("routine-running", "system", ROUTINE.name, 3, {
    source: "system",
    itemType: `routine-run-event:running:${ROUTINE.routineId}:${ROUTINE.runId}`,
  }),
  message("routine-ask", "user", "You are Chief. This run was started by a signed pod.result delivery.", 3, {
    turnId: "turn-routine",
    source: "routine",
    itemType: "routine",
    routine: ROUTINE,
  }),
  message("routine-step", "assistant", "Sorting the inbox", 4, { turnId: "turn-routine", itemType: "commentary" }),
  message("routine-answer", "assistant", "Two invoices need your approval.", 5, {
    turnId: "turn-routine",
    itemType: "final_answer",
  }),
  message("routine-succeeded", "system", ROUTINE.name, 6, {
    source: "system",
    itemType: `routine-run-event:succeeded:${ROUTINE.routineId}:${ROUTINE.runId}`,
  }),
  message("agent-in-1", "agent", "The market scan is done.", 7, {
    turnId: "turn-agent-quiet",
    source: "agent",
    itemType: "agent-exchange",
    senderAgentId: "research",
    exchange: incomingExchange("exchange-1"),
  }),
  message("agent-quiet-step", "assistant", "Filing the scan", 8, {
    turnId: "turn-agent-quiet",
    itemType: "commentary",
  }),
  message("agent-quiet-answer", "assistant", "", 9, { turnId: "turn-agent-quiet", itemType: "final_answer" }),
  message("agent-in-2", "agent", "Here are three vendors.", 10, {
    turnId: "turn-agent",
    source: "agent",
    itemType: "agent-exchange",
    senderAgentId: "research",
    exchange: incomingExchange("exchange-2"),
  }),
  message("agent-step", "assistant", "Comparing the vendors", 11, { turnId: "turn-agent", itemType: "commentary" }),
  message("agent-answer", "assistant", "Research found three vendors. The cheapest is Acme.", 12, {
    turnId: "turn-agent",
    itemType: "final_answer",
  }),
];

const STORAGE_KEY = `openbot:direct-chat-background:local:${AGENT_ID}`;

function renderChat(agent = STORY_AGENTS[0]) {
  render(() => <StoryConversation agent={agent} messages={toAgentMessages(stored, AGENT_ID)} unreadCount={0} />);
}

const stepRows = () => screen.queryAllByRole("button", { name: /step/u });

describe("background of an agent's own chat", () => {
  beforeEach(() => window.localStorage.clear());

  it("shows the person's messages and the agent's answers only, with the switch off", async () => {
    renderChat();
    const toggle = await screen.findByRole("switch", { name: "Show background" });
    expect(toggle).not.toBeChecked();
    expect(screen.getByText("Can you check the release?")).toBeInTheDocument();
    expect(screen.getByText("The release is ready.")).toBeInTheDocument();
    expect(screen.getByText("Two invoices need your approval.")).toBeInTheDocument();
    expect(screen.getByText("Research found three vendors. The cheapest is Acme.")).toBeInTheDocument();
    expect(screen.queryByText(/Inbox sweep/u)).not.toBeInTheDocument();
    expect(screen.queryByText(/pod\.result/u)).not.toBeInTheDocument();
    expect(screen.queryAllByText(/messages? with/u)).toHaveLength(0);
    expect(screen.queryAllByText(/routine/iu)).toHaveLength(0);
    expect(stepRows()).toHaveLength(1);
  });

  it("shows every row as before with the switch on, and keeps the choice for this agent", async () => {
    renderChat();
    fireEvent.click(await screen.findByRole("switch", { name: "Show background" }));
    expect(await screen.findByRole("switch", { name: "Show background" })).toBeChecked();
    expect(window.localStorage.getItem(STORAGE_KEY)).toBe("true");
    expect((await screen.findAllByText(/Inbox sweep/u)).length).toBeGreaterThan(0);
    expect((await screen.findAllByText(/2 messages with/u)).length).toBeGreaterThan(0);
    expect(stepRows().length).toBeGreaterThan(1);
  });

  it("reads a saved choice", async () => {
    window.localStorage.setItem(STORAGE_KEY, "true");
    renderChat();
    expect(await screen.findByRole("switch", { name: "Show background" })).toBeChecked();
    expect((await screen.findAllByText(/Inbox sweep/u)).length).toBeGreaterThan(0);
  });

  it("hides the background and keeps a change for the page when storage is blocked", async () => {
    const { getItem, setItem } = Storage.prototype;
    const blocked = (key: string) => {
      if (key.startsWith("openbot:direct-chat-background:")) throw new DOMException("Blocked", "SecurityError");
    };
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(function (this: Storage, key) {
      blocked(key);
      return getItem.call(this, key);
    });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (this: Storage, key, value) {
      blocked(key);
      setItem.call(this, key, value);
    });
    renderChat(STORY_AGENTS[1]);
    const toggle = await screen.findByRole("switch", { name: "Show background" });
    expect(toggle).not.toBeChecked();
    expect(screen.queryByText(/Inbox sweep/u)).not.toBeInTheDocument();
    fireEvent.click(toggle);
    expect(await screen.findByRole("switch", { name: "Show background" })).toBeChecked();
    expect((await screen.findAllByText(/Inbox sweep/u)).length).toBeGreaterThan(0);
  });
});
