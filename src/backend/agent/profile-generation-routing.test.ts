import { EventEmitter } from "node:events";
import type { AgentModelOption } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { describe, expect, it } from "vitest";
import type { AgentClient, DiagnosticOrigin } from "../agent-client";
import type { AppServerNotification, AppServerRequest, ResponseDecoder } from "../protocol";
import { providerSync } from "../provider-client-effects";
import { generateTextWithoutTools, generationEffort } from "./profile-generation";

interface ClientEvents {
  notification: [notification: AppServerNotification];
  request: [request: AppServerRequest];
  exit: [error: Error];
  diagnostic: [message: string, origin?: DiagnosticOrigin];
}

const model: AgentModelOption = {
  provider: "codex",
  id: "gpt-6.1-sol",
  name: "Synthetic model",
  description: "Synthetic model",
  defaultReasoningEffort: "high",
  supportedReasoningEfforts: ["xhigh", "medium", "low", "high"],
};

class RecordingClient extends EventEmitter<ClientEvents> implements AgentClient {
  readonly provider = "codex";
  readonly efforts: { method: string; effort: unknown }[] = [];
  running = false;
  start() {
    this.running = true;
  }
  stop() {
    return Effect.sync(() => {
      this.running = false;
    });
  }
  request<T>(method: string, params: unknown, decoder: ResponseDecoder<T>) {
    return Effect.gen({ self: this }, function* () {
      if ((method === "thread/start" || method === "turn/start") && typeof params === "object" && params !== null)
        this.efforts.push({ method, effort: "effort" in params ? params.effort : undefined });
      if (method === "turn/start")
        queueMicrotask(() => {
          this.emit("notification", { method: "item/agentMessage/delta", params: { delta: '{"agentId":"lead"}' } });
          this.emit("notification", { method: "turn/completed", params: { turn: { status: "completed" } } });
        });
      return yield* providerSync(() => decoder(method === "thread/start" ? { thread: { id: "routing" } } : {}));
    });
  }
  notify() {}
  respond() {}
  respondError() {}
}

describe("channel routing generation effort", () => {
  it("runs a routing turn at the lowest effort the model takes", () => {
    expect(generationEffort(model, "channel-routing")).toBe("low");
    expect(generationEffort({ ...model, supportedReasoningEfforts: ["high", "xhigh"] }, "channel-routing")).toBe(
      "high",
    );
  });

  it("keeps the default effort for summaries, profiles and a model with no effort setting", () => {
    expect(generationEffort(model, "channel-summary")).toBe("high");
    expect(generationEffort(model, "profile")).toBe("high");
    expect(generationEffort({ ...model, reasoningEffortConfigurable: false }, "channel-routing")).toBe("high");
  });

  it("sends the low effort with the thread and the turn of a routing generation", async () => {
    const client = new RecordingClient();
    const text = await Effect.runPromise(
      generateTextWithoutTools(client, model, "Select the members.", undefined, undefined, "channel-routing"),
    );
    expect(text).toBe('{"agentId":"lead"}');
    expect(client.efforts).toEqual([
      { method: "thread/start", effort: "low" },
      { method: "turn/start", effort: "low" },
    ]);
  });

  it("sends the lead's effort with a summary generation", async () => {
    const client = new RecordingClient();
    await Effect.runPromise(
      generateTextWithoutTools(client, model, "Summarize.", undefined, undefined, "channel-summary"),
    );
    expect(client.efforts).toEqual([
      { method: "thread/start", effort: "high" },
      { method: "turn/start", effort: "high" },
    ]);
  });
});
