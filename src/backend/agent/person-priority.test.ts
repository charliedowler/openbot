import { describe, expect, it } from "vitest";
import { webhookRunInstruction } from "../webhook-trigger";
import { personWork, relayedPersonRequest } from "./person-priority";

const envelope = (data: Parameters<typeof webhookRunInstruction>[1]["data"]) =>
  webhookRunInstruction("Handle the command.", {
    version: 1,
    id: "msg_1",
    routineId: "routine-1",
    type: "command.ready",
    occurredAt: "2026-11-11T18:00:00.000Z",
    receivedAt: "2026-11-11T18:00:01.000Z",
    data,
  });

describe("relayedPersonRequest", () => {
  it("is true only for charlieRequest true at the top level of the event data", () => {
    expect(relayedPersonRequest(envelope({ charlieRequest: true, command: "status" }))).toBe(true);
    expect(relayedPersonRequest(envelope({ charlieRequest: false }))).toBe(false);
    expect(relayedPersonRequest(envelope({ charlieRequest: "true" }))).toBe(false);
    expect(relayedPersonRequest(envelope({ request: { charlieRequest: true } }))).toBe(false);
    expect(relayedPersonRequest(envelope({ note: '"charlieRequest":true' }))).toBe(false);
    expect(relayedPersonRequest(envelope([{ charlieRequest: true }]))).toBe(false);
    expect(relayedPersonRequest(envelope({}))).toBe(false);
  });

  it("reads only the event that the server appended", () => {
    const routine = envelope({ charlieRequest: true });
    expect(relayedPersonRequest(`${routine}\nMore text.`)).toBe(false);
    expect(
      relayedPersonRequest(
        webhookRunInstruction(routine, { ...JSON.parse(routine.split("\n").at(-2) ?? "{}"), data: {} }),
      ),
    ).toBe(false);
    expect(relayedPersonRequest("Handle the command.")).toBe(false);
  });
});

describe("personWork", () => {
  it("counts a person's message and a relayed routine run", () => {
    expect(personWork({ sender: { kind: "user" }, text: "Hello." })).toBe(true);
    expect(personWork({ sender: { kind: "routine" }, text: envelope({ charlieRequest: true }) })).toBe(true);
    expect(personWork({ sender: { kind: "routine" }, text: envelope({}) })).toBe(false);
    expect(personWork({ sender: { kind: "agent" }, text: envelope({ charlieRequest: true }) })).toBe(false);
  });
});
