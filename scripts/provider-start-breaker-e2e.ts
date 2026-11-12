import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { chmod, mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Effect } from "effect";
import { ConversationRuntime } from "../src/backend/agent/conversation-runtime";
import { ProviderRuntime } from "../src/backend/agent/provider-runtime";
import { type AgentClient, AgentProcessExitError } from "../src/backend/agent-client";
import { AgentStore } from "../src/backend/agent-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import type { RequestId, ResponseDecoder, RpcError } from "../src/backend/protocol";
import { providerSync } from "../src/backend/provider-client-effects";
import { BUILT_IN_PROVIDER_DRIVERS, NO_PROVIDER_CREDENTIALS } from "../src/backend/provider-drivers";

const OUT = resolve(import.meta.dirname, "../.openbot-build/provider-start-breaker-e2e");
const root = await mkdtemp(join(tmpdir(), "openbot-provider-start-breaker-"));
const executable = join(root, "claude.cjs");
await writeFile(executable, `#!${process.execPath}\nprocess.stdout.write("2.1.318 (Claude Code)\\n");\n`);
await chmod(executable, 0o755);
const previousExecutable = process.env.OPENBOT_CLAUDE_PATH;
process.env.OPENBOT_CLAUDE_PATH = executable;
const checks: { name: string; passed: boolean; error?: string }[] = [];
let failInitialize = false;
let created = 0;
let stopped = 0;

class FixtureClient extends EventEmitter implements AgentClient {
  readonly provider = "claude";
  running = false;
  start(): void {
    this.running = true;
    created += 1;
  }
  stop() {
    return Effect.sync(() => {
      this.running = false;
      stopped += 1;
    });
  }
  request<T>(method: string, _params: unknown, decoder: ResponseDecoder<T>) {
    return providerSync(() => {
      if (method === "initialize" && failInitialize)
        throw new AgentProcessExitError("Fixture process failed to start.");
      const value =
        method === "account/read"
          ? { account: { type: "claude", email: "fixture@example.invalid" } }
          : method === "model/list"
            ? { data: [{ model: "claude-sonnet-5-5", displayName: "Sonnet" }] }
            : {};
      return decoder(value);
    });
  }
  notify(_method: string, _params?: unknown): void {}
  respond(_id: RequestId, _result: unknown): void {}
  respondError(_id: RequestId, _error: RpcError): void {}
  fail(error: Error): void {
    this.running = false;
    this.emit("exit", error);
  }
}

async function check(name: string, operation: () => Promise<void>): Promise<void> {
  try {
    await operation();
    checks.push({ name, passed: true });
  } catch (error) {
    checks.push({ name, passed: false, error: String(error) });
  }
}

async function scenario(
  name: string,
  run: (runtime: ProviderRuntime, current: () => FixtureClient) => Promise<void>,
): Promise<void> {
  const directory = join(root, name);
  const database = new OpenBotDatabase(directory);
  const agents = new AgentStore(directory, join(root, "home"), database);
  let current: FixtureClient | undefined;
  const runtime = new ProviderRuntime({
    offProviders: BUILT_IN_PROVIDER_DRIVERS.map((driver) => driver.id).filter((id) => id !== "claude"),
    conversation: new ConversationRuntime(
      agents,
      () => undefined,
      () => agents.list(),
    ),
    hooks: {
      bindClient: () => undefined,
      onProvidersReady: () => Effect.void,
      onProviderLost: () => undefined,
      onClientStopped: () => Effect.void,
      isStopping: () => false,
      isProviderBusy: () => false,
      isAgentBusy: () => false,
      isProviderAssigned: () => true,
      onAgentClientLost: () => Effect.void,
      sharedRoot: () => join(root, "shared"),
      onProviderResumed: () => undefined,
      onProviderActivated: () => Effect.void,
      captureConfigRevision: () => 0,
    },
    emit: () => undefined,
    emitError: () => undefined,
    requestTimeoutMs: 5_000,
    preferredProvider: "claude",
    bundledExecutables: {},
    credentials: NO_PROVIDER_CREDENTIALS,
    redactMcp: (text) => text,
    clientFactory: () => {
      current = new FixtureClient();
      return current;
    },
  });
  try {
    await run(runtime, () => {
      assert.ok(current);
      return current;
    });
  } finally {
    failInitialize = false;
    for (const client of await runCauseEffect(runtime.dispose())) await runCauseEffect(client.stop());
  }
}

try {
  for (const [name, message] of [
    ["missing-resume", "Claude Code returned an error result: No conversation found with session ID: fixture-session"],
    ["model-refusal", "Claude Code returned an error result: Model refused this request."],
  ] as const) {
    await check(`${name} never stops the provider across five short failures`, async () =>
      scenario(name, async (runtime, current) => {
        await runCauseEffect(runtime.ensureProvider("claude"));
        for (let attempt = 0; attempt < 5; attempt += 1) {
          current().fail(new Error(message));
          await runCauseEffect(runtime.ensureProvider("claude"));
          assert.equal(runtime.status().providers?.find((row) => row.id === "claude")?.state, "available");
        }
      }),
    );
  }
  await check("three early process exits still open the start breaker", async () =>
    scenario("process-exit", async (runtime, current) => {
      await runCauseEffect(runtime.ensureProvider("claude"));
      for (let attempt = 0; attempt < 3; attempt += 1) {
        current().fail(new AgentProcessExitError("Fixture process exited with code 1."));
        if (attempt < 2) await runCauseEffect(runtime.ensureProvider("claude"));
      }
      const attempts = created;
      await assert.rejects(runCauseEffect(runtime.ensureProvider("claude")), /did not start several times/);
      assert.equal(created, attempts);
    }),
  );
  await check("three initialization failures still open the start breaker and manual refresh clears it", async () =>
    scenario("startup-failure", async (runtime) => {
      failInitialize = true;
      for (let attempt = 0; attempt < 3; attempt += 1)
        await assert.rejects(runCauseEffect(runtime.ensureProvider("claude")));
      const attempts = created;
      await assert.rejects(runCauseEffect(runtime.ensureProvider("claude")), /did not start several times/);
      assert.equal(created, attempts);
      failInitialize = false;
      await runCauseEffect(runtime.refreshProviders({ automatic: false }));
      await runCauseEffect(runtime.ensureProvider("claude"));
      assert.equal(runtime.status().providers?.find((row) => row.id === "claude")?.state, "available");
    }),
  );
} finally {
  if (previousExecutable === undefined) delete process.env.OPENBOT_CLAUDE_PATH;
  else process.env.OPENBOT_CLAUDE_PATH = previousExecutable;
}
const report = { passed: checks.every((entry) => entry.passed), checks, created, stopped };
await mkdir(OUT, { recursive: true });
await writeFile(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (!report.passed) process.exitCode = 1;
