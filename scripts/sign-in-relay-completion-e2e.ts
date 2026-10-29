import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdir, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { resolve } from "node:path";
import type { McpTestResult } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { SignInRelayHost } from "../src/main/sign-in-relay-host";

const OUT = resolve(import.meta.dirname, "../.openbot-build/sign-in-relay-completion-e2e");
const report: { passed: boolean; cases: string[] } = { passed: false, cases: [] };
const unavailable = (): never => {
  throw new Error("This scenario uses an existing CLI sign-in.");
};

async function scenario(
  name: string,
  location: (origin: string, otherOrigin: string) => string | null,
  completes: boolean,
) {
  let result: McpTestResult | null = null;
  let cancelled = false;
  let otherRequests = 0;
  const otherListener = createServer((_request, response) => {
    otherRequests++;
    response.writeHead(200).end();
  });
  otherListener.listen(0, "127.0.0.1");
  await once(otherListener, "listening");
  const otherAddress = otherListener.address();
  assert.ok(otherAddress && typeof otherAddress !== "string");
  const otherOrigin = `http://127.0.0.1:${otherAddress.port}`;
  const requests: { path: string; query: string }[] = [];
  const listener = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    requests.push({ path: url.pathname, query: url.search });
    if (url.pathname === "/auth/callback") {
      if (url.searchParams.has("error")) {
        response.writeHead(400).end();
        return;
      }
      const target = location(origin, otherOrigin);
      if (target === null) {
        result = { toolCount: 0, error: null };
        response.writeHead(200).end();
      } else response.writeHead(302, { Location: target }).end();
    } else if (url.pathname === "/success") {
      result = { toolCount: 0, error: null };
      response.writeHead(200).end();
    } else response.writeHead(500).end();
  });
  listener.listen(0, "127.0.0.1");
  await once(listener, "listening");
  const address = listener.address();
  assert.ok(address && typeof address !== "string");
  const origin = `http://127.0.0.1:${address.port}`;
  const page = new URL("https://auth.example/authorize");
  page.searchParams.set("state", "fixture-state");
  page.searchParams.set("redirect_uri", `${origin}/auth/callback`);
  const host = new SignInRelayHost({
    mcp: { startRelay: unavailable, relayStatus: unavailable, cancel: unavailable },
    providers: { startRelay: unavailable, relayStatus: unavailable, cancel: unavailable },
    accounts: { startRelay: unavailable, relayStatus: unavailable, cancelSignIn: unavailable },
    receiveMcpGrant: () => false,
    printedLink: () => false,
  });
  const { flowId } = host.startReplay("member", {
    key: name,
    status: () => ({ pageUrl: page.toString(), result }),
    cancel: () =>
      Effect.sync(() => {
        cancelled = true;
      }),
  });
  try {
    await Effect.runPromise(host.complete("member", flowId, "code=fixture-grant&state=fixture-state"));
    assert.equal(host.status("member", flowId).result !== null, completes, name);
    assert.equal(requests.filter((request) => request.path === "/auth/callback").length, 1, name);
    const expectedRequests = completes && location(origin, otherOrigin) !== null ? 2 : 1;
    assert.equal(requests.length, expectedRequests, name);
    assert.equal(otherRequests, 0, `${name}: another listener receives nothing`);
    assert.ok(
      requests.every((request) => request.path === "/auth/callback" || request.path === "/success"),
      name,
    );
    assert.ok(
      requests.filter((request) => request.path === "/success").every((request) => request.query === ""),
      name,
    );
    const count = requests.length;
    await assert.rejects(Effect.runPromise(host.complete("member", flowId, "code=second&state=fixture-state")));
    assert.equal(requests.length, count, `${name}: no grant replay`);
    await Effect.runPromise(host.cancel("member", flowId));
    assert.equal(cancelled, !completes, `${name}: cancellation`);
    result = null;
    cancelled = false;
    const refused = host.startReplay("member", {
      key: `${name}-refused`,
      status: () => ({ pageUrl: page.toString(), result }),
      cancel: () =>
        Effect.sync(() => {
          cancelled = true;
        }),
    });
    await Effect.runPromise(host.complete("member", refused.flowId, "error=access_denied&state=fixture-state"));
    assert.equal(cancelled, true, `${name}: refusal cancels`);
    assert.equal(requests.length, count + 1, `${name}: refusal sends one callback`);
    assert.equal(requests.at(-1)?.path, "/auth/callback", `${name}: refusal does not complete`);
    report.cases.push(name);
  } finally {
    listener.closeAllConnections();
    listener.close();
    await once(listener, "close");
    otherListener.closeAllConnections();
    otherListener.close();
    await once(otherListener, "close");
  }
}

try {
  await scenario("relative success", () => "/success?id_token=fixture-token", true);
  await scenario("absolute success", (origin) => `${origin}/success?id_token=fixture-token`, true);
  await scenario("direct response", () => null, true);
  await scenario("different port", (_origin, otherOrigin) => `${otherOrigin}/success`, false);
  await scenario("different host", (origin) => `${origin.replace("127.0.0.1", "localhost")}/success`, false);
  await scenario("different scheme", (origin) => `${origin.replace("http:", "https:")}/success`, false);
  await scenario("different path", () => "/other?code=fixture-grant", false);
  await scenario("URL credentials", (origin) => `${origin.replace("http://", "http://user:pass@")}/success`, false);
  report.passed = true;
  process.stdout.write(`${JSON.stringify(report)}\n`);
} finally {
  await mkdir(OUT, { recursive: true });
  await writeFile(resolve(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
}
