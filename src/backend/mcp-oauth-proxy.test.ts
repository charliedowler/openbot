// A provider keeps a pool of idle connections to the proxy and writes its next request into one of
// them. Codex 0.166 keeps a connection for 90 s and does not notice a close from the proxy side, so
// a connection the proxy ended after Node's five quiet seconds made the next tool call fail with
// "error sending request". The proxy therefore leaves the end of an idle connection to the provider.
import { once } from "node:events";
import { createServer, type Server } from "node:http";
import { connect, type Socket } from "node:net";
import { Effect } from "effect";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { McpOAuthProxy, mcpOAuthProxyRoute } from "./mcp-oauth-proxy";

/** Node ends an idle keep-alive connection after this long when a server does not say otherwise. */
const NODE_DEFAULT_KEEP_ALIVE_MS = 5_000;
/** Node's timer runs once a second, so the close arrives up to a second after the timeout. */
const IDLE_MARGIN_MS = 1_500;

interface RawResponse {
  status: number;
  headers: Map<string, string>;
}

function listen(server: Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") reject(new Error("The server has no port."));
      else resolve(address.port);
    });
  });
}

/** One JSON-RPC POST on an open socket, read until the chunked body ends. */
function exchange(socket: Socket, path: string, bearer: string, host: string): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    let received = "";
    const body = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} });
    const onData = (chunk: Buffer) => {
      received += chunk.toString("latin1");
      if (!received.endsWith("\r\n0\r\n\r\n")) return;
      cleanup();
      const head = received.slice(0, received.indexOf("\r\n\r\n"));
      const [statusLine = "", ...headerLines] = head.split("\r\n");
      const headers = new Map<string, string>();
      for (const line of headerLines) {
        const colon = line.indexOf(":");
        headers.set(line.slice(0, colon).toLowerCase(), line.slice(colon + 1).trim());
      }
      resolve({ status: Number(statusLine.split(" ")[1]), headers });
    };
    const onClose = () => {
      cleanup();
      reject(new Error(`The proxy closed the connection after ${received.length} bytes.`));
    };
    const cleanup = () => {
      socket.off("data", onData);
      socket.off("close", onClose);
      socket.off("error", onClose);
    };
    socket.on("data", onData);
    socket.once("close", onClose);
    socket.once("error", onClose);
    socket.write(
      `POST ${path} HTTP/1.1\r\nHost: ${host}\r\nAuthorization: Bearer ${bearer}\r\n` +
        `Content-Type: application/json\r\nAccept: application/json, text/event-stream\r\n` +
        `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`,
    );
  });
}

describe("McpOAuthProxy", () => {
  let upstream: Server;
  let upstreamUrl: string;
  let proxy: McpOAuthProxy;

  beforeEach(async () => {
    upstream = createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ jsonrpc: "2.0", id: 1, result: { tools: [] } }));
    });
    const port = await listen(upstream);
    upstreamUrl = `http://127.0.0.1:${port}/mcp`;
    proxy = new McpOAuthProxy({
      settings: null,
      target: () => Effect.succeed({ url: upstreamUrl, headers: [], token: "upstream-token" }),
      refresh: () => Effect.succeed(null),
    });
  });

  afterEach(async () => {
    await Effect.runPromise(proxy.close());
    upstream.closeAllConnections();
    await new Promise((resolve) => upstream.close(() => resolve(null)));
  });

  it("keeps an idle provider connection open past Node's default keep-alive timeout", async () => {
    const route = await Effect.runPromise(proxy.route(mcpOAuthProxyRoute("mcp-1", upstreamUrl)));
    const { host, hostname, port, pathname } = new URL(route.url);
    const socket = connect(Number(port), hostname);
    await once(socket, "connect");
    try {
      const first = await exchange(socket, pathname, route.bearer, host);
      expect(first.status).toBe(200);

      await new Promise((resolve) => setTimeout(resolve, NODE_DEFAULT_KEEP_ALIVE_MS + IDLE_MARGIN_MS));
      expect(socket.destroyed).toBe(false);

      const second = await exchange(socket, pathname, route.bearer, host);
      expect(second.status).toBe(200);
      expect(second.headers.get("keep-alive")).toBeUndefined();
    } finally {
      socket.destroy();
    }
  }, 15_000);
});
