import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { decodeWorkspaceDirectory } from "@openbot/contracts/ipc";
import {
  SHARED_DIRECTORY_CAPABILITY,
  SHARED_DIRECTORY_ROUTES,
} from "@openbot/contracts/team-protocol/shared-directory-v1";
import { Effect } from "effect";
import { runCauseEffect } from "../src/backend/effect-boundary";
import {
  listSharedDirectory,
  listWorkspaceDirectory,
  resolveSharedFile,
  resolveWorkspaceFile,
} from "../src/backend/workspace-paths";
import { HttpError } from "../src/main/team-api/http-error";
import { routeSharedDirectory } from "../src/main/team-api/route-shared-directory";

const root = await mkdtemp(join(tmpdir(), "openbot-shared-directory-"));
const shared = join(root, "Shared");
const folder = join(shared, "evidence");
await mkdir(join(folder, "nested"), { recursive: true });
await writeFile(join(folder, "README.md"), "Shared evidence\n");
await writeFile(join(folder, "space %20 ü.png"), new Uint8Array([137, 80, 78, 71, 0, 255]));
await writeFile(join(root, "private.txt"), "Private fixture");
await symlink(join(root, "private.txt"), join(folder, "outside-link"));
await symlink(root, join(folder, "outside-folder"));
await symlink(join(folder, "README.md"), join(folder, "inside-link"));
let serviceCalls = 0;
const server = createServer(async (request, response) => {
  try {
    const outcome = await routeSharedDirectory(
      {
        request,
        response,
        method: request.method ?? "GET",
        url: new URL(request.url ?? "/", "http://localhost"),
        protocol: 1,
        capabilities: new Set(request.headers["x-fixture-capability"] === "yes" ? [SHARED_DIRECTORY_CAPABILITY] : []),
        member: {
          id: "fixture",
          username: "fixture",
          email: null,
          name: null,
          role: "member",
          createdAt: "",
          disabled: false,
        },
        token: "fixture",
        sessionId: "fixture",
        sessionExpiresAt: "",
        agentLevel: () => "manage",
        json: (status, value) => {
          response.writeHead(status, { "Content-Type": "application/json" });
          response.end(JSON.stringify(value));
          return "handled";
        },
        empty: (status) => {
          response.writeHead(status);
          response.end();
          return "handled";
        },
      },
      {
        listSharedDirectory: (path) => {
          serviceCalls += 1;
          return Effect.promise(() => runCauseEffect(listSharedDirectory(shared, path)));
        },
      },
    );
    if (outcome === "unmatched") {
      response.writeHead(404);
      response.end();
    }
  } catch (error) {
    response.writeHead(error instanceof HttpError ? error.status : 500);
    response.end(error instanceof Error ? error.message : "Request failed");
  }
});
server.listen(0, "127.0.0.1");
await once(server, "listening");
const address = server.address();
assert.ok(address && typeof address !== "string");
const endpoint = `http://127.0.0.1:${address.port}${SHARED_DIRECTORY_ROUTES.list}`;
const list = (path: string, capability = true) =>
  fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-fixture-capability": capability ? "yes" : "no" },
    body: JSON.stringify({ path }),
  });
const report = resolve(".openbot-build/shared-directory-e2e/report.json");
await mkdir(dirname(report), { recursive: true });
try {
  const refused = await list("Shared/evidence", false);
  assert.equal(refused.status, 400);
  await refused.arrayBuffer();
  assert.equal(serviceCalls, 0);
  const response = await list("Shared/evidence/");
  assert.equal(response.status, 200);
  const directory = decodeWorkspaceDirectory(await response.json());
  assert.equal(directory.name, "evidence");
  assert.equal(directory.path, "Shared/evidence");
  assert.equal(directory.root, "Shared/");
  assert.equal(directory.parentPath, "Shared/");
  assert.equal(directory.entries[0]?.kind, "directory");
  assert.ok(directory.entries.some((entry) => entry.name === "inside-link"));
  assert.equal(
    directory.entries.some((entry) => entry.name.startsWith("outside-")),
    false,
  );
  const workspaceAgent = { id: "fixture", workspacePath: shared };
  const workspace = await runCauseEffect(listWorkspaceDirectory(workspaceAgent, "./evidence"));
  const workspaceImage = workspace.entries.find((entry) => entry.name === "space %20 ü.png");
  assert.ok(workspaceImage);
  assert.match(workspaceImage.path, /%2520/);
  const workspaceFile = await runCauseEffect(resolveWorkspaceFile(workspaceAgent, workspaceImage.path));
  assert.deepEqual(new Uint8Array(await readFile(workspaceFile.path)), new Uint8Array([137, 80, 78, 71, 0, 255]));
  const image = directory.entries.find((entry) => entry.name === "space %20 ü.png");
  assert.ok(image);
  const file = await runCauseEffect(resolveSharedFile(shared, image.path));
  assert.deepEqual(new Uint8Array(await readFile(file.path)), new Uint8Array([137, 80, 78, 71, 0, 255]));
  const nested = await list("Shared/evidence/nested");
  const nestedDirectory = decodeWorkspaceDirectory(await nested.json());
  assert.equal(nestedDirectory.parentPath, directory.path);
  assert.deepEqual(nestedDirectory.entries, []);
  const top = await list(directory.parentPath);
  assert.equal(decodeWorkspaceDirectory(await top.json()).parentPath, null);
  const notDirectory = await list(image.path);
  assert.equal(notDirectory.status, 400);
  await notDirectory.arrayBuffer();
  for (const path of [
    "../private.txt",
    join(root, "private.txt"),
    "Shared/evidence/outside-folder",
    "Shared/missing",
  ]) {
    const denied = await list(path);
    assert.equal(denied.status, 404, path);
    const text = await denied.text();
    assert.doesNotMatch(text, /Private fixture/);
    assert.equal(text.includes(root), false);
  }
  const crowded = join(shared, "crowded");
  await mkdir(crowded);
  for (let index = 0; index < 501; index += 1) await writeFile(join(crowded, String(index)), "");
  const limited = decodeWorkspaceDirectory(await (await list("Shared/crowded")).json());
  assert.equal(limited.entries.length, 500);
  assert.equal(limited.truncated, true);
  const wrongMethod = await fetch(endpoint);
  assert.equal(wrongMethod.status, 404);
  await wrongMethod.arrayBuffer();
  await writeFile(
    report,
    JSON.stringify(
      {
        passed: true,
        checks: [
          "capability gate",
          "unchanged workspace path encoding",
          "folder entries",
          "nested navigation",
          "root boundary",
          "empty folder",
          "image bytes and special names",
          "internal link",
          "outside links hidden",
          "outside and missing refusal",
          "regular-file fallback",
          "500 entry bound",
          "wrong method",
        ],
      },
      null,
      2,
    ),
  );
  process.stdout.write(`Shared directory E2E passed: ${report}\n`);
} catch (error) {
  await writeFile(report, JSON.stringify({ passed: false, error: String(error) }, null, 2));
  throw error;
} finally {
  server.closeAllConnections();
  await new Promise<void>((done) => server.close(() => done()));
  await rm(root, { recursive: true, force: true });
}
