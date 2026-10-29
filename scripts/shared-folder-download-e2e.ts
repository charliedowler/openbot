import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdir, mkdtemp, open, realpath, rm, stat, symlink, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { ATTACHMENT_LIMITS } from "@openbot/contracts/input-limits";
import { TEAM_API_ROUTES } from "@openbot/contracts/team-api-routes";
import { Effect } from "effect";
import { unzipSync } from "fflate";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { resolveSharedFile } from "../src/backend/workspace-paths";
import { HttpError } from "../src/main/team-api/http-error";
import { routeFiles } from "../src/main/team-api/route-files";
import { withSharedFolderArchive } from "../src/main/team-api/shared-folder-archive";

export async function runSharedFolderDownloadE2e() {
  const root = await mkdtemp(join(tmpdir(), "openbot-folder-fixture-"));
  const shared = join(root, "Shared");
  const folder = join(shared, "evidence");
  await mkdir(join(folder, "nested", "empty"), { recursive: true });
  const canonicalFolder = await realpath(folder);
  await writeFile(join(folder, "README.md"), "Evidence\n");
  await writeFile(join(folder, "nested", "image.png"), new Uint8Array([137, 80, 78, 71, 0, 255]));
  await writeFile(join(folder, "empty.txt"), "");
  const cancellationSettled = Promise.withResolvers<void>();
  const abort = new AbortController();
  const server = createServer(async (request, response) => {
    if (request.headers["x-cancel-fixture"]) abort.abort(new DOMException("Cancelled", "AbortError"));
    try {
      const outcome = await routeFiles(
        {
          request,
          response,
          method: request.method ?? "GET",
          url: new URL(request.url ?? "/", "http://localhost"),
          protocol: 1,
          capabilities: new Set(),
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
            response.writeHead(status);
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
          agents: {
            resolveSharedFile: (path, options) =>
              Effect.promise(() => runCauseEffect(resolveSharedFile(shared, path, options))),
            resolveWorkspaceFile: () => Effect.die("Unused fixture operation"),
            prepareImportedAttachments: () => Effect.die("Unused fixture operation"),
            discardDraftAttachment: () => Effect.void,
          },
          mailbox: { resolveAttachment: () => Effect.succeed(null) },
        },
      );
      if (outcome === "unmatched") {
        response.writeHead(404);
        response.end();
      }
    } catch (error) {
      if (!response.destroyed) {
        if (!response.headersSent) response.writeHead(error instanceof HttpError ? error.status : 500);
        response.end(error instanceof Error ? error.message : "Request failed.");
      }
    } finally {
      if (request.headers["x-cancel-fixture"]) cancellationSettled.resolve();
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const get = (path: string) =>
    fetch(`http://127.0.0.1:${address.port}${TEAM_API_ROUTES.sharedFiles}?path=${encodeURIComponent(path)}`);
  try {
    const normal = await get("evidence/README.md");
    assert.equal(normal.status, 200);
    assert.equal(await normal.text(), "Evidence\n");
    const response = await get("evidence/");
    const bytes = new Uint8Array(await response.arrayBuffer());
    assert.equal(response.status, 200, `Folder link fails: ${new TextDecoder().decode(bytes)}`);
    assert.match(response.headers.get("content-disposition") ?? "", /evidence\.zip/);
    assert.equal(response.headers.get("content-type"), "application/zip");
    assert.equal(Number(response.headers.get("content-length")), bytes.byteLength);
    const entries = unzipSync(bytes);
    assert.equal(new TextDecoder().decode(entries["evidence/README.md"]), "Evidence\n");
    assert.deepEqual(entries["evidence/nested/image.png"], new Uint8Array([137, 80, 78, 71, 0, 255]));
    assert.equal(entries["evidence/empty.txt"]?.length, 0);
    assert.ok(entries["evidence/nested/empty/"]);
    await mkdir(join(shared, "empty"));
    const empty = await get("empty");
    assert.equal(empty.status, 200);
    assert.ok(unzipSync(new Uint8Array(await empty.arrayBuffer()))["empty/"]);
    await writeFile(join(root, "private.txt"), "Must not leave the host");
    await symlink(join(root, "private.txt"), join(folder, "linked.txt"));
    const linked = await get("evidence");
    assert.equal(linked.status, 400);
    assert.doesNotMatch(await linked.text(), /Must not leave the host/);
    await rm(join(folder, "linked.txt"));
    await symlink(root, join(folder, "linked-directory"));
    const linkedDirectory = await get("evidence");
    assert.equal(linkedDirectory.status, 400);
    await linkedDirectory.arrayBuffer();
    await rm(join(folder, "linked-directory"));
    const oversized = await open(join(folder, "large.bin"), "w");
    await oversized.truncate(ATTACHMENT_LIMITS.fileBytes + 1);
    await oversized.close();
    const large = await get("evidence");
    assert.equal(large.status, 413);
    await large.arrayBuffer();
    await rm(join(folder, "large.bin"));
    await mkdir(join(shared, "deep", ...Array.from({ length: 65 }, () => "nested")), { recursive: true });
    const deep = await get("deep");
    assert.equal(deep.status, 413);
    await deep.arrayBuffer();
    const crowded = join(shared, "crowded");
    await mkdir(crowded);
    for (let index = 0; index < 10_000; index += 1) await writeFile(join(crowded, String(index)), "");
    const tooMany = await get("crowded");
    assert.equal(tooMany.status, 413);
    await tooMany.arrayBuffer();
    await assert.rejects(
      fetch(`http://127.0.0.1:${address.port}${TEAM_API_ROUTES.sharedFiles}?path=evidence`, {
        signal: abort.signal,
        headers: { "x-cancel-fixture": "true" },
      }),
    );
    assert.equal(abort.signal.aborted, true);
    await cancellationSettled.promise;
    await assert.rejects(
      runCauseEffect(
        withSharedFolderArchive(canonicalFolder, abort.signal, async () =>
          assert.fail("Cancelled archive was delivered"),
        ),
      ),
      { name: "AbortError" },
    );
    let archivePath = "";
    await runCauseEffect(
      withSharedFolderArchive(canonicalFolder, new AbortController().signal, async (path) => {
        archivePath = path;
        assert.equal((await stat(path)).isFile(), true);
      }),
    );
    await assert.rejects(stat(dirname(archivePath)), { code: "ENOENT" });
    await assert.rejects(
      runCauseEffect(
        withSharedFolderArchive(canonicalFolder, new AbortController().signal, async (path) => {
          archivePath = path;
          throw new Error("Disconnected client fixture");
        }),
      ),
      /Disconnected client fixture/,
    );
    await assert.rejects(stat(dirname(archivePath)), { code: "ENOENT" });
    return {
      status: "pass",
      checks: [
        "regular-file",
        "nested-folder",
        "binary-bytes",
        "empty-file",
        "empty-directory",
        "empty-root",
        "file-symlink-refused",
        "directory-symlink-refused",
        "size-limit",
        "depth-limit",
        "entry-count-limit",
        "cancelled-request-cleanup",
        "temporary-cleanup",
      ],
    };
  } finally {
    server.closeAllConnections();
    await new Promise<void>((done) => server.close(() => done()));
    await rm(root, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  const report = resolve(".openbot-build/shared-folder-download-e2e/report.json");
  await mkdir(resolve(".openbot-build/shared-folder-download-e2e"), { recursive: true });
  try {
    const result = await runSharedFolderDownloadE2e();
    await writeFile(report, JSON.stringify(result, null, 2));
    process.stdout.write(`Shared folder download E2E passed: ${report}\n`);
  } catch (error) {
    await writeFile(report, JSON.stringify({ status: "fail", error: String(error) }, null, 2));
    throw error;
  }
}
