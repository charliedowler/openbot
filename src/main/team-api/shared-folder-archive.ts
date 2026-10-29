import { constants } from "node:fs";
import { lstat, mkdtemp, open, opendir, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, relative, sep } from "node:path";
import { ATTACHMENT_LIMITS } from "@openbot/contracts/input-limits";
import { sourceText } from "@openbot/i18n/source";
import { Effect } from "effect";
import { Zip, ZipPassThrough } from "fflate";
import { remoteCall } from "../remote-service-effects";
import { HttpError } from "./http-error";

const MAX_ENTRIES = 10_000;
const MAX_DEPTH = 64;
const CHUNK_BYTES = 64 * 1024;

function unsupportedEntry(): HttpError {
  return new HttpError(400, sourceText("error.team.sharedFolderUnsupportedEntry"));
}

function tooLarge(): HttpError {
  return new HttpError(413, sourceText("error.team.sharedFileTooLarge"));
}

async function writeArchive(directory: string, target: string, signal: AbortSignal): Promise<void> {
  const output = await open(target, "wx", 0o600);
  let pending = Promise.resolve();
  let archiveError: Error | null = null;
  let archiveBytes = 0;
  let sourceBytes = 0;
  let entries = 0;
  const archive = new Zip((error, chunk) => {
    if (error) archiveError = error;
    else
      pending = pending.then(async () => {
        signal.throwIfAborted();
        archiveBytes += chunk.byteLength;
        if (archiveBytes > ATTACHMENT_LIMITS.fileBytes) throw tooLarge();
        await output.writeFile(chunk);
      });
  });
  const flush = async () => {
    await pending;
    if (archiveError) throw archiveError;
  };
  const visit = async (path: string, depth: number): Promise<void> => {
    signal.throwIfAborted();
    entries += 1;
    if (entries > MAX_ENTRIES || depth > MAX_DEPTH) {
      throw new HttpError(413, sourceText("error.team.sharedFolderTooManyEntries"));
    }
    const metadata = await lstat(path);
    if (metadata.isSymbolicLink() || (!metadata.isFile() && !metadata.isDirectory())) throw unsupportedEntry();
    if ((await realpath(path)) !== path) throw unsupportedEntry();
    const relativePath = relative(directory, path);
    if ([basename(directory), ...relativePath.split(sep)].some((part) => part.includes("\\"))) throw unsupportedEntry();
    const name = [basename(directory), ...relativePath.split(sep).filter(Boolean)].join("/");
    if (metadata.isDirectory()) {
      const entry = new ZipPassThrough(`${name}/`);
      archive.add(entry);
      entry.push(new Uint8Array(), true);
      await flush();
      for await (const child of await opendir(path)) await visit(join(path, child.name), depth + 1);
      return;
    }
    sourceBytes += metadata.size;
    if (sourceBytes > ATTACHMENT_LIMITS.fileBytes) throw tooLarge();
    const input = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
      const opened = await input.stat();
      if (
        !opened.isFile() ||
        opened.dev !== metadata.dev ||
        opened.ino !== metadata.ino ||
        (await realpath(path)) !== path
      ) {
        throw unsupportedEntry();
      }
      const entry = new ZipPassThrough(name);
      archive.add(entry);
      await flush();
      let position = 0;
      while (position < metadata.size) {
        signal.throwIfAborted();
        const buffer = new Uint8Array(Math.min(CHUNK_BYTES, metadata.size - position));
        const { bytesRead } = await input.read(buffer, 0, buffer.byteLength, position);
        if (bytesRead === 0) throw new HttpError(409, sourceText("error.team.sharedFolderChanged"));
        entry.push(buffer.subarray(0, bytesRead), false);
        await flush();
        position += bytesRead;
      }
      const after = await input.stat();
      if (after.size !== metadata.size || after.mtimeMs !== metadata.mtimeMs) {
        throw new HttpError(409, sourceText("error.team.sharedFolderChanged"));
      }
      entry.push(new Uint8Array(), true);
      await flush();
    } finally {
      await input.close();
    }
  };
  try {
    await visit(directory, 0);
    archive.end();
    await flush();
  } finally {
    archive.terminate();
    await pending.catch(() => undefined);
    await output.close();
  }
}

export const withSharedFolderArchive = Effect.fn("SharedFolderArchive.download")(function* <A>(
  directory: string,
  signal: AbortSignal,
  use: (path: string, name: string) => Promise<A>,
) {
  return yield* Effect.acquireUseRelease(
    remoteCall(() => mkdtemp(join(tmpdir(), "openbot-shared-zip-"))),
    (temporary) =>
      Effect.gen(function* () {
        const path = join(temporary, "download.zip");
        yield* remoteCall(() => writeArchive(directory, path, signal));
        return yield* remoteCall(() => {
          signal.throwIfAborted();
          return use(path, `${basename(directory)}.zip`);
        });
      }),
    (temporary) => remoteCall(() => rm(temporary, { recursive: true, force: true })).pipe(Effect.orDie),
  );
});
