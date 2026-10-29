import { execFile } from "node:child_process";
import { type ContextPullRequest, contextPullRequestIdentity } from "@openbot/contracts/context-prs";
import { type DynamicRecord, isDynamicRecord } from "@openbot/contracts/runtime-values";
import { redactText } from "@openbot/logging";
import { Effect, Schema } from "effect";
import { causeHelpers } from "../backend/effect-boundary";

export class ContextPrFailure extends Schema.TaggedError<ContextPrFailure>()("ContextPrFailure", {
  reason: Schema.Literals(["not-connected", "not-found", "forbidden", "unavailable", "invalid-response", "timeout"]),
}) {}
export class ContextPrIoFailure extends Schema.TaggedError<ContextPrIoFailure>()("ContextPrIoFailure", {
  cause: Schema.Defect(),
}) {}
const { io } = causeHelpers(ContextPrIoFailure);
export type PrIdentity = NonNullable<ReturnType<typeof contextPullRequestIdentity>>;
export function contextPrChecks(value: unknown): ContextPullRequest["checks"] {
  if (!Array.isArray(value) || !value.length) return "unknown";
  let pending = false;
  for (const raw of value) {
    if (!isDynamicRecord(raw)) return "unknown";
    const conclusion = String(raw.conclusion ?? raw.state ?? "").toLowerCase();
    if (["failure", "error", "timed_out", "cancelled", "action_required", "startup_failure"].includes(conclusion))
      return "failed";
    if (!conclusion || ["pending", "expected", "queued", "in_progress", "waiting", "requested"].includes(conclusion))
      pending = true;
    else if (!["success", "neutral", "skipped"].includes(conclusion)) return "unknown";
  }
  return pending ? "pending" : "passed";
}
export function decodeContextPr(raw: unknown, identity: PrIdentity, checkedAt: string): ContextPullRequest | null {
  if (!isDynamicRecord(raw)) throw new Error("Invalid PR response.");
  const url = contextPullRequestIdentity(raw.url ?? raw.html_url);
  const headSha = raw.headRefOid ?? (isDynamicRecord(raw.head) ? raw.head.sha : undefined);
  const title = raw.title;
  const mergedAt = raw.mergedAt ?? raw.merged_at ?? null;
  const state = String(raw.state).toLowerCase();
  const draft = raw.isDraft ?? raw.draft;
  if (
    !url ||
    url.url.toLowerCase() !== identity.url.toLowerCase() ||
    (raw.number !== undefined && raw.number !== identity.number) ||
    typeof title !== "string" ||
    !title.length ||
    title.length > 1024 ||
    typeof draft !== "boolean" ||
    typeof headSha !== "string" ||
    !/^[a-f0-9]{40}$/.test(headSha) ||
    !["open", "closed", "merged"].includes(state) ||
    (mergedAt !== null &&
      (typeof mergedAt !== "string" ||
        !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(mergedAt) ||
        !Number.isFinite(Date.parse(mergedAt)) ||
        Date.parse(mergedAt) > Date.now() + 300_000)) ||
    (state === "open" && mergedAt !== null) ||
    (typeof raw.merged === "boolean" && raw.merged !== (mergedAt !== null))
  )
    throw new Error("Invalid PR response.");
  if (state === "closed" && !mergedAt) return null;
  if (state === "merged" && !mergedAt) throw new Error("Invalid merge time.");
  return {
    ...identity,
    title: redactText(title).slice(0, 1024),
    state: mergedAt ? "merged" : "open",
    draft,
    mergedAt: mergedAt ? new Date(String(mergedAt)).toISOString() : null,
    headSha,
    checks: contextPrChecks(raw.statusCheckRollup),
    checkedAt,
  };
}
async function boundedJson(response: Response): Promise<DynamicRecord> {
  if (!response.body) throw new Error("Empty PR response.");
  const reader = response.body.getReader(),
    chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const item = await reader.read();
      if (item.done) break;
      size += item.value.byteLength;
      if (size > 2 * 1024 * 1024) throw new Error("PR response exceeds its limit.");
      chunks.push(item.value);
    }
  } finally {
    await reader.cancel();
  }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.length;
  }
  const value = JSON.parse(new TextDecoder().decode(body));
  if (!isDynamicRecord(value)) throw new Error("Invalid PR response.");
  return value;
}
export const readConnectedContextPr = Effect.fn("ContextPr.readConnected")(function* (
  identity: PrIdentity,
  token: string,
  fetcher: (url: string, init: RequestInit) => Promise<Response>,
  requestSignal?: AbortSignal,
) {
  const signal = AbortSignal.any([AbortSignal.timeout(15_000), ...(requestSignal ? [requestSignal] : [])]);
  const get = (path: string) =>
    io(async (operationSignal) => {
      const response = await fetcher(`https://api.github.com/repos/${identity.repo}/${path}`, {
        method: "GET",
        redirect: "manual",
        signal: AbortSignal.any([signal, operationSignal]),
        headers: {
          Accept: "application/vnd.github+json",
          Authorization: `Bearer ${token}`,
          "X-GitHub-Api-Version": "2022-11-28",
        },
      });
      if (!response.ok) {
        await response.body?.cancel();
        return { status: response.status, value: null };
      }
      return { status: response.status, value: await boundedJson(response) };
    });
  const result = yield* get(`pulls/${identity.number}`).pipe(
    Effect.catch((error) =>
      Effect.fail(
        new ContextPrFailure({
          reason:
            error.cause instanceof ContextPrFailure ? error.cause.reason : signal.aborted ? "timeout" : "unavailable",
        }),
      ),
    ),
  );
  if (result.status !== 200)
    return yield* new ContextPrFailure({
      reason:
        result.status === 401
          ? "not-connected"
          : result.status === 403
            ? "forbidden"
            : result.status === 404
              ? "not-found"
              : "unavailable",
    });
  const pr = yield* Effect.try({
    try: () => decodeContextPr(result.value, identity, new Date().toISOString()),
    catch: () => new ContextPrFailure({ reason: "invalid-response" }),
  });
  if (!pr) return null;
  const checkData = yield* get(`commits/${pr.headSha}/check-runs?per_page=100`).pipe(
    Effect.catch(() => Effect.succeed(null)),
  );
  const statusData = yield* get(`commits/${pr.headSha}/status?per_page=100`).pipe(
    Effect.catch(() => Effect.succeed(null)),
  );
  if (
    checkData?.status !== 200 ||
    !isDynamicRecord(checkData.value) ||
    !Array.isArray(checkData.value.check_runs) ||
    !Number.isSafeInteger(checkData.value.total_count) ||
    Number(checkData.value.total_count) < 0 ||
    Number(checkData.value.total_count) > 100 ||
    statusData?.status !== 200 ||
    !isDynamicRecord(statusData.value) ||
    !Array.isArray(statusData.value.statuses) ||
    !Number.isSafeInteger(statusData.value.total_count) ||
    Number(statusData.value.total_count) < 0 ||
    Number(statusData.value.total_count) > 100
  )
    return pr;
  return { ...pr, checks: contextPrChecks([...checkData.value.check_runs, ...statusData.value.statuses]) };
});
export type ContextPrCli = (args: string[], signal: AbortSignal) => Promise<string>;
export const runContextPrCli = (
  args: string[],
  signal: AbortSignal,
  environment: NodeJS.ProcessEnv = process.env,
): Promise<string> =>
  new Promise((resolve, reject) => {
    const child = execFile(
      "gh",
      args,
      {
        shell: false,
        signal,
        timeout: 15_000,
        maxBuffer: 2 * 1024 * 1024,
        windowsHide: true,
        env: {
          ...environment,
          GH_HOST: "github.com",
          GH_PROMPT_DISABLED: "1",
          GIT_TERMINAL_PROMPT: "0",
          GH_PAGER: "cat",
        },
      },
      (error, stdout) => {
        if (error)
          reject(
            new ContextPrFailure({
              reason: signal.aborted || error.killed ? "timeout" : error.code === 4 ? "not-connected" : "unavailable",
            }),
          );
        else resolve(stdout);
      },
    );
    child.stdin?.end();
  });
export const readCliContextPr = Effect.fn("ContextPr.readCli")(function* (
  identity: PrIdentity,
  run: ContextPrCli = runContextPrCli,
  requestSignal?: AbortSignal,
) {
  const signal = AbortSignal.any([AbortSignal.timeout(15_000), ...(requestSignal ? [requestSignal] : [])]);
  const raw = yield* io((operationSignal) =>
    run(
      [
        "pr",
        "view",
        String(identity.number),
        "--repo",
        identity.repo,
        "--json",
        "title,state,isDraft,mergedAt,url,headRefOid,statusCheckRollup",
      ],
      AbortSignal.any([signal, operationSignal]),
    ),
  ).pipe(
    Effect.catch((error) =>
      Effect.fail(
        new ContextPrFailure({
          reason:
            error.cause instanceof ContextPrFailure ? error.cause.reason : signal.aborted ? "timeout" : "unavailable",
        }),
      ),
    ),
  );
  return yield* Effect.try({
    try: () => decodeContextPr(JSON.parse(raw), identity, new Date().toISOString()),
    catch: () => new ContextPrFailure({ reason: "invalid-response" }),
  });
});
