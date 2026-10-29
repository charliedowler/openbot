import {
  adminRoute,
  boolean,
  count,
  fields,
  identifier,
  list,
  nullable,
  type OptionalRouteCodec,
  oneOf,
  string,
} from "./admin-wire";
export const CONTEXT_PRS_CAPABILITY = "context-prs-v1";
export const CONTEXT_PRS_ROUTES = { read: "/v1/context-prs/read" } as const;
const scope = fields({ kind: oneOf("agent", "channel"), id: identifier });
const pr = fields({
  url: string(2048),
  repo: string(201),
  number: count,
  title: string(1024),
  state: oneOf("open", "merged"),
  draft: boolean,
  mergedAt: nullable(string(40)),
  headSha: string(40),
  checks: oneOf("passed", "pending", "failed", "unknown"),
  checkedAt: string(40),
});
export const CONTEXT_PRS_CODECS: ReadonlyMap<string, OptionalRouteCodec> = new Map([
  [
    CONTEXT_PRS_ROUTES.read,
    adminRoute(
      fields({ scope }, { refresh: boolean }),
      fields({
        scope,
        status: oneOf("ready", "not-connected", "unsupported"),
        checkedAt: string(40),
        open: list(pr, 200),
        merged: list(pr, 3),
        failures: list(
          fields({
            url: string(2048),
            reason: oneOf("not-connected", "not-found", "forbidden", "unavailable", "invalid-response", "timeout"),
          }),
          200,
        ),
        referencesLimited: boolean,
      }),
    ),
  ],
]);
