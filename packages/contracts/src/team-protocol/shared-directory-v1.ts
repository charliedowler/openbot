import {
  adminRoute,
  boolean,
  count,
  fields,
  list,
  nullable,
  type OptionalRouteCodec,
  oneOf,
  string,
} from "./admin-wire";

export const SHARED_DIRECTORY_CAPABILITY = "shared-directory-v1";

export const SHARED_DIRECTORY_ROUTES = {
  list: "/v1/shared-directory",
} as const;

const path = string(4_096);

export const SHARED_DIRECTORY_CODECS: ReadonlyMap<string, OptionalRouteCodec> = new Map([
  [
    SHARED_DIRECTORY_ROUTES.list,
    adminRoute(
      fields({ path }),
      fields({
        name: string(1_024),
        path,
        root: path,
        parentPath: nullable(path),
        entries: list(
          fields({
            name: string(1_024),
            path,
            kind: oneOf("file", "directory"),
            size: count,
            modifiedAt: count,
          }),
          500,
        ),
        truncated: boolean,
      }),
    ),
  ],
]);
