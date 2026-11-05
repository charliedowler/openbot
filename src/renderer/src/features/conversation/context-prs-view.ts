import type { ContextPullRequestsSnapshot } from "@openbot/contracts/context-prs";
import { useText } from "@openbot/ui/text";
import { createEffect, createMemo, createStore, onSettled, untrack } from "solid-js";

const SNAPSHOT_LIMIT = 50;
const OPEN_REFRESH_MS = 30_000;
const STALE_REFRESH_MS = 3_000;
const STALE_ATTEMPTS = 10;
const snapshots = new Map<string, ContextPullRequestsSnapshot>();
const reads = new Map<string, Promise<ContextPullRequestsSnapshot>>();

export interface ContextPrTarget {
  scope: { kind: "agent" | "channel"; id: string };
  serverId: string;
  authorization: string;
}

export type ContextPrCacheTarget = Pick<ContextPrTarget, "scope" | "serverId">;

export function contextPrKey(target: ContextPrTarget | null): string | null {
  return target ? JSON.stringify([target.serverId, target.scope, target.authorization]) : null;
}

export function contextPrCacheKey(target: ContextPrCacheTarget | null): string | null {
  return target ? JSON.stringify([target.serverId, target.scope.kind, target.scope.id]) : null;
}

export function cachedContextPrs(key: string | null): ContextPullRequestsSnapshot | null {
  return key === null ? null : (snapshots.get(key) ?? null);
}

function remember(key: string, snapshot: ContextPullRequestsSnapshot): void {
  snapshots.delete(key);
  if (snapshots.size >= SNAPSHOT_LIMIT) snapshots.delete(snapshots.keys().next().value ?? "");
  snapshots.set(key, snapshot);
}

export function readContextPrs(
  key: string,
  read: (refresh: boolean) => Promise<ContextPullRequestsSnapshot>,
  refresh = false,
): Promise<ContextPullRequestsSnapshot> {
  const pending = refresh ? undefined : reads.get(key);
  if (pending) return pending;
  const request = Promise.resolve()
    .then(() => read(refresh))
    .then(
      (snapshot) => {
        if (reads.get(key) === request) reads.delete(key);
        remember(key, snapshot);
        return snapshot;
      },
      (error: unknown) => {
        if (reads.get(key) === request) reads.delete(key);
        throw error;
      },
    );
  reads.set(key, request);
  return request;
}

export function prefetchContextPrs(deps: {
  target: () => ContextPrCacheTarget | null;
  supported: () => boolean;
  read: (refresh: boolean) => Promise<ContextPullRequestsSnapshot>;
}): void {
  const key = createMemo(() => (deps.supported() ? contextPrCacheKey(deps.target()) : null));
  createEffect(key, (current) => {
    if (current !== null) void readContextPrs(current, deps.read).catch(() => {});
  });
}

export function createContextPrsView(deps: {
  target: () => ContextPrTarget | null;
  revision: () => string;
  supported: () => boolean;
  read: (refresh: boolean) => Promise<ContextPullRequestsSnapshot>;
}) {
  const { t, errorMessage } = useText();
  const [state, setState] = createStore<{
    loading: boolean;
    error: string | null;
    data: ContextPullRequestsSnapshot | null;
    key: string | null;
  }>({ loading: false, error: null, data: null, key: null });
  const key = createMemo(() => contextPrKey(deps.target()));
  const cacheKey = createMemo(() => contextPrCacheKey(deps.target()));
  let request = 0;
  let disposed = false;
  let staleAttempts = 0;
  let staleTimer: ReturnType<typeof setTimeout> | null = null;
  function scheduleStale(data: ContextPullRequestsSnapshot): void {
    if (staleTimer) clearTimeout(staleTimer);
    staleTimer = null;
    if (!data.stale) {
      staleAttempts = 0;
      return;
    }
    if (staleAttempts >= STALE_ATTEMPTS) return;
    staleAttempts += 1;
    staleTimer = setTimeout(() => {
      staleTimer = null;
      void refresh();
    }, STALE_REFRESH_MS);
  }
  async function refresh(explicit = false): Promise<void> {
    const expected = untrack(key);
    const cached = untrack(cacheKey);
    if (!expected || !cached || !untrack(deps.supported) || disposed) return;
    const reading = ++request;
    setState((state) => {
      state.loading = true;
      state.error = null;
    });
    try {
      const data = await readContextPrs(cached, deps.read, explicit);
      if (disposed || reading !== request || untrack(key) !== expected) return;
      setState((state) => {
        state.data = data;
        state.key = expected;
        state.loading = false;
      });
      scheduleStale(data);
    } catch (error) {
      if (disposed || reading !== request || untrack(key) !== expected) return;
      setState((state) => {
        state.error = errorMessage(error, t("conversation.pullRequests.failed"));
        state.loading = false;
      });
    }
  }
  createEffect(
    () => ({ key: key(), revision: deps.revision(), supported: deps.supported() }),
    (target, previous) => {
      request += 1;
      if (target.key !== previous?.key || !target.supported) {
        staleAttempts = 0;
        const cached = target.supported ? cachedContextPrs(untrack(cacheKey)) : null;
        setState((state) => {
          state.data = cached;
          state.key = cached ? target.key : null;
          state.error = null;
          state.loading = false;
        });
      }
      if (target.key && target.supported) void refresh();
    },
  );
  const timer = setInterval(() => void refresh(), OPEN_REFRESH_MS);
  onSettled(() => () => {
    disposed = true;
    request += 1;
    clearInterval(timer);
    if (staleTimer) clearTimeout(staleTimer);
  });
  const data = () => (state.key === key() && key() !== null ? state.data : cachedContextPrs(cacheKey()));
  return {
    state,
    data,
    updating: () => data() !== null && (state.loading || data()?.stale === true),
    loading: () => data() === null && state.loading,
    refresh: () => {
      staleAttempts = 0;
      void refresh(true);
    },
  };
}
