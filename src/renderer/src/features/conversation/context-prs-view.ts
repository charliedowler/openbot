import type { ContextPullRequestsSnapshot } from "@openbot/contracts/context-prs";
import { useText } from "@openbot/ui/text";
import { createEffect, createMemo, createStore, onSettled, untrack } from "solid-js";

export function createContextPrsView(deps: {
  target: () => { scope: { kind: "agent" | "channel"; id: string }; serverId: string; authorization: string } | null;
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
  const key = createMemo(() => {
    const target = deps.target();
    return target ? JSON.stringify([target.serverId, target.scope, target.authorization]) : null;
  });
  let request = 0;
  let disposed = false;
  async function refresh(explicit = false): Promise<void> {
    const expected = untrack(key);
    if (!expected || !untrack(deps.supported) || disposed) return;
    const reading = ++request;
    setState((state) => {
      state.loading = true;
      state.error = null;
    });
    try {
      const data = await deps.read(explicit);
      if (disposed || reading !== request || untrack(key) !== expected) return;
      setState((state) => {
        state.data = data;
        state.key = expected;
        state.loading = false;
      });
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
        setState((state) => {
          state.data = null;
          state.key = null;
          state.error = null;
          state.loading = false;
        });
      }
      if (target.key && target.supported) void refresh();
    },
  );
  onSettled(() => () => {
    disposed = true;
    request += 1;
  });
  return {
    state,
    data: () => (state.key === key() && key() !== null ? state.data : null),
    refresh: () => void refresh(true),
  };
}
