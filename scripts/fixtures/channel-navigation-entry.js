import { createRoot, flush } from "solid-js";
import { createChannelsController } from "../../src/renderer/src/features/channels/channels-controller";

Object.defineProperty(globalThis, "window", { value: new EventTarget(), configurable: true });
const report = { passed: false, checks: [] };
function check(value, name) {
  if (!value) throw new Error(name);
  report.checks.push(name);
}
function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
async function until(condition, name) {
  for (let i = 0; i < 100; i++) {
    await Promise.resolve();
    flush();
    if (condition()) return;
  }
  throw new Error(name);
}
function fixture() {
  let supported = false;
  let dispose;
  const delayed = new Map();
  const reads = [];
  const channels = ["a", "b", "c"].map((id) => ({ id, name: id, members: [], archived: false }));
  const page = (id) => ({
    channel: channels.find((row) => row.id === id),
    messages: [],
    tasks: [],
    olderCursor: null,
    throughSequence: 0,
  });
  const controller = createRoot((stop) => {
    dispose = stop;
    return createChannelsController({
      port: () => ({
        agent: {
          listChannels: async () => channels,
          readChannel: async ({ channelId }) => {
            reads.push(channelId);
            const pending = delayed.get(channelId);
            if (pending) {
              delayed.delete(channelId);
              return pending.promise;
            }
            return page(channelId);
          },
        },
      }),
      agents: () => [],
      scopeKey: () => "fixture",
      readSelection: () => null,
      writeSelection() {},
      supported: () => supported,
      deletionSupported: () => true,
      beforeOpen() {},
      canMarkRead: () => false,
    });
  });
  flush();
  supported = true;
  return { controller, delayed, reads, page, dispose };
}
async function testLateReply(fails) {
  const f = fixture();
  const stale = deferred();
  let refresh;
  let navigation;
  try {
    await f.controller.open("a");
    check(f.controller.state.page?.channel.id === "a", "Initial channel loads");
    f.delayed.set("a", stale);
    const count = f.reads.length;
    refresh = f.controller.refresh();
    await until(() => f.reads.length > count, "The old channel read starts");
    navigation = f.controller.open("b");
    await until(
      () => f.controller.state.page?.channel.id === "b" && !f.controller.state.loading,
      "New channel is blocked by the old channel read",
    );
    check(true, fails ? "Navigation bypasses a failing old read" : "Navigation bypasses a pending old read");
    if (fails) stale.reject(new Error("Old channel is unavailable"));
    else stale.resolve(f.page("a"));
    await Promise.all([refresh, navigation]);
    flush();
    check(f.controller.state.page?.channel.id === "b", "A late reply cannot replace the selected channel");
    check(f.controller.state.error === null, "A superseded read cannot show an error on the selected channel");
  } finally {
    stale.resolve(f.page("a"));
    await Promise.allSettled([refresh, navigation].filter(Boolean));
    f.dispose();
  }
}
async function testRapidNavigation() {
  const f = fixture();
  const first = deferred();
  const second = deferred();
  let openingFirst;
  let openingSecond;
  let openingLast;
  try {
    f.delayed.set("a", first);
    f.delayed.set("b", second);
    openingFirst = f.controller.open("a");
    await until(() => f.reads.includes("a"), "First navigation starts");
    openingSecond = f.controller.open("b");
    await until(() => f.reads.includes("b"), "Second navigation is blocked by the first");
    openingLast = f.controller.open("c");
    await until(() => f.controller.state.page?.channel.id === "c", "Latest navigation is blocked by earlier requests");
    check(!f.controller.state.loading, "The latest navigation clears Loading while older reads remain pending");
    second.resolve(f.page("b"));
    first.resolve(f.page("a"));
    await Promise.all([openingFirst, openingSecond, openingLast]);
    check(
      f.controller.state.selectedId === "c" && f.controller.state.page?.channel.id === "c",
      "Out-of-order replies preserve the latest navigation",
    );
  } finally {
    first.resolve(f.page("a"));
    second.resolve(f.page("b"));
    await Promise.allSettled([openingFirst, openingSecond, openingLast].filter(Boolean));
    f.dispose();
  }
}
try {
  await testLateReply(false);
  await testLateReply(true);
  await testRapidNavigation();
  report.passed = true;
} catch (error) {
  report.error = error instanceof Error ? error.message : String(error);
  process.exitCode = 1;
}
process.stdout.write(JSON.stringify(report));
