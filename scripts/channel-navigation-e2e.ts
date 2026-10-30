import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { type BunPlugin, build } from "bun";

const out = resolve(import.meta.dirname, "../.openbot-build/channel-navigation-e2e");
mkdirSync(out, { recursive: true });
const adapters: BunPlugin = {
  name: "channel-navigation-adapters",
  setup(builder) {
    builder.onLoad({ filter: /src\/renderer\/src\/action-toast\.ts$/ }, () => ({
      contents: "export const actionToast = { success() {}, warning() {}, error() {} };",
      loader: "js",
    }));
    builder.onLoad({ filter: /src\/renderer\/src\/analytics\.ts$/ }, () => ({
      contents: "export function desktopAnalytics() {}",
      loader: "js",
    }));
    builder.onLoad({ filter: /packages\/ui\/src\/text\.tsx$/ }, () => ({
      contents:
        "export function currentText() { return { errorDetails(error) { return { message: String(error), reference: null }; }, t(key) { return key; } }; }",
      loader: "js",
    }));
  },
};
const bundled = await build({
  entrypoints: [resolve(import.meta.dirname, "fixtures/channel-navigation-entry.js")],
  target: "browser",
  format: "esm",
  plugins: [adapters],
  define: { "process.env.NODE_ENV": JSON.stringify("production") },
});
assert(bundled.success, bundled.logs.map(String).join("\n"));
const entry = resolve(out, "fixture.mjs");
const bundle = bundled.outputs[0];
assert(bundle, "The navigation fixture has no output");
writeFileSync(entry, await bundle.text());
const result = spawnSync(process.execPath, [entry], { encoding: "utf8", timeout: 15_000 });
const report = JSON.parse(result.stdout.trim());
writeFileSync(resolve(out, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
process.stdout.write(`${JSON.stringify(report)}\n`);
assert.equal(result.status, 0, result.stderr || report.error);
