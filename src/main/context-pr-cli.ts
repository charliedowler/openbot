import { homedir } from "node:os";
import { delimiter, isAbsolute, join } from "node:path";
import { Effect } from "effect";
import { agentLaunchPath } from "../backend/cli";
import { type ContextPrCli, type PrIdentity, readCliContextPr, runContextPrCli } from "./context-pr-reader";
export function contextPrCliWithEnvironment(
  environment: () => NodeJS.ProcessEnv,
  bins: () => readonly string[],
  launchPath = agentLaunchPath,
) {
  return Effect.fn("ContextPr.readRuntimeCli")(function* (
    identity: PrIdentity,
    _run?: ContextPrCli,
    signal?: AbortSignal,
  ) {
    const inherited = environment();
    const launch = yield* launchPath();
    const fallback =
      process.platform === "win32"
        ? [join(process.env.ProgramFiles ?? "C:\\Program Files", "GitHub CLI")]
        : [join(homedir(), ".local", "bin"), "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin"];
    const directories = [
      ...new Set(
        [...(launch ?? "").split(delimiter), ...(inherited.PATH ?? "").split(delimiter), ...bins(), ...fallback].filter(
          (path) => isAbsolute(path),
        ),
      ),
    ];
    return yield* readCliContextPr(
      identity,
      (args, requestSignal) =>
        runContextPrCli(args, requestSignal, { ...inherited, PATH: directories.join(delimiter) }),
      signal,
    );
  });
}
