import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { sourceText } from "@openbot/i18n/source";
import { registerSecretValue } from "@openbot/logging";
import { Option, Schema } from "effect";

export const CODEX_PROXY_FILE = "openbot-proxy.json";

const CodexProxySettings = Schema.Struct({
  baseUrl: Schema.String.check(Schema.isPattern(/^https?:\/\/\S+$/u)),
  apiKeyFile: Schema.NonEmptyString,
});

export interface CodexProxy {
  readonly baseUrl: string;
  readonly apiKey: string;
}

export function codexHome(environment: Readonly<Record<string, string | undefined>>): string {
  return environment.CODEX_HOME || process.env.CODEX_HOME || join(homedir(), ".codex");
}

/**
 * Null when the home folder has no proxy file. A file that OpenBot cannot use stops the start: a
 * silent fallback would send the agents to the own sign-in that the proxy replaces.
 */
export function readCodexProxy(home: string): CodexProxy | null {
  const path = join(home, CODEX_PROXY_FILE);
  const invalid = () => new Error(sourceText("error.provider.codexProxyInvalid", { path }));
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch (cause) {
    if (typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ENOENT") return null;
    throw invalid();
  }
  const settings = Schema.decodeUnknownOption(Schema.fromJsonString(CodexProxySettings))(text);
  if (Option.isNone(settings)) throw invalid();
  const { apiKeyFile, baseUrl } = settings.value;
  let apiKey: string;
  try {
    apiKey = readFileSync(isAbsolute(apiKeyFile) ? apiKeyFile : resolve(home, apiKeyFile), "utf8").trim();
  } catch {
    throw invalid();
  }
  if (!apiKey || /\s/u.test(apiKey)) throw invalid();
  registerSecretValue(apiKey);
  return { baseUrl: baseUrl.replace(/\/+$/u, ""), apiKey };
}

/**
 * `openai_base_url` moves only the address of the built-in `openai` provider, so the saved chats that
 * Codex binds to that provider ID continue. The ephemeral store keeps the key in memory: Codex then
 * neither reads nor writes `auth.json`, and its ChatGPT sign-in stays as it is.
 */
export function codexProxyArgs(proxy: CodexProxy): string[] {
  return ["-c", 'cli_auth_credentials_store="ephemeral"', "-c", `openai_base_url=${JSON.stringify(proxy.baseUrl)}`];
}

/**
 * The proxy moves a request to another account when one is limited, so it answers 429 only when no
 * account is left ("All credentials for model … are cooling down"). Codex keeps only the status.
 */
export function isProxyLimitError(codexErrorInfo: unknown): boolean {
  if (typeof codexErrorInfo !== "object" || codexErrorInfo === null) return false;
  return Object.values(codexErrorInfo).some(
    (detail) =>
      typeof detail === "object" && detail !== null && "httpStatusCode" in detail && detail.httpStatusCode === 429,
  );
}
