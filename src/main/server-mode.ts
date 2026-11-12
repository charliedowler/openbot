/**
 * A self-hosted server: the packaged Linux build that `install-server.sh` installs on a computer
 * with no screen. systemd starts it with `OPENBOT_SERVER=1` under a virtual display, and it is a
 * normal Remote host after its owner signs in.
 *
 * Nobody can use the window, so the `openbot` terminal command (`scripts/hosting/openbot`) talks to
 * this process over a Unix socket in the runtime directory that systemd makes for the service user
 * (mode 0700). Only that user and root can connect. Agents run as the same user and already have
 * full access to its files, so the socket gives them nothing new. The socket answers a closed list
 * of requests: status, the email-code sign-in, the sign-in with an operator's one-time claim, the
 * server name, sign-out, a Mobile Connect code, the end of the usage limits that hold a provider's
 * agents, the Stop of an agent's running turn, an agent's model and backup models, each agent's
 * queue, the agent routines and where an agent routine runs. It never sends the session token, the sign-in code
 * or the claim back. The Mobile Connect code signs one phone
 * in to the account of the server; the account service lets it work for one sign-in, for two minutes.
 *
 * The protocol is HTTP with form bodies and `key=value` text lines, so the command needs only
 * `curl --unix-socket`.
 */

import { chmod, lstat, unlink } from "node:fs/promises";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { Socket } from "node:net";
import { dirname, isAbsolute, join } from "node:path";
import { parseHostedServerName } from "@openbot/contracts/hosted-servers";
import { INPUT_LIMITS } from "@openbot/contracts/input-limits";
import {
  type AgentSummary,
  type CentralAuthState,
  type EventRoutine,
  isAgentProvider,
  isProviderAccountId,
  ROUTINE_RUN_PLACES,
  type RoutineRunPlace,
  type SaveEventRoutineInput,
} from "@openbot/contracts/ipc";
import { sourceText } from "@openbot/i18n/source";
import { Deferred, Effect } from "effect";
import QRCode from "qrcode";
import type { AgentService } from "../backend/agent-service";
import { runCauseEffect } from "../backend/effect-boundary";
import type { CentralAuthManager } from "./central-auth-manager";
import { CpuProfileBusy, type CpuProfiler, cpuProfileRequest } from "./cpu-profile";
import type { HostEventsService } from "./host-events-service";
import type { HostService } from "./host-service";
import { readBodyWithin } from "./http-body";
import { createHostedMobileConnect } from "./mobile-connect-host";
import { RemoteWorkflowError, remoteCall, toRemoteWorkflowError } from "./remote-service-effects";
import type { ServerMemory } from "./server-memory";

const CONTROL_SOCKET_FILE = "control.sock";
const MAX_BODY_BYTES = 4096;
const MAX_EMAIL_LENGTH = 254;
/** The name of a server whose owner signed in without one. The same default as a mobile connect. */
const DEFAULT_SERVER_NAME = "OpenBot";

export interface ServerModeEnvironment {
  controlSocketPath: string;
}

/** Returns null unless this process is a self-hosted server that systemd started. */
export function takeServerModeEnvironment(
  environment: NodeJS.ProcessEnv,
  isPackaged: boolean,
  platform: NodeJS.Platform,
): ServerModeEnvironment | null {
  if (!isPackaged || platform !== "linux" || environment.OPENBOT_SERVER !== "1") return null;
  // A hosted server signs in with its claim, not from a terminal.
  if (environment.OPENBOT_HOSTED_SERVER === "1") return null;
  const runtimeDirectory = environment.XDG_RUNTIME_DIR?.trim() ?? "";
  if (!isAbsolute(runtimeDirectory)) return null;
  return { controlSocketPath: join(runtimeDirectory, CONTROL_SOCKET_FILE) };
}

export interface ServerModeOptions {
  environment: ServerModeEnvironment;
  version: string;
  centralAuth: Pick<
    CentralAuthManager,
    | "getState"
    | "requestEmailCode"
    | "cancelCompanySignIn"
    | "verifyEmailCode"
    | "canPersistSession"
    | "redeemServerClaim"
    | "logout"
    | "createMobileConnect"
  >;
  host: Pick<HostService, "getStatus" | "configure" | "start" | "updateIdentity" | "getMobileConnectHost">;
  /** The memory guard, so that `status` shows the memory ceiling and the turn limit. */
  memory:
    | (Pick<ServerMemory, "level" | "sample" | "turnLimit"> & Partial<Pick<ServerMemory, "turnBufferStatus">>)
    | null;
  nativeWork?: () => { idle: boolean; compactions: number; providerOperations: number; reasons: string[] };
  usageLimits?: Pick<AgentService, "releaseUsageLimits">;
  agents?: Pick<AgentService, "listAgents" | "interruptRunningTurn" | "updateAgentRuntime" | "agentQueues"> &
    Partial<Pick<AgentService, "controlAgentContext">>;
  routines?: Pick<HostEventsService, "listRoutines" | "saveRoutine">;
  channelIds?: () => readonly string[];
  cpuProfiler?: Pick<CpuProfiler, "take">;
  onError: (message: string, error: unknown) => void;
  /** Tests only. */
  uid?: number;
}

interface Answer {
  status: number;
  lines: Record<string, string | number | null>;
}

export class ServerMode {
  readonly #options: ServerModeOptions;
  readonly #sockets = new Set<Socket>();
  #server: Server | null = null;
  #pendingName: string | null = null;
  /** Why the last publish failed. Nobody reads the journal, so `status` shows it. */
  #publishError: string | null = null;
  #publishing: Deferred.Deferred<void> | null = null;

  constructor(options: ServerModeOptions) {
    this.#options = options;
  }

  /** Binds the control socket. It refuses a runtime directory that another user could enter. */
  readonly listen = Effect.fn("ServerMode.listen")(function* (this: ServerMode) {
    const path = this.#options.environment.controlSocketPath;
    const directory = yield* remoteCall(() => lstat(dirname(path)));
    const uid = this.#options.uid ?? process.getuid?.();
    if (!directory.isDirectory() || directory.uid !== uid || (directory.mode & 0o077) !== 0) {
      return yield* new RemoteWorkflowError({
        cause: new Error("The server runtime directory must be a private directory of the service user."),
      });
    }
    // A socket from the last run stays after a crash. Anything else at that path is not ours.
    const existing = yield* remoteCall(() =>
      lstat(path).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT") return null;
        throw error;
      }),
    );
    if (existing && !existing.isSocket()) {
      return yield* new RemoteWorkflowError({ cause: new Error("The control socket path holds another file.") });
    }
    if (existing) yield* remoteCall(() => unlink(path));

    const server = createServer((request, response) => void this.#handle(request, response));
    server.on("connection", (socket) => {
      this.#sockets.add(socket);
      socket.on("close", () => this.#sockets.delete(socket));
    });
    yield* remoteCall(
      () =>
        new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(path, () => {
            server.removeListener("error", reject);
            resolve();
          });
        }),
    );
    this.#server = server;
    yield* remoteCall(() => chmod(path, 0o600));
  }).bind(this);

  readonly close = Effect.fn("ServerMode.close")(function* (this: ServerMode) {
    const server = this.#server;
    this.#server = null;
    if (!server) return;
    for (const socket of this.#sockets) socket.destroy();
    this.#sockets.clear();
    yield* Effect.callback<void>((resume) => {
      server.close(() => resume(Effect.void));
    });
  }).bind(this);

  /**
   * Names and starts the host of the signed-in account. The entry point calls this after each
   * sign-in and at the start, and the start retry calls it after a failure. Calls run one at a time,
   * so two callers never name two hosts.
   */
  readonly publish = Effect.fn("ServerMode.publish")(function* (this: ServerMode) {
    const previous = this.#publishing;
    const completed = Deferred.makeUnsafe<void>();
    this.#publishing = completed;
    return yield* Effect.gen({ self: this }, function* () {
      if (previous) yield* Deferred.await(previous);
      this.#publishError = null;
      if (this.#options.centralAuth.getState().status !== "signed_in") return;
      const { host } = this.#options;
      const serverName = this.#pendingName;
      // Only the owner supplies a name; it also renames the account's existing host.
      if (!host.getStatus().configured) yield* host.configure({ serverName: serverName ?? DEFAULT_SERVER_NAME });
      else if (serverName && host.getStatus().serverName !== serverName) yield* host.updateIdentity({ serverName });
      this.#pendingName = null;
      yield* host.start();
    }).pipe(
      Effect.tapError(({ cause }) =>
        Effect.sync(() => {
          this.#publishError = cause instanceof Error ? cause.message : sourceText("error.host.publishFailed");
        }),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          if (this.#publishing === completed) this.#publishing = null;
          Deferred.doneUnsafe(completed, Effect.void);
        }),
      ),
    );
  }, Effect.uninterruptible).bind(this);

  async #handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    let answer: Answer;
    try {
      const body = await readBody(request);
      answer = body === null ? failure(413, "too_large") : await this.#route(request.method, request.url, body);
    } catch (error) {
      this.#options.onError("A server control request failed.", error);
      // The messages of the account and host services are the ones the app shows. They name no secret.
      answer = failure(400, "failed", error instanceof Error ? error.message : null);
    }
    response
      .writeHead(answer.status, { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" })
      .end(formatLines(answer.lines));
  }

  async #route(method: string | undefined, url: string | undefined, body: URLSearchParams): Promise<Answer> {
    const path = url?.split("?")[0] ?? "";
    const route = `${method ?? ""} ${path}`;
    switch (route) {
      case "GET /v1/status":
        return this.#status();
      case "POST /v1/login/start":
        return runCauseEffect(this.#startLogin(body.get("email") ?? "", body.get("name")));
      case "POST /v1/login/verify":
        return runCauseEffect(this.#verifyLogin(body.get("challenge") ?? "", body.get("code") ?? "", body.get("name")));
      case "POST /v1/login/claim":
        return runCauseEffect(this.#claimLogin(body.get("claim") ?? "", body.get("name")));
      case "POST /v1/name":
        return runCauseEffect(this.#rename(body.get("name") ?? ""));
      case "POST /v1/mobile-connect":
        return runCauseEffect(this.#mobileConnect());
      case "POST /v1/usage/reset":
        return this.#resetUsage(body.get("provider") ?? "", body.get("accountId"));
      case "POST /v1/turns/interrupt":
        return this.#interruptTurn(body);
      case "POST /v1/agents/compact":
        return this.#compactAgent(body);
      case "POST /v1/agents/update":
        return this.#updateAgent(body);
      case "GET /v1/agents/queue":
        return this.#agentQueue();
      case "GET /v1/routines":
        return this.#routines();
      case "POST /v1/routines/update":
        return this.#updateRoutine(body);
      case "POST /v1/diagnostics/cpu-profile":
        return this.#cpuProfile(body);
      case "POST /v1/logout":
        await runCauseEffect(
          this.#options.centralAuth.logout().pipe(Effect.mapError(({ cause }) => new RemoteWorkflowError({ cause }))),
        );
        return this.#status();
      default:
        return failure(404, "not_found");
    }
  }

  #status(): Answer {
    const account = this.#options.centralAuth.getState();
    const host = this.#options.host.getStatus();
    const work = this.#options.nativeWork?.();
    const authPending =
      account.status === "loading" || account.status === "signing_in" || account.status === "code_sent";
    const reasons = [
      ...(work?.reasons ?? []),
      ...(authPending ? ["server-auth"] : []),
      ...(this.#publishing ? ["server-publish"] : []),
    ];
    const native = work ? { ...work, idle: work.idle && reasons.length === 0, reasons } : null;
    return {
      status: 200,
      lines: {
        version: this.#options.version,
        nativeWorkVersion: native ? 1 : null,
        nativeIdle: native ? String(native.idle) : "unknown",
        nativeWork: native ? JSON.stringify(native) : "unavailable",
        account: account.status,
        email: account.status === "signed_in" ? account.user.email : null,
        account_message: account.status === "error" ? account.issue.message : null,
        server: host.phase,
        server_name: host.serverName ?? this.#pendingName,
        server_message: this.#publishError ?? host.message,
        ...this.#memoryLines(),
      },
    };
  }

  async #cpuProfile(body: URLSearchParams): Promise<Answer> {
    const profiler = this.#options.cpuProfiler;
    if (!profiler) return failure(404, "not_found");
    const request = cpuProfileRequest(body.get("seconds"), body.get("intervalUs"));
    if (!request) return failure(400, "invalid_profile");
    try {
      const result = await profiler.take(request.seconds, request.intervalUs);
      return {
        status: 200,
        lines: {
          profile: result.path,
          seconds: result.seconds,
          interval_us: result.intervalUs,
          samples: result.samples,
          bytes: result.bytes,
        },
      };
    } catch (error) {
      if (error instanceof CpuProfileBusy) return failure(409, "profile_running");
      throw error;
    }
  }

  async #resetUsage(provider: string, accountId: string | null): Promise<Answer> {
    const usageLimits = this.#options.usageLimits;
    if (!usageLimits) return failure(404, "not_found");
    if (!isAgentProvider(provider)) return failure(400, "invalid_provider");
    if (accountId !== null && !isProviderAccountId(accountId)) return failure(400, "invalid_account");
    const released = await runCauseEffect(usageLimits.releaseUsageLimits(provider, accountId));
    return {
      status: 200,
      lines: {
        released: released.length,
        limits: released.map((limit) => `${limit.provider}/${limit.accountId ?? "default"}/${limit.model}`).join(" "),
      },
    };
  }

  async #interruptTurn(body: URLSearchParams): Promise<Answer> {
    const agents = this.#options.agents;
    if (!agents) return failure(404, "not_found");
    const agentId = body.get("agentId")?.trim() ?? "";
    const turnId = body.get("turnId")?.trim() || null;
    if (!agentId || agentId.length > INPUT_LIMITS.identifier) return failure(400, "invalid_agent");
    if (turnId !== null && turnId.length > INPUT_LIMITS.identifier) return failure(400, "invalid_turn");
    const outcome = await runCauseEffect(agents.interruptRunningTurn(agentId, turnId));
    switch (outcome.kind) {
      case "unknown-agent":
        return failure(404, "unknown_agent");
      case "no-running-turn":
        return failure(409, "no_running_turn");
      case "turn-mismatch":
      case "turn-ambiguous":
        return {
          status: 409,
          lines: {
            error: outcome.kind === "turn-mismatch" ? "turn_mismatch" : "turn_ambiguous",
            running: outcome.running.map((turn) => turn.turnId).join(" "),
          },
        };
      case "stopped":
        return {
          status: 200,
          lines: {
            stopped: outcome.ended === false ? "false" : outcome.turn.turnId,
            turnId: outcome.turn.turnId,
            ended: String(outcome.ended ?? true),
            interruptSent: String(outcome.interruptSent ?? true),
            threadId: outcome.turn.threadId,
            ownChat: String(outcome.turn.ownChat),
          },
        };
    }
  }

  async #compactAgent(body: URLSearchParams): Promise<Answer> {
    const agents = this.#options.agents;
    if (!agents?.controlAgentContext) return failure(404, "not_found");
    for (const key of new Set(body.keys())) {
      if (key !== "agentId" && key !== "mode") return failure(400, "unknown_field", key);
      if (body.getAll(key).length !== 1) return failure(400, "repeated_field", key);
    }
    const agentId = body.get("agentId")?.trim() ?? "";
    const mode = body.get("mode") ?? "compact";
    if (!agentId || agentId.length > INPUT_LIMITS.identifier) return failure(400, "invalid_agent");
    if (mode !== "compact" && mode !== "rotate") return failure(400, "invalid_mode");
    const result = await runCauseEffect(agents.controlAgentContext(agentId, mode));
    if (result.kind === "unknown-agent") return failure(404, "unknown_agent");
    if (result.kind === "busy" || result.kind === "unavailable") return failure(409, result.kind);
    return { status: 202, lines: { accepted: mode, agentId, threadId: result.threadId } };
  }

  async #updateAgent(body: URLSearchParams): Promise<Answer> {
    const agents = this.#options.agents;
    if (!agents) return failure(404, "not_found");
    for (const key of new Set(body.keys())) {
      if (!AGENT_UPDATE_FIELDS.has(key)) return failure(400, "unknown_field", key);
      if (body.getAll(key).length > 1) return failure(400, "repeated_field", key);
    }
    const agentId = body.get("agentId")?.trim() ?? "";
    if (!agentId || agentId.length > INPUT_LIMITS.identifier) return failure(400, "invalid_agent");
    if (!agents.listAgents().some((agent) => agent.id === agentId)) return failure(404, "unknown_agent");
    const provider = body.get("provider");
    const model = body.get("model");
    const account = body.get("providerAccountId");
    const backupText = body.get("backupModels");
    let backupModels: unknown;
    try {
      backupModels = backupText === null ? undefined : JSON.parse(backupText);
    } catch {
      return failure(400, "invalid_backup_models");
    }
    const request = {
      agentId,
      ...Object.fromEntries(
        ["sessionTokenCap", "turnTokenBudget"]
          .filter((key) => body.has(key))
          .map((key) => [key, body.get(key) === "default" ? null : Number(body.get(key))]),
      ),
      ...(provider === null ? {} : { provider }),
      ...(model === null ? {} : { model }),
      ...(account === null ? {} : { providerAccountId: account === DEFAULT_ACCOUNT_FIELD ? null : account }),
      ...(backupText === null ? {} : { backupModels }),
    };
    let updated: AgentSummary;
    try {
      updated = await runCauseEffect(agents.updateAgentRuntime(request));
    } catch (error) {
      return failure(400, "invalid_update", error instanceof Error ? error.message : String(error));
    }
    return {
      status: 200,
      lines: {
        agentId: updated.id,
        provider: updated.provider,
        model: updated.model,
        providerAccountId: updated.providerAccountId ?? DEFAULT_ACCOUNT_FIELD,
        backupModels: JSON.stringify(updated.backupModels ?? null),
        sessionTokenCap: updated.sessionTokenCap ?? "default",
        turnTokenBudget: updated.turnTokenBudget ?? "default",
        pendingRuntime: updated.pendingRuntime ? JSON.stringify(updated.pendingRuntime) : null,
      },
    };
  }

  #agentQueue(): Answer {
    const agents = this.#options.agents;
    if (!agents) return failure(404, "not_found");
    const rows = agents.agentQueues();
    return {
      status: 200,
      lines: {
        agents: rows.length,
        ...this.#turnLines(),
        ...Object.fromEntries(rows.map(({ agentId, ...row }) => [`agent.${agentId}`, JSON.stringify(row)])),
      },
    };
  }

  async #agentRoutines(): Promise<EventRoutine[] | null> {
    const { agents, routines } = this.#options;
    if (!agents || !routines) return null;
    const listed: EventRoutine[] = [];
    for (const agent of agents.listAgents())
      listed.push(...(await runCauseEffect(routines.listRoutines({ owner: { kind: "agent", id: agent.id } }))));
    return listed;
  }

  async #routines(): Promise<Answer> {
    const listed = await this.#agentRoutines();
    if (!listed) return failure(404, "not_found");
    return {
      status: 200,
      lines: {
        routines: listed.length,
        ...Object.fromEntries(
          listed.map((routine) => [
            `routine.${routine.id}`,
            JSON.stringify({
              agentId: routine.owner.id,
              name: routine.name,
              runIn: routine.runIn ?? "chat",
              active: routine.active,
              trigger: routine.trigger.kind,
            }),
          ]),
        ),
      },
    };
  }

  async #updateRoutine(body: URLSearchParams): Promise<Answer> {
    const { routines, channelIds } = this.#options;
    const listed = await this.#agentRoutines();
    if (!routines || !listed) return failure(404, "not_found");
    for (const key of new Set(body.keys())) {
      if (!ROUTINE_UPDATE_FIELDS.has(key)) return failure(400, "unknown_field", key);
      if (body.getAll(key).length > 1) return failure(400, "repeated_field", key);
    }
    const routineId = body.get("routineId")?.trim() ?? "";
    if (!routineId || routineId.length > INPUT_LIMITS.identifier) return failure(400, "invalid_routine");
    const runIn = ROUTINE_RUN_PLACES.find((place) => place === body.get("runIn"));
    if (!runIn) return failure(400, "invalid_run_in");
    const routine = listed.find((item) => item.id === routineId);
    if (!routine) {
      for (const channelId of channelIds?.() ?? []) {
        const channelRoutines = await runCauseEffect(
          routines.listRoutines({ owner: { kind: "channel", id: channelId } }),
        );
        if (channelRoutines.some((item) => item.id === routineId))
          return { status: 409, lines: { error: "channel_routine", channelId } };
      }
      return failure(404, "unknown_routine");
    }
    let saved: EventRoutine;
    try {
      saved = (await runCauseEffect(routines.saveRoutine(runInUpdate(routine, runIn)))).routine;
    } catch (error) {
      return failure(400, "invalid_update", error instanceof Error ? error.message : String(error));
    }
    return {
      status: 200,
      lines: {
        routineId: saved.id,
        agentId: saved.owner.id,
        runIn: saved.runIn ?? "chat",
        active: String(saved.active),
      },
    };
  }

  #memoryLines(): Answer["lines"] {
    const memory = this.#options.memory;
    const sample = memory?.sample();
    if (!memory || !sample) return {};
    return {
      memory_level: memory.level(),
      memory_limit: sample.total,
      memory_available: sample.available,
      ...this.#turnLines(),
    };
  }

  #turnLines(): Answer["lines"] {
    const memory = this.#options.memory;
    if (!memory?.sample()) return {};
    const buffer = memory.turnBufferStatus?.() ?? null;
    if (!buffer) return { turn_limit: memory.turnLimit() };
    return {
      turn_limit: buffer.limit,
      turn_limit_memory: buffer.memoryLimit,
      turn_limit_cap: buffer.limitCap ?? "none",
      person_reserved_slots: buffer.personReservedSlots,
      background_turn_limit: buffer.backgroundLimit ?? "none",
      shed: buffer.shed,
      shed_p99_ms: buffer.shedP99Ms,
      main_loop_p99_ms: buffer.mainLoopP99Ms === null ? null : Math.round(buffer.mainLoopP99Ms),
      shed_changed_at: buffer.shedChangedAt,
    };
  }

  readonly #startLogin = Effect.fn("ServerMode.startLogin")(function* (
    this: ServerMode,
    email: string,
    name: string | null,
  ): Effect.fn.Return<Answer, RemoteWorkflowError> {
    const normalized = email.trim();
    if (!normalized.includes("@") || normalized.length > MAX_EMAIL_LENGTH) return failure(400, "invalid_email");
    // The name is stored only at the code check. A check here refuses it before an email is sent.
    if (name !== null && !parseHostedServerName(name)) return nameFailure(name);
    if (this.#options.centralAuth.getState().status === "signed_in") return failure(409, "signed_in");
    const state = yield* this.#options.centralAuth
      .requestEmailCode(normalized)
      .pipe(Effect.mapError(({ cause }) => new RemoteWorkflowError({ cause })));
    if (state.status === "code_sent" && !state.issue) {
      return { status: 200, lines: { challenge: state.challengeId, expires_at: state.expiresAt } };
    }
    // A server has no browser for the company provider. The owner signs in with another email.
    if (state.status === "company_sign_in") {
      yield* this.#options.centralAuth.cancelCompanySignIn();
      return failure(409, "company_sign_in_required");
    }
    return issueFailure(state);
  });

  readonly #verifyLogin = Effect.fn("ServerMode.verifyLogin")(function* (
    this: ServerMode,
    challenge: string,
    code: string,
    name: string | null,
  ): Effect.fn.Return<Answer, RemoteWorkflowError> {
    if (!challenge.trim() || !code.trim()) return failure(400, "invalid_request");
    if (name !== null) {
      // The host refuses such a name only when it publishes, after the sign-in.
      const serverName = parseHostedServerName(name);
      if (!serverName) return nameFailure(name);
      this.#pendingName = serverName;
    }
    const state = yield* this.#options.centralAuth
      .verifyEmailCode(challenge.trim(), code.trim())
      .pipe(Effect.mapError(({ cause }) => new RemoteWorkflowError({ cause })));
    // The entry point publishes the host when the account change is applied. `status` shows it.
    if (state.status === "signed_in") return this.#status();
    return issueFailure(state);
  });

  /** Signs in with a one-time claim that an operator made for the owner's email. */
  readonly #claimLogin = Effect.fn("ServerMode.claimLogin")(function* (
    this: ServerMode,
    claim: string,
    name: string | null,
  ): Effect.fn.Return<Answer, RemoteWorkflowError> {
    if (!claim.trim()) return failure(400, "invalid_request");
    let serverName: string | null = null;
    if (name !== null) {
      serverName = parseHostedServerName(name);
      if (!serverName) return nameFailure(name);
    }
    const { centralAuth } = this.#options;
    const status = centralAuth.getState().status;
    if (status === "signed_in") return failure(409, "signed_in");
    // Another state can hold a stored session, which a failed claim would remove.
    if (status !== "signed_out" && status !== "code_sent") return failure(409, `account_${status}`);
    // The claim works once. A session that is not stored would end at the next start.
    if (!centralAuth.canPersistSession()) return failure(409, "session_not_storable");
    const previousName = this.#pendingName;
    if (serverName) this.#pendingName = serverName;
    const issue = yield* centralAuth
      .redeemServerClaim(claim.trim())
      .pipe(Effect.mapError(({ cause }) => new RemoteWorkflowError({ cause })));
    if (issue) {
      this.#pendingName = previousName;
      return failure(issue.code === "server_claim_invalid" ? 400 : 502, issue.code, issue.message);
    }
    // The entry point publishes the host when the account change is applied. `status` shows it.
    return this.#status();
  });

  /**
   * A Mobile Connect code as a QR code for a terminal, in base64 because an answer line holds no line
   * break. The terminal has no other way to show the code to a phone.
   */
  readonly #mobileConnect = Effect.fn("ServerMode.mobileConnect")(function* (
    this: ServerMode,
  ): Effect.fn.Return<Answer, RemoteWorkflowError> {
    const account = this.#options.centralAuth.getState();
    if (account.status !== "signed_in") return issueFailure(account);
    // A publish that runs now can give the host its name. The code binds the host that it publishes.
    yield* this.publish();
    const ticket = yield* createHostedMobileConnect(this.#options).pipe(toRemoteWorkflowError);
    const qr = yield* remoteCall(() => QRCode.toString(ticket.qrData, { type: "terminal", small: true }));
    return { status: 200, lines: { qr: Buffer.from(qr).toString("base64"), expires_at: ticket.expiresAt } };
  });

  readonly #rename = Effect.fn("ServerMode.rename")(function* (
    this: ServerMode,
    name: string,
  ): Effect.fn.Return<Answer, RemoteWorkflowError> {
    const serverName = parseHostedServerName(name);
    if (!serverName) return nameFailure(name);
    const { host } = this.#options;
    if (host.getStatus().configured) {
      yield* host.updateIdentity({ serverName });
    } else {
      this.#pendingName = serverName;
      yield* this.publish();
    }
    return this.#status();
  });
}

/** The form body, or null when it is larger than a control request can be. */
async function readBody(request: IncomingMessage): Promise<URLSearchParams | null> {
  const body = await readBodyWithin(request, MAX_BODY_BYTES);
  return body === null ? null : new URLSearchParams(body.toString("utf8"));
}

const AGENT_UPDATE_FIELDS = new Set([
  "agentId",
  "provider",
  "model",
  "providerAccountId",
  "backupModels",
  "sessionTokenCap",
  "turnTokenBudget",
]);
const ROUTINE_UPDATE_FIELDS = new Set(["routineId", "runIn"]);

function runInUpdate(routine: EventRoutine, runIn: RoutineRunPlace): SaveEventRoutineInput {
  const { trigger } = routine;
  return {
    id: routine.id,
    owner: routine.owner,
    name: routine.name,
    instruction: routine.instruction,
    active: routine.active,
    timezone: routine.timezone,
    trigger:
      trigger.kind === "schedule"
        ? { kind: "schedule", schedule: trigger.schedule }
        : { kind: "webhook", eventType: trigger.eventType, filters: trigger.filters },
    ...(routine.limitPolicy === undefined ? {} : { limitPolicy: routine.limitPolicy }),
    ...(routine.missedPolicy === undefined ? {} : { missedPolicy: routine.missedPolicy }),
    runIn,
  };
}
const DEFAULT_ACCOUNT_FIELD = "default";

function failure(status: number, error: string, message: string | null = null): Answer {
  return { status, lines: { error, message } };
}

/** The message of the team store for a name that it refuses. */
function nameFailure(name: string): Answer {
  const length = name.trim().length;
  const message =
    length < INPUT_LIMITS.serverNameMin || length > INPUT_LIMITS.serverName
      ? sourceText("error.team.serverNameLength", { min: INPUT_LIMITS.serverNameMin, max: INPUT_LIMITS.serverName })
      : sourceText("error.team.serverNameHostname");
  return failure(400, "invalid_name", message);
}

function issueFailure(state: CentralAuthState): Answer {
  if ((state.status === "code_sent" || state.status === "error") && state.issue) {
    return failure(state.status === "error" ? 502 : 400, state.issue.code, state.issue.message);
  }
  return failure(409, `account_${state.status}`);
}

/** One `key=value` line for each value. A value never breaks its line. */
function formatLines(lines: Answer["lines"]): string {
  return Object.entries(lines)
    .filter((entry): entry is [string, string | number] => entry[1] !== null && entry[1] !== "")
    .map(([key, value]) => `${key}=${String(value).replace(/[\r\n]+/gu, " ")}\n`)
    .join("");
}
