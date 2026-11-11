import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { Effect, Schema, Semaphore } from "effect";
import { AgentRoutineStore } from "./agent-routine-store";
import { type BridgeWorkCancellation, BridgeWorkStore, bridgeWorkKeys } from "./bridge-work-store";
import { ChannelRoutineStore } from "./channel-routine-store";
import type { ChannelService } from "./channel-service";
import type { MailboxStore } from "./mailbox-store";
import type { OpenBotDatabase } from "./openbot-database";

export class BridgeWorkFailure extends Schema.TaggedError<BridgeWorkFailure>()("BridgeWorkFailure", {
  cause: Schema.Defect(),
}) {}

export interface BridgeWorkResult {
  idempotencyKey: string;
  duplicate: boolean;
  results: Array<{
    id: string;
    linkedRuns: number;
    cancelledRuns: number;
    cancelledDeliveries: number;
    cancelledTasks: number;
    cancelledWaits: number;
    refusals: Array<{ id: string; reason: string }>;
  }>;
}

export interface BridgeWorkOptions {
  channels: ChannelService;
  mailbox: MailboxStore;
  cancelQueuedMessage(agentId: string, deliveryId: string): Effect.Effect<void, { readonly cause: unknown }>;
}

const step = <A>(run: () => A) => Effect.try({ try: run, catch: (cause) => new BridgeWorkFailure({ cause }) });

export class BridgeWorkService {
  readonly #serial = Semaphore.makeUnsafe(1);
  readonly store: BridgeWorkStore;

  constructor(
    readonly database: OpenBotDatabase,
    readonly options: BridgeWorkOptions,
  ) {
    this.store = new BridgeWorkStore(database);
  }

  readonly recover = Effect.fn("BridgeWork.recover")(function* (this: BridgeWorkService) {
    const closures = yield* step(() => this.store.closures());
    for (const closure of closures) yield* this.cancel(closure.authority, closure.input);
  }).bind(this);

  readonly cancel = Effect.fn("BridgeWork.cancel")(
    function* (this: BridgeWorkService, authority: string, input: BridgeWorkCancellation) {
      const fresh = yield* step(() => this.store.close(authority, input));
      const result: BridgeWorkResult = { idempotencyKey: input.idempotencyKey, duplicate: !fresh, results: [] };
      const handled = new Map<string, Omit<BridgeWorkResult["results"][number], "id" | "linkedRuns">>();
      for (const ref of bridgeWorkKeys(input)) {
        const links = yield* step(() => this.store.links(authority, ref));
        const counts: BridgeWorkResult["results"][number] = {
          id: ref,
          linkedRuns: links.length,
          cancelledRuns: 0,
          cancelledDeliveries: 0,
          cancelledTasks: 0,
          cancelledWaits: 0,
          refusals: [],
        };
        for (const link of links) {
          let runCounts = handled.get(link.runId);
          if (!runCounts) {
            runCounts = {
              cancelledRuns: 0,
              cancelledDeliveries: 0,
              cancelledTasks: 0,
              cancelledWaits: 0,
              refusals: [],
            };
            const table = link.owner.kind === "agent" ? "projection_routine_runs" : "projection_channel_routine_runs";
            const row = yield* step(() =>
              this.database.connection.prepare(`SELECT * FROM ${table} WHERE run_id = ?`).get(link.runId),
            );
            if (!isDynamicRecord(row)) runCounts.refusals.push({ id: link.runId, reason: "missing-run" });
            else if (link.owner.kind === "channel" && typeof row.request_message_id === "string") {
              const cancelled = yield* this.options.channels
                .cancelBridgeWork(
                  link.owner.id,
                  row.request_message_id,
                  `bridge-work:${authority}:${input.idempotencyKey}:${link.runId}`,
                )
                .pipe(Effect.mapError((failure) => new BridgeWorkFailure({ cause: failure.cause })));
              Object.assign(runCounts, cancelled);
              if (!cancelled.refusals.length) {
                yield* step(() =>
                  new ChannelRoutineStore(this.database).updateRunStatus(link.runId, "cancelled", null),
                );
                runCounts.cancelledRuns = row.status === "cancelled" ? 0 : 1;
              }
            } else if (link.owner.kind === "agent") {
              const context =
                typeof row.delivery_id === "string" ? this.options.mailbox.getDelivery(row.delivery_id) : null;
              if (
                row.status === "running" ||
                (context && !["queued", "cancelled", "completed", "failed"].includes(context.delivery.status))
              )
                runCounts.refusals.push({ id: link.runId, reason: "running-delivery" });
              else if (
                row.status === "succeeded" ||
                row.status === "failed" ||
                context?.delivery.status === "completed"
              ) {
                runCounts.refusals.push({ id: link.runId, reason: "settled-run" });
              } else {
                if (context?.delivery.status === "queued") {
                  yield* this.options
                    .cancelQueuedMessage(context.delivery.recipientAgentId, context.delivery.id)
                    .pipe(Effect.mapError((failure) => new BridgeWorkFailure({ cause: failure.cause })));
                  runCounts.cancelledDeliveries += 1;
                }
                yield* step(() => new AgentRoutineStore(this.database).updateRunStatus(link.runId, "cancelled"));
                runCounts.cancelledRuns = row.status === "cancelled" ? 0 : 1;
              }
            } else {
              yield* step(() => new ChannelRoutineStore(this.database).updateRunStatus(link.runId, "cancelled", null));
              runCounts.cancelledRuns = row.status === "cancelled" ? 0 : 1;
            }
            handled.set(link.runId, runCounts);
          }
          counts.cancelledRuns += runCounts.cancelledRuns;
          counts.cancelledDeliveries += runCounts.cancelledDeliveries;
          counts.cancelledTasks += runCounts.cancelledTasks;
          counts.cancelledWaits += runCounts.cancelledWaits;
          counts.refusals.push(...runCounts.refusals);
        }
        if (!links.length) counts.refusals.push({ id: ref, reason: "no-linked-work" });
        result.results.push(counts);
      }
      return result;
    },
    (operation) => this.#serial.withPermit(operation).pipe(Effect.uninterruptible),
  ).bind(this);
}
