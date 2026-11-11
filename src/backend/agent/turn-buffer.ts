import { createOpenBotLogger } from "@openbot/logging";

const logger = createOpenBotLogger("turn-buffer");

export const DEFAULT_PERSON_RESERVED_SLOTS = 3;
export const DEFAULT_SHED_P99_MS = 2_000;
export const SHED_AFTER_SAMPLES = 2;
export const RESTORE_AFTER_SAMPLES = 3;

export interface TurnBufferSettings {
  limit: number | null;
  personReservedSlots: number;
  shed: boolean;
  shedP99Ms: number;
}

export interface TurnBufferStatus {
  memoryLimit: number;
  limit: number;
  limitCap: number | null;
  personReservedSlots: number;
  backgroundLimit: number | null;
  shed: "off" | "on" | "disabled";
  shedP99Ms: number;
  mainLoopP99Ms: number | null;
  shedChangedAt: string | null;
}

export type TurnBufferLogFields = Record<string, number | string | boolean | null>;

export interface TurnBufferOptions {
  log?: (message: string, fields: TurnBufferLogFields) => void;
  now?: () => Date;
}

export function turnBufferSettings(environment: NodeJS.ProcessEnv): TurnBufferSettings {
  const limit = positiveInteger(environment.OPENBOT_TURN_LIMIT);
  const reserved = nonNegativeInteger(environment.OPENBOT_PERSON_RESERVED_SLOTS);
  const shed = environment.OPENBOT_SHED?.trim();
  return {
    limit,
    personReservedSlots: reserved ?? (limit === null ? 0 : DEFAULT_PERSON_RESERVED_SLOTS),
    shed: shed === "0" ? false : shed === "1" || limit !== null,
    shedP99Ms: positiveInteger(environment.OPENBOT_SHED_P99_MS) ?? DEFAULT_SHED_P99_MS,
  };
}

export class TurnBuffer {
  readonly settings: TurnBufferSettings;
  readonly #log: NonNullable<TurnBufferOptions["log"]>;
  readonly #now: () => Date;
  #shedding = false;
  #over = 0;
  #under = 0;
  #lastP99Ms: number | null = null;
  #changedAt: string | null = null;

  constructor(settings: TurnBufferSettings, options: TurnBufferOptions = {}) {
    this.settings = settings;
    this.#log = options.log ?? ((message, fields) => logger.info(message, fields));
    this.#now = options.now ?? (() => new Date());
  }

  limit(memoryLimit: number): number {
    return this.settings.limit === null ? memoryLimit : Math.min(memoryLimit, this.settings.limit);
  }

  backgroundLimit(limit: number): number | null {
    if (this.settings.personReservedSlots === 0 && !this.#shedding) return null;
    const unreserved = Math.max(1, limit - this.settings.personReservedSlots);
    return this.#shedding ? Math.max(1, Math.floor(unreserved / 2)) : unreserved;
  }

  shedding(): boolean {
    return this.#shedding;
  }

  observeMainLoop(p99Ms: number): void {
    if (!Number.isFinite(p99Ms)) return;
    this.#lastP99Ms = p99Ms;
    if (!this.settings.shed) return;
    if (p99Ms > this.settings.shedP99Ms) {
      this.#over += 1;
      this.#under = 0;
      if (!this.#shedding && this.#over >= SHED_AFTER_SAMPLES) this.#change(true, p99Ms);
      return;
    }
    this.#under += 1;
    this.#over = 0;
    if (this.#shedding && this.#under >= RESTORE_AFTER_SAMPLES) this.#change(false, p99Ms);
  }

  status(memoryLimit: number): TurnBufferStatus {
    const limit = this.limit(memoryLimit);
    return {
      memoryLimit,
      limit,
      limitCap: this.settings.limit,
      personReservedSlots: this.settings.personReservedSlots,
      backgroundLimit: this.backgroundLimit(limit),
      shed: this.settings.shed ? (this.#shedding ? "on" : "off") : "disabled",
      shedP99Ms: this.settings.shedP99Ms,
      mainLoopP99Ms: this.#lastP99Ms,
      shedChangedAt: this.#changedAt,
    };
  }

  #change(shedding: boolean, p99Ms: number): void {
    this.#shedding = shedding;
    this.#over = 0;
    this.#under = 0;
    this.#changedAt = this.#now().toISOString();
    this.#log(
      shedding
        ? "The main loop is slow. Background turns get half of their slots."
        : "The main loop is fast again. Background turns get all of their slots.",
      {
        shedding,
        mainLoopP99Ms: Math.round(p99Ms),
        shedP99Ms: this.settings.shedP99Ms,
        samples: shedding ? SHED_AFTER_SAMPLES : RESTORE_AFTER_SAMPLES,
      },
    );
  }
}

function positiveInteger(value: string | undefined): number | null {
  const parsed = nonNegativeInteger(value);
  return parsed === null || parsed === 0 ? null : parsed;
}

function nonNegativeInteger(value: string | undefined): number | null {
  const text = value?.trim();
  if (!text || !/^\d+$/u.test(text)) return null;
  const parsed = Number(text);
  return Number.isSafeInteger(parsed) ? parsed : null;
}
