import type { ChannelSummary } from "@openbot/contracts/ipc";

export const CHANNEL_LIST_BATCH_MS = 1_000;

interface ListEntry {
  readAt: number;
  value: ChannelSummary[];
  changed: Map<string, number>;
  timer: ReturnType<typeof setTimeout> | null;
}

export class ChannelListBatch {
  readonly #entries = new Map<string, ListEntry>();
  readonly #read: (memberId: string, signedOutMessagesAreTheirs: boolean) => ChannelSummary[];
  readonly #announce: (channelId: string, revision: number) => void;
  readonly #intervalMs: number;

  constructor(
    read: (memberId: string, signedOutMessagesAreTheirs: boolean) => ChannelSummary[],
    announce: (channelId: string, revision: number) => void,
    intervalMs = CHANNEL_LIST_BATCH_MS,
  ) {
    this.#read = read;
    this.#announce = announce;
    this.#intervalMs = intervalMs;
  }

  list(memberId: string, signedOutMessagesAreTheirs = false): ChannelSummary[] {
    const key = `${signedOutMessagesAreTheirs ? "1" : "0"}:${memberId}`;
    const now = Date.now();
    const entry = this.#entries.get(key);
    if (entry && now - entry.readAt < this.#intervalMs) {
      if (entry.changed.size && !entry.timer) {
        const timer = setTimeout(() => this.#announceAfter(key, entry), entry.readAt + this.#intervalMs - now);
        timer.unref?.();
        entry.timer = timer;
      }
      return entry.value;
    }
    const value = this.#read(memberId, signedOutMessagesAreTheirs);
    this.#entries.set(key, { readAt: Date.now(), value, changed: new Map(), timer: null });
    if (entry?.timer) {
      clearTimeout(entry.timer);
      for (const [channelId, revision] of entry.changed) this.#announce(channelId, revision);
    }
    return value;
  }

  changed(channelId: string, revision: number): void {
    for (const entry of this.#entries.values()) entry.changed.set(channelId, revision);
  }

  stop(): void {
    for (const entry of this.#entries.values()) if (entry.timer) clearTimeout(entry.timer);
    this.#entries.clear();
  }

  #announceAfter(key: string, entry: ListEntry): void {
    if (this.#entries.get(key) !== entry) return;
    this.#entries.delete(key);
    for (const [channelId, revision] of entry.changed) this.#announce(channelId, revision);
  }
}
