/** The shortest time between two reads of one server's channel list for reply notifications. */
export const CHANNEL_LIST_INTERVAL_MS = 1_000;

/**
 * Coalesces the channel list reads of each server: one read at a time, and at most one read in each
 * interval. A streaming reply changes the channels on each frame, and the local list query counts
 * the unread messages of every channel, so a read on each frame held the main thread for seconds.
 * A change during a read or inside the interval causes one more read at the end of the interval, so
 * the last change is always read. A change after a quiet interval reads at once.
 */
export function createChannelListRefresh(
  read: (serverId: string) => Promise<void>,
  intervalMs: number = CHANNEL_LIST_INTERVAL_MS,
): (serverId: string) => void {
  const reading = new Map<string, boolean>();
  const readAt = new Map<string, number>();
  const timers = new Map<string, ReturnType<typeof setTimeout>>();

  function refresh(serverId: string): void {
    if (reading.has(serverId)) {
      reading.set(serverId, true);
      return;
    }
    if (timers.has(serverId)) return;
    const wait = (readAt.get(serverId) ?? Number.NEGATIVE_INFINITY) + intervalMs - Date.now();
    if (wait > 0) {
      const timer = setTimeout(() => {
        timers.delete(serverId);
        refresh(serverId);
      }, wait);
      timer.unref?.();
      timers.set(serverId, timer);
      return;
    }
    reading.set(serverId, false);
    readAt.set(serverId, Date.now());
    void (async () => read(serverId))()
      .catch(() => undefined)
      .finally(() => {
        const changed = reading.get(serverId);
        reading.delete(serverId);
        if (changed) refresh(serverId);
      });
  }

  return refresh;
}
