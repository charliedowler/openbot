import { readFile } from "node:fs/promises";
import type { SpawnedProcess } from "@anthropic-ai/claude-agent-sdk";

export class ProviderTurnProcess {
  readonly #child: SpawnedProcess;
  readonly #writes = new Map<string, number>();
  #lastOutputAt: number | null = null;

  constructor(child: SpawnedProcess) {
    this.#child = child;
  }

  get live(): boolean {
    return this.#child.exitCode === null && this.#child.signalCode == null;
  }

  kill(): void {
    if (this.live) this.#child.kill("SIGKILL");
  }

  async toolOutputAt(activeTool: boolean): Promise<number | null> {
    if (process.platform !== "linux" || !this.live || !activeTool) {
      this.#writes.clear();
      return null;
    }
    const child = this.#child;
    if (!("pid" in child) || typeof child.pid !== "number") return null;
    const pending = [child.pid];
    const seen = new Set<string>();
    for (let index = 0; index < pending.length && index < 256; index++) {
      const pid = pending[index];
      try {
        const children = await readFile(`/proc/${pid}/task/${pid}/children`, "utf8");
        for (const item of children.trim().split(/\s+/u)) {
          const value = Number(item);
          if (Number.isSafeInteger(value) && value > 0 && !pending.includes(value)) pending.push(value);
        }
        if (pid === child.pid) continue;
        const [stat, io] = await Promise.all([
          readFile(`/proc/${pid}/stat`, "utf8"),
          readFile(`/proc/${pid}/io`, "utf8"),
        ]);
        const start = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
        const written = Number(/^wchar:\s*(\d+)/mu.exec(io)?.[1]);
        if (!start || !Number.isSafeInteger(written)) continue;
        const key = `${pid}:${start}`;
        seen.add(key);
        const previous = this.#writes.get(key);
        this.#writes.set(key, written);
        if (previous !== undefined && written > previous) this.#lastOutputAt = Date.now();
      } catch {}
    }
    for (const key of this.#writes.keys()) if (!seen.has(key)) this.#writes.delete(key);
    return this.#lastOutputAt;
  }
}
