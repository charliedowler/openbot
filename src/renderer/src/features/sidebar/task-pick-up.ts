import type { SidebarTaskPickUp } from "@openbot/ui/features/sidebar/Sidebar";
import { createSignal } from "solid-js";

const PICK_UP_AGENT_STORAGE_PREFIX = "openbot:task-pick-up-agent:";
const DEFAULT_PICK_UP_AGENT_NAME = "Compass";

function readPickUpAgent(serverId: string): string | null {
  try {
    return window.localStorage.getItem(PICK_UP_AGENT_STORAGE_PREFIX + serverId);
  } catch {
    return null;
  }
}

export function createTaskPickUp(deps: {
  serverId: () => string;
  agents: () => readonly { id: string; name: string }[];
  pickUp: (agentId: string, message: string) => Promise<boolean> | boolean;
}): () => SidebarTaskPickUp {
  const [unsaved, setUnsaved] = createSignal<ReadonlyMap<string, string>>(new Map());
  const [savedVersion, setSavedVersion] = createSignal(0);
  const agentId = () => {
    savedVersion();
    const serverId = deps.serverId();
    const agents = deps.agents();
    const chosen = unsaved().get(serverId) ?? readPickUpAgent(serverId);
    if (chosen && agents.some((agent) => agent.id === chosen)) return chosen;
    return agents.find((agent) => agent.name === DEFAULT_PICK_UP_AGENT_NAME)?.id ?? null;
  };
  const choose = (id: string) => {
    const serverId = deps.serverId();
    try {
      window.localStorage.setItem(PICK_UP_AGENT_STORAGE_PREFIX + serverId, id);
      setUnsaved((choices) => {
        const next = new Map(choices);
        next.delete(serverId);
        return next;
      });
      setSavedVersion((version) => version + 1);
    } catch {
      setUnsaved((choices) => new Map(choices).set(serverId, id));
    }
  };
  return () => ({
    agents: deps.agents(),
    agentId: agentId(),
    onChangeAgent: choose,
    onPickUp: deps.pickUp,
  });
}
