import type { WatchingSnapshot } from "@openbot/contracts/ipc";
import type { Meta, StoryObj } from "storybook-solidjs-vite";
import { PrototypeApp } from "./prototype-support";

const snapshot: WatchingSnapshot = {
  registry: {
    version: 1,
    sequence: 4,
    generatedAt: "2026-10-20T14:32:00.000Z",
    watches: [
      {
        publisherId: "preview-publisher",
        id: "release-checks",
        ownerAgentId: "chief",
        sourceKind: "GitHub checks",
        title: "Release checks",
        status: "active",
        condition: "Report when the required checks finish.",
        lastCheckedAt: "2026-10-20T14:31:52.000Z",
        expiresAt: "2026-10-21T14:30:00.000Z",
      },
      {
        publisherId: "preview-publisher",
        id: "research-thread",
        ownerAgentId: "research",
        sourceKind: "T3 thread",
        title: "Independent accessibility review",
        status: "active",
        condition: "Wake Research when the delegated review completes or needs input.",
        lastCheckedAt: "2026-10-20T14:31:57.000Z",
      },
      {
        publisherId: "preview-publisher",
        id: "release-thread",
        ownerAgentId: "chief",
        sourceKind: "GitHub discussion",
        title: "Release discussion",
        status: "error",
        lastCheckedAt: "2026-10-20T14:26:00.000Z",
        lastError: "The source account cannot read this repository. Access approval is required.",
      },
      {
        publisherId: "preview-publisher",
        id: "capacity",
        ownerAgentId: "research",
        sourceKind: "Capacity",
        title: "Background capacity check",
        status: "paused",
      },
    ],
  },
  waits: [
    {
      id: "preview-native-wait",
      ownerAgentId: "research",
      note: "Read the accessibility review and return its findings.",
      routineId: "preview-review-inbox",
      createdAt: "2026-10-20T14:20:00.000Z",
      wokenAt: null,
    },
  ],
};

const meta = {
  title: "App/Watching",
  parameters: { layout: "fullscreen" },
  render: () => <PrototypeApp options={{ watchingSnapshot: snapshot }} />,
} satisfies Meta;

export default meta;
type Story = StoryObj<typeof meta>;
export const SourceHealthAndWaitingTasks: Story = {};
export const Empty: Story = {
  render: () => <PrototypeApp options={{ watchingSnapshot: { registry: null, waits: [] } }} />,
};
