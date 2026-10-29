import type { ContextPullRequest, ContextPullRequestsSnapshot } from "@openbot/contracts/context-prs";
import type { Meta, StoryObj } from "storybook-solidjs-vite";
import type { MockChannelSeed } from "../src/preview/mock-channels";
import { PrototypeApp } from "./prototype-support";

const checkedAt = "2026-10-20T14:30:00.000Z";
const channels: MockChannelSeed[] = [
  {
    channel: {
      id: "preview-release-room",
      name: "Release room",
      title: "",
      instructions: "",
      members: [{ agentId: "chief" }, { agentId: "research" }],
      leadAgentId: null,
      archived: false,
      revision: 1,
      createdAt: checkedAt,
    },
    messages: [],
  },
];
const row = (number: number, title: string, updates: Partial<ContextPullRequest> = {}): ContextPullRequest => ({
  url: `https://github.com/example/project/pull/${number}`,
  repo: "example/project",
  number,
  title,
  state: "open",
  draft: false,
  mergedAt: null,
  headSha: "a".repeat(40),
  checks: "passed",
  checkedAt,
  ...updates,
});

const snapshot: ContextPullRequestsSnapshot = {
  scope: { kind: "agent", id: "chief" },
  status: "ready",
  checkedAt,
  open: [
    row(42, "Keep filter chips on one line", { checks: "pending" }),
    row(38, "Show dashboard details in the side panel", { draft: true, checks: "failed" }),
  ],
  merged: [
    row(40, "Improve keyboard focus", { state: "merged", mergedAt: "2026-10-20T14:20:00.000Z" }),
    row(39, "Restore saved filters", { state: "merged", mergedAt: "2026-10-20T13:45:00.000Z" }),
    row(35, "Open shared folders in the app", { state: "merged", mergedAt: "2026-10-20T11:30:00.000Z" }),
  ],
  failures: [],
  referencesLimited: false,
};

const meta = {
  title: "Conversation/Pull requests",
  parameters: { layout: "fullscreen" },
  render: () => <PrototypeApp options={{ contextPullRequests: snapshot, channels }} />,
} satisfies Meta;

export default meta;
type Story = StoryObj<typeof meta>;
export const OpenAndRecentMerges: Story = {};
export const Empty: Story = {
  render: () => <PrototypeApp options={{ contextPullRequests: { ...snapshot, open: [], merged: [] } }} />,
};
export const Partial: Story = {
  render: () => (
    <PrototypeApp
      options={{
        contextPullRequests: {
          ...snapshot,
          failures: [{ url: "https://github.com/example/project/pull/43", reason: "forbidden" }],
        },
      }}
    />
  ),
};
