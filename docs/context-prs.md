# Context pull requests

The desktop reads PR links for one selected agent or channel when its PR panel opens
or the user refreshes it. It makes no model calls and starts no recurring check.
The renderer sends only a scope and an optional refresh flag through
`contextPrs.read(input, serverId?)`.

The host derives exact `https://github.com/OWNER/REPO/pull/NUMBER` references from
persisted conversation messages. Agent scope includes its main chat, side threads
and explicitly owned watch target links that have no channel origin. Channel scope includes only that channel's
messages and task instructions or expected results. Agent membership does not make
an agent's other PRs relevant to the channel. Slack channel IDs are not native
channel IDs. An archived or expired watch link can establish relevance, but the
watch state does not supply the displayed GitHub facts.

A connected native GitHub account supplies bounded GET requests for the exact PR,
its head check runs and commit statuses. It never falls back to another identity
on a failed read. When no connector is configured or it is disconnected, the host
can use its existing `gh` sign-in. Pending and expired connections do not fall back.
The subprocess receives a fixed read-only argv with no shell, a fixed GitHub host,
closed stdin, disabled prompts, a 15-second deadline and a 2 MiB output limit.
Credentials stay in the existing connector or CLI store.

The response keeps all verified open PRs and the three newest verified merged PRs
by actual merge time. Closed, unmerged PRs are omitted. Contradictory states,
invalid merge dates and different response URLs are refused. Failed references
have bounded reason codes and no provider body. A row's `checkedAt` is its metadata
read time. The snapshot time is the time the response was prepared.

Successes are cached in memory for two minutes. Explicit refresh bypasses the cache;
authentication changes clear it. Each read extracts references again. A failed
refresh removes that PR's cached metadata. All readers share three request permits.
The whole panel read has a 12-second deadline, below the Team request timeout.
It aborts HTTP or CLI work and records queued references as timeout failures.
Each provider request also has a 15-second limit. Each HTTP body is limited to 2 MiB. There
are no cache timers and no background repository discovery.

The extraction bounds are 5,000 recent messages, 5,000 channel tasks, 200,000
characters per source text, four million scanned characters in total and 200 unique
PR references. Source rows stream as bounded SQL snippets; message ordering keeps
only IDs in the sort. `referencesLimited` marks
any bound reached. It does not claim that a partial set contains all open PRs or
the three most recent merges in the full history.

`context-prs-v1` is an optional Team API capability. Its frozen route is
`POST /v1/context-prs/read`. Older hosts return an unsupported client snapshot.
A remote request requires the selected agent to be visible. A channel request
requires every current member agent to be visible. The host checks access before
provider work and again before returning data. A missing or private scope returns
no PR identities. Known secrets in titles are redacted. No conversation text or
credentials are sent to GitHub, only the referenced repository, PR number and head
SHA. A new metadata request cannot invoke the generic MCP tool gateway or a write.

Run the focused synthetic service and authenticated HTTP check with:

```sh
npm exec --yes --package=bun@1.4.2 -- bun --preload ./scripts/team-api-routes-e2e-electron.mjs scripts/context-prs-e2e.ts
```

The report is `.openbot-build/context-prs-e2e/report.json`. The fixture uses a real
SQLite store, a loopback HTTP provider and a temporary read-only CLI executable.
It uses no live accounts and starts no agent work.
