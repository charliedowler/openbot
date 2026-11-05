# Watch registry

The Watching snapshot combines signed external watch metadata with open native waits. Reading or updating this snapshot does not start a provider turn or enqueue a routine.

The optional Team API capability is `watch-registry-v1`. A client with the capability reads `GET /v1/watch-registry`. Desktop IPC exposes `agent.getWatchingSnapshot(serverId?)`. The `watching-changed` agent event invalidates the snapshot. Clients refresh when the view opens, when the user requests a refresh, or when this event arrives.

External publishers use a dedicated, active agent webhook routine with the exact event type `bridge.watch.registry.sink`. They sign the ordinary relay delivery body with that routine's signing secret. The actual body has type `bridge.watch.registry` and data of type `WatchRegistryPublication` in [the contract](../../packages/contracts/src/watch-registry.ts). The native host authenticates the route and HMAC before storing metadata. It rejects registry bodies sent to an ordinary or wildcard routine. A dedicated metadata receiver rejects every other body type. Native Test, Run and local automation cannot start agent work from that receiver. Other receivers retain their existing handling.

A previous native host compares the event type exactly. It ignores a `bridge.watch.registry` body on the dedicated sink routine and does not enqueue AI work. No Signal change or special relay path is required. A relay HTTP success is transport acceptance only: an old host can return 202 for an ignored event. The native snapshot's sequence and generated timestamp provide the display freshness evidence.

The preference file `openbot-watch-registry-v1.json` lives in the application's user data directory. It has version 1, a nullable `managementPublisherRouteId`, a native aggregate `revision`, and a `publishers` array. Each publisher has the authenticated route ID, its receiver owner agent ID, and its most recent publication. An unknown or damaged file remains untouched and disables writes. Accepted writes use the existing atomic preference writer with private permissions. There is no database migration.

A publisher can name only its receiver owner unless its authenticated route ID equals the explicitly provisioned `managementPublisherRouteId`. This setting must name the dedicated metadata receiver, not an ordinary event receiver. Native setup provisions this setting and the existing signing credential through its normal secure path. Publishers stay disabled until the host capability and dedicated routine configuration have been verified.

Each publication has a monotonic sequence and at most 200 watches. The existing 64 KiB delivery limit still applies. Exact retries are unchanged. Older sequences and conflicting copies of one sequence fail. Native storage bounds the aggregate to 200 publishers and 2000 watches. There is no truncation. Only display metadata is accepted. URL credentials, query strings, fragments, extra fields and known secrets in identifiers or URLs are rejected. Known secrets in display text are redacted before persistence.

Team API readers receive watches and waits only for agents they can see. A channel-linked watch or wait also requires access to every origin-channel member. A native channel wait needs a matching durable assignment for its original delivery and owner. A missing, deleted or inaccessible origin omits the wait entirely. Native waits expose their note and timing metadata, never their full request text.

Run the focused service and HTTP checks with:

```sh
bun --preload ./scripts/team-api-routes-e2e-electron.mjs scripts/watch-registry-e2e.ts
```

The report is written to `.openbot-build/watch-registry-e2e/report.json`. An optional `--legacy-host-events=<module-path>` check can exercise a preserved previous host-events module without changing its handler logic.

## Version 2

The optional capability `watch-registry-v2` adds `GET /v2/watch-registry`. The host keeps accepting version 1 publications, and `/v1/watch-registry` keeps the exact version 1 shape: it has no tasks and no version 2 watch fields.

A version 2 publication has `version: 2`. Each watch may add `summary` (at most 120 characters), `purpose` (at most 240 characters) and `receiverAgentIds` (at most 8 agent IDs). `condition` is optional. Unknown receivers are dropped before persistence, and a reader sees only the receivers it can see.

A version 2 publication with `tasks` (at most 300) is a task publication. It has `watches: []` and may name `publisherId: "openbot-control-tasks"`. The host stores it in its own slot, `openbot-control-tasks`, with its own sequence, so watches and tasks each have the 64 KiB delivery budget. Only the management route can publish tasks. A task publication may carry one `health` object. Task and health fields are bounded and unknown keys are rejected. A task owner that is not a known agent is dropped. A reader sees a task when it can see its owner; a task without an owner is shown to owners, admins and the local desktop only. Known secrets in display text are redacted; a known secret in a task ID or link refuses the publication.

A version 2 watch publication from the management route can be paged so that it fits the 64 KiB limit. A page has `publisherId: "openbot-control-watches-<n>"` (n is 1 to 4) and `page: { index: n, count }`, and the host stores it in its own slot with its own sequence. An accepted page deletes the pages whose index is greater than its `count` and the route's own unpaged slot. An unpaged watch publication from the management route deletes the pages. `/v2/watch-registry` merges the stored watch slots into one list; its `sequence` is the highest slot sequence and its `generatedAt` is the oldest slot's. `/v1/watch-registry` keeps the revision and the newest time. A watch without `receiverAgentIds` is received by its owner.

Version 2 state is stored in `openbot-watch-registry-v2.json`. While that file is missing, the host reads `openbot-watch-registry-v1.json`. The first accepted publication writes the version 2 file and leaves the version 1 file unchanged, so an earlier build reads its own file after a rollback. Set `managementPublisherRouteId` in the version 2 file once it exists. Every accepted publication emits `watching-changed`.

```sh
bun --preload ./scripts/team-api-routes-e2e-electron.mjs scripts/watch-registry-v2-e2e.ts
```
