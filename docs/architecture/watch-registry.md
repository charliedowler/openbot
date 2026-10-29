# Watch registry

The Watching snapshot combines signed external watch metadata with open native waits. Reading or updating this snapshot does not start a provider turn or enqueue a routine.

The optional Team API capability is `watch-registry-v1`. A client with the capability reads `GET /v1/watch-registry`. Desktop IPC exposes `agent.getWatchingSnapshot(serverId?)`. The `watching-changed` agent event invalidates the snapshot. Clients refresh when the view opens, when the user requests a refresh, or when this event arrives.

External publishers use a dedicated, active agent webhook routine with the exact event type `bridge.watch.registry.sink`. They sign the ordinary relay delivery body with that routine's signing secret. The actual body has type `bridge.watch.registry` and data of type `WatchRegistryPublication` in [the contract](../../packages/contracts/src/watch-registry.ts). The native host authenticates the route and HMAC before storing metadata. It rejects registry bodies sent to an ordinary or wildcard routine. Other webhook event types retain their existing handling.

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
