### Added

- The server control socket lists the agent routines (`GET /v1/routines`: id, agent, name, where it runs, active, trigger kind) and sets where an agent routine runs (`POST /v1/routines/update` with `routineId` and `runIn`, `chat` or `thread`). The change saves through the path of the app's routine editor, so the webhook route and its secret stay. Bad input gets 400, an unknown routine 404 and a channel routine 409.
