### Added

- A self-hosted server's control socket accepts `POST /v1/turns/interrupt` with `agentId` and an optional `turnId`. It stops the agent's running turn as the app's Stop does. A `turnId` that is not a running turn of the agent gets 409 and stops nothing.
- The control socket accepts `POST /v1/agents/update` with `agentId` and any of `provider`, `model`, `providerAccountId` (`default` for the provider's own sign-in) and `backupModels` (JSON, as `update_profile` takes it). The change has the checks and the update of `update_profile`. Other fields get 400.
- The control socket answers `GET /v1/agents/queue` with each agent's name, provider, model, account, backup models, queued messages, oldest queued time and running turn.
