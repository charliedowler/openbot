### Fixed

- A usage limit no longer holds every agent on an account and model until the reset that one refused turn named. Every 15 minutes, one waiting turn on that account and model may try. When it completes, the limit ends and the other agents start. When the provider refuses it, the limit holds again with the new reset, and the next try waits 15 minutes. Only one such try runs at a time for one account and model.
- OpenBot now logs each usage limit hold and each end of a hold, with the provider, account, model, reset time and the agent whose turn caused it.

### Added

- A self-hosted server's control socket accepts `POST /v1/usage/reset` with `provider` and an optional `accountId`. It ends the usage limits that hold that provider's agents, as the app's usage reset does, but spends no reset credit. It answers with the number of limits it ended.
