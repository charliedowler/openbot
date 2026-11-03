### Added

- Send a Codex sign-in through an OpenAI-compatible proxy, such as CLIProxyAPI, with an `openbot-proxy.json` file in its Codex home folder. Saved chats continue, the ChatGPT sign-in stays in `auth.json`, and a 429 from the proxy holds the agents until the limit ends.
