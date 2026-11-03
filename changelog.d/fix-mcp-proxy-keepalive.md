### Fixed

- An MCP tool call from a Codex agent no longer fails with "error sending request" when more than
  five seconds passed since the agent's previous call to a signed-in server. The MCP sign-in proxy
  leaves the end of an idle connection to the provider.
