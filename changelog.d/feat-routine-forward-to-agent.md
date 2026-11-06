### Added

- A webhook routine can now send an event to the chat of another agent. A signed request with a `forwardToAgentId` field starts a routine run in that agent's own chat, with the `instruction` field of the request or else the routine's instruction. The other agent needs no webhook secret of its own.
  Only agents on `OPENBOT_ROUTINE_FORWARD_ALLOWLIST` (entries `<routine or owner agent ID>:<agent ID>,<agent ID>`) can get such events. Any other target gets 400. The signature check does not change, and a request without the field starts the routine as before.
