### Added

- The server control socket takes `POST /v1/diagnostics/cpu-profile` (`seconds` 1 to 120, `intervalUs` 100 to 100,000) and writes a CPU profile of the main process to the logs folder, with no restart and no inspector port. Slow-call lines for `AgentService.emit` and `MailboxSync.emitQueue` now give the time of each stage.
