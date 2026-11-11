### Added

- The server logs one warning line when a call of the main thread that grows with the history takes more than 250 ms: a channel packet and its summary check, the channel list, a queue change of the mailbox sync, a mailbox chat read, and the emit and publish of a chat. The line gives the function, the time and the counts that drive the cost (rows and messages read, characters, bytes). It carries no message text. Each function logs at most one line in 10 seconds, and the next line counts the slow calls that were not logged.
