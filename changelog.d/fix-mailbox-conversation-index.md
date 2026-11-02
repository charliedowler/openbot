### Fixed

- Keep the server responsive when an agent that many teammates write to gets a queue change. Each queue change serialized the whole chat of every teammate that ever sent the agent a message, and read the full mailbox history again for each chat, which blocked the server for seconds when routines ran.
