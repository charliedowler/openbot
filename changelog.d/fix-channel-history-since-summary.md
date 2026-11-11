### Fixed

- A long channel no longer holds the server for seconds on each turn. The packet of a member, the summary check and the summary passes read only the messages after the channel's summary, and the few older messages that the packet names (the request, its quoted sources, the message that holds a long brief and the requests of stopped tasks) by id. A whole capture of a channel thread reads the messages after the summary and its own older rows by id. The packet is the same as before.
