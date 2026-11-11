### Changed

- An agent reads its queued teammate messages in one turn: when a turn starts with a teammate's message, it also takes the other queued teammate messages of the same lane, oldest first, up to 20 messages and 24 KB. Each message is labelled with its sender and time. A person's message never joins the batch, and the batch never passes it. Each message keeps its own delivery.
