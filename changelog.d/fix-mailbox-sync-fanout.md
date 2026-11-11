### Fixed

- A queue change of an agent that many teammates write to no longer reads the whole mailbox for each of them. A teammate's chat is synced again only when a delivery of a message that it sent or got changed, when the queue around its queued message moved, or when its reactions changed. Teammate syncs of one tick run once, at the end of the tick. A chat read stops at the newest 100 completed messages and adds only the older messages that still run. The chats show the same messages and states as before.
