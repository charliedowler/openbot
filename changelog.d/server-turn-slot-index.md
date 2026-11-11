### Fixed

- A server with a long mailbox and many agents no longer stalls its main thread when it decides which turn starts next. The turn slot check read every delivery once for each lane of each agent, and again for each lane that waited for a slot. It now reads the mailbox once for each decision. The same turns start as before.
