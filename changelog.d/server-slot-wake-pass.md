### Fixed

- A server with many agents, a long mailbox and long channels no longer stalls its main thread each time a turn ends. Each end of a turn asked every lane that waited for a turn slot, and every other lane again for each of them, whether it could start, and each of these questions read every channel assignment from the database. A turn end now asks each lane once, the assignment state is kept until an assignment changes, and the statements are compiled once. The same turns start in the same order.
