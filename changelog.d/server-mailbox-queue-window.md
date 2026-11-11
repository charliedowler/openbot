### Fixed

- A queue change of an agent with a long history no longer sends its whole history. `queue-changed` now holds every queued, starting and running delivery, the newest 100 completed and the newest 100 failed, interrupted or cancelled deliveries, and once more each delivery that ended since the last change. Each queue change reconciles the routine runs of the deliveries that have not ended, and a delivery that ended only once. The queue read (`listQueue`, the Team API queue route, and the queue events of a client without runtime snapshots) still gives the whole queue.
