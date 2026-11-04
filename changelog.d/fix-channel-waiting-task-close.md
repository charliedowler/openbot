### Fixed

- A channel task that waits for an event no longer shows "Working" for ever when its wait is closed
  from another task or is lost. It stops with Continue and Reassign, and a restart stops a task that is
  already in this state.
- The channel lead can close a waiting task that has no open wait. A task that waits for a closed task
  continues and reads it as cancelled. Before, the lead could not close the task, and the task above it
  waited for ever.
