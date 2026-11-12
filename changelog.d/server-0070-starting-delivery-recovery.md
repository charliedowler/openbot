### Fixed

- The server interrupts a delivery that stays in starting with no turn for more than ten minutes. Boot recovery and the reaper release its lane and record the reason.
- The host control socket accepts POST /v1/deliveries/cancel. The request requires an agent, a sender kind and a UTC cutoff. It cancels queued and turnless starting deliveries through the mailbox, returns their ids and records the reason.
