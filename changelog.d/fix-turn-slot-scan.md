### Fixed

- A hosted server with a turn limit no longer stops for several seconds when it checks whether a
  queued message can start. The check read the channel assignments of every agent for each queue.
  Webhook answers, chat loading and other requests waited for it.
