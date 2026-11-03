### Fixed

- A routine run in an agent's own chat now starts while the agent works on a channel task or
  compacts a channel thread. Before, an agent with constant channel work kept its routine runs
  queued for hours. A message from a person, and a routine run that waits behind one, still wait for
  the channel work. A routine run can pass a teammate message, which keeps its place in the queue.
  An agent whose provider runs one turn at a time keeps the earlier order.
- A signed webhook event that names a channel wait no longer starts an own-chat turn before the
  wait takes the event.
