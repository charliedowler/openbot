### Fixed

- A routine run in an agent's own chat now starts while the agent works on a channel task. Before,
  an agent with constant channel work kept its routine runs queued for hours. A message from a
  person, a teammate message and a routine run that waits behind one of them still wait for the
  channel work. An agent whose provider runs one turn at a time keeps the earlier order.
