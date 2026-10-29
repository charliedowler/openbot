### Fixed

- Agents can wait for events from group tasks. Before, the wait tool refused group work. A wake now continues the same task in its group, with its original owner and request. Cancelled or changed tasks do not restart.
- Group waits use an existing event inbox. They create no watch or recurring routine. A shared inbox can wake a wait for unrelated events, so the agent checks the source before it acts.
- Keep this update when reverting other changes while group event waits remain open. Older versions can continue those requests in the wrong chat. Keep the saved waits.
