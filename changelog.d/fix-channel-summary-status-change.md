### Fixed

- A channel no longer rebuilds its whole summary after a restart when only the status of an earlier
  message changed. Before, the rebuild made one model call for each part of the channel history,
  and the next assignment in the channel waited until it ended. A change to the text of an earlier
  message still rebuilds the summary.
