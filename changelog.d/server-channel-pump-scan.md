### Fixed

- Keep the server responsive when a turn ends and a channel has a long task history. The channel scheduler read every task of the channel again for each task, which blocked the server for several seconds.
