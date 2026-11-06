### Added

- OpenBot now stops a stuck turn by itself. Once a minute it checks the running turns. It stops a turn when its provider process is gone and the turn made no progress for 8 minutes. It also stops a turn that no person started, such as a routine or webhook run, when the turn waits 8 minutes on a question or approval that nobody answers.
  The stop uses the same path as the Stop button. A live turn that makes progress is never stopped. OpenBot stops at most 2 turns for one agent in an hour, and after that it only logs. Each stop writes a log line and a `turn.reaper-stopped` event. `OPENBOT_TURN_REAPER=0` turns the check off, and `OPENBOT_TURN_REAPER_MINUTES` sets the idle limit.
