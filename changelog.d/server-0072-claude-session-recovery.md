### Fixed

- Recover a missing Claude session once with a fresh session and a bounded handoff.
- Start a rotated Claude session fresh after a restart when it has no saved transcript.
- Keep session and model errors local to the affected turn so the shared provider stays available.
