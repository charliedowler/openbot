### Changed

- Serve context PRs from a per-chat reference index and a stored snapshot with `stale` and `refreshedAt`, and refresh open PRs in the background when a chat changes and every five minutes for chats read in the last day.
