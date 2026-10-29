### Fixed

- A provider-native context compaction blocks idle restarts, including group contexts with no active SQL turn.
- The private server status socket reports native work counts and restart reasons. Pending sign-in blocks its idle status. Missing native status remains unknown.
- A native compaction blocker clears when its provider process ends, including compactions that have no active SQL turn.
