### Fixed

- A provider-native context compaction blocks idle restarts, including group contexts with no active SQL turn.
- The private server status socket reports native work counts and restart reasons. Pending sign-in blocks its idle status. Missing native status remains unknown.
