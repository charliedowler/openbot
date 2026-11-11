### Fixed

- A burst of channel changes no longer reads the channel list for each change. Each change sends `channels-changed`, and each Team API and app client then reads the list, whose query counts the unread messages of every channel. The server now reads the list at most once a second for each reader and gives the reads in that second the same list. When a change came after that read, the server sends `channels-changed` again at the end of the second, so each client reads the last state.
