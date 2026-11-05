### Fixed

- The Tasks panel of a remote server that connects over WebRTC no longer shows "The Team API path is invalid.".
  The host now forwards `GET /v2/watch-registry`. It refuses all other `/v2/` paths, as before.
