### Fixed

- Channel routing, channel summary and profile turns on Claude now work with a Claude account folder that signs in only through `ANTHROPIC_BASE_URL` and `ANTHROPIC_AUTH_TOKEN` in its `settings.json`. These turns load no setting source, so they failed with "Not logged in" and the channel showed "The provider could not generate a profile". They now get those two values from the folder's `settings.json`, and still load none of its hooks.
- A failed channel summary now logs the causes of the failure, redacted as the routing log is, so the warning names the provider error.
