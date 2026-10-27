### Fixed

- Keep the channel summary when an agent resumes after delegated work. Earlier messages stay valid, so the next request does not wait for the full history to be summarised again. A change to the request still invalidates the old context.
