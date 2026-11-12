### Fixed

- Clear saved provider failures and usage limits when an account or host usage reset succeeds. Log the reason and return each released key once.
- Let one waiting turn probe a failed model every fifteen minutes. Return agents from fallback when the probe completes.
