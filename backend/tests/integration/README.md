# Integration tests

Tests that need real infrastructure — PostgreSQL, Redis, Kafka — started with
`testcontainers` rather than mocked. Empty until Phase 1, when the first repository
arrives.

The rule from `ARCHITECTURE.md` §11.2: repository tests run against real PostgreSQL. An
in-memory substitute would be testing a different database.

Run them with `uv run pytest -m integration`; CI runs them in their own job so a missing
Docker daemon fails loudly instead of silently skipping.
