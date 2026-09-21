# Docker

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Local development stack: PostgreSQL and Redis, plus the API behind the `app` profile. |
| `Dockerfile.backend` | Two-stage image for the Python services. Build context is the repository root. |
| `.dockerignore` (repo root) | Keeps virtualenvs, `node_modules`, and `.env` out of the build context. |

## Usage

```bash
docker compose -f docker/docker-compose.yml up -d          # infrastructure
docker compose -f docker/docker-compose.yml ps             # status and health
docker compose -f docker/docker-compose.yml logs -f        # follow logs
docker compose -f docker/docker-compose.yml down           # stop
docker compose -f docker/docker-compose.yml down -v        # stop and delete data
```

The API and the frontend normally run on the host during development — the reload loop is
faster and the debugger works without extra setup. `--profile app` runs the API in a
container, which is how to check the image itself is correct.

## Notes

* Both services bind to `127.0.0.1` only. They have development credentials, so they must
  not be reachable from the network.
* Redis runs with persistence **off** on purpose. It is a cache and a fan-out bus, never
  the system of record (ADR-0003); starting it this way means that assumption is exercised
  on every restart rather than merely asserted in a document.
* PostgreSQL is initialised with `--locale=C` and `TZ=UTC` so sort order and timestamps do
  not depend on the host's locale.
* Kafka joins this file in Phase 2, when there is a consumer that needs a durable,
  replayable log (ADR-0001).
