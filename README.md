# TradeWatch

A market intelligence and paper-trading platform: live market data ingestion, historical
storage, configurable trading strategies, backtesting, simulated trading, detection of
statistically unusual market activity, and alerting — behind a FastAPI backend and a React
dashboard.

**Status: Phase 1 complete — the first vertical slice is closed.** Market data travels the
whole path and is rendered in a browser:

```
SyntheticProvider → normalize → EventBus → Processor → PostgreSQL → FastAPI → React chart
```

There is no Kafka yet (the event bus is in-process), no strategy engine, no ML and no real
dashboard. See [docs/PIPELINE.md](docs/PIPELINE.md) for how the slice works,
[docs/SCHEMA.md](docs/SCHEMA.md) for the data model, and
[DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) for what comes next.

## Scope, stated plainly

TradeWatch **simulates** trading. It has no brokerage connectivity and moves no real money.

Its detection component identifies **statistically unusual market activity relative to a
symbol's own recent behaviour**. It does not detect market manipulation, fraud, or
financial crime, and is never described as doing so — see
[ADR-0008](ADR/ADR-0008-anomaly-detection-approach.md).

No performance figure appears anywhere in this repository unless a benchmark in
[`benchmarks/`](benchmarks/) produced it, with its methodology and hardware recorded.

---

## Quick start

**Prerequisites:** [uv](https://docs.astral.sh/uv/) (installs its own Python),
[Node.js](https://nodejs.org/) 22 or newer, and Docker Desktop for PostgreSQL.

```bash
git clone <repository-url> tradewatch
cd tradewatch

./scripts/setup.sh                      # macOS / Linux / Git Bash
powershell -File scripts/setup.ps1      # Windows PowerShell (pwsh works too)
```

That checks prerequisites, copies `.env.example` to `.env`, and installs both dependency
sets. Then, in two terminals:

```bash
cd backend  && uv run tradewatch-pipeline  # market data → PostgreSQL (no credentials needed)
cd backend  && uv run tradewatch-api       # http://127.0.0.1:8000  (docs at /docs)
cd frontend && npm run dev                 # http://127.0.0.1:5173 — a chart with real data
```

The dashboard shows whether it can reach the API. If it says *unreachable*, the backend is
not running or `VITE_API_BASE_URL` points somewhere else.

Infrastructure comes first, because the API checks the database on `/ready`:

```bash
docker compose -f docker/docker-compose.yml up -d     # PostgreSQL + Redis
cd backend && uv run alembic upgrade head             # create the schema
```

The Compose stack also creates a `tradewatch_test` database on first start, which is where
the integration tests run — never the application database.

```bash
docker compose -f docker/docker-compose.yml down      # stop (add -v to delete data)
```

Running PostgreSQL some other way is fine; point `TW_DATABASE_URL` at it, create a second
database ending in `_test`, and everything works.

---

## How the repository is organised

```
tradewatch/
├── backend/                   Python services
│   ├── src/tradewatch/
│   │   ├── domain/            Entities and pure business rules — no framework, no I/O
│   │   ├── application/       Use cases; depends on ports, never on adapters
│   │   ├── infrastructure/    Adapters: database models, repositories, sessions
│   │   ├── api/               FastAPI gateway — the only inbound door
│   │   ├── workers/           Consumers and jobs
│   │   ├── config/            Typed settings and .env loading
│   │   └── observability/     Structured logging (metrics and tracing in Phase 10)
│   ├── alembic/               Migrations; the schema's source of truth
│   ├── tests/{unit,integration,api}/
│   └── pyproject.toml         Dependencies and every tool's configuration
├── frontend/                  React + TypeScript + Vite dashboard
│   ├── src/                   Components, API client, configuration
│   └── tests/                 Vitest setup; specs sit next to the code they test
├── docker/                    Compose stack and the backend image
├── scripts/                   setup and check, in bash and PowerShell
├── benchmarks/                Measured results — the only source of any number quoted
├── docs/                      Documents belonging to no single component
├── ADR/                       Architecture decision records
├── ARCHITECTURE.md            The system design
└── DEVELOPMENT_PLAN.md        Phases 0–12 with exit criteria
```

The layering rule — `domain` imports nothing from `infrastructure`, `api`, `workers`, or
any framework — is enforced by `import-linter` in CI, not by good intentions.

## Where to start reading

1. [ARCHITECTURE.md](ARCHITECTURE.md) — components, data flow, schema, Kafka topics, API and
   auth boundaries, testing strategy, failure scenarios.
2. [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) — what is built in which order, and why that
   order.
3. [ADR/](ADR/) — the reasoning behind each decision, including what was rejected.
4. [docs/SCHEMA.md](docs/SCHEMA.md) — the data model and what is deliberately *not*
   constrained.
5. `backend/src/tradewatch/config/settings.py` — every knob the system has, in one file.

---

## Commands

Run everything CI runs, in CI's order:

```bash
./scripts/check.sh                      # macOS / Linux / Git Bash
powershell -File scripts/check.ps1      # Windows PowerShell (pwsh works too)

./scripts/check.sh --fix                # format and auto-fix first
./scripts/check.sh --backend            # one side only
```

### Backend (from `backend/`)

| Command | What it does |
| --- | --- |
| `uv sync --all-groups` | Install dependencies, including dev tools |
| `uv run tradewatch-api` | Start the API with reload |
| `uv run tradewatch-pipeline` | Ingest **and** persist market data — the vertical slice |
| `uv run tradewatch-ingest` | Ingestion only, publishing to a bus nothing drains |
| `uv run pytest` | All tests |
| `uv run pytest -m unit` | Unit tests only (fast) |
| `uv run pytest -m integration` | Against real PostgreSQL; skipped with a message if none is reachable |
| `uv run pytest -m provider` | Against the live market data provider; needs credentials, opt-in |
| `uv run alembic upgrade head` | Apply migrations |
| `uv run alembic downgrade -1` | Undo the last one |
| `uv run alembic check` | Verify the models match the migrations |
| `uv run pytest --cov` | With coverage |
| `uv run ruff check .` | Lint |
| `uv run ruff format .` | Format |
| `uv run mypy` | Type check (strict) |
| `uv run lint-imports` | Verify the architectural layering |

### Frontend (from `frontend/`)

| Command | What it does |
| --- | --- |
| `npm install` | Install dependencies |
| `npm run dev` | Dev server with hot reload |
| `npm run build` | Type check, then production build |
| `npm run test` | Tests |
| `npm run test:watch` | Tests in watch mode |
| `npm run test:coverage` | Tests with coverage |
| `npm run lint` | Lint |
| `npm run format` | Format |
| `npm run typecheck` | Type check |
| `npm run check` | All of the above |

---

## Configuration

All configuration is environment variables. `.env.example` lists every one with its default
and a note on what it is for; `.env` is git-ignored.

Three variables have **no default** and the application refuses to start without them:

| Variable | Why no default |
| --- | --- |
| `TW_DATABASE_URL` | Silently connecting to a database nobody chose is how test data ends up somewhere it should not be |
| `TW_REDIS_URL` | Same reasoning |
| `TW_JWT_SECRET` | A signing key with a default is a signing key that ships to production |

`TW_PROVIDER` selects the market data source — `synthetic` (the default; no key, no network,
deterministic), `replay`, or `alpaca`. Choosing a provider without its credentials fails at
startup rather than at the first request.

A missing or invalid value fails at startup and names **every** problem at once:

```
TradeWatch configuration is invalid (3 problems):
  TW_DATABASE_URL: is required but not set
  TW_REDIS_URL: is required but not set
  TW_JWT_SECRET: is required but not set

Copy .env.example to .env and fill in the values it lists, or export the variables above.
```

The full list, grouped and explained, is `ARCHITECTURE.md` §16. The principle: if a number
could reasonably differ in another environment, on another venue, or after being calibrated
against real data, it is configuration rather than a literal in the code. With `TW_ENV=prod`
the settings additionally refuse a placeholder or short signing key, wildcard CORS, and
non-JSON logging.

Frontend configuration uses Vite's `VITE_` prefix, which is a useful accident: only
`VITE_`-prefixed values reach browser code, so a backend secret cannot leak into the bundle.

---

## Testing

| Layer | Tool | Runs |
| --- | --- | --- |
| Backend unit | pytest | Every push |
| Backend API | pytest + `TestClient` | Every push |
| Backend integration | pytest + real PostgreSQL | Own CI job |
| Live provider | pytest, marked `provider` | Opt-in only — never in CI |
| Frontend unit | Vitest + Testing Library | Every push |
| End-to-end | Playwright | Phase 11 |
| Performance | k6 / Locust | Phase 10, nightly |

Four rules hold from here on. Tests never read the developer's environment — every `TW_`
variable is stripped before each test, so a test that passes on one machine passes on all of
them. Warnings are errors: a `DeprecationWarning` is a scheduled breakage, and finding it in
CI is cheaper than finding it during an upgrade. And **the database is never mocked**
(`ARCHITECTURE.md` §11.2) — integration tests build the schema by running the real
migrations, then roll back every test, because an in-memory substitute would be testing a
different database. And **only one directory touches the network**: `tests/provider/`,
deselected by default, so `pytest` can never reach a third party by accident and CI never
depends on one being up.

## Continuous integration

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs five independent jobs, so a
failure names itself rather than hiding behind another:

- **backend-quality** — ruff lint, ruff format check, mypy strict, import-linter layering
- **backend-test** — unit and API tests with coverage
- **backend-integration** — the full integration suite against a real PostgreSQL service,
  including migrations up, down and up again
- **backend-startup** — starts the API from `.env.example` and calls `/health`, then checks
  that removing `.env` produces a non-zero exit naming the missing variable. This is what
  stops `.env.example` from drifting away from the settings model
- **frontend** — eslint, prettier, tsc, vitest, production build
- **docker** — builds the backend image and validates the Compose file

## Contributing

Before opening a pull request: `./scripts/check.sh` passes, and
[DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md)'s "Definition of Done" holds — including a
one-line justification for any new dependency, and no performance claim without a benchmark
behind it.
