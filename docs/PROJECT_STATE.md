# TradeWatch — project state and handover

**As of:** 2026-09-30, after Phase 9.
**Audience:** whoever — or whatever — picks this up next, with no prior context.

---

## 0. How to read this document

This is the one file to read before touching anything. It describes **what exists**, not what
is planned, and it is explicit about the difference — because the repository's other
documents are not always.

That distinction matters more here than in most projects:

- **`docs/BACKFILL.md`** describes history and bar aggregation as built — read it with
  `docs/PIPELINE.md`, which describes the streaming path.
- **`ARCHITECTURE.md` describes the finished system**, including components that do not
  exist yet. It is a specification, not a description. Sections carry "as built" notes where
  reality has caught up; everything else is still a plan.
- **`DEVELOPMENT_PLAN.md` describes the route**, in twelve phases. Phases 0–9 are built;
  10–12 are not started.
- **`ADR/` records decisions**, including ones for phases not yet built. An ADR being
  "Accepted" means the decision is made, not that the code exists.
- **This file is the reconciliation** between those three and the actual state of the tree.

If this document and any other disagree, this document is either right or out of date —
check `git log` for what has landed since 2026-09-30 and trust the code.

---

## 1. What TradeWatch is

A market intelligence and paper-trading platform, built as a portfolio-grade system rather
than a demo. The intended end state: live market data ingestion, historical storage,
configurable trading strategies, backtesting, simulated trading, detection of statistically
unusual market activity, and alerting — behind a FastAPI backend and a React dashboard.

### What it deliberately is not

These are load-bearing constraints, stated in the README and enforced throughout. Do not
weaken them:

- **It simulates trading.** No brokerage connectivity, no real money, ever.
- **Its detection component identifies statistically unusual market activity relative to a
  symbol's own recent behaviour.** It does *not* detect market manipulation, fraud, or
  financial crime, and must never be described as doing so (ADR-0008). This wording is
  checked by a Phase 8 exit criterion that scans code, UI, docs and commit messages.
- **No performance figure appears anywhere** unless a benchmark in `benchmarks/` produced
  it, with methodology and hardware recorded. `benchmarks/` is currently empty; therefore no
  number may be quoted about this system's performance.

### The house style, which is the actual deliverable

The code reads like it was written by someone explaining their reasoning to a reviewer.
Every non-obvious decision carries a comment saying *why*, usually naming the failure it
prevents. Docstrings explain trade-offs, not signatures. Tests are named as sentences
describing the property they pin.

This is not decoration and it is not optional — it is the thing that makes the repository
worth looking at. **Match it.** A correct patch written in a flatter style is a regression.

---

## 2. Status at a glance

| Phase | Name | Status |
| --- | --- | --- |
| 0 | Foundations | **Complete** |
| 1 | Vertical slice | **Complete** |
| 2 | Kafka backbone and service split | **Complete** |
| 3 | Real market data and historical depth | **Complete** (see the caveat in §16) |
| 4 | Live streaming and caching | **Complete** |
| 5 | Authentication, authorization, multi-tenancy | **Complete** |
| 6 | Strategy core and backtesting | **Complete** |
| 7 | Paper trading | **Complete** |
| 8 | Anomaly detection | **Complete** (frontend gap closed — see §16) |
| 9 | Alerts and notifications | **Complete** |
| 10 | Observability and performance measurement | **Complete** (findings in `PERFORMANCE.md`) |
| 11 | End-to-end and failure-recovery hardening | Not started ← **next** |
| 12 | AWS deployment | Not started |

**What runs today, end to end:**

```
Provider → normalize → Ingestion → Kafka
                                      │
                        ┌─────────────┼──────────────┬──────────────┬───────────────┐
                        ▼             ▼              ▼              ▼               ▼
                   Processor     Aggregator     Broadcaster    Paper trading   Anomaly detector
                        │             │              │              │               │
                        ▼             ▼              ▼              ▼               ▼
                   PostgreSQL    PostgreSQL     Redis Pub/Sub   PostgreSQL      PostgreSQL
                        │                            │                              │
                        │                            ▼                              ▼
                        │                       /ws/market                   tw.anomalies.v1
                        ▼                                                           │
                  FastAPI (auth,                                                    ▼
                  owner-scoped                                               Alert dispatcher
                  resources) → React                                       → PostgreSQL, /ws/alerts
```

Nine long-running OS processes (`tradewatch-api`, `-producer`, `-consumer`, `-aggregator`,
`-broadcaster`, `-backtest-runner`, `-paper-trading`, `-anomaly-detector`,
`-alert-dispatcher`) plus `-backfill`, a one-shot job. **Six consumer groups** read from
shared topics with independent offsets, the fan-out ADR-0001 chose Kafka for. Any of them can
be restarted, or be absent, without the others losing an event (§9's "Consumer groups").

The React app is no longer a single market view: sign-in/sign-up, a strategy editor and
backtest runner, a portfolio/deployment view, an anomaly feed with a per-detection
explanation panel, and an alert centre all sit behind `ProtectedRoute`, live-updated over
`/ws/market` and `/ws/alerts` (§13 has the full page list).

**Test and check status, current:**

1218 backend tests pass (841 unit/API, 377 integration against real PostgreSQL, Redis and
Kafka — the CI workflow now provisions all three for `backend-integration`, §9's "Consumer
groups"), plus 4 `provider`-marked tests that stay opt-in. 85 frontend tests across 14 files.
ruff, ruff-format, mypy strict, import-linter (5 contracts), ESLint, Prettier and `tsc` all
clean. The two known flaky tests (the WebSocket token-expiry race and the Kafka topic-partition
metadata race) were fixed in Phase 10 — see §16.

**The work is committed and pushed.** Every phase through 9 is merged into `main`, most
recently via a `--no-ff` merge per phase branch (`git log --graph` shows the shape). Commit
and push access is no longer withheld from an AI session working this repository — see §20,
which now reflects that.

---

## 3. Tech stack and versions

### Backend

| | |
| --- | --- |
| Language | Python, `>=3.13,<3.15`; 3.13.15 in use |
| Package/venv manager | `uv` (0.12.x). It installs its own Python |
| Web | FastAPI ≥0.118 on uvicorn ≥0.38 |
| Validation | Pydantic ≥2.11, pydantic-settings ≥2.11 |
| Database | PostgreSQL 17, SQLAlchemy 2.0 async + asyncpg ≥0.30 |
| Migrations | Alembic ≥1.16 |
| Broker client | `aiokafka` 0.14.0 |
| Logging | structlog ≥25.4 |
| HTTP client | `httpx2` ≥2.13 |
| Lint/format | ruff ≥0.14 (replaces black, isort, flake8) |
| Types | mypy ≥1.18, `strict = true`, over `src/` **and** `tests/` |
| Layering | import-linter ≥2.3 |
| Tests | pytest ≥8.4, pytest-asyncio ≥1.2 (`asyncio_mode = auto`), pytest-cov |

### Frontend

React 19.3, TypeScript 6, Vite 8.3, Vitest 5 + jsdom, Testing Library, Recharts 3.10,
ESLint 10, Prettier 3.9. Node 22+ required; Node 24.21 / npm 12 in use.

### Infrastructure (local, Docker Compose)

| Service | Image | Host port | Notes |
| --- | --- | --- | --- |
| PostgreSQL | `postgres:17-alpine` | `127.0.0.1:5432` | Also creates `tradewatch_test` on first start |
| Redis | `redis:8-alpine` | `127.0.0.1:6379` | Persistence **off** on purpose (ADR-0003). Caching, Pub/Sub fan-out (Phase 4), and rate limiting (Phase 5) |
| Kafka | `apache/kafka:4.0.0` | `127.0.0.1:9092` | Single broker, KRaft, no ZooKeeper |

All bind to loopback only: development credentials must not be reachable from the network.

---

## 4. Repository layout

```
tradewatch/
├── backend/
│   ├── src/tradewatch/
│   │   ├── domain/            Entities, events, enums, ids — no framework, no I/O
│   │   ├── application/       Use cases; depends on ports, never adapters
│   │   │   ├── ports/         event_bus, market_data, market_store, bar_store
│   │   │   ├── ingestion/     IngestionService, EventDeduplicator
│   │   │   ├── processing/    MarketDataProcessor, MarketDataConsumerService, errors
│   │   │   ├── aggregation/   BarAggregator, BarAggregationService
│   │   │   ├── backfill/      BackfillService
│   │   │   └── quality/       gap, session and invariant checks
│   │   ├── infrastructure/
│   │   │   ├── database/      models, session, market_store, bar_store, types, base
│   │   │   ├── repositories/  market, trading, base
│   │   │   ├── events/        envelope, memory (in-process bus), kafka/
│   │   │   └── providers/     synthetic, replay, alpaca, normalization, resilience, factory, _walk
│   │   ├── api/               FastAPI app, routes, schemas, dependencies, errors
│   │   ├── workers/           producer, consumer, bar_aggregator, backfill, ingestion, pipeline
│   │   ├── config/            settings.py (every knob), env.py
│   │   └── observability/     structured logging (metrics and tracing are Phase 10)
│   ├── alembic/               One migration: initial schema
│   ├── tests/{unit,integration,api,provider}/
│   └── pyproject.toml         Dependencies and every tool's configuration
├── frontend/src/              React app: api/, components/, hooks/, lib/, test-support/
├── docker/                    Compose stack, Dockerfile.backend, postgres-init
├── scripts/                   setup and check, in bash and PowerShell
├── benchmarks/                Empty. The only permitted source of a performance number
├── docs/                      SCHEMA, PIPELINE, INGESTION, this file
├── ADR/                       14 decision records
├── ARCHITECTURE.md            The target system design
├── DEVELOPMENT_PLAN.md        Phases 0–12 with exit criteria
└── CLAUDE.md                  Rules for AI agents working in this repo
```

---

## 5. Architectural rules — the non-negotiables

The layering is not a convention. It is checked in CI by `import-linter`, and there are five
contracts in `pyproject.toml`:

1. **Layered architecture** — `api | workers` → `application` → `domain`.
2. **Domain is independent of infrastructure** — `domain` may not import `infrastructure`,
   `api` or `workers`.
3. **Application depends on ports, not adapters** — `application` may not import
   `infrastructure` at all.
4. **Domain is independent of frameworks** — no fastapi, sqlalchemy, redis, uvicorn in
   `domain`.
5. **Kafka is an adapter** — `domain`, `application` and `api` may not import `aiokafka` or
   `kafka`. Added in Phase 2, because the thing that would quietly undo the seam is one
   `from aiokafka import ...` in a service.

### The ports

Everything crossing a layer goes through a `Protocol` in `application/ports/`:

| Port | Purpose | Adapters |
| --- | --- | --- |
| `MarketDataProvider` | Where market data comes from | `SyntheticProvider`, `ReplayProvider`, `AlpacaProvider` |
| `EventPublisher` | Where events go | `KafkaEventPublisher`, `InMemoryEventBus`, `RecordingPublisher`, `FailingPublisher` |
| `EventConsumer` | Single-event consumption (Phase 1) | `InMemoryEventBus` |
| `EventBatchConsumer` | Batch consumption with explicit ack (Phase 2) | `KafkaEventConsumer`, `InMemoryEventBus` |
| `DeadLetterSink` | Quarantine an unprocessable event (Phase 2) | `KafkaDeadLetterSink` |
| `MarketDataStore` | Durable persistence for the processor | `SqlMarketDataStore` |
| `BarStore` | Bars, read and written (aggregator, backfill, quality) | `SqlBarStore` |

Adapters raise only the exception types the port documents. `SqlMarketDataStore` translates
every SQLAlchemy and driver error into `StorageError` (retryable) or
`UnprocessableEventError` (permanent), so the processor asks one question: `retryable?`.
The Kafka adapters do the same with `BrokerUnavailableError` and `KafkaPublishError`, both
subclassing the Phase 1 `EventBusError` — which is why swapping the bus for a broker needed
no change in `IngestionService`.

---

## 6. The domain model

`domain/` is pure Pydantic. `DomainModel` is `frozen=True`, `extra="forbid"`,
`str_strip_whitespace=True`, and normalises every `datetime` to UTC, rejecting naive ones.

### Events (`domain/events.py`)

`MarketEvent` is the base: `event_id`, `event_type`, `schema_version`, `symbol`,
`occurred_at`, `ingested_at`, `source`, `producer`, `correlation_id`. Subclasses:

- `TradeEvent` — `price`, `size`, `provider_trade_id`, `conditions`
- `QuoteEvent` — `bid_price`, `bid_size`, `ask_price`, `ask_size`
- `BarEvent` — `interval`, `bar_start`, OHLCV, `vwap`, `trade_count`, `revision`

**Three timestamps, never conflated:** `occurred_at` is the venue's, `ingested_at` is ours,
`recorded_at` belongs to the database. All business logic uses `occurred_at` (F12).

**`event_id` is deterministic** — a UUIDv5 over a natural key built from the provider's own
data (ADR-0012). The same trade normalized twice produces the same id twice. This is the
foundation everything else rests on: it lets `UNIQUE (event_id)` absorb duplicates that
originate at the *provider* — a reconnect replay, an overlapping backfill, a restart — not
just duplicates introduced by Kafka. **Do not break this.** Changing `TRADEWATCH_NAMESPACE`
or any `natural_key()` would silently disable deduplication for all existing data.

`partition_key` returns the symbol. That is the Kafka key, and it is what guarantees
per-symbol ordering.

### Money

`Decimal` everywhere, never float (ADR-0009). Serialized to JSON as **strings**, both on
Kafka and in API responses, because a JSON number becomes an IEEE-754 double in every
JavaScript client.

---

## 7. The database

PostgreSQL is the **system of record** (ADR-0002). Kafka is a transport with finite
retention and is never the source of truth.

**21 application tables** (plus `alembic_version`) and **12 enum types**, all created by one
migration: `20260919_0419_61421a4db42f_initial_schema.py`.

```
Reference   instruments
Market      trades  quotes  bars
Identity    users  refresh_tokens  audit_log
Strategy    strategies  strategy_deployments
Backtest    backtest_runs  backtest_trades  backtest_equity_points
Trading     portfolios  positions  orders  fills  equity_points
Detection   anomaly_detections  model_versions
Alerting    alert_rules  alerts
```

**The whole schema was built in Phase 1, deliberately**, rather than the three tables
originally planned. `DEVELOPMENT_PLAN.md` §14's ordering constraint says *auth must land
before any multi-user feature stores user data, or every table needs a retrofit migration*;
creating `users` alongside the tables that reference it satisfied that at the schema level
for the price of one migration instead of five. The authentication **logic** is still
Phase 5.

**Consequence for anyone building Phases 5–9:** your tables already exist. Do not create new
ones; read `docs/SCHEMA.md` and use what is there.

### The idempotency guarantees

| Table | Mechanism |
| --- | --- |
| `trades` | `UNIQUE (event_id)`, written with `ON CONFLICT (event_id) DO NOTHING` |
| `quotes` | `UNIQUE (event_id)`, same |
| `bars` | PK `(instrument_id, interval, bar_start)`, upsert guarded by `WHERE revision <= excluded.revision` |
| `consumer_progress` | PK `(consumer_group, stream)`, `position` upserted to `greatest(stored, incoming)` |

**Read that table carefully, because it is easy to read wrong.** The first three rows make a
*write* idempotent. They do not make a **consumer** idempotent, and for the bar aggregator the
difference is the whole ballgame. Writing a trade is a replacement: do it twice and the row is
the same row. Folding a trade into a bar is an *addition*: do it twice and its size is in the
volume twice, and the revision-guarded upsert dutifully stores the doubled figure, because a
doubled bar satisfies every OHLC constraint the database has.

That bug was real and shipped. `consumer_progress` is the fix: the aggregator records how far
it has applied each topic-partition **in the same transaction as the bars**, and skips any
event at or below that mark ([ADR-0016](../ADR/ADR-0016-idempotent-bar-aggregation-under-redelivery.md)).

**If you add a consumer that accumulates anything — a running total, a counter, a position —
nothing forces you to use this and you will reproduce the bug.** The test that catches it is
not "replay a batch and assert the same state"; it is *rewind the consumer group's offsets and
assert the additive fields did not move*.

`bars` also carries `CHECK` constraints for every OHLC relationship (`high >= low`,
`open BETWEEN low AND high`, `close BETWEEN low AND high`, positive prices, non-negative
volume). A bar that breaks one is not a bar, and the database refuses it regardless of which
writer produced it.

Note a real behavioural detail: re-delivering an identical bar **re-upserts** it, rewriting
the row and moving `updated_at`. The final state is identical; it is not a no-op. A test
asserts this honestly rather than claiming otherwise.

---

## 8. The pipeline as built

Full detail lives in `docs/PIPELINE.md`. The summary:

```
SyntheticProvider (seeded, deterministic, no network)
  → Normalization (UTC, exact decimals, deterministic ids)
    → IngestionService (dedupe in flight, count, publish)
      → KafkaEventPublisher (acks=all, idempotent, keyed by symbol)
═══════════ process boundary ═══════════
        → Kafka: tw.market.{trades,quotes,bars.1m}.v1  (+ tw.dlq.v1)
═══════════ process boundary ═══════════
          → KafkaEventConsumer (decode, validate, quarantine what cannot be read)
            → MarketDataConsumerService (store the batch, THEN commit the offset)
              → MarketDataProcessor (batch, one transaction per flush, idempotent)
                → PostgreSQL
                  → FastAPI (money as strings)
                    → React price chart
```

### The two processes

| | Producer | Consumer |
| --- | --- | --- |
| Talks to the provider | Yes | Never |
| Talks to PostgreSQL | Instrument bootstrap only, best effort | Yes |
| Produces to Kafka | Market topics | Dead-letter topic only |
| Consumes from Kafka | Never | Market topics |
| Restart cost | The gap between trades | A redelivery, absorbed |

**Start order does not matter.** Start the consumer first and it waits; start the producer
first and the consumer catches up when it arrives.

---

## 9. Kafka

### Topology

| Topic | Key | Partitions | Retention | Producer | Consumer |
| --- | --- | --- | --- | --- | --- |
| `tw.market.trades.v1` | symbol | 6 | 7d | producer | `processor-persist` |
| `tw.market.quotes.v1` | symbol | 6 | 24h | producer | `processor-persist` |
| `tw.market.bars.1m.v1` | symbol | 6 | 30d | producer | `processor-persist` |
| `tw.dlq.v1` | original | 6 | 30d | consumer | an operator |

Naming: `<prefix>.<domain>.<entity>[.<qualifier>].v<major>`. The prefix is configuration
(`TW_KAFKA_TOPIC_PREFIX`); everything after it is a contract.

**Four topics exist and all four are used.** `ARCHITECTURE.md` §7.2 also specifies
`tw.signals.v1`, `tw.orders.v1`, `tw.fills.v1`, `tw.anomalies.v1` and `tw.alerts.v1`. None is
created — nothing produces them yet. Create each in the phase that produces it.

**Only 1-minute bars are published.** Higher intervals are derived from stored 1m bars until
the Phase 3 aggregator exists; a `BarEvent` on any other interval is refused by the producer
rather than routed to an invented topic.

**Broker-side auto-creation is off.** `ensure_topics()` runs at worker startup
(`TW_KAFKA_ENSURE_TOPICS`), is idempotent, and is safe to call concurrently. An implicitly
created topic would get one partition and infinite retention.

### The event envelope

```json
{"event_id","event_type","schema_version","symbol","occurred_at","ingested_at",
 "source","producer","correlation_id","payload":{...}}
```

Transport metadata at the top level, the domain's own fields nested under `payload`. That
split is what lets a consumer read the envelope of a message it cannot interpret.

- **Deterministic encoding:** sorted keys, fixed separators, `allow_nan=False`. Byte-stable
  for a given event.
- **Money as strings** (ADR-0009).
- **Versioning (ADR-0005):** compatible changes add optional fields that consumers ignore —
  `decode()` filters the payload to fields the target model declares, because the domain
  models are `extra="forbid"`. A **breaking** change gets a new topic with an incremented
  major version. `schema_version` is validated *before* the payload, so an incompatible
  message reports "unsupported version" rather than a confusing field error.
- Supported versions: `{1}`.

### Delivery semantics

**At-least-once, end to end.** `acks=all`, `enable_idempotence=True`, acknowledgements
awaited, offsets committed after the write, one transaction per flush.

**This is not exactly-once and nothing may claim it is.** Producer idempotence covers retries
within one session; it does not cover a producer that restarts and republishes. Kafka
transactions were rejected: they narrow the duplicate window without closing it and would not
remove the need for idempotent writes.

### Ordering

Keyed by symbol. **Guaranteed:** all events for one symbol land on one partition, in order —
what the Phase 3 aggregator and every strategy will depend on. **Not guaranteed, not
claimed:** any ordering across symbols.

### Failure classification

This is the centre of the design. Getting it wrong produces the two worst outcomes
available: a poison message that stops a partition forever, or a database outage that
silently discards an hour of market data.

| Failure | Class | Behaviour |
| --- | --- | --- |
| Broker unreachable | Retryable | Bounded retries; ingestion counts and continues (F4) |
| Message too large, topic missing | Permanent | `KafkaPublishError`, not retried |
| Event no topic carries | Permanent | Refused and counted |
| Malformed message | Permanent | Quarantined by the consumer adapter; partition keeps moving (F3) |
| Unsupported `schema_version` | Permanent | Quarantined — a half-done rollout, made visible |
| Invalid payload | Permanent | Quarantined |
| PostgreSQL down | Retryable | Retried; if exhausted, **offset not committed**, worker exits non-zero |
| Constraint violation | Permanent | Batch replayed one event at a time; poison event quarantined |
| Consumer restart | — | Resumes from last committed offset; redelivery absorbed |
| Consumer offline | — | Becomes lag, not loss |
| Cancellation | — | Buffer dropped without committing |

**The one rule above all others:** the offset is committed *after* the write, never before.

### Dead letters

`tw.dlq.v1` receives a message only when processing it can never succeed. A retryable failure
never goes there — quarantining during an outage converts a recoverable incident into a
manual replay.

A record carries the **original bytes verbatim** and the original key, plus headers:
`tw-dlq-reason`, `-error`, `-source-topic`, `-source-partition`, `-source-offset`,
`-consumer-group`, `-attempts`, `-failed-at`.

**There is no redrive tool, deliberately.** Every reason a message is there is a reason that
repeating it unchanged would fail again. A failure to *write* to the DLQ propagates and
blocks the commit.

### Consumer groups

One group per logical consumer. There are two: `processor-persist` (trades, quotes, bars →
PostgreSQL) and `bar-aggregator` (trades → 1m bars). They hold independent offsets on the
same topic and cannot affect each other, which is the fan-out ADR-0001 chose Kafka for.

**Auto-commit is off and must stay off.** It acknowledges messages that have been *fetched*,
not *stored*, converting at-least-once into at-most-once.

---

## 10. Configuration

Every tunable lives in `backend/src/tradewatch/config/settings.py` and nowhere else. Flat
`TW_`-prefixed environment variables, grouped into `BaseSettings` classes, composed and
cross-validated by `load_settings()`, which reports **every** problem at once as environment
variable names.

Groups: `app`, `market`, `provider`, `kafka`, `database`, `redis`, `execution`, `anomaly`,
`alerts`, `security`, `api`.

**Three values have no default** and the application refuses to start without them:
`TW_DATABASE_URL`, `TW_REDIS_URL`, `TW_JWT_SECRET`.

`.env.example` documents every variable with its default and the reason it exists. `.env` is
git-ignored. The rule: *if a number could reasonably differ in another environment, on
another venue, or after calibration against real data, it is a setting rather than a literal.*

Every settings group is read by production code as of Phase 9: `execution` by the backtest
runner and paper trading engine, `anomaly` by the detection service, `alerts` by the
dispatcher, `security` by the auth flow, and `redis` for caching (Phase 4) as well as
`cache_enabled`.

---

## 11. Running it locally

### Prerequisites

`uv`, Node 22+, Docker Desktop.

### First run

```bash
./scripts/setup.sh                      # macOS / Linux / Git Bash
powershell -File scripts/setup.ps1      # Windows

docker compose -f docker/docker-compose.yml up -d     # PostgreSQL + Redis + Kafka
cd backend && uv run alembic upgrade head
```

### One command

```bash
powershell -File scripts/dev.ps1        # Windows
./scripts/dev.sh                        # macOS / Linux / Git Bash
```

Starts Docker infrastructure, applies migrations, then runs the API, all eight workers and
the frontend, restarting any process that exits until Ctrl+C. That restart loop is the point:
every Kafka worker deliberately exits when the broker evicts it (a laptop sleeping is enough),
and before this script nothing brought them back — a backtest then sat on "Waiting to start…"
forever. Logs land in `backend/.run-logs/`. Each process still runs alone with
`cd backend && uv run tradewatch-<name>`.

Open `http://localhost:5173`, not `127.0.0.1:5173`: the API is `localhost:8010`, and the
refresh cookie is `SameSite=Lax`, so from `127.0.0.1` the browser treats it as cross-site and a
page reload signs you out.

The API's default port is **8010**, not the conventional 8000 — changed 2026-09-27 after a
stale dev-server process from an earlier session was found permanently squatting on host port
8000 on one development machine, answering requests with a months-old route set, with no
Windows tool (`netstat`, `Get-Process`, `Get-CimInstance`, `Stop-Process`) able to identify or
kill the process actually holding it — a suspected Docker Desktop/WSL2 networking artifact.
Set via `TW_API_PORT` / `VITE_API_BASE_URL` (`.env.example`); change both together if you pick
a different port for your own machine.

### Entry points

| Command | What it does |
| --- | --- |
| `tradewatch-producer` | Provider → Kafka. The primary path |
| `tradewatch-consumer` | Kafka → PostgreSQL. The primary path |
| `tradewatch-aggregator` | Kafka → 1m bars → 5m/1h/1d rollups. Its own consumer group |
| `tradewatch-backfill` | One-shot history fetch. Idempotent and resumable |
| `tradewatch-api` | The FastAPI gateway |
| `tradewatch-broadcaster` | Kafka → Redis Pub/Sub → `/ws/market` (Phase 4) |
| `tradewatch-backtest-runner` | Polls `backtest_runs` with `FOR UPDATE SKIP LOCKED`; retries a dropped database connection with backoff instead of exiting (Phase 6) |
| `tradewatch-paper-trading` | Runs deployed strategies against live 1m bars (Phase 7) |
| `tradewatch-anomaly-detector` | Scores bars, writes detections, publishes `tw.anomalies.v1` (Phase 8) |
| `tradewatch-alert-dispatcher` | Matches anomalies and fills against alert rules → `/ws/alerts` (Phase 9) |
| `tradewatch-ingest` | Ingestion alone, to a bus nothing drains. For watching normalization |
| `tradewatch-pipeline` | The Phase 1 single process, in-memory bus. For development with no broker |

`tradewatch-pipeline` is kept on purpose but is **not** the development default. It has no
replay, no restart safety, no independent failure domains, and — because an in-process queue
applies backpressure by refusing — it sheds live trades when the consumer falls behind. In
practice it shed 100% of them.

### Checks

```bash
./scripts/check.sh              # everything, in CI's order
./scripts/check.sh --backend    # one side
./scripts/check.sh --fix        # format and autofix first
```

### Useful Kafka commands

```bash
K='docker exec tradewatch-kafka /opt/kafka/bin'
$K/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
$K/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group processor-persist --describe
$K/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic tw.dlq.v1 \
    --from-beginning --property print.headers=true
```

---

## 12. The API surface

**Implemented today, by router:**

| Router | Purpose |
| --- | --- |
| `health` | `GET /health` (liveness, deliberately not database-backed — a liveness probe that fails during an outage turns it into a restart loop) and `GET /ready` (checks each registered dependency) |
| `market` | `GET /api/v1/instruments[/{symbol}]`, `GET /api/v1/market/{symbol}/bars`, `GET /api/v1/market/{symbol}/latest` — read-only, unauthenticated, market data belongs to no one user |
| `auth` | `POST /api/v1/auth/{register,login,refresh,logout}`, `GET /api/v1/auth/me` (Phase 5, ADR-0010) |
| `strategies` | Owner-scoped CRUD (Phase 6, ADR-0006) |
| `backtests` | Owner-scoped create/list/get, over the shared Execution Simulator (Phase 6, ADR-0007) |
| `portfolios` | Owner-scoped CRUD |
| `deployments` | Owner-scoped lifecycle: create/start/pause/resume/stop (Phase 7) |
| `anomalies` | `GET /api/v1/anomalies` — global, unauthenticated, same reasoning as `market` (Phase 8, ADR-0008) |
| `alerts` | Owner-scoped alert-rule CRUD, `GET /api/v1/alerts`, `POST /api/v1/alerts/{id}/ack` (Phase 9) |
| `websocket` | `/ws/market` (Phase 4) and `/ws/alerts` (Phase 9) — one `ConnectionManager` class, two instances, filtering by `symbol` and `user_id` respectively |

Every owner-scoped router goes through `OwnedRepository` (§5's "The ports" and
`infrastructure/repositories/base.py`): a resource that exists but belongs to someone else is
a 404, never a 403, so its existence can't be probed. `market` and `anomalies` are the two
deliberate exceptions — data that belongs to the market, not to a user (documented in each
router's own module docstring).

**Status codes carry meaning:**

- **404** — unknown resource, or one that exists but isn't the caller's.
- **422** — the request is wrong: reversed range, naive timestamp, limit out of bounds, an
  unknown `strategy_type`, an `alert_rules.event_types` value the dispatcher never consumes.
- **409** — a uniqueness constraint (a duplicate strategy or alert-rule name for the same
  user).
- **200 with an empty list** — the request was fine and there is no data. A market that has
  not opened is not an error; returning 404 makes a normal Sunday indistinguishable from a
  typo. This is the one most often got wrong.
- **503** — database unreachable. Retry later. Not 500.

Errors use one envelope: `{"error": {"code", "message", "details", "request_id"}}`. Every
response carries `X-Request-ID`.

**Operational surface (Phase 10):** `GET /ready` reports `database` (critical), `redis` and
`kafka` (reported, never fatal) by name, each with its own deadline, run concurrently — a hung
dependency shows as `timeout` instead of hanging the probe. Prometheus metrics are **not** on
this router: every process, the API included, serves `/metrics` on its own loopback port
(9310–9318, `observability/metrics.py`), so the endpoint cannot become public by a missing
auth rule (ADR-0017).

---

## 13. The frontend

Routes, all but `/login`/`/register` behind `ProtectedRoute` and rendered inside `AppShell`
(`App.tsx`) — a fixed sidebar (Dashboard, Market, Strategies, Backtests, Portfolios, Alerts,
Anomalies), the signed-in email and Sign out at its foot, and a footer stating the
simulation-only scope:

| Route | Component | What it does |
| --- | --- | --- |
| `/` | `Dashboard` | Overview cards: watchlist with last prices, portfolios, recent alerts, recent anomalies, recent backtests — each linking to its page |
| `/market` | `MarketView` | The original symbol selector, latest-price panel, price chart and live trade tape (it was the landing page until the design pass) |
| `/login`, `/register` | `LoginPage`, `RegisterPage` | Phase 5 auth flow |
| `/strategies`, `/strategies/new` | `StrategiesPage`, `StrategyEditor` | Owner-scoped strategy CRUD (Phase 6). Archiving frees the name — the uniqueness index is partial over `NOT is_archived` |
| `/backtests` | `BacktestsPage` | Every run, newest first, with status (added by the design pass) |
| `/backtests/new`, `/backtests/:id` | `BacktestForm`, `BacktestResults` | Run and read a backtest against the shared Execution Simulator: metrics, equity curve, trade list |
| `/portfolios`, `/portfolios/:id` | `PortfoliosPage`, `PortfolioDetailPage` | Owner-scoped portfolios and deployments (Phase 7) |
| `/anomalies` | `AnomalyFeed` | The feed, a symbol filter that doubles as score history, and a per-detection explanation panel over `GET /api/v1/anomalies` (Phase 8 — this page was the one deliverable Phase 8 shipped without; closed in this pass) |
| `/alerts` | `AlertCenter` | Alert-rule builder, rule list, and a live-updating feed over `/ws/alerts` (Phase 9) |

Each `api/*.ts` module is a small, self-contained fetch wrapper with its own `ApiError`,
mirroring the matching `api/schemas/*.py` file — deliberately duplicated rather than shared,
same reasoning `api/strategies.ts`'s own docstring gives: temporary, until `ARCHITECTURE.md`
§8.3's generated-from-OpenAPI types land.

Every page still handles the same four states MarketView established, because getting them
wrong is what makes a data UI feel broken: **loading**, **error** (with the API's message and
a retry), **empty** (a known symbol/resource with no data — not an error), **success**. Live
pages (`/ws/market`, `/ws/alerts`) add a fifth: connection status, reconnecting with backoff
rather than going silent.

Charting is Recharts (ADR-0013), chosen because SVG output can be asserted against in jsdom.
A canvas chart renders nothing a test can see, so the end-to-end claim would be unverifiable.
The anomaly feed's "score history" is a filtered, newest-first table rather than a second
chart — the feed already reads as a timeline once narrowed to one symbol, and a charting
dependency Phase 8 did not need was not added for it.

**Visual design.** One design pass (commit `44f2f21`, 2026-10-07) replaced the plain markup
with a dark, terminal-style theme. Every colour, space, radius and font is a custom property
in `styles/tokens.css` (`--color-accent`, `--color-border`, …); `App.css` uses only those.
Charts take their stroke colours from the same tokens — and must use the *current* names: the
pass renamed `--accent`/`--grid`, the two Recharts components kept the old ones, and both chart
lines rendered invisibly (an undefined `var()` is `stroke: none`) until 2026-10-08.

**Known gaps:** API types are hand-written rather than generated from the committed OpenAPI
schema (§8.3 wants generation, and it needs a codegen step). The price chart has fixed pixel
dimensions so it is testable in jsdom; responsive rendering needs a `ResizeObserver` polyfill.
The hand-written types are also how `/backtests/:id` went blank: the frontend read
`points` where the API sends `equity_curve`, and the component test stubbed the frontend's own
name, so it passed. Fixed 2026-10-08; generated types are the real cure.

---

## 14. Testing

### Philosophy

- **No mocking of the database** (`ARCHITECTURE.md` §11.2). An in-memory SQLite substitute
  would test a different database — no native enums, no JSONB, no partial indexes, no
  `ON CONFLICT`, different transaction semantics.
- **Integration tests skip with an explanation** when infrastructure is unreachable, rather
  than failing with a wall of connection errors.
- **The schema is built by the real Alembic migrations**, not `metadata.create_all` — which
  would test the models against themselves and never notice a migration that disagrees.
- **Each test builds its own rows.** Nothing depends on data another test wrote.
- **Warnings are errors** (`filterwarnings = ["error", ...]`). A DeprecationWarning is a
  scheduled breakage.
- Live provider tests are opt-in (`-m provider`) so `pytest` never reaches the network.

### Markers

`unit`, `integration`, `api`, `e2e`, `provider`, `slow`.

### Running

```bash
cd backend
uv run pytest -m 'not integration and not e2e and not provider'   # fast
TW_TEST_DATABASE_URL=postgresql+asyncpg://tradewatch:tradewatch@localhost:5432/tradewatch_test \
  uv run pytest -m integration                                    # needs PostgreSQL + Kafka
cd frontend && npm run test
```

Integration tests run against `TW_TEST_DATABASE_URL`, defaulting to `TW_DATABASE_URL` with
`_test` appended. **Never the application database** — the fixtures drop and recreate the
public schema.

### Where things are tested

| File | What |
| --- | --- |
| `unit/test_entities.py`, `test_ids.py` | Domain invariants, deterministic ids |
| `unit/test_normalization.py` | Timestamps in every format, decimals, symbols, event identity |
| `unit/test_ingestion.py` | Dedupe, shedding order, reconnect accounting |
| `unit/test_resilience.py` | Reconnection with backoff, staleness detection |
| `unit/test_processor.py` | Batching, dedupe accounting, retries, every failure path |
| `unit/test_event_envelope.py` | Serialization, determinism, money as strings, malformed shapes, versioning |
| `unit/test_kafka_topics.py` | Topic naming, routing, partition keys |
| `unit/test_kafka_producer.py` | Topic, key, batching, retries, broker failures, **session recovery** |
| `unit/test_kafka_consumer.py` | Decoding, quarantine, DLQ headers, poison-poll commit, auto-commit off |
| `unit/test_kafka_admin.py` | Topic creation, idempotence, partitions, retention |
| `unit/test_market_data_consumer.py` | **Commit ordering**, duplicates, storage failure, poison isolation |
| `unit/test_settings.py` | Every validator, including the Kafka cross-field rules |
| `integration/test_kafka_pipeline.py` | **The whole path against a real broker and database** |
| `integration/test_vertical_slice.py` | The Phase 1 in-process path |
| `integration/test_processor_failures.py` | Constraint violations, rollback, restart idempotency |
| `integration/test_market_api.py` | Every endpoint and failure mode |
| `integration/test_migrations.py` | Models match migrations; up→down→up |
| `frontend/src/components/MarketView.test.tsx` | The four states; a chart path from API data |

### The tests that matter most

`integration/test_kafka_pipeline.py` asserts the **exact OHLCV** the API returns is the data
the provider generated, bar by bar — not a row count, which is how an end-to-end test passes
on data that happened to already be there. It also proves, against a real broker, that
reprocessing changes nothing, that a restarted consumer does not re-read what it finished,
that an offline consumer catches up, that a database failure leaves the offset uncommitted,
and — by stopping and starting the Compose container — that a broker outage costs lag and not
data.

The unit suites exist because a real broker will not fail on demand. The adapters declare
narrow `ProducerClient` / `ConsumerClient` protocols over the handful of methods they call,
so a client that refuses to connect, times out twice then succeeds, or fails to commit is a
few lines of test code. **Use those protocols; do not patch `aiokafka`.**

---

## 15. The ADRs

Immutable once accepted. A decision that changes gets a **new** ADR superseding the old one.

| ADR | Decision | Built? |
| --- | --- | --- |
| 0000 | Record architecture decisions | — |
| 0001 | Kafka as the event backbone, introduced in Phase 2 | **Yes** |
| 0002 | PostgreSQL as the system of record | **Yes** |
| 0003 | Redis: a bounded, justified scope (cache + fan-out, never truth) | **Yes** — Phase 4 |
| 0004 | Abstract the market data provider; default to synthetic | **Yes** |
| 0005 | JSON + Pydantic event serialization, versioned by topic | **Yes** |
| 0006 | Strategies are configuration, **not user code** | **Yes** — Phase 6 |
| 0007 | One strategy and execution core shared by backtest and paper trading | **Yes** — Phase 6/7 |
| 0008 | Anomaly detection: scope, method, and honest claims | **Yes** — Phase 8 |
| 0009 | Decimal money, never floats | **Yes** |
| 0010 | JWT access tokens with rotating refresh tokens | **Yes** — Phase 5 |
| 0011 | Alpaca as the market data provider (closes D1) | Adapter exists; still never run against the live API — see §18 |
| 0012 | Deterministic event identity for market data | **Yes** |
| 0013 | Recharts for the price chart (closes D12) | **Yes** |
| 0014 | Consumer acknowledgement boundary and dead-letter policy | **Yes** |
| 0015 | Event-time bar aggregation, lateness window, corrections | **Yes** — Phase 3 |
| 0016 | Idempotent bar aggregation under redelivery (supersedes 0015's restart story) | **Yes** — Phase 3 |

**Two that will bite whoever extends the strategy or execution code next, so read them
first:**

- **ADR-0006** — strategies are declarative configuration, not arbitrary code. Do not build a
  plugin system. D17 ("strategy plugin system for user-supplied code") is marked *probably
  never*.
- **ADR-0007** — the execution simulator is a **shared library** used by both backtesting and
  paper trading (built, Phase 6/7). Splitting it into a backtest-only engine would make the
  backtest/paper consistency test — named in the plan's risk table as "the tripwire," and
  passing today — impossible without a rewrite.

---

## 16. What has been done, phase by phase

### Phase 0 — Foundations ✅

Repository scaffolding, typed configuration, structured logging, CI, Docker Compose, the
tooling matrix, ADRs 0000–0010.

### Phase 1 — Vertical slice ✅

Synthetic market data reaching a chart in a browser through a real database.

- `MarketDataProvider` port; `SyntheticProvider` (seeded value-noise, deterministic),
  `ReplayProvider`, `AlpacaProvider`.
- Normalization to domain events with deterministic ids; `EventDeduplicator`.
- `IngestionService` with its failure posture: malformed messages counted (F3), reconnection
  with backoff (F1), staleness watching (F2), and a shedding order that drops quotes before
  trades (F4).
- **The full PostgreSQL schema** with Alembic migrations and async SQLAlchemy repositories.
- `MarketDataProcessor`: batching, one transaction per flush, idempotent writes, bounded
  storage retries, a counter at every point where an error could otherwise be swallowed.
- `EventPublisher` / `EventConsumer` ports and the `InMemoryEventBus`.
- Market API and a React chart.
- ADRs 0011, 0012, 0013.

### Phase 2 — Kafka backbone and service split ✅

**Delivered:**

- Kafka (KRaft, single broker) in Compose, auto-creation off.
- Idempotent topic creation at worker startup, with §7.2's partition counts and retention.
- Kafka adapters behind the ports: producer, consumer, dead-letter sink, admin, error
  classification. `IngestionService` and `MarketDataProcessor` were **not modified**.
- Two new protocols — `EventBatchConsumer` (durable messaging acknowledges in batches) and
  `DeadLetterSink` (the application must be able to request quarantine without naming a
  broker). Documented in ADR-0014.
- `MarketDataConsumerService`, which owns the acknowledgement boundary.
- Process split: `tradewatch-producer` and `tradewatch-consumer`.
- An explicit, versioned, deterministic event envelope.
- A fifth import-linter contract forbidding broker imports above infrastructure.

**Deliberately not delivered:**

- **Consumer lag metrics endpoint.** Lag is visible through `kafka-consumer-groups` and the
  counters; Phase 10 owns Prometheus and a metrics surface built here would be built twice.
- **A replay tool.** Replay *works* — a new consumer group reads a topic from the beginning,
  and a test asserts that doing so changes no stored data — but there is no command that
  replays from a given offset or timestamp. The first real customer is the Phase 3
  aggregator.
- **Dockerfiles and health endpoints for the workers.** Relevant when something schedules
  them (Phase 12).

---

### Phase 3 — Real market data and historical depth ✅

**Delivered:**

- The **Bar Aggregator** as its own service in its own consumer group, `bar-aggregator` —
  the first second consumer of `tw.market.trades.v1`, which is what makes ADR-0001's fan-out
  argument true rather than intended. Event-time closing, a bounded lateness window producing
  `revision`-bumped corrections (F11), restart recovery by persisting the open bar with
  `is_final = false` (F13), and **idempotent accumulation under redelivery** — the consumer
  records its stream position in the same transaction as the bars it wrote (ADR-0016). Proven
  by rewinding the group's offsets by 3,000 per partition and draining: 9,000 events
  redelivered, 9,000 recognised as already applied, 0 rows written, and a checksum over every
  1-minute bar identical before and after (`2a6f5b66ec0b175b94cf63e1ed00ba12`).
- **5m/1h/1d rollups** derived from stored 1m bars in SQL, on a timer and after a backfill.
- A **historical backfill** that is idempotent (verified by checksum over a real re-fetch of
  51,840 bars) and resumable from the data rather than a job table.
- **Data quality checks**: bar invariants, and gap detection filtered by a session calendar.
- **Validation of the Alpaca adapter**, which found three defects — see the caveat below.
- ADR-0015, recording the aggregation decisions and what they cost to get wrong, and
  ADR-0016, which supersedes its restart story after the redelivery bug below.

**Not delivered, and why:**

- **The Alpaca adapter has still never run against the live API.** There are no credentials
  in this environment. It was validated by reading it against its own documented constraints,
  which is how the three defects were found, but that is not the same as having been proven
  correct. `pytest -m provider` closes the gap and needs a key pair. **Do not describe the
  real provider as working until someone has run it.**
- **The aggregator does not publish bars to Kafka.** It writes them to PostgreSQL. Phase 4's
  browser broadcaster is the first thing that will want them as events; publishing them now
  would be a topic written to and never read.
- **Duplicate-print detection is a field on the quality report that is always zero.**
  Deterministic event ids (ADR-0012) stop a duplicate print reaching the database at all, so
  there is nothing yet for it to count. It stays as the place to put a real check if a
  provider is ever seen to send genuine duplicates under different ids.
- **A multi-hour backlog on a single symbol still defeats the lateness window (F11).** Draining
  it advances that symbol's watermark by hours in seconds, sealing its earlier bars, and trades
  still queued behind the jump are counted in `late_dropped` and discarded. Reported, not
  silent. Deliberately left for **Phase 11 (failure recovery)**: it is orthogonal to redelivery,
  ADR-0016 makes no claim about it, and the fix is recovery-mode behaviour — size the window in
  stream time, or route a detected backlog through the backfill path instead of the live
  aggregator. Written up under "Known limitations against this catalogue" in `ARCHITECTURE.md` §12.

---

### Phase 4 — Live streaming and caching ✅

**Delivered:** `/ws/market` with subscribe/unsubscribe; a Kafka→WebSocket broadcaster
(`tradewatch-broadcaster`, the third independent reader of the trade/quote/bar topics) fanning
out over Redis Pub/Sub; a bounded per-connection queue implementing the F19 slow-consumer
drop-and-resync policy (a client that falls behind gets one `market.resync` message instead of
an unbounded backlog); cache-aside reads for instruments/bars/latest-price with TTLs and
explicit invalidation; a reconnecting frontend WebSocket hook with `event_id`-deduped trade
tape, live price, and a connection-status badge.
**Exit, confirmed:** a trade appears in the browser without a refresh; `TW_CACHE_ENABLED=false`
degrades to uncached reads rather than failing (F9), and is tested that way.

### Phase 5 — Authentication, authorization, multi-tenancy ✅

**Delivered:** Argon2id hashing; register/login/refresh-with-rotation/logout/me; roles;
**ownership enforcement in the repository layer** — `OwnedRepository` (§5) takes an actor on
every method and returns `None` for a row that doesn't exist *or* isn't the caller's, so a
route can't forget the check; Redis-backed rate limiting; security headers; `audit_log`; the
frontend auth flow (`LoginPage`, `RegisterPage`, `ProtectedRoute`).
**Exit, confirmed:** every owner-scoped endpoint 404s a non-owner rather than 403ing (which
would leak that the resource exists); refresh-token reuse detection revokes the session; 429
carries `Retry-After`; an expired token closes an open WebSocket.
**One flaky test surfaced here, still open** — see the Phase 11 entry below.

### Phase 6 — Strategy core and backtesting ✅

**Delivered:** a pure, incremental indicator library (SMA, EMA, RSI, ATR, rolling
volatility); the strategy core's `evaluate(state, bar) -> (state, signals)` contract; the
**Execution Simulator as a shared library** (ADR-0007 — the same code backtests and paper
trading both run through, not two engines that could disagree); a Backtest Runner worker;
owner-scoped strategy and backtest endpoints; a strategy editor and equity-curve UI.
**Exit, confirmed:** the golden backtest passes against a committed fixture dataset; running
the same backtest twice produces identical results including the trade sequence; a lookahead
test fails if a strategy is handed a future bar; changing `TW_EXEC_SLIPPAGE_BPS` changes both
the result and the stored `execution_config`.

### Phase 7 — Paper trading ✅

**Delivered:** a live strategy driver consuming `tw.market.bars.1m.v1` with per-deployment
state restored on start (`started_at` anchors the replay window, untouched by pause/resume —
see the deployments router's own reasoning); the Paper Trading Engine (position sizing, risk
limits, the F15 order-rate ceiling) built on Phase 6's shared Execution Simulator; owner-scoped
deployment lifecycle endpoints (`create → start ⇄ pause → stop`, each transition rejecting the
wrong starting status with 409); a position-rebuild job regenerating positions from fills
alone; a portfolio/deployment UI.
**Found and fixed in this phase's own audit:** a real layering violation
(`PaperTradingService` had drifted into `application/`, which may not import
`infrastructure` — moved to `workers/`), plus a rollup-bucket truncation bug, a cache
refresh-rate bug, multi-signal data loss, and an RSI bounds mismatch — all fixed before the
phase closed, not left as known issues.

### Phase 8 — Anomaly detection ✅

**Delivered:** a shared feature pipeline (return, realized volatility, volume/trade-count
z-score, spread) used identically by training and serving, to rule out train/serve skew; a
robust median/MAD z-score baseline that ships first and is the F17 fallback if no model is
active; an offline `IsolationForest` training pipeline (`scripts/train_anomaly_model.py`) that
only activates a model if it beats the baseline on held-out, chronologically-split data — and
records the comparison either way; the detection service publishing `tw.anomalies.v1`;
`GET /api/v1/anomalies`; a written model card (`docs/MODEL_CARD.md`); a grep-based test
enforcing ADR-0008's wording constraint across code, docs, commit messages, and the frontend.
**Exit, confirmed:** injected synthetic anomalies are detected; the ML detector is compared
against the baseline and only goes live if it wins; every stored detection is explainable from
its own `features` payload; no text anywhere claims detection of manipulation, fraud, or
financial crime.
**Gap found and closed in this pass, not at the time:** the anomaly feed, score history, and
per-detection explanation panel `DEVELOPMENT_PLAN.md` committed to as a Phase 8 frontend
deliverable were silently never built — the backend and its tests were complete and this one
page was skipped without being flagged anywhere. Built now (`/anomalies`, §13) rather than
left open, since the gap was real and Phase 9 depends on the same UI conventions.

### Phase 9 — Alerts and notifications ✅

**Delivered:** rule evaluation spanning **both** tracks — `application/alerts/rules.py`
matches a rule against `AnomalyEvent` (Phase 8) and `OrderFilledEvent` (Phase 7), the one
point the trading and detection tracks meet, in one dispatcher consumer group; an Alert
Dispatcher with dedupe keys (`UNIQUE (alert_rule_id, dedupe_key)`, the same idempotency
pattern `orders.client_order_id` uses for a redelivered bar), a per-(rule, symbol) cooldown,
and a per-user hourly cap with a digest (F18) rather than either flooding a user during a
volatile session or going silent about the rest of the storm; `tw.alerts.v1` relayed live over
`/ws/alerts`, which **generalizes** Phase 4's `ConnectionManager` (a `filter_key` parameter —
`symbol` for `/ws/market`, `user_id` for `/ws/alerts`) rather than duplicating it; owner-scoped
alert-rule CRUD and an alert feed with acknowledge; a frontend alert centre.
**Exit, confirmed:** a burst of 10 qualifying events against a cap of 3 produces exactly 3
alerts and one digest, not ten; a rule matching nothing, or a disabled rule, produces nothing;
a cross-user delivery test confirms alerts reach only their owner; acknowledging an
already-acknowledged alert is still a 204, proving idempotence.
**A pre-existing CI gap found and closed alongside this phase:** `backend-integration` had no
Redis or Kafka service, so `test_auth_routes.py` (Phase 5, unrelated to Phase 9) errored on
every run and `test_kafka_pipeline.py`/`test_bar_aggregator.py` silently skipped. Both
services are now provisioned in `.github/workflows/ci.yml`, matching `docker-compose.yml`'s
config; a warm-up step (`scripts/wait_for_kafka_metadata.py`) absorbs a measured
~2-second metadata-propagation lag a freshly-started single-node Kafka broker has that a
long-lived one does not.

### Phase 10 — Observability and performance measurement ✅

**Delivered:** Prometheus metrics in all nine processes, defined in one module
(`observability/metrics.py`) with bounded labels only — events published and consumed per
type and group, delivery delay (`ingested_at` → consumed) and per-batch processing time per
consumer group (measured once, in the shared `KafkaEventConsumer`), provider lag, HTTP
requests and latency by **route template**, read-through cache hits and misses, open
WebSocket connections, SQLAlchemy pool state, strategy signals, orders by outcome, anomalies
by severity, alerts by type, and log lines at warning and above as the error rate.
Consumer-group lag comes from `kafka-exporter`, outside the consumers, so it stays visible
while a consumer is down. Prometheus, Grafana (port **3300**) and `kafka-exporter` are in the
Compose stack; the dashboard is `docker/grafana/dashboards/tradewatch.json`, provisioned as
Grafana's home page. Correlation ids are bound into the log context by the shared consumer for
every batch, and each consumer logs a `kafka.consumer_progress` line every 30 s carrying it.
`/ready` names each dependency. Five benchmarks in `benchmarks/` and `PERFORMANCE.md`
(ADR-0017 records the decisions).
**Exit, confirmed:** every scrape target `up` in Prometheus with live values (verified by
querying it, not by reading code); metrics tests read `/metrics` back over HTTP through the
reference parser; one correlation id traced in real logs from a published bar through paper
trading and the anomaly detector, into a derived anomaly event, through the alert dispatcher,
to a stored alert 222 ms later; every figure in `PERFORMANCE.md` links to a committed result
file with its commit, configuration and hardware.
**Not done, and why:** *HTTP → Kafka* correlation has no subject yet — no HTTP route publishes
to Kafka in this codebase (backtests queue through PostgreSQL, deployments are rows the paper
worker reads). The request id is bound into every API log line; the first route that publishes
should take its event `correlation_id` from it (`ARCHITECTURE.md` §8.3). A nightly
performance workflow in CI is also not built: the benchmarks need a broker, Docker and Chrome,
and their numbers would describe a CI runner, not the system.
**Found by measuring — see `PERFORMANCE.md` "What the measurements found":** no process
publishes live 1m bars, so live paper trading and anomaly detection have no input; the
synthetic provider's trade ids restart every run, so new trades are dropped as duplicates; the
broadcaster replays its backlog as live after downtime; a broker outage leaves a gap in the
tape (the producer stalls, nothing catches up). None was fixed here — Phase 10's brief was
measure first.
**Also fixed:** the WebSocket token-expiry test (whole `with` inside `pytest.raises`, three
seconds of validity); the topic-partition test (polls `describe_topics` until partitions
appear); `/ready` reporting a running Kafka as `timeout` on Windows (IPv6 `::1` tried first,
~2 s refusal — now Happy Eyeballs); `dev.ps1` hanging the API's auto-reload and truncating a
crashed worker's log on restart; Grafana's port clashing with other local dev servers.

---

## 17. What remains

### Phase 11 — End-to-end and failure-recovery hardening ← **next**

Playwright E2E; a failure-injection harness; **a test for each of F1–F20**; a chaos runbook;
a load test with the shedding order proven.

### Phase 12 — AWS deployment

D6/D7/D8/D9 closed with written cost comparisons; infrastructure as code; managed PostgreSQL
and cache; CD with gated migrations and rollback; a cost report against a stated ceiling.

### The dependency graph

```
P0 → P1 → P2 → P3 → P4 → P5 → P6 → P7 → P9 → P10 → P11 → P12
                              ↘ P8 ↗
```

From `ARCHITECTURE.md` §14, where an arrow means *must exist and work before*:

```
Bar Aggregator      → Strategy core + indicator library
Auth (ownership)    → Strategy core + indicator library
Strategy core       → Execution Simulator → Backtest Runner → Backtest UI
Execution Simulator → Paper Trading Engine
```

The trading track (6→7) and the detection track (8) depended only on **Phase 5 and on bars
from Phase 3**, and were built in that order; Phase 9 needed both, and was built last.

**A caveat this graph doesn't remove just because every phase now exists:** backtest metrics
(CAGR, Sharpe, drawdown) are only as meaningful as the historical depth behind them. A
backtest run against a thin synthetic window produces an authoritative-looking number that
means nothing — check how much history is actually loaded (§16's Phase 3 entry, the backfill
job) before trusting a result, not just whether the endpoint returns one.

---

## 18. Known limitations and open concerns

**Architectural:**

- **Nothing publishes live 1m bars to Kafka** (found in Phase 10). The producer publishes
  trades and quotes; the aggregator writes bars to PostgreSQL only; the backfill writes nothing
  to Kafka. Live paper trading and the anomaly detector therefore receive no input in the
  running stack. The aggregator publishing its closed 1m bars is the likely fix.
- **Synthetic trade ids collide across producer runs** (found in Phase 10). `{symbol}-{sequence}`
  with the sequence restarting at zero means every run after the first re-emits stored ids, and
  the idempotent insert drops them. The dashboard's watchlist price (from the trades table)
  goes stale while the live Market page does not.
- **The broadcaster resumes from its committed offset**, so after downtime it relays a backlog
  as if it were live. For a live relay, starting from the newest offset is probably right.
- **A broker outage leaves a gap in the tape**: the producer stalls on the broker (one failed
  publish in a 60 s outage, not thousands), the synthetic feed generates nothing meanwhile,
  and nothing catches up — ~3,550 trades' worth of tape is simply absent (`PERFORMANCE.md`).
- **The Alpaca adapter has never run against the live API.** No credentials here. See §16.
- **One aggregator instance.** Per-symbol state lives in its memory, so a second instance
  needs that state to survive a partition reassignment — the point at which the
  stream-framework question (D5) genuinely opens.
- **A correction cannot move `open` or `close` on a bar reloaded from the database**, because
  the stored bar does not record when its trades happened. It adjusts high, low, volume, VWAP
  and trade count instead (ADR-0015).
- **The session calendar has no holidays.** It filters a report, and an unlisted holiday shows
  up as a gap an operator recognises.
- **The producer opens a database connection** for the instrument bootstrap. Reference data
  is not market data and has nowhere else to go yet, so it is best effort — if PostgreSQL is
  unreachable the producer carries on and the consumer registers provisional instruments. The
  proper fix is a reference-data sync of its own.
- **No redrive for the DLQ.** Deliberate; quarantined messages need manual work.
- **No replay command.** Replay works; there is no tool.
- **One broker, replication factor 1.** `acks=all` means less than it sounds like.
- **No WebSocket to the browser.** The chart fetches once. Phase 4.
- **Nothing uses Redis yet** despite it running in Compose.
- **No worker Dockerfiles or health endpoints.**
- **Frontend API types are hand-written** rather than generated from the OpenAPI schema.
- **`benchmarks/` is empty**, so no performance claim may be made about anything.

**Open decisions** (`ARCHITECTURE.md` §15, D1 and D12 closed): D2 asset classes beyond US
equities · D3 Avro/Protobuf + Schema Registry · D4 partitioning/TimescaleDB · D5 stream
processing framework · D6 multi-broker Kafka · D7 AWS compute model · D8 managed vs
self-hosted Kafka · D9 auth provider · D10 ML model family · D11 feature store · D13 frontend
state library · D14 notification channels · D15 multi-currency · D16 shorting/margin/leverage
· D17 strategy plugin system (*probably never*) · D18 horizontal scaling model · D19 retention
and archival · D20 monorepo vs split repos.

Each names the trigger that should force the decision. Do not close one without an ADR.

---

## 19. Traps and hard-won knowledge

Things that cost time and will cost it again.

### Environment

- **`.env` pointed at a throwaway database.** A previous session started a PostgreSQL inside
  its own session temp folder on port **55432** and pointed `.env` at it, while the Compose
  database on 5432 sat empty. Temp folders get wiped. `.env` now points at
  `localhost:5432`; the original is saved as `.env.bak`. **Check `TW_DATABASE_URL` before
  believing anything about the data.**
- **Kafka advertises two listeners.** Containers reach the broker as `kafka:19092`; the host
  reaches it as `127.0.0.1:9092`. Using the wrong one gives a client that connects, fetches
  metadata, then times out talking to an address it cannot route to.
- **Git Bash mangles container paths.** `docker exec ... /opt/kafka/bin/...` becomes
  `C:/Program Files/Git/opt/kafka/...`. Prefix with `MSYS_NO_PATHCONV=1`.
- **The API holds `.venv` executables open.** `uv add` fails with "process cannot access the
  file" while `tradewatch-api` is running. Stop it first.

### Bugs found in Phase 2, and what they teach

1. **A broker restart permanently poisons an idempotent producer.** Kafka numbers records per
   partition and the broker tracks that state; when it is lost, every send fails
   `OutOfOrderSequenceNumber` forever. `aiokafka` marks it non-retryable — true of that
   producer, misleading about the system, since a new producer works immediately. The symptom
   is the worst kind: connected, no outage reported, silently delivering nothing.
   `KafkaEventPublisher` now detects session-fatal errors and rebuilds.
2. **Closing a poisoned producer hangs forever.** `AIOKafkaProducer.stop()` flushes first, and
   the batches it holds are the ones being refused. Wrapping the `await` in `asyncio.timeout`
   *looks* correct and does nothing, because `stop()` blocks inside its own `asyncio.wait` and
   does not surface outside cancellation. The close is now waited on as a **task**, so the
   bound is enforced by our code rather than requested of someone else's.
3. **Rebuilding left a window with no client.** Dropping the old client before starting the
   new one means a concurrent publisher gets "producer is not started". Ingestion runs two
   streams through one producer, so the window is not theoretical. The replacement is now
   built before the old one is released, with a generation counter so two streams rebuild
   once.
4. **A poll where every record is malformed loops forever.** The batch is empty, so the
   application will not commit, and the same poison records return on every poll. The adapter
   commits them itself once their dead-letter writes are acknowledged.
5. **A failed `start()` leaked the client**, producing an unraisable error from a finaliser at
   a random later moment.

**The lesson worth carrying:** items 2 and 3 were *introduced* by the fix for item 1 and were
found only by running the real thing against a real broker. The unit tests passed throughout.
Verify against live infrastructure before believing a fix.

### The Phase 3 bug worth more than the rest put together

**The bar aggregator double-counted every redelivered trade**, and it did so for as long as
the aggregator existed.

- **What was wrong.** `UNIQUE (event_id)` and the revision-guarded upsert make a *write*
  idempotent. The aggregator does not write per event, it **accumulates**: `volume += size`,
  `trade_count += 1`, `notional += price * size`. At-least-once delivery hands the same trade
  over again after any restart, rebalance or offset reset, and every one of those additions
  happened twice.
- **Why nobody noticed.** `open`, `high`, `low` and `close` are first, max, min and last. All
  four are idempotent. A doubled bar therefore has **perfect prices**, satisfies every OHLC
  `CHECK` the database enforces, and renders as a completely ordinary candle. Only `volume`,
  `trade_count` and `vwap` are wrong — the three figures a volume-weighted strategy prices off.
- **Why the tests missed it.** The restart tests killed the aggregator and resumed it with
  *new* trades. Resuming is the easy half. Nothing ever handed the **same trade** to two runs,
  so nothing could fail when production did.
- **How it was caught.** By distrusting an earlier "proof". A restart incident had produced
  exactly 470 new trade rows for 470 new trades, which was read as evidence of idempotency —
  it was not, it was un-consumed *backlog*, and backlog proves nothing about redelivery. The
  real test was to rewind the consumer group's offsets by 3,000 across three partitions and
  drain the lag. Every additive field came back exactly 2×: AAPL 14:28 went 26 → 52 trades,
  6,828 → 13,656 volume, checksum `f010999066c559d2f1664d2e75e4075a` → `61497122929723a80269f689d5ed72ce`.
- **A second trigger, likelier than a rewind.** Graceful shutdown closed the open bars and
  wrote them **without committing offsets**, on the theory that a restart would rebuild better
  ones. It would not — they are written `is_final = true`, `restore()` loads only non-final
  bars, and the redelivered trades reopened them as a correction and added their volume on
  top. An ordinary Ctrl-C was enough to do it.
- **The fix.** `consumer_progress`: the service records how far it has applied each
  topic-partition in the **same transaction** as the bars, and skips anything at or below that
  mark (ADR-0016). Re-run of the same rewind afterwards: 9,000 redelivered, 9,000
  `already_applied`, 0 rows written, checksum `2a6f5b66ec0b175b94cf63e1ed00ba12` identical
  before and after.

**Three lessons, in order of how much they will cost you.**

1. **"Idempotent writes" is not "idempotent consumer."** It holds when the write is a
   *replacement* and fails when it is an *addition*. Ask which one yours is before claiming the
   guarantee. Anything that accumulates needs to record its own position.
2. **A test that replays a batch is not a test of redelivery.** Redelivery means the *same*
   event reaching a consumer that has already applied it — across a restart, with the state on
   disk. Write the test that rewinds offsets and asserts the additive fields did not move.
3. **Interrogate evidence that agrees with you.** The 470-rows observation was accepted because
   it matched the expected answer. It was measuring something else entirely.

### Two pre-existing test bugs, both masked by the old setup

- `tests/api/test_health.py` asserted `/ready` returns 503 by assuming **no** database is
  running — while pointing at `localhost:5432`, the database the README tells you to start. It
  now points at a closed port.
- `tests/integration/test_migrations.py` built a URL from `str(engine.url)`, which masks the
  password as `***`. It passed only against a trust-auth PostgreSQL. It now uses
  `render_as_string(hide_password=False)`.

---

## 20. Rules for whoever works on this next

From `CLAUDE.md` and the repository's own standards:

1. **Commit and push are permitted** (updated from earlier phases, where they were withheld):
   plain `git commit -m` messages, no attribution lines, no heredocs. Merge each phase's
   branch into `main` with `--no-ff` once its own work is committed, matching `git log
   --graph`'s existing shape. Never force-push, never rewrite published history.
2. **Creating and editing files is fully permitted.**
3. **Do not weaken a check to make it pass.** No removed tests, no relaxed assertions, no
   loosened type checking, no skipped integration tests when the containers could be started.
   The one mypy override that exists (`aiokafka.*`, which ships no `py.typed`) is scoped to
   that import and documented in place.
4. **Every phase has a Definition of Done** in `DEVELOPMENT_PLAN.md`: exit criteria objectively
   verified; tests at the right levels; no hard-coded value that §16 says must be configurable;
   `docker compose up` still produces a working system from an empty database; OpenAPI schema
   and generated frontend types updated; migrations reversible and tested up→down→up; README
   updated; any §15 decision closed recorded as an ADR; no performance claim without a
   benchmark; no new dependency without a written justification.
5. **ADRs are immutable once accepted.** Supersede, never edit.
6. **Match the commenting style.** See §1.
7. **Report honestly.** If something is partially done, say which part. If a test proves
   something narrower than it sounds like, say what it actually proves.

---

## 21. Recommended next step

**Before Phase 11, fix the two pipeline correctness gaps Phase 10 found** (§18): publish live
1m bars to `tw.market.bars.1m.v1` (without them, live paper trading and anomaly detection do
nothing), and make synthetic trade ids unique across producer runs (without that, new trades
are silently discarded). Both are small, both have a metric that will show the fix working
(`tradewatch_events_published_total{event_type="market.bar.closed"}`, and the trades table's
newest `recorded_at`), and Phase 11's end-to-end tests would otherwise be written against a
pipeline that silently drops data.

**Then Phase 11 — end-to-end and failure-recovery hardening.** `playwright-core` is already a
frontend dev dependency (the end-to-end latency benchmark drives Chrome with it), the
recovery benchmark is the start of a failure-injection harness, and the Grafana dashboard is
how a chaos run will be read.
