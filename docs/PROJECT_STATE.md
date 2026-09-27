# TradeWatch — project state and handover

**As of:** 2026-09-21, after Phase 3.
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
- **`DEVELOPMENT_PLAN.md` describes the route**, in twelve phases. Phases 0–3 are marked
  complete and are; 4–12 are not started.
- **`ADR/` records decisions**, including ones for phases not yet built. An ADR being
  "Accepted" means the decision is made, not that the code exists.
- **This file is the reconciliation** between those three and the actual state of the tree.

If this document and any other disagree, this document is either right or out of date —
check `git log` for what has landed since 2026-09-21 and trust the code.

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
| 4 | Live streaming and caching | Not started |
| 5 | Authentication, authorization, multi-tenancy | Not started |
| 6 | Strategy core and backtesting | Not started |
| 7 | Paper trading | Not started |
| 8 | Anomaly detection | Not started |
| 9 | Alerts and notifications | Not started |
| 10 | Observability and performance measurement | Not started |
| 11 | End-to-end and failure-recovery hardening | Not started |
| 12 | AWS deployment | Not started |

**What runs today, end to end:**

```
Provider → normalize → Ingestion → Kafka ─┬→ Processor  → PostgreSQL → FastAPI → React
                                          └→ Aggregator → PostgreSQL (1m bars → 5m/1h/1d)
```

Three independent OS processes and a one-shot backfill job. **Two consumer groups** read the
same trade topic with independent offsets — the fan-out ADR-0001 chose Kafka for. Any of them
can be restarted, or be absent, without the others losing an event.

**Test and check status at the close of Phase 2:**

See the Phase 3 report for the exact current numbers; as of the close of Phase 3 the suite
is roughly 630 backend unit/API tests, 245 backend integration tests, and 22 frontend tests,
with ruff, ruff-format, mypy strict, import-linter (5 contracts), ESLint, Prettier and tsc
all clean.

**The work is uncommitted.** `git log` shows two commits: `Initial commit` and the Phase 1
slice. All Phase 2 and Phase 3 work is in the working tree. Committing and pushing is reserved
for the repository owner — see §20.

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
| Redis | `redis:8-alpine` | `127.0.0.1:6379` | Persistence **off** on purpose (ADR-0003). Not yet used by any code |
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

Settings that exist but are **not yet read by any code**: the `execution`, `anomaly` and
`alerts` groups (Phases 6, 8, 9), and most of `security` (Phase 5). `redis` is read only for
`cache_enabled`; nothing caches yet.

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

### Four terminals

```bash
cd backend  && uv run tradewatch-producer  # provider → Kafka
cd backend  && uv run tradewatch-consumer  # Kafka → PostgreSQL
cd backend  && uv run tradewatch-api       # http://127.0.0.1:8010  (docs at /docs)
cd frontend && npm run dev                 # http://127.0.0.1:5173
```

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

**Implemented today — six routes, all read-only:**

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Liveness. Deliberately does **not** check the database: a liveness probe that fails during an outage turns it into a restart loop |
| `GET /ready` | Readiness. Checks each registered dependency |
| `GET /api/v1/instruments` | The universe; populates the symbol selector |
| `GET /api/v1/instruments/{symbol}` | One instrument |
| `GET /api/v1/market/{symbol}/bars` | OHLCV over `[start, end)`, with `interval` and `limit` |
| `GET /api/v1/market/{symbol}/latest` | Last stored trade and quote |

There is no client-facing route that writes a tick, a bar or an instrument, and a test
asserts no `/api/v1/market/*` route exposes anything but `GET`.

**Status codes carry meaning:**

- **404** — unknown symbol. Stop asking.
- **422** — the request is wrong: reversed range, naive timestamp, limit out of bounds.
- **200 with an empty list** — the request was fine and there is no data. A market that has
  not opened is not an error; returning 404 makes a normal Sunday indistinguishable from a
  typo. This is the one most often got wrong.
- **503** — database unreachable. Retry later. Not 500.

Errors use one envelope: `{"error": {"code", "message", "details", "request_id"}}`. Every
response carries `X-Request-ID`.

**Everything else in `ARCHITECTURE.md` §8.1 is unbuilt:** auth, strategies, backtests,
portfolios, deployments, anomalies, alerts, `/metrics`, and the entire WebSocket surface §8.2.

---

## 13. The frontend

Sixteen files. One view: a symbol selector, a price chart, the latest price.

```
src/api/{health,market}.ts      typed fetch wrappers
src/components/MarketView.tsx   the view
src/components/PriceChart.tsx   Recharts line chart
src/hooks/useAsync.ts           loading / error / data
src/lib/format.ts               money and time formatting
src/config.ts                   VITE_ configuration
```

It handles four states, because getting them wrong is what makes a data UI feel broken:
**loading**, **error** (with the API's message and a retry), **empty** (a known symbol with
no data — not an error), **success**.

Charting is Recharts (ADR-0013), chosen because SVG output can be asserted against in jsdom.
A canvas chart renders nothing a test can see, so the end-to-end claim would be unverifiable.

**Known gaps:** API types are hand-written rather than generated from the committed OpenAPI
schema (§8.3 wants generation, and it needs a codegen step). The chart has fixed pixel
dimensions so it is testable in jsdom; responsive rendering needs a `ResizeObserver` polyfill.

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
| 0003 | Redis: a bounded, justified scope (cache + fan-out, never truth) | No — Phase 4 |
| 0004 | Abstract the market data provider; default to synthetic | **Yes** |
| 0005 | JSON + Pydantic event serialization, versioned by topic | **Yes** |
| 0006 | Strategies are configuration, **not user code** | No — Phase 6 |
| 0007 | One strategy and execution core shared by backtest and paper trading | No — Phase 6/7 |
| 0008 | Anomaly detection: scope, method, and honest claims | No — Phase 8 |
| 0009 | Decimal money, never floats | **Yes** |
| 0010 | JWT access tokens with rotating refresh tokens | No — Phase 5 |
| 0011 | Alpaca as the market data provider (closes D1) | Adapter exists; unused |
| 0012 | Deterministic event identity for market data | **Yes** |
| 0013 | Recharts for the price chart (closes D12) | **Yes** |
| 0014 | Consumer acknowledgement boundary and dead-letter policy | **Yes** |

**Two that will bite whoever builds Phase 6, so read them first:**

- **ADR-0006** — strategies are declarative configuration, not arbitrary code. Do not build a
  plugin system. D17 ("strategy plugin system for user-supplied code") is marked *probably
  never*.
- **ADR-0007** — the execution simulator is a **shared library** used by both backtesting and
  paper trading. Building a backtest-only engine makes the Phase 7 backtest/paper consistency
  test — named in the plan's risk table as "the tripwire" — impossible without a rewrite.

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

## 17. What remains

### Phase 4 — Live streaming and caching ← **next**

WebSocket `/ws/market` with subscribe/unsubscribe; a Kafka→WebSocket broadcaster with Redis
Pub/Sub fan-out; slow-consumer handling (F19); Redis caching per §10 with TTLs and
invalidation; a live chart and trade tape.
**Exit:** a trade appears in the browser without a refresh; with `TW_CACHE_ENABLED=false`
every test still passes; stopping Redis degrades but does not break (F9).

### Phase 5 — Authentication, authorization, multi-tenancy

Argon2id hashing; register/login/refresh-with-rotation/logout/me; roles; **ownership
enforcement in the repository layer** (every owner-scoped query takes an actor); Redis rate
limiting; security headers; `audit_log`; the frontend auth flow.
**Exit:** for every owner-scoped endpoint, a test that user B gets 404 on user A's resource;
refresh reuse detection revokes; 429 with `Retry-After`; an expired token closes an open
WebSocket.

### Phase 6 — Strategy core and backtesting

Indicator library (SMA, EMA, RSI, ATR, rolling volatility — pure and incremental); the
strategy core's `evaluate(state, bar) -> (state, signals)` contract; the **Execution
Simulator as a shared library** (ADR-0007); a Backtest Runner worker; strategy CRUD and
backtest endpoints; a strategy editor and equity-curve UI.
**Exit:** the golden backtest passes against a committed fixture dataset; running the same
backtest twice produces identical results *including the trade sequence*; a lookahead test
fails if a strategy is given a future bar; property-based tests on position and P&L
arithmetic; changing `TW_EXEC_SLIPPAGE_BPS` changes the result **and** the stored
`execution_config`.

### Phase 7 — Paper trading

Live strategy driver consuming `tw.market.bars.1m.v1` with per-deployment state restore; the
Paper Trading Engine (position sizing, risk limits, order-rate ceiling F15); deployment
lifecycle API; a position rebuild job that regenerates positions from fills alone; portfolio
UI.

### Phase 8 — Anomaly detection

Rolling per-symbol features; a robust z-score baseline (median/MAD) shipped **first**; an
offline training pipeline; the detection service emitting `anomaly.detected`; `GET /anomalies`
and an anomaly feed; **a written model card**.
**Exit:** injected synthetic anomalies are detected; the ML detector is compared against the
baseline on held-out data; every stored detection is explainable from its own `features`
payload; no text anywhere claims detection of manipulation or fraud.

### Phase 9 — Alerts and notifications

Rule evaluation, an Alert Dispatcher with dedupe keys, per-rule cooldown and a per-user hourly
cap (F18); `tw.alerts.v1` → `/ws/alerts`; alert-rule CRUD; an alert centre.

### Phase 10 — Observability and performance measurement

Prometheus metrics across every service; correlation-id propagation from HTTP through Kafka;
a committed Grafana dashboard; **benchmarks in `benchmarks/`** each recording methodology,
configuration and hardware; `PERFORMANCE.md`.
**Exit:** every number ever quoted about TradeWatch traces to a benchmark that another person
can re-run from the documented command alone.

### Phase 11 — End-to-end and failure-recovery hardening

Playwright E2E; a failure-injection harness; **a test for each of F1–F20**; a chaos runbook;
a load test with the shedding order proven.

**Known flaky test to close here** — `backend/tests/api/test_websocket_market.py::TestConnectAndDisconnect::test_a_connection_closes_itself_the_instant_its_token_expires`
(ADR-0010's "closed continuously, not only at connect" behaviour). *Open. Found during Phase 6
verification, 2026-09-26; deliberately not fixed there — it is pre-existing test code from
Phase 5, unrelated to Phase 6's changes.* The test mints a token with ~1 second of validity
left and asserts the close arrives inside `pytest.raises(WebSocketDisconnect)`, which wraps
only `ws.receive_text()`. When that 1-second window elapses during the connect handshake
itself instead of after — which happens under load, e.g. running alongside the full ~1,050-test
suite rather than in isolation — the server closes during `client.websocket_connect(...)`'s own
`__enter__`, before `pytest.raises` is active, and the `WebSocketDisconnect` escapes uncaught.
Passes reliably standalone; intermittent in the full run. Fix is widening the connect/assert
race margin (e.g. wrap the whole `with` block in `pytest.raises`, not just the receive call) or
giving the token a few seconds of headroom instead of one.

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

The trading track (6→7) and the detection track (8) depend only on **Phase 5 and on bars from
Phase 3**, and can be interleaved. Phase 9 needs both.

**Practical consequence:** do not start Phase 6 before Phase 3 and Phase 5. There is currently
~4 hours of synthetic 1-minute data in one interval; CAGR, Sharpe and drawdown computed from
that are arithmetic exercises producing authoritative-looking numbers that mean nothing. And
`strategies` and `portfolios` are owner-scoped, so backtest endpoints built before auth get
written twice.

---

## 18. Known limitations and open concerns

**Architectural:**

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

1. **Never run `git commit` or `git push`.** Staging, committing and pushing are reserved for
   the repository owner. Read-only git (`status`, `diff`, `log`) is fine.
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

**Phase 5 — authentication, authorization and multi-tenancy**, or Phase 4 first if a live
dashboard matters more than reaching the trading track.

The plan's own ordering is 4 → 5 → 6, but it also states that the trading track (6→7) and the
detection track (8) "depend only on Phase 5 and on bars from Phase 3". Phase 3 is now done, so
**Phase 5 is the only remaining hard prerequisite for Phase 6**.

Phase 5 matters more than it sounds: `strategies`, `portfolios` and every backtest artefact
are owner-scoped in the schema, and `ARCHITECTURE.md` §14 puts auth ahead of the strategy
core. Building Phase 6's API before it means writing those endpoints twice.

Phase 4 is the better choice if the priority is something to look at — it is what makes the
chart live rather than a snapshot, and it is the first use of Redis, which has been running in
Compose since Phase 0 without a single consumer.
