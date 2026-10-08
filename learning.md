# TradeWatch — Learning Journal

**Purpose of this file:** teach *you* (not a future AI) everything about this project well
enough to talk about it in a technical interview — what it is, why it's built the way it is,
what problems it solves, and what actually happened while it was built. It has two parts:

- **Part 1 — The Concepts.** A reference you can re-read before an interview. Built up once,
  refined as the project grows.
- **Part 2 — The Session Log.** A chronological diary. After *every* prompt you give me, I
  append an entry here: what you asked, what I changed, why, and what you should now be able
  to explain because of it. Read Part 2 top-to-bottom to relive the project's history: read
  Part 1 when you want the concepts without the narrative.

Rule I'm following: this file is updated at the end of every response I give you, without
you needing to ask again.

---

# Part 1 — The Concepts

## 1.1 What TradeWatch is, in one paragraph

A **market intelligence and paper-trading platform** — not a real brokerage. It ingests
(synthetic, or optionally real via Alpaca) stock market data, stores it durably, aggregates
it into OHLCV candles, will eventually run configurable trading *strategies* against that
data in backtests and in live "paper" (simulated-money) trading, will flag statistically
unusual price/volume behaviour, and will alert on it. All of that sits behind a FastAPI
backend and a React dashboard. It's explicitly **not** real trading, and its anomaly
detector is explicitly **not** a fraud/manipulation detector — both of those boundaries are
load-bearing product decisions, not just legal disclaimers, and they're enforced in code
(a Phase 8 exit criterion literally scans code/docs/commits for forbidden language).

**Why this matters for an interview:** this is presented as a *portfolio-grade* system, i.e.
built to demonstrate production engineering judgement (failure handling, observability,
testing discipline), not a weekend demo. The interesting engineering is in the boring-looking
parts: idempotency, failure classification, layering discipline — not in the trading logic.

## 1.2 The architecture: ports and adapters (hexagonal architecture)

The codebase is organized in layers, enforced *mechanically* by a tool called
**`import-linter`** (not just code review discipline — CI fails the build if you violate it):

```
api/ , workers/   →  application/   →   domain/
   (entry points)      (use cases)      (pure business logic)
```

- **`domain/`** — Pydantic models with zero framework knowledge. No SQLAlchemy, no FastAPI,
  no Kafka client imports allowed here. This is "the business logic that would still make
  sense if you rewrote the entire web framework."
- **`application/`** — the use cases (`IngestionService`, `MarketDataProcessor`,
  `BarAggregationService`, etc.). These depend only on **ports** — `Protocol` classes
  (structural typing, like an interface) that describe *what* a dependency does, never
  *how*. Application code is not allowed to import `infrastructure` at all.
- **`infrastructure/`** — the actual adapters: `SqlMarketDataStore`, `KafkaEventPublisher`,
  `SyntheticProvider`. This is where SQLAlchemy, aiokafka, and eventually the Redis client
  live.
- **`api/` / `workers/`** — entry points. FastAPI routes, and the `tradewatch-producer` /
  `tradewatch-consumer` / etc. processes.

**Why bother?** Two concrete payoffs that already happened in this project:
1. Kafka was bolted on in Phase 2 *without modifying* `IngestionService` or
   `MarketDataProcessor` at all — because they only ever talked to `EventPublisher` /
   `EventConsumer` **ports**, and Kafka was just a new adapter behind them.
2. A fifth import-linter rule was added specifically to stop `aiokafka` being imported above
   `infrastructure` — because "one `from aiokafka import ...` in a service" is exactly the
   kind of thing that quietly undoes the whole abstraction over time. The rule makes the
   discipline a *fact checked by CI*, not a hope.

**Interview soundbite:** *"We use a ports-and-adapters architecture with import-linter
enforcing five layering contracts in CI, so swapping an infrastructure dependency — like
adding Kafka in Phase 2, or Redis pub/sub in Phase 4 — never touches business logic."*

## 1.3 The event-driven pipeline

```
Provider → normalize → IngestionService → Kafka ─┬→ Processor  → PostgreSQL → FastAPI → React
   (market data)      (dedupe, publish)          └→ Bar Aggregator → PostgreSQL (1m→5m/1h/1d)
```

Two **independent OS processes** (`tradewatch-producer`, `tradewatch-consumer`) plus a third
consumer (`tradewatch-aggregator`) all read/write through Kafka, which decouples them: any
one can restart, crash, or lag without the others losing data. This fan-out — **two
consumer groups reading the same `trades` topic with independent offsets** — is the whole
reason Kafka was chosen over, say, calling the database directly (see ADR-0001).

**Kafka concepts this project actually uses (good interview material):**
- **Partitioning by key:** every event is keyed by `symbol`, which is what guarantees all
  of one symbol's events land on one partition *in order*. Ordering across *different*
  symbols is explicitly not guaranteed or needed.
- **Consumer groups:** `processor-persist` (writes trades/quotes/bars to Postgres) and
  `bar-aggregator` (folds trades into candles) are two separate groups on the same topic —
  each with its own offsets, so restarting one never affects the other.
- **At-least-once delivery:** the system's actual guarantee. `acks=all` +
  `enable_idempotence=True` covers *producer retries within one session* — it does **not**
  mean a message is delivered exactly once end-to-end. A producer that restarts can still
  republish. The one rule that makes at-least-once safe: **commit the Kafka offset only
  after the write succeeds, never before.** (If you commit before writing and then crash,
  you lose the event forever — worse than a duplicate.)
- **Dead-letter queue (`tw.dlq.v1`):** a message that can *never* succeed (malformed,
  unsupported schema version) is quarantined here with the original bytes plus headers
  explaining why, instead of blocking the partition forever. Deliberately **no automatic
  redrive tool** — every reason a message got quarantined is a reason replaying it unchanged
  would fail again the same way.

## 1.4 Idempotency — the single most important concept in this codebase

This is the concept most worth being able to explain fluently, because a real, shipped bug
in this project hinged entirely on it (see the Phase 3 story below).

**"Idempotent write" ≠ "idempotent consumer."** They sound like the same property and are not:

- A **write is idempotent** when doing it twice produces the same result as doing it once —
  because it's a *replacement*. `trades` and `quotes` use `UNIQUE(event_id)` with
  `ON CONFLICT DO NOTHING`: replaying the same trade twice inserts the same row once. Trivial
  and correct.
- A **consumer is idempotent** only if replaying the same batch of events twice leaves its
  *state* unchanged. This is a much stronger property, and it does *not* follow automatically
  from idempotent writes — it fails exactly when the consumer **accumulates**: `volume +=
  size`, `trade_count += 1`. Fold the same trade in twice and you've added its size to the
  volume *twice*. `UNIQUE(event_id)` doesn't help here because there's no per-event row being
  written — there's one bar row being mutated by many trades.

**The bug this caused (Phase 3):** the Bar Aggregator accumulated volume/trade_count/vwap per
1-minute bar. On any restart, rebalance, or offset rewind, Kafka's at-least-once delivery
handed already-applied trades back to the aggregator, and it added their size in *again*.
Nobody noticed for a long time because `open`/`high`/`low`/`close` are first/max/min/last —
all four are naturally idempotent — so a doubled bar still had **perfect prices** and passed
every `CHECK` constraint the database enforces. Only `volume`, `trade_count`, and `vwap` were
silently wrong — which happen to be exactly the numbers a volume-weighted trading strategy
would price off. It was caught by deliberately rewinding a consumer group's offsets by 3,000
events across 3 partitions and re-draining them, then checking whether the additive fields
moved. (A test that just replays a batch once proves nothing — it has to be the *same*
events landing on a consumer that already applied them, restored from real database state.)

**The fix — `consumer_progress` table (ADR-0016):** the aggregator now records how far it has
applied each Kafka topic-partition, **in the same database transaction as the bars it
writes**. On every batch, it skips any event at or below its recorded position. Because the
position update and the bar writes commit together, there's no window where one could
succeed without the other.

**Interview soundbite:** *"Idempotent writes and idempotent consumers are different
guarantees. A write is idempotent if it's a replacement — INSERT with a unique constraint. A
consumer is only idempotent if it tracks its own position transactionally, which matters the
moment it accumulates state instead of just persisting rows. We found and fixed exactly this
gap in the bar aggregator — happy to walk through it."*

## 1.5 Deterministic event IDs

`event_id` is a **UUIDv5** (name-based, deterministic — not random UUIDv4) computed from a
natural key built out of the *provider's own data* (ADR-0012). Normalizing the same trade
twice — because of a reconnect replay, an overlapping backfill window, or a producer restart
— produces the *same* `event_id` both times. This is what makes `UNIQUE(event_id)` actually
catch duplicates that originate upstream of Kafka entirely, not just duplicates Kafka itself
might introduce. It's described as "the foundation everything else rests on" — changing the
UUID namespace or the natural-key logic would silently disable deduplication for all
existing data with no error raised anywhere.

## 1.6 Money handling

`Decimal` everywhere in the domain model, never `float` (ADR-0009) — floats can't represent
most decimal fractions exactly, which is unacceptable for money. Serialized to JSON **as
strings**, both on Kafka and in API responses, because a JSON *number* gets parsed into an
IEEE-754 double by every JavaScript client, silently reintroducing the exact bug `Decimal`
was chosen to avoid.

## 1.7 Failure handling philosophy

Every category of failure the system can hit has a name (`F1`–`F20`) in `ARCHITECTURE.md`
§12, with a detection method, a designed response, and a test. Treating failure modes as a
first-class, numbered catalogue (rather than ad hoc `try/except`) is one of the more
interview-worthy patterns here. The ones most relevant so far:

| # | Failure | Response |
| --- | --- | --- |
| F3 | Provider sends malformed data | Reject, count, DLQ — never crash the ingester |
| F4 | Kafka unavailable at publish | Bounded local buffer; if full, **drop quotes before trades** — quotes are less valuable than trades and shed first |
| F6 | Consumer crashes mid-batch | Offsets uncommitted → redelivery → idempotent writes absorb it |
| F7 | Duplicate delivery | `UNIQUE(event_id)` for replacement writes; **accumulating** consumers need their own position-tracking (see §1.4) |
| F8 | PostgreSQL unavailable | Consumers retry with backoff, lag grows but data stays safe in Kafka; API `/ready` returns 503 |
| F9 | Redis unavailable | **Degrade, never fail** — cache misses fall through to PostgreSQL; this is the one Phase 4 has to prove |
| F11 | Late/out-of-order market data | Bounded lateness window; corrections bump a `revision` field rather than mutating history |
| F19 | Slow WebSocket client | Drop the slow client's backlog and resync it; one slow browser tab must never stall the broadcaster for everyone else — this is Phase 4's central new failure mode |

The important habit this teaches: **classify every failure as retryable or permanent before
deciding what to do with it.** A retryable failure (broker down, DB down) gets retried with
the offset *uncommitted*. A permanent failure (malformed message, unsupported schema version)
gets quarantined immediately — retrying it forever would poison the partition.

## 1.8 The database

PostgreSQL is the **system of record** (ADR-0002) — Kafka is a transport with finite
retention, never a source of truth. The **entire schema (21 tables, 12 enum types)** was
built in Phase 1, in one migration, even though only ~4 tables were actually used until now.
Why front-load it: `users` had to exist before any table could reference it via foreign key,
or every later table addition would need a retrofit migration. One migration now, instead of
five painful ones later.

**Idempotency mechanisms per table**, worth knowing cold:

| Table | Mechanism | What kind of idempotency |
| --- | --- | --- |
| `trades`, `quotes` | `UNIQUE(event_id)` + `ON CONFLICT DO NOTHING` | Write idempotency (replacement) |
| `bars` | PK `(instrument_id, interval, bar_start)`, upsert guarded by `WHERE revision <= excluded.revision` | Write idempotency, but allows *later* corrections to win |
| `consumer_progress` | PK `(consumer_group, stream)`, position upserted to `greatest(stored, incoming)` | **Consumer** idempotency — the fix from §1.4 |

## 1.9 API design choices worth explaining

- **`GET /health` never checks the database.** A liveness probe that fails during a DB
  outage turns a database blip into a *restart loop* — the container orchestrator kills a
  perfectly healthy process because a dependency is temporarily down. `/ready` is the one
  that checks dependencies, and it's what load balancers/readiness gates should watch instead.
- **`200` with an empty list, not `404`, for a market that hasn't opened.** A market with no
  data for the requested window is a normal Sunday, not an error. Returning `404` there would
  make it indistinguishable from a typo'd symbol.
- **`503`, not `500`, for "database unreachable."** Tells the caller this is retryable.
- One error envelope everywhere: `{"error": {"code", "message", "details", "request_id"}}`,
  and every response carries `X-Request-ID` for tracing a request through logs.

## 1.10 Testing philosophy

- **No mocking the database.** An in-memory SQLite substitute would test a *different*
  database — no native enums, no JSONB, no partial indexes, no `ON CONFLICT`, different
  transaction semantics. Integration tests run migrations against a real PostgreSQL and roll
  back a transaction per test.
- **Integration tests skip with an explanation, not fail,** when infrastructure isn't
  reachable — a developer without Docker running should see one clear message, not a wall of
  connection errors. (CI is different: there, an unreachable dependency should be a broken
  pipeline, not a silent pass — see the Session Log entry on the CI fix for a case where this
  distinction actually mattered.)
- **Unit tests fake the *narrow client protocol*, never `aiokafka` itself.** The Kafka
  adapters declare tiny `ProducerClient`/`ConsumerClient` `Protocol`s over the handful of
  methods they actually call, so a "broker that refuses to connect" or "times out twice then
  succeeds" is a few lines of fake code — no patching a third-party library's internals.
- **The load-bearing integration test** (`test_kafka_pipeline.py`) asserts the exact OHLCV
  values the API returns match what the *provider* generated, event by event — not just a row
  count (a row-count check would pass even on stale, pre-existing data). It also proves,
  against a real broker: reprocessing changes nothing, a restarted consumer doesn't re-read
  what it finished, an offline consumer catches up, and a broker outage costs lag, not data.

## 1.11 Glossary (quick lookup before an interview)

- **At-least-once delivery** — a message may be delivered more than once, but never zero
  times (as long as it's within Kafka's retention). The opposite failure mode, losing
  messages, is considered much worse here, so every design choice trades toward "maybe
  duplicate" over "maybe lost."
- **Idempotent producer** (Kafka/aiokafka term) — the producer numbers records per partition
  so retried sends within one producer session don't duplicate. Does **not** survive a
  producer restart — a new producer session starts a new sequence and the guarantee resets.
- **Consumer offset** — a per-partition, per-consumer-group pointer into a Kafka topic
  marking "everything up to here has been processed." Committing it early risks silent data
  loss on crash; committing it late risks (safe, absorbable) duplicates.
- **DLQ (dead-letter queue)** — a side topic for messages that can never be processed
  successfully, so a poison message doesn't block everything behind it forever.
- **Port / Adapter** — a `Protocol` (port) describes a capability abstractly; a concrete class
  (adapter) implements it against a real technology (Kafka, Postgres, Alpaca). Swapping
  adapters shouldn't require touching code that only depends on the port.
- **Backpressure** — what happens when a fast producer of work outruns a slow consumer of it.
  The system's policy is almost always "shed something visibly and cheaply (quotes, a slow
  client's backlog) rather than block or crash silently."
- **Natural key** — a real-world identifying field (or combination) from source data, as
  opposed to a synthetic key generated later. TradeWatch's deterministic event IDs are built
  from the provider's natural key.

## 1.12 Phase 4 concepts (live streaming and caching)

Added when Phase 4 was built — see Part 2's entry for the full design discussion and the
real bugs found along the way. This is the quick-reference version.

- **Cache-aside** (a.k.a. lazy loading) — the pattern `_read_through` in `api/routes/
  market.py` implements: check the cache first; on a miss, read the real source (PostgreSQL)
  and populate the cache before returning. The opposite pattern, **write-through**, would
  have the *write* path populate the cache proactively — TradeWatch never does this, because
  nothing writes through the API at all (it's read-only), so cache population only ever
  happens as a side effect of a read.
- **Pub/Sub fan-out** — Redis Pub/Sub is fire-and-forget: a publish reaches only currently
  subscribed listeners, with no persistence, no replay, no acknowledgement. That's the
  opposite trade-off from Kafka (durable, replayable, acknowledged) and is the right one here
  specifically because what travels over it is *derived, ephemeral* data (live-update
  notifications), never the system of record.
- **Why a WebSocket needs its own failure mode.** An HTTP request either succeeds or fails,
  once, and the client decides what to do next. A WebSocket is long-lived and the *server*
  has to decide, continuously, what to do about a client that's still connected but not
  keeping up — that's failure F19, and "bounded queue + drop-and-resync" is the general shape
  of the answer any pub/sub-to-websocket bridge needs, not something specific to this project.
- **Why an accumulating consumer needs position-tracking but a fan-out consumer doesn't**
  (worth re-reading after §1.4): the aggregator's state (a bar's volume) is *itself* the thing
  that would be corrupted by a duplicate. The broadcaster's "state" is a notification already
  sent to a browser — sending it twice is a no-op from the receiver's point of view. The
  question to ask of any new Kafka consumer: *does redelivering the same event to this
  consumer change anything it holds?* If no, it's naturally idempotent and needs nothing
  extra. If yes, it needs its own `consumer_progress`-style tracking.

---

# Part 2 — The Session Log

Each entry: **what you asked**, **what I did and why**, **what you should now be able to
explain** because of it.

## Entry 1 — 2026-09-24 — Project status check

**You asked:** what stage the project is at and what's left before the next stage.

**What I did:** Read `docs/PROJECT_STATE.md` (the project's own authoritative handover doc)
and cross-checked it against `git log`. Reported: Phases 0–3 complete (foundations → vertical
slice → Kafka backbone → real market data/historical depth), Phases 4–12 not started, and
that all of Phase 2–3's work was still uncommitted (only two commits existed:
`Initial commit` and the Phase 1 slice — since then a third commit landed fixing the bar
aggregator bug described in §1.4).

**What you can now explain:** the project deliberately documents "what exists" (`PROJECT_STATE.md`)
separately from "what's planned" (`ARCHITECTURE.md`, which describes the *finished* system
including unbuilt pieces) and "what's decided" (`ADR/`, which can be decided before it's
built). Knowing to ask "is this describing reality or intent?" when reading unfamiliar project
docs is itself a useful habit to describe in an interview.

## Entry 2 — 2026-09-24 — This file, the CI fix, and starting Phase 4

**You asked for three things:** (1) create this `learning.md` and keep it updated after every
prompt from now on, (2) fix a backend integration-test failure that showed up in GitHub
Actions CI (but not locally), (3) implement Phase 4 (live streaming + Redis caching).

### (1) This file

Created it with the structure described at the top: a standing concepts reference (Part 1)
plus this running diary (Part 2). Everything above Part 2 was written from
`docs/PROJECT_STATE.md`, `ARCHITECTURE.md` (the ports/adapters layering rules, the Kafka
topology and delivery semantics, the full F1–F20 failure catalogue, the Redis "justified
uses" table, and the WebSocket surface spec), and the ADR table — i.e. from the project's own
documentation, read directly rather than summarized secondhand.

### (2) The CI failure — root cause and fix

**The error you pasted**, trimmed to the essentials:
```
pydantic_core._pydantic_core.ValidationError: 1 validation error for DatabaseSettings
database_url
  Field required [type=missing, input_value={}, input_type=dict]
...
ResourceWarning: unclosed transport <_SelectorSocketTransport fd=14>
========== 6 passed, 32 skipped, 623 deselected, 207 errors in 19.62s ==========
```

**How I traced it (worth understanding the method, not just the answer):**

1. `207 errors` out of the `backend-integration` job's suite is *almost the whole thing* (the
   docs put the integration suite at ~245 tests) — so this isn't one flaky test, it's a
   shared-setup failure blowing up nearly every test that needs it.
2. `DatabaseSettings.database_url` is a **required field with no default, on purpose**
   (`config/settings.py`) — "silently connecting to something a developer didn't choose is
   how test data ends up somewhere it shouldn't" is the actual docstring reasoning. So this
   error means *something* built a bare `DatabaseSettings()` from the environment, and
   `TW_DATABASE_URL` wasn't set in that environment.
3. Grepped for every place that constructs `DatabaseSettings()` directly (not through the
   normal `load_settings()` path, which wraps missing fields in a friendlier
   `ConfigurationError` instead of a raw pydantic error). Found it:
   `backend/alembic/env.py` line 34, at **module import time**:
   ```python
   config.set_main_option("sqlalchemy.url", DatabaseSettings().database_url)
   ```
   This runs unconditionally, every time Alembic's environment loads — including when a test
   is running migrations against a connection it already opened itself.
4. Checked `tests/integration/conftest.py::_run_migrations()` — the test suite calls Alembic
   by handing it an **already-open** SQLAlchemy connection via `config.attributes["connection"]`,
   specifically so Alembic doesn't need to open its own connection or know a URL at all. The
   test path *never uses* `sqlalchemy.url`. But line 34 built it anyway, unconditionally.
5. Checked why this was invisible locally: `config/env.py::load_env_file()` searches upward
   from the current directory for a `.env` file and loads it into `os.environ` *before*
   settings are read. A developer's machine almost always has a `.env` (with `TW_DATABASE_URL`
   pointed at their local dev Postgres), so `DatabaseSettings()` succeeds locally by accident.
6. Checked `.github/workflows/ci.yml`'s `backend-integration` job: it sets
   `TW_TEST_DATABASE_URL` (the *test* database) as an env var, but never copies a `.env` file
   and never sets `TW_DATABASE_URL` (the *application* database) — correctly, since that job
   should never need the application's database URL at all. So in CI, and only in CI,
   `DatabaseSettings()` had nothing to read and raised immediately.

**Root cause, stated plainly:** `alembic/env.py` read a settings value it didn't need on the
test code path, and did so in a way that crashed instead of degrading — masked locally by a
`.env` file that happens to supply the value CI never provides.

**The fix (`backend/alembic/env.py`):** only build `DatabaseSettings()` (and set
`sqlalchemy.url`) when Alembic *doesn't* already have a connection handed to it:
```python
if config.attributes.get("connection") is None:
    config.set_main_option("sqlalchemy.url", DatabaseSettings().database_url)
```
Real `alembic upgrade head` usage (dev machines, Docker, the separate `backend-startup` CI
job) is completely unaffected — it still reads `TW_DATABASE_URL` exactly as before. Only the
test path, which never used the value, stops requiring it.

**The second half of the fix (`backend/tests/integration/conftest.py`):** the session-scoped
`engine` fixture ran the migration inside `async with engine.begin()` blocks with **no**
`try`/`except` around them. When the migration step raised (from the bug above), the fixture
function exited before ever reaching its own `yield` / cleanup, so `await engine.dispose()`
at the bottom never ran. That leaked every connection the doomed engine had opened — which is
exactly what those `ResourceWarning: unclosed transport` messages in your pasted log are. And
because this test project's pytest config turns warnings into errors
(`filterwarnings = ["error", ...]`), each leaked connection became *its own separate test
failure* on top of the original one — which is why one root cause produced 207 errors instead
of one. Wrapped the schema-reset + migration block in `try/except: dispose(); raise`, so any
future migration failure (a real one, not just this bug) disposes its connections cleanly and
fails as one clear error instead of a cascade of unrelated-looking resource warnings.

**Verification:** ran the integration suite locally exactly the way CI runs it — Postgres up
via Docker Compose, **no `.env` file**, only `TW_TEST_DATABASE_URL` exported, nothing else —
to reproduce the CI environment rather than trusting the fix without seeing it pass. Result:
**245 passed, 0 errors** (versus the pasted CI run's 6 passed / 32 skipped / 207 errors).
Locally Kafka and Redis also happened to be running already, which is why nothing skipped
here — in CI itself, the Kafka-dependent integration tests will still legitimately *skip*
(no Kafka service is provisioned in the `backend-integration` job yet — `ci.yml` even has a
comment flagging this as known, deferred work: `# Phase 2 adds Kafka and Redis services to
backend-integration`, which never actually happened). That's a separate, pre-existing,
intentional gap — skipping is the correct designed behaviour when infrastructure is
unreachable — not something this fix touches. Worth knowing: **Phase 4's own new
Kafka/Redis-dependent integration tests (see below) will skip in CI the same way** until
that CI gap is closed, which I did not do here since it wasn't part of what broke and wasn't
asked for.

**What you can now explain:** how to root-cause a "works on my machine, fails in CI" failure —
find every place a value is read directly from the environment rather than through the
project's single validated path, then diff *exactly* what each environment actually provides.
Also a concrete example of why letting a single unhandled exception skip a fixture's own
cleanup code can turn one bug into what looks like 200 unrelated ones — and why
`try/finally`-style cleanup around anything that opens a real OS resource (a socket, a file
handle, a DB connection) is worth the extra few lines even when "the happy path never fails."

### (3) Phase 4 — live streaming and caching

Built the whole phase: a WebSocket endpoint, a Kafka→Redis broadcaster process, cache-aside
reads with explicit invalidation, and the frontend live-update path. Below is what got built,
why, and — because a few real bugs turned up along the way — what they were and how they were
found. Read this section like a design review, not a changelog.

#### The shape of it

```
tradewatch-broadcaster (new process, 3rd Kafka consumer group)
  reads: tw.market.trades.v1, tw.market.quotes.v1, tw.market.bars.1m.v1
  for every event: publish to Redis Pub/Sub (tw:ws:market) + delete its cache key
                                    |
                                    v
FastAPI process: ConnectionManager subscribes once, fans out in-process by symbol
                                    |
                                    v
                          /ws/market  →  browser (live trade tape, live price)

GET /api/v1/market/{symbol}/latest, /bars (default window), /instruments
  → cache-aside through Redis (tw:quote:*, tw:bars:*, tw:instruments:*),
    TTL + the broadcaster's invalidation above
```

#### Design decisions worth being able to defend

**A third Kafka consumer group, not new logic bolted onto the existing two.** Exactly the
same argument as the Bar Aggregator in Phase 3: `processor-persist` (writes Postgres),
`bar-aggregator` (builds candles), and now `broadcaster` (fans out live updates + invalidates
cache) all read the *same* topics with independent offsets. Killing any one of them only
stops that one concern — this is ADR-0001's fan-out argument proven a third time, and it's
the reason a new "live streaming" feature didn't touch `MarketDataProcessor` or
`BarAggregationService` at all.

**The broadcaster needs no `consumer_progress` tracking, unlike the aggregator.** This is
the single most important thing to be able to explain about this phase, because it's a direct
callback to the Phase 3 bug (see §1.4 of Part 1). The aggregator *accumulates* (`volume +=
size`), so a redelivered trade after a restart would double-count — that's what
`consumer_progress` fixes. The broadcaster does neither: publishing "AAPL just traded" to
Redis twice, or deleting an already-deleted cache key twice, changes nothing. It's naturally
idempotent because it has no state of its own to corrupt. Writing this out as a doc comment
in `application/broadcasting/service.py` was deliberate — the temptation to reflexively
add "the same kind of position tracking Phase 3 needed" to every new consumer is exactly the
kind of over-engineering worth naming and rejecting explicitly, not just skipping silently.

**One shared Redis Pub/Sub channel (`tw:ws:market`), not one channel per symbol.** The
`ConnectionManager` in the API process subscribes to Redis exactly once and filters
in-process by which symbols each browser tab currently wants. The alternative — one Redis
subscription per symbol per connection — would grow unbounded with open tabs. This is a
classic fan-in/fan-out trade-off: push the filtering into the cheap layer (an in-memory `set`
membership check per message) rather than the expensive one (a broker subscription).

**Cache keys match `ARCHITECTURE.md` §10 exactly**, because the invalidation trigger and the
read path have to agree on the same string or invalidation silently does nothing:
`tw:quote:{symbol}` (latest trade/quote), `tw:bars:{symbol}:{interval}` (the *default* bars
window only — see below), `tw:instruments:list:{limit}` / `tw:instrument:{symbol}` (TTL-only,
no live invalidation trigger, since nothing writes instruments live yet).

**Only the default "last N bars ending now" query is cached — never an explicit
`start`/`end` range.** The architecture doc's justification for this cache key
("the most repeated query in the app, identical across all users") only holds when nobody
picked their own window. Caching an explicit range under a key that doesn't encode the range
would serve one caller's window to a different caller's request for a different one — a
straightforward correctness bug, not just a staleness one. `get_bars` in `api/routes/
market.py` only enables the cache when `start is None and end is None and limit ==
DEFAULT_BARS_LIMIT`.

**A cache hit and "Redis is down" look identical to every caller, on purpose** (failure F9).
`RedisCache` never raises: `get_json` returns `None` on both a genuine miss and a connection
failure, `set_json`/`delete`/`publish` silently no-op if they can't reach Redis. This is what
lets `TW_CACHE_ENABLED=false` and "the Redis container is stopped" be the exact same code
path from `api/routes/market.py`'s point of view — one `_read_through` helper, one `enabled`
flag, no separate "what if Redis throws" branch anywhere above the adapter.

**`/ready` gained a `critical` flag on registered checks**, specifically for this. The
existing readiness registry (`api/routes/health.py`) only had one mode: any failing check
flips the whole endpoint to 503, which is correct for PostgreSQL (the API genuinely cannot
serve a correct response without it) and would be *wrong* for Redis — a cache outage 503'ing
`/ready` would turn a non-critical dependency into what looks like a full outage to a load
balancer, which is precisely what F9 says must not happen. `register_readiness_check(...,
critical=False)` for Redis; the check still shows up in the response body for visibility, it
just never gates traffic.

**Slow-consumer handling (F19) is a bounded queue per connection plus a resync marker**,
implemented as two separated concerns: `ConnectionManager._dispatch(message)` decides *which*
connections should receive a message (symbol-filtered), and `._deliver(connection, message)`
decides *how* — always `put_nowait`, never blocking. When a connection's queue is full, its
entire backlog is dropped and replaced with a single `{"event_type":"market.resync"}`
message; the frontend treats that exactly like a reconnect and re-fetches REST rather than
trying to reconstruct what it missed. Splitting `_dispatch` out of the Redis-reading `_relay`
loop was itself a fix — see the bug list below.

**No authentication on `/ws/market`, documented as deliberate.** `ARCHITECTURE.md` §8.2 says
this channel should be authenticated — that's the finished-system target, and Phase 5 (auth)
doesn't exist yet. Every `GET /api/v1/market/*` route is already unauthenticated today, so an
open WebSocket is consistent with the rest of the API as it stands, not a gap unique to this
endpoint. This is exactly the kind of thing `docs/PROJECT_STATE.md` warns about: reading
`ARCHITECTURE.md` as a description instead of a target and "fixing" a gap that every other
route already has.

#### Real bugs found while building this (worth knowing, because the method is reusable)

1. **`alembic/env.py` read `TW_DATABASE_URL` unconditionally, even when a test handed it an
   already-open connection that never needed a URL at all** — this is the CI fix from the
   previous entry, not new to Phase 4, but it's the reason the very first full-suite run of
   this phase was trustworthy at all instead of silently passing on a broken migration path.

2. **My own unit test called `ConnectionManager._deliver` directly and expected it to filter
   by symbol — it doesn't, `_dispatch` does.** `_deliver` is the low-level "put in this one
   queue" primitive; the symbol membership check lives one level up, in `_relay`. The test
   failure (`assert 1 == 0`) was real, but the bug was in the test's understanding of the
   split, not in the manager. Fix: extracted `_dispatch` as its own method so the filtering
   rule is unit-testable without also needing `_deliver`'s queue mechanics, and rewrote the
   tests against it. Small lesson: when a test fails against freshly-written code, check
   which of the two disagrees with the *design*, not just which one to edit to make it pass.

3. **An actual infinite hang**, not a wrong assertion: `ScriptedConsumer.batches()` yields an
   empty list forever once its scripted batches run out (this mirrors the real Kafka
   adapter's behaviour — an empty poll on a quiet topic). `max_batches` on both
   `MarketBroadcastService.run()` and the existing `MarketDataConsumerService.run()` counts
   only *non-empty* batches (documented, and for a real reason: otherwise a bounded run in a
   quiet market would finish having processed nothing). My first version of "an empty batch
   is not committed" scripted `[[]]` — a single empty batch and nothing else — so
   `max_batches=1` could never be satisfied and the test spun forever in the generator's
   infinite empty tail. Two `uv run pytest` processes were left running in the background
   before this was caught. Fix: mirror the existing test suite's own idiom for this exact
   scenario (`tests/unit/test_market_data_consumer.py::test_max_batches_counts_only_non_empty_batches`)
   — script an empty batch *followed by* a real one, so the run has something to terminate on.

4. **A slow-consumer test's own arithmetic was wrong twice in a row**, in a way worth walking
   through because both mistakes were plausible-looking: the queue's `maxsize` is 100, and
   the test wanted to prove that overflowing one connection's queue doesn't touch another's.
   First attempt dispatched the *same* 105 messages to both connections (both "overflowed"
   identically, so the assertion that one holds `QUEUE_MAXSIZE` was false — it held only what
   was left after its own overflow). Second attempt fixed that but then assumed a queue that
   overflowed once would sit at exactly the resync marker forever — forgetting that
   `_dispatch` doesn't stop calling `_deliver` after a reset; the queue keeps absorbing
   whatever arrives *after* the drop, same as production. Both were fixed by being precise
   about what each connection was actually sent, not just what the *intent* of the test was.

5. **A test fixture skipping the app lifespan silently stopped setting `app.state.cache`.**
   `tests/integration/test_market_api.py` and `test_vertical_slice.py` both build the FastAPI
   app directly and drive it through `httpx.ASGITransport` — which, unlike `TestClient`,
   never sends the ASGI lifespan protocol at all (confirmed by checking `ASGITransport`'s
   actual signature — it has no `lifespan` parameter). Before Phase 4 this was harmless: the
   only thing the lifespan set that routes needed was `app.state.database`, and `get_session`
   was already overridden to route around it. Phase 4 added `CacheDep` (backed by
   `app.state.cache`) directly onto the market routes, and that state is *only* ever set
   inside the lifespan these fixtures skip — so every route touching the cache would have
   raised `RuntimeError` under this fixture, caught by the API's own catch-all exception
   handler and turned into a quiet 500. It was caught before it could hide behind that 500:
   by reading `ASGITransport`'s constructor rather than trusting the surrounding fixture's own
   docstring, and by running the specific previously-affected test files with a real test
   database rather than trusting a "670 passed" number from a run where — it turned out —
   `TW_TEST_DATABASE_URL` had never actually been set, so nothing in those two files ran at
   all; they'd all silently skipped. Fix: both fixtures now attach a `RedisCache` to
   `app.state.cache` directly, deliberately **unconnected** — since an unconnected
   `RedisCache` always misses on read and no-ops on write (identical to "Redis is down," F9),
   this can't leak one test's cached value into another's assertions through a real shared
   Redis, which a *connected* shared cache absolutely would have. Caching's own behaviour
   (hits, misses, TTL, invalidation) got its own dedicated, properly-isolated test file
   instead (`test_market_api_caching.py`, with a real, flushed-per-test Redis) — the
   functional-correctness suite should not also have to reason about cache state.

6. **A real, reproducible ~2.1-second first-connection delay to Redis through Docker
   Desktop's Windows/WSL2 port forwarding** — not a bug in the code, but close enough to one
   that it caused an actual failure: `RedisCache`'s original `socket_connect_timeout=2.0`
   (a deliberate "fail fast" choice) was *just* under that real cold-start cost, so the very
   first command on a freshly created client would spuriously report Redis unreachable on
   this common local setup — which would misfire `/ready`'s Redis check on every fresh API
   start. Measured directly (not guessed): first `PING` after opening a client, ~2.1s every
   time; every `PING` after that on the same client, ~6ms. That split — cost of *opening* a
   connection vs. cost of a *command* on an already-open one — is exactly what
   `socket_connect_timeout` vs. `socket_timeout` are for. Fix: `socket_connect_timeout`
   raised to 5.0s (absorbs the one-time proxy cost), `socket_timeout` left at 2.0s (still
   protects against an actually slow command on a live connection). This is the kind of
   platform-specific "trap" `docs/PROJECT_STATE.md` §19 keeps a running list of — worth
   remembering if `/ready` ever spuriously flags Redis again on a freshly started process.

#### The benchmark

Per the task's own instruction — measure, don't estimate, and don't let this be mistaken for
a Phase 10 performance claim — `backend/scripts/bench_cache.py` seeds 500 real 1-minute bars
for a throwaway instrument, then times 200 repetitions each of the real `BarRepository`
query PostgreSQL answers today vs. a warm `RedisCache.get_json` hit for the same payload,
against the actual local PostgreSQL and Redis this machine runs. One real run:

```
200 reads each, 500 bars, against real local Postgres+Redis:

     uncached (DB):  min   8.637 ms   median  12.576 ms   mean  15.627 ms   p95  28.660 ms
    cached (Redis):  min   2.907 ms   median   5.155 ms   mean   5.398 ms   p95   8.791 ms

Median speedup: 2.4x
```

Read this as "the cache measurably helps, on one laptop, once" — not as a claim about
production latency. **Superseded (Phase 10):** `bench_cache.py` was removed; the cached vs
uncached figures that may be quoted now come only from `benchmarks/api_latency.py`, through
`PERFORMANCE.md`. The numbers above are kept as a record of this session, not as a result. `benchmarks/` stays empty until Phase 10 gives it a committed methodology
and a recorded hardware config, exactly as `docs/PROJECT_STATE.md` §1 requires.

#### Tests written

- `tests/unit/test_broadcast_service.py` — `MarketBroadcastService` against fakes: every
  event is published on the shared channel, money stays a string in the published message,
  the right cache key is invalidated per event type, commits only happen after a non-empty
  batch is fully broadcast.
- `tests/unit/test_websocket_manager.py` — `ConnectionManager`'s fan-out and the F19
  slow-consumer path, driven directly (no real socket, no real Redis).
- `tests/api/test_websocket_market.py` — the real `/ws/market` route through `TestClient`:
  connect/disconnect registration, the `symbols` query parameter, the subscribe/unsubscribe
  control protocol, and that malformed input never closes the connection.
- `tests/integration/test_redis_cache.py` — `RedisCache` against a real Redis: hit, miss,
  TTL actually applied, expiry actually happens, delete invalidates, publish/subscribe
  round-trips — plus the F9 fallback suite against a deliberately unreachable Redis.
- `tests/integration/test_market_api_caching.py` — the route-level proof: a *poisoned* cache
  entry (a value that could never have come from PostgreSQL) is what gets served on a hit,
  proving the cache path is actually taken rather than just written to; `TW_CACHE_ENABLED=
  false` ignores that same poison and reads PostgreSQL directly; deleting the key makes the
  next read fresh again.
- `frontend/src/hooks/useMarketSocket.test.ts` — the frontend's own reconnect-with-backoff,
  resync handling, and per-symbol state reset, driven through a `FakeWebSocket` test double
  (jsdom has no real `WebSocket` at all, so this is also what every other `MarketView` test
  now depends on by default — see `src/test-support/fakeWebSocket.ts`).

#### Exit criteria — verified, not assumed

- **"A trade appears in the browser without a page refresh."** `useMarketSocket` pushes every
  `market.trade` straight into `LatestPricePanel` (overrides the REST snapshot the instant one
  arrives) and into the trade tape — covered end-to-end by
  `useMarketSocket.test.ts::records a live trade as both the latest trade and the head of the
  tape`, and by construction: `tradewatch-broadcaster` → Redis Pub/Sub → `ConnectionManager` →
  `/ws/market` → this hook is one continuous, tested path from Kafka to the DOM update.
- **"With `TW_CACHE_ENABLED=false`, every existing test still passes."** Verified two ways:
  (1) `test_market_api_caching.py::TestCacheKillSwitch` builds real `Settings` with
  `cache_enabled=False` against a real, *poisoned* Redis key and proves the route ignores it
  and reads PostgreSQL instead — the strongest form of this claim, since it can't pass by
  accident the way "the cache happened to be empty" could. (2) The full backend suite run
  below passed with caching enabled at its default; nothing in the suite depends on caching
  being on, and `_read_through`'s `enabled` flag is the only thing that changes between the
  two configurations — there is no separate code path for `false` to skip.
- **"Stopping the Redis container degrades the system but does not break it (F9)."**
  `tests/integration/test_redis_cache.py::TestRedisUnavailableFallback` proves every
  `RedisCache` method against a Redis that was never reachable (not "was up, then stopped" —
  a deliberately unreachable address, same trick `UNREACHABLE_DATABASE_URL` uses for
  PostgreSQL) returns a safe default instead of raising. The route-level consequence follows
  from `_read_through`'s logic directly: a `None` from `get_json` is indistinguishable from a
  cache miss, so the route falls through to PostgreSQL exactly as it would with caching off.

#### Full suite result

Ran `uv run pytest -q` (all markers except `provider`) with a real PostgreSQL, Kafka and
Redis available locally — the same infrastructure `docker compose up -d` provisions. First
run: **903 passed, 1 failed** (`test_bar_aggregator.py::TestRestartMidBar::
test_a_restart_mid_bar_produces_the_correct_closed_bar`, `asyncpg.exceptions.
InternalServerError: cache lookup failed for type 35628` — asyncpg's well-documented
stale-type-OID-cache symptom after a mid-session `DROP SCHEMA ... CASCADE`/`CREATE SCHEMA`,
not anything Phase 4 touches). Ran that one file alone: **11 passed**. Ran the entire suite a
second time end to end: **909 passed, 0 failed** — confirming the first run's failure was a
one-off test-order flake in infrastructure this phase never touched (the Bar Aggregator and
its tests are untouched by any of this work), not a regression. Also caught and fixed one
genuine mypy gap while cleaning this up: a test function with an annotated return type
(`-> None`) but an unannotated parameter trips `disallow_incomplete_defs` even under the
per-module `disallow_untyped_defs = false` override for `tests.*` — that override only
relaxes "no annotations at all," not "some annotations." `tests/api/test_websocket_market.py`
now types its `FastAPI` fixture and parameters properly instead of relying on the override.

`ruff check`, `ruff format --check`, `mypy --strict` (139 source files, 0 issues) and
`lint-imports` (5/5 contracts, including the new "Redis is an adapter" reasoning documented
directly in `pyproject.toml` rather than as an added contract — see the design note above)
all pass. Frontend: `eslint`, `prettier --check`, `tsc --noEmit`, `vitest run` (30/30) and
`vite build` all pass.

Phase 4 is done. Nothing blocks starting Phase 5 (authentication, authorization,
multi-tenancy) — the next phase per `docs/PROJECT_STATE.md` §21's own recommendation, and the
actual hard prerequisite for the strategy/backtesting track (Phase 6).

## Entry 3 — 2026-09-24 — Pre-merge verification: a real bug the earlier report missed

Before merging Phase 4, you asked for two things: (1) run the *actual* full stack (not just
the test suite) and manually confirm live behavior in a browser — including deliberately
restarting the broadcaster mid-flow and checking the trade tape for a duplicate row — and
(2) confirm the `app.state.cache` wiring bug from Entry 2 has its own explicit regression
test, not just incidental coverage. Both surfaced real findings.

### What this teaches about the difference between "the tests pass" and "it works"

Entry 2 closed with "Phase 4 is done" backed by 909 passing tests. Every one of those tests
used a *fake* `WebSocket` (`FakeWebSocket`, `tests/setup.ts`) or a scripted Kafka fake
(`ScriptedConsumer`) — deliberately, because that's what makes unit tests fast and
deterministic. But it also means no test in that suite ever actually sent the same
`event_id` twice through the real reducer via a real redelivered message, because nothing in
the test suite ever modeled "the broadcaster crashes mid-batch and Kafka redelivers." That
gap is exactly what this prompt asked me to go find, and it was real: **the trade tape had no
deduplication at all.** `useMarketSocket`'s reducer appended every `market.trade` message it
received to the tape unconditionally — a redelivered trade (identical `event_id`, arriving a
second time after a broadcaster restart) would have shown up as a second, visually identical
row. This is a genuine instance of the gap between "no mocking of real infrastructure" in the
*backend* test suite (a deliberate, stated project principle — §1.10) and the frontend test
suite, which by necessity mocks the one piece of infrastructure it has (`WebSocket`) and so
can only be as good as the scenarios someone thinks to script into the fake.

### No real browser was available — said plainly, not glossed over

This session has no browser automation tool (no chrome-devtools MCP or equivalent
configured) — it's a headless background job. "Start the full stack and confirm in an actual
browser" could not be done literally. What *was* done, and why it's a reasonable substitute
rather than a shortcut:

- **The full stack was actually started for real** — not simulated: `docker compose`'s
  Postgres/Redis/Kafka (already running), plus real `tradewatch-producer`,
  `-consumer`, `-aggregator`, `-broadcaster`, and `-api` processes, plus the real Vite dev
  server serving the actual `MarketView` bundle (on port 5174 — port 5173 turned out to be a
  **separate, pre-existing dev server the user already had running on the main checkout**,
  started days earlier; identified via its Windows command line before touching anything,
  and left completely alone).
- **A real WebSocket client** (Node's native `WebSocket`, not a browser, but not a fake
  either) connected to the real `/ws/market` endpoint and printed every message with its
  `event_id`, tagging anything seen twice as `REDELIVERED`. This is the same protocol a
  browser tab speaks — a browser adds rendering on top, not a different wire format.
- **What a browser would add — actual pixels rendering correctly — was not verified**, and
  that limitation is real. What *was* verified is everything upstream of rendering: that the
  live data path genuinely delivers duplicates under a real restart, and that the exact
  reducer code a browser would run does not double-count them (proven by unit test against
  the real reducer, not a mock of it).

### Reproducing the redelivery for real — harder than expected, and worth knowing why

First attempts (a single timed `Stop-Process -Force` on the broadcaster, mid-stream) produced
**zero** redeliveries, repeatedly. Not a sign nothing was wrong — a sign the kill kept landing
*between* batches, at an already-committed boundary, which is what usually happens when a
batch's round trip (poll → publish to Redis → delete cache key → commit) is fast relative to
how precisely a human can time a kill from outside the process. Two things made it
reproducible instead of a matter of luck:

1. **Rewinding the consumer group's committed offset backward** (`kafka-consumer-groups.sh
   --reset-offsets --shift-by`), the same technique `ARCHITECTURE.md` §11 and the Phase 3 bar
   aggregator story (§1.4) already document for exactly this purpose — it turns "hope the
   kill lands mid-batch" into "guarantee the next restart has already-delivered offsets to
   replay."
2. **A live, continuous observer connection**, not a fresh script per attempt. The first
   several tries used a new script invocation for each phase of the experiment, and a fresh
   process's `seen` set starts empty — it cannot recognize "I already showed you this" about
   events a *previous* process instance received. Redelivery is only observable as
   redelivery to something that was watching continuously across the restart, which is also
   exactly what a real browser tab does and a fresh test script does not.

With both in place: **56 confirmed redeliveries** of already-delivered messages, live,
through the real `/ws/market` endpoint, across several restart cycles — consistently
`market.quote` messages for AAPL specifically. That consistency has a mundane explanation
worth knowing rather than shrugging off: `partition_key` is the symbol (§1.3), so every
AAPL quote lands on the same Kafka partition, and that partition happened to be the one still
carrying backlog (still "mid-batch" in wall-clock terms) whenever a kill landed, while other
partitions had already drained to the live tail and their batches committed too fast to
catch. **No `market.trade` redelivery was directly witnessed** in this session, despite
substantial effort (a full topic-offset reset to 0, multiple targeted rewinds, a
reduced-session-timeout rapid-restart loop) — this is disclosed rather than papered over.

**Why the missing direct trade-type observation doesn't weaken the conclusion.** `market.
trade` and `market.quote` messages are not handled by different code anywhere in this path:
`KafkaEventConsumer` batches whatever `Sequence[MarketEvent]` a poll returns regardless of
type, `MarketBroadcastService._broadcast_one` calls the identical `publish`/`delete` pair for
every event type, and Kafka's commit-after-processing behavior operates on offsets, not event
types. There is no branch anywhere that could make a `market.trade` behave differently from a
`market.quote` under redelivery. The 56 witnessed quote redeliveries are proof the mechanism
is real in this system, today, under real restart conditions; the shared code path is why
that evidence transfers to trades without needing to independently witness the identical
mechanism a second time under a different label.

### The fix

`frontend/src/hooks/useMarketSocket.ts`: `TradeMessage` now carries `event_id` (already
present on the wire — every domain event has one — just not previously declared in the
TypeScript shape). The reducer's trade case now checks whether an entry with that `event_id`
already exists in `state.trades` before appending; if so, it updates `latestTrade` (harmless
— an identical value overwriting itself) but leaves the tape alone. `latestQuote`/`latestBar`
needed no equivalent change: they're already overwrite-semantics ("the current value"), never
append-semantics, so a duplicate quote was never visibly wrong the way a duplicate tape row
would be — which the live quote redeliveries incidentally proved directly, since nothing
looked wrong in the (would-be) UI despite 56 real duplicate messages arriving.

Two new tests in `useMarketSocket.test.ts` prove this against the real reducer, not a mock of
it: one sends the identical trade message twice and asserts the tape stays at length 1 while
`latestTrade` still updates; one sends two genuinely different trades and asserts both
appear, so the dedup logic can't be satisfied by a broken version that just caps the tape at
length 1.

### The `app.state.cache` regression test

Added `tests/unit/test_dependencies.py`, in two parts:

1. **Direct dependency-level tests** — `get_cache`, `get_database`, and `get_settings_dep`
   (all three follow the identical "read `app.state.X`, raise `RuntimeError` if unset"
   shape) each get a test proving they raise with a message naming *which* dependency is
   missing, against a bare `FastAPI()` app with nothing attached — no fixture, no lifespan,
   no incidental coverage possible. Plus one positive-path test (`get_cache` returns exactly
   the object that was attached), so a typo in the attribute name would fail it too.
2. **A literal reproduction of the historical bug** — builds an app exactly the way
   `test_market_api.py`/`test_vertical_slice.py` build theirs (`create_app()` +
   `httpx.ASGITransport`, confirmed by reading `ASGITransport`'s actual constructor signature
   to have no `lifespan` parameter at all — it never sends the ASGI lifespan protocol),
   deliberately attaches nothing, and hits a real cache-dependent route. Asserts `/health`
   (needs nothing from `app.state`) still answers normally on the same unwired app, while
   `/api/v1/instruments` returns the API's generic 500 — proving the failure is specific to
   the missing cache dependency, not a broken app in general. The response body itself can't
   name the cause (`api/errors.py` deliberately strips exception detail from what it returns,
   logging it instead — "an exception message is an information leak waiting to happen"),
   which is exactly why the dependency-level tests above exist: they're where the real
   diagnostic message is actually assertable.

This file would fail immediately if either `test_market_api.py` or `test_vertical_slice.py`
ever again forgot to attach `app.state.cache`, independent of whether anything else in the
suite happened to exercise that code path — which was precisely the missing signal before.

### Housekeeping

`frontend/.env` (created to point the manually-started dev server at the real API on
`127.0.0.1:8000`) leaked into `vitest run` too — Vite loads `.env` for every command that
resolves its config, tests included — and broke two tests that assert the *default*
`apiBaseUrl` when no override is set (`config.test.ts`, `health.test.ts`). Not a product bug:
removed the file once manual verification was done, confirmed both tests pass clean without
it. Worth remembering for next time: a local `.env` written for manual poking is invisible to
`git status` (correctly gitignored) but not invisible to Vite's own test runner — it has to
be removed, not just left "for later," before trusting a test result.

All manually-started processes (five Python workers, the alternate-port frontend dev server)
were stopped at the end of this session; Docker's Postgres/Redis/Kafka were left running
(pre-existing, not started by this session). `git status` confirms a clean, minimal diff —
no stray `.env` files, no leftover process artifacts.

### Final numbers

Full backend suite, one more time, after the fix and the new regression test:
**914 passed, 0 failed** (909 + the 5 new `test_dependencies.py` tests). `ruff`,
`ruff format --check`, `mypy --strict` (140 source files) and `lint-imports` all clean.
Frontend: `eslint`, `prettier --check`, `tsc --noEmit`, and `vitest run` — **32 passed**
(30 + the 2 new redelivery-dedup tests) — all clean.
