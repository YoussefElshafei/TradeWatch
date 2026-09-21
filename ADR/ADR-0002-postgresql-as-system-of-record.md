# ADR-0002: PostgreSQL as the system of record

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 1

## Context

TradeWatch stores two quite different kinds of data:

1. **Relational, transactional, correctness-critical:** users, strategies, portfolios,
   orders, fills, positions, backtest runs. These need foreign keys, constraints, and
   transactions — a fill that updates a position and a cash balance must be atomic.
2. **Append-only time series, high volume:** trades, quotes, bars. These need efficient
   range scans by (symbol, time) and cheap sequential writes.

The obvious temptation is to use a purpose-built time-series database for the second
category. That means running and operating two databases, writing two data-access layers,
and giving up joins between the two categories — and backtests join heavily across both.

## Decision

PostgreSQL is the single system of record for all persistent data, both categories.

- Kafka is a transport with finite retention; Redis is a disposable cache. Neither is truth.
  If both were wiped, PostgreSQL alone would allow the system to be rebuilt.
- Access is through SQLAlchemy 2.0 (async) repositories; Alembic owns the schema.
- Time-series tables are append-only with `(instrument_id, occurred_at DESC)` indexes; `bars`
  uses a natural composite primary key `(instrument_id, interval, bar_start)`.
- Derived tables (`positions`) are projections: they exist for query speed, the underlying
  events (`fills`) are truth, and a rebuild job must be able to regenerate them exactly.
  That property is enforced by a test.
- Scaling the time-series tables is a **deferred** decision (D4). The escalation path, in
  order, is: indexing → native range partitioning by month → retention/downsampling →
  TimescaleDB. Each step is taken only when a measurement shows the previous one is
  insufficient.

## Alternatives considered

**PostgreSQL + TimescaleDB from the start.** Hypertables, native compression, and
continuous aggregates are genuinely well suited to bars and ticks, and it is still
PostgreSQL. Rejected for now: it adds an extension dependency and deployment constraint
(managed cloud PostgreSQL may not offer it) before any evidence exists that plain
PostgreSQL is insufficient at this project's data volume. It remains the most likely
answer to D4.

**PostgreSQL + InfluxDB / ClickHouse.** ClickHouse in particular would be dramatically
faster on large analytical scans. Rejected: two databases, two access layers, no joins
across the boundary, dual-write consistency problems, and a much heavier local setup —
all to solve a performance problem that has not been measured and may not exist at this
scale.

**MongoDB for everything.** Flexible schema, easy ingestion. Rejected: the trading domain
is relational and correctness-critical. Orders, fills, positions, and cash want foreign
keys and transactions, and giving those up to save some migration work is a bad trade for
a system whose entire point is getting the accounting right.

**SQLite.** Zero operational cost, excellent for tests. Rejected: no concurrent writers,
which the multi-service architecture requires from Phase 2 onward. Also rejected as a test
substitute — repository tests run against real PostgreSQL in a container, because testing
against a different database is testing a different system.

## Consequences

**Positive.** One database to run, back up, migrate, and reason about. Real transactions
across the trading domain. Joins between market data and trading data, which backtest and
portfolio queries need constantly. `NUMERIC` support for exact money (ADR-0009). Everything
is rebuildable from one place.

**Negative.** Plain PostgreSQL will eventually be slower on wide time-series scans than a
purpose-built store, and `trades`/`quotes` will grow large enough to need partitioning and
a retention policy. That day is a measurement away, not a guess away — and it is written
down as D4 so it is not a surprise.

**Neutral.** JSONB is used for genuinely open-ended fields (strategy parameters, anomaly
features, backtest metrics) — these are configuration and payloads, not relationships, and
forcing them into columns would be worse.

## Revisit when

A measured query p95 on the bars or trades read path degrades past what the dashboard
needs, or a time-series table crosses a row count where maintenance operations become
disruptive. Then take the next step on the escalation path — and record the measurement in
the ADR that does so.
