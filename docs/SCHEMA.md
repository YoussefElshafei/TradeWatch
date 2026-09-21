# Database schema

The tables, why they are shaped the way they are, and what was deliberately left out.
`ARCHITECTURE.md` §6 defines the conceptual schema; this describes what was actually built
and the decisions made along the way. Alembic migrations are the source of truth.

---

## Shape at a glance

```
                     ┌──────────────┐
                     │ instruments  │◄──────────────────────────┐
                     └──────┬───────┘                           │
            ┌───────────────┼───────────────┐                   │
            ▼               ▼               ▼                   │
        ┌────────┐     ┌────────┐      ┌────────┐               │
        │ trades │     │ quotes │      │  bars  │               │
        └────────┘     └────────┘      └────────┘               │
                                                                │
  ┌─────────┐                                                   │
  │  users  │──┬──► strategies ──► strategy_deployments          │
  └─────────┘  │        │                    │                   │
               │        └──► backtest_runs   │                   │
               │               ├─► backtest_trades ──────────────┤
               │               └─► backtest_equity_points        │
               │                                                 │
               ├──► portfolios ──┬──► orders ──► fills           │
               │                 ├──► positions ─────────────────┤
               │                 └──► equity_points              │
               │                                                 │
               ├──► alert_rules ──► alerts ─────────────────────┤
               ├──► refresh_tokens                               │
               └──► audit_log (SET NULL)                         │
                                                                 │
                     anomaly_detections ─────────────────────────┘
                     model_versions (standalone)
```

21 tables, 12 native enum types, one migration.

---

## Decisions

### Users exist now, authentication does not

`ARCHITECTURE.md` §14 states the constraint: *auth must land before any multi-user feature
stores user data, or every table needs a retrofit migration and every endpoint an audit.*
Creating `users` alongside the tables that reference it satisfies that at the schema level.

What is **not** here is the authentication logic — password hashing, token issuance,
rotation, reuse detection. That is ADR-0010 and Phase 5. `password_hash` is a column with a
home, not a claim that anything safe fills it yet.

This is a deliberate deviation from `DEVELOPMENT_PLAN.md`, which scheduled these tables for
Phases 5–9. Building the whole schema in one coherent migration set is cheaper than five
retrofits, and it is the option the architecture's own ordering constraint argues for.

### Roles are a column, not three tables

§9.3 defines exactly three roles: `viewer`, `trader`, `admin`. A `roles` table plus a
`permissions` table plus a join table would be three joins to resolve three constant
values — machinery with no question to answer. The moment a role becomes something a user
can define, that is a table and a migration worth writing.

### Events are truth; state is a projection

`fills` is append-only and authoritative. `positions` and `equity_points` are derived and
stored only because recomputing a portfolio from every fill on each page load would be
absurd. `PositionRepository.rebuild_from_fills` regenerates positions from fills alone, and
a test asserts the result matches the stored rows — so the claim is checked rather than
asserted.

The same reasoning gives `bars` a `revision` rather than an in-place edit: a late trade
produces a correction a consumer can notice, instead of history quietly changing underneath
one (failure F11).

### Identity: three different choices, for three different reasons

| Table group | Primary key | Why |
| --- | --- | --- |
| Entities (`users`, `orders`, `strategies`, …) | UUIDv7 | Generated client-side, so an object knows its id before commit and a graph of rows can be built in memory. Time-ordered, so inserts land at the right-hand edge of the index instead of scattering across it the way UUIDv4 does. |
| `trades`, `quotes` | `BIGINT` identity + `UNIQUE(event_id)` | Eight bytes against sixteen, repeated across every index on the largest tables in the system. The unique `event_id` carries the idempotency instead. |
| `bars`, `positions`, `equity_points`, `backtest_equity_points` | Natural composite | Their identity *is* the composite — "one bar per symbol per minute" is what uniqueness means for a bar. It also happens to be exactly the index the read path wants, and it makes the upsert a plain `ON CONFLICT`. |

### Idempotency is in the schema, not in application code

Kafka delivers at least once (§7.4), so every ingestion path will be called twice. Three
constraints absorb that:

- `UNIQUE (trades.event_id)` / `UNIQUE (quotes.event_id)` — a replayed batch writes nothing.
- `PRIMARY KEY (bars.instrument_id, interval, bar_start)` — a corrected bar overwrites the
  bar it corrects, and `BarRepository.upsert_many` additionally refuses to let an older
  revision overwrite a newer one.
- `UNIQUE (orders.client_order_id)` — one signal produces one order, even across a restart.
- `UNIQUE (alerts.alert_rule_id, dedupe_key)` — the alert-storm guard (F18), which holds
  across a dispatcher restart and a second replica in a way in-process state could not.

### Native enums, with their values spelled out

Twelve `CREATE TYPE`s. Four bytes instead of a repeated string, and the database rejects a
value the application never intended rather than storing a typo that is discovered when a
query silently returns nothing.

The trap is that SQLAlchemy persists enum *member names* by default, so
`BarInterval.ONE_MINUTE` would be stored as `ONE_MINUTE` while configuration, logs and the
wire format all say `1m`. `values_callable` fixes that, and a migration test asserts the
stored labels are `['1m', '5m', '1h', '1d']`.

Enums are created and dropped explicitly in the migration. PostgreSQL does not drop a type
when the last table using it is dropped, so an implicit approach leaves types behind and the
next upgrade fails on "type already exists" — which the `upgrade → downgrade → upgrade` test
catches.

### Case-insensitive email without an extension

`UNIQUE INDEX uq_users_email_lower ON users (lower(email))` rather than a `citext` column.
Same guarantee, no extension dependency (managed PostgreSQL does not always allow them), and
the stored value keeps the case the user typed. The cost is that every lookup must say
`lower(email) = lower(:email)` to use the index — which the repository does, in one place.

### What is *not* constrained, and why

A constraint that rejects real data is worse than no constraint:

- **Quotes may be crossed.** No `ask >= bid` check. Locked and crossed markets are real,
  brief, and precisely the moments worth keeping.
- **Positions may be negative.** Short selling is deferred (D16), but a schema forbidding it
  would need a migration rather than a feature flag when it arrives.
- **Cash may go negative.** An over-drawn simulated account is a state worth surfacing; a
  constraint would hide a bug in the execution simulator instead of exposing it.
- **`ingested_at` may precede `occurred_at`.** Provider clocks drift, and dropping real
  market data because someone else's NTP is off is the worse failure. Phase 10 turns the
  difference into a lag metric.

### Cascades

Everything a user owns cascades on delete: strategies, portfolios, backtests, alert rules,
alerts, sessions. There is no real money and no retention requirement, so "delete my
account" should actually delete it.

Two exceptions:

- **`instruments` uses `ON DELETE RESTRICT` everywhere.** Market data is the system of record
  (ADR-0002); a delisted symbol changes `status`, it does not take its price history with it.
- **`audit_log.user_id` is `ON DELETE SET NULL`.** The record that an action happened
  outlives the account that performed it, which is the entire point of an audit trail.

### Money and time

`NUMERIC(18, 8)` for every price, quantity, and cash amount, mapped to Python `Decimal`
(ADR-0009). A test asserts `0.1 + 0.2 == 0.3` through a round trip, which is false in binary
floating point and true here. Python's refusal to add `Decimal` to `float` caught a bug in
the test fixtures, which is the rule working.

Every timestamp is `TIMESTAMPTZ`. A schema test asserts no timestamp column is
`TIMESTAMP WITHOUT TIME ZONE`, because that is the bug that shows up twice a year.

---

## Indexes

Each one exists for a named query, and `tests/integration/test_queries.py` checks both that
it exists and that the query actually uses it.

| Index | Query it serves |
| --- | --- |
| `pk_bars (instrument_id, interval, bar_start)` | The chart and backtest read path |
| `ix_bars_interval_bar_start` | Cross-sectional: every symbol in one window, which the PK cannot serve because its leading column is the instrument |
| `ix_trades_instrument_id_occurred_at` | The tape, and backfill gap checks |
| `ix_quotes_instrument_id_occurred_at` | Same, for quotes |
| `uq_trades_event_id`, `uq_quotes_event_id` | Idempotent ingestion |
| `ix_orders_portfolio_id_created_at` | The order blotter |
| `ix_anomaly_detections_instrument_id_detected_at` | One symbol's detection history |
| `ix_anomaly_detections_detected_at` | The market-wide anomaly feed |
| `ix_alerts_user_id_created_at` | The alert feed |
| FK indexes on `fills`, `refresh_tokens`, `strategies`, `portfolios`, `orders`, `backtest_trades`, `positions` | PostgreSQL does not index foreign keys automatically, and cascades scan them |

Four partial indexes, each covering a hot query over a small slice of a table that grows
without bound:

| Partial index | Predicate |
| --- | --- |
| `ix_backtest_runs_queued` | `status = 'queued'` — the runner's claim query |
| `ix_strategy_deployments_running` | `status = 'running'` — the strategy engine's startup query |
| `ix_alerts_unacknowledged` | `acknowledged_at IS NULL` — the unread badge |
| `uq_model_versions_one_active_per_model` | `is_active` — enforces one active model version |

### What is deliberately not indexed

There is no index on `trades.occurred_at` alone. No current query filters trades by time
across all symbols, and an unused index still costs a write on every insert into the largest
table in the system. `test_a_query_without_the_leading_column_cannot_use_the_index` documents
this, so the day such a query appears there is a note explaining what to add.

Column order matters for the same reason: `(instrument_id, occurred_at)` serves "this symbol,
this window" because the leading column is what a B-tree can seek on. The reverse would not.

---

## Growth

`trades` and `quotes` grow fastest. The escalation path, from `ARCHITECTURE.md` §6.1 and
decision D4:

1. Indexing — where things stand now.
2. Native range partitioning by month.
3. A retention job and downsampling.
4. TimescaleDB.

Each step is taken when a **measurement** shows the previous one is insufficient, not before.
No step has been taken, because no measurement exists yet.

---

## Testing

| File | What it covers |
| --- | --- |
| `tests/integration/test_migrations.py` | Schema builds from empty; `upgrade → downgrade → upgrade` on a scratch database; autogenerate finds no difference between models and migrations; every migration has a real downgrade; no migration imports application code |
| `tests/integration/test_models.py` | Every CHECK, UNIQUE, NOT NULL and cascade, by constraint name |
| `tests/integration/test_repositories.py` | CRUD, idempotent replay, ownership scoping with a second user, rebuilding positions from fills |
| `tests/integration/test_queries.py` | Index existence, column order, query plans, `NUMERIC` precision, `TIMESTAMPTZ` |
| `tests/unit/test_entities.py` | Domain invariants with no database |

The single most valuable test is `test_autogenerate_detects_no_difference`. Models and
migrations drift silently — someone edits a model, forgets the migration, and tests pass
because the test schema was built with `create_all`. Building the test schema from the real
migrations and then asking autogenerate whether it still has anything to say is what catches
that.
