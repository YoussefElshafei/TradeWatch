# The vertical slice

How market data gets from a provider to a chart in a browser, as actually built.

**Kafka is not in this path.** The event bus today is an in-process queue. That is a
deliberate Phase 1 choice (ADR-0001), and its limitations are the argument for Phase 2.

---

## The path

```
┌──────────────────────┐
│ SyntheticProvider    │  seeded, deterministic, no network   (or Alpaca — ADR-0011)
└──────────┬───────────┘
           │  vendor-shaped data
┌──────────▼───────────┐
│ Normalization        │  UTC timestamps, exact decimals, deterministic event ids
└──────────┬───────────┘
           │  TradeEvent / QuoteEvent / BarEvent
┌──────────▼───────────┐
│ IngestionService     │  deduplicate in flight, count, publish
└──────────┬───────────┘
           │  EventPublisher port
┌──────────▼───────────┐
│ InMemoryEventBus     │  ← Kafka replaces this in Phase 2
└──────────┬───────────┘
           │  EventConsumer port
┌──────────▼───────────┐
│ MarketDataProcessor  │  batch, one transaction per flush, idempotent
└──────────┬───────────┘
           │  MarketDataStore port
┌──────────▼───────────┐
│ PostgreSQL           │  instruments, trades, quotes, bars
└──────────┬───────────┘
           │
┌──────────▼───────────┐
│ FastAPI              │  GET /api/v1/market/{symbol}/bars
└──────────┬───────────┘
           │  JSON, money as strings
┌──────────▼───────────┐
│ React price chart    │
└──────────────────────┘
```

Each arrow crosses a port, so each box can be replaced without the others noticing. That is
what Phase 2 relies on.

---

## Running it

Three terminals, from a clone:

```bash
# 1. Infrastructure and schema
docker compose -f docker/docker-compose.yml up -d
cd backend && uv run alembic upgrade head

# 2. The pipeline: provider → bus → processor → PostgreSQL
cd backend && uv run tradewatch-pipeline

# 3. The API and the UI
cd backend  && uv run tradewatch-api      # http://127.0.0.1:8000/docs
cd frontend && npm run dev                # http://127.0.0.1:5173
```

The pipeline backfills four hours of minute bars per symbol on startup, so the chart has
history the moment it opens, then streams live trades.

`tradewatch-ingest` still exists and runs ingestion alone, publishing to a bus nothing
drains. Useful for watching normalization; useless for getting data into the database.

---

## Why one process

`tradewatch-pipeline` runs ingestion **and** processing together. That is not a design
preference — an in-process queue cannot be read from another process, so today the producer
and the consumer have to live in the same one.

Everything ADR-0001 lists as a reason for Kafka follows from that:

| Property | Today | With Kafka |
| --- | --- | --- |
| Independent failure domains | A processor crash takes ingestion with it | Separate processes |
| Restart safety | Queued events are lost on restart | Offsets survive |
| Replay | None | Re-read from any offset |
| Fan-out | One consumer | Aggregator, strategy engine, detector, all independent |
| Backpressure | Bounded queue; ingestion sheds when full | Broker absorbs it |

So the limitations are real, and they are the specification for Phase 2.

---

## The processor

`tradewatch.application.processing.MarketDataProcessor` consumes events and persists them.
It lives in `application` and depends on the `MarketDataStore` port, not on a repository —
`import-linter` enforces that, which is what lets its batching and failure handling be
unit-tested with no database at all.

**Batching.** Events accumulate per type and flush when the buffer fills or the caller asks.
Each flush is **one transaction**: a batch lands completely or not at all, because a
half-written batch of trades is a corrupted tape and every number computed from it afterwards
is wrong in a way nobody can see.

**Instrument resolution.** A symbol is resolved to an instrument id once and cached. A symbol
nobody has registered gets a provisional row on first sight, because a symbol appearing
mid-stream is normal and dropping its data until somebody runs a sync script would produce a
gap that looks exactly like a quiet market.

### Failure handling

| Failure | What happens |
| --- | --- |
| **Duplicate event** | Absorbed by the store's idempotent writes and counted. Deterministic event ids (ADR-0012) mean a reprocessed stream writes nothing new. |
| **Unstorable event** | Counted, logged with context, set aside. Retrying replays the same rejection forever, and one bad tick must not stop the pipeline (F3). |
| **Database unavailable** | The batch is **kept**, retried with backoff, and propagates if attempts run out. Nothing is dropped, and the supervisor sees a failed processor rather than one quietly making no progress (F8). |
| **Constraint violation** | Classified as permanent, not retried. The whole batch rolls back. |
| **Connection refused / timeout** | Classified as retryable — these arrive from the driver, not SQLAlchemy, so they are caught explicitly. |
| **Restart** | Reprocessing is free. Verified by a test. |
| **Cancellation** | Flushes what was accepted, then propagates. Buffered events are not lost to a Ctrl-C. |

Every one of those increments a counter on `ProcessorStats` that a test asserts on. That is
what makes "does not silently discard" checkable rather than aspirational.

---

## The API

Read-only. There is no client-facing route that writes a tick, a bar or an instrument — that
path exists only inside the trust boundary (`ARCHITECTURE.md` §3.9), and a test asserts that
no `/api/v1/market/*` route exposes anything but `GET`.

| Endpoint | Purpose |
| --- | --- |
| `GET /api/v1/instruments` | The universe; populates the symbol selector |
| `GET /api/v1/instruments/{symbol}` | One instrument |
| `GET /api/v1/market/{symbol}/bars` | OHLCV over `[start, end)`, with `interval` and `limit` |
| `GET /api/v1/market/{symbol}/latest` | Last stored trade and quote |

**Status codes carry meaning**, because a client can act on some failures and not others:

- **404** — the symbol is unknown. Stop asking.
- **422** — the request is wrong: reversed range, naive timestamp, limit out of bounds.
- **200 with an empty list** — the request was fine and there is no data. A market that has
  not opened yet is not an error, and returning 404 for it makes a normal Sunday
  indistinguishable from a typo. This is the one most often got wrong.
- **503** — the database is unreachable. Retry later. Not 500, which tells a client the
  server is broken and retrying is pointless.

Money is serialized as **strings**. A JSON number becomes an IEEE-754 double in every
JavaScript client, which would undo ADR-0009 on the last hop.

---

## The frontend

One view: a symbol selector, a price chart, and the latest price. Deliberately not the
TradeWatch dashboard — every extra surface here is a surface to rewrite when the real one is
designed.

It handles four states, because getting them wrong is what makes a data UI feel broken:
**loading**, **error** (with the message the API supplied, and a retry), **empty** (a known
symbol with no data — not an error), and **success**.

Charting is Recharts (ADR-0013), chosen because SVG output can be asserted against in jsdom.
A canvas chart renders nothing a test can see, so the end-to-end claim would be unverifiable.

---

## Testing

| Where | What |
| --- | --- |
| `tests/unit/test_processor.py` | Batching, deduplication, retries, every failure path, against a fake store |
| `tests/integration/test_processor_failures.py` | Constraint violations, transaction rollback, restart idempotency, a genuinely unreachable database |
| `tests/integration/test_market_api.py` | Every endpoint and every failure mode, against real PostgreSQL |
| `tests/integration/test_vertical_slice.py` | **The whole path**, with provenance checked value by value |
| `frontend/src/components/MarketView.test.tsx` | The four states, and that a chart path renders from API data |

The end-to-end test is the one that matters. It asserts that the OHLCV the API returns is
**the exact data the provider generated**, matched bar by bar — not a row count, which is
how an end-to-end test passes on data that happened to already be there.

---

## Known limitations

- **No Kafka**, so no replay, no restart safety, no independent processes. Phase 2.
- **Bars come from the provider's historical endpoint, not from the live trade stream.** The
  aggregator that derives bars from trades is Phase 3 (`ARCHITECTURE.md` §3.3). Until then
  `trades` and `bars` are independent series for the same symbol.
- **No WebSocket to the browser.** The chart fetches once; it does not update live. Phase 4.
- **The chart has fixed pixel dimensions** so it is testable in jsdom. Responsive rendering
  needs a `ResizeObserver` polyfill in the test setup (ADR-0013).
- **Frontend API types are hand-written**, mirroring the backend schemas. `ARCHITECTURE.md`
  §8.3 calls for generating them from the committed OpenAPI schema so a contract change
  breaks the build; that needs a codegen step in CI.
- **No authentication.** The API is bound to loopback and must not be exposed until Phase 5.
