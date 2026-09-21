# Market data ingestion

How market data gets into TradeWatch, and what happens when it goes wrong.

---

## The shape

```
  ┌──────────────────┐
  │ External provider│  Alpaca REST + WebSocket, or the synthetic generator
  └────────┬─────────┘
           │  vendor JSON — field names stop here
  ┌────────▼─────────────────────────────────┐
  │ Adapter (infrastructure/providers/*.py)  │  the only code that knows the vendor
  │   ├─ normalization.py: timestamps,       │
  │   │  decimals, symbols, event identity   │
  │   └─ resilience.py: retry, backoff,      │
  │      rate limit, reconnect, staleness    │
  └────────┬─────────────────────────────────┘
           │  TradeEvent / QuoteEvent / BarEvent — the internal format
  ┌────────▼─────────────────────────────────┐
  │ IngestionService (application/ingestion) │  deduplicate, count, publish
  └────────┬─────────────────────────────────┘
           │  EventPublisher port
  ┌────────▼─────────────────────────────────┐
  │ InMemoryEventBus  →  Kafka in Phase 2    │
  └──────────────────────────────────────────┘
```

Nothing above the adapter knows a vendor exists. That is ADR-0004, and the test for it is
that swapping `TW_PROVIDER` changes one line of configuration and nothing else.

---

## The normalized event

Every provider produces the same three types, defined in `tradewatch.domain.events`. The
envelope every one of them carries:

| Field | Meaning |
| --- | --- |
| `event_id` | Deterministic UUIDv5 over a natural key (ADR-0012). The idempotency key. |
| `event_type` | `market.trade`, `market.quote`, `market.bar.closed`, `market.bar.corrected` |
| `symbol` | Upper-case ticker. Also the Kafka partition key, which is what guarantees per-symbol ordering. |
| `occurred_at` | The venue's timestamp. Drives all business logic. |
| `ingested_at` | When we received it. Drives lag metrics. |
| `source` | Which provider it came from — `alpaca`, `synthetic`, `replay`. |
| `producer` | Which process emitted it, with its version. |
| `correlation_id` | Inherited by every derived event, so an alert traces back to a trade print. |

`source` and `producer` are both there because they answer different debugging questions.
After a provider swap, `source` is the only thing that says which rows came from where.

Per-type payloads add `price`/`size` for trades, the four book fields for quotes, and OHLCV
for bars. Money is always `Decimal`, and always a **string** in JSON — a JSON number becomes
an IEEE-754 double in every JavaScript client there is (ADR-0009).

---

## Providers

| `TW_PROVIDER` | What it is | Needs |
| --- | --- | --- |
| `synthetic` *(default)* | Seeded generator. No key, no network, no quota, deterministic. | `TW_SYNTHETIC_SEED` |
| `replay` | Playback of a recorded JSONL session. | `TW_REPLAY_PATH` |
| `alpaca` | Alpaca Market Data API v2 (ADR-0011). | `TW_PROVIDER_API_KEY` + `_API_SECRET` |

Setting a provider without its credentials fails at **startup**, not at the first request —
because an auth failure hours later, in a stack trace that does not mention environment
variables, is a bad way to learn you forgot one.

### The synthetic provider is not a toy

It is the default for development and CI, and it does something real data cannot: produce a
specific failure on demand.

```python
SyntheticProvider(
    ["AAPL"],
    scenario=SyntheticScenario(
        drop_after=50,                      # socket closes mid-session      (F1)
        malformed_at=frozenset({10, 20}),   # unparseable frames             (F3)
        duplicate_at=frozenset({5}),        # a repeated print               (F7)
        volume_spike_at=frozenset({300}),   # something for Phase 8 to find
        stall_after=100,                    # connected, silent — the nasty one
    ),
)
```

Its distributions are invented, so a strategy that works on it proves nothing about real
markets. What it proves is that the pipeline works.

---

## Resilience

| Failure | What happens |
| --- | --- |
| **Connection drops** (F1) | `reconnecting()` reopens with exponential backoff and full jitter. The attempt counter resets after any successful message, so a week of uptime does not inherit an old outage's backoff. |
| **Rate limited** (F2) | A client-side token bucket stays under the documented ceiling. A 429 is still honoured, using the provider's own `Retry-After` in preference to our curve. |
| **Malformed message** (F3) | Counted, logged with the offending fragment, skipped. Never retried — the same bytes parse the same way — and never silently dropped. One bad tick must not end a session. |
| **Timeout** | Its own error class, because it usually means degraded rather than gone, and that is worth seeing separately. |
| **Bad credentials** | Not retryable. A bad key does not become good on the fourth attempt, and retrying burns the rate-limit budget to learn nothing. |
| **Duplicate events** | Two layers: a bounded in-flight window, and `UNIQUE (event_id)` in the database. See below. |
| **Publish fails** (F4) | Counted, never raised. Quotes are shed first: losing a quote costs a tick of book detail, losing a trade corrupts the tape. |
| **Silent stall** | `StalenessMonitor` watches per-symbol gaps — including symbols that have *never* sent anything, which is how a failed subscription is caught. |
| **Cancellation** | Propagates immediately. It is not an error and must never be retried, or shutdown hangs. |

**Full jitter, not fixed backoff.** `sleep = random(0, min(cap, base × 2^attempt))`. Fixed
backoff makes every client retry in lockstep, so a provider recovering from an outage is
immediately hit by a synchronised herd from everyone who was waiting.

### Nothing is silently discarded

Every failure path increments a counter on `IngestionStats` — `duplicates`, `malformed`,
`publish_failures`, `reconnects`, `stale_warnings` — and every one of those counters is
asserted on by a test. That is what makes "do not silently discard errors" checkable rather
than aspirational. They become Prometheus metrics in Phase 10.

---

## Deduplication

Two layers, catching different things.

**`UNIQUE (event_id)` in PostgreSQL** is the guarantee. It is correct across restarts,
replicas and Kafka redeliveries, because it is a fact about stored rows.

**The in-flight window** is the optimisation. It stops a duplicate before it is serialized,
published and round-tripped through a database only to be discarded. It is bounded by both
age and count, and it *forgets* — deliberately. An unbounded set would add no correctness
the database does not already provide, in exchange for a memory leak that grows with every
tick.

Both only work because **event ids are deterministic** (ADR-0012). An earlier draft of
`ARCHITECTURE.md` §7.1 specified a fresh UUIDv7 per event, which cannot deduplicate anything
— the same trade normalized twice would get two ids and the unique constraint would never
fire. Since a reconnect replay, an overlapping backfill and a Kafka replay all re-normalize
the same trade, that was a real hole.

---

## Running it

```bash
uv run tradewatch-ingest        # from backend/
```

With the default configuration it streams synthetic trades and quotes for `TW_SYMBOLS` into
an in-process bus, reporting stats on shutdown. Phase 2 swaps the bus for Kafka; nothing
else in the worker changes, which is the point of the port.

---

## Testing

| Where | What | Network |
| --- | --- | --- |
| `tests/unit/test_normalization.py` | Timestamps in every format and epoch unit, decimals, symbols, event identity | No |
| `tests/unit/test_resilience.py` | Backoff curves, jitter bounds, rate limiting, reconnect, staleness | No |
| `tests/unit/test_ingestion.py` | Deduplication, the synthetic and replay providers, the service's failure paths | No |
| `tests/unit/test_alpaca_provider.py` | The real adapter against a mock transport and a scripted socket | No |
| `tests/integration/test_ingestion_persistence.py` | Ingestion through to PostgreSQL, including replay-produces-no-duplicates | Database only |
| `tests/provider/test_alpaca_live.py` | The live service | **Yes — opt-in** |

Live provider tests are marked `provider` and deselected by default in `pyproject.toml`, and
the check scripts exclude them again by name because an explicit `-m` overrides addopts.
They skip with an explanatory message when credentials are absent.

```bash
uv run pytest -m provider     # needs TW_PROVIDER_API_KEY and TW_PROVIDER_API_SECRET
```

### Recording a fixture

The most useful thing to do with credentials is capture a real session once and commit it,
so everyone else can replay it forever without any:

```python
from tradewatch.infrastructure.providers.replay import capture_stream

await capture_stream(provider.stream_trades(["AAPL"]), "tests/fixtures/alpaca-aapl.jsonl")
```

---

## Known limitations

- **The Alpaca adapter has never run against the live service.** It is written to the
  documented API v2 shapes and every test drives it with constructed payloads. If the live
  API differs, the normalizers in `alpaca.py` are what need changing. `pytest -m provider`
  with real credentials is what closes that gap.
- **Alpaca's free tier is the IEX feed**, a single venue rather than the consolidated tape.
  Fine for a simulation; it belongs in any statement about the data, and particularly in
  Phase 8's model card, since the detector's features are volume-based.
- **One WebSocket connection per account**, so the ingestion service must run as a
  singleton. That constrains Phase 12's deployment topology.
- **Bars are not yet aggregated from trades.** The stateful aggregator is Phase 3; today
  bars come from the provider's own historical endpoint.
- **Ingested events are not yet written to the database by a worker.** The service publishes
  to the bus; the consumer that persists them is the Phase 2 processor. The integration
  tests do the mapping themselves to prove the two halves fit.
