# The market data pipeline

How market data gets from a provider to a chart in a browser, as actually built.

**Kafka is now in this path.** Phase 1's in-process queue could not survive a restart, could
not replay and could not fan out; Phase 2 replaced it with a durable log (ADR-0001) and split
the one process into two. The in-memory bus still exists, and still matters — it is what every
consumer is unit-tested against — but it is no longer how data reaches the database.

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
│ KafkaEventPublisher  │  acks=all, idempotent, keyed by symbol
└──────────┬───────────┘
           │
═══════════▼═══════════════════════════════  process boundary  ═══════════
           │
┌──────────▼───────────┐
│ Kafka                │  tw.market.{trades,quotes,bars.1m}.v1   (+ tw.dlq.v1)
└──────────┬───────────┘
           │
═══════════▼═══════════════════════════════  process boundary  ═══════════
           │
┌──────────▼───────────┐
│ KafkaEventConsumer   │  decode, validate, quarantine what cannot be read
└──────────┬───────────┘
           │  EventBatchConsumer port
┌──────────▼────────────────┐
│ MarketDataConsumerService │  store the batch, THEN commit the offset
└──────────┬────────────────┘
           │
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

Each arrow crosses a port, so each box can be replaced without the others noticing. Phase 2
is the test of that claim: swapping `InMemoryEventBus` for the Kafka adapter required **no
change** to `IngestionService` or `MarketDataProcessor`.

---

## Running it

From a clone:

```bash
# 1. Infrastructure and schema
docker compose -f docker/docker-compose.yml up -d     # PostgreSQL, Redis, Kafka
cd backend && uv run alembic upgrade head

# 2. History, once. Idempotent, so re-run it freely.
cd backend && uv run tradewatch-backfill --days 30

# 3. The producer: provider → Kafka
cd backend && uv run tradewatch-producer

# 4. The two consumers, in their own groups
cd backend && uv run tradewatch-consumer      # → PostgreSQL
cd backend && uv run tradewatch-aggregator    # → 1m bars, then 5m/1h/1d rollups

# 5. The API and the UI
cd backend  && uv run tradewatch-api      # http://127.0.0.1:8000/docs
cd frontend && npm run dev                # http://127.0.0.1:5173
```

**Order does not matter.** That is the point of the log. Start the consumer first and it
waits; start the producer first and the consumer catches up when it arrives. Kill either one
and the other keeps working.

The producer backfills four hours of minute bars per symbol on startup, so the chart has
history the moment it opens, then streams live trades and quotes. Topics are created on
first start (`TW_KAFKA_ENSURE_TOPICS`), because broker-side auto-creation is switched off —
an implicitly created topic gets one partition and infinite retention, and both would be
discovered much later.

### Other entry points

| Command | What it does | When |
| --- | --- | --- |
| `tradewatch-producer` | Provider → Kafka | The primary path |
| `tradewatch-consumer` | Kafka → PostgreSQL | The primary path |
| `tradewatch-aggregator` | Kafka → 1m bars → rollups | The primary path |
| `tradewatch-backfill` | Provider history → PostgreSQL | Once, then whenever a gap appears |
| `tradewatch-ingest` | Ingestion alone, to a bus nothing drains | Watching normalization |
| `tradewatch-pipeline` | The Phase 1 single process, in-memory bus, no broker | Development with no Kafka |

`tradewatch-pipeline` is kept deliberately. It is the fastest way to get data into a database
on a machine with no broker, and it is honest about what it gives up: no replay, no restart
safety, no independent failure domains, and — because an in-process queue applies
backpressure by refusing — it sheds live trades when the consumer falls behind. It is not the
production path and it is not the development default.

---

## The three processes

| | Producer | Consumer | Aggregator |
| --- | --- | --- | --- |
| Talks to the provider | Yes | Never | Never |
| Talks to PostgreSQL | Only the instrument bootstrap (best effort) | Yes | Yes |
| Produces to Kafka | Market topics | Dead-letter topic only | Dead-letter topic only |
| Consumes from Kafka | Never | Market topics | Trades only |
| Consumer group | — | `processor-persist` | `bar-aggregator` |
| Restart cost | The gap between trades | A redelivery, absorbed | A redelivery, absorbed |
| How redelivery is absorbed | — | `UNIQUE (event_id)`: the write is a *replacement* | `consumer_progress`: the write is an *addition*, so the position is stored with the bars and applied events are skipped |

**Read the last row rather than skimming it.** The two consumers absorb a redelivery by
completely different means, because only one of them can get away with the easy one. Persisting
a trade twice stores the same row; folding a trade into a bar twice adds its size twice, and
the resulting bar has perfect OHLC and a doubled volume. That bug was live here. An
accumulating consumer must record how far it has applied each partition, in the same
transaction as its output ([ADR-0016](../ADR/ADR-0016-idempotent-bar-aggregation-under-redelivery.md)).

A crash in one is not a crash in the others, and none loses data when another is down: the
producer keeps writing to the log, and each consumer's own offsets tell it where to resume.
That is the property Phase 1 could not have at any price — and with two consumer groups it is
now demonstrated rather than asserted.

---

## Kafka topology

Naming is `<prefix>.<domain>.<entity>[.<qualifier>].v<major>` (`ARCHITECTURE.md` §7.2). The
prefix is configuration; everything after it is a contract.

| Topic | Key | Partitions | Retention | Produced by | Consumed by |
| --- | --- | --- | --- | --- | --- |
| `tw.market.trades.v1` | `symbol` | 6 | 7d | producer | `processor-persist`, `bar-aggregator` |
| `tw.market.quotes.v1` | `symbol` | 6 | 24h | producer | `processor-persist` |
| `tw.market.bars.1m.v1` | `symbol` | 6 | 30d | producer | `processor-persist` |
| `tw.dlq.v1` | original key | 6 | 30d | consumer | an operator |

**Four topics, and this phase uses all four.** §7.2 also specifies `tw.signals.v1`,
`tw.orders.v1`, `tw.fills.v1`, `tw.anomalies.v1` and `tw.alerts.v1`. None is created: there
is no strategy engine, no paper trading engine and no detector, so those would be empty names
in a broker.

**Only 1-minute bars are published, and only by the backfill's producer path.** The other
intervals in `TW_BAR_INTERVALS` are derived from stored 1m bars rather than published as
events, and a bar event on another interval is refused by the producer rather than routed to
an invented topic.

The Phase 3 aggregator writes its bars straight to PostgreSQL rather than publishing them.
Phase 4's browser broadcaster is the first thing that will want them as events; publishing
them now would be a topic written to and never read.

### Partitioning, and what it actually guarantees

Every record is keyed by **symbol**. Kafka orders records within a partition, so:

- **Guaranteed:** every event for one symbol — trades, quotes and bars alike — lands on one
  partition and is consumed in the order it was produced. This is what the Phase 3 bar
  aggregator and every strategy will depend on.
- **Not guaranteed, and not claimed:** any ordering between symbols. AAPL's 10:00:01 trade
  may be processed after MSFT's 10:00:02 one.

Six partitions is the ceiling on consumer parallelism within the group. It is deliberately
not 1: a single-partition topic is a single consumer forever, and changing the count later
repartitions the keyspace and breaks the per-symbol ordering of anything in flight.

---

## The event envelope

One envelope for every message on every topic (`ARCHITECTURE.md` §7.1), JSON, with the
Pydantic models as the single schema definition (ADR-0005).

```json
{
  "event_id":       "e6b6...",
  "event_type":     "market.trade",
  "schema_version": 1,
  "symbol":         "AAPL",
  "occurred_at":    "2026-03-02T14:30:00Z",
  "ingested_at":    "2026-03-02T14:30:00.112Z",
  "source":         "synthetic",
  "producer":       "ingestion@0.1.0",
  "correlation_id": "9f1a...",
  "payload":        { "price": "130.01", "size": "25", "provider_trade_id": "…", "conditions": [] }
}
```

**Transport metadata is at the top level; the domain's own fields are nested under
`payload`.** That split is what lets a consumer read the envelope of a message it cannot
interpret — a future `schema_version`, an unknown `event_type` — and route it somewhere
useful instead of failing to parse the whole thing.

**Money is a string, never a JSON number** (ADR-0009). A float round-trip loses cents
silently, and every number computed from it afterwards is wrong in a way no exception reports.

**Encoding is deterministic**: sorted keys, fixed separators. The same event always produces
identical bytes, which makes the encoder assertable by equality and means a dead-letter
record can carry the event re-encoded and still be the original message byte for byte.

### Versioning

`schema_version` is explicit and checked before the payload is. This build accepts version
**1** and rejects anything else as permanent — not malformed, just from a future it does not
know about.

- **Compatible change** (a new optional field): same topic, same version. Consumers ignore
  fields they do not recognise. The domain models are `extra="forbid"`, so this is done
  deliberately — `decode` filters the payload to the fields the target model declares.
- **Breaking change** (removing or retyping a field, changing meaning): a **new topic** with
  an incremented major version, and a dual-write window. A topic's meaning never silently
  changes.

---

## Delivery semantics

**At-least-once, end to end.** Stated precisely, because the difference between this and what
people usually claim is where the data goes missing:

| Link | Guarantee | How |
| --- | --- | --- |
| Producer → broker | The broker has the record before `publish` returns | `acks=all`, and the acknowledgement is awaited |
| Producer retries | A retried batch does not become two records | `enable_idempotence=True` |
| Broker → consumer | Every record is delivered at least once | Offsets committed after the write |
| Consumer → PostgreSQL | A batch lands completely or not at all | One transaction per flush |

**This is not exactly-once, and nothing here claims it is.** Producer idempotence covers
retries *within one producer session*. It does not cover a producer that crashes after the
broker wrote the record but before the acknowledgement arrived — the process that restarts
and republishes is a different session, and the record exists twice. Kafka transactions would
narrow that window and would not remove the need for idempotent writes anyway, so they are
not used.

The duplicate is **expected**, and absorbed downstream. That is the design, not a gap in it.

---

## Idempotency

Three layers, and the important one is the last:

1. **Deterministic event ids** (ADR-0012). `event_id` is a UUIDv5 over a natural key built
   from the provider's own data, so the same trade normalized twice — after a reconnect,
   after an overlapping backfill, after a replay — produces the same id both times.
2. **An in-flight dedupe window** in `IngestionService`. Bounded, forgetful, and purely an
   optimisation: it saves a round trip and guarantees nothing.
3. **Database constraints.** This is the guarantee.
   - `trades` and `quotes`: `UNIQUE (event_id)`, written with `ON CONFLICT (event_id) DO NOTHING`.
   - `bars`: primary key `(instrument_id, interval, bar_start)`, written with an upsert
     guarded by `WHERE revision <= excluded.revision`, so a correction overwrites the bar it
     corrects and a stale revision never overwrites a newer one.

**What that means for the scenario that matters.** An event is processed, the consumer dies
before committing its offset, the event is delivered again, and it is processed again:

- a trade or quote is rejected by the unique constraint and counted as a duplicate;
- a bar is re-upserted with identical values, which rewrites the row and moves `updated_at`
  but changes nothing anyone reads.

The final database state is identical either way. `test_reprocessing_the_whole_topic_changes_nothing`
asserts exactly that, against a real broker and a real database, comparing stored values
rather than row counts.

---

## Failure handling

The whole design rests on one distinction: **is this failure retryable, or is it permanent?**
Confusing them produces the two worst outcomes available — a poison message that stops a
partition forever, or a database outage that silently discards an hour of market data.

| Failure | Classification | What happens |
| --- | --- | --- |
| **Broker unreachable (producer)** | Retryable | Bounded retries with backoff, then `BrokerUnavailableError`. Ingestion counts it and keeps going (F4); nothing is dropped silently. |
| **Broker unreachable (consumer)** | Retryable | Startup fails loudly; a mid-run failure propagates and the offset is not committed. |
| **Message too large / topic missing** | Permanent | `KafkaPublishError`. Not retried — repeating it only delays the error. |
| **Event no topic carries** | Permanent | Refused and counted. Never routed to a default topic. |
| **Malformed message** | Permanent | Quarantined to `tw.dlq.v1` by the consumer adapter, which holds the bytes. The partition keeps moving (F3). |
| **Unsupported `schema_version`** | Permanent | Quarantined. Not corruption — a rollout that is half done, and the DLQ is where that becomes visible. |
| **Invalid payload** | Permanent | Quarantined. |
| **PostgreSQL down** | Retryable | The processor retries with backoff (`TW_CONSUMER_MAX_RETRIES`). If it runs out, **the offset is not committed**, the worker exits non-zero, and the restart redelivers. |
| **Constraint violation** | Permanent | The batch is replayed one event at a time to find the poison; the survivors are written and the offending event is quarantined. |
| **Consumer restart** | — | Resumes from the last committed offset. In-flight events are redelivered and absorbed. |
| **Consumer offline** | — | Becomes lag, not loss. It catches up when it returns. |
| **Cancellation** | — | The buffer is dropped without committing. Those events were never stored, so redelivery is correct. |

**The one rule that matters more than any of the others:** the offset is committed *after*
the write, never before. Reverse them and a crash in between is a permanent gap.

### Retry budget

`TW_CONSUMER_MAX_RETRIES` becomes the processor's storage-attempt budget, and **nothing else
retries**. One owner, one bounded budget. Nesting a retry loop inside another retry loop
produces a number of attempts nobody can predict from configuration and an outage that lasts
far longer than the operator expects.

---

## Dead letters

`tw.dlq.v1` receives a message when, and only when, processing it can **never** succeed.

**What goes there**, by the two routes it can arrive by:

- *Undecodable bytes* — not JSON, not an object, an envelope that fails validation, an
  unknown `event_type`, an unsupported `schema_version`, a payload the domain model rejects.
  These never become domain events, so the consumer adapter quarantines them.
- *Decoded but unstorable* — a valid event the database refuses permanently. The application
  discovers this and quarantines it through the `DeadLetterSink` port.

**What never goes there:** a retryable failure. A database that is down produces a
`StorageError`, the offset stays uncommitted, and the events wait on their original topic.
Quarantining during an outage would convert a recoverable incident into a manual replay of
everything that arrived while it lasted — the most common way a DLQ makes an outage worse.

**What a record carries.** The original bytes, verbatim, and the original key, so
partitioning is preserved. Plus namespaced headers:

| Header | Meaning |
| --- | --- |
| `tw-dlq-reason` | A short, stable classifier: `not_json`, `invalid_envelope`, `unsupported_schema_version`, `unsupported_event_type`, `invalid_payload`, `unprocessable` |
| `tw-dlq-error` | The human-readable detail |
| `tw-dlq-source-topic` / `-partition` / `-offset` | Where the original record is |
| `tw-dlq-consumer-group` | Which consumer gave up on it |
| `tw-dlq-attempts` | How many times processing was tried |
| `tw-dlq-failed-at` | When |

**What happens afterwards: nothing automatic, deliberately.** There is no redrive tool. Every
reason a message is here is a reason that repeating it unchanged would fail again, so it is a
message a human has to look at. An automated replay is worth building once there is evidence
about what actually lands here — not before.

A failure to *write* to the DLQ is not swallowed. It propagates, the offset is not committed,
and the message is redelivered. Acknowledging a message that was neither stored nor
quarantined would be the one path in this system that loses data silently.

Setting `TW_DLQ_ENABLED=false` replaces quarantining with counting and an error-level log.
That is a choice an operator can make; it is not a default and it is not quiet.

---

## The consumer groups

One group per logical consumer (`ARCHITECTURE.md` §7.4). There are two:

| Group | Reads | Does |
| --- | --- | --- |
| `processor-persist` | trades, quotes, bars | Writes them to PostgreSQL |
| `bar-aggregator` | trades | Builds 1m bars, then rolls up 5m/1h/1d. Also records its own position in `consumer_progress`, keyed by this group name, so an offset rewind replays events it recognises rather than double-counting them |

They have independent offsets and cannot affect each other — which is the fan-out ADR-0001
chose Kafka for, and which Phase 2 had asserted without demonstrating. A live run shows them
at genuinely different positions on the same topic:

```
processor-persist  committed=452839  logEnd=452859  lag=20
bar-aggregator     committed=292700  logEnd=453436  lag=160736
```

See [BACKFILL.md](BACKFILL.md) for the aggregator itself.

Within the group, partitions are distributed across members. Running a second
`tradewatch-consumer` doubles throughput up to the partition count; running a seventh does
nothing, because there are six partitions.

`auto_offset_reset=earliest`, so a brand-new group processes the history on the topic rather
than silently skipping it. **Auto-commit is off and must stay off**: committing on a timer
acknowledges messages that have been *fetched*, not messages that have been *stored*, which
turns at-least-once into at-most-once and loses precisely the data that was in flight when a
consumer died.

---

## The processor

`tradewatch.application.processing.MarketDataProcessor` is Phase 1 code, unchanged except for
one addition. It lives in `application` and depends on the `MarketDataStore` port, not on a
repository — `import-linter` enforces that, which is what lets its batching and failure
handling be unit-tested with no database at all.

**Batching.** Events accumulate per type and flush when the buffer fills or the caller asks.
Each flush is **one transaction**: a batch lands completely or not at all.

**Instrument resolution.** A symbol is resolved to an instrument id once and cached. An
unregistered symbol gets a provisional row on first sight, because a symbol appearing
mid-stream is normal and dropping its data would produce a gap that looks exactly like a
quiet market.

**What Phase 2 added:** `discard_pending()`. Until there was a Kafka consumer, a permanently
unwritable batch was only ever seen by a process about to exit, so keeping the buffer was free
diagnostic information. A consumer cannot exit — it has to abandon the batch, quarantine what
caused it and keep its place. Without this the buffer would still hold the poison rows and
every subsequent flush would fail identically, which is a partition stopped forever on one bad
message. Discarded rows are counted as unprocessable and logged at warning: a consumer
dropping data must never be something you have to read the code to discover.

---

## The API and the frontend

Unchanged by Phase 2. Both read from PostgreSQL and neither knows a broker exists — which is
the point: the system of record is the database (ADR-0002), and Kafka is a transport with
finite retention.

Read-only. There is no client-facing route that writes a tick, a bar or an instrument.

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
  indistinguishable from a typo.
- **503** — the database is unreachable. Retry later. Not 500, which tells a client the
  server is broken and retrying is pointless.

The frontend handles four states, because getting them wrong is what makes a data UI feel
broken: **loading**, **error** (with the API's message, and a retry), **empty** (a known
symbol with no data — not an error), and **success**. Charting is Recharts (ADR-0013), chosen
because SVG output can be asserted against in jsdom.

---

## Observability

Structured events, with the event and instrument identifiers attached. No credentials, and
never a full payload at error level — a quote book logged per tick is how a disk fills up
during a volatile open.

| Event | Where |
| --- | --- |
| `kafka.producer_connected` / `kafka.producer_closed` | Producer lifecycle, with acks and idempotence settings |
| `kafka.published` | One record (debug) |
| `kafka.publish_retry` / `kafka.publish_failed` / `kafka.publish_exhausted` | Producer failures, with the attempt number |
| `kafka.topics_created` | Topic bootstrap, with partitions and replication |
| `kafka.consumer_started` / `kafka.consumer_stopped` | Consumer lifecycle, with group and topics |
| `consumer.batch_committed` | One acknowledged batch (debug) |
| `kafka.malformed_quarantined` / `kafka.dead_lettered` | DLQ placement, with reason and source offset |
| `kafka.poison_poll_committed` | A poll where every record was quarantined |
| `consumer.event_quarantined` | A decoded event the store refused permanently |
| `consumer.storage_failed_uncommitted` | The offset was deliberately not committed |
| `processor.storage_retry` / `processor.storage_failed` | Retry and give-up |
| `processor.pending_discarded` | A batch abandoned after a permanent rejection |

Counters sit wherever an event could otherwise have vanished: `PublisherStats`,
`KafkaConsumerStats`, `ConsumerStats`, `ProcessorStats`, `IngestionStats`. They are split on
purpose — "committed the offset" and "wrote the row" are different claims, and a bug that
makes them disagree is the one worth catching. All become Prometheus metrics in Phase 10.

---

## Testing

| Where | What |
| --- | --- |
| `tests/unit/test_event_envelope.py` | Serialization, determinism, money as strings, every malformed shape, schema versioning, forward compatibility |
| `tests/unit/test_kafka_topics.py` | Topic naming, routing, partition keys |
| `tests/unit/test_kafka_producer.py` | Topic, key, batching, retries, broker failures, serialization failures, shutdown |
| `tests/unit/test_kafka_consumer.py` | Decoding, quarantine, DLQ headers, the poison-poll commit, auto-commit being off |
| `tests/unit/test_kafka_admin.py` | Topic creation, idempotence, partitions, retention |
| `tests/unit/test_market_data_consumer.py` | **Commit ordering**, duplicates, storage failures, poison isolation, cancellation |
| `tests/unit/test_processor.py` | Batching, deduplication, retries, every failure path |
| `tests/integration/test_kafka_pipeline.py` | **The whole path against a real broker and database** |
| `tests/integration/test_processor_failures.py` | Constraint violations, rollback, restart idempotency |
| `tests/integration/test_market_api.py` | Every endpoint and failure mode |
| `tests/integration/test_vertical_slice.py` | The Phase 1 in-process path, still working |
| `frontend/src/components/MarketView.test.tsx` | The four states, and a chart path rendered from API data |

The integration suite is the one that matters. It asserts the exact OHLCV the API returns is
**the data the provider generated**, bar by bar — not a row count, which is how an end-to-end
test passes on data that happened to already be there. It also proves, against a real broker,
that reprocessing changes nothing, that a restarted consumer does not re-read what it
finished, that an offline consumer catches up, and that a database failure leaves the offset
uncommitted so the events come back.

The unit suites exist because a real broker will not fail on demand. The adapters declare
narrow `ProducerClient` and `ConsumerClient` protocols over the handful of methods they call,
so a client that refuses to connect, times out twice then succeeds, or fails to commit is a
few lines of test code rather than an unpluggable network cable.

---

## Known limitations

- **Bars still do not travel over Kafka except as the backfill's 1m series.** The
  aggregator writes bars straight to PostgreSQL rather than publishing them to
  `tw.market.bars.1m.v1`. Phase 4's broadcaster is the first thing that will want them as
  events, and publishing them before anything consumes them would be a topic written to and
  never read.
- **No WebSocket to the browser.** The chart fetches once; it does not update live. Phase 4.
- **The DLQ has no redrive.** Quarantined messages are diagnosable and not replayable without
  manual work. Deliberate — see above.
- **The producer opens a database connection** for the instrument bootstrap, which is
  reference data rather than market data and has nowhere else to go yet. It is best effort:
  if PostgreSQL is unreachable the producer logs and carries on, and the consumer registers
  provisional instruments on first sight.
- **One broker, replication factor 1.** Correct for a laptop and wrong for anything else;
  `acks=all` means less than it sounds like when there is one replica.
- **Frontend API types are hand-written**, mirroring the backend schemas. `ARCHITECTURE.md`
  §8.3 calls for generating them from the committed OpenAPI schema; that needs a codegen step.
- **The chart has fixed pixel dimensions** so it is testable in jsdom. Responsive rendering
  needs a `ResizeObserver` polyfill (ADR-0013).
