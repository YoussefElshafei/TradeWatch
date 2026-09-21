# History and bars

How TradeWatch gets enough stored history to compute a number from, and how live trades
become OHLCV bars. Phase 3.

Two jobs, with different shapes and different failure modes:

| | `tradewatch-backfill` | `tradewatch-aggregator` |
| --- | --- | --- |
| Shape | One-shot | Long-running service |
| Source | The provider's historical endpoint | `tw.market.trades.v1` |
| Consumer group | — | `bar-aggregator` |
| Writes | 1m bars (final), then rollups | 1m bars (open and final), then rollups |
| Reruns | Idempotent and resumable | Redelivery absorbed by the revision guard |

---

## The second consumer group

The aggregator reads the same topic as `processor-persist`, in its own group, with its own
offsets. Neither can affect the other: kill the aggregator and trades keep being persisted;
kill the persistence consumer and bars keep being built.

**This is the point of Phase 3, more than the bars are.** ADR-0001 chose Kafka on the
argument that several independent consumers need the same events with independent failure
domains and independent offsets — and Phase 2 then shipped exactly one consumer, which is the
condition ADR-0001's own *revisit when* clause names as the point at which the broker stops
being justified. A live run shows the two groups at genuinely different positions:

```
processor-persist  committed=452839  logEnd=452859  lag=20
bar-aggregator     committed=292700  logEnd=453436  lag=160736
```

The aggregator is replaying the topic from the beginning while the persistence consumer
stays current. Neither notices the other.

---

## The aggregator

```
tw.market.trades.v1 → BarAggregator → PostgreSQL → rollups to 5m/1h/1d
```

The decisions below are recorded in
[ADR-0015](../ADR/ADR-0015-event-time-bar-aggregation.md), including what was rejected.

### Event time, and two watermarks

A bar closes because a trade arrived whose **venue** timestamp is past the boundary, not
because a clock in this process ticked (failure F12). There are two watermarks and they do
different jobs:

| Watermark | What it is | What it decides |
| --- | --- | --- |
| Per symbol | Highest `occurred_at` for that symbol | When that symbol's bars are **sealed** |
| Global | Highest across all symbols | When a bar is **closed and written**, and when it may be evicted |

Closing on the global watermark is what stops a symbol that trades once an hour from holding
a bar open until it trades again.

**Sealing on the global watermark destroys data**, and this is not theoretical: the first
live replay dropped **153,128 trades** before the split was made. Kafka orders a partition,
not a topic, so a consumer catching up on a backlog reads partitions at different rates and
one symbol's trades can be minutes ahead of another's. A single global watermark then sealed
the lagging symbol's bars and discarded everything it read, and the only outward sign was a
counter going up while the bars themselves looked reasonable.

The asymmetry is the whole rule: **closing early is harmless, sealing early is not.** A bar
closed too early is reopened by the next trade as a correction. A bar sealed too early has
already thrown the trade away.

### Corrections, and the bounded window

A bar is written the moment its minute ends — `is_final = true`, `revision = 0` — rather
than being held for the length of the lateness allowance. A downstream consumer waiting for a
bar should not also wait out a grace period that exists for a rare case.

A trade arriving afterwards, while that symbol's watermark is still inside
`TW_LATE_DATA_GRACE_SECONDS`, reopens the bar, increments `revision`, and rewrites it through
the revision-guarded upsert. Once the window passes, the bar is sealed and a straggler is
counted in `late_dropped` and discarded — because a bar that can be revised forever is a bar
nothing downstream can ever act on (failure F11).

One honest limitation: a bar reopened *from the database* cannot place its late trade
relative to the trades it no longer holds, so the correction adjusts high, low, volume, VWAP
and trade count and leaves `open` and `close` alone. Guessing would mean a close rewritten
with a price from the middle of the minute, and everything downstream is priced off the
close.

### Restart recovery

The in-progress bar is written on every flush with `is_final = false`, and offsets are
committed only afterwards. On startup the aggregator reads back every non-final bar and
resumes accumulating into it (failure F13).

A restart mid-minute therefore produces the bar that would have existed without one. Both
suites assert this by comparison rather than by inspection: build a bar straight through,
build the same bar with a restart in the middle, and require the two to be identical.

Resuming an unfinished bar is **not** a correction and does not bump `revision`. Announcing a
correction that never happened would mislead every consumer watching `revision` to detect
rewritten history.

### The one that would have been silent

A trade for a bar the process has no memory of — after a restart, or after eviction — reads
the bar from the store first and corrects it. Without that step the aggregator would build a
fresh bar containing that single trade, and the revision-guarded upsert would **accept** it:
a new bar's revision is 0 and so is the stored one's, and `revision <= excluded.revision`
holds. A correct bar would be replaced by a one-trade bar, nothing would report it, and the
only evidence would be a volume figure that is wrong.

### VWAP

Exact for 1-minute bars: the aggregator sees the trades, so it computes
`sum(price * size) / sum(size)` in `Decimal`. **Null for every rolled-up interval**, because
averaging five VWAPs is not a VWAP and writing a number that looks right would be worse than
writing nothing.

---

## Rollups

5m, 1h and 1d bars are derived from stored 1m bars, in SQL, never from a second pass over
trades (`ARCHITECTURE.md` §3.3). Two independent paths to the same number is two chances to
disagree, and the disagreement would surface as a backtest nobody can reproduce.

The aggregator runs a pass every `TW_ROLLUP_INTERVAL_SECONDS` over a trailing
`TW_ROLLUP_LOOKBACK_MINUTES`, and the backfill runs one when it finishes. Both are
idempotent: a pass recomputes its window and upserts, so a missed pass costs nothing and a
repeated one writes the same rows.

The interesting part of the query is that `open` and `close` are **not** `min` and `max`. The
open of a 5-minute bar is the open of its earliest minute; `min(open)` would look reasonable
and be wrong, and the bug would survive into a backtest result.

Bucket boundaries are anchored at the Unix epoch in both the aggregator and the SQL, so the
two agree about where a five-minute bar starts. Two anchors would produce two different bars
for the same data and only one of them would be wrong.

---

## The backfill

```bash
uv run tradewatch-backfill --days 30
uv run tradewatch-backfill --symbols AAPL,MSFT --start 2026-08-01 --end 2026-09-01
uv run tradewatch-backfill --days 30 --no-resume     # re-fetch, to repair corrected history
```

It fetches bars for a date range, stores them, rolls the coarser intervals up, and reports on
what it produced. Exit code `3` means the data is stored but the quality checks found
something.

**It writes nothing to Kafka.** Backfilled bars go straight to the database. They are not
market *events* — nobody is waiting to react to a bar from three weeks ago — and publishing
them would drive the aggregator's watermark backwards through history and seal every live bar
behind it. History is loaded; events are streamed.

### Idempotent

Bars are written through the revision-guarded upsert, keyed on
`(instrument_id, interval, bar_start)`. Verified live rather than assumed: re-fetching nine
days of history rewrote 51,840 rows and left the 1-minute series **byte-identical**, compared
by checksum over every price and volume.

That is what makes "just run it again over a generous window" the correct recovery from a
provider outage, rather than a way to double-count volume.

### Resumable, and the rule that is not obvious

The resume point comes from the data, not from a job table. A job table would be a second
statement about what has been fetched, and the interesting question would immediately become
which of the two is right when they disagree — usually after a crash, which is exactly when
it matters.

The rule is **not** "resume after the newest stored bar". That looks equivalent and is wrong,
and it was wrong in production on the first real run: the database already held a few hours
of recent bars, so a request for the previous month resumed from *today*, fetched 152
minutes, and reported success over an empty month.

So: **resume only when the stored data looks like a prefix somebody was filling forward.** If
the oldest bar inside the range is at the beginning of it, the newest bar is a genuine
high-water mark and fetching continues from there. If the oldest bar sits somewhere in the
middle, the data is a patch rather than a prefix and the whole range is fetched. Being wrong
in the cautious direction costs a re-fetch, which is idempotent; being wrong in the other
direction costs a month of history and says nothing.

### Chunked

The range is walked in windows — a day at a time for 1-minute bars — each fetched and written
before the next is requested. A crash costs the current window rather than the job, and
memory is bounded by a window rather than by the range. Because a window is written in one
call, a crash leaves it empty rather than half-filled, which is what makes the resume rule
sound.

---

## Data quality

Three checks, run by the backfill and available on their own:

| Check | What it finds |
| --- | --- |
| Bar invariants | A stored bar whose OHLC relationships do not hold |
| Gap detection | Missing minutes, split into "during a session" and "not" |
| Duplicate prints | The same trade counted twice |

**Invariants are verified, not enforced, here.** The database already refuses an incoherent
bar — `high >= low`, open and close inside the range, positive prices — as `CHECK`
constraints, and `NewBar` validates the same rules before a write is attempted. This check
exists to confirm those two layers are doing their job, not to be a third. A non-empty result
means something wrote around the constraints, which should be impossible and is worth knowing
immediately if it stops being.

**Gaps are reported, not judged.** A hole in a minute series is only a fault if the market
was open, so the session calendar is applied before anything is called missing. A checker that
reports every weekend as an outage is a checker people switch off.

The calendar is deliberately simple — weekdays, one continuous session, no holidays. It
filters a *report*, and an unlisted holiday shows up as a gap an operator recognises, which is
a better failure than a half-correct holiday table silently hiding a real outage on a day it
got wrong. A maintained calendar arrives when something needs to make a decision from it
rather than a report.

---

## The provider

`TW_PROVIDER=synthetic` remains the default and nothing about its behaviour changed in this
phase. `TW_PROVIDER=alpaca` selects the real adapter (ADR-0011) and needs
`TW_PROVIDER_API_KEY` and `TW_PROVIDER_API_SECRET`.

Rate limiting, reconnection with backoff and staleness detection are not new — they are the
Phase 1 `resilience` module, and the adapter and the ingestion worker were already wired to
them. What Phase 3 added is the validation described in the Phase 3 report: three defects
found by reading the adapter against its own documented constraints, the most serious being
that it opened a WebSocket per stream while Alpaca's free tier permits one connection and
silently displaces the older one.

**The adapter is still unverified against the live API**, because no credentials exist in
this environment. `pytest -m provider` is the suite that closes that gap and it needs a key
pair. Until someone runs it, treat the normalizers as tested against documented shapes rather
than against the service.

---

## Testing

| Where | What |
| --- | --- |
| `unit/test_bar_aggregator.py` | Hand-calculated OHLCV and VWAP, event-time closing, corrections, sealing, restart, the replay-ordering bug |
| `unit/test_bar_aggregation_consumer.py` | Write-then-commit ordering, storage failures, rejected bars |
| `unit/test_backfill.py` | Chunking, idempotency, resuming, the "recent bars skip the month" bug |
| `unit/test_quality_checks.py` | Sessions across timezones and weekends, gap classification |
| `unit/test_alpaca_provider.py` | Normalization, HTTP failures, and `TestMultiplexing` — one socket, two streams |
| `integration/test_bar_aggregator.py` | The whole path against a real broker and database |

The integration suite is the one that settles things a fake cannot: that two consumer groups
really do get independent copies, that a bar survives a real restart, and that PostgreSQL
accepts what the aggregator writes. The `CHECK` constraints are PostgreSQL's, and the only
way to know a bar satisfies them is to hand it to PostgreSQL.

---

## Known limitations

- **A correction cannot move `open` or `close` on a bar reloaded from the database.** See
  above; it adjusts the aggregate figures instead.
- **One aggregator instance.** Per-symbol state lives in its memory, so a second instance
  would need that state to survive a partition reassignment. That is the point at which the
  stream-framework question (decision D5) genuinely opens.
- **The session calendar has no holidays.** Deliberate, and stated where it matters.
- **Duplicate-print detection is reported but not yet computed** — the field exists on the
  report and is always zero, because deterministic event ids (ADR-0012) mean a duplicate
  print never reaches the database in the first place. It stays in the report as the place to
  put a real check if a provider is ever seen to send genuine duplicates under different ids.
- **The Alpaca adapter has never run against the live service.**
