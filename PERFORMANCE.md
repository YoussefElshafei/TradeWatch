# Performance

Every number in this file comes from a benchmark in [`benchmarks/`](benchmarks/), and each
links to the dated result file that produced it — with the commit, configuration, hardware
and raw percentiles inside. Nothing here was estimated. If a figure is not in this file, it
has not been measured, and it should not be quoted.

**Read this first.** All five benchmarks ran on one developer laptop (below), with
PostgreSQL, Redis and a single Kafka broker in Docker Desktop on the same machine as the
code under test and the load generator. They describe *that* machine. They are not
production figures, not capacity planning, and not comparable with another machine's
numbers without re-running. Each was run once at the reported commit; run-to-run variance
was not characterised, so treat differences of less than about 1.5x as noise.

| | |
| --- | --- |
| CPU | 13th Gen Intel Core i7-13620H (10 cores, 16 threads), laptop |
| Memory | 31.7 GB |
| OS | Windows 11 (10.0.26200), Docker Desktop 29.8.0 (WSL2) |
| Python | 3.13.15 |
| Date | 2026-10-08 |

---

## Results

### Ingest throughput — 750 events/sec to committed rows

[`results/ingest_throughput/2026-10-08T183909Z.json`](benchmarks/results/ingest_throughput/2026-10-08T183909Z.json) · commit `8247e27`

| Measure | Value |
| --- | --- |
| Sustained, first committed row to last | **750 trade events/sec** |
| Including consumer-group join | 702 events/sec (142.5 s for 100,000 events) |
| Producer publish rate (not the ingest figure) | 4,428 events/sec |

100,000 trade events, 8 symbols, drained from a Kafka backlog by **one** persistence
consumer (`run_consumer`, batch size 100, 6 partitions) into an empty, freshly migrated
database. Counted by a second connection, so only committed rows count.

**Does not prove:** anything about more consumers or partitions; quotes or bars (different
write paths); latency under a live feed.

### End-to-end latency — p50 37 ms, p95 60 ms, p99 77 ms, provider to browser

[`results/e2e_latency/2026-10-08T173821Z.json`](benchmarks/results/e2e_latency/2026-10-08T173821Z.json) · commit `2df4806`

| Interval | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- |
| Provider `occurred_at` → message event in Chrome | **37.0 ms** | **60.1 ms** | **76.8 ms** | 150.7 ms |
| `ingested_at` → message event in Chrome | 36.9 ms | 60.0 ms | 76.8 ms | 150.6 ms |
| Provider `occurred_at` → next animation frame | 45.3 ms | 71.1 ms | 96.2 ms | 864.3 ms |

15,338 trades and quotes over 120 s, on the live stack: producer → Kafka → broadcaster →
Redis Pub/Sub → API `/ws/market` → a WebSocket in headless Chrome. One machine clock for
every timestamp.

**Does not prove:** latency over a real network, through a load balancer or TLS; with more
than one connected browser; at the ingest benchmark's saturation rate (this is the
synthetic feed's live rate, ~110 events/sec); React render time (the "next frame" row is
the earliest paint opportunity, not a measured paint).

### API latency — cached reads 1.7–2.5x faster at concurrency 1

[`results/api_latency/2026-10-08T183548Z.json`](benchmarks/results/api_latency/2026-10-08T183548Z.json) · commit `8247e27`

Two API processes (cache on, cache off) against the same seeded database, each endpoint
measured on one and then the other, back to back. 1,000 requests per endpoint per level.

Concurrency 1 — service time:

| Endpoint | Cache on p50 / p95 / p99 (ms) | Cache off p50 / p95 / p99 (ms) |
| --- | --- | --- |
| `GET /health` (control) | 7.2 / 17.8 / 21.9 | 7.5 / 16.9 / 21.5 |
| `GET /api/v1/instruments` | 8.7 / 33.5 / 39.2 | 15.3 / 63.9 / 75.8 |
| `GET /api/v1/market/{symbol}/bars` (500 bars) | 44.0 / 95.3 / 137.2 | 111.8 / 233.1 / 450.3 |
| `GET /api/v1/market/{symbol}/latest` | 16.6 / 33.8 / 38.9 | 28.8 / 87.0 / 99.9 |
| `GET /api/v1/strategies` (uncached control) | 18.0 / 72.3 / 85.0 | 18.6 / 65.2 / 83.3 |
| `GET /api/v1/backtests` (uncached control) | 17.6 / 65.0 / 82.8 | 24.2 / 76.4 / 90.3 |

Concurrency 16 — saturated:

| Endpoint | Cache on p50 / p99 (ms), req/s | Cache off p50 / p99 (ms), req/s |
| --- | --- | --- |
| `GET /health` | 94.6 / 209.3, 155 | 88.9 / 204.7, 158 |
| `GET /api/v1/market/{symbol}/bars` | 441.3 / 6,353.8, 29 | 1,415.3 / 5,589.4, 9 |
| `GET /api/v1/market/{symbol}/latest` | 97.6 / 1,001.4, 123 | 176.7 / 3,303.6, 46 |

At concurrency 16 throughput barely rises above concurrency 1: one single-threaded process
is saturated (the API worker or the one-process load generator — not separated), so these
latencies are mostly queueing. p99s of several seconds appear at this level.

**Does not prove:** anything about more than one API worker, a remote client, or a large
table; the cached/uncached ratio at concurrency 16 has heavy tails in both directions.

### Backtest throughput — 9,237 bars/sec engine, 2,013 bars/sec end to end

[`results/backtest_throughput/2026-10-08T182122Z.json`](benchmarks/results/backtest_throughput/2026-10-08T182122Z.json) · commit `8247e27`

| Path | Median bars/sec | Each of 5 runs |
| --- | --- | --- |
| Engine (`run_backtest`, in memory) | **9,237** | 7,824 · 10,586 · 10,456 · 9,237 · 9,033 |
| Runner (claim, load from PostgreSQL, run, persist) | **2,013** | 1,989 · 1,996 · 2,013 · 2,730 · 2,775 |

SMA crossover 10/30, 10 symbols × 2,520 daily bars (25,200 bars, 470 trades per run),
slippage 2 bps, $0.005/share commission, next-bar-open fills.

**Does not prove:** anything about RSI reversion or other strategies; intervals other than
daily (the runner reads only `1d` bars).

### Recovery after a 60 s broker outage — lag back to baseline as soon as the broker answers

[`results/recovery_time/2026-10-08T181415Z.json`](benchmarks/results/recovery_time/2026-10-08T181415Z.json) · commit `e5bf04a`

| Measure | Value |
| --- | --- |
| `docker start` → broker answering | 19.3 s |
| Consumer groups back at baseline lag once the broker answered | 5 of 6 immediately; broadcaster after 10.2 s |
| Outage start → every group recovered | 90.0 s (60 s outage + 19.3 s boot + 10.2 s) |
| Peak lag after restart | 103 messages (broadcaster); ≤ 62 for every other group |
| Workers that exited during the outage | none |
| Trades acknowledged by the broker, before / after | 59.2 / 42.5 per second |
| Trades never published (estimate: pre-outage rate × 60 s) | **≈ 3,550** |

**What this actually shows:** there was almost nothing to drain, because the producer does
not buffer while the broker is down — events generated during the outage are dropped, not
delayed. The after-restart rate is *lower* than before, so there was no catch-up burst.
"Fast recovery" here means "fast to resume", not "nothing lost".

**Does not prove:** behaviour under a network partition, a crashed broker with log
recovery, or a multi-broker failover; the drain rate of a large backlog (the ingest
benchmark is the better guide to that).

---

## What the measurements and the new metrics found

Phase 10 was "instrument, measure, then decide". Nothing below was optimised; each is
recorded so the decision can be made with the number in hand.

**Correctness problems the instrumentation exposed** (pre-existing, not performance):

1. **No process publishes live bars.** `tw.market.bars.1m.v1` receives nothing in the running
   stack: the producer publishes trades and quotes only, the aggregator writes bars straight
   to PostgreSQL, and the backfill writes nothing to Kafka. So **live paper trading and live
   anomaly detection have no input** — the `events published/sec` panel shows
   `market.bar.closed` at zero. The "nothing reads that topic yet" reasoning in
   `docs/PIPELINE.md` predates the two consumers Phases 7 and 8 added.
2. **New trades are silently dropped as duplicates after a producer restart.** The synthetic
   provider numbers trades `{symbol}-{sequence}` and restarts the sequence every run, so its
   trade ids — and their deterministic event ids — collide with trades already stored, and
   the idempotent insert discards them. On the development database the newest stored trade
   was 15 hours old while the consumer reported zero lag. The dashboard's watchlist price
   reads that table, which is why it disagreed with the live Market page.
3. **The broadcaster replays its backlog as live data.** It resumes from its committed
   offset, so after downtime browsers receive hours-old ticks presented as live: 2,093,299
   messages (~3 hours) behind before the group was reset to latest for these measurements.
   A live relay probably wants to start from the newest offset.
4. **The producer does not buffer during a broker outage** (recovery benchmark, above).

**Bottlenecks recorded, not optimised:**

5. **The persistence consumer** writes ~750 events/sec, and in the live stack its per-batch
   processing p95 was ~0.8 s for 100 events — the slowest stage in the pipeline.
6. **One API worker** tops out around 150–200 req/s even on `/health`, and the cached bars
   endpoint still takes ~44 ms at concurrency 1: the cache saves the query, not the
   validation and serialisation of 500 bars.

**Operational findings, fixed in this phase:**

7. On Windows, `localhost` resolves to `::1` first and a refused IPv6 connection costs ~2 s,
   so `/ready` reported a running Kafka as `timeout`. The check now tries both families.
8. Under `scripts/dev.ps1`, the API's auto-reload hung forever after the first edit (uvicorn
   stops the old server with a console Ctrl+C that a hidden process never receives). The
   script now runs the API without reload, like every other process.
9. `dev.ps1` truncated a worker's log when restarting it, destroying the record of why it
   crashed; five exits earlier in the session could not be diagnosed for that reason. The
   previous log is now kept.
10. Grafana's port 3000 collided with another local dev server; it is on 3300.

## Reproducing

From `backend/`, with the Compose stack up. `benchmarks/README.md` lists each command and
what must or must not be running. Each run writes a new dated file under
`benchmarks/results/`; compare it with the file linked above, not with the headline.
