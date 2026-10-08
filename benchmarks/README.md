# Benchmarks

**Nothing is claimed about TradeWatch's performance unless it was produced here.**

That rule is in `ARCHITECTURE.md` §1.3. [`PERFORMANCE.md`](../PERFORMANCE.md) summarises
the results; every figure there links to the result file it came from.

## Running them

From `backend/`, so the project's own environment is the one measured. Each run writes
`results/<benchmark>/<UTC timestamp>.json`; commit the file alongside any number you quote.

| Benchmark | Command | Needs |
| --- | --- | --- |
| Ingest throughput | `uv run python ../benchmarks/ingest_throughput.py` | Compose PostgreSQL + Kafka; TradeWatch processes **stopped** |
| API latency | `uv run python ../benchmarks/api_latency.py` | Compose PostgreSQL + Redis; port 8099 free; processes stopped |
| Backtest throughput | `uv run python ../benchmarks/backtest_throughput.py` | Compose PostgreSQL; processes stopped |
| End-to-end latency | `uv run python ../benchmarks/e2e_latency.py` | The whole stack running (`scripts/dev.ps1`), Chrome installed, the broadcaster caught up |
| Recovery time | `uv run python ../benchmarks/recovery_time.py` | The whole stack running under `scripts/dev.ps1` (it restarts evicted workers) |

"Processes stopped" matters: the three isolated benchmarks build their own empty database
and topics, and a running stack draining a backlog on the same machine would take CPU and
disk from the thing being measured. The last two measure the running stack itself.

## What gets measured

| Benchmark | Metric |
| --- | --- |
| Ingest throughput | Events/sec sustained from a Kafka backlog to committed PostgreSQL rows |
| End-to-end latency | Provider timestamp → WebSocket message in a real browser, p50/p95/p99 |
| API latency | p50/p95/p99 per endpoint at concurrency 1 and 16, cached vs uncached |
| Backtest throughput | Bars/sec for SMA crossover 10/30, engine alone and full runner |
| Recovery time | Consumer-lag drain after a 60-second broker outage |

## Rules for a result

Each run writes a dated JSON file recording:

* the commit it ran against;
* the full configuration, including every setting that affects the result;
* the hardware and operating system;
* the workload — symbols, event rate, duration, concurrency;
* the raw percentiles, not a summary.

A number without its conditions is not a measurement. Before quoting anything from here,
state what it was measured on — and state what it does not prove.
