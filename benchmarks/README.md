# Benchmarks

**Nothing is claimed about TradeWatch's performance unless it was produced here.**

That rule is in `ARCHITECTURE.md` §1.3 and it is the reason this directory exists in
Phase 0 with no benchmarks in it: an empty directory with a rule is more honest than a
README with a number nobody measured.

## What gets measured (Phase 10)

| Benchmark | Metric |
| --- | --- |
| Ingest throughput | Events/sec sustained from provider to a committed database write |
| End-to-end latency | Provider timestamp → browser render, p50/p95/p99 |
| API latency | p50/p95/p99 per endpoint at a stated concurrency, cached vs uncached |
| Backtest throughput | Bars/sec for a stated strategy and window |
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
