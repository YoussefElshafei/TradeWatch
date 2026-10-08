# ADR-0017: Metrics per process, lag from the broker, numbers only from benchmarks

- **Status:** Accepted
- **Date:** 2026-10-08
- **Phase:** 10 — Observability and performance measurement

## Context

Phase 10 has to make nine processes observable and produce the first performance numbers
anyone may quote. `ARCHITECTURE.md` §8.1 lists `GET /metrics` and says it is not public;
§11.3 lists five measurements; §1.3 says no number appears anywhere unless a benchmark in
`benchmarks/` produced it. The import-linter contract "Kafka is an adapter" forbids the API
from importing a broker client, transitively. Three decisions follow.

## Decision

**1. Every process exports Prometheus metrics on its own port**, bound to `TW_METRICS_HOST`
(loopback by default), using the reference `prometheus-client`. The API does not add
`/metrics` to its public router. Ports are fixed per process (`DEFAULT_PORTS` in
`observability/metrics.py`, 9310–9318) and overridable with `TW_METRICS_PORT`. Every metric
is defined in that one module; labels are bounded sets (event type, consumer group, route
*template*, cache kind) — never a symbol, user, or raw path.

**2. Consumer-group lag is measured from the broker, by `kafka-exporter`**, not reported by
the consumers. Per-group processing time is measured once, in the shared
`KafkaEventConsumer`, as the time the caller holds each yielded batch. The error rate is a
counter of log lines at warning and above, fed by a processor in the logging pipeline.

**3. `/ready` reports `database`, `redis` and `kafka` separately**, each with its own
deadline, run concurrently. The Kafka check is a TCP connection to a bootstrap address,
not a metadata request.

## Alternatives considered

- **`/metrics` on the API's main port, behind an auth check.** One less port to know about,
  and it matches §8.1's wording literally. Not chosen: it is one forgotten dependency away
  from being public, and the workers need their own listener anyway, so the API would be the
  one process done differently.
- **Each consumer reporting its own lag** (aiokafka exposes `highwater` and `position`). No
  extra container. Not chosen: a consumer that has crashed or been evicted — failure F10,
  and the whole recovery benchmark — reports nothing at all, so the series vanishes exactly
  when it is needed.
- **A Kafka metadata request in `/ready`.** Proves the broker serves requests, not merely
  accepts connections. Not chosen: it needs `aiokafka` in the API's import graph, which the
  layering contract forbids, and weakening the contract for a health check would be the
  wrong trade. Lag and the exporter's own broker metrics cover request-level health.
- **OpenTelemetry tracing for correlation.** Real spans, sampling, a trace UI. Not chosen
  for this phase: the event envelope already carries `correlation_id`, every derived event
  inherits it, and binding it into the structured-log context makes it searchable across
  every process with no new infrastructure. A tracing backend is a Phase 12 decision.

## Consequences

**Positive** — Metrics are scrapeable even when the API is down; lag stays visible while a
consumer is dead; a hung dependency names itself in `/ready` instead of hanging the probe;
the metric list is one file a reviewer can read.

**Negative** — Nine ports to keep free on a developer machine. A broker that accepts TCP but
cannot serve reads as `ok` in `/ready` (marked `ponytail:` in `broker_health.py`). The
benchmarks run on one laptop with everything co-located, so their numbers describe that
machine and nothing more — `PERFORMANCE.md` says so next to every figure.

**Neutral** — Prometheus scrapes every 5 s rather than 15 s, so one-minute windows have
enough points; retention is seven days locally.

## Revisit when

The API runs more than one worker process (each would need a distinct metrics port, or the
multiprocess collector); or `/ready` starts gating traffic on Kafka; or Phase 12 introduces
a tracing backend.
