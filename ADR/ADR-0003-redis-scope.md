# ADR-0003: Redis — a bounded, justified scope

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 4

## Context

"Add Redis for caching" is the least examined sentence in system design. Added without a
specific job, a cache becomes a source of stale data, an extra failure mode, and a place
where correctness bugs hide — in exchange for a speedup nobody measured.

TradeWatch does have workloads that genuinely suit Redis, and it also has workloads that
superficially look like they do. This ADR separates them, so that Redis's presence is
defensible line by line.

## Decision

Redis is used for exactly these jobs, and adding a new one requires amending this ADR:

| Use | Key | Why Redis |
| --- | --- | --- |
| Latest quote/trade per symbol | `tw:quote:{symbol}` | A single mutable hot value, read on every dashboard render and WS connect. A point read of the newest row from a large append-only table is comparatively expensive, and it is the same read for every user. |
| Recent bar window for charts | `tw:bars:{symbol}:{interval}` | The most repeated query in the application, identical across all users, and naturally short-lived. |
| Rate limiting | `tw:rl:{scope}:{id}` | Atomic increment-with-expiry at request frequency. Implementing this in PostgreSQL means a write per request to serve zero business value. |
| WebSocket fan-out between API replicas | Pub/Sub `tw:ws:{channel}` | Required the moment the API runs more than one process; each replica holds only some of the connections. |
| Refresh-token emergency denylist | `tw:revoked:{jti}` | Entries are inherently TTL-bounded; a table would need a sweeper. |
| Backtest job status for polling | `tw:backtest:{id}:status` | A fast-changing value polled frequently while a job runs. |

Redis is explicitly **not**:

- the system of record (ADR-0002);
- a job queue — Kafka is the event transport, and the backtest queue is a `backtest_runs`
  table, because a queued job is durable state that survives a Redis restart;
- a session store — access tokens are stateless JWTs (ADR-0010);
- a cache for anything already cheap to query.

Three rules make this enforceable:

1. **Every key has a TTL and a documented invalidation trigger.** No key is written without
   both.
2. **`TW_CACHE_ENABLED=false` must leave the application fully correct**, only slower. This
   is a test, run in CI — it proves the cache is an optimization and not load-bearing.
3. **Redis being down degrades, never fails** (F9): cache misses fall through to PostgreSQL;
   rate limiting fails closed on authenticated writes and open on reads.

A cache that cannot be shown to improve a measured p95 in Phase 10 is removed.

## Alternatives considered

**No cache at all.** One fewer service, no staleness, no invalidation bugs. Genuinely
attractive, and correct for a system with few users. Rejected because of the two uses that
are not caching at all: cross-replica WebSocket fan-out has no PostgreSQL equivalent, and
per-request rate-limit counters in the database are an abuse of the database. Those two
justify Redis on their own; the caching uses then come along at no additional operational
cost.

**In-process memory cache (`lru_cache`, a dict with TTLs).** Faster than Redis — no network
hop — and needs no infrastructure. Rejected as the primary mechanism: it is per-replica, so
cache hit rates fall as the API scales, invalidation cannot be coordinated across replicas,
and it does nothing for fan-out or rate limiting. It may still be used *in front of* Redis
for genuinely immutable data such as the instrument list.

**Memcached.** Simpler and slightly faster for pure key/value caching. Rejected: no Pub/Sub,
no data structures, no persistence option — and Pub/Sub is one of the two load-bearing uses.

**PostgreSQL `UNLOGGED` tables as a cache.** No new service. Rejected: still a network round
trip to the same database whose load the cache exists to reduce, and no TTL support.

## Consequences

**Positive.** Hot reads are cheap and uniform across users. The API can scale horizontally
without breaking live streaming. Rate limiting is correct under concurrency. Every Redis key
has a named owner and a written reason.

**Negative.** Another service to run and monitor. Staleness windows exist and must be
understood — bounded by TTL, and the values cached are ones where a few seconds of staleness
is acceptable. Cache invalidation bugs remain possible and are guarded by the
`TW_CACHE_ENABLED=false` test.

**Neutral.** The cache layer sits behind a port, so an in-memory implementation is used in
unit tests.

## Revisit when

Phase 10 benchmarks show a cached path is not measurably faster than the uncached one — then
that key is deleted. Or the API is definitively single-replica and rate limiting moves
elsewhere, at which point Redis's justification should be re-examined from scratch.
