# ADR-0001: Kafka as the event backbone

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** introduced in Phase 2, not Phase 1

## Context

Market events produced by ingestion are needed by several independent consumers: the
persistence path, bar aggregation, strategy evaluation, anomaly scoring, and live browser
streaming. These consumers have different processing speeds, different failure modes, and
different restart behaviour. One slow or crashing consumer must not affect the others, and a
consumer that was down must be able to catch up rather than lose the data it missed.

Development also needs replay: re-running a day of market data through a changed aggregator
or a changed detector, without re-fetching it from the provider — which may be impossible,
since free-tier providers generally do not let you rewind a live feed.

At the same time, TradeWatch's principle is that infrastructure is added when a requirement
forces it. A message broker added on day one would be decoration.

## Decision

Apache Kafka is the event backbone between ingestion and every downstream consumer, with
topics, keys, and retention as specified in `ARCHITECTURE.md` §7.

It is introduced in **Phase 2**, not Phase 1. Phase 1 uses an in-process implementation of
the `EventPublisher` / `EventConsumer` ports. Kafka arrives as an adapter behind those same
ports, at the point where the in-process bus provably cannot meet the requirement: process
isolation, independent failure domains, restart-safe offsets, and replay.

Specifics:

- Delivery is **at-least-once**; every consumer is idempotent, and offsets are committed
  after the side effect.
- Market topics are keyed by `symbol` to guarantee per-symbol ordering, which the bar
  aggregator and every strategy depend on. `tw.signals.v1` is keyed by `deployment_id` for
  the same reason at the deployment level.
- Kafka is a **transport with finite retention**, never the system of record (ADR-0002).
- Local development runs a single broker in KRaft mode — no ZooKeeper.

## Alternatives considered

**Redis Streams.** Redis is already in the stack, so this would add no new infrastructure;
consumer groups exist; it is much lighter to run. Rejected because durability is tied to
Redis persistence configuration, retention and replay semantics are weaker, and partitioned
ordering with independent consumer-group offsets is not the model Redis Streams is strongest
at. Given that a durable, replayable log is precisely the requirement, choosing the tool
whose entire design centre is that requirement is the more honest engineering choice.

**PostgreSQL as a queue (`SELECT ... FOR UPDATE SKIP LOCKED`).** No new infrastructure, real
transactions, and the operational story is one database instead of two. Genuinely the right
answer for many systems. Rejected here because a continuous multi-consumer fan-out stream is
exactly the workload that turns a database queue into a bottleneck, and because it provides
no replay-from-offset without building a second system on top.

**RabbitMQ.** Excellent routing, mature, easy to operate. Rejected: it is a message queue,
not a log. Messages are consumed and gone; replay and independent consumer offsets are not
its model, and replay is a first-class requirement here.

**Direct in-process calls, permanently.** Simplest possible thing. Rejected: it makes every
consumer share a failure domain and a process lifetime, and makes horizontal scaling and
replay impossible. It is, however, right for Phase 1 — which is why it is used there.

## Consequences

**Positive.** Consumers are independently deployable, restartable, and scalable. A consumer
outage becomes lag rather than data loss. Replay enables real iteration on aggregation and
detection logic. The seam also happens to make every consumer unit-testable against an
in-memory bus.

**Negative.** Real operational weight: a broker to run, topics to manage, lag to monitor,
offsets to reason about, and a genuinely harder local setup. At-least-once delivery forces
idempotency into every consumer, which is real work. Kafka's memory footprint is
uncomfortable for a small cloud deployment, which is why D8 (managed vs self-hosted vs
alternative, in the cloud) is deliberately left open.

**Neutral.** Event schemas become a contract that needs versioning (ADR-0005).

## Revisit when

- The cloud deployment cost of running Kafka exceeds the budget (see D8): the port
  abstraction means the adapter can be swapped for Redpanda or a managed alternative.
- The number of independent consumers drops to one and replay stops being used — at which
  point the justification is gone and so should Kafka be.
