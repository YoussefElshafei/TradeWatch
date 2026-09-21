# ADR-0005: JSON + Pydantic event serialization, versioned by topic

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 2

## Context

Events on Kafka are a contract between processes. Once a producer and several consumers
depend on a shape, changing it badly breaks things at runtime rather than at build time.
The standard industrial answer is a binary format with a schema registry — Avro or Protobuf
— which gives compact messages and enforced compatibility checks.

TradeWatch's situation: one producer language (Python), one team, nine topics, and Pydantic
models that already exist as the domain's validation layer. The volume is moderate — human
dashboard scale, not exchange scale.

## Decision

Events are serialized as **JSON**, with **Pydantic models as the single schema definition**,
and an explicit `schema_version` integer in the envelope.

- The Pydantic model is the schema. It is used for both producing and consuming, and a
  consumer validates on receipt; failures go to the DLQ rather than crashing the consumer.
- Every event carries the envelope in `ARCHITECTURE.md` §7.1 — `event_id`, `event_type`,
  `schema_version`, `occurred_at`, `ingested_at`, `producer`, `correlation_id`, `payload`.
- **Compatible changes** (adding an optional field with a default) keep the topic and bump
  nothing; consumers ignore unknown fields.
- **Breaking changes** (removing or retyping a field, changing semantics) produce a **new
  topic** with an incremented major version (`tw.market.trades.v2`) and a dual-write window
  while consumers migrate. A topic's meaning never silently changes.
- JSON Schema is generated from the Pydantic models and committed, so a contract change is
  visible in a diff during review.
- Money and quantity fields are serialized as **strings**, never JSON numbers
  (ADR-0009) — a float round-trip through JSON is exactly how a cent goes missing.

Avro/Protobuf and a Schema Registry are deferred (D3).

## Alternatives considered

**Avro + Confluent Schema Registry.** The industry default for Kafka: compact binary
messages, registry-enforced compatibility checking at publish time, and code generation for
consumers. Genuinely better at scale and across languages. Rejected for now: it adds a
registry service to run and to deploy, a build step, and a second source of schema truth
alongside Pydantic — for compatibility guarantees that matter most when multiple teams and
languages produce to the same topic, which is not this system. The size advantage is real
but irrelevant at this throughput.

**Protobuf.** Similar benefits, strong cross-language story, no registry strictly required.
Rejected for the same reason: `.proto` files become a second schema definition parallel to
the Pydantic models, and the duplication is a live source of drift.

**MessagePack or orjson-encoded dicts with no schema.** Fastest to write. Rejected: no
validation at the boundary means malformed events are discovered deep inside a consumer,
which is precisely the failure mode F3 exists to prevent.

**Plain JSON with no version field.** Even simpler. Rejected: the first breaking change
then has no safe migration path, and retention means old-format messages are still on the
topic during any rollout.

## Consequences

**Positive.** One schema definition (the Pydantic model) shared by the API, the workers, and
the tests. Messages are human-readable, which makes debugging with `kafka-console-consumer`
trivial — a bigger advantage during development than is usually admitted. No extra
infrastructure. Validation happens at every boundary for free.

**Negative.** Messages are several times larger than Avro, and serialization is slower;
neither matters at this volume, but both would at a much higher one. Compatibility is
enforced by review discipline and tests rather than by a registry that refuses an
incompatible publish — a weaker guarantee, and an honest one to state. A second producer
language would immediately make this the wrong choice.

**Neutral.** Topic-level versioning means more topics over time; retention keeps that
bounded.

## Revisit when

- A second producer language appears (D3), or another team consumes these topics.
- Message volume grows to where serialization cost or network bandwidth shows up in a
  Phase 10 benchmark.
- A breaking schema change causes an incident despite review — evidence that discipline is
  not a sufficient substitute for a registry.
