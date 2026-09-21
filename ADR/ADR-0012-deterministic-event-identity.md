# ADR-0012: Deterministic event identity for market data

- **Status:** Accepted
- **Date:** 2026-09-19
- **Phase:** 1
- **Amends:** `ARCHITECTURE.md` §7.1, which specified UUIDv7 for `event_id`

## Context

`ARCHITECTURE.md` §7.1 says `event_id` is "a UUIDv7 generated at the boundary" and is "the
idempotency key, end to end". Building the ingestion subsystem showed those two statements
are in conflict.

A UUIDv7 is generated fresh each time. So when the same trade is normalized twice, it gets
two different ids — and the duplicate is invisible to `UNIQUE (trades.event_id)`, which was
supposed to be the thing absorbing it.

That is not a hypothetical. Normalizing the same trade twice is the *normal* case:

- **A reconnect** (failure F1). A provider resends recent prints after a dropped socket;
  that is a feature, not a bug, and it is how a gap gets filled.
- **A backfill overlapping live data.** The recovery for an outage is "fetch a generous
  window around the gap", which deliberately re-fetches bars already stored.
- **A restart.** In-process deduplication state is gone; the database is all that is left.
- **A Kafka replay** (Phase 2). Reprocessing a topic re-normalizes every message in it.

With random ids, every one of those writes duplicate rows: a double-counted tape, inflated
volume, and a backtest confidently computed on both.

The in-flight deduplicator does not solve this. It is bounded and it forgets — deliberately,
because an unbounded one is a memory leak — so it cannot cover a restart or a replay hours
later.

## Decision

**`event_id` for market data is a UUIDv5 over a natural key derived from the provider's own
data**, not a UUIDv7. `deterministic_event_id()` in `tradewatch.domain.events` builds it
from a fixed namespace UUID and the components below.

| Event | Natural key |
| --- | --- |
| Trade, with a provider trade id | `source \| "trade" \| symbol \| provider_trade_id` |
| Trade, without one | `source \| "trade" \| symbol \| occurred_at \| price \| size` |
| Quote | `source \| "quote" \| symbol \| occurred_at \| bid \| bid_size \| ask \| ask_size` |
| Bar | `source \| "bar" \| symbol \| interval \| bar_start \| revision` |

Four details that each matter:

1. **`source` is in every key.** The same trade seen through two providers is two
   observations, not one, and collapsing them would make a future provider migration
   silently lose data.
2. **`revision` is in the bar key.** A correction is a *different* event about the same
   bar; deduplicating it away would mean the correction never arrived (failure F11).
3. **Quote sizes are in the key.** The same prices with different sizes is a new quote, and
   treating it as a duplicate would hide book activity.
4. **`None` components render as empty rather than being skipped**, so `(a, None, b)` and
   `(a, b)` cannot collide.

The namespace UUID is fixed forever. Changing it would orphan every stored `event_id` and
deduplication would silently stop working.

`ARCHITECTURE.md` §7.1's UUIDv7 rule still holds for **entity primary keys** — users,
orders, strategies — where the value is generated once and time-ordering buys index
locality (see `tradewatch.domain.ids`). This ADR narrows it for market data events only.

## Alternatives considered

**Keep UUIDv7 and add `UNIQUE (provider, provider_trade_id)`.** Would work for trades and
is arguably clearer. Rejected because most quotes carry no provider id at all, so quotes
would need a different mechanism, and bars would need a third — three deduplication stories
instead of one. It also costs a second unique index on the largest tables in the system.

**Keep UUIDv7 and rely on the in-flight deduplicator alone.** Simplest change: none.
Rejected because the deduplicator is bounded and forgetful by design, so it cannot survive a
restart or a Kafka replay — exactly the cases that matter most.

**Hash the entire normalized payload.** No key design to get wrong. Rejected: any
insignificant field the provider adds or reorders changes the hash, so the same trade
becomes a new event on a provider's minor release. Naming the identifying fields explicitly
means the key changes only when the *meaning* changes.

**Let the database deduplicate on a natural composite key instead of an id.** Honest, and it
is what `bars` already does with its composite primary key. Rejected for trades and quotes
because the key would be five or six columns wide on the highest-volume tables, and the
event id has to exist and be stable anyway for Kafka and for tracing.

## Consequences

**Positive.** `UNIQUE (event_id)` now absorbs duplicates that originate at the *provider*,
not just ones Kafka introduced — which is what §7.1 claimed all along. A reconnect, an
overlapping backfill, a restart and a topic replay all become no-ops instead of corruption.
Identity is reproducible across processes and machines, so a fixture recorded today
deduplicates against live data tomorrow.

**Negative.** UUIDv5 values are effectively random, so they scatter across the
`uq_trades_event_id` index rather than appending to its right edge the way UUIDv7 would.
That costs index locality on the largest tables — accepted because the primary key on those
tables is a `BIGINT` identity, which keeps the *table* append-ordered, and because
correctness beats a write-locality optimisation nobody has measured. If a Phase 10 benchmark
shows it matters, the answer is partitioning (D4), not random ids.

Two genuinely distinct trades, of the same size at the same price in the same microsecond,
from a provider that supplies no trade id, collapse into one event. Accepted: nothing in the
feed distinguishes them either.

The key components are now part of the storage contract. Changing one is a migration, not a
refactor, and would orphan existing ids.

**Neutral.** `ARCHITECTURE.md` §7.1 needs a note pointing here.

## Revisit when

- A provider supplies a stable id for quotes, which would let the quote key shrink to it.
- A Phase 10 benchmark shows index write locality on `trades` is a real bottleneck.
- A second provider is added and the "same trade from two sources" question needs a
  different answer than "two observations".
