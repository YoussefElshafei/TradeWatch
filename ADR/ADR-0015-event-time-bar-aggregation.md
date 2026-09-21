# ADR-0015: Event-time bar aggregation, with per-symbol sealing

- **Status:** Accepted
- **Date:** 2026-09-21
- **Phase:** 3

## Context

`ARCHITECTURE.md` §3.3 says the Bar Aggregator "maintains per-symbol open bars in memory and
closes a bar when the interval boundary passes — driven by event time, with a wall-clock
grace period for late data", and failure F11 says late data inside a bounded window produces
a correction. That is the shape of the answer. Building it raised four questions it does not
settle, and three of them have an answer that looks obviously right and loses data.

1. **What advances event time?** A bar closes when "the boundary passes", but the boundary
   passes according to *whose* clock — the symbol's own trades, or every symbol's?
2. **When is a bar written: at its boundary, or after the lateness window?** Writing at the
   boundary means corrections; waiting means no corrections but no bars either, until the
   window expires.
3. **How does a restart mid-bar produce a correct bar?** The trades folded into an open bar
   have been consumed. If their offsets are committed, they will not come back.
4. **What happens to a trade for a bar this process has never seen?** After a restart, or
   after a bar has been evicted from memory.

## Decision

### Event time advances per symbol *and* globally, and the two are used for different things

- A **per-symbol watermark** — the highest `occurred_at` seen for that symbol — decides when
  that symbol's bars are **sealed**, after which a trade for them is counted and dropped.
- A **global watermark** — the highest seen across all symbols — decides when a bar is
  **closed and written**, and when a written bar may be evicted from memory.

Closing on the global watermark is what stops a symbol that trades once an hour from holding
a bar open until it trades again. Sealing on it is destructive, and this is not theoretical:
the first live replay dropped **153,128 trades**. Kafka orders a partition, not a topic, so
a consumer catching up on a backlog reads partitions at different rates and one symbol's
trades can be minutes ahead of another's. A single global watermark sealed the lagging
symbol's bars and discarded everything it then read, and the only outward sign was a counter
going up while the bars themselves looked perfectly reasonable.

The asymmetry is the point: **closing early is harmless and sealing early is not.** A bar
closed too early is reopened by the next trade as a correction. A bar sealed too early has
already thrown the trade away.

### A bar is written at its boundary, and corrected afterwards

A bar closes and is written as soon as the global watermark passes its end, carrying
`is_final = true` and `revision = 0`. A trade that arrives for it afterwards, while that
symbol's watermark is still inside the lateness window, reopens it, increments `revision`,
and rewrites it through the revision-guarded upsert.

The alternative — hold the bar until `bar_end + lateness` and write it once — produces no
corrections at all, which is tidier. It was rejected because every downstream consumer would
then wait out the lateness allowance before seeing any bar, and the allowance exists for a
rare case. Corrections are a solved problem here: `revision` is in the schema, the upsert is
already guarded on it, and a consumer that cannot handle a correction cannot handle a
provider's corrected history either.

### A restart is survived by writing the open bar, not by replaying trades

On every flush the in-progress bar is written with `is_final = false`, and the consumer's
offsets are committed only afterwards. On startup the aggregator loads back every non-final
bar and resumes accumulating into it.

The alternative is to not commit offsets past the oldest trade still inside an open bar, so a
restart replays them. That is arguably purer — the bar is rebuilt from its inputs rather than
restored from a partial output — and it was rejected for two reasons. It requires committing
a *specific* offset rather than "everything consumed", which the `EventBatchConsumer` port
does not express and would have to grow a Kafka-shaped method to express. And it makes the
committed offset lag the read position by up to a full bar interval per partition, so a
crash replays a minute of every symbol rather than nothing.

### A trade for an unknown bar is loaded and corrected, never overwritten

When a trade arrives for a bar the process has no memory of, the bar is read from the store
first. If it exists, it is reopened and corrected; only if it genuinely does not is a new bar
started.

Without this step the aggregator would build a fresh bar containing that one trade, and the
revision-guarded upsert would **accept** it — a new bar's revision is 0 and so is the stored
one's, and `revision <= excluded.revision` is satisfied. A correct bar would be silently
replaced by a one-trade bar. Nothing would report it, and the only evidence would be a
volume figure that is wrong.

Resuming an *unfinished* bar this way is not a correction and does not increment `revision`.
Announcing a correction that never happened would mislead every consumer that watches
`revision` to detect rewritten history.

## Alternatives considered

**Wall-clock closing.** Close the bar when the process's own clock passes the boundary.
Simple, and wrong for the same reason `ARCHITECTURE.md` §4.4 insists every business decision
uses `occurred_at`: it makes the bars depend on how fast the consumer happens to be reading,
so a replay produces different bars from the live run that produced the same trades (F12).

**Per-symbol watermarks only.** Correct for sealing, and it leaves a quiet symbol's bar open
indefinitely — including across a close, so the last bar of the session is never written
until the process stops.

**A stateful stream processor (Kafka Streams, Flink).** Windowing, watermarks and state
restoration are exactly what these are for, and they would supply all four answers.
Rejected: decision D5 defers a stream framework until "stateful joins or cross-symbol
windowed aggregation is genuinely needed", this is neither, and the operational weight is
substantial. Revisit at D5's trigger.

**Aggregating in SQL from stored trades.** No in-memory state at all, and therefore no
restart problem. Rejected: it makes bars a batch product rather than a stream, so nothing can
consume a bar as it closes — which Phase 4's live chart and Phase 7's strategy driver both
need. The rollups to 5m and above *are* done this way, because they have no such requirement.

## Consequences

**Positive.** Bars are correct under replay, under restart, and under out-of-order delivery,
and each of those is asserted by a test that actually does the thing. Corrections are visible
in `revision` rather than implicit. Memory is bounded by an eviction margin rather than by
the length of a session.

**Negative.** Two watermarks is more to hold in your head than one, and the reason only
becomes obvious once you have seen the failure. A bar reopened from the store cannot place
its late trade relative to the trades it no longer has, so a correction adjusts high, low,
volume, VWAP and trade count but leaves `open` and `close` alone — a real limitation,
documented where it happens, and preferable to a close rewritten with a price from the middle
of the minute. Writing the open bar on every flush means one upsert per symbol per flush for
a row that is about to be rewritten.

**Neutral.** VWAP is exact for 1-minute bars, because the aggregator sees the trades, and
null for every rolled-up interval, because averaging VWAPs is not a VWAP.

## Revisit when

- A second aggregator instance is needed for throughput, at which point per-symbol state has
  to survive a partition reassignment and the stream-framework question (D5) is properly
  open.
- Corrections stop being rare enough to treat as an exception — visible as `corrections`
  approaching `bars_closed`, which would mean the lateness window is mis-sized rather than
  that the design is wrong.
- An interval other than 1 minute needs building from trades, which would make
  `BASE_INTERVAL` a setting rather than a constant.
