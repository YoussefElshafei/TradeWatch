# ADR-0016: Idempotent bar aggregation under redelivery

- **Status:** Accepted
- **Date:** 2026-09-21
- **Phase:** 3
- **Supersedes:** [ADR-0015](ADR-0015-event-time-bar-aggregation.md)

> ADR-0015 is immutable and its file is unchanged, so its own header still reads *Accepted*.
> The index in [README.md](README.md) carries its superseded status. Everything ADR-0015
> decided about **event time** — two watermarks, per-symbol sealing, writing at the boundary
> and correcting afterwards, loading an unknown bar before touching it — is still in force and
> restated here unchanged. What it got wrong is one paragraph, *"A restart is survived by
> writing the open bar, not by replaying trades"*, and that paragraph is replaced.

## Context

ADR-0015 claimed the aggregator survives a restart because the open bar is written on every
flush with `is_final = false`, offsets are committed only afterwards, and startup loads the
non-final bars back. Every word of that is true, and it is not sufficient.

The gap is that **offsets are committed after the write, so events between the last flush and
the crash are delivered again** — deliberately, because that is the at-least-once guarantee
ADR-0014 relies on everywhere else. The persistence consumer absorbs a redelivery without
noticing, because `UNIQUE (event_id)` makes writing a trade a *replacement*: write it twice
and the row is the same row. Folding a trade into a bar is an *addition*. Fold it twice and
its size is in the volume twice.

ADR-0015 never confronted this because its restart tests only ever delivered *new* trades to
the resumed process. Nothing in them handed the same trade to two runs, so nothing in them
could fail when production did.

**How it was found, and what it looked like.** The consumer group's offsets were rewound by
3,000 per partition across three partitions and the aggregator was allowed to drain the lag.
The bars it rewrote were checksummed before and after:

| | Before | After |
| --- | --- | --- |
| Checksum over the affected bars | `f010999066c559d2f1664d2e75e4075a` | `61497122929723a80269f689d5ed72ce` |
| AAPL 14:28 `trade_count` | 26 | 52 |
| AAPL 14:28 `volume` | 6,828 | 13,656 |
| AAPL 14:28 `open/high/low/close` | unchanged | unchanged |

Every additive field exactly doubled. **Every price was perfect**, because open, high, low and
close are first, max, min and last, and all four are idempotent. That is what made this worth
an ADR rather than a bug fix: a doubled bar passes every OHLC invariant the database enforces,
renders as a normal candle, and is wrong in precisely the fields a volume-weighted strategy
would price off.

The same bug has a second, likelier trigger than a rewind. On shutdown the service closed its
open bars and wrote them **without committing offsets**, on ADR-0015's argument that a restart
would rebuild better bars from the redelivered trades. It would not: those bars are written
`is_final = true`, `restore()` loads only non-final bars, so the redelivered trades reopened
them as a *correction* and added their volume on top. An ordinary graceful shutdown was enough.

## Decision

**The aggregator records how far it has applied each stream, in the same transaction as the
bars that account for it, and skips any event at or below that mark.**

Concretely:

1. A `consumer_progress` table keyed `(consumer_group, stream)` holding one `position`.
   `stream` is one ordered sequence — a Kafka topic-partition, written `topic/partition`.
2. `BarStore.save_bars_with_progress(bars, consumer_group=…, positions=…)` writes both in one
   transaction. The position upsert takes `greatest(stored, incoming)`, so a rebalance that
   hands a partition back after another member advanced it cannot move a position backwards.
3. The event bus port gains `StreamPosition` and `PositionedBatchConsumer`, so a consumer can
   be told where each event sat without importing Kafka.
4. `BarAggregationService` loads its progress at startup, drops any event at or below the mark
   for its stream, counts those in `already_applied`, and flushes bars-with-progress **before**
   committing offsets. `close()` does the same, which is what fixes the shutdown path.

The ordering is unchanged from ADR-0014 and now has three steps instead of two: fold, write
bars *and* position atomically, then commit offsets.

**Why atomicity, specifically.** Written separately, a crash between the two writes produces
one of exactly two states, and both are bad: a bar counting an event the progress says was
never applied — which doubles on the next delivery, the bug we started with — or a progress
claiming an event no bar ever saw, which is a silent hole. One transaction admits neither.

**Where the guard lives.** In the service, not in `BarAggregator`. The aggregator stays a
plain accumulator with no idempotency logic in it; the service guarantees that every event
reaching it is reaching it for the first time ever. Putting the check inside the accumulator
would mean every future consumer of bars re-implements it, and would put a transport concept
inside the thing that is meant to be pure arithmetic over trades.

**One deliberate exception.** When the database permanently refuses a bar (failure F3), the
bar is discarded and the offsets are committed, but the position is **not** recorded. Nothing
in the database counts those events, so there is no double count to protect against, and
leaving the mark behind means a future replay may rebuild the bar rather than skipping past a
hole forever.

## Alternatives considered

**Rebuild the bar from the `trades` table instead of accumulating.** Genuinely idempotent —
recomputing a bar from rows is a replacement, not an addition — and rejected, because it
creates a cross-consumer-group data dependency that the entire Kafka argument (ADR-0001)
exists to avoid. The aggregator would be reading the *persistence* consumer's output. Those
two groups run at wildly different offsets in practice; it was observed live. When
`processor-persist` lags, the aggregator would compute a bar from a partial trade table and
write it `is_final = true`. That is silent, catastrophic, and indistinguishable from a quiet
minute. The obvious safety valve — only trust the table when its count is at least what was
accumulated — defeats the fix under the very case it is for, since twice the accumulated count
is never at most the table's count.

**Record the `event_id` of every trade folded into each bar.** Exact, and it is the trades
table again: one row per trade, which is the storage this design already has and does not need
twice. Held in memory instead, it does not survive the restart that is the whole problem;
persisted per flush, it is tens of kilobytes rewritten every flush to express what one integer
expresses.

**Hold the offsets until the bar seals, so a redelivered trade is one that was never counted.**
No new table, and it fails the requirement outright: it does nothing about a *manual* offset
rewind, which is an operational action someone will take during an incident and which is the
test this fix had to pass. It also reintroduces exactly what ADR-0015 rejected — committing a
specific offset rather than "everything consumed" — and makes the committed offset lag the read
position by up to a bar interval per partition.

**Make the upsert compute the bar rather than replace it** (`volume = bars.volume + excluded.volume`).
Moves the addition into SQL without making it idempotent; the database cannot know whether it
has seen this trade before either.

## Consequences

**Positive.** An offset rewind is now a non-event: the aggregator recognises the replayed
events, counts them in `already_applied`, and the bars do not move. Graceful shutdown stops
doubling the last minute. The property is asserted by tests that redeliver an already-folded
event rather than only resuming with new ones, which is the coverage ADR-0015 was missing.
`already_applied` makes the guard observable — zero in steady state, non-zero after a restart,
rebalance or rewind.

**Negative.** One more table, one more concept, and a transport-shaped idea (`StreamPosition`)
in the application ports, justified in the port's own docstring. Any *other* accumulating
consumer added later has to opt into the same mechanism; nothing forces it to, and forgetting
would reproduce this bug in a new place. A `positions` write happens on every flush even when
no bar changed.

**Neutral.** `EventBatchConsumer` is untouched and the persistence consumer still uses it. It
does not accumulate, so positions would be surface it never reads.

**Not fixed here, and knowingly so.** A single symbol backlogged by hours still defeats the
lateness window: its own watermark jumps forward as the backlog drains, sealing its earlier
bars, and trades behind that jump are counted in `late_dropped` and discarded. That is failure
**F11** (`ARCHITECTURE.md` §12), it is orthogonal to redelivery, and it belongs to Phase 11's
failure-recovery work. This ADR makes redelivery safe; it does not widen the lateness window.

## Revisit when

- A second accumulating consumer exists, at which point the load-skip-flush-commit sequence
  should become a shared component rather than a pattern repeated by hand.
- More than one aggregator instance runs, where `greatest(stored, incoming)` is doing real work
  during rebalances and deserves a test that actually rebalances.
- A stream framework is adopted under decision D5, since offset-with-state is what their
  changelog topics do for you.
