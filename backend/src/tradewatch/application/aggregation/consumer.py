"""Driving the aggregator from a batch consumer.

The same acknowledgement rule as the persistence consumer (ADR-0014): fold the batch in,
flush, **then** commit. Reversing the last two makes a crash in between a permanent hole in
the bar series.

**Every event reaching the aggregator is reaching it for the first time ever.** This class
is what makes that true, and it is the reason :class:`BarAggregator` contains no idempotency
logic at all: it stays a plain accumulator, and the accumulation is protected one level up.

The protection is needed because an accumulator cannot be made idempotent by its writes.
`UNIQUE (event_id)` makes persisting a trade idempotent because that write is a *replacement*;
folding a trade into a bar is an *addition*, and at-least-once delivery adds it again. That
was not hypothetical — rewinding this group's offsets doubled `volume`, `trade_count` and
VWAP on every affected bar, while OHLC came back perfect, because first/max/min/last are
idempotent and sum is not. The bars looked entirely normal and were wrong (ADR-0016).

So the service records how far it has applied each stream, in the same transaction as the
bars that account for it, and skips anything at or below that mark.

**Why this is not `MarketDataConsumerService`.** That class isolates a poison *event*: it
replays a rejected batch one event at a time, because there the unit of work is one row per
event and one bad row need cost only itself. Here many trades fold into one bar, so "which
event was poison" has no answer — a bar the database refuses is refused as a bar, and the
trades that built it are individually fine. The two loops look similar and mean different
things, and merging them would produce a class with a flag that changes what its failure
handling means.

What is genuinely shared is the ordering rule, and that lives in ADR-0014 where both can
point at it.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from tradewatch.application.aggregation.service import BarAggregator
from tradewatch.application.ports.event_bus import PositionedBatchConsumer, StreamPosition
from tradewatch.application.processing.errors import StorageError, UnprocessableEventError
from tradewatch.domain.events import MarketEvent, TradeEvent
from tradewatch.observability.logging import get_logger

__all__ = ["AggregationConsumerStats", "BarAggregationService"]

log = get_logger(__name__)


@dataclass(slots=True)
class AggregationConsumerStats:
    """Counters for the acknowledgement path.

    Kept apart from :class:`~tradewatch.application.aggregation.service.AggregatorStats` for
    the same reason the persistence consumer keeps its own: "committed the offset" and
    "wrote the bar" are different claims, and a bug that makes them disagree is the one
    worth catching.
    """

    batches: int = 0
    trades: int = 0
    #: Events skipped because the recorded position proves they are already in a bar. Zero in
    #: steady state; non-zero after a restart, a rebalance or a deliberate offset rewind, and
    #: that is the counter that says the idempotency guard actually did something.
    already_applied: int = 0
    #: Events on the topic that are not trades. Should be zero — the aggregator subscribes
    #: to the trades topic alone — so a non-zero value means a routing mistake upstream.
    ignored: int = 0
    empty_polls: int = 0
    commits: int = 0
    rejected_bars: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "batches": self.batches,
            "trades": self.trades,
            "already_applied": self.already_applied,
            "ignored": self.ignored,
            "empty_polls": self.empty_polls,
            "commits": self.commits,
            "rejected_bars": self.rejected_bars,
        }


class BarAggregationService:
    """Consume trades, build bars, acknowledge only once they are stored."""

    def __init__(
        self,
        consumer: PositionedBatchConsumer,
        aggregator: BarAggregator,
        *,
        consumer_group: str,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self._consumer = consumer
        self._aggregator = aggregator
        self._group = consumer_group
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))
        #: stream -> the highest offset already folded into a stored bar. Loaded once at
        #: startup and advanced only once the bars accounting for it are durable.
        self._applied: dict[str, int] = {}
        #: Folded since the last flush, waiting to be made durable alongside those bars.
        self._pending: dict[str, int] = {}
        self._stats = AggregationConsumerStats()

    @property
    def stats(self) -> AggregationConsumerStats:
        return self._stats

    @property
    def aggregator(self) -> BarAggregator:
        return self._aggregator

    async def run(
        self,
        *,
        max_batches: int | None = None,
        stop: asyncio.Event | None = None,
    ) -> AggregationConsumerStats:
        """Consume until cancelled, until ``stop`` is set, or until ``max_batches``."""
        self._applied = dict(await self._aggregator.store.load_progress(self._group))
        log.info(
            "aggregation.started",
            consumer_group=self._group,
            resumed_streams=len(self._applied),
        )

        try:
            async for batch in self._consumer.positioned_batches():
                if stop is not None and stop.is_set():
                    log.info("aggregation.stop_requested", **self._stats.as_dict())
                    break

                if not batch:
                    # A poll that timed out. Flush anyway: a bar whose minute has passed
                    # should not wait for the next trade in a quiet market to be written.
                    self._stats.empty_polls += 1
                    if self._aggregator.pending:
                        await self._flush_and_commit()
                    continue

                await self._handle(batch)
                await self._flush_and_commit()

                self._stats.batches += 1
                if max_batches is not None and self._stats.batches >= max_batches:
                    break
        except asyncio.CancelledError:
            # Whatever is unflushed was never written, so it must not be acknowledged. The
            # trades come back on restart and rebuild the same bars.
            log.info("aggregation.cancelled", pending=self._aggregator.pending)
            raise
        except StorageError:
            log.error(
                "aggregation.storage_failed_uncommitted",
                pending=self._aggregator.pending,
                **self._stats.as_dict(),
            )
            raise
        finally:
            # Nested rather than splatted: both dicts carry a `trades` key, and merging
            # them raises `TypeError: got multiple values for keyword argument` — from
            # inside a `finally`, on the shutdown path, where it would mask whatever was
            # actually being shut down.
            log.info(
                "aggregation.stopped",
                consumed=self._stats.as_dict(),
                aggregated=self._aggregator.stats.as_dict(),
            )

        return self._stats

    async def _handle(self, batch: Sequence[tuple[StreamPosition, MarketEvent]]) -> None:
        """Fold each event into its bar, at most once ever.

        An event at or below the recorded position for its stream is provably already in a
        stored bar, because that position was written in the same transaction as the bar.
        Folding it again would add its size to the volume a second time, so it is counted and
        skipped instead.
        """
        for position, event in batch:
            if position.offset <= self._applied.get(position.stream, -1):
                self._stats.already_applied += 1
                continue

            if isinstance(event, TradeEvent):
                self._stats.trades += 1
                await self._aggregator.handle(event)
            else:
                self._stats.ignored += 1
                log.warning(
                    "aggregation.unexpected_event_type",
                    event_type=event.event_type.value,
                    detail="the aggregator subscribes to the trades topic only",
                )

            # Advanced whether or not the event produced a bar. An event this service has
            # decided about is dealt with, and leaving the mark behind it would mean
            # reconsidering the same non-trade event after every restart.
            self._pending[position.stream] = max(
                self._pending.get(position.stream, -1), position.offset
            )

    async def _flush_and_commit(self) -> None:
        """Write, then acknowledge. Never the other way round.

        Raises:
            StorageError: If the write failed in a way worth retrying. The offset is not
                committed, so the trades are redelivered and the bars rebuilt.
        """
        try:
            await self._aggregator.flush(
                consumer_group=self._group, positions=self._pending or None
            )
        except UnprocessableEventError as error:
            # A bar the database refuses — an OHLC invariant it does not satisfy. Retrying
            # produces the same refusal, so the offset advances and the bar is lost rather
            # than the partition stopping on it forever (failure F3). Logged at error
            # because a bar that cannot be stored means this symbol's series now has a hole.
            self._stats.rejected_bars += 1
            dropped = self._aggregator.discard_pending()
            log.error(
                "aggregation.bar_rejected",
                reason=error.message,
                detail=error.detail,
                dropped_bars=dropped,
                total_rejected=self._stats.rejected_bars,
            )
            # The position is deliberately *not* recorded. Nothing in the database counts
            # these events now, so if they are ever delivered again — a rewind, a reset —
            # they may be folded into a fresh bar and heal the hole. Recording progress here
            # would make the gap permanent in exchange for protection against a double count
            # that cannot happen, because there is no bar left to double.
        else:
            # Bars and position are durable together, so what was pending is now applied.
            # Only now is it safe to acknowledge these events to the broker.
            self._applied.update(self._pending)
            self._pending.clear()

        await self._consumer.commit()
        self._stats.commits += 1

    async def close(self) -> None:
        """Close every open bar and write it, with the position that accounts for it.

        The Kafka offsets are *not* committed afterwards, deliberately — the loop has already
        committed everything it flushed, and this path runs after a cancellation, where the
        last batch may not have been flushed at all. Every trade in these bars is therefore
        liable to be delivered again.

        Which is exactly why the position goes in. This used to write the bars alone, on the
        argument that a restart would rebuild better ones from the redelivered trades. It
        would not: the bars are written ``is_final=True``, so :meth:`BarAggregator.restore`
        skips them, and the redelivered trades reopen them as a correction and **add their
        volume a second time**. Recording the position with the bars is what makes the
        shutdown path agree with every other path in the service (ADR-0016).
        """
        try:
            written = await self._aggregator.close_all(
                consumer_group=self._group, positions=self._pending or None
            )
        except (StorageError, UnprocessableEventError) as error:
            log.error("aggregation.final_flush_failed", error=str(error))
            return
        self._applied.update(self._pending)
        self._pending.clear()
        if written:
            log.info("aggregation.final_flush", rows=written)
