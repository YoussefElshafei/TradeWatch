"""Driving the aggregator from a batch consumer: when an offset is safe to acknowledge.

The same rule as the persistence consumer (ADR-0014) — write, then commit — and the same
reason for testing it: a crash between those two steps is either a redelivery or a permanent
hole in the bar series, depending entirely on their order.

What differs, and is tested here rather than assumed: the aggregator folds many trades into
one bar, so a rejected bar cannot be traced back to one poison trade. It is dropped as a bar
and the partition keeps moving.

And what differs more than anything: the persistence consumer is idempotent because its write
is a replacement. This one accumulates, so it is only idempotent because it records how far it
has applied each stream, in the same transaction as the bars (ADR-0016). `TestRedelivery` is
that claim under test, and it is the claim that was false in production.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal
from typing import Any

import pytest

from tests.bar_support import FakeBarStore, trade
from tests.kafka_support import quote
from tradewatch.application.aggregation import BarAggregationService, BarAggregator
from tradewatch.application.ports.event_bus import StreamPosition
from tradewatch.application.processing.errors import StorageError, UnprocessableEventError
from tradewatch.domain.entities import NewBar
from tradewatch.domain.events import BarEvent, EventType, MarketEvent

pytestmark = pytest.mark.unit


STREAM = "tw.market.trades.v1/0"


class ScriptedConsumer:
    """A `PositionedBatchConsumer` yielding a fixed script, then going quiet.

    Offsets start at ``start_offset`` and advance by one per event, which is what a single
    Kafka partition does. ``start_offset`` is the whole point: replaying the same script from
    the same offset into a fresh service is a rewind, and a rewind is the failure that
    doubled every bar in production.
    """

    def __init__(
        self,
        script: Sequence[Sequence[MarketEvent]],
        log: list[str],
        *,
        start_offset: int = 0,
        stream: str = STREAM,
    ) -> None:
        self.script = list(script)
        self.log = log
        self.stream = stream
        self.next_offset = start_offset
        self.delivered = 0
        self.commits: list[int] = []

    async def positioned_batches(
        self,
    ) -> AsyncIterator[Sequence[tuple[StreamPosition, MarketEvent]]]:
        for batch in self.script:
            await asyncio.sleep(0)
            self.delivered += len(batch)
            positioned = []
            for event in batch:
                positioned.append((StreamPosition(self.stream, self.next_offset), event))
                self.next_offset += 1
            yield positioned
        while True:
            await asyncio.sleep(0)
            yield []

    async def commit(self) -> None:
        self.log.append("commit")
        self.commits.append(self.delivered)


class RecordingStore(FakeBarStore):
    """A store that writes into a shared activity log, so ordering is assertable."""

    def __init__(self, log: list[str]) -> None:
        super().__init__()
        self.log = log

    async def save_bars(self, bars: Sequence[NewBar]) -> int:
        written = await super().save_bars(bars)
        self.log.append(f"write:{written}")
        return written


GROUP = "bar-aggregator"


def build(
    script: Sequence[Sequence[MarketEvent]],
    *,
    store: RecordingStore | None = None,
    start_offset: int = 0,
) -> tuple[BarAggregationService, ScriptedConsumer, RecordingStore, list[str]]:
    log: list[str] = []
    store = store if store is not None else RecordingStore(log)
    store.log = log
    consumer = ScriptedConsumer(script, log, start_offset=start_offset)
    aggregator = BarAggregator(store, lateness=dt.timedelta(seconds=5))
    service = BarAggregationService(consumer, aggregator, consumer_group=GROUP)
    return service, consumer, store, log


class TestOrdering:
    async def test_the_bar_is_written_before_the_offset_is_committed(self) -> None:
        """Reverse these and a crash in between is a gap in the series, forever."""
        service, _, _, log = build([[trade(seconds=10, price="100")]])

        await service.run(max_batches=1)

        assert log == ["write:1", "commit"]

    async def test_a_batch_is_committed_once_stored(self) -> None:
        service, consumer, store, _ = build(
            [[trade(seconds=10, price="100"), trade(seconds=20, price="102")]]
        )

        await service.run(max_batches=1)

        assert consumer.commits == [2]
        assert store.bar_for("AAPL") is not None

    async def test_several_batches_are_committed_one_by_one(self) -> None:
        script = [
            [trade(seconds=index * 5, price="100", trade_id=f"t{index}")] for index in range(3)
        ]
        service, consumer, _, _ = build(script)

        await service.run(max_batches=3)

        assert consumer.commits == [1, 2, 3]


class TestQuietMarkets:
    async def test_an_empty_poll_with_nothing_pending_does_not_commit(self) -> None:
        """A quiet poll must not re-commit the previous batch for no reason.

        The empty batch comes first, so the only commit in `commits` is the one the trade
        batch earned. (`max_batches` counts non-empty batches — a script of nothing but
        empty polls would never reach it, which is correct for a consumer and would hang a
        test.)
        """
        service, consumer, _, _ = build([[], [trade(seconds=10, price="100")]])

        await service.run(max_batches=1)

        assert consumer.commits == [1], "one commit, from the batch that had a trade in it"
        assert service.stats.empty_polls >= 1

    async def test_a_bar_is_flushed_on_a_quiet_poll_rather_than_waiting_for_a_trade(
        self,
    ) -> None:
        """Otherwise a bar that closed at 16:00 waits for the next session to be written."""
        service, consumer, store, _ = build([[trade(seconds=10, price="100")], []])
        # The first batch is handled and flushed; the second is empty with nothing pending.

        await service.run(max_batches=1)

        assert store.bar_for("AAPL") is not None
        assert consumer.commits == [1]


class TestFailures:
    async def test_a_storage_failure_is_not_committed(self) -> None:
        """The single most important assertion here.

        If this commits, the trades are acknowledged without their bar being written, the
        restart never sees them again, and the series has a hole nothing reports.
        """
        service, consumer, store, _ = build([[trade(seconds=10, price="100")]])
        store.fail_next = StorageError("database is down")

        with pytest.raises(StorageError):
            await service.run(max_batches=1)

        assert consumer.commits == [], "nothing may be acknowledged"

    async def test_a_bar_the_database_refuses_does_not_stop_the_partition(self) -> None:
        """Failure F3. A bar that can never be written must not block every later one."""
        service, consumer, store, _ = build([[trade(seconds=10, price="100")]])
        store.fail_next = UnprocessableEventError("violates a check constraint")

        await service.run(max_batches=1)

        assert consumer.commits == [1], "the partition moved on"
        assert service.stats.rejected_bars == 1

    async def test_a_rejected_bar_is_dropped_rather_than_retried_forever(self) -> None:
        service, _, store, _ = build([[trade(seconds=10, price="100")]])
        store.fail_next = UnprocessableEventError("violates a check constraint")

        await service.run(max_batches=1)

        assert service.aggregator.pending == 0, "the poison bar is not still buffered"

    async def test_cancellation_does_not_commit(self) -> None:
        service, consumer, store, _ = build([[trade(seconds=10, price="100")]])
        blocked = asyncio.Event()
        original = store.save_bars

        async def block(bars: Sequence[NewBar]) -> int:
            await blocked.wait()
            return await original(bars)

        store.save_bars = block  # type: ignore[method-assign]

        task = asyncio.create_task(service.run())
        for _ in range(5):
            await asyncio.sleep(0)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert consumer.commits == []


class TestEventTypes:
    async def test_a_non_trade_event_is_counted_and_skipped(self) -> None:
        """The aggregator subscribes to the trades topic alone, so this should never happen.

        It is counted rather than ignored precisely because a non-zero value means a routing
        mistake upstream, and a silent skip would hide it.
        """
        service, _, store, _ = build([[quote(), trade(seconds=10, price="100")]])

        await service.run(max_batches=1)

        assert service.stats.ignored == 1
        assert service.stats.trades == 1
        assert store.bar_for("AAPL") is not None


class TestShutdown:
    async def test_stop_ends_the_loop(self) -> None:
        stop = asyncio.Event()
        stop.set()
        service, consumer, _, _ = build([[trade(seconds=10, price="100")]])

        await service.run(stop=stop)

        assert consumer.commits == []

    async def test_close_finalises_open_bars(self) -> None:
        service, _, store, _ = build([[trade(seconds=10, price="100")]])
        await service.run(max_batches=1)
        assert store.bar_for("AAPL").is_final is False  # type: ignore[union-attr]

        await service.close()

        assert store.bar_for("AAPL").is_final is True  # type: ignore[union-attr]


class TestRedelivery:
    """The bug that made ADR-0016 necessary, asserted rather than argued.

    `TestRestart` in `test_bar_aggregator.py` covers a restart that resumes with *new*
    trades, and passes without any of this machinery, because resuming is not the hard part.
    At-least-once delivery means a restart routinely redelivers trades a previous run already
    folded in, and folding a trade twice adds its size to the volume twice. Nothing in those
    tests ever delivered the same trade to two runs, so nothing in them could fail when it
    did — which is how the doubling reached production and was only found by rewinding the
    consumer group's offsets by hand.

    What makes these tests worth their length is what they assert *does not* change. OHLC is
    first/max/min/last and is idempotent on its own, so a doubled bar has perfect prices; only
    `volume`, `trade_count` and `vwap` move. Asserting the prices survive is not the point.
    """

    #: How far two VWAPs are compared. A bar rebuilt across a restart re-derives its
    #: `notional` as `stored_vwap * volume` (`OpenBar.from_stored`), which is a division
    #: reversed, so the last digit of a 28-significant-figure Decimal can differ. Ten decimal
    #: places is far beyond any price this represents and nowhere near the doubling this
    #: class exists to catch, which moves the VWAP by whole currency units or not at all.
    VWAP_PLACES = Decimal("0.0000000001")

    @staticmethod
    def figures(store: RecordingStore) -> tuple[Any, ...]:
        """Everything a doubled fold would disturb, exactly, except VWAP."""
        bar = store.bar_for("AAPL")
        assert bar is not None
        return (bar.open, bar.high, bar.low, bar.close, bar.volume, bar.trade_count)

    @classmethod
    def vwap(cls, store: RecordingStore) -> Decimal | None:
        bar = store.bar_for("AAPL")
        assert bar is not None
        return None if bar.vwap is None else bar.vwap.quantize(cls.VWAP_PLACES)

    async def test_a_rewound_batch_is_skipped_rather_than_folded_again(self) -> None:
        """Rewind the offsets and the bar must come back identical.

        This is the exact production reproduction: the same events, from the same offsets,
        into a process that has already applied them.
        """
        script = [
            [trade(seconds=10, price="100", size="10", trade_id="a")],
            [trade(seconds=20, price="104", size="7", trade_id="b")],
        ]

        first, _, store, _ = build(script)
        await first.run(max_batches=2)
        before = self.figures(store)
        before_vwap = self.vwap(store)

        # The offsets are rewound to zero and every event is delivered a second time.
        second, _, _, _ = build(script, store=store, start_offset=0)
        await second.aggregator.restore(["AAPL"])
        await second.run(max_batches=2)

        assert self.figures(store) == before, "redelivery changed a stored bar"
        assert self.vwap(store) == before_vwap
        assert second.stats.already_applied == 2, "both events were recognised as applied"
        assert second.stats.trades == 0, "and none of them reached the aggregator"

    async def test_a_restart_that_redelivers_then_continues_counts_each_trade_once(
        self,
    ) -> None:
        """The realistic shape: the tail of the last batch comes back, then new trades do.

        A restart replays from the last committed offset, so the events between that offset
        and the crash arrive twice. The bar must end up as though each trade were folded in
        exactly once — which is the property `TestRestart` asserts for the resume case and
        could not assert for this one.
        """
        early = [
            trade(seconds=10, price="100", size="10", trade_id="a"),
            trade(seconds=20, price="104", size="7", trade_id="b"),
        ]
        late = [trade(seconds=40, price="98", size="5", trade_id="c")]

        first, _, store, _ = build([early])
        await first.run(max_batches=1)

        # Restart: `early` is redelivered from its original offsets, `late` follows on.
        second, _, _, _ = build([early, late], store=store, start_offset=0)
        await second.aggregator.restore(["AAPL"])
        await second.run(max_batches=2)

        straight, _, clean_store, _ = build([early + late])
        await straight.run(max_batches=1)

        assert self.figures(store) == self.figures(clean_store)
        assert self.vwap(store) == self.vwap(clean_store)
        assert second.stats.already_applied == 2
        assert second.stats.trades == 1, "only the trade it had never seen"

    async def test_a_later_offset_is_still_folded_even_on_a_stream_seen_before(self) -> None:
        """The guard must skip what is at or below the mark, and nothing above it.

        A guard that skipped too much would be far worse than the doubling it replaces: lost
        trades leave no trace at all, where a doubled one at least shows up in the volume.
        """
        first, _, store, _ = build([[trade(seconds=10, price="100", size="10", trade_id="a")]])
        await first.run(max_batches=1)

        second, _, _, _ = build(
            [[trade(seconds=20, price="104", size="7", trade_id="b")]],
            store=store,
            start_offset=1,
        )
        await second.aggregator.restore(["AAPL"])
        await second.run(max_batches=1)

        bar = store.bar_for("AAPL")
        assert bar is not None
        assert bar.volume == Decimal("17"), "the new trade was folded in"
        assert bar.trade_count == 2
        assert second.stats.already_applied == 0


class TestProgressIsWrittenWithTheBars:
    """Progress that can outrun its bars is a silent hole; behind them, a double count."""

    async def test_progress_advances_only_when_the_bars_are_written(self) -> None:
        service, _, store, _ = build([[trade(seconds=10, price="100")]])
        store.fail_next = StorageError("database is down")

        with pytest.raises(StorageError):
            await service.run(max_batches=1)

        assert store.progress.get(GROUP, {}) == {}, "the position must not move without bars"

    async def test_a_written_batch_records_its_position(self) -> None:
        service, _, store, _ = build(
            [[trade(seconds=10, price="100"), trade(seconds=20, price="104")]]
        )

        await service.run(max_batches=1)

        assert store.progress[GROUP][STREAM] == 1, "the offset of the last event folded in"

    async def test_a_rejected_bar_does_not_record_its_position(self) -> None:
        """Deliberate, and the one place progress is knowingly left behind the offsets.

        The bar was discarded, so nothing in the database counts these events and there is
        no double count to protect against. Leaving the mark behind means a future replay may
        rebuild the bar instead of skipping past a hole forever.
        """
        service, consumer, store, _ = build([[trade(seconds=10, price="100")]])
        store.fail_next = UnprocessableEventError("violates a check constraint")

        await service.run(max_batches=1)

        assert service.stats.rejected_bars == 1
        assert consumer.commits == [1], "the partition still moved on"
        assert store.progress.get(GROUP, {}) == {}


class TestShutdownIsIdempotentToo:
    """`close()` writes bars on a path that never commits its offsets.

    So every trade in them is certain to be delivered again, and until ADR-0016 they were
    written without the position that accounts for them — which made the ordinary shutdown
    the easiest way to double a bar.
    """

    @staticmethod
    async def cancel_mid_flush(service: BarAggregationService, store: RecordingStore) -> None:
        """Run until the first write is in flight, then kill the process there."""
        blocked = asyncio.Event()
        original = store.save_bars

        async def block(bars: Sequence[NewBar]) -> int:
            await blocked.wait()
            return await original(bars)

        store.save_bars = block  # type: ignore[method-assign]
        task = asyncio.create_task(service.run())
        for _ in range(5):
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        blocked.set()
        store.save_bars = original  # type: ignore[method-assign]

    async def test_close_records_the_position_of_the_bars_it_finalises(self) -> None:
        service, _, store, _ = build([[trade(seconds=10, price="100", size="10")]])

        await self.cancel_mid_flush(service, store)
        assert store.progress.get(GROUP, {}) == {}, "nothing written, so nothing claimed"

        await service.close()

        bar = store.bar_for("AAPL")
        assert bar is not None and bar.is_final is True
        assert store.progress[GROUP][STREAM] == 0, "the bar and its position, together"

    async def test_the_trades_a_shutdown_finalised_are_not_folded_in_again(self) -> None:
        """End to end: cancel, close, redeliver everything, and the volume must not move."""
        script = [[trade(seconds=10, price="100", size="10")]]
        service, _, store, _ = build(script)

        await self.cancel_mid_flush(service, store)
        await service.close()

        # Nothing was ever committed to the broker, so the restart sees all of it again.
        restarted, _, _, _ = build(script, store=store, start_offset=0)
        await restarted.aggregator.restore(["AAPL"])
        await restarted.run(max_batches=1)

        bar = store.bar_for("AAPL")
        assert bar is not None
        assert bar.volume == Decimal("10"), "one trade, counted once"
        assert bar.trade_count == 1
        assert restarted.stats.already_applied == 1


class RecordingPublisher:
    """An `EventPublisher` that records what it was given into the shared activity log."""

    def __init__(self, log: list[str], *, fail: bool = False) -> None:
        self.log = log
        self.fail = fail
        self.events: list[MarketEvent] = []

    async def publish(self, event: MarketEvent) -> None:
        await self.publish_many([event])

    async def publish_many(self, events: Sequence[MarketEvent]) -> int:
        if self.fail:
            raise ConnectionError("broker down")
        self.events.extend(events)
        self.log.append(f"publish:{len(events)}")
        return len(events)

    async def flush(self) -> None:
        return None


def build_publishing(
    script: Sequence[Sequence[MarketEvent]], *, fail: bool = False
) -> tuple[BarAggregationService, RecordingPublisher, list[str]]:
    log: list[str] = []
    store = RecordingStore(log)
    consumer = ScriptedConsumer(script, log)
    aggregator = BarAggregator(store, lateness=dt.timedelta(seconds=5))
    publisher = RecordingPublisher(log, fail=fail)
    service = BarAggregationService(consumer, aggregator, consumer_group=GROUP, publisher=publisher)
    return service, publisher, log


class TestPublishingClosedBars:
    """Phase 10 found that nothing published live bars, so their consumers had no input."""

    async def test_a_closed_bar_is_published_after_the_write_and_before_the_commit(self) -> None:
        first, second = trade(seconds=10, price="100"), trade(seconds=70, price="101")
        service, publisher, log = build_publishing([[first, second]])

        await service.run(max_batches=1)

        assert log == ["write:2", "publish:1", "commit"]
        (bar,) = publisher.events
        assert isinstance(bar, BarEvent)
        assert bar.event_type is EventType.BAR_CLOSED
        assert bar.bar_start == first.occurred_at.replace(second=0, microsecond=0)
        assert (bar.open, bar.close, bar.volume) == (Decimal("100"), Decimal("100"), Decimal("10"))
        # A derived event: the source and correlation id of the trades it was built from.
        assert bar.source == first.source
        assert bar.correlation_id == first.correlation_id

    async def test_an_open_bar_is_stored_but_not_published(self) -> None:
        service, publisher, log = build_publishing([[trade(seconds=10, price="100")]])

        await service.run(max_batches=1)

        assert publisher.events == []
        assert log == ["write:1", "commit"]

    async def test_a_failed_publish_does_not_stop_the_stored_bar_being_committed(self) -> None:
        service, _, log = build_publishing(
            [[trade(seconds=10, price="100"), trade(seconds=70, price="101")]], fail=True
        )

        await service.run(max_batches=1)

        assert log == ["write:2", "commit"]
        assert service.stats.bar_publish_failures == 1
