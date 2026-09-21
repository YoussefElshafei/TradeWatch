"""The bar aggregator, against an in-memory store.

The numbers here are worked out by hand and written into the assertions, because a test that
recomputes the aggregation it is checking proves only that the code agrees with itself. A
bar whose OHLCV is wrong is the kind of defect that survives into a backtest and produces a
confident, wrong Sharpe ratio, so the arithmetic is pinned exactly.

The properties that matter most, in order:

* the bar is right — open, high, low, close, volume and VWAP, to the cent;
* a bar closes on **event time**, so a quiet symbol still gets its bars;
* a late trade inside the window corrects the bar; outside it, the trade is counted and
  dropped rather than silently accepted;
* a restart mid-bar produces the bar that would have existed without one (failure F13);
* a trade for a bar this process never saw never overwrites it with a one-trade bar.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from tests.bar_support import MINUTE, ONE_MINUTE, FakeBarStore, trade
from tradewatch.application.aggregation import BarAggregator, floor_to_interval
from tradewatch.domain.enums import BarInterval

pytestmark = pytest.mark.unit


def build(lateness_seconds: float = 5) -> tuple[BarAggregator, FakeBarStore]:
    store = FakeBarStore()
    return (
        BarAggregator(store, lateness=dt.timedelta(seconds=lateness_seconds)),
        store,
    )


class TestBucketing:
    def test_a_trade_lands_in_its_own_minute(self) -> None:
        assert floor_to_interval(MINUTE + dt.timedelta(seconds=59), ONE_MINUTE) == MINUTE
        assert floor_to_interval(MINUTE + dt.timedelta(seconds=60), ONE_MINUTE) == (
            MINUTE + dt.timedelta(minutes=1)
        )

    def test_buckets_are_anchored_to_the_clock_not_the_data(self) -> None:
        """The SQL rollup anchors at the epoch too. Two anchors would disagree."""
        odd = dt.datetime(2026, 3, 2, 14, 37, 23, 456789, tzinfo=dt.UTC)

        assert floor_to_interval(odd, ONE_MINUTE) == dt.datetime(2026, 3, 2, 14, 37, tzinfo=dt.UTC)
        assert floor_to_interval(odd, BarInterval.FIVE_MINUTES) == dt.datetime(
            2026, 3, 2, 14, 35, tzinfo=dt.UTC
        )
        assert floor_to_interval(odd, BarInterval.ONE_HOUR) == dt.datetime(
            2026, 3, 2, 14, 0, tzinfo=dt.UTC
        )

    def test_the_aggregator_only_builds_the_base_interval(self) -> None:
        """Everything coarser is rolled up from 1m, never built from trades twice."""
        with pytest.raises(ValueError, match="1m"):
            BarAggregator(FakeBarStore(), interval=BarInterval.FIVE_MINUTES)


class TestTheBarItself:
    """Hand-calculated. Three trades in one minute:

    ==========  ======  ====  ==========
    at          price   size  notional
    ==========  ======  ====  ==========
    14:30:10    100.00    10     1000.00
    14:30:30    102.00     5      510.00
    14:30:50     99.00     5      495.00
    ==========  ======  ====  ==========

    open 100, high 102, low 99, close 99, volume 20, notional 2005, vwap 2005/20 = 100.25
    """

    async def build_minute(self) -> tuple[BarAggregator, FakeBarStore]:
        aggregator, store = build()
        for seconds, price, size in ((10, "100", "10"), (30, "102", "5"), (50, "99", "5")):
            await aggregator.handle(trade(seconds=seconds, price=price, size=size))
        return aggregator, store

    async def test_ohlcv_is_exact(self) -> None:
        aggregator, store = await self.build_minute()

        await aggregator.flush()
        bar = store.bar_for("AAPL")

        assert bar is not None
        assert bar.open == Decimal("100")
        assert bar.high == Decimal("102")
        assert bar.low == Decimal("99")
        assert bar.close == Decimal("99")
        assert bar.volume == Decimal("20")
        assert bar.trade_count == 3

    async def test_vwap_is_the_real_volume_weighted_price(self) -> None:
        """2005 / 20 = 100.25. Not the mean of the prices, which would be 100.33."""
        aggregator, store = await self.build_minute()

        await aggregator.flush()
        bar = store.bar_for("AAPL")

        assert bar is not None
        assert bar.vwap == Decimal("100.25")

    async def test_a_single_trade_makes_a_flat_bar(self) -> None:
        aggregator, store = build()
        await aggregator.handle(trade(seconds=5, price="50", size="3"))

        await aggregator.flush()
        bar = store.bar_for("AAPL")

        assert bar is not None
        assert (bar.open, bar.high, bar.low, bar.close) == (
            Decimal("50"),
            Decimal("50"),
            Decimal("50"),
            Decimal("50"),
        )
        assert bar.vwap == Decimal("50")

    async def test_symbols_do_not_mix(self) -> None:
        aggregator, store = build()
        await aggregator.handle(trade(seconds=10, price="100", symbol="AAPL"))
        await aggregator.handle(trade(seconds=20, price="200", symbol="MSFT"))

        await aggregator.flush()

        apple = store.bar_for("AAPL")
        microsoft = store.bar_for("MSFT")
        assert apple is not None and apple.close == Decimal("100")
        assert microsoft is not None and microsoft.close == Decimal("200")

    async def test_a_written_bar_always_satisfies_the_database_constraints(self) -> None:
        """`NewBar` validates the same invariants the CHECK constraints do.

        Constructing one is therefore the assertion: an incoherent bar raises here rather
        than becoming an IntegrityError in the consumer.
        """
        aggregator, store = await self.build_minute()

        await aggregator.flush()
        bar = store.bar_for("AAPL")

        assert bar is not None
        assert bar.high >= bar.low
        assert bar.low <= bar.open <= bar.high
        assert bar.low <= bar.close <= bar.high
        assert bar.volume >= 0


class TestClosing:
    async def test_a_bar_closes_when_event_time_passes_its_end(self) -> None:
        aggregator, store = build()
        await aggregator.handle(trade(seconds=10, price="100"))

        await aggregator.flush()
        assert store.bar_for("AAPL") is not None
        assert store.bar_for("AAPL").is_final is False  # type: ignore[union-attr]

        # A trade in the next minute moves the watermark past 14:31:00.
        await aggregator.handle(trade(seconds=65, price="101"))
        await aggregator.flush()

        assert store.bar_for("AAPL").is_final is True  # type: ignore[union-attr]

    async def test_another_symbols_activity_closes_a_quiet_symbols_bar(self) -> None:
        """One venue clock. Otherwise a symbol that trades hourly has a bar open all hour."""
        aggregator, store = build()
        await aggregator.handle(trade(seconds=10, price="100", symbol="QUIET"))

        await aggregator.handle(trade(seconds=70, price="500", symbol="BUSY"))
        await aggregator.flush()

        quiet = store.bar_for("QUIET")
        assert quiet is not None
        assert quiet.is_final is True

    async def test_the_watermark_never_goes_backwards(self) -> None:
        """A straggler must not reopen bars the watermark already closed."""
        aggregator, _ = build()
        await aggregator.handle(trade(seconds=120, price="100"))
        high_water = aggregator.watermark

        await aggregator.handle(trade(seconds=10, price="100", trade_id="straggler"))

        assert aggregator.watermark == high_water

    async def test_an_open_bar_is_written_as_not_final(self) -> None:
        """This is what makes a restart uneventful."""
        aggregator, store = build()
        await aggregator.handle(trade(seconds=10, price="100"))

        await aggregator.flush()

        bar = store.bar_for("AAPL")
        assert bar is not None
        assert bar.is_final is False

    async def test_close_all_finalises_everything_open(self) -> None:
        aggregator, store = build()
        await aggregator.handle(trade(seconds=10, price="100"))

        await aggregator.close_all()

        assert store.bar_for("AAPL").is_final is True  # type: ignore[union-attr]


class TestLateTrades:
    """Failure F11, and the reason the window is bounded."""

    async def test_a_late_trade_inside_the_window_corrects_the_bar(self) -> None:
        aggregator, store = build(lateness_seconds=30)
        await aggregator.handle(trade(seconds=10, price="100", size="10"))
        # Closes the 14:30 bar: watermark is 14:31:05.
        await aggregator.handle(trade(seconds=65, price="101", size="1"))
        await aggregator.flush()
        assert store.bar_for("AAPL").revision == 0  # type: ignore[union-attr]

        # 14:30:50 arrives late, still inside the 30-second window.
        await aggregator.handle(trade(seconds=50, price="105", size="10", trade_id="late"))
        await aggregator.flush()

        bar = store.bar_for("AAPL")
        assert bar is not None
        assert bar.revision == 1, "a correction, not a new bar"
        assert bar.high == Decimal("105")
        assert bar.volume == Decimal("20")
        assert aggregator.stats.corrections == 1

    async def test_a_correction_does_not_rewrite_the_close_with_an_earlier_trade(self) -> None:
        """The close belongs to the last trade by *venue* time, not by arrival."""
        aggregator, store = build(lateness_seconds=30)
        await aggregator.handle(trade(seconds=10, price="100"))
        await aggregator.handle(trade(seconds=55, price="99", trade_id="last"))
        await aggregator.handle(trade(seconds=65, price="101"))
        await aggregator.flush()

        await aggregator.handle(trade(seconds=20, price="104", trade_id="late-middle"))
        await aggregator.flush()

        bar = store.bar_for("AAPL")
        assert bar is not None
        assert bar.close == Decimal("99"), "14:30:55 is still the last trade of the minute"
        assert bar.high == Decimal("104"), "but the late trade is the new high"

    async def test_a_late_trade_earlier_than_the_open_moves_the_open(self) -> None:
        aggregator, store = build(lateness_seconds=30)
        await aggregator.handle(trade(seconds=30, price="100"))
        await aggregator.handle(trade(seconds=65, price="101"))
        await aggregator.flush()

        await aggregator.handle(trade(seconds=5, price="97", trade_id="earliest"))
        await aggregator.flush()

        bar = store.bar_for("AAPL")
        assert bar is not None
        assert bar.open == Decimal("97")

    async def test_a_trade_outside_the_window_is_counted_and_dropped(self) -> None:
        aggregator, store = build(lateness_seconds=5)
        await aggregator.handle(trade(seconds=10, price="100", size="10"))
        # Watermark 14:31:30 — well past 14:31:00 + 5s, so the 14:30 bar is sealed.
        await aggregator.handle(trade(seconds=90, price="101", size="1"))
        await aggregator.flush()
        before = store.bar_for("AAPL")
        assert before is not None

        await aggregator.handle(trade(seconds=50, price="999", size="50", trade_id="way-late"))
        await aggregator.flush()

        after = store.bar_for("AAPL")
        assert after is not None
        assert after.volume == before.volume, "the sealed bar was not touched"
        assert after.high == before.high
        assert aggregator.stats.late_dropped == 1
        assert aggregator.stats.corrections == 0

    async def test_one_symbol_running_ahead_does_not_seal_another_symbols_bars(self) -> None:
        """Found by watching a live replay throw away data.

        Kafka orders a partition, not a topic. Catching up on a backlog reads partitions at
        different rates, so one symbol's trades can be minutes ahead of another's. A single
        global watermark seals the lagging symbol's bars and discards every one of its
        trades as late — with nothing to show for it but a rising counter.
        """
        aggregator, store = build(lateness_seconds=5)

        # BUSY races ahead by five minutes.
        await aggregator.handle(trade(seconds=300, price="500", symbol="BUSY"))

        # SLOW's first trade is for the very first minute, long "behind" the global mark.
        await aggregator.handle(trade(seconds=10, price="100", size="7", symbol="SLOW"))
        await aggregator.flush()

        assert aggregator.stats.late_dropped == 0, "SLOW's own clock has not passed this bar"
        bar = store.bar_for("SLOW")
        assert bar is not None
        assert bar.volume == Decimal("7")

    async def test_a_symbols_own_progress_still_seals_its_bars(self) -> None:
        """The per-symbol rule must not make the window unbounded."""
        aggregator, _ = build(lateness_seconds=5)
        await aggregator.handle(trade(seconds=10, price="100", symbol="SOLO"))
        await aggregator.handle(trade(seconds=200, price="101", symbol="SOLO"))

        await aggregator.handle(trade(seconds=30, price="999", symbol="SOLO", trade_id="late"))

        assert aggregator.stats.late_dropped == 1

    async def test_the_window_boundary_is_inclusive_of_being_sealed(self) -> None:
        """At exactly bar_end + lateness the bar is sealed. A boundary that drifts by one
        tick is a boundary nobody can reason about."""
        aggregator, _ = build(lateness_seconds=10)
        await aggregator.handle(trade(seconds=10, price="100"))
        # 14:31:10 == 14:31:00 + 10s.
        await aggregator.handle(trade(seconds=70, price="101"))

        await aggregator.handle(trade(seconds=30, price="150", trade_id="on-the-boundary"))

        assert aggregator.stats.late_dropped == 1


class TestRestart:
    """Failure F13: killing the aggregator mid-bar must not cost the minute."""

    async def test_an_open_bar_is_restored_and_completed(self) -> None:
        first, store = build()
        await first.handle(trade(seconds=10, price="100", size="10"))
        await first.handle(trade(seconds=20, price="104", size="10"))
        await first.flush()

        # The process dies here. A new one starts against the same database.
        second = BarAggregator(store, lateness=dt.timedelta(seconds=5))
        restored = await second.restore(["AAPL"])

        assert restored == 1

        await second.handle(trade(seconds=40, price="98", size="10"))
        await second.handle(trade(seconds=65, price="101"))
        await second.flush()

        bar = store.bar_for("AAPL")
        assert bar is not None
        assert bar.is_final is True
        assert bar.open == Decimal("100"), "the pre-restart open survived"
        assert bar.high == Decimal("104"), "the pre-restart high survived"
        assert bar.low == Decimal("98")
        assert bar.volume == Decimal("30"), "all three trades are in it"
        assert bar.trade_count == 3

    async def test_the_restarted_bar_matches_the_uninterrupted_one(self) -> None:
        """The property that actually matters, asserted by comparison."""
        prices = ((10, "100", "10"), (20, "104", "5"), (40, "98", "7"), (50, "101", "3"))

        straight, straight_store = build()
        for seconds, price, size in prices:
            await straight.handle(trade(seconds=seconds, price=price, size=size))
        await straight.handle(trade(seconds=65, price="102"))
        await straight.flush()

        interrupted, interrupted_store = build()
        for seconds, price, size in prices[:2]:
            await interrupted.handle(trade(seconds=seconds, price=price, size=size))
        await interrupted.flush()

        resumed = BarAggregator(interrupted_store, lateness=dt.timedelta(seconds=5))
        await resumed.restore(["AAPL"])
        for seconds, price, size in prices[2:]:
            await resumed.handle(trade(seconds=seconds, price=price, size=size))
        await resumed.handle(trade(seconds=65, price="102"))
        await resumed.flush()

        expected = straight_store.bar_for("AAPL")
        actual = interrupted_store.bar_for("AAPL")
        assert expected is not None and actual is not None
        assert (actual.open, actual.high, actual.low, actual.close) == (
            expected.open,
            expected.high,
            expected.low,
            expected.close,
        )
        assert actual.volume == expected.volume
        assert actual.trade_count == expected.trade_count
        assert actual.vwap == expected.vwap

    async def test_restoring_nothing_is_fine(self) -> None:
        aggregator, _ = build()

        assert await aggregator.restore(["AAPL"]) == 0

    async def test_a_trade_for_an_unseen_closed_bar_never_replaces_it(self) -> None:
        """The bug this guards against destroys data silently.

        A fresh bar has revision 0 and so does the stored one, so the revision-guarded
        upsert would accept a one-trade bar in place of a correct one and nothing would
        report it.
        """
        first, store = build(lateness_seconds=600)
        for seconds, price, size in ((10, "100", "10"), (30, "104", "10"), (50, "99", "10")):
            await first.handle(trade(seconds=seconds, price=price, size=size))
        await first.handle(trade(seconds=65, price="101"))
        await first.flush()

        original = store.bar_for("AAPL")
        assert original is not None and original.volume == Decimal("30")

        # A new process with no memory of that bar receives a late trade for it.
        second = BarAggregator(store, lateness=dt.timedelta(seconds=600))
        await second.handle(trade(seconds=70, price="101"))  # seeds the watermark
        await second.handle(trade(seconds=55, price="103", size="5", trade_id="straggler"))
        await second.flush()

        bar = store.bar_for("AAPL")
        assert bar is not None
        assert bar.volume == Decimal("35"), "the trade was added to the existing bar"
        assert bar.open == Decimal("100"), "the original open survived"
        assert bar.revision == 1
        assert second.stats.reopened_from_store == 1


class TestMemory:
    async def test_old_bars_are_evicted(self) -> None:
        """Memory is bounded by the eviction margin, not by the length of the session.

        The bound is deliberately looser than the lateness window: an evicted bar is
        reloaded from the store if a correction turns up for it, so holding a few extra
        minutes buys fewer queries and risks nothing.
        """
        aggregator, _ = build(lateness_seconds=5)

        for minute in range(30):
            await aggregator.handle(
                trade(seconds=minute * 60 + 10, price="100", trade_id=f"t{minute}")
            )
            await aggregator.flush()

        assert aggregator.tracked <= 4, "a handful of recent minutes, not thirty"

    async def test_a_dirty_bar_is_never_evicted(self) -> None:
        """Eviction must not drop a correction that has not been written."""
        aggregator, store = build(lateness_seconds=5)
        await aggregator.handle(trade(seconds=10, price="100"))
        await aggregator.handle(trade(seconds=200, price="101"))

        assert aggregator.pending >= 1
        await aggregator.flush()
        assert store.bar_for("AAPL") is not None
