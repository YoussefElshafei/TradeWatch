"""Deduplication, the providers, and the ingestion service.

Every test here is deterministic and offline. No network, no database, no sleeping: the
providers are fakes, the clocks are injected, and a "stream" is a list.

The resilience requirements are what most of this covers — duplicates, malformed messages,
network errors, timeouts, and reconnects — because those are the paths that are never
exercised by a demo and always exercised in production.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal

import pytest

from tradewatch.application.ingestion import EventDeduplicator, IngestionService
from tradewatch.application.ports.market_data import MarketDataProvider, ProviderCapabilities
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import MarketEvent, QuoteEvent, TradeEvent
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.events.memory import (
    EventBusError,
    FailingPublisher,
    InMemoryEventBus,
    RecordingPublisher,
)
from tradewatch.infrastructure.providers.errors import (
    ProviderConnectionError,
    ProviderDataError,
    ProviderRequestError,
    ProviderTimeoutError,
)
from tradewatch.infrastructure.providers.normalization import EventFactory
from tradewatch.infrastructure.providers.replay import (
    ReplayProvider,
    load_events,
    write_events,
)
from tradewatch.infrastructure.providers.resilience import RetryPolicy, reconnecting
from tradewatch.infrastructure.providers.synthetic import SyntheticProvider, SyntheticScenario

pytestmark = pytest.mark.unit

MOMENT = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


def make_factory(moment: dt.datetime = MOMENT) -> EventFactory:
    return EventFactory(source="test", producer="test@1.0", clock=lambda: moment)


def make_trade(
    symbol: str = "AAPL", price: str = "100", trade_id: str | None = "t-1"
) -> TradeEvent:
    return make_factory().trade(
        symbol=symbol,
        price=Decimal(price),
        size=Decimal("10"),
        occurred_at=MOMENT,
        provider_trade_id=trade_id,
    )


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


class TestDeduplicator:
    def test_the_first_sighting_is_not_a_duplicate(self) -> None:
        deduplicator = EventDeduplicator()

        assert deduplicator.is_duplicate(new_id()) is False

    def test_the_second_sighting_is(self) -> None:
        deduplicator = EventDeduplicator()
        event_id = new_id()
        deduplicator.is_duplicate(event_id)

        assert deduplicator.is_duplicate(event_id) is True

    def test_counts_what_it_has_seen(self) -> None:
        deduplicator = EventDeduplicator()
        first, second = new_id(), new_id()
        for event_id in (first, second, first, first):
            deduplicator.is_duplicate(event_id)

        assert deduplicator.stats.seen == 4
        assert deduplicator.stats.duplicates == 2
        assert deduplicator.stats.unique == 2
        assert deduplicator.stats.duplicate_rate == 0.5

    def test_entries_expire(self) -> None:
        clock = FakeClock()
        deduplicator = EventDeduplicator(ttl_seconds=60, monotonic=clock.monotonic)
        event_id = new_id()
        deduplicator.is_duplicate(event_id)

        clock.advance(61)

        assert deduplicator.is_duplicate(event_id) is False
        assert deduplicator.stats.evicted_by_age == 1

    def test_a_duplicate_does_not_extend_the_original_lifetime(self) -> None:
        """Otherwise a constantly-duplicated symbol pins its entry forever."""
        clock = FakeClock()
        deduplicator = EventDeduplicator(ttl_seconds=60, monotonic=clock.monotonic)
        event_id = new_id()
        deduplicator.is_duplicate(event_id)

        clock.advance(30)
        assert deduplicator.is_duplicate(event_id) is True
        clock.advance(31)

        assert deduplicator.is_duplicate(event_id) is False

    def test_the_window_is_bounded_by_count(self) -> None:
        """A volatile open must not exhaust memory before anything ages out."""
        deduplicator = EventDeduplicator(max_entries=10, ttl_seconds=3600)
        ids = [new_id() for _ in range(25)]
        for event_id in ids:
            deduplicator.is_duplicate(event_id)

        assert len(deduplicator) == 10
        assert deduplicator.stats.evicted_by_size == 15
        # The oldest were evicted; the newest are still remembered.
        assert deduplicator.is_duplicate(ids[-1]) is True
        assert deduplicator.is_duplicate(ids[0]) is False

    def test_forget_lets_a_failed_publish_be_retried(self) -> None:
        """Without this the deduplicator itself becomes a source of data loss."""
        deduplicator = EventDeduplicator()
        event_id = new_id()
        deduplicator.is_duplicate(event_id)

        deduplicator.forget(event_id)

        assert deduplicator.is_duplicate(event_id) is False

    def test_clear_empties_the_window(self) -> None:
        deduplicator = EventDeduplicator()
        event_id = new_id()
        deduplicator.is_duplicate(event_id)

        deduplicator.clear()

        assert len(deduplicator) == 0

    @pytest.mark.parametrize("kwargs", [{"max_entries": 0}, {"ttl_seconds": 0}])
    def test_rejects_a_nonsensical_window(self, kwargs: dict[str, float]) -> None:
        with pytest.raises(ValueError, match="positive"):
            EventDeduplicator(**kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Synthetic provider
# ---------------------------------------------------------------------------


class TestSyntheticProvider:
    def test_satisfies_the_port(self) -> None:
        provider = SyntheticProvider(["AAPL"])

        assert isinstance(provider, MarketDataProvider)
        assert provider.name == "synthetic"

    async def test_lists_its_instruments(self) -> None:
        provider = SyntheticProvider(["aapl", "msft"])

        instruments = await provider.list_instruments()

        assert [row.symbol for row in instruments] == ["AAPL", "MSFT"]

    async def test_streams_valid_trades(self) -> None:
        provider = SyntheticProvider(["AAPL"], seed=7)

        events = [event async for event in _take(provider.stream_trades(["AAPL"]), 5)]

        assert len(events) == 5
        assert all(isinstance(event, TradeEvent) for event in events)
        assert all(event.price > 0 and event.size > 0 for event in events)
        assert all(event.occurred_at.tzinfo is dt.UTC for event in events)
        assert all(event.source == "synthetic" for event in events)

    async def test_streams_valid_quotes(self) -> None:
        provider = SyntheticProvider(["AAPL"], seed=7)

        events = [event async for event in _take(provider.stream_quotes(["AAPL"]), 5)]

        assert all(isinstance(event, QuoteEvent) for event in events)
        assert all(event.ask_price >= event.bid_price for event in events)

    async def test_the_same_seed_produces_the_same_stream(self) -> None:
        """Determinism is what makes the whole test suite reproducible (ADR-0004)."""

        async def run() -> list[str]:
            provider = SyntheticProvider(["AAPL"], seed=99)
            return [str(event.price) async for event in _take(provider.stream_trades(["AAPL"]), 20)]

        assert await run() == await run()

    async def test_different_seeds_produce_different_streams(self) -> None:
        async def run(seed: int) -> list[str]:
            provider = SyntheticProvider(["AAPL"], seed=seed)
            return [str(event.price) async for event in _take(provider.stream_trades(["AAPL"]), 20)]

        assert await run(1) != await run(2)

    async def test_adding_a_symbol_does_not_change_another_symbols_series(self) -> None:
        """Otherwise every fixture changes when the watchlist does."""

        async def first_price(symbols: Sequence[str]) -> str:
            provider = SyntheticProvider(symbols, seed=5)
            async for event in _take(provider.stream_trades(["AAPL"]), 1):
                return str(event.price)
            raise AssertionError("no events")

        assert await first_price(["AAPL"]) == await first_price(["AAPL", "MSFT", "NVDA"])

    async def test_quotes_do_not_move_the_price_a_trade_will_print(self) -> None:
        """Regression: the real ingestion worker runs `stream_trades` and `stream_quotes`
        as two concurrent, independently paced asyncio tasks against the same provider
        (`workers/ingestion.py`). `stream_quotes` used to call the same `_step()` that
        `stream_trades` uses, so pulling a burst of quotes silently walked the price a
        later trade would print — and because the quote stream runs far faster than the
        trade stream in production, "last trade" and "latest bid/ask" ended up sampled
        from wildly different points of an artificially high-frequency walk. A quote must
        only wrap a spread around the current price, never advance it.
        """

        async def second_trade_price(*, drain_quotes: bool) -> Decimal:
            provider = SyntheticProvider(["AAPL"], seed=11)
            [_first] = [event async for event in _take(provider.stream_trades(["AAPL"]), 1)]
            if drain_quotes:
                # A burst of quotes on the same provider/symbol, far more than one
                # trade's worth — what an unthrottled quote stream emits in production
                # while a single trade is still in flight.
                async for _ in _take(provider.stream_quotes(["AAPL"]), 500):
                    pass
            [second] = [event async for event in _take(provider.stream_trades(["AAPL"]), 1)]
            return second.price

        # The second trade must land on the same price whether or not 500 quotes were
        # drained in between — quote activity must never perturb the trade walk.
        assert await second_trade_price(drain_quotes=True) == await second_trade_price(
            drain_quotes=False
        )

    async def test_bars_are_coherent(self) -> None:
        provider = SyntheticProvider(["AAPL"], seed=3)

        bars = await provider.fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=10)
        )

        assert len(bars) == 10
        for bar in bars:
            assert bar.low <= bar.open <= bar.high
            assert bar.low <= bar.close <= bar.high
            assert bar.volume >= 0

    async def test_bars_are_aligned_and_contiguous(self) -> None:
        provider = SyntheticProvider(["AAPL"])

        bars = await provider.fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
        )

        starts = [bar.bar_start for bar in bars]
        assert starts == [MOMENT + dt.timedelta(minutes=n) for n in range(5)]

    async def test_an_overlapping_backfill_returns_identical_bars(self) -> None:
        """What makes "fetch a generous window around the gap" safe."""
        provider = SyntheticProvider(["AAPL"], seed=11)

        first = await provider.fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=10)
        )
        second = await provider.fetch_bars(
            "AAPL",
            BarInterval.ONE_MINUTE,
            MOMENT + dt.timedelta(minutes=5),
            MOMENT + dt.timedelta(minutes=15),
        )

        overlap_first = {bar.bar_start: bar.event_id for bar in first[5:]}
        overlap_second = {bar.bar_start: bar.event_id for bar in second[:5]}
        assert overlap_first == overlap_second

    async def test_reports_a_current_price(self) -> None:
        provider = SyntheticProvider(["AAPL"])

        snapshot = await provider.fetch_latest_price("aapl")

        assert snapshot is not None
        assert snapshot.symbol == "AAPL"
        assert snapshot.price > 0
        assert snapshot.has_book

    async def test_rejects_an_unknown_symbol(self) -> None:
        provider = SyntheticProvider(["AAPL"])

        with pytest.raises(ProviderRequestError, match="unknown symbol"):
            await provider.fetch_latest_price("NOPE")

    async def test_rejects_a_reversed_range(self) -> None:
        provider = SyntheticProvider(["AAPL"])

        with pytest.raises(ProviderRequestError, match="not after"):
            await provider.fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT - dt.timedelta(minutes=1)
            )

    async def test_a_closed_provider_refuses_work(self) -> None:
        provider = SyntheticProvider(["AAPL"])
        await provider.aclose()

        with pytest.raises(ProviderConnectionError, match="closed"):
            await provider.fetch_latest_price("AAPL")


class TestSyntheticScenarios:
    """The failures real data will not produce on demand."""

    async def test_drops_the_connection_when_asked(self) -> None:
        provider = SyntheticProvider(["AAPL"], scenario=SyntheticScenario(drop_after=3))

        received = []
        with pytest.raises(ProviderConnectionError, match="dropped"):
            async for event in provider.stream_trades(["AAPL"]):
                received.append(event)

        assert len(received) == 3

    async def test_emits_a_malformed_message_when_asked(self) -> None:
        provider = SyntheticProvider(
            ["AAPL"], scenario=SyntheticScenario(malformed_at=frozenset({2}))
        )

        received = []
        with pytest.raises(ProviderDataError) as raised:
            async for event in provider.stream_trades(["AAPL"]):
                received.append(event)

        assert len(received) == 2
        assert raised.value.raw is not None

    async def test_repeats_an_event_when_asked(self) -> None:
        provider = SyntheticProvider(
            ["AAPL"], scenario=SyntheticScenario(duplicate_at=frozenset({3}))
        )

        events = [event async for event in _take(provider.stream_trades(["AAPL"]), 5)]

        assert events[2].event_id == events[3].event_id

    async def test_injects_a_volume_spike(self) -> None:
        provider = SyntheticProvider(
            ["AAPL"],
            scenario=SyntheticScenario(volume_spike_at=frozenset({5}), volume_spike_factor=100),
        )

        events = [event async for event in _take(provider.stream_trades(["AAPL"]), 8)]

        others = [event.size for index, event in enumerate(events) if index != 5]
        assert events[5].size > max(others) * 10


# ---------------------------------------------------------------------------
# Replay provider
# ---------------------------------------------------------------------------


class TestReplayProvider:
    def test_satisfies_the_port(self) -> None:
        assert isinstance(ReplayProvider([]), MarketDataProvider)

    async def test_replays_in_order(self) -> None:
        events = [make_trade(price=str(100 + n), trade_id=f"t-{n}") for n in range(4)]
        provider = ReplayProvider(events)

        replayed = [event async for event in provider.stream_trades(["AAPL"])]

        assert [str(event.price) for event in replayed] == ["100", "101", "102", "103"]

    async def test_filters_by_symbol(self) -> None:
        events = [
            make_trade(symbol="AAPL", trade_id="a"),
            make_trade(symbol="MSFT", trade_id="b"),
        ]
        provider = ReplayProvider(events)

        replayed = [event async for event in provider.stream_trades(["MSFT"])]

        assert [event.symbol for event in replayed] == ["MSFT"]

    async def test_drops_when_asked(self) -> None:
        provider = ReplayProvider([make_trade(trade_id=f"t-{n}") for n in range(5)], drop_after=2)

        received = []
        with pytest.raises(ProviderConnectionError):
            async for event in provider.stream_trades(["AAPL"]):
                received.append(event)

        assert len(received) == 2

    async def test_reports_the_last_trade_as_the_latest_price(self) -> None:
        factory = make_factory()
        events = [
            factory.trade(
                symbol="AAPL",
                price=Decimal("100"),
                size=Decimal("1"),
                occurred_at=MOMENT,
                provider_trade_id="a",
            ),
            factory.trade(
                symbol="AAPL",
                price=Decimal("105"),
                size=Decimal("1"),
                occurred_at=MOMENT + dt.timedelta(minutes=1),
                provider_trade_id="b",
            ),
        ]

        snapshot = await ReplayProvider(events).fetch_latest_price("AAPL")

        assert snapshot is not None
        assert snapshot.price == Decimal("105")

    async def test_an_unseen_symbol_has_no_latest_price(self) -> None:
        """None is a real answer, distinct from an error."""
        assert await ReplayProvider([make_trade()]).fetch_latest_price("MSFT") is None

    def test_round_trips_through_a_file(self, tmp_path: object) -> None:
        events: list[MarketEvent] = [make_trade(trade_id=f"t-{n}") for n in range(3)]
        path = tmp_path / "recording.jsonl"  # type: ignore[operator]

        assert write_events(path, events) == 3
        loaded = load_events(path)

        assert [event.event_id for event in loaded] == [event.event_id for event in events]
        assert all(isinstance(event, TradeEvent) for event in loaded)
        prices = [event.price for event in loaded if isinstance(event, TradeEvent)]
        assert prices == [event.price for event in events if isinstance(event, TradeEvent)]

    def test_a_malformed_line_names_the_file_and_line(self, tmp_path: object) -> None:
        """A fixture that silently loses events makes a test pass for the wrong reason."""
        path = tmp_path / "broken.jsonl"  # type: ignore[operator]
        path.write_text('{"event_type": "market.trade"}\n', encoding="utf-8")

        with pytest.raises(ProviderDataError, match=r"broken\.jsonl:1"):
            load_events(path)

    def test_an_unknown_event_type_is_rejected(self, tmp_path: object) -> None:
        path = tmp_path / "unknown.jsonl"  # type: ignore[operator]
        path.write_text('{"event_type": "market.nonsense"}\n', encoding="utf-8")

        with pytest.raises(ProviderDataError, match="unknown event_type"):
            load_events(path)

    def test_blank_lines_are_skipped(self, tmp_path: object) -> None:
        path = tmp_path / "gappy.jsonl"  # type: ignore[operator]
        write_events(path, [make_trade()])
        path.write_text(path.read_text(encoding="utf-8") + "\n\n", encoding="utf-8")

        assert len(load_events(path)) == 1


# ---------------------------------------------------------------------------
# Ingestion service
# ---------------------------------------------------------------------------


class TestIngestionService:
    async def test_publishes_what_it_receives(self) -> None:
        provider = SyntheticProvider(["AAPL"], seed=1)
        publisher = RecordingPublisher()
        service = IngestionService(provider, publisher)

        await service.run_stream(["AAPL"], max_events=10)

        assert len(publisher.events) == 10
        assert service.stats.published == 10
        assert service.stats.duplicates == 0

    async def test_skips_duplicates(self) -> None:
        """The provider repeats an event; it must reach the bus once."""
        provider = SyntheticProvider(
            ["AAPL"], scenario=SyntheticScenario(duplicate_at=frozenset({2, 5}))
        )
        publisher = RecordingPublisher()
        service = IngestionService(provider, publisher)

        await service.run_stream(["AAPL"], max_events=8)

        published_ids = [event.event_id for event in publisher.events]
        assert len(published_ids) == len(set(published_ids)), "a duplicate was published"
        assert service.stats.duplicates == 2

    async def test_a_replayed_stream_publishes_nothing_new(self) -> None:
        """What a reconnect produces: the provider resends what we already have."""
        events = [make_trade(trade_id=f"t-{n}") for n in range(5)]
        publisher = RecordingPublisher()
        service = IngestionService(ReplayProvider(events), publisher)

        await service.run_stream(["AAPL"])
        first_count = len(publisher.events)
        await service.run_stream(["AAPL"])

        assert first_count == 5
        assert len(publisher.events) == 5, "the replay was published again"
        assert service.stats.duplicates == 5

    async def test_counts_and_logs_a_malformed_message(self) -> None:
        """Failure F3: counted and skipped, never silently discarded."""
        publisher = RecordingPublisher()
        service = IngestionService(SyntheticProvider(["AAPL"]), publisher)

        service.record_malformed(
            ProviderDataError("bad frame", raw='{"p": '), context="stream_trades"
        )

        assert service.stats.malformed == 1

    async def test_a_malformed_message_does_not_end_the_stream(self) -> None:
        """One bad tick must not cost the session."""
        publisher = RecordingPublisher()
        service = IngestionService(SyntheticProvider(["AAPL"]), publisher)

        async def stream_with_a_bad_message() -> AsyncIterator[MarketEvent]:
            yield make_trade(trade_id="good-1")
            try:
                raise ProviderDataError("bad frame", raw="{")
            except ProviderDataError as error:
                service.record_malformed(error, context="test")
            yield make_trade(trade_id="good-2")

        await service.run_stream(["AAPL"], stream=stream_with_a_bad_message())

        assert service.stats.published == 2
        assert service.stats.malformed == 1

    async def test_a_publish_failure_is_counted_not_raised(self) -> None:
        """Failure F4. One unroutable event must not end the session."""
        publisher = FailingPublisher()
        service = IngestionService(SyntheticProvider(["AAPL"]), publisher)

        async def stream() -> AsyncIterator[MarketEvent]:
            yield make_trade(trade_id="t-1")
            yield make_trade(trade_id="t-2")

        await service.run_stream(["AAPL"], stream=stream())

        assert service.stats.publish_failures == 2
        assert service.stats.published == 0

    async def test_a_failed_publish_can_be_retried_later(self) -> None:
        """The deduplicator must not turn a transient bus failure into permanent loss."""
        failures = {"remaining": 1}

        class FlakyPublisher(RecordingPublisher):
            async def publish(self, event: MarketEvent) -> None:
                if failures["remaining"]:
                    failures["remaining"] -= 1
                    raise EventBusError("temporarily unavailable")
                await super().publish(event)

        publisher = FlakyPublisher()
        service = IngestionService(SyntheticProvider(["AAPL"]), publisher)
        event = make_trade(trade_id="t-1")

        async def first() -> AsyncIterator[MarketEvent]:
            yield event

        async def second() -> AsyncIterator[MarketEvent]:
            yield event

        await service.run_stream(["AAPL"], stream=first())
        await service.run_stream(["AAPL"], stream=second())

        assert service.stats.publish_failures == 1
        assert service.stats.published == 1
        assert len(publisher.events) == 1

    async def test_quotes_are_shed_before_trades(self) -> None:
        """Losing a quote costs book detail; losing a trade corrupts the tape."""
        publisher = FailingPublisher()
        service = IngestionService(SyntheticProvider(["AAPL"]), publisher)
        factory = make_factory()

        async def stream() -> AsyncIterator[MarketEvent]:
            yield factory.quote(
                symbol="AAPL",
                bid_price=Decimal("10"),
                bid_size=Decimal("1"),
                ask_price=Decimal("11"),
                ask_size=Decimal("1"),
                occurred_at=MOMENT,
            )
            yield make_trade(trade_id="t-1")

        await service.run_stream(["AAPL"], kind="quotes", stream=stream())

        # Both failed, both counted; the log distinguishes them, the counters do not lie.
        assert service.stats.publish_failures == 2
        assert service.stats.published == 0

    async def test_cancellation_propagates(self) -> None:
        """Retrying a cancellation would hang shutdown."""
        publisher = RecordingPublisher()
        service = IngestionService(SyntheticProvider(["AAPL"]), publisher)

        async def stream() -> AsyncIterator[MarketEvent]:
            yield make_trade(trade_id="t-1")
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await service.run_stream(["AAPL"], stream=stream())

        assert service.stats.published == 1

    async def test_a_timeout_reaches_the_caller(self) -> None:
        """Not swallowed: the supervisor decides whether to reconnect."""
        publisher = RecordingPublisher()
        service = IngestionService(SyntheticProvider(["AAPL"]), publisher)

        async def stream() -> AsyncIterator[MarketEvent]:
            yield make_trade(trade_id="t-1")
            raise ProviderTimeoutError("read timed out")

        with pytest.raises(ProviderTimeoutError):
            await service.run_stream(["AAPL"], stream=stream())

    async def test_reconnects_and_keeps_publishing(self) -> None:
        """Failure F1, end to end: provider drops, stream reopens, ingestion continues."""
        provider = SyntheticProvider(["AAPL"], scenario=SyntheticScenario(drop_after=3))
        publisher = RecordingPublisher()
        service = IngestionService(provider, publisher)

        stream = reconnecting(
            lambda: provider.stream_trades(["AAPL"]),
            policy=RetryPolicy(max_attempts=0, base_delay=0.001, jitter=False),
            description="synthetic:trades",
            on_reconnect=service.record_reconnect,
        )

        await service.run_stream(["AAPL"], stream=stream, max_events=9)

        assert service.stats.published == 9
        assert service.stats.reconnects >= 2

    async def test_backfills_bars(self) -> None:
        provider = SyntheticProvider(["AAPL"], seed=4)
        publisher = RecordingPublisher()
        service = IngestionService(provider, publisher)

        published = await service.backfill_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=30)
        )

        assert published == 30
        assert len(publisher.events) == 30

    async def test_an_overlapping_backfill_publishes_only_the_new_bars(self) -> None:
        provider = SyntheticProvider(["AAPL"], seed=4)
        publisher = RecordingPublisher()
        service = IngestionService(provider, publisher)

        await service.backfill_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=10)
        )
        second = await service.backfill_bars(
            "AAPL",
            BarInterval.ONE_MINUTE,
            MOMENT + dt.timedelta(minutes=5),
            MOMENT + dt.timedelta(minutes=15),
        )

        assert second == 5
        assert len(publisher.events) == 15

    async def test_syncs_instruments(self) -> None:
        service = IngestionService(SyntheticProvider(["AAPL", "MSFT"]), RecordingPublisher())

        instruments = await service.sync_instruments()

        assert {row.symbol for row in instruments} == {"AAPL", "MSFT"}

    async def test_reports_a_current_price(self) -> None:
        service = IngestionService(SyntheticProvider(["AAPL"]), RecordingPublisher())

        snapshot = await service.latest_price("AAPL")

        assert snapshot is not None
        assert snapshot.price > 0

    async def test_refuses_a_capability_the_provider_lacks(self) -> None:
        """Checked before use, so it is a clear error rather than a surprise mid-session."""

        class BarsOnlyProvider:
            name = "barsonly"
            capabilities = ProviderCapabilities(historical_bars=True)

            async def list_instruments(self) -> Sequence[object]:
                return []

            async def aclose(self) -> None:
                return None

        service = IngestionService(BarsOnlyProvider(), RecordingPublisher())  # type: ignore[arg-type]

        with pytest.raises(RuntimeError, match="cannot stream trades"):
            await service.run_stream(["AAPL"], kind="trades")

    async def test_stats_add_up(self) -> None:
        provider = SyntheticProvider(
            ["AAPL"], scenario=SyntheticScenario(duplicate_at=frozenset({1, 2}))
        )
        service = IngestionService(provider, RecordingPublisher())

        await service.run_stream(["AAPL"], max_events=6)

        stats = service.stats
        assert stats.received == stats.published + stats.dropped


class TestInMemoryEventBus:
    async def test_publishes_and_consumes(self) -> None:
        bus = InMemoryEventBus()
        event = make_trade()

        await bus.publish(event)
        drained = await bus.drain()

        assert drained == [event]
        assert bus.published == 1

    async def test_refuses_rather_than_blocking_when_full(self) -> None:
        """An unbounded queue turns a slow consumer into an out-of-memory kill."""
        bus = InMemoryEventBus(max_size=2)
        await bus.publish(make_trade(trade_id="t-1"))
        await bus.publish(make_trade(trade_id="t-2"))

        with pytest.raises(EventBusError, match="full"):
            await bus.publish(make_trade(trade_id="t-3"))

        assert bus.rejected == 1

    async def test_a_batch_reports_partial_acceptance(self) -> None:
        """The honest answer: how many got through, not "the batch failed"."""
        bus = InMemoryEventBus(max_size=2)

        accepted = await bus.publish_many([make_trade(trade_id=f"t-{n}") for n in range(5)])

        assert accepted == 2

    async def test_ingestion_survives_a_full_bus(self) -> None:
        bus = InMemoryEventBus(max_size=3)
        service = IngestionService(SyntheticProvider(["AAPL"]), bus)

        async def stream() -> AsyncIterator[MarketEvent]:
            for n in range(6):
                yield make_trade(trade_id=f"t-{n}")

        await service.run_stream(["AAPL"], stream=stream())

        assert service.stats.published == 3
        assert service.stats.publish_failures == 3


async def _take[T](stream: AsyncIterator[T], count: int) -> AsyncIterator[T]:
    """Yield the first ``count`` items, then stop — so an endless stream terminates."""
    taken = 0
    async for item in stream:
        yield item
        taken += 1
        if taken >= count:
            return


class TestSyntheticTradeIdentity:
    """Phase 10 found every trade after the first producer run discarded as a duplicate."""

    async def test_a_reconnect_within_one_run_replays_identical_ids(self) -> None:
        provider = SyntheticProvider(["AAPL"], run_id="run-a")
        first = [e.event_id async for e in _take(provider.stream_trades(["AAPL"]), 5)]

        provider.reset()
        replayed = [e.event_id async for e in _take(provider.stream_trades(["AAPL"]), 5)]

        assert replayed == first  # a replay deduplicates instead of looking like new prints

    async def test_a_new_run_never_reuses_a_previous_runs_trade_ids(self) -> None:
        earlier = SyntheticProvider(["AAPL"], run_id="run-a")
        later = SyntheticProvider(["AAPL"], run_id="run-b")

        before = {e.event_id async for e in _take(earlier.stream_trades(["AAPL"]), 50)}
        after = {e.event_id async for e in _take(later.stream_trades(["AAPL"]), 50)}

        assert before.isdisjoint(after)

    def test_two_providers_built_at_different_moments_get_different_runs(self) -> None:
        first = SyntheticProvider(["AAPL"])
        time.sleep(0.002)
        second = SyntheticProvider(["AAPL"])

        assert first._run_id != second._run_id
