"""The Market Data Processor, against an in-memory store.

No database. The store is a fake implementing the same port PostgreSQL does, so batching,
deduplication accounting, retry behaviour and every failure path run deterministically and
in milliseconds. The tests that need a real database — constraint violations, transaction
rollback, restart idempotency — live in `tests/integration/test_processor_failures.py`,
because those are properties of PostgreSQL rather than of this class.

Most of what follows is failure handling, which is the part a demo never exercises and
production always does.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import uuid
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal

import pytest

from tradewatch.application.ports.market_store import MarketDataStore
from tradewatch.application.processing import (
    MarketDataProcessor,
    StorageError,
    UnprocessableEventError,
)
from tradewatch.domain.entities import NewBar, NewQuote, NewTrade
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import MarketEvent
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.providers.normalization import EventFactory

pytestmark = pytest.mark.unit

MOMENT = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


def factory(moment: dt.datetime = MOMENT) -> EventFactory:
    return EventFactory(source="test", producer="test@1.0", clock=lambda: moment)


def trade(symbol: str = "AAPL", trade_id: str = "t-1", price: str = "100") -> MarketEvent:
    return factory().trade(
        symbol=symbol,
        price=Decimal(price),
        size=Decimal("10"),
        occurred_at=MOMENT,
        provider_trade_id=trade_id,
    )


def quote(symbol: str = "AAPL", bid: str = "99.99") -> MarketEvent:
    return factory().quote(
        symbol=symbol,
        bid_price=Decimal(bid),
        bid_size=Decimal("100"),
        ask_price=Decimal("100.01"),
        ask_size=Decimal("100"),
        occurred_at=MOMENT,
    )


def bar(symbol: str = "AAPL", minute: int = 0) -> MarketEvent:
    return factory().bar(
        symbol=symbol,
        interval=BarInterval.ONE_MINUTE,
        bar_start=MOMENT + dt.timedelta(minutes=minute),
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        volume=Decimal("1000"),
        trade_count=5,
    )


async def stream_of(*events: MarketEvent) -> AsyncIterator[MarketEvent]:
    for event in events:
        yield event


class FakeStore:
    """An in-memory market data store.

    Idempotent like the real one — a row whose key it has already seen is not written
    again — so the processor's duplicate accounting is exercised for real rather than
    assumed.
    """

    def __init__(self) -> None:
        self.trades: dict[uuid.UUID, NewTrade] = {}
        self.quotes: dict[uuid.UUID, NewQuote] = {}
        self.bars: dict[tuple[uuid.UUID, str, dt.datetime], NewBar] = {}
        self.instruments: dict[str, uuid.UUID] = {}
        self.calls: list[str] = []
        #: Raise this on the next write, then clear it. Set by the failure tests.
        self.fail_next: Exception | None = None
        #: Raise this on every write until cleared.
        self.fail_always: Exception | None = None
        self.resolve_failure: Exception | None = None
        self.healthy = True

    def _maybe_fail(self) -> None:
        if self.fail_always is not None:
            raise self.fail_always
        if self.fail_next is not None:
            error, self.fail_next = self.fail_next, None
            raise error

    async def resolve_instrument(self, symbol: str) -> uuid.UUID:
        self.calls.append(f"resolve:{symbol}")
        if self.resolve_failure is not None:
            raise self.resolve_failure
        return self.instruments.setdefault(symbol, new_id())

    async def save_trades(self, trades: Sequence[NewTrade]) -> int:
        self.calls.append(f"save_trades:{len(trades)}")
        self._maybe_fail()
        written = 0
        for row in trades:
            if row.event_id not in self.trades:
                self.trades[row.event_id] = row
                written += 1
        return written

    async def save_quotes(self, quotes: Sequence[NewQuote]) -> int:
        self.calls.append(f"save_quotes:{len(quotes)}")
        self._maybe_fail()
        written = 0
        for row in quotes:
            if row.event_id not in self.quotes:
                self.quotes[row.event_id] = row
                written += 1
        return written

    async def save_bars(self, bars: Sequence[NewBar]) -> int:
        self.calls.append(f"save_bars:{len(bars)}")
        self._maybe_fail()
        written = 0
        for row in bars:
            key = (row.instrument_id, row.interval.value, row.bar_start)
            if key not in self.bars:
                written += 1
            self.bars[key] = row
        return written

    async def health_check(self) -> str | None:
        return None if self.healthy else "unreachable"


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


async def no_sleep(_seconds: float) -> None:
    """Retry backoff without the waiting."""
    return None


def make_processor(store: FakeStore, **kwargs: object) -> MarketDataProcessor:
    defaults: dict[str, object] = {"sleep": no_sleep, "retry_delay": 0.0}
    return MarketDataProcessor(store, **(defaults | kwargs))  # type: ignore[arg-type]


class TestPortConformance:
    def test_the_fake_satisfies_the_port(self, store: FakeStore) -> None:
        """If the fake drifts from the real store, these tests stop meaning anything."""
        assert isinstance(store, MarketDataStore)


class TestPersistence:
    async def test_persists_a_trade(self, store: FakeStore) -> None:
        processor = make_processor(store)

        await processor.run(stream_of(trade()))

        assert len(store.trades) == 1
        assert processor.stats.persisted == 1
        assert processor.stats.received == 1

    async def test_persists_each_event_type_to_its_own_table(self, store: FakeStore) -> None:
        processor = make_processor(store)

        await processor.run(stream_of(trade(), quote(), bar()))

        assert (len(store.trades), len(store.quotes), len(store.bars)) == (1, 1, 1)
        assert processor.stats.persisted == 3

    async def test_maps_event_fields_onto_the_row(self, store: FakeStore) -> None:
        processor = make_processor(store)
        event = trade(price="187.42")

        await processor.run(stream_of(event))

        row = next(iter(store.trades.values()))
        assert row.price == Decimal("187.42")
        assert row.occurred_at == MOMENT
        assert row.provider == "test"
        assert row.provider_trade_id == "t-1"

    async def test_resolves_an_instrument_once_per_symbol(self, store: FakeStore) -> None:
        """A round trip per tick for a value that never changes would be absurd."""
        processor = make_processor(store)

        await processor.run(stream_of(*[trade(trade_id=f"t-{n}") for n in range(10)]))

        assert store.calls.count("resolve:AAPL") == 1

    async def test_resolves_each_symbol_separately(self, store: FakeStore) -> None:
        processor = make_processor(store)

        await processor.run(stream_of(trade(symbol="AAPL"), trade(symbol="MSFT", trade_id="t-2")))

        assert store.calls.count("resolve:AAPL") == 1
        assert store.calls.count("resolve:MSFT") == 1


class TestBatching:
    async def test_buffers_until_the_batch_is_full(self, store: FakeStore) -> None:
        processor = make_processor(store, batch_size=5)

        for index in range(4):
            await processor.handle(trade(trade_id=f"t-{index}"))

        assert processor.pending == 4
        assert store.trades == {}

    async def test_flushes_when_the_batch_fills(self, store: FakeStore) -> None:
        processor = make_processor(store, batch_size=5)

        for index in range(5):
            await processor.handle(trade(trade_id=f"t-{index}"))

        assert processor.pending == 0
        assert len(store.trades) == 5

    async def test_a_batch_is_one_write_per_table(self, store: FakeStore) -> None:
        """The point of batching: not one insert per tick."""
        processor = make_processor(store, batch_size=100)

        await processor.run(stream_of(*[trade(trade_id=f"t-{n}") for n in range(50)]))

        assert store.calls.count("save_trades:50") == 1

    async def test_the_run_flushes_what_is_left(self, store: FakeStore) -> None:
        processor = make_processor(store, batch_size=1000)

        await processor.run(stream_of(*[trade(trade_id=f"t-{n}") for n in range(7)]))

        assert processor.pending == 0
        assert len(store.trades) == 7

    async def test_an_empty_flush_is_a_no_op(self, store: FakeStore) -> None:
        processor = make_processor(store)

        assert await processor.flush() == 0
        assert store.calls == []

    async def test_an_empty_stream_is_a_no_op(self, store: FakeStore) -> None:
        processor = make_processor(store)

        stats = await processor.run(stream_of())

        assert stats.received == 0
        assert stats.persisted == 0

    async def test_stops_at_the_limit(self, store: FakeStore) -> None:
        processor = make_processor(store, batch_size=2)

        await processor.run(stream_of(*[trade(trade_id=f"t-{n}") for n in range(20)]), max_events=6)

        assert processor.stats.received == 6

    async def test_the_limit_counts_this_call_not_the_lifetime(self, store: FakeStore) -> None:
        """A processor that had already handled a backfill must still honour a limit.

        Counting from cumulative stats would leave the budget spent before the stream opened
        — which is a bug this test exists because we shipped once.
        """
        processor = make_processor(store)
        await processor.run(stream_of(*[trade(trade_id=f"warm-{n}") for n in range(10)]))

        await processor.run(stream_of(*[trade(trade_id=f"t-{n}") for n in range(10)]), max_events=4)

        assert processor.stats.received == 14


class TestDuplicates:
    async def test_a_repeated_event_is_counted_not_written_twice(self, store: FakeStore) -> None:
        processor = make_processor(store)
        event = trade()

        await processor.run(stream_of(event, event, event))

        assert len(store.trades) == 1
        assert processor.stats.persisted == 1
        assert processor.stats.duplicates == 2

    async def test_reprocessing_a_stream_writes_nothing_new(self, store: FakeStore) -> None:
        """What a restart produces. Deterministic event ids (ADR-0012) make it a no-op."""
        events = [trade(trade_id=f"t-{n}") for n in range(5)]
        first = make_processor(store)
        await first.run(stream_of(*events))

        second = make_processor(store)
        await second.run(stream_of(*events))

        assert len(store.trades) == 5
        assert second.stats.persisted == 0
        assert second.stats.duplicates == 5

    async def test_the_counters_account_for_every_event(self, store: FakeStore) -> None:
        processor = make_processor(store)
        event = trade()

        await processor.run(stream_of(event, event, trade(trade_id="t-2")))

        assert processor.stats.buffered_or_lost == 0


class TestUnprocessableEvents:
    async def test_an_event_with_no_mapping_is_skipped(self, store: FakeStore) -> None:
        """Failure F3. One bad event must not end the run."""

        class UnknownEvent(MarketEvent):
            pass

        unknown = UnknownEvent(
            event_id=new_id(),
            event_type="market.trade",  # type: ignore[arg-type]
            symbol="AAPL",
            occurred_at=MOMENT,
            ingested_at=MOMENT,
            source="test",
            producer="test",
            correlation_id=new_id(),
        )
        processor = make_processor(store)

        await processor.run(stream_of(trade(), unknown, trade(trade_id="t-2")))

        assert processor.stats.unprocessable == 1
        assert processor.stats.persisted == 2, "the good events either side were lost"

    async def test_rejection_is_counted_never_silent(self, store: FakeStore) -> None:
        processor = make_processor(store)

        processor.record_unprocessable(trade(), reason="deserialization failed")

        assert processor.stats.unprocessable == 1

    async def test_a_failure_to_resolve_an_instrument_propagates(self, store: FakeStore) -> None:
        """Resolution failure is a storage problem, not a bad event — it must not be
        misfiled as unprocessable and quietly skipped."""
        store.resolve_failure = StorageError("database is down")
        processor = make_processor(store, max_storage_attempts=1)

        with pytest.raises(StorageError):
            await processor.run(stream_of(trade()))


class TestStorageFailures:
    async def test_a_transient_failure_is_retried(self, store: FakeStore) -> None:
        store.fail_next = StorageError("connection reset")
        processor = make_processor(store, max_storage_attempts=3)

        await processor.run(stream_of(trade()))

        assert processor.stats.storage_retries == 1
        assert processor.stats.persisted == 1

    async def test_the_batch_survives_a_retry(self, store: FakeStore) -> None:
        """Nothing is dropped between attempts (failure F8)."""
        store.fail_next = StorageError("deadlock")
        processor = make_processor(store, max_storage_attempts=3)

        await processor.run(stream_of(*[trade(trade_id=f"t-{n}") for n in range(5)]))

        assert len(store.trades) == 5

    async def test_gives_up_and_raises_after_exhausting_attempts(self, store: FakeStore) -> None:
        store.fail_always = StorageError("database is gone")
        processor = make_processor(store, max_storage_attempts=3)

        with pytest.raises(StorageError, match="database is gone"):
            await processor.run(stream_of(trade()))

        assert processor.stats.storage_retries == 2

    async def test_the_buffer_is_kept_when_storage_gives_up(self, store: FakeStore) -> None:
        """The events are not silently dropped — they are visibly still pending."""
        store.fail_always = StorageError("database is gone")
        processor = make_processor(store, max_storage_attempts=2)

        with pytest.raises(StorageError):
            await processor.run(stream_of(trade(), trade(trade_id="t-2")))

        assert processor.pending == 2
        assert processor.stats.buffered_or_lost == 2

    async def test_a_permanent_failure_is_not_retried(self, store: FakeStore) -> None:
        """An unprocessable batch produces the same rejection forever."""
        store.fail_always = UnprocessableEventError("check constraint violated")
        processor = make_processor(store, max_storage_attempts=5)

        with pytest.raises(UnprocessableEventError):
            await processor.run(stream_of(trade()))

        assert processor.stats.storage_retries == 0

    async def test_recovers_once_the_database_comes_back(self, store: FakeStore) -> None:
        processor = make_processor(store, max_storage_attempts=1, batch_size=1)
        store.fail_next = StorageError("down")

        with pytest.raises(StorageError):
            await processor.handle(trade())

        # The database recovers; the same processor flushes what it kept.
        written = await processor.flush()

        assert written == 1
        assert processor.pending == 0


class TestLifecycle:
    async def test_cancellation_flushes_what_was_accepted(self, store: FakeStore) -> None:
        """Buffered events must not be lost because somebody pressed Ctrl-C."""
        processor = make_processor(store, batch_size=1000)

        async def cancelling() -> AsyncIterator[MarketEvent]:
            yield trade(trade_id="t-1")
            yield trade(trade_id="t-2")
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await processor.run(cancelling())

        assert len(store.trades) == 2
        assert processor.pending == 0

    async def test_a_shutdown_flush_failure_is_logged_not_masked(self, store: FakeStore) -> None:
        """A failure during teardown must not replace the exception that caused it."""
        processor = make_processor(store, batch_size=1000, max_storage_attempts=1)

        async def cancelling() -> AsyncIterator[MarketEvent]:
            yield trade()
            store.fail_always = StorageError("down")
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await processor.run(cancelling())

        assert processor.pending == 1

    async def test_reports_store_health(self, store: FakeStore) -> None:
        processor = make_processor(store)

        assert await processor.health_check() is None

        store.healthy = False
        assert await processor.health_check() == "unreachable"

    @pytest.mark.parametrize(
        "kwargs", [{"batch_size": 0}, {"batch_size": -1}, {"max_storage_attempts": 0}]
    )
    def test_rejects_a_nonsensical_configuration(
        self, store: FakeStore, kwargs: dict[str, int]
    ) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            MarketDataProcessor(store, **kwargs)  # type: ignore[arg-type]
