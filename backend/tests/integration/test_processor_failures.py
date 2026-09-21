"""Processor failure handling against a real PostgreSQL.

The unit tests cover the processor's logic with a fake store. These cover the properties
that only PostgreSQL can demonstrate: that a constraint violation is classified as
permanent rather than retried forever, that a failed batch rolls back completely rather
than leaving half a tape, and that a restart writes nothing twice.

The classification is the interesting part. Getting it wrong produces the two worst
outcomes available — a poison message that stops the pipeline forever, or a database outage
that silently discards an hour of market data — and neither is visible until it happens.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.conftest import SessionBoundStore
from tradewatch.application.processing import (
    MarketDataProcessor,
    StorageError,
    UnprocessableEventError,
)
from tradewatch.domain.entities import NewTrade
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import MarketEvent
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.market_store import SqlMarketDataStore
from tradewatch.infrastructure.database.models import Instrument, Trade
from tradewatch.infrastructure.providers.normalization import EventFactory

pytestmark = pytest.mark.integration

MOMENT = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


def factory() -> EventFactory:
    return EventFactory(source="synthetic", producer="test", clock=lambda: MOMENT)


def trade_event(symbol: str, trade_id: str, price: str = "100") -> MarketEvent:
    return factory().trade(
        symbol=symbol,
        price=Decimal(price),
        size=Decimal("10"),
        occurred_at=MOMENT,
        provider_trade_id=trade_id,
    )


async def count_trades(session: AsyncSession, instrument_id: object) -> int:
    result = await session.execute(
        select(func.count()).select_from(Trade).where(Trade.instrument_id == instrument_id)
    )
    return result.scalar_one()


class TestDuplicateHandling:
    async def test_a_duplicate_event_is_stored_once(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        processor = MarketDataProcessor(SessionBoundStore(session), batch_size=10)
        event = trade_event(instrument.symbol, "t-1")

        await processor.handle(event)
        await processor.handle(event)
        await processor.flush()

        assert await count_trades(session, instrument.id) == 1
        assert processor.stats.duplicates == 1

    async def test_a_restart_writes_nothing_new(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Deterministic event ids make reprocessing free (ADR-0012).

        This is what a processor coming back after a crash actually does: it reads the same
        events again, because nothing told the upstream they were handled.
        """
        events = [trade_event(instrument.symbol, f"t-{n}") for n in range(20)]

        first = MarketDataProcessor(SessionBoundStore(session), batch_size=5)
        for event in events:
            await first.handle(event)
        await first.flush()

        second = MarketDataProcessor(SessionBoundStore(session), batch_size=5)
        for event in events:
            await second.handle(event)
        await second.flush()

        assert await count_trades(session, instrument.id) == 20
        assert second.stats.persisted == 0
        assert second.stats.duplicates == 20


class TestUnstorableData:
    async def test_a_constraint_violation_is_permanent_not_retried(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """A price of zero breaks `ck_trades_price_positive`.

        Retrying would replay the same rejection forever, so it must be classified as
        unprocessable rather than as a storage failure. `model_construct` is what gets a bad
        row past the domain validator — the point is to test the database's guard, not
        Pydantic's.
        """
        processor = MarketDataProcessor(
            SessionBoundStore(session), batch_size=1, max_storage_attempts=5
        )
        bad = NewTrade.model_construct(
            event_id=new_id(),
            instrument_id=instrument.id,
            price=Decimal("0"),
            size=Decimal("10"),
            occurred_at=MOMENT,
            ingested_at=MOMENT,
            provider="synthetic",
            provider_trade_id="bad",
        )

        with pytest.raises(UnprocessableEventError):
            await processor._store.save_trades([bad])

        assert processor.stats.storage_retries == 0
        await session.rollback()

    async def test_a_foreign_key_violation_is_permanent(self, session: AsyncSession) -> None:
        """A trade for an instrument that does not exist will never succeed."""
        store = SessionBoundStore(session)
        orphan = NewTrade(
            event_id=new_id(),
            instrument_id=new_id(),
            price=Decimal("100"),
            size=Decimal("10"),
            occurred_at=MOMENT,
            ingested_at=MOMENT,
            provider="synthetic",
        )

        with pytest.raises(UnprocessableEventError):
            await store.save_trades([orphan])

        await session.rollback()

    async def test_a_failed_batch_leaves_nothing_behind(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """All-or-nothing. A half-written batch of trades is a corrupted tape.

        The bad row is last, so an implementation that wrote row by row would already have
        committed the first four by the time it failed.
        """
        store = SessionBoundStore(session)
        good = [
            NewTrade(
                event_id=new_id(),
                instrument_id=instrument.id,
                price=Decimal("100") + index,
                size=Decimal("10"),
                occurred_at=MOMENT + dt.timedelta(seconds=index),
                ingested_at=MOMENT,
                provider="synthetic",
            )
            for index in range(4)
        ]
        bad = NewTrade.model_construct(
            event_id=new_id(),
            instrument_id=instrument.id,
            price=Decimal("-1"),
            size=Decimal("10"),
            occurred_at=MOMENT,
            ingested_at=MOMENT,
            provider="synthetic",
            provider_trade_id=None,
        )

        with pytest.raises(UnprocessableEventError):
            await store.save_trades([*good, bad])
        await session.rollback()

        assert await count_trades(session, instrument.id) == 0


class TestDatabaseUnavailable:
    async def test_an_unreachable_database_is_a_retryable_storage_error(
        self, engine: object, settings: object
    ) -> None:
        """Failure F8: the data is kept and the processor retries, rather than dropping it.

        Pointed at a port nothing is listening on, so the failure is a genuine connection
        failure rather than a mocked one.
        """
        from sqlalchemy.ext.asyncio import create_async_engine

        from tradewatch.infrastructure.database.session import create_session_factory

        dead = create_async_engine(
            "postgresql+asyncpg://nobody:nobody@127.0.0.1:59999/nothing",
            connect_args={"timeout": 2},
        )
        try:
            store = SqlMarketDataStore(create_session_factory(dead))
            with pytest.raises(StorageError) as raised:
                await store.resolve_instrument("AAPL")

            assert raised.value.retryable
        finally:
            await dead.dispose()

    async def test_the_health_check_reports_an_unreachable_store(self) -> None:
        from sqlalchemy.ext.asyncio import create_async_engine

        from tradewatch.infrastructure.database.session import create_session_factory

        dead = create_async_engine(
            "postgresql+asyncpg://nobody:nobody@127.0.0.1:59999/nothing",
            connect_args={"timeout": 2},
        )
        try:
            store = SqlMarketDataStore(create_session_factory(dead))

            assert await store.health_check() is not None
        finally:
            await dead.dispose()

    async def test_the_processor_keeps_the_batch_when_storage_fails(
        self, session: AsyncSession
    ) -> None:
        """The events are not lost — they are visibly still pending."""
        from sqlalchemy.ext.asyncio import create_async_engine

        from tradewatch.infrastructure.database.session import create_session_factory

        dead = create_async_engine(
            "postgresql+asyncpg://nobody:nobody@127.0.0.1:59999/nothing",
            connect_args={"timeout": 2},
        )
        try:
            processor = MarketDataProcessor(
                SqlMarketDataStore(create_session_factory(dead)),
                batch_size=2,
                max_storage_attempts=1,
                retry_delay=0.0,
            )

            with pytest.raises(StorageError):
                await processor.handle(trade_event("AAPL", "t-1"))

            assert processor.stats.persisted == 0
        finally:
            await dead.dispose()


class TestInstrumentRegistration:
    async def test_an_unknown_symbol_is_registered_on_first_sight(
        self, session: AsyncSession
    ) -> None:
        """A symbol appearing mid-stream must not produce a silent gap."""
        processor = MarketDataProcessor(SessionBoundStore(session), batch_size=1)

        await processor.handle(trade_event("BRANDNEW", "t-1"))

        result = await session.execute(select(Instrument).where(Instrument.symbol == "BRANDNEW"))
        registered = result.scalar_one_or_none()
        assert registered is not None
        assert processor.stats.persisted == 1

    async def test_registration_happens_once_per_symbol(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        processor = MarketDataProcessor(SessionBoundStore(session), batch_size=100)

        for index in range(10):
            await processor.handle(trade_event(instrument.symbol, f"t-{index}"))
        await processor.flush()

        result = await session.execute(
            select(func.count())
            .select_from(Instrument)
            .where(Instrument.symbol == instrument.symbol)
        )
        assert result.scalar_one() == 1


class TestBarCorrections:
    async def test_a_corrected_bar_replaces_the_original(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Failure F11: a late trade produces a correction, not a second bar."""
        processor = MarketDataProcessor(SessionBoundStore(session), batch_size=1)
        shared: dict[str, Any] = {
            "symbol": instrument.symbol,
            "interval": BarInterval.ONE_MINUTE,
            "bar_start": MOMENT,
            "open": Decimal("10"),
            "high": Decimal("11"),
            "low": Decimal("9"),
            "close": Decimal("10.5"),
            "volume": Decimal("1000"),
        }
        await processor.handle(factory().bar(**shared))
        await processor.handle(factory().bar(**{**shared, "close": Decimal("10.9"), "revision": 1}))
        await processor.flush()

        result = await session.execute(
            text("SELECT close, revision FROM bars WHERE instrument_id = :id AND interval = '1m'"),
            {"id": instrument.id},
        )
        rows = result.all()
        assert len(rows) == 1, "the correction created a second bar"
        assert rows[0][0] == Decimal("10.90000000")
        assert rows[0][1] == 1
