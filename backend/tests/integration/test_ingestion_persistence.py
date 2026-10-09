"""Ingestion through to PostgreSQL.

The unit tests prove the ingestion service publishes the right events; the repository tests
prove the repositories store what they are given. This proves the join between them: that a
normalized event maps onto the schema, survives the round trip, and — the part that matters
most — that a replayed stream produces no duplicate rows.

That last property spans both layers, which is why it needs a test that spans both. The
in-flight deduplicator catches a repeat within its window; the database's
`UNIQUE (event_id)` catches everything else. Neither alone is the guarantee.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.application.ingestion import IngestionService
from tradewatch.domain.entities import NewBar, NewTrade
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import BarEvent, MarketEvent, TradeEvent
from tradewatch.infrastructure.database.models import Bar, Instrument, Trade
from tradewatch.infrastructure.events.memory import RecordingPublisher
from tradewatch.infrastructure.providers.normalization import EventFactory
from tradewatch.infrastructure.providers.synthetic import SyntheticProvider, SyntheticScenario
from tradewatch.infrastructure.repositories import BarRepository, TradeRepository

pytestmark = pytest.mark.integration

MOMENT = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


def to_new_trade(event: TradeEvent, instrument_id: object) -> NewTrade:
    """Map a normalized event onto the persistence model.

    This mapping is the processor's job in Phase 2. Doing it here keeps the ingestion
    service free of a database — which is what makes it unit-testable — while still proving
    the two halves fit.
    """
    return NewTrade(
        event_id=event.event_id,
        instrument_id=instrument_id,  # type: ignore[arg-type]
        price=event.price,
        size=event.size,
        occurred_at=event.occurred_at,
        ingested_at=event.ingested_at,
        provider=event.source,
        provider_trade_id=event.provider_trade_id,
    )


def to_new_bar(event: BarEvent, instrument_id: object) -> NewBar:
    return NewBar(
        instrument_id=instrument_id,  # type: ignore[arg-type]
        interval=event.interval,
        bar_start=event.bar_start,
        open=event.open,
        high=event.high,
        low=event.low,
        close=event.close,
        volume=event.volume,
        vwap=event.vwap,
        trade_count=event.trade_count,
        revision=event.revision,
    )


class TestIngestedEventsPersist:
    async def test_a_streamed_trade_becomes_a_row(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        provider = SyntheticProvider([instrument.symbol], seed=13)
        publisher = RecordingPublisher()
        service = IngestionService(provider, publisher)

        await service.run_stream([instrument.symbol], max_events=20)
        written = await TradeRepository(session).bulk_insert(
            [to_new_trade(event, instrument.id) for event in publisher.events]  # type: ignore[arg-type]
        )

        assert written == 20
        result = await session.execute(
            select(func.count()).select_from(Trade).where(Trade.instrument_id == instrument.id)
        )
        assert result.scalar_one() == 20

    async def test_values_survive_the_round_trip_exactly(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """ADR-0009 and §4.4, end to end: exact decimals and aware UTC timestamps.

        The provider's clock is frozen so the query window is deterministic; with the real
        clock the assertion would depend on when the suite happened to run.
        """
        provider = SyntheticProvider(
            [instrument.symbol],
            seed=17,
            factory=EventFactory(source="synthetic", producer="test", clock=lambda: MOMENT),
        )
        publisher = RecordingPublisher()
        service = IngestionService(provider, publisher)
        await service.run_stream([instrument.symbol], max_events=5)

        repository = TradeRepository(session)
        await repository.bulk_insert(
            [to_new_trade(event, instrument.id) for event in publisher.events]  # type: ignore[arg-type]
        )

        stored = await repository.list_range(
            instrument.id, MOMENT - dt.timedelta(days=1), MOMENT + dt.timedelta(days=1)
        )
        by_event_id = {row.event_id: row for row in stored}
        for event in publisher.events:
            assert isinstance(event, TradeEvent)
            row = by_event_id[event.event_id]
            assert row.price == event.price
            assert isinstance(row.price, Decimal)
            assert row.occurred_at == event.occurred_at
            assert row.occurred_at.tzinfo is not None
            assert row.provider == "synthetic"

    async def test_a_new_producer_run_is_stored_not_discarded(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Phase 10's bug, against the real database: a restarted producer (a new provider
        instance) restarted its trade sequence, collided with every stored event id, and the
        insert discarded its trades as duplicates. Now its prints are new prints."""
        repository = TradeRepository(session)

        async def ingest(run_id: str) -> list[MarketEvent]:
            provider = SyntheticProvider([instrument.symbol], seed=21, run_id=run_id)
            publisher = RecordingPublisher()
            await IngestionService(provider, publisher).run_stream(
                [instrument.symbol], max_events=20
            )
            return publisher.events

        first = await ingest("run-1")
        await repository.bulk_insert([to_new_trade(e, instrument.id) for e in first])  # type: ignore[arg-type]
        written = await repository.bulk_insert(
            [to_new_trade(e, instrument.id) for e in await ingest("run-2")]  # type: ignore[arg-type]
        )

        assert written == 20, "the second run's trades were discarded as duplicates"

    async def test_a_replayed_stream_produces_no_duplicate_rows(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Failures F1 and F7 together: a reconnect replays, and nothing duplicates.

        Deterministic event ids (ADR-0012) are what make this work. With random ids the
        second pass would insert twenty more rows and the tape would be wrong.

        A reconnect is the *same* provider rewinding (`reset()`), not a new one: since Phase
        10 a new provider instance is a new producer run with its own trade ids, so that its
        prints are never mistaken for an earlier run's (see the next test).
        """
        repository = TradeRepository(session)
        provider = SyntheticProvider([instrument.symbol], seed=21)

        async def ingest() -> list[MarketEvent]:
            provider.reset()
            publisher = RecordingPublisher()
            await IngestionService(provider, publisher).run_stream(
                [instrument.symbol], max_events=20
            )
            return publisher.events

        first = await ingest()
        await repository.bulk_insert([to_new_trade(e, instrument.id) for e in first])  # type: ignore[arg-type]
        second = await ingest()
        written = await repository.bulk_insert(
            [to_new_trade(e, instrument.id) for e in second]  # type: ignore[arg-type]
        )

        assert [e.event_id for e in first] == [e.event_id for e in second]
        assert written == 0, "a replayed stream wrote new rows"
        result = await session.execute(
            select(func.count()).select_from(Trade).where(Trade.instrument_id == instrument.id)
        )
        assert result.scalar_one() == 20

    async def test_the_deduplicator_and_the_constraint_agree(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Two layers, same answer.

        The provider repeats events; the deduplicator catches them in flight, and the
        database would have caught them anyway. Both counts must line up.
        """
        provider = SyntheticProvider(
            [instrument.symbol], scenario=SyntheticScenario(duplicate_at=frozenset({2, 4, 6}))
        )
        publisher = RecordingPublisher()
        service = IngestionService(provider, publisher)

        await service.run_stream([instrument.symbol], max_events=10)
        written = await TradeRepository(session).bulk_insert(
            [to_new_trade(e, instrument.id) for e in publisher.events]  # type: ignore[arg-type]
        )

        assert service.stats.duplicates == 3
        assert written == 10, "the deduplicator let a duplicate reach the database"


class TestBackfillPersistence:
    async def test_backfilled_bars_are_stored(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        provider = SyntheticProvider([instrument.symbol], seed=31)
        publisher = RecordingPublisher()
        service = IngestionService(provider, publisher)

        await service.backfill_bars(
            instrument.symbol,
            BarInterval.ONE_MINUTE,
            MOMENT,
            MOMENT + dt.timedelta(minutes=60),
        )
        written = await BarRepository(session).upsert_many(
            [to_new_bar(event, instrument.id) for event in publisher.events]  # type: ignore[arg-type]
        )

        assert written == 60

    async def test_an_overlapping_backfill_does_not_duplicate_history(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Why "fetch a generous window around the gap" is the right recovery (F1)."""
        provider = SyntheticProvider([instrument.symbol], seed=31)
        repository = BarRepository(session)

        first_publisher = RecordingPublisher()
        await IngestionService(provider, first_publisher).backfill_bars(
            instrument.symbol, BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=30)
        )
        await repository.upsert_many(
            [to_new_bar(e, instrument.id) for e in first_publisher.events]  # type: ignore[arg-type]
        )

        second_publisher = RecordingPublisher()
        await IngestionService(provider, second_publisher).backfill_bars(
            instrument.symbol,
            BarInterval.ONE_MINUTE,
            MOMENT + dt.timedelta(minutes=15),
            MOMENT + dt.timedelta(minutes=45),
        )
        await repository.upsert_many(
            [to_new_bar(e, instrument.id) for e in second_publisher.events]  # type: ignore[arg-type]
        )

        result = await session.execute(
            select(func.count()).select_from(Bar).where(Bar.instrument_id == instrument.id)
        )
        assert result.scalar_one() == 45, "the overlap was stored twice"

    async def test_ingested_bars_satisfy_the_database_constraints(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """The generator's bars are not merely plausible; they are storable.

        A synthetic provider that produced a bar with high below low would pass every unit
        test and fail at the first insert.
        """
        provider = SyntheticProvider([instrument.symbol], seed=41)
        publisher = RecordingPublisher()
        await IngestionService(provider, publisher).backfill_bars(
            instrument.symbol, BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(hours=4)
        )

        written = await BarRepository(session).upsert_many(
            [to_new_bar(event, instrument.id) for event in publisher.events]  # type: ignore[arg-type]
        )

        assert written == 240


class TestInstrumentSync:
    async def test_provider_instruments_map_onto_the_schema(self, session: AsyncSession) -> None:
        """The provider's reference data must be storable without translation loss."""
        from tradewatch.infrastructure.repositories import InstrumentRepository

        provider = SyntheticProvider(["SYNCA", "SYNCB"])
        service = IngestionService(provider, RecordingPublisher())
        repository = InstrumentRepository(session)

        snapshots = await service.sync_instruments()
        for snapshot in snapshots:
            await repository.upsert(
                Instrument(
                    symbol=snapshot.symbol,
                    name=snapshot.name,
                    asset_class=snapshot.asset_class,
                    exchange=snapshot.exchange,
                    currency=snapshot.currency,
                    tick_size=snapshot.tick_size,
                    lot_size=snapshot.lot_size,
                    provider_metadata=dict(snapshot.provider_metadata),
                )
            )

        assert await repository.get_by_symbol("SYNCA") is not None
        assert await repository.get_by_symbol("SYNCB") is not None

    async def test_resyncing_instruments_is_idempotent(self, session: AsyncSession) -> None:
        """Providers restate their universe on every reconnect."""
        from tradewatch.infrastructure.repositories import InstrumentRepository

        provider = SyntheticProvider(["RESYNC"])
        service = IngestionService(provider, RecordingPublisher())
        repository = InstrumentRepository(session)

        for _ in range(3):
            for snapshot in await service.sync_instruments():
                await repository.upsert(
                    Instrument(
                        symbol=snapshot.symbol,
                        asset_class=snapshot.asset_class,
                        exchange=snapshot.exchange,
                        currency=snapshot.currency,
                        tick_size=snapshot.tick_size,
                        lot_size=snapshot.lot_size,
                    )
                )

        result = await session.execute(
            select(func.count()).select_from(Instrument).where(Instrument.symbol == "RESYNC")
        )
        assert result.scalar_one() == 1
