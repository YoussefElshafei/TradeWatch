"""Repositories against a real PostgreSQL.

Three things matter here beyond ordinary CRUD:

* **Idempotency.** Kafka delivers at least once, so every ingestion method will be called
  twice with the same input. These tests call them twice and assert nothing changed.
* **Ownership.** Every owner-scoped method is tested with a second user, because the
  repository layer is where the IDOR defence lives (`ARCHITECTURE.md` §9.3). A test that
  only ever uses one user proves nothing about isolation.
* **Queries the indexes exist for.** A composite index nobody queries in the order it was
  built for is wasted; `test_queries.py` checks the plans, and these check the results.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.domain.entities import NewBar, NewQuote, NewTrade
from tradewatch.domain.enums import (
    AssetClass,
    BacktestStatus,
    BarInterval,
    DeploymentStatus,
    InstrumentStatus,
    OrderSide,
    OrderType,
)
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import (
    BacktestRun,
    Fill,
    Instrument,
    Order,
    Portfolio,
    Strategy,
    StrategyDeployment,
    User,
)
from tradewatch.infrastructure.repositories import (
    BacktestRunRepository,
    BarRepository,
    DeploymentRepository,
    InstrumentRepository,
    OrderRepository,
    PortfolioRepository,
    PositionRepository,
    QuoteRepository,
    StrategyRepository,
    TradeRepository,
)

pytestmark = pytest.mark.integration

NOW = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


def make_trade(instrument_id: object, **overrides: object) -> NewTrade:
    values = {
        "event_id": new_id(),
        "instrument_id": instrument_id,
        "price": Decimal("10.00"),
        "size": Decimal("100"),
        "occurred_at": NOW,
        "ingested_at": NOW,
        "provider": "synthetic",
        "provider_trade_id": None,
    }
    return NewTrade(**(values | overrides))  # type: ignore[arg-type]


def make_bar(instrument_id: object, minute: int = 0, **overrides: object) -> NewBar:
    values = {
        "instrument_id": instrument_id,
        "interval": BarInterval.ONE_MINUTE,
        "bar_start": NOW + dt.timedelta(minutes=minute),
        "open": Decimal("10.00"),
        "high": Decimal("10.50"),
        "low": Decimal("9.80"),
        "close": Decimal("10.20"),
        "volume": Decimal("1000"),
        "trade_count": 10,
    }
    return NewBar(**(values | overrides))  # type: ignore[arg-type]


class TestInstrumentRepository:
    async def test_create_and_read_back(self, session: AsyncSession) -> None:
        repository = InstrumentRepository(session)
        instrument = Instrument(
            id=new_id(),
            symbol="CRUD1",
            asset_class=AssetClass.EQUITY,
            exchange="XTST",
            currency="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("1"),
        )

        await repository.add(instrument)
        found = await repository.get_by_symbol("CRUD1")

        assert found is not None
        assert found.id == instrument.id

    async def test_lookup_is_case_insensitive_for_the_caller(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Symbols are stored uppercase; callers should not have to know that."""
        repository = InstrumentRepository(session)

        assert await repository.get_by_symbol(instrument.symbol.lower()) is not None

    async def test_missing_symbol_returns_none(self, session: AsyncSession) -> None:
        assert await InstrumentRepository(session).get_by_symbol("NOSUCH") is None

    async def test_resolves_many_symbols_in_one_query(self, session: AsyncSession) -> None:
        """Avoids the N+1 that turns a fast consumer into a slow one."""
        repository = InstrumentRepository(session)
        for symbol in ("MANY1", "MANY2", "MANY3"):
            await repository.add(
                Instrument(
                    id=new_id(),
                    symbol=symbol,
                    asset_class=AssetClass.EQUITY,
                    exchange="XTST",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                )
            )

        found = await repository.get_many_by_symbol(["many1", "MANY3", "ABSENT"])

        assert set(found) == {"MANY1", "MANY3"}

    async def test_upsert_is_idempotent_and_keeps_the_id_stable(
        self, session: AsyncSession
    ) -> None:
        """Providers restate their instrument list on every reconnect.

        A changing id would orphan every row referencing the instrument.
        """
        repository = InstrumentRepository(session)
        original = Instrument(
            id=new_id(),
            symbol="UPSERT1",
            name="First Name",
            asset_class=AssetClass.EQUITY,
            exchange="XTST",
            currency="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("1"),
        )
        first = await repository.upsert(original)

        updated = Instrument(
            id=new_id(),  # a different id, as a provider re-sync would produce
            symbol="UPSERT1",
            name="Renamed",
            asset_class=AssetClass.ETF,
            exchange="XTST",
            currency="USD",
            tick_size=Decimal("0.05"),
            lot_size=Decimal("1"),
            status=InstrumentStatus.HALTED,
        )
        second = await repository.upsert(updated)

        assert second.id == first.id, "upsert must not change the primary key"
        assert second.name == "Renamed"
        assert second.tick_size == Decimal("0.05")
        assert second.status is InstrumentStatus.HALTED
        assert await repository.count() == 1

    async def test_lists_only_active_instruments(self, session: AsyncSession) -> None:
        repository = InstrumentRepository(session)
        for symbol, status in (
            ("ACT1", InstrumentStatus.ACTIVE),
            ("DEL1", InstrumentStatus.DELISTED),
        ):
            await repository.add(
                Instrument(
                    id=new_id(),
                    symbol=symbol,
                    asset_class=AssetClass.EQUITY,
                    exchange="XTST",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    status=status,
                )
            )

        symbols = {row.symbol for row in await repository.list_active()}

        assert "ACT1" in symbols
        assert "DEL1" not in symbols


class TestTradeRepository:
    async def test_bulk_insert_reports_rows_written(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        repository = TradeRepository(session)

        written = await repository.bulk_insert(
            [make_trade(instrument.id, occurred_at=NOW + dt.timedelta(seconds=i)) for i in range(5)]
        )

        assert written == 5
        assert await repository.count() == 5

    async def test_an_empty_batch_is_a_no_op(self, session: AsyncSession) -> None:
        assert await TradeRepository(session).bulk_insert([]) == 0

    async def test_replaying_a_batch_writes_nothing_new(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Failure F7. This is the whole reason `trades.event_id` is unique.

        A consumer that dies after writing but before committing its Kafka offset replays
        the batch on restart; the replay must be free rather than a duplicate-volume bug.
        """
        repository = TradeRepository(session)
        batch = [
            make_trade(instrument.id, occurred_at=NOW + dt.timedelta(seconds=i)) for i in range(5)
        ]

        first = await repository.bulk_insert(batch)
        second = await repository.bulk_insert(batch)

        assert first == 5
        assert second == 0, "a replayed batch must write nothing"
        assert await repository.count() == 5

    async def test_a_partially_overlapping_batch_writes_only_the_new_rows(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """The realistic case: a consumer restarts mid-batch and resumes from an old offset."""
        repository = TradeRepository(session)
        first_batch = [
            make_trade(instrument.id, occurred_at=NOW + dt.timedelta(seconds=i)) for i in range(5)
        ]
        await repository.bulk_insert(first_batch)

        overlapping = first_batch[3:] + [
            make_trade(instrument.id, occurred_at=NOW + dt.timedelta(seconds=i))
            for i in range(5, 8)
        ]
        written = await repository.bulk_insert(overlapping)

        assert written == 3
        assert await repository.count() == 8

    async def test_range_query_is_half_open(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """``[start, end)`` so consecutive windows tile without double-counting a print."""
        repository = TradeRepository(session)
        await repository.bulk_insert(
            [
                make_trade(instrument.id, occurred_at=NOW + dt.timedelta(minutes=minute))
                for minute in range(5)
            ]
        )

        window = await repository.list_range(
            instrument.id, NOW + dt.timedelta(minutes=1), NOW + dt.timedelta(minutes=3)
        )

        assert len(window) == 2
        starts = {trade.occurred_at for trade in window}
        assert NOW + dt.timedelta(minutes=3) not in starts

    async def test_range_query_returns_newest_first(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        repository = TradeRepository(session)
        await repository.bulk_insert(
            [
                make_trade(instrument.id, occurred_at=NOW + dt.timedelta(minutes=minute))
                for minute in range(5)
            ]
        )

        window = await repository.list_range(instrument.id, NOW, NOW + dt.timedelta(hours=1))

        timestamps = [trade.occurred_at for trade in window]
        assert timestamps == sorted(timestamps, reverse=True)

    async def test_range_query_is_scoped_to_one_instrument(
        self, session: AsyncSession, instrument_factory: object
    ) -> None:
        first: Instrument = instrument_factory()  # type: ignore[operator]
        second: Instrument = instrument_factory()  # type: ignore[operator]
        session.add_all([first, second])
        await session.flush()
        repository = TradeRepository(session)
        await repository.bulk_insert([make_trade(first.id), make_trade(second.id)])

        window = await repository.list_range(first.id, NOW, NOW + dt.timedelta(hours=1))

        assert len(window) == 1
        assert window[0].instrument_id == first.id

    async def test_latest_returns_the_most_recent_print(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        repository = TradeRepository(session)
        await repository.bulk_insert(
            [
                make_trade(
                    instrument.id,
                    occurred_at=NOW + dt.timedelta(minutes=minute),
                    price=Decimal("10") + minute,
                )
                for minute in range(5)
            ]
        )

        latest = await repository.latest(instrument.id)

        assert latest is not None
        assert latest.price == Decimal("14")

    async def test_latest_on_an_untraded_instrument_is_none(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        assert await TradeRepository(session).latest(instrument.id) is None


class TestQuoteRepository:
    async def test_bulk_insert_is_idempotent(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        repository = QuoteRepository(session)
        batch = [
            NewQuote(
                event_id=new_id(),
                instrument_id=instrument.id,
                bid_price=Decimal("9.99"),
                bid_size=Decimal("100"),
                ask_price=Decimal("10.01"),
                ask_size=Decimal("100"),
                occurred_at=NOW + dt.timedelta(seconds=i),
                ingested_at=NOW,
                provider="synthetic",
            )
            for i in range(3)
        ]

        assert await repository.bulk_insert(batch) == 3
        assert await repository.bulk_insert(batch) == 0


class TestBarRepository:
    async def test_upsert_creates_then_replaces(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """A correction must overwrite the bar it corrects, not add a second one."""
        repository = BarRepository(session)
        await repository.upsert_many([make_bar(instrument.id)])

        await repository.upsert_many([make_bar(instrument.id, close=Decimal("10.45"), revision=1)])

        stored = await repository.latest(instrument.id, BarInterval.ONE_MINUTE)
        assert stored is not None
        assert stored.close == Decimal("10.45")
        assert stored.revision == 1
        assert await repository.count() == 1

    async def test_an_older_revision_cannot_overwrite_a_newer_one(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """A backfill job and the live aggregator write the same bar from different directions."""
        repository = BarRepository(session)
        await repository.upsert_many([make_bar(instrument.id, close=Decimal("10.45"), revision=2)])

        await repository.upsert_many([make_bar(instrument.id, close=Decimal("9.99"), revision=1)])

        stored = await repository.latest(instrument.id, BarInterval.ONE_MINUTE)
        assert stored is not None
        assert stored.close == Decimal("10.45"), "a stale revision overwrote a newer bar"
        assert stored.revision == 2

    async def test_the_same_revision_is_applied(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Replay of an identical bar must be harmless, not rejected."""
        repository = BarRepository(session)
        bar = make_bar(instrument.id)

        await repository.upsert_many([bar])
        await repository.upsert_many([bar])

        assert await repository.count() == 1

    async def test_range_query_returns_bars_oldest_first(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """The order a chart draws and a backtest replays — and the order the PK stores."""
        repository = BarRepository(session)
        await repository.upsert_many(
            [make_bar(instrument.id, minute=minute) for minute in range(10)]
        )

        bars = await repository.list_range(
            instrument.id,
            BarInterval.ONE_MINUTE,
            NOW + dt.timedelta(minutes=2),
            NOW + dt.timedelta(minutes=5),
        )

        assert [bar.bar_start for bar in bars] == [
            NOW + dt.timedelta(minutes=minute) for minute in (2, 3, 4)
        ]

    async def test_intervals_do_not_bleed_into_one_another(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        repository = BarRepository(session)
        await repository.upsert_many(
            [
                make_bar(instrument.id),
                make_bar(instrument.id, interval=BarInterval.FIVE_MINUTES),
            ]
        )

        minute_bars = await repository.list_range(
            instrument.id, BarInterval.ONE_MINUTE, NOW, NOW + dt.timedelta(hours=1)
        )

        assert len(minute_bars) == 1
        assert minute_bars[0].interval is BarInterval.ONE_MINUTE

    async def test_finds_a_gap_in_the_series(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """What the Phase 3 backfill uses to work out what to re-fetch after an outage (F1)."""
        repository = BarRepository(session)
        await repository.upsert_many(
            [make_bar(instrument.id, minute=minute) for minute in (0, 1, 2, 7, 8)]
        )

        gaps = await repository.find_gaps(
            instrument.id, BarInterval.ONE_MINUTE, NOW, NOW + dt.timedelta(hours=1)
        )

        assert len(gaps) == 1
        gap_start, gap_end = gaps[0]
        assert gap_start == NOW + dt.timedelta(minutes=3)
        assert gap_end == NOW + dt.timedelta(minutes=7)

    async def test_a_continuous_series_has_no_gaps(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        repository = BarRepository(session)
        await repository.upsert_many(
            [make_bar(instrument.id, minute=minute) for minute in range(10)]
        )

        assert (
            await repository.find_gaps(
                instrument.id, BarInterval.ONE_MINUTE, NOW, NOW + dt.timedelta(hours=1)
            )
            == []
        )


class TestBarAggregation:
    """Rolling 1m bars up to 5m — the query most likely to be subtly wrong."""

    async def test_open_and_close_come_from_the_edges_not_from_min_and_max(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """The bug this test exists to prevent.

        ``min(open)`` looks reasonable and is wrong: a 5-minute bar opens at the price of
        its *first* minute, whatever that price happens to be.
        """
        repository = BarRepository(session)
        # Prices chosen so that first != lowest and last != highest.
        prices = [
            (Decimal("10.00"), Decimal("10.60"), Decimal("9.90"), Decimal("10.50")),
            (Decimal("10.50"), Decimal("10.80"), Decimal("9.50"), Decimal("9.70")),
            (Decimal("9.70"), Decimal("10.20"), Decimal("9.60"), Decimal("10.10")),
            (Decimal("10.10"), Decimal("10.30"), Decimal("10.00"), Decimal("10.25")),
            (Decimal("10.25"), Decimal("10.40"), Decimal("10.05"), Decimal("10.35")),
        ]
        await repository.upsert_many(
            [
                make_bar(
                    instrument.id,
                    minute=minute,
                    open=o,
                    high=h,
                    low=lo,
                    close=c,
                    volume=Decimal("100"),
                    trade_count=5,
                )
                for minute, (o, h, lo, c) in enumerate(prices)
            ]
        )

        rolled = await repository.aggregate(
            instrument.id,
            BarInterval.ONE_MINUTE,
            BarInterval.FIVE_MINUTES,
            NOW,
            NOW + dt.timedelta(hours=1),
        )

        assert len(rolled) == 1
        bar = rolled[0]
        assert bar.open == Decimal("10.00"), "open must be the first minute's open"
        assert bar.close == Decimal("10.35"), "close must be the last minute's close"
        assert bar.high == Decimal("10.80")
        assert bar.low == Decimal("9.50")
        assert bar.volume == Decimal("500")
        assert bar.trade_count == 25
        assert bar.interval is BarInterval.FIVE_MINUTES

    async def test_buckets_align_to_wall_clock_boundaries(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Anchored at the epoch, so 14:30-14:35 is one bucket regardless of where data starts."""
        repository = BarRepository(session)
        await repository.upsert_many(
            [make_bar(instrument.id, minute=minute) for minute in range(12)]
        )

        rolled = await repository.aggregate(
            instrument.id,
            BarInterval.ONE_MINUTE,
            BarInterval.FIVE_MINUTES,
            NOW,
            NOW + dt.timedelta(hours=1),
        )

        assert [bar.bar_start for bar in rolled] == [
            NOW,
            NOW + dt.timedelta(minutes=5),
            NOW + dt.timedelta(minutes=10),
        ]

    async def test_refuses_to_aggregate_downwards(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """You cannot make minutes out of hours."""
        with pytest.raises(ValueError, match="cannot aggregate"):
            await BarRepository(session).aggregate(
                instrument.id,
                BarInterval.ONE_HOUR,
                BarInterval.ONE_MINUTE,
                NOW,
                NOW + dt.timedelta(hours=1),
            )

    async def test_rolled_up_bars_satisfy_the_bar_invariants(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """The result is a NewBar, so the domain validators ran — and it can be stored."""
        repository = BarRepository(session)
        await repository.upsert_many(
            [make_bar(instrument.id, minute=minute) for minute in range(5)]
        )

        rolled = await repository.aggregate(
            instrument.id,
            BarInterval.ONE_MINUTE,
            BarInterval.FIVE_MINUTES,
            NOW,
            NOW + dt.timedelta(hours=1),
        )
        written = await repository.upsert_many(rolled)

        assert written == 1


class TestOwnership:
    """The IDOR defence. Every owner-scoped method, with a second user."""

    async def test_a_user_sees_their_own_strategy(
        self, session: AsyncSession, user: User, strategy: Strategy
    ) -> None:
        found = await StrategyRepository(session).get_for(strategy.id, user.id)

        assert found is not None

    async def test_another_user_gets_nothing(
        self, session: AsyncSession, other_user: User, strategy: Strategy
    ) -> None:
        """Returns None, not the row — and None is also what a missing id returns."""
        found = await StrategyRepository(session).get_for(strategy.id, other_user.id)

        assert found is None

    async def test_a_missing_id_is_indistinguishable_from_someone_elses(
        self, session: AsyncSession, other_user: User, strategy: Strategy
    ) -> None:
        """Both None, so the API answers 404 either way and ids cannot be probed."""
        repository = StrategyRepository(session)

        not_yours = await repository.get_for(strategy.id, other_user.id)
        not_there = await repository.get_for(new_id(), other_user.id)

        assert not_yours is None
        assert not_there is None

    async def test_listing_returns_only_the_actors_rows(
        self, session: AsyncSession, user: User, other_user: User
    ) -> None:
        repository = StrategyRepository(session)
        for owner, name in ((user, "Mine A"), (user, "Mine B"), (other_user, "Theirs")):
            await repository.add(
                Strategy(
                    id=new_id(),
                    user_id=owner.id,
                    name=name,
                    strategy_type="sma_crossover",
                    symbols=["TEST001"],
                )
            )

        mine = await repository.list_for(user.id)

        assert {row.name for row in mine} == {"Mine A", "Mine B"}

    async def test_deleting_someone_elses_row_does_nothing(
        self, session: AsyncSession, other_user: User, strategy: Strategy
    ) -> None:
        repository = StrategyRepository(session)

        deleted = await repository.delete_for(strategy.id, other_user.id)

        assert deleted is False
        assert await session.get(Strategy, strategy.id) is not None

    async def test_deleting_your_own_row_works(
        self, session: AsyncSession, user: User, strategy: Strategy
    ) -> None:
        repository = StrategyRepository(session)

        deleted = await repository.delete_for(strategy.id, user.id)

        assert deleted is True

    async def test_counting_is_scoped_to_the_actor(
        self, session: AsyncSession, user: User, other_user: User, strategy: Strategy
    ) -> None:
        """Backs the per-user resource limits in §9.3."""
        repository = StrategyRepository(session)

        assert await repository.count_for(user.id) == 1
        assert await repository.count_for(other_user.id) == 0

    async def test_portfolio_scoping(
        self, session: AsyncSession, user: User, other_user: User, portfolio: Portfolio
    ) -> None:
        repository = PortfolioRepository(session)

        assert await repository.get_for(portfolio.id, user.id) is not None
        assert await repository.get_for(portfolio.id, other_user.id) is None


class TestStrategyRepository:
    async def test_archiving_hides_a_strategy_without_deleting_it(
        self, session: AsyncSession, user: User, strategy: Strategy
    ) -> None:
        """Backtest results reference their strategy; deleting it orphans the explanation."""
        repository = StrategyRepository(session)

        assert await repository.archive(strategy.id, user.id) is True

        assert await repository.list_active(user.id) == []
        assert await repository.get_for(strategy.id, user.id) is not None

    async def test_archiving_someone_elses_strategy_fails(
        self, session: AsyncSession, other_user: User, strategy: Strategy
    ) -> None:
        assert await StrategyRepository(session).archive(strategy.id, other_user.id) is False

    async def test_lookup_by_name_is_scoped(
        self, session: AsyncSession, user: User, other_user: User, strategy: Strategy
    ) -> None:
        repository = StrategyRepository(session)

        assert await repository.get_by_name(strategy.name, user.id) is not None
        assert await repository.get_by_name(strategy.name, other_user.id) is None


class TestPortfolioRepository:
    async def test_loads_positions_eagerly(
        self, session: AsyncSession, user: User, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        await PositionRepository(session).upsert(
            portfolio.id,
            instrument.id,
            quantity=Decimal("100"),
            avg_entry_price=Decimal("10"),
            realized_pnl=Decimal("0"),
        )

        loaded = await PortfolioRepository(session).get_with_positions(portfolio.id, user.id)

        assert loaded is not None
        # Touching the collection must not need another query.
        assert len(loaded.positions) == 1

    async def test_equity_is_cash_plus_cost_basis(
        self, session: AsyncSession, user: User, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        await PositionRepository(session).upsert(
            portfolio.id,
            instrument.id,
            quantity=Decimal("100"),
            avg_entry_price=Decimal("25"),
            realized_pnl=Decimal("0"),
        )

        equity = await PortfolioRepository(session).total_equity(portfolio.id, user.id)

        assert equity == Decimal("100000") + Decimal("2500")

    async def test_equity_for_someone_elses_portfolio_is_none(
        self, session: AsyncSession, other_user: User, portfolio: Portfolio
    ) -> None:
        assert await PortfolioRepository(session).total_equity(portfolio.id, other_user.id) is None


class TestPositionRepository:
    async def test_upsert_creates_then_updates(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        repository = PositionRepository(session)

        await repository.upsert(
            portfolio.id,
            instrument.id,
            quantity=Decimal("100"),
            avg_entry_price=Decimal("10"),
            realized_pnl=Decimal("0"),
        )
        updated = await repository.upsert(
            portfolio.id,
            instrument.id,
            quantity=Decimal("150"),
            avg_entry_price=Decimal("10.5"),
            realized_pnl=Decimal("25"),
        )

        assert updated.quantity == Decimal("150")
        assert await repository.count() == 1

    async def test_closed_positions_are_kept_but_not_listed_as_open(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        """A closed position keeps its row — realized P&L is history worth keeping."""
        repository = PositionRepository(session)
        await repository.upsert(
            portfolio.id,
            instrument.id,
            quantity=Decimal("0"),
            avg_entry_price=Decimal("10"),
            realized_pnl=Decimal("500"),
        )

        assert await repository.list_open(portfolio.id) == []
        assert await repository.get_position(portfolio.id, instrument.id) is not None

    async def test_get_by_single_id_is_refused(self, session: AsyncSession) -> None:
        """Positions have a composite key; a single-id lookup is a caller's mistake."""
        with pytest.raises(NotImplementedError):
            await PositionRepository(session).get(new_id())

    async def test_positions_can_be_rebuilt_from_fills_alone(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        """`ARCHITECTURE.md` §6.2 requires this, so it is asserted rather than assumed.

        Fills are the truth; the positions table is a projection stored for query speed.
        """
        orders = [
            (OrderSide.BUY, Decimal("100")),
            (OrderSide.BUY, Decimal("50")),
            (OrderSide.SELL, Decimal("30")),
        ]
        for side, quantity in orders:
            order = Order(
                id=new_id(),
                portfolio_id=portfolio.id,
                instrument_id=instrument.id,
                client_order_id=f"rebuild-{new_id()}",
                side=side,
                order_type=OrderType.MARKET,
                quantity=quantity,
                filled_quantity=quantity,
            )
            session.add(order)
            await session.flush()
            session.add(
                Fill(
                    id=new_id(),
                    order_id=order.id,
                    price=Decimal("10"),
                    quantity=quantity,
                    filled_at=NOW,
                )
            )
        await session.flush()

        repository = PositionRepository(session)
        await repository.upsert(
            portfolio.id,
            instrument.id,
            quantity=Decimal("120"),
            avg_entry_price=Decimal("10"),
            realized_pnl=Decimal("0"),
        )

        rebuilt = await repository.rebuild_from_fills(portfolio.id)
        stored = await repository.get_position(portfolio.id, instrument.id)

        assert rebuilt[instrument.id].quantity == Decimal("120")
        assert rebuilt[instrument.id].avg_entry_price == Decimal("10")
        assert rebuilt[instrument.id].realized_pnl == Decimal("0")
        assert stored is not None
        assert stored.quantity == rebuilt[instrument.id].quantity


class TestOrderRepository:
    async def test_client_order_id_lookup_supports_idempotency(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        """Answers "have I already turned this signal into an order?" (failure F6)."""
        repository = OrderRepository(session)
        client_order_id = f"signal-{new_id()}"
        await repository.add(
            Order(
                id=new_id(),
                portfolio_id=portfolio.id,
                instrument_id=instrument.id,
                client_order_id=client_order_id,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("10"),
            )
        )

        assert await repository.get_by_client_order_id(client_order_id) is not None
        assert await repository.get_by_client_order_id("never-issued") is None

    async def test_blotter_is_newest_first(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        repository = OrderRepository(session)
        for index in range(3):
            await repository.add(
                Order(
                    id=new_id(),
                    portfolio_id=portfolio.id,
                    instrument_id=instrument.id,
                    client_order_id=f"blotter-{index}-{new_id()}",
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=Decimal("10"),
                )
            )

        orders = await repository.list_for_portfolio(portfolio.id)

        created = [order.created_at for order in orders]
        assert created == sorted(created, reverse=True)

    async def test_counts_recent_orders_for_the_runaway_guard(
        self,
        session: AsyncSession,
        portfolio: Portfolio,
        instrument: Instrument,
        strategy: Strategy,
    ) -> None:
        """Failure F15: a strategy that signals every bar must not be able to storm."""
        deployment = StrategyDeployment(
            id=new_id(),
            strategy_id=strategy.id,
            portfolio_id=portfolio.id,
            status=DeploymentStatus.RUNNING,
        )
        session.add(deployment)
        await session.flush()

        repository = OrderRepository(session)
        for index in range(4):
            await repository.add(
                Order(
                    id=new_id(),
                    portfolio_id=portfolio.id,
                    instrument_id=instrument.id,
                    deployment_id=deployment.id,
                    client_order_id=f"storm-{index}-{new_id()}",
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=Decimal("1"),
                )
            )

        recent = await repository.count_recent_for_deployment(
            deployment.id, dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1)
        )

        assert recent == 4


class TestDeploymentRepository:
    async def test_lists_only_running_deployments(
        self, session: AsyncSession, strategy: Strategy, portfolio: Portfolio
    ) -> None:
        """The strategy engine's startup query, served by a partial index."""
        for status in (
            DeploymentStatus.RUNNING,
            DeploymentStatus.PAUSED,
            DeploymentStatus.STOPPED,
        ):
            session.add(
                StrategyDeployment(
                    id=new_id(),
                    strategy_id=strategy.id,
                    portfolio_id=portfolio.id,
                    status=status,
                )
            )
        await session.flush()

        running = await DeploymentRepository(session).list_running()

        assert len(running) == 1
        assert running[0].status is DeploymentStatus.RUNNING


class TestBacktestRunRepository:
    def _run(self, user: User, strategy: Strategy, **overrides: object) -> BacktestRun:
        values = {
            "id": new_id(),
            "user_id": user.id,
            "strategy_id": strategy.id,
            "symbols": ["TEST001"],
            "start_date": dt.date(2026, 1, 1),
            "end_date": dt.date(2026, 2, 1),
            "initial_cash": Decimal("100000"),
        }
        return BacktestRun(**(values | overrides))

    async def test_claims_the_oldest_queued_run(
        self, session: AsyncSession, user: User, strategy: Strategy
    ) -> None:
        repository = BacktestRunRepository(session)
        first = self._run(user, strategy)
        session.add(first)
        await session.flush()
        second = self._run(user, strategy)
        session.add(second)
        await session.flush()

        claimed = await repository.claim_next_queued()

        assert claimed is not None
        assert claimed.id == first.id
        assert claimed.status is BacktestStatus.RUNNING

    async def test_claiming_moves_the_run_out_of_the_queue(
        self, session: AsyncSession, user: User, strategy: Strategy
    ) -> None:
        """Otherwise a second worker picks up the same job."""
        repository = BacktestRunRepository(session)
        session.add(self._run(user, strategy))
        await session.flush()

        first = await repository.claim_next_queued()
        second = await repository.claim_next_queued()

        assert first is not None
        assert second is None

    async def test_an_empty_queue_returns_none(self, session: AsyncSession) -> None:
        assert await BacktestRunRepository(session).claim_next_queued() is None

    async def test_counts_only_active_runs_for_the_actor(
        self, session: AsyncSession, user: User, other_user: User, strategy: Strategy
    ) -> None:
        """Enforces TW_MAX_BACKTESTS_PER_USER_CONCURRENT."""
        session.add_all(
            [
                self._run(user, strategy, status=BacktestStatus.QUEUED),
                self._run(user, strategy, status=BacktestStatus.RUNNING),
                self._run(user, strategy, status=BacktestStatus.COMPLETED),
                self._run(
                    user, strategy, status=BacktestStatus.FAILED, error_message="ran out of bars"
                ),
            ]
        )
        await session.flush()

        assert await BacktestRunRepository(session).count_active_for(user.id) == 2
        assert await BacktestRunRepository(session).count_active_for(other_user.id) == 0


class TestTransactionBoundaries:
    async def test_repositories_flush_but_do_not_commit(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """The caller owns the transaction.

        A repository that committed on its own would make it impossible to write an order
        and its fill atomically.
        """
        repository = TradeRepository(session)
        await repository.bulk_insert([make_trade(instrument.id)])

        # Visible inside this transaction...
        assert await repository.count() == 1

        # ...and gone after a rollback, which proves nothing was committed.
        await session.rollback()
        assert await repository.count() == 0

    async def test_a_failed_statement_does_not_leave_partial_data(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """One bad row rejects the whole batch — a half-written batch is worse than none."""
        repository = TradeRepository(session)
        good = make_trade(instrument.id)
        orphan = make_trade(new_id())  # no such instrument

        with pytest.raises(Exception, match="fk_trades_instrument_id_instruments"):
            await repository.bulk_insert([good, orphan])
        await session.rollback()

        result = await session.execute(select(text("count(*)")).select_from(text("trades")))
        assert result.scalar_one() == 0
