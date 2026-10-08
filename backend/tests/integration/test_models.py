"""ORM models against a real PostgreSQL.

The constraints are the interesting part. A CHECK that was never exercised is a CHECK that
might have a typo in it, and the whole argument for putting invariants in the database — that
they hold regardless of which writer produced the row — collapses if nobody verified they
fire.

Each test that expects a violation rolls back afterwards. Inside a savepoint that restores
the session to a usable state, so one expected failure does not poison the rest of the test.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError, StatementError
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.domain.enums import (
    AlertSeverity,
    AssetClass,
    BacktestStatus,
    BarInterval,
    DeploymentStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    UserRole,
)
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import (
    Alert,
    AlertRule,
    AnomalyDetection,
    BacktestRun,
    BacktestTrade,
    Bar,
    EquityPoint,
    Fill,
    Instrument,
    ModelVersion,
    Order,
    Portfolio,
    Position,
    Quote,
    RefreshToken,
    Strategy,
    StrategyDeployment,
    Trade,
    User,
)

pytestmark = pytest.mark.integration

NOW = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


async def expect_violation(session: AsyncSession, fragment: str, row: Any) -> None:
    """Assert that inserting ``row`` breaks the named constraint, then recover.

    Matching on the constraint name is what makes these tests specific: without it, a test
    passes whenever *any* error occurs, including one from a typo in the test itself.
    """
    session.add(row)
    with pytest.raises(IntegrityError) as raised:
        await session.flush()
    assert fragment in str(raised.value), f"expected {fragment!r} in: {raised.value}"
    await session.rollback()


class TestInstrument:
    async def test_round_trips(self, session: AsyncSession) -> None:
        instrument = Instrument(
            id=new_id(),
            symbol="AAPL",
            name="Apple Inc.",
            asset_class=AssetClass.EQUITY,
            exchange="XNAS",
            currency="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("1"),
            provider_metadata={"sector": "technology"},
        )
        session.add(instrument)
        await session.flush()

        stored = await session.get(Instrument, instrument.id)
        assert stored is not None
        assert stored.symbol == "AAPL"
        assert stored.asset_class is AssetClass.EQUITY
        assert stored.tick_size == Decimal("0.01")
        # JSONB round trips as a dict, not a string.
        assert stored.provider_metadata == {"sector": "technology"}
        assert stored.created_at.tzinfo is not None

    async def test_symbol_is_unique(self, session: AsyncSession, instrument: Instrument) -> None:
        duplicate = Instrument(
            id=new_id(),
            symbol=instrument.symbol,
            asset_class=AssetClass.EQUITY,
            exchange="XNAS",
            currency="USD",
            tick_size=Decimal("0.01"),
        )
        await expect_violation(session, "uq_instruments_symbol", duplicate)

    @pytest.mark.parametrize(
        ("field", "value", "constraint"),
        [
            ("tick_size", Decimal("0"), "ck_instruments_tick_size_positive"),
            ("tick_size", Decimal("-0.01"), "ck_instruments_tick_size_positive"),
            ("lot_size", Decimal("0"), "ck_instruments_lot_size_positive"),
        ],
    )
    async def test_rejects_non_positive_sizes(
        self, session: AsyncSession, field: str, value: Decimal, constraint: str
    ) -> None:
        values: dict[str, Any] = {
            "id": new_id(),
            "symbol": "BADSIZE",
            "asset_class": AssetClass.EQUITY,
            "exchange": "XNAS",
            "currency": "USD",
            "tick_size": Decimal("0.01"),
            "lot_size": Decimal("1"),
            field: value,
        }
        await expect_violation(session, constraint, Instrument(**values))

    async def test_rejects_a_lowercase_symbol(self, session: AsyncSession) -> None:
        """Symbols are stored uppercase so a lookup never needs to guess which case is stored."""
        await expect_violation(
            session,
            "ck_instruments_symbol_is_upper",
            Instrument(
                id=new_id(),
                symbol="aapl",
                asset_class=AssetClass.EQUITY,
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0.01"),
            ),
        )

    async def test_rejects_a_currency_that_is_not_an_iso_code(self, session: AsyncSession) -> None:
        await expect_violation(
            session,
            "ck_instruments_currency_is_iso_code",
            Instrument(
                id=new_id(),
                symbol="BADCCY",
                asset_class=AssetClass.EQUITY,
                exchange="XNAS",
                currency="US",
                tick_size=Decimal("0.01"),
            ),
        )

    async def test_rejects_an_asset_class_outside_the_enum(self, session: AsyncSession) -> None:
        """Native enums reject unknown values; a VARCHAR column would have stored the typo.

        SQLAlchemy raises the LookupError while binding parameters and wraps it in a
        StatementError, so the statement never reaches PostgreSQL at all.
        """
        session.add(
            Instrument(
                id=new_id(),
                symbol="BADCLASS",
                asset_class="commodity",
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0.01"),
            )
        )
        with pytest.raises(StatementError) as raised:
            await session.flush()
        assert "not among the defined enum values" in str(raised.value)
        await session.rollback()


class TestTrade:
    async def test_round_trips(self, session: AsyncSession, instrument: Instrument) -> None:
        trade = Trade(
            event_id=new_id(),
            instrument_id=instrument.id,
            price=Decimal("187.42"),
            size=Decimal("100"),
            occurred_at=NOW,
            ingested_at=NOW,
            provider="synthetic",
        )
        session.add(trade)
        await session.flush()

        assert trade.id is not None
        assert trade.recorded_at is not None

    async def test_price_survives_as_an_exact_decimal(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """ADR-0009: money is NUMERIC, so a price that a float cannot represent survives."""
        trade = Trade(
            event_id=new_id(),
            instrument_id=instrument.id,
            price=Decimal("0.10000001"),
            size=Decimal("3"),
            occurred_at=NOW,
            ingested_at=NOW,
            provider="synthetic",
        )
        session.add(trade)
        await session.flush()
        session.expunge(trade)

        stored = await session.get(Trade, trade.id)
        assert stored is not None
        assert stored.price == Decimal("0.10000001")
        assert isinstance(stored.price, Decimal)

    async def test_event_id_is_unique(self, session: AsyncSession, instrument: Instrument) -> None:
        """The idempotency guard for at-least-once Kafka delivery (F7)."""
        event_id = new_id()
        first = Trade(
            event_id=event_id,
            instrument_id=instrument.id,
            price=Decimal("10"),
            size=Decimal("1"),
            occurred_at=NOW,
            ingested_at=NOW,
            provider="synthetic",
        )
        session.add(first)
        await session.flush()

        duplicate = Trade(
            event_id=event_id,
            instrument_id=instrument.id,
            price=Decimal("11"),
            size=Decimal("2"),
            occurred_at=NOW,
            ingested_at=NOW,
            provider="synthetic",
        )
        await expect_violation(session, "uq_trades_event_id", duplicate)

    @pytest.mark.parametrize(
        ("field", "value", "constraint"),
        [
            ("price", Decimal("0"), "ck_trades_price_positive"),
            ("price", Decimal("-1"), "ck_trades_price_positive"),
            ("size", Decimal("0"), "ck_trades_size_positive"),
        ],
    )
    async def test_rejects_invalid_values(
        self,
        session: AsyncSession,
        instrument: Instrument,
        field: str,
        value: Decimal,
        constraint: str,
    ) -> None:
        values: dict[str, Any] = {
            "event_id": new_id(),
            "instrument_id": instrument.id,
            "price": Decimal("10"),
            "size": Decimal("1"),
            "occurred_at": NOW,
            "ingested_at": NOW,
            "provider": "synthetic",
            field: value,
        }
        await expect_violation(session, constraint, Trade(**values))

    async def test_requires_an_existing_instrument(self, session: AsyncSession) -> None:
        await expect_violation(
            session,
            "fk_trades_instrument_id_instruments",
            Trade(
                event_id=new_id(),
                instrument_id=new_id(),
                price=Decimal("10"),
                size=Decimal("1"),
                occurred_at=NOW,
                ingested_at=NOW,
                provider="synthetic",
            ),
        )

    async def test_timestamps_come_back_in_utc(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """A naive timestamp here would be a silent hour-long error twice a year."""
        occurred = dt.datetime(2026, 3, 2, 9, 30, tzinfo=dt.timezone(dt.timedelta(hours=-5)))
        trade = Trade(
            event_id=new_id(),
            instrument_id=instrument.id,
            price=Decimal("10"),
            size=Decimal("1"),
            occurred_at=occurred,
            ingested_at=NOW,
            provider="synthetic",
        )
        session.add(trade)
        await session.flush()
        session.expunge(trade)

        stored = await session.get(Trade, trade.id)
        assert stored is not None
        assert stored.occurred_at.tzinfo is not None
        assert stored.occurred_at == occurred
        assert stored.occurred_at.astimezone(dt.UTC).hour == 14


class TestQuote:
    async def test_accepts_a_crossed_market(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Deliberately not constrained.

        Crossed markets (ask below bid) are real and brief. A constraint forbidding them
        would look tidy and would silently discard the most interesting ticks of the day.
        """
        quote = Quote(
            event_id=new_id(),
            instrument_id=instrument.id,
            bid_price=Decimal("10.05"),
            bid_size=Decimal("100"),
            ask_price=Decimal("10.01"),
            ask_size=Decimal("100"),
            occurred_at=NOW,
            ingested_at=NOW,
            provider="synthetic",
        )
        session.add(quote)
        await session.flush()

        assert quote.id is not None

    async def test_accepts_zero_size(self, session: AsyncSession, instrument: Instrument) -> None:
        """A zero-size side means the book is empty there, which is information, not an error."""
        quote = Quote(
            event_id=new_id(),
            instrument_id=instrument.id,
            bid_price=Decimal("10.00"),
            bid_size=Decimal("0"),
            ask_price=Decimal("10.02"),
            ask_size=Decimal("500"),
            occurred_at=NOW,
            ingested_at=NOW,
            provider="synthetic",
        )
        session.add(quote)
        await session.flush()

        assert quote.bid_size == Decimal("0")

    async def test_rejects_a_negative_size(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        await expect_violation(
            session,
            "ck_quotes_bid_size_not_negative",
            Quote(
                event_id=new_id(),
                instrument_id=instrument.id,
                bid_price=Decimal("10.00"),
                bid_size=Decimal("-1"),
                ask_price=Decimal("10.02"),
                ask_size=Decimal("100"),
                occurred_at=NOW,
                ingested_at=NOW,
                provider="synthetic",
            ),
        )


def _bar(instrument_id: Any, **overrides: Any) -> Bar:
    values: dict[str, Any] = {
        "instrument_id": instrument_id,
        "interval": BarInterval.ONE_MINUTE,
        "bar_start": NOW,
        "open": Decimal("10.00"),
        "high": Decimal("10.50"),
        "low": Decimal("9.80"),
        "close": Decimal("10.20"),
        "volume": Decimal("1000"),
        "trade_count": 42,
    }
    return Bar(**(values | overrides))


class TestBar:
    async def test_round_trips(self, session: AsyncSession, instrument: Instrument) -> None:
        bar = _bar(instrument.id, vwap=Decimal("10.13"))
        session.add(bar)
        await session.flush()

        stored = await session.get(Bar, (instrument.id, BarInterval.ONE_MINUTE, NOW))
        assert stored is not None
        assert stored.close == Decimal("10.20")
        assert stored.is_final is True
        assert stored.revision == 0

    async def test_identity_is_instrument_interval_and_start(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """The composite primary key is what "one bar per symbol per minute" means."""
        session.add(_bar(instrument.id))
        await session.flush()

        await expect_violation(session, "pk_bars", _bar(instrument.id, close=Decimal("10.30")))

    async def test_the_same_minute_at_a_different_interval_is_a_different_bar(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        session.add(_bar(instrument.id, interval=BarInterval.ONE_MINUTE))
        session.add(_bar(instrument.id, interval=BarInterval.FIVE_MINUTES))
        await session.flush()

        result = await session.execute(select(Bar).where(Bar.instrument_id == instrument.id))
        assert len(result.scalars().all()) == 2

    @pytest.mark.parametrize(
        ("overrides", "constraint"),
        [
            ({"open": Decimal("11.00")}, "ck_bars_open_within_range"),
            ({"open": Decimal("9.00")}, "ck_bars_open_within_range"),
            ({"close": Decimal("11.00")}, "ck_bars_close_within_range"),
            ({"volume": Decimal("-1")}, "ck_bars_volume_not_negative"),
            ({"trade_count": -1}, "ck_bars_trade_count_not_negative"),
            ({"revision": -1}, "ck_bars_revision_not_negative"),
        ],
    )
    async def test_rejects_an_incoherent_bar(
        self,
        session: AsyncSession,
        instrument: Instrument,
        overrides: dict[str, Any],
        constraint: str,
    ) -> None:
        """A bar breaking these is not a bar, and a backtest on it produces a confident lie."""
        await expect_violation(session, constraint, _bar(instrument.id, **overrides))

    async def test_rejects_a_bar_whose_high_is_below_its_low(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """The most basic incoherence, and the only one that cannot be isolated.

        ``high < low`` makes the ``[low, high]`` range empty, so open and close are
        necessarily outside it too. PostgreSQL reports whichever of the three coherence
        constraints it evaluates first, and all three are describing the same defect —
        asserting on one specific name here would be asserting on an implementation detail
        of the planner.
        """
        session.add(_bar(instrument.id, high=Decimal("9.00")))
        with pytest.raises(IntegrityError) as raised:
            await session.flush()
        assert any(
            name in str(raised.value)
            for name in (
                "ck_bars_high_not_below_low",
                "ck_bars_open_within_range",
                "ck_bars_close_within_range",
            )
        )
        await session.rollback()

    async def test_a_flat_bar_is_valid(self, session: AsyncSession, instrument: Instrument) -> None:
        """Open == high == low == close happens in thin names and must not be rejected."""
        flat = _bar(
            instrument.id,
            open=Decimal("10.00"),
            high=Decimal("10.00"),
            low=Decimal("10.00"),
            close=Decimal("10.00"),
        )
        session.add(flat)
        await session.flush()

        assert flat.high == flat.low

    async def test_zero_volume_is_valid(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """A minute with no trades still produces a bar, carried forward at the last price."""
        empty = _bar(instrument.id, volume=Decimal("0"), trade_count=0)
        session.add(empty)
        await session.flush()

        assert empty.volume == Decimal("0")


class TestUser:
    async def test_defaults(self, session: AsyncSession) -> None:
        user = User(id=new_id(), email="someone@test.invalid", password_hash="x")
        session.add(user)
        await session.flush()
        session.expunge(user)

        stored = await session.get(User, user.id)
        assert stored is not None
        assert stored.role is UserRole.VIEWER
        assert stored.last_login_at is None

    async def test_email_uniqueness_ignores_case(self, session: AsyncSession) -> None:
        """Two accounts differing only in case are one account to every human involved."""
        session.add(User(id=new_id(), email="Trader@Test.invalid", password_hash="x"))
        await session.flush()

        await expect_violation(
            session,
            "uq_users_email_lower",
            User(id=new_id(), email="trader@test.INVALID", password_hash="x"),
        )

    async def test_the_stored_email_keeps_its_original_case(self, session: AsyncSession) -> None:
        """Case-insensitive matching, case-preserving storage."""
        user = User(id=new_id(), email="Mixed.Case@Test.invalid", password_hash="x")
        session.add(user)
        await session.flush()
        session.expunge(user)

        stored = await session.get(User, user.id)
        assert stored is not None
        assert stored.email == "Mixed.Case@Test.invalid"


class TestRelationships:
    async def test_instrument_to_trades(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        for index in range(3):
            session.add(
                Trade(
                    event_id=new_id(),
                    instrument_id=instrument.id,
                    price=Decimal("10") + index,
                    size=Decimal("1"),
                    occurred_at=NOW + dt.timedelta(seconds=index),
                    ingested_at=NOW,
                    provider="synthetic",
                )
            )
        await session.flush()

        result = await session.execute(select(Trade).where(Trade.instrument_id == instrument.id))
        trades = result.scalars().all()
        assert len(trades) == 3
        assert all(trade.instrument_id == instrument.id for trade in trades)

    async def test_order_to_fills(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        order = Order(
            id=new_id(),
            portfolio_id=portfolio.id,
            instrument_id=instrument.id,
            client_order_id=f"test-{new_id()}",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("100"),
        )
        session.add(order)
        await session.flush()

        session.add_all(
            [
                Fill(
                    id=new_id(),
                    order_id=order.id,
                    price=Decimal("10.00"),
                    quantity=Decimal("60"),
                    filled_at=NOW,
                ),
                Fill(
                    id=new_id(),
                    order_id=order.id,
                    price=Decimal("10.01"),
                    quantity=Decimal("40"),
                    filled_at=NOW,
                ),
            ]
        )
        await session.flush()

        result = await session.execute(select(Fill).where(Fill.order_id == order.id))
        fills = result.scalars().all()
        assert len(fills) == 2
        assert sum(fill.quantity for fill in fills) == Decimal("100")

    async def test_user_owns_strategies_and_portfolios(
        self, session: AsyncSession, user: User, strategy: Strategy, portfolio: Portfolio
    ) -> None:
        loaded = await session.get(User, user.id)
        assert loaded is not None
        assert strategy.user_id == user.id
        assert portfolio.user_id == user.id


class TestCascades:
    async def test_deleting_a_user_removes_everything_they_own(
        self, session: AsyncSession, user: User, strategy: Strategy, portfolio: Portfolio
    ) -> None:
        """Deleting an account means it. No real money, no retention requirement."""
        session.add(
            RefreshToken(
                id=new_id(),
                user_id=user.id,
                token_hash=f"hash-{new_id()}",
                family_id=new_id(),
                expires_at=NOW + dt.timedelta(days=30),
            )
        )
        await session.flush()

        await session.execute(text("DELETE FROM users WHERE id = :id"), {"id": user.id})
        await session.flush()

        for table in ("strategies", "portfolios", "refresh_tokens"):
            result = await session.execute(
                # The table names come from the literal tuple above, not from input.
                text(f"SELECT count(*) FROM {table} WHERE user_id = :id"),  # noqa: S608
                {"id": user.id},
            )
            assert result.scalar_one() == 0, f"{table} was not cascaded"

    async def test_an_instrument_with_market_data_cannot_be_deleted(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Market data is the system of record. A delisted symbol changes status instead."""
        session.add(
            Trade(
                event_id=new_id(),
                instrument_id=instrument.id,
                price=Decimal("10"),
                size=Decimal("1"),
                occurred_at=NOW,
                ingested_at=NOW,
                provider="synthetic",
            )
        )
        await session.flush()

        with pytest.raises(IntegrityError) as raised:
            await session.execute(
                text("DELETE FROM instruments WHERE id = :id"), {"id": instrument.id}
            )
            await session.flush()
        assert "fk_trades_instrument_id_instruments" in str(raised.value)
        await session.rollback()

    async def test_deleting_an_order_removes_its_fills(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        order = Order(
            id=new_id(),
            portfolio_id=portfolio.id,
            instrument_id=instrument.id,
            client_order_id=f"test-{new_id()}",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        session.add(order)
        await session.flush()
        session.add(
            Fill(
                id=new_id(),
                order_id=order.id,
                price=Decimal("10"),
                quantity=Decimal("10"),
                filled_at=NOW,
            )
        )
        await session.flush()

        await session.execute(text("DELETE FROM orders WHERE id = :id"), {"id": order.id})
        await session.flush()

        result = await session.execute(
            text("SELECT count(*) FROM fills WHERE order_id = :id"), {"id": order.id}
        )
        assert result.scalar_one() == 0

    async def test_deleting_a_user_nulls_their_audit_entries(
        self, session: AsyncSession, user: User
    ) -> None:
        """An audit trail outlives the account it describes; that is the point of it."""
        await session.execute(
            text(
                "INSERT INTO audit_log (id, user_id, action, resource_type) "
                "VALUES (:id, :user_id, 'login', 'session')"
            ),
            {"id": new_id(), "user_id": user.id},
        )
        await session.execute(text("DELETE FROM users WHERE id = :id"), {"id": user.id})
        await session.flush()

        result = await session.execute(text("SELECT count(*) FROM audit_log WHERE user_id IS NULL"))
        assert result.scalar_one() >= 1


class TestOrderConstraints:
    def _order(self, portfolio: Portfolio, instrument: Instrument, **overrides: Any) -> Order:
        values: dict[str, Any] = {
            "id": new_id(),
            "portfolio_id": portfolio.id,
            "instrument_id": instrument.id,
            "client_order_id": f"test-{new_id()}",
            "side": OrderSide.BUY,
            "order_type": OrderType.MARKET,
            "quantity": Decimal("100"),
        }
        return Order(**(values | overrides))

    async def test_client_order_id_is_unique(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        """The idempotency guard on the order path: one signal, one order (F6)."""
        client_order_id = f"test-{new_id()}"
        session.add(self._order(portfolio, instrument, client_order_id=client_order_id))
        await session.flush()

        await expect_violation(
            session,
            "uq_orders_client_order_id",
            self._order(portfolio, instrument, client_order_id=client_order_id),
        )

    async def test_a_limit_order_requires_a_price(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        await expect_violation(
            session,
            "ck_orders_limit_price_matches_order_type",
            self._order(portfolio, instrument, order_type=OrderType.LIMIT, limit_price=None),
        )

    async def test_a_market_order_must_not_carry_a_price(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        """The other direction, which is the half people forget."""
        await expect_violation(
            session,
            "ck_orders_limit_price_matches_order_type",
            self._order(
                portfolio, instrument, order_type=OrderType.MARKET, limit_price=Decimal("10")
            ),
        )

    async def test_a_valid_limit_order_is_accepted(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        order = self._order(
            portfolio, instrument, order_type=OrderType.LIMIT, limit_price=Decimal("9.50")
        )
        session.add(order)
        await session.flush()

        assert order.status is OrderStatus.PENDING

    async def test_rejects_a_non_positive_quantity(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        await expect_violation(
            session,
            "ck_orders_quantity_positive",
            self._order(portfolio, instrument, quantity=Decimal("0")),
        )

    async def test_filled_quantity_cannot_exceed_quantity(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        await expect_violation(
            session,
            "ck_orders_filled_quantity_within_quantity",
            self._order(
                portfolio, instrument, quantity=Decimal("100"), filled_quantity=Decimal("101")
            ),
        )


class TestPosition:
    async def test_is_keyed_by_portfolio_and_instrument(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        session.add(
            Position(
                portfolio_id=portfolio.id,
                instrument_id=instrument.id,
                quantity=Decimal("100"),
                avg_entry_price=Decimal("10"),
                realized_pnl=Decimal("0"),
            )
        )
        await session.flush()

        await expect_violation(
            session,
            "pk_positions",
            Position(
                portfolio_id=portfolio.id,
                instrument_id=instrument.id,
                quantity=Decimal("50"),
                avg_entry_price=Decimal("11"),
                realized_pnl=Decimal("0"),
            ),
        )

    async def test_allows_a_negative_quantity(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        """Short selling is deferred (D16). A schema forbidding it would need a migration."""
        position = Position(
            portfolio_id=portfolio.id,
            instrument_id=instrument.id,
            quantity=Decimal("-100"),
            avg_entry_price=Decimal("10"),
            realized_pnl=Decimal("0"),
        )
        session.add(position)
        await session.flush()

        assert position.quantity < 0


class TestEquityPoint:
    async def test_equity_must_equal_cash_plus_positions(
        self, session: AsyncSession, portfolio: Portfolio
    ) -> None:
        """The one output of this system someone might act on. It has to add up."""
        await expect_violation(
            session,
            "ck_equity_points_equity_is_cash_plus_positions",
            EquityPoint(
                portfolio_id=portfolio.id,
                as_of=NOW,
                cash=Decimal("50000"),
                positions_value=Decimal("25000"),
                total_equity=Decimal("80000"),
            ),
        )

    async def test_accepts_a_consistent_snapshot(
        self, session: AsyncSession, portfolio: Portfolio
    ) -> None:
        point = EquityPoint(
            portfolio_id=portfolio.id,
            as_of=NOW,
            cash=Decimal("50000"),
            positions_value=Decimal("25000"),
            total_equity=Decimal("75000"),
        )
        session.add(point)
        await session.flush()

        assert point.total_equity == Decimal("75000")


class TestDeployment:
    async def test_an_errored_deployment_must_say_why(
        self, session: AsyncSession, strategy: Strategy, portfolio: Portfolio
    ) -> None:
        """Failure F14 isolates a broken strategy — an unexplained stop helps nobody."""
        await expect_violation(
            session,
            "ck_strategy_deployments_errored_has_message",
            StrategyDeployment(
                id=new_id(),
                strategy_id=strategy.id,
                portfolio_id=portfolio.id,
                status=DeploymentStatus.ERRORED,
                error_message=None,
            ),
        )

    async def test_accepts_an_errored_deployment_with_a_message(
        self, session: AsyncSession, strategy: Strategy, portfolio: Portfolio
    ) -> None:
        deployment = StrategyDeployment(
            id=new_id(),
            strategy_id=strategy.id,
            portfolio_id=portfolio.id,
            status=DeploymentStatus.ERRORED,
            error_message="ZeroDivisionError in evaluate()",
        )
        session.add(deployment)
        await session.flush()

        assert deployment.status is DeploymentStatus.ERRORED


class TestStrategyAndPortfolio:
    async def test_strategy_names_are_unique_per_user_not_globally(
        self, session: AsyncSession, user: User, other_user: User
    ) -> None:
        """Two people may both have a strategy called "SMA crossover"."""
        for owner in (user, other_user):
            session.add(
                Strategy(
                    id=new_id(),
                    user_id=owner.id,
                    name="Shared Name",
                    strategy_type="sma_crossover",
                    symbols=["TEST001"],
                )
            )
        await session.flush()

        await expect_violation(
            session,
            "uq_strategies_user_id_name_active",
            Strategy(
                id=new_id(),
                user_id=user.id,
                name="Shared Name",
                strategy_type="rsi_reversion",
                symbols=["TEST001"],
            ),
        )

    async def test_a_strategy_needs_at_least_one_symbol(
        self, session: AsyncSession, user: User
    ) -> None:
        await expect_violation(
            session,
            "ck_strategies_symbols_not_empty",
            Strategy(
                id=new_id(),
                user_id=user.id,
                name="No Symbols",
                strategy_type="sma_crossover",
                symbols=[],
            ),
        )

    async def test_a_portfolio_needs_positive_starting_cash(
        self, session: AsyncSession, user: User
    ) -> None:
        await expect_violation(
            session,
            "ck_portfolios_initial_cash_positive",
            Portfolio(
                id=new_id(),
                user_id=user.id,
                name="Broke",
                base_currency="USD",
                initial_cash=Decimal("0"),
                cash_balance=Decimal("0"),
            ),
        )

    async def test_cash_balance_may_go_negative(self, session: AsyncSession, user: User) -> None:
        """Margin is deferred, but an over-drawn simulated account is a state worth storing.

        Rejecting it at the schema level would hide a bug in the execution simulator instead
        of surfacing it.
        """
        portfolio = Portfolio(
            id=new_id(),
            user_id=user.id,
            name="Overdrawn",
            base_currency="USD",
            initial_cash=Decimal("1000"),
            cash_balance=Decimal("-50"),
        )
        session.add(portfolio)
        await session.flush()

        assert portfolio.cash_balance < 0


class TestBacktest:
    def _run(self, user: User, strategy: Strategy, **overrides: Any) -> BacktestRun:
        values: dict[str, Any] = {
            "id": new_id(),
            "user_id": user.id,
            "strategy_id": strategy.id,
            "symbols": ["TEST001"],
            "start_date": dt.date(2026, 1, 1),
            "end_date": dt.date(2026, 2, 1),
            "initial_cash": Decimal("100000"),
            "execution_config": {"slippage_bps": "2", "fill_model": "next_bar_open"},
        }
        return BacktestRun(**(values | overrides))

    async def test_stores_its_execution_assumptions(
        self, session: AsyncSession, user: User, strategy: Strategy
    ) -> None:
        """§16.5: a result that does not carry its assumptions cannot be reproduced."""
        run = self._run(user, strategy)
        session.add(run)
        await session.flush()
        session.expunge(run)

        stored = await session.get(BacktestRun, run.id)
        assert stored is not None
        assert stored.execution_config["slippage_bps"] == "2"
        assert stored.status is BacktestStatus.QUEUED

    async def test_rejects_a_reversed_date_range(
        self, session: AsyncSession, user: User, strategy: Strategy
    ) -> None:
        await expect_violation(
            session,
            "ck_backtest_runs_date_range_ordered",
            self._run(user, strategy, start_date=dt.date(2026, 2, 1), end_date=dt.date(2026, 1, 1)),
        )

    async def test_a_single_day_range_is_valid(
        self, session: AsyncSession, user: User, strategy: Strategy
    ) -> None:
        run = self._run(
            user, strategy, start_date=dt.date(2026, 1, 5), end_date=dt.date(2026, 1, 5)
        )
        session.add(run)
        await session.flush()

        assert run.start_date == run.end_date

    async def test_a_failed_run_must_say_why(
        self, session: AsyncSession, user: User, strategy: Strategy
    ) -> None:
        await expect_violation(
            session,
            "ck_backtest_runs_failed_has_message",
            self._run(user, strategy, status=BacktestStatus.FAILED, error_message=None),
        )

    async def test_an_open_trade_keeps_null_exit_fields(
        self, session: AsyncSession, user: User, strategy: Strategy, instrument: Instrument
    ) -> None:
        """A trade still open when the window ends is not force-closed at the last price."""
        run = self._run(user, strategy)
        session.add(run)
        await session.flush()

        trade = BacktestTrade(
            id=new_id(),
            backtest_run_id=run.id,
            instrument_id=instrument.id,
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            entry_price=Decimal("10"),
            entry_at=NOW,
        )
        session.add(trade)
        await session.flush()

        assert trade.exit_at is None
        assert trade.pnl is None

    async def test_rejects_a_half_closed_trade(
        self, session: AsyncSession, user: User, strategy: Strategy, instrument: Instrument
    ) -> None:
        run = self._run(user, strategy)
        session.add(run)
        await session.flush()

        await expect_violation(
            session,
            "ck_backtest_trades_exit_fields_consistent",
            BacktestTrade(
                id=new_id(),
                backtest_run_id=run.id,
                instrument_id=instrument.id,
                side=OrderSide.BUY,
                quantity=Decimal("100"),
                entry_price=Decimal("10"),
                entry_at=NOW,
                exit_at=NOW + dt.timedelta(hours=1),
                exit_price=Decimal("11"),
                pnl=None,
            ),
        )


class TestDetection:
    async def test_an_anomaly_stores_the_features_behind_its_score(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """ADR-0008: a detection nobody can explain is a detection nobody should act on."""
        detection = AnomalyDetection(
            id=new_id(),
            event_id=new_id(),
            instrument_id=instrument.id,
            detected_at=NOW,
            window_start=NOW - dt.timedelta(hours=1),
            window_end=NOW,
            anomaly_score=Decimal("4.2"),
            threshold=Decimal("3.5"),
            severity=AlertSeverity.WARNING,
            model_name="baseline_zscore",
            model_version="1",
            features={"volume_z": 5.1, "return": -0.03, "spread": 0.02},
        )
        session.add(detection)
        await session.flush()
        session.expunge(detection)

        stored = await session.get(AnomalyDetection, detection.id)
        assert stored is not None
        assert stored.features["volume_z"] == 5.1
        assert stored.threshold == Decimal("3.5")

    async def test_rejects_a_reversed_window(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        await expect_violation(
            session,
            "ck_anomaly_detections_window_ordered",
            AnomalyDetection(
                id=new_id(),
                event_id=new_id(),
                instrument_id=instrument.id,
                detected_at=NOW,
                window_start=NOW,
                window_end=NOW - dt.timedelta(hours=1),
                anomaly_score=Decimal("4.2"),
                threshold=Decimal("3.5"),
                severity=AlertSeverity.WARNING,
                model_name="baseline_zscore",
                model_version="1",
            ),
        )

    async def test_event_id_must_be_unique(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """ADR-0012: a redelivered `anomaly.detected` must not duplicate a detection."""
        shared_event_id = new_id()
        session.add(
            AnomalyDetection(
                id=new_id(),
                event_id=shared_event_id,
                instrument_id=instrument.id,
                detected_at=NOW,
                window_start=NOW - dt.timedelta(hours=1),
                window_end=NOW,
                anomaly_score=Decimal("4.2"),
                threshold=Decimal("3.5"),
                severity=AlertSeverity.WARNING,
                model_name="baseline_zscore",
                model_version="1",
            )
        )
        await session.flush()

        await expect_violation(
            session,
            "uq_anomaly_detections_event_id",
            AnomalyDetection(
                id=new_id(),
                event_id=shared_event_id,
                instrument_id=instrument.id,
                detected_at=NOW,
                window_start=NOW - dt.timedelta(hours=1),
                window_end=NOW,
                anomaly_score=Decimal("9.9"),
                threshold=Decimal("3.5"),
                severity=AlertSeverity.CRITICAL,
                model_name="baseline_zscore",
                model_version="1",
            ),
        )

    async def test_only_one_model_version_can_be_active(self, session: AsyncSession) -> None:
        """Enforced by a partial unique index, not by whoever writes the activation code."""

        def version(label: str, *, active: bool) -> ModelVersion:
            return ModelVersion(
                id=new_id(),
                model_name="isolation_forest",
                version=label,
                trained_at=NOW,
                training_window_start=NOW - dt.timedelta(days=90),
                training_window_end=NOW,
                artifact_uri=f"file:///models/{label}.joblib",
                is_active=active,
            )

        session.add(version("1", active=True))
        await session.flush()

        await expect_violation(
            session, "uq_model_versions_one_active_per_model", version("2", active=True)
        )

    async def test_an_inactive_version_alongside_an_active_one_is_fine(
        self, session: AsyncSession
    ) -> None:
        for label, active in (("1", True), ("2", False), ("3", False)):
            session.add(
                ModelVersion(
                    id=new_id(),
                    model_name="isolation_forest",
                    version=label,
                    trained_at=NOW,
                    training_window_start=NOW - dt.timedelta(days=90),
                    training_window_end=NOW,
                    artifact_uri=f"file:///models/{label}.joblib",
                    is_active=active,
                )
            )
        await session.flush()

        result = await session.execute(
            select(ModelVersion).where(ModelVersion.model_name == "isolation_forest")
        )
        assert len(result.scalars().all()) == 3


class TestAlerting:
    async def test_alert_deduplication_is_enforced_by_the_schema(
        self, session: AsyncSession, user: User
    ) -> None:
        """Failure F18. In the database, so it survives a restart and a second replica."""
        rule = AlertRule(
            id=new_id(),
            user_id=user.id,
            name="Unusual volume",
            event_types=["anomaly.detected"],
            channels=["in_app"],
        )
        session.add(rule)
        await session.flush()

        def alert() -> Alert:
            return Alert(
                id=new_id(),
                alert_rule_id=rule.id,
                user_id=user.id,
                event_type="anomaly.detected",
                severity=AlertSeverity.WARNING,
                title="Unusual activity in TEST001",
                body="Volume 5 standard deviations above its recent median.",
                dedupe_key="TEST001:2026-03-02T14:30",
            )

        session.add(alert())
        await session.flush()

        await expect_violation(session, "uq_alerts_alert_rule_id_dedupe_key", alert())

    async def test_a_rule_with_no_symbols_means_every_symbol(
        self, session: AsyncSession, user: User
    ) -> None:
        """NULL means "all", which is different from an empty array meaning "none"."""
        rule = AlertRule(
            id=new_id(),
            user_id=user.id,
            name="Everything",
            event_types=["anomaly.detected"],
            symbols=None,
            channels=["in_app"],
        )
        session.add(rule)
        await session.flush()

        assert rule.symbols is None

    async def test_an_empty_symbol_list_is_rejected(
        self, session: AsyncSession, user: User
    ) -> None:
        """A rule matching nothing is a mistake, not a configuration."""
        await expect_violation(
            session,
            "ck_alert_rules_symbols_null_or_not_empty",
            AlertRule(
                id=new_id(),
                user_id=user.id,
                name="Nothing",
                event_types=["anomaly.detected"],
                symbols=[],
                channels=["in_app"],
            ),
        )

    async def test_a_rule_needs_at_least_one_event_type(
        self, session: AsyncSession, user: User
    ) -> None:
        await expect_violation(
            session,
            "ck_alert_rules_event_types_not_empty",
            AlertRule(
                id=new_id(),
                user_id=user.id,
                name="No events",
                event_types=[],
                channels=["in_app"],
            ),
        )


class TestNullability:
    async def test_required_columns_are_required(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """NOT NULL is the cheapest constraint there is and the easiest to forget."""
        await session.execute(text("SAVEPOINT nullability"))
        with pytest.raises((IntegrityError, DBAPIError)):
            await session.execute(
                text(
                    "INSERT INTO trades (event_id, instrument_id, price, size, "
                    "occurred_at, ingested_at, provider) "
                    "VALUES (:event_id, :instrument_id, NULL, 1, now(), now(), 'synthetic')"
                ),
                {"event_id": new_id(), "instrument_id": instrument.id},
            )
        await session.rollback()
