"""`scripts/rebuild_positions.py` against a real database.

Uses the same `_FakeDatabase`-over-the-test-session pattern as
`tests/integration/test_backtest_runner.py`: `rebuild_one_portfolio` takes a `SessionSource`
protocol, not the concrete `Database`, precisely so a test can hand it the test's own
transactional session and see its writes without a second, independently-committing
connection.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from decimal import Decimal

import pytest
from scripts.rebuild_positions import SessionSource, rebuild_one_portfolio
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.domain.enums import OrderSide, OrderType
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import Fill, Instrument, Order, Portfolio
from tradewatch.infrastructure.repositories import PositionRepository

pytestmark = pytest.mark.integration

NOW = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


class _FakeDatabase:
    """See `test_backtest_runner.py`'s `_FakeDatabase` for why this exists."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        return self._passthrough()

    @asynccontextmanager
    async def _passthrough(self) -> AsyncIterator[AsyncSession]:
        yield self._session
        await self._session.commit()


async def _fill(
    session: AsyncSession,
    portfolio: Portfolio,
    instrument: Instrument,
    side: OrderSide,
    quantity: Decimal,
) -> None:
    order = Order(
        id=new_id(),
        portfolio_id=portfolio.id,
        instrument_id=instrument.id,
        client_order_id=f"rebuild-job-{new_id()}",
        side=side,
        order_type=OrderType.MARKET,
        quantity=quantity,
        filled_quantity=quantity,
    )
    session.add(order)
    await session.flush()
    session.add(
        Fill(id=new_id(), order_id=order.id, price=Decimal("10"), quantity=quantity, filled_at=NOW)
    )
    await session.flush()


class TestRebuildOnePortfolio:
    async def test_reports_drift_without_correcting_it_by_default(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        await _fill(session, portfolio, instrument, OrderSide.BUY, Decimal("100"))
        repository = PositionRepository(session)
        # Simulate drift: a stored position that fills do not actually justify.
        await repository.upsert(
            portfolio.id,
            instrument.id,
            quantity=Decimal("999"),
            avg_entry_price=Decimal("1"),
            realized_pnl=Decimal("0"),
        )

        database: SessionSource = _FakeDatabase(session)
        drifted = await rebuild_one_portfolio(database, portfolio.id, fix=False)

        assert drifted == 1
        stored = await repository.get_position(portfolio.id, instrument.id)
        assert stored is not None
        assert stored.quantity == Decimal("999")  # unchanged: report-only

    async def test_fix_corrects_stored_positions_to_exactly_match_the_fills(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        await _fill(session, portfolio, instrument, OrderSide.BUY, Decimal("100"))
        await _fill(session, portfolio, instrument, OrderSide.BUY, Decimal("50"))
        await _fill(session, portfolio, instrument, OrderSide.SELL, Decimal("30"))
        repository = PositionRepository(session)
        await repository.upsert(
            portfolio.id,
            instrument.id,
            quantity=Decimal("999"),
            avg_entry_price=Decimal("1"),
            realized_pnl=Decimal("-500"),
        )

        database: SessionSource = _FakeDatabase(session)
        drifted = await rebuild_one_portfolio(database, portfolio.id, fix=True)
        assert drifted == 1

        stored = await repository.get_position(portfolio.id, instrument.id)
        rebuilt = await repository.rebuild_from_fills(portfolio.id)
        assert stored is not None
        assert stored.quantity == rebuilt[instrument.id].quantity == Decimal("120")
        assert stored.avg_entry_price == rebuilt[instrument.id].avg_entry_price == Decimal("10")
        assert stored.realized_pnl == rebuilt[instrument.id].realized_pnl == Decimal("0")

    async def test_a_portfolio_with_no_drift_reports_none(
        self, session: AsyncSession, portfolio: Portfolio, instrument: Instrument
    ) -> None:
        await _fill(session, portfolio, instrument, OrderSide.BUY, Decimal("100"))
        repository = PositionRepository(session)
        await repository.upsert(
            portfolio.id,
            instrument.id,
            quantity=Decimal("100"),
            avg_entry_price=Decimal("10"),
            realized_pnl=Decimal("0"),
        )

        database: SessionSource = _FakeDatabase(session)
        drifted = await rebuild_one_portfolio(database, portfolio.id, fix=True)
        assert drifted == 0
