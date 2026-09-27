"""`PaperTradingService` against a real database: the live half of the backtest/paper
consistency claim, plus the idempotency and F15 behaviour a pure unit test cannot exercise
(these need real `orders`/`fills`/`positions` rows to prove nothing was duplicated).

Uses the same `_FakeDatabase`-over-the-test-session pattern as
`tests/integration/test_backtest_runner.py`: `PaperTradingService` takes a `SessionSource`
protocol, not the concrete `Database`, so a fake can hand it the test's own transactional
session.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.domain.enums import (
    BarInterval,
    DeploymentStatus,
    OrderStatus,
)
from tradewatch.domain.events import BarEvent, EventType
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import (
    Fill,
    Instrument,
    Order,
    Portfolio,
    Position,
    Strategy,
    StrategyDeployment,
    User,
)
from tradewatch.workers.paper_trading import PaperTradingService

pytestmark = pytest.mark.integration

_START = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)
# The golden fixture from tests/unit/test_backtest_engine.py::TestGoldenBacktest: SMA(2)/
# SMA(3), one long entry at day 3's close, exit at day 5's close.
_CLOSES = [10, 10, 10, 12, 14, 8]


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


def _bar_event(*, symbol: str, day: int, close: float) -> BarEvent:
    bar_start = _START + dt.timedelta(days=day)
    price = Decimal(str(close))
    return BarEvent(
        event_id=uuid.uuid4(),
        event_type=EventType.BAR_CLOSED,
        symbol=symbol,
        occurred_at=bar_start + dt.timedelta(minutes=1),
        ingested_at=bar_start + dt.timedelta(minutes=1),
        source="synthetic",
        producer="test",
        correlation_id=uuid.uuid4(),
        interval=BarInterval.ONE_MINUTE,
        bar_start=bar_start,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal(1000),
        vwap=price,
        trade_count=10,
        revision=0,
    )


async def _make_deployment(
    session: AsyncSession,
    *,
    user: User,
    instrument: Instrument,
    max_orders_per_minute: int = 1000,
) -> tuple[StrategyDeployment, Portfolio]:
    strategy = Strategy(
        id=new_id(),
        user_id=user.id,
        name="Paper trading golden fixture",
        strategy_type="sma_crossover",
        parameters={"fast_period": 2, "slow_period": 3},
        symbols=[instrument.symbol],
    )
    portfolio = Portfolio(
        id=new_id(),
        user_id=user.id,
        name="Paper trading test portfolio",
        base_currency="USD",
        initial_cash=Decimal(12000),
        cash_balance=Decimal(12000),
    )
    session.add_all([strategy, portfolio])
    await session.flush()

    deployment = StrategyDeployment(
        id=new_id(),
        strategy_id=strategy.id,
        portfolio_id=portfolio.id,
        status=DeploymentStatus.RUNNING,
        started_at=_START - dt.timedelta(days=1),
        position_sizing={"target_equity_pct": "1"},
        risk_limits={
            "max_position_pct": "1",
            "max_gross_exposure_pct": "1",
            "max_orders_per_minute": max_orders_per_minute,
        },
        execution_config={
            "slippage_bps": "0",
            "commission_per_share": "0",
            "commission_pct": "0",
            "fill_model": "current_close",
            "allow_partial_fills": False,
        },
    )
    session.add(deployment)
    await session.flush()
    return deployment, portfolio


class TestGoldenFixtureLive:
    async def test_the_full_bar_sequence_produces_the_same_trade_the_backtest_does(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        _deployment, portfolio = await _make_deployment(session, user=user, instrument=instrument)
        service = PaperTradingService(consumer=None, database=_FakeDatabase(session))  # type: ignore[arg-type]
        await service.warm_up()

        for day, close in enumerate(_CLOSES):
            await service.handle_bar(_bar_event(symbol=instrument.symbol, day=day, close=close))

        orders = (
            (await session.execute(select(Order).where(Order.portfolio_id == portfolio.id)))
            .scalars()
            .all()
        )
        assert len(orders) == 2  # entry (buy) and exit (sell)
        assert {order.status for order in orders} == {OrderStatus.FILLED}

        fills = (await session.execute(select(Fill))).scalars().all()
        fills_by_price = {fill.price: fill for fill in fills}
        assert Decimal(12) in fills_by_price
        assert Decimal(8) in fills_by_price
        assert fills_by_price[Decimal(12)].quantity == Decimal(1000)
        assert fills_by_price[Decimal(8)].quantity == Decimal(1000)

        position = await session.get(Position, (portfolio.id, instrument.id))
        assert position is not None
        assert position.quantity == Decimal(0)
        assert position.realized_pnl == Decimal(-4000)  # (8 - 12) * 1000, per the golden fixture

        await session.refresh(portfolio)
        assert portfolio.cash_balance == Decimal(8000)  # 12000 - 4000 realized loss

    async def test_redelivering_the_same_bar_does_not_duplicate_the_order_or_position(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        _deployment, portfolio = await _make_deployment(session, user=user, instrument=instrument)
        service = PaperTradingService(consumer=None, database=_FakeDatabase(session))  # type: ignore[arg-type]
        await service.warm_up()

        # Warm up to just before the crossover, then redeliver the entry bar twice — exactly
        # what an at-least-once broker does on a redelivery (ARCHITECTURE.md §7.4).
        for day, close in enumerate(_CLOSES[:3]):
            await service.handle_bar(_bar_event(symbol=instrument.symbol, day=day, close=close))
        entry_event = _bar_event(symbol=instrument.symbol, day=3, close=_CLOSES[3])
        await service.handle_bar(entry_event)
        await service.handle_bar(entry_event)  # redelivery of the identical event

        orders = (
            (await session.execute(select(Order).where(Order.portfolio_id == portfolio.id)))
            .scalars()
            .all()
        )
        assert len(orders) == 1

        position = await session.get(Position, (portfolio.id, instrument.id))
        assert position is not None
        assert position.quantity == Decimal(1000)  # not 2000

        await session.refresh(portfolio)
        assert portfolio.cash_balance == Decimal(0)  # spent once, not twice


class TestOrderRateCeiling:
    async def test_a_deployment_that_signals_every_bar_is_throttled_by_f15(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        """A zig-zag price series makes SMA(2)/SMA(3) cross back and forth every bar, so this
        deployment tries to place an order on nearly every one — exactly the "order storm"
        F15 exists to stop. With the ceiling at 2/minute, the 3rd attempt must be rejected,
        not filled.
        """
        deployment, portfolio = await _make_deployment(
            session, user=user, instrument=instrument, max_orders_per_minute=2
        )
        service = PaperTradingService(consumer=None, database=_FakeDatabase(session))  # type: ignore[arg-type]
        await service.warm_up()

        zigzag = [10, 20, 10, 20, 10, 20, 10, 20]
        for day, close in enumerate(zigzag):
            await service.handle_bar(_bar_event(symbol=instrument.symbol, day=day, close=close))

        orders = (
            (await session.execute(select(Order).where(Order.portfolio_id == portfolio.id)))
            .scalars()
            .all()
        )
        assert len(orders) > 2  # the strategy did try to place more than the ceiling allows
        by_status = [order.status for order in orders]
        assert by_status.count(OrderStatus.FILLED) == 2
        rejected = [order for order in orders if order.status is OrderStatus.REJECTED]
        assert rejected
        assert all(order.reason_code == "order_rate_ceiling" for order in rejected)

        await session.refresh(deployment)
        assert deployment.status is DeploymentStatus.RUNNING  # a rejection isn't an error


class TestLifecycleReconciliation:
    async def test_pausing_a_deployment_through_a_different_session_stops_it_being_traded(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        """The deployment lifecycle API runs in a different process (the API server) and a
        different session than this worker. Without reconciling against the database, the
        worker would have no way to learn a deployment it already loaded was paused, and
        would keep trading it forever.
        """
        deployment, portfolio = await _make_deployment(session, user=user, instrument=instrument)
        service = PaperTradingService(consumer=None, database=_FakeDatabase(session))  # type: ignore[arg-type]
        await service.warm_up()

        for day, close in enumerate(_CLOSES[:3]):
            await service.handle_bar(_bar_event(symbol=instrument.symbol, day=day, close=close))

        # Simulate the lifecycle API pausing the deployment, in its own transaction.
        deployment.status = DeploymentStatus.PAUSED
        await session.flush()

        # This bar would otherwise trigger the golden fixture's entry (day 3, close 12).
        await service.handle_bar(_bar_event(symbol=instrument.symbol, day=3, close=_CLOSES[3]))

        orders = (
            (await session.execute(select(Order).where(Order.portfolio_id == portfolio.id)))
            .scalars()
            .all()
        )
        assert orders == []

        await session.refresh(portfolio)
        assert portfolio.cash_balance == Decimal(12000)  # untouched
