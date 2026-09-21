"""Repositories for user-owned trading data.

Every one of these is owner-scoped: the actor is a required argument, not an optional
filter. See :mod:`tradewatch.infrastructure.repositories.base` for why that lives here
rather than in a route handler.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Sequence
from decimal import Decimal

from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import selectinload

from tradewatch.domain.enums import BacktestStatus, DeploymentStatus, OrderSide
from tradewatch.infrastructure.database.models import (
    BacktestRun,
    Fill,
    Order,
    Portfolio,
    Position,
    Strategy,
    StrategyDeployment,
)
from tradewatch.infrastructure.repositories.base import OwnedRepository, Repository


class StrategyRepository(OwnedRepository[Strategy]):
    """User-owned strategy configurations."""

    model = Strategy

    async def get_by_name(self, name: str, actor: uuid.UUID) -> Strategy | None:
        result = await self.session.execute(self._owned(actor).where(Strategy.name == name))
        return result.scalar_one_or_none()

    async def list_active(self, actor: uuid.UUID, *, limit: int = 100) -> Sequence[Strategy]:
        """Everything not archived. Archiving is the normal alternative to deleting."""
        result = await self.session.execute(
            self._owned(actor)
            .where(Strategy.is_archived.is_(False))
            .order_by(Strategy.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()

    async def archive(self, strategy_id: uuid.UUID, actor: uuid.UUID) -> bool:
        """Archive rather than delete, so backtest results keep a strategy to point at."""
        strategy = await self.get_for(strategy_id, actor)
        if strategy is None:
            return False
        strategy.is_archived = True
        await self.session.flush()
        return True


class PortfolioRepository(OwnedRepository[Portfolio]):
    """Simulated accounts."""

    model = Portfolio

    async def get_with_positions(
        self, portfolio_id: uuid.UUID, actor: uuid.UUID
    ) -> Portfolio | None:
        """Fetch a portfolio and its positions in one query.

        Eager loading, because the caller always wants both and the lazy alternative issues
        a second query per position — inside an async handler, after the transaction that
        could have served it cheaply.
        """
        result = await self.session.execute(
            self._owned(actor)
            .where(Portfolio.id == portfolio_id)
            .options(selectinload(Portfolio.positions))
        )
        return result.scalar_one_or_none()

    async def total_equity(self, portfolio_id: uuid.UUID, actor: uuid.UUID) -> Decimal | None:
        """Cash plus the cost basis of open positions.

        Cost basis, not market value: that needs current prices, which is the service
        layer's job in Phase 7, not the repository's.
        """
        portfolio = await self.get_for(portfolio_id, actor)
        if portfolio is None:
            return None
        result = await self.session.execute(
            select(func.coalesce(func.sum(Position.quantity * Position.avg_entry_price), 0)).where(
                Position.portfolio_id == portfolio_id
            )
        )
        return portfolio.cash_balance + result.scalar_one()


class PositionRepository(Repository[Position]):
    """Positions, projected from fills.

    Scoped by portfolio rather than by user: a caller reaches a portfolio through
    :class:`PortfolioRepository`, which has already checked ownership, so repeating the
    join here would cost a query to re-prove something already established.
    """

    model = Position

    async def get(self, entity_id: uuid.UUID) -> Position | None:
        """Not meaningful for a composite key — use :meth:`get_position`."""
        raise NotImplementedError("positions are keyed by (portfolio_id, instrument_id)")

    async def get_position(
        self, portfolio_id: uuid.UUID, instrument_id: uuid.UUID
    ) -> Position | None:
        return await self.session.get(Position, (portfolio_id, instrument_id))

    async def list_open(self, portfolio_id: uuid.UUID) -> Sequence[Position]:
        """Positions with a non-zero quantity.

        A closed position keeps its row, because ``realized_pnl`` is history worth keeping.
        """
        result = await self.session.execute(
            select(Position).where(Position.portfolio_id == portfolio_id, Position.quantity != 0)
        )
        return result.scalars().all()

    async def upsert(
        self,
        portfolio_id: uuid.UUID,
        instrument_id: uuid.UUID,
        *,
        quantity: Decimal,
        avg_entry_price: Decimal,
        realized_pnl: Decimal,
    ) -> Position:
        """Write a position, creating it if this is the first fill for the instrument."""
        statement = (
            pg_insert(Position)
            .values(
                portfolio_id=portfolio_id,
                instrument_id=instrument_id,
                quantity=quantity,
                avg_entry_price=avg_entry_price,
                realized_pnl=realized_pnl,
            )
            .on_conflict_do_update(
                index_elements=[Position.portfolio_id, Position.instrument_id],
                set_={
                    "quantity": quantity,
                    "avg_entry_price": avg_entry_price,
                    "realized_pnl": realized_pnl,
                    "updated_at": func.now(),
                },
            )
            .returning(Position)
        )
        result = await self.session.execute(statement)
        await self.session.flush()
        return result.scalar_one()

    async def rebuild_from_fills(self, portfolio_id: uuid.UUID) -> dict[uuid.UUID, Decimal]:
        """Recompute net quantity per instrument from fills alone.

        `ARCHITECTURE.md` §6.2 requires that positions be reproducible from fills, and a
        requirement nobody exercises is a requirement that quietly stops holding. This is
        what the test exercises.

        Returns net quantity per instrument. Average entry price and realized P&L need the
        execution accounting rules that arrive with the simulator in Phase 6; quantity is
        the part that is well defined today.
        """
        signed = func.sum(
            case((Order.side == OrderSide.BUY.value, Fill.quantity), else_=-Fill.quantity)
        )
        result = await self.session.execute(
            select(Order.instrument_id, signed)
            .join(Fill, Fill.order_id == Order.id)
            .where(Order.portfolio_id == portfolio_id)
            .group_by(Order.instrument_id)
        )
        return dict(result.all())


class OrderRepository(Repository[Order]):
    """Orders, scoped by portfolio."""

    model = Order

    async def get_by_client_order_id(self, client_order_id: str) -> Order | None:
        """The idempotency lookup: has this signal already produced an order?"""
        result = await self.session.execute(
            select(Order).where(Order.client_order_id == client_order_id)
        )
        return result.scalar_one_or_none()

    async def list_for_portfolio(
        self, portfolio_id: uuid.UUID, *, limit: int = 100
    ) -> Sequence[Order]:
        """The order blotter. Served by ``ix_orders_portfolio_id_created_at``."""
        result = await self.session.execute(
            select(Order)
            .where(Order.portfolio_id == portfolio_id)
            .order_by(Order.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()

    async def count_recent_for_deployment(
        self, deployment_id: uuid.UUID, since: dt.datetime
    ) -> int:
        """Orders from one deployment since a moment.

        This is the runaway-strategy guard (failure F15): the paper trading engine calls it
        before submitting, and refuses when a deployment exceeds its per-minute ceiling.
        """
        result = await self.session.execute(
            select(func.count())
            .select_from(Order)
            .where(Order.deployment_id == deployment_id, Order.created_at >= since)
        )
        return result.scalar_one()


class DeploymentRepository(Repository[StrategyDeployment]):
    """Paper-trading deployments."""

    model = StrategyDeployment

    async def list_running(self) -> Sequence[StrategyDeployment]:
        """Every running deployment, across all users.

        Not owner-scoped on purpose: this is the strategy engine's startup query, and the
        engine is a worker inside the trust boundary rather than a request from a browser.
        Served by the partial index ``ix_strategy_deployments_running``.
        """
        result = await self.session.execute(
            select(StrategyDeployment).where(
                StrategyDeployment.status == DeploymentStatus.RUNNING.value
            )
        )
        return result.scalars().all()


class BacktestRunRepository(OwnedRepository[BacktestRun]):
    """Backtest jobs and their results."""

    model = BacktestRun

    async def claim_next_queued(self) -> BacktestRun | None:
        """Claim one queued run for this worker.

        ``FOR UPDATE SKIP LOCKED`` is what makes this safe with several workers: each takes
        a different row instead of blocking on the same one. Not owner-scoped — the runner
        is a worker, not a user — and served by the partial index
        ``ix_backtest_runs_queued``.
        """
        result = await self.session.execute(
            select(BacktestRun)
            .where(BacktestRun.status == BacktestStatus.QUEUED.value)
            .order_by(BacktestRun.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        run = result.scalar_one_or_none()
        if run is not None:
            run.status = BacktestStatus.RUNNING
            run.started_at = func.now()
            await self.session.flush()
        return run

    async def count_active_for(self, actor: uuid.UUID) -> int:
        """Queued or running backtests for one user.

        Enforces ``TW_MAX_BACKTESTS_PER_USER_CONCURRENT`` — a user cannot make the queue
        their own by submitting a hundred runs.
        """
        result = await self.session.execute(
            select(func.count())
            .select_from(BacktestRun)
            .where(
                BacktestRun.user_id == actor,
                BacktestRun.status.in_([BacktestStatus.QUEUED.value, BacktestStatus.RUNNING.value]),
            )
        )
        return result.scalar_one()


__all__ = [
    "BacktestRunRepository",
    "DeploymentRepository",
    "OrderRepository",
    "PortfolioRepository",
    "PositionRepository",
    "StrategyRepository",
]
