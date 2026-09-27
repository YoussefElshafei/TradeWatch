"""Repositories for user-owned trading data.

Every one of these is owner-scoped: the actor is a required argument, not an optional
filter. See :mod:`tradewatch.infrastructure.repositories.base` for why that lives here
rather than in a route handler.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import selectinload

from tradewatch.application.execution import Fill as ExecutionFill
from tradewatch.application.execution import SymbolLedger
from tradewatch.domain.enums import BacktestStatus, DeploymentStatus, OrderSide
from tradewatch.infrastructure.database.models import (
    BacktestRun,
    EquityPoint,
    Fill,
    Order,
    Portfolio,
    Position,
    Strategy,
    StrategyDeployment,
)
from tradewatch.infrastructure.repositories.base import OwnedRepository, Repository

#: A replay ledger only needs correct quantity/avg-entry-price/realized-P&L math, never a
#: real cash balance — `rebuild_from_fills` is not reconstructing "how much cash was on hand
#: at each historical moment", only "what position each fill left behind". Seeding with an
#: unbounded balance lets every historical buy replay through the same `SymbolLedger.buy`
#: the live path used, without `InsufficientCashError` (a real, finite check that a rebuild
#: has no business re-litigating) tripping on a fill that was affordable at the time only
#: because of cash this replay was never given.
_UNBOUNDED_CASH = Decimal("Infinity")


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


@dataclass(frozen=True, slots=True)
class RebuiltPosition:
    """One instrument's position, as `PositionRepository.rebuild_from_fills` recomputed it."""

    quantity: Decimal
    avg_entry_price: Decimal
    realized_pnl: Decimal


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
        ``instrument`` is eager-loaded: every caller of this method wants the symbol to show
        alongside the quantity, and the lazy alternative is a query per row inside an async
        handler, after the transaction that could have served it in one.
        """
        result = await self.session.execute(
            select(Position)
            .where(Position.portfolio_id == portfolio_id, Position.quantity != 0)
            .options(selectinload(Position.instrument))
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

    async def rebuild_from_fills(self, portfolio_id: uuid.UUID) -> dict[uuid.UUID, RebuiltPosition]:
        """Recompute quantity, average entry price, and realized P&L per instrument from
        fills alone, chronologically, through the same `SymbolLedger.buy`/`.sell` the live
        Paper Trading Engine used to produce the stored positions in the first place.

        `ARCHITECTURE.md` §6.2 requires that positions be reproducible from fills, and a
        requirement nobody exercises is a requirement that quietly stops holding — this is
        both the consistency check that requirement demands and the disaster-recovery path
        if stored position state is ever suspected of drifting from the fills that justify
        it (`scripts/rebuild_positions.py`).
        """
        result = await self.session.execute(
            select(
                Order.instrument_id,
                Order.side,
                Fill.quantity,
                Fill.price,
                Fill.commission,
                Fill.filled_at,
            )
            .join(Fill, Fill.order_id == Order.id)
            .where(Order.portfolio_id == portfolio_id)
            # `Fill.filled_at` alone does not break ties between two fills in the same bar;
            # `Fill.created_at` (insertion order) does, and insertion order is the order they
            # were actually applied in.
            .order_by(Fill.filled_at, Fill.created_at)
        )

        ledgers: dict[uuid.UUID, SymbolLedger] = {}
        for instrument_id, side, quantity, price, commission, filled_at in result.all():
            ledger = ledgers.get(instrument_id, SymbolLedger(cash=_UNBOUNDED_CASH))
            fill = ExecutionFill(
                price=price,
                quantity=quantity,
                commission=commission,
                # Not replayed: slippage already happened, priced into `price` itself, and
                # nothing here needs to know how much of it there was.
                slippage_bps=Decimal(0),
                filled_at=filled_at,
            )
            ledgers[instrument_id] = (
                ledger.buy(fill) if side == OrderSide.BUY else ledger.sell(fill)
            )

        return {
            instrument_id: RebuiltPosition(
                quantity=ledger.quantity,
                avg_entry_price=ledger.avg_entry_price,
                realized_pnl=ledger.realized_pnl,
            )
            for instrument_id, ledger in ledgers.items()
        }


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


class EquityPointRepository(Repository[EquityPoint]):
    """The live equity curve. Scoped by portfolio, like :class:`PositionRepository`."""

    model = EquityPoint

    async def get(self, entity_id: uuid.UUID) -> EquityPoint | None:
        """Not meaningful for a composite key."""
        raise NotImplementedError("equity points are keyed by (portfolio_id, as_of)")

    async def upsert(
        self,
        portfolio_id: uuid.UUID,
        as_of: dt.datetime,
        *,
        cash: Decimal,
        positions_value: Decimal,
    ) -> EquityPoint:
        """Write one point, overwriting rather than duplicating a re-emitted minute — the
        same idempotency the model's own docstring describes.
        """
        total_equity = cash + positions_value
        statement = (
            pg_insert(EquityPoint)
            .values(
                portfolio_id=portfolio_id,
                as_of=as_of,
                cash=cash,
                positions_value=positions_value,
                total_equity=total_equity,
            )
            .on_conflict_do_update(
                index_elements=[EquityPoint.portfolio_id, EquityPoint.as_of],
                set_={
                    "cash": cash,
                    "positions_value": positions_value,
                    "total_equity": total_equity,
                },
            )
            .returning(EquityPoint)
        )
        result = await self.session.execute(statement)
        await self.session.flush()
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

    async def list_for_portfolio(self, portfolio_id: uuid.UUID) -> Sequence[StrategyDeployment]:
        """A portfolio's deployments. Ownership is enforced one level up, by the caller
        already having reached this `portfolio_id` through :class:`PortfolioRepository`
        (`StrategyDeployment` itself carries no `user_id` to filter by directly).
        """
        result = await self.session.execute(
            select(StrategyDeployment)
            .where(StrategyDeployment.portfolio_id == portfolio_id)
            .order_by(StrategyDeployment.created_at.desc())
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
    "EquityPointRepository",
    "OrderRepository",
    "PortfolioRepository",
    "PositionRepository",
    "RebuiltPosition",
    "StrategyRepository",
]
