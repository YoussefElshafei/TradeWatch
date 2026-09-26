"""Repositories for backtest results.

Scoped by `backtest_run_id`, not by user — the same reasoning as `PositionRepository` in
`trading.py`: a caller reaches a run through `BacktestRunRepository`, which has already
checked ownership, so repeating that check here would cost a query to re-prove something
already established.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tradewatch.application.backtest.engine import BacktestTradeResult, EquityPointResult
from tradewatch.domain.enums import OrderSide
from tradewatch.infrastructure.database.models import BacktestEquityPoint, BacktestTrade
from tradewatch.infrastructure.repositories.base import Repository

__all__ = ["BacktestEquityPointRepository", "BacktestTradeRepository"]


class BacktestTradeRepository(Repository[BacktestTrade]):
    """Round trips produced by one backtest run. Append-only — a run's trades are written
    once, when it finishes, and never revised.
    """

    model = BacktestTrade

    async def bulk_insert(
        self,
        backtest_run_id: uuid.UUID,
        trades: Sequence[BacktestTradeResult],
        symbol_to_instrument: Mapping[str, uuid.UUID],
    ) -> None:
        if not trades:
            return
        rows = [
            {
                "backtest_run_id": backtest_run_id,
                "instrument_id": symbol_to_instrument[trade.symbol],
                "side": OrderSide.BUY.value,  # the engine only ever enters long (D16)
                "quantity": trade.quantity,
                "entry_price": trade.entry_price,
                "entry_at": trade.entry_at,
                "exit_price": trade.exit_price,
                "exit_at": trade.exit_at,
                "pnl": trade.pnl,
                "return_pct": trade.return_pct,
            }
            for trade in trades
        ]
        await self.session.execute(pg_insert(BacktestTrade).values(rows))
        await self.session.flush()

    async def list_for_run(
        self, backtest_run_id: uuid.UUID, *, limit: int = 1000
    ) -> Sequence[BacktestTrade]:
        result = await self.session.execute(
            select(BacktestTrade)
            .where(BacktestTrade.backtest_run_id == backtest_run_id)
            .order_by(BacktestTrade.entry_at)
            .limit(limit)
        )
        return result.scalars().all()


class BacktestEquityPointRepository(Repository[BacktestEquityPoint]):
    """A backtest's equity curve."""

    model = BacktestEquityPoint

    async def bulk_upsert(
        self, backtest_run_id: uuid.UUID, points: Sequence[EquityPointResult]
    ) -> None:
        """Overwrite rather than duplicate.

        `BacktestEquityPoint`'s own docstring is explicit: re-running the persistence step
        of a run that died halfway through writing its curve must not double it — the
        composite `(backtest_run_id, as_of)` primary key is exactly what makes an upsert here
        idempotent the same way a bar's `(instrument_id, interval, bar_start)` key does.
        """
        if not points:
            return
        rows = [
            {
                "backtest_run_id": backtest_run_id,
                "as_of": point.as_of,
                "total_equity": point.total_equity,
                "drawdown_pct": point.drawdown_pct,
            }
            for point in points
        ]
        statement = pg_insert(BacktestEquityPoint).values(rows)
        statement = statement.on_conflict_do_update(
            index_elements=[BacktestEquityPoint.backtest_run_id, BacktestEquityPoint.as_of],
            set_={
                "total_equity": statement.excluded.total_equity,
                "drawdown_pct": statement.excluded.drawdown_pct,
            },
        )
        await self.session.execute(statement)
        await self.session.flush()

    async def list_for_run(
        self, backtest_run_id: uuid.UUID, *, limit: int = 20_000
    ) -> Sequence[BacktestEquityPoint]:
        result = await self.session.execute(
            select(BacktestEquityPoint)
            .where(BacktestEquityPoint.backtest_run_id == backtest_run_id)
            .order_by(BacktestEquityPoint.as_of)
            .limit(limit)
        )
        return result.scalars().all()
