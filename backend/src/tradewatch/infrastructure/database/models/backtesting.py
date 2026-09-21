"""Backtest runs and their results.

A backtest is a **job row**, not a request. `ARCHITECTURE.md` §4.3 routes it deliberately
around Kafka: it is a batch computation over data that already exists, so a broker would add
latency and failure modes for nothing. The queue is this table, claimed with
``FOR UPDATE SKIP LOCKED`` — which is exactly the workload a database queue is good at
(bounded, low rate, transactional), as opposed to the continuous multi-consumer fan-out that
justified Kafka in ADR-0001.

Results are split across three tables rather than stuffed into one JSON blob, because they
are read three different ways: summary metrics for a list view, trades for a table, and the
equity curve for a chart that may hold tens of thousands of points. Only `metrics` stays
JSONB — the set of computed statistics will grow, and growing it should not be a migration.

`execution_config` is stored on every run. Slippage, commission, and the fill model change
the result of every backtest (`ARCHITECTURE.md` §16.5), so a result that does not carry its
assumptions is a number nobody can reproduce.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tradewatch.domain.enums import BacktestStatus, OrderSide
from tradewatch.infrastructure.database.base import (
    MONEY_PRECISION,
    MONEY_SCALE,
    Base,
    TimestampedMixin,
    UUIDPrimaryKey,
)
from tradewatch.infrastructure.database.types import backtest_status_enum, order_side_enum

if TYPE_CHECKING:
    from tradewatch.infrastructure.database.models.identity import User
    from tradewatch.infrastructure.database.models.market import Instrument
    from tradewatch.infrastructure.database.models.trading import Strategy


class BacktestRun(UUIDPrimaryKey, TimestampedMixin, Base):
    """One historical simulation: its inputs, its status, and its summary metrics."""

    __tablename__ = "backtest_runs"

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )

    symbols: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    start_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    end_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    initial_cash: Mapped[Decimal] = mapped_column(nullable=False)
    execution_config: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )

    status: Mapped[BacktestStatus] = mapped_column(
        backtest_status_enum, nullable=False, server_default=BacktestStatus.QUEUED.value
    )
    error_message: Mapped[str | None] = mapped_column(Text)
    metrics: Mapped[dict[str, Any] | None] = mapped_column(JSONB)

    started_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))

    user: Mapped[User] = relationship(back_populates="backtest_runs")
    strategy: Mapped[Strategy] = relationship(back_populates="backtest_runs")
    trades: Mapped[list[BacktestTrade]] = relationship(
        back_populates="run", cascade="all, delete-orphan", passive_deletes=True
    )
    equity_points: Mapped[list[BacktestEquityPoint]] = relationship(
        back_populates="run", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        CheckConstraint("end_date >= start_date", name="date_range_ordered"),
        CheckConstraint("initial_cash > 0", name="initial_cash_positive"),
        CheckConstraint("cardinality(symbols) > 0", name="symbols_not_empty"),
        CheckConstraint(
            "(status <> 'failed') OR (error_message IS NOT NULL)",
            name="failed_has_message",
        ),
        Index("ix_backtest_runs_user_id_created_at", "user_id", "created_at"),
        Index("ix_backtest_runs_strategy_id", "strategy_id"),
        # The runner's claim query. Partial, so it stays proportional to the queue depth
        # rather than to every backtest ever run — which is the difference between a
        # constant-time poll and one that degrades as the table grows.
        Index(
            "ix_backtest_runs_queued",
            "created_at",
            postgresql_where=text("status = 'queued'"),
        ),
    )


class BacktestTrade(UUIDPrimaryKey, Base):
    """A round trip within a backtest: an entry, and an exit if there was one.

    A trade still open when the window ended keeps NULL exit columns rather than being
    force-closed at the last price. Marking it closed would inflate the win rate with a
    trade that never actually happened.
    """

    __tablename__ = "backtest_trades"

    backtest_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False
    )
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("instruments.id", ondelete="RESTRICT"), nullable=False
    )

    side: Mapped[OrderSide] = mapped_column(order_side_enum, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(nullable=False)
    entry_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    exit_price: Mapped[Decimal | None] = mapped_column(Numeric(MONEY_PRECISION, MONEY_SCALE))
    exit_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    pnl: Mapped[Decimal | None] = mapped_column(Numeric(MONEY_PRECISION, MONEY_SCALE))
    return_pct: Mapped[Decimal | None] = mapped_column(Numeric(MONEY_PRECISION, MONEY_SCALE))

    run: Mapped[BacktestRun] = relationship(back_populates="trades")
    instrument: Mapped[Instrument] = relationship()

    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint("entry_price > 0", name="entry_price_positive"),
        CheckConstraint("exit_price IS NULL OR exit_price > 0", name="exit_price_positive"),
        # Either the trade is closed — exit price, exit time and P&L all present — or it is
        # open and all three are absent. A half-closed trade is a bug in the runner.
        CheckConstraint(
            "(exit_at IS NULL) = (exit_price IS NULL) AND (exit_at IS NULL) = (pnl IS NULL)",
            name="exit_fields_consistent",
        ),
        CheckConstraint("exit_at IS NULL OR exit_at >= entry_at", name="exit_after_entry"),
        Index("ix_backtest_trades_backtest_run_id", "backtest_run_id"),
    )


class BacktestEquityPoint(Base):
    """One point on a backtest's equity curve.

    Composite key (run, timestamp), so re-running the persistence step of a job that died
    halfway overwrites rather than duplicating.
    """

    __tablename__ = "backtest_equity_points"

    backtest_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("backtest_runs.id", ondelete="CASCADE"), primary_key=True
    )
    as_of: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    total_equity: Mapped[Decimal] = mapped_column(nullable=False)
    drawdown_pct: Mapped[Decimal | None] = mapped_column(Numeric(MONEY_PRECISION, MONEY_SCALE))

    run: Mapped[BacktestRun] = relationship(back_populates="equity_points")

    __table_args__ = (
        CheckConstraint("drawdown_pct IS NULL OR drawdown_pct <= 0", name="drawdown_not_positive"),
    )
