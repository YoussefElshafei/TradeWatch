"""Strategies, portfolios, and the order-to-position chain.

The shape of this half of the schema follows one rule: **events are the truth, state is a
projection.** `fills` is an append-only record of what happened. `positions` and
`equity_points` are derived from it and stored only because recomputing a portfolio from
every fill on each page load would be absurd. `ARCHITECTURE.md` §6.2 makes that explicit,
and there is a test asserting that positions rebuilt from fills alone match the stored rows.

The chain is `strategy → deployment → signal → order → fill → position`. Each step is a
row, so an answer to "why do I hold 100 shares of AAPL?" is a walk backwards through
foreign keys rather than a guess.

Strategy parameters, position sizing, risk limits, and execution config are all JSONB. They
are configuration documents whose shape depends on the strategy type (ADR-0006), and
promoting them to columns would mean a migration for every new strategy type while still
not letting the database validate them. The per-type Pydantic schema that does validate
them arrives in Phase 6.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tradewatch.domain.enums import DeploymentStatus, OrderSide, OrderStatus, OrderType
from tradewatch.infrastructure.database.base import (
    MONEY_PRECISION,
    MONEY_SCALE,
    Base,
    TimestampedMixin,
    UUIDPrimaryKey,
)
from tradewatch.infrastructure.database.types import (
    deployment_status_enum,
    order_side_enum,
    order_status_enum,
    order_type_enum,
)

if TYPE_CHECKING:
    from tradewatch.infrastructure.database.models.backtesting import BacktestRun
    from tradewatch.infrastructure.database.models.identity import User
    from tradewatch.infrastructure.database.models.market import Instrument


class Strategy(UUIDPrimaryKey, TimestampedMixin, Base):
    """A user-owned, parameterized configuration of a built-in strategy type.

    Archived rather than deleted by default. A backtest run references the strategy that
    produced it, and deleting the strategy would leave a result nobody can explain.
    """

    __tablename__ = "strategies"

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(50), nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default="{}")
    symbols: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    is_archived: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")

    user: Mapped[User] = relationship(back_populates="strategies")
    deployments: Mapped[list[StrategyDeployment]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan", passive_deletes=True
    )
    backtest_runs: Mapped[list[BacktestRun]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        # Scoped to the user, not global: two people may both have a strategy called
        # "SMA crossover" and neither should have to care.
        UniqueConstraint("user_id", "name", name="uq_strategies_user_id_name"),
        CheckConstraint("cardinality(symbols) > 0", name="symbols_not_empty"),
        Index("ix_strategies_user_id", "user_id"),
    )


class Portfolio(UUIDPrimaryKey, TimestampedMixin, Base):
    """A simulated account. No real money passes through it, ever."""

    __tablename__ = "portfolios"

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    base_currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default="USD")
    initial_cash: Mapped[Decimal] = mapped_column(nullable=False)
    cash_balance: Mapped[Decimal] = mapped_column(nullable=False)

    user: Mapped[User] = relationship(back_populates="portfolios")
    positions: Mapped[list[Position]] = relationship(
        back_populates="portfolio", cascade="all, delete-orphan", passive_deletes=True
    )
    orders: Mapped[list[Order]] = relationship(
        back_populates="portfolio", cascade="all, delete-orphan", passive_deletes=True
    )
    equity_points: Mapped[list[EquityPoint]] = relationship(
        back_populates="portfolio", cascade="all, delete-orphan", passive_deletes=True
    )
    deployments: Mapped[list[StrategyDeployment]] = relationship(
        back_populates="portfolio", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_portfolios_user_id_name"),
        CheckConstraint("initial_cash > 0", name="initial_cash_positive"),
        CheckConstraint("char_length(base_currency) = 3", name="base_currency_is_iso_code"),
        Index("ix_portfolios_user_id", "user_id"),
    )


class StrategyDeployment(UUIDPrimaryKey, TimestampedMixin, Base):
    """A strategy bound to a portfolio and made live for paper trading."""

    __tablename__ = "strategy_deployments"

    strategy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False
    )
    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[DeploymentStatus] = mapped_column(
        deployment_status_enum, nullable=False, server_default=DeploymentStatus.CREATED.value
    )

    position_sizing: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )
    risk_limits: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default="{}")
    # Stored per deployment, not read from the environment at fill time. Two results
    # produced under different slippage assumptions must never be silently comparable
    # (ARCHITECTURE.md §16.5).
    execution_config: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )

    started_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    stopped_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)

    strategy: Mapped[Strategy] = relationship(back_populates="deployments")
    portfolio: Mapped[Portfolio] = relationship(back_populates="deployments")
    orders: Mapped[list[Order]] = relationship(back_populates="deployment")

    __table_args__ = (
        CheckConstraint(
            "(status <> 'errored') OR (error_message IS NOT NULL)",
            name="errored_has_message",
        ),
        Index("ix_strategy_deployments_strategy_id", "strategy_id"),
        Index("ix_strategy_deployments_portfolio_id", "portfolio_id"),
        # The strategy engine's startup query is "which deployments should I be running?".
        # A partial index keeps it proportional to the number of running deployments rather
        # than to every deployment ever created.
        Index(
            "ix_strategy_deployments_running",
            "status",
            postgresql_where=text("status = 'running'"),
        ),
    )


class Order(UUIDPrimaryKey, TimestampedMixin, Base):
    """An intent to trade.

    ``client_order_id`` is globally unique and supplied by the producer. That is the
    idempotency guard for the order path: a paper-trading engine that restarts and
    reprocesses a signal must not open the position twice
    (`ARCHITECTURE.md` §7.4, failure F6).
    """

    __tablename__ = "orders"

    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=False
    )
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("instruments.id", ondelete="RESTRICT"), nullable=False
    )
    deployment_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("strategy_deployments.id", ondelete="SET NULL")
    )

    client_order_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    side: Mapped[OrderSide] = mapped_column(order_side_enum, nullable=False)
    order_type: Mapped[OrderType] = mapped_column(order_type_enum, nullable=False)
    status: Mapped[OrderStatus] = mapped_column(
        order_status_enum, nullable=False, server_default=OrderStatus.PENDING.value
    )

    quantity: Mapped[Decimal] = mapped_column(nullable=False)
    filled_quantity: Mapped[Decimal] = mapped_column(nullable=False, server_default="0")
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(MONEY_PRECISION, MONEY_SCALE))
    reason_code: Mapped[str | None] = mapped_column(String(50))

    portfolio: Mapped[Portfolio] = relationship(back_populates="orders")
    deployment: Mapped[StrategyDeployment | None] = relationship(back_populates="orders")
    instrument: Mapped[Instrument] = relationship()
    fills: Mapped[list[Fill]] = relationship(
        back_populates="order", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint(
            "filled_quantity >= 0 AND filled_quantity <= quantity",
            name="filled_quantity_within_quantity",
        ),
        # A limit order without a price, or a market order with one, is a bug in whatever
        # produced it. The database is the last place that can still catch it.
        CheckConstraint(
            "(order_type = 'limit') = (limit_price IS NOT NULL)",
            name="limit_price_matches_order_type",
        ),
        CheckConstraint("limit_price IS NULL OR limit_price > 0", name="limit_price_positive"),
        Index("ix_orders_portfolio_id_created_at", "portfolio_id", "created_at"),
        Index("ix_orders_deployment_id", "deployment_id"),
        Index("ix_orders_instrument_id", "instrument_id"),
    )


class Fill(UUIDPrimaryKey, Base):
    """An execution against an order. Append-only, and the source of truth for positions.

    ``slippage_bps`` records what the execution simulator assumed, not a measurement. It is
    stored per fill so a result can be re-examined later without having to guess which
    configuration produced it.
    """

    __tablename__ = "fills"

    order_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"), nullable=False
    )
    price: Mapped[Decimal] = mapped_column(nullable=False)
    quantity: Mapped[Decimal] = mapped_column(nullable=False)
    commission: Mapped[Decimal] = mapped_column(nullable=False, server_default="0")
    slippage_bps: Mapped[Decimal] = mapped_column(nullable=False, server_default="0")
    filled_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    order: Mapped[Order] = relationship(back_populates="fills")

    __table_args__ = (
        CheckConstraint("price > 0", name="price_positive"),
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint("commission >= 0", name="commission_not_negative"),
        Index("ix_fills_order_id", "order_id"),
        Index("ix_fills_filled_at", "filled_at"),
    )


class Position(Base):
    """A holding, projected from fills.

    Composite primary key: a portfolio has exactly one position per instrument, and that is
    what identity means here. No surrogate key, so the upsert on each fill is a plain
    ``ON CONFLICT (portfolio_id, instrument_id)``.

    ``quantity`` may be negative. Short selling is deferred (D16), but a schema that forbade
    it would need a migration rather than a feature flag when it arrives.
    """

    __tablename__ = "positions"

    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("portfolios.id", ondelete="CASCADE"), primary_key=True
    )
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("instruments.id", ondelete="RESTRICT"), primary_key=True
    )

    quantity: Mapped[Decimal] = mapped_column(nullable=False)
    avg_entry_price: Mapped[Decimal] = mapped_column(nullable=False)
    realized_pnl: Mapped[Decimal] = mapped_column(nullable=False, server_default="0")
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    portfolio: Mapped[Portfolio] = relationship(back_populates="positions")
    instrument: Mapped[Instrument] = relationship()

    __table_args__ = (
        CheckConstraint("avg_entry_price >= 0", name="avg_entry_price_not_negative"),
        Index("ix_positions_instrument_id", "instrument_id"),
    )


class EquityPoint(Base):
    """One point on a portfolio's equity curve.

    The composite key (portfolio, timestamp) makes writing a snapshot idempotent: a
    restarted engine that re-emits the same minute overwrites rather than duplicating,
    which would otherwise put a step in the curve.
    """

    __tablename__ = "equity_points"

    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("portfolios.id", ondelete="CASCADE"), primary_key=True
    )
    as_of: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), primary_key=True)

    cash: Mapped[Decimal] = mapped_column(nullable=False)
    positions_value: Mapped[Decimal] = mapped_column(nullable=False)
    total_equity: Mapped[Decimal] = mapped_column(nullable=False)

    portfolio: Mapped[Portfolio] = relationship(back_populates="equity_points")

    __table_args__ = (
        # Equity is cash plus the value of what is held. If those three numbers ever
        # disagree the curve is wrong, and a wrong curve is the one output of this system
        # someone might act on.
        CheckConstraint(
            "total_equity = cash + positions_value", name="equity_is_cash_plus_positions"
        ),
    )
