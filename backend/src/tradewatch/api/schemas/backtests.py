"""Request and response schemas for the backtest endpoints (`ARCHITECTURE.md` §4.3)."""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from tradewatch.config.settings import FillModel
from tradewatch.domain.enums import BacktestStatus, OrderSide

__all__ = [
    "BacktestEquityListResponse",
    "BacktestEquityPointResponse",
    "BacktestListResponse",
    "BacktestResponse",
    "BacktestSubmitRequest",
    "BacktestTradeListResponse",
    "BacktestTradeResponse",
    "ExecutionConfigSchema",
]


class ExecutionConfigSchema(BaseModel):
    """Every field optional: a submission overrides only what it names, and the response
    always reports the fully-resolved config a run actually used (`ARCHITECTURE.md` §16.5).
    """

    slippage_bps: Decimal | None = Field(default=None, ge=0)
    commission_per_share: Decimal | None = Field(default=None, ge=0)
    commission_pct: Decimal | None = Field(default=None, ge=0)
    fill_model: FillModel | None = None
    allow_partial_fills: bool | None = None


class BacktestSubmitRequest(BaseModel):
    strategy_id: uuid.UUID
    #: Defaults to the strategy's own universe — most submissions want exactly that.
    symbols: list[str] | None = None
    start_date: dt.date
    end_date: dt.date
    initial_cash: Decimal | None = Field(default=None, gt=0)
    execution_config: ExecutionConfigSchema | None = None

    @model_validator(mode="after")
    def _range_is_ordered(self) -> BacktestSubmitRequest:
        if self.end_date < self.start_date:
            raise ValueError(
                f"end_date ({self.end_date}) must not be before start_date ({self.start_date})"
            )
        return self


class BacktestResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    strategy_id: uuid.UUID
    symbols: list[str]
    start_date: dt.date
    end_date: dt.date
    initial_cash: Decimal
    execution_config: dict[str, Any]
    status: BacktestStatus
    error_message: str | None
    metrics: dict[str, Any] | None
    started_at: dt.datetime | None
    completed_at: dt.datetime | None
    created_at: dt.datetime


class BacktestListResponse(BaseModel):
    count: int
    backtests: list[BacktestResponse]


class BacktestTradeResponse(BaseModel):
    id: uuid.UUID
    symbol: str
    side: OrderSide
    quantity: Decimal
    entry_price: Decimal
    entry_at: dt.datetime
    exit_price: Decimal | None
    exit_at: dt.datetime | None
    pnl: Decimal | None
    return_pct: Decimal | None


class BacktestTradeListResponse(BaseModel):
    count: int
    trades: list[BacktestTradeResponse]


class BacktestEquityPointResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    as_of: dt.datetime
    total_equity: Decimal
    drawdown_pct: Decimal | None


class BacktestEquityListResponse(BaseModel):
    count: int
    equity_curve: list[BacktestEquityPointResponse]
