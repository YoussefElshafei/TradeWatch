"""Request and response schemas for the portfolio endpoints."""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "PortfolioCreateRequest",
    "PortfolioDetailResponse",
    "PortfolioListResponse",
    "PortfolioResponse",
    "PositionResponse",
]


class PortfolioCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    base_currency: str = Field(default="USD", min_length=3, max_length=3)
    initial_cash: Decimal = Field(gt=0)


class PortfolioResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    base_currency: str
    initial_cash: Decimal
    cash_balance: Decimal
    created_at: dt.datetime
    updated_at: dt.datetime


class PositionResponse(BaseModel):
    instrument_id: uuid.UUID
    symbol: str
    quantity: Decimal
    avg_entry_price: Decimal
    realized_pnl: Decimal
    updated_at: dt.datetime


class PortfolioDetailResponse(PortfolioResponse):
    """The portfolio plus its open positions — what the portfolio view actually renders.

    ``total_equity`` is cash plus the cost basis of open positions (`PortfolioRepository.
    total_equity`'s own definition) — not a mark-to-market value, which would need a live
    price per symbol the API does not fetch here.
    """

    positions: list[PositionResponse]
    total_equity: Decimal


class PortfolioListResponse(BaseModel):
    count: int
    portfolios: list[PortfolioResponse]
