"""Request and response schemas for the strategy endpoints (ADR-0006)."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = [
    "StrategyCreateRequest",
    "StrategyListResponse",
    "StrategyResponse",
]


class StrategyCreateRequest(BaseModel):
    """`parameters` is validated against `strategy_type`'s own schema in the route, not here —
    this schema only knows the shape every strategy shares (ADR-0006).
    """

    name: str = Field(min_length=1, max_length=100)
    strategy_type: str = Field(min_length=1, max_length=50)
    parameters: dict[str, Any] = Field(default_factory=dict)
    symbols: list[str] = Field(min_length=1)

    @field_validator("symbols")
    @classmethod
    def _upper_symbols(cls, value: list[str]) -> list[str]:
        return [symbol.strip().upper() for symbol in value if symbol.strip()]


class StrategyResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    strategy_type: str
    parameters: dict[str, Any]
    symbols: list[str]
    is_archived: bool
    created_at: dt.datetime
    updated_at: dt.datetime


class StrategyListResponse(BaseModel):
    count: int
    strategies: list[StrategyResponse]
