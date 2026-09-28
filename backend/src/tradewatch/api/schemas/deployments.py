"""Request and response schemas for the strategy deployment lifecycle endpoints."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict

from tradewatch.application.trading import PositionSizingConfig, RiskLimitsConfig
from tradewatch.domain.enums import DeploymentStatus

__all__ = [
    "DeploymentCreateRequest",
    "DeploymentListResponse",
    "DeploymentResponse",
]


class DeploymentCreateRequest(BaseModel):
    """`position_sizing`/`risk_limits`/`execution_config` are optional: unset, a deployment
    gets the environment's defaults, captured onto the row at creation — the same "stored
    per run, not read from the environment at fill time" rule `ExecutionConfig` already
    follows (`ARCHITECTURE.md` §16.5), applied to sizing and risk limits too.
    """

    strategy_id: uuid.UUID
    portfolio_id: uuid.UUID
    position_sizing: PositionSizingConfig | None = None
    risk_limits: RiskLimitsConfig | None = None
    execution_config: dict[str, Any] | None = None


class DeploymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    strategy_id: uuid.UUID
    portfolio_id: uuid.UUID
    status: DeploymentStatus
    position_sizing: dict[str, Any]
    risk_limits: dict[str, Any]
    execution_config: dict[str, Any]
    started_at: dt.datetime | None
    stopped_at: dt.datetime | None
    error_message: str | None
    created_at: dt.datetime
    updated_at: dt.datetime


class DeploymentListResponse(BaseModel):
    count: int
    deployments: list[DeploymentResponse]
