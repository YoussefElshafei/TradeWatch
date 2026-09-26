"""Execution assumptions: what a fill costs, and at what price it happens.

`ARCHITECTURE.md` §16.5 is explicit that slippage, commission, and the fill model change the
result of every backtest — which is why they are captured here as a value object rather than
read from the environment at fill time, and why `ExecutionConfig` is exactly what gets stored
as `backtest_runs.execution_config` and `strategy_deployments.execution_config`. A result
that does not carry its own assumptions is a number nobody else can reproduce.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field

from tradewatch.config.settings import ExecutionSettings, FillModel

__all__ = ["ExecutionConfig"]


class ExecutionConfig(BaseModel):
    """Frozen so a run's assumptions cannot change after the fact — see the module docstring."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    slippage_bps: Decimal = Field(ge=0)
    commission_per_share: Decimal = Field(ge=0)
    commission_pct: Decimal = Field(ge=0)
    fill_model: FillModel = FillModel.NEXT_BAR_OPEN
    allow_partial_fills: bool = False

    @classmethod
    def from_settings(cls, settings: ExecutionSettings) -> Self:
        """The environment's defaults — what a run gets unless it asks for something else."""
        return cls(
            slippage_bps=settings.exec_slippage_bps,
            commission_per_share=settings.exec_commission_per_share,
            commission_pct=settings.exec_commission_pct,
            fill_model=settings.exec_fill_model,
            allow_partial_fills=settings.exec_allow_partial_fills,
        )

    def as_json(self) -> dict[str, Any]:
        """The shape stored in the `execution_config` JSONB column."""
        return self.model_dump(mode="json")
