"""Position sizing and risk limits: the Paper Trading Engine's own configuration, stored per
deployment (`strategy_deployments.position_sizing` / `.risk_limits`) for the same reason
`ExecutionConfig` is stored per run — a live result must carry the assumptions that produced
it, immune to a later change to the environment (`ARCHITECTURE.md` §16.5).

`PositionSizingConfig.target_equity_pct` is a *policy*, not a limit: "spend up to this share
of current equity on a new entry." `RiskLimitsConfig` is the hard ceiling checked against the
fill that policy actually produces, and it is deliberately allowed to be tighter than the
sizing target (a sizing bug then still rejects loudly, `reason_code` and all, instead of
quietly buying more than the account is supposed to risk).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field

from tradewatch.config.settings import ExecutionSettings

__all__ = ["PositionSizingConfig", "RiskLimitsConfig"]


class PositionSizingConfig(BaseModel):
    """How large a new entry should be, as a share of current total equity."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    target_equity_pct: Decimal = Field(gt=0, le=1)

    @classmethod
    def from_settings(cls, settings: ExecutionSettings) -> Self:
        """The environment's default: size each entry at the same share the risk limit
        allows, so a deployment created without an explicit sizing choice spends its whole
        risk budget rather than an arbitrary fraction of it.
        """
        return cls(target_equity_pct=Decimal(str(settings.max_position_pct)))

    def as_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class RiskLimitsConfig(BaseModel):
    """Hard ceilings, checked against the fill sizing actually produced (failure F15)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_position_pct: Decimal = Field(gt=0, le=1)
    max_gross_exposure_pct: Decimal = Field(gt=0)
    max_orders_per_minute: int = Field(gt=0)

    @classmethod
    def from_settings(cls, settings: ExecutionSettings) -> Self:
        return cls(
            max_position_pct=Decimal(str(settings.max_position_pct)),
            max_gross_exposure_pct=Decimal(str(settings.max_gross_exposure_pct)),
            max_orders_per_minute=settings.max_orders_per_minute_per_deployment,
        )

    def as_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
