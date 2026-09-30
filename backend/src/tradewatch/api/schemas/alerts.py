"""Request/response schemas for alert-rule CRUD and the alert feed (Phase 9, `ARCHITECTURE.md`
§3.8).

`event_types` is restricted to the two values the Alert Dispatcher actually consumes
(`workers/alert_dispatcher.py` subscribes to `tw.anomalies.v1` and `tw.fills.v1` only) — a
rule listing anything else would be accepted and then silently never fire
(`application.alerts.rules.match` only evaluates those two), which is a worse experience than
a 422 telling the caller now.

`min_anomaly_score` is a first-class field here even though the database stores it inside
`alert_rules.conditions` (a JSONB catch-all for future conditions) —
`workers/alert_dispatcher.py::_to_rule_config` is the other place that reads this one key,
and the route is what translates between the two.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from tradewatch.domain.enums import AlertSeverity

__all__ = [
    "ALLOWED_ALERT_EVENT_TYPES",
    "AlertListResponse",
    "AlertResponse",
    "AlertRuleCreateRequest",
    "AlertRuleListResponse",
    "AlertRuleResponse",
    "AlertRuleUpdateRequest",
]

#: The only event types `workers/alert_dispatcher.py` subscribes to — see the module
#: docstring for why a rule is not allowed to name anything else.
ALLOWED_ALERT_EVENT_TYPES = frozenset({"anomaly.detected", "order.filled"})


def _validate_event_types(value: list[str]) -> list[str]:
    unknown = sorted(set(value) - ALLOWED_ALERT_EVENT_TYPES)
    if unknown:
        allowed = sorted(ALLOWED_ALERT_EVENT_TYPES)
        raise ValueError(f"Unknown event type(s) {unknown}; must be one of {allowed}.")
    return value


def _upper_symbols(value: list[str] | None) -> list[str] | None:
    if value is None:
        return None
    return [symbol.strip().upper() for symbol in value if symbol.strip()]


class AlertRuleCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    event_types: list[str] = Field(min_length=1)
    #: `None` means every symbol (`AlertRuleConfig.symbols`'s own docstring). An empty list
    #: is rejected here rather than left to the database's `symbols_null_or_not_empty` check,
    #: so the caller gets a 422 instead of a 500-shaped IntegrityError.
    symbols: list[str] | None = Field(default=None, min_length=1)
    min_anomaly_score: Decimal | None = None
    cooldown_seconds: int = Field(default=300, ge=0)
    channels: list[str] = Field(default_factory=lambda: ["in_app"], min_length=1)
    is_enabled: bool = True

    @field_validator("event_types")
    @classmethod
    def _check_event_types(cls, value: list[str]) -> list[str]:
        return _validate_event_types(value)

    @field_validator("symbols")
    @classmethod
    def _check_symbols(cls, value: list[str] | None) -> list[str] | None:
        return _upper_symbols(value)


class AlertRuleUpdateRequest(BaseModel):
    """Every field optional: only what is provided is changed."""

    name: str | None = Field(default=None, min_length=1, max_length=100)
    event_types: list[str] | None = Field(default=None, min_length=1)
    symbols: list[str] | None = Field(default=None, min_length=1)
    #: Distinguishing "leave unchanged" from "clear the floor" needs a real tri-state, which
    #: this phase's rule shape does not otherwise need — so clearing the floor once set is a
    #: recreate, not a PATCH. Simple and honest about what it does not support (ponytail: add
    #: a sentinel if that gap is ever reported as a real workflow).
    min_anomaly_score: Decimal | None = None
    cooldown_seconds: int | None = Field(default=None, ge=0)
    channels: list[str] | None = Field(default=None, min_length=1)
    is_enabled: bool | None = None

    @field_validator("event_types")
    @classmethod
    def _check_event_types(cls, value: list[str] | None) -> list[str] | None:
        return None if value is None else _validate_event_types(value)

    @field_validator("symbols")
    @classmethod
    def _check_symbols(cls, value: list[str] | None) -> list[str] | None:
        return _upper_symbols(value)


class AlertRuleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    event_types: list[str]
    symbols: list[str] | None
    min_anomaly_score: Decimal | None
    cooldown_seconds: int
    channels: list[str]
    is_enabled: bool
    created_at: dt.datetime
    updated_at: dt.datetime

    @classmethod
    def from_row(cls, row: Any) -> AlertRuleResponse:
        """`min_anomaly_score` lives inside `row.conditions`, not its own column — see the
        module docstring. Every other field maps straight through, so this is the one place
        that needs to be `from_attributes`-not-quite.
        """
        raw_score = (row.conditions or {}).get("min_anomaly_score")
        return cls(
            id=row.id,
            user_id=row.user_id,
            name=row.name,
            event_types=list(row.event_types),
            symbols=list(row.symbols) if row.symbols is not None else None,
            min_anomaly_score=Decimal(str(raw_score)) if raw_score is not None else None,
            cooldown_seconds=row.cooldown_seconds,
            channels=list(row.channels),
            is_enabled=row.is_enabled,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


class AlertRuleListResponse(BaseModel):
    count: int
    rules: list[AlertRuleResponse]


class AlertResponse(BaseModel):
    """One fired rule instance — the alert feed's own row shape, distinct from
    `AlertRuleResponse`. `symbol` is `None` for the F18 digest row, which has no
    `instrument_id` (`workers/alert_dispatcher.py::_raise_digest_if_needed`).
    """

    id: uuid.UUID
    alert_rule_id: uuid.UUID
    symbol: str | None
    event_type: str
    severity: AlertSeverity
    title: str
    body: str
    payload: dict[str, Any]
    created_at: dt.datetime
    acknowledged_at: dt.datetime | None


class AlertListResponse(BaseModel):
    count: int
    alerts: list[AlertResponse]
