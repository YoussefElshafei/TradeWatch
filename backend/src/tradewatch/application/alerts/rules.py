"""Pure alert-rule matching: does one incoming event satisfy one user's rule?

`ARCHITECTURE.md` §3.8: the Alert Dispatcher "evaluates user-owned `alert_rules` (symbol
filters, score thresholds, event types)". Those three gates are what :func:`match` checks,
in the order most likely to reject a busy topic's events cheaply.

No I/O here, no database, no Kafka — `workers/alert_dispatcher.py` is the only thing in
Phase 9 that touches infrastructure. That split is what `application` → not-`infrastructure`
(`pyproject.toml`'s import-linter contract) enforces, and it is what lets this module be
tested with plain events and plain config, the same way `application.trading.step` is.

**Cooldown and the per-user cap (F18) are not here.** Both need to see every other alert a
rule or user has raised recently, which is database state this module has none of — that is
`workers/alert_dispatcher.py`'s job, once a candidate this module produced needs to be
weighed against history.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from tradewatch.domain.enums import AlertSeverity
from tradewatch.domain.events import AnomalyEvent, MarketEvent, OrderFilledEvent

__all__ = ["AlertCandidate", "AlertRuleConfig", "match"]


@dataclass(frozen=True, slots=True)
class AlertRuleConfig:
    """The slice of one `alert_rules` row that matching needs — never the ORM row itself,
    since `application` may not import `infrastructure`. `workers/alert_dispatcher.py` builds
    this from the row it loaded.
    """

    id: uuid.UUID
    user_id: uuid.UUID
    #: `MarketEvent.event_type` values this rule listens for, e.g. `{"anomaly.detected"}`.
    event_types: frozenset[str]
    #: `None` means every symbol; an empty set is rejected at the schema level (§6.3's
    #: `symbols_null_or_not_empty` check), so this module never has to treat it specially.
    symbols: frozenset[str] | None
    #: The one condition `conditions` currently carries: an anomaly must score at or above
    #: this to match. `None` means no floor. Meaningless for a fill and ignored there.
    min_anomaly_score: Decimal | None
    is_enabled: bool
    #: Seconds this rule must wait before firing again for the same symbol — the dispatcher's
    #: cooldown clock, not something `match` itself enforces (see the module docstring).
    cooldown_seconds: int


@dataclass(frozen=True, slots=True)
class AlertCandidate:
    """What one matched (rule, event) pair wants to become an `alerts` row.

    `dedupe_key` is the triggering event's own type and id — the same event redelivered
    (a dispatcher restart, a consumer replay) always derives the identical key, so
    `UNIQUE (alert_rule_id, dedupe_key)` absorbs the duplicate exactly the way
    `orders.client_order_id` absorbs a redelivered bar in `workers/paper_trading.py`. Two
    *different* underlying detections are two different keys on purpose — cooldown, not
    dedup, is what suppresses a rule that fires on every one of them.
    """

    alert_rule_id: uuid.UUID
    user_id: uuid.UUID
    symbol: str
    triggering_event_type: str
    severity: AlertSeverity
    title: str
    body: str
    payload: dict[str, Any]
    dedupe_key: str


def match(rule: AlertRuleConfig, event: MarketEvent) -> AlertCandidate | None:
    """`None` if `rule` does not fire for `event`, otherwise the alert it wants raised."""
    if not rule.is_enabled:
        return None
    if not isinstance(event, AnomalyEvent | OrderFilledEvent):
        return None
    if event.event_type.value not in rule.event_types:
        return None
    if rule.symbols is not None and event.symbol not in rule.symbols:
        return None

    dedupe_key = f"{event.event_type.value}:{event.event_id}"

    if isinstance(event, AnomalyEvent):
        if rule.min_anomaly_score is not None and event.anomaly_score < rule.min_anomaly_score:
            return None
        return AlertCandidate(
            alert_rule_id=rule.id,
            user_id=rule.user_id,
            symbol=event.symbol,
            triggering_event_type=event.event_type.value,
            severity=event.severity,
            title=f"Unusual activity in {event.symbol}",
            body=(
                f"{event.symbol} scored {event.anomaly_score} against a threshold of "
                f"{event.threshold} ({event.model_name})."
            ),
            payload={
                "anomaly_score": str(event.anomaly_score),
                "threshold": str(event.threshold),
                "model_name": event.model_name,
            },
            dedupe_key=dedupe_key,
        )

    return AlertCandidate(
        alert_rule_id=rule.id,
        user_id=rule.user_id,
        symbol=event.symbol,
        triggering_event_type=event.event_type.value,
        severity=AlertSeverity.INFO,
        title=f"Order filled: {event.symbol}",
        body=f"{event.quantity} shares of {event.symbol} filled at {event.price}.",
        payload={
            "price": str(event.price),
            "quantity": str(event.quantity),
            "order_id": str(event.order_id),
            "fill_id": str(event.fill_id),
        },
        dedupe_key=dedupe_key,
    )
