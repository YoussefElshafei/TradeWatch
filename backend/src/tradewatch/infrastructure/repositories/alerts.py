"""Repositories for `alert_rules` and `alerts` (Phase 9, `ARCHITECTURE.md` §3.8/§6.3).

`AlertRuleRepository` is owner-scoped for the CRUD API — a rule belongs to one user — but
also exposes :meth:`AlertRuleRepository.list_enabled`, an unscoped, cross-user query. That is
not a hole in the ownership rule `base.py` describes: `list_enabled` is what the Alert
Dispatcher calls to match one incoming event against *every* user's rules, which is
legitimately cross-user, unlike anything a route handler should ever do.

`AlertRepository` is owner-scoped too, for the alert feed and acknowledgement, and adds the
two queries the dispatcher's F18 guard needs: the last time a (rule, symbol) pair fired, and
how many alerts one user has received in a trailing window.
"""

from __future__ import annotations

import datetime as dt
import uuid

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tradewatch.infrastructure.database.models import Alert, AlertRule
from tradewatch.infrastructure.repositories.base import OwnedRepository

__all__ = ["AlertRepository", "AlertRuleRepository"]

#: `event_type` for the F18 digest row — never a real triggering event, so it is spelled
#: distinctly from every `EventType` value a real anomaly or fill carries.
DIGEST_EVENT_TYPE = "alert.digest"


class AlertRuleRepository(OwnedRepository[AlertRule]):
    model = AlertRule

    async def list_enabled(self) -> list[AlertRule]:
        """Every enabled rule, across every user. See the module docstring for why this is
        the one unscoped query on this repository.
        """
        result = await self.session.execute(
            select(AlertRule).where(AlertRule.is_enabled.is_(True))
        )
        return list(result.scalars().all())


class AlertRepository(OwnedRepository[Alert]):
    model = Alert

    async def insert_if_new(self, alert: Alert) -> bool:
        """Idempotent on `(alert_rule_id, dedupe_key)` — the storm guard `Alert.dedupe_key`'s
        own docstring describes. Returns whether this call actually inserted a row, which is
        what tells the caller whether there is anything new to publish.
        """
        statement = (
            pg_insert(Alert)
            .values(
                id=alert.id,
                alert_rule_id=alert.alert_rule_id,
                user_id=alert.user_id,
                instrument_id=alert.instrument_id,
                event_type=alert.event_type,
                severity=alert.severity,
                title=alert.title,
                body=alert.body,
                payload=alert.payload,
                dedupe_key=alert.dedupe_key,
            )
            .on_conflict_do_nothing(
                index_elements=[Alert.alert_rule_id, Alert.dedupe_key],
            )
            .returning(Alert.id)
        )
        result = await self.session.execute(statement)
        await self.session.flush()
        return result.first() is not None

    async def last_fired_at(
        self, alert_rule_id: uuid.UUID, instrument_id: uuid.UUID
    ) -> dt.datetime | None:
        """When this (rule, symbol) pair last produced an alert — the per-rule cooldown
        clock (§3.8: "deduplication and cooldown per (rule, symbol)").
        """
        result = await self.session.execute(
            select(func.max(Alert.created_at)).where(
                Alert.alert_rule_id == alert_rule_id,
                Alert.instrument_id == instrument_id,
            )
        )
        return result.scalar_one_or_none()

    async def count_for_user_since(self, user_id: uuid.UUID, since: dt.datetime) -> int:
        """How many alerts this user has received in a trailing window — F18's hourly cap."""
        result = await self.session.execute(
            select(func.count())
            .select_from(Alert)
            .where(Alert.user_id == user_id, Alert.created_at >= since)
        )
        return result.scalar_one()

    async def has_digest_since(self, user_id: uuid.UUID, since: dt.datetime) -> bool:
        """Whether this user already has a digest in the current cap window — so hitting the
        cap raises exactly one digest per window, not one per suppressed alert.
        """
        result = await self.session.execute(
            select(Alert.id)
            .where(
                Alert.user_id == user_id,
                Alert.event_type == DIGEST_EVENT_TYPE,
                Alert.created_at >= since,
            )
            .limit(1)
        )
        return result.first() is not None

    async def acknowledge_for(self, alert_id: uuid.UUID, actor: uuid.UUID) -> bool:
        """Idempotent: acknowledging an already-acknowledged alert still returns True, and a
        missing or not-owned alert returns False without distinguishing the two (`base.py`'s
        404-vs-403 rule).
        """
        alert = await self.get_for(alert_id, actor)
        if alert is None:
            return False
        if alert.acknowledged_at is None:
            alert.acknowledged_at = dt.datetime.now(dt.UTC)
            await self.session.flush()
        return True
