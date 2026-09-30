"""Alert-rule CRUD and the alert feed (Phase 9), against a real database — same shape as
`test_strategy_api.py`: owner-scoped, 404-not-403 for another user's rows.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable

import httpx2 as httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.domain.enums import AlertSeverity
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import Alert, AlertRule, Instrument, User

pytestmark = pytest.mark.integration

AuthHeadersFor = Callable[[User], dict[str, str]]

_RULE_PAYLOAD = {
    "name": "AAPL anomalies",
    "event_types": ["anomaly.detected"],
    "symbols": ["aapl"],
    "min_anomaly_score": "5.0",
    "cooldown_seconds": 120,
}


class TestCreateAlertRule:
    async def test_creates_and_returns_the_rule(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.post(
            "/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=auth_headers_for(user)
        )
        assert response.status_code == 201
        body = response.json()
        assert body["name"] == "AAPL anomalies"
        assert body["symbols"] == ["AAPL"]
        assert body["min_anomaly_score"] == "5.0"
        assert body["cooldown_seconds"] == 120
        assert body["channels"] == ["in_app"]
        assert body["is_enabled"] is True
        assert body["user_id"] == str(user.id)

    async def test_defaults_cooldown_and_channels_when_omitted(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.post(
            "/api/v1/alert-rules",
            json={"name": "Any anomaly", "event_types": ["anomaly.detected"]},
            headers=auth_headers_for(user),
        )
        assert response.status_code == 201
        body = response.json()
        assert body["symbols"] is None
        assert body["min_anomaly_score"] is None
        assert body["cooldown_seconds"] == 300
        assert body["channels"] == ["in_app"]

    async def test_an_unknown_event_type_is_422(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.post(
            "/api/v1/alert-rules",
            json={"name": "Bad", "event_types": ["market.trade"]},
            headers=auth_headers_for(user),
        )
        assert response.status_code == 422

    async def test_an_empty_symbols_list_is_422(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.post(
            "/api/v1/alert-rules",
            json={"name": "Empty symbols", "event_types": ["anomaly.detected"], "symbols": []},
            headers=auth_headers_for(user),
        )
        assert response.status_code == 422

    async def test_duplicate_name_for_the_same_user_is_409(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        headers = auth_headers_for(user)
        first = await api.post("/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=headers)
        assert first.status_code == 201
        second = await api.post("/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=headers)
        assert second.status_code == 409

    async def test_the_same_name_for_a_different_user_is_fine(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        first = await api.post(
            "/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=auth_headers_for(user)
        )
        second = await api.post(
            "/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=auth_headers_for(other_user)
        )
        assert first.status_code == 201
        assert second.status_code == 201

    async def test_requires_authentication(self, api: httpx.AsyncClient) -> None:
        response = await api.post("/api/v1/alert-rules", json=_RULE_PAYLOAD)
        assert response.status_code == 401


class TestListAndGetAlertRule:
    async def test_lists_only_the_callers_own_rules(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        headers = auth_headers_for(user)
        await api.post("/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=headers)

        response = await api.get("/api/v1/alert-rules", headers=headers)
        assert response.status_code == 200
        assert response.json()["count"] == 1

    async def test_another_users_rules_are_invisible(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        await api.post("/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=auth_headers_for(user))
        response = await api.get("/api/v1/alert-rules", headers=auth_headers_for(other_user))
        assert response.status_code == 200
        assert response.json()["count"] == 0

    async def test_owner_gets_one_by_id(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        headers = auth_headers_for(user)
        created = await api.post("/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=headers)
        rule_id = created.json()["id"]

        response = await api.get(f"/api/v1/alert-rules/{rule_id}", headers=headers)
        assert response.status_code == 200
        assert response.json()["id"] == rule_id

    async def test_another_user_gets_404_not_403(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        created = await api.post(
            "/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=auth_headers_for(user)
        )
        rule_id = created.json()["id"]

        response = await api.get(
            f"/api/v1/alert-rules/{rule_id}", headers=auth_headers_for(other_user)
        )
        assert response.status_code == 404

    async def test_a_missing_id_is_also_404(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.get(
            f"/api/v1/alert-rules/{uuid.uuid4()}", headers=auth_headers_for(user)
        )
        assert response.status_code == 404


class TestUpdateAlertRule:
    async def test_owner_can_disable_it(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        headers = auth_headers_for(user)
        created = await api.post("/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=headers)
        rule_id = created.json()["id"]

        response = await api.patch(
            f"/api/v1/alert-rules/{rule_id}", json={"is_enabled": False}, headers=headers
        )
        assert response.status_code == 200
        body = response.json()
        assert body["is_enabled"] is False
        # Untouched fields survive a partial update.
        assert body["cooldown_seconds"] == 120
        assert body["name"] == "AAPL anomalies"

    async def test_owner_can_change_the_threshold(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        headers = auth_headers_for(user)
        created = await api.post("/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=headers)
        rule_id = created.json()["id"]

        response = await api.patch(
            f"/api/v1/alert-rules/{rule_id}",
            json={"min_anomaly_score": "9.5"},
            headers=headers,
        )
        assert response.status_code == 200
        assert response.json()["min_anomaly_score"] == "9.5"

    async def test_another_user_gets_404_not_403(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        created = await api.post(
            "/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=auth_headers_for(user)
        )
        rule_id = created.json()["id"]

        response = await api.patch(
            f"/api/v1/alert-rules/{rule_id}",
            json={"is_enabled": False},
            headers=auth_headers_for(other_user),
        )
        assert response.status_code == 404


class TestDeleteAlertRule:
    async def test_owner_can_delete(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        headers = auth_headers_for(user)
        created = await api.post("/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=headers)
        rule_id = created.json()["id"]

        response = await api.delete(f"/api/v1/alert-rules/{rule_id}", headers=headers)
        assert response.status_code == 204
        listing = await api.get("/api/v1/alert-rules", headers=headers)
        assert listing.json()["count"] == 0

    async def test_another_user_gets_404_not_403(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        created = await api.post(
            "/api/v1/alert-rules", json=_RULE_PAYLOAD, headers=auth_headers_for(user)
        )
        rule_id = created.json()["id"]

        response = await api.delete(
            f"/api/v1/alert-rules/{rule_id}", headers=auth_headers_for(other_user)
        )
        assert response.status_code == 404


async def _make_alert(
    session: AsyncSession,
    *,
    rule: AlertRule,
    user: User,
    instrument: Instrument | None,
    dedupe_key: str,
) -> Alert:
    alert = Alert(
        id=new_id(),
        alert_rule_id=rule.id,
        user_id=user.id,
        instrument_id=instrument.id if instrument is not None else None,
        event_type="anomaly.detected",
        severity=AlertSeverity.WARNING,
        title="Unusual activity in AAPL",
        body="AAPL scored 6.0 against a threshold of 5.0 (baseline-zscore).",
        payload={"anomaly_score": "6.0"},
        dedupe_key=dedupe_key,
    )
    session.add(alert)
    await session.flush()
    return alert


@pytest.fixture
async def alert_rule(session: AsyncSession, user: User) -> AlertRule:
    rule = AlertRule(
        id=new_id(),
        user_id=user.id,
        name="fixture-rule",
        event_types=["anomaly.detected"],
        symbols=None,
        conditions={},
        cooldown_seconds=300,
        channels=["in_app"],
        is_enabled=True,
    )
    session.add(rule)
    await session.flush()
    return rule


class TestAlertFeed:
    async def test_lists_only_the_callers_own_alerts(
        self,
        api: httpx.AsyncClient,
        session: AsyncSession,
        user: User,
        instrument: Instrument,
        alert_rule: AlertRule,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        await _make_alert(
            session, rule=alert_rule, user=user, instrument=instrument, dedupe_key="k1"
        )
        await session.commit()

        response = await api.get("/api/v1/alerts", headers=auth_headers_for(user))
        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 1
        assert body["alerts"][0]["symbol"] == instrument.symbol
        assert body["alerts"][0]["acknowledged_at"] is None

    async def test_another_users_alerts_are_invisible(
        self,
        api: httpx.AsyncClient,
        session: AsyncSession,
        user: User,
        other_user: User,
        instrument: Instrument,
        alert_rule: AlertRule,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        await _make_alert(
            session, rule=alert_rule, user=user, instrument=instrument, dedupe_key="k1"
        )
        await session.commit()

        response = await api.get("/api/v1/alerts", headers=auth_headers_for(other_user))
        assert response.status_code == 200
        assert response.json()["count"] == 0

    async def test_unacknowledged_only_filters_out_acknowledged_alerts(
        self,
        api: httpx.AsyncClient,
        session: AsyncSession,
        user: User,
        instrument: Instrument,
        alert_rule: AlertRule,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        first = await _make_alert(
            session, rule=alert_rule, user=user, instrument=instrument, dedupe_key="k1"
        )
        await _make_alert(
            session, rule=alert_rule, user=user, instrument=instrument, dedupe_key="k2"
        )
        await session.commit()
        headers = auth_headers_for(user)

        ack = await api.post(f"/api/v1/alerts/{first.id}/ack", headers=headers)
        assert ack.status_code == 204

        response = await api.get(
            "/api/v1/alerts", params={"unacknowledged_only": "true"}, headers=headers
        )
        assert response.json()["count"] == 1

    async def test_a_digest_row_has_no_symbol(
        self,
        api: httpx.AsyncClient,
        session: AsyncSession,
        user: User,
        alert_rule: AlertRule,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        await _make_alert(session, rule=alert_rule, user=user, instrument=None, dedupe_key="k1")
        await session.commit()

        response = await api.get("/api/v1/alerts", headers=auth_headers_for(user))
        assert response.json()["alerts"][0]["symbol"] is None


class TestAcknowledgeAlert:
    async def test_owner_can_acknowledge(
        self,
        api: httpx.AsyncClient,
        session: AsyncSession,
        user: User,
        instrument: Instrument,
        alert_rule: AlertRule,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        alert = await _make_alert(
            session, rule=alert_rule, user=user, instrument=instrument, dedupe_key="k1"
        )
        await session.commit()

        response = await api.post(f"/api/v1/alerts/{alert.id}/ack", headers=auth_headers_for(user))
        assert response.status_code == 204

    async def test_acknowledging_twice_is_still_204(
        self,
        api: httpx.AsyncClient,
        session: AsyncSession,
        user: User,
        instrument: Instrument,
        alert_rule: AlertRule,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        alert = await _make_alert(
            session, rule=alert_rule, user=user, instrument=instrument, dedupe_key="k1"
        )
        await session.commit()
        headers = auth_headers_for(user)

        first = await api.post(f"/api/v1/alerts/{alert.id}/ack", headers=headers)
        second = await api.post(f"/api/v1/alerts/{alert.id}/ack", headers=headers)
        assert first.status_code == 204
        assert second.status_code == 204

    async def test_another_user_gets_404_not_403(
        self,
        api: httpx.AsyncClient,
        session: AsyncSession,
        user: User,
        other_user: User,
        instrument: Instrument,
        alert_rule: AlertRule,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        alert = await _make_alert(
            session, rule=alert_rule, user=user, instrument=instrument, dedupe_key="k1"
        )
        await session.commit()

        response = await api.post(
            f"/api/v1/alerts/{alert.id}/ack", headers=auth_headers_for(other_user)
        )
        assert response.status_code == 404

    async def test_a_missing_id_is_also_404(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.post(
            f"/api/v1/alerts/{uuid.uuid4()}/ack", headers=auth_headers_for(user)
        )
        assert response.status_code == 404
