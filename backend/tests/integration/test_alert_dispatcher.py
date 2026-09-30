"""`AlertDispatcherService` against a real database (Phase 9, `ARCHITECTURE.md` §3.8):
dedupe, per-rule cooldown, and the F18 per-user hourly cap with its digest — none of which a
pure `application.alerts.rules.match` unit test can prove, since all three need real `alerts`
rows and the unique constraint on `(alert_rule_id, dedupe_key)` to check against.

Uses the same `_FakeDatabase`-over-the-test-session pattern as `test_paper_trading.py` and
`test_anomaly_detector.py`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tests.kafka_support import FakeProducerClient, anomaly, fill, kafka_settings
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import Alert, AlertRule, Instrument, User
from tradewatch.infrastructure.events.kafka import KafkaEventPublisher, Topics
from tradewatch.workers.alert_dispatcher import AlertDispatcherService

pytestmark = pytest.mark.integration


class _FakeDatabase:
    """See `test_backtest_runner.py`'s `_FakeDatabase` for why this exists."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        return self._passthrough()

    @asynccontextmanager
    async def _passthrough(self) -> AsyncIterator[AsyncSession]:
        yield self._session


async def _make_rule(
    session: AsyncSession,
    *,
    user: User,
    event_types: list[str] | None = None,
    symbols: list[str] | None = None,
    cooldown_seconds: int = 300,
    min_anomaly_score: str | None = None,
    name: str = "rule",
) -> AlertRule:
    conditions: dict[str, Any] = {}
    if min_anomaly_score is not None:
        conditions["min_anomaly_score"] = min_anomaly_score
    rule = AlertRule(
        id=new_id(),
        user_id=user.id,
        name=name,
        event_types=event_types or ["anomaly.detected"],
        symbols=symbols,
        conditions=conditions,
        cooldown_seconds=cooldown_seconds,
        channels=["in_app"],
        is_enabled=True,
    )
    session.add(rule)
    await session.flush()
    return rule


async def _service(
    session: AsyncSession, *, cap: int = 50
) -> tuple[AlertDispatcherService, FakeProducerClient]:
    client = FakeProducerClient()
    publisher = KafkaEventPublisher(kafka_settings(), client=client, topics=Topics())
    await publisher.start()
    service = AlertDispatcherService(
        consumer=None,  # type: ignore[arg-type]  # only `.run()` needs a real consumer
        database=_FakeDatabase(session),
        publisher=publisher,
        alert_cap_per_user_per_hour=cap,
    )
    await service.warm_up()
    return service, client


async def _alerts_for(session: AsyncSession, user: User) -> list[Alert]:
    result = await session.execute(
        select(Alert).where(Alert.user_id == user.id).order_by(Alert.created_at)
    )
    return list(result.scalars().all())


class TestDedupe:
    async def test_the_same_underlying_event_redelivered_never_produces_two_alerts(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        await _make_rule(session, user=user)
        service, client = await _service(session)

        event = anomaly(symbol=instrument.symbol)
        await service.handle_event(event)
        await service.handle_event(event)  # a Kafka redelivery of the identical event

        alerts = await _alerts_for(session, user)
        assert len(alerts) == 1
        assert client.topics() == ["tw.alerts.v1"]

    async def test_two_distinct_detections_are_not_deduped_against_each_other(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        await _make_rule(session, user=user, cooldown_seconds=0)
        service, _client = await _service(session)

        await service.handle_event(anomaly(symbol=instrument.symbol, model_version="1"))
        await service.handle_event(anomaly(symbol=instrument.symbol, model_version="2"))

        alerts = await _alerts_for(session, user)
        assert len(alerts) == 2


class TestCooldown:
    async def test_a_second_distinct_detection_within_cooldown_is_suppressed(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        await _make_rule(session, user=user, cooldown_seconds=300)
        service, client = await _service(session)

        await service.handle_event(anomaly(symbol=instrument.symbol, model_version="1"))
        await service.handle_event(anomaly(symbol=instrument.symbol, model_version="2"))

        alerts = await _alerts_for(session, user)
        assert len(alerts) == 1
        assert client.topics() == ["tw.alerts.v1"]

    async def test_a_zero_second_cooldown_never_suppresses(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        await _make_rule(session, user=user, cooldown_seconds=0)
        service, _client = await _service(session)

        await service.handle_event(anomaly(symbol=instrument.symbol, model_version="1"))
        await service.handle_event(anomaly(symbol=instrument.symbol, model_version="2"))

        alerts = await _alerts_for(session, user)
        assert len(alerts) == 2


class TestHourlyCap:
    """F18: a noisy rule during a volatile session must not flood a user."""

    async def test_a_burst_of_qualifying_events_stops_at_the_cap_and_raises_one_digest(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        # cooldown=0 isolates the cap: every event in the burst is a distinct detection that
        # would otherwise be allowed through, so anything suppressed is the cap's doing.
        await _make_rule(session, user=user, cooldown_seconds=0)
        service, client = await _service(session, cap=3)

        for version in range(10):
            await service.handle_event(
                anomaly(symbol=instrument.symbol, model_version=str(version))
            )

        alerts = await _alerts_for(session, user)
        real_alerts = [row for row in alerts if row.event_type != "alert.digest"]
        digests = [row for row in alerts if row.event_type == "alert.digest"]

        assert len(real_alerts) == 3
        assert len(digests) == 1
        assert client.topics() == ["tw.alerts.v1"] * 4

    async def test_the_cap_is_per_user_not_global(
        self, session: AsyncSession, user: User, other_user: User, instrument: Instrument
    ) -> None:
        await _make_rule(session, user=user, cooldown_seconds=0, name="rule-a")
        await _make_rule(session, user=other_user, cooldown_seconds=0, name="rule-b")
        service, _client = await _service(session, cap=2)

        for version in range(5):
            await service.handle_event(
                anomaly(symbol=instrument.symbol, model_version=f"user-{version}")
            )
        for version in range(5):
            await service.handle_event(
                anomaly(symbol=instrument.symbol, model_version=f"other-{version}")
            )

        user_real = [
            row for row in await _alerts_for(session, user) if row.event_type != "alert.digest"
        ]
        other_real = [
            row
            for row in await _alerts_for(session, other_user)
            if row.event_type != "alert.digest"
        ]
        assert len(user_real) == 2
        assert len(other_real) == 2


class TestBothTracksFeedTheDispatcher:
    """Phase 9's meeting point: a rule can listen to anomalies, fills, or both."""

    async def test_a_rule_listening_to_both_event_types_fires_on_either(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        await _make_rule(
            session,
            user=user,
            event_types=["anomaly.detected", "order.filled"],
            cooldown_seconds=0,
        )
        service, client = await _service(session)

        await service.handle_event(anomaly(symbol=instrument.symbol))
        await service.handle_event(fill(symbol=instrument.symbol))

        alerts = await _alerts_for(session, user)
        assert {row.event_type for row in alerts} == {"anomaly.detected", "order.filled"}
        assert client.topics() == ["tw.alerts.v1", "tw.alerts.v1"]

    async def test_a_disabled_rule_is_reloaded_out_and_stops_firing(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        rule = await _make_rule(session, user=user, cooldown_seconds=0)
        service, _client = await _service(session)

        rule.is_enabled = False
        await session.flush()
        await service.warm_up()  # reloads the enabled-rules cache from the database

        await service.handle_event(anomaly(symbol=instrument.symbol))

        assert await _alerts_for(session, user) == []
