"""The Alert Dispatcher: `uv run tradewatch-alert-dispatcher` (Phase 9, `ARCHITECTURE.md` §3.8).

```
Kafka (tw.anomalies.v1, tw.fills.v1) → AlertDispatcherService → alerts, tw.alerts.v1
```

The one consumer where the Phase 6/7 trading track and the Phase 8 detection track meet, in
its own group `alert-dispatcher` (`ARCHITECTURE.md` §7.4) — killing this worker affects no
other consumer, and vice versa (ADR-0001).

**Rule matching is pure** (`application.alerts.rules.match`); this file is the driver that
loads `alert_rules`, evaluates every enabled one against every incoming event, and turns a
match into a durably stored, deduplicated, rate-limited `alerts` row.

**Three independent guards, per §3.8 and the F18 failure-catalogue entry:**

- **Dedupe** — `alerts`' `UNIQUE (alert_rule_id, dedupe_key)` absorbs the same underlying
  event redelivered, the same idempotency pattern `orders.client_order_id` gives a redelivered
  bar in `workers/paper_trading.py`.
- **Per-rule, per-symbol cooldown** — a rule that matches ten anomalies for the same symbol
  in a minute fires once every `cooldown_seconds`, not ten times.
- **Per-user hourly cap (F18)** — independent of which rule is firing: once a user has
  received `TW_ALERT_MAX_PER_USER_PER_HOUR` alerts in the trailing hour, further alerts are
  suppressed and a single digest notes that alerts are being held back, rather than either
  flooding the user or going silent about the rest of the storm.

**Rules are reloaded once per batch**, not once per event. A toggled-off rule or an edited
threshold takes effect within one batch's worth of lag — good enough for a control the user
just changed in the UI, and far cheaper than a query per event on a busy topic. Unlike
`workers/paper_trading.py`'s deployment contexts, no per-rule state accumulates between
events, so a full reload is trivially correct — there is nothing to diff.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import signal
import sys
import uuid
from contextlib import AbstractAsyncContextManager
from decimal import Decimal
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.application.alerts import AlertCandidate, AlertRuleConfig, match
from tradewatch.application.ports.event_bus import EventBatchConsumer
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.domain.enums import AlertSeverity
from tradewatch.domain.events import (
    AlertEvent,
    AnomalyEvent,
    OrderFilledEvent,
    deterministic_event_id,
)
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import Alert, AlertRule
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.events.kafka import (
    KafkaEventConsumer,
    KafkaEventPublisher,
    Topics,
    ensure_topics,
    open_dead_letter_sink,
)
from tradewatch.infrastructure.repositories import (
    AlertRepository,
    AlertRuleRepository,
    InstrumentRepository,
)
from tradewatch.infrastructure.repositories.alerts import DIGEST_EVENT_TYPE
from tradewatch.observability.logging import configure_logging, get_logger
from tradewatch.observability.metrics import ALERTS_DISPATCHED, start_metrics_server

log = get_logger(__name__)

__all__ = ["AlertDispatcherService", "SessionSource", "main", "run_alert_dispatcher"]

#: F18's rolling window (`ARCHITECTURE.md` §16.7's `TW_ALERT_MAX_PER_USER_PER_HOUR`).
_CAP_WINDOW = dt.timedelta(hours=1)


class SessionSource(Protocol):
    """What this service needs: a way to open a transactional session. A `Protocol`, exactly
    like `workers/paper_trading.py`'s, so a test can satisfy it with a fake sharing the
    test's own rolled-back transaction.
    """

    def session(self) -> AbstractAsyncContextManager[AsyncSession]: ...


def _to_rule_config(row: AlertRule) -> AlertRuleConfig:
    """The one condition `conditions` currently carries is `min_anomaly_score` — the score
    threshold §3.8 names. `workers/alert_dispatcher.py` and the CRUD API (Task 25) are the
    only two places that need to agree on this key.
    """
    min_score = (row.conditions or {}).get("min_anomaly_score")
    return AlertRuleConfig(
        id=row.id,
        user_id=row.user_id,
        event_types=frozenset(row.event_types),
        symbols=frozenset(row.symbols) if row.symbols is not None else None,
        min_anomaly_score=Decimal(str(min_score)) if min_score is not None else None,
        is_enabled=row.is_enabled,
        cooldown_seconds=row.cooldown_seconds,
    )


def _build_alert_event(
    alert_id: uuid.UUID,
    *,
    symbol: str,
    user_id: uuid.UUID,
    alert_rule_id: uuid.UUID,
    triggering_event_type: str,
    severity: AlertSeverity,
    title: str,
    body: str,
    payload: dict[str, Any],
    source: str,
    correlation_id: uuid.UUID,
    occurred_at: dt.datetime,
) -> AlertEvent:
    return AlertEvent(
        event_id=alert_id,
        symbol=symbol,
        occurred_at=occurred_at,
        ingested_at=occurred_at,
        source=source,
        producer="alert-dispatcher@1.0",
        correlation_id=correlation_id,
        alert_id=alert_id,
        user_id=user_id,
        alert_rule_id=alert_rule_id,
        triggering_event_type=triggering_event_type,
        severity=severity,
        title=title,
        body=body,
        payload=payload,
    )


class AlertDispatcherService:
    """Consume anomalies and fills, match against every user's rules, dispatch. See module
    docstring.
    """

    def __init__(
        self,
        consumer: EventBatchConsumer,
        database: SessionSource,
        publisher: KafkaEventPublisher,
        *,
        alert_cap_per_user_per_hour: int,
    ) -> None:
        self._consumer = consumer
        self._database = database
        self._publisher = publisher
        self._cap = alert_cap_per_user_per_hour
        self._rules: list[AlertRuleConfig] = []

    async def warm_up(self) -> None:
        await self._reload_rules()

    async def _reload_rules(self) -> None:
        async with self._database.session() as session:
            rows = await AlertRuleRepository(session).list_enabled()
            self._rules = [_to_rule_config(row) for row in rows]

    async def handle_event(self, event: AnomalyEvent | OrderFilledEvent) -> None:
        """Evaluate every currently-enabled rule against one event."""
        for rule in self._rules:
            try:
                await self._evaluate(rule, event)
            except Exception as error:  # F14-style isolation: one bad rule, never the batch
                log.warning(
                    "alert_dispatcher.rule_errored",
                    alert_rule_id=str(rule.id),
                    error=str(error),
                    error_type=type(error).__name__,
                )

    async def _evaluate(
        self, rule: AlertRuleConfig, event: AnomalyEvent | OrderFilledEvent
    ) -> None:
        candidate = match(rule, event)
        if candidate is None:
            return

        alert_event: AlertEvent | None = None
        async with self._database.session() as session:
            instrument = await InstrumentRepository(session).get_by_symbol(candidate.symbol)
            if instrument is None:  # pragma: no cover — anomalies/fills only exist for
                return  # instruments already known to the system

            alert_repository = AlertRepository(session)
            now = dt.datetime.now(dt.UTC)

            last_fired = await alert_repository.last_fired_at(rule.id, instrument.id)
            if last_fired is not None and now - last_fired < dt.timedelta(
                seconds=rule.cooldown_seconds
            ):
                return

            window_start = now - _CAP_WINDOW
            recent_count = await alert_repository.count_for_user_since(
                candidate.user_id, window_start
            )
            if recent_count >= self._cap:
                alert_event = await self._raise_digest_if_needed(
                    alert_repository, candidate, event, window_start=window_start, now=now
                )
            else:
                alert_id = new_id()
                row = Alert(
                    id=alert_id,
                    alert_rule_id=candidate.alert_rule_id,
                    user_id=candidate.user_id,
                    instrument_id=instrument.id,
                    event_type=candidate.triggering_event_type,
                    severity=candidate.severity,
                    title=candidate.title,
                    body=candidate.body,
                    payload=candidate.payload,
                    dedupe_key=candidate.dedupe_key,
                )
                inserted = await alert_repository.insert_if_new(row)
                if inserted:
                    ALERTS_DISPATCHED.labels(event_type=candidate.triggering_event_type).inc()
                    alert_event = _build_alert_event(
                        alert_id,
                        symbol=candidate.symbol,
                        user_id=candidate.user_id,
                        alert_rule_id=candidate.alert_rule_id,
                        triggering_event_type=candidate.triggering_event_type,
                        severity=candidate.severity,
                        title=candidate.title,
                        body=candidate.body,
                        payload=candidate.payload,
                        source=event.source,
                        correlation_id=event.correlation_id,
                        occurred_at=now,
                    )

        if alert_event is not None:
            try:
                await self._publisher.publish(alert_event)
            except Exception as error:
                # The row is already durably stored; a live subscriber missing this one
                # notification is not the same failure as losing the alert itself.
                log.warning(
                    "alert_dispatcher.publish_failed",
                    alert_id=str(alert_event.alert_id),
                    error=str(error),
                    error_type=type(error).__name__,
                )

    async def _raise_digest_if_needed(
        self,
        alert_repository: AlertRepository,
        candidate: AlertCandidate,
        triggering_event: AnomalyEvent | OrderFilledEvent,
        *,
        window_start: dt.datetime,
        now: dt.datetime,
    ) -> AlertEvent | None:
        """F18: once a user is capped, exactly one digest per rolling window, not one per
        suppressed alert.

        Attributed to whichever rule's match tipped the cap over. `alerts.alert_rule_id` is
        `NOT NULL` and a cap is a per-user fact with no rule of its own to attach to — the
        first rule to trip it is the simplest honest choice available without a schema
        change (ponytail: a dedicated user-level alert kind would be cleaner; add one if a
        digest ever needs to carry more than a count).
        """
        if await alert_repository.has_digest_since(candidate.user_id, window_start):
            return None

        digest_id = new_id()
        bucket = now.strftime("%Y-%m-%dT%H")
        title = "Alert limit reached"
        body = (
            f"You've reached your hourly alert limit ({self._cap}). Further alerts are "
            "being held back until it resets."
        )
        payload: dict[str, Any] = {"cap": self._cap}
        row = Alert(
            id=digest_id,
            alert_rule_id=candidate.alert_rule_id,
            user_id=candidate.user_id,
            instrument_id=None,
            event_type=DIGEST_EVENT_TYPE,
            severity=AlertSeverity.WARNING,
            title=title,
            body=body,
            payload=payload,
            dedupe_key=f"digest:{bucket}",
        )
        inserted = await alert_repository.insert_if_new(row)
        if not inserted:
            return None
        ALERTS_DISPATCHED.labels(event_type=DIGEST_EVENT_TYPE).inc()

        return _build_alert_event(
            digest_id,
            symbol=candidate.symbol,
            user_id=candidate.user_id,
            alert_rule_id=candidate.alert_rule_id,
            triggering_event_type=DIGEST_EVENT_TYPE,
            severity=AlertSeverity.WARNING,
            title=title,
            body=body,
            payload=payload,
            source=triggering_event.source,
            correlation_id=deterministic_event_id("alert-digest", candidate.user_id, bucket),
            occurred_at=now,
        )

    async def run(
        self, *, max_batches: int | None = None, stop: asyncio.Event | None = None
    ) -> None:
        """Consume anomalies and fills until cancelled, until `stop` is set, or until
        `max_batches`.
        """
        batches = 0
        async for batch in self._consumer.batches():
            if stop is not None and stop.is_set():
                break
            await self._reload_rules()
            for event in batch:
                if isinstance(event, AnomalyEvent | OrderFilledEvent):
                    await self.handle_event(event)
            await self._consumer.commit()
            batches += 1
            if max_batches is not None and batches >= max_batches:
                break


async def run_alert_dispatcher(
    settings: Settings,
    *,
    max_batches: int | None = None,
    stop: asyncio.Event | None = None,
    database: Database | None = None,
) -> AlertDispatcherService:
    """Consume anomalies and fills and run the Alert Dispatcher until cancelled, `stop`, or
    `max_batches`.
    """
    topics = Topics(settings.kafka.kafka_topic_prefix)
    if settings.kafka.kafka_ensure_topics:
        await ensure_topics(settings.kafka, topics=topics)

    owns_database = database is None
    database = database or Database(settings.database)
    if owns_database:
        await database.connect()

    dlq_producer = None
    dead_letters = None
    if settings.kafka.dlq_enabled:
        dead_letters, dlq_producer = await open_dead_letter_sink(
            settings.kafka,
            topics=topics,
            consumer_group=settings.kafka.kafka_alert_dispatcher_group,
            client_id="tradewatch-alert-dispatcher-dlq",
        )

    consumer = KafkaEventConsumer(
        settings.kafka,
        topics=topics,
        dead_letters=dead_letters,
        group_id=settings.kafka.kafka_alert_dispatcher_group,
        subscribe_to=(topics.anomalies, topics.fills),
    )
    publisher = KafkaEventPublisher(settings.kafka, topics=topics)
    service = AlertDispatcherService(
        consumer,
        database,
        publisher,
        alert_cap_per_user_per_hour=settings.alerts.alert_max_per_user_per_hour,
    )

    await consumer.start()
    await publisher.start()
    await service.warm_up()
    log.info(
        "alert_dispatcher.starting",
        group_id=consumer.group_id,
        topics=list(consumer.subscribed),
    )

    try:
        await service.run(max_batches=max_batches, stop=stop)
    finally:
        await consumer.stop()
        await publisher.stop()
        if dlq_producer is not None:
            with contextlib.suppress(Exception):
                await dlq_producer.stop()
        if owns_database:
            await database.disconnect()

    log.info("alert_dispatcher.stopped")
    return service


def main() -> int:
    """Entry point. Returns a process exit code."""
    load_env_file()

    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)  # noqa: T201 — logging is not configured yet
        return 2

    configure_logging(level=settings.app.log_level, log_format=settings.app.log_format)
    start_metrics_server("alert-dispatcher", settings.app)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signal_name in ("SIGINT", "SIGTERM"):
            handler = getattr(signal, signal_name, None)
            if handler is None:
                continue
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(handler, stop.set)
        await run_alert_dispatcher(settings, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("alert_dispatcher.interrupted")
    except Exception as error:
        log.error("alert_dispatcher.failed", error=str(error), error_type=type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
