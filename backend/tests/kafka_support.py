"""Builders and fake clients shared by the Kafka tests.

The fakes implement the narrow ``ProducerClient`` and ``ConsumerClient`` protocols the
adapters declare, which is the whole reason those protocols exist. Every producer and
consumer failure path — a broker that refuses to connect, a send that times out twice and
then succeeds, a commit that fails, a record whose body is not JSON — is reachable from here
in milliseconds, with no broker and without patching ``aiokafka``.

The tests that need a *real* broker live in `tests/integration/test_kafka_pipeline.py` and
assert the things only a real broker can settle: that offsets survive a restart, that a
consumer group catches up after being offline, and that the bytes on the topic are the bytes
the consumer reads.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from pydantic import SecretStr

from tradewatch.config.settings import (
    DatabaseSettings,
    KafkaSettings,
    RedisSettings,
    SecuritySettings,
    Settings,
    load_settings,
)
from tradewatch.domain.enums import AlertSeverity, BarInterval
from tradewatch.domain.events import (
    AnomalyEvent,
    BarEvent,
    MarketEvent,
    QuoteEvent,
    TradeEvent,
    deterministic_event_id,
)
from tradewatch.infrastructure.providers.normalization import EventFactory

__all__ = [
    "MOMENT",
    "FakeConsumerClient",
    "FakeProducerClient",
    "FakeRecord",
    "anomaly",
    "bar",
    "factory",
    "kafka_settings",
    "quote",
    "settings_with",
    "trade",
]

MOMENT = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


# --- event builders --------------------------------------------------------


def factory(moment: dt.datetime = MOMENT) -> EventFactory:
    return EventFactory(source="test", producer="test@1.0", clock=lambda: moment)


def trade(symbol: str = "AAPL", trade_id: str = "t-1", price: str = "100") -> TradeEvent:
    return factory().trade(
        symbol=symbol,
        price=Decimal(price),
        size=Decimal("10"),
        occurred_at=MOMENT,
        provider_trade_id=trade_id,
    )


def quote(symbol: str = "AAPL", bid: str = "99.99") -> QuoteEvent:
    return factory().quote(
        symbol=symbol,
        bid_price=Decimal(bid),
        bid_size=Decimal("100"),
        ask_price=Decimal("100.01"),
        ask_size=Decimal("100"),
        occurred_at=MOMENT,
    )


def bar(
    symbol: str = "AAPL",
    minute: int = 0,
    interval: BarInterval = BarInterval.ONE_MINUTE,
) -> BarEvent:
    return factory().bar(
        symbol=symbol,
        interval=interval,
        bar_start=MOMENT + dt.timedelta(minutes=minute),
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        volume=Decimal("1000"),
        trade_count=5,
    )


def anomaly(
    symbol: str = "AAPL",
    *,
    score: str = "4.2",
    model_name: str = "baseline-zscore",
    model_version: str = "1",
) -> AnomalyEvent:
    """Not built through `EventFactory`: that class normalizes a *provider's* raw data, and
    an anomaly is derived inside this system, not received from one.
    """
    window_start = MOMENT - dt.timedelta(minutes=60)
    window_end = MOMENT
    return AnomalyEvent(
        event_id=deterministic_event_id(
            *AnomalyEvent.natural_key(
                source="internal",
                symbol=symbol,
                model_name=model_name,
                model_version=model_version,
                window_start=window_start,
                window_end=window_end,
            )
        ),
        symbol=symbol,
        occurred_at=window_end,
        ingested_at=window_end,
        source="internal",
        producer="anomaly-detector@1.0",
        correlation_id=deterministic_event_id(symbol, window_end.isoformat()),
        window_start=window_start,
        anomaly_score=Decimal(score),
        threshold=Decimal("3.5"),
        severity=AlertSeverity.WARNING,
        model_name=model_name,
        model_version=model_version,
        features={"return": Decimal("0.08"), "volume_z": Decimal(score)},
    )


# --- settings --------------------------------------------------------------


def kafka_settings(**overrides: Any) -> KafkaSettings:
    """Kafka settings built in-process, never from the environment."""
    defaults: dict[str, Any] = {
        "kafka_bootstrap": "kafka.invalid:9092",
        "kafka_topic_prefix": "tw",
        "consumer_batch_size": 10,
        "kafka_consumer_poll_timeout_ms": 5,
        "kafka_producer_max_attempts": 3,
    }
    return KafkaSettings(**(defaults | overrides))


def settings_with(kafka: KafkaSettings, *, database_url: str | None = None) -> Settings:
    """A full settings object with a given Kafka group, for the worker-level tests."""
    return load_settings(
        kafka=kafka,
        database=DatabaseSettings(
            database_url=database_url
            or "postgresql+asyncpg://tradewatch:tradewatch@127.0.0.1:1/tradewatch_unreachable"
        ),
        redis=RedisSettings(redis_url="redis://localhost:6379/1"),
        security=SecuritySettings(jwt_secret=SecretStr("test-secret-not-used-anywhere-real-01234")),
    )


# --- fake broker clients ---------------------------------------------------


@dataclass(slots=True)
class FakeRecord:
    """One consumer record, shaped like ``aiokafka``'s."""

    value: bytes | None
    topic: str = "tw.market.trades.v1"
    partition: int = 0
    offset: int = 0
    key: bytes | None = None
    timestamp: int = 0


@dataclass(slots=True)
class _Sent:
    topic: str
    key: bytes | None
    value: bytes | None
    headers: Sequence[tuple[str, bytes]] | None


class FakeProducerClient:
    """A ``ProducerClient`` that records what it was asked to send.

    ``start_error`` makes connecting fail. ``send_errors`` is a list consumed one entry per
    send: ``None`` means that send succeeds, an exception means it fails. That is what lets
    a test say "fail twice, then succeed" and assert the retry actually happened rather than
    assuming it did.
    """

    def __init__(
        self,
        *,
        start_error: Exception | None = None,
        send_errors: list[Exception | None] | None = None,
        flush_error: Exception | None = None,
    ) -> None:
        self.start_error = start_error
        self.send_errors = send_errors or []
        self.flush_error = flush_error
        self.sent: list[_Sent] = []
        self.started = False
        self.stopped = False
        self.flushes = 0

    async def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
    ) -> asyncio.Future[Any]:
        error = self.send_errors.pop(0) if self.send_errors else None
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        if error is not None:
            future.set_exception(error)
        else:
            self.sent.append(_Sent(topic=topic, key=key, value=value, headers=headers))
            future.set_result(object())
        return future

    async def flush(self) -> None:
        if self.flush_error is not None:
            raise self.flush_error
        self.flushes += 1

    # --- assertions helpers ---

    def topics(self) -> list[str]:
        return [item.topic for item in self.sent]

    def keys(self) -> list[bytes | None]:
        return [item.key for item in self.sent]

    def values(self) -> list[bytes | None]:
        return [item.value for item in self.sent]

    def headers_of(self, index: int = 0) -> dict[str, bytes]:
        raw = self.sent[index].headers or ()
        return dict(raw)


@dataclass(frozen=True, slots=True)
class _Partition:
    """Stands in for ``TopicPartition``, which is the dict key aiokafka groups records by.

    Frozen because that key has to be hashable, exactly as the real one is.
    """

    topic: str
    partition: int


class FakeConsumerClient:
    """A ``ConsumerClient`` driven by a script of polls.

    ``polls`` is a list of record lists: one entry per :meth:`getmany` call. When it runs
    out, every further poll returns nothing, which is what a quiet topic looks like.

    ``commits`` records the number of records consumed at the moment of each commit, so a
    test can assert not only *that* a commit happened but *what it covered* — which is the
    only interesting question about a commit.
    """

    def __init__(
        self,
        polls: list[list[FakeRecord]] | None = None,
        *,
        start_error: Exception | None = None,
        commit_error: Exception | None = None,
        poll_error: Exception | None = None,
    ) -> None:
        self.polls = polls or []
        self.start_error = start_error
        self.commit_error = commit_error
        self.poll_error = poll_error
        self.started = False
        self.stopped = False
        self.consumed = 0
        self.commits: list[int] = []
        self.poll_count = 0

    async def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getmany(self, *, timeout_ms: int, max_records: int) -> dict[Any, list[Any]]:
        if self.poll_error is not None:
            raise self.poll_error
        self.poll_count += 1
        if not self.polls:
            # A poll that timed out with nothing to read. Yield to the loop so a caller
            # waiting on a stop event actually gets a turn.
            await asyncio.sleep(0)
            return {}

        records = self.polls.pop(0)
        self.consumed += len(records)
        grouped: dict[Any, list[Any]] = {}
        for record in records:
            key = _Partition(record.topic, record.partition)
            grouped.setdefault(key, []).append(record)
        return grouped

    async def commit(self) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commits.append(self.consumed)


@dataclass(slots=True)
class RecordingDeadLetters:
    """A ``DeadLetterSink`` that remembers what it was given."""

    events: list[MarketEvent] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    failure: Exception | None = None

    async def send(
        self,
        event: MarketEvent,
        *,
        reason: str,
        error: str,
        attempts: int = 1,
    ) -> None:
        if self.failure is not None:
            raise self.failure
        self.events.append(event)
        self.reasons.append(reason)
        self.errors.append(error)
