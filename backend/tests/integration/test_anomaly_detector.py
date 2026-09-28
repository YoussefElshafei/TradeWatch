"""`AnomalyDetectionService` against a real database (ADR-0008): idempotency, F17's fallback
to the baseline, and that a stored detection is genuinely explainable from its own row —
the things a unit test of `application.detection.baseline`/`.features` alone cannot prove,
because they need real `anomaly_detections` rows to check.

Uses the same `_FakeDatabase`-over-the-test-session pattern as `test_paper_trading.py`.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from decimal import Decimal

import numpy as np
import pytest
from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tests.kafka_support import FakeProducerClient
from tradewatch.config.settings import (
    AnomalyDetectorMode,
    AnomalySettings,
    DatabaseSettings,
    RedisSettings,
    SecuritySettings,
    Settings,
    load_settings,
)
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import BarEvent, EventType
from tradewatch.infrastructure.database.models import AnomalyDetection, Instrument
from tradewatch.infrastructure.events.kafka import KafkaEventPublisher, Topics
from tradewatch.workers.anomaly_detector import AnomalyDetectionService

pytestmark = pytest.mark.integration

_START = dt.datetime(2026, 6, 1, 14, 30, tzinfo=dt.UTC)


def _quiet_closes(count: int) -> list[float]:
    """A small random walk, not a repeating cycle.

    A fixed-period repeating price cycle (e.g. 100, 100.5, 101, 100, 100.5, 101, ...)
    produces a `return` series with only as many distinct values as the cycle's length —
    and if two of those values happen to sit close together, the window's median/MAD
    collapses onto that near-degenerate pair, making the third, perfectly regular value look
    like a huge outlier by comparison every time it recurs. A random walk does not have this
    problem: its returns vary continuously, so the baseline sees a real, non-degenerate
    spread of "normal" and nothing repeats often enough to look artificially tight.
    """
    rng = np.random.default_rng(20260601)
    price = 100.0
    closes = []
    for step in rng.uniform(-0.3, 0.3, size=count):
        price += float(step)
        closes.append(round(price, 2))
    return closes


class _FakeDatabase:
    """See `test_backtest_runner.py`'s `_FakeDatabase` for why this exists."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        return self._passthrough()

    @asynccontextmanager
    async def _passthrough(self) -> AsyncIterator[AsyncSession]:
        yield self._session
        await self._session.commit()


def _bar_event(*, symbol: str, minute: int, close: float) -> BarEvent:
    bar_start = _START + dt.timedelta(minutes=minute)
    price = Decimal(str(close))
    return BarEvent(
        event_id=uuid.uuid4(),
        event_type=EventType.BAR_CLOSED,
        symbol=symbol,
        occurred_at=bar_start + dt.timedelta(minutes=1),
        ingested_at=bar_start + dt.timedelta(minutes=1),
        source="synthetic",
        producer="test",
        correlation_id=uuid.uuid4(),
        interval=BarInterval.ONE_MINUTE,
        bar_start=bar_start,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal(1000),
        vwap=price,
        trade_count=10,
        revision=0,
    )


def _settings_for(symbol: str, **anomaly_overrides: object) -> Settings:
    # A small window makes the median/MAD estimate noisy — with only 5-10 samples, a purely
    # random "quiet" value spuriously crossing 3.5 robust-sigma by chance is common, not a
    # bug (this is exactly the false-positive-rate-at-small-n behaviour ADR-0008's offline
    # evaluation methodology exists to measure, not something a single test should assume
    # away). 30/15 gives a stable enough estimate that a bounded random walk practically
    # never crosses threshold on its own, so "quiet data is not flagged" is a meaningful
    # assertion here.
    defaults: dict[str, object] = {
        "anomaly_window_bars": 30,
        "anomaly_min_bars_before_scoring": 15,
        "anomaly_baseline_z_threshold": 3.5,
        "anomaly_detector": AnomalyDetectorMode.BASELINE,
    }
    settings = load_settings(
        anomaly=AnomalySettings.model_validate(defaults | anomaly_overrides),
        database=DatabaseSettings(
            database_url="postgresql+asyncpg://tradewatch:tradewatch@127.0.0.1:1/tradewatch_unreachable"
        ),
        redis=RedisSettings(redis_url="redis://localhost:6379/1"),
        security=SecuritySettings(jwt_secret=SecretStr("test-secret-not-used-anywhere-real-01234")),
    )
    return settings.model_copy(
        update={"market": settings.market.model_copy(update={"symbols": [symbol]})}
    )


async def _service(session: AsyncSession, settings: Settings) -> AnomalyDetectionService:
    publisher = KafkaEventPublisher(settings.kafka, client=FakeProducerClient(), topics=Topics())
    await publisher.start()
    return AnomalyDetectionService(
        # Only `.run()` needs a real consumer; nothing here calls it.
        consumer=None,  # type: ignore[arg-type]
        database=_FakeDatabase(session),
        publisher=publisher,
        settings=settings,
    )


async def _feed_quiet_bars(service: AnomalyDetectionService, symbol: str, count: int) -> None:
    for minute, close in enumerate(_quiet_closes(count)):
        await service.handle_bar(_bar_event(symbol=symbol, minute=minute, close=close))


class TestBaselineDetection:
    async def test_quiet_bars_produce_no_detections(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        service = await _service(session, _settings_for(instrument.symbol))
        await service.warm_up()

        await _feed_quiet_bars(service, instrument.symbol, 50)

        rows = (await session.execute(select(AnomalyDetection))).scalars().all()
        assert rows == []

    async def test_an_injected_spike_is_detected_and_explainable(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        service = await _service(session, _settings_for(instrument.symbol))
        await service.warm_up()

        await _feed_quiet_bars(service, instrument.symbol, 40)
        # A wildly-out-of-range close: an unmistakable synthetic anomaly.
        await service.handle_bar(_bar_event(symbol=instrument.symbol, minute=40, close=5000.0))

        rows = (await session.execute(select(AnomalyDetection))).scalars().all()
        assert len(rows) == 1
        row = rows[0]
        assert row.instrument_id == instrument.id
        assert row.model_name == "baseline-zscore"
        assert row.anomaly_score > row.threshold
        # Explainable: the features that produced the score are on the row itself.
        assert row.features
        assert "return" in row.features

    async def test_redelivering_the_same_bar_does_not_duplicate_the_detection(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        service = await _service(session, _settings_for(instrument.symbol))
        await service.warm_up()

        await _feed_quiet_bars(service, instrument.symbol, 40)
        spike = _bar_event(symbol=instrument.symbol, minute=40, close=5000.0)
        await service.handle_bar(spike)
        await service.handle_bar(spike)  # redelivery of the identical event

        rows = (await session.execute(select(AnomalyDetection))).scalars().all()
        assert len(rows) == 1

    async def test_a_symbol_not_in_tw_symbols_is_ignored(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        service = await _service(session, _settings_for("SOMEOTHERSYMBOL"))
        await service.warm_up()

        await service.handle_bar(_bar_event(symbol=instrument.symbol, minute=0, close=100.0))

        rows = (await session.execute(select(AnomalyDetection))).scalars().all()
        assert rows == []


class TestF17Fallback:
    async def test_mode_model_with_no_active_model_falls_back_to_the_baseline(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """No `ModelVersion` has ever been trained in this test database — the service must
        still detect using the baseline rather than running with no detector at all.
        """
        settings = _settings_for(instrument.symbol, anomaly_detector=AnomalyDetectorMode.MODEL)
        service = await _service(session, settings)
        await service.warm_up()

        await _feed_quiet_bars(service, instrument.symbol, 40)
        await service.handle_bar(_bar_event(symbol=instrument.symbol, minute=40, close=5000.0))

        rows = (await session.execute(select(AnomalyDetection))).scalars().all()
        assert len(rows) == 1
        assert rows[0].model_name == "baseline-zscore"
