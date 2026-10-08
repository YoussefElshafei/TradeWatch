"""The Anomaly Detection Service: `uv run tradewatch-anomaly-detector` (ADR-0008).

```
Kafka (tw.market.bars.1m.v1) → AnomalyDetectionService → anomaly_detections, tw.anomalies.v1
```

The fifth independent reader of the bar topic, in its own group `anomaly-detector`
(`ARCHITECTURE.md` §7.4) — killing this worker affects no other consumer, and vice versa
(ADR-0001).

**Global, not per-user.** Unlike the Strategy Engine or Paper Trading Engine, there is no
deployment to watch: every symbol in `TW_SYMBOLS` is scored continuously, because
"statistically unusual" is a property of the market itself, not something a user opts a
specific strategy into (`ARCHITECTURE.md` §3.7).

**Every claim here is about statistically unusual activity, and nothing more (ADR-0008).**
That constraint governs this file's own naming and log messages, not only what gets shown
to a user — see ADR-0008 for the reasoning and the words it rules out.

**F17: never runs with no detector.** If `TW_ANOMALY_DETECTOR` asks for the model tier and no
active `ModelVersion` exists, or its artifact is missing or fails to load, this logs a
warning once at startup and scores every symbol with the baseline instead — a broken
dependency degrades this service, it does not stop it, the same rule Phase 7's paper trading
applies to one deployment's own exception, generalized here to the one detector this service
has.

**State is rebuildable from stored bars**, exactly like Phase 7's live driver: `warm_up()`
replays each symbol's recent bars through the same `FeatureState`/`BaselineState` fold a live
`handle_bar()` call uses, so a restart does not lose a symbol's rolling window. Bars are
folded through the feature/baseline state during warm-up without re-scoring them — only bars
arriving after startup are evaluated for detections, the same way `warm_up` rebuilds
`strategy_state` without re-submitting orders for signals that already fired.

**No quotes, so no `spread` feature — see `application.detection.features` and
`application.detection.model` for why that is an honestly incomplete measurement rather than
a fabricated one, not a gap unique to this file.**
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import signal
import sys
import uuid
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.application.detection.baseline import (
    BaselineResult,
    BaselineState,
    initial_baseline_state,
    score_baseline,
    severity_for,
)
from tradewatch.application.detection.features import FeatureState, initial_feature_state
from tradewatch.application.detection.model import AnomalyModel, load_model
from tradewatch.application.ports.event_bus import EventBatchConsumer
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import (
    AnomalyDetectorMode,
    ConfigurationError,
    Settings,
    get_settings,
)
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import AlertSeverity, BarInterval
from tradewatch.domain.events import AnomalyEvent, BarEvent, deterministic_event_id
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import AnomalyDetection
from tradewatch.infrastructure.database.models import Bar as BarRow
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.events.kafka import (
    KafkaEventConsumer,
    KafkaEventPublisher,
    Topics,
    ensure_topics,
    open_dead_letter_sink,
)
from tradewatch.infrastructure.repositories import (
    AnomalyDetectionRepository,
    BarRepository,
    InstrumentRepository,
    ModelVersionRepository,
)
from tradewatch.observability.logging import configure_logging, get_logger
from tradewatch.observability.metrics import ANOMALIES_DETECTED, start_metrics_server

log = get_logger(__name__)

__all__ = ["AnomalyDetectionService", "SessionSource", "main", "run_anomaly_detector"]

#: How far back state warm-up ever looks — a cheap guard against an unbounded backfill.
_MAX_WARM_UP_LOOKBACK = dt.timedelta(days=7)
#: `model_name` recorded for a baseline-driven detection, so `AnomalyDetection.model_name`
#: never lies about which tier actually produced a given row.
_BASELINE_MODEL_NAME = "baseline-zscore"
#: The one model this system trains and serves (ADR-0008 names a single model tier,
#: `IsolationForest` — matches `scripts/train_anomaly_model.py`'s own default).
_MODEL_NAME = "isolation-forest"


@dataclass(slots=True)
class _SymbolState:
    """The only state this service keeps between bars — see the module docstring.

    `last_bar_at` is the same state-level idempotency guard `workers/paper_trading.py`'s
    `_SymbolMemory` uses, and for the identical reason: a brand-new consumer group starts
    from `TW_KAFKA_AUTO_OFFSET_RESET` (earliest, by default), which would otherwise
    redeliver every bar `warm_up`'s database replay already folded into these states.
    """

    feature_state: FeatureState
    baseline_state: BaselineState
    #: `None` when the symbol has no `instruments` row yet — nothing to store a detection
    #: against, so this symbol's bars are folded into state but never scored.
    instrument_id: uuid.UUID | None = None
    last_bar_at: dt.datetime | None = None


@dataclass(frozen=True, slots=True)
class _Detection:
    """One tier's verdict, reduced to exactly what an `AnomalyDetection` row needs —
    independent of whether the baseline or the model produced it.
    """

    anomaly_score: Decimal
    threshold: Decimal
    severity: AlertSeverity
    model_name: str
    model_version: str


class SessionSource(Protocol):
    """What this service needs: a way to open a transactional session. A `Protocol`, exactly
    like `workers/paper_trading.py`'s, so a test can satisfy it with a fake sharing the
    test's own rolled-back transaction.
    """

    def session(self) -> AbstractAsyncContextManager[AsyncSession]: ...


async def load_active_model(
    session: AsyncSession, model_name: str
) -> tuple[AnomalyModel, Decimal, str] | None:
    """The active `ModelVersion` for `model_name`, loaded and ready to score — or `None` if
    there is not one, or its artifact cannot be loaded. `None` is F17's trigger, not an
    error this function raises: a missing or corrupt model is an expected, handled state,
    and the caller's job is to fall back, not to crash.
    """
    version = await ModelVersionRepository(session).get_active(model_name)
    if version is None:
        return None
    try:
        model = load_model(Path(version.artifact_uri))
    except Exception as error:
        log.warning(
            "anomaly_detector.model_load_failed",
            model_name=model_name,
            model_version=version.version,
            artifact_uri=version.artifact_uri,
            error=str(error),
            error_type=type(error).__name__,
        )
        return None
    threshold = Decimal(str(version.offline_metrics.get("model_threshold", "0")))
    return model, threshold, version.version


class AnomalyDetectionService:
    """Consume bars, score every symbol, persist and publish what crosses a threshold.
    See module docstring.
    """

    def __init__(
        self,
        consumer: EventBatchConsumer,
        database: SessionSource,
        publisher: KafkaEventPublisher,
        *,
        settings: Settings,
    ) -> None:
        self._consumer = consumer
        self._database = database
        self._publisher = publisher
        self._settings = settings
        self._states: dict[str, _SymbolState] = {}
        self._model: AnomalyModel | None = None
        self._model_version: str | None = None
        self._model_threshold: Decimal | None = None

    async def warm_up(self) -> None:
        """Rebuild every configured symbol's rolling state from stored bars, and load the
        active model if the configured mode wants one (F17 applies from the very first bar).
        """
        window = self._settings.anomaly.anomaly_window_bars
        min_bars = self._settings.anomaly.anomaly_min_bars_before_scoring
        now = dt.datetime.now(dt.UTC)
        start = now - _MAX_WARM_UP_LOOKBACK

        async with self._database.session() as session:
            if self._settings.anomaly.anomaly_detector is not AnomalyDetectorMode.BASELINE:
                loaded = await load_active_model(session, _MODEL_NAME)
                if loaded is not None:
                    self._model, self._model_threshold, self._model_version = loaded
                elif self._settings.anomaly.anomaly_detector is AnomalyDetectorMode.MODEL:
                    log.warning(
                        "anomaly_detector.no_active_model_falling_back_to_baseline",
                        model_name=_MODEL_NAME,
                    )

            instrument_repository = InstrumentRepository(session)
            bar_repository = BarRepository(session)
            for symbol in self._settings.market.symbols:
                feature_state = initial_feature_state(window)
                baseline_state = initial_baseline_state(
                    window_bars=window, min_bars_before_scoring=min_bars
                )
                last_bar_at: dt.datetime | None = None

                instrument = await instrument_repository.get_by_symbol(symbol)
                if instrument is not None:
                    bars = await bar_repository.list_range(
                        instrument.id, BarInterval.ONE_MINUTE, start, now
                    )
                    for row in bars:
                        feature_state, features = feature_state.update(_to_domain_bar(row))
                        baseline_state, _ = score_baseline(
                            baseline_state,
                            features,
                            threshold=Decimal(
                                str(self._settings.anomaly.anomaly_baseline_z_threshold)
                            ),
                        )
                        last_bar_at = row.bar_start

                self._states[symbol] = _SymbolState(
                    feature_state=feature_state,
                    baseline_state=baseline_state,
                    instrument_id=instrument.id if instrument is not None else None,
                    last_bar_at=last_bar_at,
                )

        log.info(
            "anomaly_detector.warmed_up",
            symbols=list(self._settings.market.symbols),
            model_loaded=self._model is not None,
        )

    async def handle_bar(self, event: BarEvent) -> None:
        """Score one bar for its symbol, isolated from every other symbol's failures."""
        if event.symbol not in self._states:
            return  # not one of TW_SYMBOLS — nothing to score it against
        try:
            await self._score_bar(event)
        except Exception as error:  # F14-equivalent: isolate this symbol, never take others down
            log.warning(
                "anomaly_detector.symbol_errored",
                symbol=event.symbol,
                error=str(error),
                error_type=type(error).__name__,
            )

    async def _score_bar(self, event: BarEvent) -> None:
        state = self._states[event.symbol]
        if state.last_bar_at is not None and event.bar_start <= state.last_bar_at:
            return  # already folded in during warm-up — the state-level idempotency guard

        if state.instrument_id is None:
            # A symbol configured in TW_SYMBOLS that had no `instruments` row at warm-up —
            # normal right after a fresh deploy, before ingestion has registered it. One
            # fresh lookup per bar until it appears is cheap and self-healing; caching a
            # permanent "no instrument" would mean this symbol never gets scored again.
            async with self._database.session() as session:
                instrument = await InstrumentRepository(session).get_by_symbol(event.symbol)
            state.instrument_id = instrument.id if instrument is not None else None
            if state.instrument_id is None:
                return

        baseline_threshold = Decimal(str(self._settings.anomaly.anomaly_baseline_z_threshold))
        bar = _to_domain_bar_from_event(event, state.instrument_id)
        feature_state, features = state.feature_state.update(bar)
        baseline_state, baseline_result = score_baseline(
            state.baseline_state, features, threshold=baseline_threshold
        )
        state.feature_state = feature_state
        state.baseline_state = baseline_state
        state.last_bar_at = event.bar_start

        mode = self._settings.anomaly.anomaly_detector
        detection = self._decide(mode, features, baseline_result)
        if detection is None:
            return

        anomaly_event = _build_event(
            event, instrument_id=state.instrument_id, detection=detection, features=features
        )
        row = AnomalyDetection(
            id=new_id(),
            event_id=anomaly_event.event_id,
            instrument_id=state.instrument_id,
            detected_at=anomaly_event.window_end,
            window_start=anomaly_event.window_start,
            window_end=anomaly_event.window_end,
            anomaly_score=detection.anomaly_score,
            threshold=detection.threshold,
            severity=detection.severity,
            model_name=detection.model_name,
            model_version=detection.model_version,
            features={name: str(value) for name, value in features.items()},
        )
        async with self._database.session() as session:
            await AnomalyDetectionRepository(session).upsert(row)
            await session.commit()

        try:
            await self._publisher.publish(anomaly_event)
        except Exception as error:
            # The row is already durably stored; a live subscriber missing this one
            # notification is not the same failure as losing the detection itself.
            log.warning(
                "anomaly_detector.publish_failed",
                symbol=event.symbol,
                error=str(error),
                error_type=type(error).__name__,
            )

        ANOMALIES_DETECTED.labels(severity=detection.severity.value).inc()
        log.info(
            "anomaly_detector.detected",
            symbol=event.symbol,
            anomaly_score=str(detection.anomaly_score),
            severity=detection.severity.value,
            model_name=detection.model_name,
        )

    def _decide(
        self,
        mode: AnomalyDetectorMode,
        features: dict[str, Decimal],
        baseline_result: BaselineResult | None,
    ) -> _Detection | None:
        baseline_detection = None
        if baseline_result is not None:
            baseline_detection = _Detection(
                anomaly_score=baseline_result.anomaly_score,
                threshold=baseline_result.threshold,
                severity=baseline_result.severity,
                model_name=_BASELINE_MODEL_NAME,
                model_version=f"window={self._settings.anomaly.anomaly_window_bars}",
            )

        model_detection = None
        if self._model is not None and self._model_threshold is not None:
            score = self._model.score(features)
            if score is not None and score > self._model_threshold:
                model_detection = _Detection(
                    anomaly_score=score,
                    threshold=self._model_threshold,
                    severity=severity_for(score, self._model_threshold),
                    model_name=_MODEL_NAME,
                    model_version=self._model_version or "unknown",
                )

        if mode is AnomalyDetectorMode.BASELINE:
            return baseline_detection
        if mode is AnomalyDetectorMode.MODEL:
            # F17: no model loaded (missing/corrupt artifact) falls all the way back to the
            # baseline, not to silence — this service never runs with no detector at all.
            return model_detection if self._model is not None else baseline_detection
        # BOTH: whichever tier actually flagged it; the model's verdict wins if both did,
        # since a model only stays active once it has already beaten the baseline offline.
        return model_detection or baseline_detection

    async def run(
        self, *, max_batches: int | None = None, stop: asyncio.Event | None = None
    ) -> None:
        """Consume bars until cancelled, until `stop` is set, or until `max_batches`."""
        batches = 0
        async for batch in self._consumer.batches():
            if stop is not None and stop.is_set():
                break
            for event in batch:
                if isinstance(event, BarEvent):
                    await self.handle_bar(event)
            await self._consumer.commit()
            batches += 1
            if max_batches is not None and batches >= max_batches:
                break


def _to_domain_bar(row: BarRow) -> Bar:
    return Bar(
        instrument_id=row.instrument_id,
        interval=row.interval,
        bar_start=row.bar_start,
        open=row.open,
        high=row.high,
        low=row.low,
        close=row.close,
        volume=row.volume,
        vwap=row.vwap,
        trade_count=row.trade_count,
        is_final=row.is_final,
        revision=row.revision,
        recorded_at=row.recorded_at,
    )


def _to_domain_bar_from_event(event: BarEvent, instrument_id: uuid.UUID) -> Bar:
    return Bar(
        instrument_id=instrument_id,
        interval=event.interval,
        bar_start=event.bar_start,
        open=event.open,
        high=event.high,
        low=event.low,
        close=event.close,
        volume=event.volume,
        vwap=event.vwap,
        trade_count=event.trade_count,
        is_final=True,
        revision=event.revision,
        recorded_at=event.ingested_at,
    )


def _build_event(
    bar_event: BarEvent,
    *,
    instrument_id: uuid.UUID,
    detection: _Detection,
    features: dict[str, Decimal],
) -> AnomalyEvent:
    window_start = bar_event.bar_start - bar_event.interval.duration
    window_end = bar_event.bar_start
    return AnomalyEvent(
        event_id=deterministic_event_id(
            *AnomalyEvent.natural_key(
                source=bar_event.source,
                symbol=bar_event.symbol,
                model_name=detection.model_name,
                model_version=detection.model_version,
                window_start=window_start,
                window_end=window_end,
            )
        ),
        symbol=bar_event.symbol,
        occurred_at=window_end,
        ingested_at=dt.datetime.now(dt.UTC),
        source=bar_event.source,
        producer="anomaly-detector@1.0",
        correlation_id=bar_event.correlation_id,
        window_start=window_start,
        anomaly_score=detection.anomaly_score,
        threshold=detection.threshold,
        severity=detection.severity,
        model_name=detection.model_name,
        model_version=detection.model_version,
        features=features,
    )


async def run_anomaly_detector(
    settings: Settings,
    *,
    max_batches: int | None = None,
    stop: asyncio.Event | None = None,
    database: Database | None = None,
) -> AnomalyDetectionService:
    """Consume bars and run the Anomaly Detection Service until cancelled, `stop`, or
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
            consumer_group=settings.kafka.kafka_anomaly_detector_group,
            client_id="tradewatch-anomaly-detector-dlq",
        )

    consumer = KafkaEventConsumer(
        settings.kafka,
        topics=topics,
        dead_letters=dead_letters,
        group_id=settings.kafka.kafka_anomaly_detector_group,
        subscribe_to=(topics.bars_1m,),
    )
    publisher = KafkaEventPublisher(settings.kafka, topics=topics)
    service = AnomalyDetectionService(consumer, database, publisher, settings=settings)

    await consumer.start()
    await publisher.start()
    await service.warm_up()
    log.info(
        "anomaly_detector.starting",
        group_id=consumer.group_id,
        topics=list(consumer.subscribed),
        mode=settings.anomaly.anomaly_detector.value,
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

    log.info("anomaly_detector.stopped")
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
    start_metrics_server("anomaly-detector", settings.app)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signal_name in ("SIGINT", "SIGTERM"):
            handler = getattr(signal, signal_name, None)
            if handler is None:
                continue
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(handler, stop.set)
        await run_anomaly_detector(settings, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("anomaly_detector.interrupted")
    except Exception as error:
        log.error("anomaly_detector.failed", error=str(error), error_type=type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
