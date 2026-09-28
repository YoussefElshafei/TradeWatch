"""`scripts/train_anomaly_model.py` against a real database: the whole offline pipeline, from
stored bars to a persisted `ModelVersion` and a loadable artifact.

Uses the same `_FakeDatabase`-over-the-test-session pattern as `test_rebuild_positions.py`:
`train_and_persist` takes a `SessionSource` protocol, not the concrete `Database`, so the
test can hand it the test's own transactional session.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import SecretStr
from scripts.train_anomaly_model import train_and_persist
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.application.detection.model import load_model
from tradewatch.config.settings import (
    AnomalySettings,
    DatabaseSettings,
    RedisSettings,
    SecuritySettings,
    load_settings,
)
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.database.models import Bar, Instrument, ModelVersion

pytestmark = pytest.mark.integration

_START = dt.datetime(2026, 5, 1, 14, 0, tzinfo=dt.UTC)
_CYCLE = ("100.00", "100.50", "101.00")


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


async def _seed_bars(session: AsyncSession, instrument: Instrument, count: int) -> None:
    for minute in range(count):
        price = Decimal(_CYCLE[minute % 3])
        start = _START + dt.timedelta(minutes=minute)
        session.add(
            Bar(
                instrument_id=instrument.id,
                interval=BarInterval.ONE_MINUTE,
                bar_start=start,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=Decimal(1000),
                vwap=price,
                trade_count=10,
                is_final=True,
                revision=0,
            )
        )
    await session.flush()


def _settings(*, window_bars: int, min_bars_before_scoring: int):
    return load_settings(
        anomaly=AnomalySettings(
            anomaly_window_bars=window_bars,
            anomaly_min_bars_before_scoring=min_bars_before_scoring,
        ),
        database=DatabaseSettings(
            database_url="postgresql+asyncpg://tradewatch:tradewatch@127.0.0.1:1/tradewatch_unreachable"
        ),
        redis=RedisSettings(redis_url="redis://localhost:6379/1"),
        security=SecuritySettings(jwt_secret=SecretStr("test-secret-not-used-anywhere-real-01234")),
    )


class TestTrainAndPersist:
    async def test_a_full_run_persists_a_loadable_model_and_a_version_row(
        self, session: AsyncSession, instrument: Instrument, tmp_path: Path
    ) -> None:
        await _seed_bars(session, instrument, count=50)
        output = tmp_path / "model.joblib"
        settings = _settings(window_bars=5, min_bars_before_scoring=3)

        code = await train_and_persist(
            _FakeDatabase(session),
            settings=settings,
            symbols=[instrument.symbol],
            start=_START,
            end=_START + dt.timedelta(minutes=60),
            split_fraction=0.8,
            n_estimators=50,
            contamination="auto",
            random_state=42,
            model_name="isolation-forest",
            version="test-1",
            output=output,
        )

        assert code == 0
        assert output.exists()
        # The artifact is genuinely loadable and scoreable, not just a file that exists.
        model = load_model(output)
        zeroed = {
            "return": Decimal("0"),
            "volatility": Decimal("0"),
            "volume_z": Decimal("0"),
            "trade_count_z": Decimal("0"),
        }
        assert model.score(zeroed) is not None

        rows = (
            (await session.execute(select(ModelVersion).where(ModelVersion.version == "test-1")))
            .scalars()
            .all()
        )
        assert len(rows) == 1
        version_row = rows[0]
        assert version_row.model_name == "isolation-forest"
        assert version_row.artifact_uri == str(output)
        assert version_row.training_window_start == _START
        assert "model" in version_row.offline_metrics
        assert "baseline" in version_row.offline_metrics
        assert "beats_baseline" in version_row.offline_metrics
        # Activation must agree with the recorded comparison, whichever way it went — the
        # exact outcome depends on the random split/injection, which this test does not pin.
        assert version_row.is_active is bool(version_row.offline_metrics["beats_baseline"])

    async def test_activating_a_new_version_deactivates_the_previous_one(
        self, session: AsyncSession, instrument: Instrument, tmp_path: Path
    ) -> None:
        await _seed_bars(session, instrument, count=50)
        settings = _settings(window_bars=5, min_bars_before_scoring=3)
        common = {
            "settings": settings,
            "symbols": [instrument.symbol],
            "start": _START,
            "end": _START + dt.timedelta(minutes=60),
            "split_fraction": 0.8,
            "n_estimators": 50,
            "contamination": "auto",
            "random_state": 42,
            "model_name": "isolation-forest",
        }

        await train_and_persist(
            _FakeDatabase(session), version="v1", output=tmp_path / "v1.joblib", **common
        )
        await train_and_persist(
            _FakeDatabase(session), version="v2", output=tmp_path / "v2.joblib", **common
        )

        rows = (
            (
                await session.execute(
                    select(ModelVersion).where(ModelVersion.model_name == "isolation-forest")
                )
            )
            .scalars()
            .all()
        )
        active = [row for row in rows if row.is_active]
        # Both versions were trained on identical data with identical hyperparameters and a
        # fixed random_state, so they either both beat the baseline (leaving exactly the
        # newest one active — the partial-unique-index guarantee this test exists to check)
        # or neither does (leaving none active). Either is consistent; two simultaneously
        # active rows for the same model is the one outcome that must never happen.
        assert len(active) <= 1
        if active:
            assert active[0].version == "v2"

    async def test_an_unknown_symbol_is_skipped_not_fatal(
        self, session: AsyncSession, instrument: Instrument, tmp_path: Path
    ) -> None:
        await _seed_bars(session, instrument, count=50)
        settings = _settings(window_bars=5, min_bars_before_scoring=3)

        code = await train_and_persist(
            _FakeDatabase(session),
            settings=settings,
            symbols=["NOSUCHSYMBOL", instrument.symbol],
            start=_START,
            end=_START + dt.timedelta(minutes=60),
            split_fraction=0.8,
            n_estimators=50,
            contamination="auto",
            random_state=42,
            model_name="isolation-forest",
            version="test-unknown-symbol",
            output=tmp_path / "model.joblib",
        )

        assert code == 0

    async def test_no_bars_at_all_is_reported_not_a_crash(
        self, session: AsyncSession, instrument: Instrument, tmp_path: Path
    ) -> None:
        # No bars seeded at all.
        settings = _settings(window_bars=5, min_bars_before_scoring=3)

        code = await train_and_persist(
            _FakeDatabase(session),
            settings=settings,
            symbols=[instrument.symbol],
            start=_START,
            end=_START + dt.timedelta(minutes=60),
            split_fraction=0.8,
            n_estimators=50,
            contamination="auto",
            random_state=42,
            model_name="isolation-forest",
            version="test-empty",
            output=tmp_path / "model.joblib",
        )

        assert code == 1
