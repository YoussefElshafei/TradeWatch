"""Train the anomaly detection model tier (ADR-0008): an `IsolationForest` over historical
1-minute bars, evaluated against the robust z-score baseline before it earns a place ahead
of it.

**Training data separated from evaluation data.** The feature rows built from `--start` to
`--end` are split chronologically — the first `--split-fraction` for training, the rest held
out — never shuffled, because these are a time series and a model trained on data adjacent to
what it is evaluated on would be evaluated on a much easier problem than serving ever poses.

**No accuracy number, because there are no labels.** This is unsupervised: nothing in this
database has ever been confirmed as (or ruled out as) an anomaly. What this script reports
instead — synthetic-anomaly detection rate and false-positive rate on quiet held-out data,
for both the model and the baseline — is what ADR-0008 asks for in place of a fabricated
precision/recall figure.

**The model only goes live if it beats the baseline.** "Beats" means: at least as good a
synthetic-detection rate, and no worse a false-positive rate. If it does not, the trained
version is still recorded (so the run is not lost), but `is_active` stays false and the
baseline keeps serving — an acceptable, reportable outcome, not a failure to hide.

Usage::

    uv run python scripts/train_anomaly_model.py --start 2026-01-01 --end 2026-06-01
    uv run python scripts/train_anomaly_model.py --symbols AAPL,MSFT --split-fraction 0.75
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sys
import uuid
from contextlib import AbstractAsyncContextManager
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from sklearn.ensemble import IsolationForest
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.application.detection.baseline import initial_baseline_state, score_baseline
from tradewatch.application.detection.features import initial_feature_state
from tradewatch.application.detection.model import (
    MODEL_FEATURE_NAMES,
    AnomalyModel,
    evaluate_against_baseline,
    inject_synthetic_anomalies,
    model_hyperparameters,
    save_model,
    to_vector,
)
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.database.models import Bar as BarRow
from tradewatch.infrastructure.database.models import ModelVersion
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.repositories import (
    BarRepository,
    InstrumentRepository,
    ModelVersionRepository,
)

__all__ = [
    "SessionSource",
    "build_feature_rows",
    "main",
    "train_and_persist",
    "train_one_model",
]

_MAX_BARS_PER_SYMBOL = 200_000
_DEFAULT_MODEL_NAME = "isolation-forest"


class SessionSource(Protocol):
    """Same role as `workers/backtest_runner.py`'s: a test satisfies this with a fake that
    shares its own transactional session, so the training path is exercised against a real
    database without a second, independently-committing connection.
    """

    def session(self) -> AbstractAsyncContextManager[AsyncSession]: ...


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


def build_feature_rows(bars: list[Bar], *, window_bars: int) -> list[dict[str, Decimal]]:
    """Replay one symbol's bars, in order, through the shared feature module.

    Returns only the feature vectors — every bar's, not just the ones a scoreable window
    eventually produces — with `{}` for a bar too early to have any features yet. The caller
    decides which rows are complete enough to train or evaluate on; this function's only job
    is to run the exact same fold a live detector would.
    """
    state = initial_feature_state(window_bars)
    rows: list[dict[str, Decimal]] = []
    for bar in bars:
        state, features = state.update(bar)
        rows.append(features)
    return rows


def _split(
    rows: list[dict[str, Decimal]], *, split_fraction: float
) -> tuple[list[dict[str, Decimal]], list[dict[str, Decimal]]]:
    cut = int(len(rows) * split_fraction)
    return rows[:cut], rows[cut:]


def _flags_from_baseline(
    rows: list[dict[str, Decimal]],
    *,
    window_bars: int,
    min_bars_before_scoring: int,
    threshold: Decimal,
) -> list[bool]:
    state = initial_baseline_state(
        window_bars=window_bars, min_bars_before_scoring=min_bars_before_scoring
    )
    flags: list[bool] = []
    for row in rows:
        state, result = score_baseline(state, row, threshold=threshold)
        flags.append(result is not None)
    return flags


def _flags_from_model(
    model: AnomalyModel, rows: list[dict[str, Decimal]], *, threshold: Decimal
) -> list[bool]:
    flags: list[bool] = []
    for row in rows:
        score = model.score(row)
        flags.append(score is not None and score > threshold)
    return flags


def train_one_model(
    train_rows: list[dict[str, Decimal]],
    held_out_rows: list[dict[str, Decimal]],
    *,
    settings: Settings,
    n_estimators: int,
    contamination: float | str,
    random_state: int,
) -> tuple[AnomalyModel, dict[str, object]]:
    """Fit the estimator and evaluate it against the baseline on held-out data.

    Returns the model and a JSON-safe `offline_metrics` dict — everything
    `ModelVersion.offline_metrics` needs, computed once, here, rather than recomputed by
    whoever reads the row later.
    """
    complete_train = [vector for row in train_rows if (vector := to_vector(row)) is not None]
    if len(complete_train) < 10:
        raise ValueError(
            f"only {len(complete_train)} complete training rows (need at least 10); "
            "widen --start/--end or lower --min-bars-before-scoring"
        )

    estimator = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        random_state=random_state,
    )
    estimator.fit([[float(value) for value in vector] for vector in complete_train])
    model = AnomalyModel(estimator=estimator, feature_names=MODEL_FEATURE_NAMES)

    # sklearn's own inlier/outlier boundary (`score_samples(X) - offset_`, compared to 0),
    # translated into this module's sign convention (`AnomalyModel.score` negates
    # `score_samples`) — the threshold that reproduces exactly the split `contamination`
    # asked for, not a second, independently-guessed one.
    model_threshold = Decimal(str(-estimator.offset_))

    injected_held_out, is_injected = inject_synthetic_anomalies(held_out_rows)
    model_flags = _flags_from_model(model, injected_held_out, threshold=model_threshold)
    baseline_flags = _flags_from_baseline(
        injected_held_out,
        window_bars=settings.anomaly.anomaly_window_bars,
        min_bars_before_scoring=settings.anomaly.anomaly_min_bars_before_scoring,
        threshold=Decimal(str(settings.anomaly.anomaly_baseline_z_threshold)),
    )

    model_eval = evaluate_against_baseline(flags=model_flags, is_injected=is_injected)
    baseline_eval = evaluate_against_baseline(flags=baseline_flags, is_injected=is_injected)

    metrics: dict[str, object] = {
        "held_out_rows": len(held_out_rows),
        "complete_training_rows": len(complete_train),
        "model_threshold": str(model_threshold),
        "model": {
            "synthetic_detection_rate": model_eval.synthetic_detection_rate,
            "false_positive_rate": model_eval.false_positive_rate,
        },
        "baseline": {
            "synthetic_detection_rate": baseline_eval.synthetic_detection_rate,
            "false_positive_rate": baseline_eval.false_positive_rate,
        },
        "beats_baseline": (
            model_eval.synthetic_detection_rate >= baseline_eval.synthetic_detection_rate
            and model_eval.false_positive_rate <= baseline_eval.false_positive_rate
        ),
    }
    return model, metrics


async def _run(
    *,
    symbols: list[str],
    start: dt.datetime,
    end: dt.datetime,
    split_fraction: float,
    n_estimators: int,
    contamination: float | str,
    random_state: int,
    model_name: str,
    version: str,
    output: Path,
) -> int:
    load_env_file()
    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    database = Database(settings.database)
    await database.connect()
    try:
        return await train_and_persist(
            database,
            settings=settings,
            symbols=symbols,
            start=start,
            end=end,
            split_fraction=split_fraction,
            n_estimators=n_estimators,
            contamination=contamination,
            random_state=random_state,
            model_name=model_name,
            version=version,
            output=output,
        )
    finally:
        await database.disconnect()


async def train_and_persist(
    database: SessionSource,
    *,
    settings: Settings,
    symbols: list[str],
    start: dt.datetime,
    end: dt.datetime,
    split_fraction: float,
    n_estimators: int,
    contamination: float | str,
    random_state: int,
    model_name: str,
    version: str,
    output: Path,
) -> int:
    """The whole training run against an already-connected database: load bars, build
    features, train, evaluate, and persist. Split out of `_run` so a test can drive it with
    a `SessionSource` that shares the test's own transactional session — the same reason
    `workers/backtest_runner.py`'s `process_one_backtest` and
    `scripts/rebuild_positions.py`'s `rebuild_one_portfolio` are split the same way.
    """
    async with database.session() as session:
        instrument_repository = InstrumentRepository(session)
        bar_repository = BarRepository(session)
        symbol_to_instrument = await instrument_repository.get_many_by_symbol(symbols)

        train_rows: list[dict[str, Decimal]] = []
        held_out_rows: list[dict[str, Decimal]] = []
        for symbol in symbols:
            instrument = symbol_to_instrument.get(symbol)
            if instrument is None:
                print(f"skipping unknown symbol {symbol!r}", file=sys.stderr)
                continue
            rows = await bar_repository.list_range(
                instrument.id, BarInterval.ONE_MINUTE, start, end, limit=_MAX_BARS_PER_SYMBOL
            )
            bars = [_to_domain_bar(row) for row in rows]
            if not bars:
                print(f"no bars for {symbol!r} in the requested range", file=sys.stderr)
                continue
            features = build_feature_rows(bars, window_bars=settings.anomaly.anomaly_window_bars)
            # Each symbol is split on its own, then the splits are pooled — pooling first and
            # splitting once would let a later symbol's earliest bars land in an earlier
            # symbol's "training" split by coincidence of list order, which is not a
            # time-respecting split at all.
            symbol_train, symbol_held_out = _split(features, split_fraction=split_fraction)
            train_rows.extend(symbol_train)
            held_out_rows.extend(symbol_held_out)

        if not train_rows:
            print("No bars found for any requested symbol/range.", file=sys.stderr)
            return 1

        model, metrics = train_one_model(
            train_rows,
            held_out_rows,
            settings=settings,
            n_estimators=n_estimators,
            contamination=contamination,
            random_state=random_state,
        )

        save_model(model, output)

        version_row = ModelVersion(
            id=uuid.uuid4(),
            model_name=model_name,
            version=version,
            trained_at=dt.datetime.now(dt.UTC),
            training_window_start=start,
            training_window_end=end,
            hyperparameters=model_hyperparameters(model.estimator),
            offline_metrics=metrics,
            artifact_uri=str(output),
            is_active=False,
        )
        repository = ModelVersionRepository(session)
        await repository.add(version_row)

        beats_baseline = bool(metrics["beats_baseline"])
        if beats_baseline:
            await repository.activate(version_row)

        await session.commit()

    print(f"Trained {model_name!r} version {version!r}, {len(train_rows)} training rows, ")
    print(f"{len(held_out_rows)} held-out rows. Artifact: {output}")
    print(f"Model:    {metrics['model']}")
    print(f"Baseline: {metrics['baseline']}")
    print(
        f"{'Beats' if beats_baseline else 'Does not beat'} the baseline — "
        f"{'activated' if beats_baseline else 'baseline remains active'}."
    )
    return 0


def _parse_datetime(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=dt.UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=_parse_datetime, required=True)
    parser.add_argument("--end", type=_parse_datetime, required=True)
    parser.add_argument(
        "--symbols", type=lambda value: [item.strip() for item in value.split(",")], default=None
    )
    parser.add_argument("--split-fraction", type=float, default=0.8)
    parser.add_argument("--n-estimators", type=int, default=100)
    parser.add_argument("--contamination", default="auto")
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--model-name", default=_DEFAULT_MODEL_NAME)
    parser.add_argument("--version", default=None)
    parser.add_argument("--output", type=Path, default=Path("models") / "isolation-forest.joblib")
    args = parser.parse_args(argv)

    contamination: float | str = args.contamination
    if contamination != "auto":
        contamination = float(contamination)

    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2
    symbols = args.symbols or list(settings.market.symbols)
    version = args.version or dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")

    return asyncio.run(
        _run(
            symbols=symbols,
            start=args.start,
            end=args.end,
            split_fraction=args.split_fraction,
            n_estimators=args.n_estimators,
            contamination=contamination,
            random_state=args.random_state,
            model_name=args.model_name,
            version=version,
            output=args.output,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
