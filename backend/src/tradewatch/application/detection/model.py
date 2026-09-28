"""The model tier: a trained `IsolationForest` (ADR-0008), and the evaluation math the
offline training pipeline (`backend/scripts/train_anomaly_model.py`) uses to decide whether
it earned a place ahead of the baseline.

**Four features, not five.** `spread` needs a quote joined to each bar, which this training
pipeline does not do (`ARCHITECTURE.md` §3.7 lists the detector's inputs as "bars, optionally
trades" — no quotes). Training on a feature that is essentially always missing would mean
dropping almost every row, so the model is trained and served on the four features that come
from bars alone. The baseline is not affected: it scores each feature independently and
`spread` still contributes there whenever a live quote happens to be available (see
`application.detection.baseline`).

**Higher score means more anomalous, for both tiers.** `IsolationForest.score_samples`
returns the opposite convention (higher means more normal), so this module negates it —
`AnomalyModel.score` and `application.detection.baseline`'s z-score agree on which direction
"more unusual" points.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

__all__ = [
    "MODEL_FEATURE_NAMES",
    "AnomalyModel",
    "EvaluationResult",
    "evaluate_against_baseline",
    "inject_synthetic_anomalies",
    "load_model",
    "save_model",
    "to_vector",
]

#: The features the *model* tier trains and scores on — see the module docstring for why
#: this is four, not `application.detection.features.FEATURE_NAMES`'s five.
MODEL_FEATURE_NAMES: Final[tuple[str, ...]] = ("return", "volatility", "volume_z", "trade_count_z")


def to_vector(
    features: Mapping[str, Decimal], names: Sequence[str] = MODEL_FEATURE_NAMES
) -> tuple[Decimal, ...] | None:
    """`features` reduced to a fixed-order vector, or `None` if any named feature is missing.

    A model needs every row to have the same shape; a partial vector cannot be scored and is
    not padded or imputed — a fabricated feature value would make the score meaningless
    rather than merely absent.
    """
    if not all(name in features for name in names):
        return None
    return tuple(features[name] for name in names)


@dataclass(frozen=True, slots=True)
class AnomalyModel:
    """A trained `IsolationForest` plus the feature order it expects.

    The feature order travels with the model (not just the estimator) because a model
    artifact loaded months after training must not silently score columns in the wrong
    order if `MODEL_FEATURE_NAMES` is ever reordered.
    """

    estimator: IsolationForest
    feature_names: tuple[str, ...]

    def score(self, features: Mapping[str, Decimal]) -> Decimal | None:
        """The model's anomaly score for one bar's features, or `None` if a needed feature
        is missing (still warming up, most commonly).
        """
        vector = to_vector(features, self.feature_names)
        if vector is None:
            return None
        matrix = np.array([[float(value) for value in vector]])
        # `score_samples`: higher means more normal. Negated so both tiers agree that a
        # larger `anomaly_score` means more unusual — see the module docstring.
        raw = float(self.estimator.score_samples(matrix)[0])
        return Decimal(str(-raw))


def save_model(model: AnomalyModel, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"estimator": model.estimator, "feature_names": model.feature_names}, path)


def load_model(path: Path) -> AnomalyModel:
    """Raises whatever `joblib.load` raises on a missing or corrupt file — a bare
    `FileNotFoundError` or an unpickling error, both permanent. The caller (the detection
    service) is where F17's fallback to the baseline belongs, not here: this function's job
    is to load a model, not to decide what running without one means.
    """
    payload = joblib.load(path)
    return AnomalyModel(estimator=payload["estimator"], feature_names=payload["feature_names"])


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    """One detector's behaviour on held-out data — the numbers ADR-0008 asks for instead of
    an accuracy figure a method with no labels cannot honestly report.
    """

    #: Fraction of *injected* synthetic anomalies this detector flagged.
    synthetic_detection_rate: float
    #: Fraction of *real, unmodified* held-out rows this detector flagged. A method that
    #: flags a large slice of ordinary, unremarkable trading is not usable regardless of how
    #: well it catches injected spikes.
    false_positive_rate: float
    flagged_count: int
    total_count: int


def inject_synthetic_anomalies(
    rows: Sequence[Mapping[str, Decimal]],
    *,
    fraction: float = 0.05,
    multiplier: Decimal = Decimal(20),
    seed: int = 0,
) -> tuple[list[dict[str, Decimal]], list[bool]]:
    """Copy `rows`, replacing a random `fraction` of them with an exaggerated version of
    themselves, and return which rows were modified.

    Multiplying every present feature by `multiplier` is a deliberately crude synthetic
    anomaly — the point is a row a detector *should* find obviously unlike its neighbours,
    not a realistic one. A detector that cannot catch this could not be trusted to catch a
    subtler real one either.
    """
    rng = np.random.default_rng(seed)
    injected_indices = set(
        rng.choice(len(rows), size=max(1, int(len(rows) * fraction)), replace=False).tolist()
    )
    modified: list[dict[str, Decimal]] = []
    is_injected: list[bool] = []
    for index, row in enumerate(rows):
        if index in injected_indices:
            modified.append({name: value * multiplier for name, value in row.items()})
            is_injected.append(True)
        else:
            modified.append(dict(row))
            is_injected.append(False)
    return modified, is_injected


def evaluate_against_baseline(
    *,
    flags: Sequence[bool],
    is_injected: Sequence[bool],
) -> EvaluationResult:
    """Turn a detector's per-row flag/no-flag verdicts into the two rates that matter.

    Takes the verdicts rather than the detector itself, so the identical function scores the
    baseline and the model — the thing ADR-0008 requires ("the model must beat the baseline,
    measured ... and the comparison is written down") only means something if both sides are
    reduced to numbers the same way.
    """
    if len(flags) != len(is_injected):
        raise ValueError("flags and is_injected must be the same length")

    paired = list(zip(flags, is_injected, strict=True))
    injected_total = sum(is_injected)
    injected_caught = sum(1 for flag, injected in paired if flag and injected)
    real_total = len(is_injected) - injected_total
    real_flagged = sum(1 for flag, injected in paired if flag and not injected)

    return EvaluationResult(
        synthetic_detection_rate=(injected_caught / injected_total) if injected_total else 0.0,
        false_positive_rate=(real_flagged / real_total) if real_total else 0.0,
        flagged_count=sum(flags),
        total_count=len(flags),
    )


def model_hyperparameters(estimator: IsolationForest) -> dict[str, Any]:
    """The subset of `IsolationForest`'s parameters worth recording — enough to reproduce
    the training run, not every scikit-learn default that never changed.
    """
    return {
        "n_estimators": estimator.n_estimators,
        "contamination": (
            estimator.contamination
            if isinstance(estimator.contamination, str)
            else float(estimator.contamination)
        ),
        "max_samples": estimator.max_samples,
        "random_state": estimator.random_state,
        "features": list(MODEL_FEATURE_NAMES),
    }
