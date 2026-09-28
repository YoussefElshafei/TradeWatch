"""The baseline detector: rolling median/MAD robust z-score (ADR-0008).

Ships before any model, on purpose. It needs no training, its arithmetic fits in one
sentence a user can be shown ("this bar's volume was N standard deviations outside its usual
range"), and it is the benchmark a fancier model has to beat before it is worth running at
all (`application.detection.model` compares against exactly this). It also never goes away:
if a trained model artifact is missing or fails to load, this is the fallback the detection
service runs instead of refusing to start (failure F17).

**Median and MAD, not mean and stdev.** A mean and a standard deviation are themselves
dragged around by the very outliers a detector is trying to find — one enormous print
inflates the "normal" range enough to hide the next one. The median and the median absolute
deviation barely move when a handful of points are extreme, which is the entire reason a
"robust" statistic is the word for this.

Two warm-up thresholds, both configuration, matching `AnomalySettings`:

* `window_bars` — how much history one feature's rolling statistics are computed over.
* `min_bars_before_scoring` — smaller than the window, and how soon scoring starts. Waiting
  for a full window (60 bars, by default) before producing anything would mean roughly an
  hour of silence on 1-minute bars every time a symbol starts being tracked; scoring against
  a partial window once there is *some* history is the honest trade-off, and the window
  still grows toward full size as more bars arrive.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Final

from tradewatch.domain.enums import AlertSeverity

__all__ = ["BaselineResult", "BaselineState", "score_baseline", "severity_for"]

#: The standard constant that makes MAD comparable to a standard deviation under a normal
#: distribution (`1 / Phi^-1(3/4)`) — what turns "a MAD" into "a robust sigma".
_MAD_SCALE: Final = Decimal("1.4826")


@dataclass(frozen=True, slots=True)
class BaselineState:
    """One rolling window of past values per feature, independent of each other.

    Independent windows, not one shared one, because features do not all become available
    at the same bar — `spread` depends on a quote having arrived, `volatility` needs its own
    warm-up — and a feature that started late must not be penalized by being compared against
    a window shorter than the bars it was actually present for.
    """

    window_bars: int
    min_bars_before_scoring: int
    history: dict[str, tuple[Decimal, ...]] = field(default_factory=dict)

    def update(self, features: dict[str, Decimal]) -> BaselineState:
        history = dict(self.history)
        for name, value in features.items():
            history[name] = (*history.get(name, ()), value)[-self.window_bars :]
        return BaselineState(self.window_bars, self.min_bars_before_scoring, history)


@dataclass(frozen=True, slots=True)
class BaselineResult:
    """A flagged bar. Never constructed for a bar that did not cross the threshold — the
    baseline reports anomalies, not a running score for every bar.
    """

    anomaly_score: Decimal
    threshold: Decimal
    severity: AlertSeverity
    #: The single feature whose deviation produced `anomaly_score`, so "why was this
    #: flagged" has a one-word answer before anyone opens the full `features` payload.
    driving_feature: str


def _robust_z(value: Decimal, sample: tuple[Decimal, ...]) -> Decimal | None:
    """Median/MAD z-score of `value` against `sample`, which already includes it."""
    floats = [float(item) for item in sample]
    median = Decimal(str(statistics.median(floats)))
    deviations = [abs(Decimal(str(item)) - median) for item in floats]
    mad = Decimal(str(statistics.median([float(d) for d in deviations])))
    if mad == 0:
        # Every value in the window is within rounding of the median, including this one:
        # nothing to be unusual relative to.
        return Decimal(0)
    return abs(value - median) / (mad * _MAD_SCALE)


def score_baseline(
    state: BaselineState,
    features: dict[str, Decimal],
    *,
    threshold: Decimal,
) -> tuple[BaselineState, BaselineResult | None]:
    """Fold one bar's features in and, if any of them crossed `threshold`, report it.

    The reported `anomaly_score` is the largest single-feature robust z-score this bar
    produced — not an average or a sum across features, both of which would let several
    mildly-unusual features add up to a false "extreme" reading no single measurement
    actually supports.
    """
    new_state = state.update(features)

    best_name: str | None = None
    best_z: Decimal | None = None
    for name, value in features.items():
        sample = new_state.history.get(name, ())
        if len(sample) < state.min_bars_before_scoring:
            continue
        z = _robust_z(value, sample)
        if z is None:
            continue
        if best_z is None or z > best_z:
            best_name, best_z = name, z

    if best_z is None or best_name is None or best_z <= threshold:
        return new_state, None

    return new_state, BaselineResult(
        anomaly_score=best_z,
        threshold=threshold,
        severity=severity_for(best_z, threshold),
        driving_feature=best_name,
    )


def severity_for(score: Decimal, threshold: Decimal) -> AlertSeverity:
    """Both boundaries are guesses until calibrated against real data (ADR-0008); 1.5x the
    flag threshold as the line between "warning" and "critical" is a starting point, not a
    claim. Shared by both tiers (`workers.anomaly_detector` uses this for the model's score
    too) so a detection's severity means the same thing regardless of which tier produced it.
    """
    return AlertSeverity.CRITICAL if score >= threshold * Decimal("1.5") else AlertSeverity.WARNING


def initial_baseline_state(*, window_bars: int, min_bars_before_scoring: int) -> BaselineState:
    """A fresh, empty state. Mirrors `application.detection.features.initial_feature_state`."""
    return BaselineState(window_bars=window_bars, min_bars_before_scoring=min_bars_before_scoring)
