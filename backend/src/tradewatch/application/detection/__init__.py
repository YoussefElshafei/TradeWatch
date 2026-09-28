"""Anomaly detection: features, the robust z-score baseline, and the model interface.

ADR-0008's wording constraint applies to every module under this package, including comments
and variable names: this component finds statistically unusual market activity relative to a
symbol's own recent behaviour, and that is the only claim any code, comment, or docstring
under this package may make about what it does. See ADR-0008 for the full reasoning and the
handful of words that constraint rules out.
"""

from __future__ import annotations

from tradewatch.application.detection.baseline import (
    BaselineResult,
    BaselineState,
    initial_baseline_state,
    score_baseline,
    severity_for,
)
from tradewatch.application.detection.features import (
    FEATURE_NAMES,
    FeatureState,
    initial_feature_state,
)

__all__ = [
    "FEATURE_NAMES",
    "BaselineResult",
    "BaselineState",
    "FeatureState",
    "initial_baseline_state",
    "initial_feature_state",
    "score_baseline",
    "severity_for",
]
