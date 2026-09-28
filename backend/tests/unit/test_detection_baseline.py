"""The baseline detector (ADR-0008): the robust z-score math, the two warm-up thresholds,
and that a synthetic anomaly injected into otherwise-quiet data is actually flagged — the
exit criterion this suite exists to prove.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tradewatch.application.detection.baseline import (
    BaselineState,
    initial_baseline_state,
    score_baseline,
)
from tradewatch.domain.enums import AlertSeverity

pytestmark = pytest.mark.unit

_THRESHOLD = Decimal("3.5")


def _feed(
    state: BaselineState, values: list[str], *, feature: str = "return"
) -> tuple[BaselineState, object]:
    """Fold a series of single-feature observations through the baseline. Returns the final
    (state, result) pair — `result` is whatever the *last* value produced.
    """
    result = None
    for value in values:
        state, result = score_baseline(state, {feature: Decimal(value)}, threshold=_THRESHOLD)
    return state, result


def _quiet(n: int) -> list[str]:
    """`n` values cycling through 0.99, 1.00, 1.01 — median 1.00, MAD 0.01, and (unlike a
    two-value 50/50 split) both stay exactly there even after one more extreme value joins
    the sample: the two outer thirds are wide enough that the sample's middle index still
    falls inside the "1.00" block whether the total count is `n` or `n + 1`. An all-identical
    baseline has MAD 0, which `TestZeroMad` below covers on its own — this generator is for
    every test that needs a stable, hand-checkable scale to measure a real deviation against.

    With median 1.00 and MAD 0.01, a value `V`'s robust z-score is
    `abs(V - 1.00) / (0.01 * 1.4826)` — every threshold value below is picked by solving
    that equation, not guessed.
    """
    cycle = ("0.99", "1.00", "1.01")
    return [cycle[i % 3] for i in range(n)]


class TestWarmUp:
    def test_nothing_is_reported_before_min_bars_before_scoring(self) -> None:
        state = initial_baseline_state(window_bars=20, min_bars_before_scoring=10)

        # Even a wild value: with fewer than 10 observations, there is nothing to compare it
        # against yet, so the baseline must stay silent rather than guess.
        _, result = _feed(state, [*_quiet(8), "1000"])

        assert result is None

    def test_scoring_starts_at_min_bars_before_scoring_not_at_the_full_window(self) -> None:
        state = initial_baseline_state(window_bars=20, min_bars_before_scoring=5)

        # Five quiet observations (== min_bars_before_scoring, well short of window_bars=20)
        # is enough to start scoring against.
        _, result = _feed(state, [*_quiet(5), "1000"])

        assert result is not None


class TestSyntheticAnomalyIsDetected:
    """The exit criterion: an injected synthetic anomaly is flagged by the baseline."""

    def test_an_extreme_value_after_a_quiet_run_is_flagged(self) -> None:
        state = initial_baseline_state(window_bars=30, min_bars_before_scoring=10)

        state, result = _feed(state, _quiet(30))
        assert result is None, "quiet, unremarkable data must not be flagged"

        _, spike_result = score_baseline(state, {"return": Decimal("50")}, threshold=_THRESHOLD)

        assert spike_result is not None
        assert spike_result.driving_feature == "return"
        assert spike_result.anomaly_score > _THRESHOLD

    def test_a_value_inside_the_normal_range_is_not_flagged(self) -> None:
        state = initial_baseline_state(window_bars=30, min_bars_before_scoring=10)
        state, _ = _feed(state, _quiet(30))

        # z = |1.005 - 1.00| / (0.01 * 1.4826) ≈ 0.34 — comfortably inside the quiet range.
        _, result = score_baseline(state, {"return": Decimal("1.005")}, threshold=_THRESHOLD)

        assert result is None


class TestZeroMad:
    def test_an_all_identical_history_scores_zero_not_a_division_by_zero(self) -> None:
        """A window with no variation at all has no scale to measure a deviation against.
        Reporting "not anomalous" is the honest answer — not a crash, and not a fabricated
        infinite score for the first bar that ever differs from a too-small sample.
        """
        state = initial_baseline_state(window_bars=10, min_bars_before_scoring=5)
        state, _ = _feed(state, ["1"] * 6)

        _, result = score_baseline(state, {"return": Decimal("1000")}, threshold=_THRESHOLD)

        assert result is None


class TestRobustness:
    """Why median/MAD, not mean/stdev: the module docstring's claim, checked directly."""

    def test_one_earlier_outlier_does_not_hide_the_next_one(self) -> None:
        """A mean/stdev baseline would have its own "normal" range dragged wide by the first
        outlier, making the second one look unremarkable by comparison. Median/MAD barely
        moves, so the second outlier is still caught.
        """
        state = initial_baseline_state(window_bars=21, min_bars_before_scoring=10)
        state, _ = _feed(state, _quiet(10))
        # One earlier extreme value enters the window ...
        state, first = score_baseline(state, {"return": Decimal("500")}, threshold=_THRESHOLD)
        assert first is not None

        # ... nine more quiet bars (window_bars=21 still holds the 500 alongside them) ...
        state, _ = _feed(state, _quiet(9))
        # ... and a second, smaller-but-still-genuine anomaly must still be caught.
        _, second = score_baseline(state, {"return": Decimal("20")}, threshold=_THRESHOLD)

        assert second is not None
        assert second.driving_feature == "return"


class TestExplainability:
    def test_the_result_carries_enough_to_explain_itself(self) -> None:
        state = initial_baseline_state(window_bars=30, min_bars_before_scoring=10)
        state, _ = _feed(state, _quiet(30))

        _, result = score_baseline(state, {"return": Decimal("50")}, threshold=_THRESHOLD)

        assert result is not None
        assert result.threshold == _THRESHOLD
        assert result.driving_feature == "return"
        assert result.anomaly_score > 0

    def test_only_the_single_most_extreme_feature_drives_the_score(self) -> None:
        """Several mildly-unusual features must not add up to a false "extreme" reading no
        one measurement actually supports — the score is a max, not a sum."""
        state = initial_baseline_state(window_bars=30, min_bars_before_scoring=10)
        quiet_return = _quiet(30)
        quiet_volume_z = _quiet(30)
        for return_value, volume_value in zip(quiet_return, quiet_volume_z, strict=True):
            state, _ = score_baseline(
                state,
                {"return": Decimal(return_value), "volume_z": Decimal(volume_value)},
                threshold=_THRESHOLD,
            )

        _, result = score_baseline(
            state,
            {"return": Decimal("1.005"), "volume_z": Decimal("50")},  # only volume_z spikes
            threshold=_THRESHOLD,
        )

        assert result is not None
        assert result.driving_feature == "volume_z"


class TestSeverity:
    def test_just_over_threshold_is_a_warning(self) -> None:
        state = initial_baseline_state(window_bars=30, min_bars_before_scoring=10)
        state, _ = _feed(state, _quiet(30))

        # z = |1.06 - 1.00| / (0.01 * 1.4826) ≈ 4.05 — just over the 3.5 threshold, and well
        # under the 1.5x-threshold (≈5.25) line into CRITICAL.
        _, result = score_baseline(state, {"return": Decimal("1.06")}, threshold=_THRESHOLD)

        assert result is not None
        assert result.severity is AlertSeverity.WARNING

    def test_far_over_threshold_is_critical(self) -> None:
        state = initial_baseline_state(window_bars=30, min_bars_before_scoring=10)
        state, _ = _feed(state, _quiet(30))

        _, result = score_baseline(state, {"return": Decimal("500")}, threshold=_THRESHOLD)

        assert result is not None
        assert result.severity is AlertSeverity.CRITICAL


class TestIndependentFeatureWindows:
    def test_a_feature_that_starts_late_is_not_penalized_for_a_short_history(self) -> None:
        """`spread` might not appear until bar 20 (no quote yet); it must warm up on its own
        schedule, not be judged against a window it was never present for."""
        state = initial_baseline_state(window_bars=30, min_bars_before_scoring=5)
        state, result = _feed(state, _quiet(20))
        assert result is None, "return alone is genuinely quiet the whole time"

        # `spread` appears for the first time now — must not be scoreable yet, even though
        # `return` already has 20 bars of history.
        _, result = score_baseline(
            state, {"return": Decimal("1.0"), "spread": Decimal("0.05")}, threshold=_THRESHOLD
        )
        assert result is None
