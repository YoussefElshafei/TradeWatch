"""The model tier's pure pieces: turning a feature dict into a fixed-order vector, injecting
synthetic anomalies for evaluation, and reducing flag/no-flag verdicts to the two rates
ADR-0008 asks for. None of this touches scikit-learn directly — `to_vector`,
`inject_synthetic_anomalies`, and `evaluate_against_baseline` are all plain data
transformations, testable without fitting a model.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tradewatch.application.detection.model import (
    MODEL_FEATURE_NAMES,
    evaluate_against_baseline,
    inject_synthetic_anomalies,
    to_vector,
)

pytestmark = pytest.mark.unit


class TestToVector:
    def test_a_complete_row_becomes_a_vector_in_the_configured_order(self) -> None:
        features = {
            "trade_count_z": Decimal("0.1"),
            "return": Decimal("0.01"),
            "volatility": Decimal("0.02"),
            "volume_z": Decimal("1.5"),
        }

        vector = to_vector(features)

        assert vector == tuple(features[name] for name in MODEL_FEATURE_NAMES)

    def test_a_row_missing_a_required_feature_is_not_scoreable(self) -> None:
        features = {"return": Decimal("0.01"), "volatility": Decimal("0.02")}

        assert to_vector(features) is None

    def test_an_extra_unrequested_feature_is_ignored_not_rejected(self) -> None:
        features = {
            "return": Decimal("0.01"),
            "volatility": Decimal("0.02"),
            "volume_z": Decimal("1.5"),
            "trade_count_z": Decimal("0.1"),
            "spread": Decimal("0.05"),  # present, but not one of MODEL_FEATURE_NAMES
        }

        assert to_vector(features) is not None

    def test_a_caller_supplied_name_order_is_honoured(self) -> None:
        features = {"a": Decimal(1), "b": Decimal(2)}

        assert to_vector(features, names=("b", "a")) == (Decimal(2), Decimal(1))


class TestInjectSyntheticAnomalies:
    def test_the_row_count_is_unchanged(self) -> None:
        rows = [{"return": Decimal(1)} for _ in range(50)]

        modified, is_injected = inject_synthetic_anomalies(rows, fraction=0.1)

        assert len(modified) == len(rows) == len(is_injected)

    def test_roughly_the_requested_fraction_is_marked_injected(self) -> None:
        rows = [{"return": Decimal(1)} for _ in range(100)]

        _, is_injected = inject_synthetic_anomalies(rows, fraction=0.1)

        assert sum(is_injected) == 10

    def test_an_injected_row_is_the_original_scaled_by_the_multiplier(self) -> None:
        rows = [{"return": Decimal(2)} for _ in range(20)]

        modified, is_injected = inject_synthetic_anomalies(
            rows, fraction=0.5, multiplier=Decimal(10)
        )

        for row, injected in zip(modified, is_injected, strict=True):
            assert row["return"] == (Decimal(20) if injected else Decimal(2))

    def test_an_untouched_row_is_identical_to_the_original(self) -> None:
        rows = [{"return": Decimal("1.23")} for _ in range(10)]

        modified, is_injected = inject_synthetic_anomalies(rows, fraction=0.3)

        for row, injected in zip(modified, is_injected, strict=True):
            if not injected:
                assert row == {"return": Decimal("1.23")}

    def test_the_same_seed_produces_the_same_injection_set(self) -> None:
        rows = [{"return": Decimal(i)} for i in range(30)]

        _, first = inject_synthetic_anomalies(rows, fraction=0.2, seed=7)
        _, second = inject_synthetic_anomalies(rows, fraction=0.2, seed=7)

        assert first == second


class TestEvaluateAgainstBaseline:
    def test_perfect_detection_and_no_false_positives(self) -> None:
        is_injected = [True, True, False, False, False]
        flags = [True, True, False, False, False]

        result = evaluate_against_baseline(flags=flags, is_injected=is_injected)

        assert result.synthetic_detection_rate == 1.0
        assert result.false_positive_rate == 0.0
        assert result.flagged_count == 2
        assert result.total_count == 5

    def test_missing_every_injected_anomaly(self) -> None:
        is_injected = [True, True, False, False]
        flags = [False, False, False, False]

        result = evaluate_against_baseline(flags=flags, is_injected=is_injected)

        assert result.synthetic_detection_rate == 0.0

    def test_flagging_real_quiet_rows_is_a_false_positive(self) -> None:
        is_injected = [False, False, False, False]
        flags = [True, False, False, False]

        result = evaluate_against_baseline(flags=flags, is_injected=is_injected)

        assert result.false_positive_rate == 0.25

    def test_no_injected_rows_gives_a_defined_zero_rate_not_a_division_by_zero(self) -> None:
        result = evaluate_against_baseline(flags=[False, False], is_injected=[False, False])

        assert result.synthetic_detection_rate == 0.0

    def test_mismatched_lengths_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="same length"):
            evaluate_against_baseline(flags=[True], is_injected=[True, False])
