"""The shared feature vocabulary (ADR-0008): correctness of each of the five features, and
that missing input (no previous close, a cold window, no quote) produces an honestly partial
vector rather than a fabricated or crashing one.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from tradewatch.application.detection.features import (
    FEATURE_NAMES,
    RollingZScoreState,
    initial_feature_state,
)
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.ids import new_id

pytestmark = pytest.mark.unit

_INSTRUMENT_ID = new_id()
_START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)


def _bar(minute: int, *, close: str = "100", volume: str = "1000", trade_count: int = 10) -> Bar:
    price = Decimal(close)
    start = _START + dt.timedelta(minutes=minute)
    return Bar(
        instrument_id=_INSTRUMENT_ID,
        interval=BarInterval.ONE_MINUTE,
        bar_start=start,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal(volume),
        vwap=price,
        trade_count=trade_count,
        is_final=True,
        revision=0,
        recorded_at=start,
    )


class TestFeatureVocabulary:
    def test_five_names_matching_the_anomaly_settings_default(self) -> None:
        """`AnomalySettings.anomaly_features` and this vocabulary must never drift apart —
        that default is exactly this tuple, in this order."""
        assert FEATURE_NAMES == ("return", "volatility", "volume_z", "trade_count_z", "spread")


class TestReturn:
    def test_the_first_bar_has_no_return_there_is_nothing_to_compare_it_to(self) -> None:
        state = initial_feature_state(window=5)

        _, features = state.update(_bar(0, close="100"))

        assert "return" not in features

    def test_the_second_bar_has_a_correct_simple_return(self) -> None:
        state = initial_feature_state(window=5)
        state, _ = state.update(_bar(0, close="100"))

        _, features = state.update(_bar(1, close="110"))

        assert features["return"] == Decimal("0.1")

    def test_a_price_drop_is_a_negative_return(self) -> None:
        state = initial_feature_state(window=5)
        state, _ = state.update(_bar(0, close="100"))

        _, features = state.update(_bar(1, close="90"))

        assert features["return"] == Decimal("-0.1")


class TestVolatility:
    def test_absent_until_the_window_of_returns_fills(self) -> None:
        state = initial_feature_state(window=3)
        closes = ["100", "101", "102"]  # 2 returns from 3 closes — one short of window=3
        for minute, close in enumerate(closes):
            state, features = state.update(_bar(minute, close=close))
            assert "volatility" not in features

    def test_present_once_the_window_of_returns_fills(self) -> None:
        state = initial_feature_state(window=3)
        for minute, close in enumerate(["100", "101", "99", "103"]):
            state, features = state.update(_bar(minute, close=close))

        assert "volatility" in features
        assert features["volatility"] > 0


class TestVolumeAndTradeCountZScore:
    def test_absent_until_the_window_fills(self) -> None:
        state = initial_feature_state(window=4)
        for minute in range(3):
            state, features = state.update(_bar(minute, volume="1000"))
            assert "volume_z" not in features
            assert "trade_count_z" not in features

    def test_a_volume_spike_produces_a_large_positive_z(self) -> None:
        # A window this small could not produce z > 3 no matter how large the spike is: with
        # n-1 identical points and one outlier, the self-inclusive sample z-score of the
        # outlier is capped at (n-1)/sqrt(n) regardless of its magnitude (for n=20, ~4.25) —
        # the window has to be wide enough to leave headroom above the threshold this test
        # checks.
        window = 20
        state = initial_feature_state(window=window)
        for minute in range(window - 1):
            # Quiet bars, all identical volume ...
            state, features = state.update(_bar(minute, volume="1000"))
        # ... then one bar with ten times the volume.
        _, features = state.update(_bar(window - 1, volume="10000"))

        assert features["volume_z"] > 3

    def test_identical_history_scores_zero_not_a_division_by_zero(self) -> None:
        """`RollingZScoreState`'s stdev-is-zero guard, exercised end to end."""
        state = initial_feature_state(window=4)
        for minute in range(4):
            state, features = state.update(_bar(minute, volume="500", trade_count=5))

        assert features["volume_z"] == Decimal(0)
        assert features["trade_count_z"] == Decimal(0)

    def test_trade_count_is_scored_independently_of_volume(self) -> None:
        """One huge print (high volume, one trade) must not look like a busy market (many
        trades) — the two features have to be able to disagree."""
        window = 20
        state = initial_feature_state(window=window)
        for minute in range(window - 1):
            state, _ = state.update(_bar(minute, volume="1000", trade_count=10))

        _, features = state.update(_bar(window - 1, volume="50000", trade_count=10))

        assert features["volume_z"] > 3
        assert features["trade_count_z"] == Decimal(0)


class TestSpread:
    def test_absent_when_the_caller_has_no_quote(self) -> None:
        state = initial_feature_state(window=3)

        _, features = state.update(_bar(0))

        assert "spread" not in features

    def test_present_and_exact_when_the_caller_supplies_one(self) -> None:
        state = initial_feature_state(window=3)

        _, features = state.update(_bar(0), spread=Decimal("0.05"))

        assert features["spread"] == Decimal("0.05")


class TestRollingZScoreState:
    def test_the_window_is_bounded_old_values_fall_off(self) -> None:
        state = RollingZScoreState(window=3)
        for value in (Decimal(1), Decimal(2), Decimal(3), Decimal(4)):
            state, _ = state.update(value)

        assert state.values == (Decimal(2), Decimal(3), Decimal(4))
