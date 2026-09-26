"""The indicator library, checked against hand computation.

Every expected value below is worked out with the documented formula on paper (or in the
test's own comment), never by running the code and trusting its answer — that is the
difference between a test and a tautology.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tradewatch.application.strategy.indicators import (
    AtrState,
    EmaState,
    RsiState,
    SmaState,
    VolatilityState,
    atr,
    ema,
    rolling_volatility,
    rsi,
    sma,
)

pytestmark = pytest.mark.unit


def _d(*values: int | float | str) -> list[Decimal]:
    return [Decimal(str(value)) for value in values]


class TestSma:
    def test_hand_computed_period_3(self) -> None:
        # [1,2,3,4,5], period 3: mean(1,2,3)=2, mean(2,3,4)=3, mean(3,4,5)=4.
        values = _d(1, 2, 3, 4, 5)
        assert sma(values, 3) == [None, None, Decimal(2), Decimal(3), Decimal(4)]

    def test_rejects_non_positive_period(self) -> None:
        with pytest.raises(ValueError, match="period"):
            sma(_d(1, 2), 0)

    def test_incremental_matches_batch(self) -> None:
        values = _d(1, 2, 3, 4, 5, 6, 7)
        expected = sma(values, 3)
        state = SmaState(3)
        actual: list[Decimal | None] = []
        for value in values:
            state, current = state.update(value)
            actual.append(current)
        assert actual == expected

    def test_state_is_immutable_across_updates(self) -> None:
        """`update` must not mutate `self` — a shared prior state would corrupt every
        strategy still holding a reference to it (the whole reason states are frozen)."""
        state = SmaState(2)
        state, _ = state.update(Decimal(1))
        before = state
        state.update(Decimal(2))
        assert state == before


class TestEma:
    def test_hand_computed_period_3(self) -> None:
        # Seed = mean(1,2,3) = 2. alpha = 2/4 = 0.5.
        # ema[3] (value=4) = (4-2)*0.5 + 2 = 3
        # ema[4] (value=5) = (5-3)*0.5 + 3 = 4
        values = _d(1, 2, 3, 4, 5)
        result = ema(values, 3)
        assert result[:2] == [None, None]
        assert result[2] == Decimal(2)
        assert result[3] == Decimal(3)
        assert result[4] == Decimal(4)

    def test_incremental_matches_batch(self) -> None:
        values = _d(10, 11, 9, 12, 14, 13, 15, 16)
        expected = ema(values, 4)
        alpha = Decimal(2) / Decimal(5)
        state = EmaState(4, alpha)
        actual: list[Decimal | None] = []
        for value in values:
            state, current = state.update(value)
            actual.append(current)
        assert actual == expected


class TestRsi:
    def test_hand_computed_period_3_all_gains_is_100(self) -> None:
        # Strictly rising prices: every change is a gain, avg_loss stays 0 forever, so RSI
        # is defined as 100 rather than dividing by zero.
        values = _d(1, 2, 3, 4, 5)
        result = rsi(values, 3)
        assert result[0] is None  # no previous close yet
        assert result[1] is None
        assert result[2] is None
        # index 3: deltas so far (from closes 1,2,3,4) = [1,1,1], seeded avg_gain=1, avg_loss=0
        assert result[3] == Decimal(100)
        assert result[4] == Decimal(100)

    def test_hand_computed_mixed_period_3(self) -> None:
        # closes: 10, 11, 10, 12  -> deltas: +1, -1, +2
        # seed (period=3) at the 3rd delta: avg_gain=(1+0+2)/3=1, avg_loss=(0+1+0)/3=1/3
        # RS = 1 / (1/3) = 3; RSI = 100 - 100/(1+3) = 75
        values = _d(10, 11, 10, 12)
        result = rsi(values, 3)
        assert result[3] == Decimal(75)

    def test_incremental_matches_batch(self) -> None:
        values = _d(44, 44.5, 43.75, 44.25, 45, 44.5, 46, 47, 46.5, 48)
        expected = rsi(values, 4)
        state = RsiState(4)
        previous = values[0]
        actual: list[Decimal | None] = [None]
        for value in values[1:]:
            state, current = state.update(previous, value)
            actual.append(current)
            previous = value
        assert actual == expected


class TestAtr:
    def test_hand_computed_period_2_no_gaps(self) -> None:
        # bar1: H=10 L=8 (no previous close -> TR = 2)
        # bar2: H=11 L=9, prev close = 9 -> TR = max(2, |11-9|=2, |9-9|=0) = 2
        # bar3: H=12 L=10, prev close = 10 -> TR = max(2, |12-10|=2, |10-10|=0) = 2
        # every true range is 2, so every ATR is 2 once seeded (period=2, seeded at index 1)
        highs = _d(10, 11, 12)
        lows = _d(8, 9, 10)
        closes = _d(9, 10, 11)
        result = atr(highs, lows, closes, 2)
        assert result == [None, Decimal(2), Decimal(2)]

    def test_gap_up_widens_true_range(self) -> None:
        # bar1: H=10 L=9, close=10 (no previous close -> TR=1)
        # bar2 gaps up: H=15 L=14, prev close=10 -> TR = max(1, |15-10|=5, |14-10|=4) = 5
        # seeded ATR (period=2) = (1+5)/2 = 3
        highs = _d(10, 15)
        lows = _d(9, 14)
        closes = _d(10, 14.5)
        result = atr(highs, lows, closes, 2)
        assert result == [None, Decimal(3)]

    def test_incremental_matches_batch(self) -> None:
        highs = _d(48, 48.5, 49, 48.75, 49.5, 50)
        lows = _d(47, 47.5, 47.75, 47.5, 48.5, 49)
        closes = _d(47.5, 48, 48.5, 48, 49, 49.5)
        expected = atr(highs, lows, closes, 3)
        state = AtrState(3)
        previous_close: Decimal | None = None
        actual: list[Decimal | None] = []
        for high, low, close in zip(highs, lows, closes, strict=True):
            state, current = state.update(high, low, previous_close)
            actual.append(current)
            previous_close = close
        assert actual == expected


class TestRollingVolatility:
    def test_hand_computed_window_2(self) -> None:
        # closes: 100, 110, 99, 108.9
        # returns: (110-100)/100=0.10, (99-110)/110=-0.10, (108.9-99)/99=0.10
        # sample stdev of first two returns [0.10, -0.10]: mean=0, variance=((0.1)^2+(0.1)^2)/1=0.02
        # stdev = sqrt(0.02) ~= 0.14142135623730951
        values = _d(100, 110, 99, "108.9")
        result = rolling_volatility(values, 2)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is not None
        assert result[2] == pytest.approx(Decimal("0.1414213562373095"), abs=Decimal("1e-9"))

    def test_rejects_window_below_two(self) -> None:
        with pytest.raises(ValueError, match="window"):
            rolling_volatility(_d(1, 2), 1)

    def test_incremental_matches_batch(self) -> None:
        values = _d(50, 51, 49, 52, 53, 50, 55)
        expected = rolling_volatility(values, 3)
        state = VolatilityState(3)
        previous = values[0]
        actual: list[Decimal | None] = [None]
        for value in values[1:]:
            state, current = state.update(previous, value)
            actual.append(current)
            previous = value
        assert actual == expected
