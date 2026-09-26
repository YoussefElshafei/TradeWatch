"""Backtest metrics, checked against hand computation."""

from __future__ import annotations

import datetime as dt
import math
from decimal import Decimal
from typing import ClassVar

import pytest

from tradewatch.application.backtest.metrics import (
    cagr_pct,
    exposure_pct,
    max_drawdown_pct,
    sharpe_ratio,
    total_return_pct,
    volatility_pct,
    win_rate_pct,
)

pytestmark = pytest.mark.unit


def _d(value: int | str) -> Decimal:
    return Decimal(str(value))


class TestTotalReturn:
    def test_hand_computed(self) -> None:
        assert total_return_pct(_d(100), _d(110)) == _d(10)

    def test_loss_is_negative(self) -> None:
        assert total_return_pct(_d(100), _d(90)) == _d(-10)

    def test_rejects_non_positive_initial_cash(self) -> None:
        with pytest.raises(ValueError, match="initial_cash"):
            total_return_pct(_d(0), _d(100))


class TestCagr:
    def test_one_year_hand_computed(self) -> None:
        assert cagr_pct(_d(100), _d(150), years=_d(1)) == _d(50)

    def test_two_years_hand_computed(self) -> None:
        # (400/100)^(1/2) - 1 = sqrt(4) - 1 = 1.0 -> 100%.
        assert cagr_pct(_d(100), _d(400), years=_d(2)) == _d(100)

    def test_flat_return_is_zero_regardless_of_horizon(self) -> None:
        assert cagr_pct(_d(100), _d(100), years=_d(5)) == _d(0)

    def test_non_positive_years_is_undefined(self) -> None:
        assert cagr_pct(_d(100), _d(150), years=_d(0)) is None

    def test_wiped_out_account_is_undefined_not_negative_100(self) -> None:
        assert cagr_pct(_d(100), _d(0), years=_d(1)) is None


class TestMaxDrawdown:
    def test_hand_computed(self) -> None:
        # peaks: 100,120,120,120,120,130. Worst drawdown at 80: (80-120)/120 = -33.33...%.
        curve = [_d(v) for v in (100, 120, 90, 110, 80, 130)]
        result = max_drawdown_pct(curve)
        assert result == pytest.approx(Decimal("-33.333333333333333333333333333"), abs=_d("1e-6"))

    def test_never_declining_curve_has_zero_drawdown(self) -> None:
        curve = [_d(v) for v in (100, 110, 120, 130)]
        assert max_drawdown_pct(curve) == _d(0)

    def test_empty_curve_is_zero(self) -> None:
        assert max_drawdown_pct([]) == _d(0)


class TestSharpeAndVolatility:
    """equity 100 -> 110 -> 99: returns [+10%, -10%], mean 0, so Sharpe is exactly 0
    regardless of annualization — the cleanest fixture that does not require trusting the
    annualization arithmetic to also be right.
    """

    _CURVE: ClassVar[list[Decimal]] = [_d(100), _d(110), _d(99)]
    _TIMESTAMPS: ClassVar[list[dt.datetime]] = [
        dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
        dt.datetime(2026, 1, 3, tzinfo=dt.UTC),
    ]

    def test_sharpe_is_zero_when_mean_return_is_zero(self) -> None:
        assert sharpe_ratio(self._CURVE, self._TIMESTAMPS) == _d(0)

    def test_volatility_matches_the_documented_formula(self) -> None:
        # stdev([0.10, -0.10], sample) = sqrt(((0.1)^2 + (0.1)^2) / 1) = sqrt(0.02).
        # Annualization: sqrt(seconds_per_year / seconds_per_period), period = 1 day here.
        stdev = math.sqrt(0.02)
        seconds_per_year = 365.25 * 24 * 3600
        annualization = math.sqrt(seconds_per_year / 86400)
        expected = Decimal(str(stdev * annualization * 100))
        result = volatility_pct(self._CURVE, self._TIMESTAMPS)
        assert result is not None
        assert result == pytest.approx(expected, rel=Decimal("1e-9"))

    def test_fewer_than_two_returns_is_undefined(self) -> None:
        assert sharpe_ratio([_d(100)], [self._TIMESTAMPS[0]]) is None
        assert volatility_pct([_d(100)], [self._TIMESTAMPS[0]]) is None

    def test_zero_variance_sharpe_is_undefined_not_infinite(self) -> None:
        flat = [_d(100), _d(100), _d(100)]
        assert sharpe_ratio(flat, self._TIMESTAMPS) is None


class TestWinRate:
    def test_hand_computed(self) -> None:
        pnls = [_d(10), _d(-5), _d(20), _d(-1), _d(0)]
        assert win_rate_pct(pnls) == _d(40)

    def test_no_closed_trades_is_undefined_not_zero(self) -> None:
        assert win_rate_pct([]) is None


class TestExposure:
    def test_hand_computed(self) -> None:
        assert exposure_pct([True, True, False, True]) == _d(75)

    def test_no_data_is_zero(self) -> None:
        assert exposure_pct([]) == _d(0)
