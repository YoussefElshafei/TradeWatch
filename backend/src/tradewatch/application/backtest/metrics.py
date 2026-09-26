"""Backtest summary statistics: total return, CAGR, max drawdown, Sharpe, volatility, win
rate, exposure — the numbers `ARCHITECTURE.md` §3.6 names as what the Backtest Runner writes
to `backtest_runs.metrics`.

Every function here is pure and takes plain sequences, not a `BacktestResult` — so a metric
can be tested (and hand-verified) in complete isolation from the engine that produces the
equity curve it will eventually be fed.

Returns and their derived statistics (Sharpe, volatility) use `float`, not `Decimal`: they
are ratios computed from `statistics.stdev`, not stored money, and ADR-0009's Decimal rule is
about avoiding float rounding in amounts that get summed and compared — a Sharpe ratio is
neither.
"""

from __future__ import annotations

import datetime as dt
import math
import statistics
from collections.abc import Sequence
from decimal import Decimal, InvalidOperation

__all__ = [
    "cagr_pct",
    "exposure_pct",
    "max_drawdown_pct",
    "sharpe_ratio",
    "total_return_pct",
    "volatility_pct",
    "win_rate_pct",
]

_ZERO = Decimal(0)
_SECONDS_PER_YEAR = Decimal("365.25") * 24 * 3600


def total_return_pct(initial_cash: Decimal, final_equity: Decimal) -> Decimal:
    if initial_cash <= 0:
        raise ValueError(f"initial_cash must be positive, got {initial_cash}")
    return (final_equity / initial_cash - 1) * 100


def cagr_pct(initial_cash: Decimal, final_equity: Decimal, years: Decimal) -> Decimal | None:
    """Compound annual growth rate, or `None` when it is not a meaningful number.

    `years` is the caller's job to compute (`(end_date - start_date).days / 365.25`) rather
    than a `start`/`end` pair here, so this function stays testable with round numbers
    instead of every test needing a date arithmetic detour to hit an exact year count.

    `None` rather than a number for a non-positive `years` (an empty or reversed range) and
    for a non-positive `final_equity` (the account went to zero or below, where a fractional
    power of a non-positive base is not a real number) — both are "undefined", not "0%".
    """
    if initial_cash <= 0:
        raise ValueError(f"initial_cash must be positive, got {initial_cash}")
    if years <= 0 or final_equity <= 0:
        return None
    try:
        growth = (final_equity / initial_cash) ** (1 / years)
    except InvalidOperation:
        return None
    return (growth - 1) * 100


def max_drawdown_pct(equity_curve: Sequence[Decimal]) -> Decimal:
    """The worst peak-to-trough decline, as a negative percentage (or 0 if none)."""
    if not equity_curve:
        return _ZERO
    peak = equity_curve[0]
    worst = _ZERO
    for value in equity_curve:
        peak = max(peak, value)
        if peak > 0:
            drawdown = (value - peak) / peak * 100
            worst = min(worst, drawdown)
    return worst


def _returns(equity_curve: Sequence[Decimal]) -> list[float]:
    """Simple period-over-period returns, skipping a division were the prior point ever 0."""
    return [
        float(equity_curve[i] / equity_curve[i - 1] - 1)
        for i in range(1, len(equity_curve))
        if equity_curve[i - 1] != 0
    ]


def _annualization_factor(timestamps: Sequence[dt.datetime]) -> float | None:
    """`sqrt(periods per year)`, inferred from the equity curve's own spacing.

    Bars can be 1-minute or 1-day; hard-coding "252 trading days" would silently mis-annualize
    an intraday backtest. Deriving the period length from the timestamps themselves works for
    every interval without the caller having to say which one it used.
    """
    if len(timestamps) < 2:
        return None
    total_seconds = Decimal((timestamps[-1] - timestamps[0]).total_seconds())
    periods = len(timestamps) - 1
    if total_seconds <= 0 or periods <= 0:
        return None
    average_period_seconds = total_seconds / periods
    return math.sqrt(float(_SECONDS_PER_YEAR / average_period_seconds))


def sharpe_ratio(
    equity_curve: Sequence[Decimal], timestamps: Sequence[dt.datetime]
) -> Decimal | None:
    """Annualized Sharpe over the run, assuming a zero risk-free rate.

    A stated, deliberate simplification — the free-rate term matters for cash-like
    strategies more than for anything Phase 6 ships, and adding it later is a one-line
    change to this function, not a redesign.
    """
    returns = _returns(equity_curve)
    if len(returns) < 2:
        return None
    stdev = statistics.stdev(returns)
    if stdev == 0:
        return None
    annualization = _annualization_factor(timestamps)
    if annualization is None:
        return None
    sharpe = (statistics.mean(returns) / stdev) * annualization
    return Decimal(str(sharpe))


def volatility_pct(
    equity_curve: Sequence[Decimal], timestamps: Sequence[dt.datetime]
) -> Decimal | None:
    """Annualized standard deviation of returns, as a percentage."""
    returns = _returns(equity_curve)
    if len(returns) < 2:
        return None
    stdev = statistics.stdev(returns)
    annualization = _annualization_factor(timestamps)
    if annualization is None:
        return None
    return Decimal(str(stdev * annualization * 100))


def win_rate_pct(trade_pnls: Sequence[Decimal]) -> Decimal | None:
    """Share of *closed* trades with positive P&L. `None` when nothing has closed yet —
    a 0% win rate and "no evidence yet" are different claims and must not look the same.
    """
    if not trade_pnls:
        return None
    wins = sum(1 for pnl in trade_pnls if pnl > 0)
    return Decimal(wins) / Decimal(len(trade_pnls)) * 100


def exposure_pct(position_open_flags: Sequence[bool]) -> Decimal:
    """Share of the equity curve's points where any position was open."""
    if not position_open_flags:
        return _ZERO
    open_count = sum(1 for flag in position_open_flags if flag)
    return Decimal(open_count) / Decimal(len(position_open_flags)) * 100
