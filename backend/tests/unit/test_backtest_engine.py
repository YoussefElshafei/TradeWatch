"""The Backtest Engine: `DEVELOPMENT_PLAN.md`'s Phase 6 exit criteria, one test per bullet.

The golden fixture's expected trade sequence and equity curve are worked out by hand in the
comments below, from the same SMA-crossover rule the strategy implements — not by running
`run_backtest` and asserting whatever it happened to return.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any, ClassVar
from uuid import uuid4

import pytest

from tradewatch.application.backtest import run_backtest
from tradewatch.application.backtest.engine import BacktestResult
from tradewatch.application.execution import ExecutionConfig
from tradewatch.application.strategy import (
    InvalidStrategyParametersError,
    UnknownStrategyTypeError,
)
from tradewatch.config.settings import FillModel
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import BarInterval

pytestmark = pytest.mark.unit

_INSTRUMENT_ID = uuid4()
_ZERO_COST = ExecutionConfig(
    slippage_bps=Decimal(0),
    commission_per_share=Decimal(0),
    commission_pct=Decimal(0),
    fill_model=FillModel.CURRENT_CLOSE,
)


def _bar(day: int, close: float | str) -> Bar:
    price = Decimal(str(close))
    start = dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(days=day)
    return Bar(
        instrument_id=_INSTRUMENT_ID,
        interval=BarInterval.ONE_DAY,
        bar_start=start,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal(1000),
        vwap=price,
        trade_count=10,
        is_final=True,
        revision=0,
        recorded_at=start,
    )


class TestGoldenBacktest:
    """SMA(2)/SMA(3) crossover, one symbol, zero cost, `CURRENT_CLOSE` fills.

    Closes: 10, 10, 10, 12, 14, 8.

    | day | close | fast SMA(2) | slow SMA(3) | signal |
    |-----|-------|-------------|-------------|--------|
    | 0   | 10    | None        | None        | -      |
    | 1   | 10    | 10          | None        | -      |
    | 2   | 10    | 10          | 10          | -  (10 is not > 10)
    | 3   | 12    | 11          | 10.667      | LONG   |
    | 4   | 14    | 13          | 12          | -  (already long)
    | 5   | 8     | 11          | 11.333      | FLAT   |

    12000 initial cash, entered entirely at day 3's close of 12 (zero commission, zero
    slippage) buys exactly 1000 shares. Exiting at day 5's close of 8 realizes
    `(8 - 12) * 1000 = -4000` P&L. Equity is cash plus mark-to-market position value at each
    bar's own close, which is why buying/selling at zero cost never itself moves the curve:
    12000, 12000, 12000, 12000 (bought, 0 cash + 1000@12), 14000 (0 + 1000@14),
    8000 (sold, 8000 cash + 0).
    """

    _CLOSES: ClassVar[list[int]] = [10, 10, 10, 12, 14, 8]

    def _bars(self) -> list[Bar]:
        return [_bar(day, close) for day, close in enumerate(self._CLOSES)]

    def _run(self) -> BacktestResult:
        bars = self._bars()
        return run_backtest(
            strategy_type="sma_crossover",
            parameters={"fast_period": 2, "slow_period": 3},
            symbols=["AAPL"],
            bars_by_symbol={"AAPL": bars},
            initial_cash=Decimal(12000),
            execution_config=_ZERO_COST,
            start=bars[0].bar_start,
            end=bars[-1].bar_start + dt.timedelta(days=1),
        )

    def test_trade_sequence_matches_hand_calculation(self) -> None:
        result = self._run()
        assert len(result.trades) == 1
        trade = result.trades[0]
        assert trade.symbol == "AAPL"
        assert trade.quantity == Decimal(1000)
        assert trade.entry_price == Decimal(12)
        assert trade.entry_at == _bar(3, 12).bar_start
        assert trade.exit_price == Decimal(8)
        assert trade.exit_at == _bar(5, 8).bar_start
        assert trade.pnl == Decimal(-4000)
        assert trade.return_pct == pytest.approx(Decimal(-100) / Decimal(3), abs=Decimal("1e-9"))

    def test_equity_curve_matches_hand_calculation(self) -> None:
        result = self._run()
        expected = [12000, 12000, 12000, 12000, 14000, 8000]
        actual = [point.total_equity for point in result.equity_curve]
        assert actual == [Decimal(v) for v in expected]

    def test_final_equity_and_total_return(self) -> None:
        result = self._run()
        assert result.final_equity == Decimal(8000)
        assert result.metrics.total_return_pct == pytest.approx(
            Decimal(-100) / Decimal(3), abs=Decimal("1e-9")
        )

    def test_max_drawdown_is_the_drop_from_the_day4_peak(self) -> None:
        # Peak 14000 at day 4, trough 8000 at day 5: (8000-14000)/14000 * 100.
        result = self._run()
        expected = (Decimal(8000) - Decimal(14000)) / Decimal(14000) * 100
        assert result.metrics.max_drawdown_pct == expected

    def test_win_rate_is_zero_for_a_single_losing_trade(self) -> None:
        assert self._run().metrics.win_rate_pct == Decimal(0)

    def test_trade_count(self) -> None:
        assert self._run().metrics.trade_count == 1


class TestDeterminism:
    def test_same_backtest_twice_is_byte_identical(self) -> None:
        bars = [_bar(i, c) for i, c in enumerate([100, 102, 99, 105, 96, 110, 90, 120, 80])]
        kwargs: dict[str, Any] = {
            "strategy_type": "rsi_reversion",
            "parameters": {"rsi_period": 3},
            "symbols": ["AAPL"],
            "bars_by_symbol": {"AAPL": bars},
            "initial_cash": Decimal(50000),
            "execution_config": ExecutionConfig(
                slippage_bps=Decimal(5),
                commission_per_share=Decimal("0.01"),
                commission_pct=Decimal("0.0005"),
                fill_model=FillModel.NEXT_BAR_OPEN,
            ),
            "start": bars[0].bar_start,
            "end": bars[-1].bar_start + dt.timedelta(days=1),
        }
        first = run_backtest(**kwargs)
        second = run_backtest(**kwargs)
        assert first.trades == second.trades
        assert first.equity_curve == second.equity_curve
        assert first.metrics == second.metrics


class TestNoLookahead:
    def test_a_future_bar_does_not_change_the_backtest_over_the_bars_before_it(self) -> None:
        prefix = [_bar(i, c) for i, c in enumerate([100, 101, 99, 103, 108, 94, 111, 89])]
        wild_future = [_bar(len(prefix) + i, c) for i, c in enumerate([2000, 1, 3000, 0.5])]

        base = run_backtest(
            strategy_type="sma_crossover",
            parameters={"fast_period": 2, "slow_period": 4},
            symbols=["AAPL"],
            bars_by_symbol={"AAPL": prefix},
            initial_cash=Decimal(10000),
            execution_config=_ZERO_COST,
            start=prefix[0].bar_start,
            end=prefix[-1].bar_start + dt.timedelta(days=1),
        )
        extended_bars = prefix + wild_future
        extended = run_backtest(
            strategy_type="sma_crossover",
            parameters={"fast_period": 2, "slow_period": 4},
            symbols=["AAPL"],
            bars_by_symbol={"AAPL": extended_bars},
            initial_cash=Decimal(10000),
            execution_config=_ZERO_COST,
            start=extended_bars[0].bar_start,
            end=extended_bars[-1].bar_start + dt.timedelta(days=1),
        )

        prefix_cutoff = prefix[-1].bar_start
        assert base.trades == [t for t in extended.trades if t.entry_at <= prefix_cutoff]
        assert base.equity_curve == [p for p in extended.equity_curve if p.as_of <= prefix_cutoff]


class TestSlippageChangesTheResult:
    def test_higher_slippage_produces_a_worse_result(self) -> None:
        bars = [_bar(i, c) for i, c in enumerate([10, 10, 10, 12, 14, 8])]
        common: dict[str, Any] = {
            "strategy_type": "sma_crossover",
            "parameters": {"fast_period": 2, "slow_period": 3},
            "symbols": ["AAPL"],
            "bars_by_symbol": {"AAPL": bars},
            "initial_cash": Decimal(12000),
            "start": bars[0].bar_start,
            "end": bars[-1].bar_start + dt.timedelta(days=1),
        }
        cheap = run_backtest(
            execution_config=ExecutionConfig(
                slippage_bps=Decimal(0),
                commission_per_share=Decimal(0),
                commission_pct=Decimal(0),
                fill_model=FillModel.CURRENT_CLOSE,
            ),
            **common,
        )
        expensive = run_backtest(
            execution_config=ExecutionConfig(
                slippage_bps=Decimal(200),  # 2%
                commission_per_share=Decimal(0),
                commission_pct=Decimal(0),
                fill_model=FillModel.CURRENT_CLOSE,
            ),
            **common,
        )
        # Buying higher and selling lower than the zero-slippage case must leave less money.
        assert expensive.final_equity < cheap.final_equity


class TestEdgeCases:
    def test_empty_dataset_completes_flat_rather_than_crashing(self) -> None:
        start = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        end = start + dt.timedelta(days=30)
        result = run_backtest(
            strategy_type="sma_crossover",
            parameters={"fast_period": 5, "slow_period": 10},
            symbols=["AAPL"],
            bars_by_symbol={"AAPL": []},
            initial_cash=Decimal(10000),
            execution_config=_ZERO_COST,
            start=start,
            end=end,
        )
        assert result.trades == []
        assert result.equity_curve == []
        assert result.final_equity == Decimal(10000)
        assert result.metrics.total_return_pct == Decimal(0)
        assert result.metrics.trade_count == 0
        assert result.metrics.win_rate_pct is None

    def test_insufficient_history_for_the_lookback_window_produces_no_trades(self) -> None:
        # slow_period=10 but only 4 bars: the slow SMA never warms up, so no signal can fire.
        bars = [_bar(i, 100 + i) for i in range(4)]
        result = run_backtest(
            strategy_type="sma_crossover",
            parameters={"fast_period": 3, "slow_period": 10},
            symbols=["AAPL"],
            bars_by_symbol={"AAPL": bars},
            initial_cash=Decimal(10000),
            execution_config=_ZERO_COST,
            start=bars[0].bar_start,
            end=bars[-1].bar_start + dt.timedelta(days=1),
        )
        assert result.trades == []
        assert all(point.total_equity == Decimal(10000) for point in result.equity_curve)

    def test_invalid_parameters_raise_rather_than_run(self) -> None:
        with pytest.raises(InvalidStrategyParametersError):
            run_backtest(
                strategy_type="sma_crossover",
                parameters={"fast_period": 10, "slow_period": 5},  # fast >= slow: invalid
                symbols=["AAPL"],
                bars_by_symbol={"AAPL": [_bar(0, 100)]},
                initial_cash=Decimal(10000),
                execution_config=_ZERO_COST,
                start=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                end=dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
            )

    def test_unknown_strategy_type_raises(self) -> None:
        with pytest.raises(UnknownStrategyTypeError):
            run_backtest(
                strategy_type="not_a_strategy",
                parameters={},
                symbols=["AAPL"],
                bars_by_symbol={"AAPL": []},
                initial_cash=Decimal(10000),
                execution_config=_ZERO_COST,
                start=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                end=dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
            )

    def test_zero_width_date_range_does_not_crash_cagr(self) -> None:
        same_instant = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        result = run_backtest(
            strategy_type="sma_crossover",
            parameters={"fast_period": 2, "slow_period": 3},
            symbols=["AAPL"],
            bars_by_symbol={"AAPL": []},
            initial_cash=Decimal(10000),
            execution_config=_ZERO_COST,
            start=same_instant,
            end=same_instant,
        )
        assert result.metrics.cagr_pct is None

    def test_empty_symbols_rejected(self) -> None:
        with pytest.raises(ValueError, match="symbols"):
            run_backtest(
                strategy_type="sma_crossover",
                parameters={"fast_period": 2, "slow_period": 3},
                symbols=[],
                bars_by_symbol={},
                initial_cash=Decimal(10000),
                execution_config=_ZERO_COST,
                start=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                end=dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
            )

    def test_non_positive_initial_cash_rejected(self) -> None:
        with pytest.raises(ValueError, match="initial_cash"):
            run_backtest(
                strategy_type="sma_crossover",
                parameters={"fast_period": 2, "slow_period": 3},
                symbols=["AAPL"],
                bars_by_symbol={"AAPL": []},
                initial_cash=Decimal(0),
                execution_config=_ZERO_COST,
                start=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                end=dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
            )
