"""The backtest/paper consistency test (ADR-0007, Phase 7 exit criterion): the same
strategy, over the same bars, produces the same fills whether run through
`application.backtest.run_backtest` or through `application.trading.step` — the function the
live paper-trading driver calls once per bar.

This is not a coincidence the test happens to observe: both paths bottom out in
`application.execution.apply_signal`/`simulate_fill`. What this test actually exercises is
the *orchestration* around that shared core — bar iteration, the `NEXT_BAR_OPEN` deferral,
and (on the paper side) sizing/risk-limit wiring — configured so a single-symbol paper
deployment's cash budget matches exactly what the backtester's fixed sub-allocation already
assumes: 100% of equity, no risk limit tighter than that, no order-rate ceiling low enough to
interfere.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from tradewatch.application.backtest import run_backtest
from tradewatch.application.execution import ExecutionConfig, SymbolLedger
from tradewatch.application.strategy import compile_strategy
from tradewatch.application.trading import (
    OrderExecuted,
    PositionSizingConfig,
    RiskLimitsConfig,
    SymbolRuntimeState,
    step,
)
from tradewatch.config.settings import FillModel
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import BarInterval

pytestmark = pytest.mark.unit

_INSTRUMENT_ID = uuid4()

#: A permissive paper configuration matching what a single-symbol backtest already assumes:
#: spend the whole ledger, no risk limit tighter than "all of it", no rate ceiling low enough
#: to ever fire in a handful of bars.
_FULL_SIZING = PositionSizingConfig(target_equity_pct=Decimal(1))
_PERMISSIVE_LIMITS = RiskLimitsConfig(
    max_position_pct=Decimal(1), max_gross_exposure_pct=Decimal(1), max_orders_per_minute=1000
)


@dataclass(frozen=True, slots=True)
class _PaperFill:
    quantity: Decimal
    price: Decimal
    filled_at: dt.datetime


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


def _simulate_paper(
    bars: list[Bar],
    *,
    strategy_type: str,
    parameters: dict[str, Any],
    initial_cash: Decimal,
    execution_config: ExecutionConfig,
) -> list[_PaperFill]:
    """Fold `bars` through `step`, exactly the way the live worker folds one bar at a time
    off Kafka — the only difference from the real worker being that bars arrive from a list
    here instead of a consumer, and outcomes are collected instead of persisted.
    """
    compiled = compile_strategy(strategy_type, parameters)
    runtime = SymbolRuntimeState(
        strategy_state=compiled.initial_state(),
        pending_signal=None,
        ledger=SymbolLedger(cash=initial_cash),
    )
    fills: list[_PaperFill] = []
    for bar in bars:
        runtime, outcome = step(
            runtime,
            bar,
            compiled=compiled,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=0,
            position_sizing=_FULL_SIZING,
            risk_limits=_PERMISSIVE_LIMITS,
            execution_config=execution_config,
        )
        if isinstance(outcome, OrderExecuted):
            fills.append(
                _PaperFill(
                    quantity=outcome.fill.quantity,
                    price=outcome.fill.price,
                    filled_at=outcome.fill.filled_at,
                )
            )
    return fills


class TestConsistency:
    def test_sma_crossover_produces_the_same_fill_sequence_live_and_backtested(self) -> None:
        closes = [10, 10, 10, 12, 14, 8, 9, 11, 15, 7]
        bars = [_bar(i, c) for i, c in enumerate(closes)]
        execution_config = ExecutionConfig(
            slippage_bps=Decimal(0),
            commission_per_share=Decimal(0),
            commission_pct=Decimal(0),
            fill_model=FillModel.CURRENT_CLOSE,
        )
        initial_cash = Decimal(12000)
        strategy_type = "sma_crossover"
        parameters = {"fast_period": 2, "slow_period": 3}

        backtest_result = run_backtest(
            strategy_type=strategy_type,
            parameters=parameters,
            symbols=["AAPL"],
            bars_by_symbol={"AAPL": bars},
            initial_cash=initial_cash,
            execution_config=execution_config,
            start=bars[0].bar_start,
            end=bars[-1].bar_start + dt.timedelta(days=1),
        )
        paper_fills = _simulate_paper(
            bars,
            strategy_type=strategy_type,
            parameters=parameters,
            initial_cash=initial_cash,
            execution_config=execution_config,
        )

        # Every backtest trade's entry is a buy fill and (if closed) its exit is a sell fill,
        # in the same order the paper path produced them.
        expected: list[tuple[Decimal, Decimal, dt.datetime]] = []
        for trade in backtest_result.trades:
            expected.append((trade.quantity, trade.entry_price, trade.entry_at))
            if trade.exit_price is not None and trade.exit_at is not None:
                expected.append((trade.quantity, trade.exit_price, trade.exit_at))

        assert len(paper_fills) == len(expected)
        for fill, (quantity, price, filled_at) in zip(paper_fills, expected, strict=True):
            assert fill.quantity == quantity
            assert fill.price == price
            assert fill.filled_at == filled_at
        assert len(backtest_result.trades) >= 1  # the fixture must actually exercise a trade

    def test_next_bar_open_deferral_also_agrees(self) -> None:
        """The default fill model, which defers a signal's fill to the following bar — the
        path most likely to disagree if the paper driver's deferral logic diverged from the
        backtester's.
        """
        closes = [10, 10, 10, 12, 14, 8, 9, 11]
        bars = [_bar(i, c) for i, c in enumerate(closes)]
        execution_config = ExecutionConfig(
            slippage_bps=Decimal(5),
            commission_per_share=Decimal("0.01"),
            commission_pct=Decimal("0.0005"),
            fill_model=FillModel.NEXT_BAR_OPEN,
        )
        initial_cash = Decimal(50000)
        strategy_type = "sma_crossover"
        parameters = {"fast_period": 2, "slow_period": 3}

        backtest_result = run_backtest(
            strategy_type=strategy_type,
            parameters=parameters,
            symbols=["AAPL"],
            bars_by_symbol={"AAPL": bars},
            initial_cash=initial_cash,
            execution_config=execution_config,
            start=bars[0].bar_start,
            end=bars[-1].bar_start + dt.timedelta(days=1),
        )
        paper_fills = _simulate_paper(
            bars,
            strategy_type=strategy_type,
            parameters=parameters,
            initial_cash=initial_cash,
            execution_config=execution_config,
        )

        expected: list[tuple[Decimal, Decimal, dt.datetime]] = []
        for trade in backtest_result.trades:
            expected.append((trade.quantity, trade.entry_price, trade.entry_at))
            if trade.exit_price is not None and trade.exit_at is not None:
                expected.append((trade.quantity, trade.exit_price, trade.exit_at))

        assert len(paper_fills) == len(expected)
        for fill, (quantity, price, filled_at) in zip(paper_fills, expected, strict=True):
            assert fill.quantity == quantity
            assert fill.price == price
            assert fill.filled_at == filled_at
