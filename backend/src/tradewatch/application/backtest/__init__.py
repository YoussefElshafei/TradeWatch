"""The Backtest Runner's pure core (`engine.py`) and the metrics it computes (`metrics.py`).

`service.py` is the impure half — the driver that reads a queued `backtest_runs` row, loads
bars, calls `run_backtest`, and persists the result — kept separate so this package's own
tests never need a database.
"""

from __future__ import annotations

from tradewatch.application.backtest.engine import (
    BacktestMetrics,
    BacktestResult,
    BacktestTradeResult,
    EquityPointResult,
    run_backtest,
)

__all__ = [
    "BacktestMetrics",
    "BacktestResult",
    "BacktestTradeResult",
    "EquityPointResult",
    "run_backtest",
]
