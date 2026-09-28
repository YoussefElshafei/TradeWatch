"""The Paper Trading Engine (Phase 7): position sizing, risk limits, and the order-rate
ceiling (failure F15), on top of the same Execution Simulator the Backtest Runner uses.
"""

from __future__ import annotations

from tradewatch.application.trading.config import PositionSizingConfig, RiskLimitsConfig
from tradewatch.application.trading.driver import SymbolRuntimeState, step
from tradewatch.application.trading.engine import OrderExecuted, OrderRejected, process_signal

__all__ = [
    "OrderExecuted",
    "OrderRejected",
    "PositionSizingConfig",
    "RiskLimitsConfig",
    "SymbolRuntimeState",
    "process_signal",
    "step",
]
