"""The Execution Simulator (ADR-0007): shared by the Backtest Runner and Phase 7's Paper
Trading Engine. See `simulator.py` for why it takes the shape it does.
"""

from __future__ import annotations

from tradewatch.application.execution.config import ExecutionConfig
from tradewatch.application.execution.signals import (
    SignalOutcome,
    apply_signal,
    size_long_entry,
)
from tradewatch.application.execution.simulator import (
    Fill,
    InsufficientCashError,
    SymbolLedger,
    simulate_fill,
)

__all__ = [
    "ExecutionConfig",
    "Fill",
    "InsufficientCashError",
    "SignalOutcome",
    "SymbolLedger",
    "apply_signal",
    "simulate_fill",
    "size_long_entry",
]
