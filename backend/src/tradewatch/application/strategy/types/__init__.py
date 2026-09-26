"""Built-in strategy types. Importing this package registers all of them.

Adding a third strategy type is: a new module here that calls `register_strategy_type` at
import time, and one more line below importing it. Nothing in `application/backtest` or
`application/execution` changes — that is the whole point of ADR-0006's registry design.
"""

from __future__ import annotations

from tradewatch.application.strategy.types import rsi_reversion, sma_crossover

__all__ = ["rsi_reversion", "sma_crossover"]
