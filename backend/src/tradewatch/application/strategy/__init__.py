"""Strategy core (ADR-0006, ADR-0007): indicators, the `evaluate` contract, and the registry.

Importing this package is what registers every built-in strategy type — see
`application.strategy.types`. A caller that only needs `compile_strategy` never has to know
that; importing `tradewatch.application.strategy` is enough to make every registered type
available to it.
"""

from __future__ import annotations

from tradewatch.application.strategy import types as _types  # noqa: F401 — import for registration
from tradewatch.application.strategy.contracts import (
    CompiledStrategy,
    InvalidStrategyParametersError,
    Signal,
    StrategyParams,
    StrategySpec,
    UnknownStrategyTypeError,
    compile_strategy,
    known_strategy_types,
    register_strategy_type,
)

__all__ = [
    "CompiledStrategy",
    "InvalidStrategyParametersError",
    "Signal",
    "StrategyParams",
    "StrategySpec",
    "UnknownStrategyTypeError",
    "compile_strategy",
    "known_strategy_types",
    "register_strategy_type",
]
