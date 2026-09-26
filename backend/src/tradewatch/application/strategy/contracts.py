"""The strategy core contract (ADR-0006, ADR-0007): `evaluate(state, bar) -> (state, signals)`.

A **strategy type** is registered code — `sma_crossover.py`, `rsi_reversion.py` — pairing a
bounded Pydantic parameter schema with two pure functions: build an initial state from
validated parameters, and fold one more bar into that state to produce a possibly-empty list
of signals. Nothing here reads a clock, touches a database, or accepts a callable loaded from
outside the process; ADR-0006 is explicit that a strategy is a *configuration* of one of
these registered types, never code a user supplies.

A **compiled strategy** is a `strategy_type` string plus a `parameters` dict, validated once
against its type's schema. `compile_strategy` is the only way to get one, and the only way
a caller (the Backtest Runner, and Phase 7's live driver) ever touches strategy logic — it
never imports `types/sma_crossover.py` directly, which is what makes "adding a third
strategy must not require touching the backtest engine" true rather than aspirational.

No I/O, no wall-clock reads, and no way to see a bar later than the one just passed to
`evaluate` — that last property is what a lookahead test asserts, and it holds here simply
because there is no parameter through which a future bar could arrive.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError

from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import SignalDirection

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


class StrategyParams(BaseModel):
    """Base for a strategy type's parameter schema.

    `extra="forbid"` catches a typo'd field name at validation time instead of silently
    ignoring it; `frozen=True` matches every other value the strategy core touches, so a
    parameter set cannot be mutated out from under a state that already captured it.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")


@dataclass(frozen=True, slots=True)
class Signal:
    """Strategy output (`ARCHITECTURE.md` §5.2): a direction, a reason, and nothing else.

    Sizing, order type, and risk limits are the Paper Trading Engine's job (Phase 7) and the
    Backtest Runner's job (this phase) — not the strategy's. A strategy that decided how many
    shares to buy would be a strategy that could not be reused against a portfolio of a
    different size, which defeats the point of separating the two.
    """

    direction: SignalDirection
    reason: str
    bar_start: datetime
    #: Conviction, 0-1. Every built-in strategy in this phase emits full-strength signals;
    #: the field exists because `ARCHITECTURE.md` names it, and a strategy that wants to
    #: express partial conviction later can, without changing this type.
    strength: float = 1.0


@dataclass(frozen=True, slots=True)
class StrategySpec[ParamsT: StrategyParams, StateT]:
    """A registered strategy type: its parameter schema and its two pure functions."""

    params_model: type[ParamsT]
    initial_state: Callable[[ParamsT], StateT]
    evaluate: Callable[[ParamsT, StateT, Bar], tuple[StateT, list[Signal]]]


class UnknownStrategyTypeError(ValueError):
    """`strategy_type` does not name a registered strategy."""


class InvalidStrategyParametersError(ValueError):
    """`parameters` failed the strategy type's own schema.

    Raised both when a strategy is first created and again whenever it is loaded for a
    backtest (ADR-0006: "validated on write and again on load, because a row can be changed
    by a migration or by hand") — carrying the underlying `ValidationError` so a caller can
    report which field and why, rather than just "invalid".
    """

    def __init__(self, strategy_type: str, errors: ValidationError) -> None:
        super().__init__(f"invalid parameters for strategy type {strategy_type!r}: {errors}")
        self.strategy_type = strategy_type
        self.errors = errors


_REGISTRY: dict[str, StrategySpec[Any, Any]] = {}


def register_strategy_type(strategy_type: str, spec: StrategySpec[Any, Any]) -> None:
    """Add a strategy type to the registry. Called once, at import time, by each type module."""
    _REGISTRY[strategy_type] = spec


def known_strategy_types() -> list[str]:
    return sorted(_REGISTRY)


@dataclass(frozen=True, slots=True)
class CompiledStrategy[ParamsT: StrategyParams, StateT]:
    """A strategy type plus its validated parameters, ready to run.

    This is the only object the Backtest Runner (or, in Phase 7, the live driver) needs: it
    never has to know which strategy type it is holding, only that `.initial_state()` and
    `.evaluate(state, bar)` exist — which is what makes the engine generic over every
    strategy type this registry will ever hold.
    """

    strategy_type: str
    params: ParamsT
    spec: StrategySpec[ParamsT, StateT]

    def initial_state(self) -> StateT:
        return self.spec.initial_state(self.params)

    def evaluate(self, state: StateT, bar: Bar) -> tuple[StateT, list[Signal]]:
        return self.spec.evaluate(self.params, state, bar)


def compile_strategy(strategy_type: str, parameters: dict[str, Any]) -> CompiledStrategy[Any, Any]:
    """Validate `parameters` against `strategy_type`'s schema and return a runnable strategy.

    Raises:
        UnknownStrategyTypeError: `strategy_type` is not registered.
        InvalidStrategyParametersError: `parameters` does not satisfy the type's schema.
    """
    spec = _REGISTRY.get(strategy_type)
    if spec is None:
        raise UnknownStrategyTypeError(
            f"unknown strategy_type {strategy_type!r}; known types: {known_strategy_types()}"
        )
    try:
        params = spec.params_model.model_validate(parameters)
    except ValidationError as error:
        raise InvalidStrategyParametersError(strategy_type, error) from error
    return CompiledStrategy(strategy_type=strategy_type, params=params, spec=spec)
