"""`sma_crossover`: go long when a fast SMA crosses above a slow one, flat when it crosses back.

The oldest trend-following rule there is, and the reason it is the first built-in strategy:
it exercises the whole signal-to-fill path (crossings are rare enough to hand-verify, common
enough to produce real trades) without needing a second indicator to explain.

Long-only, matching the rest of the system today — short selling is D16, explicitly deferred,
and `OrderSide` in the execution simulator only ever sees a `BUY` to open and a `SELL` to
close a strategy built this way.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from pydantic import Field, model_validator

from tradewatch.application.strategy.contracts import (
    Signal,
    StrategyParams,
    StrategySpec,
    register_strategy_type,
)
from tradewatch.application.strategy.indicators import SmaState
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import SignalDirection

__all__ = ["SmaCrossoverParams", "SmaCrossoverState"]

STRATEGY_TYPE = "sma_crossover"


class SmaCrossoverParams(StrategyParams):
    #: Bounds per ADR-0006: a lookback is a bounded integer, not an arbitrary one that can be
    #: set to a number large enough to hold years of bars in memory for no reason.
    fast_period: int = Field(ge=2, le=200, description="Bars in the fast SMA.")
    slow_period: int = Field(ge=2, le=400, description="Bars in the slow SMA.")

    @model_validator(mode="after")
    def _fast_is_faster(self) -> Self:
        if self.fast_period >= self.slow_period:
            raise ValueError(
                f"fast_period ({self.fast_period}) must be less than "
                f"slow_period ({self.slow_period})"
            )
        return self


@dataclass(frozen=True, slots=True)
class SmaCrossoverState:
    fast: SmaState
    slow: SmaState
    #: LONG or FLAT — never SHORT (D16). Tracked so a signal fires only on the crossing
    #: itself, not on every bar the fast average happens to sit above the slow one.
    position: SignalDirection


def initial_state(params: SmaCrossoverParams) -> SmaCrossoverState:
    return SmaCrossoverState(
        fast=SmaState(params.fast_period),
        slow=SmaState(params.slow_period),
        position=SignalDirection.FLAT,
    )


def evaluate(
    params: SmaCrossoverParams, state: SmaCrossoverState, bar: Bar
) -> tuple[SmaCrossoverState, list[Signal]]:
    fast_state, fast_value = state.fast.update(bar.close)
    slow_state, slow_value = state.slow.update(bar.close)

    signals: list[Signal] = []
    position = state.position

    if fast_value is not None and slow_value is not None:
        if fast_value > slow_value and position is not SignalDirection.LONG:
            position = SignalDirection.LONG
            signals.append(
                _signal(
                    SignalDirection.LONG,
                    bar,
                    f"fast SMA {fast_value} crossed above slow SMA {slow_value}",
                )
            )
        elif fast_value <= slow_value and position is SignalDirection.LONG:
            position = SignalDirection.FLAT
            signals.append(
                _signal(
                    SignalDirection.FLAT,
                    bar,
                    f"fast SMA {fast_value} crossed below slow SMA {slow_value}",
                )
            )

    return SmaCrossoverState(fast_state, slow_state, position), signals


def _signal(direction: SignalDirection, bar: Bar, reason: str) -> Signal:
    return Signal(direction=direction, reason=reason, bar_start=bar.bar_start)


register_strategy_type(
    STRATEGY_TYPE,
    StrategySpec(
        params_model=SmaCrossoverParams,
        initial_state=initial_state,
        evaluate=evaluate,
    ),
)
