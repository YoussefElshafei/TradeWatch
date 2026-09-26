"""`rsi_reversion`: buy an oversold dip, exit once it recovers to overbought.

The second built-in strategy, deliberately different in shape from `sma_crossover` — it is
mean-reverting rather than trend-following, and its indicator needs the *previous* close as
well as the current one. That is exactly the case `application/strategy/indicators.RsiState`
was designed to make cheap: this module owns no smoothing logic of its own.

Long-only, for the same reason as `sma_crossover` (D16 defers short selling).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Self

from pydantic import Field, model_validator

from tradewatch.application.strategy.contracts import (
    Signal,
    StrategyParams,
    StrategySpec,
    register_strategy_type,
)
from tradewatch.application.strategy.indicators import RsiState
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import SignalDirection

__all__ = ["RsiReversionParams", "RsiReversionState"]

STRATEGY_TYPE = "rsi_reversion"


class RsiReversionParams(StrategyParams):
    rsi_period: int = Field(ge=2, le=100, description="Bars in the Wilder RSI average.")
    #: Bounded to the open interval (0, 100): an RSI reading itself can never reach either
    #: end, so a threshold that could would simply never fire.
    oversold: float = Field(default=30.0, gt=0, lt=100, description="Enter below this RSI.")
    overbought: float = Field(default=70.0, gt=0, lt=100, description="Exit above this RSI.")

    @model_validator(mode="after")
    def _oversold_below_overbought(self) -> Self:
        if self.oversold >= self.overbought:
            raise ValueError(
                f"oversold ({self.oversold}) must be less than overbought ({self.overbought})"
            )
        return self


@dataclass(frozen=True, slots=True)
class RsiReversionState:
    rsi: RsiState
    #: `None` for exactly the first bar the strategy ever sees — `RsiState.update` needs a
    #: previous close to compute a change, and the first bar has none to offer.
    previous_close: Decimal | None
    position: SignalDirection


def initial_state(params: RsiReversionParams) -> RsiReversionState:
    return RsiReversionState(
        rsi=RsiState(params.rsi_period),
        previous_close=None,
        position=SignalDirection.FLAT,
    )


def evaluate(
    params: RsiReversionParams, state: RsiReversionState, bar: Bar
) -> tuple[RsiReversionState, list[Signal]]:
    if state.previous_close is None:
        return RsiReversionState(state.rsi, bar.close, state.position), []

    rsi_state, rsi_value = state.rsi.update(state.previous_close, bar.close)

    signals: list[Signal] = []
    position = state.position

    if rsi_value is not None:
        if rsi_value < params.oversold and position is not SignalDirection.LONG:
            position = SignalDirection.LONG
            signals.append(
                _signal(SignalDirection.LONG, bar, f"RSI {rsi_value} fell below {params.oversold}")
            )
        elif rsi_value > params.overbought and position is SignalDirection.LONG:
            position = SignalDirection.FLAT
            signals.append(
                _signal(
                    SignalDirection.FLAT, bar, f"RSI {rsi_value} rose above {params.overbought}"
                )
            )

    return RsiReversionState(rsi_state, bar.close, position), signals


def _signal(direction: SignalDirection, bar: Bar, reason: str) -> Signal:
    return Signal(direction=direction, reason=reason, bar_start=bar.bar_start)


register_strategy_type(
    STRATEGY_TYPE,
    StrategySpec(
        params_model=RsiReversionParams,
        initial_state=initial_state,
        evaluate=evaluate,
    ),
)
