"""One bar's worth of work for one deployment/symbol: `step`.

Mirrors `application.backtest.engine`'s per-bar loop deliberately and closely — apply any
signal deferred from the previous bar, evaluate the strategy fresh, then defer or apply
whatever it produces, depending on the fill model. A live driver that folds bars through
`step` one at a time, and a backtest that folds the same bars through `run_backtest`, are
running the identical state machine over the identical bars; the backtest/paper consistency
test (ADR-0007) is exactly this claim, checked by a test instead of by inspection.

This is also what the Strategy Engine and Paper Trading Engine being "separate" components
(`ARCHITECTURE.md` §7.2's `tw.signals.v1`) collapses to when nothing yet needs the extra
Kafka hop: one function, called once per bar, that both evaluates the strategy and decides
what its signal does. Splitting evaluation from execution across a topic is a later
decoupling to make if a second, independent consumer of raw signals ever exists — the same
"do not create a topic with no real consumer" rule `infrastructure/events/kafka/topics.py`
already states outright.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from tradewatch.application.execution import ExecutionConfig, SymbolLedger
from tradewatch.application.strategy import CompiledStrategy, Signal
from tradewatch.application.trading.config import PositionSizingConfig, RiskLimitsConfig
from tradewatch.application.trading.engine import OrderExecuted, OrderRejected, process_signal
from tradewatch.domain.entities import Bar

__all__ = ["SymbolRuntimeState", "step"]


@dataclass(frozen=True, slots=True)
class SymbolRuntimeState:
    """Everything one deployment needs to remember about one of its symbols, between bars.

    `strategy_state` is exactly what `CompiledStrategy.evaluate` threads through — the same
    object a backtest would hold for this symbol at this point in the bar sequence. Because
    it holds nothing else (no session, no clock), rebuilding it is just replaying bars
    through `evaluate` again, which is how the live driver restores it after a restart.
    """

    strategy_state: Any
    pending_signal: Signal | None
    ledger: SymbolLedger


def step(
    runtime: SymbolRuntimeState,
    bar: Bar,
    *,
    compiled: CompiledStrategy[Any, Any],
    other_positions_cost_basis: Decimal,
    recent_order_count: int,
    position_sizing: PositionSizingConfig,
    risk_limits: RiskLimitsConfig,
    execution_config: ExecutionConfig,
) -> tuple[SymbolRuntimeState, OrderExecuted | OrderRejected | None]:
    """Advance one symbol's state by one bar. Returns the new state and at most one outcome.

    At most one, never two: under `FillModel.NEXT_BAR_OPEN` a fresh signal only ever sets
    `pending_signal` for the *next* call, so a step with a pending fill never also produces
    an immediate one; under `CURRENT_CLOSE`/`VWAP` nothing is ever deferred, so
    `pending_signal` is always `None` and a step has only the immediate signal (if any) to
    apply. Either way, one bar advances a symbol by at most one order — matching a strategy
    core that fires on a crossing, not on every bar it happens to hold a position.
    """
    ledger = runtime.ledger
    outcome: OrderExecuted | OrderRejected | None = None

    if runtime.pending_signal is not None:
        outcome = process_signal(
            runtime.pending_signal.direction,
            bar=bar,
            ledger=ledger,
            other_positions_cost_basis=other_positions_cost_basis,
            recent_order_count=recent_order_count,
            position_sizing=position_sizing,
            risk_limits=risk_limits,
            execution_config=execution_config,
        )
        if isinstance(outcome, OrderExecuted):
            ledger = outcome.ledger

    new_state, signals = compiled.evaluate(runtime.strategy_state, bar)
    if len(signals) > 1:
        # Silently unsafe otherwise: NEXT_BAR_OPEN would keep only the last signal as
        # `pending` (dropping the others without ever applying them), and an immediate fill
        # model would apply every signal to `ledger` but return only the last `outcome` —
        # `PaperTradingService` persists exactly one order/fill per call, so earlier fills
        # would change cash/position in memory yet never reach the `orders`/`fills` tables,
        # breaking "the position-rebuild job reproduces the live-tracked position state
        # exactly from fills alone." No registered strategy type does this today (`evaluate`
        # is contractually `list[Signal]` for future strategies, but every current one
        # returns at most one) — this is a deliberate fail-fast, not a supported case.
        raise NotImplementedError(
            f"strategy produced {len(signals)} signals for one bar; step() only supports "
            "at most one signal per symbol per bar"
        )

    new_pending: Signal | None = None
    for signal in signals:
        if execution_config.fill_model.value == "next_bar_open":
            new_pending = signal
        else:
            outcome = process_signal(
                signal.direction,
                bar=bar,
                ledger=ledger,
                other_positions_cost_basis=other_positions_cost_basis,
                recent_order_count=recent_order_count,
                position_sizing=position_sizing,
                risk_limits=risk_limits,
                execution_config=execution_config,
            )
            if isinstance(outcome, OrderExecuted):
                ledger = outcome.ledger

    return (
        SymbolRuntimeState(strategy_state=new_state, pending_signal=new_pending, ledger=ledger),
        outcome,
    )
