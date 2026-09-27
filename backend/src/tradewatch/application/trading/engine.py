"""The Paper Trading Engine's core: one pure function, `process_signal`.

Everything about *how* a signal becomes a fill — sizing, then the fill itself — goes through
`application.execution.apply_signal`, the exact function `application.backtest.engine` calls.
This module adds only what the backtest engine deliberately does not need: configurable
position sizing (backtest fixes it at "the sub-ledger's whole cash"), risk limits that can
reject a fill outright, and the order-rate ceiling (failure F15). That both drivers still
bottom out in `apply_signal` is what makes the backtest/paper consistency test a property of
the code, not a coincidence the test happens to observe (ADR-0007).

No I/O, no session, no clock — like the backtest engine, on purpose. The worker
(`workers/paper_trading.py`) owns fetching a deployment's portfolio/position state and
persisting the outcome; this function only decides what that outcome should be.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from tradewatch.application.execution import ExecutionConfig, Fill, SymbolLedger, apply_signal
from tradewatch.application.trading.config import PositionSizingConfig, RiskLimitsConfig
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import OrderSide, SignalDirection

__all__ = ["OrderExecuted", "OrderRejected", "process_signal"]

#: Long-only (D16): opening a position is always a buy, closing one always a sell. Recording
#: it on the outcome — rather than making every caller re-derive it from `direction` — is
#: what lets a rejected order (which never reaches `apply_signal`, so has no `Fill` to read a
#: side off) still know what side it *would* have been, for the persisted `orders` row.
_SIDE_FOR_DIRECTION = {
    SignalDirection.LONG: OrderSide.BUY,
    SignalDirection.FLAT: OrderSide.SELL,
}


@dataclass(frozen=True, slots=True)
class OrderExecuted:
    """A signal that produced a fill. `ledger` is the instrument's position/cash after it."""

    side: OrderSide
    ledger: SymbolLedger
    fill: Fill


@dataclass(frozen=True, slots=True)
class OrderRejected:
    """A signal that would have traded, but was refused before (or instead of) a fill.

    `reason_code` is the stable, machine-checkable value stored on the `orders` row
    (`ARCHITECTURE.md`'s F15 entry: "a recorded reason_code"); `reason` is the human sentence
    that explains it in a UI.
    """

    side: OrderSide
    reason_code: str
    reason: str


def process_signal(
    direction: SignalDirection,
    *,
    bar: Bar,
    ledger: SymbolLedger,
    other_positions_cost_basis: Decimal,
    recent_order_count: int,
    position_sizing: PositionSizingConfig,
    risk_limits: RiskLimitsConfig,
    execution_config: ExecutionConfig,
) -> OrderExecuted | OrderRejected | None:
    """Decide what one deployment's signal does to one instrument's position.

    Returns `None` when the signal does not change the position at all (already long and
    told to go long again, already flat and told to go flat) — the same silent no-op
    `apply_signal` itself returns via `fill=None`, surfaced here as a distinct case so a
    caller never has to guess whether `None` means "no-op" or "rejected".

    Order of checks matters and matches F15 exactly: the order-rate ceiling is checked
    *before* sizing or the simulator ever run, because a runaway strategy should be stopped
    by counting its orders, not by computing a fill for the one that will be thrown away.
    """
    side = _SIDE_FOR_DIRECTION.get(direction)
    if side is None:
        return None

    if recent_order_count >= risk_limits.max_orders_per_minute:
        return OrderRejected(
            side=side,
            reason_code="order_rate_ceiling",
            reason=(
                f"{recent_order_count} orders already placed in the last minute, "
                f"at or above the {risk_limits.max_orders_per_minute}/minute ceiling"
            ),
        )

    is_open = ledger.quantity > 0
    total_equity = ledger.cash + ledger.quantity * ledger.avg_entry_price
    total_equity += other_positions_cost_basis

    if direction is SignalDirection.LONG and not is_open:
        # Capped at this instrument's own ledger cash: `total_equity` includes other
        # positions' cost basis for the *risk* calculation below, but that is not spendable
        # cash in this ledger, and `apply_signal`/`SymbolLedger.buy` would raise trying to
        # spend money the ledger does not actually hold.
        cash_budget = min(ledger.cash, total_equity * position_sizing.target_equity_pct)
        outcome = apply_signal(
            direction, cash_budget=cash_budget, bar=bar, config=execution_config, ledger=ledger
        )
        if outcome.fill is None:
            return OrderRejected(
                side=side,
                reason_code="insufficient_cash",
                reason=(
                    f"a {position_sizing.target_equity_pct:.0%} allocation of "
                    f"{total_equity} equity does not buy one share at this fill"
                ),
            )

        position_value = outcome.ledger.quantity * outcome.fill.price
        if position_value > total_equity * risk_limits.max_position_pct:
            return OrderRejected(
                side=side,
                reason_code="max_position_pct_exceeded",
                reason=(
                    f"a {position_value} position would be "
                    f"{position_value / total_equity:.1%} of {total_equity} equity, "
                    f"above the {risk_limits.max_position_pct:.0%} limit"
                ),
            )
        gross_exposure = other_positions_cost_basis + position_value
        if gross_exposure > total_equity * risk_limits.max_gross_exposure_pct:
            return OrderRejected(
                side=side,
                reason_code="max_gross_exposure_pct_exceeded",
                reason=(
                    f"gross exposure of {gross_exposure} would be "
                    f"{gross_exposure / total_equity:.1%} of {total_equity} equity, above "
                    f"the {risk_limits.max_gross_exposure_pct:.0%} limit"
                ),
            )
        return OrderExecuted(side=side, ledger=outcome.ledger, fill=outcome.fill)

    if direction is SignalDirection.FLAT and is_open:
        # Closing a position never increases risk, so no risk limit ever blocks an exit.
        outcome = apply_signal(
            direction, cash_budget=Decimal(0), bar=bar, config=execution_config, ledger=ledger
        )
        assert outcome.fill is not None
        return OrderExecuted(side=side, ledger=outcome.ledger, fill=outcome.fill)

    return None
