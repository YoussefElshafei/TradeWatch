"""Turning one strategy signal into (at most) one fill against a `SymbolLedger`.

Extracted from the Backtest Runner (`application/backtest/engine.py`) so the Paper Trading
Engine (Phase 7) calls the exact same function rather than a second implementation of "what
does a LONG/FLAT signal do to a position" that could quietly disagree with the backtester's.
That sameness is what makes the backtest/paper consistency test (ADR-0007) hold by
construction: both drivers fold their bars through `evaluate()`, then through `apply_signal`,
then through the same `simulate_fill` this module already shares — nothing about *how* a
signal becomes a position differs between the two contexts, only where the bars and the
ledger come from.

Position sizing here is one policy — "spend the given cash budget entirely, net of
commission" — not the only one. The backtester passes its ledger's whole cash balance as the
budget (a fixed, equal sub-allocation decided once, per that module's own docstring); the
Paper Trading Engine passes whatever its configured position-sizing rule computes (e.g. a
percentage of current total equity). `apply_signal` does not know or care which; it just
spends the budget it is handed.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal

from tradewatch.application.execution.config import ExecutionConfig
from tradewatch.application.execution.simulator import Fill, SymbolLedger, simulate_fill
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import OrderSide, SignalDirection

__all__ = ["SignalOutcome", "apply_signal", "size_long_entry"]

#: Fractional shares are allowed (this is a simulator, not a broker with lot-size rules), but
#: a sizing calculation still needs a stopping precision. Matches the database's money scale.
_SIZE_PRECISION = Decimal("0.00000001")


def size_long_entry(cash_budget: Decimal, bar: Bar, config: ExecutionConfig) -> Decimal:
    """How many shares `cash_budget` buys, net of commission, at this fill.

    A probe fill of quantity 1 reveals the exact per-share all-in cost — slippage is a fixed
    fraction of the reference price, not a function of size in this model — so the real
    quantity can be solved for directly rather than searched for.
    """
    probe = simulate_fill(OrderSide.BUY, Decimal(1), bar, config)
    per_share_cost = probe.price * (1 + config.commission_pct) + config.commission_per_share
    if per_share_cost <= 0:
        return Decimal(0)
    quantity = (cash_budget / per_share_cost).quantize(_SIZE_PRECISION, rounding=ROUND_DOWN)
    return max(quantity, Decimal(0))


@dataclass(frozen=True, slots=True)
class SignalOutcome:
    """The result of folding one signal into a ledger.

    `fill` is `None` when the signal did not change the position — already long and told to
    go long again, already flat and told to go flat, or a long entry whose sizing produced
    zero shares (a cash budget too small to afford even one share after commission).
    """

    ledger: SymbolLedger
    fill: Fill | None


def apply_signal(
    direction: SignalDirection,
    *,
    cash_budget: Decimal,
    bar: Bar,
    config: ExecutionConfig,
    ledger: SymbolLedger,
) -> SignalOutcome:
    """Long-only, per D16: a LONG signal opens a position only when flat, a FLAT signal
    closes one only when long. Any other direction/state pairing is a no-op — most often
    "already long" on a bar that reasserts the same trend, which is not a new decision.
    """
    is_open = ledger.quantity > 0

    if direction is SignalDirection.LONG and not is_open:
        quantity = size_long_entry(cash_budget, bar, config)
        if quantity <= 0:
            return SignalOutcome(ledger, None)
        fill = simulate_fill(OrderSide.BUY, quantity, bar, config)
        return SignalOutcome(ledger.buy(fill), fill)

    if direction is SignalDirection.FLAT and is_open:
        fill = simulate_fill(OrderSide.SELL, ledger.quantity, bar, config)
        return SignalOutcome(ledger.sell(fill), fill)

    return SignalOutcome(ledger, None)
