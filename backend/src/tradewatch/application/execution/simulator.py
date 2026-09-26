"""The Execution Simulator (ADR-0007): fill logic, and the cash/position ledger it feeds.

A **library, not a service** — no I/O, no clock, nothing that only makes sense inside a
worker. That is what lets the Backtest Runner (this phase) and the Paper Trading Engine
(Phase 7) import the exact same module rather than two implementations that can quietly
disagree about what a fill costs.

**Long-only.** `SymbolLedger` has no concept of a short position: `buy` only ever increases
`quantity` and `sell` only ever reduces it, and `sell` refuses to reduce past zero. Short
selling is D16, explicitly deferred system-wide (`OrderType`'s docstring, `Position.is_short`
in the domain layer) — modelling borrow cost and margin here before either exists elsewhere
would be speculative machinery with nothing to connect it to.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal

from tradewatch.application.execution.config import ExecutionConfig
from tradewatch.config.settings import FillModel
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import OrderSide

__all__ = ["Fill", "InsufficientCashError", "SymbolLedger", "simulate_fill"]

_BPS = Decimal(10_000)


class InsufficientCashError(ValueError):
    """A buy was sized larger than the cash available to pay for it.

    The Backtest Runner is expected to size orders so this never happens (see
    `application/backtest/engine.py`); it exists here as the ledger's own defence, the same
    way a repository enforces ownership rather than trusting every caller to check first.
    """


@dataclass(frozen=True, slots=True)
class Fill:
    """An execution against an order — the shared shape `fills.price/.../.slippage_bps`
    mirrors column for column, so persisting one is a direct mapping, not a translation.
    """

    price: Decimal
    quantity: Decimal
    commission: Decimal
    slippage_bps: Decimal
    filled_at: dt.datetime


def _reference_price(bar: Bar, fill_model: FillModel) -> Decimal:
    """The price a fill model reads off `bar`.

    Note what this function does *not* decide: *which* bar to pass. `FillModel.NEXT_BAR_OPEN`
    means "fill using the open of the bar after the one that produced the signal" — but that
    is a question of timing the caller answers by choosing which `bar` to hand over here, not
    a question this function can answer by inspecting one bar in isolation. Both backtest and
    live drivers make that same timing decision the same way, which is exactly the point of
    keeping it out of the simulator: a driver difference here would be the ADR-0007 bug this
    whole module exists to prevent.
    """
    if fill_model is FillModel.NEXT_BAR_OPEN:
        return bar.open
    if fill_model is FillModel.CURRENT_CLOSE:
        return bar.close
    return bar.vwap if bar.vwap is not None else bar.close  # FillModel.VWAP


def simulate_fill(side: OrderSide, quantity: Decimal, bar: Bar, config: ExecutionConfig) -> Fill:
    """Apply slippage and commission to a market order against `bar`.

    Slippage always costs the trader — a buy fills *above* the reference price, a sell
    *below* it — because modelling it as sometimes-favourable would make every backtest look
    better than the fills it claims to simulate.
    """
    if quantity <= 0:
        raise ValueError(f"quantity must be positive, got {quantity}")

    reference = _reference_price(bar, config.fill_model)
    slippage_factor = config.slippage_bps / _BPS
    price = (
        reference * (1 + slippage_factor)
        if side is OrderSide.BUY
        else reference * (1 - slippage_factor)
    )
    commission = config.commission_per_share * quantity + config.commission_pct * (price * quantity)
    return Fill(
        price=price,
        quantity=quantity,
        commission=commission,
        slippage_bps=config.slippage_bps,
        filled_at=bar.bar_start,
    )


@dataclass(frozen=True, slots=True)
class SymbolLedger:
    """Cash and one instrument's position, for one sub-account of a backtest's portfolio.

    `avg_entry_price` includes entry commission, folded into the cost basis at the moment of
    the buy. That is what makes `sell`'s P&L a complete accounting of the round trip —
    ``proceeds - cost_basis`` already nets both the entry and exit commission — rather than a
    figure that quietly omits half of what the trade actually cost.
    """

    cash: Decimal
    quantity: Decimal = Decimal(0)
    avg_entry_price: Decimal = Decimal(0)
    realized_pnl: Decimal = Decimal(0)

    def market_value(self, price: Decimal) -> Decimal:
        return self.quantity * price

    def total_equity(self, price: Decimal) -> Decimal:
        return self.cash + self.market_value(price)

    def buy(self, fill: Fill) -> SymbolLedger:
        total_cost = fill.price * fill.quantity + fill.commission
        if total_cost > self.cash:
            raise InsufficientCashError(
                f"buying {fill.quantity} at {fill.price} plus {fill.commission} commission "
                f"costs {total_cost}, more than the {self.cash} available"
            )
        new_quantity = self.quantity + fill.quantity
        cost_basis_per_share = total_cost / fill.quantity
        new_avg_entry = (
            self.avg_entry_price * self.quantity + cost_basis_per_share * fill.quantity
        ) / new_quantity
        return SymbolLedger(
            cash=self.cash - total_cost,
            quantity=new_quantity,
            avg_entry_price=new_avg_entry,
            realized_pnl=self.realized_pnl,
        )

    def sell(self, fill: Fill) -> SymbolLedger:
        if fill.quantity > self.quantity:
            raise ValueError(
                f"cannot sell {fill.quantity}, only {self.quantity} held (D16: no shorting)"
            )
        proceeds = fill.price * fill.quantity - fill.commission
        pnl = proceeds - self.avg_entry_price * fill.quantity
        new_quantity = self.quantity - fill.quantity
        return SymbolLedger(
            cash=self.cash + proceeds,
            quantity=new_quantity,
            # A fully closed position has no cost basis left to remember; keeping the old
            # `avg_entry_price` around would make the next fresh buy average against a stale
            # number instead of starting clean.
            avg_entry_price=self.avg_entry_price if new_quantity > 0 else Decimal(0),
            realized_pnl=self.realized_pnl + pnl,
        )
