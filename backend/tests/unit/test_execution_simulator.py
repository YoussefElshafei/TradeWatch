"""The Execution Simulator: fill math against hand computation, and ledger invariants that
must hold for *any* sequence of fills — the property-based half of `DEVELOPMENT_PLAN.md`'s
Phase 6 exit criteria.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tradewatch.application.execution import (
    ExecutionConfig,
    Fill,
    InsufficientCashError,
    SymbolLedger,
    simulate_fill,
)
from tradewatch.config.settings import FillModel
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import BarInterval, OrderSide

pytestmark = pytest.mark.unit

_INSTRUMENT_ID = uuid4()
_START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)


def _bar(*, open_: str, high: str, low: str, close: str, vwap: str | None) -> Bar:
    return Bar(
        instrument_id=_INSTRUMENT_ID,
        interval=BarInterval.ONE_DAY,
        bar_start=_START,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(1000),
        vwap=Decimal(vwap) if vwap is not None else None,
        trade_count=10,
        is_final=True,
        revision=0,
        recorded_at=_START,
    )


class TestSimulateFill:
    def test_buy_fills_above_reference_by_slippage(self) -> None:
        # NEXT_BAR_OPEN reads bar.open = 100. 50 bps slippage -> 100 * 1.005 = 100.5.
        # commission = 0.01/share * 10 + 0.001 * (100.5 * 10 = 1005) = 0.1 + 1.005 = 1.105.
        bar = _bar(open_="100", high="105", low="95", close="102", vwap="101")
        config = ExecutionConfig(
            slippage_bps=Decimal(50),
            commission_per_share=Decimal("0.01"),
            commission_pct=Decimal("0.001"),
            fill_model=FillModel.NEXT_BAR_OPEN,
        )
        fill = simulate_fill(OrderSide.BUY, Decimal(10), bar, config)
        assert fill.price == Decimal("100.500")
        assert fill.commission == Decimal("1.105")
        assert fill.filled_at == bar.bar_start

    def test_sell_fills_below_reference_by_slippage(self) -> None:
        # CURRENT_CLOSE reads bar.close = 100. 50 bps -> 100 * 0.995 = 99.5.
        # commission = 0 + 0.002 * (99.5 * 5 = 497.5) = 0.995.
        bar = _bar(open_="98", high="101", low="97", close="100", vwap="99.5")
        config = ExecutionConfig(
            slippage_bps=Decimal(50),
            commission_per_share=Decimal(0),
            commission_pct=Decimal("0.002"),
            fill_model=FillModel.CURRENT_CLOSE,
        )
        fill = simulate_fill(OrderSide.SELL, Decimal(5), bar, config)
        assert fill.price == Decimal("99.500")
        assert fill.commission == Decimal("0.995")

    def test_vwap_model_reads_vwap_field(self) -> None:
        bar = _bar(open_="100", high="110", low="90", close="105", vwap="103")
        config = ExecutionConfig(
            slippage_bps=Decimal(0),
            commission_per_share=Decimal(0),
            commission_pct=Decimal(0),
            fill_model=FillModel.VWAP,
        )
        fill = simulate_fill(OrderSide.BUY, Decimal(1), bar, config)
        assert fill.price == Decimal("103")

    def test_vwap_model_falls_back_to_close_when_vwap_missing(self) -> None:
        bar = _bar(open_="100", high="110", low="90", close="105", vwap=None)
        config = ExecutionConfig(
            slippage_bps=Decimal(0),
            commission_per_share=Decimal(0),
            commission_pct=Decimal(0),
            fill_model=FillModel.VWAP,
        )
        fill = simulate_fill(OrderSide.BUY, Decimal(1), bar, config)
        assert fill.price == Decimal("105")

    def test_zero_or_negative_quantity_rejected(self) -> None:
        bar = _bar(open_="100", high="100", low="100", close="100", vwap="100")
        config = ExecutionConfig(
            slippage_bps=Decimal(0),
            commission_per_share=Decimal(0),
            commission_pct=Decimal(0),
            fill_model=FillModel.CURRENT_CLOSE,
        )
        with pytest.raises(ValueError, match="quantity"):
            simulate_fill(OrderSide.BUY, Decimal(0), bar, config)


class TestSymbolLedger:
    def test_buy_then_sell_round_trip_hand_computed(self) -> None:
        # Buy 10 @ 100 + 1 commission -> cost basis (100*10+1)/10 = 100.1/share,
        # cash 10000-1001=8999.
        # Sell 10 @ 110 - 1 commission -> proceeds 1099, pnl = 1099 - 100.1*10 = 1099-1001 = 98.
        ledger = SymbolLedger(cash=Decimal(10000))
        buy_fill = _fill(price=Decimal(100), quantity=Decimal(10), commission=Decimal(1))
        ledger = ledger.buy(buy_fill)
        assert ledger.cash == Decimal(8999)
        assert ledger.quantity == Decimal(10)
        assert ledger.avg_entry_price == Decimal("100.1")

        sell_fill = _fill(price=Decimal(110), quantity=Decimal(10), commission=Decimal(1))
        ledger = ledger.sell(sell_fill)
        assert ledger.cash == Decimal(8999) + Decimal(1099)
        assert ledger.quantity == Decimal(0)
        assert ledger.realized_pnl == Decimal(98)
        # A fully closed position resets its cost basis rather than keeping a stale one.
        assert ledger.avg_entry_price == Decimal(0)

    def test_buying_more_than_cash_allows_raises(self) -> None:
        ledger = SymbolLedger(cash=Decimal(100))
        fill = _fill(price=Decimal(100), quantity=Decimal(10), commission=Decimal(0))
        with pytest.raises(InsufficientCashError):
            ledger.buy(fill)

    def test_selling_more_than_held_raises(self) -> None:
        ledger = SymbolLedger(cash=Decimal(0), quantity=Decimal(5), avg_entry_price=Decimal(10))
        fill = _fill(price=Decimal(10), quantity=Decimal(6), commission=Decimal(0))
        with pytest.raises(ValueError, match="shorting"):
            ledger.sell(fill)

    def test_averaging_in_across_two_buys(self) -> None:
        # Buy 10 @ 100 (basis 100/share), then 10 @ 120 (basis 120/share) -> avg (100+120)/2=110.
        ledger = SymbolLedger(cash=Decimal(100000))
        ledger = ledger.buy(_fill(price=Decimal(100), quantity=Decimal(10), commission=Decimal(0)))
        ledger = ledger.buy(_fill(price=Decimal(120), quantity=Decimal(10), commission=Decimal(0)))
        assert ledger.quantity == Decimal(20)
        assert ledger.avg_entry_price == Decimal(110)


def _fill(*, price: Decimal, quantity: Decimal, commission: Decimal) -> Fill:
    return Fill(
        price=price,
        quantity=quantity,
        commission=commission,
        slippage_bps=Decimal(0),
        filled_at=_START,
    )


# ---------------------------------------------------------------------------
# Property-based: cash + market value stays internally consistent across any fill sequence.
# ---------------------------------------------------------------------------

_prices = st.decimals(
    min_value="1", max_value="1000", places=2, allow_nan=False, allow_infinity=False
)
_fractions = st.decimals(
    min_value="0.01", max_value="1", places=2, allow_nan=False, allow_infinity=False
)
_sides = st.sampled_from(["buy", "sell"])


@given(steps=st.lists(st.tuples(_sides, _prices, _fractions), min_size=1, max_size=25))
def test_equity_at_the_fill_price_drops_by_exactly_the_commission(
    steps: list[tuple[str, Decimal, Decimal]],
) -> None:
    """For *any* sequence of buys and sells: marking the ledger to market at the price a
    fill just executed at, equity must fall by exactly that fill's commission — never more,
    never less, and never in the other direction. This is what "cash plus the market value
    of positions stays internally consistent" means operationally: the only way this
    simulator ever destroys value is the commission it was configured to charge.
    """
    ledger = SymbolLedger(cash=Decimal(1_000_000))
    config = ExecutionConfig(
        slippage_bps=Decimal(0),  # isolates the commission effect from the slippage one
        commission_per_share=Decimal("0.005"),
        commission_pct=Decimal("0.0005"),
        fill_model=FillModel.CURRENT_CLOSE,
    )

    for side, price, fraction in steps:
        price_str = str(price)
        bar = _bar(open_=price_str, high=price_str, low=price_str, close=price_str, vwap=price_str)

        if side == "buy":
            per_share_cost = price * (1 + config.commission_pct) + config.commission_per_share
            affordable = ledger.cash / per_share_cost
            quantity = (affordable * fraction).quantize(Decimal("0.0001"))
            if quantity <= 0:
                continue
            fill = simulate_fill(OrderSide.BUY, quantity, bar, config)
            if fill.price * fill.quantity + fill.commission > ledger.cash:
                continue  # a rounding-edge order Hypothesis generated; not what this test probes
            before = ledger.total_equity(fill.price)
            ledger = ledger.buy(fill)
        else:
            if ledger.quantity <= 0:
                continue
            quantity = (ledger.quantity * fraction).quantize(Decimal("0.0001"))
            if quantity <= 0:
                continue
            fill = simulate_fill(OrderSide.SELL, quantity, bar, config)
            before = ledger.total_equity(fill.price)
            ledger = ledger.sell(fill)

        after = ledger.total_equity(fill.price)
        assert after == before - fill.commission

    # Regardless of path: never conjure shares from nothing, never spend cash you don't have.
    assert ledger.cash >= 0
    assert ledger.quantity >= 0
