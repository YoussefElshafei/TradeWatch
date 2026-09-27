"""The Paper Trading Engine's `process_signal`: sizing, risk limits, and the order-rate
ceiling (failure F15) — the behaviour `application.backtest.engine` deliberately does not
need, layered on top of the same shared `apply_signal`/`simulate_fill` both drivers call.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from uuid import uuid4

import pytest

from tradewatch.application.execution import ExecutionConfig, SymbolLedger
from tradewatch.application.trading import (
    OrderExecuted,
    OrderRejected,
    PositionSizingConfig,
    RiskLimitsConfig,
    process_signal,
)
from tradewatch.config.settings import FillModel
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import BarInterval, SignalDirection

pytestmark = pytest.mark.unit

_INSTRUMENT_ID = uuid4()
_START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)

_ZERO_COST = ExecutionConfig(
    slippage_bps=Decimal(0),
    commission_per_share=Decimal(0),
    commission_pct=Decimal(0),
    fill_model=FillModel.CURRENT_CLOSE,
)

_PERMISSIVE_LIMITS = RiskLimitsConfig(
    max_position_pct=Decimal(1),
    max_gross_exposure_pct=Decimal(1),
    max_orders_per_minute=1000,
)
_FULL_SIZING = PositionSizingConfig(target_equity_pct=Decimal(1))


def _bar(close: str) -> Bar:
    price = Decimal(close)
    return Bar(
        instrument_id=_INSTRUMENT_ID,
        interval=BarInterval.ONE_DAY,
        bar_start=_START,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal(1000),
        vwap=price,
        trade_count=10,
        is_final=True,
        revision=0,
        recorded_at=_START,
    )


class TestOrdinaryFills:
    def test_long_signal_while_flat_opens_a_position_sized_to_the_target(self) -> None:
        ledger = SymbolLedger(cash=Decimal(10000))
        outcome = process_signal(
            SignalDirection.LONG,
            bar=_bar("10"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=0,
            position_sizing=PositionSizingConfig(target_equity_pct=Decimal("0.5")),
            risk_limits=_PERMISSIVE_LIMITS,
            execution_config=_ZERO_COST,
        )
        assert isinstance(outcome, OrderExecuted)
        assert outcome.fill.quantity == Decimal(500)  # 50% of 10000 / 10 per share
        assert outcome.ledger.cash == Decimal(5000)

    def test_flat_signal_while_open_closes_the_position_regardless_of_risk_limits(self) -> None:
        ledger = SymbolLedger(cash=Decimal(0), quantity=Decimal(100), avg_entry_price=Decimal(10))
        tight_limits = RiskLimitsConfig(
            max_position_pct=Decimal("0.0001"),
            max_gross_exposure_pct=Decimal("0.0001"),
            max_orders_per_minute=1000,
        )
        outcome = process_signal(
            SignalDirection.FLAT,
            bar=_bar("12"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=0,
            position_sizing=_FULL_SIZING,
            risk_limits=tight_limits,
            execution_config=_ZERO_COST,
        )
        assert isinstance(outcome, OrderExecuted)
        assert outcome.ledger.quantity == Decimal(0)

    def test_long_while_already_long_is_a_no_op(self) -> None:
        ledger = SymbolLedger(cash=Decimal(0), quantity=Decimal(100), avg_entry_price=Decimal(10))
        outcome = process_signal(
            SignalDirection.LONG,
            bar=_bar("12"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=0,
            position_sizing=_FULL_SIZING,
            risk_limits=_PERMISSIVE_LIMITS,
            execution_config=_ZERO_COST,
        )
        assert outcome is None

    def test_flat_while_already_flat_is_a_no_op(self) -> None:
        ledger = SymbolLedger(cash=Decimal(1000))
        outcome = process_signal(
            SignalDirection.FLAT,
            bar=_bar("10"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=0,
            position_sizing=_FULL_SIZING,
            risk_limits=_PERMISSIVE_LIMITS,
            execution_config=_ZERO_COST,
        )
        assert outcome is None


class TestOrderRateCeiling:
    def test_rejects_when_the_ceiling_is_already_reached(self) -> None:
        ledger = SymbolLedger(cash=Decimal(10000))
        limits = RiskLimitsConfig(
            max_position_pct=Decimal(1), max_gross_exposure_pct=Decimal(1), max_orders_per_minute=5
        )
        outcome = process_signal(
            SignalDirection.LONG,
            bar=_bar("10"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=5,
            position_sizing=_FULL_SIZING,
            risk_limits=limits,
            execution_config=_ZERO_COST,
        )
        assert isinstance(outcome, OrderRejected)
        assert outcome.reason_code == "order_rate_ceiling"

    def test_checked_before_sizing_even_runs(self) -> None:
        """A ceiling breach rejects even a signal that would otherwise be unaffordable
        anyway — the two failure modes must not be confused with each other by a caller
        inspecting `reason_code`.
        """
        ledger = SymbolLedger(cash=Decimal(0))
        limits = RiskLimitsConfig(
            max_position_pct=Decimal(1), max_gross_exposure_pct=Decimal(1), max_orders_per_minute=1
        )
        outcome = process_signal(
            SignalDirection.LONG,
            bar=_bar("10"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=1,
            position_sizing=_FULL_SIZING,
            risk_limits=limits,
            execution_config=_ZERO_COST,
        )
        assert isinstance(outcome, OrderRejected)
        assert outcome.reason_code == "order_rate_ceiling"


class TestRiskLimits:
    def test_rejects_a_position_larger_than_max_position_pct(self) -> None:
        ledger = SymbolLedger(cash=Decimal(10000))
        limits = RiskLimitsConfig(
            max_position_pct=Decimal("0.1"),  # 10% of equity
            max_gross_exposure_pct=Decimal(1),
            max_orders_per_minute=1000,
        )
        # Sizing targets 100% of equity, i.e. a position ten times the 10% limit.
        outcome = process_signal(
            SignalDirection.LONG,
            bar=_bar("10"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=0,
            position_sizing=_FULL_SIZING,
            risk_limits=limits,
            execution_config=_ZERO_COST,
        )
        assert isinstance(outcome, OrderRejected)
        assert outcome.reason_code == "max_position_pct_exceeded"

    def test_rejects_when_gross_exposure_across_positions_would_be_exceeded(self) -> None:
        ledger = SymbolLedger(cash=Decimal(5000))
        limits = RiskLimitsConfig(
            max_position_pct=Decimal(1),
            max_gross_exposure_pct=Decimal("0.5"),
            max_orders_per_minute=1000,
        )
        # Another position already worth 4000 against a would-be 10000 total equity: adding
        # this one (sized to all 5000 cash) pushes gross exposure to 9000 / 10000 = 90%.
        outcome = process_signal(
            SignalDirection.LONG,
            bar=_bar("10"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(4000),
            recent_order_count=0,
            position_sizing=_FULL_SIZING,
            risk_limits=limits,
            execution_config=_ZERO_COST,
        )
        assert isinstance(outcome, OrderRejected)
        assert outcome.reason_code == "max_gross_exposure_pct_exceeded"

    def test_accepts_a_position_within_both_limits(self) -> None:
        ledger = SymbolLedger(cash=Decimal(10000))
        limits = RiskLimitsConfig(
            max_position_pct=Decimal("0.5"),
            max_gross_exposure_pct=Decimal("0.5"),
            max_orders_per_minute=1000,
        )
        sizing = PositionSizingConfig(target_equity_pct=Decimal("0.5"))
        outcome = process_signal(
            SignalDirection.LONG,
            bar=_bar("10"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=0,
            position_sizing=sizing,
            risk_limits=limits,
            execution_config=_ZERO_COST,
        )
        assert isinstance(outcome, OrderExecuted)


class TestInsufficientCash:
    def test_rejects_rather_than_crashes_when_the_budget_buys_nothing(self) -> None:
        # Budget smaller than the sizing precision (1e-8 shares) at this price quantizes
        # down to exactly zero shares.
        ledger = SymbolLedger(cash=Decimal("0.0000001"))
        outcome = process_signal(
            SignalDirection.LONG,
            bar=_bar("10000"),
            ledger=ledger,
            other_positions_cost_basis=Decimal(0),
            recent_order_count=0,
            position_sizing=_FULL_SIZING,
            risk_limits=_PERMISSIVE_LIMITS,
            execution_config=_ZERO_COST,
        )
        assert isinstance(outcome, OrderRejected)
        assert outcome.reason_code == "insufficient_cash"
