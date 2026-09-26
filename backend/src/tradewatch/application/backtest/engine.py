"""The Backtest Engine: the pure computation at the centre of the Backtest Runner.

`ARCHITECTURE.md` §3.6 splits the Backtest Runner into a driver (owns bar streaming, database
reads and writes — see `application/backtest/service.py`) and this: a forward-only fold over
bars that calls the strategy core (`application/strategy`) and the Execution Simulator
(`application/execution`) and nothing else. No session, no clock, no logging. That is what
makes the golden-fixture, determinism, and lookahead tests possible at all — there is nothing
here for a database or a wall clock to make non-reproducible.

**Multi-symbol accounting.** A run's `initial_cash` is split evenly across its `symbols` up
front, into an independent `SymbolLedger` per symbol. This is a deliberate simplification,
not an oversight:

    ponytail: equal-weight fixed sub-allocation per symbol, decided once at the start rather
    than rebalanced against total equity as positions change. True dynamic position sizing
    and cross-symbol risk limits are Phase 7's Paper Trading Engine's job (`max_position_pct`,
    `max_gross_exposure_pct` in `ExecutionSettings` are read there, not here). A backtest over
    a basket only needs a rule that is deterministic and never lets one symbol's fill starve
    another's — equal, fixed, independent sub-accounts are the smallest thing that satisfies
    both without guessing at a sizing scheme nobody has asked for yet.

**Fill timing.** `FillModel.NEXT_BAR_OPEN` defers a signal's fill to the *next* bar of the
*same* symbol — the standard way to keep a backtest honest about the fact that nobody can
react to a bar's close before it has closed. `CURRENT_CLOSE` and `VWAP` fill within the same
bar that produced the signal, which is not lookahead: by the time a bar's close (or VWAP) is
known, the bar itself has already finished. Either way, this module never receives a bar its
forward-only iterator has not already reached, which is what the lookahead test exercises.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Any

from tradewatch.application.backtest.metrics import (
    cagr_pct,
    exposure_pct,
    max_drawdown_pct,
    sharpe_ratio,
    total_return_pct,
    volatility_pct,
    win_rate_pct,
)
from tradewatch.application.execution import ExecutionConfig, SymbolLedger, simulate_fill
from tradewatch.application.strategy import Signal, compile_strategy
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import OrderSide, SignalDirection

__all__ = [
    "BacktestMetrics",
    "BacktestResult",
    "BacktestTradeResult",
    "EquityPointResult",
    "run_backtest",
]

#: Fractional shares are allowed (this is a simulator, not a broker with lot-size rules —
#: `Instrument.lot_size` is a real-money concern for Phase 7, not this pure engine), but a
#: sizing calculation still needs a stopping precision. Matches the database's money scale.
_SIZE_PRECISION = Decimal("0.00000001")


@dataclass(frozen=True, slots=True)
class BacktestTradeResult:
    """One row of `backtest_trades`. `exit_*` fields are all `None` together, or none are —
    a trade still open when the run ended is not force-closed at the last price, because
    that would report a P&L for an exit that never happened.
    """

    symbol: str
    side: OrderSide
    quantity: Decimal
    entry_price: Decimal
    entry_at: dt.datetime
    exit_price: Decimal | None
    exit_at: dt.datetime | None
    pnl: Decimal | None
    return_pct: Decimal | None


@dataclass(frozen=True, slots=True)
class EquityPointResult:
    """One row of `backtest_equity_points`."""

    as_of: dt.datetime
    total_equity: Decimal
    drawdown_pct: Decimal


@dataclass(frozen=True, slots=True)
class BacktestMetrics:
    """The `backtest_runs.metrics` JSONB payload."""

    total_return_pct: Decimal
    cagr_pct: Decimal | None
    max_drawdown_pct: Decimal
    sharpe_ratio: Decimal | None
    volatility_pct: Decimal | None
    win_rate_pct: Decimal | None
    trade_count: int
    exposure_pct: Decimal

    def as_json(self) -> dict[str, Any]:
        """Decimals as strings, same convention as every other money value (ADR-0009)."""

        def _opt(value: Decimal | None) -> str | None:
            return None if value is None else str(value)

        return {
            "total_return_pct": str(self.total_return_pct),
            "cagr_pct": _opt(self.cagr_pct),
            "max_drawdown_pct": str(self.max_drawdown_pct),
            "sharpe_ratio": _opt(self.sharpe_ratio),
            "volatility_pct": _opt(self.volatility_pct),
            "win_rate_pct": _opt(self.win_rate_pct),
            "trade_count": self.trade_count,
            "exposure_pct": str(self.exposure_pct),
        }


@dataclass(frozen=True, slots=True)
class BacktestResult:
    final_equity: Decimal
    trades: list[BacktestTradeResult]
    equity_curve: list[EquityPointResult]
    metrics: BacktestMetrics


@dataclass(frozen=True, slots=True)
class _OpenTrade:
    quantity: Decimal
    entry_price: Decimal
    entry_at: dt.datetime


def run_backtest(
    *,
    strategy_type: str,
    parameters: dict[str, Any],
    symbols: Sequence[str],
    bars_by_symbol: Mapping[str, Sequence[Bar]],
    initial_cash: Decimal,
    execution_config: ExecutionConfig,
    start: dt.datetime,
    end: dt.datetime,
) -> BacktestResult:
    """Run one strategy over historical bars and return trades, an equity curve, and metrics.

    Deterministic: bars are processed in one fixed order — chronological, tied-broken by
    symbol — so calling this twice with the same arguments produces byte-identical results,
    including the exact trade sequence, not just the same summary numbers.

    Raises:
        ValueError: `symbols` is empty, or `initial_cash` is not positive.
        UnknownStrategyTypeError: `strategy_type` is not registered.
        InvalidStrategyParametersError: `parameters` fails the strategy type's schema.
    """
    if not symbols:
        raise ValueError("symbols must not be empty")
    if initial_cash <= 0:
        raise ValueError(f"initial_cash must be positive, got {initial_cash}")

    compiled = compile_strategy(strategy_type, parameters)
    per_symbol_cash = initial_cash / Decimal(len(symbols))

    states: dict[str, Any] = {symbol: compiled.initial_state() for symbol in symbols}
    ledgers: dict[str, SymbolLedger] = {
        symbol: SymbolLedger(cash=per_symbol_cash) for symbol in symbols
    }
    pending: dict[str, Signal | None] = dict.fromkeys(symbols)
    open_trades: dict[str, _OpenTrade | None] = dict.fromkeys(symbols)
    last_price: dict[str, Decimal] = {}

    trades: list[BacktestTradeResult] = []
    equity_by_time: dict[dt.datetime, Decimal] = {}
    position_open_by_time: dict[dt.datetime, bool] = {}

    events = sorted(
        ((bar, symbol) for symbol in symbols for bar in bars_by_symbol.get(symbol, ())),
        key=lambda item: (item[0].bar_start, item[1]),
    )

    for bar, symbol in events:
        deferred = pending[symbol]
        if deferred is not None:
            ledgers[symbol], open_trades[symbol], closed = _apply_signal(
                symbol, deferred, bar, execution_config, ledgers[symbol], open_trades[symbol]
            )
            if closed is not None:
                trades.append(closed)
            pending[symbol] = None

        states[symbol], signals = compiled.evaluate(states[symbol], bar)

        for signal in signals:
            if execution_config.fill_model.value == "next_bar_open":
                pending[symbol] = signal
            else:
                ledgers[symbol], open_trades[symbol], closed = _apply_signal(
                    symbol, signal, bar, execution_config, ledgers[symbol], open_trades[symbol]
                )
                if closed is not None:
                    trades.append(closed)

        last_price[symbol] = bar.close
        # A symbol with no bar yet has never traded, so its idle cash — not a mark-to-market
        # position value — is its whole contribution to equity.
        equity_by_time[bar.bar_start] = sum(
            (
                ledgers[sym].total_equity(last_price[sym])
                if sym in last_price
                else ledgers[sym].cash
                for sym in symbols
            ),
            Decimal(0),
        )
        position_open_by_time[bar.bar_start] = any(
            ledger.quantity > 0 for ledger in ledgers.values()
        )

    for symbol in symbols:
        open_trade = open_trades[symbol]
        if open_trade is not None:
            trades.append(
                BacktestTradeResult(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    quantity=open_trade.quantity,
                    entry_price=open_trade.entry_price,
                    entry_at=open_trade.entry_at,
                    exit_price=None,
                    exit_at=None,
                    pnl=None,
                    return_pct=None,
                )
            )

    equity_curve = _build_equity_curve(equity_by_time)
    final_equity = equity_curve[-1].total_equity if equity_curve else initial_cash
    years = Decimal((end - start).days) / Decimal("365.25")
    equity_values = [point.total_equity for point in equity_curve]
    equity_times = [point.as_of for point in equity_curve]
    closed_pnls = [trade.pnl for trade in trades if trade.pnl is not None]
    exposure_flags = [
        position_open_by_time[timestamp] for timestamp in sorted(position_open_by_time)
    ]

    metrics = BacktestMetrics(
        total_return_pct=total_return_pct(initial_cash, final_equity),
        cagr_pct=cagr_pct(initial_cash, final_equity, years),
        max_drawdown_pct=max_drawdown_pct(equity_values),
        sharpe_ratio=sharpe_ratio(equity_values, equity_times),
        volatility_pct=volatility_pct(equity_values, equity_times),
        win_rate_pct=win_rate_pct(closed_pnls),
        trade_count=len(trades),
        exposure_pct=exposure_pct(exposure_flags),
    )

    return BacktestResult(
        final_equity=final_equity, trades=trades, equity_curve=equity_curve, metrics=metrics
    )


def _size_long_entry(ledger: SymbolLedger, bar: Bar, config: ExecutionConfig) -> Decimal:
    """How many shares the ledger's available cash buys, net of commission, at this fill.

    A probe fill of quantity 1 reveals the exact per-share all-in cost — slippage is a fixed
    fraction of the reference price, not a function of size in this model — so the real
    quantity can be solved for directly rather than searched for.
    """
    probe = simulate_fill(OrderSide.BUY, Decimal(1), bar, config)
    per_share_cost = probe.price * (1 + config.commission_pct) + config.commission_per_share
    if per_share_cost <= 0:
        return Decimal(0)
    quantity = (ledger.cash / per_share_cost).quantize(_SIZE_PRECISION, rounding=ROUND_DOWN)
    return max(quantity, Decimal(0))


def _apply_signal(
    symbol: str,
    signal: Signal,
    fill_bar: Bar,
    config: ExecutionConfig,
    ledger: SymbolLedger,
    open_trade: _OpenTrade | None,
) -> tuple[SymbolLedger, _OpenTrade | None, BacktestTradeResult | None]:
    """Turn one signal into (at most) one fill against `fill_bar`.

    Returns the possibly-updated ledger and open-trade tracker, and a completed
    `BacktestTradeResult` when this signal closed a position — `None` otherwise, including
    when the signal does not change the current position (already long, or already flat).
    """
    if signal.direction is SignalDirection.LONG and open_trade is None:
        quantity = _size_long_entry(ledger, fill_bar, config)
        if quantity <= 0:
            return ledger, open_trade, None
        fill = simulate_fill(OrderSide.BUY, quantity, fill_bar, config)
        new_ledger = ledger.buy(fill)
        new_open_trade = _OpenTrade(
            quantity=fill.quantity, entry_price=fill.price, entry_at=fill.filled_at
        )
        return new_ledger, new_open_trade, None

    if signal.direction is SignalDirection.FLAT and open_trade is not None:
        fill = simulate_fill(OrderSide.SELL, open_trade.quantity, fill_bar, config)
        new_ledger = ledger.sell(fill)
        pnl = new_ledger.realized_pnl - ledger.realized_pnl
        entry_notional = open_trade.entry_price * open_trade.quantity
        return_pct = (pnl / entry_notional * 100) if entry_notional > 0 else Decimal(0)
        completed = BacktestTradeResult(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=open_trade.quantity,
            entry_price=open_trade.entry_price,
            entry_at=open_trade.entry_at,
            exit_price=fill.price,
            exit_at=fill.filled_at,
            pnl=pnl,
            return_pct=return_pct,
        )
        return new_ledger, None, completed

    return ledger, open_trade, None


def _build_equity_curve(equity_by_time: Mapping[dt.datetime, Decimal]) -> list[EquityPointResult]:
    """Sort the recorded points chronologically and attach a running drawdown to each."""
    points: list[EquityPointResult] = []
    peak: Decimal | None = None
    for timestamp in sorted(equity_by_time):
        equity = equity_by_time[timestamp]
        peak = equity if peak is None else max(peak, equity)
        drawdown = (equity - peak) / peak * 100 if peak > 0 else Decimal(0)
        points.append(
            EquityPointResult(as_of=timestamp, total_equity=equity, drawdown_pct=drawdown)
        )
    return points
