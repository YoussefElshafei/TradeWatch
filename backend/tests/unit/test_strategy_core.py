"""The strategy core contract: `compile_strategy` + `evaluate(state, bar) -> (state, signals)`.

`ARCHITECTURE.md` §3.4 requires this to be pure, deterministic, and blind to any bar later
than the one just passed in. The tests below check exactly those three properties, plus the
crossing logic of each built-in type.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from tradewatch.application.strategy import (
    InvalidStrategyParametersError,
    UnknownStrategyTypeError,
    compile_strategy,
)
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import BarInterval

pytestmark = pytest.mark.unit

_INSTRUMENT_ID = uuid4()


def _bar(day: int, close: float | str) -> Bar:
    """A flat daily bar (open = high = low = close) — only the close price matters here."""
    price = Decimal(str(close))
    start = dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(days=day)
    return Bar(
        instrument_id=_INSTRUMENT_ID,
        interval=BarInterval.ONE_DAY,
        bar_start=start,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal(1000),
        vwap=price,
        trade_count=10,
        is_final=True,
        revision=0,
        recorded_at=start,
    )


def _run(compiled: object, bars: list[Bar]) -> list[list[str]]:
    """Directions signalled on each bar, as plain strings for easy comparison."""
    state = compiled.initial_state()  # type: ignore[attr-defined]
    out: list[list[str]] = []
    for bar in bars:
        state, signals = compiled.evaluate(state, bar)  # type: ignore[attr-defined]
        out.append([signal.direction.value for signal in signals])
    return out


class TestCompileStrategy:
    def test_unknown_strategy_type(self) -> None:
        with pytest.raises(UnknownStrategyTypeError):
            compile_strategy("not_a_real_strategy", {})

    def test_invalid_parameters_reports_the_field(self) -> None:
        with pytest.raises(InvalidStrategyParametersError) as excinfo:
            compile_strategy("sma_crossover", {"fast_period": 10, "slow_period": 5})
        assert isinstance(excinfo.value.errors, ValidationError)

    def test_unknown_field_rejected(self) -> None:
        """ADR-0006's schema-per-type promise means a typo'd parameter is caught, not ignored."""
        with pytest.raises(InvalidStrategyParametersError):
            compile_strategy("sma_crossover", {"fast_period": 3, "slow_period": 10, "typo": 1})


class TestSmaCrossover:
    def test_crosses_long_then_flat(self) -> None:
        compiled = compile_strategy("sma_crossover", {"fast_period": 2, "slow_period": 3})
        # fast(2)/slow(3) of: 10,10,10 (flat, both warm at bar index 2 -> 10/10, no cross),
        # then a rally (fast rises above slow), then a drop (fast falls back below slow).
        closes = [10, 10, 10, 12, 14, 16, 10, 8, 6]
        bars = [_bar(i, c) for i, c in enumerate(closes)]
        signals = _run(compiled, bars)
        fired = [(i, s) for i, directions in enumerate(signals) for s in directions]
        directions_fired = [s for _, s in fired]
        assert directions_fired == ["long", "flat"]
        long_index = next(i for i, s in fired if s == "long")
        flat_index = next(i for i, s in fired if s == "flat")
        assert long_index < flat_index

    def test_no_signal_before_warm_up(self) -> None:
        compiled = compile_strategy("sma_crossover", {"fast_period": 3, "slow_period": 5})
        bars = [_bar(i, 100 + i) for i in range(4)]  # slow needs 5 bars
        signals = _run(compiled, bars)
        assert all(direction_list == [] for direction_list in signals)

    def test_flat_market_never_signals(self) -> None:
        compiled = compile_strategy("sma_crossover", {"fast_period": 2, "slow_period": 3})
        bars = [_bar(i, 100) for i in range(10)]
        signals = _run(compiled, bars)
        assert all(direction_list == [] for direction_list in signals)


class TestRsiReversion:
    def test_dip_then_recovery_round_trips(self) -> None:
        compiled = compile_strategy(
            "rsi_reversion", {"rsi_period": 2, "oversold": 30, "overbought": 70}
        )
        # A sharp drop pushes RSI(2) below 30 (buy), then a sharp rally pushes it above 70 (exit).
        closes = [100, 95, 90, 85, 95, 105, 115, 125]
        bars = [_bar(i, c) for i, c in enumerate(closes)]
        signals = _run(compiled, bars)
        fired = [s for directions in signals for s in directions]
        assert "long" in fired
        assert "flat" in fired

    def test_first_bar_never_signals(self) -> None:
        """The very first bar has no previous close, so RSI cannot even be computed yet."""
        compiled = compile_strategy("rsi_reversion", {"rsi_period": 3})
        state = compiled.initial_state()
        _, signals = compiled.evaluate(state, _bar(0, 100))
        assert signals == []


class TestDeterminism:
    """`DEVELOPMENT_PLAN.md`'s own exit criterion: the same inputs produce the same outputs,
    not just the same summary — the exact same signal sequence, every run."""

    @pytest.mark.parametrize(
        ("strategy_type", "parameters"),
        [
            ("sma_crossover", {"fast_period": 3, "slow_period": 7}),
            ("rsi_reversion", {"rsi_period": 4}),
        ],
    )
    def test_same_bars_produce_the_same_signals_every_run(
        self, strategy_type: str, parameters: dict[str, object]
    ) -> None:
        closes = [100, 102, 99, 105, 108, 95, 90, 101, 112, 99, 97, 130, 88, 102]
        bars = [_bar(i, c) for i, c in enumerate(closes)]

        first = _run(compile_strategy(strategy_type, parameters), bars)
        second = _run(compile_strategy(strategy_type, parameters), bars)

        assert first == second


class TestNoLookahead:
    """Construct a case where seeing a future bar would change today's decision, and prove
    the strategy core cannot see it: replaying the same prefix with different, more dramatic
    bars appended afterward must not change a single signal already produced for that prefix.
    """

    @pytest.mark.parametrize(
        ("strategy_type", "parameters"),
        [
            ("sma_crossover", {"fast_period": 2, "slow_period": 4}),
            ("rsi_reversion", {"rsi_period": 3}),
        ],
    )
    def test_a_future_price_spike_does_not_change_earlier_signals(
        self, strategy_type: str, parameters: dict[str, object]
    ) -> None:
        prefix_closes = [100, 101, 99, 103, 97, 96, 94, 92, 105, 110]
        prefix_bars = [_bar(i, c) for i, c in enumerate(prefix_closes)]

        # A wildly different continuation, designed to look nothing like a natural drift —
        # if the strategy could see it, a crossover or an RSI extreme would fire early.
        extended_bars = prefix_bars + [
            _bar(len(prefix_bars) + offset, close)
            for offset, close in enumerate([1000, 5, 900, 2, 1500])
        ]

        prefix_signals = _run(compile_strategy(strategy_type, parameters), prefix_bars)
        extended_signals = _run(compile_strategy(strategy_type, parameters), extended_bars)

        assert extended_signals[: len(prefix_bars)] == prefix_signals
