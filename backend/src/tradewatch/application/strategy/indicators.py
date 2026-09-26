"""The indicator library: SMA, EMA, RSI, ATR, rolling volatility.

Two forms of every indicator, because two different drivers need them (ADR-0007):

* **Batch** (`sma`, `ema`, ...) takes a full series and returns a same-length list, `None`
  wherever the window has not yet filled. The Backtest Runner never actually calls these —
  it drives the strategy core one bar at a time, same as the live path would — but they are
  the easiest thing to hand-verify against a spreadsheet, so the incremental state below is
  tested *against* the batch function rather than against its own hand-derived numbers alone.
* **Incremental** (`SmaState`, `EmaState`, ...) holds just enough state to accept one new
  value and produce the next output, with no access to history beyond what it already folded
  in. This is what a strategy's `evaluate(state, bar)` call actually uses, and it is what
  Phase 7's live driver needs: a deployment that restarts must resume from serialized state,
  not by re-fetching a year of bars to warm a batch function back up.

Every state type here is an immutable, frozen dataclass. `update()` returns a new state
rather than mutating `self` — the same reason `domain.entities.DomainModel` is frozen
(`ARCHITECTURE.md` and ADR-0007): a strategy's history has to be replayable and comparable,
and a mutable accumulator can't be either.

Decimal throughout, not float, matching every other price in the system (ADR-0009). Two
determinism-sensitive backtests computed on two machines must produce bit-identical numbers,
and `Decimal` is exact where a float's binary rounding is only "close enough."
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from statistics import stdev

__all__ = [
    "AtrState",
    "EmaState",
    "RsiState",
    "SmaState",
    "VolatilityState",
    "atr",
    "ema",
    "rolling_volatility",
    "rsi",
    "sma",
]

_ZERO = Decimal(0)
_HUNDRED = Decimal(100)


# ---------------------------------------------------------------------------
# Simple moving average
# ---------------------------------------------------------------------------


def sma(values: Sequence[Decimal], period: int) -> list[Decimal | None]:
    """The mean of the trailing `period` values, `None` until there are enough of them."""
    if period < 1:
        raise ValueError(f"period must be at least 1, got {period}")
    out: list[Decimal | None] = []
    window: list[Decimal] = []
    total = _ZERO
    for value in values:
        window.append(value)
        total += value
        if len(window) > period:
            total -= window.pop(0)
        out.append(total / period if len(window) == period else None)
    return out


@dataclass(frozen=True, slots=True)
class SmaState:
    """Incremental SMA: a bounded trailing window.

    `values` is a tuple rather than a `deque` so the state stays hashable and comparable,
    which is what makes the determinism test ("same inputs, same state, byte for byte")
    meaningful rather than trivially true of a mutable buffer.
    """

    period: int
    values: tuple[Decimal, ...] = ()

    def update(self, value: Decimal) -> tuple[SmaState, Decimal | None]:
        window = (*self.values, value)[-self.period :]
        state = SmaState(self.period, window)
        current = sum(window, _ZERO) / self.period if len(window) == self.period else None
        return state, current


# ---------------------------------------------------------------------------
# Exponential moving average
# ---------------------------------------------------------------------------


def ema(values: Sequence[Decimal], period: int) -> list[Decimal | None]:
    """EMA seeded with the SMA of the first `period` values (the standard convention).

    An EMA needs a starting point, and starting from the very first value biases every
    later one toward it forever with a small period. Seeding with the simple average of the
    warm-up window is what every reference implementation does, and it is what makes this
    function's output something a spreadsheet can reproduce.
    """
    if period < 1:
        raise ValueError(f"period must be at least 1, got {period}")
    alpha = Decimal(2) / Decimal(period + 1)
    out: list[Decimal | None] = []
    state = EmaState(period, alpha)
    for value in values:
        state, current = state.update(value)
        out.append(current)
    return out


@dataclass(frozen=True, slots=True)
class EmaState:
    period: int
    alpha: Decimal
    seed_sum: Decimal = _ZERO
    seed_count: int = 0
    value: Decimal | None = None

    def update(self, value: Decimal) -> tuple[EmaState, Decimal | None]:
        if self.value is not None:
            new_value = (value - self.value) * self.alpha + self.value
            return EmaState(self.period, self.alpha, self.seed_sum, self.seed_count, new_value), (
                new_value
            )

        seed_sum = self.seed_sum + value
        seed_count = self.seed_count + 1
        if seed_count < self.period:
            return EmaState(self.period, self.alpha, seed_sum, seed_count, None), None

        seeded = seed_sum / self.period
        return EmaState(self.period, self.alpha, seed_sum, seed_count, seeded), seeded


# ---------------------------------------------------------------------------
# Relative Strength Index (Wilder's smoothing)
# ---------------------------------------------------------------------------


def rsi(values: Sequence[Decimal], period: int) -> list[Decimal | None]:
    """RSI over `period`, using Wilder's smoothing — the original and still the standard.

    A simple-moving-average RSI exists and is easier to explain, but "RSI" without
    qualification means Wilder's version in every charting package a user has seen, and
    disagreeing with that silently would make every number look wrong even when it isn't.
    """
    if period < 1:
        raise ValueError(f"period must be at least 1, got {period}")
    out: list[Decimal | None] = []
    state = RsiState(period)
    previous = values[0] if values else None
    out.append(None)
    for value in values[1:]:
        state, current = state.update(previous, value)  # type: ignore[arg-type]
        out.append(current)
        previous = value
    return out


@dataclass(frozen=True, slots=True)
class RsiState:
    """Wilder's average gain/loss, updated one price change at a time.

    `previous_close` lives in the *caller's* hands, not here — deliberately. `update` takes
    both the previous and current price rather than remembering the last one internally,
    because a strategy already holds the bar it just saw, and asking it to hand back both
    prices costs nothing while keeping this state's shape identical to every other
    indicator's (`period`, running accumulators, a count).
    """

    period: int
    avg_gain: Decimal | None = None
    avg_loss: Decimal | None = None
    seed_gain_sum: Decimal = _ZERO
    seed_loss_sum: Decimal = _ZERO
    seed_count: int = 0

    def update(self, previous_close: Decimal, close: Decimal) -> tuple[RsiState, Decimal | None]:
        change = close - previous_close
        gain = change if change > 0 else _ZERO
        loss = -change if change < 0 else _ZERO

        if self.avg_gain is not None and self.avg_loss is not None:
            new_avg_gain = (self.avg_gain * (self.period - 1) + gain) / self.period
            new_avg_loss = (self.avg_loss * (self.period - 1) + loss) / self.period
            state = RsiState(self.period, new_avg_gain, new_avg_loss)
            return state, _rsi_value(new_avg_gain, new_avg_loss)

        seed_gain_sum = self.seed_gain_sum + gain
        seed_loss_sum = self.seed_loss_sum + loss
        seed_count = self.seed_count + 1
        if seed_count < self.period:
            return (
                RsiState(self.period, None, None, seed_gain_sum, seed_loss_sum, seed_count),
                None,
            )

        seeded_gain = seed_gain_sum / self.period
        seeded_loss = seed_loss_sum / self.period
        state = RsiState(self.period, seeded_gain, seeded_loss)
        return state, _rsi_value(seeded_gain, seeded_loss)


def _rsi_value(avg_gain: Decimal, avg_loss: Decimal) -> Decimal:
    """0-100. A market with no losses in the window is maximally overbought, not undefined."""
    if avg_loss == 0:
        return _HUNDRED
    rs = avg_gain / avg_loss
    return _HUNDRED - (_HUNDRED / (1 + rs))


# ---------------------------------------------------------------------------
# Average True Range (Wilder's smoothing)
# ---------------------------------------------------------------------------


def atr(
    highs: Sequence[Decimal], lows: Sequence[Decimal], closes: Sequence[Decimal], period: int
) -> list[Decimal | None]:
    """Wilder's ATR: the smoothed average of the true range over `period` bars."""
    if period < 1:
        raise ValueError(f"period must be at least 1, got {period}")
    if not (len(highs) == len(lows) == len(closes)):
        raise ValueError("highs, lows, and closes must be the same length")
    out: list[Decimal | None] = []
    state = AtrState(period)
    previous_close: Decimal | None = None
    for high, low, close in zip(highs, lows, closes, strict=True):
        state, current = state.update(high, low, previous_close)
        out.append(current)
        previous_close = close
    return out


@dataclass(frozen=True, slots=True)
class AtrState:
    """True range needs the *previous* bar's close, which the caller supplies each call —
    see `RsiState` for why this state does not keep its own copy of it.
    """

    period: int
    avg_true_range: Decimal | None = None
    seed_sum: Decimal = _ZERO
    seed_count: int = 0

    def update(
        self, high: Decimal, low: Decimal, previous_close: Decimal | None
    ) -> tuple[AtrState, Decimal | None]:
        true_range = _true_range(high, low, previous_close)

        if self.avg_true_range is not None:
            new_average = (self.avg_true_range * (self.period - 1) + true_range) / self.period
            return AtrState(self.period, new_average), new_average

        seed_sum = self.seed_sum + true_range
        seed_count = self.seed_count + 1
        if seed_count < self.period:
            return AtrState(self.period, None, seed_sum, seed_count), None

        seeded = seed_sum / self.period
        return AtrState(self.period, seeded), seeded


def _true_range(high: Decimal, low: Decimal, previous_close: Decimal | None) -> Decimal:
    """`max` of three spans, one of which needs a previous close that the first bar lacks.

    The first bar's true range is just its own high-low: there is no gap to measure against
    a close that does not exist, so the two gap terms are skipped rather than guessed at.
    """
    span = high - low
    if previous_close is None:
        return span
    return max(span, abs(high - previous_close), abs(low - previous_close))


# ---------------------------------------------------------------------------
# Rolling volatility (annualization is the caller's job — see application/backtest/metrics.py)
# ---------------------------------------------------------------------------


def rolling_volatility(values: Sequence[Decimal], window: int) -> list[Decimal | None]:
    """Sample standard deviation of simple returns over a trailing `window`.

    Needs `window + 1` prices to produce `window` returns, so the first `window` outputs
    are `None` — one more than `sma`'s warm-up, for the same reason a return needs two
    prices where a level needs one.
    """
    if window < 2:
        raise ValueError(f"window must be at least 2, got {window}")
    out: list[Decimal | None] = []
    state = VolatilityState(window)
    previous = values[0] if values else None
    out.append(None)
    for value in values[1:]:
        state, current = state.update(previous, value)  # type: ignore[arg-type]
        out.append(current)
        previous = value
    return out


@dataclass(frozen=True, slots=True)
class VolatilityState:
    """A bounded window of simple returns. See `SmaState` for why a tuple, not a deque."""

    window: int
    returns: tuple[Decimal, ...] = ()

    def update(
        self, previous_close: Decimal, close: Decimal
    ) -> tuple[VolatilityState, Decimal | None]:
        simple_return = (close - previous_close) / previous_close
        returns = (*self.returns, simple_return)[-self.window :]
        state = VolatilityState(self.window, returns)
        if len(returns) < self.window:
            return state, None
        # `statistics.stdev` wants floats; a return is a ratio, not a stored money amount,
        # so this is the one place in the indicator library that leaves Decimal on purpose.
        current = Decimal(str(stdev(float(item) for item in returns)))
        return state, current
