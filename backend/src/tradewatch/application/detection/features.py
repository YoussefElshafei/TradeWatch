"""The feature vocabulary (ADR-0008): one implementation, used by the baseline, the offline
training pipeline, and the live detection service alike.

Sharing this module is the whole point. A detector trained on features computed one way and
served on features computed a slightly different way is a detector that was never really
evaluated on what it runs against in production — a training/serving skew bug that is easy
to introduce by accident and very hard to notice, because both sides still "work."

Five features, each picked for a distinct way a symbol's activity can look unlike its own
recent behaviour — no feature is here without that reason:

* **return** — the plainest signal there is. A single bar's simple return is often exactly
  what "unusual" means to someone looking at a chart: the price moved further, one bar to
  the next, than it normally does.
* **volatility** — realized volatility over the rolling window (reusing
  :func:`tradewatch.application.strategy.indicators.rolling_volatility` directly, rather than
  a second implementation of the same stdev-of-returns calculation). A symbol that turns
  choppy is unusual even on a bar where no single return looks extreme.
* **volume_z** — this bar's volume, z-scored against its own trailing window. Raw volume
  spans orders of magnitude across symbols and across the trading day (the open is not the
  middle of the afternoon); a z-score is what makes "this is unusually heavy for *this*
  symbol right now" a comparable number instead of a raw count nothing can be judged against.
* **trade_count_z** — the same idea, for trade count. Volume and trade count usually move
  together, but not always — one enormous print looks different from many small ones, and
  this feature is what lets a detector tell them apart.
* **spread** — the quoted bid-ask spread nearest the bar's close. A spread that widens
  sharply is a classic sign of thin, unusual liquidity that a price-only feature cannot see.
  It is the one feature that needs quote data the caller must supply; bars alone do not carry
  it, so it is simply absent from the feature vector on a bar with no recent quote — an
  honestly incomplete measurement, not a fabricated one.

**Everything here is pure and takes an explicit, small state.** No I/O, no wall-clock reads,
matching the strategy core's `evaluate(state, bar)` shape exactly, for the same reason: the
live service must be able to rebuild a symbol's feature state after a restart by replaying
stored bars, not by inventing a second, serialization-specific representation of "the last
N bars."
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from decimal import Decimal
from typing import Final

from tradewatch.application.strategy.indicators import VolatilityState
from tradewatch.domain.entities import Bar

__all__ = [
    "FEATURE_NAMES",
    "FeatureState",
    "RollingZScoreState",
    "initial_feature_state",
]

#: The feature vocabulary, in the order `AnomalySettings.anomaly_features` defaults to.
#: A stored detection's `features` dict uses exactly these names — the vocabulary a stored
#: detection and the code that produced it always agree on (ADR-0008: every detection must
#: be explainable from its own `features` payload).
FEATURE_NAMES: Final[tuple[str, ...]] = (
    "return",
    "volatility",
    "volume_z",
    "trade_count_z",
    "spread",
)


@dataclass(frozen=True, slots=True)
class RollingZScoreState:
    """A bounded trailing window of raw values, scored against its own mean and stdev.

    Shared by `volume_z` and `trade_count_z`: two features that are the same shape — "is
    this bar's raw count unusual relative to its own recent history" — with only the input
    series differing. Mirrors `VolatilityState`'s convention exactly: the new value joins the
    window *before* the statistics are computed, and nothing is produced until the window is
    full, so a z-score is never computed from a window smaller than the one that is about to
    replace it.
    """

    window: int
    values: tuple[Decimal, ...] = ()

    def update(self, value: Decimal) -> tuple[RollingZScoreState, Decimal | None]:
        values = (*self.values, value)[-self.window :]
        state = RollingZScoreState(self.window, values)
        if len(values) < self.window:
            return state, None
        floats = [float(item) for item in values]
        mean = statistics.mean(floats)
        stdev = statistics.stdev(floats)
        if stdev == 0:
            # Every value in the window was identical, including this one. There is no
            # variation to be unusual relative to, so this bar is not unusual by definition
            # — not undefined, and not a division by zero.
            return state, Decimal(0)
        return state, Decimal(str((float(value) - mean) / stdev))


@dataclass(frozen=True, slots=True)
class FeatureState:
    """Everything one symbol needs remembered between bars to compute its next feature
    vector. Rebuildable after a restart by replaying stored bars through `update`, the same
    way `application.trading.driver.SymbolRuntimeState` is.
    """

    window: int
    volatility: VolatilityState
    volume: RollingZScoreState
    trade_count: RollingZScoreState
    previous_close: Decimal | None = None

    def update(
        self, bar: Bar, *, spread: Decimal | None = None
    ) -> tuple[FeatureState, dict[str, Decimal]]:
        """Fold one more bar in. Returns the new state and whatever features are computable.

        The returned dict is not always all five names: `return` and `volatility` are absent
        from the very first bar (no previous close yet), `volatility`/`volume_z`/
        `trade_count_z` stay absent until their window fills, and `spread` is absent whenever
        the caller has no recent quote to supply. A partial vector is the honest one — see
        the module docstring on why `spread` is optional input rather than a fabricated
        value.
        """
        features: dict[str, Decimal] = {}
        volatility_state = self.volatility

        if self.previous_close is not None:
            features["return"] = (bar.close - self.previous_close) / self.previous_close
            volatility_state, volatility_value = self.volatility.update(
                self.previous_close, bar.close
            )
            if volatility_value is not None:
                features["volatility"] = volatility_value

        volume_state, volume_z = self.volume.update(bar.volume)
        if volume_z is not None:
            features["volume_z"] = volume_z

        trade_count_state, trade_count_z = self.trade_count.update(Decimal(bar.trade_count))
        if trade_count_z is not None:
            features["trade_count_z"] = trade_count_z

        if spread is not None:
            features["spread"] = spread

        new_state = FeatureState(
            window=self.window,
            volatility=volatility_state,
            volume=volume_state,
            trade_count=trade_count_state,
            previous_close=bar.close,
        )
        return new_state, features


def initial_feature_state(window: int) -> FeatureState:
    """A fresh, empty state for one symbol. `window` is `TW_ANOMALY_WINDOW_BARS`."""
    return FeatureState(
        window=window,
        volatility=VolatilityState(window),
        volume=RollingZScoreState(window),
        trade_count=RollingZScoreState(window),
    )
