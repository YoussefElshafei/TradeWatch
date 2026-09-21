"""A deterministic, continuous price path.

The synthetic provider needs two properties that pull in opposite directions.

**Continuity.** Consecutive bars must connect — one minute's close is the next minute's
open — or a chart of them is white noise, and a strategy backtested on it is measuring the
random number generator rather than anything about markets.

**Addressability.** The level at any bar must be computable from that bar's own timestamp,
in constant time, without replaying the path from an origin. That is what makes an
overlapping backfill return *identical* bars for the overlap: ADR-0012's deterministic event
ids make a re-fetch deduplicate, but only if the re-fetched data is actually the same.

A cumulative random walk gives continuity and fails addressability — reaching bar 100,000
means generating 100,000 steps. Independent per-bar randomness gives addressability and
fails continuity, which is what the first implementation did and why its charts looked like
static.

**Value noise** gives both. Pseudo-random values are anchored at coarse intervals and
interpolated smoothly between them, so the path is continuous; and every value is a hash of
its own coordinates, so any point is one constant-time computation. Summing a few octaves —
a slow trend, a medium swing, a fast wobble — is the standard fractal-noise construction and
produces something that reads as a price series without pretending to model one.

This is not a claim about market microstructure. It is the cheapest function that is
continuous, reproducible, and addressable, which is what the pipeline needs to be
demonstrable.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import Final

__all__ = ["bar_level", "unit_noise"]

#: Octaves as (amplitude, period in bars). The long period carries the trend, the short one
#: the wobble. Amplitudes fall off so the trend dominates and the wobble decorates.
_OCTAVES: Final[tuple[tuple[float, int], ...]] = ((1.0, 480), (0.45, 90), (0.18, 20))

#: Total swing of the summed octaves, as a fraction of the starting price. 0.18 gives a
#: series that moves visibly over a session without wandering somewhere implausible.
_AMPLITUDE: Final = 0.18


def unit_noise(*parts: object) -> float:
    """A deterministic value in ``[0, 1)`` derived from its arguments.

    BLAKE2b rather than Python's `hash()`, which is randomised per process by default and
    would make every restart produce different "deterministic" data.
    """
    key = "|".join(str(part) for part in parts).encode("utf-8")
    digest = hashlib.blake2b(key, digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64


def _signed_noise(*parts: object) -> float:
    """Deterministic noise in ``[-1, 1)``."""
    return unit_noise(*parts) * 2.0 - 1.0


def _smoothstep(t: float) -> float:
    """Ease between anchors so the path has no visible corners at octave boundaries."""
    return t * t * (3.0 - 2.0 * t)


def _octave(key: str, index: int, period: int) -> float:
    """One octave of value noise: interpolate between anchors ``period`` bars apart."""
    anchor = index // period
    fraction = (index % period) / period
    start = _signed_noise(key, period, anchor)
    end = _signed_noise(key, period, anchor + 1)
    return start + (end - start) * _smoothstep(fraction)


def bar_level(key: str, index: int, *, base: Decimal, volatility: float = 1.0) -> Decimal:
    """The price level at bar ``index``, as an exact decimal.

    Continuous in ``index`` and constant-time for any value of it. Two callers asking for
    the same ``(key, index)`` always get the same answer, which is what lets overlapping
    backfills agree.

    Args:
        key: Distinguishes one series from another — seed, symbol and interval.
        index: Bar number, derived from the bar's start time so it is absolute rather
            than relative to whenever the process happened to start.
        base: The price the series is centred on.
        volatility: Scales the swing. A symbol-specific value keeps two symbols from
            moving identically.
    """
    combined = sum(amplitude * _octave(key, index, period) for amplitude, period in _OCTAVES)
    scale = 1.0 + _AMPLITUDE * volatility * combined / sum(a for a, _ in _OCTAVES)
    # Through str, never float arithmetic on a Decimal: ADR-0009.
    level = base * Decimal(str(round(scale, 8)))
    return max(level, Decimal("0.01"))
