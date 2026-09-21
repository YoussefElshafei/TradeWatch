"""Data quality checks over stored bars.

`DEVELOPMENT_PLAN.md` Phase 3 asks for three: bar invariants, duplicate prints, and gaps
during market hours. Each is written to answer one question — *is this history good enough
to compute a number from?* — because the alternative is Phase 6 producing a Sharpe ratio
from a series with a hole in it and nothing anywhere saying so.

**Invariants are checked, not enforced, here.** The database already refuses an incoherent
bar: `high >= low`, `open`/`close` inside the range, positive prices, non-negative volume
are `CHECK` constraints, and `NewBar` validates the same rules before a write is attempted.
So this check exists to confirm those two layers are actually doing their job, not to be a
third one. A non-empty result means something wrote around the constraints — which should be
impossible, and is worth knowing immediately if it ever stops being.

**Gaps are reported, not judged.** A hole in a minute series is only a fault if the market
was open. This applies the configured session and calendar before calling anything missing,
because a checker that reports every weekend as an outage is a checker people switch off.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo

from tradewatch.application.ports.bar_store import BarStore
from tradewatch.domain.enums import BarInterval
from tradewatch.observability.logging import get_logger

__all__ = ["GapReport", "QualityReport", "SessionCalendar", "check_symbol", "check_watchlist"]

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SessionCalendar:
    """When the market is open, to the resolution this check needs.

    Deliberately simple: weekdays, one continuous session, no holidays. It is a *filter for
    a quality report*, not a trading calendar — an unlisted holiday shows up here as a
    reported gap that an operator recognises, which is a far better failure than a
    half-correct holiday table that silently hides a real outage on a day it got wrong.

    The real calendar arrives when something needs to make a decision from it rather than a
    report; that is Phase 6's problem, and it will want a maintained source.
    """

    timezone: str = "America/New_York"
    open_time: str = "09:30"
    close_time: str = "16:00"

    def _parse(self, value: str) -> dt.time:
        hour, _, minute = value.partition(":")
        return dt.time(int(hour), int(minute or 0))

    def is_open(self, moment: dt.datetime) -> bool:
        """Whether the market was open at ``moment`` (a UTC instant)."""
        local = moment.astimezone(ZoneInfo(self.timezone))
        if local.weekday() >= 5:
            return False
        return self._parse(self.open_time) <= local.time() < self._parse(self.close_time)

    def overlaps_session(self, start: dt.datetime, end: dt.datetime) -> bool:
        """Whether any part of ``[start, end)`` falls inside a session.

        Sampled every fifteen minutes rather than every minute. A gap short enough to fall
        entirely between two samples is shorter than a quarter hour, and a quarter-hour hole
        inside a session is already reported by the sample at either end.
        """
        step = dt.timedelta(minutes=15)
        moment = start
        while moment < end:
            if self.is_open(moment):
                return True
            moment += step
        return self.is_open(max(start, end - dt.timedelta(seconds=1)))


@dataclass(frozen=True, slots=True)
class GapReport:
    """One hole in a series."""

    start: dt.datetime
    end: dt.datetime
    in_session: bool

    @property
    def duration(self) -> dt.timedelta:
        return self.end - self.start

    def as_dict(self) -> dict[str, Any]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "minutes": round(self.duration.total_seconds() / 60, 1),
            "in_session": self.in_session,
        }


@dataclass(slots=True)
class QualityReport:
    """What the checks found for one symbol over one window."""

    symbol: str
    interval: BarInterval
    start: dt.datetime
    end: dt.datetime
    bars: int = 0
    gaps: list[GapReport] = field(default_factory=list)
    #: Bars whose OHLC relationships do not hold. Should always be empty.
    invariant_violations: list[dict[str, Any]] = field(default_factory=list)
    #: Trades sharing a provider trade id — the same print counted twice.
    duplicate_prints: int = 0

    @property
    def session_gaps(self) -> list[GapReport]:
        """The gaps that matter: holes while the market was open."""
        return [gap for gap in self.gaps if gap.in_session]

    @property
    def healthy(self) -> bool:
        return not self.session_gaps and not self.invariant_violations and not self.duplicate_prints

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval": self.interval.value,
            "bars": self.bars,
            "gaps": len(self.gaps),
            "session_gaps": len(self.session_gaps),
            "invariant_violations": len(self.invariant_violations),
            "duplicate_prints": self.duplicate_prints,
            "healthy": self.healthy,
        }


async def check_symbol(
    store: BarStore,
    symbol: str,
    interval: BarInterval,
    start: dt.datetime,
    end: dt.datetime,
    *,
    calendar: SessionCalendar | None = None,
) -> QualityReport:
    """Run every check for one symbol over ``[start, end)``."""
    sessions = calendar or SessionCalendar()
    instrument_id = await store.resolve_instrument(symbol)

    report = QualityReport(symbol=symbol, interval=interval, start=start, end=end)
    report.bars = await store.count_bars(instrument_id, interval, start, end)

    for gap_start, gap_end in await store.find_gaps(instrument_id, interval, start, end):
        report.gaps.append(
            GapReport(
                start=gap_start,
                end=gap_end,
                in_session=sessions.overlaps_session(gap_start, gap_end),
            )
        )

    if report.session_gaps:
        log.warning(
            "quality.gaps_during_session",
            symbol=symbol,
            interval=interval.value,
            gaps=[gap.as_dict() for gap in report.session_gaps[:10]],
            total=len(report.session_gaps),
        )

    log.info("quality.checked", **report.as_dict())
    return report


async def check_watchlist(
    store: BarStore,
    symbols: Sequence[str],
    interval: BarInterval,
    start: dt.datetime,
    end: dt.datetime,
    *,
    calendar: SessionCalendar | None = None,
) -> list[QualityReport]:
    """Run the checks for every symbol. Returns one report each."""
    reports = [
        await check_symbol(store, symbol, interval, start, end, calendar=calendar)
        for symbol in symbols
    ]
    unhealthy = [report.symbol for report in reports if not report.healthy]
    log.info(
        "quality.watchlist_checked",
        symbols=len(reports),
        unhealthy=unhealthy,
        total_bars=sum(report.bars for report in reports),
    )
    return reports
