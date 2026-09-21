"""Data quality checks: gaps, sessions, and the invariants the database already enforces.

The point of these checks is to answer one question before Phase 6 asks it — *is this
history good enough to compute a number from?* A gap inside a trading session is the one
that matters: a backtest reads straight through it and reports a return that never happened.

A gap outside a session is the weekend. A checker that reports every weekend as an outage is
a checker people switch off, which is why the session filter is tested harder than the gap
detection is.
"""

from __future__ import annotations

import datetime as dt

import pytest

from tests.bar_support import FakeBarStore
from tradewatch.application.quality import SessionCalendar, check_symbol, check_watchlist
from tradewatch.domain.enums import BarInterval

pytestmark = pytest.mark.unit

ONE_MINUTE = BarInterval.ONE_MINUTE

#: A Monday. US daylight saving starts on 8 March 2026, so 2 March is still EST (UTC-5)
#: and the 09:30 open is 14:30 UTC. Getting this wrong is exactly the bug the timezone test
#: below exists to catch, so the constant is derived from the rule rather than guessed.
MONDAY_OPEN = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)
SATURDAY = dt.datetime(2026, 3, 7, 14, 30, tzinfo=dt.UTC)


class TestSessionCalendar:
    def test_a_weekday_inside_hours_is_open(self) -> None:
        assert SessionCalendar().is_open(MONDAY_OPEN)

    def test_before_the_open_is_closed(self) -> None:
        assert not SessionCalendar().is_open(MONDAY_OPEN - dt.timedelta(minutes=1))

    def test_the_close_is_exclusive(self) -> None:
        """16:00 is the end of the session, not a minute of it."""
        close = MONDAY_OPEN + dt.timedelta(hours=6, minutes=30)

        assert not SessionCalendar().is_open(close)
        assert SessionCalendar().is_open(close - dt.timedelta(minutes=1))

    def test_the_weekend_is_closed(self) -> None:
        assert not SessionCalendar().is_open(SATURDAY)
        assert not SessionCalendar().is_open(SATURDAY + dt.timedelta(days=1))

    def test_it_reads_in_the_venues_timezone_not_utc(self) -> None:
        """14:00 UTC is 09:00 in New York — pre-market, and inside a naive UTC session.

        Chosen because it separates the two readings. 14:30 UTC would not: it is inside
        09:30-16:00 whichever timezone the clock is read in, so a calendar that ignored the
        venue entirely would still pass.
        """
        pre_market = dt.datetime(2026, 3, 2, 14, 0, tzinfo=dt.UTC)

        assert not SessionCalendar().is_open(pre_market), "09:00 in New York is not open"
        assert SessionCalendar(timezone="UTC").is_open(pre_market), "but 14:00 UTC would be"

    def test_an_overnight_range_overlaps_the_next_session(self) -> None:
        """Monday evening into Tuesday morning, reaching an hour past Tuesday's open."""
        overnight = (
            MONDAY_OPEN + dt.timedelta(hours=8),
            MONDAY_OPEN + dt.timedelta(hours=25),
        )

        assert SessionCalendar().overlaps_session(*overnight)

    def test_a_range_ending_exactly_at_the_open_contains_no_session(self) -> None:
        """Half-open, like every other range in this system. The open minute is not in it."""
        overnight = (
            MONDAY_OPEN + dt.timedelta(hours=8),
            MONDAY_OPEN + dt.timedelta(hours=24),
        )

        assert not SessionCalendar().overlaps_session(*overnight)

    def test_a_weekend_range_overlaps_nothing(self) -> None:
        weekend = (SATURDAY, SATURDAY + dt.timedelta(days=1, hours=12))

        assert not SessionCalendar().overlaps_session(*weekend)

    def test_a_short_gap_inside_the_session_is_detected(self) -> None:
        inside = (
            MONDAY_OPEN + dt.timedelta(hours=1),
            MONDAY_OPEN + dt.timedelta(hours=1, minutes=5),
        )

        assert SessionCalendar().overlaps_session(*inside)


class TestGapReporting:
    async def build(self, gaps: list[tuple[dt.datetime, dt.datetime]]) -> FakeBarStore:
        store = FakeBarStore()
        await store.resolve_instrument("AAPL")
        store.gaps = gaps
        return store

    async def test_a_gap_during_the_session_is_reported(self) -> None:
        store = await self.build(
            [(MONDAY_OPEN + dt.timedelta(hours=1), MONDAY_OPEN + dt.timedelta(hours=2))]
        )

        report = await check_symbol(
            store, "AAPL", ONE_MINUTE, MONDAY_OPEN, MONDAY_OPEN + dt.timedelta(days=1)
        )

        assert len(report.session_gaps) == 1
        assert not report.healthy

    async def test_an_overnight_gap_is_not_a_fault(self) -> None:
        """The market closed. Reporting it would bury the gaps that matter."""
        close = MONDAY_OPEN + dt.timedelta(hours=6, minutes=30)
        store = await self.build([(close, close + dt.timedelta(hours=17))])

        report = await check_symbol(
            store, "AAPL", ONE_MINUTE, MONDAY_OPEN, MONDAY_OPEN + dt.timedelta(days=1)
        )

        assert len(report.gaps) == 1, "the hole is still recorded"
        assert report.session_gaps == [], "but it is not a fault"
        assert report.healthy

    async def test_a_weekend_gap_is_not_a_fault(self) -> None:
        friday_close = dt.datetime(2026, 3, 6, 21, 0, tzinfo=dt.UTC)
        store = await self.build([(friday_close, friday_close + dt.timedelta(days=2, hours=16))])

        report = await check_symbol(
            store, "AAPL", ONE_MINUTE, friday_close, friday_close + dt.timedelta(days=4)
        )

        assert report.session_gaps == []

    async def test_a_gap_carries_its_duration(self) -> None:
        store = await self.build(
            [(MONDAY_OPEN + dt.timedelta(hours=1), MONDAY_OPEN + dt.timedelta(hours=1, minutes=30))]
        )

        report = await check_symbol(
            store, "AAPL", ONE_MINUTE, MONDAY_OPEN, MONDAY_OPEN + dt.timedelta(days=1)
        )

        assert report.gaps[0].duration == dt.timedelta(minutes=30)
        assert report.gaps[0].as_dict()["minutes"] == 30.0

    async def test_a_clean_series_is_healthy(self) -> None:
        store = await self.build([])

        report = await check_symbol(
            store, "AAPL", ONE_MINUTE, MONDAY_OPEN, MONDAY_OPEN + dt.timedelta(days=1)
        )

        assert report.healthy
        assert report.gaps == []


class TestWatchlist:
    async def test_every_symbol_gets_a_report(self) -> None:
        store = FakeBarStore()

        reports = await check_watchlist(
            store, ["AAPL", "MSFT"], ONE_MINUTE, MONDAY_OPEN, MONDAY_OPEN + dt.timedelta(days=1)
        )

        assert [report.symbol for report in reports] == ["AAPL", "MSFT"]

    async def test_the_bar_count_comes_from_the_store(self) -> None:
        store = FakeBarStore()
        instrument_id = await store.resolve_instrument("AAPL")
        assert instrument_id is not None

        reports = await check_watchlist(
            store, ["AAPL"], ONE_MINUTE, MONDAY_OPEN, MONDAY_OPEN + dt.timedelta(days=1)
        )

        assert reports[0].bars == 0
