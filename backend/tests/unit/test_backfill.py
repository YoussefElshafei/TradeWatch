"""The historical backfill job.

Two properties, and everything here serves one of them:

**Idempotent.** Running the same range twice leaves the table identical. That is what makes
"just re-run it over a generous window" the correct recovery from a provider outage rather
than a way to double-count volume.

**Resumable.** A job that dies partway through restarts without re-fetching what it already
stored, and without a job table to disagree with the rows.

The provider is a fake. `pytest -m provider` is where the live Alpaca endpoint is exercised,
and it needs credentials; nothing in the deterministic suite touches the network.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from tests.bar_support import MINUTE, FakeBarStore, FakeHistoricalProvider
from tradewatch.application.backfill import BackfillService, default_chunk_for
from tradewatch.application.ports.market_data import ProviderCapabilities
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.providers.errors import ProviderUnavailableError

pytestmark = pytest.mark.unit

ONE_MINUTE = BarInterval.ONE_MINUTE
DAY = dt.timedelta(days=1)


def build(
    **provider_options: object,
) -> tuple[BackfillService, FakeHistoricalProvider, FakeBarStore]:
    provider = FakeHistoricalProvider(**provider_options)  # type: ignore[arg-type]
    store = FakeBarStore()
    return BackfillService(provider, store), provider, store


class TestFetching:
    async def test_it_stores_what_the_provider_returns(self) -> None:
        service, _, store = build()

        result = await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=10))

        assert result.fetched == 10
        assert result.written == 10
        assert len(store.stored_for("AAPL")) == 10

    async def test_the_values_are_the_providers(self) -> None:
        """The fake encodes the minute offset in the price, so a misplaced bar is visible."""
        service, _, store = build()

        await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=3))

        stored = store.stored_for("AAPL")
        assert [row.close for row in stored] == [Decimal(100), Decimal(101), Decimal(102)]
        assert [row.bar_start for row in stored] == [
            MINUTE,
            MINUTE + dt.timedelta(minutes=1),
            MINUTE + dt.timedelta(minutes=2),
        ]

    async def test_a_backfilled_bar_is_final(self) -> None:
        """Only the aggregator writes a bar still in progress."""
        service, _, store = build()

        await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=2))

        assert all(row.is_final for row in store.stored_for("AAPL"))

    async def test_every_symbol_is_fetched(self) -> None:
        service, provider, store = build()

        result = await service.run(
            ["AAPL", "MSFT"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=5)
        )

        assert {call[0] for call in provider.calls} == {"AAPL", "MSFT"}
        assert len(result.symbols) == 2
        assert len(store.stored_for("MSFT")) == 5

    async def test_a_long_range_is_walked_in_windows(self) -> None:
        """A month is not one request, and a crash must not cost the whole job."""
        service, provider, _ = build(minutes=10_000)

        await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + DAY * 3)

        assert len(provider.calls) == 3, "one window per day at 1m"
        assert default_chunk_for(ONE_MINUTE) == DAY

    async def test_windows_tile_without_overlapping(self) -> None:
        """Half-open ranges. An overlap would re-fetch a bar on every boundary."""
        service, provider, _ = build()

        await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + DAY * 3)

        for previous, following in zip(provider.calls, provider.calls[1:], strict=False):
            assert previous[2] == following[1]


class TestIdempotency:
    async def test_running_twice_stores_the_same_rows(self) -> None:
        service, _, store = build()
        window = (MINUTE, MINUTE + dt.timedelta(minutes=10))

        await service.run(["AAPL"], ONE_MINUTE, *window)
        before = {row.bar_start: row.close for row in store.stored_for("AAPL")}

        await service.run(["AAPL"], ONE_MINUTE, *window, resume=False)
        after = {row.bar_start: row.close for row in store.stored_for("AAPL")}

        assert after == before
        assert len(store.stored_for("AAPL")) == 10

    async def test_a_re_fetch_rewrites_the_same_rows_rather_than_adding_any(self) -> None:
        """`written` counts rows the database accepted, which on a re-run is all of them.

        Bars are upserted, not inserted-or-ignored, so an identical row is rewritten with
        identical values. The number to check is the size of the table, not the size of the
        write — which is exactly why this job reports no "duplicates" figure: for bars it
        would always read zero and imply fresh history had been fetched.
        """
        service, _, store = build()
        window = (MINUTE, MINUTE + dt.timedelta(minutes=10))
        await service.run(["AAPL"], ONE_MINUTE, *window)

        second = await service.run(["AAPL"], ONE_MINUTE, *window, resume=False)

        assert second.fetched == 10
        assert len(store.stored_for("AAPL")) == 10, "the table did not grow"

    async def test_overlapping_windows_do_not_duplicate(self) -> None:
        """The recovery pattern: fetch a generous window around a suspected gap."""
        service, _, store = build()

        await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=10))
        await service.run(
            ["AAPL"],
            ONE_MINUTE,
            MINUTE + dt.timedelta(minutes=5),
            MINUTE + dt.timedelta(minutes=15),
            resume=False,
        )

        assert len(store.stored_for("AAPL")) == 15


class TestResuming:
    async def test_it_continues_after_the_last_stored_bar(self) -> None:
        service, provider, store = build()
        await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=10))
        provider.calls.clear()

        result = await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=20))

        assert provider.calls[0][1] == MINUTE + dt.timedelta(minutes=10), "resumed, not restarted"
        assert result.fetched == 10
        assert result.written == 10
        assert len(store.stored_for("AAPL")) == 20

    async def test_a_job_that_died_partway_restarts_where_it_stopped(self) -> None:
        """The scenario the requirement names: fetch two days, fall over, run it again."""
        service, _, store = build(fail_after=2)

        with pytest.raises(ProviderUnavailableError):
            await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + DAY * 4)

        stored_after_crash = len(store.stored_for("AAPL"))
        assert stored_after_crash > 0, "the windows that completed were written"

        # A new process, the same database, a provider that is working again.
        recovered = BackfillService(FakeHistoricalProvider(), store)
        result = await recovered.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + DAY * 4)

        assert result.symbols[0].effective_start > MINUTE, "it did not start from the beginning"
        assert len(store.stored_for("AAPL")) == 4 * 24 * 60, "and the range ended up complete"

    async def test_recent_bars_do_not_make_it_skip_the_month_before_them(self) -> None:
        """The bug this rule exists for, found by running the job for real.

        The database already held a few hours of bars near *now* — the aggregator's, and an
        earlier backfill's. Asking for the previous month then resumed from today, fetched
        152 minutes, and reported success over an empty month.
        """
        service, provider, _ = build(minutes=100_000)
        month_ago = MINUTE - DAY * 30

        # A patch of recent bars, nowhere near the start of the range we are about to ask for.
        await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=10))
        provider.calls.clear()

        result = await service.run(
            ["AAPL"], ONE_MINUTE, month_ago, MINUTE + dt.timedelta(minutes=10)
        )

        assert result.symbols[0].effective_start == month_ago, "the month was fetched, not skipped"
        assert provider.calls, "and the provider was actually asked"

    async def test_a_prefix_of_stored_bars_is_still_resumed_from(self) -> None:
        """The cautious rule must not disable resuming in the case it was built for."""
        service, provider, _ = build()
        await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=10))
        provider.calls.clear()

        result = await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=20))

        assert result.symbols[0].effective_start == MINUTE + dt.timedelta(minutes=10)

    async def test_a_fully_covered_range_fetches_nothing(self) -> None:
        service, provider, _ = build()
        window = (MINUTE, MINUTE + dt.timedelta(minutes=10))
        await service.run(["AAPL"], ONE_MINUTE, *window)
        provider.calls.clear()

        result = await service.run(["AAPL"], ONE_MINUTE, *window)

        assert provider.calls == []
        assert result.symbols[0].skipped_existing is True
        assert result.fetched == 0

    async def test_resume_off_re_fetches_everything(self) -> None:
        """For when the provider has *corrected* history: the upsert repairs it."""
        service, provider, _ = build()
        window = (MINUTE, MINUTE + dt.timedelta(minutes=10))
        await service.run(["AAPL"], ONE_MINUTE, *window)
        provider.calls.clear()

        await service.run(["AAPL"], ONE_MINUTE, *window, resume=False)

        assert provider.calls[0][1] == MINUTE

    async def test_the_resume_point_is_scoped_to_the_requested_range(self) -> None:
        """Bars outside the window must not make it think the window is done."""
        service, provider, _ = build()
        await service.run(
            ["AAPL"],
            ONE_MINUTE,
            MINUTE + dt.timedelta(minutes=100),
            MINUTE + dt.timedelta(minutes=110),
        )
        provider.calls.clear()

        await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + dt.timedelta(minutes=10))

        assert provider.calls, "an earlier window is still fetched"
        assert provider.calls[0][1] == MINUTE


class TestRefusals:
    async def test_a_reversed_range_is_refused(self) -> None:
        service, _, _ = build()

        with pytest.raises(ValueError, match="not after"):
            await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE - DAY)

    async def test_an_empty_range_is_refused(self) -> None:
        service, _, _ = build()

        with pytest.raises(ValueError, match="not after"):
            await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE)

    async def test_a_provider_without_history_is_refused(self) -> None:
        service, _, _ = build(
            capabilities=ProviderCapabilities(historical_bars=False, intervals=frozenset())
        )

        with pytest.raises(ValueError, match="historical bars"):
            await service.run(["AAPL"], ONE_MINUTE, MINUTE, MINUTE + DAY)

    async def test_an_interval_the_provider_does_not_serve_is_refused(self) -> None:
        """At the top of the job, not after two days of fetching."""
        service, _, _ = build()

        with pytest.raises(ValueError, match="1d"):
            await service.run(["AAPL"], BarInterval.ONE_DAY, MINUTE, MINUTE + DAY * 3)

    async def test_no_symbols_does_nothing(self) -> None:
        service, provider, _ = build()

        result = await service.run([], ONE_MINUTE, MINUTE, MINUTE + DAY)

        assert result.symbols == []
        assert provider.calls == []
