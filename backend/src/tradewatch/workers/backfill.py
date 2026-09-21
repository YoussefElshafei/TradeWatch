"""Historical backfill: ``uv run tradewatch-backfill``.

```
Provider (historical endpoint) → BackfillService → PostgreSQL → quality report
```

A one-shot job, not a service. It fetches bars for a date range, stores them idempotently,
rolls the coarser intervals up from them, and reports on what it produced.

```bash
uv run tradewatch-backfill --days 30
uv run tradewatch-backfill --symbols AAPL,MSFT --start 2026-08-01 --end 2026-09-01
uv run tradewatch-backfill --days 30 --no-resume     # re-fetch, to repair corrected history
```

**It writes to Kafka: nothing.** Backfilled bars go straight to the database. They are not
market *events* — nobody is waiting to react to a bar from three weeks ago — and publishing
them would make the aggregator's watermark jump backwards through history, sealing every
live bar behind it. History is loaded; events are streamed. Conflating the two is how a
backfill silently stops a live feed.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sys
from collections.abc import Sequence

from tradewatch.application.backfill import BackfillResult, BackfillService
from tradewatch.application.quality.checks import QualityReport, check_watchlist
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.database.bar_store import SqlBarStore
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.providers.factory import build_provider
from tradewatch.observability.logging import configure_logging, get_logger
from tradewatch.workers.bar_aggregator import run_rollups

log = get_logger(__name__)

__all__ = ["main", "run_backfill"]

DEFAULT_DAYS = 30


async def run_backfill(
    settings: Settings,
    *,
    symbols: Sequence[str] | None = None,
    interval: BarInterval = BarInterval.ONE_MINUTE,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
    days: int = DEFAULT_DAYS,
    resume: bool = True,
    rollups: bool = True,
    quality: bool = True,
    database: Database | None = None,
) -> tuple[BackfillResult, list[QualityReport]]:
    """Fetch and store history, then roll it up and check it."""
    watchlist = list(symbols or settings.market.symbols)
    finish = end or dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
    begin = start or finish - dt.timedelta(days=days)

    owns_database = database is None
    database = database or Database(settings.database)
    if owns_database:
        await database.connect()

    provider = build_provider(settings.provider, settings.market, symbols=watchlist)
    store = SqlBarStore(database.session_factory)
    service = BackfillService(provider, store)

    log.info(
        "backfill.starting",
        provider=provider.name,
        symbols=watchlist,
        interval=interval.value,
        start=begin.isoformat(),
        end=finish.isoformat(),
        resume=resume,
    )

    try:
        result = await service.run(watchlist, interval, begin, finish, resume=resume)

        if rollups:
            await run_rollups(settings, store, now=finish)

        reports: list[QualityReport] = []
        if quality:
            reports = await check_watchlist(store, watchlist, interval, begin, finish)
    finally:
        await provider.aclose()
        if owns_database:
            await database.disconnect()

    return result, reports


def _parse_day(value: str) -> dt.datetime:
    """A date or an ISO timestamp, always interpreted as UTC.

    A naive date on the command line means the UTC day, not the operator's local one. Two
    people in two timezones running the same command must backfill the same window.
    """
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not a date (YYYY-MM-DD) or an ISO timestamp"
        ) from error
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tradewatch-backfill",
        description="Fetch historical bars for a date range and store them idempotently.",
    )
    parser.add_argument(
        "--symbols",
        help="Comma-separated. Defaults to TW_SYMBOLS.",
    )
    parser.add_argument(
        "--interval",
        default=BarInterval.ONE_MINUTE.value,
        choices=[member.value for member in BarInterval],
        help="Bar interval to fetch. Coarser intervals are rolled up from 1m afterwards.",
    )
    parser.add_argument("--start", type=_parse_day, help="Inclusive, UTC.")
    parser.add_argument("--end", type=_parse_day, help="Exclusive, UTC. Defaults to now.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Days back from --end when --start is omitted (default {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Re-fetch the whole range instead of continuing after the last stored bar. "
        "Use when the provider has corrected history: the upsert repairs rather than "
        "duplicates.",
    )
    parser.add_argument("--no-rollups", action="store_true", help="Skip 5m/1h/1d rollups.")
    parser.add_argument("--no-quality", action="store_true", help="Skip the quality report.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point. Returns a process exit code."""
    load_env_file()
    options = _arguments(argv)

    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)  # noqa: T201 — logging is not configured yet
        return 2

    configure_logging(level=settings.app.log_level, log_format=settings.app.log_format)

    symbols = (
        [item.strip().upper() for item in options.symbols.split(",") if item.strip()]
        if options.symbols
        else None
    )

    async def _run() -> tuple[BackfillResult, list[QualityReport]]:
        return await run_backfill(
            settings,
            symbols=symbols,
            interval=BarInterval(options.interval),
            start=options.start,
            end=options.end,
            days=options.days,
            resume=not options.no_resume,
            rollups=not options.no_rollups,
            quality=not options.no_quality,
        )

    try:
        result, reports = asyncio.run(_run())
    except KeyboardInterrupt:
        # Partial work is already committed, and the next run resumes from it. That is the
        # point of writing each window before requesting the next.
        log.info("backfill.interrupted", detail="re-run to continue from the last stored bar")
        return 130
    except Exception as error:
        log.error("backfill.failed", error=str(error), error_type=type(error).__name__)
        return 1

    unhealthy = [report.symbol for report in reports if not report.healthy]
    log.info("backfill.finished", **result.as_dict(), unhealthy=unhealthy)

    # A quality problem is not a crash — the data is stored and the operator needs to see
    # the report — but it must not look like an unqualified success either.
    return 3 if unhealthy else 0


if __name__ == "__main__":
    raise SystemExit(main())
