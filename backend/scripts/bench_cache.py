"""A one-off, honest measurement of cached vs. uncached reads for `GET /market/{symbol}/bars`.

This is **not** a Phase 10 benchmark: it has no committed methodology, no hardware record,
and produces no number that may be quoted anywhere in the project (`ARCHITECTURE.md` §1.3 /
`docs/PROJECT_STATE.md` §1: "no performance figure appears anywhere unless a benchmark in
`benchmarks/` produced it"). It exists only to prove, once, with real numbers instead of an
estimate, that Phase 4's cache does what it is for — see `learning.md` for the run this
produced and its actual output.

Needs a real, reachable PostgreSQL and Redis (`docker compose up -d postgres redis`) and the
schema already migrated. Seeds and cleans up its own throwaway instrument (`BENCH1`) so it
never touches real data.

Usage: ``uv run python scripts/bench_cache.py``
"""

from __future__ import annotations

import asyncio
import datetime as dt
import statistics
import time
from decimal import Decimal

from sqlalchemy import delete

from tradewatch.api.schemas.market import BarPoint, BarSeriesResponse
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, get_settings
from tradewatch.domain.entities import Instrument, NewBar
from tradewatch.domain.enums import AssetClass, BarInterval
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.cache import RedisCache
from tradewatch.infrastructure.database.models import Bar as BarModel
from tradewatch.infrastructure.database.models import Instrument as InstrumentModel
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.repositories import BarRepository, InstrumentRepository

SYMBOL = "BENCH1"
BAR_COUNT = 500
REPETITIONS = 200


async def _seed(database: Database) -> tuple[object, dt.datetime, dt.datetime]:
    end = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
    start = end - dt.timedelta(minutes=BAR_COUNT)

    async with database.session() as session:
        instruments = InstrumentRepository(session)
        existing = await instruments.get_by_symbol(SYMBOL)
        if existing is None:
            existing = await instruments.upsert(
                Instrument(
                    id=new_id(),
                    symbol=SYMBOL,
                    asset_class=AssetClass.EQUITY,
                    exchange="XBENCH",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    lot_size=Decimal("1"),
                    first_seen_at=dt.datetime.now(dt.UTC),
                )
            )
        bars = [
            NewBar(
                instrument_id=existing.id,
                interval=BarInterval.ONE_MINUTE,
                bar_start=start + dt.timedelta(minutes=i),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
                volume=Decimal("1000"),
                vwap=Decimal("100.4"),
                trade_count=10,
                is_final=True,
                revision=0,
            )
            for i in range(BAR_COUNT)
        ]
        await BarRepository(session).upsert_many(bars)
    return existing, start, end


async def _cleanup(database: Database, instrument_id: object) -> None:
    async with database.session() as session:
        await session.execute(delete(BarModel).where(BarModel.instrument_id == instrument_id))
        await session.execute(delete(InstrumentModel).where(InstrumentModel.id == instrument_id))


def _summarize(label: str, samples_ms: list[float]) -> None:
    print(
        f"{label:>18}:  min {min(samples_ms):7.3f} ms   "
        f"median {statistics.median(samples_ms):7.3f} ms   "
        f"mean {statistics.mean(samples_ms):7.3f} ms   "
        f"p95 {statistics.quantiles(samples_ms, n=20)[18]:7.3f} ms"
    )


async def main() -> None:
    load_env_file()
    try:
        settings = get_settings()
    except ConfigurationError as error:
        raise SystemExit(str(error)) from error

    database = Database(settings.database)
    await database.connect()
    cache = RedisCache(settings.redis)
    await cache.connect()

    if await database.check() is not None:
        raise SystemExit(
            "PostgreSQL is not reachable — start it with `docker compose up -d postgres`"
        )
    if await cache.check() is not None:
        raise SystemExit("Redis is not reachable — start it with `docker compose up -d redis`")

    print(f"Seeding {BAR_COUNT} bars for {SYMBOL}...")
    instrument, start, end = await _seed(database)

    try:
        # --- Uncached: the real repository query PostgreSQL would answer every time. ---
        uncached_ms: list[float] = []
        async with database.session() as session:
            repo = BarRepository(session)
            for _ in range(REPETITIONS):
                t0 = time.perf_counter()
                await repo.list_range(
                    instrument.id, BarInterval.ONE_MINUTE, start, end, limit=BAR_COUNT
                )
                uncached_ms.append((time.perf_counter() - t0) * 1000)

        # --- Cached: what `_read_through` in api/routes/market.py serves on a hit. ---
        async with database.session() as session:
            bars = await BarRepository(session).list_range(
                instrument.id, BarInterval.ONE_MINUTE, start, end, limit=BAR_COUNT
            )
        response = BarSeriesResponse(
            symbol=SYMBOL,
            interval=BarInterval.ONE_MINUTE,
            start=start,
            end=end,
            count=len(bars),
            bars=[BarPoint.model_validate(bar) for bar in bars],
        )
        cache_key = f"tw:bars:{SYMBOL}:1m"
        await cache.set_json(cache_key, response.model_dump(mode="json"), ttl_seconds=30)

        cached_ms: list[float] = []
        for _ in range(REPETITIONS):
            t0 = time.perf_counter()
            await cache.get_json(cache_key)
            cached_ms.append((time.perf_counter() - t0) * 1000)

        print(f"\n{REPETITIONS} reads each, {BAR_COUNT} bars, against real local Postgres+Redis:\n")
        _summarize("uncached (DB)", uncached_ms)
        _summarize("cached (Redis)", cached_ms)
        speedup = statistics.median(uncached_ms) / statistics.median(cached_ms)
        print(f"\nMedian speedup: {speedup:.1f}x")
        print(
            "\nNot a Phase 10 benchmark — no committed methodology or hardware record. "
            "One local run, printed so the number in learning.md is real, not estimated."
        )
    finally:
        await _cleanup(database, instrument.id)
        await database.disconnect()
        await cache.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
