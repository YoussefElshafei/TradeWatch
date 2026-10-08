"""API latency per endpoint at a fixed concurrency, with the read-through cache on and off.

**Method.** The script seeds an empty bench database through the real write path (the
`MarketDataProcessor` the consumer uses), then for each cache setting starts its own API
process — `python -m tradewatch.api`, the same entry point as `tradewatch-api`, with
`TW_CACHE_ENABLED` set — and drives every endpoint with `--concurrency` simultaneous
clients for `--requests` requests, after a warm-up. Latency is measured client-side, per
request, from send to full response body.

Isolation: the bench API uses the bench database and Redis database 15, so it can neither
read nor poison the development API's cache entries (same `tw:` keys, different data).

    cd backend && uv run python ../benchmarks/api_latency.py [--requests 1000 --concurrency 1,16]

Needs the Compose stack's PostgreSQL and Redis, and ports 8098-8099 free.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx2 as httpx
from _common import percentiles, write_result
from _isolated import BACKEND, bench_database

from tradewatch.application.processing import MarketDataProcessor
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import DatabaseSettings
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.database.market_store import SqlMarketDataStore
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.providers.normalization import EventFactory
from tradewatch.observability.logging import configure_logging

#: Both API processes run at once — cache on and cache off — and every endpoint is measured
#: on one and then the other, back to back. Run sequentially instead, the two halves were
#: minutes apart, and on a laptop that was enough for the *uncached control endpoints* to
#: differ by 2x between runs: drift as large as the effect being measured.
PORTS = {True: 8099, False: 8098}
SYMBOLS = ("AAPL", "MSFT", "NVDA", "SPY")
BARS_PER_SYMBOL = 500
REDIS_DB = 15
WARMUP = 200

PUBLIC = {
    "GET /health": "/health",
    "GET /api/v1/instruments": "/api/v1/instruments",
    "GET /api/v1/market/{symbol}/bars": "/api/v1/market/AAPL/bars?interval=1m",
    "GET /api/v1/market/{symbol}/latest": "/api/v1/market/AAPL/latest",
}
AUTHENTICATED = {
    "GET /api/v1/strategies": "/api/v1/strategies",
    "GET /api/v1/backtests": "/api/v1/backtests",
}


async def _seed(url: str) -> None:
    database = Database(DatabaseSettings(database_url=url))
    await database.connect()
    processor = MarketDataProcessor(SqlMarketDataStore(database.session_factory), batch_size=500)
    factory = EventFactory(source="bench", producer="api-benchmark@1")
    now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
    try:
        for symbol in SYMBOLS:
            for minute in range(BARS_PER_SYMBOL, 0, -1):
                price = Decimal(100) + Decimal(minute % 37) / 10
                await processor.handle(
                    factory.bar(
                        symbol=symbol,
                        interval=BarInterval.ONE_MINUTE,
                        bar_start=now - dt.timedelta(minutes=minute),
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                        volume=Decimal(1000),
                        trade_count=10,
                    )
                )
            await processor.handle(
                factory.trade(symbol=symbol, price=Decimal(101), size=Decimal(5), occurred_at=now)
            )
            await processor.handle(
                factory.quote(
                    symbol=symbol,
                    bid_price=Decimal("100.99"),
                    bid_size=Decimal(100),
                    ask_price=Decimal("101.01"),
                    ask_size=Decimal(100),
                    occurred_at=now,
                )
            )
        await processor.flush()
    finally:
        await database.disconnect()


def _start_api(url: str, cache: bool) -> subprocess.Popen[bytes]:
    port = PORTS[cache]
    redis = os.environ["TW_REDIS_URL"].rsplit("/", 1)[0] + f"/{REDIS_DB}"
    env = os.environ | {
        "TW_DATABASE_URL": url,
        "TW_REDIS_URL": redis,
        "TW_API_PORT": str(port),
        "TW_API_HOST": "127.0.0.1",
        "TW_CACHE_ENABLED": str(cache).lower(),
        "TW_ENV": "ci",  # no reloader: one serving process, like a deployment
        "TW_LOG_LEVEL": "WARNING",
        "TW_METRICS_ENABLED": "false",
    }
    return subprocess.Popen(
        [sys.executable, "-m", "tradewatch.api"],
        cwd=BACKEND,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


async def _wait_healthy(client: httpx.AsyncClient) -> None:
    for _ in range(120):
        try:
            if (await client.get("/health")).status_code == 200:
                return
        except httpx.TransportError:
            pass
        await asyncio.sleep(0.5)
    raise RuntimeError("bench API did not become healthy")


async def _token(client: httpx.AsyncClient) -> str:
    credentials = {"email": "bench@example.com", "password": "benchmark-password-1"}
    await client.post("/api/v1/auth/register", json=credentials)
    response = await client.post("/api/v1/auth/login", json=credentials)
    response.raise_for_status()
    token: str = response.json()["access_token"]
    return token


async def _load(
    client: httpx.AsyncClient, path: str, headers: dict[str, str], requests: int, concurrency: int
) -> dict:  # type: ignore[type-arg]
    latencies: list[float] = []
    failures = 0
    remaining = iter(range(requests))

    async def worker() -> None:
        nonlocal failures
        for _ in remaining:
            started = time.perf_counter()
            response = await client.get(path, headers=headers)
            latencies.append((time.perf_counter() - started) * 1000)
            if response.status_code != 200:
                failures += 1

    for _ in range(WARMUP):  # connection pool, first cache fill, query plans, allocator
        await client.get(path, headers=headers)
    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed = time.perf_counter() - started
    if failures:
        raise RuntimeError(f"{path}: {failures} non-200 responses")
    summary = {key: round(value, 3) for key, value in percentiles(latencies).items()}
    return {"latency_ms": summary, "requests_per_second": round(requests / elapsed, 1)}


async def _measure_both(url: str, requests: int, levels: list[int]) -> dict:  # type: ignore[type-arg]
    processes = [_start_api(url, cache) for cache in PORTS]
    top = max(levels)
    limits = httpx.Limits(max_connections=top, max_keepalive_connections=top)
    clients = {
        cache: httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", limits=limits, timeout=60)
        for cache, port in PORTS.items()
    }
    try:
        for client in clients.values():
            await _wait_healthy(client)
        # One user in the shared database; each API issues its own token for it.
        auth = {
            cache: {"Authorization": f"Bearer {await _token(c)}"} for cache, c in clients.items()
        }
        results: dict[str, dict] = {"cache_on": {}, "cache_off": {}}  # type: ignore[type-arg]
        endpoints = [(name, path, False) for name, path in PUBLIC.items()]
        endpoints += [(name, path, True) for name, path in AUTHENTICATED.items()]
        for concurrency in levels:
            level = f"concurrency_{concurrency}"
            for name, path, needs_auth in endpoints:
                for cache, label in ((True, "cache_on"), (False, "cache_off")):
                    headers = auth[cache] if needs_auth else {}
                    results[label].setdefault(level, {})[name] = await _load(
                        clients[cache], path, headers, requests, concurrency
                    )
        return results
    finally:
        for client in clients.values():
            await client.aclose()
        for process in processes:
            process.terminate()
            process.wait(timeout=30)


async def main(requests: int, levels: list[int]) -> None:
    load_env_file()
    configure_logging(level="WARNING")
    async with bench_database(os.environ["TW_DATABASE_URL"]) as url:
        await _seed(url)
        measured = await _measure_both(url, requests, levels)

    write_result(
        "api_latency",
        description="Client-side latency per endpoint at fixed concurrency, cache on vs off.",
        command=(
            "cd backend && uv run python ../benchmarks/api_latency.py "
            f"--requests {requests} --concurrency {','.join(map(str, levels))}"
        ),
        workload={
            "requests_per_endpoint_per_level": requests,
            "concurrency_levels": levels,
            "warmup_requests_per_endpoint": WARMUP,
            "data": f"{len(SYMBOLS)} symbols x {BARS_PER_SYMBOL} 1m bars, one trade and quote each",
            "client": "httpx2 AsyncClient on the same machine, keep-alive connections",
        },
        configuration={
            "api_processes": "2 (one cache on, one cache off), measured alternately per endpoint",
            "uvicorn_workers": 1,
            "reload": False,
            "rate_limit": "applies only to /auth routes, none of which are measured",
            "log_level": "WARNING",
            "redis_db": REDIS_DB,
        },
        results=measured,
        caveats=[
            "Concurrency 1 is service time. At higher concurrency throughput barely rises "
            "above concurrency 1, so something single-threaded is saturated and most of each "
            "latency is queueing. Whether that is the one API worker or the one-process load "
            "generator was not separated; both are single Python event loops on one machine.",
            "The load generator ran on the same machine as the API, PostgreSQL and Redis and "
            "competed with them for CPU; a remote client would see network time added and "
            "less contention.",
            "One API process with one worker. It says nothing about horizontal scaling.",
            "Only the four market endpoints are cached; the authenticated endpoints are "
            "listed in both runs as a control and should not differ beyond noise.",
            "A small, fixed dataset. Bars for one symbol at the default window; latency on "
            "a much larger table or a cold OS page cache will differ.",
        ],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", default="1,16", help="comma-separated levels")
    options = parser.parse_args()
    asyncio.run(main(options.requests, [int(c) for c in options.concurrency.split(",")]))
