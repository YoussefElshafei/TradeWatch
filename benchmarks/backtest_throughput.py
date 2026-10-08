"""Backtest throughput: bars/sec, for the pure engine and for the whole runner.

**Two numbers, because they answer different questions.**

* *Engine* — `run_backtest`, the pure function both backtests and paper trading call, over
  bars already in memory. How fast the strategy and execution simulation are.
* *Runner* — `process_one_backtest`, what `tradewatch-backtest-runner` does per queued run:
  claim the row, load bars from PostgreSQL, run the engine, write trades and the equity
  curve, mark it complete. How long a user actually waits, minus the poll interval.

Workload: an SMA crossover (fast 10 / slow 30 — the editor's defaults) over `--symbols`
symbols of `--days` daily bars each, a seeded random walk so the strategy actually trades.
Each is repeated `--repeats` times; every repetition is reported, not just the best.

    cd backend && uv run python ../benchmarks/backtest_throughput.py [--symbols 10 --days 2520]

Needs the Compose stack's PostgreSQL (for the runner half). The bench database is dropped
afterwards.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import random
import statistics
import sys
import time
from decimal import Decimal
from pathlib import Path
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import write_result
from _isolated import bench_database

from tradewatch.application.backtest.engine import run_backtest
from tradewatch.application.execution import ExecutionConfig
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import DatabaseSettings, FillModel
from tradewatch.domain.entities import Bar, NewBar
from tradewatch.domain.enums import AssetClass, BacktestStatus, BarInterval, UserRole
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import BacktestRun, Instrument, Strategy, User
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.repositories import BarRepository
from tradewatch.observability.logging import configure_logging
from tradewatch.workers.backtest_runner import process_one_backtest

START = dt.datetime(2016, 1, 1, tzinfo=dt.UTC)
PARAMETERS = {"fast_period": 10, "slow_period": 30}
EXECUTION = {
    "slippage_bps": "2",
    "commission_per_share": "0.005",
    "commission_pct": "0",
    "fill_model": FillModel.NEXT_BAR_OPEN.value,
    "allow_partial_fills": False,
}


def _walk(symbol_index: int, days: int) -> list[tuple[dt.datetime, Decimal]]:
    rng = random.Random(1000 + symbol_index)  # noqa: S311 - a benchmark workload
    price = 100.0
    series = []
    for day in range(days):
        price = max(price * (1 + rng.gauss(0.0003, 0.015)), 1.0)
        series.append((START + dt.timedelta(days=day), Decimal(f"{price:.4f}")))
    return series


def _domain_bars(instrument_id: UUID, series: list[tuple[dt.datetime, Decimal]]) -> list[Bar]:
    return [
        Bar(
            instrument_id=instrument_id,
            interval=BarInterval.ONE_DAY,
            bar_start=start,
            open=close,
            high=close,
            low=close,
            close=close,
            volume=Decimal(100_000),
            vwap=close,
            trade_count=1_000,
            is_final=True,
            revision=0,
            recorded_at=start,
        )
        for start, close in series
    ]


def _bench_engine(symbols: int, days: int, repeats: int) -> dict:  # type: ignore[type-arg]
    bars = {f"BT{i:02d}": _domain_bars(new_id(), _walk(i, days)) for i in range(symbols)}
    total = symbols * days
    runs = []
    trades = 0
    for _ in range(repeats):
        started = time.perf_counter()
        result = run_backtest(
            strategy_type="sma_crossover",
            parameters=PARAMETERS,
            symbols=list(bars),
            bars_by_symbol=bars,
            initial_cash=Decimal(100_000),
            execution_config=ExecutionConfig.model_validate(EXECUTION),
            start=START,
            end=START + dt.timedelta(days=days),
        )
        runs.append(time.perf_counter() - started)
        trades = len(result.trades)
    return _summarise(total, runs) | {"trades_per_run": trades}


async def _bench_runner(url: str, symbols: int, days: int, repeats: int) -> dict:  # type: ignore[type-arg]
    database = Database(DatabaseSettings(database_url=url))
    await database.connect()
    try:
        async with database.session() as session:
            user = User(
                id=new_id(),
                email="bench@example.invalid",
                password_hash="not-a-real-hash",  # noqa: S106 - never a credential
                role=UserRole.TRADER,
            )
            session.add(user)
            names = []
            for i in range(symbols):
                instrument = Instrument(
                    id=new_id(),
                    symbol=f"BT{i:02d}",
                    name="Benchmark",
                    exchange="XBNC",
                    asset_class=AssetClass.EQUITY,
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    lot_size=Decimal(1),
                )
                session.add(instrument)
                await session.flush()
                names.append(instrument.symbol)
                await BarRepository(session).upsert_many(
                    [
                        NewBar(
                            instrument_id=instrument.id,
                            interval=BarInterval.ONE_DAY,
                            bar_start=start,
                            open=close,
                            high=close,
                            low=close,
                            close=close,
                            volume=Decimal(100_000),
                            vwap=close,
                            trade_count=1_000,
                            is_final=True,
                            revision=0,
                        )
                        for start, close in _walk(i, days)
                    ]
                )
            strategy = Strategy(
                id=new_id(),
                user_id=user.id,
                name="bench",
                symbols=names,
                strategy_type="sma_crossover",
                parameters=PARAMETERS,
            )
            session.add(strategy)

        runs = []
        for _ in range(repeats):
            async with database.session() as session:
                session.add(
                    BacktestRun(
                        id=new_id(),
                        user_id=user.id,
                        strategy_id=strategy.id,
                        symbols=names,
                        start_date=START.date(),
                        end_date=(START + dt.timedelta(days=days - 1)).date(),
                        initial_cash=Decimal(100_000),
                        execution_config=EXECUTION,
                    )
                )
            started = time.perf_counter()
            assert await process_one_backtest(database) is True
            runs.append(time.perf_counter() - started)

        async with database.session() as session:
            from sqlalchemy import func, select

            statuses = (
                await session.execute(
                    select(BacktestRun.status, func.count()).group_by(BacktestRun.status)
                )
            ).all()
        assert dict(statuses) == {BacktestStatus.COMPLETED: repeats}, statuses
    finally:
        await database.disconnect()
    return _summarise(symbols * days, runs)


def _summarise(bars: int, runs: list[float]) -> dict:  # type: ignore[type-arg]
    rates = [bars / seconds for seconds in runs]
    return {
        "bars_per_run": bars,
        "seconds_per_run": [round(seconds, 4) for seconds in runs],
        "bars_per_second_each_run": [round(rate) for rate in rates],
        "bars_per_second_median": round(statistics.median(rates)),
    }


async def main(symbols: int, days: int, repeats: int) -> None:
    load_env_file()
    configure_logging(level="WARNING")
    engine = _bench_engine(symbols, days, repeats)
    async with bench_database(os.environ["TW_DATABASE_URL"]) as url:
        runner = await _bench_runner(url, symbols, days, repeats)

    write_result(
        "backtest_throughput",
        description="Bars/sec through the pure backtest engine and through the full runner.",
        command=(
            "cd backend && uv run python ../benchmarks/backtest_throughput.py "
            f"--symbols {symbols} --days {days} --repeats {repeats}"
        ),
        workload={
            "strategy": "sma_crossover",
            "parameters": PARAMETERS,
            "symbols": symbols,
            "daily_bars_per_symbol": days,
            "price_series": "seeded geometric random walk (drift 0.03%/day, sigma 1.5%)",
            "repeats": repeats,
        },
        configuration={"execution_config": EXECUTION, "initial_cash": 100_000},
        results={"engine": engine, "runner": runner},
        caveats=[
            "One strategy type. RSI reversion and future strategy types do different work "
            "per bar and were not measured.",
            "Engine figures exclude all I/O; runner figures include PostgreSQL on the same "
            "machine, so they are a local-dev figure, not a production one.",
            "Daily bars only — the runner reads only `1d` bars today.",
        ],
    )
    print(  # noqa: T201
        f"engine: {engine['bars_per_second_median']} bars/sec, "
        f"runner: {runner['bars_per_second_median']} bars/sec"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--symbols", type=int, default=10)
    parser.add_argument("--days", type=int, default=2520)
    parser.add_argument("--repeats", type=int, default=5)
    options = parser.parse_args()
    asyncio.run(main(options.symbols, options.days, options.repeats))
