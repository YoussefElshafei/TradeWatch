"""The Backtest Runner: `uv run tradewatch-backtest-runner`.

`ARCHITECTURE.md` §4.3: a `backtest_runs` row is a job, not a request, and the queue is the
table itself — claimed with `FOR UPDATE SKIP LOCKED`
(`BacktestRunRepository.claim_next_queued`), which is exactly the workload a database queue
is good at (bounded, low rate, transactional) rather than a reason to stand up a broker for a
batch computation over data that already exists.

Two transactions per run, deliberately not one:

1. **Claim.** Mark the row `running` and commit immediately, so the claim is durable and
   visible to anyone watching the row before the (potentially slow) computation starts. A
   crash between claiming and finishing leaves a `running` row rather than losing the claim
   back to `queued` — recovering *that* is a Phase 11 failure-recovery concern, not this one.
2. **Run and record.** Load the strategy and bars, call the pure `run_backtest` (see
   `application/backtest/engine.py`), persist trades and the equity curve, and mark the run
   `completed` or `failed` — all in the same transaction, so a crash here leaves the row
   `running` rather than a half-written result masquerading as a finished one.

**Daily bars, hardcoded.** `backtest_runs.start_date`/`end_date` are `Date`, not `datetime`,
and the table has no `interval` column — this schema was built for one granularity, and daily
is the only one a `start_date`/`end_date` pair honestly represents. Intraday backtesting is a
schema change (an `interval` column), not a parameter this worker can invent for itself.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sys
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.application.backtest.engine import run_backtest
from tradewatch.application.execution import ExecutionConfig
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import BacktestStatus, BarInterval
from tradewatch.infrastructure.database.models import BacktestRun
from tradewatch.infrastructure.database.models import Bar as BarRow
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.repositories import (
    BacktestEquityPointRepository,
    BacktestRunRepository,
    BacktestTradeRepository,
    BarRepository,
    InstrumentRepository,
    StrategyRepository,
)
from tradewatch.observability.logging import configure_logging, get_logger

log = get_logger(__name__)

__all__ = ["SessionSource", "main", "process_one_backtest", "run_backtest_worker"]

#: A generous ceiling on bars per symbol per run. A multi-year daily series is a few
#: thousand rows; this only guards against a mis-entered decades-long range flooding memory.
_MAX_BARS_PER_SYMBOL = 20_000


class SessionSource(Protocol):
    """What `process_one_backtest` actually needs from a `Database`: a way to open a
    transactional session, twice, independently. Declared as a `Protocol` rather than typed
    directly against `Database` so a test can satisfy it with a fake that shares the test's
    own rolled-back transaction — see `tests/integration/test_backtest_runner.py`.
    """

    def session(self) -> AbstractAsyncContextManager[AsyncSession]: ...


async def process_one_backtest(database: SessionSource) -> bool:
    """Claim and run one queued backtest. Returns False when the queue was empty."""
    async with database.session() as claim_session:
        claimed = await BacktestRunRepository(claim_session).claim_next_queued()
        if claimed is None:
            return False
        run_id = claimed.id

    log.info("backtest.claimed", backtest_run_id=str(run_id))

    async with database.session() as session:
        run = await BacktestRunRepository(session).get(run_id)
        if run is None:  # pragma: no cover — cannot happen: just claimed, and nothing deletes runs
            raise RuntimeError(f"backtest run {run_id} vanished between claim and load")

        try:
            metrics = await _run_backtest(session, run)
        except Exception as error:
            log.warning(
                "backtest.failed",
                backtest_run_id=str(run_id),
                error=str(error),
                error_type=type(error).__name__,
            )
            run.status = BacktestStatus.FAILED
            run.error_message = str(error)
            run.completed_at = dt.datetime.now(dt.UTC)
            return True

        run.status = BacktestStatus.COMPLETED
        run.metrics = metrics
        run.completed_at = dt.datetime.now(dt.UTC)
        log.info("backtest.completed", backtest_run_id=str(run_id))
        return True


def _to_domain_bar(row: BarRow) -> Bar:
    """`BarRepository.list_range` returns ORM rows; `run_backtest` takes domain entities.

    The engine is pure application code and must not know an ORM model exists (the
    `domain`/`infrastructure` split `domain/entities.py`'s own docstring describes) — so the
    one place that has both in scope, this worker, is where the conversion belongs.
    """
    return Bar(
        instrument_id=row.instrument_id,
        interval=row.interval,
        bar_start=row.bar_start,
        open=row.open,
        high=row.high,
        low=row.low,
        close=row.close,
        volume=row.volume,
        vwap=row.vwap,
        trade_count=row.trade_count,
        is_final=row.is_final,
        revision=row.revision,
        recorded_at=row.recorded_at,
    )


async def _run_backtest(session: AsyncSession, run: BacktestRun) -> dict[str, Any]:
    """Load inputs, run the pure engine, and persist trades and the equity curve.

    Split out of `process_one_backtest` only so that function's try/except stays about
    *catching* a failure, not about the several steps that can produce one.
    """
    strategy = await StrategyRepository(session).get_for(run.strategy_id, run.user_id)
    if strategy is None:
        raise ValueError(
            f"strategy {run.strategy_id} no longer exists or is not owned by the run's user"
        )

    start_dt = dt.datetime.combine(run.start_date, dt.time.min, tzinfo=dt.UTC)
    end_dt = dt.datetime.combine(run.end_date, dt.time.min, tzinfo=dt.UTC) + dt.timedelta(days=1)

    instrument_repository = InstrumentRepository(session)
    bar_repository = BarRepository(session)
    symbol_to_instrument = await instrument_repository.get_many_by_symbol(run.symbols)

    bars_by_symbol: dict[str, Sequence[Bar]] = {}
    for symbol in run.symbols:
        instrument = symbol_to_instrument.get(symbol)
        if instrument is None:
            # An unknown symbol does not fail the whole basket — the engine already treats
            # a symbol with no bars as an idle, fully-cash sub-account, and reporting that
            # honestly is more useful than aborting a multi-symbol run over one typo.
            log.warning("backtest.unknown_symbol", backtest_run_id=str(run.id), symbol=symbol)
            bars_by_symbol[symbol] = []
            continue
        rows = await bar_repository.list_range(
            instrument.id, BarInterval.ONE_DAY, start_dt, end_dt, limit=_MAX_BARS_PER_SYMBOL
        )
        bars_by_symbol[symbol] = [_to_domain_bar(row) for row in rows]

    # The execution assumptions are read from the row, never from the environment: this is
    # what makes `execution_config` an honest record of what actually produced the result
    # (`ARCHITECTURE.md` §16.5), immune to a later change to `TW_EXEC_SLIPPAGE_BPS`.
    execution_config = ExecutionConfig.model_validate(run.execution_config)

    result = run_backtest(
        strategy_type=strategy.strategy_type,
        parameters=strategy.parameters,
        symbols=run.symbols,
        bars_by_symbol=bars_by_symbol,
        initial_cash=run.initial_cash,
        execution_config=execution_config,
        start=start_dt,
        end=end_dt,
    )

    symbol_to_instrument_id = {
        symbol: instrument.id for symbol, instrument in symbol_to_instrument.items()
    }
    await BacktestTradeRepository(session).bulk_insert(
        run.id, result.trades, symbol_to_instrument_id
    )
    await BacktestEquityPointRepository(session).bulk_upsert(run.id, result.equity_curve)

    return result.metrics.as_json()


async def run_backtest_worker(
    settings: Settings,
    *,
    database: Database | None = None,
    poll_interval_seconds: float = 2.0,
    once: bool = False,
) -> None:
    """Poll `backtest_runs` for queued work and run it, forever (or once, for tests/`--once`)."""
    owns_database = database is None
    database = database or Database(settings.database)
    if owns_database:
        await database.connect()

    try:
        while True:
            processed = await process_one_backtest(database)
            if once:
                return
            if not processed:
                await asyncio.sleep(poll_interval_seconds)
    finally:
        if owns_database:
            await database.disconnect()


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tradewatch-backtest-runner",
        description="Claim and run queued backtests, forever or once.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process at most one queued backtest, then exit — for scripts and tests.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="How long to sleep after finding an empty queue (default 2.0).",
    )
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

    try:
        asyncio.run(
            run_backtest_worker(
                settings,
                poll_interval_seconds=options.poll_interval_seconds,
                once=options.once,
            )
        )
    except KeyboardInterrupt:
        log.info("backtest_runner.interrupted")
        return 130
    except Exception as error:
        log.error("backtest_runner.failed", error=str(error), error_type=type(error).__name__)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
