"""The Backtest Runner worker, against a real database.

`process_one_backtest` takes a `SessionSource` (a `Protocol`, not the concrete `Database`),
so this file drives it with a fake whose `.session()` yields the test's own transactional
`session` fixture both times it is called. That was the smaller amount of new plumbing than
building a second, independently-committing `Database` against the test database and cleaning
up its writes by hand afterward — and it still exercises the real repositories, the real
`run_backtest` engine, and the real persistence, which is what this test is actually for. The
one thing it does *not* exercise is the production claim/run transaction *boundary* itself
(that the claim survives even if the run crashes) — that is a Phase 11 failure-recovery
concern, not a Phase 6 one.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.domain.entities import NewBar
from tradewatch.domain.enums import BacktestStatus, BarInterval
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import BacktestRun, Instrument, Strategy, User
from tradewatch.infrastructure.repositories import BarRepository
from tradewatch.workers.backtest_runner import SessionSource, process_one_backtest

pytestmark = pytest.mark.integration

# The golden-fixture closes from `tests/unit/test_backtest_engine.py::TestGoldenBacktest`:
# a warm-up, one crossover long, a rally, then a cross back down to flat.
_CLOSES = [10, 10, 10, 12, 14, 8]


class _FakeDatabase:
    """Satisfies `SessionSource` by handing back the same test session every time.

    `process_one_backtest` opens two separate sessions in production (claim, then run) so the
    claim is durable before the heavy work starts. Sharing one session here collapses that
    into a single test transaction — the whole point of the fixture-provided `session`, which
    a real second `Database` connection could not see uncommitted rows from at all.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        return self._passthrough()

    @asynccontextmanager
    async def _passthrough(self) -> AsyncIterator[AsyncSession]:
        # Mirrors `session_scope`'s own commit-on-success behaviour (`database/session.py`),
        # which is what actually persists `process_one_backtest`'s status/metrics updates in
        # production. Safe here only because the fixture's `session` runs in
        # `join_transaction_mode="create_savepoint"` mode, where `commit()` releases a
        # savepoint rather than committing for real — the whole point of that mode.
        yield self._session
        await self._session.commit()


async def _crossover_strategy(
    session: AsyncSession, user: User, instrument: Instrument
) -> Strategy:
    """A strategy sized to actually cross over within `_CLOSES`'s 6 bars.

    Not the shared `strategy` fixture from `conftest.py`: that one uses `fast_period=10,
    slow_period=30`, which needs 30 bars just to warm up and would produce zero trades over
    this series — a different, and equally valid, test data set for the API tests that only
    care that *a* strategy exists, but the wrong one for a test that asserts a specific trade
    count.
    """
    row = Strategy(
        id=new_id(),
        user_id=user.id,
        name="Runner test crossover",
        strategy_type="sma_crossover",
        parameters={"fast_period": 2, "slow_period": 3},
        symbols=[instrument.symbol],
    )
    session.add(row)
    await session.flush()
    return row


async def _seed_daily_bars(session: AsyncSession, instrument: Instrument) -> None:
    bar_repository = BarRepository(session)
    rows = [
        NewBar(
            instrument_id=instrument.id,
            interval=BarInterval.ONE_DAY,
            bar_start=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(days=day),
            open=Decimal(str(close)),
            high=Decimal(str(close)),
            low=Decimal(str(close)),
            close=Decimal(str(close)),
            volume=Decimal(1000),
            vwap=Decimal(str(close)),
            trade_count=10,
            is_final=True,
            revision=0,
        )
        for day, close in enumerate(_CLOSES)
    ]
    await bar_repository.upsert_many(rows)


async def _queued_run(
    session: AsyncSession, user: User, strategy: Strategy, *, slippage_bps: str = "0"
) -> BacktestRun:
    run = BacktestRun(
        id=new_id(),
        user_id=user.id,
        strategy_id=strategy.id,
        symbols=strategy.symbols,
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, 6),
        initial_cash=Decimal(12000),
        execution_config={
            "slippage_bps": slippage_bps,
            "commission_per_share": "0",
            "commission_pct": "0",
            "fill_model": "current_close",
            "allow_partial_fills": False,
        },
    )
    session.add(run)
    await session.flush()
    return run


class TestProcessOneBacktest:
    async def test_runs_the_queued_backtest_to_completion(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        await _seed_daily_bars(session, instrument)
        crossover_strategy = await _crossover_strategy(session, user, instrument)
        run = await _queued_run(session, user, crossover_strategy)
        database: SessionSource = _FakeDatabase(session)

        processed = await process_one_backtest(database)
        assert processed is True

        await session.refresh(run)
        assert run.status == BacktestStatus.COMPLETED
        assert run.completed_at is not None
        assert run.metrics is not None
        assert run.metrics["trade_count"] == 1
        assert "total_return_pct" in run.metrics
        assert "max_drawdown_pct" in run.metrics

    async def test_empty_queue_returns_false(self, session: AsyncSession) -> None:
        database: SessionSource = _FakeDatabase(session)
        assert await process_one_backtest(database) is False

    async def test_an_invalid_strategy_type_fails_the_run_not_the_worker(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        bad_strategy = Strategy(
            id=new_id(),
            user_id=user.id,
            name="Bad strategy",
            strategy_type="not_a_real_strategy",
            parameters={},
            symbols=[instrument.symbol],
        )
        session.add(bad_strategy)
        await session.flush()
        run = await _queued_run(session, user, bad_strategy)
        database: SessionSource = _FakeDatabase(session)

        processed = await process_one_backtest(database)
        assert processed is True

        await session.refresh(run)
        assert run.status == BacktestStatus.FAILED
        assert run.error_message is not None
        assert "not_a_real_strategy" in run.error_message

    async def test_slippage_changes_the_stored_result(
        self, session: AsyncSession, user: User, instrument: Instrument
    ) -> None:
        """The `TW_EXEC_SLIPPAGE_BPS` exit criterion, end to end: two runs differing only in
        the `execution_config` persisted on the row produce different stored metrics.
        """
        await _seed_daily_bars(session, instrument)
        crossover_strategy = await _crossover_strategy(session, user, instrument)
        database: SessionSource = _FakeDatabase(session)

        cheap_run = await _queued_run(session, user, crossover_strategy, slippage_bps="0")
        assert await process_one_backtest(database) is True
        await session.refresh(cheap_run)

        expensive_run = await _queued_run(session, user, crossover_strategy, slippage_bps="500")
        assert await process_one_backtest(database) is True
        await session.refresh(expensive_run)

        assert cheap_run.status == BacktestStatus.COMPLETED
        assert expensive_run.status == BacktestStatus.COMPLETED
        assert cheap_run.metrics is not None
        assert expensive_run.metrics is not None
        assert cheap_run.metrics["total_return_pct"] != expensive_run.metrics["total_return_pct"]
