"""The runner's poll loop survives a dropped database connection instead of exiting for good."""

from __future__ import annotations

from typing import Any

import pytest

from tradewatch.workers import backtest_runner

pytestmark = pytest.mark.unit


class _StopError(Exception):
    pass


async def test_transient_database_error_is_retried_and_other_errors_still_end_the_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcomes: list[BaseException | bool] = [TimeoutError(), OSError("reset"), True, _StopError()]
    sleeps: list[float] = []

    async def fake_process(_database: Any) -> bool:
        outcome = outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(backtest_runner, "process_one_backtest", fake_process)
    monkeypatch.setattr("asyncio.sleep", fake_sleep)

    with pytest.raises(_StopError):
        await backtest_runner.run_backtest_worker(
            settings=None,  # type: ignore[arg-type]  # unused when a database is supplied
            database=object(),  # type: ignore[arg-type]
            poll_interval_seconds=1.0,
        )

    assert outcomes == []
    assert sleeps == [1.0, 2.0]  # backs off while the database is down
