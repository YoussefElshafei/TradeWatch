"""Rebuild every portfolio's positions from `fills` alone and compare against what is stored.

This is `ARCHITECTURE.md` §6.2's own requirement made runnable: "events are the truth, state
is a projection." It is both a consistency check (run it any time; a clean report means the
projection agrees with the events that produced it) and the disaster-recovery path if
`positions` is ever suspected of drifting — a bad manual `UPDATE`, a bug in an older version
of the Paper Trading Engine, a restore from a backup taken between a fill and its position
write.

Read-only by default: it reports drift without correcting it. Pass ``--fix`` to overwrite
the stored `positions` rows with what the fills actually justify.

Usage::

    uv run python scripts/rebuild_positions.py                     # report only
    uv run python scripts/rebuild_positions.py --fix                # report and correct
    uv run python scripts/rebuild_positions.py --portfolio <uuid>   # one portfolio only
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from contextlib import AbstractAsyncContextManager
from decimal import Decimal
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, get_settings
from tradewatch.infrastructure.database.models import Portfolio, Position
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.repositories import PositionRepository, RebuiltPosition

__all__ = ["SessionSource", "main", "rebuild_one_portfolio"]

_ZERO = Decimal(0)


class SessionSource(Protocol):
    """What `rebuild_one_portfolio` actually needs: a way to open a transactional session.

    Declared as a `Protocol` rather than typed directly against `Database` so a test can
    satisfy it with a fake that shares the test's own rolled-back transaction — the same
    reason `workers/backtest_runner.py`'s `SessionSource` exists.
    """

    def session(self) -> AbstractAsyncContextManager[AsyncSession]: ...


async def rebuild_one_portfolio(
    database: SessionSource, portfolio_id: uuid.UUID, *, fix: bool
) -> int:
    """Rebuild one portfolio's positions and report (or fix) drift.

    Returns the number of instruments whose stored row disagreed with the fills.
    """
    drifted = 0
    async with database.session() as session:
        repository = PositionRepository(session)
        rebuilt = await repository.rebuild_from_fills(portfolio_id)

        stored_rows = (
            await session.execute(select(Position).where(Position.portfolio_id == portfolio_id))
        ).scalars()
        stored: dict[uuid.UUID, Position] = {row.instrument_id: row for row in stored_rows}

        every_instrument = set(rebuilt) | set(stored)
        for instrument_id in sorted(every_instrument, key=str):
            expected = rebuilt.get(instrument_id, RebuiltPosition(_ZERO, _ZERO, _ZERO))
            actual = stored.get(instrument_id)
            matches = (
                actual is not None
                and actual.quantity == expected.quantity
                and actual.avg_entry_price == expected.avg_entry_price
                and actual.realized_pnl == expected.realized_pnl
            )
            if matches:
                continue

            drifted += 1
            actual_repr = (
                "no stored row"
                if actual is None
                else (
                    f"quantity={actual.quantity} avg_entry_price={actual.avg_entry_price} "
                    f"realized_pnl={actual.realized_pnl}"
                )
            )
            print(
                f"  DRIFT portfolio={portfolio_id} instrument={instrument_id}\n"
                f"    stored:   {actual_repr}\n"
                f"    rebuilt:  quantity={expected.quantity} "
                f"avg_entry_price={expected.avg_entry_price} realized_pnl={expected.realized_pnl}"
            )
            if fix:
                await repository.upsert(
                    portfolio_id,
                    instrument_id,
                    quantity=expected.quantity,
                    avg_entry_price=expected.avg_entry_price,
                    realized_pnl=expected.realized_pnl,
                )

        if fix and drifted:
            await session.commit()

    return drifted


async def _run(*, portfolio_id: uuid.UUID | None, fix: bool) -> int:
    load_env_file()
    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    database = Database(settings.database)
    await database.connect()
    try:
        async with database.session() as session:
            if portfolio_id is not None:
                portfolio_ids = [portfolio_id]
            else:
                result = await session.execute(select(Portfolio.id))
                portfolio_ids = list(result.scalars())

        if not portfolio_ids:
            print("No portfolios to check.")
            return 0

        total_drift = 0
        for one_id in portfolio_ids:
            total_drift += await rebuild_one_portfolio(database, one_id, fix=fix)

        verb = "corrected" if fix else "found"
        print(
            f"\nChecked {len(portfolio_ids)} portfolio(s): "
            f"{total_drift} instrument(s) with drift {verb}."
        )
        return 1 if (total_drift and not fix) else 0
    finally:
        await database.disconnect()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Overwrite stored positions with what the fills rebuild — default is report only.",
    )
    parser.add_argument(
        "--portfolio",
        type=uuid.UUID,
        default=None,
        help="Check one portfolio instead of every portfolio in the database.",
    )
    args = parser.parse_args(argv)
    return asyncio.run(_run(portfolio_id=args.portfolio, fix=args.fix))


if __name__ == "__main__":
    raise SystemExit(main())
