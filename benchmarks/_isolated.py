"""A throwaway, freshly migrated database for one benchmark run.

A benchmark that writes into the development database would measure whatever happened to
be in it — and leave its own rows behind for the next run and the next developer. So each
run gets `<dev database>_bench`, built from empty by the real migrations, and dropped at
the end. Same server, same settings: what changes is only that the tables start empty.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg

BACKEND = Path(__file__).resolve().parent.parent / "backend"


def _plain_dsn(url: str) -> str:
    """`postgresql+asyncpg://...` (SQLAlchemy) -> `postgresql://...` (asyncpg)."""
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _with_database(url: str, name: str) -> str:
    base, _, _old = url.rpartition("/")
    return f"{base}/{name}"


@asynccontextmanager
async def bench_database(dev_url: str) -> AsyncIterator[str]:
    """Create, migrate and yield a SQLAlchemy URL for an empty bench database; drop it after."""
    dev_name = dev_url.rpartition("/")[2].split("?")[0]
    name = f"{dev_name}_bench"
    url = _with_database(dev_url, name)

    admin = await asyncpg.connect(_plain_dsn(dev_url))
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()

    await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=BACKEND,
        env={**os.environ, "TW_DATABASE_URL": url},
        check=True,
        capture_output=True,
    )
    try:
        yield url
    finally:
        admin = await asyncpg.connect(_plain_dsn(dev_url))
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        finally:
            await admin.close()
