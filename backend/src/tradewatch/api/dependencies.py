"""Shared FastAPI dependencies.

The important one is :func:`get_session`. A request gets one session, opened at the start
and closed at the end, and the route never sees the engine — so a handler cannot accidentally
leak a connection or run a query after its transaction has closed.

It is a dependency rather than a module-level object because that is what makes it
overridable. The end-to-end test replaces it with the session its fixtures already hold,
which is how the API can be asserted against data the test wrote but has not committed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated, cast

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.config.settings import Settings
from tradewatch.infrastructure.database.session import Database

__all__ = ["SessionDep", "SettingsDep", "get_database", "get_session", "get_settings_dep"]


def get_database(request: Request) -> Database:
    """The process-wide database, attached to the app during startup."""
    database = getattr(request.app.state, "database", None)
    if database is None:
        raise RuntimeError("the database was not attached during application startup")
    return cast("Database", database)


def get_settings_dep(request: Request) -> Settings:
    """The settings this application was built with, not the process-wide singleton.

    Routes read configuration from here so that a test can build an app with its own
    settings and have the routes honour them.
    """
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("settings were not attached to the application")
    return cast("Settings", settings)


async def get_session(
    database: Annotated[Database, Depends(get_database)],
) -> AsyncIterator[AsyncSession]:
    """One session per request, committed on success and rolled back on any exception."""
    async with database.session() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
SettingsDep = Annotated[Settings, Depends(get_settings_dep)]
