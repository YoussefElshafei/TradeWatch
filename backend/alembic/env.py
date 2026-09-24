"""Alembic environment.

The database URL comes from `TW_DATABASE_URL` through the typed settings object, never from
`alembic.ini`. That means one source of truth for where the database is, and no credential
in a committed file.

The engine is async, matching the application's, so migrations run against the same driver
the application uses rather than a second sync one that might behave differently.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import Connection, Enum, pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from tradewatch.config.env import load_env_file
from tradewatch.config.settings import DatabaseSettings

# Importing the models package registers every table on the shared metadata. Without this,
# autogenerate sees an empty schema and cheerfully writes a migration dropping everything.
from tradewatch.infrastructure.database.models import metadata as target_metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

load_env_file()

# Skip reading TW_DATABASE_URL when a connection is already provided (the test path, set
# in tests/integration/conftest.py before invoking Alembic against an existing event loop
# and transaction). That path never reads `sqlalchemy.url`, but building `DatabaseSettings`
# here unconditionally still required TW_DATABASE_URL to be set — which is only true by
# accident on a machine with a `.env` file. CI's integration job sets TW_TEST_DATABASE_URL,
# not TW_DATABASE_URL, so this raised a raw, uncaught ValidationError before any test ran.
if config.attributes.get("connection") is None:
    config.set_main_option("sqlalchemy.url", DatabaseSettings().database_url)


def include_object(
    obj: Any,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: Any,
) -> bool:
    """Decide whether autogenerate should consider a database object.

    Alembic's own version table is not part of the application schema and must never appear
    in a migration.
    """
    return not (type_ == "table" and name == "alembic_version")


def render_item(type_: str, obj: Any, autogen_context: Any) -> str | bool:
    """Render a native enum as a PostgreSQL ENUM that does not create its own type.

    Autogenerate renders `sa.Enum` in a way that depends on how the type was constructed,
    and an enum built by `tradewatch.infrastructure.database.types.pg_enum` renders as an
    expression the migration cannot even import. Rendering it explicitly as
    ``postgresql.ENUM(..., create_type=False)`` makes migrations say what they mean: the
    column uses this type, and creating the type is a separate, visible step.
    """
    if type_ == "type" and isinstance(obj, Enum) and obj.native_enum:
        autogen_context.imports.add("from sqlalchemy.dialects import postgresql")
        values = ", ".join(repr(value) for value in obj.enums)
        return f"postgresql.ENUM({values}, name={obj.name!r}, create_type=False)"
    return False


def _configure(connection: Connection | None = None, *, url: str | None = None) -> None:
    context.configure(
        connection=connection,
        url=url,
        target_metadata=target_metadata,
        include_object=include_object,
        render_item=render_item,
        # Without this, a column changing from VARCHAR(50) to VARCHAR(100) produces no
        # migration and the mismatch is discovered by a truncated value in production.
        compare_type=True,
        compare_server_default=True,
        # Render constraint and index names using the naming convention from
        # `infrastructure.database.base`, so generated migrations match what the models
        # actually create.
        render_as_batch=False,
        literal_binds=url is not None,
        dialect_opts={"paramstyle": "named"},
    )


def run_migrations_offline() -> None:
    """Emit SQL to stdout instead of running it.

    `alembic upgrade head --sql` produces a script for a DBA to review, which is how a
    migration reaches an environment where the application has no DDL privileges.
    """
    _configure(url=config.get_main_option("sqlalchemy.url"))
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    _configure(connection)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations against a live database.

    When Alembic is invoked from inside an already-running event loop — which is what the
    migration tests do — a nested `asyncio.run` would fail, so the caller passes its own
    connection through the config instead.
    """
    connection = config.attributes.get("connection")
    if connection is not None:
        do_run_migrations(connection)
        return
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
