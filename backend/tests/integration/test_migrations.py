"""Migrations.

Three questions, each of which has bitten a real project:

1. Does the schema build from **nothing**? A migration chain that only works on a database
   that already has last month's tables is not reproducible.
2. Does it **reverse**? `DEVELOPMENT_PLAN.md`'s Definition of Done requires `upgrade →
   downgrade → upgrade`, and the usual failure is a downgrade that drops tables but leaves
   enum types behind, so the second upgrade dies on "type already exists".
3. Do the migrations **agree with the models**? This is the one that matters most. Models
   and migrations drift silently — someone adds a column, forgets to generate a migration,
   and tests pass because the test schema was built with `create_all`. Asking autogenerate
   whether it still has anything to say is the check that catches it.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.conftest import BACKEND_ROOT, _run_migrations
from tradewatch.infrastructure.database.models import metadata

pytestmark = pytest.mark.integration

#: Every table ARCHITECTURE.md section 6 calls for.
EXPECTED_TABLES = {
    # Reference and market data (§6.1)
    "instruments",
    "trades",
    "quotes",
    "bars",
    # Trading (§6.2)
    "users",
    "refresh_tokens",
    "strategies",
    "portfolios",
    "strategy_deployments",
    "orders",
    "fills",
    "positions",
    "equity_points",
    "backtest_runs",
    "backtest_trades",
    "backtest_equity_points",
    # Streaming bookkeeping. Not in §6 because it is not domain data: it is how an
    # accumulating consumer records what it has already applied (ADR-0016).
    "consumer_progress",
    # Detection and alerting (§6.3)
    "anomaly_detections",
    "model_versions",
    "alert_rules",
    "alerts",
    "audit_log",
}

EXPECTED_ENUM_TYPES = {
    "user_role",
    "user_status",
    "asset_class",
    "instrument_status",
    "bar_interval",
    "signal_direction",
    "order_side",
    "order_type",
    "order_status",
    "deployment_status",
    "backtest_status",
    "alert_severity",
}


def _alembic_config(connection: Connection) -> Config:
    config = Config(str(BACKEND_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(BACKEND_ROOT / "alembic"))
    config.attributes["connection"] = connection
    return config


class TestSchemaIsComplete:
    """The migrated schema contains what the architecture says it should."""

    async def test_every_expected_table_exists(self, engine: AsyncEngine) -> None:
        async with engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))

        missing = EXPECTED_TABLES - tables
        assert not missing, f"missing tables: {sorted(missing)}"

    async def test_no_unexpected_tables(self, engine: AsyncEngine) -> None:
        """A table nobody designed is either dead weight or a forgotten rename."""
        async with engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))

        unexpected = tables - EXPECTED_TABLES - {"alembic_version"}
        assert not unexpected, f"unexpected tables: {sorted(unexpected)}"

    async def test_enum_types_exist_with_their_values(self, engine: AsyncEngine) -> None:
        async with engine.connect() as connection:
            result = await connection.execute(
                text("SELECT typname FROM pg_type WHERE typtype = 'e'")
            )
            types = {row[0] for row in result}

        assert types >= EXPECTED_ENUM_TYPES

    async def test_bar_interval_stores_values_not_member_names(self, engine: AsyncEngine) -> None:
        """The values_callable in `database/types.py` is easy to omit and invisible without this.

        Without it PostgreSQL would hold 'ONE_MINUTE' where every other part of the system
        — config, wire format, logs — says '1m'.
        """
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT e.enumlabel FROM pg_enum e "
                    "JOIN pg_type t ON t.oid = e.enumtypid "
                    "WHERE t.typname = 'bar_interval' ORDER BY e.enumsortorder"
                )
            )
            labels = [row[0] for row in result]

        assert labels == ["1m", "5m", "1h", "1d"]

    async def test_alembic_is_at_head(self, engine: AsyncEngine) -> None:
        scripts = ScriptDirectory(str(BACKEND_ROOT / "alembic"))
        async with engine.connect() as connection:
            current = await connection.run_sync(
                lambda sync: MigrationContext.configure(sync).get_current_revision()
            )

        assert current == scripts.get_current_head()


class TestMigrationsMatchModels:
    """The check that stops silent drift."""

    async def test_autogenerate_detects_no_difference(self, engine: AsyncEngine) -> None:
        """A migrated database and the ORM models must describe the same schema.

        If this fails, someone changed a model without generating a migration (or edited a
        migration without updating the model). Run:

            uv run alembic revision --autogenerate -m "describe the change"
        """

        def _diff(sync_connection: Connection) -> list[tuple[object, ...]]:
            context = MigrationContext.configure(
                sync_connection,
                opts={
                    "compare_type": True,
                    "compare_server_default": True,
                    "include_object": lambda obj, name, type_, reflected, compare_to: (
                        not (type_ == "table" and name == "alembic_version")
                    ),
                },
            )
            return list(compare_metadata(context, metadata))

        async with engine.connect() as connection:
            differences = await connection.run_sync(_diff)

        assert not differences, "The models and the migrations disagree:\n" + "\n".join(
            f"  {difference}" for difference in differences
        )


class TestReversibility:
    """`upgrade → downgrade → upgrade` on a scratch database."""

    async def test_downgrade_then_upgrade_rebuilds_the_schema(self, engine: AsyncEngine) -> None:
        """The whole chain reverses and reapplies.

        Runs on its own database so a failure halfway cannot leave the shared test schema
        unusable for every other test in the session.
        """
        # `str(url)` masks the password as `***`, so reconstructing a URL from it produces
        # one that authenticates as the literal string "***". That works against a
        # trust-auth PostgreSQL and fails against every other kind, which is a bad way to
        # find out.
        base = engine.url.render_as_string(hide_password=False).rsplit("/", 1)[0]
        scratch_url = f"{base}/tradewatch_migration_check"
        admin_url = f"{base}/postgres"

        from sqlalchemy.ext.asyncio import create_async_engine

        admin = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            async with admin.connect() as connection:
                await connection.execute(
                    text("DROP DATABASE IF EXISTS tradewatch_migration_check WITH (FORCE)")
                )
                await connection.execute(text("CREATE DATABASE tradewatch_migration_check"))
        finally:
            await admin.dispose()

        scratch = create_async_engine(scratch_url)
        try:
            # Up from empty.
            async with scratch.begin() as connection:
                await connection.run_sync(_run_migrations)
            async with scratch.connect() as connection:
                tables = await connection.run_sync(
                    lambda sync: set(inspect(sync).get_table_names())
                )
            assert tables >= EXPECTED_TABLES

            # All the way back down.
            async with scratch.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(_alembic_config(sync), "base")
                )
            async with scratch.connect() as connection:
                tables = await connection.run_sync(
                    lambda sync: set(inspect(sync).get_table_names())
                )
                enum_result = await connection.execute(
                    text("SELECT typname FROM pg_type WHERE typtype = 'e'")
                )
                remaining_types = {row[0] for row in enum_result}

            assert tables <= {"alembic_version"}, f"downgrade left tables: {sorted(tables)}"
            # The failure this test exists for: enum types are not dropped by DROP TABLE,
            # so a downgrade that forgets them makes the next upgrade fail.
            assert not (EXPECTED_ENUM_TYPES & remaining_types), (
                f"downgrade left enum types behind: {sorted(EXPECTED_ENUM_TYPES & remaining_types)}"
            )

            # And up again, which is only possible if the downgrade was complete.
            async with scratch.begin() as connection:
                await connection.run_sync(_run_migrations)
            async with scratch.connect() as connection:
                tables = await connection.run_sync(
                    lambda sync: set(inspect(sync).get_table_names())
                )
            assert tables >= EXPECTED_TABLES
        finally:
            await scratch.dispose()
            admin = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
            try:
                async with admin.connect() as connection:
                    await connection.execute(
                        text("DROP DATABASE IF EXISTS tradewatch_migration_check WITH (FORCE)")
                    )
            finally:
                await admin.dispose()


class TestMigrationHygiene:
    """Rules from `alembic/README.md`, enforced rather than hoped for."""

    def test_every_migration_has_a_real_downgrade(self) -> None:
        """A downgrade body of `pass` is a migration that cannot be rolled back."""
        versions = sorted((BACKEND_ROOT / "alembic" / "versions").glob("*.py"))
        assert versions, "no migrations found"

        for path in versions:
            source = path.read_text(encoding="utf-8")
            body = source.split("def downgrade()", 1)[1]
            statements = [
                line.strip()
                for line in body.splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            meaningful = [line for line in statements if line not in {"pass", "-> None:"}]
            assert meaningful, f"{path.name} has an empty downgrade()"

    def test_there_is_a_single_head(self) -> None:
        """Two heads means someone branched the history and nobody merged it."""
        scripts = ScriptDirectory(str(BACKEND_ROOT / "alembic"))

        assert len(scripts.get_heads()) == 1, "multiple migration heads; run `alembic merge`"

    def test_migrations_do_not_import_application_code(self) -> None:
        """A migration must still run when the model it came from has changed.

        Importing `tradewatch.*` ties a migration to today's code, and it stops running the
        day that code moves.
        """
        versions = sorted((BACKEND_ROOT / "alembic" / "versions").glob("*.py"))
        offenders = [
            path.name
            for path in versions
            if "from tradewatch" in path.read_text(encoding="utf-8")
            or "import tradewatch" in path.read_text(encoding="utf-8")
        ]

        assert not offenders, f"migrations importing application code: {offenders}"


def test_alembic_ini_does_not_contain_a_url() -> None:
    """A connection string in a committed file is a credential in a committed file."""
    content = (BACKEND_ROOT / "alembic.ini").read_text(encoding="utf-8")
    configured = [
        line
        for line in content.splitlines()
        if line.strip().startswith("sqlalchemy.url") and not line.strip().startswith("#")
    ]

    assert not configured, "alembic.ini sets sqlalchemy.url; it must come from TW_DATABASE_URL"


def test_versions_directory_is_tracked() -> None:
    assert (Path(BACKEND_ROOT) / "alembic" / "versions").is_dir()
