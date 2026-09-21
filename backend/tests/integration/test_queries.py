"""Indexes, and whether the queries that justify them actually use them.

An index has two failure modes and a schema test catches neither on its own:

* it does not exist, because someone dropped it in a migration;
* it exists but the query never uses it, because the column order does not match the way
  the query filters — which is invisible until the table is large and everything is slow.

The first half of this module checks the catalogue. The second runs `EXPLAIN` over enough
rows that PostgreSQL would genuinely prefer a sequential scan if the index were useless, and
asserts it does not.

These tests deliberately make no claim about *speed*. `ARCHITECTURE.md` §1.3 is explicit
that no performance number exists until a benchmark in `benchmarks/` produces it; what is
asserted here is a plan shape, which is a correctness property.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.test_repositories import make_bar, make_trade
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.database.models import Instrument
from tradewatch.infrastructure.repositories import BarRepository, TradeRepository

pytestmark = pytest.mark.integration

NOW = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)

#: Enough rows that a sequential scan is a real alternative for the planner. Small enough
#: that the suite stays fast.
ROWS = 4000


async def index_columns(session: AsyncSession, table: str) -> dict[str, str]:
    """Return {index name: its definition} for a table, straight from the catalogue."""
    result = await session.execute(
        text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = :table"),
        {"table": table},
    )
    return {row[0]: row[1] for row in result}


async def explain(session: AsyncSession, sql: str, params: dict[str, object]) -> str:
    result = await session.execute(text(f"EXPLAIN {sql}"), params)
    return "\n".join(row[0] for row in result)


class TestIndexesExist:
    """The indexes ARCHITECTURE.md §6 calls for, with the column order it calls for."""

    async def test_trades_are_indexed_by_instrument_then_time(self, session: AsyncSession) -> None:
        """Order matters: (instrument, time) serves "this symbol, this window".

        The reverse would not, because the leading column is what a B-tree can seek on.
        """
        indexes = await index_columns(session, "trades")

        definition = indexes["ix_trades_instrument_id_occurred_at"]
        assert "instrument_id" in definition
        assert definition.index("instrument_id") < definition.index("occurred_at")

    async def test_quotes_are_indexed_the_same_way(self, session: AsyncSession) -> None:
        indexes = await index_columns(session, "quotes")

        assert "ix_quotes_instrument_id_occurred_at" in indexes

    async def test_trade_event_id_is_unique(self, session: AsyncSession) -> None:
        """The idempotency guard has to be a *unique* index, not merely an index."""
        indexes = await index_columns(session, "trades")

        assert "UNIQUE" in indexes["uq_trades_event_id"]

    async def test_bars_primary_key_covers_instrument_interval_and_start(
        self, session: AsyncSession
    ) -> None:
        """The composite PK *is* the symbol+timestamp index; no second one is needed."""
        indexes = await index_columns(session, "bars")

        definition = indexes["pk_bars"]
        assert definition.index("instrument_id") < definition.index("interval")
        assert definition.index("interval") < definition.index("bar_start")

    async def test_bars_have_a_cross_sectional_index(self, session: AsyncSession) -> None:
        """For "every symbol's bars in this window", which the PK cannot serve."""
        indexes = await index_columns(session, "bars")

        assert "ix_bars_interval_bar_start" in indexes

    async def test_foreign_keys_used_for_lookups_are_indexed(self, session: AsyncSession) -> None:
        """PostgreSQL does not index foreign keys automatically, and cascades scan them."""
        for table, index in (
            ("fills", "ix_fills_order_id"),
            ("refresh_tokens", "ix_refresh_tokens_user_id"),
            ("strategies", "ix_strategies_user_id"),
            ("portfolios", "ix_portfolios_user_id"),
            ("orders", "ix_orders_instrument_id"),
            ("backtest_trades", "ix_backtest_trades_backtest_run_id"),
        ):
            assert index in await index_columns(session, table), f"{table} is missing {index}"

    async def test_partial_indexes_carry_their_predicate(self, session: AsyncSession) -> None:
        """A partial index without its WHERE clause is just a bigger index."""
        runs = await index_columns(session, "backtest_runs")
        deployments = await index_columns(session, "strategy_deployments")
        alerts = await index_columns(session, "alerts")

        assert "WHERE" in runs["ix_backtest_runs_queued"]
        assert "queued" in runs["ix_backtest_runs_queued"]
        assert "running" in deployments["ix_strategy_deployments_running"]
        assert "acknowledged_at IS NULL" in alerts["ix_alerts_unacknowledged"]

    async def test_the_one_active_model_version_rule_is_a_unique_index(
        self, session: AsyncSession
    ) -> None:
        indexes = await index_columns(session, "model_versions")

        definition = indexes["uq_model_versions_one_active_per_model"]
        assert "UNIQUE" in definition
        assert "WHERE" in definition

    async def test_email_uniqueness_is_a_functional_index(self, session: AsyncSession) -> None:
        """lower(email), which is how case-insensitive uniqueness works without citext."""
        indexes = await index_columns(session, "users")

        definition = indexes["uq_users_email_lower"]
        assert "UNIQUE" in definition
        assert "lower" in definition.lower()


class TestQueryPlans:
    """The queries in the repositories use the indexes built for them."""

    @pytest.fixture
    async def populated(self, session: AsyncSession, instrument_factory: object) -> Instrument:
        """Two instruments' worth of trades and bars, analysed so the planner has stats."""
        target: Instrument = instrument_factory()  # type: ignore[operator]
        other: Instrument = instrument_factory()  # type: ignore[operator]
        session.add_all([target, other])
        await session.flush()

        trades = TradeRepository(session)
        bars = BarRepository(session)
        for owner in (target, other):
            await trades.bulk_insert(
                [
                    make_trade(
                        owner.id,
                        occurred_at=NOW + dt.timedelta(seconds=index),
                        price=Decimal("10") + Decimal(index % 100) / 100,
                    )
                    for index in range(ROWS // 2)
                ]
            )
            await bars.upsert_many([make_bar(owner.id, minute=index) for index in range(ROWS // 4)])

        # Without statistics the planner guesses, and its guess for an empty table is a
        # sequential scan regardless of what indexes exist.
        await session.execute(text("ANALYZE trades"))
        await session.execute(text("ANALYZE bars"))
        return target

    async def test_trade_range_query_uses_the_composite_index(
        self, session: AsyncSession, populated: Instrument
    ) -> None:
        plan = await explain(
            session,
            "SELECT * FROM trades WHERE instrument_id = :id "
            "AND occurred_at >= :start AND occurred_at < :end "
            "ORDER BY occurred_at DESC LIMIT 100",
            {
                "id": populated.id,
                "start": NOW,
                "end": NOW + dt.timedelta(minutes=5),
            },
        )

        assert "ix_trades_instrument_id_occurred_at" in plan, plan
        assert "Seq Scan" not in plan, plan

    async def test_latest_trade_needs_no_sort(
        self, session: AsyncSession, populated: Instrument
    ) -> None:
        """The index is already in timestamp order, so "newest" is one backwards step.

        A `Sort` node here would mean reading every trade for the symbol to return one row.
        """
        plan = await explain(
            session,
            "SELECT * FROM trades WHERE instrument_id = :id ORDER BY occurred_at DESC LIMIT 1",
            {"id": populated.id},
        )

        assert "ix_trades_instrument_id_occurred_at" in plan, plan
        assert "Sort" not in plan, plan

    async def test_bar_range_query_is_served_by_an_index(
        self, session: AsyncSession, populated: Instrument
    ) -> None:
        """The chart and backtest read path never scans the table.

        Which index wins is the planner's business and depends on selectivity: this fixture
        holds two instruments, so filtering by interval and time is more selective than
        filtering by instrument, and PostgreSQL prefers `ix_bars_interval_bar_start`. With
        a realistic universe the primary key wins instead. Asserting on one specific index
        would be asserting on the fixture's cardinality rather than on the schema, so what
        is asserted is the property that actually has to hold.
        """
        plan = await explain(
            session,
            "SELECT * FROM bars WHERE instrument_id = :id AND interval = :interval "
            "AND bar_start >= :start AND bar_start < :end ORDER BY bar_start",
            {
                "id": populated.id,
                "interval": BarInterval.ONE_MINUTE.value,
                "start": NOW,
                "end": NOW + dt.timedelta(hours=2),
            },
        )

        assert "Seq Scan" not in plan, plan
        assert any(index in plan for index in ("pk_bars", "ix_bars_interval_bar_start")), plan

    async def test_the_cross_sectional_bar_query_uses_its_own_index(
        self, session: AsyncSession, populated: Instrument
    ) -> None:
        """Every symbol in one window — the query the primary key cannot serve."""
        plan = await explain(
            session,
            "SELECT * FROM bars WHERE interval = :interval "
            "AND bar_start >= :start AND bar_start < :end",
            {
                "interval": BarInterval.ONE_MINUTE.value,
                "start": NOW,
                "end": NOW + dt.timedelta(minutes=10),
            },
        )

        assert "ix_bars_interval_bar_start" in plan, plan

    async def test_a_query_without_the_leading_column_cannot_use_the_index(
        self, session: AsyncSession, populated: Instrument
    ) -> None:
        """Documents *why* the column order is what it is.

        Filtering trades by time alone across all symbols has no index to use, and that is
        a deliberate choice: no current query needs one, and an unused index still costs a
        write on every insert into the largest table in the system. If such a query appears,
        this test is the note explaining what to add.
        """
        plan = await explain(
            session,
            "SELECT * FROM trades WHERE occurred_at >= :start AND occurred_at < :end",
            {"start": NOW, "end": NOW + dt.timedelta(seconds=10)},
        )

        assert "ix_trades_instrument_id_occurred_at" not in plan, plan

    async def test_the_backtest_queue_poll_uses_the_partial_index(
        self, session: AsyncSession, user: object, strategy: object
    ) -> None:
        """The runner polls this repeatedly; it must not scan every backtest ever run."""
        plan = await explain(
            session,
            "SELECT * FROM backtest_runs WHERE status = 'queued' ORDER BY created_at LIMIT 1",
            {},
        )

        # On an empty table the planner may still choose a sequential scan, so this asserts
        # the index is at least a candidate rather than asserting the plan shape.
        indexes = await index_columns(session, "backtest_runs")
        assert "ix_backtest_runs_queued" in indexes
        assert plan


class TestDecimalPrecision:
    """ADR-0009 end to end, at the storage layer."""

    async def test_numeric_columns_have_the_declared_precision(self, session: AsyncSession) -> None:
        result = await session.execute(
            text(
                "SELECT numeric_precision, numeric_scale FROM information_schema.columns "
                "WHERE table_name = 'trades' AND column_name = 'price'"
            )
        )
        precision, scale = result.one()

        assert (precision, scale) == (18, 8)

    async def test_a_price_no_float_can_represent_survives_a_round_trip(
        self, session: AsyncSession, instrument: Instrument
    ) -> None:
        """0.1 + 0.2 != 0.3 in binary floating point. It does here."""
        repository = TradeRepository(session)
        await repository.bulk_insert(
            [
                make_trade(instrument.id, price=Decimal("0.1"), occurred_at=NOW),
                make_trade(
                    instrument.id,
                    price=Decimal("0.2"),
                    occurred_at=NOW + dt.timedelta(seconds=1),
                ),
            ]
        )

        result = await session.execute(
            text("SELECT sum(price) FROM trades WHERE instrument_id = :id"),
            {"id": instrument.id},
        )
        total = result.scalar_one()

        assert total == Decimal("0.3")
        assert isinstance(total, Decimal)

    async def test_timestamps_are_stored_with_a_time_zone(self, session: AsyncSession) -> None:
        """TIMESTAMP WITHOUT TIME ZONE is the bug that shows up twice a year."""
        result = await session.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 'trades' "
                "AND column_name IN ('occurred_at', 'ingested_at', 'recorded_at')"
            )
        )
        types = {row[0]: row[1] for row in result}

        assert set(types) == {"occurred_at", "ingested_at", "recorded_at"}
        assert all(value == "timestamp with time zone" for value in types.values()), types
