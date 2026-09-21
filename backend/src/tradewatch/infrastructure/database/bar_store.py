"""PostgreSQL implementation of the bar storage port.

Thin, like its sibling :mod:`~tradewatch.infrastructure.database.market_store`. The
repositories already know how to write bars idempotently and how to roll them up in SQL, so
this adapter's job is to own the **transaction boundary**, map ORM rows to domain entities,
and translate SQLAlchemy's exceptions into the two the application understands.

The exception translation is deliberately *not* reimplemented here. It is imported from
`market_store`, because two copies of "which database errors are worth retrying" is two
chances to disagree, and the disagreement would show up as an outage the aggregator treats
as a poison batch — or a poison batch it retries forever.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from decimal import Decimal
from typing import Final

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tradewatch.domain.entities import Bar as BarEntity
from tradewatch.domain.entities import NewBar
from tradewatch.domain.enums import AssetClass, BarInterval
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.market_store import (
    _DRIVER_ERRORS,
    _connection_failure,
    _translate,
)
from tradewatch.infrastructure.database.models import Bar, ConsumerProgress, Instrument
from tradewatch.infrastructure.database.session import session_scope
from tradewatch.infrastructure.repositories import BarRepository, InstrumentRepository
from tradewatch.observability.logging import get_logger

__all__ = ["SqlBarStore"]

log = get_logger(__name__)

_PROVISIONAL_EXCHANGE: Final = "UNKNOWN"
_PROVISIONAL_TICK_SIZE: Final = Decimal("0.01")
_PROVISIONAL_LOT_SIZE: Final = Decimal("1")


def _to_entity(row: Bar) -> BarEntity:
    """ORM row to domain entity.

    Explicit rather than clever: an automatic mapping would silently start carrying any
    column added later, and the domain entity is a contract rather than a mirror of the
    table.
    """
    return BarEntity(
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


class SqlBarStore:
    """Reads and writes bars in PostgreSQL.

    Implements :class:`~tradewatch.application.ports.bar_store.BarStore`.

    Takes a session *factory*, not a session: each call opens and closes its own
    transaction, which is what lets one write fail without poisoning the next.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    # --- reference data ----------------------------------------------------

    async def resolve_instrument(self, symbol: str) -> uuid.UUID:
        """Find the instrument, registering a provisional row if it is unknown."""
        wanted = symbol.strip().upper()
        return await self._run(
            f"resolving instrument {wanted}", lambda session: self._resolve(session, wanted)
        )

    @staticmethod
    async def _resolve(session: AsyncSession, wanted: str) -> uuid.UUID:
        repository = InstrumentRepository(session)
        existing = await repository.get_by_symbol(wanted)
        if existing is not None:
            return existing.id

        created = await repository.upsert(
            Instrument(
                id=new_id(),
                symbol=wanted,
                asset_class=AssetClass.EQUITY,
                exchange=_PROVISIONAL_EXCHANGE,
                currency="USD",
                tick_size=_PROVISIONAL_TICK_SIZE,
                lot_size=_PROVISIONAL_LOT_SIZE,
                provider_metadata={"provisional": True},
            )
        )
        log.info("bar_store.instrument_registered", symbol=wanted, provisional=True)
        return created.id

    # --- writing -----------------------------------------------------------

    async def save_bars(self, bars: Sequence[NewBar]) -> int:
        if not bars:
            return 0
        return await self._run(
            f"writing {len(bars)} bars",
            lambda session: BarRepository(session).upsert_many(bars),
        )

    async def save_bars_with_progress(
        self,
        bars: Sequence[NewBar],
        *,
        consumer_group: str,
        positions: Mapping[str, int],
    ) -> int:
        """Write bars and the consumer's position in a single transaction.

        The atomicity is the whole point, and it is why this is one method rather than two
        calls the caller sequences. Written separately, a crash between them leaves either a
        bar counting an event the progress says was never applied — which double counts on
        redelivery — or a progress claiming an event that never reached a bar, which is a
        hole. One transaction admits neither.

        The progress upsert takes the **greater** of the stored and incoming positions.
        A rebalance can hand the same partition back after another member advanced it, and
        moving a position backwards would re-admit events that are already folded.
        """
        if not bars and not positions:
            return 0

        async def work(session: AsyncSession) -> int:
            written = await BarRepository(session).upsert_many(bars) if bars else 0
            if positions:
                statement = pg_insert(ConsumerProgress).values(
                    [
                        {
                            "consumer_group": consumer_group,
                            "stream": stream,
                            "position": offset,
                        }
                        for stream, offset in sorted(positions.items())
                    ]
                )
                statement = statement.on_conflict_do_update(
                    index_elements=[
                        ConsumerProgress.consumer_group,
                        ConsumerProgress.stream,
                    ],
                    set_={
                        "position": func.greatest(
                            ConsumerProgress.position, statement.excluded.position
                        ),
                        "updated_at": func.now(),
                    },
                )
                await session.execute(statement)
            return written

        return await self._run(f"writing {len(bars)} bars and progress", work)

    async def load_progress(self, consumer_group: str) -> Mapping[str, int]:
        """How far this consumer group has applied each stream."""

        async def work(session: AsyncSession) -> Mapping[str, int]:
            result = await session.execute(
                select(ConsumerProgress.stream, ConsumerProgress.position).where(
                    ConsumerProgress.consumer_group == consumer_group
                )
            )
            return {row.stream: row.position for row in result.all()}

        return await self._run("loading consumer progress", work)

    async def roll_up(
        self,
        instrument_id: uuid.UUID,
        source: BarInterval,
        target: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> int:
        """Derive coarser bars from finer ones, in one transaction.

        The aggregation happens in SQL and the result is written straight back, so a year of
        minutes never enters Python. Re-running it is safe: the rows carry the same natural
        key and the same revision, so the upsert replaces them with identical values.
        """

        async def work(session: AsyncSession) -> int:
            repository = BarRepository(session)
            rolled = await repository.aggregate(instrument_id, source, target, start, end)
            if not rolled:
                return 0
            return await repository.upsert_many(rolled)

        return await self._run(f"rolling {source.value} up to {target.value}", work)

    # --- reading -----------------------------------------------------------

    async def load_bar(
        self, instrument_id: uuid.UUID, interval: BarInterval, bar_start: dt.datetime
    ) -> BarEntity | None:
        async def work(session: AsyncSession) -> BarEntity | None:
            row = await session.get(Bar, (instrument_id, interval, bar_start))
            return _to_entity(row) if row is not None else None

        return await self._run("loading one bar", work)

    async def load_open_bars(
        self, interval: BarInterval, *, symbols: Sequence[str] | None = None
    ) -> Sequence[tuple[str, BarEntity]]:
        """Every bar still marked in progress, with its symbol.

        Joined rather than looked up per row: the aggregator asks for this once at startup,
        and the join is what turns "N+1 queries at boot" into one.
        """

        async def work(session: AsyncSession) -> Sequence[tuple[str, BarEntity]]:
            statement = (
                select(Instrument.symbol, Bar)
                .join(Instrument, Bar.instrument_id == Instrument.id)
                .where(Bar.interval == interval, Bar.is_final.is_(False))
                .order_by(Bar.bar_start)
            )
            if symbols:
                statement = statement.where(
                    Instrument.symbol.in_([symbol.upper() for symbol in symbols])
                )
            result = await session.execute(statement)
            return [(symbol, _to_entity(row)) for symbol, row in result.all()]

        return await self._run("loading open bars", work)

    async def latest_bar_start(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        *,
        within: tuple[dt.datetime, dt.datetime] | None = None,
    ) -> dt.datetime | None:
        async def work(session: AsyncSession) -> dt.datetime | None:
            statement = select(func.max(Bar.bar_start)).where(
                Bar.instrument_id == instrument_id, Bar.interval == interval
            )
            if within is not None:
                start, end = within
                statement = statement.where(Bar.bar_start >= start, Bar.bar_start < end)
            found: dt.datetime | None = await session.scalar(statement)
            return found

        return await self._run("finding the latest bar", work)

    async def earliest_bar_start(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        *,
        within: tuple[dt.datetime, dt.datetime] | None = None,
    ) -> dt.datetime | None:
        async def work(session: AsyncSession) -> dt.datetime | None:
            statement = select(func.min(Bar.bar_start)).where(
                Bar.instrument_id == instrument_id, Bar.interval == interval
            )
            if within is not None:
                start, end = within
                statement = statement.where(Bar.bar_start >= start, Bar.bar_start < end)
            found: dt.datetime | None = await session.scalar(statement)
            return found

        return await self._run("finding the earliest bar", work)

    async def count_bars(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> int:
        async def work(session: AsyncSession) -> int:
            found: int | None = await session.scalar(
                select(func.count())
                .select_from(Bar)
                .where(
                    Bar.instrument_id == instrument_id,
                    Bar.interval == interval,
                    Bar.bar_start >= start,
                    Bar.bar_start < end,
                )
            )
            return int(found or 0)

        return await self._run("counting bars", work)

    async def find_gaps(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Sequence[tuple[dt.datetime, dt.datetime]]:
        return await self._run(
            "finding gaps",
            lambda session: BarRepository(session).find_gaps(instrument_id, interval, start, end),
        )

    async def health_check(self) -> str | None:
        try:
            async with session_scope(self._session_factory) as session:
                await session.scalar(select(func.count()).select_from(Instrument))
        except SQLAlchemyError as error:
            return type(error).__name__
        except _DRIVER_ERRORS as error:
            return type(error).__name__
        return None

    # --- internals ---------------------------------------------------------

    async def _run[T](self, context: str, work: Callable[[AsyncSession], Awaitable[T]]) -> T:
        """One unit of work in one transaction, with every failure classified."""
        try:
            async with session_scope(self._session_factory) as session:
                return await work(session)
        except SQLAlchemyError as error:
            raise _translate(error, context) from error
        except _DRIVER_ERRORS as error:
            raise _connection_failure(error, context) from error
