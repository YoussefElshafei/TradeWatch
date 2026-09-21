"""Where a consumer has got to, recorded next to what it produced.

One table, and it exists to close a specific hole: **an accumulator is not idempotent**.

`UNIQUE (event_id)` makes writing a trade idempotent, because writing a trade is a
*replacement*. Folding a trade into a bar is an *addition*, and adding the same trade twice
adds its volume twice. At-least-once delivery therefore corrupts any consumer whose output
accumulates, and Kafka's delivery guarantee is at-least-once by design (`ARCHITECTURE.md`
§7.4).

The fix is the standard one for an accumulating sink: the consumer stores **its own
position** transactionally with **its own output**. Bars and progress are written in one
transaction, so they can never disagree; an event at or below the stored position has
provably already been folded, and is skipped.

That is why the position lives here rather than in Kafka's own offsets. Kafka's offsets are
committed *after* the write and can legitimately be rewound — by an operator, by a replay, by
a rebalance that redelivers an uncommitted batch. This row is the consumer's own record of
what it has actually applied, and nothing moves it backwards by accident.

Six rows per consumer group, one per partition. The alternative — recording which event ids
went into each bar — is exact too and costs one row per trade, which is the `trades` table
again. A position is the compact encoding of "every event up to here", and within a Kafka
partition that encoding is exact because delivery order is guaranteed.
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import BigInteger, CheckConstraint, DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from tradewatch.infrastructure.database.base import Base

__all__ = ["ConsumerProgress"]


class ConsumerProgress(Base):
    """How far one consumer group has applied one stream.

    Keyed by ``(consumer_group, stream)`` rather than given a surrogate id: the pair *is* the
    identity, and a surrogate would allow two rows to claim the same position for the same
    partition, which is the one thing this table must make impossible.
    """

    __tablename__ = "consumer_progress"

    #: The logical consumer, matching its Kafka group — `bar-aggregator` today.
    consumer_group: Mapped[str] = mapped_column(String(100), primary_key=True)
    #: The partition, as `topic/partition`. A string rather than two columns because the
    #: consumer treats it as an opaque identifier, and a non-Kafka transport would name its
    #: shards differently without that being a schema change.
    stream: Mapped[str] = mapped_column(String(200), primary_key=True)
    #: The offset of the newest event applied. Everything at or below it is already folded.
    position: Mapped[int] = mapped_column(BigInteger, nullable=False)

    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        # A negative position would mean "nothing applied", which is the absence of a row.
        CheckConstraint("position >= 0", name="position_not_negative"),
    )
