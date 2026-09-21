"""consumer progress for idempotent bar aggregation

Closes a data-correctness bug in the bar aggregator (ADR-0016, superseding ADR-0015).

Writing a trade is idempotent because it is a replacement guarded by `UNIQUE (event_id)`.
Folding a trade into a bar is an *addition*, and adding the same trade twice adds its volume
twice — so at-least-once delivery silently doubled `volume`, `trade_count` and VWAP on every
redelivery. Proven by rewinding the consumer group's offsets: every additive field came back
exactly 2x while OHLC, being first/max/min/last, looked perfectly normal.

This table lets the aggregator record its own position transactionally with the bars it
wrote, so an event at or below that position is known to be already folded and is skipped.

Revision ID: 3a55071f77d3
Revises: 61421a4db42f
Created: 2026-09-21 15:50:08.485380+00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "3a55071f77d3"
down_revision: str | None = "61421a4db42f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "consumer_progress",
        sa.Column("consumer_group", sa.String(length=100), nullable=False),
        sa.Column("stream", sa.String(length=200), nullable=False),
        sa.Column("position", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "position >= 0", name=op.f("ck_consumer_progress_position_not_negative")
        ),
        sa.PrimaryKeyConstraint("consumer_group", "stream", name=op.f("pk_consumer_progress")),
    )


def downgrade() -> None:
    op.drop_table("consumer_progress")
