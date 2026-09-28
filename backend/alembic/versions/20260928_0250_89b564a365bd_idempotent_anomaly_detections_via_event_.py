"""idempotent anomaly detections via event_id

`anomaly_detections` was created in the initial schema with no natural-key or event-id
column, unlike every other table this pipeline writes to at least once (`ARCHITECTURE.md`
§7.4: "every consumer is idempotent"). Phase 8 is the first phase to actually write to this
table, and a Kafka redelivery of `anomaly.detected` must not create a second row for the
same (symbol, model, window) — so this adds the same `UNIQUE (event_id)` guard `trades`/
`quotes` already use, built the same way (ADR-0012: a deterministic UUIDv5 over the natural
key), so `INSERT ... ON CONFLICT (event_id) DO NOTHING` absorbs a redelivery for free.

The table has no producer yet, so it is empty in every environment this migration will ever
run against — the column is added `NOT NULL` directly rather than nullable-then-backfilled.

Revision ID: 89b564a365bd
Revises: 3a55071f77d3
Created: 2026-09-28 02:50:19.539860+00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "89b564a365bd"
down_revision: str | None = "3a55071f77d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("anomaly_detections", sa.Column("event_id", sa.UUID(), nullable=False))
    op.create_unique_constraint(
        op.f("uq_anomaly_detections_event_id"), "anomaly_detections", ["event_id"]
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("uq_anomaly_detections_event_id"), "anomaly_detections", type_="unique"
    )
    op.drop_column("anomaly_detections", "event_id")
