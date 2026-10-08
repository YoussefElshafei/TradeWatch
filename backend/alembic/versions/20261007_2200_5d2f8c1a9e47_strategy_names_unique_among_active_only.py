"""strategy names unique among active strategies only

`uq_strategies_user_id_name` counted archived rows, so archiving "Strategy 1" and creating a
new "Strategy 1" returned 409 for a name the user could no longer see anywhere. Archiving is
the only delete the API offers, so the archived row keeps its name forever. A partial unique
index over `NOT is_archived` keeps the rule for live strategies and frees the name otherwise.

Downgrade restores the full constraint, which fails if an active and an archived strategy now
share a name — correct: that data cannot satisfy the old rule.

Revision ID: 5d2f8c1a9e47
Revises: 89b564a365bd
Created: 2026-10-07 22:00:00.000000+00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "5d2f8c1a9e47"
down_revision: str | None = "89b564a365bd"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("uq_strategies_user_id_name", "strategies", type_="unique")
    op.create_index(
        "uq_strategies_user_id_name_active",
        "strategies",
        ["user_id", "name"],
        unique=True,
        postgresql_where=sa.text("NOT is_archived"),
    )


def downgrade() -> None:
    op.drop_index("uq_strategies_user_id_name_active", table_name="strategies")
    op.create_unique_constraint("uq_strategies_user_id_name", "strategies", ["user_id", "name"])
