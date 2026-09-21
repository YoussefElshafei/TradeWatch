"""Repository foundations.

Two ideas live here.

**Repositories take a session, they do not own one.** A repository issues statements; the
caller decides where the transaction begins and ends. That is what lets one use case write
an order and its fill atomically through two repositories, which would be impossible if each
committed on its own.

**Ownership is enforced here, not in route handlers.** `ARCHITECTURE.md` §9.3 is explicit
about this, and the reason is that IDOR — fetching a resource by id and forgetting to check
who owns it — is the most likely real vulnerability in an application of this shape. A route
that forgets the check is a mistake waiting to happen; a repository that *cannot* return
another user's rows is a mistake that cannot happen. :class:`OwnedRepository` takes an actor
on every method and filters by it, with no unfiltered escape hatch.

A missing row and a row belonging to somebody else are deliberately indistinguishable: both
return ``None``, so the API answers 404 either way and the existence of another user's
resource cannot be probed.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any, cast

from sqlalchemy import CursorResult, Result, Select, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute

from tradewatch.infrastructure.database.base import Base


def rowcount(result: Result[Any]) -> int:
    """Rows affected by a DML statement.

    ``AsyncSession.execute`` is typed as returning ``Result``, which has no ``rowcount`` —
    but every INSERT, UPDATE and DELETE actually returns a ``CursorResult``, which does.
    One cast here beats one at every call site.
    """
    return cast("CursorResult[Any]", result).rowcount


class Repository[ModelT: DeclarativeBase]:
    """Base for repositories over a single table."""

    model: type[ModelT]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def column(self, name: str) -> InstrumentedAttribute[Any]:
        """Look up a mapped column by name.

        Needed because ``ModelT`` is only bound to the declarative base, which does not
        declare ``id`` or ``created_at`` — those come from mixins that a generic parameter
        cannot express. Resolution still fails loudly at first use rather than silently
        returning nothing.
        """
        attribute = getattr(self.model, name, None)
        if attribute is None:
            raise AttributeError(f"{self.model.__name__} has no mapped column {name!r}")
        return cast("InstrumentedAttribute[Any]", attribute)

    async def get(self, entity_id: uuid.UUID) -> ModelT | None:
        """Fetch by primary key. Not owner-scoped — see :class:`OwnedRepository`."""
        return await self.session.get(self.model, entity_id)

    async def add(self, entity: ModelT) -> ModelT:
        """Stage an insert and flush it, so server-side defaults are populated.

        Flush, not commit: the caller still owns the transaction.
        """
        self.session.add(entity)
        await self.session.flush()
        return entity

    async def add_all(self, entities: Sequence[ModelT]) -> Sequence[ModelT]:
        if not entities:
            return []
        self.session.add_all(list(entities))
        await self.session.flush()
        return entities

    async def delete(self, entity: ModelT) -> None:
        await self.session.delete(entity)
        await self.session.flush()

    async def count(self) -> int:
        result = await self.session.execute(select(func.count()).select_from(self.model))
        return result.scalar_one()


class OwnedRepository[ModelT: Base](Repository[ModelT]):
    """A repository for rows belonging to a user.

    Every method takes ``actor``, the id of the user making the request, and every query is
    filtered by it. There is no method that returns a row without that filter — adding one
    would defeat the point.
    """

    #: The column that identifies the owner. Overridden where it is not ``user_id``.
    owner_column: str = "user_id"

    def _owned(self, actor: uuid.UUID) -> Select[tuple[ModelT]]:
        return select(self.model).where(self.column(self.owner_column) == actor)

    async def get_for(self, entity_id: uuid.UUID, actor: uuid.UUID) -> ModelT | None:
        """Fetch one row the actor owns.

        Returns None both when the row does not exist and when it belongs to somebody
        else. The caller cannot tell the difference, which is the point: a 404 that differs
        from a 403 tells an attacker which ids are real.
        """
        result = await self.session.execute(
            self._owned(actor).where(self.column("id") == entity_id)
        )
        return result.scalar_one_or_none()

    async def list_for(
        self,
        actor: uuid.UUID,
        *,
        limit: int = 100,
        offset: int = 0,
        **filters: Any,
    ) -> Sequence[ModelT]:
        """List the actor's rows, newest first where the model has a creation time."""
        statement = self._owned(actor)
        for column_name, value in filters.items():
            statement = statement.where(self.column(column_name) == value)
        if hasattr(self.model, "created_at"):
            statement = statement.order_by(self.column("created_at").desc())
        result = await self.session.execute(statement.limit(limit).offset(offset))
        return result.scalars().all()

    async def count_for(self, actor: uuid.UUID, **filters: Any) -> int:
        """Count the actor's rows. Used for the per-user resource limits in §9.3."""
        statement = (
            select(func.count())
            .select_from(self.model)
            .where(self.column(self.owner_column) == actor)
        )
        for column_name, value in filters.items():
            statement = statement.where(self.column(column_name) == value)
        result = await self.session.execute(statement)
        return result.scalar_one()

    async def delete_for(self, entity_id: uuid.UUID, actor: uuid.UUID) -> bool:
        """Delete one row the actor owns.

        Returns True when a row was deleted, False when there was nothing to delete —
        again without distinguishing "absent" from "not yours".
        """
        owner = self.column(self.owner_column)
        result = await self.session.execute(
            delete(self.model).where(self.column("id") == entity_id, owner == actor)
        )
        await self.session.flush()
        return rowcount(result) > 0
