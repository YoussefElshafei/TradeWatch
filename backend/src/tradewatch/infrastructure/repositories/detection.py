"""Repositories for anomaly detections and model versions (ADR-0008).

Neither table is owner-scoped: an anomaly detection describes the market, the same way a
trade or a bar does (see `market.py`'s docstring for the identical reasoning), and a model
version is an operational artifact, not anyone's data.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tradewatch.infrastructure.database.models import AnomalyDetection, ModelVersion
from tradewatch.infrastructure.repositories.base import Repository

__all__ = ["AnomalyDetectionRepository", "ModelVersionRepository"]


class AnomalyDetectionRepository(Repository[AnomalyDetection]):
    model = AnomalyDetection

    async def upsert(self, detection: AnomalyDetection) -> None:
        """Idempotent on `event_id` (ADR-0012): a Kafka redelivery of the same detection
        must not create a second row.
        """
        statement = pg_insert(AnomalyDetection).values(
            id=detection.id,
            event_id=detection.event_id,
            instrument_id=detection.instrument_id,
            detected_at=detection.detected_at,
            window_start=detection.window_start,
            window_end=detection.window_end,
            anomaly_score=detection.anomaly_score,
            threshold=detection.threshold,
            severity=detection.severity,
            model_name=detection.model_name,
            model_version=detection.model_version,
            features=detection.features,
        )
        statement = statement.on_conflict_do_nothing(index_elements=[AnomalyDetection.event_id])
        await self.session.execute(statement)
        await self.session.flush()

    async def list_recent(
        self,
        *,
        instrument_id: uuid.UUID | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[AnomalyDetection]:
        """The global feed (all symbols) or one symbol's, newest first.

        `instrument_id=None` is the cross-symbol "what was unusual this morning" query —
        `ix_anomaly_detections_detected_at` exists specifically to serve it.
        """
        statement = select(AnomalyDetection).order_by(AnomalyDetection.detected_at.desc())
        if instrument_id is not None:
            statement = statement.where(AnomalyDetection.instrument_id == instrument_id)
        result = await self.session.execute(statement.limit(limit).offset(offset))
        return result.scalars().all()


class ModelVersionRepository(Repository[ModelVersion]):
    model = ModelVersion

    async def get_active(self, model_name: str) -> ModelVersion | None:
        """The one version currently in service for `model_name`, or `None` if every
        version has been deactivated (F17: the detection service falls back to the
        baseline in that case, same as a missing artifact).
        """
        result = await self.session.execute(
            select(ModelVersion).where(
                ModelVersion.model_name == model_name, ModelVersion.is_active.is_(True)
            )
        )
        return result.scalar_one_or_none()

    async def activate(self, version: ModelVersion) -> None:
        """Make `version` the active one for its `model_name`, deactivating whichever
        version held that spot.

        Two statements, not an upsert: the partial unique index
        (`uq_model_versions_one_active_per_model`) only ever allows one `is_active=true` row
        per model, so the old row must be turned off before the new one can be turned on —
        doing it the other way round would violate the index for the instant between the two.
        """
        result = await self.session.execute(
            select(ModelVersion).where(
                ModelVersion.model_name == version.model_name, ModelVersion.is_active.is_(True)
            )
        )
        for previous in result.scalars().all():
            previous.is_active = False
        await self.session.flush()
        version.is_active = True
        await self.session.flush()

    async def list_for_model(self, model_name: str, *, limit: int = 50) -> Sequence[ModelVersion]:
        result = await self.session.execute(
            select(ModelVersion)
            .where(ModelVersion.model_name == model_name)
            .order_by(ModelVersion.trained_at.desc())
            .limit(limit)
        )
        return result.scalars().all()
