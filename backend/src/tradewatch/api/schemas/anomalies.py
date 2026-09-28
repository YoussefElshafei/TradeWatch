"""Response schemas for the anomaly feed (ADR-0008).

**Wording.** Every field name and every docstring in this module says *anomaly*, and that is
the only claim made about what a row means (see ADR-0008 for the full reasoning). `severity`
describes how far the score sits from the threshold — a statement about the number, not
about how serious anything is.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

from pydantic import BaseModel

from tradewatch.domain.enums import AlertSeverity

__all__ = ["AnomalyDetectionListResponse", "AnomalyDetectionResponse"]


class AnomalyDetectionResponse(BaseModel):
    """One scored window of a symbol's activity.

    `features` is exactly what `application.detection.features` computed for this window —
    stored as strings, matching how they are persisted, so a client can show the caveat
    ADR-0008 requires ("statistically unusual activity has many ordinary explanations")
    without losing precision by round-tripping through a JSON number.
    """

    id: uuid.UUID
    symbol: str
    detected_at: dt.datetime
    window_start: dt.datetime
    window_end: dt.datetime
    anomaly_score: Decimal
    threshold: Decimal
    severity: AlertSeverity
    model_name: str
    model_version: str
    features: dict[str, str]


class AnomalyDetectionListResponse(BaseModel):
    count: int
    detections: list[AnomalyDetectionResponse]
