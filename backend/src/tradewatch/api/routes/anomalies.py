"""The anomaly feed: `GET /api/v1/anomalies` (ADR-0008).

**Ownership decision, made explicit (this phase's task list asks for one):** anomaly
detections are **global, not owner-scoped**. There is no per-user watchlist in this system —
`TW_SYMBOLS` is a deployment-wide configuration, not a per-user resource — and a detection
describes the market's own behaviour, the same reasoning `market.py`'s routes already apply
to instruments and bars ("a trade print belongs to the market, not to a user"). Every symbol's
anomaly feed is visible to every authenticated user; there is no owner to check and no 404-
vs-403 question to answer, because nothing here is anyone's private data.

Unauthenticated, like every other route in `market.py` — this is a read of market data, not
of anything a user owns, and Phase 5's authentication boundary was never meant to gate that.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from tradewatch.api.dependencies import SessionDep
from tradewatch.api.schemas.anomalies import AnomalyDetectionListResponse, AnomalyDetectionResponse
from tradewatch.infrastructure.repositories import AnomalyDetectionRepository, InstrumentRepository

router = APIRouter(prefix="/api/v1/anomalies", tags=["anomalies"])


@router.get("", response_model=AnomalyDetectionListResponse)
async def list_anomalies(
    session: SessionDep,
    symbol: Annotated[str | None, Query(description="Filter to one symbol.")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> AnomalyDetectionListResponse:
    """The cross-symbol feed ("what was unusual this morning?"), or one symbol's history.

    Newest first. `symbol` is validated against `instruments` the same way `market.py`'s
    routes do: an unknown symbol is a 404, not an empty list, because the two mean different
    things — "this symbol does not exist" versus "this symbol exists and nothing was flagged."
    """
    instrument_repository = InstrumentRepository(session)
    instrument_id: uuid.UUID | None = None
    if symbol is not None:
        instrument = await instrument_repository.get_by_symbol(symbol)
        if instrument is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Unknown symbol {symbol!r}."
            )
        instrument_id = instrument.id

    rows = await AnomalyDetectionRepository(session).list_recent(
        instrument_id=instrument_id, limit=limit, offset=offset
    )

    # One lookup per row rather than a batched reverse-symbol query: a page is at most
    # `limit` (<=500) rows, and `InstrumentRepository` has no "many by id" method today —
    # see `backtests.py`'s identical reasoning for its trade listing.
    responses = []
    for row in rows:
        instrument = await instrument_repository.get(row.instrument_id)
        row_symbol = instrument.symbol if instrument is not None else str(row.instrument_id)
        responses.append(
            AnomalyDetectionResponse(
                id=row.id,
                symbol=row_symbol,
                detected_at=row.detected_at,
                window_start=row.window_start,
                window_end=row.window_end,
                anomaly_score=row.anomaly_score,
                threshold=row.threshold,
                severity=row.severity,
                model_name=row.model_name,
                model_version=row.model_version,
                features=row.features,
            )
        )
    return AnomalyDetectionListResponse(count=len(responses), detections=responses)
