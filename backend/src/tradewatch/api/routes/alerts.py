"""Alert-rule CRUD and the alert feed (Phase 9, `ARCHITECTURE.md` §3.8/§9.3).

Every route requires a bearer token (`CurrentUserDep`) and every repository call goes
through `OwnedRepository`/`AlertRuleRepository`/`AlertRepository`, so a rule or alert that is
not the caller's own is a 404 — never a 403 — the same rule every other owner-scoped resource
follows since Phase 5 (`infrastructure/repositories/base.py`).

`workers/alert_dispatcher.py` is the only other reader/writer of `alert_rules` and `alerts`;
this file is the only place a user can create, change, or acknowledge them.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError

from tradewatch.api.dependencies import CurrentUserDep, SessionDep
from tradewatch.api.schemas.alerts import (
    AlertListResponse,
    AlertResponse,
    AlertRuleCreateRequest,
    AlertRuleListResponse,
    AlertRuleResponse,
    AlertRuleUpdateRequest,
)
from tradewatch.infrastructure.database.models import AlertRule
from tradewatch.infrastructure.repositories import (
    AlertRepository,
    AlertRuleRepository,
    InstrumentRepository,
)

router = APIRouter(tags=["alerts"])


def _conditions_for(min_anomaly_score: Any) -> dict[str, Any]:
    return {} if min_anomaly_score is None else {"min_anomaly_score": str(min_anomaly_score)}


@router.post(
    "/api/v1/alert-rules",
    response_model=AlertRuleResponse,
    status_code=status.HTTP_201_CREATED,
    responses={409: {"description": "A rule with this name already exists"}},
)
async def create_alert_rule(
    session: SessionDep, claims: CurrentUserDep, request: AlertRuleCreateRequest
) -> AlertRuleResponse:
    rule = AlertRule(
        user_id=claims.user_id,
        name=request.name,
        event_types=request.event_types,
        symbols=request.symbols,
        conditions=_conditions_for(request.min_anomaly_score),
        cooldown_seconds=request.cooldown_seconds,
        channels=request.channels,
        is_enabled=request.is_enabled,
    )
    try:
        await AlertRuleRepository(session).add(rule)
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"An alert rule named {request.name!r} already exists.",
        ) from error
    return AlertRuleResponse.from_row(rule)


@router.get("/api/v1/alert-rules", response_model=AlertRuleListResponse)
async def list_alert_rules(session: SessionDep, claims: CurrentUserDep) -> AlertRuleListResponse:
    rows = await AlertRuleRepository(session).list_for(claims.user_id)
    return AlertRuleListResponse(
        count=len(rows), rules=[AlertRuleResponse.from_row(row) for row in rows]
    )


@router.get(
    "/api/v1/alert-rules/{rule_id}",
    response_model=AlertRuleResponse,
    responses={404: {"description": "Not found"}},
)
async def get_alert_rule(
    session: SessionDep, claims: CurrentUserDep, rule_id: uuid.UUID
) -> AlertRuleResponse:
    rule = await AlertRuleRepository(session).get_for(rule_id, claims.user_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert rule not found.")
    return AlertRuleResponse.from_row(rule)


@router.patch(
    "/api/v1/alert-rules/{rule_id}",
    response_model=AlertRuleResponse,
    responses={404: {"description": "Not found"}, 409: {"description": "Name already in use"}},
)
async def update_alert_rule(
    session: SessionDep,
    claims: CurrentUserDep,
    rule_id: uuid.UUID,
    request: AlertRuleUpdateRequest,
) -> AlertRuleResponse:
    rule = await AlertRuleRepository(session).get_for(rule_id, claims.user_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert rule not found.")

    new_name = request.name
    fields = request.model_dump(exclude_unset=True, exclude={"min_anomaly_score"})
    for field_name, value in fields.items():
        setattr(rule, field_name, value)
    if "min_anomaly_score" in request.model_fields_set:
        rule.conditions = _conditions_for(request.min_anomaly_score)

    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"An alert rule named {new_name!r} already exists.",
        ) from error
    await session.refresh(rule)
    return AlertRuleResponse.from_row(rule)


@router.delete(
    "/api/v1/alert-rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"description": "Not found"}},
)
async def delete_alert_rule(
    session: SessionDep, claims: CurrentUserDep, rule_id: uuid.UUID
) -> None:
    deleted = await AlertRuleRepository(session).delete_for(rule_id, claims.user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert rule not found.")
    await session.commit()


@router.get("/api/v1/alerts", response_model=AlertListResponse)
async def list_alerts(
    session: SessionDep,
    claims: CurrentUserDep,
    unacknowledged_only: Annotated[bool, Query()] = False,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> AlertListResponse:
    """The signed-in user's own alert feed, newest first — the REST counterpart to the live
    `/ws/alerts` push, for the initial page load and for history a socket cannot replay.
    """
    repository = AlertRepository(session)
    filters: dict[str, Any] = {"acknowledged_at": None} if unacknowledged_only else {}
    rows = await repository.list_for(claims.user_id, limit=limit, offset=offset, **filters)

    instrument_repository = InstrumentRepository(session)
    responses = []
    for row in rows:
        symbol = None
        if row.instrument_id is not None:
            instrument = await instrument_repository.get(row.instrument_id)
            symbol = instrument.symbol if instrument is not None else None
        responses.append(
            AlertResponse(
                id=row.id,
                alert_rule_id=row.alert_rule_id,
                symbol=symbol,
                event_type=row.event_type,
                severity=row.severity,
                title=row.title,
                body=row.body,
                payload=row.payload,
                created_at=row.created_at,
                acknowledged_at=row.acknowledged_at,
            )
        )
    return AlertListResponse(count=len(responses), alerts=responses)


@router.post(
    "/api/v1/alerts/{alert_id}/ack",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"description": "Not found"}},
)
async def acknowledge_alert(
    session: SessionDep, claims: CurrentUserDep, alert_id: uuid.UUID
) -> None:
    acknowledged = await AlertRepository(session).acknowledge_for(alert_id, claims.user_id)
    if not acknowledged:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found.")
    await session.commit()
