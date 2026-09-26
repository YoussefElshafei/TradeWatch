"""Backtest submit/list/get/trades/equity (`ARCHITECTURE.md` §4.3), against a real database.

Submission never touches bar data — it only validates ownership, parameters, and the
concurrency limit, then inserts a `queued` row. The Backtest Runner (tested separately in
`test_backtest_runner.py`) is what actually reads bars and computes a result.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Callable

import httpx2 as httpx
import pytest

from tradewatch.config.settings import Settings
from tradewatch.infrastructure.database.models import Strategy, User

pytestmark = pytest.mark.integration

AuthHeadersFor = Callable[[User], dict[str, str]]

_TODAY = dt.date(2026, 1, 1)


def _submit_payload(strategy_id: uuid.UUID, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "strategy_id": str(strategy_id),
        "start_date": _TODAY.isoformat(),
        "end_date": (_TODAY + dt.timedelta(days=30)).isoformat(),
    }
    payload.update(overrides)
    return payload


class TestSubmitBacktest:
    async def test_creates_a_queued_run(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.post(
            "/api/v1/backtests",
            json=_submit_payload(strategy.id),
            headers=auth_headers_for(user),
        )
        assert response.status_code == 201
        body = response.json()
        assert body["status"] == "queued"
        assert body["strategy_id"] == str(strategy.id)
        assert body["symbols"] == strategy.symbols
        # The environment's execution defaults, fully resolved and stored on the row —
        # never re-read from settings later (`ARCHITECTURE.md` §16.5).
        assert "slippage_bps" in body["execution_config"]

    async def test_unknown_strategy_is_404(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.post(
            "/api/v1/backtests",
            json=_submit_payload(uuid.uuid4()),
            headers=auth_headers_for(user),
        )
        assert response.status_code == 404

    async def test_another_users_strategy_is_404_not_403(
        self,
        api: httpx.AsyncClient,
        other_user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.post(
            "/api/v1/backtests",
            json=_submit_payload(strategy.id),
            headers=auth_headers_for(other_user),
        )
        assert response.status_code == 404

    async def test_reversed_date_range_is_422(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.post(
            "/api/v1/backtests",
            json=_submit_payload(
                strategy.id,
                start_date=(_TODAY + dt.timedelta(days=30)).isoformat(),
                end_date=_TODAY.isoformat(),
            ),
            headers=auth_headers_for(user),
        )
        assert response.status_code == 422

    async def test_exceeding_the_concurrency_limit_is_409(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
        settings: Settings,
    ) -> None:
        headers = auth_headers_for(user)
        limit = settings.security.max_backtests_per_user_concurrent
        for _ in range(limit):
            response = await api.post(
                "/api/v1/backtests", json=_submit_payload(strategy.id), headers=headers
            )
            assert response.status_code == 201
        over_limit = await api.post(
            "/api/v1/backtests", json=_submit_payload(strategy.id), headers=headers
        )
        assert over_limit.status_code == 409

    async def test_slippage_override_is_stored(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.post(
            "/api/v1/backtests",
            json=_submit_payload(strategy.id, execution_config={"slippage_bps": "37"}),
            headers=auth_headers_for(user),
        )
        assert response.status_code == 201
        assert response.json()["execution_config"]["slippage_bps"] == "37"


class TestListAndGetBacktest:
    async def test_owner_sees_the_run(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        headers = auth_headers_for(user)
        created = await api.post(
            "/api/v1/backtests", json=_submit_payload(strategy.id), headers=headers
        )
        run_id = created.json()["id"]

        listing = await api.get("/api/v1/backtests", headers=headers)
        assert listing.status_code == 200
        assert any(run["id"] == run_id for run in listing.json()["backtests"])

        detail = await api.get(f"/api/v1/backtests/{run_id}", headers=headers)
        assert detail.status_code == 200
        assert detail.json()["id"] == run_id

    async def test_another_user_gets_404_not_403(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        created = await api.post(
            "/api/v1/backtests", json=_submit_payload(strategy.id), headers=auth_headers_for(user)
        )
        run_id = created.json()["id"]

        response = await api.get(
            f"/api/v1/backtests/{run_id}", headers=auth_headers_for(other_user)
        )
        assert response.status_code == 404

    async def test_a_missing_id_is_also_404(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.get(
            f"/api/v1/backtests/{uuid.uuid4()}", headers=auth_headers_for(user)
        )
        assert response.status_code == 404


class TestTradesAndEquity:
    """No worker has run, so both lists are empty — this only proves the ownership gate and
    the response shape, not the numbers a completed run produces (see `test_backtest_runner.py`).
    """

    async def test_owner_gets_empty_lists_for_a_queued_run(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        headers = auth_headers_for(user)
        created = await api.post(
            "/api/v1/backtests", json=_submit_payload(strategy.id), headers=headers
        )
        run_id = created.json()["id"]

        trades = await api.get(f"/api/v1/backtests/{run_id}/trades", headers=headers)
        assert trades.status_code == 200
        assert trades.json() == {"count": 0, "trades": []}

        equity = await api.get(f"/api/v1/backtests/{run_id}/equity", headers=headers)
        assert equity.status_code == 200
        assert equity.json() == {"count": 0, "equity_curve": []}

    async def test_another_user_gets_404_not_403_for_trades(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        created = await api.post(
            "/api/v1/backtests", json=_submit_payload(strategy.id), headers=auth_headers_for(user)
        )
        run_id = created.json()["id"]

        response = await api.get(
            f"/api/v1/backtests/{run_id}/trades", headers=auth_headers_for(other_user)
        )
        assert response.status_code == 404

    async def test_another_user_gets_404_not_403_for_equity(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        created = await api.post(
            "/api/v1/backtests", json=_submit_payload(strategy.id), headers=auth_headers_for(user)
        )
        run_id = created.json()["id"]

        response = await api.get(
            f"/api/v1/backtests/{run_id}/equity", headers=auth_headers_for(other_user)
        )
        assert response.status_code == 404
