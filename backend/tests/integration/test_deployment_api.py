"""The deployment lifecycle API (Phase 7): create / start / pause / resume / stop, against a
real database — the `api` fixture is shared with `test_strategy_api.py`/`test_backtest_api.py`.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2 as httpx
import pytest

from tradewatch.infrastructure.database.models import Strategy, User

pytestmark = pytest.mark.integration

AuthHeadersFor = Callable[[User], dict[str, str]]


async def _create_portfolio(api: httpx.AsyncClient, user: User, headers: dict[str, str]) -> str:
    response = await api.post(
        "/api/v1/portfolios",
        json={"name": "Deployment test portfolio", "initial_cash": "10000"},
        headers=headers,
    )
    assert response.status_code == 201
    return str(response.json()["id"])


async def _create_deployment(
    api: httpx.AsyncClient, strategy_id: str, portfolio_id: str, headers: dict[str, str]
) -> httpx.Response:
    return await api.post(
        "/api/v1/deployments",
        json={"strategy_id": strategy_id, "portfolio_id": portfolio_id},
        headers=headers,
    )


class TestCreateDeployment:
    async def test_creates_with_defaults_from_settings(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        headers = auth_headers_for(user)
        portfolio_id = await _create_portfolio(api, user, headers)
        response = await _create_deployment(api, str(strategy.id), portfolio_id, headers)

        assert response.status_code == 201
        body = response.json()
        assert body["status"] == "created"
        assert body["started_at"] is None
        assert body["risk_limits"]["max_orders_per_minute"] > 0

    async def test_someone_elses_strategy_is_404(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        headers = auth_headers_for(other_user)
        portfolio_id = await _create_portfolio(api, other_user, headers)
        response = await _create_deployment(api, str(strategy.id), portfolio_id, headers)
        assert response.status_code == 404


class TestLifecycleTransitions:
    async def test_the_full_happy_path(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        headers = auth_headers_for(user)
        portfolio_id = await _create_portfolio(api, user, headers)
        created = await _create_deployment(api, str(strategy.id), portfolio_id, headers)
        deployment_id = created.json()["id"]

        started = await api.post(f"/api/v1/deployments/{deployment_id}/start", headers=headers)
        assert started.status_code == 200
        assert started.json()["status"] == "running"
        assert started.json()["started_at"] is not None
        first_started_at = started.json()["started_at"]

        paused = await api.post(f"/api/v1/deployments/{deployment_id}/pause", headers=headers)
        assert paused.status_code == 200
        assert paused.json()["status"] == "paused"

        resumed = await api.post(f"/api/v1/deployments/{deployment_id}/resume", headers=headers)
        assert resumed.status_code == 200
        assert resumed.json()["status"] == "running"
        # started_at must not move on a resume — it anchors the live driver's warm-up window.
        assert resumed.json()["started_at"] == first_started_at

        stopped = await api.post(f"/api/v1/deployments/{deployment_id}/stop", headers=headers)
        assert stopped.status_code == 200
        assert stopped.json()["status"] == "stopped"
        assert stopped.json()["stopped_at"] is not None

    async def test_starting_a_running_deployment_is_409(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        headers = auth_headers_for(user)
        portfolio_id = await _create_portfolio(api, user, headers)
        created = await _create_deployment(api, str(strategy.id), portfolio_id, headers)
        deployment_id = created.json()["id"]
        await api.post(f"/api/v1/deployments/{deployment_id}/start", headers=headers)

        second_start = await api.post(f"/api/v1/deployments/{deployment_id}/start", headers=headers)
        assert second_start.status_code == 409

    async def test_pausing_a_created_deployment_is_409(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        headers = auth_headers_for(user)
        portfolio_id = await _create_portfolio(api, user, headers)
        created = await _create_deployment(api, str(strategy.id), portfolio_id, headers)
        deployment_id = created.json()["id"]

        response = await api.post(f"/api/v1/deployments/{deployment_id}/pause", headers=headers)
        assert response.status_code == 409

    async def test_another_users_deployment_actions_are_404_not_403(
        self,
        api: httpx.AsyncClient,
        user: User,
        other_user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        owner_headers = auth_headers_for(user)
        portfolio_id = await _create_portfolio(api, user, owner_headers)
        created = await _create_deployment(api, str(strategy.id), portfolio_id, owner_headers)
        deployment_id = created.json()["id"]

        stranger_headers = auth_headers_for(other_user)
        get_response = await api.get(
            f"/api/v1/deployments/{deployment_id}", headers=stranger_headers
        )
        assert get_response.status_code == 404
        start_response = await api.post(
            f"/api/v1/deployments/{deployment_id}/start", headers=stranger_headers
        )
        assert start_response.status_code == 404


class TestPortfolioDetail:
    async def test_returns_positions_alongside_the_portfolio(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        headers = auth_headers_for(user)
        portfolio_id = await _create_portfolio(api, user, headers)

        response = await api.get(f"/api/v1/portfolios/{portfolio_id}", headers=headers)
        assert response.status_code == 200
        body = response.json()
        assert body["id"] == portfolio_id
        assert body["positions"] == []
