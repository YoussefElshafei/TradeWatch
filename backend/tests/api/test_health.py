"""The operational surface, and the cross-cutting behaviour every later route inherits."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tradewatch import __version__
from tradewatch.api.app import REQUEST_ID_HEADER, create_app
from tradewatch.api.routes.health import register_readiness_check
from tradewatch.config.settings import Settings

pytestmark = pytest.mark.api


class TestHealth:
    def test_reports_ok_and_the_version(self, client: TestClient) -> None:
        response = client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok", "version": __version__}


class TestReadiness:
    def test_reports_not_ready_when_the_database_is_unreachable(
        self, offline_client: TestClient
    ) -> None:
        """Failure F8: the API refuses traffic rather than failing request by request.

        `offline_client` points the application at a closed port, so the check registered
        at startup fails for a reason this test controls rather than one the developer's
        Docker happens to supply.
        """
        response = offline_client.get("/ready")

        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "not_ready"
        assert "database" in body["checks"]

    def test_ready_with_no_dependencies_registered(self, registry_client: TestClient) -> None:
        response = registry_client.get("/ready")

        assert response.status_code == 200
        assert response.json() == {"status": "ready", "checks": {}}

    def test_ready_when_every_check_passes(self, registry_client: TestClient) -> None:
        async def healthy() -> str | None:
            return None

        register_readiness_check("database", healthy)

        response = registry_client.get("/ready")

        assert response.status_code == 200
        assert response.json()["checks"] == {"database": "ok"}

    def test_not_ready_when_a_check_fails(self, registry_client: TestClient) -> None:
        async def failing() -> str | None:
            return "connection refused"

        register_readiness_check("database", failing)

        response = registry_client.get("/ready")

        assert response.status_code == 503
        assert response.json() == {
            "status": "not_ready",
            "checks": {"database": "connection refused"},
        }

    def test_a_check_that_raises_counts_as_a_failure(self, registry_client: TestClient) -> None:
        async def exploding() -> str | None:
            raise TimeoutError("took too long")

        register_readiness_check("kafka", exploding)

        response = registry_client.get("/ready")

        assert response.status_code == 503
        assert response.json()["checks"] == {"kafka": "TimeoutError"}

    def test_liveness_is_independent_of_readiness(self, registry_client: TestClient) -> None:
        """A liveness probe that fails during an outage turns it into a restart loop."""

        async def failing() -> str | None:
            return "down"

        register_readiness_check("database", failing)

        assert registry_client.get("/ready").status_code == 503
        assert registry_client.get("/health").status_code == 200


class TestRequestId:
    def test_every_response_carries_one(self, client: TestClient) -> None:
        response = client.get("/health")

        assert response.headers[REQUEST_ID_HEADER]

    def test_an_inbound_request_id_is_preserved(self, client: TestClient) -> None:
        """A request id from the edge must survive, or a trace has a hole in it."""
        response = client.get("/health", headers={REQUEST_ID_HEADER: "abc-123"})

        assert response.headers[REQUEST_ID_HEADER] == "abc-123"

    def test_generated_ids_differ_between_requests(self, client: TestClient) -> None:
        first = client.get("/health").headers[REQUEST_ID_HEADER]
        second = client.get("/health").headers[REQUEST_ID_HEADER]

        assert first != second


class TestErrorEnvelope:
    def test_not_found_uses_the_shared_shape(self, client: TestClient) -> None:
        response = client.get("/no-such-route")

        assert response.status_code == 404
        body = response.json()["error"]
        assert body["code"] == "not_found"
        assert body["request_id"] == response.headers[REQUEST_ID_HEADER]

    def test_unknown_query_parameters_are_ignored(self, client: TestClient) -> None:
        # Documents current behaviour, so a later change that starts rejecting them is a
        # deliberate one rather than a surprise.
        response = client.get("/health", params={"unexpected": "value"})

        assert response.status_code == 200


class TestOpenApi:
    def test_schema_is_generated(self, client: TestClient) -> None:
        response = client.get("/openapi.json")

        assert response.status_code == 200
        assert "/health" in response.json()["paths"]

    def test_description_states_the_scope_honestly(self, client: TestClient) -> None:
        """ADR-0008: the anomaly component must never be described as detecting crime."""
        raw = client.get("/openapi.json").json()["info"]["description"]
        description = " ".join(raw.lower().split())

        assert "no real money" in description
        assert "statistically unusual market activity" in description
        assert "not market manipulation or financial crime" in description


class TestApplicationStartup:
    def test_app_starts_with_valid_settings(self, settings: Settings) -> None:
        """The Phase 0 exit criterion: the application starts without configuration errors."""
        with TestClient(create_app(settings)) as client:
            assert client.get("/health").status_code == 200
