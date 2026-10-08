"""HTTP request metrics: labelled by route template and status, read back from the registry.

The exporter's HTTP surface is covered in `tests/unit/test_metrics.py`; here the question is
what the API middleware writes into the registry that exporter serves.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

pytestmark = pytest.mark.api


def _requests(route: str, status: str, method: str = "GET") -> float:
    value = REGISTRY.get_sample_value(
        "tradewatch_http_requests_total", {"method": method, "route": route, "status": status}
    )
    return value or 0.0


def test_a_request_is_counted_under_its_route_and_status(client: TestClient) -> None:
    before = _requests("/health", "200")

    client.get("/health")
    client.get("/health")

    assert _requests("/health", "200") - before == 2


def test_an_id_in_the_path_becomes_the_template_not_a_new_series(client: TestClient) -> None:
    """One series per route, not one per backtest id — ids are unbounded cardinality."""
    template = "/api/v1/backtests/{backtest_id}"
    before = _requests(template, "401")

    for _ in range(3):
        client.get(f"/api/v1/backtests/{uuid.uuid4()}")  # no token: 401, still routed

    assert _requests(template, "401") - before == 3


def test_an_unmatched_path_folds_into_one_series(client: TestClient) -> None:
    before = _requests("unmatched", "404")

    client.get("/wp-admin/setup.php")
    client.get(f"/{uuid.uuid4()}")

    assert _requests("unmatched", "404") - before == 2


def test_request_duration_is_observed(client: TestClient) -> None:
    labels = {"method": "GET", "route": "/health"}
    before = REGISTRY.get_sample_value("tradewatch_http_request_duration_seconds_count", labels)

    client.get("/health")

    after = REGISTRY.get_sample_value("tradewatch_http_request_duration_seconds_count", labels)
    assert (after or 0.0) - (before or 0.0) == 1
