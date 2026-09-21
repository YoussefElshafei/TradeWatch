"""Shared test fixtures.

Two rules shape everything here:

* **Tests never read the developer's environment.** ``clean_env`` strips every ``TW_``
  variable before each test, so a test that passes on one machine passes on all of them.
* **Tests build their own settings.** Nothing depends on a ``.env`` file existing.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from tradewatch.api.app import create_app
from tradewatch.api.routes.health import clear_readiness_checks
from tradewatch.config.settings import (
    DatabaseSettings,
    RedisSettings,
    SecuritySettings,
    Settings,
    load_settings,
)

#: The three variables with no default (see ARCHITECTURE.md §16).
REQUIRED_ENV = {
    "TW_DATABASE_URL": "postgresql+asyncpg://tradewatch:tradewatch@localhost:5432/tradewatch_test",
    "TW_REDIS_URL": "redis://localhost:6379/1",
    "TW_JWT_SECRET": "test-secret-not-used-anywhere-real-0123456789",
}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Remove every TW_ variable so tests are isolated from the local environment."""
    for name in list(os.environ):
        if name.startswith("TW_"):
            monkeypatch.delenv(name, raising=False)
    yield


@pytest.fixture
def required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set only the variables that have no default."""
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)


@pytest.fixture
def settings() -> Settings:
    """Valid settings built in-process, independent of the environment."""
    return load_settings(
        database=DatabaseSettings(database_url=REQUIRED_ENV["TW_DATABASE_URL"]),
        redis=RedisSettings(redis_url=REQUIRED_ENV["TW_REDIS_URL"]),
        security=SecuritySettings(jwt_secret=SecretStr(REQUIRED_ENV["TW_JWT_SECRET"])),
    )


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    """A test client over the real application.

    Startup registers the real database readiness check, and these tests run with no
    database — so `/ready` legitimately reports 503 here. That is behaviour worth asserting
    (failure F8), and `registry_client` exists for the tests that need to exercise the
    readiness registry itself.
    """
    clear_readiness_checks()
    with TestClient(create_app(settings)) as test_client:
        yield test_client
    clear_readiness_checks()


@pytest.fixture
def registry_client(settings: Settings) -> Iterator[TestClient]:
    """A client whose dependency checks have been cleared after startup.

    Lets a test register exactly the checks it wants to reason about, without a real
    database in the mix.
    """
    clear_readiness_checks()
    with TestClient(create_app(settings)) as test_client:
        clear_readiness_checks()
        yield test_client
    clear_readiness_checks()
