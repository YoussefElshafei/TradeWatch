"""Configuration behaviour.

Phase 0's exit criterion is that a missing required variable fails at startup with a clear
message. These tests are what makes that a fact rather than an intention.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from tradewatch.config.settings import (
    AnomalySettings,
    ApiSettings,
    AppSettings,
    ConfigurationError,
    DatabaseSettings,
    Environment,
    LogFormat,
    MarketSettings,
    RedisSettings,
    SecuritySettings,
    Settings,
    load_settings,
)

pytestmark = pytest.mark.unit


class TestRequiredValues:
    def test_loads_when_required_variables_are_present(self, required_env: None) -> None:
        settings = load_settings()

        assert settings.database.database_url.startswith("postgresql")
        assert settings.app.env is Environment.LOCAL

    def test_missing_required_variables_raise_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError) as raised:
            load_settings()

        message = str(raised.value)
        assert "TW_DATABASE_URL" in message
        assert "TW_REDIS_URL" in message
        assert "TW_JWT_SECRET" in message

    def test_error_reports_every_problem_at_once(self) -> None:
        """One restart per missing variable is a bad way to configure a system."""
        with pytest.raises(ConfigurationError) as raised:
            load_settings()

        assert "3 problems" in str(raised.value)

    def test_error_points_at_the_example_file(self) -> None:
        with pytest.raises(ConfigurationError) as raised:
            load_settings()

        assert ".env.example" in str(raised.value)


class TestParsing:
    def test_comma_separated_lists(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TW_SYMBOLS", "aapl, msft ,tsla")

        assert MarketSettings().symbols == ["AAPL", "MSFT", "TSLA"]

    def test_log_level_is_case_insensitive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TW_LOG_LEVEL", "debug")

        assert AppSettings().log_level == "DEBUG"

    def test_unknown_log_level_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TW_LOG_LEVEL", "chatty")

        with pytest.raises(ValueError, match="must be one of"):
            AppSettings()

    def test_unknown_provider_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A provider without an adapter must fail at startup, not at the first tick."""
        monkeypatch.setenv("TW_PROVIDER", "polygon")

        with pytest.raises(ConfigurationError, match="TW_PROVIDER"):
            load_settings()

    def test_currency_must_be_an_iso_code(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TW_BASE_CURRENCY", "dollars")

        with pytest.raises(ValueError, match="ISO 4217"):
            MarketSettings()


class TestCrossFieldRules:
    def test_page_size_default_may_not_exceed_maximum(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TW_API_PAGE_SIZE_DEFAULT", "500")
        monkeypatch.setenv("TW_API_PAGE_SIZE_MAX", "100")

        with pytest.raises(ValueError, match="TW_API_PAGE_SIZE_DEFAULT"):
            ApiSettings()

    def test_anomaly_scoring_window_must_fit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TW_ANOMALY_WINDOW_BARS", "30")
        monkeypatch.setenv("TW_ANOMALY_MIN_BARS_BEFORE_SCORING", "60")

        with pytest.raises(ValueError, match="TW_ANOMALY_MIN_BARS_BEFORE_SCORING"):
            AnomalySettings()


class TestProductionHardening:
    """Local development may be convenient. Production may not."""

    @staticmethod
    def _production(jwt_secret: str, **app_kwargs: object) -> Settings:
        return load_settings(
            app=AppSettings(
                env=Environment.PROD,
                log_format=LogFormat.JSON,
                **app_kwargs,  # type: ignore[arg-type]
            ),
            database=DatabaseSettings(database_url="postgresql+asyncpg://x/y"),
            redis=RedisSettings(redis_url="redis://localhost:6379/0"),
            security=SecuritySettings(jwt_secret=SecretStr(jwt_secret)),
        )

    def test_accepts_a_strong_configuration(self) -> None:
        settings = self._production("a" * 48)

        assert settings.app.env is Environment.PROD

    def test_rejects_a_placeholder_secret(self) -> None:
        with pytest.raises(ConfigurationError, match="placeholder"):
            self._production("change-me")

    def test_rejects_a_short_secret(self) -> None:
        with pytest.raises(ConfigurationError, match="at least 32 characters"):
            self._production("short")

    def test_rejects_console_logging(self) -> None:
        with pytest.raises(ConfigurationError, match="TW_LOG_FORMAT"):
            load_settings(
                app=AppSettings(env=Environment.PROD, log_format=LogFormat.CONSOLE),
                database=DatabaseSettings(database_url="postgresql+asyncpg://x/y"),
                redis=RedisSettings(redis_url="redis://localhost:6379/0"),
                security=SecuritySettings(jwt_secret=SecretStr("a" * 48)),
            )

    def test_rejects_wildcard_cors(self) -> None:
        with pytest.raises(ConfigurationError, match="TW_CORS_ORIGINS"):
            load_settings(
                app=AppSettings(env=Environment.PROD, log_format=LogFormat.JSON),
                database=DatabaseSettings(database_url="postgresql+asyncpg://x/y"),
                redis=RedisSettings(redis_url="redis://localhost:6379/0"),
                security=SecuritySettings(jwt_secret=SecretStr("a" * 48), cors_origins=["*"]),
            )

    def test_the_same_weak_configuration_is_fine_locally(self) -> None:
        settings = load_settings(
            app=AppSettings(env=Environment.LOCAL),
            database=DatabaseSettings(database_url="postgresql+asyncpg://x/y"),
            redis=RedisSettings(redis_url="redis://localhost:6379/0"),
            security=SecuritySettings(jwt_secret=SecretStr("change-me")),
        )

        assert settings.app.env is Environment.LOCAL


class TestSecrets:
    def test_secrets_are_not_printed(self, required_env: None) -> None:
        """A settings object ends up in logs and tracebacks. Secrets must not."""
        settings = load_settings()

        assert "test-secret" not in repr(settings)
        assert settings.security.jwt_secret.get_secret_value().startswith("test-secret")
