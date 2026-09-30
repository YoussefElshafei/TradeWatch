"""Typed application configuration.

Every value that `ARCHITECTURE.md` §16 lists as "an assumption that must remain
configurable" lives here, and nowhere else. The rule is simple: if a number could
reasonably be different in another environment, on another venue, or after calibration
against real data, it is a setting rather than a literal in the code.

Settings are grouped by concern, and each group is an independent ``BaseSettings`` class
reading flat ``TW_``-prefixed environment variables (``TW_DATABASE_URL``, not
``TW_DATABASE__URL``) so that the names in the architecture document, the ``.env.example``
file, and the Compose file are all literally the same strings.

The groups are composed by :func:`load_settings`, which validates all of them and reports
*every* problem at once, as environment variable names — because being told about one
missing variable per run is a bad way to spend an afternoon.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Final, Self

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from tradewatch.domain.enums import BarInterval

ENV_PREFIX: Final = "TW_"

#: Values a placeholder secret may take before we refuse to start outside development.
PLACEHOLDER_SECRETS: Final = frozenset(
    {"change-me", "changeme", "secret", "dev-secret", "insecure", ""}
)


class ConfigurationError(RuntimeError):
    """Raised when configuration is missing or invalid.

    Carries a message listing every offending environment variable, so a developer or an
    operator can fix all of them in one pass instead of one per restart.
    """


class Environment(StrEnum):
    LOCAL = "local"
    CI = "ci"
    PROD = "prod"


class LogFormat(StrEnum):
    CONSOLE = "console"
    JSON = "json"


class ProviderName(StrEnum):
    """Market data providers with an adapter in this repository.

    A member here is a promise that an adapter exists. Adding one without an adapter would
    let the application start and then fail at the first tick, which is worse than refusing
    to start (ADR-0004).
    """

    #: Seeded generator. The default: no key, no network, no quota, deterministic.
    SYNTHETIC = "synthetic"
    #: Playback of a recorded session. Used by tests and demos.
    REPLAY = "replay"
    #: Alpaca Market Data API v2 (ADR-0011). Needs TW_PROVIDER_API_KEY and _API_SECRET.
    ALPACA = "alpaca"


class FillModel(StrEnum):
    NEXT_BAR_OPEN = "next_bar_open"
    CURRENT_CLOSE = "current_close"
    VWAP = "vwap"


class AnomalyDetectorMode(StrEnum):
    BASELINE = "baseline"
    MODEL = "model"
    BOTH = "both"


CsvList = Annotated[list[str], NoDecode]
"""A list read from a comma-separated environment variable.

``NoDecode`` stops pydantic-settings from trying to parse the value as JSON first, so
``TW_SYMBOLS=AAPL,MSFT`` works the way anyone would expect it to.
"""


class _Group(BaseSettings):
    """Base for every settings group: same prefix, same parsing rules."""

    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_file=None,  # loading .env is the entrypoint's job, not the model's
        extra="ignore",
        frozen=True,
    )

    @field_validator("*", mode="before")
    @classmethod
    def _split_csv(cls, value: Any, info: Any) -> Any:
        """Turn ``a,b,c`` into ``["a", "b", "c"]`` for the CSV list fields."""
        field = cls.model_fields.get(info.field_name)
        if field is None or not isinstance(value, str):
            return value
        if NoDecode not in field.metadata:
            return value
        return [item.strip() for item in value.split(",") if item.strip()]


class AppSettings(_Group):
    """Process identity and logging (`ARCHITECTURE.md` §16.9)."""

    env: Environment = Environment.LOCAL
    log_level: str = "INFO"
    log_format: LogFormat = LogFormat.CONSOLE
    metrics_enabled: bool = True

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalise_level(cls, value: Any) -> Any:
        return value.upper() if isinstance(value, str) else value

    @field_validator("log_level")
    @classmethod
    def _known_level(cls, value: str) -> str:
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
        if value not in allowed:
            raise ValueError(f"must be one of {sorted(allowed)}")
        return value


class MarketSettings(_Group):
    """Instruments, sessions, and bar shape (`ARCHITECTURE.md` §16.1)."""

    symbols: CsvList = Field(default=["AAPL", "MSFT", "NVDA", "SPY"])
    market_timezone: str = "America/New_York"
    market_session_open: str = "09:30"
    market_session_close: str = "16:00"
    base_currency: str = "USD"
    bar_intervals: CsvList = Field(default=["1m", "5m", "1h", "1d"])
    #: How long after a bar closes a late trade may still revise it (failure F11). Past
    #: this the bar is sealed and a straggler is counted and dropped — a bar that can be
    #: revised forever is a bar nothing downstream can ever act on.
    late_data_grace_seconds: int = Field(default=5, ge=0, le=3600)
    #: How often the aggregator derives 5m/1h/1d bars from stored 1m bars. Rolling up on
    #: every flush would re-read the same minutes dozens of times a minute for a result
    #: that only changes when a bar closes.
    rollup_interval_seconds: int = Field(default=60, gt=0)
    #: How far back each rollup pass reaches. Must comfortably exceed the largest interval
    #: in `bar_intervals`, or the current daily bar is never recomputed as minutes arrive.
    rollup_lookback_minutes: int = Field(default=2880, gt=0)

    @field_validator("symbols")
    @classmethod
    def _upper_symbols(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("at least one symbol is required")
        return [symbol.upper() for symbol in value]

    @model_validator(mode="after")
    def _rollup_window_covers_the_intervals(self) -> Self:
        """A lookback shorter than the coarsest bar can never complete one.

        Silent when wrong: daily bars would simply be missing their most recent entry, and
        nothing would say why.
        """
        try:
            coarsest = max(BarInterval(value).duration for value in self.bar_intervals)
        except ValueError:
            # An unknown interval string; the field validator below reports it properly.
            return self
        if dt.timedelta(minutes=self.rollup_lookback_minutes) < coarsest:
            raise ValueError(
                "TW_ROLLUP_LOOKBACK_MINUTES must be at least as long as the largest "
                "interval in TW_BAR_INTERVALS"
            )
        return self

    @field_validator("bar_intervals")
    @classmethod
    def _known_intervals(cls, value: list[str]) -> list[str]:
        """Fail at startup rather than at the first rollup."""
        allowed = {member.value for member in BarInterval}
        unknown = [item for item in value if item not in allowed]
        if unknown:
            raise ValueError(f"unknown bar interval(s) {unknown}; allowed: {sorted(allowed)}")
        if BarInterval.ONE_MINUTE.value not in value:
            raise ValueError(
                "TW_BAR_INTERVALS must include 1m: every other interval is rolled up from it"
            )
        return value

    @field_validator("base_currency")
    @classmethod
    def _currency_code(cls, value: str) -> str:
        if len(value) != 3 or not value.isalpha():
            raise ValueError("must be a three-letter ISO 4217 code, e.g. USD")
        return value.upper()


class ProviderSettings(_Group):
    """Market data provider (`ARCHITECTURE.md` §16.2, ADR-0004)."""

    provider: ProviderName = ProviderName.SYNTHETIC
    provider_api_key: SecretStr | None = None
    #: Alpaca and most brokers issue a key *pair*; a key alone will not authenticate.
    provider_api_secret: SecretStr | None = None
    provider_base_url: str | None = None
    provider_rate_limit_per_min: int = Field(default=60, gt=0)
    provider_reconnect_max_backoff_s: float = Field(default=30.0, gt=0)
    provider_staleness_threshold_s: float = Field(default=30.0, gt=0)
    provider_timeout_s: float = Field(default=10.0, gt=0)
    #: 0 means retry a stream forever, which is what a market data feed wants: giving up at
    #: 04:00 because the provider blipped is not better than reconnecting at 04:01.
    provider_max_reconnect_attempts: int = Field(default=0, ge=0)
    #: Bounded in-flight duplicate window. The database's UNIQUE(event_id) is the real
    #: guarantee; this only saves the round trip (see application.ingestion.dedupe).
    dedupe_window_seconds: float = Field(default=900.0, gt=0)
    dedupe_max_entries: int = Field(default=100_000, gt=0)
    synthetic_seed: int = 42
    #: Path to a JSONL recording, required when TW_PROVIDER=replay.
    replay_path: Path | None = None

    @model_validator(mode="after")
    def _credentials_match_the_provider(self) -> Self:
        """Fail at startup rather than at the first request.

        A provider that needs credentials and has none will authenticate-fail on its first
        call, minutes or hours later, in a stack trace that does not obviously say
        "you forgot an environment variable".
        """
        if self.provider is ProviderName.ALPACA and not (
            self.provider_api_key and self.provider_api_secret
        ):
            raise ValueError(
                "TW_PROVIDER=alpaca requires TW_PROVIDER_API_KEY and TW_PROVIDER_API_SECRET"
            )
        if self.provider is ProviderName.REPLAY and self.replay_path is None:
            raise ValueError("TW_PROVIDER=replay requires TW_REPLAY_PATH")
        return self


class KafkaSettings(_Group):
    """Event backbone (`ARCHITECTURE.md` §16.3, ADR-0001).

    The names were settled in Phase 0; Phase 2 is where they started being read.

    Nothing here is a Kafka default repeated for decoration. Every value is one a
    deployment would plausibly need to change — a broker address, a consumer group, the
    partition count the per-symbol ordering guarantee depends on — or one where the client
    library's default is wrong for this system and the difference is worth stating
    (``acks``, ``enable_idempotence``, ``auto_offset_reset``).
    """

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic_prefix: str = "tw"

    # --- topics ------------------------------------------------------------
    #: Partitions per market topic. Ordering is guaranteed *per partition* and the key is
    #: the symbol, so this is the ceiling on consumer parallelism and never a threat to the
    #: per-symbol ordering the aggregator depends on. Changing it later repartitions the
    #: keyspace, which is why it is deliberately not 1.
    kafka_topic_partitions: int = Field(default=6, gt=0)
    #: One broker locally means nothing can be replicated. Production overrides this; a
    #: value above the broker count makes topic creation fail rather than silently degrade.
    kafka_topic_replication: int = Field(default=1, gt=0)
    #: Create missing topics at worker startup. Right for local and CI, where the broker is
    #: ephemeral. Production creates topics deliberately and sets this to false.
    kafka_ensure_topics: bool = True

    # --- producer ----------------------------------------------------------
    #: "all" means every in-sync replica acknowledged. "1" and "0" trade durability for
    #: latency, which for market data is the wrong side of the trade.
    kafka_producer_acks: str = "all"
    #: Kafka's idempotent producer: a retry inside one producer session stops creating a
    #: duplicate. It does not make the *pipeline* exactly-once — see ARCHITECTURE.md §7.4.
    kafka_producer_idempotence: bool = True
    kafka_producer_linger_ms: int = Field(default=5, ge=0)
    #: gzip is the only codec available without adding a dependency.
    kafka_producer_compression: str = "gzip"
    kafka_producer_request_timeout_ms: int = Field(default=30_000, gt=0)
    #: Attempts to hand one batch to the broker before publish raises. The caller decides
    #: what that means; ingestion counts the failure and never drops it silently.
    kafka_producer_max_attempts: int = Field(default=3, gt=0)

    # --- consumer ----------------------------------------------------------
    #: One consumer group per logical consumer (`ARCHITECTURE.md` §7.4). The persistence
    #: consumer is `processor-persist`; a second consumer of the same topics gets its own
    #: group and therefore its own offsets.
    kafka_consumer_group: str = "processor-persist"
    #: The bar aggregator's group. Separate from the persistence consumer's on purpose:
    #: separate groups mean separate offsets and separate failure domains, which is the
    #: entire argument for Kafka (ADR-0001). Sharing one would make the two services
    #: compete for partitions and each would see half the trades.
    kafka_aggregator_group: str = "bar-aggregator"
    #: The broadcaster's group (Phase 4). A third independent reader of the same market
    #: topics, for the same reason the aggregator is a second one: its own offsets mean a
    #: broadcaster restart or lag never affects persistence or aggregation, and vice versa.
    kafka_broadcast_group: str = "broadcaster"
    #: The Paper Trading Engine's group (Phase 7). A fourth independent reader of
    #: `tw.market.bars.1m.v1`, again for its own offsets — a paper-trading restart or lag
    #: must not affect persistence, aggregation, or broadcasting, and vice versa.
    kafka_paper_trading_group: str = "paper-trading"
    #: The Anomaly Detection Service's group (Phase 8, `ARCHITECTURE.md` §7.4's
    #: `anomaly-detector`). A fifth independent reader of `tw.market.bars.1m.v1` — its own
    #: offsets mean a detector restart, a model reload, or a slow scoring pass never affects
    #: any other consumer of the same topic, and vice versa.
    kafka_anomaly_detector_group: str = "anomaly-detector"
    #: The Alert Dispatcher's group (Phase 9, `ARCHITECTURE.md` §7.4's `alert-dispatcher`).
    #: The one consumer that reads both `tw.anomalies.v1` and `tw.fills.v1` — its own offsets
    #: on each mean a dispatcher restart or a noisy rule never affects detection or trading.
    kafka_alert_dispatcher_group: str = "alert-dispatcher"
    #: Where a brand-new group starts. "earliest", so a consumer added to an existing topic
    #: processes the history instead of silently skipping it.
    kafka_auto_offset_reset: str = "earliest"
    #: How long one poll waits for records before yielding an empty batch. This is what
    #: bounds how long a graceful shutdown can take.
    kafka_consumer_poll_timeout_ms: int = Field(default=1000, gt=0)
    kafka_consumer_session_timeout_ms: int = Field(default=45_000, gt=0)
    kafka_consumer_heartbeat_ms: int = Field(default=3_000, gt=0)
    consumer_batch_size: int = Field(default=100, gt=0)
    consumer_max_poll_interval_ms: int = Field(default=300_000, gt=0)
    #: Bounded retries for a *retryable* failure — a database that is down. A permanent
    #: failure is never retried: it goes to the DLQ on the first attempt.
    consumer_max_retries: int = Field(default=3, ge=0)
    consumer_retry_backoff_s: float = Field(default=0.5, gt=0)

    # --- dead letters ------------------------------------------------------
    #: With this false a permanently bad message is counted, logged and skipped rather than
    #: quarantined. Nothing blocks either way; the difference is whether the message can be
    #: recovered afterwards.
    dlq_enabled: bool = True

    @field_validator("kafka_producer_acks", mode="before")
    @classmethod
    def _acks_as_text(cls, value: Any) -> Any:
        """``TW_KAFKA_PRODUCER_ACKS=1`` is an int to some config sources, a string here."""
        return str(value) if isinstance(value, int) else value

    @field_validator("kafka_producer_acks")
    @classmethod
    def _known_acks(cls, value: str) -> str:
        allowed = {"0", "1", "all"}
        if value not in allowed:
            raise ValueError(f"must be one of {sorted(allowed)}")
        return value

    @field_validator("kafka_producer_compression")
    @classmethod
    def _known_compression(cls, value: str) -> str:
        # snappy, lz4 and zstd each need a codec package that is not installed. Accepting
        # them here would fail at the first produce instead of at startup.
        allowed = {"gzip", "none"}
        if value not in allowed:
            raise ValueError(f"must be one of {sorted(allowed)}")
        return value

    @field_validator("kafka_auto_offset_reset")
    @classmethod
    def _known_offset_reset(cls, value: str) -> str:
        allowed = {"earliest", "latest"}
        if value not in allowed:
            raise ValueError(f"must be one of {sorted(allowed)}")
        return value

    @field_validator("kafka_topic_prefix")
    @classmethod
    def _usable_prefix(cls, value: str) -> str:
        """Topic names are built from this, and Kafka accepts a narrow character set."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("must not be empty")
        if not all(character.isalnum() or character in "-_" for character in cleaned):
            raise ValueError("may contain only letters, digits, '-' and '_'")
        return cleaned

    @model_validator(mode="after")
    def _heartbeat_fits_the_session(self) -> Self:
        """Kafka needs the heartbeat interval well under the session timeout.

        The broker's own rule of thumb is a third. Getting this wrong produces a consumer
        that is evicted from its group under load, which presents as random rebalancing.
        """
        if self.kafka_consumer_heartbeat_ms >= self.kafka_consumer_session_timeout_ms // 3:
            raise ValueError(
                "TW_KAFKA_CONSUMER_HEARTBEAT_MS must be under a third of "
                "TW_KAFKA_CONSUMER_SESSION_TIMEOUT_MS"
            )
        return self

    @model_validator(mode="after")
    def _idempotence_requires_full_acks(self) -> Self:
        """Kafka's idempotent producer is only defined with acks=all.

        aiokafka rejects the combination when it connects; refusing it here names the two
        environment variables involved instead of raising from inside the client.
        """
        if self.kafka_producer_idempotence and self.kafka_producer_acks != "all":
            raise ValueError("TW_KAFKA_PRODUCER_IDEMPOTENCE requires TW_KAFKA_PRODUCER_ACKS=all")
        return self


class DatabaseSettings(_Group):
    """PostgreSQL (`ARCHITECTURE.md` §16.4, ADR-0002).

    ``database_url`` has no default on purpose: silently connecting to something a
    developer did not choose is how test data ends up somewhere it should not be.
    """

    database_url: str
    database_pool_size: int = Field(default=10, gt=0)
    database_pool_timeout_s: float = Field(default=10.0, gt=0)


class RedisSettings(_Group):
    """Cache (`ARCHITECTURE.md` §16.4, ADR-0003).

    ``cache_enabled=False`` must leave the application fully correct, only slower. That is
    a test in CI, not a claim.
    """

    redis_url: str
    cache_enabled: bool = True
    cache_ttl_quote_s: int = Field(default=5, gt=0)
    cache_ttl_bars_s: int = Field(default=30, gt=0)
    cache_ttl_instruments_s: int = Field(default=3600, gt=0)


class ExecutionSettings(_Group):
    """Trading simulation assumptions (`ARCHITECTURE.md` §16.5).

    These change the result of every backtest, which is why they are also stored per run
    and per deployment rather than being read from the environment at read time.
    """

    exec_slippage_bps: Decimal = Field(default=Decimal("2"), ge=0)
    exec_commission_per_share: Decimal = Field(default=Decimal("0"), ge=0)
    exec_commission_pct: Decimal = Field(default=Decimal("0"), ge=0)
    exec_fill_model: FillModel = FillModel.NEXT_BAR_OPEN
    exec_allow_partial_fills: bool = False
    initial_cash: Decimal = Field(default=Decimal("100000"), gt=0)
    max_position_pct: float = Field(default=0.25, gt=0, le=1)
    max_gross_exposure_pct: float = Field(default=1.0, gt=0)
    max_orders_per_minute_per_deployment: int = Field(default=10, gt=0)


class AnomalySettings(_Group):
    """Detection thresholds (`ARCHITECTURE.md` §16.6, ADR-0008).

    Every threshold here is a guess until it is calibrated against real data in Phase 8,
    which is exactly why none of them may be a literal in the detector.
    """

    anomaly_window_bars: int = Field(default=60, gt=1)
    anomaly_baseline_z_threshold: float = Field(default=3.5, gt=0)
    anomaly_model_threshold: float | None = None
    anomaly_min_bars_before_scoring: int = Field(default=30, gt=0)
    anomaly_detector: AnomalyDetectorMode = AnomalyDetectorMode.BASELINE
    anomaly_model_path: Path | None = None
    anomaly_features: CsvList = Field(
        default=["return", "volatility", "volume_z", "trade_count_z", "spread"]
    )

    @model_validator(mode="after")
    def _scoring_window_fits(self) -> Self:
        if self.anomaly_min_bars_before_scoring > self.anomaly_window_bars:
            raise ValueError(
                "TW_ANOMALY_MIN_BARS_BEFORE_SCORING must not exceed TW_ANOMALY_WINDOW_BARS"
            )
        return self


class AlertSettings(_Group):
    """Alert volume control (`ARCHITECTURE.md` §16.7)."""

    alert_cooldown_seconds: int = Field(default=300, ge=0)
    alert_max_per_user_per_hour: int = Field(default=50, gt=0)
    alert_channels_enabled: CsvList = Field(default=["in_app"])


class SecuritySettings(_Group):
    """Authentication, authorization, and abuse limits (`ARCHITECTURE.md` §16.8, ADR-0010).

    ``jwt_secret`` has no default. A signing key with a default is a signing key that ships
    to production.
    """

    jwt_secret: SecretStr
    access_token_ttl_min: int = Field(default=15, gt=0, le=1440)
    refresh_token_ttl_days: int = Field(default=30, gt=0)
    password_min_length: int = Field(default=12, ge=8)
    #: Both spellings of the dev server's address. A browser treats `localhost` and
    #: `127.0.0.1` as different origins, and Vite prints the numeric one — so listing only
    #: `localhost` blocks every request the dev server actually makes.
    cors_origins: CsvList = Field(default=["http://localhost:5173", "http://127.0.0.1:5173"])
    rate_limit_anon_per_min: int = Field(default=60, gt=0)
    rate_limit_user_per_min: int = Field(default=600, gt=0)
    max_backtests_per_user_concurrent: int = Field(default=2, gt=0)
    max_deployments_per_user: int = Field(default=5, gt=0)
    registration_enabled: bool = True


class ApiSettings(_Group):
    """HTTP and WebSocket surface (`ARCHITECTURE.md` §16.9).

    ``api_host`` defaults to loopback deliberately. There is no authentication until
    Phase 5, so the API must not be reachable from the network by accident before then.
    """

    api_host: str = "127.0.0.1"
    # 8010, not the conventional 8000: left at 8000 in earlier phases, a stale dev-server
    # process from an earlier session was found permanently squatting on host port 8000 with
    # no way to identify or kill it (see PROJECT_STATE.md's known issues). Moved off it
    # rather than keep fighting a single machine's port history.
    api_port: int = Field(default=8010, gt=0, lt=65536)
    api_page_size_default: int = Field(default=100, gt=0)
    api_page_size_max: int = Field(default=1000, gt=0)
    backtest_timeout_seconds: int = Field(default=300, gt=0)
    ws_max_connections_per_user: int = Field(default=5, gt=0)
    ws_send_buffer_limit: int = Field(default=1000, gt=0)

    @model_validator(mode="after")
    def _page_sizes_consistent(self) -> Self:
        if self.api_page_size_default > self.api_page_size_max:
            raise ValueError("TW_API_PAGE_SIZE_DEFAULT must not exceed TW_API_PAGE_SIZE_MAX")
        return self


class Settings(BaseModel):
    """The whole configuration, assembled and cross-validated."""

    model_config = {"frozen": True}

    app: AppSettings
    market: MarketSettings
    provider: ProviderSettings
    kafka: KafkaSettings
    database: DatabaseSettings
    redis: RedisSettings
    execution: ExecutionSettings
    anomaly: AnomalySettings
    alerts: AlertSettings
    security: SecuritySettings
    api: ApiSettings

    @model_validator(mode="after")
    def _production_hardening(self) -> Self:
        """Rules that only bite outside development.

        Local development is allowed to be convenient. Production is not.
        """
        if self.app.env is not Environment.PROD:
            return self

        problems: list[str] = []
        secret = self.security.jwt_secret.get_secret_value()
        if secret.lower() in PLACEHOLDER_SECRETS:
            problems.append("TW_JWT_SECRET is a placeholder value")
        if len(secret) < 32:
            problems.append("TW_JWT_SECRET must be at least 32 characters")
        if "*" in self.security.cors_origins:
            problems.append("TW_CORS_ORIGINS must not be '*'")
        if self.app.log_format is not LogFormat.JSON:
            problems.append("TW_LOG_FORMAT must be 'json' so logs are machine-readable")
        if problems:
            raise ValueError("; ".join(problems))
        return self


_GROUPS: Final[dict[str, type[_Group]]] = {
    "app": AppSettings,
    "market": MarketSettings,
    "provider": ProviderSettings,
    "kafka": KafkaSettings,
    "database": DatabaseSettings,
    "redis": RedisSettings,
    "execution": ExecutionSettings,
    "anomaly": AnomalySettings,
    "alerts": AlertSettings,
    "security": SecuritySettings,
    "api": ApiSettings,
}


def _env_var_name(group: type[_Group], field_name: str) -> str:
    prefix = group.model_config.get("env_prefix", ENV_PREFIX)
    return f"{prefix}{field_name}".upper()


def _describe(group: type[_Group], error: ValidationError) -> list[str]:
    """Render a group's validation errors as environment-variable advice."""
    lines: list[str] = []
    for item in error.errors():
        location = item["loc"]
        field_name = str(location[0]) if location else "<model>"
        detail = item["msg"]
        if field_name in group.model_fields:
            name = _env_var_name(group, field_name)
            missing = item["type"] == "missing"
            lines.append(f"  {name}: {'is required but not set' if missing else detail}")
        else:
            lines.append(f"  {detail}")
    return lines


def load_settings(**overrides: Any) -> Settings:
    """Build and validate the configuration.

    Every group is validated even if an earlier one failed, so the error message names all
    of the problems at once.

    Args:
        **overrides: Group name to an already-constructed settings group. Used by tests to
            replace one group without touching the process environment.

    Raises:
        ConfigurationError: If any value is missing or invalid.
    """
    groups: dict[str, _Group] = {}
    problems: list[str] = []

    for name, group_type in _GROUPS.items():
        if name in overrides:
            groups[name] = overrides[name]
            continue
        try:
            groups[name] = group_type()
        except ValidationError as error:
            problems.extend(_describe(group_type, error))

    if problems:
        raise ConfigurationError(_format_problems(problems))

    try:
        return Settings(**groups)  # type: ignore[arg-type]
    except ValidationError as error:
        details = [f"  {item['msg']}" for item in error.errors()]
        raise ConfigurationError(_format_problems(details)) from error


def _format_problems(problems: list[str]) -> str:
    count = len(problems)
    noun = "problem" if count == 1 else "problems"
    return (
        f"TradeWatch configuration is invalid ({count} {noun}):\n"
        + "\n".join(problems)
        + "\n\nCopy .env.example to .env and fill in the values it lists, or export the "
        "variables above. See ARCHITECTURE.md section 16 for what each one means."
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings, loaded once.

    Tests should call :func:`load_settings` directly, or
    ``get_settings.cache_clear()`` after changing the environment — a cached singleton that
    silently ignores a changed environment is a confusing thing to debug.
    """
    return load_settings()
