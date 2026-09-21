# Graph Report - TradeWatch  (2026-09-21)

## Corpus Check
- 196 files · ~174,266 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 11 file(s) not represented in the graph (top: (none) 5, .example 1, .ini 1)

## Summary
- 3993 nodes · 9894 edges · 167 communities (145 shown, 22 thin omitted)
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 1053 edges (avg confidence: 0.91)
- Token cost: 247,976 input · 60,680 output

## Community Hubs (Navigation)
- Kafka Event Envelope
- Domain Enums and Status
- Application Ports and Entities
- Event Publishing and Consumption
- FastAPI App Bootstrap
- Processor Test Harness
- Broker Failure Handling Tests
- Bar Aggregator Core
- In-Flight Deduplication Service
- Event Encoding and Decoding
- Bar Store Port
- Deterministic Event Identity
- Provider Configuration Factory
- Market Data Ingestion Service
- Database Constraint Tests
- Historical Backfill Job
- Rate Limiting and Staleness
- Kafka Integration Fixtures
- Stream Recording and Replay
- User Ownership and Backtests
- Reconnection and Retry Policy
- Market API Route Tests
- Environment File Loading
- Health and Readiness Endpoints
- Fake Kafka Producer
- Transactional Session Store
- Bar Entity Persistence
- SQL Bar Store
- Kafka Topic Administration
- Consumer Commit Semantics
- Bounded Event Deduplicator
- Aggregator Shutdown Tests
- Data Quality Gap Checks
- Alpaca HTTP Adapter
- Alpaca Provider Normalization
- Kafka Test Support Fakes
- SQL Market Data Store
- Trade and Portfolio Repositories
- Database Index Verification
- Structured Logging Setup
- Provider Payload Normalization
- Backfill Resume Tests
- Synthetic Provider Scenarios
- Market Data Processor
- Processing Error Types
- Market Data Provider Port
- Session Bound Store
- ORM Model Tests
- Domain Model Base
- Owner Scoped Repository
- API Provider Fixtures
- Portfolio and Position Queries
- Repository Base Class
- Publisher Retry Tests
- Market API Routes
- Bar Repository
- Frontend Lint Config
- Bar Aggregation Consumer
- Kafka Settings
- Kafka Client Protocols
- Ingestion Integration Tests
- TypeScript Compiler Config
- Deferred Architecture Components
- Write Buffer and Trade Entity
- Quote Streaming and Entity
- Failure Scenario Catalogue
- API Error Envelope
- Synthetic Bar Generation
- Alpaca HTTP Client Tests
- Dotenv Loading Tests
- Frontend Market Client
- Socket Stream Multiplexing
- Instrument Repository
- Integration Test Fixtures
- Frontend Dev Dependencies
- Core Architecture Components
- Settings Value Normalizers
- Controlled Socket Fixtures
- Alpaca Response Fixtures
- CI Quality and Layering
- Ordering and Idempotency Design
- Market Store Port
- Bar Query Surface
- Provider Test Builders
- Testing Strategy Principles
- Retry Call Wrapper
- Fake Kafka Consumer
- Redelivery Double-Count Tests
- Backbone and Storage Decisions
- Infrastructure Service Topology
- Architecture and Plan Overview
- Alembic Migration Environment
- Position Entity
- Frontend App Shell
- Project Discipline Rules
- Redis and Security Settings
- Decimal Money Parsing
- Stream Multiplexing Tests
- API Gateway and Trust Zones
- API Response Schemas
- Settings Loading and Validation
- Cross-Field Settings Validators
- Quote Entity
- Kafka Broker Fixtures
- Bar Invariant Tests
- Provider Error Mapping Tests
- Frontend NPM Scripts
- Frontend Health Client
- Alembic Migration Tests
- Price Chart Component
- Market Data Consumer Worker
- Alert and Execution Settings
- App Settings Parsing
- Test Engine Fixtures
- Schema Completeness Tests
- Latest Price Snapshot Tests
- Minute Bar Arithmetic Tests
- CI Backend Jobs
- Frontend CI and Deferrals
- ADR Process and Provider Port
- Auth and Strategy Decisions
- Anomaly Detection Scope
- Alpaca Frame Decoding
- Topic Naming Tests
- Market View Tests
- Alembic Migration Revisions
- Anomaly and API Settings
- Kafka Admin Client
- Migration Hygiene Tests
- Shutdown Idempotency Tests
- Prettier Formatting Config
- Benchmark Honesty Rules
- UUID Timestamp Extraction
- Anomaly Detection Tests
- Live Provider Smoke Tests
- Bar Bucketing Tests
- Bar Validators
- UTC Datetime Enforcement
- Bar Correction Validators
- Migration Drift Test
- Cascade Delete Tests
- Decimal Precision Storage Tests
- Advancing Clock Fixture
- Encoding Determinism Tests
- Dedup Retry Test
- Bar Natural Key
- PostgreSQL Enum Helper
- Port Conformance Tests
- Frontend Runtime Dependencies
- VWAP Calculation
- Malformed Message Counter
- Validation Error Formatting
- Vite Environment Types
- Bash Check Script
- Application Layer Package
- Domain Layer Package
- Infrastructure Layer Package
- Workers Layer Package
- Test Suite Package
- Provider Test Package
- Bash Setup Script
- TradeWatch Root Package

## God Nodes (most connected - your core abstractions)
1. `BarInterval` - 138 edges
2. `MarketEvent` - 118 edges
3. `new_id()` - 115 edges
4. `SyntheticProvider` - 97 edges
5. `trade()` - 97 edges
6. `KafkaSettings` - 90 edges
7. `NewBar` - 75 edges
8. `IngestionService` - 67 edges
9. `TradeEvent` - 61 edges
10. `make_provider()` - 59 edges

## Surprising Connections (you probably didn't know these)
- `Full Jitter Backoff` --semantically_similar_to--> `Alert Dispatcher`  [INFERRED] [semantically similar]
  docs/INGESTION.md → ARCHITECTURE.md
- `Ownership Enforced in the Data Access Layer` --semantically_similar_to--> `import-linter Layering Enforcement`  [INFERRED] [semantically similar]
  ARCHITECTURE.md → backend/README.md
- `Three Variables With No Default` --semantically_similar_to--> `Provider Adapters (synthetic / replay / alpaca)`  [INFERRED] [semantically similar]
  README.md → docs/INGESTION.md
- `Live Provider Tests` --semantically_similar_to--> `SyntheticProvider`  [INFERRED] [semantically similar]
  backend/tests/provider/README.md → ARCHITECTURE.md
- `VWAP Exact for 1m, Null for Rollups` --semantically_similar_to--> `No Invented Numbers`  [INFERRED] [semantically similar]
  docs/BACKFILL.md → ARCHITECTURE.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Ports-and-adapters seam isolating infrastructure from the domain** — adr_adr_0004_market_data_provider_abstraction_marketdataprovider, adr_adr_0001_kafka_as_the_event_backbone_eventpublisher_port, adr_adr_0014_consumer_acknowledgement_and_dead_letter_policy_eventbatchconsumer, adr_adr_0014_consumer_acknowledgement_and_dead_letter_policy_deadlettersink, adr_adr_0016_idempotent_bar_aggregation_under_redelivery_streamposition, _github_workflows_ci_import_linter_contract [INFERRED 0.85]
- **Idempotency chain under at-least-once delivery** — adr_adr_0001_kafka_as_the_event_backbone_at_least_once_delivery, adr_adr_0012_deterministic_event_identity_deterministic_event_id, adr_adr_0016_idempotent_bar_aggregation_under_redelivery_consumer_progress, adr_adr_0015_event_time_bar_aggregation_revision_guarded_upsert, adr_adr_0014_consumer_acknowledgement_and_dead_letter_policy_marketdataconsumerservice [EXTRACTED 1.00]
- **Exact-money chain enabling reproducible backtests** — adr_adr_0009_decimal_money_representation_decimal_money, adr_adr_0009_decimal_money_representation_money_as_json_strings, adr_adr_0009_decimal_money_representation_explicit_rounding, adr_adr_0007_shared_strategy_execution_core_execution_simulator, adr_adr_0002_postgresql_as_system_of_record_postgresql_system_of_record [EXTRACTED 1.00]
- **Absorbing At-Least-Once Redelivery End to End** — architecture_at_least_once_delivery, architecture_deterministic_event_identity, docs_ingestion_two_layer_deduplication, docs_schema_idempotency_constraints, docs_project_state_consumer_progress, docs_project_state_aggregator_double_count_bug, architecture_f7_duplicate_delivery [EXTRACTED 1.00]
- **The Application Port Protocols** — docs_project_state_ports, architecture_marketdataprovider_port, architecture_synthetic_provider, docs_ingestion_provider_adapters, docs_pipeline_marketdataprocessor, docs_pipeline_narrow_client_protocols, architecture_ports_before_adapters [EXTRACTED 1.00]
- **The Honest-Claims Discipline** — architecture_no_invented_numbers, architecture_not_a_manipulation_detector, benchmarks_readme_result_rules, docs_backfill_vwap_policy, backend_tests_provider_readme_live_provider_tests, docs_project_state_rules_for_next_worker, architecture_consistency_check [INFERRED 0.85]

## Communities (167 total, 22 thin omitted)

### Community 0 - "Kafka Event Envelope"
Cohesion: 0.03
Nodes (85): aiokafka, aiokafka_errors, AIOKafkaProducer, event_summary(), Any, The fields worth putting in a log line. Deliberately not the whole event:…, EventDecodeError, EventEncodeError (+77 more)

### Community 1 - "Domain Enums and Status"
Cohesion: 0.06
Nodes (86): AlertSeverity, BacktestStatus, DeploymentStatus, InstrumentStatus, OrderSide, OrderStatus, OrderType, StrEnum (+78 more)

### Community 2 - "Application Ports and Entities"
Cohesion: 0.08
Nodes (53): aiokafka_admin, asyncio, The Bar Aggregator: trades in, OHLCV bars out. `ARCHITECTURE.md` §3.3. It…, Historical backfill: fetch bars for a date range and store them. The job that…, The bar storage port. Phase 3 adds three things that need durable bars: the…, The market data provider port. ADR-0004: all market data enters through this…, The market data persistence port. The processor lives in `application`, so it…, Domain entities. These are Pydantic models, not ORM rows. The separation is not… (+45 more)

### Community 3 - "Event Publishing and Consumption"
Cohesion: 0.03
Nodes (42): Fold each event into its bar, at most once ever. An event at or below the…, Consume a live stream until it ends, the limit is reached, or it is cancelled.…, Publish one event. Returns whether it was accepted. A failure here is counted…, Yield batches of events until cancelled, possibly empty ones., Quarantine one event. Args: event: The event that could not be processed.…, Where an event sat in its stream. ``stream`` names one ordered sequence — a…, Yield batches of ``(position, event)``, possibly empty ones., Publish one event. Raises: EventBusError: If the event could not be accepted.… (+34 more)

### Community 4 - "FastAPI App Bootstrap"
Cohesion: 0.05
Nodes (66): bootstrap(), create_app(), lifespan(), request_context(), FastAPI, FastAPI application factory. Phase 0 wires the things every later route will…, Entry point used by uvicorn: configure logging, then build the app.…, Build the application. Args: settings: Configuration to use. Defaults to the… (+58 more)

### Community 5 - "Processor Test Harness"
Cohesion: 0.07
Nodes (39): The store could not complete an operation. Retryable. The batch that failed is…, StorageError, bar(), factory(), FakeStore, make_processor(), no_sleep(), datetime (+31 more)

### Community 6 - "Broker Failure Handling Tests"
Cohesion: 0.06
Nodes (29): trade(), An unbounded retry is a producer that never reports the outage., Repeating a message the broker considers too large only delays the error., TestBrokerFailures, build(), Exception, An ``EventBatchConsumer`` that yields a fixed script, then goes quiet., `ARCHITECTURE.md` §7.4. Reverse these two and a crash between them is a gap. (+21 more)

### Community 7 - "Bar Aggregator Core"
Cohesion: 0.05
Nodes (31): AggregatorStats, BarAggregator, floor_to_interval(), OpenBar, Any, datetime, timedelta, A bar being accumulated. ``notional`` is carried rather than derived so VWAP… (+23 more)

### Community 8 - "In-Flight Deduplication Service"
Cohesion: 0.05
Nodes (39): DeduplicationStats, _default_clock(), In-flight deduplication. There are two layers of duplicate protection in…, A monotonic clock that does not depend on an event loop being present., What the deduplicator has seen. Reported by the ingestion service., Share of events that were duplicates, 0.0 when nothing has been seen., Market data ingestion: provider → deduplicate → publish., IngestionStats (+31 more)

### Community 9 - "Event Encoding and Decoding"
Cohesion: 0.07
Nodes (24): decode(), encode(), Split a domain event into transport metadata and payload. ``mode="json"`` is…, Serialize an event to deterministic UTF-8 JSON. ``sort_keys`` and fixed…, Turn a message body back into a validated domain event. Four things are…, to_envelope(), body(), Any (+16 more)

### Community 10 - "Bar Store Port"
Cohesion: 0.07
Nodes (27): BarStore, datetime, Protocol, UUID, The newest stored ``bar_start``, optionally inside ``[start, end)``. The…, The oldest stored ``bar_start``, optionally inside ``[start, end)``. Paired…, How many bars are stored in ``[start, end)``., Holes in a bar series, as ``(gap_start, gap_end)`` pairs. A gap is not… (+19 more)

### Community 11 - "Deterministic Event Identity"
Cohesion: 0.05
Nodes (28): deterministic_event_id(), EventType, Decimal, StrEnum, UUID, The identity of a trade, as the provider describes it. A provider trade id is…, Quotes rarely carry an id, so the whole book state is the key. Including the…, `ARCHITECTURE.md` §7.3. (+20 more)

### Community 12 - "Provider Configuration Factory"
Cohesion: 0.06
Nodes (47): ProviderSettings, Market data provider (`ARCHITECTURE.md` §16.2, ADR-0004)., build_provider(), datetime, Building a provider from configuration. The one place that maps `TW_PROVIDER`…, The default clock. A function rather than a lambda so it can be patched by name., The retry policy every adapter and every stream shares., Construct the configured provider. Raises: ValueError: If the configured… (+39 more)

### Community 13 - "Market Data Ingestion Service"
Cohesion: 0.06
Nodes (24): IngestionService, Fetch the provider's instrument universe. Returns snapshots rather than writing…, The current price for one symbol, or None if it has never traded., Fetch historical bars and publish them. Returns how many were published.…, Note a reconnection attempt. Wired into `reconnecting`'s callback., Note symbols that have gone quiet. A warning, not an error: markets close, and…, Provider → deduplicate → publish., Keeps every published event in memory. The simplest useful publisher, and what… (+16 more)

### Community 14 - "Database Constraint Tests"
Cohesion: 0.07
Nodes (25): expect_violation(), AsyncSession, Decimal, parametrize, Failure F18. In the database, so it survives a restart and a second replica., NULL means "all", which is different from an empty array meaning "none"., A rule matching nothing is a mistake, not a configuration., Symbols are stored uppercase so a lookup never needs to guess which case is… (+17 more)

### Community 15 - "Historical Backfill Job"
Cohesion: 0.06
Nodes (36): argparse, Historical backfill: fetch bars for a date range and store them., BackfillResult, BackfillService, default_chunk_for(), Any, datetime, timedelta (+28 more)

### Community 16 - "Rate Limiting and Staleness"
Cohesion: 0.06
Nodes (22): Client-side rate limiting (failure F2). Refuses to exceed the provider's…, Tokens available now. Refills before answering. Without that it reports the…, Take tokens if available. Returns False rather than waiting., Seconds until ``tokens`` would be available. 0 when they already are., Wait until ``tokens`` are available, then take them., Notices when a feed goes quiet (failure F1). A dropped connection usually…, Note that a message arrived for ``symbol``., Time since the last message, measured from startup if there has never been one. (+14 more)

### Community 17 - "Kafka Integration Fixtures"
Cohesion: 0.08
Nodes (34): committing_store(), consume(), kafka_config(), publish(), Any, AsyncEngine, Kafka settings pointed at the real broker and this test's topics., The real store, writing real committed rows. The `session` fixture used by the… (+26 more)

### Community 18 - "Stream Recording and Replay"
Cohesion: 0.06
Nodes (27): capture_stream(), load_events(), _parse_line(), Any, datetime, E, Path, Write events as JSONL. Returns how many were written. (+19 more)

### Community 19 - "User Ownership and Backtests"
Cohesion: 0.09
Nodes (24): An account. Everything a user owns cascades on delete: strategies, portfolios,…, User, BacktestRunRepository, Backtest jobs and their results., Claim one queued run for this worker. ``FOR UPDATE SKIP LOCKED`` is what makes…, User-owned strategy configurations., StrategyRepository, build() (+16 more)

### Community 20 - "Reconnection and Retry Policy"
Cohesion: 0.07
Nodes (25): ProviderConnectionError, The connection failed or dropped — DNS, refused, reset, closed mid-stream. The…, BaseException, Keep a stream alive across disconnections (failure F1). Calls ``open_stream``…, How hard to try before giving up. Attributes: max_attempts: Total attempts…, Seconds to wait before attempt ``attempt`` (1-based). A provider's own…, Whether to try again after ``error`` on attempt ``attempt``., reconnecting() (+17 more)

### Community 21 - "Market API Route Tests"
Cohesion: 0.09
Nodes (24): api(), _session_override(), AsyncClient, AsyncSession, datetime, fixture, Instrument, parametrize (+16 more)

### Community 22 - "Environment File Loading"
Cohesion: 0.07
Nodes (43): main(), Run the API: ``uv run tradewatch-api`` or ``python -m tradewatch.api``.…, Start the development server. Returns a process exit code., find_env_file(), load_env_file(), Path, Loading the ``.env`` file. This is deliberately not done at import time. A…, Return the nearest ``.env`` at or above ``start``, or None if there is none. (+35 more)

### Community 23 - "Health and Readiness Endpoints"
Cohesion: 0.06
Nodes (28): health(), HealthResponse, BaseModel, get, Response, Operational endpoints. `/health` answers "is this process alive?" and must…, Add a dependency check to `/ready`. Raises: ValueError: If a check is already…, Report that the process is running. Checks nothing external, on purpose. (+20 more)

### Community 24 - "Fake Kafka Producer"
Cohesion: 0.06
Nodes (18): FakeProducerClient, A ``ProducerClient`` that records what it was asked to send. ``start_error``…, publisher(), Not silently dropped, and not sent to some default topic either., This is what guarantees per-symbol ordering. It is a correctness property., So they land on one partition and stay in order relative to each other., One bad bar must not cost a 240-bar backfill., Reporting "0 accepted" would let ingestion carry on as if the events were… (+10 more)

### Community 25 - "Transactional Session Store"
Cohesion: 0.07
Nodes (23): AbstractAsyncContextManager, async_sessionmaker, AsyncSession, Bar, datetime, T, UUID, Write bars and the consumer's position in a single transaction. The atomicity… (+15 more)

### Community 26 - "Bar Entity Persistence"
Cohesion: 0.09
Nodes (22): The row to write. Validates the OHLC invariants before the database has to., Persist bars idempotently. Returns rows written. One transaction. The upsert is…, Persist bars **and** the consumer's position, in one transaction. The atomicity…, NewBar, An OHLCV bar on its way in. The invariants below are the ones a bad aggregator…, bar_values(), Decimal, parametrize (+14 more)

### Community 27 - "SQL Bar Store"
Cohesion: 0.12
Nodes (33): Reads and writes bars in PostgreSQL. Implements…, SqlBarStore, aggregate(), bar_store(), _broker_reachable(), kafka(), kafka_config(), build() (+25 more)

### Community 28 - "Kafka Topic Administration"
Cohesion: 0.09
Nodes (21): ensure_topics(), Create any of this phase's topics that do not exist. Returns the ones created.…, kafka_settings(), Kafka settings built in-process, never from the environment., FakeAdmin, Any, Exception, NewTopic (+13 more)

### Community 29 - "Consumer Commit Semantics"
Cohesion: 0.10
Nodes (20): build(), first_batches(), Any, Anything uncommitted was unfinished. Committing to look tidy loses it., Yielded, not swallowed, so the caller gets a turn to notice a shutdown., A repaired or truncated copy destroys the only evidence worth having., Acknowledging a message that was neither stored nor quarantined loses it., Otherwise those offsets are redelivered on every poll, forever. (+12 more)

### Community 30 - "Bounded Event Deduplicator"
Cohesion: 0.07
Nodes (20): EventDeduplicator, UUID, Drop one id, so a failed publish can be retried rather than swallowed. Without…, Forget everything. Called on reconnect, when a replay is expected anyway., A bounded, expiring set of event ids. Args: max_entries: Hard ceiling on…, Record ``event_id`` and report whether it had been seen. Records and answers in…, new_id(), UUID (+12 more)

### Community 31 - "Aggregator Shutdown Tests"
Cohesion: 0.09
Nodes (22): A trade ``seconds`` after 14:30:00., trade(), TestShutdown, build(), The bar aggregator, against an in-memory store. The numbers here are worked out…, One venue clock. Otherwise a symbol that trades hourly has a bar open all hour., A straggler must not reopen bars the watermark already closed., This is what makes a restart uneventful. (+14 more)

### Community 32 - "Data Quality Gap Checks"
Cohesion: 0.07
Nodes (20): check_symbol(), GapReport, Any, datetime, timedelta, The gaps that matter: holes while the market was open., Run every check for one symbol over ``[start, end)``., When the market is open, to the resolution this check needs. Deliberately… (+12 more)

### Community 33 - "Alpaca HTTP Adapter"
Cohesion: 0.09
Nodes (30): attempt(), AlpacaSettings, Exception, Response, Alpaca market data adapter (ADR-0011). This is the one place in TradeWatch that…, Internal: the socket closed cleanly. Converted to a reconnectable error by the…, Connection details, kept out of the adapter so tests can point it anywhere., Map HTTP status to the error class that says what the caller should do. (+22 more)

### Community 34 - "Alpaca Provider Normalization"
Cohesion: 0.09
Nodes (21): AlpacaProvider, _close_connection(), Any, AsyncClient, datetime, Close a socket whether its ``close`` is a coroutine function or not., Alpaca Market Data API v2. Implements…, Historical bars over ``[start, end)``, following pagination to the end. (+13 more)

### Community 35 - "Kafka Test Support Fakes"
Cohesion: 0.09
Nodes (23): bar(), factory(), FakeRecord, datetime, quote(), Builders and fake clients shared by the Kafka tests. The fakes implement the…, One consumer record, shaped like ``aiokafka``'s., A ``DeadLetterSink`` that remembers what it was given. (+15 more)

### Community 36 - "SQL Market Data Store"
Cohesion: 0.07
Nodes (26): _connection_failure(), async_sessionmaker, AsyncSession, BaseException, RowT, UUID, Run one write inside one transaction, translating whatever comes back., Decide whether the caller should retry, or give up on this batch. A constraint… (+18 more)

### Community 37 - "Trade and Portfolio Repositories"
Cohesion: 0.13
Nodes (18): TradeRepository, PortfolioRepository, 0.1 + 0.2 != 0.3 in binary floating point. It does here., make_trade(), AsyncSession, Instrument, Portfolio, The caller owns the transaction. A repository that committed on its own would… (+10 more)

### Community 38 - "Database Index Verification"
Cohesion: 0.10
Nodes (23): explain(), index_columns(), AsyncSession, fixture, Instrument, A partial index without its WHERE clause is just a bigger index., lower(email), which is how case-insensitive uniqueness works without citext., The queries in the repositories use the indexes built for them. (+15 more)

### Community 39 - "Structured Logging Setup"
Cohesion: 0.10
Nodes (28): LogFormat, Logging now; metrics and tracing in Phase 10., bind_request_context(), configure_logging(), _drop_color_message(), get_logger(), Any, Structured logging. Two renderers, one pipeline: human-readable colour in a… (+20 more)

### Community 40 - "Provider Payload Normalization"
Cohesion: 0.09
Nodes (22): The most recent trade, with the book alongside it. Uses…, ProviderDataError, A response arrived and could not be understood (failure F3). Malformed JSON, a…, _from_epoch(), normalize_symbol(), parse_timestamp(), Any, Interpret an epoch number, working out its unit from its magnitude. (+14 more)

### Community 41 - "Backfill Resume Tests"
Cohesion: 0.08
Nodes (17): build(), `written` counts rows the database accepted, which on a re-run is all of them.…, The recovery pattern: fetch a generous window around a suspected gap., The scenario the requirement names: fetch two days, fall over, run it again., The bug this rule exists for, found by running the job for real. The database…, The cautious rule must not disable resuming in the case it was built for., For when the provider has *corrected* history: the upsert repairs it., Bars outside the window must not make it think the window is done. (+9 more)

### Community 42 - "Synthetic Provider Scenarios"
Cohesion: 0.10
Nodes (16): A deterministic fake market. Implements…, Failures and market conditions to inject, on demand. This is the part real data…, SyntheticProvider, SyntheticScenario, T, Determinism is what makes the whole test suite reproducible (ADR-0004)., Otherwise every fixture changes when the watchlist does., What makes "fetch a generous window around the gap" safe. (+8 more)

### Community 43 - "Market Data Processor"
Cohesion: 0.08
Nodes (18): MarketDataProcessor, ProcessorStats, Any, datetime, Consumes normalized market data events and persists them., Events buffered but not yet written. Zero after a successful flush., Consume ``stream`` until it ends, the limit is reached, or it is cancelled.…, Accept one event, buffering it and flushing when the buffer is full. An event… (+10 more)

### Community 44 - "Processing Error Types"
Cohesion: 0.08
Nodes (20): ProcessingError, Exception, Base for every market data processing failure., One event cannot be stored, and never will be. Not retryable — the same event…, UnprocessableEventError, build(), Reverse these and a crash in between is a gap in the series, forever., A quiet poll must not re-commit the previous batch for no reason. The empty… (+12 more)

### Community 45 - "Market Data Provider Port"
Cohesion: 0.07
Nodes (17): InstrumentSnapshot, MarketDataProvider, PriceSnapshot, ProviderCapabilities, datetime, Protocol, Everything TradeWatch needs from a market data vendor. Implementations raise…, Short identifier, stored as `source` on every event this provider produces. (+9 more)

### Community 46 - "Session Bound Store"
Cohesion: 0.12
Nodes (19): RowT, The real repositories and the real error translation, on the test's…, SessionBoundStore, count_trades(), factory(), AsyncSession, Instrument, A price of zero breaks `ck_trades_price_positive`. Retrying would replay the… (+11 more)

### Community 47 - "ORM Model Tests"
Cohesion: 0.11
Nodes (16): Instrument, Portfolio, ORM models against a real PostgreSQL. The constraints are the interesting part.…, NOT NULL is the cheapest constraint there is and the easiest to forget., Deliberately not constrained. Crossed markets (ask below bid) are real and…, A zero-size side means the book is empty there, which is information, not an…, The idempotency guard on the order path: one signal, one order (F6)., The other direction, which is the half people forget. (+8 more)

### Community 48 - "Domain Model Base"
Cohesion: 0.09
Nodes (18): Deployment, DomainModel, Instrument, Portfolio, Any, BaseModel, field_validator, A user-owned, parameterized configuration of a built-in strategy type.… (+10 more)

### Community 49 - "Owner Scoped Repository"
Cohesion: 0.10
Nodes (17): Any, UUID, Fetch one row the actor owns. Returns None both when the row does not exist and…, List the actor's rows, newest first where the model has a creation time., Count the actor's rows. Used for the per-user resource limits in §9.3., Delete one row the actor owns. Returns True when a row was deleted, False when…, Rows affected by a DML statement. ``AsyncSession.execute`` is typed as…, Look up a mapped column by name. Needed because ``ModelT`` is only bound to the… (+9 more)

### Community 50 - "API Provider Fixtures"
Cohesion: 0.12
Nodes (21): api(), _session_override(), provider(), AsyncClient, AsyncSession, fixture, Drive provider → bus → processor, returning what the provider produced. The…, The whole slice, with provenance checked value by value. Not a row count: the… (+13 more)

### Community 51 - "Portfolio and Position Queries"
Cohesion: 0.09
Nodes (16): datetime, Decimal, Portfolio, Position, Strategy, UUID, Not meaningful for a composite key — use :meth:`get_position`., Positions with a non-zero quantity. A closed position keeps its row, because… (+8 more)

### Community 52 - "Repository Base Class"
Cohesion: 0.08
Nodes (19): AsyncSession, Base for repositories over a single table., Repository, QuoteRepository, Top-of-book snapshots., DeploymentRepository, OrderRepository, PositionRepository (+11 more)

### Community 53 - "Publisher Retry Tests"
Cohesion: 0.10
Nodes (15): _noop_sleep(), Any, The failure that a broker restart produces, and that nothing else recovers…, A publisher that builds a new client from `clients` each time it starts., Retry backoff, without the wait., The attempt budget still applies, so a permanently broken broker is reported., A caller that handed us a client owns its lifetime — usually a test double., The broker is gone entirely. Say so rather than looping on a rebuild. (+7 more)

### Community 54 - "Market API Routes"
Cohesion: 0.14
Nodes (24): get_bars(), get_instrument(), get_latest(), list_instruments(), _normalize(), datetime, get, Market data endpoints. The read half of the vertical slice: whatever the… (+16 more)

### Community 55 - "Bar Repository"
Cohesion: 0.13
Nodes (15): BarRepository, OHLCV bars — the read surface for every chart and every backtest., make_bar(), A correction must overwrite the bar it corrects, not add a second one., A backfill job and the live aggregator write the same bar from different…, Replay of an identical bar must be harmless, not rejected., The order a chart draws and a backtest replays — and the order the PK stores., What the Phase 3 backfill uses to work out what to re-fetch after an outage… (+7 more)

### Community 56 - "Frontend Lint Config"
Cohesion: 0.10
Nodes (22): ADR-0009, name, private, type, version, eslint, eslint-config-prettier, @eslint/js (+14 more)

### Community 57 - "Bar Aggregation Consumer"
Cohesion: 0.11
Nodes (15): AggregationConsumerStats, BarAggregationService, Any, datetime, Event, Driving the aggregator from a batch consumer. The same acknowledgement rule as…, Consume until cancelled, until ``stop`` is set, or until ``max_batches``., Write, then acknowledge. Never the other way round. Raises: StorageError: If… (+7 more)

### Community 58 - "Kafka Settings"
Cohesion: 0.14
Nodes (9): KafkaSettings, Event backbone (`ARCHITECTURE.md` §16.3, ADR-0001). The names were settled in…, TestTopicCreation, Phase 2 configuration. The validators here exist to turn a class of runtime…, `ARCHITECTURE.md` §7.4: one group per logical consumer., aiokafka refuses this combination at connect time; naming the variables is…, Otherwise it fails on the first produce, hours later., Getting this wrong presents as random rebalancing under load. (+1 more)

### Community 59 - "Kafka Client Protocols"
Cohesion: 0.10
Nodes (10): ConsumerClient, Any, Protocol, Yield one batch of decoded events per poll, until cancelled. An empty batch…, One poll: fetch, decode, quarantine what cannot be read, return the survivors., Name the partition the way the progress table stores it., Commit the offsets of everything consumed so far. Raises: ConsumerEvictedError:…, Turn one record into an event, or quarantine it and return None. (+2 more)

### Community 60 - "Ingestion Integration Tests"
Cohesion: 0.14
Nodes (15): AsyncSession, Instrument, Failures F1 and F7 together: a reconnect replays, and nothing duplicates.…, Two layers, same answer. The provider repeats events; the deduplicator catches…, Why "fetch a generous window around the gap" is the right recovery (F1)., The generator's bars are not merely plausible; they are storable. A synthetic…, The provider's reference data must be storable without translation loss., Providers restate their universe on every reconnect. (+7 more)

### Community 61 - "TypeScript Compiler Config"
Cohesion: 0.08
Nodes (23): compilerOptions, allowImportingTsExtensions, exactOptionalPropertyTypes, forceConsistentCasingInFileNames, isolatedModules, jsx, lib, module (+15 more)

### Community 62 - "Deferred Architecture Components"
Cohesion: 0.13
Nodes (23): Alert Dispatcher, Anomaly Detection Service, Backtest Runner, Component Dependency Graph and Critical Path, Execution Simulator, Not a Market-Manipulation Detector, Ownership Enforced in the Data Access Layer, Paper Trading Engine (+15 more)

### Community 63 - "Write Buffer and Trade Entity"
Cohesion: 0.12
Nodes (11): _Buffer, Pending writes, grouped by table., NewTrade, A trade print on its way in, before the database assigns a surrogate key.…, A persisted trade print., Trade, Dropping real market data because someone else's NTP is off is the worse…, TestTrade (+3 more)

### Community 64 - "Quote Streaming and Entity"
Cohesion: 0.13
Nodes (9): Live top-of-book for ``symbols``., QuoteEvent, A top-of-book snapshot. No validator requires the ask to be at or above the…, Decimal, Yield trade prints until cancelled, or until the scenario says otherwise., Rewind to the seed state, so a reconnect replays identically., The shared body of both streams, including scenario injection., One symbol's walk. Kept separate so symbols are independent and reproducible. (+1 more)

### Community 65 - "Failure Scenario Catalogue"
Cohesion: 0.13
Nodes (21): At-Least-Once Delivery Semantics, F11 — Out-of-Order / Late Market Data, F13 — Bar Aggregator Restart Loses Open Bars, F7 — Duplicate Delivery, Failure Scenario Catalogue F1–F20, Decisions Deliberately Not Made Yet (D1–D20), Expand and Contract Migrations, Phase 11 — End-to-End and Failure-Recovery Hardening (+13 more)

### Community 66 - "API Error Envelope"
Cohesion: 0.15
Nodes (20): error_response(), ErrorDetail, ErrorEnvelope, install_exception_handlers(), _database_error(), _http_exception(), _unhandled(), _validation_error() (+12 more)

### Community 67 - "Synthetic Bar Generation"
Cohesion: 0.13
Nodes (17): datetime, timedelta, Generate bars over ``[start, end)``. Deterministic in the *range*, not just in…, A bar that depends only on (seed, symbol, interval, bar_start). The level comes…, bar_level(), _octave(), Decimal, A deterministic, continuous price path. The synthetic provider needs two… (+9 more)

### Community 68 - "Alpaca HTTP Client Tests"
Cohesion: 0.10
Nodes (3): handler(), The same bytes would parse the same way; retrying just wastes the quota., Retrying burns the rate-limit budget to learn nothing.

### Community 69 - "Dotenv Loading Tests"
Cohesion: 0.20
Nodes (11): MonkeyPatch, parametrize, Path, Loading `.env`. Small surface, but it is the first thing a new developer…, `uv run` from backend/ must still find the repository-root .env., An exported value is a deliberate act; a file should not quietly override it., Docker and CI supply configuration through the environment directly., TestDiscovery (+3 more)

### Community 70 - "Frontend Market Client"
Cohesion: 0.18
Nodes (16): ApiError, BarQuery, BarSeries, ErrorEnvelope, fetchBars(), fetchInstruments(), fetchLatestPrice(), Instrument (+8 more)

### Community 71 - "Socket Stream Multiplexing"
Cohesion: 0.15
Nodes (8): One Alpaca socket, fanned out to the trade and quote subscribers. **Why this…, Yield events of one ``kind`` until the socket fails or the caller stops., Stop the reader and close the socket, leaving subscriber queues alone. Their…, Decode frames and route events, until the socket fails or this task is…, Tell every subscriber the stream finished, without inventing an error., Hand the failure to every subscriber so each one reconnects., _StreamMultiplexer, Queue

### Community 72 - "Instrument Repository"
Cohesion: 0.14
Nodes (10): InstrumentRepository, Instrument, Reference data. Small, read constantly, and cached in Redis from Phase 4., Look up by ticker. Symbols are stored uppercase, enforced by a CHECK., Resolve several tickers in one round trip. The ingestion path needs an…, Insert, or update the mutable reference fields if the symbol already exists.…, Symbols are stored uppercase; callers should not have to know that., Avoids the N+1 that turns a fast consumer into a slow one. (+2 more)

### Community 73 - "Integration Test Fixtures"
Cohesion: 0.22
Nodes (18): anyio_backend(), instrument(), instrument_factory(), other_user(), portfolio(), Any, AsyncSession, fixture (+10 more)

### Community 74 - "Frontend Dev Dependencies"
Cohesion: 0.10
Nodes (20): devDependencies, eslint, eslint-config-prettier, @eslint/js, eslint-plugin-react-hooks, eslint-plugin-react-refresh, globals, jsdom (+12 more)

### Community 75 - "Core Architecture Components"
Cohesion: 0.13
Nodes (19): Bar Aggregator, Architecture Consistency Self-Audit, Deterministic event_id, Kafka Event Envelope, Market Data Ingestion Service, Market Data Processor, Three Timestamps (occurred_at / ingested_at / recorded_at), tradewatch-backfill Job (+11 more)

### Community 76 - "Settings Value Normalizers"
Cohesion: 0.13
Nodes (8): MarketSettings, Any, field_validator, Turn ``a,b,c`` into ``["a", "b", "c"]`` for the CSV list fields., Instruments, sessions, and bar shape (`ARCHITECTURE.md` §16.1)., Fail at startup rather than at the first rollup., ``TW_KAFKA_PRODUCER_ACKS=1`` is an int to some config sources, a string here., Topic names are built from this, and Kafka accepts a narrow character set.

### Community 77 - "Controlled Socket Fixtures"
Cohesion: 0.14
Nodes (12): ConnectionClosedOK, controlled_socket_factory(), connect(), ControlledSocket, FakeSocket, make_settings(), Any, Stands in for `websockets.exceptions.ConnectionClosedOK`. Matched by *name* by… (+4 more)

### Community 78 - "Alpaca Response Fixtures"
Cohesion: 0.15
Nodes (8): json_response(), MonkeyPatch, ADR-0009: JSON floats must not carry their binary error into the ledger., Alpaca happens to serve every BarInterval today. The rejection path still needs…, Every one of these arrives over a healthy connection and must be rejected…, High below low. The domain validator catches what the provider got wrong., TestFetchBars, TestMalformedResponses

### Community 79 - "CI Quality and Layering"
Cohesion: 0.12
Nodes (17): backend-quality job (lint, format, types, layering), Architecture layering enforced by import-linter, ADR-0005: JSON + Pydantic event serialization, Breaking changes produce a new versioned topic, Consumer validates on receipt; failures go to the DLQ, Money carried as JSON strings, ADR-0014: Consumer acknowledgement boundary and dead-letter policy, DeadLetterSink port (+9 more)

### Community 80 - "Ordering and Idempotency Design"
Cohesion: 0.13
Nodes (18): Symbol-keyed topics for per-symbol ordering, Event envelope (event_id, schema_version, occurred_at, correlation_id), Bounded in-flight deduplicator (insufficient alone), deterministic_event_id() UUIDv5 over a natural key, Fixed namespace UUID, never changed, Per-event natural key composition (source, type, symbol, revision), Closing early is harmless, sealing early is not, ADR-0015: Event-time bar aggregation with per-symbol sealing (+10 more)

### Community 81 - "Market Store Port"
Cohesion: 0.12
Nodes (11): InstrumentRef, MarketDataStore, Protocol, UUID, The minimum the processor needs to know about an instrument: its id and symbol., Durable storage for normalized market data. Implementations raise…, Return the instrument id for ``symbol``, registering it if it is unknown.…, Persist trades idempotently. Returns the number of rows written. One… (+3 more)

### Community 82 - "Bar Query Surface"
Cohesion: 0.14
Nodes (11): Bar, datetime, UUID, Trades for one instrument in a half-open window, newest first. Served by…, The most recent print. Cached in Redis from Phase 4 (ADR-0003). Tie-broken by…, The most recent quote, tie-broken by insertion order like `TradeRepository`., Bars in a half-open window, oldest first. Oldest first because that is the…, Return the holes in a bar series, as (gap_start, gap_end) pairs. Used by the… (+3 more)

### Community 83 - "Provider Test Builders"
Cohesion: 0.21
Nodes (7): make_provider(), An adapter wired to a fake transport and a clock that never really sleeps., A connector that records every socket it hands out., Distinct from an auth failure: waiting it out can work., Otherwise one stream reconnects and the other waits on an empty queue forever., socket_factory(), TestStreaming

### Community 84 - "Testing Strategy Principles"
Cohesion: 0.15
Nodes (17): Deterministic Where It Matters, The Golden Backtest, MarketDataProvider Port, Ports Before Adapters, SyntheticProvider, Testing Strategy (Layers and Rules), Integration Test Suite Policy, Recording a Replay Fixture (capture_stream) (+9 more)

### Community 85 - "Retry Call Wrapper"
Cohesion: 0.15
Nodes (9): call_with_retry(), T, Run ``operation``, retrying retryable provider failures with backoff. Non-…, A bug in our own code must not be retried into a storm., TestCallWithRetry, unauthorised(), always_fails(), limited() (+1 more)

### Community 86 - "Fake Kafka Consumer"
Cohesion: 0.12
Nodes (9): FakeConsumerClient, _Partition, Any, Exception, Stands in for ``TopicPartition``, which is the dict key aiokafka groups records…, A ``ConsumerClient`` driven by a script of polls. ``polls`` is a list of record…, _Sent, TestConfiguration (+1 more)

### Community 87 - "Redelivery Double-Count Tests"
Cohesion: 0.15
Nodes (10): Any, Decimal, The bug that made ADR-0016 necessary, asserted rather than argued.…, Everything a doubled fold would disturb, exactly, except VWAP., Rewind the offsets and the bar must come back identical. This is the exact…, The realistic shape: the tail of the last batch comes back, then new trades do.…, The guard must skip what is at or below the mark, and nothing above it. A guard…, A store that writes into a shared activity log, so ordering is assertable. (+2 more)

### Community 88 - "Backbone and Storage Decisions"
Cohesion: 0.16
Nodes (16): At-least-once delivery with idempotent consumers, ADR-0001: Kafka as the event backbone, ADR-0002: PostgreSQL as the system of record, Derived tables are rebuildable projections, TW_CACHE_ENABLED=false correctness test, Redis down degrades, never fails (F9), ADR-0003: Redis, a bounded justified scope, Pure evaluate(state, bar) -> (state, signals) (+8 more)

### Community 89 - "Infrastructure Service Topology"
Cohesion: 0.14
Nodes (16): Assumptions That Must Remain Configurable, The Database Is the System of Record, Redis: Justified Uses Only, Topic Naming and Partition Keying, api service (app profile), kafka service (KRaft, single broker), postgres service, redis service (+8 more)

### Community 90 - "Architecture and Plan Overview"
Cohesion: 0.19
Nodes (16): TradeWatch Architecture, Vertical Slice First, import-linter Layering Enforcement, Backend Package Layout, TradeWatch Development Plan, Phase 0 — Foundations, Phase 1 — Vertical Slice, History and Bars (BACKFILL) (+8 more)

### Community 91 - "Alembic Migration Environment"
Cohesion: 0.20
Nodes (15): _configure(), do_run_migrations(), include_object(), Any, Connection, Alembic environment. The database URL comes from `TW_DATABASE_URL` through the…, Run migrations against a live database. When Alembic is invoked from inside an…, Decide whether autogenerate should consider a database object. Alembic's own… (+7 more)

### Community 92 - "Position Entity"
Cohesion: 0.17
Nodes (7): Position, A holding, derived from fills. Stored for query speed, but `fills` is the…, Value of the holding at ``price``. Negative for a short position., Profit that would be realised by closing the position at ``price``., Short selling is deferred (D16), but the arithmetic has to be right when it…, ADR-0009. The same sum in floats drifts; here it does not., TestPosition

### Community 93 - "Frontend App Shell"
Cohesion: 0.14
Nodes (11): App(), EMPTY_UNIVERSE, ADR-0008, ADR-0008, AsyncResult, AsyncState, Keyed, LOADING (+3 more)

### Community 94 - "Project Discipline Rules"
Cohesion: 0.13
Nodes (15): Every Technology Earns Its Place, Autogenerate Drift Test, Alembic Migration Rules, File Editing Permission, Git Restrictions (no commit or push), Definition of Done (Every Phase), The Second Consumer Group, The Two Consumer Groups (+7 more)

### Community 95 - "Redis and Security Settings"
Cohesion: 0.25
Nodes (8): Cache (`ARCHITECTURE.md` §16.4, ADR-0003). ``cache_enabled=False`` must leave…, Authentication, authorization, and abuse limits (`ARCHITECTURE.md` §16.8,…, RedisSettings, SecuritySettings, A full settings object with a given Kafka group, for the worker-level tests., settings_with(), Local development may be convenient. Production may not., TestProductionHardening

### Community 96 - "Decimal Money Parsing"
Cohesion: 0.20
Nodes (7): parse_decimal(), Turn a JSON number or string into an exact Decimal. Floats go through ``str``…, Decimal, ADR-0009. `Decimal(0.1)` carries the binary error; `Decimal("0.1")` does not., A price of zero is a bad tick, not a free share., A quote side with no size is information, not an error., TestParseDecimal

### Community 97 - "Stream Multiplexing Tests"
Cohesion: 0.21
Nodes (6): Event, Trades and quotes share one socket. This is the class that would have caught…, Ending rather than raising: `reconnecting` is what decides to reopen., One stream stopping must not take the other's connection with it., One socket means one trade subscription. Two would silently interleave., TestMultiplexing

### Community 98 - "API Gateway and Trust Zones"
Cohesion: 0.14
Nodes (14): FastAPI API Gateway, Dead-Letter by Classification, Not Retry Count, React + TypeScript Frontend, Trust Zones, Kafka Command-Line Recipes, Full Jitter Backoff, Ingestion Resilience Policy, StalenessMonitor (+6 more)

### Community 99 - "API Response Schemas"
Cohesion: 0.23
Nodes (12): Request and response schemas. Separate from the ORM models on purpose…, ApiModel, BarPoint, BarSeriesResponse, InstrumentListResponse, InstrumentResponse, BaseModel, Response schemas for the market data endpoints. `ARCHITECTURE.md` §8.3: ORM… (+4 more)

### Community 100 - "Settings Loading and Validation"
Cohesion: 0.19
Nodes (8): _format_problems(), load_settings(), Build and validate the configuration. Every group is validated even if an…, Configuration behaviour. Phase 0's exit criterion is that a missing required…, A settings object ends up in logs and tracebacks. Secrets must not., One restart per missing variable is a bad way to configure a system., TestRequiredValues, TestSecrets

### Community 101 - "Cross-Field Settings Validators"
Cohesion: 0.22
Nodes (7): model_validator, Self, A lookback shorter than the coarsest bar can never complete one. Silent when…, Fail at startup rather than at the first request. A provider that needs…, Kafka needs the heartbeat interval well under the session timeout. The broker's…, Kafka's idempotent producer is only defined with acks=all. aiokafka rejects the…, Rules that only bite outside development. Local development is allowed to be…

### Community 102 - "Quote Entity"
Cohesion: 0.21
Nodes (7): NewQuote, Decimal, Quote, A top-of-book snapshot on its way in. Note what is *not* validated: that the…, Any, Deliberately permitted; see the class docstring in `domain.entities`., TestQuote

### Community 103 - "Kafka Broker Fixtures"
Cohesion: 0.23
Nodes (9): _broker_reachable(), kafka(), build(), prefix(), fixture, The bootstrap address, or a skip with an explanation., A topic namespace unique to one test, deleted afterwards. Without this, a rerun…, The broker actually goes away, and comes back. `DEVELOPMENT_PLAN.md` makes this… (+1 more)

### Community 104 - "Bar Invariant Tests"
Cohesion: 0.23
Nodes (7): _bar(), Any, A bar breaking these is not a bar, and a backtest on it produces a confident…, The most basic incoherence, and the only one that cannot be isolated. ``high <…, Open == high == low == close happens in thin names and must not be rejected., A minute with no trades still produces a bar, carried forward at the last price., TestBar

### Community 105 - "Provider Error Mapping Tests"
Cohesion: 0.15
Nodes (6): Exception, parametrize, The mapping is what decides whether the caller retries or gives up., The WebSocket message shapes, without a WebSocket., A reconnect replays recent prints; they must deduplicate., TestStreamNormalization

### Community 106 - "Frontend NPM Scripts"
Cohesion: 0.15
Nodes (13): scripts, build, check, dev, format, format:check, lint, lint:fix (+5 more)

### Community 107 - "Frontend Health Client"
Cohesion: 0.26
Nodes (7): ApiError, fetchHealth(), HealthResponse, apiUrl(), AppConfig, config, vitest

### Community 108 - "Alembic Migration Tests"
Cohesion: 0.17
Nodes (10): alembic_autogenerate, alembic_config, alembic_migration, alembic_script, _alembic_config(), Connection, Migrations. Three questions, each of which has bitten a real project: 1. Does…, A connection string in a committed file is a credential in a committed file. (+2 more)

### Community 109 - "Price Chart Component"
Cohesion: 0.23
Nodes (10): Bar, ChartPoint, ChartTooltip(), PriceChart(), PriceChartProps, toPoint(), ADR-0009, formatMoney() (+2 more)

### Community 110 - "Market Data Consumer Worker"
Cohesion: 0.24
Nodes (7): MarketDataConsumerService, Event, Consume until cancelled, until ``stop`` is set, or until ``max_batches``.…, Buffer and write one batch, isolating a poison event if the store refuses it.…, Replay a rejected batch one event at a time to find what is actually poison.…, Send one permanently unstorable event to the dead-letter sink. With no sink…, Consume batches of market events, persist them, then acknowledge.

### Community 111 - "Alert and Execution Settings"
Cohesion: 0.20
Nodes (11): AlertSettings, _describe(), _env_var_name(), ExecutionSettings, _Group, ValidationError, Base for every settings group: same prefix, same parsing rules., Trading simulation assumptions (`ARCHITECTURE.md` §16.5). These change the… (+3 more)

### Community 112 - "App Settings Parsing"
Cohesion: 0.29
Nodes (5): AppSettings, Process identity and logging (`ARCHITECTURE.md` §16.9)., MonkeyPatch, A provider without an adapter must fail at startup, not at the first tick., TestParsing

### Community 113 - "Test Engine Fixtures"
Cohesion: 0.20
Nodes (9): engine(), AsyncEngine, Connection, Apply every migration to the connection Alembic is handed. Passing the…, A session-wide engine with the schema built by the real migrations., _run_migrations(), `upgrade → downgrade → upgrade` on a scratch database., The whole chain reverses and reapplies. Runs on its own database so a failure… (+1 more)

### Community 114 - "Schema Completeness Tests"
Cohesion: 0.29
Nodes (5): AsyncEngine, The values_callable in `database/types.py` is easy to omit and invisible…, The migrated schema contains what the architecture says it should., A table nobody designed is either dead weight or a forgotten rename., TestSchemaIsComplete

### Community 115 - "Latest Price Snapshot Tests"
Cohesion: 0.20
Nodes (4): Reads ``/v2/stocks/{symbol}/snapshot``. Not ``/trades/latest``, which returns…, A real answer, not an error., Pre-open, or a symbol with no book. A price without a book is not an error., TestLatestPrice

### Community 116 - "Minute Bar Arithmetic Tests"
Cohesion: 0.27
Nodes (4): 2005 / 20 = 100.25. Not the mean of the prices, which would be 100.33., `NewBar` validates the same invariants the CHECK constraints do. Constructing…, Hand-calculated. Three trades in one minute: ========== ====== ==== ==========…, TestTheBarItself

### Community 117 - "CI Backend Jobs"
Cohesion: 0.25
Nodes (9): backend-integration job (real PostgreSQL service), backend-startup job (documented configuration check), backend-test job (unit and API tests), TradeWatch CI Pipeline, docker job (backend image build), Provider-marked tests excluded from CI, EventPublisher / EventConsumer ports, SyntheticProvider (default TW_PROVIDER=synthetic) (+1 more)

### Community 118 - "Frontend CI and Deferrals"
Cohesion: 0.22
Nodes (9): frontend job (lint, types, vitest, build), Replay from offset as a first-class requirement, bars natural composite primary key (instrument_id, interval, bar_start), Deferred time-series scaling escalation path (D4), ADR-0012: Deterministic event identity for market data, ResponsiveContainer deliberately not used, ADR-0013: Recharts for the price chart, SVG output chosen for jsdom testability (+1 more)

### Community 119 - "ADR Process and Provider Port"
Cohesion: 0.33
Nodes (9): ADR immutability and supersession, ADR-0000: Record architecture decisions, Provider API key confined to the ingestion service, Deferred provider choice (D1), MarketDataProvider protocol, ADR-0004: Abstract the market data provider, ADR-0011: Alpaca as the market data provider, ADR Index (+1 more)

### Community 120 - "Auth and Strategy Decisions"
Cohesion: 0.22
Nodes (9): Refresh/access token emergency denylist (tw:revoked:{jti}), Redis Pub/Sub WebSocket fan-out across API replicas, Bounded Pydantic parameter schema per strategy_type, ADR-0006: Strategies are configuration, not user code, ADR-0010: JWT access tokens with rotating refresh tokens, Refresh token rotation with reuse detection, Ownership enforced in the repository layer, WebSocket connection closed when its token expires (+1 more)

### Community 121 - "Anomaly Detection Scope"
Cohesion: 0.25
Nodes (9): No remote code execution surface, ADR-0008: Anomaly detection scope, method, honest claims, Forbidden claim vocabulary, grep-enforced, IsolationForest model tier with model_versions registry, Model card, Robust z-score baseline (rolling median + MAD), One feature implementation shared by training and serving, Floats allowed only for statistics (+1 more)

### Community 122 - "Alpaca Frame Decoding"
Cohesion: 0.33
Nodes (3): Alpaca batches messages into a JSON array per frame., Alpaca batches messages into one frame., TestFrameDecoding

### Community 123 - "Topic Naming Tests"
Cohesion: 0.22
Nodes (4): `ARCHITECTURE.md` §7.2, literally., The other five in §7.2 have no producer and no consumer yet., Subscribing to its own DLQ is how a consumer builds an infinite loop., TestNaming

### Community 124 - "Market View Tests"
Cohesion: 0.31
Nodes (7): barsFor(), INSTRUMENTS, LATEST, RouteMap, stubApi(), requestUrl(), @testing-library/user-event

### Community 126 - "Anomaly and API Settings"
Cohesion: 0.32
Nodes (5): AnomalySettings, ApiSettings, Detection thresholds (`ARCHITECTURE.md` §16.6, ADR-0008). Every threshold here…, HTTP and WebSocket surface (`ARCHITECTURE.md` §16.9). ``api_host`` defaults to…, TestCrossFieldRules

### Community 127 - "Kafka Admin Client"
Cohesion: 0.25
Nodes (4): AdminClient, NewTopic, Protocol, The slice of ``AIOKafkaAdminClient`` used here, so tests need no broker.

### Community 128 - "Migration Hygiene Tests"
Cohesion: 0.25
Nodes (5): Rules from `alembic/README.md`, enforced rather than hoped for., A downgrade body of `pass` is a migration that cannot be rolled back., Two heads means someone branched the history and nobody merged it., A migration must still run when the model it came from has changed. Importing…, TestMigrationHygiene

### Community 129 - "Shutdown Idempotency Tests"
Cohesion: 0.32
Nodes (4): `close()` writes bars on a path that never commits its offsets. So every trade…, Run until the first write is in flight, then kill the process there., End to end: cancel, close, redeliver everything, and the volume must not move., TestShutdownIsIdempotentToo

### Community 130 - "Prettier Formatting Config"
Cohesion: 0.25
Nodes (7): arrowParens, endOfLine, printWidth, semi, singleQuote, tabWidth, trailingComma

### Community 131 - "Benchmark Honesty Rules"
Cohesion: 0.29
Nodes (7): No Invented Numbers, Benchmarks Directory, Rules for a Benchmark Result, Phase 10 — Observability and Performance Measurement, SQL Rollups to 5m/1h/1d, VWAP Exact for 1m, Null for Rollups, Structured Observability Events and Split Counters

### Community 132 - "UUID Timestamp Extraction"
Cohesion: 0.38
Nodes (4): Extract the embedded Unix-millisecond timestamp from a UUIDv7. Raises:…, timestamp_of(), Reading the leading bits of a v4 would produce a plausible-looking lie., TestTimestampOf

### Community 133 - "Anomaly Detection Tests"
Cohesion: 0.29
Nodes (4): ADR-0008: a detection nobody can explain is a detection nobody should act on., Enforced by a partial unique index, not by whoever writes the activation code., TestDetection, version()

### Community 134 - "Live Provider Smoke Tests"
Cohesion: 0.29
Nodes (7): A week back, so the window contains a trading day whatever day this runs., Needs the market to be open. Skips rather than fails when it is not., test_fetches_historical_bars(), test_lists_instruments(), test_reports_a_current_price(), test_streams_live_trades(), requires_credentials

### Community 135 - "Bar Bucketing Tests"
Cohesion: 0.33
Nodes (3): The SQL rollup anchors at the epoch too. Two anchors would disagree., Everything coarser is rolled up from 1m, never built from trades twice., TestBucketing

### Community 136 - "Bar Validators"
Cohesion: 0.50
Nodes (3): model_validator, Self, A bar must start on a boundary of its own interval. A 1m bar starting at…

### Community 137 - "UTC Datetime Enforcement"
Cohesion: 0.40
Nodes (4): datetime, Reject naive datetimes; normalise aware ones to UTC. A naive datetime in a…, _require_utc(), work()

### Community 138 - "Bar Correction Validators"
Cohesion: 0.50
Nodes (3): model_validator, Self, Checked here as well as in the database. The database protects the data from…

### Community 139 - "Migration Drift Test"
Cohesion: 0.40
Nodes (3): The check that stops silent drift., A migrated database and the ORM models must describe the same schema. If this…, TestMigrationsMatchModels

### Community 140 - "Cascade Delete Tests"
Cohesion: 0.40
Nodes (3): Market data is the system of record. A delisted symbol changes status instead., An audit trail outlives the account it describes; that is the point of it., TestCascades

### Community 141 - "Decimal Precision Storage Tests"
Cohesion: 0.40
Nodes (3): ADR-0009 end to end, at the storage layer., TIMESTAMP WITHOUT TIME ZONE is the bug that shows up twice a year., TestDecimalPrecision

### Community 142 - "Advancing Clock Fixture"
Cohesion: 0.40
Nodes (4): _advancing_clock(), datetime, timedelta, A clock that moves forward one step per call, like a real feed's timestamps.

### Community 146 - "PostgreSQL Enum Helper"
Cohesion: 0.50
Nodes (4): pg_enum(), E, Build a native PostgreSQL enum type for a domain enum. ``values_callable`` is…, SAEnum

### Community 148 - "Frontend Runtime Dependencies"
Cohesion: 0.50
Nodes (4): dependencies, react, react-dom, recharts

### Community 151 - "Validation Error Formatting"
Cohesion: 0.67
Nodes (3): _first_problem(), ValidationError, One readable line from a pydantic error, short enough for a log and a DLQ…

## Ambiguous Edges - Review These
- `Deferred time-series scaling escalation path (D4)` → `ADR-0013: Recharts for the price chart`  [AMBIGUOUS]
  ADR/ADR-0013-recharts-for-the-price-chart.md · relation: conceptually_related_to
- `Redis Pub/Sub WebSocket fan-out across API replicas` → `One WebSocket connection forces singleton ingestion`  [AMBIGUOUS]
  ADR/ADR-0011-alpaca-as-the-market-data-provider.md · relation: conceptually_related_to

## Knowledge Gaps
- **118 isolated node(s):** `tradewatch`, `semi`, `singleQuote`, `trailingComma`, `printWidth` (+113 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1488 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **22 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Deferred time-series scaling escalation path (D4)` and `ADR-0013: Recharts for the price chart`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Redis Pub/Sub WebSocket fan-out across API replicas` and `One WebSocket connection forces singleton ingestion`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `BarInterval` connect `Bar Store Port` to `Kafka Event Envelope`, `Domain Enums and Status`, `Application Ports and Entities`, `Processor Test Harness`, `Live Provider Smoke Tests`, `Bar Aggregator Core`, `In-Flight Deduplication Service`, `Bar Bucketing Tests`, `Event Encoding and Decoding`, `Deterministic Event Identity`, `Provider Configuration Factory`, `Market Data Ingestion Service`, `Historical Backfill Job`, `Bar Natural Key`, `Stream Recording and Replay`, `Port Conformance Tests`, `Market API Route Tests`, `Environment File Loading`, `Fake Kafka Producer`, `Transactional Session Store`, `Bar Entity Persistence`, `SQL Bar Store`, `Data Quality Gap Checks`, `Alpaca HTTP Adapter`, `Alpaca Provider Normalization`, `Kafka Test Support Fakes`, `Database Index Verification`, `Backfill Resume Tests`, `Synthetic Provider Scenarios`, `Market Data Provider Port`, `Session Bound Store`, `API Provider Fixtures`, `Market API Routes`, `Bar Repository`, `Ingestion Integration Tests`, `Synthetic Bar Generation`, `Settings Value Normalizers`, `Alpaca Response Fixtures`, `Bar Query Surface`, `API Response Schemas`, `Cross-Field Settings Validators`, `Bar Invariant Tests`?**
  _High betweenness centrality (0.181) - this node is a cross-community bridge._
- **Why does `MarketEvent` connect `Event Publishing and Consumption` to `Kafka Event Envelope`, `Application Ports and Entities`, `Processor Test Harness`, `Broker Failure Handling Tests`, `Bar Aggregator Core`, `In-Flight Deduplication Service`, `Event Encoding and Decoding`, `Provider Configuration Factory`, `Market Data Ingestion Service`, `Kafka Integration Fixtures`, `Stream Recording and Replay`, `Reconnection and Retry Policy`, `Consumer Commit Semantics`, `Kafka Test Support Fakes`, `Market Data Processor`, `Processing Error Types`, `Session Bound Store`, `Domain Model Base`, `API Provider Fixtures`, `Bar Aggregation Consumer`, `Kafka Client Protocols`, `Ingestion Integration Tests`, `Quote Streaming and Entity`, `Market Data Consumer Worker`?**
  _High betweenness centrality (0.085) - this node is a cross-community bridge._
- **Why does `description` connect `Market API Routes` to `Frontend Lint Config`?**
  _High betweenness centrality (0.076) - this node is a cross-community bridge._
- **Are the 64 inferred relationships involving `BarInterval` (e.g. with `get_bars()` and `BarSeriesResponse`) actually correct?**
  _`BarInterval` has 64 INFERRED edges - model-reasoned connections that need verification._
- **Are the 49 inferred relationships involving `MarketEvent` (e.g. with `BarAggregationService` and `IngestionService`) actually correct?**
  _`MarketEvent` has 49 INFERRED edges - model-reasoned connections that need verification._