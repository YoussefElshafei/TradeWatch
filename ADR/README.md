# Architecture Decision Records

Each file records one decision: what was decided, what else was considered, and what it
costs. An ADR is written when a choice is expensive to reverse, when it will otherwise be
re-argued, or when a future reader would reasonably ask "why on earth is it like this?".

ADRs are **immutable once accepted**. A decision that changes gets a new ADR that supersedes
the old one; the old one stays, with its status updated. Deleting history hides the
reasoning, which is the only thing an ADR is for.

## Status values

| Status | Meaning |
| --- | --- |
| Proposed | Written, not yet committed to |
| Accepted | In force |
| Superseded by ADR-NNNN | Replaced; kept for the record |
| Deprecated | No longer relevant, not replaced |

## Index

| ADR | Title | Status |
| --- | --- | --- |
| [0000](ADR-0000-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0001](ADR-0001-kafka-as-the-event-backbone.md) | Kafka as the event backbone | Accepted |
| [0002](ADR-0002-postgresql-as-system-of-record.md) | PostgreSQL as the system of record | Accepted |
| [0003](ADR-0003-redis-scope.md) | Redis: a bounded, justified scope | Accepted |
| [0004](ADR-0004-market-data-provider-abstraction.md) | Abstract the market data provider; default to synthetic | Accepted |
| [0005](ADR-0005-json-event-serialization.md) | JSON + Pydantic event serialization, versioned by topic | Accepted |
| [0006](ADR-0006-strategies-as-configuration.md) | Strategies are configuration, not user code | Accepted |
| [0007](ADR-0007-shared-strategy-execution-core.md) | One strategy and execution core for backtest and paper trading | Accepted |
| [0008](ADR-0008-anomaly-detection-approach.md) | Anomaly detection: scope, method, and honest claims | Accepted |
| [0009](ADR-0009-decimal-money-representation.md) | Decimal money, never floats | Accepted |
| [0010](ADR-0010-authentication-model.md) | JWT access tokens with rotating refresh tokens | Accepted |
| [0011](ADR-0011-alpaca-as-the-market-data-provider.md) | Alpaca as the market data provider (closes D1) | Accepted |
| [0012](ADR-0012-deterministic-event-identity.md) | Deterministic event identity for market data | Accepted |
| [0013](ADR-0013-recharts-for-the-price-chart.md) | Recharts for the price chart (closes D12) | Accepted |

Template: [TEMPLATE.md](TEMPLATE.md)
