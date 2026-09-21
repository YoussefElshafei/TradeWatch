# Documentation

| Where | What |
| --- | --- |
| [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | The system design: components, data flow, schema, topics, API and auth boundaries, testing strategy, failure scenarios. |
| [`../DEVELOPMENT_PLAN.md`](../DEVELOPMENT_PLAN.md) | Phases 0–12, with exit criteria. |
| [`PROJECT_STATE.md`](PROJECT_STATE.md) | **Start here.** What actually exists versus what is only specified, phase by phase: the state of the tree, what is built, what remains, the traps, and the recommended next step. |
| [`SCHEMA.md`](SCHEMA.md) | The database: tables, indexes, cascades, and what is deliberately left unconstrained. |
| [`PIPELINE.md`](PIPELINE.md) | The market data pipeline as built: provider → Kafka → consumer → PostgreSQL → API → chart, with the topology, delivery semantics, idempotency and failure handling. |
| [`BACKFILL.md`](BACKFILL.md) | Historical backfill and the bar aggregator: idempotency, resumability, event-time closing, corrections and rollups. |
| [`INGESTION.md`](INGESTION.md) | Market data ingestion: the provider port, the normalized event format, resilience, and deduplication. |
| [`../ADR/`](../ADR/) | Why each decision was made, what else was considered, what it costs. |
| [`../backend/README.md`](../backend/README.md) | Backend package layout and commands. |
| [`../docker/README.md`](../docker/README.md) | The local stack. |
| [`../benchmarks/`](../benchmarks/) | Measured performance results — the only source of any number quoted about this project. |

This directory holds documents that belong to no single component. Phase 8 adds the
anomaly detector's model card here; Phase 10 adds `PERFORMANCE.md`; Phase 11 adds the
chaos runbook.
