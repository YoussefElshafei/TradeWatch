# Documentation

| Where | What |
| --- | --- |
| [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | The system design: components, data flow, schema, topics, API and auth boundaries, testing strategy, failure scenarios. |
| [`../DEVELOPMENT_PLAN.md`](../DEVELOPMENT_PLAN.md) | Phases 0–12, with exit criteria. |
| [`SCHEMA.md`](SCHEMA.md) | The database: tables, indexes, cascades, and what is deliberately left unconstrained. |
| [`PIPELINE.md`](PIPELINE.md) | The vertical slice as built: provider → bus → processor → PostgreSQL → API → chart, and what fails how. |
| [`INGESTION.md`](INGESTION.md) | Market data ingestion: the provider port, the normalized event format, resilience, and deduplication. |
| [`../ADR/`](../ADR/) | Why each decision was made, what else was considered, what it costs. |
| [`../backend/README.md`](../backend/README.md) | Backend package layout and commands. |
| [`../docker/README.md`](../docker/README.md) | The local stack. |
| [`../benchmarks/`](../benchmarks/) | Measured performance results — the only source of any number quoted about this project. |

This directory holds documents that belong to no single component. Phase 8 adds the
anomaly detector's model card here; Phase 10 adds `PERFORMANCE.md`; Phase 11 adds the
chaos runbook.
