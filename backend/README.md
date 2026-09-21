# TradeWatch — backend

Python services: the FastAPI gateway, the workers, and the shared domain/application code.

See the repository [README](../README.md) for setup, and
[ARCHITECTURE.md](../ARCHITECTURE.md) for what each package is for.

## Layout

| Package | Responsibility |
| --- | --- |
| `tradewatch.domain` | Entities and pure business rules. Imports nothing from the rest of the system, and no framework. |
| `tradewatch.application` | Use cases orchestrating the domain. Depends on ports, never on concrete infrastructure. |
| `tradewatch.infrastructure` | Adapters: database, cache, event bus, market data providers. |
| `tradewatch.api` | FastAPI gateway — HTTP and WebSocket. |
| `tradewatch.workers` | Long-running consumers and jobs. |
| `tradewatch.config` | Typed settings (`ARCHITECTURE.md` §16). |
| `tradewatch.observability` | Logging, and later metrics and tracing. |

The layering is enforced by `import-linter`, configured in `pyproject.toml` and run in CI —
a violation fails the build rather than being noticed in review.

## Commands

```bash
uv sync --all-groups     # install
uv run pytest            # tests
uv run ruff check .      # lint
uv run ruff format .     # format
uv run mypy              # types
uv run lint-imports      # layering
```
