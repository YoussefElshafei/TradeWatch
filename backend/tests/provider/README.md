# Live provider tests

**These are the only tests in the repository that touch the network.** Everything else —
including every test of the Alpaca adapter's normalization, retries and error mapping —
runs offline against fakes.

They are marked `provider` and are **deselected by default**, so `pytest` never reaches
them by accident, and CI never depends on a third party's uptime, quota or weekend
maintenance window.

## Running them

```bash
export TW_PROVIDER_API_KEY=...        # Alpaca key id
export TW_PROVIDER_API_SECRET=...     # Alpaca secret
uv run pytest -m provider
```

Without credentials every test skips with a message saying so. That is deliberate: a
skipped test is honest, a silently passing one is not.

## Why they exist at all

The offline tests prove the adapter handles Alpaca's **documented** payload shapes. They
cannot prove the documentation matches what the live service actually sends — field
renames, a changed timestamp format, a new error code. These tests are what closes that
gap, and until somebody with credentials runs them, the adapter is unverified integration
code.

They are deliberately shallow: connect, fetch a little, assert the shape normalizes. They
assert nothing about *values*, because the market decides those, and a test that fails
when the market is closed is a test nobody will trust.

## Recording a fixture

The most valuable thing to do here is capture a real session once and commit it, so
everyone else can replay it forever without credentials:

```python
from tradewatch.infrastructure.providers.replay import capture_stream

await capture_stream(provider.stream_trades(["AAPL"]), "tests/fixtures/alpaca-aapl.jsonl")
```
