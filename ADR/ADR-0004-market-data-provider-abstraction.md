# ADR-0004: Abstract the market data provider; default to synthetic

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 1 (port + synthetic), 3 (real adapter)

## Context

The system's entire input is an external market data provider. Every candidate provider —
Finnhub, Alpaca, Polygon, Twelve Data — has a different payload shape, a different
subscription protocol, a different rate limit, different symbol coverage, and different
licensing terms about storing and redisplaying data. Free tiers change without notice, and
some providers do not permit persistence of their data at all.

If provider details leak into the domain, four bad things follow: development requires
network access and a valid API key; tests become non-deterministic and rate-limited;
switching providers becomes a rewrite; and CI depends on a third party's uptime.

There is also a timing problem. Choosing a provider well requires knowing what the system
needs from it, which is not knowable before the system exists. Choosing early means choosing
badly.

## Decision

All market data enters through a single port:

```python
class MarketDataProvider(Protocol):
    async def list_instruments(self) -> list[Instrument]: ...
    async def fetch_bars(self, symbol, interval, start, end) -> list[Bar]: ...
    def stream_trades(self, symbols) -> AsyncIterator[Trade]: ...
    def stream_quotes(self, symbols) -> AsyncIterator[Quote]: ...
```

Only the ingestion service imports an implementation. Nothing else in the system knows a
provider exists.

Two implementations:

1. **`SyntheticProvider` (the default, `TW_PROVIDER=synthetic`)** — a seeded generator
   producing trades and quotes from a random walk with configurable volatility and volume
   profile, plus deterministic replay of recorded fixture files. It can also inject
   scenarios on demand: volume spikes, price gaps, spread blow-outs, connection drops, and
   malformed messages. It is the input for all development, all CI, all E2E tests, and all
   load tests.
2. **One real adapter**, added in Phase 3, selected by configuration.

**Choosing the real provider is deliberately deferred (D1).** It is decided before Phase 3
on written criteria: free-tier WebSocket support, symbol coverage for the target watchlist,
historical bar depth, rate limits, and — non-negotiable — licensing that permits storing and
redisplaying the data in this application.

The provider API key lives only in the ingestion service. No other component can reach the
provider even if compromised.

## Alternatives considered

**Pick a provider now and code against its SDK directly.** Fastest path to real data, and
no abstraction to design. Rejected: it couples the entire project to a third-party free tier
whose terms may change, makes CI depend on network and quota, makes tests non-deterministic,
and converts "switch provider" into "rewrite ingestion".

**Abstract, but make a recorded real-data fixture the default test input instead of a
generator.** Realistic data, no invented distributions. Rejected as the *only* mechanism —
though fixtures are supported and used — because a fixture cannot be asked for a volume
spike at bar 300, cannot be scaled up for a load test, and cannot generate ten years of
history. Both mechanisms ship; they serve different purposes.

**Multiple real providers behind the port from the start, with failover.** Resilience, and
it proves the abstraction. Rejected: doubles integration work and reconciliation complexity
(two providers disagree about prices, and resolving that is its own project) before one
provider has been shown to work.

## Consequences

**Positive.** Development and CI run with no API key, no network, no quota, and no
flakiness. Tests are deterministic given a seed. Failure scenarios (F1, F2, F3) can be
injected on demand rather than waited for. The provider choice can be made late, on
evidence. Provider swap is one adapter and one config value.

**Negative.** Synthetic data is not real data: its distributions are invented, so a strategy
or detector that works on it proves very little about real markets. Phase 3's exit criteria
address this by requiring the real adapter to be invisible above the port, and Phase 8's
model evaluation is done on real historical data, never synthetic. There is also a risk of
the port's design being shaped by the synthetic implementation; the Phase 3 exit criterion
("swapping providers changes nothing above the port") is the check on that.

**Neutral.** The port's surface is the union of what providers offer, so it is deliberately
narrow — anything one provider offers and another does not stays out of the port until a
concrete need appears.

## Revisit when

- The real provider is chosen (D1) — record that choice in its own ADR, with the criteria
  it was judged against.
- A second asset class is added (D2), which may require extending the port with session and
  contract metadata.
