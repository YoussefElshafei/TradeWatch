# ADR-0011: Alpaca as the market data provider

- **Status:** Accepted
- **Date:** 2026-09-19
- **Phase:** 1 (adapter written), 3 (validated against the live service)
- **Closes:** decision D1 in `ARCHITECTURE.md` §15

## Context

ADR-0004 put every provider behind a port and deferred the choice of which one, on the
grounds that choosing well requires knowing what the system needs and that is not knowable
before the system exists. The persistence layer and the ingestion subsystem now exist, so
the requirements are concrete:

- **Streaming trades over a free tier.** The live path is the point of the project; a
  provider whose free tier is end-of-day snapshots cannot exercise it.
- **Historical bars deep enough to backtest.** Phase 6 needs months of 1m bars.
- **A licence permitting storage and redisplay.** `ARCHITECTURE.md` makes PostgreSQL the
  system of record, which means the data is *stored*. Several free tiers forbid that, and a
  provider whose terms we would be violating is not a provider.
- **Coverage of the configured watchlist** — large-cap US equities and ETFs.
- **A documented rate limit**, so the client-side limiter can stay under it rather than
  discovering it with a 429 (failure F2).

## Decision

**Alpaca Market Data API v2** is the real provider, implemented in
`tradewatch.infrastructure.providers.alpaca`. `TW_PROVIDER=synthetic` remains the default
for development and CI (ADR-0004); `alpaca` is opt-in and requires
`TW_PROVIDER_API_KEY` and `TW_PROVIDER_API_SECRET`.

Specifics that follow from the choice:

- The **IEX feed** on the free tier. Single-venue, so prices are IEX's rather than the
  consolidated tape — fine for a simulation, and a fact that must accompany any number
  quoted from it.
- **One WebSocket connection**, with trades and quotes multiplexed over it, because a
  second connection displaces the first. The ingestion service therefore runs as a
  singleton, which is worth knowing before Phase 12 scales anything.
- **Timeframes** `1Min`, `5Min`, `1Hour`, `1Day` map directly to `BarInterval`; anything
  else is rolled up from 1m locally.
- The client-side limiter is set to the documented free-tier ceiling.

**This adapter has not been run against the live service.** It is written to Alpaca's
documented shapes, and every test drives it with constructed payloads, because no
credentials exist in the environment where it was written. `tests/provider/` holds the
tests that close that gap, marked `provider` and skipped without credentials. Until someone
runs them, this is unverified integration code and should be described that way.

## Alternatives considered

**Finnhub.** Free WebSocket trades for US equities and a generous request limit, and the
API is pleasant. Rejected because its historical candle endpoints moved behind a paid plan,
which removes the backtesting half of the requirement — and a provider that can stream but
not backfill would mean two providers, which is the complexity ADR-0004 was avoiding.

**Polygon.io.** The best data quality of the candidates, with genuine consolidated-tape
coverage. Rejected on the free tier: end-of-day only, five requests per minute, and no
WebSocket. Excellent if the project had a budget; it does not.

**Twelve Data.** Broad asset coverage including FX and crypto, which would make decision D2
cheaper later. Rejected because WebSocket streaming is a paid feature, and 800 requests per
day is thin for backfilling minute bars.

**Yahoo Finance via `yfinance`.** Free, no key, widely used. Rejected on licensing: it is an
unofficial scrape of an endpoint with no terms permitting storage or redisplay, and
`ARCHITECTURE.md` stores everything. Building on it would mean the project's central data
store is populated in violation of someone's terms of service, which is not a trade-off
worth making for a portfolio piece.

**Two providers with failover.** Resilience, and it would prove the port. Rejected: two
integrations to maintain, and reconciling disagreements between two feeds about the same
symbol is its own project. The port means a second provider is an adapter away if one is
ever needed.

## Consequences

**Positive.** One provider covers streaming, historical bars, latest price, and the
instrument universe, so there is no seam between "the streaming provider" and "the history
provider". The free tier is usable without a card. Alpaca's paper-trading orientation
matches a platform that simulates trading. Terms permit storage.

**Negative.** IEX is a single venue with perhaps a few percent of consolidated volume, so
prices are thinner and less representative than the tape — this matters most for Phase 8's
detector, whose features are volume-based, and it belongs in the model card. One concurrent
connection constrains the deployment topology. The free tier can change, which is why the
port exists and why the synthetic provider remains the default. And the adapter is
currently unverified against the live API, which is the honest state of it.

**Neutral.** Adds `httpx2` and `websockets` as runtime dependencies; both were already
present transitively.

## Revisit when

- The live provider tests are run and disagree with the adapter — then fix the normalizers
  and note what the documentation got wrong.
- The free tier changes materially, or IEX-only pricing turns out to distort the anomaly
  detector's features enough to matter (Phase 8).
- Decision D2 adds an asset class Alpaca does not serve.
