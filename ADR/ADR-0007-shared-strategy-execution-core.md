# ADR-0007: One strategy and execution core for backtest and paper trading

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 6–7

## Context

TradeWatch runs the same strategies in two contexts:

- **Backtesting** — historical bars, pulled from PostgreSQL, as fast as possible, in batch.
- **Paper trading** — live bars, arriving from Kafka, in real time.

The naive implementation writes these separately, because their inputs and lifecycles look
different: one is a loop over a query result, the other is a consumer callback. This is the
single most common structural mistake in trading systems, and its consequence is specific
and severe: the backtest and the live path quietly disagree. The backtest says a strategy is
profitable, the paper run says otherwise, and there is no way to tell whether the difference
is market conditions or a bug in one of two implementations of the same idea.

The credibility of every backtest result in this project depends on the answer to "is this
the same code that runs live?".

## Decision

There is **one** strategy implementation and **one** execution simulator. They are driven by
two different drivers.

```
                  ┌─────────────────────────────────────────┐
Historical bars → │  Backtest driver                        │
  (PostgreSQL)    │    forward-only bar iterator            │ ┐
                  └─────────────────────────────────────────┘ │
                                                              ├→ Strategy core
                  ┌─────────────────────────────────────────┐ │   evaluate(state, bar)
Live bars      →  │  Live driver                            │ │        ↓
  (Kafka)         │    consumer, per-deployment state       │ ┘   Execution Simulator
                  └─────────────────────────────────────────┘        ↓
                                                                 Orders / Fills
```

Concretely:

- **Strategy core:** `evaluate(state: StrategyState, bar: Bar) -> (StrategyState, list[Signal])`.
  Pure. No I/O, no clock access, no ability to see any bar later than `bar`. State is
  explicit and serializable, which is what lets a live deployment restart and resume.
- **Execution Simulator:** a library, not a service. Takes an order and market context,
  applies the configured fill model, slippage, and commission, and returns a fill or a
  rejection. Identical inputs produce identical outputs in both contexts.
- **Drivers** own only the differences: where bars come from, how state is persisted, and
  what happens to the emitted orders. They contain no strategy logic and no fill logic.
- **Execution assumptions are recorded per run.** `execution_config` (slippage, commission,
  fill model) is stored on every `backtest_run` and every `strategy_deployment`, so a
  backtest result can never be silently compared against a live result produced under
  different assumptions.
- **The equivalence is a test, not a claim.** Phase 7's consistency test replays the same
  bars through both drivers and asserts the fill sequences are identical. It is a gate: if
  it fails, the divergence is fixed in the shared core, never patched on one side.

## Alternatives considered

**Separate backtest and live implementations, kept in sync by discipline.** Each can be
optimized for its own shape — a tight vectorized loop for backtests, an event handler for
live. Rejected: "kept in sync by discipline" fails, and its failure is silent. The
performance argument is also weak here, because bar-level backtests over this project's data
volumes are not the bottleneck; if they ever become one, the fix is a faster driver, not a
second strategy implementation.

**Vectorized backtesting (pandas over the whole series at once).** Much faster, and the
standard approach in quant research. Rejected: it makes lookahead bias easy to introduce
accidentally — one careless `shift()` and the strategy trades on tomorrow's close — and it
cannot share code with an event-driven live path. Determinism and honesty about lookahead
matter more here than backtest wall-clock time.

**Run the live path against a replayed historical feed to "backtest".** Perfect fidelity,
because it is literally the same path. Rejected as the primary mechanism: it requires the
whole Kafka stack for a research query, is slow, and makes a parameter sweep impractical. It
remains, however, exactly what the Phase 7 consistency test does, as a verification
technique.

## Consequences

**Positive.** A backtest result is meaningful, because the code that produced it is the code
that runs live. Strategy logic is unit-testable as a pure function with no infrastructure at
all. Execution assumptions are explicit, stored, and comparable. Reproducibility is
achievable: same bars, same parameters, same config → identical results, enforced by the
golden backtest.

**Negative.** The backtester is slower than a vectorized implementation, bar by bar — an
acceptable cost, and one that Phase 10 will measure rather than assume. The shared core must
serve both contexts, so neither gets a context-specific optimization without care. Strategy
state must be explicitly modelled and serializable, which is more work than closing over
local variables.

**Neutral.** The driver boundary becomes a natural extension point: a walk-forward driver or
a parameter-sweep driver can be added later without touching strategy or fill logic.

## Revisit when

Backtest throughput, measured in Phase 10, becomes a real constraint on research. The
response is a faster driver or parallelism across symbols and parameter sets — not a second
strategy implementation. That distinction is the whole point of this ADR.
