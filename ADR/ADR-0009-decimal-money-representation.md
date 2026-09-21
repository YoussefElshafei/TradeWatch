# ADR-0009: Decimal money, never floats

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 1

## Context

Binary floating point cannot represent most decimal fractions exactly. `0.1 + 0.2` is
`0.30000000000000004`, and that error compounds. In a system that accumulates thousands of
fills into a cash balance and a P&L figure, the result is a balance that is wrong in the
last digits, positions that do not quite close to zero, and equity curves that cannot be
reproduced exactly — which destroys the reproducibility guarantee in ADR-0007.

The error is small and it is invisible in a demo. It is also exactly the kind of defect that
a reviewer with a finance background checks for first, because it is the standard mistake.

There is a complication: the values here are not all currency. Prices can have more decimal
places than cents, quantities can be fractional, and percentages and ratios are genuinely
real-valued. A single rule applied without thought would be wrong in the other direction.

## Decision

**Money and quantities are exact decimals end to end. Floats appear only in statistics.**

| Layer | Representation |
| --- | --- |
| PostgreSQL | `NUMERIC(18, 8)` for prices, quantities, cash, P&L, commission |
| Python domain | `decimal.Decimal`, never `float` |
| Pydantic | `Decimal` fields with explicit `max_digits` / `decimal_places` |
| JSON (API and Kafka) | **strings** — `"142.35"`, never `142.35` |
| TypeScript | `string` at the boundary; formatted for display, and arithmetic only via a decimal library |

Rules:

1. No `float` may reach a monetary value. A lint rule and mypy strict mode on the domain
   package enforce this; `Decimal(str(x))` is the only permitted conversion from a float
   source, and it happens once, at the provider boundary.
2. **JSON carries money as strings.** A JSON number is parsed into an IEEE-754 double by
   every JavaScript client in existence, which silently undoes the exactness the rest of the
   system maintains.
3. **Rounding is explicit, never incidental.** Every rounding point names its mode — prices
   to the instrument's `tick_size`, quantities to `lot_size`, currency to the currency's
   minor unit — using `ROUND_HALF_EVEN` unless a domain rule says otherwise.
4. **Floats are allowed, deliberately, for statistics**: Sharpe ratio, drawdown percentage,
   returns, volatility, anomaly scores, and ML features. These are real-valued measurements
   where the sixteenth decimal place is meaningless, and `Decimal` would only make them slow
   and awkward. The boundary between the two worlds is explicit: statistics are computed
   *from* exact values, and the conversion happens at the point of computation.
5. Aggregation over fills is done in `Decimal`, and the total is never reconstructed from a
   float intermediate.

## Alternatives considered

**Integer minor units (store cents as `int`).** The classic exact-money representation:
fast, unambiguous, impossible to get wrong by accident, and ideal for a currency ledger.
Rejected here because the domain is not only currency. Prices carry more precision than
cents, instruments have varying tick sizes, and quantities may be fractional — so a single
scale factor does not fit, and per-field scale factors are a bug factory. `NUMERIC` +
`Decimal` handles the whole domain with one rule.

**`float` everywhere, rounded at display time.** Fastest, simplest, and what most tutorial
projects do. Rejected: rounding at display hides an error that has already accumulated in
the stored balance. It also makes the exact-reproducibility guarantee of ADR-0007
unachievable, since floating-point accumulation is order-sensitive.

**`NUMERIC` in the database, `float` in Python.** Half-measures like this are common and
worse than either extreme: the database is exact, the application's arithmetic is not, and
the two disagree in ways that are hard to trace.

**A money library (`py-moneyed`) with an attached currency.** Prevents adding USD to EUR at
the type level — genuinely valuable. Rejected for now: the system is single-currency
(`TW_BASE_CURRENCY`) until D15 is decided, so it would add a dependency to prevent an error
that cannot currently occur. It is the right answer the moment multi-currency arrives.

## Consequences

**Positive.** Balances and P&L are exact, so backtests are exactly reproducible and the
golden-backtest test is meaningful. Cross-language transport is safe because money never
becomes a JSON number. Rounding is visible in the code, which means it can be reviewed.

**Negative.** `Decimal` arithmetic is slower than `float` — irrelevant at bar granularity,
and something Phase 10 will measure rather than assume. NumPy and pandas do not work
naturally with `Decimal`, so the boundary into statistical code requires an explicit
conversion, which is deliberate friction at the right place. Frontend arithmetic on money
needs a decimal library rather than `+`.

**Neutral.** `NUMERIC(18, 8)` accommodates crypto-scale precision without a schema change if
D2 is ever decided in that direction.

## Revisit when

Multi-currency support is added (D15) — at which point a money type carrying a currency
becomes worth its dependency. Or a Phase 10 benchmark shows `Decimal` arithmetic is a real
bottleneck in the backtest path, which would justify a float fast-path *for statistics only*,
never for the ledger.
