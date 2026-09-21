# ADR-0006: Strategies are configuration, not user code

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 6

## Context

The requirement is "configurable trading strategies". That admits two very different
readings:

1. Users choose from built-in strategy types and tune their parameters.
2. Users write and upload code that the platform executes.

Reading (2) is more impressive in a demo and is what "configurable" sometimes means in
commercial platforms. It is also arbitrary remote code execution as a product feature. Doing
it safely requires real isolation — a separate runtime, a locked-down container per
execution, a CPU and memory budget, no filesystem, no network, no imports — and getting any
part of that wrong hands an attacker the machine, the database credentials, and the provider
API key.

Reading (1) covers every strategy this project actually needs: moving-average crossovers,
mean reversion, breakouts, volatility filters. What varies between useful strategies is
overwhelmingly parameters and composition, not novel control flow.

## Decision

A **Strategy** is a validated configuration of a built-in strategy type. There is no
mechanism for users to supply executable code, and adding one requires superseding this ADR.

- Each `strategy_type` (`sma_crossover`, `rsi_reversion`, `breakout`, …) has a Pydantic
  parameter schema with **explicit bounds** — a lookback is an integer in a range, not an
  arbitrary integer that can be set to two billion and exhaust memory.
- Parameters are stored as JSONB in `strategies.parameters` and validated on write and again
  on load, because a row can be changed by a migration or by hand.
- The strategy core is a pure function: `evaluate(state, bar) -> (state, signals)`. No I/O,
  no clock, no network, no filesystem.
- There is no `eval`, no `exec`, no dynamic import, and no deserialization of callables
  anywhere in the strategy path. A lint rule enforces this.
- Composition is supported through configuration — combining an entry rule with filters and
  a position-sizing rule — which covers most of what users would otherwise want code for.
- New strategy types are added by the maintainer, with tests, as ordinary code.

## Alternatives considered

**User-uploaded Python, sandboxed with RestrictedPython or an AST allowlist.** Maximum
flexibility, and it keeps everything in one process. Rejected: in-process sandboxing of
Python is widely understood to be unreliable. Escapes via introspection chains are a
recurring finding, and one escape means total compromise. Betting the system's security on
being cleverer than that body of work is not a good bet.

**User-uploaded Python in a per-run isolated container with no network and strict limits.**
This is how it should be done if it is done at all, and it is a genuinely interesting
engineering problem. Rejected for scope: it requires a container orchestration path, an
artifact pipeline, resource quotas, timeouts, output validation, and an ongoing security
posture — a project in itself, and one whose failure mode is severe. Recorded as D17, with
"probably never" attached.

**A small domain-specific language for strategy rules.** No arbitrary execution, more
expressive than fixed parameters, and a fun thing to build. Rejected for now as premature:
it is a parser, an evaluator, an editor experience, and a new source of bugs, all before
anyone has hit the limits of parameterized types. Worth revisiting if users genuinely run
out of expressiveness — that would be evidence, rather than anticipation.

**A visual/block strategy builder.** Nice UX, safe. Rejected as a large frontend project
that adds no new capability over parameterized types plus composition.

## Consequences

**Positive.** No remote code execution surface at all — the single largest security risk in
a platform like this simply does not exist. Strategies are trivially serializable,
diffable, reproducible, and comparable. Parameter validation gives good error messages
before a backtest runs rather than a stack trace during one. Strategy evaluation stays pure,
which is what makes the determinism guarantees in ADR-0007 achievable.

**Negative.** A user with a genuinely novel idea cannot express it without a code change to
the platform. This is a real limitation and it is accepted deliberately: the risk/benefit is
not close. Adding strategy types is maintainer work, so the library grows slowly.

**Neutral.** The parameter schemas become part of the public API contract and drive the
frontend's strategy editor form generation — which is a pleasant side effect.

## Revisit when

There is a concrete, funded need for user-supplied strategies *and* a hard isolation story
(separate runtime, no shared credentials, enforced resource limits, reviewed threat model).
Both conditions, not either.
