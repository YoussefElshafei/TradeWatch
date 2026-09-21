# ADR-0000: Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 0

## Context

TradeWatch spans a number of areas — streaming, storage, trading simulation, ML, security,
deployment — where several defensible choices exist for every problem. Without a record,
three things happen: the same arguments get re-litigated months later, the reasoning behind
a constraint is lost and the constraint gets violated by accident, and there is no way for
anyone else to distinguish a deliberate choice from an accident.

This project is also intended to demonstrate engineering judgement. Judgement is not visible
in code; it is visible in the alternatives that were considered and rejected, and why.

## Decision

Architecture decisions are recorded as numbered Markdown files in `ADR/`, following
[TEMPLATE.md](TEMPLATE.md).

An ADR is written when a decision:

- is expensive to reverse (data model, event schema, storage engine, auth model); or
- adds a dependency or a piece of infrastructure; or
- constrains future work (a rule about what must never be done); or
- rejects an obvious alternative, where the rejection would otherwise look like an oversight.

ADRs are immutable once accepted. Changing a decision means writing a new ADR that supersedes
the old one and updating the old one's status. Routine choices — a library for date
formatting, a file layout — do not need an ADR.

`ARCHITECTURE.md` describes the system as it is intended to be; ADRs explain why it is that
way. When they disagree, the ADR with the later date wins and `ARCHITECTURE.md` gets fixed.

## Alternatives considered

**No formal record; rely on commit messages and code comments.** Zero overhead, and the
information is close to the code. Rejected: commit messages record *what changed*, not what
was rejected, and they are effectively unsearchable for "why is it like this" a year later.

**A single long DESIGN_DECISIONS.md.** Easier to skim in one pass. Rejected: it invites
in-place editing, which destroys the history that is the whole point, and it produces merge
conflicts on every parallel change.

**A wiki or issue tracker.** Better cross-linking and discussion. Rejected: the decisions
drift out of sync with the code they govern, and they are not present in a clone of the
repository.

## Consequences

**Positive.** Decisions and their trade-offs are discoverable, versioned with the code, and
reviewable in a pull request. Constraints (for example ADR-0008's rules about claims) have a
citable home.

**Negative.** Writing them takes time, and a stale ADR is worse than none — so status
discipline is required.

**Neutral.** ADR numbers become a shared vocabulary ("that's an ADR-0007 violation").

## Revisit when

The project gains multiple contributors with a formal RFC process, or the ADR set grows
large enough that the index alone is insufficient to navigate it.
