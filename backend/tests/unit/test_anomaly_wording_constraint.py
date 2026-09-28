"""ADR-0008's wording constraint, enforced mechanically (DEVELOPMENT_PLAN.md's Phase 8 exit
criterion): "No text anywhere in the code, UI, docs, or commit messages claims detection of
manipulation, fraud, or financial crime."

**The rule is about claims, not mentions.** ADR-0008 itself, `ARCHITECTURE.md`, `README.md`,
and this project's own UI (`Dashboard.tsx`) all correctly *name* these words — to disclaim
them ("does not detect market manipulation, fraud, or financial crime"). Banning the words
outright would fail against that already-correct, already-reviewed content. What must never
happen is one of these words appearing as a **positive claim** with no nearby negation — a
new field called `manipulation_score`, a log line saying "fraud detected", a UI string
claiming this system catches spoofing. So a match only fails this test when no negation cue
(`not`, `never`, `n't`, `no `, `nothing`, `without`) appears within a generous window of
characters around it — wide enough to cover a markdown table cell or a wrapped sentence,
without being so wide that an unrelated disclaimer on the other side of a file could
launder an unrelated bad claim (a real risk this test accepts as a tradeoff for staying a
simple, dependency-free grep rather than an NLP claim classifier).

**`layering` is excluded.** ADR-0008 bans it as a manipulation technique (order-book
layering), but this codebase's own architecture vocabulary uses the identical word
constantly and unrelatedly — "a layering violation" in the import-linter sense, "layered
architecture" in `ARCHITECTURE.md`'s own title. Scanning for it here would test whether this
codebase talks about its own layers, not whether it overclaims about markets.

**Commit messages are not scanned here.** Past commits are immutable, so a test re-run today
cannot retroactively fix one, and this suite runs against a working tree, not `git log`. The
discipline for a new commit's own wording is applied when it is written, not caught after
the fact by a pytest run.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[3]
_BACKEND_ROOT = Path(__file__).resolve().parents[2]

#: ADR-0008's own list, minus `layering` — see the module docstring.
_FORBIDDEN_WORDS = (
    "manipulation",
    "spoofing",
    "wash trading",
    "fraud",
    "crime",
    "illegal",
    "surveillance",
)
_WORD_PATTERN = re.compile("|".join(re.escape(word) for word in _FORBIDDEN_WORDS), re.IGNORECASE)
_NEGATION_CUES = ("not", "never", "n't", "no ", "nothing", "without")
#: Characters of context on each side of a match — wide enough for a markdown table row or a
#: wrapped sentence, per the module docstring.
_WINDOW = 220

#: Files whose entire purpose is discussing this constraint at a level of nuance the
#: negation-window heuristic cannot fully parse (a "that claim would be false" a few
#: sentences later, a risk-table cell naming the trigger word with no negation in the same
#: cell) — reviewed by a human rather than this mechanical check, the same reason ADR-0008
#: itself says a grep test enforces this "in the repository" right after using every one of
#: these words itself. `ARCHITECTURE.md`, `README.md`, and `learning.md` are deliberately
#: *not* here: their existing disclaimers already satisfy the heuristic on their own, so
#: exempting them would only make this test weaker for no reason.
_EXEMPT_FILES = frozenset(
    {
        _REPO_ROOT / "ADR" / "ADR-0008-anomaly-detection-approach.md",
        _REPO_ROOT / "DEVELOPMENT_PLAN.md",
        _REPO_ROOT / "docs" / "MODEL_CARD.md",
        Path(__file__).resolve(),
    }
)

_SCAN_GLOBS = (
    (_BACKEND_ROOT / "src", "**/*.py"),
    (_BACKEND_ROOT / "scripts", "**/*.py"),
    (_BACKEND_ROOT / "tests", "**/*.py"),
    (_REPO_ROOT / "frontend" / "src", "**/*.ts"),
    (_REPO_ROOT / "frontend" / "src", "**/*.tsx"),
    (_REPO_ROOT, "*.md"),
    (_REPO_ROOT / "ADR", "*.md"),
    (_REPO_ROOT / "docs", "*.md"),
)
_EXCLUDED_DIR_PARTS = frozenset({"__pycache__", "node_modules", ".venv"})


def _candidate_files() -> list[Path]:
    files: list[Path] = []
    for root, pattern in _SCAN_GLOBS:
        if not root.exists():
            continue
        for path in root.glob(pattern):
            if path in _EXEMPT_FILES:
                continue
            if any(part in _EXCLUDED_DIR_PARTS for part in path.parts):
                continue
            files.append(path)
    return files


def _unguarded_matches(text: str) -> list[str]:
    """Every forbidden-word match with no negation cue in its surrounding window."""
    findings = []
    for match in _WORD_PATTERN.finditer(text):
        start = max(0, match.start() - _WINDOW)
        end = min(len(text), match.end() + _WINDOW)
        window = text[start:end].lower()
        if not any(cue in window for cue in _NEGATION_CUES):
            line_number = text.count("\n", 0, match.start()) + 1
            findings.append(f"line {line_number}: {match.group()!r}")
    return findings


class TestNoOverclaiming:
    def test_no_unguarded_forbidden_word_in_source_or_docs(self) -> None:
        violations: dict[str, list[str]] = {}
        for path in _candidate_files():
            text = path.read_text(encoding="utf-8")
            found = _unguarded_matches(text)
            if found:
                violations[str(path.relative_to(_REPO_ROOT))] = found

        assert not violations, (
            "ADR-0008: these words appear with no nearby negation, which reads as a "
            "claim rather than a disclaimer:\n"
            + "\n".join(f"  {file}: {items}" for file, items in violations.items())
        )

    def test_the_scan_actually_covers_something(self) -> None:
        """A silently-empty file list would make the test above pass for the wrong reason."""
        assert len(_candidate_files()) > 50

    def test_the_negation_heuristic_is_not_vacuous(self) -> None:
        """Sanity-checks the heuristic itself against known-shapes, so a change to the
        window size or cue list cannot silently stop testing anything.
        """
        assert _unguarded_matches("this system does not detect fraud.") == []
        assert _unguarded_matches("this system detects fraud.") == ["line 1: 'fraud'"]
