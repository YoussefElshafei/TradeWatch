# Model card: TradeWatch anomaly detection

**Status:** Phase 8. **Decision record:** [ADR-0008](../ADR/ADR-0008-anomaly-detection-approach.md).
**Code:** `backend/src/tradewatch/application/detection/`, `backend/src/tradewatch/workers/anomaly_detector.py`,
`backend/scripts/train_anomaly_model.py`.

---

## The wording constraint (read this first)

**This system detects statistically unusual market activity relative to a symbol's own
recent behaviour. That is the only claim made about it, anywhere — in this document, in
code, in the API, in the UI, and in any description of this project outside the repository.**

The following words are never used to describe this component, in code, comments,
docstrings, UI copy, API responses, commit messages, or documentation: *manipulation*,
*spoofing*, *layering* (in the order-book-manipulation sense — the identical word is used
constantly elsewhere in this codebase to mean *layered architecture*, which is unrelated),
*wash trading*, *fraud*, *crime*, *illegal*, *surveillance*. If you are editing this system
and find yourself reaching for one of those words to explain what it does, stop — it does
not do that, and ADR-0008 explains why claiming otherwise would be a real misrepresentation,
not a rounding error. A test (`backend/tests/unit/test_anomaly_wording_constraint.py`) scans
the repository for exactly this and fails the suite if one of these words appears without a
nearby disclaimer.

Anyone extending this component — a new feature, a new model, a new UI surface — inherits
this constraint. It is not specific to the code that exists today.

---

## What it does

Scores each closed 1-minute bar, per symbol, for how unlike that symbol's own recent history
it is. Two tiers, in the order they were built:

1. **Baseline (ships first, always available):** a rolling, robust z-score. For each feature,
   the current value is compared to the median and median-absolute-deviation (MAD) of that
   feature's own recent window (`TW_ANOMALY_WINDOW_BARS`, `TW_ANOMALY_MIN_BARS_BEFORE_SCORING`
   for how soon scoring starts). Flags when the largest single-feature deviation exceeds
   `TW_ANOMALY_BASELINE_Z_THRESHOLD`. No training, and it is the fallback the service always
   has (see "Failure modes" below).
2. **Model (optional, only if it earned the spot):** an `IsolationForest` (scikit-learn),
   trained offline on historical features and loaded from a versioned artifact. It only ever
   runs in production if an offline comparison (below) showed it detects synthetic anomalies
   at least as well as the baseline, with no worse a false-positive rate. If no such version
   exists, or its artifact is missing or fails to load, the service runs the baseline instead
   and logs a warning — it never runs with no detector at all (`ARCHITECTURE.md`'s F17).

A detection is stored (`anomaly_detections`) and published (`tw.anomalies.v1`) with the score,
the threshold it crossed, the severity, which model produced it, and the full feature vector
that produced the score — see "Explainability" below.

## What it does not do

- It does not detect market manipulation, spoofing, wash trading, fraud, or any other crime.
  There are no labels for any of those in this system, and it has never been evaluated
  against a confirmed case of any of them.
- It is not a surveillance system. It sees the same public trades and bars this project
  ingests for a handful of symbols — not order-book depth, not cross-venue data, not account
  identities.
- It does not explain *why* the market moved. A detection says a bar's behaviour was
  statistically unlike the symbol's own recent past; it says nothing about the cause.
  Earnings, index rebalances, news, a trading halt, and a thin pre-market open all produce
  exactly this signature, and none of them is unusual in the sense a human would mean it.

## Features

One implementation (`application/detection/features.py`), shared by the baseline, the
training pipeline, and the live service, to rule out training/serving skew as a source of a
model that looks good offline and wrong in production:

| Feature | What it measures | Why it is here |
| --- | --- | --- |
| `return` | Simple return, this bar's close vs. the previous | The plainest signal: how far the price moved, one bar to the next |
| `volatility` | Realized volatility (stdev of returns) over the rolling window | A symbol that turns choppy is unusual even when no single return looks extreme |
| `volume_z` | This bar's volume, z-scored against its own rolling window | Raw volume spans orders of magnitude by symbol and time of day; a z-score makes "unusually heavy for *this* symbol *right now*" comparable |
| `trade_count_z` | Same idea, for trade count | Volume and trade count usually move together, but not always — this tells one huge print apart from many small ones |
| `spread` (baseline only — see below) | Quoted bid-ask spread at the bar's close | A spread that widens sharply is a liquidity signal a price-only feature cannot see |

**The model tier trains and scores on four features, not five.** `spread` needs a quote
joined to each bar; the training pipeline (`scripts/train_anomaly_model.py`) does not do that
join, so it is essentially always missing from training rows. The model is trained on
`return`, `volatility`, `volume_z`, `trade_count_z` only
(`application/detection/model.py::MODEL_FEATURE_NAMES`). The baseline is unaffected: it
scores each feature independently, so `spread` still contributes there on any bar where the
live service has a recent quote to supply — an honestly incomplete measurement for the model,
never a fabricated one.

A partial feature vector is normal, not an error: `return`/`volatility` are absent on the
very first bar a symbol is seen, `volume_z`/`trade_count_z` stay absent until their window
fills, and `spread` is absent whenever no quote has been supplied. Both tiers score whatever
is actually present.

## Training data and pipeline

`scripts/train_anomaly_model.py`. For one or more symbols and a date range:

1. Load stored 1-minute bars (`BarRepository.list_range`) — the same interval and the same
   `Bar` shape the live service scores.
2. Replay them through `application/detection/features.py`, exactly as the live service
   would, to build a feature-row dataset.
3. **Split chronologically**, never shuffled: the first `--split-fraction` (default 0.8) of
   each symbol's rows for training, the rest held out. A model trained on data adjacent to
   what it is evaluated on would be evaluated on a much easier problem than serving ever
   poses — this is what "training data separated from evaluation data" means concretely here.
4. Fit an `IsolationForest` (`n_estimators`, `contamination`, `random_state` all
   configurable) on the training split's complete rows.
5. Evaluate (below) and decide whether the new version becomes active.
6. Persist the artifact (`joblib`) and a `model_versions` row recording the training window,
   hyperparameters, and the offline metrics — so six months later, nobody has to guess what
   went into a model that is still running.

**Preprocessing:** none beyond what feature computation already does. No scaling, no
imputation — a row missing a required feature is excluded from training rather than filled
with a guessed value (`application/detection/model.py::to_vector`).

## Evaluation methodology — and why there is no accuracy number

This is unsupervised. There is no labelled dataset of confirmed anomalies in this system, and
manufacturing one (synthetic "manipulation" cases, hand-labelled market moves) would mean
evaluating the model against a generator or a human guess, not against reality — reporting a
precision or recall number from that would be a fabricated confidence in a system that does
not have grounds for it. ADR-0008 rejects that path explicitly.

Instead, both tiers are reduced to the same two numbers on the same held-out data
(`application/detection/model.py::evaluate_against_baseline`):

- **Synthetic-anomaly detection rate.** A random subset of held-out rows has every present
  feature multiplied by a large factor (`inject_synthetic_anomalies`, default 20x on 5% of
  rows) — a deliberately crude, unmistakable synthetic anomaly. The fraction of these a
  detector flags is its detection rate. A method that misses these would not be trusted to
  catch a subtler real one either.
- **False-positive rate.** The fraction of the *unmodified* held-out rows a detector flags.
  A method that flags a large slice of ordinary trading is not usable regardless of how well
  it catches injected spikes.

**The model only goes live if it beats the baseline**, defined as: at least as good a
detection rate, and no worse a false-positive rate, both measured this way on the same
held-out data. If it does not, the trained version is still recorded — the training run is
not lost — but `is_active` stays false and the baseline keeps serving. That is a legitimate,
reportable outcome, not a failure to hide (ADR-0008: "a detector that cannot beat a rolling
z-score is not an achievement").

Two additional evaluation ideas ADR-0008 names — stability of scores across symbols, and a
small manually-reviewed sample — are not yet automated; the two rates above are what
`scripts/train_anomaly_model.py` currently computes and records.

## Explainability

Every stored `anomaly_detections` row, and every published `anomaly.detected` event, carries:

- `anomaly_score` and `threshold` — the number and the line it crossed.
- `severity` — how far past the threshold, not how serious the underlying cause is.
- `model_name` / `model_version` — `baseline-zscore` (with the window size) or the trained
  model's own name and version, so a detection's provenance is never ambiguous.
- `features` — the complete feature vector that produced the score, by name.

That payload alone is enough to answer "why was this flagged" without reference to anything
else. A detection that could not be reconstructed this way is not shown — there is no code
path that stores or publishes a score without its features.

## Known failure modes and limitations

- **Small-window false positives.** With few bars accumulated
  (`TW_ANOMALY_MIN_BARS_BEFORE_SCORING` close to its minimum), a robust z-score estimated
  from a handful of points is itself noisy — a purely ordinary value can spuriously cross the
  threshold by chance. This is expected statistical behaviour at low sample size, not a bug;
  it is exactly what the false-positive-rate metric above exists to measure and report, and
  it is why the default window (`TW_ANOMALY_WINDOW_BARS`) is large enough that this is rare
  in practice.
- **Degenerate zero-variance windows.** If every value in a feature's window is identical
  (constant volume, a session with no price movement), its MAD is zero and there is no scale
  to measure a deviation against. Both the feature-level z-scores and the baseline treat this
  as "not anomalous" rather than dividing by zero or reporting an undefined, arbitrarily huge
  score.
- **A regularly repeating pattern can look unusual.** Median/MAD is a *robust* statistic —
  resistant to a minority of outliers — but a distribution with more than one dense cluster
  (e.g. a price that alternates between exactly two levels) can produce a small MAD estimated
  from the dominant cluster, making the minority cluster's recurring, entirely regular value
  look extreme by comparison. Real, continuous market data does not typically produce this
  shape; a live feed with genuinely bimodal ticks (a stale, throttled, or synthetic feed with
  too few distinct price levels) could.
- **No cross-symbol or cross-venue signal.** Every score is a symbol's activity compared only
  to its own history. Coordinated activity that only looks unusual when compared *across*
  symbols or accounts is invisible to this design by construction — it is not the problem
  this component was built to solve (see "What it does not do").
- **F17 — missing or corrupt model artifact.** Handled by design: the service falls back to
  the baseline and logs a warning rather than failing to start or running with no detector.
  Confirmed in `tests/integration/test_anomaly_detector.py::TestF17Fallback`.
- **Deferred, not rejected (ADR-0008, D10):** a deep-learning detector (autoencoder
  reconstruction error, LSTM forecasting residuals) could capture temporal structure a
  z-score cannot. Deliberately not built yet — only once the baseline's real false-positive
  rate is measured against live data and found wanting, so there remains a real benchmark to
  beat rather than a second sophisticated method with nothing to compare against.
