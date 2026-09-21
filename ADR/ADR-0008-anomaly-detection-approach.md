# ADR-0008: Anomaly detection — scope, method, and honest claims

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 8

## Context

TradeWatch includes an ML component that flags unusual market activity. Two distinct
problems need deciding: **what to claim** about it, and **how to build it**.

### The claims problem

It is tempting to describe a detector like this as finding "market manipulation", "spoofing",
or "financial crime". That claim would be false, and specifically so:

- There are no labels. No dataset here contains confirmed manipulation cases, so the
  detector has never been evaluated against the thing it would be claiming to find.
- The method is unsupervised. It identifies activity that is statistically unlike a symbol's
  own recent behaviour. Earnings, index rebalances, news, halts, and low-liquidity opens all
  produce exactly that signature, and none of them are crime.
- Real surveillance systems use order-book-level data, cross-venue and cross-account
  linkage, and human review. This system sees public trades and quotes for a handful of
  symbols.

Claiming otherwise in a README or an interview is not an exaggeration; it is a
misrepresentation that a knowledgeable interviewer will identify in one question, and it
would undermine everything else in the project.

### The method problem

The other temptation is to reach straight for a sophisticated model because it sounds more
impressive. A detector that cannot beat a rolling z-score is not an achievement, and without
a baseline there is no way to know whether it does.

## Decision

### Claims

The system detects **statistically unusual market activity relative to a symbol's own recent
behaviour**. That is the claim, everywhere: in code, in the API, in the UI, in documentation,
and in any résumé or interview description of this project.

The following words are not used to describe this component: *manipulation*, *spoofing*,
*layering*, *wash trading*, *fraud*, *crime*, *illegal*, *surveillance*. A grep-based test in
the Phase 8 suite enforces this in the repository.

Naming follows the claim: the field is `anomaly_score`, the event is `anomaly.detected`, the
table is `anomaly_detections`, and the UI says "unusual activity". Every detection surfaced
to a user carries the caveat that unusual activity has many ordinary explanations.

### Method

Two tiers, in this order:

1. **Baseline — robust z-score.** Rolling median and MAD over `TW_ANOMALY_WINDOW_BARS` per
   feature, flagging when a feature exceeds `TW_ANOMALY_BASELINE_Z_THRESHOLD`. Transparent,
   requires no training, and is explainable to a user in one sentence. Ships first, and
   remains the fallback when no model artifact is available (F17).
2. **Model — `IsolationForest`** (scikit-learn), trained offline on historical features,
   versioned in `model_versions` with its training window, hyperparameters, and offline
   metrics, and loaded from an artifact at service startup.

Supporting decisions:

- **Features** — return, realized volatility, volume z-score, trade-count z-score, quoted
  spread — come from **one implementation shared by training and serving**, to avoid
  training/serving skew.
- **Every detection is explainable.** The stored `features` payload, the model version, and
  the threshold crossed are enough to reconstruct why a score was produced. A detection that
  cannot be explained is not shown.
- **The model must beat the baseline, measured on held-out real historical data, and the
  comparison is written down.** If it does not, the baseline stays the default. That is an
  acceptable result and it gets reported as one.
- Thresholds are configuration, not constants — they are guesses until calibrated against
  real data, and must be tunable without a redeploy.
- Evaluation without labels uses what is honestly available: detection rate on injected
  synthetic anomalies, false-positive rate during quiet periods, stability of scores across
  symbols, and a small manually reviewed sample. None of these is accuracy against ground
  truth, and the model card says so.
- A **model card** is written and committed, stating what the detector does, what it does
  not do, how it was evaluated, and its known limitations.

## Alternatives considered

**Supervised classification of manipulation.** Would produce a far stronger claim. Rejected:
there are no labels, and there is no ethical or practical way to manufacture them. A model
trained on synthetic "manipulation" would learn the generator, not the phenomenon.

**Deep learning (autoencoder reconstruction error, LSTM forecasting residuals).** Captures
temporal structure that a z-score cannot, and would be a better showcase of ML skill.
Deferred (D10), not rejected — but only after the baseline's false-positive rate is measured
and found wanting. Starting here would mean no baseline to compare against, a much harder
explainability story, and a heavier training and serving path.

**Order-book microstructure features (order flow imbalance, cancel-to-trade ratios).** These
are the features that would actually be relevant to manipulation-like patterns. Rejected:
they require full depth-of-book data, which the target providers do not offer at a free tier,
and which the architecture does not ingest.

**Skip ML; use fixed thresholds only.** Simple, transparent, and honestly quite effective.
Rejected as the endpoint — but note that it *is* the baseline tier, which ships first and
may well remain the default. The ML tier exists to be compared against it, and that
comparison is the actual engineering content.

## Consequences

**Positive.** The claim matches the evidence, which is defensible under questioning — and
the reasoning behind the restraint is itself a stronger signal than an inflated claim would
be. The baseline gives a real benchmark, so the ML work has a measurable point. Explainable
detections make alerts actionable rather than mysterious. The fallback path means the service
never runs with no detector.

**Negative.** "Detects unusual market activity" is a less exciting headline than "detects
market manipulation". That is the correct trade. Unsupervised detection without labels means
precision and recall cannot be reported honestly, so they are not reported at all — only the
proxy metrics above, with their limitations stated.

**Neutral.** The two-tier design adds a configuration switch (`TW_ANOMALY_DETECTOR`) and a
model registry table.

## Revisit when

- Labelled data becomes genuinely available (a public enforcement dataset, or a supervised
  proxy task that can be defended) — that would justify a different ADR and a different
  claim.
- The baseline's measured false-positive rate makes it unusable, justifying D10.
- Order-book data becomes available, which would change the feature set entirely.
