# ADR-0013: Recharts for the price chart

- **Status:** Accepted
- **Date:** 2026-09-21
- **Phase:** 1
- **Closes:** decision D12 in `ARCHITECTURE.md` §15

## Context

`ARCHITECTURE.md` §15 deferred the charting library with the note *"a prototype answers this
faster than an argument"*, and `DEVELOPMENT_PLAN.md` makes closing D12 a Phase 1 exit
criterion. Phase 1 now has a chart, so the decision is due.

What the chart actually has to do at this point is narrow: draw a close-price line over a
few hundred minute bars, show OHLCV on hover, and — this turned out to be the deciding
constraint — **be assertable in a test**. The end-to-end requirement for this phase is that
data reaching the screen is verified automatically, not by someone looking at it.

## Decision

**Recharts**, rendering SVG, with explicit numeric dimensions.

`ResponsiveContainer` is deliberately not used. It sizes the chart from a `ResizeObserver`,
which jsdom does not implement, so the chart renders as a 0×0 SVG — and a test asserting
that data reached the screen passes while showing nothing. Fixed dimensions with a
horizontally scrolling wrapper keep the component genuinely testable. Making it responsive
means adding a `ResizeObserver` polyfill to the test setup, which is worth doing when the
real dashboard is built and is not worth doing to close this slice.

Prices are converted from string to number **only at the point of drawing**, never fed back,
and the tooltip displays the original string (ADR-0009).

## Alternatives considered

**TradingView Lightweight Charts.** Purpose-built for exactly this: candlesticks, crosshair,
volume panes, and smooth interaction over a hundred thousand points, in about 45 KB. It is
the better library for the dashboard TradeWatch is eventually meant to have, and it is the
obvious choice for a trading UI.

Rejected for now on testability. It draws to a `<canvas>`, so in jsdom there is nothing to
assert against — `getByLabelText('AAPL price chart')` finds an empty canvas whether the data
arrived or not. Verifying it would mean Playwright and visual snapshots, which is Phase 11
infrastructure, and this phase's job is to prove the data path with automated tests rather
than to build the best-looking chart. Its imperative ref-and-effect API is also a poorer fit
for React than a declarative one.

**visx.** The most flexible option and the nicest primitives; also the most code. A price
chart in visx means composing scales, axes, a line path and tooltip positioning by hand.
That is the right trade when a design demands something no charting library provides, and it
is a lot of hand-written geometry to prove that a JSON response reached a browser.

**A hand-rolled SVG line chart.** Genuinely tempting — an SVG polyline from a price series is
about thirty lines, adds no dependency, and is trivially testable. Rejected because axes,
tick selection, tooltips and hover hit-testing are the other ninety percent, and all of them
are solved problems that get re-solved badly.

**Chart.js.** Popular and well documented, but canvas-based like Lightweight Charts, so it
fails the same test, and its React wrapper is a third-party layer over an imperative API.

## Consequences

**Positive.** SVG output means `MarketView.test.tsx` can assert that a line path exists with
the right number of points — the end-to-end claim is verified rather than asserted.
Declarative React components compose normally. Recharts is widely used and unlikely to be
abandoned.

**Negative.** Recharts is the largest dependency in the frontend and pushes the bundle past
Vite's 500 KB warning; that is acceptable for an internal dashboard and would need
code-splitting before anything public. SVG scales worse than canvas — a few thousand points
is fine, a hundred thousand is not, which matters the day someone charts a year of minute
bars. It has no candlestick primitive, so OHLC bars need a custom shape. And the fixed
dimensions are a real limitation, traded knowingly for a test that means something.

**Neutral.** The chart is one component behind one prop interface. Swapping it later is a
contained change, which is most of why choosing imperfectly now is safe.

## Revisit when

- The dashboard is designed for real, and candlesticks, volume panes and a crosshair stop
  being nice-to-haves. That is the moment for Lightweight Charts, alongside the Playwright
  suite from Phase 11 that can actually verify a canvas.
- A chart needs more points than SVG renders comfortably.
- Bundle size becomes a measured problem rather than a warning.
