/**
 * The anomaly feed (Phase 8, ADR-0008): the frontend deliverable `DEVELOPMENT_PLAN.md`
 * committed to and Phase 8 shipped without — an anomaly feed, score history, and a
 * per-detection explanation panel, all against data `GET /api/v1/anomalies` already returns.
 *
 * Deliberately plain: same four-states discipline and the same unstyled table/panel
 * conventions as `AlertCenter.tsx`, not a design pass. Filtering by symbol *is* the score
 * history view — the feed is already newest-first, so narrowing it to one symbol reads as
 * that symbol's own timeline, without a second component or a charting dependency this
 * phase does not need.
 */

import { useCallback, useState } from 'react';

import { type AnomalyDetection, fetchAnomalies } from '../api/anomalies';
import { fetchInstruments } from '../api/market';
import { useAsync } from '../hooks/useAsync';
import { Message } from './MarketView';

function ExplanationPanel({ detection }: { detection: AnomalyDetection }) {
  const features = Object.entries(detection.features);

  return (
    <section aria-labelledby="explanation-heading" className="panel panel-info">
      <h3 id="explanation-heading">Why was this flagged?</h3>
      <p>
        {detection.symbol} scored <strong>{detection.anomaly_score}</strong> against a
        threshold of {detection.threshold} ({detection.model_name} {detection.model_version}
        ).
      </p>
      <p>
        Window: {new Date(detection.window_start).toLocaleString()} –{' '}
        {new Date(detection.window_end).toLocaleString()}
      </p>
      {features.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Feature</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {features.map(([name, value]) => (
              <tr key={name}>
                <td>{name}</td>
                <td>{value}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <p className="panel-detail">
        Statistically unusual activity has many ordinary explanations — earnings, index
        rebalances, news, a trading halt, or a thin pre-market open all produce this same
        signature. This is not a claim of market manipulation, fraud, or financial crime
        (ADR-0008).
      </p>
    </section>
  );
}

export function AnomalyFeed() {
  const [symbol, setSymbol] = useState('');
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const { state: instrumentsState } = useAsync(
    useCallback((signal) => fetchInstruments(signal), []),
    [],
  );

  const { state, reload } = useAsync(
    useCallback(
      (signal) => fetchAnomalies({ ...(symbol && { symbol }), limit: 100 }, signal),
      [symbol],
    ),
    [symbol],
  );

  const selected =
    state.status === 'success'
      ? state.data.detections.find((detection) => detection.id === selectedId)
      : undefined;

  return (
    <main className="app">
      <header className="app-header">
        <h1>Anomaly feed</h1>
      </header>

      <section aria-labelledby="filter-heading">
        <h2 id="filter-heading">Filter</h2>
        <label>
          Symbol (blank = every symbol, newest first)
          <select
            value={symbol}
            onChange={(event) => {
              setSymbol(event.target.value);
              setSelectedId(null);
            }}
          >
            <option value="">All symbols</option>
            {instrumentsState.status === 'success' &&
              instrumentsState.data.instruments.map((instrument) => (
                <option key={instrument.symbol} value={instrument.symbol}>
                  {instrument.symbol}
                </option>
              ))}
          </select>
        </label>
      </section>

      <section aria-labelledby="feed-heading">
        <h2 id="feed-heading">{symbol ? `${symbol} score history` : 'All detections'}</h2>

        {state.status === 'loading' && <Message tone="info" title="Loading detections…" />}
        {state.status === 'error' && (
          <Message
            tone="error"
            title="Could not load the anomaly feed"
            detail={state.error.message}
            action={{ label: 'Try again', onClick: reload }}
          />
        )}
        {state.status === 'success' && state.data.detections.length === 0 && (
          <Message
            tone="info"
            title="No detections"
            detail={symbol ? `Nothing unusual for ${symbol} yet.` : 'Nothing unusual yet.'}
          />
        )}
        {state.status === 'success' && state.data.detections.length > 0 && (
          <table>
            <thead>
              <tr>
                <th>Detected</th>
                <th>Symbol</th>
                <th>Score</th>
                <th>Threshold</th>
                <th>Severity</th>
                <th>Model</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {state.data.detections.map((detection) => (
                <tr key={detection.id}>
                  <td>{new Date(detection.detected_at).toLocaleString()}</td>
                  <td>{detection.symbol}</td>
                  <td>{detection.anomaly_score}</td>
                  <td>{detection.threshold}</td>
                  <td>{detection.severity}</td>
                  <td>
                    {detection.model_name} {detection.model_version}
                  </td>
                  <td>
                    <button
                      type="button"
                      onClick={() => {
                        setSelectedId(detection.id);
                      }}
                    >
                      Explain
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {selected && <ExplanationPanel detection={selected} />}
    </main>
  );
}
