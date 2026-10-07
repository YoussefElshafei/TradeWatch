/**
 * List a user's backtest runs (Phase 6). Same four-states pattern as `StrategiesPage.tsx`.
 *
 * Added in the frontend design pass: `/backtests/new` and `/backtests/:id` existed, but
 * nothing linked them together or let someone find a past run again — the nav needed a real
 * destination for "Backtests", and `fetchBacktests` already existed to serve it.
 */

import { useCallback } from 'react';
import { Link } from 'react-router-dom';

import { type BacktestRunList, fetchBacktests } from '../api/backtests';
import { useAsync } from '../hooks/useAsync';
import { useAuth } from '../hooks/useAuth';
import { formatPercent, formatTimestamp } from '../lib/format';
import { Message } from './MarketView';

const STATUS_CLASS: Record<string, string> = {
  completed: 'badge-positive',
  failed: 'badge-negative',
  cancelled: 'badge-neutral',
  running: 'badge-info',
  queued: 'badge-neutral',
};

export function BacktestsPage() {
  const { accessToken } = useAuth();
  const { state, reload } = useAsync<BacktestRunList>(
    useCallback((signal) => fetchBacktests(accessToken ?? '', signal), [accessToken]),
    [accessToken],
  );

  return (
    <main className="page">
      <header className="page-header">
        <h2>Backtests</h2>
        <Link to="/backtests/new" className="button-primary">
          New backtest
        </Link>
      </header>

      {state.status === 'loading' && <Message tone="info" title="Loading backtests…" />}

      {state.status === 'error' && (
        <Message
          tone="error"
          title="Could not load backtests"
          detail={state.error.message}
          action={{ label: 'Try again', onClick: reload }}
        />
      )}

      {state.status === 'success' && state.data.backtests.length === 0 && (
        <Message
          tone="info"
          title="No backtests yet"
          detail="Run one against a strategy to see results here."
        />
      )}

      {state.status === 'success' && state.data.backtests.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Symbols</th>
              <th>Window</th>
              <th>Status</th>
              <th>Return</th>
              <th>Submitted</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {state.data.backtests.map((run) => (
              <tr key={run.id}>
                <td>{run.symbols.join(', ')}</td>
                <td>
                  {run.start_date} → {run.end_date}
                </td>
                <td>
                  <span className={`badge ${STATUS_CLASS[run.status] ?? 'badge-neutral'}`}>
                    {run.status}
                  </span>
                </td>
                <td className="figure">
                  {run.metrics ? formatPercent(run.metrics.total_return_pct) : '—'}
                </td>
                <td>{formatTimestamp(run.created_at)}</td>
                <td>
                  <Link to={`/backtests/${run.id}`}>View</Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
