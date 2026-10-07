/** List a user's strategies (Phase 6). Four-states pattern, same as `MarketView.tsx`. */

import { useCallback, useState } from 'react';
import { Link } from 'react-router-dom';

import {
  type Strategy,
  type StrategyList,
  archiveStrategy,
  fetchStrategies,
} from '../api/strategies';
import { useAsync } from '../hooks/useAsync';
import { useAuth } from '../hooks/useAuth';
import { Message } from './MarketView';

export function StrategiesPage() {
  const { accessToken } = useAuth();
  const [archiving, setArchiving] = useState<string | null>(null);
  const { state, reload } = useAsync<StrategyList>(
    useCallback(
      (signal) => fetchStrategies(accessToken ?? '', false, signal),
      [accessToken],
    ),
    [accessToken],
  );

  async function handleArchive(strategy: Strategy): Promise<void> {
    if (!accessToken) return;
    setArchiving(strategy.id);
    try {
      await archiveStrategy(strategy.id, accessToken);
      reload();
    } finally {
      setArchiving(null);
    }
  }

  return (
    <main className="page">
      <header className="page-header">
        <h2>Strategies</h2>
        <Link to="/strategies/new" className="button-primary">
          New strategy
        </Link>
      </header>

      {state.status === 'loading' && <Message tone="info" title="Loading strategies…" />}

      {state.status === 'error' && (
        <Message
          tone="error"
          title="Could not load strategies"
          detail={state.error.message}
          action={{ label: 'Try again', onClick: reload }}
        />
      )}

      {state.status === 'success' && state.data.strategies.length === 0 && (
        <Message
          tone="info"
          title="No strategies yet"
          detail="Create one to run a backtest against it."
        />
      )}

      {state.status === 'success' && state.data.strategies.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Symbols</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {state.data.strategies.map((strategy) => (
              <tr key={strategy.id}>
                <td>{strategy.name}</td>
                <td>{strategy.strategy_type}</td>
                <td>{strategy.symbols.join(', ')}</td>
                <td>
                  <Link to={`/backtests/new?strategyId=${encodeURIComponent(strategy.id)}`}>
                    Backtest
                  </Link>{' '}
                  <button
                    type="button"
                    disabled={archiving === strategy.id}
                    onClick={() => void handleArchive(strategy)}
                  >
                    {archiving === strategy.id ? 'Archiving…' : 'Archive'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
