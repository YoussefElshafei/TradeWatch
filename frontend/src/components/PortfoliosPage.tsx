/** List a user's portfolios and create new ones (Phase 7). Four-states pattern, same as `StrategiesPage.tsx`. */

import { useCallback, useState, type SubmitEvent } from 'react';
import { Link } from 'react-router-dom';

import {
  ApiError,
  type PortfolioList,
  createPortfolio,
  fetchPortfolios,
} from '../api/portfolios';
import { useAsync } from '../hooks/useAsync';
import { useAuth } from '../hooks/useAuth';
import { formatMoney } from '../lib/format';
import { Message } from './MarketView';

export function PortfoliosPage() {
  const { accessToken } = useAuth();
  const { state, reload } = useAsync<PortfolioList>(
    useCallback((signal) => fetchPortfolios(accessToken ?? '', signal), [accessToken]),
    [accessToken],
  );

  const [name, setName] = useState('');
  const [baseCurrency, setBaseCurrency] = useState('USD');
  const [initialCash, setInitialCash] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: SubmitEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setError(null);

    if (!accessToken) {
      setError('You must be signed in.');
      return;
    }

    setSubmitting(true);
    try {
      await createPortfolio(
        { name, base_currency: baseCurrency, initial_cash: initialCash },
        accessToken,
      );
      setName('');
      setInitialCash('');
      reload();
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause.message
          : 'Something went wrong. Please try again.',
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="app">
      <header className="app-header">
        <h1>Portfolios</h1>
      </header>

      <form className="auth-form" onSubmit={(event) => void handleSubmit(event)}>
        <label htmlFor="portfolio-name">Name</label>
        <input
          id="portfolio-name"
          type="text"
          required
          maxLength={100}
          value={name}
          onChange={(event) => {
            setName(event.target.value);
          }}
        />

        <label htmlFor="portfolio-currency">Base currency</label>
        <input
          id="portfolio-currency"
          type="text"
          required
          maxLength={3}
          value={baseCurrency}
          onChange={(event) => {
            setBaseCurrency(event.target.value.toUpperCase());
          }}
        />

        <label htmlFor="portfolio-cash">Initial cash</label>
        <input
          id="portfolio-cash"
          type="number"
          required
          min="0"
          step="0.01"
          value={initialCash}
          onChange={(event) => {
            setInitialCash(event.target.value);
          }}
        />

        {error && (
          <div className="panel panel-error" role="alert">
            <p className="panel-title">{error}</p>
          </div>
        )}

        <button type="submit" disabled={submitting}>
          {submitting ? 'Creating…' : 'Create portfolio'}
        </button>
      </form>

      {state.status === 'loading' && <Message tone="info" title="Loading portfolios…" />}

      {state.status === 'error' && (
        <Message
          tone="error"
          title="Could not load portfolios"
          detail={state.error.message}
          action={{ label: 'Try again', onClick: reload }}
        />
      )}

      {state.status === 'success' && state.data.portfolios.length === 0 && (
        <Message
          tone="info"
          title="No portfolios yet"
          detail="Create one to deploy a strategy against it."
        />
      )}

      {state.status === 'success' && state.data.portfolios.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Cash balance</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {state.data.portfolios.map((portfolio) => (
              <tr key={portfolio.id}>
                <td>{portfolio.name}</td>
                <td>
                  {formatMoney(portfolio.cash_balance)} {portfolio.base_currency}
                </td>
                <td>
                  <Link to={`/portfolios/${encodeURIComponent(portfolio.id)}`}>View</Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
