/**
 * A portfolio's cash, positions, and strategy deployments (Phase 7 — the "Portfolio UI").
 *
 * Four-states pattern, same as `StrategiesPage.tsx`/`BacktestResults.tsx`. Three independent
 * `useAsync` calls (portfolio, deployments, strategies-for-the-dropdown) rather than one
 * combined fetch — each has its own loading/error lifecycle and none blocks the others.
 */

import { useCallback, useState, type SubmitEvent } from 'react';
import { useParams } from 'react-router-dom';

import {
  type DeploymentList,
  type DeploymentResponse,
  ApiError as DeploymentApiError,
  createDeployment,
  fetchDeployments,
  pauseDeployment,
  resumeDeployment,
  startDeployment,
  stopDeployment,
} from '../api/deployments';
import { type PortfolioDetailResponse, fetchPortfolio } from '../api/portfolios';
import { type StrategyList, fetchStrategies } from '../api/strategies';
import { useAsync } from '../hooks/useAsync';
import { useAuth } from '../hooks/useAuth';
import { formatMoney } from '../lib/format';
import { Message } from './MarketView';

/** Which action buttons apply to a deployment in this status, per the transition rules. */
const ACTIONS_BY_STATUS: Record<
  DeploymentResponse['status'],
  { action: 'start' | 'pause' | 'resume' | 'stop'; label: string }[]
> = {
  created: [{ action: 'start', label: 'Start' }],
  running: [
    { action: 'pause', label: 'Pause' },
    { action: 'stop', label: 'Stop' },
  ],
  paused: [
    { action: 'resume', label: 'Resume' },
    { action: 'stop', label: 'Stop' },
  ],
  stopped: [],
  errored: [],
};

const TRANSITION_FN = {
  start: startDeployment,
  pause: pauseDeployment,
  resume: resumeDeployment,
  stop: stopDeployment,
};

function DeploymentRow({
  deployment,
  accessToken,
  onChanged,
}: {
  deployment: DeploymentResponse;
  accessToken: string;
  onChanged: () => void;
}) {
  const [pending, setPending] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function handleAction(
    action: 'start' | 'pause' | 'resume' | 'stop',
  ): Promise<void> {
    setPending(action);
    setError(null);
    try {
      await TRANSITION_FN[action](deployment.id, accessToken);
      onChanged();
    } catch (cause) {
      setError(cause instanceof DeploymentApiError ? cause.message : 'Action failed.');
    } finally {
      setPending(null);
    }
  }

  return (
    <tr>
      <td>{deployment.strategy_id}</td>
      <td>{deployment.status}</td>
      <td>
        {ACTIONS_BY_STATUS[deployment.status].map(({ action, label }) => (
          <button
            key={action}
            type="button"
            disabled={pending !== null}
            onClick={() => void handleAction(action)}
          >
            {pending === action ? `${label}…` : label}
          </button>
        ))}
        {error && <span role="alert">{error}</span>}
      </td>
    </tr>
  );
}

function NewDeploymentForm({
  portfolioId,
  accessToken,
  onCreated,
}: {
  portfolioId: string;
  accessToken: string;
  onCreated: () => void;
}) {
  const { state } = useAsync<StrategyList>(
    useCallback((signal) => fetchStrategies(accessToken, false, signal), [accessToken]),
    [accessToken],
  );
  const [strategyId, setStrategyId] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: SubmitEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setError(null);
    if (!strategyId) {
      setError('Choose a strategy.');
      return;
    }
    setSubmitting(true);
    try {
      await createDeployment(
        { strategy_id: strategyId, portfolio_id: portfolioId },
        accessToken,
      );
      setStrategyId('');
      onCreated();
    } catch (cause) {
      setError(cause instanceof DeploymentApiError ? cause.message : 'Could not deploy.');
    } finally {
      setSubmitting(false);
    }
  }

  if (state.status === 'loading')
    return <Message tone="info" title="Loading strategies…" />;
  if (state.status === 'error') {
    return (
      <Message
        tone="error"
        title="Could not load strategies"
        detail={state.error.message}
      />
    );
  }
  if (state.data.strategies.length === 0) {
    return (
      <Message tone="info" title="No strategies yet" detail="Create one to deploy it." />
    );
  }

  return (
    <form className="auth-form" onSubmit={(event) => void handleSubmit(event)}>
      <label htmlFor="deployment-strategy">Strategy</label>
      <select
        id="deployment-strategy"
        required
        value={strategyId}
        onChange={(event) => {
          setStrategyId(event.target.value);
        }}
      >
        <option value="" disabled>
          Choose a strategy
        </option>
        {state.data.strategies.map((strategy) => (
          <option key={strategy.id} value={strategy.id}>
            {strategy.name} ({strategy.strategy_type})
          </option>
        ))}
      </select>

      {error && (
        <div className="panel panel-error" role="alert">
          <p className="panel-title">{error}</p>
        </div>
      )}

      <button type="submit" disabled={submitting}>
        {submitting ? 'Deploying…' : 'Deploy strategy'}
      </button>
    </form>
  );
}

export function PortfolioDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { accessToken } = useAuth();

  const portfolioResult = useAsync<PortfolioDetailResponse>(
    useCallback(
      (signal) => fetchPortfolio(id ?? '', accessToken ?? '', signal),
      [id, accessToken],
    ),
    [id, accessToken],
  );
  const deploymentsResult = useAsync<DeploymentList>(
    useCallback(
      (signal) => fetchDeployments(id ?? '', accessToken ?? '', signal),
      [id, accessToken],
    ),
    [id, accessToken],
  );

  if (!id) return null;

  const { state: portfolioState, reload: reloadPortfolio } = portfolioResult;
  const { state: deploymentsState, reload: reloadDeployments } = deploymentsResult;

  return (
    <main className="app">
      <header className="app-header">
        <h1>Portfolio</h1>
      </header>

      {portfolioState.status === 'loading' && (
        <Message tone="info" title="Loading portfolio…" />
      )}

      {portfolioState.status === 'error' && (
        <Message
          tone="error"
          title="Could not load portfolio"
          detail={portfolioState.error.message}
          action={{ label: 'Try again', onClick: reloadPortfolio }}
        />
      )}

      {portfolioState.status === 'success' && (
        <>
          <section aria-labelledby="portfolio-heading">
            <h2 id="portfolio-heading">{portfolioState.data.name}</h2>
            <dl className="quote">
              <div>
                <dt>Cash balance</dt>
                <dd>
                  {formatMoney(portfolioState.data.cash_balance)}{' '}
                  {portfolioState.data.base_currency}
                </dd>
              </div>
              <div>
                <dt>Total equity</dt>
                <dd>
                  {formatMoney(portfolioState.data.total_equity)}{' '}
                  {portfolioState.data.base_currency}
                </dd>
              </div>
            </dl>
          </section>

          <section aria-labelledby="positions-heading">
            <h2 id="positions-heading">Positions</h2>
            {portfolioState.data.positions.length === 0 ? (
              <Message tone="info" title="No open positions" />
            ) : (
              <table aria-label="Positions">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Quantity</th>
                    <th>Avg entry price</th>
                    <th>Realized P&amp;L</th>
                  </tr>
                </thead>
                <tbody>
                  {portfolioState.data.positions.map((position) => (
                    <tr key={position.instrument_id}>
                      <td>{position.symbol}</td>
                      <td>{formatMoney(position.quantity)}</td>
                      <td>{formatMoney(position.avg_entry_price)}</td>
                      <td>{formatMoney(position.realized_pnl)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        </>
      )}

      <section aria-labelledby="deployments-heading">
        <h2 id="deployments-heading">Deployments</h2>

        {deploymentsState.status === 'loading' && (
          <Message tone="info" title="Loading deployments…" />
        )}

        {deploymentsState.status === 'error' && (
          <Message
            tone="error"
            title="Could not load deployments"
            detail={deploymentsState.error.message}
            action={{ label: 'Try again', onClick: reloadDeployments }}
          />
        )}

        {deploymentsState.status === 'success' &&
          deploymentsState.data.deployments.length > 0 && (
            <table aria-label="Deployments">
              <thead>
                <tr>
                  <th>Strategy</th>
                  <th>Status</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {deploymentsState.data.deployments.map((deployment) => (
                  <DeploymentRow
                    key={deployment.id}
                    deployment={deployment}
                    accessToken={accessToken ?? ''}
                    onChanged={reloadDeployments}
                  />
                ))}
              </tbody>
            </table>
          )}

        {accessToken && (
          <NewDeploymentForm
            portfolioId={id}
            accessToken={accessToken}
            onCreated={reloadDeployments}
          />
        )}
      </section>
    </main>
  );
}
