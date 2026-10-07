/**
 * The real landing page (frontend design pass).
 *
 * Before this, `/` just rendered `MarketView` directly — useful to prove the vertical slice
 * in Phase 1, but not a dashboard. `MarketView` now lives at its own `/market` route; this
 * page ties the product together instead: a watchlist snapshot, a portfolio summary, and
 * the most recent alerts, anomalies, and backtests, each linking through to its full page.
 *
 * Every panel is its own `useAsync` call with its own loading/error/empty state, same
 * discipline as `MarketView.tsx`'s four states — one panel failing to load must not take
 * the rest of the dashboard down with it.
 */

import { useCallback, type ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { type AlertList, fetchAlerts } from '../api/alerts';
import { type AnomalyDetectionList, fetchAnomalies } from '../api/anomalies';
import { type BacktestRunList, fetchBacktests } from '../api/backtests';
import {
  type Instrument,
  type InstrumentList,
  type LatestPrice,
  fetchInstruments,
  fetchLatestPrice,
} from '../api/market';
import { type PortfolioList, fetchPortfolios } from '../api/portfolios';
import { useAsync } from '../hooks/useAsync';
import { useAuth } from '../hooks/useAuth';
import { formatMoney, formatPercent, formatTimestamp } from '../lib/format';
import { Message } from './MarketView';

const WATCHLIST_SIZE = 5;
const PANEL_ROW_LIMIT = 5;

function DashboardPanel({
  title,
  viewAllTo,
  children,
}: {
  title: string;
  viewAllTo: string;
  children: ReactNode;
}) {
  return (
    <section className="dashboard-panel" aria-labelledby={`panel-${title}`}>
      <header className="dashboard-panel-header">
        <h3 id={`panel-${title}`}>{title}</h3>
        <Link to={viewAllTo}>View all</Link>
      </header>
      {children}
    </section>
  );
}

function WatchlistRow({ instrument }: { instrument: Instrument }) {
  const { state } = useAsync<LatestPrice>(
    useCallback(
      (signal) => fetchLatestPrice(instrument.symbol, signal),
      [instrument.symbol],
    ),
    [instrument.symbol],
  );
  const price = state.status === 'success' ? state.data.price : null;

  return (
    <tr>
      <td>{instrument.symbol}</td>
      <td className="figure">
        {state.status === 'loading' && '…'}
        {state.status === 'error' && '—'}
        {state.status === 'success' && (price ? formatMoney(price) : 'no trades yet')}
      </td>
    </tr>
  );
}

function WatchlistPanel() {
  const { state, reload } = useAsync<InstrumentList>(
    useCallback((signal) => fetchInstruments(signal), []),
    [],
  );

  return (
    <DashboardPanel title="Watchlist" viewAllTo="/market">
      {state.status === 'loading' && <Message tone="info" title="Loading instruments…" />}
      {state.status === 'error' && (
        <Message
          tone="error"
          title="Could not reach the TradeWatch API"
          detail={state.error.message}
          action={{ label: 'Try again', onClick: reload }}
        />
      )}
      {state.status === 'success' && state.data.instruments.length === 0 && (
        <Message
          tone="info"
          title="No instruments yet"
          detail="The database has no instruments. Start the ingestion pipeline to register some."
        />
      )}
      {state.status === 'success' && state.data.instruments.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Last</th>
            </tr>
          </thead>
          <tbody>
            {state.data.instruments.slice(0, WATCHLIST_SIZE).map((instrument) => (
              <WatchlistRow key={instrument.symbol} instrument={instrument} />
            ))}
          </tbody>
        </table>
      )}
    </DashboardPanel>
  );
}

function PortfolioSummaryPanel() {
  const { accessToken } = useAuth();
  const { state, reload } = useAsync<PortfolioList>(
    useCallback((signal) => fetchPortfolios(accessToken ?? '', signal), [accessToken]),
    [accessToken],
  );

  return (
    <DashboardPanel title="Portfolios" viewAllTo="/portfolios">
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
              <th>Cash</th>
            </tr>
          </thead>
          <tbody>
            {state.data.portfolios.slice(0, PANEL_ROW_LIMIT).map((portfolio) => (
              <tr key={portfolio.id}>
                <td>
                  <Link to={`/portfolios/${encodeURIComponent(portfolio.id)}`}>
                    {portfolio.name}
                  </Link>
                </td>
                <td className="figure">
                  {formatMoney(portfolio.cash_balance)} {portfolio.base_currency}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </DashboardPanel>
  );
}

function RecentAlertsPanel() {
  const { accessToken } = useAuth();
  const { state, reload } = useAsync<AlertList>(
    useCallback((signal) => fetchAlerts(accessToken ?? '', false, signal), [accessToken]),
    [accessToken],
  );

  const alerts =
    state.status === 'success'
      ? [...state.data.alerts]
          .sort((a, b) => b.created_at.localeCompare(a.created_at))
          .slice(0, PANEL_ROW_LIMIT)
      : [];

  return (
    <DashboardPanel title="Recent alerts" viewAllTo="/alerts">
      {state.status === 'loading' && <Message tone="info" title="Loading alerts…" />}
      {state.status === 'error' && (
        <Message
          tone="error"
          title="Could not load alerts"
          detail={state.error.message}
          action={{ label: 'Try again', onClick: reload }}
        />
      )}
      {state.status === 'success' && alerts.length === 0 && (
        <Message
          tone="info"
          title="No alerts yet"
          detail="Nothing has fired for your rules."
        />
      )}
      {alerts.length > 0 && (
        <ul className="dashboard-list">
          {alerts.map((alert) => (
            <li key={alert.id}>
              <span
                className={`badge ${alert.severity === 'critical' ? 'badge-negative' : alert.severity === 'warning' ? 'badge-warning' : 'badge-info'}`}
              >
                {alert.severity}
              </span>
              <span className="dashboard-list-title">{alert.title}</span>
              <span className="dashboard-list-time">
                {formatTimestamp(alert.created_at)}
              </span>
            </li>
          ))}
        </ul>
      )}
    </DashboardPanel>
  );
}

function RecentAnomaliesPanel() {
  const { state, reload } = useAsync<AnomalyDetectionList>(
    useCallback((signal) => fetchAnomalies({ limit: PANEL_ROW_LIMIT }, signal), []),
    [],
  );

  return (
    <DashboardPanel title="Recent anomalies" viewAllTo="/anomalies">
      {state.status === 'loading' && <Message tone="info" title="Loading anomalies…" />}
      {state.status === 'error' && (
        <Message
          tone="error"
          title="Could not load anomalies"
          detail={state.error.message}
          action={{ label: 'Try again', onClick: reload }}
        />
      )}
      {state.status === 'success' && state.data.detections.length === 0 && (
        <Message tone="info" title="No detections" detail="Nothing unusual yet." />
      )}
      {state.status === 'success' && state.data.detections.length > 0 && (
        <ul className="dashboard-list">
          {state.data.detections.map((detection) => (
            <li key={detection.id}>
              <span
                className={`badge ${detection.severity === 'critical' ? 'badge-negative' : detection.severity === 'warning' ? 'badge-warning' : 'badge-info'}`}
              >
                {detection.severity}
              </span>
              <span className="dashboard-list-title">{detection.symbol}</span>
              <span className="figure dashboard-list-time">
                score {formatMoney(detection.anomaly_score)}
              </span>
            </li>
          ))}
        </ul>
      )}
    </DashboardPanel>
  );
}

function RecentBacktestsPanel() {
  const { accessToken } = useAuth();
  const { state, reload } = useAsync<BacktestRunList>(
    useCallback((signal) => fetchBacktests(accessToken ?? '', signal), [accessToken]),
    [accessToken],
  );

  const runs =
    state.status === 'success'
      ? [...state.data.backtests]
          .sort((a, b) => b.created_at.localeCompare(a.created_at))
          .slice(0, PANEL_ROW_LIMIT)
      : [];

  return (
    <DashboardPanel title="Recent backtests" viewAllTo="/backtests">
      {state.status === 'loading' && <Message tone="info" title="Loading backtests…" />}
      {state.status === 'error' && (
        <Message
          tone="error"
          title="Could not load backtests"
          detail={state.error.message}
          action={{ label: 'Try again', onClick: reload }}
        />
      )}
      {state.status === 'success' && runs.length === 0 && (
        <Message
          tone="info"
          title="No backtests yet"
          detail="Run one against a strategy to see results here."
        />
      )}
      {runs.length > 0 && (
        <ul className="dashboard-list">
          {runs.map((run) => (
            <li key={run.id}>
              <Link to={`/backtests/${run.id}`} className="dashboard-list-title">
                {run.symbols.join(', ')}
              </Link>
              <span className="figure dashboard-list-time">
                {run.metrics ? formatPercent(run.metrics.total_return_pct) : run.status}
              </span>
            </li>
          ))}
        </ul>
      )}
    </DashboardPanel>
  );
}

export function Dashboard() {
  return (
    <main className="page">
      <header className="page-header">
        <h2>Dashboard</h2>
      </header>

      <div className="dashboard-grid">
        <WatchlistPanel />
        <PortfolioSummaryPanel />
        <RecentAlertsPanel />
        <RecentAnomaliesPanel />
        <RecentBacktestsPanel />
      </div>

      <section aria-labelledby="scope-heading" className="dashboard-scope">
        <h3 id="scope-heading">Scope</h3>
        <p>
          TradeWatch simulates trading. It has no brokerage connectivity and moves no real
          money. Its detection component reports statistically unusual market activity
          relative to a symbol&rsquo;s own recent behaviour — not market manipulation,
          fraud, or financial crime.
        </p>
      </section>
    </main>
  );
}
