/**
 * The signed-in dashboard shell.
 *
 * What `App.tsx` rendered directly before Phase 5 — header, `MarketView`, the scope
 * statement, footer — now behind `ProtectedRoute`, plus who is signed in and a way to stop
 * being.
 */

import { Link } from 'react-router-dom';

import { MarketView } from './MarketView';
import { config } from '../config';
import { useAuth } from '../hooks/useAuth';

export function Dashboard() {
  const { user, logout } = useAuth();

  return (
    <main className="app">
      <header className="app-header">
        <div>
          <h1>TradeWatch</h1>
          <p className="tagline">Market intelligence and paper-trading platform</p>
        </div>
        {user && (
          <div className="account">
            <Link to="/strategies">Strategies</Link>
            <Link to="/portfolios">Portfolios</Link>
            <Link to="/alerts">Alerts</Link>
            <Link to="/anomalies">Anomalies</Link>
            <span>{user.email}</span>
            <button type="button" onClick={() => void logout()}>
              Sign out
            </button>
          </div>
        )}
      </header>

      <MarketView />

      <section aria-labelledby="scope-heading">
        <h2 id="scope-heading">Scope</h2>
        <p>
          TradeWatch simulates trading. It has no brokerage connectivity and moves no real
          money. Its detection component reports statistically unusual market activity
          relative to a symbol&rsquo;s own recent behaviour — not market manipulation,
          fraud, or financial crime.
        </p>
      </section>

      <footer>
        <p>
          Phase 5 — accounts, sessions, and per-user access control sit in front of the
          Phase 1–4 pipeline. API at <code>{config.apiBaseUrl}</code>.
        </p>
      </footer>
    </main>
  );
}
