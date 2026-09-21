/**
 * The application shell.
 *
 * Phase 1 closes the first vertical slice, so the page now shows real market data that
 * travelled the whole pipeline rather than a connectivity check. The scope statement stays:
 * it is a hard requirement (ADR-0008), not decoration.
 */

import { MarketView } from './components/MarketView';
import { config } from './config';
import './App.css';

export function App() {
  return (
    <main className="app">
      <header>
        <h1>TradeWatch</h1>
        <p className="tagline">Market intelligence and paper-trading platform</p>
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
          Phase 1 — synthetic provider → event bus → processor → PostgreSQL → API → this
          chart. API at <code>{config.apiBaseUrl}</code>.
        </p>
      </footer>
    </main>
  );
}
