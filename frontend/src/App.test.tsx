/**
 * The application shell.
 *
 * The market data behaviour is covered in `components/MarketView.test.tsx`; this only
 * checks what the shell itself owns — the heading, the scope statement, and that the market
 * view is actually mounted, once `ProtectedRoute` (Phase 5) lets a signed-in session through.
 *
 * The scope statement is asserted rather than trusted because ADR-0008 makes its wording a
 * hard requirement. It is the kind of text that gets "tightened" by someone who does not
 * know why it is precise.
 */

import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { MemoryRouter } from 'react-router-dom';

import { AuthProvider } from './hooks/useAuth';
import { requestUrl } from './test-support/requestUrl';
import { TOKEN_RESPONSE } from './test-support/authFixtures';

import { App } from './App';

/**
 * Every route this shell needs to reach on a cold render behind `ProtectedRoute` (Phase 5):
 * `/auth/refresh` so `AuthProvider` resolves to a signed-in session, plus every endpoint the
 * real `Dashboard` now fetches (the design pass's watchlist/portfolio/alert/anomaly/backtest
 * panels) — each needs its own correctly-shaped empty body, not one universal stub, or a
 * panel reading `data.portfolios` off a `{ instruments: [] }` response throws during render.
 */
function stubEmptyApi() {
  const emptyByPath: [RegExp, unknown][] = [
    [/\/auth\/refresh/, TOKEN_RESPONSE],
    [/\/api\/v1\/instruments/, { count: 0, instruments: [] }],
    [/\/api\/v1\/portfolios/, { count: 0, portfolios: [] }],
    [/\/api\/v1\/alerts/, { count: 0, alerts: [] }],
    [/\/api\/v1\/anomalies/, { count: 0, detections: [] }],
    [/\/api\/v1\/backtests/, { count: 0, backtests: [] }],
  ];

  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = requestUrl(input);
      const match = emptyByPath.find(([pattern]) => pattern.test(url));
      return Promise.resolve(new Response(JSON.stringify(match?.[1] ?? { count: 0 })));
    }),
  );
}

function renderApp() {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider>
        <App />
      </AuthProvider>
    </MemoryRouter>,
  );
}

describe('App', () => {
  it('renders the application name', async () => {
    stubEmptyApi();

    renderApp();

    expect(
      await screen.findByRole('heading', { level: 1, name: 'TradeWatch' }),
    ).toBeInTheDocument();
  });

  it('mounts the market view', async () => {
    stubEmptyApi();

    renderApp();

    // With no instruments the view renders its empty state, which is proof enough that it
    // mounted and reached the API.
    expect(await screen.findByText(/no instruments yet/i)).toBeInTheDocument();
  });

  it('states the scope honestly', async () => {
    // ADR-0008. The wording is a requirement, so it is asserted rather than trusted.
    stubEmptyApi();

    renderApp();

    const scope = await screen.findByText(/statistically unusual market activity/i);
    expect(scope).toHaveTextContent(
      /not market manipulation, fraud, or\s+financial crime/i,
    );
    expect(scope).toHaveTextContent(/no real\s+money/i);
  });

  it('names the pipeline it is the end of', async () => {
    stubEmptyApi();

    renderApp();

    expect(await screen.findByRole('contentinfo')).toHaveTextContent(
      /Simulated trading only/i,
    );
  });
});
