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

const EMPTY_UNIVERSE = { count: 0, instruments: [] };

/**
 * Every route this shell needs to reach on a cold render behind `ProtectedRoute`
 * (Phase 5): `/auth/refresh` so `AuthProvider` resolves to a signed-in session, and the
 * market endpoints `MarketView` itself calls once that session exists.
 */
function stubEmptyApi() {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = requestUrl(input);
      if (url.includes('/auth/refresh')) {
        return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
      }
      return Promise.resolve(new Response(JSON.stringify(EMPTY_UNIVERSE)));
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
      /Phase 5.*accounts.*sessions/i,
    );
  });
});
