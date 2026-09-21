/**
 * The application shell.
 *
 * The market data behaviour is covered in `components/MarketView.test.tsx`; this only
 * checks what the shell itself owns — the heading, the scope statement, and that the market
 * view is actually mounted.
 *
 * The scope statement is asserted rather than trusted because ADR-0008 makes its wording a
 * hard requirement. It is the kind of text that gets "tightened" by someone who does not
 * know why it is precise.
 */

import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { App } from './App';

const EMPTY_UNIVERSE = { count: 0, instruments: [] };

function stubEmptyApi() {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => Promise.resolve(new Response(JSON.stringify(EMPTY_UNIVERSE)))),
  );
}

describe('App', () => {
  it('renders the application name', () => {
    stubEmptyApi();

    render(<App />);

    expect(
      screen.getByRole('heading', { level: 1, name: 'TradeWatch' }),
    ).toBeInTheDocument();
  });

  it('mounts the market view', async () => {
    stubEmptyApi();

    render(<App />);

    // With no instruments the view renders its empty state, which is proof enough that it
    // mounted and reached the API.
    expect(await screen.findByText(/no instruments yet/i)).toBeInTheDocument();
  });

  it('states the scope honestly', () => {
    // ADR-0008. The wording is a requirement, so it is asserted rather than trusted.
    stubEmptyApi();

    render(<App />);

    const scope = screen.getByText(/statistically unusual market activity/i);
    expect(scope).toHaveTextContent(
      /not market manipulation, fraud, or\s+financial crime/i,
    );
    expect(scope).toHaveTextContent(/no real\s+money/i);
  });

  it('names the pipeline it is the end of', () => {
    stubEmptyApi();

    render(<App />);

    expect(screen.getByRole('contentinfo')).toHaveTextContent(
      /processor.*PostgreSQL.*API/i,
    );
  });
});
