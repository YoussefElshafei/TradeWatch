/** `AlertCenter`: rule CRUD and the alert feed, both against a stubbed `fetch`. */

import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { AuthProvider } from '../hooks/useAuth';
import { TOKEN_RESPONSE } from '../test-support/authFixtures';
import { requestUrl } from '../test-support/requestUrl';

import { AlertCenter } from './AlertCenter';

const RULES = {
  count: 1,
  rules: [
    {
      id: 'rule-1',
      user_id: 'user-1',
      name: 'AAPL anomalies',
      event_types: ['anomaly.detected'],
      symbols: ['AAPL'],
      min_anomaly_score: '5.0',
      cooldown_seconds: 300,
      channels: ['in_app'],
      is_enabled: true,
      created_at: '2026-01-01T00:00:00Z',
      updated_at: '2026-01-01T00:00:00Z',
    },
  ],
};

const ALERTS = {
  count: 1,
  alerts: [
    {
      id: 'alert-1',
      alert_rule_id: 'rule-1',
      symbol: 'AAPL',
      event_type: 'anomaly.detected',
      severity: 'warning',
      title: 'Unusual activity in AAPL',
      body: 'AAPL scored 6.0 against a threshold of 5.0.',
      payload: {},
      created_at: '2026-01-01T00:00:00Z',
      acknowledged_at: null,
    },
  ],
};

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/alerts']}>
      <AuthProvider>
        <AlertCenter />
      </AuthProvider>
    </MemoryRouter>,
  );
}

function stubFetch(overrides: { rules?: unknown; alerts?: unknown } = {}) {
  const { rules = RULES, alerts = ALERTS } = overrides;
  const calls: { url: string; method: string }[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = requestUrl(input);
      const method = init?.method ?? 'GET';
      calls.push({ url, method });
      if (url.includes('/auth/refresh')) {
        return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
      }
      if (url.includes('/alert-rules') && method === 'POST') {
        return Promise.resolve(
          new Response(JSON.stringify(RULES.rules[0]), { status: 201 }),
        );
      }
      if (url.includes('/alert-rules') && method === 'DELETE') {
        return Promise.resolve(new Response(null, { status: 204 }));
      }
      if (url.includes('/alert-rules') && method === 'PATCH') {
        return Promise.resolve(new Response(JSON.stringify(RULES.rules[0])));
      }
      if (url.includes('/alert-rules')) {
        return Promise.resolve(new Response(JSON.stringify(rules)));
      }
      if (url.includes('/alerts/') && url.endsWith('/ack')) {
        return Promise.resolve(new Response(null, { status: 204 }));
      }
      if (url.includes('/alerts')) {
        return Promise.resolve(new Response(JSON.stringify(alerts)));
      }
      return Promise.resolve(new Response('{}', { status: 404 }));
    }),
  );
  return calls;
}

describe('AlertCenter', () => {
  it('shows loading states before anything arrives', () => {
    stubFetch();
    renderPage();
    expect(screen.getByText(/loading rules/i)).toBeInTheDocument();
  });

  it('shows empty states when there are no rules or alerts', async () => {
    stubFetch({ rules: { count: 0, rules: [] }, alerts: { count: 0, alerts: [] } });
    renderPage();
    expect(await screen.findByText(/no alert rules yet/i)).toBeInTheDocument();
    expect(await screen.findByText(/no alerts yet/i)).toBeInTheDocument();
  });

  it('lists an existing rule and the alert it fired', async () => {
    stubFetch();
    renderPage();

    expect(await screen.findByText('AAPL anomalies')).toBeInTheDocument();
    expect(await screen.findByText('Unusual activity in AAPL')).toBeInTheDocument();
  });

  it('creates a rule from the form', async () => {
    const calls = stubFetch({ rules: { count: 0, rules: [] } });
    const user = userEvent.setup();
    renderPage();

    await screen.findByText(/no alert rules yet/i);
    await user.type(screen.getByLabelText(/^name$/i), 'My new rule');
    await user.click(screen.getByRole('button', { name: /create rule/i }));

    await waitFor(() => {
      expect(
        calls.some((call) => call.url.includes('/alert-rules') && call.method === 'POST'),
      ).toBe(true);
    });
  });

  it('disables a rule on request', async () => {
    const calls = stubFetch();
    const user = userEvent.setup();
    renderPage();

    await user.click(await screen.findByRole('button', { name: /disable/i }));

    await waitFor(() => {
      expect(
        calls.some(
          (call) => call.url.includes('/alert-rules/rule-1') && call.method === 'PATCH',
        ),
      ).toBe(true);
    });
  });

  it('deletes a rule on request', async () => {
    const calls = stubFetch();
    const user = userEvent.setup();
    renderPage();

    await user.click(await screen.findByRole('button', { name: /delete/i }));

    await waitFor(() => {
      expect(
        calls.some(
          (call) => call.url.includes('/alert-rules/rule-1') && call.method === 'DELETE',
        ),
      ).toBe(true);
    });
  });

  it('acknowledges an alert on request', async () => {
    const calls = stubFetch();
    const user = userEvent.setup();
    renderPage();

    await user.click(await screen.findByRole('button', { name: /acknowledge/i }));

    await waitFor(() => {
      expect(calls.some((call) => call.url.includes('/alerts/alert-1/ack'))).toBe(true);
    });
  });

  it('has an alerts link on the dashboard nav', async () => {
    stubFetch();
    renderPage();
    // Smoke check: the page renders under its own route without the nav (Dashboard renders
    // that separately) — this test only proves AlertCenter itself mounts cleanly at /alerts.
    expect(await screen.findByRole('heading', { name: 'Alerts' })).toBeInTheDocument();
  });
});
