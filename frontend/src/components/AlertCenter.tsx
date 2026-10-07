/**
 * The alert centre (Phase 9): create/manage alert rules, and watch alerts arrive live.
 *
 * Same four-states discipline as `MarketView.tsx` for both the rule list and the alert feed.
 * The feed merges two sources — the REST fetch on mount (history) and `useAlertSocket`'s live
 * push (`/ws/alerts`) — deduped by `id`, the same idempotency reasoning `useMarketSocket`'s
 * trade tape and `useAlertSocket`'s own reducer already apply at their respective layers.
 */

import { useCallback, useMemo, useState, type SubmitEvent } from 'react';

import {
  type Alert,
  type AlertRule,
  ALERT_EVENT_TYPES,
  acknowledgeAlert,
  createAlertRule,
  deleteAlertRule,
  fetchAlertRules,
  fetchAlerts,
  updateAlertRule,
} from '../api/alerts';
import { useAlertSocket, type ConnectionStatus } from '../hooks/useAlertSocket';
import { useAsync } from '../hooks/useAsync';
import { useAuth } from '../hooks/useAuth';
import { formatTimestamp } from '../lib/format';
import { Message } from './MarketView';

const STATUS_LABEL: Record<ConnectionStatus, string> = {
  connecting: 'Connecting…',
  open: 'Live',
  reconnecting: 'Reconnecting…',
  closed: 'Offline',
};

function LiveBadge({ status }: { status: ConnectionStatus }) {
  return (
    <span className={`live-badge live-badge-${status}`} role="status">
      <span aria-hidden="true" className="live-dot" />
      {STATUS_LABEL[status]}
    </span>
  );
}

function mergeAlerts(fetched: Alert[], live: Alert[]): Alert[] {
  const byId = new Map<string, Alert>();
  for (const alert of [...live, ...fetched]) {
    if (!byId.has(alert.id)) byId.set(alert.id, alert);
  }
  return [...byId.values()].sort((a, b) => b.created_at.localeCompare(a.created_at));
}

interface RuleFormState {
  name: string;
  eventTypes: Set<string>;
  symbols: string;
  minAnomalyScore: string;
  cooldownSeconds: string;
}

const EMPTY_FORM: RuleFormState = {
  name: '',
  eventTypes: new Set(['anomaly.detected']),
  symbols: '',
  minAnomalyScore: '',
  cooldownSeconds: '300',
};

function NewRuleForm({ onCreated }: { onCreated: () => void }) {
  const { accessToken } = useAuth();
  const [form, setForm] = useState<RuleFormState>(EMPTY_FORM);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function toggleEventType(value: string): void {
    setForm((previous) => {
      const eventTypes = new Set(previous.eventTypes);
      if (eventTypes.has(value)) {
        eventTypes.delete(value);
      } else {
        eventTypes.add(value);
      }
      return { ...previous, eventTypes };
    });
  }

  async function handleSubmit(event: SubmitEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (!accessToken || form.eventTypes.size === 0) return;

    setSubmitting(true);
    setError(null);
    try {
      const symbols = form.symbols
        .split(',')
        .map((symbol) => symbol.trim())
        .filter(Boolean);
      await createAlertRule(
        {
          name: form.name,
          event_types: [...form.eventTypes],
          symbols: symbols.length > 0 ? symbols : null,
          min_anomaly_score: form.minAnomalyScore.trim() || null,
          cooldown_seconds: Number(form.cooldownSeconds) || 0,
        },
        accessToken,
      );
      setForm(EMPTY_FORM);
      onCreated();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not create the rule.');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form
      className="auth-form"
      onSubmit={(event) => void handleSubmit(event)}
      aria-label="New alert rule"
    >
      <h3>New rule</h3>
      {error && <Message tone="error" title="Could not create the rule" detail={error} />}
      <label>
        Name
        <input
          value={form.name}
          onChange={(event) => {
            setForm({ ...form, name: event.target.value });
          }}
          required
          minLength={1}
        />
      </label>
      <fieldset>
        <legend>Listen for</legend>
        {ALERT_EVENT_TYPES.map((type) => (
          <label key={type.value}>
            <input
              type="checkbox"
              checked={form.eventTypes.has(type.value)}
              onChange={() => {
                toggleEventType(type.value);
              }}
            />
            {type.label}
          </label>
        ))}
      </fieldset>
      <label>
        Symbols (comma-separated, blank = every symbol)
        <input
          value={form.symbols}
          onChange={(event) => {
            setForm({ ...form, symbols: event.target.value });
          }}
          placeholder="AAPL, MSFT"
        />
      </label>
      {form.eventTypes.has('anomaly.detected') && (
        <label>
          Minimum anomaly score
          <input
            value={form.minAnomalyScore}
            onChange={(event) => {
              setForm({ ...form, minAnomalyScore: event.target.value });
            }}
            placeholder="e.g. 5.0"
            inputMode="decimal"
          />
        </label>
      )}
      <label>
        Cooldown (seconds)
        <input
          type="number"
          min={0}
          value={form.cooldownSeconds}
          onChange={(event) => {
            setForm({ ...form, cooldownSeconds: event.target.value });
          }}
        />
      </label>
      <button type="submit" disabled={submitting || form.eventTypes.size === 0}>
        {submitting ? 'Creating…' : 'Create rule'}
      </button>
    </form>
  );
}

function RuleRow({ rule, onChanged }: { rule: AlertRule; onChanged: () => void }) {
  const { accessToken } = useAuth();
  const [busy, setBusy] = useState(false);

  async function toggleEnabled(): Promise<void> {
    if (!accessToken) return;
    setBusy(true);
    try {
      await updateAlertRule(rule.id, { is_enabled: !rule.is_enabled }, accessToken);
      onChanged();
    } finally {
      setBusy(false);
    }
  }

  async function handleDelete(): Promise<void> {
    if (!accessToken) return;
    setBusy(true);
    try {
      await deleteAlertRule(rule.id, accessToken);
      onChanged();
    } finally {
      setBusy(false);
    }
  }

  return (
    <tr>
      <td>{rule.name}</td>
      <td>{rule.event_types.join(', ')}</td>
      <td>{rule.symbols ? rule.symbols.join(', ') : 'Every symbol'}</td>
      <td>{rule.cooldown_seconds}s</td>
      <td>
        <button type="button" disabled={busy} onClick={() => void toggleEnabled()}>
          {rule.is_enabled ? 'Disable' : 'Enable'}
        </button>{' '}
        <button type="button" disabled={busy} onClick={() => void handleDelete()}>
          Delete
        </button>
      </td>
    </tr>
  );
}

function AlertRow({ alert, onChanged }: { alert: Alert; onChanged: () => void }) {
  const { accessToken } = useAuth();
  const [busy, setBusy] = useState(false);

  async function handleAcknowledge(): Promise<void> {
    if (!accessToken) return;
    setBusy(true);
    try {
      await acknowledgeAlert(alert.id, accessToken);
      onChanged();
    } finally {
      setBusy(false);
    }
  }

  return (
    <li className={`panel panel-${alert.severity === 'critical' ? 'error' : 'info'}`}>
      <p className="panel-title">
        {alert.title} {alert.symbol && <em>({alert.symbol})</em>}
      </p>
      <p className="panel-detail">{alert.body}</p>
      <p className="panel-detail">{formatTimestamp(alert.created_at)}</p>
      {alert.acknowledged_at === null ? (
        <button type="button" disabled={busy} onClick={() => void handleAcknowledge()}>
          Acknowledge
        </button>
      ) : (
        <span>Acknowledged</span>
      )}
    </li>
  );
}

export function AlertCenter() {
  const { accessToken } = useAuth();
  const socket = useAlertSocket(accessToken);

  const { state: rulesState, reload: reloadRules } = useAsync(
    useCallback((signal) => fetchAlertRules(accessToken ?? '', signal), [accessToken]),
    [accessToken],
  );
  const { state: alertsState, reload: reloadAlerts } = useAsync(
    useCallback((signal) => fetchAlerts(accessToken ?? '', false, signal), [accessToken]),
    [accessToken],
  );

  const alerts = useMemo(
    () =>
      alertsState.status === 'success'
        ? mergeAlerts(alertsState.data.alerts, socket.liveAlerts)
        : socket.liveAlerts,
    [alertsState, socket.liveAlerts],
  );

  return (
    <main className="page">
      <header className="page-header">
        <h2>Alerts</h2>
        <LiveBadge status={socket.status} />
      </header>

      <section aria-labelledby="rules-heading">
        <h3 id="rules-heading">Alert rules</h3>
        <NewRuleForm onCreated={reloadRules} />

        {rulesState.status === 'loading' && <Message tone="info" title="Loading rules…" />}
        {rulesState.status === 'error' && (
          <Message
            tone="error"
            title="Could not load alert rules"
            detail={rulesState.error.message}
            action={{ label: 'Try again', onClick: reloadRules }}
          />
        )}
        {rulesState.status === 'success' && rulesState.data.rules.length === 0 && (
          <Message tone="info" title="No alert rules yet" detail="Create one above." />
        )}
        {rulesState.status === 'success' && rulesState.data.rules.length > 0 && (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Listens for</th>
                <th>Symbols</th>
                <th>Cooldown</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {rulesState.data.rules.map((rule) => (
                <RuleRow key={rule.id} rule={rule} onChanged={reloadRules} />
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section aria-labelledby="feed-heading">
        <h3 id="feed-heading">Alert feed</h3>
        {alertsState.status === 'loading' && (
          <Message tone="info" title="Loading alerts…" />
        )}
        {alertsState.status === 'error' && (
          <Message
            tone="error"
            title="Could not load alerts"
            detail={alertsState.error.message}
            action={{ label: 'Try again', onClick: reloadAlerts }}
          />
        )}
        {alerts.length === 0 && alertsState.status === 'success' && (
          <Message
            tone="info"
            title="No alerts yet"
            detail="Nothing has fired for your rules."
          />
        )}
        {alerts.length > 0 && (
          // `aria-live="polite"` so a screen reader announces a new alert arriving over the
          // socket, the same way the live badge and trade tape are meant to be felt visually.
          <ul className="alert-feed" aria-live="polite" aria-relevant="additions">
            {alerts.map((alert) => (
              <AlertRow key={alert.id} alert={alert} onChanged={reloadAlerts} />
            ))}
          </ul>
        )}
      </section>
    </main>
  );
}
