/** Create a strategy (Phase 6, ADR-0006). Form pattern copied from `LoginPage.tsx`. */

import { useState, type SubmitEvent } from 'react';
import { useNavigate } from 'react-router-dom';

import { ApiError, STRATEGY_TYPES, createStrategy } from '../api/strategies';
import { useAuth } from '../hooks/useAuth';

function defaultParams(typeValue: string): Record<string, number> {
  const type = STRATEGY_TYPES.find((candidate) => candidate.value === typeValue);
  const params: Record<string, number> = {};
  for (const field of type?.fields ?? []) {
    params[field.name] = field.defaultValue;
  }
  return params;
}

export function StrategyEditor() {
  const { accessToken } = useAuth();
  const navigate = useNavigate();

  const [name, setName] = useState('');
  const [typeValue, setTypeValue] = useState(STRATEGY_TYPES[0]?.value ?? '');
  const [params, setParams] = useState<Record<string, number>>(() =>
    defaultParams(typeValue),
  );
  const [symbolsText, setSymbolsText] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const type = STRATEGY_TYPES.find((candidate) => candidate.value === typeValue);

  function selectType(value: string): void {
    setTypeValue(value);
    setParams(defaultParams(value));
  }

  async function handleSubmit(event: SubmitEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setError(null);

    const symbols = symbolsText
      .split(',')
      .map((symbol) => symbol.trim().toUpperCase())
      .filter((symbol) => symbol !== '');
    if (symbols.length === 0) {
      setError('Enter at least one symbol.');
      return;
    }
    const validationError = type?.validate(params) ?? null;
    if (validationError) {
      setError(validationError);
      return;
    }
    if (!accessToken) {
      setError('You must be signed in.');
      return;
    }

    setSubmitting(true);
    try {
      const strategy = await createStrategy(
        { name, strategy_type: typeValue, parameters: params, symbols },
        accessToken,
      );
      await navigate(`/backtests/new?strategyId=${encodeURIComponent(strategy.id)}`);
    } catch (cause) {
      const detailText =
        cause instanceof ApiError && cause.details?.length
          ? ` (${cause.details.map((detail) => detail.message).join('; ')})`
          : '';
      setError(
        cause instanceof ApiError
          ? `${cause.message}${detailText}`
          : 'Something went wrong. Please try again.',
      );
      setSubmitting(false);
    }
  }

  return (
    <main className="app">
      <header>
        <h1>New strategy</h1>
      </header>

      <form className="auth-form" onSubmit={(event) => void handleSubmit(event)}>
        <label htmlFor="strategy-name">Name</label>
        <input
          id="strategy-name"
          type="text"
          required
          maxLength={100}
          value={name}
          onChange={(event) => {
            setName(event.target.value);
          }}
        />

        <label htmlFor="strategy-type">Strategy type</label>
        <select
          id="strategy-type"
          value={typeValue}
          onChange={(event) => {
            selectType(event.target.value);
          }}
        >
          {STRATEGY_TYPES.map((candidate) => (
            <option key={candidate.value} value={candidate.value}>
              {candidate.label}
            </option>
          ))}
        </select>

        {type?.fields.map((field) => (
          <div key={field.name}>
            <label htmlFor={`strategy-param-${field.name}`}>{field.label}</label>
            <input
              id={`strategy-param-${field.name}`}
              type="number"
              required
              min={field.min}
              max={field.max}
              value={params[field.name] ?? field.defaultValue}
              onChange={(event) => {
                setParams((current) => ({
                  ...current,
                  [field.name]: Number(event.target.value),
                }));
              }}
            />
          </div>
        ))}

        <label htmlFor="strategy-symbols">Symbols (comma-separated)</label>
        <input
          id="strategy-symbols"
          type="text"
          required
          placeholder="AAPL, MSFT"
          value={symbolsText}
          onChange={(event) => {
            setSymbolsText(event.target.value);
          }}
        />

        {error && (
          <div className="panel panel-error" role="alert">
            <p className="panel-title">{error}</p>
          </div>
        )}

        <button type="submit" disabled={submitting}>
          {submitting ? 'Creating…' : 'Create strategy'}
        </button>
      </form>
    </main>
  );
}
