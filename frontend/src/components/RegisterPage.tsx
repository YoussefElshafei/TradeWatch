/** Registration form (Phase 5, ADR-0010). */

import { useState, type SubmitEvent } from 'react';
import { Link, useNavigate } from 'react-router-dom';

import { ApiError } from '../api/auth';
import { useAuth } from '../hooks/useAuth';

//: Matches the backend default (`TW_PASSWORD_MIN_LENGTH`, `SecuritySettings`). Not the
//: source of truth — the server rejects a request that gets past this anyway — only a
//: same-page hint so a person does not submit and wait for a round trip to be told the
//: same thing this could have said immediately.
const PASSWORD_MIN_LENGTH_HINT = 12;

export function RegisterPage() {
  const { register } = useAuth();
  const navigate = useNavigate();

  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: SubmitEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await register(email, password);
      await navigate('/', { replace: true });
    } catch (cause) {
      setError(
        cause instanceof ApiError ? cause.message : 'Something went wrong. Please try again.',
      );
      setSubmitting(false);
    }
  }

  return (
    <main className="app auth-page">
      <header>
        <h1>TradeWatch</h1>
        <p className="tagline">Create an account</p>
      </header>

      <form className="auth-form" onSubmit={(event) => void handleSubmit(event)}>
        <label htmlFor="register-email">Email</label>
        <input
          id="register-email"
          type="email"
          autoComplete="username"
          required
          value={email}
          onChange={(event) => {
            setEmail(event.target.value);
          }}
        />

        <label htmlFor="register-password">Password</label>
        <input
          id="register-password"
          type="password"
          autoComplete="new-password"
          required
          minLength={PASSWORD_MIN_LENGTH_HINT}
          value={password}
          onChange={(event) => {
            setPassword(event.target.value);
          }}
        />
        <p className="panel-detail">At least {PASSWORD_MIN_LENGTH_HINT} characters.</p>

        {error && (
          <div className="panel panel-error" role="alert">
            <p className="panel-title">{error}</p>
          </div>
        )}

        <button type="submit" disabled={submitting}>
          {submitting ? 'Creating account…' : 'Create account'}
        </button>
      </form>

      <p>
        Already have an account? <Link to="/login">Sign in</Link>
      </p>
    </main>
  );
}
