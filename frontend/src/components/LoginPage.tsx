/** Sign-in form (Phase 5, ADR-0010). */

import { useState, type SubmitEvent } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';

import { ApiError } from '../api/auth';
import { useAuth } from '../hooks/useAuth';

interface NavigationState {
  from?: { pathname: string };
}

export function LoginPage() {
  const { login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: SubmitEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await login(email, password);
      const state = location.state as NavigationState | null;
      // Same status and message whichever way the login actually failed
      // (`ARCHITECTURE.md` §9.2) — the frontend has nothing more specific to show either.
      await navigate(state?.from?.pathname ?? '/', { replace: true });
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
        <p className="tagline">Sign in</p>
      </header>

      <form className="auth-form" onSubmit={(event) => void handleSubmit(event)}>
        <label htmlFor="login-email">Email</label>
        <input
          id="login-email"
          type="email"
          autoComplete="username"
          required
          value={email}
          onChange={(event) => {
            setEmail(event.target.value);
          }}
        />

        <label htmlFor="login-password">Password</label>
        <input
          id="login-password"
          type="password"
          autoComplete="current-password"
          required
          value={password}
          onChange={(event) => {
            setPassword(event.target.value);
          }}
        />

        {error && (
          <div className="panel panel-error" role="alert">
            <p className="panel-title">{error}</p>
          </div>
        )}

        <button type="submit" disabled={submitting}>
          {submitting ? 'Signing in…' : 'Sign in'}
        </button>
      </form>

      <p>
        No account? <Link to="/register">Register</Link>
      </p>
    </main>
  );
}
