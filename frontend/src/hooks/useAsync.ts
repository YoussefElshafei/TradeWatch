/**
 * A minimal async-state hook: loading, data, error, and re-run.
 *
 * Deliberately not TanStack Query. `ARCHITECTURE.md` names it as the eventual server-state
 * layer and decision D13 says no state library lands before something needs it — this page
 * makes three requests and caches nothing, so a library would be answering a question
 * nobody has asked yet. When a second view wants the same data without refetching, that is
 * the moment, and this hook is small enough to delete.
 *
 * Two things a hand-rolled version usually gets wrong, both handled here:
 *
 * **Stale responses.** The in-flight request is aborted when its inputs change, and a
 * response whose key no longer matches is ignored. Without that, switching symbols quickly
 * renders whichever response happens to land last rather than the one that was asked for.
 *
 * **Loading is derived, not assigned.** The obvious implementation calls `setState` in the
 * effect body to flip to "loading", which React 19 flags — it schedules an extra render
 * before the effect's real work even starts. Instead the result carries the key it belongs
 * to, and anything that does not match the current key *is* loading. Same behaviour, one
 * fewer render, and no rule to suppress.
 */

import { useCallback, useEffect, useRef, useState } from 'react';

export type AsyncState<T> =
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; error: Error };

export interface AsyncResult<T> {
  state: AsyncState<T>;
  reload: () => void;
}

interface Keyed<T> {
  key: string;
  state: AsyncState<T>;
}

const LOADING = { status: 'loading' } as const;

export function useAsync<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  dependencies: readonly unknown[],
): AsyncResult<T> {
  const [attempt, setAttempt] = useState(0);
  const key = `${JSON.stringify(dependencies)}#${attempt}`;

  const [result, setResult] = useState<Keyed<T>>({ key: '', state: LOADING });
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();

    operation(controller.signal)
      .then((data) => {
        if (controller.signal.aborted || !mounted.current) return;
        setResult({ key, state: { status: 'success', data } });
      })
      .catch((cause: unknown) => {
        if (controller.signal.aborted || !mounted.current) return;
        setResult({
          key,
          state: {
            status: 'error',
            error: cause instanceof Error ? cause : new Error('Unknown error'),
          },
        });
      });

    return () => {
      controller.abort();
    };
    // `operation` is a fresh closure on every render by design; `key` is what decides when
    // to refetch, and it is derived from the caller's dependency list.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const reload = useCallback(() => {
    setAttempt((value) => value + 1);
  }, []);

  // A result from a previous key is not this key's result, so it reads as loading.
  return { state: result.key === key ? result.state : LOADING, reload };
}
