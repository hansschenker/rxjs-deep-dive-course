// Module 8 — Error and Recovery Policies: runnable demo

import { of, EMPTY, throwError, timer, TimeoutError } from 'rxjs';
import { catchError, finalize, retry, timeout, tap } from 'rxjs/operators';
import type { OperatorFunction } from 'rxjs';

// --- catchError with fallback value ---
// Replaces a failed stream with another stream (the fallback).
const risky$ = throwError(() => new Error('network failure'));

export const safe$ = risky$.pipe(
  catchError(() => of('fallback'))
);

// --- EMPTY recovery: swallow the error, complete cleanly ---
// Use when an error is expected, harmless, and the stream should just stop.
export const silenced$ = risky$.pipe(
  catchError(() => EMPTY)
);

// --- finalize: cleanup runs on complete, error, or unsubscribe ---
// The RxJS equivalent of a try/finally block.
const request$ = of('data');

export const tracked$ = request$.pipe(
  tap(() => console.log('show spinner')),
  finalize(() => console.log('hide spinner'))
);

// --- recoverAsAction: custom operator — error becomes a domain action ---
// Keeps the outer stream alive by converting errors to typed values.
export function recoverAsAction<T, A>(
  toAction: (error: unknown) => A
): OperatorFunction<T, T | A> {
  return source$ =>
    source$.pipe(
      catchError(error => of(toAction(error)))
    );
}

// --- retry with exponential backoff ---
// Attempt 1 → wait 2s → attempt 2 → wait 4s → ... → propagate after maxRetries
export const retriedRequest$ = request$.pipe(
  retry({
    count: 4,
    delay: (_error, retryCount) => timer(Math.pow(2, retryCount) * 1000)
  })
);

// --- timeout + TimeoutError handling ---
// timeout(ms) throws TimeoutError if the source doesn't emit in time.
// Check instanceof TimeoutError to handle it separately from other errors.
export const timedRequest$ = request$.pipe(
  timeout(5000),
  catchError(error => {
    if (error instanceof TimeoutError) {
      return of({ type: 'Timeout' as const });
    }
    return throwError(() => error as Error);
  })
);

/*
 * recoverAsAction usage:
 *
 *   const resultAction$ = loadUser$.pipe(
 *     map(user => ({ type: 'LoadSucceeded', user }) as Action),
 *     recoverAsAction(error => ({
 *       type: 'LoadFailed',
 *       error: String(error)
 *     }) as Action)
 *   );
 *
 * Inner vs outer catchError — the most important error boundary rule:
 *
 * OUTER (terminates the outer stream — usually wrong for live streams):
 *   source$.pipe(
 *     switchMap(term => api(term)),
 *     catchError(() => of([]))  // outer stream replaced; no more searches
 *   );
 *
 * INNER (correct — each request handles its own error; outer stays alive):
 *   source$.pipe(
 *     switchMap(term =>
 *       api(term).pipe(
 *         catchError(() => of([]))  // only this request's error is caught
 *       )
 *     )
 *   );
 *
 * finalize runs on:
 *   source complete  ✓
 *   source error     ✓
 *   unsubscribe      ✓
 */
