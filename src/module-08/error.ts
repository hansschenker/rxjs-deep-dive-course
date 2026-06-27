// Module 8 — Error and Recovery Policies | Companion code for RxJS Deep Dive

import {
  of, throwError, EMPTY, Subject, timer, TimeoutError,
  catchError, map, finalize, tap, retry, timeout, switchMap,
  type Observable, type OperatorFunction
} from 'rxjs';

// ─── Shared types ─────────────────────────────────────────────────────────────

type Result = { id: string };

export type SearchAction =
  | { type: 'SearchStarted'; term: string }
  | { type: 'SearchSucceeded'; results: Result[] }
  | { type: 'SearchFailed'; error: string };

// Mock API — returns data for normal terms, errors for the term 'fail'
function mockSearchApi(term: string): Observable<Result[]> {
  if (term === 'fail') return throwError(() => new Error('API error for: ' + term));
  return of([{ id: term }]);
}

// ─── Section 1: Error terminates the stream ───────────────────────────────────
// After # (error), no more values flow.
// catchError intercepts and replaces the failed stream with another Observable.

function demoErrorTerminal(): void {
  throwError(() => new Error('fail'))
    .pipe(catchError(e => of('recovered: ' + (e as Error).message)))
    .subscribe(v => console.log('[terminal]', v));
  // [terminal] recovered: fail
}

// ─── Section 2: catchError basic ──────────────────────────────────────────────
// Replace a failed stream with a fallback Observable.

function demoCatchErrorFallback(): void {
  throwError(() => new Error('network failure'))
    .pipe(catchError(() => of('fallback value')))
    .subscribe(v => console.log('[catchError]', v));
  // [catchError] fallback value
}

// ─── Section 3: Inner vs outer error boundary ─────────────────────────────────
// This is one of the most important RxJS architecture rules.

function demoOuterCatchWrong(): void {
  // WRONG — outer catchError replaces the entire switchMap stream.
  // After the first API error, the outer stream is replaced by of([]).
  // Future values from searchText$ are never processed.
  const searchText$ = new Subject<string>();

  searchText$.pipe(
    switchMap(term => mockSearchApi(term)),
    catchError(() => of([] as Result[]))  // outer: kills the search stream
  ).subscribe(v => console.log('[outer-WRONG]', v));

  searchText$.next('hello');  // → [{id:'hello'}]
  searchText$.next('fail');   // → outer catch fires → [] → outer stream ends
  searchText$.next('world');  // never reaches switchMap — outer stream is gone
  searchText$.complete();
}

function demoInnerCatchCorrect(): void {
  // CORRECT — inner catchError handles only one failing request.
  // The outer search stream stays alive for future terms.
  const searchText$ = new Subject<string>();

  searchText$.pipe(
    switchMap(term =>
      mockSearchApi(term).pipe(
        catchError(() => of([] as Result[]))  // inner: only this request fails
      )
    )
  ).subscribe(v => console.log('[inner-OK]', v));

  searchText$.next('hello');  // → [{id:'hello'}]
  searchText$.next('fail');   // → [] (request error caught; outer stream alive)
  searchText$.next('world');  // → [{id:'world'}]
  searchText$.complete();
}

// ─── Section 4: Error as value (discriminated union) ─────────────────────────
// Errors become domain actions — the state stream never terminates.

function demoErrorAsValue(): void {
  const searchText$ = new Subject<string>();

  searchText$.pipe(
    switchMap(term =>
      mockSearchApi(term).pipe(
        map(results => ({ type: 'SearchSucceeded', results }) as SearchAction),
        catchError(e =>
          of({ type: 'SearchFailed', error: String(e) } as SearchAction)
        )
      )
    )
  ).subscribe(a => console.log('[error-as-value]', a));

  searchText$.next('hello');  // SearchSucceeded { results: [{id:'hello'}] }
  searchText$.next('fail');   // SearchFailed { error: 'Error: API error for: fail' }
  searchText$.complete();
}

// ─── Section 5: recoverAsAction — custom error-policy operator ────────────────
// Converts any upstream error into a domain action value.
// Exported so it can be reused across the application.

export function recoverAsAction<T, A>(
  toAction: (error: unknown) => A
): OperatorFunction<T, T | A> {
  return source$ =>
    source$.pipe(
      catchError(error => of(toAction(error)))
    );
}

function demoRecoverAsAction(): void {
  type LoadAction =
    | { type: 'LoadSucceeded'; data: string }
    | { type: 'LoadFailed'; error: string };

  throwError(() => new Error('load failed')).pipe(
    map(data => ({ type: 'LoadSucceeded', data }) as LoadAction),
    recoverAsAction(error => ({ type: 'LoadFailed', error: String(error) }) as LoadAction)
  ).subscribe(a => console.log('[recover-as-action]', a));
  // [recover-as-action] { type: 'LoadFailed', error: 'Error: load failed' }
}

// ─── Section 6: EMPTY as recovery ────────────────────────────────────────────
// Returning EMPTY swallows the error and completes the stream cleanly.
// Use when the error is expected, harmless, and the right response is silence.

function demoEmptyRecovery(): void {
  throwError(() => new Error('silent error'))
    .pipe(catchError(() => EMPTY))
    .subscribe({
      next: v => console.log('[empty-recovery] value:', v),
      complete: () => console.log('[empty-recovery] completed cleanly — no error propagated')
    });
  // [empty-recovery] completed cleanly — no error propagated
}

// ─── Section 7: finalize ──────────────────────────────────────────────────────
// finalize runs on: source complete ✓  |  source error ✓  |  unsubscribe ✓
// The RxJS equivalent of a finally block.

function demoFinalize(): void {
  // Stream that completes normally — finalize still runs
  of('data').pipe(
    tap(() => console.log('[finalize] start')),
    finalize(() => console.log('[finalize] end — completed normally'))
  ).subscribe(v => console.log('[finalize] value:', v));

  // Stream that errors — finalize runs before catchError yields a recovery value
  throwError(() => new Error('boom')).pipe(
    tap(() => console.log('[finalize-error] start')),
    finalize(() => console.log('[finalize-error] end — error path')),
    catchError(e => of('[finalize-error] caught: ' + (e as Error).message))
  ).subscribe(v => console.log('[finalize-error] value:', v));
}

// ─── Section 8: retry basic ───────────────────────────────────────────────────
// retry({ count: N }) resubscribes to the source up to N times on error.
// Retries are immediate — no delay between attempts.

function demoRetryBasic(): void {
  let attempt = 0;
  of(null).pipe(
    map(() => {
      attempt += 1;
      if (attempt < 3) throw new Error(`attempt ${attempt} failed`);
      return 'success after retries';
    }),
    retry({ count: 3 })
  ).subscribe({
    next: v => console.log('[retry-basic]', v),
    error: e => console.log('[retry-basic] gave up:', (e as Error).message)
  });
  // [retry-basic] success after retries  (succeeds on attempt 3)
}

// ─── Section 9: Exponential backoff ──────────────────────────────────────────
// delay: (error, retryCount) => Observable — retry waits for that Observable to emit.
// retryCount starts at 1 on the first retry.
// Using 100 ms base so the demo finishes quickly (use 1000 ms in production).
//
// Attempt 1 fails → wait 2^1 × 100 = 200 ms → retry
// Attempt 2 fails → wait 2^2 × 100 = 400 ms → retry
// Attempt 3 succeeds

function demoExponentialBackoff(): void {
  let attempt = 0;
  of(null).pipe(
    map(() => {
      attempt += 1;
      if (attempt <= 2) throw new Error(`attempt ${attempt} failed`);
      return 'recovered after backoff';
    }),
    retry({
      count: 4,
      delay: (_error, retryCount) => timer(Math.pow(2, retryCount) * 100)
    })
  ).subscribe({
    next: v => console.log('[backoff]', v),
    error: e => console.log('[backoff] final error:', (e as Error).message)
  });
  // After ~600 ms total: [backoff] recovered after backoff
}

// ─── Section 10: timeout policy ───────────────────────────────────────────────
// timeout(ms) throws TimeoutError when the source does not emit within the window.
// Check instanceof TimeoutError in catchError to separate timeout from other failures.

function demoTimeout(): void {
  // timer(2000) would emit after 2 s; timeout(500) fires first with TimeoutError
  timer(2000).pipe(
    map(() => 'late response'),
    timeout(500),
    catchError(error => {
      if (error instanceof TimeoutError) {
        return of({ type: 'Timeout' as const });
      }
      return throwError(() => error as Error);
    })
  ).subscribe(v => console.log('[timeout]', v));
  // After 500 ms: [timeout] { type: 'Timeout' }
}

// ─── Run all demos ────────────────────────────────────────────────────────────

demoErrorTerminal();
demoCatchErrorFallback();
demoOuterCatchWrong();
demoInnerCatchCorrect();
demoErrorAsValue();
demoRecoverAsAction();
demoEmptyRecovery();
demoFinalize();
demoRetryBasic();
demoExponentialBackoff();
demoTimeout();
