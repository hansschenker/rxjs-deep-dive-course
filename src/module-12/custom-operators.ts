// Module 12 — Custom Operators and DSL Design | Companion code for RxJS Deep Dive

import {
  of,
  timer,
  map,
  filter,
  debounceTime,
  distinctUntilChanged,
  switchMap,
  mergeMap,
  concatMap,
  exhaustMap,
  catchError,
  startWith,
  scan,
  retry,
  type OperatorFunction,
  type MonoTypeOperatorFunction,
  type ObservableInput,
} from 'rxjs';

// ─── Section 1: Composition-First Style ──────────────────────────────────────
// A custom operator is a named policy with tests and error handling.
// Raw pipeline noise becomes readable architecture:
//   input$.pipe(validSearchText())  reads as a behavior story, not implementation.

/** Trim, minimum-length filter, debounce, and deduplicate a text stream. */
export function validSearchText(
  minLength = 3,
  waitMs    = 300,
): OperatorFunction<string, string> {
  return source$ =>
    source$.pipe(
      map(value => value.trim()),
      filter(value => value.length >= minLength),
      debounceTime(waitMs),
      distinctUntilChanged(),
    );
}

// ─── Section 2: Policy-Named Flattening Operators ────────────────────────────
// Renaming RxJS flattening operators makes code read as architecture.
//   const results$ = searchText$.pipe(keepLatest(term => searchApi(term)));

/** switchMap policy — cancel the previous inner when a new outer value arrives.
 *  Use for reads: typeahead, route loading, live preview.
 *  Do NOT use for non-idempotent writes. */
export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>,
): OperatorFunction<T, R> {
  return source$ => source$.pipe(switchMap(project));
}

/** mergeMap policy — all inners run concurrently; no cancellation, no queuing.
 *  Use when order and overlap do not matter (e.g. parallel analytics calls). */
export function allowConcurrent<T, R>(
  project: (value: T) => ObservableInput<R>,
): OperatorFunction<T, R> {
  return source$ => source$.pipe(mergeMap(project));
}

/** concatMap policy — queue outer values; start each inner only when the previous completes.
 *  Use for ordered writes: upload queue, sequential animations. */
export function queueWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>,
): OperatorFunction<T, R> {
  return source$ => source$.pipe(concatMap(project));
}

/** exhaustMap policy — drop new outer values while an inner is still active.
 *  Use for login buttons, submit-once guards, and double-click prevention. */
export function ignoreWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>,
): OperatorFunction<T, R> {
  return source$ => source$.pipe(exhaustMap(project));
}

// ─── Section 3: State DSL Operators ──────────────────────────────────────────

/** Prepend an initial value so every subscriber receives a synchronous seed.
 *  Makes the state pipeline read: scan → startWithInitial → shareReplay. */
export function startWithInitial<T>(
  initial: T,
): OperatorFunction<T, T> {
  return source$ => source$.pipe(startWith(initial));
}

// ─── Section 4: Error DSL Operators ──────────────────────────────────────────

/** Convert an error into a typed domain action so the outer stream stays alive.
 *  The stream completes normally after the recovery action. */
export function recoverAsAction<T, A>(
  toAction: (error: unknown) => A,
): OperatorFunction<T, T | A> {
  return source$ =>
    source$.pipe(
      catchError(error => of(toAction(error))),
    );
}

/**
 * Retry with exponential backoff.
 * Attempt n waits baseDelayMs × 2^n ms before the next attempt.
 * After maxRetries are exhausted the error propagates.
 *
 * Retry delays (baseDelayMs = 1000):
 *   attempt 1 → 2000 ms, attempt 2 → 4000 ms, attempt 3 → 8000 ms → error
 */
export function retryWithBackoff<T>(
  maxRetries  = 3,
  baseDelayMs = 1000,
): MonoTypeOperatorFunction<T> {
  return retry({
    count: maxRetries,
    delay: (_, n) => timer(Math.pow(2, n) * baseDelayMs),
  });
}

// ─── Section 5: Stateful Custom Operators ────────────────────────────────────

/**
 * Emit the running average of a number stream.
 * State lives entirely inside the scan accumulator — no external mutable variable.
 */
export function runningAverage(): OperatorFunction<number, number> {
  return source$ =>
    source$.pipe(
      scan(
        (acc, value) => ({ sum: acc.sum + value, count: acc.count + 1 }),
        { sum: 0, count: 0 },
      ),
      map(({ sum, count }) => sum / count),
    );
}

/**
 * Suppress consecutive emissions that are deeply equal.
 * Uses a JSON.stringify comparator as a simple structural equality check.
 * Note: does not handle circular references or non-serialisable values.
 */
export function distinctUntilDeepChanged<T>(): MonoTypeOperatorFunction<T> {
  return distinctUntilChanged<T>(
    (a, b) => JSON.stringify(a) === JSON.stringify(b),
  );
}

// ─── Section 6: Naming Rules ──────────────────────────────────────────────────
/*
 * Good operator names express POLICY, not implementation detail:
 *
 *   Weak:   processValue, handleData, customMap
 *   Better: validValues, validSearchText, keepLatest, queueWhileBusy,
 *           recoverAsAction, startWithInitial, retryWithBackoff, shareLatestState
 *
 * A custom operator is only a reliable building block when it has tests.
 * Use TestScheduler marble tests (Module 13) to verify time-dependent behaviour.
 */

// ─── Section 7: Demonstrations ───────────────────────────────────────────────

function demonstrateValidSearchText(): void {
  // waitMs = 0 so debounce does not suppress values in this synchronous of() demo
  of('  ', 'rx', 'rxjs', 'rxjs')
    .pipe(validSearchText(3, 0))
    .subscribe(text => console.log('[validSearchText]', text));
  // '  ' → trimmed to '' (too short)
  // 'rx' → trimmed to 'rx' (too short)
  // 'rxjs' → passes filter, emits 'rxjs'
  // 'rxjs' → duplicate, suppressed by distinctUntilChanged
  // Output: rxjs
}

function demonstrateRunningAverage(): void {
  of(10, 20, 30, 40)
    .pipe(runningAverage())
    .subscribe(avg => console.log('[runningAverage]', avg));
  // Output: 10, 15, 20, 25
}

demonstrateValidSearchText();
demonstrateRunningAverage();
