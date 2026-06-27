// Module 12 — Custom Operators and DSL Design: runnable demo

import { of, timer } from 'rxjs';
import {
  map, filter, debounceTime, distinctUntilChanged,
  switchMap, exhaustMap, catchError, startWith, scan, retry
} from 'rxjs/operators';
import type { OperatorFunction, MonoTypeOperatorFunction, ObservableInput } from 'rxjs';

// --- validSearchText: named policy operator ---
// Raw pipeline becomes readable architecture.
// "input$.pipe(validSearchText())" reads like a behavior story.
export function validSearchText(
  minLength = 3,
  waitMs    = 300
): OperatorFunction<string, string> {
  return source$ =>
    source$.pipe(
      map(value => value.trim()),
      filter(value => value.length >= minLength),
      debounceTime(waitMs),
      distinctUntilChanged()
    );
}

// --- keepLatest: switchMap as an explicit cancellation policy ---
// Use for reads (typeahead, route loading, live preview).
// Do NOT use for non-idempotent writes.
export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(switchMap(project));
}

// --- ignoreWhileBusy: exhaustMap as an explicit ignore policy ---
// Use for login buttons, submit-once guards.
export function ignoreWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(exhaustMap(project));
}

// --- recoverAsAction: error → domain action, stream stays alive ---
export function recoverAsAction<T, A>(
  toAction: (error: unknown) => A
): OperatorFunction<T, T | A> {
  return source$ =>
    source$.pipe(
      catchError(error => of(toAction(error)))
    );
}

// --- retryWithBackoff: exponential delay between retry attempts ---
// Retry delays: attempt 1 → baseDelayMs × 2^0, attempt 2 → × 2^1, …
// After maxRetries the error propagates.
export function retryWithBackoff<T>(
  maxRetries  = 4,
  baseDelayMs = 1000
): MonoTypeOperatorFunction<T> {
  return retry({
    count: maxRetries,
    delay: (_error, retryCount) => timer(Math.pow(2, retryCount - 1) * baseDelayMs)
  });
}

// --- runningAverage: stateful operator using scan ---
// scan accumulates {sum, count}; map derives the average.
// State lives in the scan accumulator — no external closure needed.
export function runningAverage(): OperatorFunction<number, number> {
  return source$ =>
    source$.pipe(
      scan(
        (acc, value) => ({ sum: acc.sum + value, count: acc.count + 1 }),
        { sum: 0, count: 0 }
      ),
      map(({ sum, count }) => sum / count)
    );
}

// --- startWithInitial: wraps startWith for the state DSL ---
// Makes the state pattern read as: scan → startWithInitial → shareReplay
export function startWithInitial<T>(
  initialValue: T
): OperatorFunction<T, T> {
  return source$ =>
    source$.pipe(
      startWith(initialValue)
    );
}

/*
 * Naming rules — good operator names express policy, not implementation:
 *
 *   Weak:   processValue, handleData, customMap
 *   Better: validValues, validSearchText, keepLatest, queueWhileBusy,
 *           recoverAsAction, startWithInitial, retryWithBackoff, shareLatestState
 *
 * A custom operator is only a reliable building block when it has tests.
 * Use TestScheduler marble tests (Module 13) to verify time-dependent behavior.
 */
