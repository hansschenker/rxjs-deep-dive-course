// Module 5 — Flattening Policies: runnable demo

import { Observable, defer, from, EMPTY } from 'rxjs';
import { mergeMap, switchMap, concatMap, exhaustMap, expand, toArray } from 'rxjs/operators';
import type { OperatorFunction, ObservableInput } from 'rxjs';

/*
 * Four flattening policies:
 *   mergeMap   = allowConcurrent  — overlap allowed
 *   switchMap  = keepLatest       — cancel previous on new outer value
 *   concatMap  = queueWhileBusy   — one at a time, queue extras
 *   exhaustMap = ignoreWhileBusy  — drop new values while inner is active
 */

// --- mergeMap policy: Allow Overlap ---
export function allowConcurrent<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(mergeMap(project));
}

// --- switchMap policy: Keep Latest ---
// Use for reads (typeahead, route loading, live preview).
// Do NOT use for non-idempotent writes (payments, audit logs).
export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(switchMap(project));
}

// --- concatMap policy: Queue While Busy ---
// Each inner must complete before the next starts.
// Hazard: a never-completing inner blocks the queue forever.
export function queueWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(concatMap(project));
}

// --- exhaustMap policy: Ignore While Busy ---
// New outer values are dropped while an inner is active.
// Use for login buttons and submit-once flows.
export function ignoreWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(exhaustMap(project));
}

// --- expand: recursive flattening for pagination ---
// expand projects each output value back through the same function
// until the projection returns EMPTY, at which point recursion stops.
type Page = {
  items: string[];
  nextCursor: string | null;
};

function loadPage(cursor: string | null): Observable<Page> {
  const url = cursor ? `/api/items?cursor=${cursor}` : '/api/items';
  return defer(() => from(fetch(url))).pipe(
    mergeMap(r => from(r.json() as Promise<Page>))
  );
}

export const allItems$ = loadPage(null).pipe(
  expand(page => page.nextCursor ? loadPage(page.nextCursor) : EMPTY),
  mergeMap(page => page.items),
  toArray()
);
