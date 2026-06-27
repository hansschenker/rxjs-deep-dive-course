// Module 5 — Flattening Policies | Companion code for RxJS Deep Dive

/*
 * Higher-Order Observable Problem
 *
 * Sometimes a value creates another Observable:
 *
 *   const result$ = searchText$.pipe(
 *     map(term => searchApi(term))
 *   );
 *
 * The type becomes: Observable<Observable<SearchResult[]>>
 *
 * Notation: [{T, Observable<a>}] -> ???
 *
 * To use the inner values, the outer stream must be flattened.
 * The four flattening operators are not just operators — they are policies.
 */

/*
 * Four Flattening Policies
 *
 * Operator     | Policy            | New outer while inner active | Good for                  | Dangerous for
 * -------------|-------------------|------------------------------|---------------------------|----------------------
 * mergeMap     | allow overlap     | starts another inner         | independent parallel work | unbounded concurrency
 * switchMap    | only latest       | cancels previous inner       | cancelable reads          | critical writes
 * concatMap    | queue             | waits                        | ordered writes            | never-ending inner streams
 * exhaustMap   | ignore while busy | ignores new value            | submit/login protection   | important repeated changes
 */

import {
  of, EMPTY,
  mergeMap, switchMap, concatMap, exhaustMap,
  expand, toArray
} from 'rxjs';
import type { Observable, OperatorFunction, ObservableInput } from 'rxjs';

// ─── Domain types and mock services ──────────────────────────────────────────

interface DraftForm    { data: string }
interface SaveResult   { id: string }
interface SearchResult { term: string; results: string[] }
interface UploadResult { name: string; url: string }
interface LoginResult  { token: string }

function saveDraft(form: DraftForm): Observable<SaveResult> {
  return of({ id: `saved-${form.data}` });
}

function searchApi(term: string): Observable<SearchResult> {
  return of({ term, results: [`result for "${term}"`] });
}

function uploadFile(file: { name: string }): Observable<UploadResult> {
  return of({ name: file.name, url: `https://cdn.example.com/${file.name}` });
}

function login(credentials: { user: string }): Observable<LoginResult> {
  return of({ token: `token-for-${credentials.user}` });
}

// ─── Section 3: mergeMap — Allow Overlap ─────────────────────────────────────
/*
 * Source:       outer values over time
 * Trigger:      each outer value
 * Value:        values emitted by each projected inner Observable
 * Cardinality:  one outer value → many inner values
 * Time:         inner values flow whenever each inner emits
 * Concurrency:  overlap is allowed
 * Cancellation: outer unsubscribe cancels all active inner subscriptions;
 *               new outer values do NOT cancel previous inner streams
 * Termination:  completes after outer + all active inner streams complete
 *
 * Concurrency cap: mergeMap(project, 4) — limit to 4 concurrent inner subscriptions
 */

function demonstrateMergeMap(): void {
  const saveClicks$ = of(
    { data: 'draft-1' } satisfies DraftForm,
    { data: 'draft-2' } satisfies DraftForm
  );
  saveClicks$.pipe(
    mergeMap(form => saveDraft(form))
    // To cap concurrency: mergeMap(form => saveDraft(form), 4)
  ).subscribe(result => console.log('[mergeMap]', result));
}

// ─── Section 4: switchMap — Only Latest ──────────────────────────────────────
/*
 * Source:       outer values over time
 * Trigger:      each outer value
 * Value:        values from the latest projected inner Observable
 * Cardinality:  one outer value → many inner values
 * Time:         latest inner values flow when the active inner emits
 * Concurrency:  only one inner subscription is active at a time
 * Cancellation: new outer value cancels the previous inner subscription
 * Termination:  completes after outer + active inner complete
 *
 * Rule: switchMap is a READ policy, not a write policy.
 *   Use switchMap when cancellation is semantically correct:
 *     the previous result is no longer needed.
 *   Do NOT use switchMap when every operation must complete:
 *     payments, form saves, audit logs, non-idempotent mutations.
 *     Use concatMap (queue) or exhaustMap (ignore while busy) instead.
 */

function demonstrateSwitchMap(): void {
  const searchInput$ = of('r', 'rx', 'rxjs');
  searchInput$.pipe(
    switchMap(term => searchApi(term))
  ).subscribe(result => console.log('[switchMap]', result));
  // Earlier inner streams are cancelled; only the last term's result matters
}

// ─── Section 5: concatMap — Queue ────────────────────────────────────────────
/*
 * Source:       outer values over time
 * Trigger:      each outer value
 * Value:        values from each inner Observable in source order
 * Cardinality:  one outer value → many inner values
 * Time:         an inner starts ONLY after the previous inner completes
 * Concurrency:  no overlap
 * Cancellation: outer unsubscribe cancels active inner and clears queued work
 * Termination:  completes after outer + all queued inner streams complete
 *
 * Hazard: a never-completing inner blocks the queue forever.
 * Mitigation: pipe take(1) on the inner to force completion.
 */

function demonstrateConcatMap(): void {
  const files$ = of(
    { name: 'a.jpg' },
    { name: 'b.png' },
    { name: 'c.gif' }
  );
  files$.pipe(
    concatMap(file => uploadFile(file))
  ).subscribe(result => console.log('[concatMap]', result));
}

// ─── Section 6: exhaustMap — Ignore While Busy ───────────────────────────────
/*
 * Source:       outer values over time
 * Trigger:      an outer value ONLY when no inner is currently active
 * Value:        values from the active inner Observable
 * Cardinality:  accepted outer values → many inner values; ignored values → nothing
 * Time:         inner values flow while active inner emits
 * Concurrency:  only one inner at a time
 * Cancellation: new outer values do NOT cancel the active inner — they are dropped
 * Termination:  completes after outer + active inner complete
 *
 * Hazard: ignored values are permanently lost.
 */

function demonstrateExhaustMap(): void {
  // Second click is dropped while the first login is still in progress
  const loginClicks$ = of({ user: 'alice' }, { user: 'bob' });
  loginClicks$.pipe(
    exhaustMap(credentials => login(credentials))
  ).subscribe(result => console.log('[exhaustMap]', result));
}

// ─── Section 7: Exported policy wrappers ─────────────────────────────────────
// Named wrappers make the chosen policy explicit at each call site.

export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(switchMap(project));
}

export function allowConcurrent<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(mergeMap(project));
}

export function queueWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(concatMap(project));
}

export function ignoreWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(exhaustMap(project));
}

// ─── Section 8: expand — Recursive Flattening / Pagination ───────────────────
/*
 * expand projects each output value back through the same projection function
 * until the projection returns EMPTY, at which point recursion stops.
 *
 * Source:       first page emission
 * Trigger:      each output value is projected back into the same function
 * Value:        values from each recursively projected inner Observable
 * Cancellation: return EMPTY from the projection to stop recursion
 * Termination:  completes when the projection returns EMPTY
 *
 * expand replaces recursive subscribe-inside-subscribe patterns with
 * a declarative, managed flattening.
 */

interface Page {
  items: string[];
  page: number;
  hasMore: boolean;
}

function simulatePage(pageNum: number): Observable<Page> {
  // Simulated data — in production this would be an HTTP call
  const items = [`item-${pageNum}-a`, `item-${pageNum}-b`];
  return of({ items, page: pageNum, hasMore: pageNum < 3 });
}

function demonstrateExpand(): void {
  simulatePage(1).pipe(
    expand(page => page.hasMore ? simulatePage(page.page + 1) : EMPTY),
    toArray()
  ).subscribe(pages => console.log('[expand] all pages', pages));
  // Loads pages 1, 2, 3 then stops (hasMore becomes false at page 3)
}

// ─── Run all examples ─────────────────────────────────────────────────────────

demonstrateMergeMap();
demonstrateSwitchMap();
demonstrateConcatMap();
demonstrateExhaustMap();
demonstrateExpand();
