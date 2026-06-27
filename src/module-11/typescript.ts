// Module 11 — TypeScript and Runtime Safety: runnable demo

import { from, mergeMap, filter, map } from 'rxjs';
import type { OperatorFunction, MonoTypeOperatorFunction, ObservableInput } from 'rxjs';

// --- validValues: filter out null and undefined, narrowing the type ---
// TypeScript narrows T | null | undefined → T inside the pipeline.
export function validValues<T>(): OperatorFunction<T | null | undefined, T> {
  return source$ =>
    source$.pipe(
      filter((value): value is T => value !== null && value !== undefined)
    );
}

// --- toString: transform any value to its string representation ---
export function toString<T>(): OperatorFunction<T, string> {
  return source$ => source$.pipe(map(v => String(v)));
}

// --- requireNonEmpty: keep only non-empty strings (MonoTypeOperatorFunction) ---
// MonoTypeOperatorFunction<T> is a specialization of OperatorFunction<T, T>.
export function requireNonEmpty(): MonoTypeOperatorFunction<string> {
  return source$ => source$.pipe(filter(v => v.length > 0));
}

// --- keepLatestTyped: illustrates ObservableInput<T> ---
// ObservableInput<R> covers Observable<R>, Promise<R>, arrays, and iterables,
// so the project function can return any of those without wrapping.
export function keepLatestTyped<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(mergeMap(project));
}

// --- ActionSchema: discriminated union (plain objects, no zod required) ---
// TypeScript enforces the union at compile time inside the pipeline.
// For runtime boundary validation, pair with a parser like zod (not installed here).
type SearchChangedAction = { type: 'SearchChanged'; term: string };
type LoadSucceededAction = {
  type: 'LoadSucceeded';
  results: Array<{ id: string; title: string }>;
};
type LoadFailedAction = { type: 'LoadFailed'; error: string };

export type ActionSchema = SearchChangedAction | LoadSucceededAction | LoadFailedAction;

// --- Usage example: validValues in a pipeline ---
// Use from() with a typed array to avoid the variadic-tuple overload of of()
const possibleNames$ = from(
  ['Alice', null, 'Bob', undefined, 'Carol'] as Array<string | null | undefined>
);

// After validValues(), TypeScript knows the value is string — no cast needed.
export const cleanNames$ = possibleNames$.pipe(
  validValues(),
  map(name => name.toUpperCase())
);

/*
 * Trust boundary table:
 *
 *   Source                 | Needs runtime validation?
 *   HTTP response          | yes
 *   WebSocket message      | yes
 *   localStorage           | yes
 *   URL params             | yes
 *   form data              | usually yes
 *   internal reducer output| no, if typed correctly
 *   hardcoded test data    | usually no
 *
 * TypeScript protects the inside of the application.
 * Runtime validation (zod, etc.) protects the boundary.
 */
