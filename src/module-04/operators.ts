// Module 4 — Operators as Behavior Stories: runnable demo

import { of, interval, Subject } from 'rxjs';
import type { Observable } from 'rxjs';
import {
  map, filter, scan, tap, startWith, distinctUntilChanged,
  take, takeUntil, reduce
} from 'rxjs/operators';

// Domain type used throughout
type User = { name: string; active: boolean };

// Type annotation avoids the variadic-tuple overload of `of` in TS 6
const users$: Observable<User> = of(
  { name: 'Alice', active: true },
  { name: 'Bob',   active: false },
  { name: 'Carol', active: true }
);

// --- map ---
// for each source value, apply a function and emit the result
export const names$ = users$.pipe(
  map(user => user.name)
);

// --- filter ---
// keep values that satisfy the predicate; others are dropped
export const validUsers$ = users$.pipe(
  filter(user => user.active)
);

// --- scan --- accumulates state, emits after each value
// Behavior: scan(reducer, seed): [{T,a}] -> [{T,state}]
const clicks$ = of(null, null, null); // stub — would be fromEvent in a real app
export const count$ = clicks$.pipe(
  map(() => 1),
  scan((acc, delta) => acc + delta, 0)
  // emits: 1, 2, 3
);

// --- tap --- side effects; value passes through unchanged
// tap is always safe to add or remove — it never affects downstream values
export const debugPipeline$ = users$.pipe(
  tap(user => console.log('[source]',       user)),
  filter(user => user.active),
  tap(user => console.log('[after filter]', user)),
  map(user => user.name),
  tap(name => console.log('[after map]',    name))
);

// --- startWith --- emits an initial value synchronously before the source
const serverStatus$ = of('online');
export const status$ = serverStatus$.pipe(
  startWith('connecting')
  // emits: 'connecting', 'online'
);

// --- distinctUntilChanged --- suppresses consecutive duplicate values
export const dedupedStatus$ = serverStatus$.pipe(
  distinctUntilChanged()
);

// --- take --- completes after n values regardless of source
export const threeTicks$ = interval(1000).pipe(
  take(3)
  // emits: 0, 1, 2  then completes
);

// --- takeUntil --- completes when the notifier emits its first value
const destroy$ = new Subject<void>();
export const managedTicks$ = interval(1000).pipe(
  takeUntil(destroy$)
);

/** Ends managedTicks$ — mirrors component onDestroy / service cleanup. */
export function stopTicks(): void {
  destroy$.next();
  destroy$.complete();
}

// --- scan vs reduce ---
// scan: emits 1, 3, 6, 10 as values arrive
export const scanStream$ = of(1, 2, 3, 4).pipe(
  scan((acc, val) => acc + val, 0)
);

// reduce: emits only 10 after the source completes
export const reduceStream$ = of(1, 2, 3, 4).pipe(
  reduce((acc, val) => acc + val, 0)
);
// In reactive UI work, scan is almost always the right choice —
// state must be visible after each action, not only at the end.
