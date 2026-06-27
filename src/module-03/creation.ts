// Module 3 — Creation and Boundaries: runnable demo

import {
  of, from, defer, fromEvent, interval, timer, EMPTY, NEVER, throwError
} from 'rxjs';
import { mergeMap, map } from 'rxjs/operators';

// --- of --- synchronous, known values
export const numbers$ = of(1, 2, 3);
// Notation: [{T0,n(1)}, {T0,n(2)}, {T0,n(3)}, {T0,c}]

// --- from --- array
export const items$ = from([10, 20, 30]);

// --- from --- Promise (note: Promise may already be running)
export const helloPromise$ = from(Promise.resolve('hello'));

// --- defer --- creates the source lazily at subscription time
// Each subscription creates a fresh fetch (unlike from(fetch(...)) which is eager)
export const lazyFetch$ = defer(() =>
  from(
    fetch('/api/user').then(r => r.json() as Promise<{ id: string; name: string }>)
  )
);

// --- fromEvent --- DOM event source (stub element; teardown removes listener)
const inputElement = document.createElement('input');
export const inputEvents$ = fromEvent<InputEvent>(inputElement, 'input');

// --- interval --- repeated time ticks (never completes)
export const ticks$ = interval(1000);

// --- timer --- single delayed emission
export const delayed$ = timer(2000);

// --- EMPTY --- completes immediately with no values
// Marble: |
// Use: catchError fallback, iif disabled branch
export const empty$ = EMPTY;

// --- NEVER --- never emits and never completes
// Marble: ----------
// Use: disable a polling interval when a feature flag is off
export const never$ = NEVER;

// --- throwError --- errors immediately
// Marble: #
export const failed$ = throwError(() => new Error('something went wrong'));

// --- HTTP boundary (without zod — TypeScript cast only) ---
// TypeScript protects the inside; runtime validation protects the boundary.
type UserDto = { id: string; name: string };

export const loadUser$ = defer(() =>
  from(fetch('/api/user'))
).pipe(
  mergeMap(response => from(response.json() as Promise<unknown>)),
  map(value => value as UserDto)
);
