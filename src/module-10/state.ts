// Module 10 — State as a Stream: runnable demo

import { Subject, BehaviorSubject, merge, of } from 'rxjs';
import {
  scan, startWith, shareReplay, map, filter,
  distinctUntilChanged, catchError, switchMap, debounceTime
} from 'rxjs/operators';

// --- Domain Types ---
export type State = {
  count:  number;
  status: 'idle' | 'loading' | 'success' | 'failure';
  users:  string[];
};

export type Action =
  | { type: 'Increment' }
  | { type: 'Decrement' }
  | { type: 'Reset' }
  | { type: 'SearchChanged'; term: string }
  | { type: 'SearchSucceeded'; users: string[] }
  | { type: 'SearchFailed'; error: string };

// --- Initial State ---
export const initialState: State = {
  count:  0,
  status: 'idle',
  users:  []
};

// --- Pure Reducer ---
// Must be pure: no side effects, no async, no mutation.
export function update(state: State, action: Action): State {
  switch (action.type) {
    case 'Increment':
      return { ...state, count: state.count + 1 };
    case 'Decrement':
      return { ...state, count: state.count - 1 };
    case 'Reset':
      return initialState;
    case 'SearchChanged':
      return { ...state, status: 'loading' };
    case 'SearchSucceeded':
      return { ...state, status: 'success', users: action.users };
    case 'SearchFailed':
      return { ...state, status: 'failure', users: [] };
  }
}

// --- Actions Subject: the imperative entry point ---
// UI code calls actions$.next(action) to dispatch.
export const actions$ = new Subject<Action>();

// --- Search Effect: observes SearchChanged, produces more actions ---
function searchApi(term: string) {
  return of([`user for ${term}`]);
}

// Narrow the action stream to SearchChanged only, then extract the term
const searchTerm$ = actions$.pipe(
  filter((action): action is Extract<Action, { type: 'SearchChanged' }> =>
    action.type === 'SearchChanged'
  ),
  map(action => action.term)
);

// Effect: debounce → cancel stale requests → map result to actions
const searchEffect$ = searchTerm$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(term =>
    searchApi(term).pipe(
      map(users => ({ type: 'SearchSucceeded', users }) as Action),
      catchError(error =>
        of({ type: 'SearchFailed', error: String(error) } as Action)
      )
    )
  )
);

// --- State Stream: core MVU formula ---
// State = initial state + actions over time
// scan + startWith + shareReplay is the fundamental state pattern.
export const state$ = merge(actions$, searchEffect$).pipe(
  scan(update, initialState),
  startWith(initialState),
  shareReplay({ bufferSize: 1, refCount: true })
);

// --- Derived State Slices (observable selectors) ---
// distinctUntilChanged ensures slice-specific subscribers re-render only
// when their slice changes, not on every unrelated action.
export const count$ = state$.pipe(
  map(s => s.count),
  distinctUntilChanged()
);

export const userList$ = state$.pipe(
  map(s => s.users),
  distinctUntilChanged()
);

export const statusSlice$ = state$.pipe(
  map(s => s.status),
  distinctUntilChanged()
);

// --- BehaviorSubject: simple local state alternative ---
// Use when state has one value, one owner, and no action history is needed.
// Use scan + startWith + shareReplay when multiple action types or effects feed state.
export const localCount$ = new BehaviorSubject(0);
