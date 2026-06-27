// Module 10 — State as a Stream | Companion code for RxJS Deep Dive

import {
  Subject, BehaviorSubject, merge, of,
  scan, startWith, shareReplay, map, filter,
  distinctUntilChanged, catchError, switchMap, debounceTime
} from 'rxjs';

// ─── Section 2: Counter state types ───────────────────────────────────────────

export type State = {
  count: number;
};

export type Action =
  | { type: 'Increment' }
  | { type: 'Decrement' }
  | { type: 'Reset' };

export const initialState: State = { count: 0 };

// Pure reducer — no side effects, no async, no mutation
export function update(state: State, action: Action): State {
  switch (action.type) {
    case 'Increment': return { count: state.count + 1 };
    case 'Decrement': return { count: state.count - 1 };
    case 'Reset':     return initialState;
  }
}

// ─── Section 6: Search state types ────────────────────────────────────────────

export type SearchResult = { id: string; title: string };

export type SearchAction =
  | { type: 'SearchChanged'; term: string }
  | { type: 'SearchStarted'; term: string }
  | { type: 'SearchSucceeded'; results: SearchResult[] }
  | { type: 'SearchFailed'; error: string };

type SearchState = {
  status: 'idle' | 'loading' | 'success' | 'failure';
  results: SearchResult[];
};

const initialSearchState: SearchState = { status: 'idle', results: [] };

function updateSearch(state: SearchState, action: SearchAction): SearchState {
  switch (action.type) {
    case 'SearchChanged': return { ...state, status: 'loading' };
    case 'SearchStarted': return { ...state, status: 'loading' };
    case 'SearchSucceeded': return { status: 'success', results: action.results };
    case 'SearchFailed':  return { status: 'failure', results: [] };
  }
}

// ─── Section 1 & 2: Counter state demo ───────────────────────────────────────
// Formula: State = initial state + actions over time
//
// /*
//  * MVU Architecture
//  *
//  *   View → Action → Update → State → View
//  *
//  * RxJS version:
//  *
//  *   UI events → actions$ → scan(update, initialState) → state$ → render
//  *              ↑                                              |
//  *              └──────── effects ◄──────────────────────────┘
//  * Core state pattern:
//  *   scan + startWith(initialState) + shareReplay({ bufferSize: 1, refCount: true })
//  */

function demoCounterState(): void {
  const actions$ = new Subject<Action>();

  const state$ = actions$.pipe(
    scan(update, initialState),
    startWith(initialState),               // emit initial state before any action
    shareReplay({ bufferSize: 1, refCount: true })  // share + cache for late subscribers
  );

  state$.subscribe(s => console.log('[counter]', s));

  actions$.next({ type: 'Increment' });   // { count: 1 }
  actions$.next({ type: 'Increment' });   // { count: 2 }
  actions$.next({ type: 'Decrement' });   // { count: 1 }
  actions$.next({ type: 'Reset' });       // { count: 0 }
  actions$.complete();
}

// ─── Section 6: Effects and search pipeline ───────────────────────────────────
// Effects are asynchronous operations triggered by actions or state.
// UI actions and effect actions are merged into a single action stream.

function demoSearchEffect(): void {
  function mockSearch(term: string) {
    return of([{ id: 'r1', title: `result for ${term}` }] as SearchResult[]);
  }

  const uiActions$ = new Subject<SearchAction>();

  // Narrow to SearchChanged, extract term, then run the async search
  const searchEffect$ = uiActions$.pipe(
    filter((action): action is Extract<SearchAction, { type: 'SearchChanged' }> =>
      action.type === 'SearchChanged'
    ),
    map(action => action.term),
    debounceTime(300),        // wait for the user to stop typing
    distinctUntilChanged(),   // skip if the term has not changed
    switchMap(term =>
      mockSearch(term).pipe(
        map(results => ({ type: 'SearchSucceeded', results }) as SearchAction),
        catchError(e => of({ type: 'SearchFailed', error: String(e) } as SearchAction))
      )
    )
  );

  // Merge UI-dispatched actions and effect-produced actions
  const allActions$ = merge(uiActions$, searchEffect$);

  // Section 7: distinctUntilChanged() prevents re-renders when state did not change.
  // Default reference equality — reducers return new objects on every action, so for
  // object-shaped state you usually need a value comparator:
  //   distinctUntilChanged((a, b) => a.status === b.status && a.results === b.results)
  const searchState$ = allActions$.pipe(
    scan(updateSearch, initialSearchState),
    startWith(initialSearchState),
    distinctUntilChanged(),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  searchState$.subscribe(s => console.log('[search-state]', s));

  // UI dispatches SearchChanged; the effect pipeline converts it to SearchSucceeded.
  // debounceTime(300) means the effect fires 300 ms after the last emission.
  uiActions$.next({ type: 'SearchChanged', term: 'hello' });
  uiActions$.next({ type: 'SearchChanged', term: 'hello' }); // duplicate — deduplicated
  // After 300 ms debounce, searchEffect$ fires SearchSucceeded
  uiActions$.complete();
}

// ─── Section 8: Derived state slices ─────────────────────────────────────────
// Subscribers rarely need the full state object.
// Map to a slice and add distinctUntilChanged() to re-render only when that slice changes.
// This is the observable equivalent of a Redux selector.

function demoDerivedSlices(): void {
  const actions$ = new Subject<Action>();

  const state$ = actions$.pipe(
    scan(update, initialState),
    startWith(initialState),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  // Primitive slices compare by value — distinctUntilChanged() works without a comparator
  const count$ = state$.pipe(
    map(s => s.count),
    distinctUntilChanged()  // won't re-emit if count stays the same
  );

  count$.subscribe(c => console.log('[count-slice]', c));

  actions$.next({ type: 'Increment' });  // count: 1
  actions$.next({ type: 'Increment' });  // count: 2
  actions$.next({ type: 'Decrement' });  // count: 1
  actions$.next({ type: 'Reset' });      // count: 0
  actions$.complete();
}

// ─── Section 9: BehaviorSubject as simple local state ─────────────────────────
// Use BehaviorSubject when:
//   state has one value, one owner, and no action history is needed.
// Use scan + startWith + shareReplay when:
//   state evolves from multiple action types, effects feed back into state,
//   or multiple streams produce actions.
//
// /*
//  * State Design Rules
//  *
//  *   Reducers must be pure — no side effects, no async, no mutation.
//  *   Effects may be asynchronous — they read actions and produce more actions.
//  *   Rendering is a side effect at the edge — subscribe at the UI boundary.
//  */

function demoBehaviorSubjectState(): void {
  const count$ = new BehaviorSubject(0);

  // Imperative update using getValue()
  count$.next(count$.getValue() + 1);
  console.log('[behavior-subject] current:', count$.getValue()); // 1 (synchronous read)

  // Subscribe — immediately receives the current value (1)
  count$.subscribe(v => console.log('[behavior-subject] subscriber:', v));

  count$.next(count$.getValue() + 1); // 2 — subscriber also receives this
  count$.complete();
}

// ─── Run all demos ────────────────────────────────────────────────────────────

demoCounterState();
demoSearchEffect();
demoDerivedSlices();
demoBehaviorSubjectState();
