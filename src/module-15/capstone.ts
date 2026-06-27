// Module 15 — Capstone Project | Companion code for RxJS Deep Dive
//
// A complete MVU reactive application combining the full course vocabulary:
// input stream → validation → debounce → cancellation → HTTP effect →
// error-as-value recovery → state stream → shared state → render

import {
  Subject,
  merge,
  of,
  defer,
  timer,
  map,
  filter,
  debounceTime,
  distinctUntilChanged,
  switchMap,
  exhaustMap,
  catchError,
  startWith,
  scan,
  retry,
  shareReplay,
  type Observable,
  type OperatorFunction,
  type MonoTypeOperatorFunction,
  type ObservableInput,
} from 'rxjs';

// ─── Domain Model ─────────────────────────────────────────────────────────────

export type Result = {
  id:      string;
  title:   string;
  snippet: string;
};

export type AppState = {
  query:    string;
  results:  Result[];
  status:   'idle' | 'loading' | 'success' | 'error';
  error:    string | null;
  savedIds: Set<string>;
};

export const initialState: AppState = {
  query:    '',
  results:  [],
  status:   'idle',
  error:    null,
  savedIds: new Set<string>(),
};

// ─── Action Union (8 variants) ────────────────────────────────────────────────

export type Action =
  | { type: 'QueryChanged';    query:   string   }
  | { type: 'SearchStarted'                      }
  | { type: 'SearchSucceeded'; results: Result[] }
  | { type: 'SearchFailed';    error:   string   }
  | { type: 'SaveRequested';   id:      string   }
  | { type: 'SaveSucceeded';   id:      string   }
  | { type: 'SaveFailed';      error:   string   }
  | { type: 'Reset'                              };

// ─── Pure Reducer ─────────────────────────────────────────────────────────────
// Exhaustive switch — every Action variant produces a new AppState.
// No side effects; given the same (state, action) always returns the same result.

export function reducer(state: AppState, action: Action): AppState {
  switch (action.type) {
    case 'QueryChanged':
      return { ...state, query: action.query, error: null };

    case 'SearchStarted':
      return { ...state, status: 'loading', error: null };

    case 'SearchSucceeded':
      return { ...state, status: 'success', results: action.results, error: null };

    case 'SearchFailed':
      return { ...state, status: 'error', error: action.error, results: [] };

    case 'SaveRequested':
      // Optimistic: no state change until SaveSucceeded / SaveFailed
      return { ...state };

    case 'SaveSucceeded':
      return {
        ...state,
        savedIds: new Set([...state.savedIds, action.id]),
        error:    null,
      };

    case 'SaveFailed':
      return { ...state, error: action.error };

    case 'Reset':
      return initialState;
  }
}

// ─── DSL Operators ────────────────────────────────────────────────────────────
// Re-exported vocabulary so consumers can import from this module directly.

/** Trim, minimum-length filter (≥ 2), debounce 300 ms, deduplicate. */
export function validQuery(): OperatorFunction<string, string> {
  return source$ =>
    source$.pipe(
      map(v => v.trim()),
      filter(v => v.length >= 2),
      debounceTime(300),
      distinctUntilChanged(),
    );
}

/** Prepend an initial value so every subscriber receives a synchronous seed. */
export function startWithInitial<T>(v: T): OperatorFunction<T, T> {
  return source$ => source$.pipe(startWith(v));
}

/** Convert an error into a typed domain action so the outer stream stays alive. */
export function recoverAsAction<T, A>(
  fn: (error: unknown) => A,
): OperatorFunction<T, T | A> {
  return source$ =>
    source$.pipe(
      catchError(error => of(fn(error))),
    );
}

/**
 * Retry with exponential backoff.
 * Attempt n waits base × 2^n ms before the next try.
 * After max retries the error propagates to recoverAsAction.
 */
export function retryWithBackoff<T>(
  max  = 3,
  base = 1000,
): MonoTypeOperatorFunction<T> {
  return retry({
    count: max,
    delay: (_, n) => timer(Math.pow(2, n) * base),
  });
}

/** switchMap policy — cancel the previous inner when a new outer value arrives. */
export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>,
): OperatorFunction<T, R> {
  return source$ => source$.pipe(switchMap(project));
}

/** exhaustMap policy — drop new outer values while an inner is still active. */
export function ignoreWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>,
): OperatorFunction<T, R> {
  return source$ => source$.pipe(exhaustMap(project));
}

// ─── Mock APIs ────────────────────────────────────────────────────────────────
// In production: replace with real HTTP calls (Angular HttpClient, fetch, etc.)

function searchApi(query: string): Observable<Result[]> {
  return defer(() =>
    timer(50).pipe(
      map(() => [
        { id: '1', title: `RxJS — ${query}`,   snippet: `Reactive result for "${query}"` },
        { id: '2', title: `Guide — ${query}`,   snippet: `Deep-dive into "${query}"`      },
      ]),
    ),
  );
}

function saveApi(_id: string): Observable<void> {
  return defer(() =>
    timer(30).pipe(map(() => void 0 as void)),
  );
}

// ─── Action Source ────────────────────────────────────────────────────────────
// All user intent flows through this Subject.
// Effects filter their trigger action then merge back derived actions.

export const actions$ = new Subject<Action>();

// ─── queryChanged$ — valid search terms ──────────────────────────────────────
// Filters QueryChanged, extracts the query string, applies the DSL pipeline.
// Debounce and dedup live here; downstream effects stay simple.

export const queryChanged$ = actions$.pipe(
  filter((a): a is Extract<Action, { type: 'QueryChanged' }> =>
    a.type === 'QueryChanged',
  ),
  map(a => a.query),
  validQuery(),
);

// ─── searchEffect$ — keepLatest (switchMap) ───────────────────────────────────
// Newer query cancels the in-flight request for the older one.
// Each valid term emits: SearchStarted → (SearchSucceeded | SearchFailed).

export const searchEffect$: Observable<Action> = queryChanged$.pipe(
  keepLatest(term =>
    searchApi(term).pipe(
      retryWithBackoff(2, 200),
      map((results): Action => ({ type: 'SearchSucceeded', results })),
      startWith<Action>({ type: 'SearchStarted' }),
      recoverAsAction((error): Action => ({
        type:  'SearchFailed',
        error: String(error),
      })),
    ),
  ),
);

// ─── saveEffect$ — ignoreWhileBusy (exhaustMap) ───────────────────────────────
// Double-clicks or rapid retries while a save is in flight are dropped.
// Cancelling mid-flight would leave data in an unknown state — exhaust is safer.

export const saveEffect$: Observable<Action> = actions$.pipe(
  filter((a): a is Extract<Action, { type: 'SaveRequested' }> =>
    a.type === 'SaveRequested',
  ),
  ignoreWhileBusy(({ id }) =>
    saveApi(id).pipe(
      map((): Action => ({ type: 'SaveSucceeded', id })),
      recoverAsAction((error): Action => ({
        type:  'SaveFailed',
        error: String(error),
      })),
    ),
  ),
);

// ─── State Stream ─────────────────────────────────────────────────────────────
// All action sources merge into a single timeline.
// scan folds each action into immutable state.
// startWithInitial seeds every new subscriber synchronously.
// distinctUntilChanged prevents re-renders for reference-equal states.
// shareReplay(1) gives every late subscriber the latest state immediately.

export const allActions$ = merge(actions$, searchEffect$, saveEffect$);

export const state$ = allActions$.pipe(
  scan(reducer, initialState),
  startWithInitial(initialState),
  distinctUntilChanged(),
  shareReplay({ bufferSize: 1, refCount: true }),
);

// ─── Render ───────────────────────────────────────────────────────────────────
// The edge of the system: pure state → side effect (log / DOM update).

export function render(state: AppState): void {
  const saved = state.savedIds.size;
  const error = state.error ?? 'none';
  console.log(
    `[render] status=${state.status} query="${state.query}" ` +
    `results=${state.results.length} error=${error} saved=${saved}`,
  );
}

// ─── Architecture Diagram ─────────────────────────────────────────────────────
/*
 *   DOM input                           Save button click
 *       ↓                                     ↓
 *   actions$.next(QueryChanged)      actions$.next(SaveRequested)
 *       ↓                                     ↓
 *   queryChanged$ (filter + validQuery)  saveEffect$ (ignoreWhileBusy → saveApi)
 *       ↓                                     ↓
 *   searchEffect$ (keepLatest → searchApi)    SaveSucceeded / SaveFailed
 *       ↓                                     ↓
 *   SearchStarted / SearchSucceeded /          |
 *   SearchFailed                               |
 *       ↓                                     ↓
 *                      merge(actions$, searchEffect$, saveEffect$)
 *                                    ↓
 *                          scan(reducer, initialState)
 *                                    ↓
 *                            startWithInitial
 *                                    ↓
 *                           distinctUntilChanged
 *                                    ↓
 *                             shareReplay(1)
 *                                    ↓
 *                                  state$
 *                                    ↓
 *                           render / framework binding
 *
 * Policy contrast:
 *   Search → keepLatest      newer query cancels old; only latest intent matters
 *   Save   → ignoreWhileBusy mid-flight save must not be interrupted
 */

// ─── Wire-up and Demo ─────────────────────────────────────────────────────────

const subscription = state$.subscribe(render);

// Synchronous actions: state transitions happen immediately
actions$.next({ type: 'QueryChanged', query: 'rx' });         // too short for validQuery (min 2 after trim)
actions$.next({ type: 'QueryChanged', query: 'rxjs' });       // valid — triggers debounce (300 ms)
actions$.next({ type: 'SearchSucceeded', results: [         // direct dispatch for demo
  { id: '1', title: 'RxJS Operators', snippet: 'Map, filter, reduce over time' },
  { id: '2', title: 'RxJS Subjects',  snippet: 'Hot multicasting primitives'   },
]});
actions$.next({ type: 'SaveRequested', id: '1' });            // fires saveEffect$ → SaveSucceeded in ~30 ms

// Async dispatches within the 500 ms window
setTimeout(() => actions$.next({ type: 'Reset' }), 300);

// Unsubscribe after 500 ms — subscription lifetime ends here
setTimeout(() => subscription.unsubscribe(), 500);
