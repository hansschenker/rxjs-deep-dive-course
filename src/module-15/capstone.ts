// Module 15 — Capstone Project: runnable demo

import { fromEvent, of, merge, Observable } from 'rxjs';
import {
  map, filter, debounceTime, distinctUntilChanged,
  switchMap, exhaustMap, catchError, startWith,
  scan, retry, shareReplay
} from 'rxjs/operators';
import { timer } from 'rxjs';
import type { OperatorFunction, MonoTypeOperatorFunction, ObservableInput } from 'rxjs';

// ============================================================
// Domain Model
// ============================================================

export type Result = { id: string; title: string };

export type SearchStatus = 'idle' | 'typing' | 'loading' | 'success' | 'failure';

export type State = {
  query:   string;
  status:  SearchStatus;
  results: Result[];
  error?:  string;
};

export type Action =
  | { type: 'QueryChanged';    query:   string     }
  | { type: 'SearchStarted';   query:   string     }
  | { type: 'SearchSucceeded'; results: Result[]   }
  | { type: 'SearchFailed';    error:   string     }
  | { type: 'SaveRequested'                        }
  | { type: 'SaveSucceeded'                        }
  | { type: 'SaveFailed';      error:   string     }
  | { type: 'SearchCleared'                        };

// ============================================================
// Initial State
// ============================================================

export const initialState: State = {
  query:   '',
  status:  'idle',
  results: []
};

// ============================================================
// Pure Reducer
// ============================================================

export function update(state: State, action: Action): State {
  switch (action.type) {
    case 'QueryChanged':
      return {
        ...state,
        query:  action.query,
        status: action.query.trim() ? 'typing' : 'idle'
      };
    case 'SearchStarted':
      return { ...state, query: action.query, status: 'loading', error: undefined };
    case 'SearchSucceeded':
      return { ...state, status: 'success', results: action.results, error: undefined };
    case 'SearchFailed':
      return { ...state, status: 'failure', results: [], error: action.error };
    case 'SaveRequested':
      return state;
    case 'SaveSucceeded':
      return state;
    case 'SaveFailed':
      return { ...state, error: action.error };
    case 'SearchCleared':
      return initialState;
  }
}

// ============================================================
// DSL Operators (full course vocabulary)
// ============================================================

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

/** switchMap policy: cancel previous inner when a new outer value arrives. */
export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(switchMap(project));
}

/** exhaustMap policy: drop new outer values while an inner is active. */
export function ignoreWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(exhaustMap(project));
}

/** Convert errors to typed domain actions so the outer stream stays alive. */
export function recoverAsAction<T, A>(
  toAction: (error: unknown) => A
): OperatorFunction<T, T | A> {
  return source$ =>
    source$.pipe(
      catchError(error => of(toAction(error)))
    );
}

/** Exponential backoff: attempt n → wait baseDelayMs × 2^(n-1) → retry. */
export function retryWithBackoff<T>(
  maxRetries  = 3,
  baseDelayMs = 500
): MonoTypeOperatorFunction<T> {
  return retry({
    count: maxRetries,
    delay: (_err, retryCount) => timer(Math.pow(2, retryCount - 1) * baseDelayMs)
  });
}

// ============================================================
// Stubs (replace with real implementations in production)
// ============================================================

// DOM elements — in a real app, query from the DOM or pass in
const inputElement = document.createElement('input');
const saveButton   = document.createElement('button');

function searchApi(query: string): Observable<Result[]> {
  return of([{ id: '1', title: `Result for ${query}` }]);
}

let currentResults: Result[] = [];

function saveResults(_results: Result[]): Observable<void> {
  // Stub — would POST to the API in production
  return of(void 0);
}

// ============================================================
// Input Stream
// ============================================================

// Every keystroke becomes a QueryChanged action.
export const queryChanged$ = fromEvent<InputEvent>(inputElement, 'input').pipe(
  map(event => (event.target as HTMLInputElement).value),
  map((query): Action => ({ type: 'QueryChanged', query }))
);

// ============================================================
// Search Effect (keepLatest = switchMap)
// ============================================================
// If the user types again before results arrive, the earlier request is cancelled.

export const searchEffect$ = queryChanged$.pipe(
  filter((a): a is Extract<Action, { type: 'QueryChanged' }> =>
    a.type === 'QueryChanged'
  ),
  map(a => a.query),
  validSearchText(3, 300),
  keepLatest(query =>
    searchApi(query).pipe(
      retryWithBackoff(2, 200),
      map((results): Action => ({ type: 'SearchSucceeded', results })),
      startWith<Action>({ type: 'SearchStarted', query }),
      recoverAsAction((error): Action => ({
        type:  'SearchFailed',
        error: String(error)
      }))
    )
  )
);

// ============================================================
// Save Effect (ignoreWhileBusy = exhaustMap)
// ============================================================
// Double-clicks on Save are dropped while a save is in flight.

export const saveEffect$ = fromEvent(saveButton, 'click').pipe(
  ignoreWhileBusy(() =>
    saveResults(currentResults).pipe(
      map((): Action => ({ type: 'SaveSucceeded' })),
      recoverAsAction((error): Action => ({
        type:  'SaveFailed',
        error: String(error)
      }))
    )
  )
);

// ============================================================
// State Stream
// ============================================================
// All action sources merge; scan builds immutable state after each action.
// shareReplay(1) gives every subscriber the latest state immediately.

export const actions$ = merge(queryChanged$, searchEffect$, saveEffect$);

export const state$ = actions$.pipe(
  scan(update, initialState),
  startWith(initialState),
  shareReplay({ bufferSize: 1, refCount: true })
);

/*
 * Architecture diagram:
 *
 *   DOM input                       Save button click
 *       ↓                                 ↓
 *   fromEvent                         fromEvent
 *       ↓                                 ↓
 *   QueryChanged actions          ignoreWhileBusy(saveResults)
 *       ↓                                 ↓
 *   validSearchText        SaveRequested / SaveSucceeded / SaveFailed
 *       ↓                                 ↓
 *   keepLatest(searchApi                  |
 *     + retryWithBackoff)                 |
 *       ↓                                 |
 *   SearchStarted / Succeeded / Failed    |
 *       ↓                                 ↓
 *                 merge all actions
 *                        ↓
 *               scan(update, initialState)
 *                        ↓
 *                  shareReplay(1)
 *                        ↓
 *                      state$
 *                        ↓
 *               render / framework binding
 *
 * Policy contrast:
 *   Search → keepLatest   (newer query cancels old; only latest intent matters)
 *   Save   → ignoreWhileBusy (mid-flight save must not be interrupted)
 */

