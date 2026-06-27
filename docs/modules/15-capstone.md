# Module 15 — Capstone Project

## Goal

Build a complete reactive application that combines the full course.

## Project: Reactive Search Dashboard

The capstone is a small but complete RxJS application.

It includes:

* input stream
* validation
* debouncing
* cancellation
* HTTP effect
* error-as-value recovery
* state stream
* shared state
* custom operators
* marble tests
* UI integration

## 1. Domain Model

```ts
type Result = {
  id: string;
  title: string;
};

type SearchStatus =
  | 'idle'
  | 'typing'
  | 'loading'
  | 'success'
  | 'failure';

type State = {
  query: string;
  status: SearchStatus;
  results: Result[];
  error?: string;
};
```

## 2. Actions

```ts
type Action =
  | { type: 'QueryChanged'; query: string }
  | { type: 'SearchStarted'; query: string }
  | { type: 'SearchSucceeded'; results: Result[] }
  | { type: 'SearchFailed'; error: string }
  | { type: 'SearchCleared' };
```

## 3. Initial State

```ts
const initialState: State = {
  query: '',
  status: 'idle',
  results: []
};
```

## 4. Reducer

```ts
function update(state: State, action: Action): State {
  switch (action.type) {
    case 'QueryChanged':
      return {
        ...state,
        query: action.query,
        status: action.query.trim()
          ? 'typing'
          : 'idle'
      };

    case 'SearchStarted':
      return {
        ...state,
        query: action.query,
        status: 'loading',
        error: undefined
      };

    case 'SearchSucceeded':
      return {
        ...state,
        status: 'success',
        results: action.results,
        error: undefined
      };

    case 'SearchFailed':
      return {
        ...state,
        status: 'failure',
        results: [],
        error: action.error
      };

    case 'SearchCleared':
      return initialState;
  }
}
```

## 5. Custom Operators

```ts
export function validSearchText(
  minLength = 3,
  waitMs = 300
): OperatorFunction<string, string> {
  return source$ =>
    source$.pipe(
      map(value => value.trim()),
      filter(value => value.length >= minLength),
      debounceTime(waitMs),
      distinctUntilChanged()
    );
}
```

```ts
export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ =>
    source$.pipe(
      switchMap(project)
    );
}
```

```ts
export function ignoreWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ =>
    source$.pipe(
      exhaustMap(project)
    );
}
```

```ts
export function recoverAsAction<T, A>(
  toAction: (error: unknown) => A
): OperatorFunction<T, T | A> {
  return source$ =>
    source$.pipe(
      catchError(error => of(toAction(error)))
    );
}
```

```ts
export function retryWithBackoff<T>(
  maxRetries = 3,
  baseDelayMs = 500
): MonoTypeOperatorFunction<T> {
  return retry({
    count: maxRetries,
    delay: (_err, retryCount) => timer(Math.pow(2, retryCount - 1) * baseDelayMs)
  });
}
```

## 6. Input Stream

```ts
const queryChanged$ = fromEvent<InputEvent>(inputElement, 'input').pipe(
  map(event => (event.target as HTMLInputElement).value),
  map(query => ({
    type: 'QueryChanged',
    query
  }) as Action)
);
```

## 7. Search Effect

The search effect uses `keepLatest` (`switchMap`) — if the user types again before results arrive, the earlier request is cancelled:

```ts
const searchEffect$ = queryChanged$.pipe(
  map(action => action.query),
  validSearchText(3, 300),
  keepLatest(query =>
    searchApi(query).pipe(
      retryWithBackoff(2, 200),
      map(results => ({
        type: 'SearchSucceeded',
        results
      }) as Action),
      startWith({
        type: 'SearchStarted',
        query
      } as Action),
      recoverAsAction(error => ({
        type: 'SearchFailed',
        error: String(error)
      }) as Action)
    )
  )
);
```

`retryWithBackoff(2, 200)` retries up to twice on transient errors before the `recoverAsAction` fallback fires.

## 7b. Submit Action with `exhaustMap`

The search dashboard also has a "Save" button that submits the current results. The policy here is `ignoreWhileBusy` (`exhaustMap`) — double-clicks must not trigger a second save:

```ts
type Action =
  | { type: 'QueryChanged'; query: string }
  | { type: 'SearchStarted'; query: string }
  | { type: 'SearchSucceeded'; results: Result[] }
  | { type: 'SearchFailed'; error: string }
  | { type: 'SaveRequested' }
  | { type: 'SaveSucceeded' }
  | { type: 'SaveFailed'; error: string }
  | { type: 'SearchCleared' };

const saveEffect$ = fromEvent(saveButton, 'click').pipe(
  map(() => ({ type: 'SaveRequested' }) as Action),
  ignoreWhileBusy(() =>
    saveResults(currentResults).pipe(
      map(() => ({ type: 'SaveSucceeded' }) as Action),
      recoverAsAction(error => ({
        type: 'SaveFailed',
        error: String(error)
      }) as Action)
    )
  )
);
```

Policy comparison:
- Search uses `keepLatest` — newer query cancels the old one, and we only want the result for the latest intent.
- Save uses `ignoreWhileBusy` — while a save is in flight, additional clicks are dropped. Cancelling a save mid-flight would leave data in an unknown state.

This closes the loop on the full flattening policy table from Module 5.

## 8. State Stream

```ts
const actions$ = merge(
  queryChanged$,
  searchEffect$,
  saveEffect$
);

const state$ = actions$.pipe(
  scan(update, initialState),
  startWith(initialState),
  shareReplay({ bufferSize: 1, refCount: true })
);
```

## 9. Render

```ts
const subscription = state$.subscribe(state => {
  render(state);
});
```

Rendering is the edge. The stream computes the model. The UI reflects it.

## 10. Behavior Story

```txt
Input values flow from the DOM over time.

Each input becomes a QueryChanged action.

Valid search text is trimmed, filtered, debounced, and deduplicated.

Each valid query starts a search request.

keepLatest means only the latest query may produce results.

If a newer query arrives, the previous request subscription is canceled.

Request success becomes SearchSucceeded.

Request failure becomes SearchFailed.

All actions flow through scan.

scan produces a new immutable state after each action.

shareReplay(1) remembers the latest state for all subscribers.
```

## 11. Required Tests

The capstone should include tests for:

1. short queries are ignored
2. repeated queries are ignored
3. rapid queries debounce to the latest value
4. `switchMap` cancels previous requests
5. successful request produces `SearchSucceeded`
6. failed request produces `SearchFailed`
7. transient errors retry before `SearchFailed` fires
8. double-click on Save button does not trigger a second save (`exhaustMap`)
9. state reducer transitions are deterministic
10. late state subscriber receives latest state

## 12. Stretch Goals

For learners who complete the core capstone and want to push further:

**Pagination** — extend `SearchSucceeded` to carry a `nextCursor` field. Use `expand` to traverse pages:

```ts
const allResults$ = searchApi(query).pipe(
  expand(page => page.nextCursor ? searchApi(query, page.nextCursor) : EMPTY),
  mergeMap(page => page.results),
  toArray()
);
```

**Optimistic updates** — dispatch a `SaveOptimistic` action immediately on save click, render the change, then reconcile with `SaveSucceeded` or roll back with `SaveFailed`.

**Loading skeleton** — add a `skeletonCount` field to state. On `SearchStarted`, set it to a placeholder count (e.g., 5). On `SearchSucceeded`, clear it. The view renders skeleton cards while loading.

**Polling** — add a `PollStarted` / `PollStopped` action pair. The poll effect uses `interval + switchMap + takeUntil`:

```ts
const pollEffect$ = actions$.pipe(
  filter(a => a.type === 'PollStarted'),
  switchMap(() =>
    interval(10_000).pipe(
      switchMap(() => searchApi(lastQuery).pipe(map(toSearchSucceeded))),
      takeUntil(actions$.pipe(filter(a => a.type === 'PollStopped')))
    )
  )
);
```

## 13. Final Architecture Diagram

```txt
DOM input                         Save button click
   ↓                                    ↓
fromEvent                           fromEvent
   ↓                                    ↓
QueryChanged actions            ignoreWhileBusy(saveResults)
   ↓                                    ↓
validSearchText            SaveRequested / SaveSucceeded / SaveFailed
   ↓                                    ↓
keepLatest(searchApi                    |
  + retryWithBackoff)                   |
   ↓                                    |
SearchStarted / SearchSucceeded /       |
SearchFailed                            |
   ↓                                    ↓
              merge all actions
                     ↓
            scan(update, initialState)
                     ↓
               shareReplay(1)
                     ↓
                   state$
                     ↓
          render / framework binding
```

## Final Course Outcome

At the end of the course, the learner should be able to:

* describe Observables as lazy values over time
* read operators as behavior stories
* choose flattening operators by policy
* make time, cancellation, sharing, and termination explicit
* build state with `scan + startWith + shareReplay(1)`
* protect runtime boundaries with validation
* write reusable custom operators
* test streams with virtual time
* integrate RxJS cleanly with UI frameworks
* build complete reactive applications with predictable architecture

---

