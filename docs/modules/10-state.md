# Module 10 — State as a Stream

## Goal

Build application state with actions, reducers, `scan`, and `shareReplay`.

## 1. State Is Not a Variable

In RxJS architecture, state is not a mutable object that is manually changed from many places.

State is a stream that remembers.

```txt
actions$ -> scan(update, initialState) -> state$
```

The formula:

```txt
State = initial state + actions over time
```

## 2. Basic Counter State

```ts
import { Subject, scan, startWith, shareReplay } from 'rxjs';

type State = {
  count: number;
};

type Action =
  | { type: 'Increment' }
  | { type: 'Decrement' }
  | { type: 'Reset' };

const initialState: State = {
  count: 0
};

function update(state: State, action: Action): State {
  switch (action.type) {
    case 'Increment':
      return { count: state.count + 1 };

    case 'Decrement':
      return { count: state.count - 1 };

    case 'Reset':
      return initialState;
  }
}

const actions$ = new Subject<Action>();

const state$ = actions$.pipe(
  scan(update, initialState),
  startWith(initialState),
  shareReplay({ bufferSize: 1, refCount: true })
);
```

## 3. Why `startWith`

`scan` emits only after the first action.

`startWith(initialState)` makes the initial state visible immediately.

## 4. Why `shareReplay(1)`

Multiple UI subscribers should observe the same state.

Late subscribers need the latest state.

```txt
scan + startWith + shareReplay(1)
```

This is the core state pattern.

## 5. MVU Architecture

Model-View-Update:

```txt
View -> Action -> Update -> State -> View
```

RxJS version:

```txt
UI events -> action$ -> scan(update) -> state$ -> render
```

## 6. Effects

Effects are asynchronous operations triggered by actions or state.

Example:

```ts
type Action =
  | { type: 'SearchChanged'; term: string }
  | { type: 'SearchStarted'; term: string }
  | { type: 'SearchSucceeded'; results: Result[] }
  | { type: 'SearchFailed'; error: string };
```

Input stream:

```ts
const searchText$ = actions$.pipe(
  filter((action): action is Extract<Action, { type: 'SearchChanged' }> =>
    action.type === 'SearchChanged'
  ),
  map(action => action.term)
);
```

Effect stream:

```ts
const searchEffect$ = searchText$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(term =>
    searchApi(term).pipe(
      map(results => ({
        type: 'SearchSucceeded',
        results
      }) as Action),
      catchError(error =>
        of({
          type: 'SearchFailed',
          error: String(error)
        } as Action)
      )
    )
  )
);
```

Merge UI actions and effect actions:

```ts
const allActions$ = merge(
  uiActions$,
  searchEffect$
);

const state$ = allActions$.pipe(
  scan(update, initialState),
  startWith(initialState),
  shareReplay({ bufferSize: 1, refCount: true })
);
```

## 7. Preventing Redundant Renders

`scan` emits a new state object on every action — even when the relevant slice did not change.

Add `distinctUntilChanged()` to prevent downstream re-renders for unchanged state:

```ts
import { distinctUntilChanged, map } from 'rxjs';

const state$ = allActions$.pipe(
  scan(update, initialState),
  startWith(initialState),
  distinctUntilChanged(),
  shareReplay({ bufferSize: 1, refCount: true })
);
```

`distinctUntilChanged()` uses reference equality by default. Because reducers return new objects on every action, pass a comparator when comparing by value:

```ts
distinctUntilChanged((a, b) => a.count === b.count && a.status === b.status)
```

## 8. Derived State Slices

Subscribers rarely need the full state. Map to a slice and add `distinctUntilChanged()` so that slice-specific subscribers only re-render when their slice changes:

```ts
const userList$ = state$.pipe(
  map(s => s.users),
  distinctUntilChanged()
);

const status$ = state$.pipe(
  map(s => s.status),
  distinctUntilChanged()
);
```

This is the observable equivalent of a selector in Redux.

## 9. BehaviorSubject as Simple Local State

For simple, single-value local state where the full `scan + startWith + shareReplay` pattern is unnecessary overhead, `BehaviorSubject` is appropriate:

```ts
import { BehaviorSubject } from 'rxjs';

const count$ = new BehaviorSubject(0);

// Update
count$.next(count$.getValue() + 1);

// Read current value synchronously
console.log(count$.getValue());

// Subscribe as usual
count$.subscribe(v => render(v));
```

**Use `BehaviorSubject` when:** state has one value that a single owner controls and no action history is needed.

**Use `scan + startWith + shareReplay` when:** state evolves from multiple action types, multiple streams produce actions, or effects feed back into state.

## 10. State Design Rule

Reducers must be pure.

Effects may be asynchronous.

Rendering is a side effect at the edge.

## Learning Outcome

The learner should be able to model application state as a deterministic stream of actions over time and derive focused state slices to minimize unnecessary re-renders.

---

