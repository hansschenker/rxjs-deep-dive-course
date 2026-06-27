# Module 5 — Flattening Policies

## Goal

Understand higher-order Observables and the four main flattening policies.

## 1. Higher-Order Observable Problem

Sometimes a value creates another Observable.

```ts
const result$ = searchText$.pipe(
  map(term => searchApi(term))
);
```

The type is:

```ts
Observable<Observable<SearchResult[]>>
```

That means values over time where each value is itself a stream.

Notation:

```txt
[{T, Observable<a>}] -> ???
```

To use the inner values, the stream must be flattened.

## 2. Four Flattening Policies

The four core flattening operators are not just operators. They are policies.

```txt
mergeMap   = allow overlap
switchMap  = only latest
concatMap  = queue
exhaustMap = ignore while busy
```

## 3. `mergeMap` — Allow Overlap

Use when multiple inner operations may run at the same time.

```ts
const saves$ = saveClicks$.pipe(
  mergeMap(form => saveDraft(form))
);
```

Behavior story:

```txt
Source:
outer values over time

Trigger:
each outer value

Value:
values emitted by each projected inner Observable

Cardinality:
one outer value may produce many inner values

Time:
inner values flow whenever each inner emits

Concurrency:
overlap is allowed

Cancellation:
outer unsubscribe cancels all active inner subscriptions
new outer values do not cancel previous inner streams

Termination:
output completes after outer completes and all active inner streams complete
```

Practical name:

```ts
allowConcurrent(project)
```

Implementation:

```ts
import { mergeMap, OperatorFunction, ObservableInput } from 'rxjs';

export function allowConcurrent<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(
    mergeMap(project)
  );
}
```

Use cases:

* logging
* independent background requests
* fire-and-collect workflows
* parallel loading

Hazard:

```txt
Too many source values can create too many active inner subscriptions.
```

Mitigation:

```ts
mergeMap(project, 4)
```

## 4. `switchMap` — Only Latest

Use when only the newest operation matters.

```ts
const results$ = searchText$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(term => searchApi(term))
);
```

Behavior story:

```txt
Source:
outer values over time

Trigger:
each outer value

Value:
values from the latest projected inner Observable

Cardinality:
one outer value may produce many inner values

Time:
latest inner values flow when the active inner emits

Concurrency:
only one inner subscription is active

Cancellation:
new outer value cancels the previous inner subscription

Termination:
output completes after outer completes and the active inner completes
```

Practical name:

```ts
keepLatest(project)
```

Implementation:

```ts
import { switchMap, OperatorFunction, ObservableInput } from 'rxjs';

export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(
    switchMap(project)
  );
}
```

Use cases:

* typeahead search
* route parameter loading
* live preview
* cancelable reads

Hazard:

```txt
Dangerous for writes when every write must finish.
```

Rule:

```txt
switchMap is a read policy, not a write policy.

Use switchMap when cancellation is semantically correct:
  the previous result is no longer needed.

Do not use switchMap when every operation must complete:
  payments, form saves, audit logs, non-idempotent mutations.
  Use concatMap (queue) or exhaustMap (ignore while busy) instead.
```

## 5. `concatMap` — Queue

Use when operations must run one after another.

```ts
const uploadResults$ = files$.pipe(
  concatMap(file => uploadFile(file))
);
```

Behavior story:

```txt
Source:
outer values over time

Trigger:
each outer value

Value:
values from each inner Observable in source order

Cardinality:
one outer value may produce many inner values

Time:
an inner starts only after the previous inner completes

Concurrency:
no overlap

Cancellation:
outer unsubscribe cancels active inner and clears queued work

Termination:
output completes after outer completes and the queue drains
```

Practical name:

```ts
queueWhileBusy(project)
```

Implementation:

```ts
import { concatMap, OperatorFunction, ObservableInput } from 'rxjs';

export function queueWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(
    concatMap(project)
  );
}
```

Use cases:

* ordered writes
* upload queues
* sequential command processing
* transactional flows

Hazard:

```txt
If an inner stream never completes, the queue is blocked forever.
```

Mitigation:

```ts
const results$ = triggers$.pipe(
  concatMap(value =>
    longRunningOperation(value).pipe(take(1))
  )
);
```

`take(1)` forces the inner stream to complete after its first value, preventing the queue from blocking.

## 6. `exhaustMap` — Ignore While Busy

Use when a new trigger should be ignored while work is already running.

```ts
const loginResult$ = loginClicks$.pipe(
  exhaustMap(credentials => login(credentials))
);
```

Behavior story:

```txt
Source:
outer values over time

Trigger:
an outer value only when no inner is active

Value:
values from the active inner Observable

Cardinality:
accepted outer values may produce many inner values
ignored outer values produce nothing

Time:
inner values flow while active inner emits

Concurrency:
only one inner at a time

Cancellation:
new outer values do not cancel the active inner
they are ignored

Termination:
output completes after outer completes and active inner completes
```

Practical name:

```ts
ignoreWhileBusy(project)
```

Implementation:

```ts
import { exhaustMap, OperatorFunction, ObservableInput } from 'rxjs';

export function ignoreWhileBusy<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(
    exhaustMap(project)
  );
}
```

Use cases:

* login button
* submit button double-click protection
* preventing duplicate command execution
* one-at-a-time workflow entry

Hazard:

```txt
Ignored values are lost.
```

## 7. Policy Table

| Operator     | Policy            | New outer value while inner active | Good for                  | Dangerous for              |
| ------------ | ----------------- | ---------------------------------- | ------------------------- | -------------------------- |
| `mergeMap`   | allow overlap     | starts another inner               | independent parallel work | unbounded concurrency      |
| `switchMap`  | only latest       | cancels previous inner             | cancelable reads          | critical writes            |
| `concatMap`  | queue             | waits                              | ordered writes            | never-ending inner streams |
| `exhaustMap` | ignore while busy | ignores new value                  | submit/login protection   | important repeated changes |

## 8. `expand` — Recursive Flattening

`expand` is a higher-order operator that recursively projects each output value back through the same projection function.

```ts
expand(project): each output value is re-projected until the inner returns EMPTY
```

Use case: paginated API traversal.

```ts
import { expand, mergeMap, takeWhile, toArray, EMPTY } from 'rxjs';

type Page = {
  items: string[];
  nextCursor: string | null;
};

function loadPage(cursor: string | null): Observable<Page> {
  const url = cursor ? `/api/items?cursor=${cursor}` : '/api/items';
  return defer(() => from(fetch(url))).pipe(
    mergeMap(r => from(r.json() as Promise<Page>))
  );
}

const allItems$ = loadPage(null).pipe(
  expand(page => page.nextCursor ? loadPage(page.nextCursor) : EMPTY),
  mergeMap(page => page.items),
  toArray()
);
```

Behavior story:

```txt
Source:
first page emission

Trigger:
each output value is projected back into the same function

Value:
values from each recursively projected inner Observable

Cancellation:
return EMPTY from the projection to stop recursion

Termination:
output completes when the projection returns EMPTY
```

`expand` replaces recursive subscribe-inside-subscribe patterns with a declarative, managed flattening.

## Learning Outcome

The learner should be able to choose a flattening operator by policy, not by habit, and recognise `expand` as the recursive extension of the flattening family.

---

