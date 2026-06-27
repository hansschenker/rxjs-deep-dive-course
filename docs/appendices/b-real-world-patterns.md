# Appendix B — Real-World Patterns

Reusable patterns that appear across production RxJS applications.

---

## B.1 Polling

Periodically re-fetch data. Stop when the component is destroyed.

```ts
const destroy$ = new Subject<void>();

const poll$ = interval(10_000).pipe(
  startWith(0),                          // fetch immediately on subscribe
  switchMap(() => fetchData()),
  takeUntil(destroy$)
);

poll$.subscribe(data => render(data));

// On teardown:
destroy$.next();
```

`startWith(0)` triggers the first fetch immediately rather than waiting for the first interval tick.

---

## B.2 Request Deduplication / Shared Cache

Multiple subscribers should not each trigger a separate HTTP request for the same resource.

```ts
const user$ = defer(() => from(fetch('/api/user').then(r => r.json()))).pipe(
  map(raw => UserSchema.parse(raw)),
  shareReplay({ bufferSize: 1, refCount: true })
);

// Both subscribers share one HTTP request
user$.subscribe(renderHeader);
user$.subscribe(renderProfile);
```

`shareReplay({ bufferSize: 1, refCount: true })` multicasts the response and replays it for late subscribers. `refCount: true` means the HTTP request is re-issued if all subscribers unsubscribe and a new one arrives later.

---

## B.3 Exponential Backoff Retry

Retry a failing request with increasing delays. Stop after a maximum number of attempts.

```ts
import { retry, timer } from 'rxjs';

function retryWithBackoff<T>(
  maxRetries = 4,
  baseDelayMs = 500
): MonoTypeOperatorFunction<T> {
  return retry({
    count: maxRetries,
    delay: (_err, retryCount) => timer(Math.pow(2, retryCount - 1) * baseDelayMs)
  });
}

// Retry delays: 500ms → 1000ms → 2000ms → 4000ms → error
fetchData().pipe(
  retryWithBackoff(4, 500)
).subscribe({ next: render, error: showError });
```

---

## B.4 Optimistic Updates

Emit the expected outcome immediately, then reconcile with the server response.

```ts
type Action =
  | { type: 'SaveOptimistic'; item: Item }
  | { type: 'SaveConfirmed'; item: Item }
  | { type: 'SaveRolledBack'; item: Item; error: string };

function saveWithOptimism(item: Item): Observable<Action> {
  return merge(
    of({ type: 'SaveOptimistic', item } as Action),
    saveToServer(item).pipe(
      map(() => ({ type: 'SaveConfirmed', item }) as Action),
      catchError(error =>
        of({ type: 'SaveRolledBack', item, error: String(error) } as Action)
      )
    )
  );
}

saveButton$.pipe(
  exhaustMap(item => saveWithOptimism(item))
).subscribe(actions$.next.bind(actions$));
```

`SaveOptimistic` is dispatched immediately — the UI updates at once. `SaveConfirmed` or `SaveRolledBack` arrives when the server responds, allowing the reducer to reconcile or undo.

---

## B.5 Type-Safe Action Dispatcher

Filter the action stream to a specific type without losing the payload type.

```ts
function ofType<A extends { type: string }, K extends A['type']>(
  ...types: K[]
): OperatorFunction<A, Extract<A, { type: K }>> {
  return filter((action): action is Extract<A, { type: K }> =>
    types.includes(action.type as K)
  );
}

// Usage — result is typed as { type: 'SearchSucceeded'; results: Result[] }
actions$.pipe(
  ofType('SearchSucceeded')
).subscribe(action => render(action.results));
```

---

## B.6 Pagination with `expand`

Traverse all pages of a paginated API without knowing the page count upfront.

```ts
interface Page {
  items: Item[];
  nextCursor: string | null;
}

function loadAllPages(firstCursor: string | null = null): Observable<Item[]> {
  return fetchPage(firstCursor).pipe(
    expand(page =>
      page.nextCursor ? fetchPage(page.nextCursor) : EMPTY
    ),
    mergeMap(page => page.items),
    toArray()
  );
}
```

`expand` recursively projects each page into the next request. `EMPTY` stops the recursion when there is no next cursor. `toArray()` collects all items after the final page completes.

---

