# Module 8 — Error and Recovery Policies

## Goal

Understand how errors move through streams and how to design recovery policies.

## 1. Error Terminates the Stream

In RxJS, `error` is terminal.

```txt
--1--2--# 
```

After `#`, no more values flow.

Example:

```ts
source$.pipe(
  map(value => {
    if (value < 0) {
      throw new Error('negative');
    }
    return value;
  })
);
```

If an error escapes, the stream terminates.

## 2. `catchError`

`catchError` replaces a failed stream with another stream.

```ts
const safe$ = risky$.pipe(
  catchError(error => of(fallbackValue))
);
```

Behavior story:

```txt
When upstream errors, catch the error.
Subscribe to replacement Observable.
Continue with replacement values.
```

## 3. Inner vs Outer Error Boundary

This is one of the most important RxJS architecture rules.

### Outer catch

```ts
const results$ = searchText$.pipe(
  switchMap(term => searchApi(term)),
  catchError(error => of([]))
);
```

Problem:

```txt
The outer stream is replaced.
The search pipeline may stop listening to future search terms.
```

### Inner catch

```ts
const results$ = searchText$.pipe(
  switchMap(term =>
    searchApi(term).pipe(
      catchError(error => of([]))
    )
  )
);
```

Better behavior:

```txt
Each request handles its own error.
The outer search text stream remains alive.
Future searches still work.
```

## 4. Error as Value

Sometimes errors should become domain actions, not terminate the state stream.

```ts
type SearchAction =
  | { type: 'SearchStarted'; term: string }
  | { type: 'SearchSucceeded'; results: Result[] }
  | { type: 'SearchFailed'; error: string };
```

Pipeline:

```ts
const searchAction$ = searchText$.pipe(
  switchMap(term =>
    searchApi(term).pipe(
      map(results => ({
        type: 'SearchSucceeded',
        results
      }) as SearchAction),
      catchError(error =>
        of({
          type: 'SearchFailed',
          error: String(error)
        } as SearchAction)
      )
    )
  )
);
```

The error becomes a package moving through the stream.

The state stream remains alive.

## 5. Custom Error Policy

```ts
import { catchError, map, of, OperatorFunction } from 'rxjs';

export function recoverAsAction<T, A>(
  toAction: (error: unknown) => A
): OperatorFunction<T, T | A> {
  return source$ =>
    source$.pipe(
      catchError(error => of(toAction(error)))
    );
}
```

Usage:

```ts
const resultAction$ = request$.pipe(
  map(data => ({ type: 'LoadSucceeded', data }) as Action),
  recoverAsAction(error => ({
    type: 'LoadFailed',
    error: String(error)
  }) as Action)
);
```

## 6. `EMPTY` as a Recovery Option

`catchError` does not have to return a fallback value. Returning `EMPTY` swallows the error and completes the stream cleanly.

```ts
const safe$ = risky$.pipe(
  catchError(() => EMPTY)
);
```

Use this when an error is expected, harmless, and the correct response is to stop the stream without propagating the failure — for example, when a cancelled request error should produce no output.

## 7. `finalize` — Teardown Side Effects

`finalize` runs a callback when a stream ends for any reason: completion, error, or unsubscription.

```ts
import { finalize } from 'rxjs';

const tracked$ = request$.pipe(
  tap(() => showSpinner()),
  finalize(() => hideSpinner())
);
```

`finalize` is the RxJS equivalent of a `finally` block.

```txt
finalize runs on:
  source complete  ✓
  source error     ✓
  unsubscribe      ✓
```

Use `finalize` for cleanup that must always happen regardless of how the stream ends — hiding loaders, releasing resources, logging teardown.

## 8. Retry Policy

Use `retry` when an operation may fail temporarily.

```ts
const data$ = request$.pipe(
  retry({ count: 3 })
);
```

But `retry({ count: 3 })` retries immediately with no delay. Production networks require backoff.

### Exponential Backoff

```ts
import { retry, timer } from 'rxjs';

const data$ = request$.pipe(
  retry({
    count: 4,
    delay: (error, retryCount) => timer(Math.pow(2, retryCount) * 1000)
  })
);
```

Behavior:

```txt
Attempt 1 fails → wait 2 s → retry
Attempt 2 fails → wait 4 s → retry
Attempt 3 fails → wait 8 s → retry
Attempt 4 fails → wait 16 s → retry
Attempt 5 fails → error propagates
```

The `delay` function receives the error and the retry count (starting at 1) and returns an Observable. The retry waits for that Observable to emit before resubscribing to the source.

Good for:

* unstable network
* idempotent reads
* temporary service errors

Dangerous for:

* payments
* non-idempotent writes
* operations with side effects

## 9. Timeout Policy

Use `timeout` when an operation must not wait forever.

```ts
import { timeout, catchError, of, TimeoutError } from 'rxjs';

const response$ = request$.pipe(
  timeout(5000),
  catchError(error => {
    if (error instanceof TimeoutError) {
      return of({ type: 'Timeout' });
    }
    return throwError(() => error);
  })
);
```

`timeout` throws a `TimeoutError` when the source does not emit within the allowed window. Checking `instanceof TimeoutError` in `catchError` lets you handle timeout separately from other network errors without swallowing everything.

## Learning Outcome

The learner should see error handling as a stream survival policy — placing error boundaries inside inner streams, converting errors to domain values, recovering with `EMPTY`, retrying with backoff, and cleaning up with `finalize`.

---

