# Module 13 — Testing with Virtual Time

## Goal

Test streams deterministically using marble diagrams and the TestScheduler.

## 1. Why Virtual Time

Real-time tests are slow and flaky.

Virtual time tests are fast and deterministic.

Instead of waiting 300 milliseconds, the test simulates 300 milliseconds.

## 2. Marble Syntax Reference

```txt
-   one frame of virtual time (10 ms in scheduler.run)
a   next value named 'a' — the actual value is passed in the values map
|   complete notification
#   error notification
^   subscription point (hot observables only)
!   unsubscription point (used in expectSubscriptions)
```

Example:

```txt
source:   -a-b---c---|
expected: -------c---|
```

Spaces are ignored inside marble strings — use them for alignment:

```txt
cold('  a---b---|')
hot( '--^-a-b-|')
```

## 3. Cold vs Hot Marble Observables

**`cold(marbles, values?)`** — the timeline starts at the moment of subscription. Each subscriber gets its own execution from frame 0.

```txt
cold('--a--b--|')  →  subscriber sees: --a--b--|
                       (from the moment it subscribes)
```

**`hot(marbles, values?)`** — the timeline is fixed; it runs from the beginning of the test. The `^` character marks where subscription happens. Emissions before `^` are not seen by subscribers.

```txt
hot('a--^--b--|')  →  subscriber sees: --b--|
                       (only what comes after ^)
```

Use `cold` for request/response streams (each subscription is its own fetch). Use `hot` for shared sources like user events, WebSocket connections, or state streams.

## 4. Testing `debounceTime`

```ts
import { TestScheduler } from 'rxjs/testing';
import { debounceTime } from 'rxjs';

describe('debounceTime', () => {
  it('emits after silence', () => {
    const scheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });

    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a 100ms b 100ms c 300ms |');
      const result$ = source$.pipe(
        debounceTime(250)
      );

      expectObservable(result$).toBe('350ms c 50ms |');
    });
  });
});
```

## 5. Testing `switchMap` Cancellation

```ts
scheduler.run(({ cold, expectObservable }) => {
  const source$ = cold('a---b----|');
  const inner$ =       cold('----x|');

  const result$ = source$.pipe(
    switchMap(() => inner$)
  );

  expectObservable(result$).toBe('--------x|');
});
```

Behavior:

```txt
a starts inner.
b arrives before a's inner emits.
a's inner is canceled.
Only b's inner emits.
```

### Verifying subscriptions with `expectSubscriptions`

`expectSubscriptions` checks when subscriptions and unsubscriptions actually happen — essential for verifying that `switchMap` cancels the inner stream on time:

```ts
scheduler.run(({ cold, hot, expectObservable, expectSubscriptions }) => {
  const source$ = hot('^-a---b----|');
  const inner$ =      cold('----x|');

  const result$ = source$.pipe(switchMap(() => inner$));

  const innerSubs = [
    '^---!',      // first inner: subscribed at frame 2, unsubscribed at frame 6
    '------^----!'  // second inner: subscribed at frame 6, completes
  ];

  expectObservable(result$).toBe('----------x|');
  expectSubscriptions(inner$.subscriptions).toBe(innerSubs);
});
```

`^` in a subscription marble = subscribed; `!` = unsubscribed. A subscription with no `!` runs to completion.

## 7. Testing `concatMap` Queue

```ts
scheduler.run(({ cold, expectObservable }) => {
  const source$ = cold('a-b-|');
  const inner$ =       cold('--x|');

  const result$ = source$.pipe(
    concatMap(() => inner$)
  );

  expectObservable(result$).toBe('--x--x|');
});
```

Behavior:

```txt
b waits until a's inner completes.
```

## 8. Testing `exhaustMap` Ignore Policy

```ts
scheduler.run(({ cold, expectObservable }) => {
  const source$ = cold('a-b---|');
  const inner$ =       cold('----x|');

  const result$ = source$.pipe(
    exhaustMap(() => inner$)
  );

  expectObservable(result$).toBe('----x-|');
});
```

Behavior:

```txt
b is ignored because a's inner is still active.
```

## 9. Testing Error Recovery

`catchError` and `retry` can be tested with `#` in the marble string:

```ts
scheduler.run(({ cold, expectObservable }) => {
  const source$ = cold('--#', {}, new Error('network'));

  const result$ = source$.pipe(
    catchError(() => of('fallback'))
  );

  expectObservable(result$).toBe('--(a|)', { a: 'fallback' });
});
```

Testing that `catchError` re-throws unknown errors while handling known ones:

```ts
scheduler.run(({ cold, expectObservable }) => {
  const timeoutErr = new TimeoutError();
  const source$ = cold('--#', {}, timeoutErr);

  const result$ = source$.pipe(
    catchError(err => {
      if (err instanceof TimeoutError) return of({ type: 'Timeout' });
      return throwError(() => err);
    })
  );

  expectObservable(result$).toBe('--(a|)', { a: { type: 'Timeout' } });
});
```

Testing a retry — the source is subscribed once per attempt:

```ts
scheduler.run(({ cold, expectObservable }) => {
  let attempt = 0;
  const source$ = defer(() => {
    attempt++;
    return attempt < 3
      ? cold('--#', {}, new Error('fail'))
      : cold('--a|', { a: 'ok' });
  }).pipe(retry(2));

  expectObservable(source$).toBe('------a|', { a: 'ok' });
});
```

## 10. Testing State

```ts
scheduler.run(({ hot, expectObservable }) => {
  const actions$ = hot('-a-b-c|', {
    a: { type: 'Increment' },
    b: { type: 'Increment' },
    c: { type: 'Decrement' }
  });

  const state$ = actions$.pipe(
    scan(update, { count: 0 }),
    startWith({ count: 0 })
  );

  expectObservable(state$).toBe('x-y-z-w|', {
    x: { count: 0 },
    y: { count: 1 },
    z: { count: 2 },
    w: { count: 1 }
  });
});
```

## Learning Outcome

The learner should be able to test time, cancellation, queueing, ignoring, error recovery, and state without real timers — and verify subscription timing with `expectSubscriptions`.

---

