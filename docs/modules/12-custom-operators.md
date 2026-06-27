# Module 12 — Custom Operators and DSL Design

## Goal

Create reusable, readable RxJS vocabulary by wrapping operator combinations in named policies.

## 1. Why Custom Operators Exist

Raw RxJS is powerful but can become noisy.

```ts
const searchText$ = input$.pipe(
  map(event => event.target.value),
  map(value => value.trim()),
  filter(value => value.length >= 3),
  debounceTime(300),
  distinctUntilChanged()
);
```

A custom operator gives this behavior a name.

```ts
const searchText$ = input$.pipe(
  map(event => event.target.value),
  validSearchText()
);
```

A custom operator is a named policy with tests and error handling.

## 2. Composition-First Style

Prefer composition-first custom operators.

```ts
import { OperatorFunction } from 'rxjs';
import { debounceTime, distinctUntilChanged, filter, map } from 'rxjs/operators';

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

## 3. Avoid Low-Level Operators Unless Necessary

Low-level custom Observables require manual subscription handling.

```ts
export function logValues<T>(): OperatorFunction<T, T> {
  return source$ =>
    new Observable<T>(subscriber => {
      const subscription = source$.subscribe({
        next: value => {
          console.log(value);
          subscriber.next(value);
        },
        error: error => subscriber.error(error),
        complete: () => subscriber.complete()
      });

      return () => subscription.unsubscribe();
    });
}
```

Use this style only when composition cannot express the behavior.

## 4. Policy-Named Flattening Operators

```ts
export const keepLatest = switchMap;
export const allowConcurrent = mergeMap;
export const queueWhileBusy = concatMap;
export const ignoreWhileBusy = exhaustMap;
```

Better with function wrappers:

```ts
export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(
    switchMap(project)
  );
}
```

This makes code read as architecture:

```ts
const results$ = searchText$.pipe(
  keepLatest(term => searchApi(term))
);
```

## 5. State DSL Operators

```ts
export function startWithInitial<T>(
  initialValue: T
): OperatorFunction<T, T> {
  return source$ =>
    source$.pipe(
      startWith(initialValue)
    );
}
```

Usage:

```ts
const state$ = actions$.pipe(
  scan(update, initialState),
  startWithInitial(initialState),
  shareReplay({ bufferSize: 1, refCount: true })
);
```

## 6. Error DSL Operators

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

Usage:

```ts
const loadAction$ = loadUser$.pipe(
  map(user => ({ type: 'LoadSucceeded', user }) as Action),
  recoverAsAction(error => ({
    type: 'LoadFailed',
    error: String(error)
  }) as Action)
);
```

## 7. Testing Custom Operators

A custom operator is only a reliable building block if it has tests. Use `TestScheduler` marble tests (Module 13) to verify behavior:

```ts
import { TestScheduler } from 'rxjs/testing';
import { validSearchText } from './operators';

describe('validSearchText', () => {
  let scheduler: TestScheduler;

  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });
  });

  it('suppresses values shorter than minLength', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a-b-c-|', { a: 'hi', b: 'hey', c: 'hello' });
      const result$ = source$.pipe(validSearchText(4, 0));
      expectObservable(result$).toBe('----c-|', { c: 'hello' });
    });
  });

  it('debounces rapid input', () => {
    scheduler.run(({ cold, expectObservable }) => {
      // Three keystrokes; only the last passes debounce
      const source$ = cold('aaa 300ms b|', { a: 'abcd', b: 'abcde' });
      const result$ = source$.pipe(validSearchText(3, 300));
      expectObservable(result$).toBe('300ms a 300ms b|', { a: 'abcd', b: 'abcde' });
    });
  });
});
```

## 8. Stateful Custom Operators

Some operators require internal closure state — a value that persists between emissions:

```ts
import { OperatorFunction } from 'rxjs';
import { scan, map } from 'rxjs';

// Emit the running average of a number stream
export function runningAverage(): OperatorFunction<number, number> {
  return source$ =>
    source$.pipe(
      scan(
        (acc, value) => ({ sum: acc.sum + value, count: acc.count + 1 }),
        { sum: 0, count: 0 }
      ),
      map(({ sum, count }) => sum / count)
    );
}
```

When the state is more complex than `scan` can express cleanly, use closure:

```ts
export function distinctUntilDeepChanged<T>(
  isEqual: (a: T, b: T) => boolean
): MonoTypeOperatorFunction<T> {
  return source$ =>
    new Observable<T>(subscriber => {
      let lastSeen: T | typeof UNSET = UNSET;
      const UNSET = Symbol('UNSET');

      return source$.subscribe({
        next: value => {
          if (lastSeen === UNSET || !isEqual(lastSeen as T, value)) {
            lastSeen = value;
            subscriber.next(value);
          }
        },
        error: e => subscriber.error(e),
        complete: () => subscriber.complete()
      });
    });
}
```

## 9. `retryWithBackoff` — A Complete DSL Operator

This example connects Module 8 (retry), Module 12 (DSL design), and Module 13 (testing):

```ts
import { OperatorFunction, timer } from 'rxjs';
import { retry } from 'rxjs';

export function retryWithBackoff<T>(
  maxRetries = 4,
  baseDelayMs = 1000
): MonoTypeOperatorFunction<T> {
  return retry({
    count: maxRetries,
    delay: (_error, retryCount) => timer(Math.pow(2, retryCount - 1) * baseDelayMs)
  });
}
```

Usage reads as architecture intent, not implementation:

```ts
const user$ = loadUserFromApi().pipe(
  retryWithBackoff(3, 500),
  recoverAsAction(error => ({ type: 'LoadFailed', error: String(error) } as Action))
);
```

Retry delays: attempt 1 → 500 ms, attempt 2 → 1000 ms, attempt 3 → 2000 ms, then errors.

Testing with virtual time (no real waiting):

```ts
scheduler.run(({ cold, expectObservable }) => {
  let attempt = 0;
  const source$ = defer(() => {
    attempt++;
    return attempt < 3 ? throwError(() => new Error('fail')) : of('ok');
  }).pipe(retryWithBackoff(3, 10));

  expectObservable(source$).toBe('10ms (a|)', { a: 'ok' });
});
```

## 10. Naming Rules

Good custom operator names should express policy.

Weak:

```txt
processValue
handleData
customMap
```

Better:

```txt
validValues
validSearchText
keepLatest
queueWhileBusy
recoverAsAction
startWithInitial
retryWithBackoff
shareLatestState
```

## Learning Outcome

The learner should understand how to create a project-specific RxJS vocabulary, test that vocabulary with marble tests, and recognize when stateful closures are needed versus pure composition.

---

