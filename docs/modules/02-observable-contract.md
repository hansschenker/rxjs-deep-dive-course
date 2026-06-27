# Module 2 — The Observable Contract

## Goal

Understand the runtime contract of an Observable: `next`, `error`, `complete`, subscription, and teardown.

## 1. Observable Definition

An Observable is a lazy producer function wrapped in a composable object.

```ts
import { Observable } from 'rxjs';

const source$ = new Observable<number>(subscriber => {
  console.log('producer starts');

  subscriber.next(1);
  subscriber.next(2);
  subscriber.next(3);
  subscriber.complete();

  return () => {
    console.log('teardown runs');
  };
});
```

Nothing runs yet.

Only subscription starts the producer:

```ts
const subscription = source$.subscribe({
  next: value => console.log(value),
  error: err => console.error(err),
  complete: () => console.log('complete')
});
```

## 2. Observable Grammar

An Observable follows a strict notification grammar:

```txt
next* (complete | error)?
```

That means:

* zero or more `next(value)` notifications
* then either `complete()`
* or `error(err)`
* after complete or error, no more values may flow

Valid streams:

```txt
--1--2--3--|
--1--2--#
--|
--#
--------1--------2--------3--------...
```

Invalid stream:

```txt
--1--|--2
```

After completion, value `2` cannot be emitted.

## 3. Teardown

Teardown is what happens when a running subscription stops.

A stream can stop because:

* it completes
* it errors
* the subscriber unsubscribes
* an operator cancels an inner subscription
* a lifecycle helper cleans it up

Example:

```ts
const timer$ = new Observable<number>(subscriber => {
  const id = setInterval(() => {
    subscriber.next(Date.now());
  }, 1000);

  return () => {
    clearInterval(id);
  };
});

const subscription = timer$.subscribe(console.log);

setTimeout(() => {
  subscription.unsubscribe();
}, 5000);
```

The teardown clears the interval.

## 4. Synchronous and Asynchronous Observables

Observables are not automatically asynchronous.

```ts
import { of } from 'rxjs';

console.log('before');

of(1, 2, 3).subscribe(value => {
  console.log(value);
});

console.log('after');
```

Output:

```txt
before
1
2
3
after
```

The stream runs synchronously.

But this stream is asynchronous:

```ts
import { interval, take } from 'rxjs';

console.log('before');

interval(1000).pipe(
  take(3)
).subscribe(console.log);

console.log('after');
```

Output:

```txt
before
after
0
1
2
```

## 5. Subscription Management

`.subscribe()` returns a `Subscription` object. That object owns the running connection between the source and the observer.

```ts
const subscription = interval(1000).subscribe(console.log);

setTimeout(() => {
  subscription.unsubscribe();
}, 3000);
```

If the subscription is never unsubscribed and the source never completes, the producer runs forever. This is the primary source of memory leaks in RxJS applications.

### Managing Lifetime with `take` and `takeUntil`

`take(n)` completes the stream after `n` values.

```ts
interval(1000).pipe(
  take(5)
).subscribe(console.log);
```

The stream completes automatically after five values. No manual unsubscribe is needed.

`takeUntil(notifier$)` completes the stream when a notifier emits.

```ts
const destroy$ = new Subject<void>();

interval(1000).pipe(
  takeUntil(destroy$)
).subscribe(console.log);

setTimeout(() => {
  destroy$.next();
  destroy$.complete();
}, 5000);
```

`takeUntil` is the standard pattern for tying a subscription's lifetime to a component or service lifecycle.

## 6. The Nested Subscription Anti-Pattern

The most common RxJS mistake is subscribing inside a subscription.

Anti-pattern:

```ts
source$.subscribe(value => {
  inner$(value).subscribe(result => {
    console.log(result);
  });
});
```

Problems:

```txt
The inner subscription is not managed by the outer teardown.
Cancellation of source$ does not cancel inner$.
Each source value creates a new unmanaged subscription.
```

Correct pattern:

```ts
source$.pipe(
  mergeMap(value => inner$(value))
).subscribe(result => {
  console.log(result);
});
```

Flattening operators (`mergeMap`, `switchMap`, `concatMap`, `exhaustMap`) exist precisely to manage inner subscriptions safely. They are covered in Module 5.

## 7. Practical Rule

Do not ask:

```txt
Is an Observable async?
```

Ask:

```txt
What producer drives this Observable?
What scheduler or external source controls time?
```

## Learning Outcome

The learner should understand that an Observable is a lazy dataflow contract, not a running process. Subscription starts execution, teardown stops it, and subscription lifetime must always be explicitly managed.

---

