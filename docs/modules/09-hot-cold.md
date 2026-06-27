# Module 9 — Hot, Cold, and Shared Streams

## Goal

Understand producer ownership, sharing, multicasting, late subscribers, and replay.

## 1. Cold Observable

A cold Observable creates a fresh producer for each subscription.

```ts
const random$ = new Observable<number>(subscriber => {
  console.log('producer starts');
  subscriber.next(Math.random());
  subscriber.complete();
});

random$.subscribe(console.log);
random$.subscribe(console.log);
```

Behavior:

```txt
producer starts
0.123

producer starts
0.847
```

Each subscriber gets its own execution.

Important correction:

```txt
Cold does not mean replay.
Cold means fresh execution per subscriber.
```

## 2. Hot Observable

A hot source exists independently of a particular subscription.

Examples:

* DOM event source
* WebSocket
* Subject
* shared stream
* global state store

Late subscribers do not receive past values unless replay is explicitly configured.

## 3. Subject

A Subject is both Observer and Observable.

```ts
const subject = new Subject<number>();

subject.subscribe(value => console.log('A', value));
subject.subscribe(value => console.log('B', value));

subject.next(1);
subject.next(2);
```

Behavior:

```txt
One pushed value fans out to many subscribers.
```

A standard Subject has no memory.

Late subscriber example:

```ts
subject.next(1);

subject.subscribe(value => console.log(value));

subject.next(2);
```

The subscriber receives only `2`.

## 4. When to Use a Subject

A Subject is the right tool in two situations:

```txt
1. You need to push values imperatively from outside an Observable chain.
   Example: a component dispatches actions via subject.next(action).

2. You need to bridge a non-Observable system into RxJS.
   Example: wrapping a third-party callback API where fromEventPattern is not applicable.
```

In all other cases, prefer cold creation operators (`fromEvent`, `defer`, `interval`, etc.).

### The Subject Over-Use Anti-Pattern

```ts
// WRONG — Subject used where fromEvent would be correct
const clicks$ = new Subject<MouseEvent>();
document.addEventListener('click', e => clicks$.next(e));

// CORRECT
const clicks$ = fromEvent<MouseEvent>(document, 'click');
```

When a Subject wraps something that already has an Observable creator, the Subject adds complexity without benefit: the event listener is never removed on unsubscribe, and the source is no longer cold.

Rule:

```txt
If fromEvent, fromEventPattern, interval, timer, or defer can model the source,
use them. Reach for Subject only when no creation operator fits.
```

## 5. Subject Variants

| Type              | Memory policy                  |
| ----------------- | ------------------------------ |
| `Subject`         | no memory                      |
| `BehaviorSubject` | remembers current value        |
| `ReplaySubject`   | remembers configured history   |
| `AsyncSubject`    | emits last value on completion |

### `AsyncSubject` — One-Time Computation Cache

`AsyncSubject` is the Subject equivalent of a resolved Promise: it emits exactly one value (the last one) when it completes, and replays that value to any late subscriber.

```ts
import { AsyncSubject } from 'rxjs';

const result$ = new AsyncSubject<string>();

result$.subscribe(v => console.log('A', v));

result$.next('computing...');
result$.next('done');
result$.complete();

// Late subscriber still receives 'done'
result$.subscribe(v => console.log('B', v));
```

Output:

```txt
A done
B done
```

Use `AsyncSubject` for memoizing a single expensive one-time computation — for example, caching the result of an initialization sequence that only needs to run once but whose result must be available to any subscriber at any time.

## 5. `share`

`share` turns one subscription to the source into shared execution while subscribers exist.

```ts
const shared$ = source$.pipe(
  share()
);
```

Behavior:

```txt
First subscriber subscribes to source.
Additional subscribers share the same source subscription.
When all subscribers unsubscribe, source is unsubscribed.
```

No replay.

Late subscribers receive only future values.

### `share` and Source Completion

When the source completes, `share` tears down. A new subscriber after completion re-subscribes to the source and starts a fresh execution.

```ts
const shared$ = of(1, 2, 3).pipe(share());

shared$.subscribe(v => console.log('A', v)); // A 1, A 2, A 3
shared$.subscribe(v => console.log('B', v)); // B 1, B 2, B 3 — fresh execution
```

This is intentional for finite sources. For infinite sources (WebSocket, interval) `share` keeps a single execution alive for all concurrent subscribers and restarts it only if all subscribers leave and a new one arrives.

## 7. `shareReplay`

`shareReplay` shares the source and replays buffered values to late subscribers.

```ts
const cachedUser$ = loadUser$.pipe(
  shareReplay({ bufferSize: 1, refCount: true })
);
```

Behavior:

```txt
Share one source execution.
Remember the latest value.
Late subscribers receive the latest value.
When refCount is true, unsubscribe from source when nobody listens.
```

This is the common policy for cached read models.

## 7. Sharing Hazard

`shareReplay(1)` without careful configuration can keep subscriptions or cached values longer than expected.

Prefer being explicit:

```ts
shareReplay({
  bufferSize: 1,
  refCount: true
})
```

## 8. `connectable`

`connectable` is an advanced manual sharing policy.

Use it when you want to explicitly control when the source starts.

```ts
const connected$ = connectable(source$, {
  connector: () => new Subject()
});

connected$.subscribe(console.log);
const connection = connected$.connect();
```

Most learners should understand `share` and `shareReplay` first.

## Learning Outcome

The learner should be able to state who owns the producer, whether execution is shared, and whether late subscribers receive history.

---

