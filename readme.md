# RxJS Deep Dive

## Reactive Architecture, Operator Policies, and State Streams

### Course Thesis

RxJS is not only a library for asynchronous JavaScript. It is a language for describing how values move over time.

An Observable is a lazy description of a dataflow. Nothing runs until something subscribes. Operators rewire the stream. User functions transform the values inside the stream. Time, cancellation, sharing, errors, and completion are explicit architectural decisions.

The central principle of this course is:

> The domain can change. The RxJS machine stays the same.

A button click, an HTTP request, a WebSocket message, a form value, an animation frame, a router event, and application state can all be modeled as streams. Once they are streams, the same small set of RxJS policies can be reused everywhere.

This course therefore teaches RxJS in four layers:

1. **The Observable layer**
   What flows over time?

2. **The operator layer**
   How is the stream rewired?

3. **The policy layer**
   What happens with timing, concurrency, cancellation, sharing, and termination?

4. **The architecture layer**
   How do streams become state, effects, UI, tests, and reusable DSLs?

---

# Course Architecture

## Module 0 — Course Setup and Mental Model

### Goal

Establish the core RxJS model before learning individual operators.

### Core Idea

An Observable is a lazy, potentially infinite sequence of notifications over time.

A simple value sequence can be written as:

```txt
[{ T, a } ...]
```

Where:

* `T` is time
* `a` is a value

A more precise notification-aware notation is:

```txt
[{ T, n(a) } ... { T, c }]
```

or:

```txt
[{ T, n(a) } ... { T, e }]
```

Where:

* `n(a)` means a `next` notification carrying value `a`
* `c` means `complete`
* `e` means `error`

An Observable can emit zero values, one value, many values, infinitely many values, then either complete, error, or continue forever.

### Course Vocabulary

Throughout the course, use the following practical vocabulary:

```txt
Observable = lazy dataflow over time
Observer = consumer of notifications
Subscription = running connection between source and observer
Operator = stream rewiring function
Scheduler = time/execution coordination policy
Subject = hot multicast bridge
State = stream that remembers
```

### Marble Notation

Throughout the course, stream behavior is shown using marble diagrams. This is the visual companion to the `[{T, n(a)}]` notation.

```txt
-   one frame of virtual time
a   a next value named 'a'
|   complete notification
#   error notification
```

Examples:

```txt
--a--b--c--|       emit a, b, c then complete
--a--#             emit a then error
------|             complete without values
--a-----------     emit a and continue forever (infinite stream)
```

Marble notation is used throughout the course as a reading tool. In Module 13 it becomes a testing tool, using the same syntax inside `TestScheduler`.

### The Four Questions for Every RxJS Pipeline

Every pipeline should be readable by answering these questions:

1. What flows over time?
2. What happens when a new value arrives?
3. Which operator policy is being used?
4. What happens to cancellation, sharing, errors, and completion?

### Example

```ts
const searchResults$ = searchText$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(term => searchApi(term))
);
```

Behavior story:

```txt
searchText$ emits text values over time.
debounceTime waits until typing becomes quiet.
distinctUntilChanged ignores repeated text.
switchMap starts a request for the latest text.
If a newer text arrives, the previous request subscription is canceled.
Only the latest request result is allowed to flow downstream.
```

### Learning Outcome

After this module, the learner should stop seeing RxJS as callbacks or Promises and start seeing it as values moving over time.

---

# Module 1 — History and Lineage

## Goal

Understand why RxJS exists and why its design is connected to functional programming, LINQ, iterators, observers, and asynchronous UI architecture.

## 1. From Collections to Streams

Traditional collections are spread across space.

```txt
Array in memory:

[10, 20, 30, 40]
```

An Observable is spread across time.

```txt
Observable over time:

---10---20----------30---40---|
```

Arrays are space-indexed pull structures. Observables are time-indexed push structures.

With an array, the consumer asks for the next value.

With an Observable, the producer pushes the next value when it becomes available.

## 2. Historical Lineage

A simplified lineage is:

```txt
Functional Programming
        ↓
Haskell / List Comprehensions
        ↓
LINQ (Erik Meijer, Microsoft)
        ↓
Rx.NET (Erik Meijer, 2010)
        ↓
ReactiveX (cross-language standard)
        ↓
RxJS
```

Erik Meijer recognized that LINQ's query operators — `map`, `filter`, `reduce`, `zip` — could be applied not just to values already in memory but to values arriving over time. Rx.NET was the first implementation. RxJS brought that model to JavaScript.

The deep idea is that a collection and an event stream can be treated with similar transformation operations:

```txt
map
filter
reduce / scan
combine
flatten
```

With arrays, those operations happen over values already present in memory.

With Observables, those operations happen over values that may arrive in the future.

## 3. Iterator and Observer Duality

The Iterator pattern is pull-based.

```txt
Consumer ---- next() ----> Producer
Consumer <--- value ------ Producer
```

The Observer pattern is push-based.

```txt
Producer ---- next(value) ----> Consumer
```

The Observable/Observer pair reverses the direction of control.

An Iterator says:

```txt
Give me the next value.
```

An Observer receives:

```txt
Here is the next value.
```

## 4. Why JavaScript Needed RxJS

JavaScript applications are naturally event-heavy:

* mouse events
* keyboard events
* form changes
* HTTP requests
* timers
* animations
* WebSockets
* route changes
* application state updates

Callbacks can handle one event but become difficult when events must be combined, canceled, delayed, retried, shared, or tested.

Promises improved asynchronous syntax but have three limitations for reactive UI work:

1. A Promise is eager.
2. A Promise produces one value.
3. A Promise has no built-in multi-value stream protocol.

RxJS gives JavaScript a unified abstraction for zero, one, many, or infinite values over time.

## 5. `async/await` vs RxJS

Most JavaScript developers arrive with `async/await` experience. The comparison is:

| Concern                | `async/await`        | RxJS Observable            |
| ---------------------- | -------------------- | -------------------------- |
| Number of values       | exactly one          | zero, one, many, infinite  |
| Eagerness              | eager (starts now)   | lazy (starts on subscribe) |
| Cancellation           | no built-in support  | unsubscribe cancels        |
| Composition operators  | none                 | full operator library      |
| Time control           | no                   | debounce, throttle, delay  |
| Multicast              | no                   | share, shareReplay         |

`async/await` is the right tool for a single asynchronous value.

RxJS is the right tool when values arrive over time, when multiple subscribers must share one execution, or when cancellation, retry, or time control is needed.

They compose: `from(promise)` converts a Promise into an Observable. `firstValueFrom(source$)` converts an Observable into a Promise. The two models are not in conflict — RxJS wraps where promises fall short.

## 6. Key Takeaway

RxJS is the result of applying functional collection operations to asynchronous values distributed across time.

---

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

# Module 3 — Creation and Boundaries

## Goal

Learn how values enter RxJS from JavaScript sources, DOM events, HTTP APIs, Promises, arrays, timers, and custom producers.

## 1. Creation Operators

Creation operators turn ordinary JavaScript sources into Observables.

### `of`

Use `of` when values are already known.

```ts
import { of } from 'rxjs';

const numbers$ = of(1, 2, 3);
```

Behavior:

```txt
On subscription, emit 1, then 2, then 3, then complete.
```

Notation:

```txt
of(1,2,3) -> [{T0,n(1)}, {T0,n(2)}, {T0,n(3)}, {T0,c}]
```

### `from`

Use `from` to convert arrays, iterables, Promises, or Observable-like sources.

```ts
import { from } from 'rxjs';

const items$ = from([10, 20, 30]);
```

For a Promise:

```ts
const user$ = from(fetch('/api/user'));
```

Important:

```txt
from(promise) does not make the Promise lazy.
The Promise may already be running.
```

Use `defer` when laziness matters.

### `defer`

Use `defer` to create the source at subscription time.

```ts
import { defer, from } from 'rxjs';

const user$ = defer(() => from(fetch('/api/user')));
```

Behavior:

```txt
No fetch is created until subscription.
Each subscription creates a fresh fetch.
```

### `fromEvent`

Use `fromEvent` for DOM and EventTarget sources.

```ts
import { fromEvent } from 'rxjs';

const clicks$ = fromEvent<MouseEvent>(document, 'click');
```

Behavior:

```txt
On subscription, add event listener.
On unsubscribe, remove event listener.
```

### `interval` and `timer`

Use `interval` for repeated time ticks.

```ts
import { interval } from 'rxjs';

const ticks$ = interval(1000);
```

Use `timer` for delayed or delayed-repeated emissions.

```ts
import { timer } from 'rxjs';

const delayed$ = timer(2000);
```

### `EMPTY` and `NEVER`

`EMPTY` is an Observable that completes immediately without emitting any values.

```ts
import { EMPTY } from 'rxjs';
```

Marble:

```txt
EMPTY: |
```

Use `EMPTY` inside `catchError` to swallow an error and complete cleanly, or inside `iif` to disable a branch.

`NEVER` is an Observable that never emits and never completes.

```ts
import { NEVER } from 'rxjs';
```

Marble:

```txt
NEVER: ----------
```

Use `NEVER` to disable a stream entirely — for example, suppressing a polling interval when a feature flag is off.

### `throwError`

Use `throwError` to create an Observable that errors immediately.

```ts
import { throwError } from 'rxjs';

const failed$ = throwError(() => new Error('something went wrong'));
```

Marble:

```txt
throwError: #
```

Use `throwError` inside a `switchMap` or `mergeMap` projection to propagate a domain error as an Observable error, or in tests to simulate a failing source.

### `fromEventPattern`

Use `fromEventPattern` for event sources that do not implement `EventTarget` — such as Node.js `EventEmitter`, third-party libraries, or custom pub/sub systems.

```ts
import { fromEventPattern } from 'rxjs';
import { EventEmitter } from 'node:events';

const emitter = new EventEmitter();

const messages$ = fromEventPattern<string>(
  handler => emitter.on('message', handler),
  handler => emitter.off('message', handler)
);
```

The first function adds the listener on subscribe. The second removes it on unsubscribe.

## 2. Boundary Thinking

Every RxJS app has boundaries where untrusted or external values enter:

* DOM events
* HTTP responses
* WebSocket messages
* localStorage
* URL params
* form inputs
* JSON parsing
* third-party callbacks
* manually called `Subject.next(...)`

At these boundaries, the value should be parsed, validated, or narrowed before entering the trusted application pipeline.

## 3. Example: HTTP Boundary

```ts
import { defer, from, mergeMap, map } from 'rxjs';

type UserDto = {
  id: string;
  name: string;
};

const loadUser$ = defer(() =>
  from(fetch('/api/user'))
).pipe(
  mergeMap(response => from(response.json())),
  map(value => value as UserDto)
);
```

This is not safe enough because `as UserDto` only tells TypeScript to trust the value. It does not validate runtime data.

A safer version uses runtime validation at the boundary.

```ts
import { z } from 'zod';

const UserDtoSchema = z.object({
  id: z.string(),
  name: z.string()
});

const loadUser$ = defer(() =>
  from(fetch('/api/user'))
).pipe(
  mergeMap(response => from(response.json())),
  map(value => UserDtoSchema.parse(value))
);
```

Note: `mergeMap` is used here to unwrap the one-shot `response.json()` Promise. The flattening policies — `switchMap`, `concatMap`, `exhaustMap` — are covered in Module 5.

## 4. Practical Rule

TypeScript protects the inside of the application. Runtime validation protects the boundary.

## Learning Outcome

The learner should know how values enter RxJS and where runtime validation belongs.

---

# Module 4 — Operators as Behavior Stories

## Goal

Learn to read every operator as a behavior story over a running Observable.

## 1. Operators Rewire Streams

An operator does not care what the domain value means.

`map` does not know whether it receives a user, an order, a pixel, a price, or a keyboard event.

RxJS moves packages. It never cares what is inside them.

The business meaning lives in user functions:

```ts
source$.pipe(
  map(user => user.name)
);
```

RxJS provides the rewiring:

```txt
for each source value, apply a function and emit the result
```

The user function provides the domain logic:

```txt
user => user.name
```

## 2. Operator Behavior Notation

A basic operator can be described as:

```txt
[{T,a}] -> [{T,b}]
```

Example:

```txt
map(f): [{T,a}] -> [{T,f(a)}]
```

`filter` keeps or removes values:

```txt
filter(p): [{T,a}] -> subset [{T,a}]
```

`scan` accumulates state:

```txt
scan(reducer, seed): [{T,a}] -> [{T,state}]
```

`delay` shifts time:

```txt
delay(d): [{T,a}] -> [{T+d,a}]
```

`debounceTime(d)` filters and shifts:

```txt
debounceTime(d): burst [{T,a}] -> last quiet value [{T+d,a}]
```

## 3. The 8 Policy Framework

Every operator can be read through eight policies.

| Policy       | Question                                                         |
| ------------ | ---------------------------------------------------------------- |
| Source       | What input stream does the operator read?                        |
| Trigger      | What causes output?                                              |
| Value        | What value is emitted?                                           |
| Cardinality  | How many output values can one input value produce?              |
| Time         | When does output happen?                                         |
| Concurrency  | Can multiple inner streams run at the same time?                 |
| Cancellation | What gets stopped when new values arrive or unsubscribe happens? |
| Termination  | What happens on complete or error?                               |

## 4. Example: `map`

```txt
map(project)
```

Behavior story:

```txt
Source:
a stream of values

Trigger:
each source next value

Value:
project(value)

Cardinality:
one input value produces one output value

Time:
same time as source emission

Concurrency:
none

Cancellation:
no inner subscription to cancel

Termination:
source complete completes output
source error errors output
```

Code:

```ts
const names$ = users$.pipe(
  map(user => user.name)
);
```

## 5. Example: `filter`

```txt
filter(predicate)
```

Behavior story:

```txt
Source:
a stream of values

Trigger:
each source next value

Value:
the original value, unchanged

Cardinality:
one input value produces zero or one output values

Time:
same time as source emission

Concurrency:
none

Cancellation:
none

Termination:
source complete completes output
source error errors output
```

Code:

```ts
const validUsers$ = users$.pipe(
  filter(user => user.active)
);
```

## 6. Example: `scan`

```txt
scan(reducer, initialState)
```

Behavior story:

```txt
Source:
a stream of actions or values

Trigger:
each source next value

Value:
new accumulated state

Cardinality:
one input value produces one accumulated output value

Time:
same time as source emission

Concurrency:
none

Cancellation:
none

Termination:
source complete completes output
source error errors output
```

Code:

```ts
const count$ = clicks$.pipe(
  map(() => 1),
  scan((count, delta) => count + delta, 0)
);
```

## 7. Example: `tap`

```txt
tap(sideEffect)
```

Behavior story:

```txt
Source:
a stream of values

Trigger:
each source next value, error, or complete

Value:
the original value, unchanged

Cardinality:
one input value produces one output value

Time:
same time as source emission

Concurrency:
none

Cancellation:
none

Termination:
source complete completes output
source error errors output
```

Code:

```ts
const names$ = users$.pipe(
  tap(user => console.log('before filter', user)),
  filter(user => user.active),
  tap(user => console.log('after filter', user)),
  map(user => user.name)
);
```

`tap` is the primary debugging tool. It runs a side effect for each notification without changing the value or breaking the stream.

Rule: `tap` is always safe to add or remove. It never affects the values downstream.

## 8. Example: `startWith`

```txt
startWith(initialValue)
```

Behavior story:

```txt
Source:
a stream of values

Trigger:
immediately on subscription, then each source next value

Value:
the initial value first, then source values

Cardinality:
adds one extra value at the start

Time:
initial value emits synchronously before source starts

Concurrency:
none

Cancellation:
none

Termination:
source complete completes output
source error errors output
```

Code:

```ts
const status$ = serverStatus$.pipe(
  startWith('connecting')
);
```

`startWith` is used to give a stream an immediate value before the source emits. This is required in the state pattern (`scan + startWith + shareReplay`) so that late subscribers receive initial state without waiting for the first action.

## 9. Example: `distinctUntilChanged`

```txt
distinctUntilChanged()
```

Behavior story:

```txt
Source:
a stream of values

Trigger:
each source value that differs from the previous value

Value:
the original value, unchanged

Cardinality:
one input value produces zero or one output values

Time:
same time as source emission

Concurrency:
none

Cancellation:
none

Termination:
source complete completes output
source error errors output
```

Code:

```ts
const searchText$ = input$.pipe(
  map(event => (event.target as HTMLInputElement).value),
  distinctUntilChanged()
);
```

`distinctUntilChanged` prevents redundant downstream work when a source emits the same value twice in a row. It is almost always paired with `debounceTime` for search inputs and with `map` for state slices.

## 10. Example: `take` and `takeUntil`

```txt
take(count)
```

Behavior story:

```txt
Trigger:
each source value up to count

Cardinality:
passes at most count values

Termination:
completes after count values regardless of source
```

Code:

```ts
interval(1000).pipe(
  take(3)
).subscribe(console.log);
// emits 0, 1, 2 then completes
```

```txt
takeUntil(notifier$)
```

Behavior story:

```txt
Trigger:
each source value until notifier emits

Cardinality:
passes values until notified

Termination:
completes when notifier emits its first value
```

Code:

```ts
const destroy$ = new Subject<void>();

clicks$.pipe(
  takeUntil(destroy$)
).subscribe(console.log);
```

`takeUntil` is the idiomatic way to tie a stream's lifetime to a component or service lifecycle. When `destroy$.next()` is called, the stream completes and teardown runs automatically.

## 11. `scan` vs `reduce`

`scan` emits the accumulated state after every source value.

`reduce` emits only the final accumulated value after the source completes.

```ts
// scan: emits 1, 3, 6, 10 as values arrive
of(1, 2, 3, 4).pipe(
  scan((acc, val) => acc + val, 0)
).subscribe(console.log);

// reduce: emits only 10 after source completes
of(1, 2, 3, 4).pipe(
  reduce((acc, val) => acc + val, 0)
).subscribe(console.log);
```

In reactive UI work, `scan` is almost always the right choice because state must be visible after each action, not only at the end.

## 12. Debugging Pipelines

The standard technique is to insert named `tap` calls at each pipeline stage.

```ts
const result$ = source$.pipe(
  tap(v => console.log('[source]', v)),
  filter(user => user.active),
  tap(v => console.log('[after filter]', v)),
  map(user => user.name),
  tap(v => console.log('[after map]', v))
);
```

Guidelines:

```txt
Name every tap with a stage label.
Remove tap calls before committing — they are development scaffolding.
If a stage never logs, the stream stopped before that point.
If values are wrong at a stage, the operator above is the culprit.
```

For complex pipelines, assign intermediate streams to named variables:

```ts
const activeUsers$ = users$.pipe(filter(u => u.active));
const activeNames$ = activeUsers$.pipe(map(u => u.name));
```

Named variables make stack traces and `tap` labels easier to read.

## Learning Outcome

The learner should be able to explain an operator without relying only on its TypeScript signature, inspect any pipeline with `tap`, and manage stream lifetime with `take` and `takeUntil`.

---

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

# Module 6 — Time, Rate Limiting, and Schedulers

## Goal

Understand time-based operators and schedulers as explicit time policies.

## 1. Time Comes from Sources and Schedulers

RxJS operators do not create time by magic.

Time comes from:

* DOM events
* timers
* animation frames
* WebSockets
* HTTP responses
* schedulers
* external producers

A scheduler controls when work is scheduled. It does not make JavaScript multithreaded.

## 2. Rate-Limiting Operators

### `debounceTime`

Policy:

```txt
wait for silence, then emit the latest value
```

Use for typeahead:

```ts
const searchText$ = input$.pipe(
  map(event => event.target.value),
  debounceTime(300),
  distinctUntilChanged()
);
```

Behavior story:

```txt
Each value resets the timer.
Only the latest value after a quiet window is emitted.
```

### `throttleTime`

Policy:

```txt
emit first, then ignore for a time window
```

Use for double-click protection or high-frequency input.

```ts
const safeClicks$ = clicks$.pipe(
  throttleTime(1000)
);
```

### `auditTime`

Policy:

```txt
open a time window, then emit the latest value when the window closes
```

Use for resize, scroll, and rendering loops.

```ts
const layout$ = resize$.pipe(
  auditTime(100)
);
```

### `sampleTime`

Policy:

```txt
at fixed intervals, emit the latest value if one exists
```

Use for dashboards or telemetry.

```ts
const sampledTelemetry$ = telemetry$.pipe(
  sampleTime(1000)
);
```

### `sample`

Policy:

```txt
whenever a notifier emits, emit the latest source value if one exists
```

`sample` is event-triggered; `sampleTime` is interval-triggered.

```ts
const snapshot$ = mouseMove$.pipe(
  sample(frameClicks$)
);
```

Use when the sampling trigger is itself an Observable rather than a fixed interval — for example, sampling mouse position on every animation frame tick or on every button click.

## 3. Batching Operators

### `bufferTime`

Policy:

```txt
collect source values into an array, emit the array at fixed intervals
```

```ts
import { bufferTime } from 'rxjs';

const batched$ = highFrequency$.pipe(
  bufferTime(500)
);
```

Behavior:

```txt
Every 500 ms, emit an array of all values received in that window.
If no values arrived, emit an empty array.
```

Use for batching high-frequency events before processing them together — analytics, log shipping, bulk API calls.

### `windowTime`

Policy:

```txt
like bufferTime, but emits an Observable window instead of an array
```

```ts
import { windowTime, mergeAll } from 'rxjs';

const windowed$ = source$.pipe(
  windowTime(500),
  mergeAll()
);
```

Use `bufferTime` when you need an array. Use `windowTime` when you need to apply operators to each window as a stream.

## 4. Time Policy Table

| Operator       | Policy              | Emits               | Typical use          |
| -------------- | ------------------- | ------------------- | -------------------- |
| `debounceTime` | wait for silence    | latest after quiet  | search input         |
| `throttleTime` | first then suppress | first               | button protection    |
| `auditTime`    | wait then latest    | latest after window | resize/render        |
| `sampleTime`   | periodic sample     | latest at interval  | dashboard            |
| `sample`       | event-triggered     | latest at signal    | frame/click snapshot |
| `bufferTime`   | collect then emit   | array of values     | batch processing     |

## 5. Schedulers

A scheduler controls when work is queued and executed. It does not make JavaScript multithreaded.

| Scheduler                 | When work runs                                     |
| ------------------------- | -------------------------------------------------- |
| `queueScheduler`          | synchronous queue, useful for recursive scheduling |
| `asapScheduler`           | microtask queue (like Promise.resolve)             |
| `asyncScheduler`          | macrotask queue (like setTimeout)                  |
| `animationFrameScheduler` | browser animation frame (requestAnimationFrame)    |

Practical rule:

```txt
Most applications never need to set schedulers explicitly.
Schedulers are usually implicit — interval uses asyncScheduler internally.

The primary reason to understand schedulers is testing:
VirtualTimeScheduler (used in Module 13) replaces real time with simulated time,
making time-based operators deterministic and instant in tests.
```

Example of explicit scheduler use — delivering state on animation frames:

```ts
import { observeOn, animationFrameScheduler } from 'rxjs';

const render$ = state$.pipe(
  observeOn(animationFrameScheduler)
);
```

## 6. `subscribeOn` vs `observeOn`

`subscribeOn` controls when the source subscription starts.

`observeOn` controls when notifications are delivered downstream.

```ts
source$.pipe(
  subscribeOn(asyncScheduler),      // start subscribing on next macrotask
  observeOn(animationFrameScheduler) // deliver values on animation frames
);
```

`observeOn` is the more commonly used of the two. It moves value delivery to a different execution context without changing when the source starts.

## Learning Outcome

The learner should treat time as an explicit policy, not as incidental behavior, and understand that schedulers are the mechanism that makes virtual-time testing possible.

---

# Module 7 — Combining Streams

## Goal

Learn how multiple streams coordinate over time.

## 1. Why Combining Matters

Real applications rarely depend on one stream only.

Examples:

* form fields combine into form validity
* route params combine with loaded data
* user preferences combine with API results
* mouse position combines with animation frame
* state combines with effects
* authentication combines with permissions

## 2. `combineLatest`

Policy:

```txt
Whenever any source emits, combine the latest value from every source.
But wait until every source has emitted at least once.
```

Example:

```ts
const formValid$ = combineLatest([
  nameValid$,
  emailValid$,
  passwordValid$
]).pipe(
  map(([nameValid, emailValid, passwordValid]) =>
    nameValid && emailValid && passwordValid
  )
);
```

Behavior story:

```txt
Each input stream contributes its latest value.
Output starts only when all streams have emitted.
After that, every new value from any input recomputes the result.
```

Hazard:

```txt
If one stream never emits, combineLatest never emits.
```

Solution:

```ts
field$.pipe(
  startWith(initialValue)
);
```

## 3. `withLatestFrom`

Policy:

```txt
Only the primary stream triggers output.
Secondary streams provide their latest values.
```

Example:

```ts
const submit$ = submitClicks$.pipe(
  withLatestFrom(formValue$),
  map(([_, form]) => form)
);
```

Behavior story:

```txt
Click is the trigger.
Form value is context.
A form change alone does not submit.
```

## 4. `zip`

Policy:

```txt
Pair values by index.
Wait until each stream has the next value.
```

Example:

```ts
const pairs$ = zip(firstName$, lastName$);
```

Notation:

```txt
zip([a], [b]) -> [(a,b)]
```

Use when order and pairing matter.

Hazard:

```txt
Fast streams wait for slow streams.
```

## 5. `forkJoin`

Policy:

```txt
Wait for all sources to complete, then emit their final values once.
```

Example:

```ts
const pageData$ = forkJoin({
  user: loadUser$,
  settings: loadSettings$,
  permissions: loadPermissions$
});
```

Use for one-time page loading.

Hazard:

```txt
If one source never completes, forkJoin never emits.
```

## 6. `merge`

Policy:

```txt
Forward values from all sources as they arrive.
```

Example:

```ts
const userActions$ = merge(
  saveClicks$,
  cancelClicks$,
  routeChanges$
);
```

## 7. `concat`

Policy:

```txt
Subscribe to the next source only after the previous source completes.
```

Example:

```ts
const startup$ = concat(
  loadConfig$,
  loadUser$,
  loadDashboard$
);
```

## 8. `race`

Policy:

```txt
The first source to emit wins.
All others are unsubscribed.
```

Example:

```ts
const result$ = race(
  apiRequest$,
  timeoutWarning$
);
```

## 9. `partition`

Policy:

```txt
Split one stream into two by a predicate.
The first stream receives values where predicate is true.
The second stream receives values where predicate is false.
```

```ts
import { partition } from 'rxjs';

const [admins$, regularUsers$] = partition(
  users$,
  user => user.role === 'admin'
);
```

Use `partition` when a single source stream must feed two separate downstream pipelines — for example, routing admin actions and user actions to different handlers, or separating valid and invalid form entries.

Marble:

```txt
source:   --a--b--c--d--|
          (a,c pass predicate; b,d do not)
match$:   --a-----c-----|
noMatch$: -----b-----d--|
```

## 10. `iif`

Policy:

```txt
Choose between two Observables at subscription time based on a condition.
```

```ts
import { iif, of, EMPTY } from 'rxjs';

const result$ = iif(
  () => isLoggedIn(),
  loadDashboard$,
  EMPTY
);
```

`iif` evaluates the condition at the moment of subscription, not when `iif` is called. This makes it lazy and suitable for dynamic routing between two sources.

## 11. Pipeable Operator Variants

The static combination operators each have a pipeable counterpart that can be used inside `.pipe()`:

| Static form                      | Pipeable form                 |
| -------------------------------- | ----------------------------- |
| `combineLatest([a$, b$])`        | `a$.pipe(combineLatestWith(b$))` |
| `merge(a$, b$)`                  | `a$.pipe(mergeWith(b$))`      |
| `concat(a$, b$)`                 | `a$.pipe(concatWith(b$))`     |
| `zip(a$, b$)`                    | `a$.pipe(zipWith(b$))`        |
| `race(a$, b$)`                   | `a$.pipe(raceWith(b$))`       |

Both forms produce identical behavior. The pipeable variants are useful when one source is already the result of a `.pipe()` chain and adding a static wrapper would reduce readability.

## Combining Policy Table

| Operator         | Trigger           | Waits for all first values? | Needs completion? | Use case             |
| ---------------- | ----------------- | --------------------------- | ----------------- | -------------------- |
| `combineLatest`  | any source        | yes                         | no                | live derived state   |
| `withLatestFrom` | primary source    | secondary latest needed     | no                | event + state        |
| `zip`            | all next values   | yes                         | no                | ordered pairing      |
| `forkJoin`       | all complete      | yes                         | yes               | page load bundle     |
| `merge`          | any source        | no                          | no                | action streams       |
| `concat`         | previous complete | no                          | yes               | sequential workflows |
| `race`           | first source      | no                          | no                | first response wins  |
| `partition`      | any source        | no                          | no                | split by predicate   |
| `iif`            | subscription      | no                          | no                | conditional source   |

## Learning Outcome

The learner should select a combination operator by trigger policy, not by memorized examples, and know how to split a stream with `partition` or conditionally select a source with `iif`.

---

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

# Module 11 — TypeScript and Runtime Safety

## Goal

Use TypeScript for internal safety and runtime validation for external input.

## 1. TypeScript Inside the Pipeline

TypeScript gives compile-time guarantees.

Example:

```ts
type User = {
  id: string;
  name: string;
  active: boolean;
};
```

Typed stream:

```ts
const activeUserNames$ = users$.pipe(
  filter(user => user.active),
  map(user => user.name)
);
```

## 2. Type Guards

A type guard narrows the stream.

```ts
function isNotNull<T>(value: T | null | undefined): value is T {
  return value !== null && value !== undefined;
}

const validValues$ = source$.pipe(
  filter(isNotNull)
);
```

Custom operator:

```ts
import { filter, OperatorFunction } from 'rxjs';

export function validValues<T>(): OperatorFunction<T | null | undefined, T> {
  return source$ =>
    source$.pipe(
      filter((value): value is T =>
        value !== null && value !== undefined
      )
    );
}
```

Usage:

```ts
const names$ = possibleNames$.pipe(
  validValues(),
  map(name => name.toUpperCase())
);
```

## 3. Discriminated Union Actions

Use discriminated unions for state actions.

```ts
type Action =
  | { type: 'LoadRequested' }
  | { type: 'LoadSucceeded'; users: User[] }
  | { type: 'LoadFailed'; error: string };
```

Reducer:

```ts
function update(state: State, action: Action): State {
  switch (action.type) {
    case 'LoadRequested':
      return { ...state, status: 'loading' };

    case 'LoadSucceeded':
      return { ...state, status: 'success', users: action.users };

    case 'LoadFailed':
      return { ...state, status: 'failure', error: action.error };
  }
}
```

## 4. Runtime Boundary Validation

External data is not trusted just because TypeScript says it has a type.

Use Zod or a similar parser at boundaries.

```ts
import { z } from 'zod';

const UserSchema = z.object({
  id: z.string(),
  name: z.string(),
  active: z.boolean()
});

type User = z.infer<typeof UserSchema>;
```

Boundary pipeline:

```ts
const users$ = defer(() => from(fetch('/api/users'))).pipe(
  switchMap(response => from(response.json())),
  map(value => UserSchema.array().parse(value))
);
```

## 5. Trust Boundary Table

| Source                  | Needs runtime validation? |
| ----------------------- | ------------------------- |
| HTTP response           | yes                       |
| WebSocket message       | yes                       |
| localStorage            | yes                       |
| URL params              | yes                       |
| form data               | usually yes               |
| internal reducer output | no, if typed correctly    |
| hardcoded test data     | usually no                |

## 6. Operator Authoring Types

When writing custom operators (Module 12), three TypeScript types matter:

**`OperatorFunction<T, R>`** — an operator that transforms the element type from `T` to `R`:

```ts
import { OperatorFunction } from 'rxjs';

export function toString<T>(): OperatorFunction<T, string> {
  return source$ => source$.pipe(map(v => String(v)));
}
```

**`MonoTypeOperatorFunction<T>`** — an operator that keeps the same element type (a specialization of `OperatorFunction<T, T>`):

```ts
import { MonoTypeOperatorFunction } from 'rxjs';

export function requireNonEmpty(): MonoTypeOperatorFunction<string> {
  return source$ => source$.pipe(filter(v => v.length > 0));
}
```

**`ObservableInput<T>`** — the type that higher-order operators (`mergeMap`, `switchMap`, `concatMap`, `exhaustMap`) accept as the return type of their `project` function. It covers `Observable<T>`, `Promise<T>`, arrays, and iterables:

```ts
import { ObservableInput, OperatorFunction, mergeMap } from 'rxjs';

export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(mergeMap(project));
}
```

Use `ObservableInput<R>` (not `Observable<R>`) so the project function can return a `Promise` or array without requiring wrapping.

## 7. Schema Composition with Zod

Module 3 introduced single-schema validation at HTTP boundaries. In production, schemas compose.

**Discriminated union for action types** — validates that an incoming message is a known action:

```ts
import { z } from 'zod';

const SearchChangedSchema = z.object({
  type: z.literal('SearchChanged'),
  term: z.string()
});

const LoadSucceededSchema = z.object({
  type: z.literal('LoadSucceeded'),
  results: z.array(z.object({ id: z.string(), title: z.string() }))
});

const LoadFailedSchema = z.object({
  type: z.literal('LoadFailed'),
  error: z.string()
});

const ActionSchema = z.discriminatedUnion('type', [
  SearchChangedSchema,
  LoadSucceededSchema,
  LoadFailedSchema
]);

type Action = z.infer<typeof ActionSchema>;
```

**`safeParse` instead of `parse` in streams** — `parse` throws on invalid input; `safeParse` returns a result object and keeps the stream alive:

```ts
const safeAction$ = rawWebSocket$.pipe(
  map(raw => ActionSchema.safeParse(JSON.parse(raw))),
  filter(result => result.success),
  map(result => result.data)
);
```

**Schema reuse across layers** — define the schema once at the boundary, derive the TypeScript type with `z.infer`, and use that type throughout the application:

```ts
// Schema is the source of truth
const UserSchema = z.object({ id: z.string(), name: z.string(), active: z.boolean() });

// TypeScript type derived from schema — no duplication
type User = z.infer<typeof UserSchema>;
```

## Learning Outcome

The learner should know that TypeScript is internal compile-time safety, while Zod or runtime validation protects incoming values — and that `OperatorFunction<T, R>`, `MonoTypeOperatorFunction<T>`, and `ObservableInput<T>` are the types used when authoring operators.

---

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

# Module 14 — Framework Integration

## Goal

Integrate RxJS into frontend frameworks while keeping responsibilities clear.

## 1. RxJS and UI Frameworks

RxJS should not fight the rendering framework.

A good separation is:

```txt
RxJS:
events, effects, async workflows, cancellation, state streams

UI framework:
rendering, template binding, component lifecycle, local view concerns
```

## 2. Angular Signals and RxJS

Signals are good for fine-grained synchronous state at rest.

RxJS is good for asynchronous state in motion.

Architecture:

```txt
DOM / HTTP / WebSocket / Router
        ↓
      RxJS
        ↓
    state stream
        ↓
     toSignal
        ↓
   Angular template
```

## 3. Example

```ts
@Component({
  selector: 'app-search',
  template: `
    <input
      [value]="query()"
      (input)="query.set($any($event.target).value)"
    />

    <div *ngIf="state().status === 'loading'">
      Loading...
    </div>

    <ul>
      <li *ngFor="let item of state().results">
        {{ item.name }}
      </li>
    </ul>
  `
})
export class SearchComponent {
  private http = inject(HttpClient);

  query = signal('');

  private query$ = toObservable(this.query);

  private state$ = this.query$.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    switchMap(query =>
      this.http.get<Result[]>(`/api/search?q=${query}`).pipe(
        map(results => ({
          status: 'success' as const,
          results
        })),
        startWith({
          status: 'loading' as const,
          results: []
        }),
        catchError(error =>
          of({
            status: 'failure' as const,
            results: [],
            error: String(error)
          })
        )
      )
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  state = toSignal(this.state$, {
    initialValue: {
      status: 'idle' as const,
      results: []
    }
  });
}
```

## 4. Angular Service Pattern

The component example above handles simple search. For shared application state, move the stream logic into a service:

```ts
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Subject, merge, scan, startWith, shareReplay } from 'rxjs';
import { switchMap, map, catchError } from 'rxjs';
import { of } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class SearchService {
  private http = inject(HttpClient);
  private actions$ = new Subject<Action>();

  readonly state$ = this.actions$.pipe(
    scan(update, initialState),
    startWith(initialState),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  dispatch(action: Action): void {
    this.actions$.next(action);
  }
}
```

Components inject the service and subscribe via `toSignal`:

```ts
export class SearchComponent {
  private service = inject(SearchService);
  state = toSignal(this.service.state$, { initialValue: initialState });

  onInput(query: string): void {
    this.service.dispatch({ type: 'QueryChanged', query });
  }
}
```

The service owns the subscription lifetime. Components are pure view layers.

## 5. `takeUntilDestroyed`

`takeUntilDestroyed` (Angular 16+) automatically completes a stream when the injection context that created it is destroyed — no manual unsubscribe needed:

```ts
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Injectable({ providedIn: 'root' })
export class NotificationService {
  private destroyRef = inject(DestroyRef);

  readonly notifications$ = interval(5000).pipe(
    switchMap(() => this.http.get<Notification[]>('/api/notifications')),
    takeUntilDestroyed(this.destroyRef)
  );
}
```

In a component (where `DestroyRef` is available from the injection context implicitly):

```ts
export class FeedComponent {
  constructor() {
    this.feed$.pipe(takeUntilDestroyed()).subscribe(this.render.bind(this));
  }
}
```

## 6. Lifecycle Rule

Subscriptions must be owned by a lifecycle.

Options:

* template async pipe (Angular, Vue)
* `toSignal` (Angular)
* `takeUntilDestroyed` (Angular 16+)
* `watchEffect` cleanup return (Vue)
* `useEffect` cleanup return (React)
* framework-specific cleanup APIs

Avoid unmanaged subscriptions in components.

## 7. React Integration

In React, RxJS can be integrated with hooks.

A stream should be subscribed to inside an effect and cleaned up when the component unmounts.

```ts
function useObservable<T>(source$: Observable<T>, initial: T): T {
  const [value, setValue] = useState(initial);

  useEffect(() => {
    const subscription = source$.subscribe(setValue);
    return () => subscription.unsubscribe();
  }, [source$]);

  return value;
}
```

## 8. Vue Integration

In Vue, `watchEffect` runs a side effect and tracks reactive dependencies. Use the cleanup callback to unsubscribe:

```ts
import { ref, watchEffect, onUnmounted } from 'vue';
import { Subject } from 'rxjs';
import { debounceTime, switchMap } from 'rxjs';

export function useSearch() {
  const query = ref('');
  const results = ref<Result[]>([]);
  const status = ref<'idle' | 'loading' | 'success' | 'failure'>('idle');

  const query$ = new Subject<string>();

  const subscription = query$.pipe(
    debounceTime(300),
    switchMap(q =>
      searchApi(q).pipe(
        map(r => ({ status: 'success' as const, results: r })),
        startWith({ status: 'loading' as const, results: [] }),
        catchError(() => of({ status: 'failure' as const, results: [] }))
      )
    )
  ).subscribe(state => {
    status.value = state.status;
    results.value = state.results;
  });

  watchEffect(() => {
    query$.next(query.value);
  });

  onUnmounted(() => subscription.unsubscribe());

  return { query, results, status };
}
```

The composable owns the subscription. The component uses `query`, `results`, and `status` as reactive refs.

## Learning Outcome

The learner should know where RxJS belongs in an application, how to connect it to rendering without leaking subscriptions, and how each major framework's lifecycle maps to RxJS subscription management.

---

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

# Final Course Principles

```txt
RxJS moves values over time.

An Observable is a lazy dataflow description.

Nothing runs until subscription.

Operators rewire streams.

User functions transform values.

Flattening operators are policies.

mergeMap means allow overlap.

switchMap means keep only the latest.

concatMap means queue while busy.

exhaustMap means ignore while busy.

Time comes from sources and schedulers.

State is a stream that remembers.

Sharing is explicit.

Cancellation is explicit.

Errors are either terminal notifications or domain values.

A custom operator is a named policy with tests and error handling.

The domain can change. The RxJS machine stays the same.
```
