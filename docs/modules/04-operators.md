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

