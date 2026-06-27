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

