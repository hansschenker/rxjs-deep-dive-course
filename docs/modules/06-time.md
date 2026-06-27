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

