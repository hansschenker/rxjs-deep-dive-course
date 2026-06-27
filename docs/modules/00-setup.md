# Module 0 — Course Setup and Mental Model


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

