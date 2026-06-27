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

