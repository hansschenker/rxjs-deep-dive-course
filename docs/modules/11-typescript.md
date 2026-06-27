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

