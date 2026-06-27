// Module 11 — TypeScript and Runtime Safety | Companion code for RxJS Deep Dive

import {
  of, from, Subject,
  filter, map, mergeMap,
  type OperatorFunction, type MonoTypeOperatorFunction, type ObservableInput
} from 'rxjs';
import { z } from 'zod';

// ─── Zod UserSchema — defined first so the derived User type is available ─────
// This is Section 4 (runtime boundary) defined early so User works throughout.
// In production, define schemas at the I/O boundary and derive all types from them.

export const UserSchema = z.object({
  id: z.string(),
  name: z.string(),
  active: z.boolean()
});

// TypeScript type derived from schema — single source of truth, no duplication
export type User = z.infer<typeof UserSchema>;

// ─── Section 1: TypeScript inside the pipeline ────────────────────────────────
// TypeScript gives compile-time guarantees about the type of each value in the chain.

function demoTypedPipeline(): void {
  const users$ = from([
    { id: '1', name: 'Alice', active: true },
    { id: '2', name: 'Bob',   active: false },
    { id: '3', name: 'Carol', active: true }
  ] as User[]);

  // TypeScript infers Observable<string> after the two operators
  const activeUserNames$ = users$.pipe(
    filter(user => user.active),
    map(user => user.name)
  );

  activeUserNames$.subscribe(name => console.log('[typed-pipeline]', name));
  // [typed-pipeline] Alice
  // [typed-pipeline] Carol
}

// ─── Section 2: Type guards ───────────────────────────────────────────────────
// A type guard narrows the stream element type at compile time.

export function isNotNull<T>(value: T | null | undefined): value is T {
  return value !== null && value !== undefined;
}

// Reusable operator: filters out null and undefined, narrowing T | null | undefined → T
export function validValues<T>(): OperatorFunction<T | null | undefined, T> {
  return source$ =>
    source$.pipe(
      filter((value): value is T => value !== null && value !== undefined)
    );
}

function demoTypeGuards(): void {
  const possibleNames$ = from(
    ['Alice', null, 'Bob', undefined, 'Carol'] as Array<string | null | undefined>
  );

  // After validValues(), TypeScript knows the value is string — no cast needed
  const cleanNames$ = possibleNames$.pipe(
    validValues(),
    map(name => name.toUpperCase())  // TypeScript: name is string, not string|null|undefined
  );

  cleanNames$.subscribe(n => console.log('[type-guard]', n));
  // [type-guard] ALICE, BOB, CAROL

  // Standalone type guard used with filter
  const withFilter$ = possibleNames$.pipe(
    filter(isNotNull),
    map(name => name.toUpperCase())
  );

  withFilter$.subscribe(n => console.log('[isNotNull]', n));
}

// ─── Section 3: Discriminated Union Actions ───────────────────────────────────
// Use discriminated unions for state actions.
// The switch in the reducer exhausts all cases — TypeScript enforces this.

export type Action =
  | { type: 'LoadRequested' }
  | { type: 'LoadSucceeded'; users: User[] }
  | { type: 'LoadFailed'; error: string };

export type AppState = {
  status: 'idle' | 'loading' | 'success' | 'failure';
  users: User[];
  error: string | null;
};

export function update(state: AppState, action: Action): AppState {
  switch (action.type) {
    case 'LoadRequested':
      return { ...state, status: 'loading' };

    case 'LoadSucceeded':
      return { ...state, status: 'success', users: action.users, error: null };

    case 'LoadFailed':
      return { ...state, status: 'failure', error: action.error };
  }
}

function demoDiscriminatedUnion(): void {
  const initialAppState: AppState = { status: 'idle', users: [], error: null };
  const users: User[] = [{ id: '1', name: 'Alice', active: true }];

  const stateAfterLoad = update(
    update(initialAppState, { type: 'LoadRequested' }),
    { type: 'LoadSucceeded', users }
  );

  console.log('[discriminated-union] state after load:', stateAfterLoad.status);
  // [discriminated-union] state after load: success

  const stateAfterError = update(
    update(initialAppState, { type: 'LoadRequested' }),
    { type: 'LoadFailed', error: 'network error' }
  );

  console.log('[discriminated-union] state after error:', stateAfterError);
}

// ─── Section 4: Runtime Boundary Validation ───────────────────────────────────
// TypeScript only protects compile-time. Data arriving from outside the app
// (HTTP, WebSocket, localStorage, URL params) must be validated at runtime.

// UserSchema and User are already defined at the top — see above.

// Usage in a boundary pipeline (conceptual — requires browser fetch API):
//
//   const users$ = defer(() => from(fetch('/api/users'))).pipe(
//     switchMap(response => from(response.json())),
//     map(value => UserSchema.array().parse(value))
//   );
//   // parse() throws ZodError on invalid input — wrapping in catchError is recommended

function demoRuntimeValidation(): void {
  // Validate an inbound value at the boundary using the schema
  const raw: unknown = { id: '1', name: 'Alice', active: true };
  const result = UserSchema.safeParse(raw);

  if (result.success) {
    console.log('[runtime-validation] parsed user:', result.data.name);
  } else {
    console.log('[runtime-validation] invalid:', result.error.message);
  }

  // Invalid input
  const bad: unknown = { id: 42, name: null };
  const badResult = UserSchema.safeParse(bad);
  console.log('[runtime-validation] bad input success:', badResult.success);
  // [runtime-validation] bad input success: false
}

// ─── Section 5: Trust Boundary Table ────────────────────────────────────────
/*
 * Trust boundary: where runtime validation is required
 *
 *   Source                   | Needs runtime validation?
 *   HTTP response            | yes
 *   WebSocket message        | yes
 *   localStorage             | yes
 *   URL params               | yes
 *   form data                | usually yes
 *   internal reducer output  | no, if typed correctly
 *   hardcoded test data      | usually no
 *
 * TypeScript protects the inside of the application (compile-time).
 * Zod / runtime parsing protects the edges (runtime).
 */

// ─── Section 6: Operator authoring types ─────────────────────────────────────
// Three TypeScript types matter when writing custom operators.

// OperatorFunction<T, R> — transforms element type from T to R
export function toString<T>(): OperatorFunction<T, string> {
  return source$ => source$.pipe(map(v => String(v)));
}

// MonoTypeOperatorFunction<T> — keeps the same element type (OperatorFunction<T, T>)
export function requireNonEmpty(): MonoTypeOperatorFunction<string> {
  return source$ => source$.pipe(filter(v => v.length > 0));
}

// ObservableInput<T> — accepts Observable<T>, Promise<T>, array, or iterable
// Use ObservableInput<R> (not Observable<R>) so the project function can return a Promise
export function keepLatest<T, R>(
  project: (value: T) => ObservableInput<R>
): OperatorFunction<T, R> {
  return source$ => source$.pipe(mergeMap(project));
}

function demoOperatorTypes(): void {
  // toString: any stream → string stream
  of(1, 2, 3).pipe(toString()).subscribe(v => console.log('[toString]', v, typeof v));

  // requireNonEmpty: filter empty strings
  of('hello', '', 'world', '').pipe(requireNonEmpty()).subscribe(v => console.log('[requireNonEmpty]', v));

  // keepLatest: project each value to an ObservableInput
  of(1, 2, 3).pipe(
    keepLatest(v => [v * 10, v * 100])  // project returns an array (ObservableInput)
  ).subscribe(v => console.log('[keepLatest]', v));
}

// ─── Section 7: Schema Composition with Zod ──────────────────────────────────
// Schemas compose. Validate complex discriminated-union messages at the boundary.

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

export const ActionSchema = z.discriminatedUnion('type', [
  SearchChangedSchema,
  LoadSucceededSchema,
  LoadFailedSchema
]);

// Derive the TypeScript type from the schema — no duplication
export type ParsedAction = z.infer<typeof ActionSchema>;

// safeParse instead of parse keeps the stream alive on invalid input.
// parse() throws (unhandled error); safeParse() returns a result object.

function demoSafeParsePipeline(): void {
  const rawWebSocket$ = new Subject<string>();

  // Type guard so TypeScript narrows the success branch to { data: ParsedAction }
  const safeAction$ = rawWebSocket$.pipe(
    map(raw => ActionSchema.safeParse(JSON.parse(raw) as unknown)),
    filter((r): r is { success: true; data: ParsedAction } => r.success),
    map(r => r.data)
  );

  safeAction$.subscribe(a => console.log('[safe-action]', a));

  // Valid — passes schema validation
  rawWebSocket$.next(JSON.stringify({ type: 'SearchChanged', term: 'hello' }));

  // Unknown type — safeParse returns { success: false }; filtered out; stream stays alive
  rawWebSocket$.next(JSON.stringify({ type: 'UnknownAction', foo: 'bar' }));

  // Valid — passes schema validation
  rawWebSocket$.next(JSON.stringify({ type: 'LoadSucceeded', results: [{ id: '1', title: 'item' }] }));

  rawWebSocket$.complete();
  // Expected:
  //   [safe-action] { type: 'SearchChanged', term: 'hello' }
  //   [safe-action] { type: 'LoadSucceeded', results: [...] }
  //   (UnknownAction is silently dropped — no stream termination)
}

// Schema reuse — define once, derive the type, use everywhere:
//
//   const UserSchema = z.object({ id: z.string(), name: z.string(), active: z.boolean() });
//   type User = z.infer<typeof UserSchema>;   ← no duplication of field names
//
// The schema is the single source of truth: it validates at runtime AND
// TypeScript derives the compile-time type from it. No separate interface needed.

// ─── Run all demos ────────────────────────────────────────────────────────────

demoTypedPipeline();
demoTypeGuards();
demoDiscriminatedUnion();
demoRuntimeValidation();
demoOperatorTypes();
demoSafeParsePipeline();
