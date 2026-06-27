// Module 3 — Creation and Boundaries | Companion code for RxJS Deep Dive

import {
  of, from, defer, fromEvent, fromEventPattern,
  interval, timer, EMPTY, NEVER, throwError,
  catchError, mergeMap, map, take,
} from 'rxjs';
import { EventEmitter } from 'node:events';
import { z } from 'zod';

// --- 1. of — synchronous emission of known values ---
//   Notation: of(1,2,3) -> [{T0,n(1)}, {T0,n(2)}, {T0,n(3)}, {T0,c}]
//   On subscribe: emit 1, then 2, then 3, then complete. All synchronous.

function demoOf(): void {
  const numbers$ = of(1, 2, 3);
  const sub = numbers$.subscribe({
    next: v => console.log('[of] value:', v),
    complete: () => console.log('[of] complete'),
  });
  sub.unsubscribe();
}

// --- 2. from — array to Observable ---

function demoFromArray(): void {
  const items$ = from([10, 20, 30]);
  const sub = items$.subscribe({
    next: v => console.log('[from array] value:', v),
    complete: () => console.log('[from array] complete'),
  });
  sub.unsubscribe();
}

// --- 3. from(Promise) — one-shot Observable from a Promise ---
//   Caveat: from(promise) does NOT make the Promise lazy.
//   The Promise may already be running at the time from() is called.
//   Use defer() when each subscription must create a fresh execution.

function demoFromPromise(): void {
  const user$ = from(Promise.resolve({ id: '1', name: 'Alice' }));
  const sub = user$.subscribe({
    next: user => console.log('[from promise] user:', user),
    complete: () => console.log('[from promise] complete'),
  });
  setTimeout(() => sub.unsubscribe(), 100);
}

// --- 4. defer — lazy source creation at subscription time ---
//   Unlike from(promise), defer calls its factory on every subscribe,
//   so each subscriber gets a fresh Promise / source.

function demoDefer(): void {
  console.log('[defer] Observable defined — no Promise created yet');

  const deferred$ = defer(() => {
    console.log('[defer] factory called — Promise created on subscribe');
    return from(Promise.resolve({ id: '99', name: 'Lazy User' }));
  });

  console.log('[defer] About to subscribe...');
  const sub = deferred$.subscribe({
    next: user => console.log('[defer] user:', user),
    complete: () => console.log('[defer] complete'),
  });
  setTimeout(() => sub.unsubscribe(), 100);
}

// --- 5. fromEvent — DOM / EventTarget source (browser-only) ---
//   On subscribe: adds the event listener.
//   On unsubscribe: removes the event listener.
//   Exported for illustrative use — NOT called at module level (requires browser).

export function demoFromEvent(): void {
  if (typeof document !== 'undefined') {
    const clicks$ = fromEvent<MouseEvent>(document, 'click');
    const sub = clicks$.subscribe(ev =>
      console.log('[fromEvent] click at x:', ev.clientX, 'y:', ev.clientY),
    );
    setTimeout(() => sub.unsubscribe(), 5000);
  }
}

// --- 6. fromEventPattern — for non-EventTarget sources (Node.js EventEmitter) ---
//   Node.js only. First function adds listener on subscribe;
//   second removes it on unsubscribe. Called at the bottom because
//   tests run in Node where EventEmitter is available.

function demoFromEventPattern(): void {
  const emitter = new EventEmitter();

  const messages$ = fromEventPattern<string>(
    handler => emitter.on('message', handler),
    handler => emitter.off('message', handler),
  );

  const sub = messages$.subscribe(msg =>
    console.log('[fromEventPattern] message:', msg),
  );

  emitter.emit('message', 'hello from EventEmitter');
  emitter.emit('message', 'second message');

  sub.unsubscribe(); // triggers the remove-listener function (second fromEventPattern arg)
}

// --- 7. interval — repeated time ticks (never completes without a limiter) ---

function demoInterval(): void {
  const sub = interval(1000).pipe(take(3)).subscribe({
    next: v => console.log('[interval] tick:', v),
    complete: () => console.log('[interval] complete after 3 ticks'),
  });
  setTimeout(() => sub.unsubscribe(), 4000); // defensive — take(3) completes at ~3 s
}

// --- 8. timer — single delayed emission then complete ---
//   timer(2000) emits 0 once after 2 000 ms, then completes automatically.

function demoTimer(): void {
  const sub = timer(2000).subscribe({
    next: v => console.log('[timer] fired, value:', v),
    complete: () => console.log('[timer] complete (single-shot)'),
  });
  setTimeout(() => sub.unsubscribe(), 3000); // defensive after auto-complete at ~2 s
}

// --- 9. EMPTY with catchError — swallow an error and complete cleanly ---
//   EMPTY marble: |
//   Returning EMPTY from catchError silences the error and ends the stream cleanly.
//   Also usable in iif() to disable a branch entirely.

function demoEmptyCatchError(): void {
  const sub = throwError(() => new Error('transient error')).pipe(
    catchError(_err => EMPTY),
  ).subscribe({
    complete: () => console.log('[EMPTY catchError] swallowed error, completed cleanly'),
  });
  sub.unsubscribe();
}

// --- 10. NEVER — an Observable that never emits and never completes ---
//   NEVER marble: ----------
//
//   Use case: suppress a polling interval when a feature flag is off.
//   Example:
//     const poll$ = featureEnabled ? interval(5_000) : NEVER;

function demoNever(): void {
  const sub = NEVER.subscribe({
    complete: () => console.log('[NEVER] (this never fires)'),
  });
  sub.unsubscribe(); // NEVER does not self-terminate; explicit cleanup needed
  console.log('[NEVER] subscribed and cleaned up — no emissions expected');
}

// --- 11. throwError with catchError — propagate and recover from a domain error ---
//   throwError marble: #
//   Use throwError inside switchMap / mergeMap to signal a domain error as an
//   Observable error, or in tests to simulate a failing source.

function demoThrowError(): void {
  const sub = throwError(() => new Error('something went wrong')).pipe(
    catchError(err => {
      console.error('[throwError] caught:', (err as Error).message);
      return EMPTY; // recover by completing the stream cleanly
    }),
  ).subscribe({
    complete: () => console.log('[throwError] recovered with EMPTY'),
  });
  sub.unsubscribe();
}

// --- 12. HTTP boundary: defer + fetch + Zod runtime validation ---
//   Exported for illustrative use — NOT called at module level
//   because it requires a real network and a running server.
//
//   TypeScript protects the inside of the application.
//   Zod's schema.parse() protects the boundary at runtime.

export function demoHttpBoundary(): void {
  if (typeof fetch !== 'undefined') {
    const UserDtoSchema = z.object({
      id: z.string(),
      name: z.string(),
    });

    type UserDto = z.infer<typeof UserDtoSchema>;

    const loadUser$ = defer(() =>
      from(fetch('/api/user')),
    ).pipe(
      mergeMap(response => from(response.json() as Promise<unknown>)),
      // schema.parse() throws ZodError if the runtime shape doesn't match.
      map(value => UserDtoSchema.parse(value)),
      catchError(err => {
        console.error('[http boundary] error:', err);
        return EMPTY;
      }),
    );

    const sub = loadUser$.subscribe({
      next: (user: UserDto) => console.log('[http boundary] user:', user),
      complete: () => console.log('[http boundary] done'),
    });

    setTimeout(() => sub.unsubscribe(), 5000);
  }
}

demoOf();
demoFromArray();
demoFromPromise();
demoDefer();
// demoFromEvent()     — browser-only; exported above for illustrative use
demoFromEventPattern();
demoInterval();
demoTimer();
demoEmptyCatchError();
demoNever();
demoThrowError();
// demoHttpBoundary()  — requires real network; exported above for illustrative use
