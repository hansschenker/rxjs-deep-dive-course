// Module 13 — Testing with Virtual Time | Companion code for RxJS Deep Dive
// See testing.test.ts for TestScheduler marble tests.
// This file re-expresses each tested pattern as plain, runnable code.

/*
 * Marble syntax reference (TestScheduler.run mode)
 * ─────────────────────────────────────────────────
 *   -        one frame of virtual time (1 ms in scheduler.run)
 *   a        next value named 'a' — actual value supplied in the values map
 *   |        complete notification
 *   #        error notification
 *   ^        subscription point (hot observables only)
 *   !        unsubscription point (used with expectSubscriptions)
 *   300ms    explicit time advance of 300 virtual frames
 *
 * Spaces inside marble strings are ignored — use them for alignment:
 *   cold('  a---b---|')
 *   hot( '--^-a-b-|')
 */

/*
 * Cold vs Hot
 * ──────────────────────────────────────────────────────────────────────────
 * cold(marbles, values?)  Timeline starts AT SUBSCRIPTION.
 *                          Each subscriber gets its own independent execution
 *                          beginning at frame 0.
 *                          Use for: HTTP requests, file reads — each sub is a
 *                          fresh fetch.
 *
 * hot(marbles, values?)   Timeline is FIXED from the start of the test.
 *                          The ^ character marks where subscription happens.
 *                          Emissions BEFORE ^ are not seen by the subscriber.
 *                          Use for: user events, WebSocket messages, state streams.
 *
 * Example:
 *   cold('--a--b--|')  →  subscriber sees: --a--b--|  (from subscription point)
 *   hot('a--^--b--|')  →  subscriber sees:    --b--|  (^ is the sub point)
 */

/*
 * expectSubscriptions — purpose
 * ──────────────────────────────────────────────────────────────────────────
 * Verifies WHEN subscriptions and unsubscriptions actually happen.
 * Essential for confirming that switchMap truly cancels the inner stream:
 *
 *   scheduler.run(({ cold, hot, expectObservable, expectSubscriptions }) => {
 *     const source$ = hot('^-a---b----|');
 *     const inner$ =      cold('----x|');
 *     const result$ = source$.pipe(switchMap(() => inner$));
 *
 *     const innerSubs = [
 *       '^---!',         // first inner: subscribed frame 2, unsubscribed frame 6
 *       '------^----!'   // second inner: subscribed frame 6, completes
 *     ];
 *
 *     expectObservable(result$).toBe('----------x|');
 *     expectSubscriptions(inner$.subscriptions).toBe(innerSubs);
 *   });
 *
 * ^  in a subscription marble = subscribed
 * !  in a subscription marble = unsubscribed
 * A subscription with no ! runs to natural completion.
 */

/*
 * TestScheduler setup skeleton (requires Vitest / Jest describe context):
 *
 *   import { TestScheduler } from 'rxjs/testing';
 *
 *   describe('my operator', () => {
 *     let scheduler: TestScheduler;
 *
 *     beforeEach(() => {
 *       scheduler = new TestScheduler((actual, expected) => {
 *         expect(actual).toEqual(expected);
 *       });
 *     });
 *
 *     it('example', () => {
 *       scheduler.run(({ cold, hot, expectObservable, expectSubscriptions }) => {
 *         const source$ = cold('--a--b--|', { a: 'hello', b: 'world' });
 *         const result$ = source$.pipe(myOperator());
 *         expectObservable(result$).toBe('--A--B--|', { A: 'HELLO', B: 'WORLD' });
 *       });
 *     });
 *   });
 */

import {
  Subject,
  of,
  throwError,
  defer,
  timer,
  map,
  debounceTime,
  switchMap,
  concatMap,
  exhaustMap,
  catchError,
  scan,
  startWith,
  retry,
} from 'rxjs';

// Re-export the operators that testing.test.ts exercises with marble tests.
// These are the production implementations; the tests run them under virtual time.
export { validSearchText, retryWithBackoff } from '../module-12/custom-operators';

// ─── Section 1: debounceTime pattern ─────────────────────────────────────────
// Marble equivalent:  source = 'a 100ms b 100ms c 300ms |'
//                     result = '452ms c 50ms |'   (250ms debounce)

function demonstrateDebounceTime(): void {
  const input$ = new Subject<string>();
  const debounced$ = input$.pipe(debounceTime(250));

  debounced$.subscribe(value => console.log('[debounceTime] emitted:', value));

  // Rapid keystrokes inside the 250 ms window
  input$.next('r');
  input$.next('rx');
  input$.next('rxj');
  // None of the above emit yet — the 250 ms timer resets on each keystroke.

  // 'rxjs' arrives after 300 ms of silence → passes debounce
  setTimeout(() => input$.next('rxjs'), 300);
  setTimeout(() => input$.complete(), 600);
}

// ─── Section 2: switchMap cancellation ───────────────────────────────────────
// Marble equivalent:  source = 'a---b----|'
//                     inner  = '----x|'
//                     result = '--------x|'

function demonstrateSwitchMapCancellation(): void {
  const trigger$ = new Subject<string>();

  const result$ = trigger$.pipe(
    switchMap(term =>
      // Simulates a 200 ms async operation (e.g. HTTP request)
      timer(200).pipe(map(() => `result for: ${term}`)),
    ),
  );

  result$.subscribe(value => console.log('[switchMap]', value));

  trigger$.next('a');                        // starts inner for 'a'
  setTimeout(() => trigger$.next('b'), 100); // 'a' cancelled; inner for 'b' starts
  // Only 'result for: b' prints at ~300 ms
  setTimeout(() => trigger$.complete(), 500);
}

// ─── Section 3: concatMap queuing ────────────────────────────────────────────
// Marble equivalent:  source = 'a-b-|'
//                     inner  = '--x|'
//                     result = '--x--x|'
// 'b' waits until 'a's inner completes before starting.

function demonstrateConcatMapQueuing(): void {
  const trigger$ = new Subject<string>();

  const result$ = trigger$.pipe(
    concatMap(label =>
      timer(100).pipe(map(() => `${label} done`)),
    ),
  );

  result$.subscribe(value => console.log('[concatMap]', value));

  trigger$.next('first');
  trigger$.next('second'); // queued — starts only after 'first' inner finishes
  // Output (in order): 'first done', then 'second done'
  setTimeout(() => trigger$.complete(), 400);
}

// ─── Section 4: exhaustMap ignore policy ─────────────────────────────────────
// Marble equivalent:  source = 'a-b---|'
//                     inner  = '----x|'
//                     result = '----x-|'
// 'b' is dropped because 'a's inner is still active.

function demonstrateExhaustMapIgnoring(): void {
  const trigger$ = new Subject<string>();

  const result$ = trigger$.pipe(
    exhaustMap(label =>
      timer(150).pipe(map(() => `${label} done`)),
    ),
  );

  result$.subscribe(value => console.log('[exhaustMap]', value));

  trigger$.next('first');
  trigger$.next('second'); // dropped — first inner still active
  // Output: 'first done' only
  setTimeout(() => trigger$.complete(), 400);
}

// ─── Section 5: catchError recovery ──────────────────────────────────────────
// Marble equivalent:  source = '--#'  (error at frame 2)
//                     result = '--(a|)'  ('fallback' synchronously after error)

function demonstrateCatchErrorRecovery(): void {
  throwError(() => new Error('network'))
    .pipe(catchError(() => of('fallback')))
    .subscribe(value => console.log('[catchError]', value));
  // Output: fallback
}

// ─── Section 6: retry with defer counter ─────────────────────────────────────
// Marble equivalent:  result = '------a|'
// defer re-subscribes on each retry; attempt < 3 errors; attempt 3 succeeds.

function demonstrateRetryWithDefer(): void {
  let attempt = 0;

  const source$ = defer(() => {
    attempt++;
    console.log('[retry] attempt', attempt);
    return attempt < 3
      ? throwError(() => new Error('fail'))
      : of('ok');
  }).pipe(retry(2));

  source$.subscribe({
    next:  value => console.log('[retry] success:', value),
    error: err   => console.log('[retry] final error:', String(err)),
  });
  // Output: attempt 1, attempt 2, attempt 3, success: ok
}

// ─── Section 7: scan + startWith state ───────────────────────────────────────
// The CountAction / CountState pattern from the marble test file,
// expressed as plain runnable code with a Subject dispatcher.

type CountAction = { type: 'Increment' } | { type: 'Decrement' };
type CountState  = { count: number };

function demonstrateScanState(): void {
  const actions$ = new Subject<CountAction>();

  const state$ = actions$.pipe(
    scan((state: CountState, action: CountAction): CountState => {
      switch (action.type) {
        case 'Increment': return { count: state.count + 1 };
        case 'Decrement': return { count: state.count - 1 };
      }
    }, { count: 0 }),
    startWith<CountState>({ count: 0 }),
  );

  state$.subscribe(state => console.log('[scan+startWith] count:', state.count));
  // Synchronous seed: count 0
  actions$.next({ type: 'Increment' }); // count 1
  actions$.next({ type: 'Increment' }); // count 2
  actions$.next({ type: 'Decrement' }); // count 1
  actions$.complete();
}

// ─── Run all demonstrations ───────────────────────────────────────────────────

demonstrateDebounceTime();
demonstrateSwitchMapCancellation();
demonstrateConcatMapQueuing();
demonstrateExhaustMapIgnoring();
demonstrateCatchErrorRecovery();
demonstrateRetryWithDefer();
demonstrateScanState();
