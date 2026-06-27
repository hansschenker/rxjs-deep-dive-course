// Module 2 — The Observable Contract | Companion code for RxJS Deep Dive

/*
 * OBSERVABLE GRAMMAR
 *
 *   next* (complete | error)?
 *
 * Rules:
 *   • zero or more next(value) notifications
 *   • then either complete() or error(err)
 *   • after complete or error, no further notifications may flow
 *
 * Valid streams:
 *   --1--2--3--|       values then complete
 *   --1--2--#          values then error
 *   --|                complete without values
 *   --#                error without values
 *   --------1---2---3--------...  infinite (no terminal event)
 *
 * Invalid stream:
 *   --1--|--2          value AFTER complete — the contract is broken
 */

import { Observable, Subject, of, interval, mergeMap, take, takeUntil } from 'rxjs';

// --- 1. Observable Definition ---
//   new Observable wraps a lazy producer function.
//   Nothing runs until .subscribe() is called.

function demoObservableDefinition(): void {
  const source$ = new Observable<number>(subscriber => {
    console.log('[definition] producer starts');

    subscriber.next(1);
    subscriber.next(2);
    subscriber.next(3);
    subscriber.complete();

    // Teardown: returned function runs on completion, error, or unsubscribe.
    return () => {
      console.log('[definition] teardown runs');
    };
  });

  const sub = source$.subscribe({
    next: v => console.log('[definition] next:', v),
    error: err => console.error('[definition] error:', err),
    complete: () => console.log('[definition] complete'),
  });

  sub.unsubscribe();
}

// --- 2. Teardown with setInterval / clearInterval ---
//   The subscription is stored and manually unsubscribed after ~3 ticks (3.5 s).
//   The teardown function clears the interval so the producer truly stops.

function demoTeardown(): void {
  let tickCount = 0;

  const timer$ = new Observable<number>(subscriber => {
    const id = setInterval(() => {
      tickCount += 1;
      subscriber.next(tickCount);
    }, 1000);

    return () => {
      clearInterval(id);
      console.log('[teardown] interval cleared');
    };
  });

  const sub = timer$.subscribe({
    next: v => console.log('[teardown] tick:', v),
  });

  // Unsubscribing after 3 ticks fires the teardown / clearInterval.
  setTimeout(() => sub.unsubscribe(), 3500);
}

// --- 3. Synchronous Emission ---
//   of(1,2,3) emits all three values synchronously within .subscribe().
//   'after' is logged LAST, proving no async gap.

function demoSynchronous(): void {
  console.log('[sync] before');
  // Expected order: before → 1 → 2 → 3 → after
  const sub = of(1, 2, 3).subscribe(v => console.log('[sync]', v));
  console.log('[sync] after');
  sub.unsubscribe();
}

// --- 4. Asynchronous Emission ---
//   interval uses the event loop; 'before' and 'after' appear before any tick.

function demoAsynchronous(): void {
  console.log('[async] before');
  // Expected order: before → after → 0 → 1 → 2
  const sub = interval(1000).pipe(take(3)).subscribe(
    v => console.log('[async] tick:', v),
  );
  console.log('[async] after');
  setTimeout(() => sub.unsubscribe(), 4000); // defensive after auto-complete at ~3 s
}

// --- 5. take(5) Auto-Complete ---
//   The stream terminates automatically after 5 values; no manual unsubscribe needed.

function demoTake(): void {
  const sub = interval(1000).pipe(take(5)).subscribe({
    next: v => console.log('[take] value:', v),
    complete: () => console.log('[take] auto-complete after 5 values'),
  });
  setTimeout(() => sub.unsubscribe(), 6000); // defensive — stream completes at ~5 s
}

// --- 6. takeUntil(destroy$) Lifetime ---
//   Ties the subscription lifetime to an explicit signal.
//   This is the standard Angular / teardown-lifecycle pattern.

function demoTakeUntil(): void {
  const destroy$ = new Subject<void>();

  const sub = interval(1000).pipe(takeUntil(destroy$)).subscribe({
    next: v => console.log('[takeUntil] tick:', v),
    complete: () => console.log('[takeUntil] stream ended via destroy$'),
  });

  setTimeout(() => {
    destroy$.next();
    destroy$.complete();
    sub.unsubscribe(); // already completed; defensive cleanup
  }, 3500);
}

// --- 7. Nested Subscription Anti-Pattern (shown as comment) ---
//   Correct mergeMap version is runnable below.

/*
 * ANTI-PATTERN — subscribing inside a subscribe (DO NOT do this):
 *
 *   source$.subscribe(value => {
 *     inner$(value).subscribe(result => {
 *       console.log(result);    // inner subscription is unmanaged!
 *     });
 *   });
 *
 * Problems:
 *   • inner subscription is NOT managed by the outer teardown
 *   • canceling source$ does NOT cancel inner$
 *   • each source emission creates a new, untracked subscription
 *
 * Correct pattern — let a flattening operator manage inner subscriptions:
 */
function demoMergeMap(): void {
  const source$ = of(1, 2, 3);
  const inner$ = (value: number) => of(`response for ${value}`);

  // mergeMap subscribes to inner$ and forwards its values downstream.
  // Canceling the outer subscription also cancels any active inner subscriptions.
  const sub = source$.pipe(
    mergeMap(value => inner$(value)),
  ).subscribe(result => console.log('[mergeMap] result:', result));

  sub.unsubscribe();
}

demoObservableDefinition();
demoTeardown();
demoSynchronous();
demoAsynchronous();
demoTake();
demoTakeUntil();
demoMergeMap();
