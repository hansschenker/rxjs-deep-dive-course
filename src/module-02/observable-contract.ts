// Module 2 — The Observable Contract: runnable demo

import { Observable, Subject, interval, of } from 'rxjs';
import { take, takeUntil } from 'rxjs/operators';

/*
 * Observable grammar: next* (complete | error)?
 *   Valid:    --1--2--3--|   --1--2--#   --|   --#
 *   Invalid:  --1--|--2      (values after complete are forbidden)
 *
 * Subscription starts the producer.
 * Teardown (returned function) stops it.
 */

// (1) Observable with explicit setInterval teardown
export const timer$ = new Observable<number>(subscriber => {
  const id = setInterval(() => {
    subscriber.next(Date.now());
  }, 1000);

  return () => {
    clearInterval(id);
  };
});

// (2) Synchronous source — of(1,2,3) emits all values before 'after' logs
export const syncSource$ = of(1, 2, 3);

// (3) Managed subscription — take(5) auto-completes; no manual unsubscribe needed
export const managedSub$ = interval(1000).pipe(take(5));

// (4) takeUntil lifecycle pattern — ties lifetime to a destroy signal
const destroy$ = new Subject<void>();

export const lifecycleSub$ = interval(1000).pipe(
  takeUntil(destroy$)
);

/** Call this to end the lifecycleSub$ stream (e.g. on component destroy). */
export function teardown(): void {
  destroy$.next();
  destroy$.complete();
}

/*
 * Nested subscription anti-pattern (DO NOT do this):
 *
 *   source$.subscribe(value => {
 *     inner$(value).subscribe(result => console.log(result)); // unmanaged!
 *   });
 *
 * Correct — let a flattening operator manage inner subscriptions:
 *
 *   source$.pipe(
 *     mergeMap(value => inner$(value))
 *   ).subscribe(result => console.log(result));
 *
 * Flattening operators are covered in Module 5.
 */
