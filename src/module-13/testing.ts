// Module 13 — Testing with Virtual Time: runnable demo
// See testing.test.ts for TestScheduler marble tests

// Re-export the operators that testing.test.ts will exercise with marble tests.
// These are the production implementations — the tests run them under virtual time.
export { validSearchText, retryWithBackoff } from '../module-12/custom-operators';

import { scan, map } from 'rxjs';
import type { OperatorFunction } from 'rxjs';

// --- runningAverage: standalone copy for direct marble testing ---
// Defined here independently of module-12 so testing.test.ts can import it
// without pulling in all of module-12's dependencies.
export function runningAverage(): OperatorFunction<number, number> {
  return source$ =>
    source$.pipe(
      scan(
        (acc, value) => ({ sum: acc.sum + value, count: acc.count + 1 }),
        { sum: 0, count: 0 }
      ),
      map(({ sum, count }) => sum / count)
    );
}

/*
 * Marble syntax reference (TestScheduler.run mode):
 *
 *   -        one frame of virtual time (1 ms)
 *   a        next value named 'a' — actual value passed in the values map
 *   |        complete notification
 *   #        error notification
 *   ^        subscription point (hot observables only)
 *   !        unsubscription point (used in expectSubscriptions)
 *   300ms    explicit time advance of 300 frames
 *
 * cold('--a--b--|')     timeline starts at subscription; each subscriber gets frame 0
 * hot('--^--a--|')      fixed timeline; ^ marks subscription; pre-^ values not seen
 *
 * Example test skeleton:
 *
 *   import { TestScheduler } from 'rxjs/testing';
 *   import { validSearchText } from './testing';
 *
 *   describe('validSearchText', () => {
 *     let scheduler: TestScheduler;
 *
 *     beforeEach(() => {
 *       scheduler = new TestScheduler((actual, expected) => {
 *         expect(actual).toEqual(expected);
 *       });
 *     });
 *
 *     it('suppresses short values', () => {
 *       scheduler.run(({ cold, expectObservable }) => {
 *         const source$ = cold('a-b-c-|', { a: 'hi', b: 'hey', c: 'hello' });
 *         const result$ = source$.pipe(validSearchText(4, 0));
 *         expectObservable(result$).toBe('----c-|', { c: 'hello' });
 *       });
 *     });
 *   });
 */
