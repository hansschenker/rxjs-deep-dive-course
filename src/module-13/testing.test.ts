import { beforeEach, describe, expect, it } from 'vitest';
import { TestScheduler } from 'rxjs/testing';
import {
  catchError,
  concatMap,
  debounceTime,
  defer,
  exhaustMap,
  of,
  retry,
  scan,
  startWith,
  switchMap,
} from 'rxjs';

describe('Module 13 – Marble Tests', () => {
  let scheduler: TestScheduler;

  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });
  });

  it('debounceTime – emits after silence', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a 100ms b 100ms c 300ms |');
      const result$ = source$.pipe(debounceTime(250));
      expectObservable(result$).toBe('350ms c 50ms |');
    });
  });

  it('switchMap – cancels previous inner on new outer value', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a---b----|');
      const inner$ = cold('----x|');
      const result$ = source$.pipe(switchMap(() => inner$));
      expectObservable(result$).toBe('--------x|');
    });
  });

  it('concatMap – queues second inner until first completes', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a-b-|');
      const inner$ = cold('--x|');
      const result$ = source$.pipe(concatMap(() => inner$));
      expectObservable(result$).toBe('--x--x|');
    });
  });

  it('exhaustMap – ignores second value while first inner active', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a-b---|');
      const inner$ = cold('----x|');
      const result$ = source$.pipe(exhaustMap(() => inner$));
      expectObservable(result$).toBe('----x-|');
    });
  });

  it('catchError – recovery', () => {
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('--#', {}, new Error('network'));
      const result$ = source$.pipe(catchError(() => of('fallback')));
      expectObservable(result$).toBe('--(a|)', { a: 'fallback' });
    });
  });

  it('retry with defer', () => {
    scheduler.run(({ cold, expectObservable }) => {
      let attempt = 0;
      const source$ = defer(() => {
        attempt++;
        return attempt < 3
          ? cold('--#', {}, new Error('fail'))
          : cold('--a|', { a: 'ok' });
      }).pipe(retry(2));
      expectObservable(source$).toBe('------a|', { a: 'ok' });
    });
  });

  it('scan + startWith – produces correct state sequence', () => {
    scheduler.run(({ hot, expectObservable }) => {
      type CountAction = { type: 'Increment' } | { type: 'Decrement' };
      type CountState = { count: number };
      const actions$ = hot('-a-b-c|', {
        a: { type: 'Increment' } as CountAction,
        b: { type: 'Increment' } as CountAction,
        c: { type: 'Decrement' } as CountAction,
      });
      const state$ = actions$.pipe(
        scan((state: CountState, action: CountAction): CountState => {
          switch (action.type) {
            case 'Increment': return { count: state.count + 1 };
            case 'Decrement': return { count: state.count - 1 };
          }
        }, { count: 0 }),
        startWith({ count: 0 }),
      );
      expectObservable(state$).toBe('x-y-z-w|', {
        x: { count: 0 },
        y: { count: 1 },
        z: { count: 2 },
        w: { count: 1 },
      });
    });
  });
});
