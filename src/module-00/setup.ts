// Module 0 — Course Setup and Mental Model: runnable demo

/*
 * Marble notation constants (visual companion to [{T, n(a)}] notation):
 *
 *   -   one frame of virtual time
 *   a   a next value named 'a'
 *   |   complete notification
 *   #   error notification
 *
 * Examples:
 *   --a--b--c--|       emit a, b, c then complete
 *   --a--#             emit a then error
 *   ------|             complete without values
 *   --a-----------     emit a and continue forever (infinite stream)
 *
 * Course vocabulary:
 *   Observable  = lazy dataflow over time
 *   Observer    = consumer of notifications
 *   Subscription= running connection between source and observer
 *   Operator    = stream rewiring function
 *   Scheduler   = time/execution coordination policy
 *   Subject     = hot multicast bridge
 *   State       = stream that remembers
 */

import { of } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap } from 'rxjs/operators';
import type { Observable } from 'rxjs';

// Stub for the search API — in real usage, this would call an HTTP endpoint
function searchApi(term: string): Observable<string[]> {
  return of([`result for ${term}`]);
}

// Stub for searchText$ — in real usage this would be fromEvent on an <input>
const searchText$ = of('rxjs');

/*
 * Behavior story:
 *   searchText$ emits text values over time.
 *   debounceTime waits until typing becomes quiet.
 *   distinctUntilChanged ignores repeated text.
 *   switchMap starts a request for the latest text.
 *   If a newer text arrives, the previous request subscription is canceled.
 *   Only the latest request result is allowed to flow downstream.
 */
export const searchResults$ = searchText$.pipe(
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(term => searchApi(term))
);
