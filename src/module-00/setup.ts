// Module 0 — Course Setup and Mental Model | Companion code for RxJS Deep Dive

/*
 * COURSE VOCABULARY
 *
 *   Observable   = lazy dataflow over time
 *   Observer     = consumer of notifications
 *   Subscription = running connection between source and observer
 *   Operator     = stream rewiring function
 *   Scheduler    = time/execution coordination policy
 *   Subject      = hot multicast bridge
 *   State        = stream that remembers
 */

/*
 * MARBLE NOTATION REFERENCE
 *
 *   -   one frame of virtual time
 *   a   a next value named 'a'
 *   |   complete notification
 *   #   error notification
 *   ^   subscription point  (used in cold / hot marble diagrams)
 *   !   unsubscription point
 *
 * Examples:
 *   --a--b--c--|       emit a, b, c then complete
 *   --a--#             emit a then error
 *   ------|             complete without values
 *   --a-----------     emit a and continue forever (infinite stream)
 *
 * Formal notation (time-indexed notification sequences):
 *   [{ T, n(a) } ... { T, c }]   —  next values then complete
 *   [{ T, n(a) } ... { T, e }]   —  next values then error
 *
 *   T    = time coordinate
 *   n(a) = next notification carrying value a
 *   c    = complete notification
 *   e    = error notification
 */

/*
 * THE FOUR QUESTIONS FOR EVERY RXJS PIPELINE
 *
 *   1. What flows over time?
 *   2. What happens when a new value arrives?
 *   3. Which operator policy is being used?
 *   4. What happens to cancellation, sharing, errors, and completion?
 */

import { Observable, Subject, of, debounceTime, distinctUntilChanged, switchMap } from 'rxjs';

// --- 1. SearchResult Interface and Mock Search API ---

interface SearchResult {
  id: string;
  title: string;
  snippet: string;
}

function searchApi(term: string): Observable<SearchResult[]> {
  // In production this calls an HTTP endpoint; here we return a stub.
  return of([
    { id: '1', title: `Result for "${term}"`, snippet: `Snippet about ${term}` },
  ]);
}

// --- 2. The searchResults$ Behavior Story Pipeline ---

/*
 * Behavior story:
 *   searchText$ emits text values over time.
 *   debounceTime waits until typing becomes quiet (300 ms of silence).
 *   distinctUntilChanged ignores repeated text (same value twice in a row).
 *   switchMap starts a new request for the latest text and cancels the previous.
 *   Only the latest request result is allowed to flow downstream.
 */
function demoBehaviorStory(): void {
  const searchText$ = new Subject<string>();

  const searchResults$ = searchText$.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    switchMap(term => searchApi(term)),
  );

  const sub = searchResults$.subscribe({
    next: results => console.log('[search] results:', results),
  });

  // Simulate a burst of keystrokes — only the final value survives debounce.
  searchText$.next('rx');
  searchText$.next('rxj');
  searchText$.next('rxjs');

  setTimeout(() => {
    sub.unsubscribe();
    searchText$.complete();
  }, 500);
}

// --- 3. Laziness Demonstration ---
//   Nothing inside the Observable producer runs until .subscribe() is called.

function demoLaziness(): void {
  console.log('[laziness] Observable defined — producer has NOT started');

  const lazy$ = new Observable<number>(subscriber => {
    console.log('[laziness] Producer starts — triggered by .subscribe()');
    subscriber.next(1);
    subscriber.next(2);
    subscriber.complete();

    // Teardown: runs on completion, error, or explicit unsubscribe.
    return () => {
      console.log('[laziness] Teardown runs after completion');
    };
  });

  console.log('[laziness] Still no producer — subscribing now...');
  const sub = lazy$.subscribe({
    next: v => console.log('[laziness] next:', v),
    complete: () => console.log('[laziness] complete'),
  });
  sub.unsubscribe();
}

demoBehaviorStory();
demoLaziness();
