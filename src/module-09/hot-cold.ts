// Module 9 — Hot, Cold, and Shared Streams: runnable demo

import {
  Observable, BehaviorSubject, ReplaySubject, AsyncSubject, of
} from 'rxjs';
import { share, shareReplay } from 'rxjs/operators';

// --- Cold Observable: fresh producer per subscriber ---
// Each subscription runs an independent execution.
// Cold ≠ replay; cold means fresh execution.
export const cold$ = new Observable<number>(subscriber => {
  console.log('producer starts');
  subscriber.next(Math.random());
  subscriber.complete();
});

// --- BehaviorSubject: remembers current value ---
// Late subscribers immediately receive the current value.
// Use for simple local state where scan+shareReplay is overkill.
export const behavior$ = new BehaviorSubject<number>(0);

// Example: behavior$.next(behavior$.getValue() + 1)
//          behavior$.getValue()   // synchronous read

// --- ReplaySubject: remembers a configured history ---
// bufferSize=3: last 3 values are replayed to late subscribers.
export const replay$ = new ReplaySubject<number>(3);

// --- AsyncSubject: emits last value on completion ---
// Equivalent to a resolved Promise: replays the single final value.
// Use for memoizing a one-time expensive computation.
export const asyncSubject$ = new AsyncSubject<string>();

// --- share: shared execution while at least one subscriber exists ---
// No replay; late subscribers receive only future values.
// When all subscribers leave, the source is unsubscribed.
const source$ = of(1, 2, 3);
export const shared$ = source$.pipe(share());

// --- shareReplay: shared + replays buffered values to late subscribers ---
// refCount: true — unsubscribes from the source when nobody listens.
// This is the standard pattern for cached read models.
export const cachedUser$ = of({ id: '1', name: 'Alice' }).pipe(
  shareReplay({ bufferSize: 1, refCount: true })
);

/*
 * Subject variants summary:
 *   Subject         no memory
 *   BehaviorSubject remembers current value
 *   ReplaySubject   remembers configured history
 *   AsyncSubject    emits last value on completion
 *
 * When to use a Subject:
 *   1. Push values imperatively from outside an Observable chain
 *      (e.g. component dispatches actions via subject.next(action))
 *   2. Bridge a non-Observable system into RxJS when no creation operator fits
 *
 * Subject anti-pattern (event listener leaks):
 *   const clicks$ = new Subject<MouseEvent>();
 *   document.addEventListener('click', e => clicks$.next(e)); // WRONG — no teardown
 *
 * Correct:
 *   const clicks$ = fromEvent<MouseEvent>(document, 'click'); // teardown managed
 *
 * Rule: if fromEvent, fromEventPattern, interval, timer, or defer can model
 * the source, use them. Reach for Subject only when no creation operator fits.
 */
