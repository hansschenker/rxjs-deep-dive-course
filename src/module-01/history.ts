// Module 1 — History and Lineage: runnable demo

import { from, interval } from 'rxjs';
import { take } from 'rxjs/operators';

/*
 * Historical lineage:
 *   Functional Programming → Haskell → LINQ (Erik Meijer) →
 *   Rx.NET (2010) → ReactiveX → RxJS
 *
 * The deep idea: collection operators (map, filter, reduce, zip, flatten)
 * apply equally to values already in memory AND to values arriving over time.
 *
 * Iterator (pull): Consumer asks  "give me the next value"
 * Observer  (push): Producer says "here is the next value"
 */

// from(Promise) — converts an eager Promise into an Observable
export const helloAsync$ = from(Promise.resolve('hello'));

// interval + take — asynchronous counter, completes after 3 values
export const threeTimerTicks$ = interval(1000).pipe(take(3));

/*
 * async/await vs Observable comparison:
 *
 * Concern               | async/await         | Observable
 * ----------------------|---------------------|-----------------------------
 * Number of values      | exactly one         | zero, one, many, infinite
 * Eagerness             | eager (starts now)  | lazy (starts on subscribe)
 * Cancellation          | no built-in         | unsubscribe cancels
 * Composition operators | none                | full operator library
 * Time control          | no                  | debounce, throttle, delay
 * Multicast             | no                  | share, shareReplay
 *
 * They compose:
 *   from(promise)          converts a Promise  → Observable
 *   firstValueFrom(obs$)   converts an Observable → Promise
 */
