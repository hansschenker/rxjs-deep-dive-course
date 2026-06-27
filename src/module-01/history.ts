// Module 1 — History and Lineage | Companion code for RxJS Deep Dive

/*
 * COLLECTIONS VS STREAMS
 *
 * Array in memory (space-indexed, pull-based):
 *
 *   [10, 20, 30, 40]
 *
 * Observable over time (time-indexed, push-based):
 *
 *   ---10---20----------30---40---|
 *
 * With an array, the consumer asks for the next value (pull).
 * With an Observable, the producer pushes the next value when it's ready.
 */

/*
 * HISTORICAL LINEAGE
 *
 *   Functional Programming
 *           ↓
 *   Haskell / List Comprehensions
 *           ↓
 *   LINQ (Erik Meijer, Microsoft)
 *           ↓
 *   Rx.NET (Erik Meijer, 2010)
 *           ↓
 *   ReactiveX (cross-language standard)
 *           ↓
 *   RxJS
 *
 * Erik Meijer recognized that LINQ's query operators — map, filter, reduce, zip —
 * could be applied not just to values already in memory but to values arriving
 * over time.  Rx.NET was the first implementation; RxJS brought that model to JS.
 *
 * The same transformation operations work across both worlds:
 *   map · filter · reduce / scan · combine · flatten
 */

/*
 * ITERATOR / OBSERVER DUALITY
 *
 * Iterator pattern (pull-based — consumer drives):
 *
 *   Consumer ---- next() ----> Producer
 *   Consumer <--- value ------ Producer
 *
 * Observer pattern (push-based — producer drives):
 *
 *   Producer ---- next(value) ----> Consumer
 *
 * An Iterator says:     "Give me the next value."
 * An Observer receives: "Here is the next value."
 *
 * The Observable / Observer pair reverses the direction of control.
 * The producer decides when to push; the consumer just reacts.
 */

/*
 * async/await VS RxJS OBSERVABLE
 *
 * Concern               | async/await           | RxJS Observable
 * ----------------------|-----------------------|-----------------------------
 * Number of values      | exactly one           | zero, one, many, infinite
 * Eagerness             | eager (starts now)    | lazy (starts on subscribe)
 * Cancellation          | no built-in support   | unsubscribe cancels
 * Composition operators | none                  | full operator library
 * Time control          | no                    | debounce, throttle, delay
 * Multicast             | no                    | share, shareReplay
 *
 * They compose:
 *   from(promise)            converts a Promise      → Observable (one-shot)
 *   firstValueFrom(source$)  converts an Observable  → Promise   (first value)
 *
 * The two models are not in conflict — RxJS wraps where Promises fall short.
 */

import { from, firstValueFrom } from 'rxjs';

// --- 1. from(promise) — converts a Promise into a one-shot Observable ---

function demoFromPromise(): void {
  // from() wraps the already-started Promise; the Observable emits once then completes.
  const user$ = from(Promise.resolve({ id: '1', name: 'Alice' }));

  user$.subscribe({
    next: user => console.log('[from(promise)] user:', user),
    complete: () => console.log('[from(promise)] complete'),
  });
}

// --- 2. firstValueFrom — converts the first Observable emission into a Promise ---

async function demoFirstValueFrom(): Promise<void> {
  // Awaitable bridge back to imperative / async-await code.
  const user$ = from(Promise.resolve({ id: '2', name: 'Bob' }));
  const user = await firstValueFrom(user$);
  console.log('[firstValueFrom] user:', user);
}

demoFromPromise();
demoFirstValueFrom().catch(err => console.error('[firstValueFrom] error:', err));
