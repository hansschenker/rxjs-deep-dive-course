// Module 6 — Time, Rate Limiting, and Schedulers | Companion code for RxJS Deep Dive

/*
 * Time Sources
 *
 * RxJS operators do not create time by magic. Time comes from:
 *   - DOM events
 *   - timers
 *   - animation frames
 *   - WebSockets
 *   - HTTP responses
 *   - schedulers
 *   - external producers
 *
 * A scheduler controls when work is scheduled.
 * It does not make JavaScript multithreaded.
 */

import {
  Subject, of,
  debounceTime, throttleTime, auditTime, sampleTime, sample,
  bufferTime, windowTime, mergeAll,
  observeOn, subscribeOn,
  asyncScheduler, animationFrameScheduler
} from 'rxjs';

// ─── Section 2: Rate-Limiting Operators ───────────────────────────────────────

// debounceTime — wait for silence, emit latest
/*
 * debounceTime(300)
 *
 * Policy: wait for silence, then emit the latest value
 * Each new value resets the timer; only the final value after a quiet window passes.
 * Use for: typeahead search input
 */

function demonstrateDebounceTime(): void {
  const input$ = new Subject<string>();
  const sub = input$.pipe(debounceTime(300)).subscribe(
    v => console.log('[debounceTime]', v)
  );
  // Rapid burst — only the last value emits after 300 ms of silence
  input$.next('r');
  input$.next('rx');
  input$.next('rxj');
  input$.next('rxjs');
  // After 300 ms quiet window: '[debounceTime] rxjs'
  sub.unsubscribe();
  input$.complete();
}

// throttleTime — emit first, then ignore for a window
/*
 * throttleTime(1000)
 *
 * Policy: emit first, then suppress for the window duration
 * Use for: double-click protection, high-frequency buttons
 */

function demonstrateThrottleTime(): void {
  const clicks$ = new Subject<void>();
  const sub = clicks$.pipe(throttleTime(1000)).subscribe(
    () => console.log('[throttleTime] click passed through')
  );
  clicks$.next(); // passes through — starts the suppression window
  clicks$.next(); // suppressed (within 1000 ms window)
  clicks$.next(); // suppressed
  sub.unsubscribe();
  clicks$.complete();
}

// auditTime — open window, emit latest when it closes
/*
 * auditTime(100)
 *
 * Policy: open a time window, then emit the latest value when the window closes
 * Use for: resize events, scroll handlers, rendering loops
 */

function demonstrateAuditTime(): void {
  const resize$ = new Subject<{ width: number }>();
  const sub = resize$.pipe(auditTime(100)).subscribe(
    v => console.log('[auditTime]', v)
  );
  resize$.next({ width: 800 });
  resize$.next({ width: 900 });
  resize$.next({ width: 1024 }); // latest — emitted after the 100 ms window closes
  sub.unsubscribe();
  resize$.complete();
}

// sampleTime — periodic sample
/*
 * sampleTime(1000)
 *
 * Policy: at fixed intervals, emit the latest value if one exists
 * Use for: dashboards, telemetry, periodic snapshots
 */

function demonstrateSampleTime(): void {
  const telemetry$ = new Subject<number>();
  const sub = telemetry$.pipe(sampleTime(1000)).subscribe(
    v => console.log('[sampleTime]', v)
  );
  telemetry$.next(42);
  telemetry$.next(43);
  // After 1000 ms interval: '[sampleTime] 43'
  sub.unsubscribe();
  telemetry$.complete();
}

// sample — event-triggered snapshot
/*
 * sample(notifier$)
 *
 * Policy: whenever the notifier emits, emit the latest source value if one exists
 * sample is event-triggered; sampleTime is interval-triggered.
 * Use when the sampling trigger is itself an Observable (animation frame, button click).
 */

function demonstrateSample(): void {
  const source$   = new Subject<number>();
  const notifier$ = new Subject<void>();
  const sub = source$.pipe(sample(notifier$)).subscribe(
    v => console.log('[sample]', v)
  );
  source$.next(1);
  source$.next(2);
  source$.next(3);
  notifier$.next(); // emits 3 — latest value at the moment of signal
  source$.next(4);
  notifier$.next(); // emits 4
  sub.unsubscribe();
  source$.complete();
  notifier$.complete();
}

// ─── Section 3: Batching Operators ───────────────────────────────────────────

// bufferTime — collect into arrays at fixed intervals
/*
 * bufferTime(500)
 *
 * Policy: collect source values into an array, emit the array at fixed intervals
 * Every 500 ms, emit an array of all values received in that window.
 * If no values arrived, emit an empty array.
 * Use for: batching analytics events, log shipping, bulk API calls
 */

function demonstrateBufferTime(): void {
  const source$ = new Subject<number>();
  const sub = source$.pipe(bufferTime(500)).subscribe(
    batch => console.log('[bufferTime] batch', batch)
  );
  source$.next(1);
  source$.next(2);
  source$.next(3);
  // After 500 ms: '[bufferTime] batch [1, 2, 3]'
  sub.unsubscribe();
  source$.complete();
}

// windowTime — Observable windows
/*
 * windowTime(500).pipe(mergeAll())
 *
 * Policy: like bufferTime, but emits an Observable window instead of an array.
 * Use bufferTime when you need an array.
 * Use windowTime when you need to apply operators to each window as a stream.
 */

function demonstrateWindowTime(): void {
  const source$ = new Subject<number>();
  const sub = source$.pipe(
    windowTime(500),
    mergeAll()
  ).subscribe(v => console.log('[windowTime]', v));
  source$.next(10);
  source$.next(20);
  sub.unsubscribe();
  source$.complete();
}

/*
 * Time Policy Table
 *
 * Operator       | Policy              | Emits                | Typical use
 * ---------------|---------------------|----------------------|---------------------
 * debounceTime   | wait for silence    | latest after quiet   | search input
 * throttleTime   | first then suppress | first                | button protection
 * auditTime      | wait then latest    | latest after window  | resize / render
 * sampleTime     | periodic sample     | latest at interval   | dashboard
 * sample         | event-triggered     | latest at signal     | frame / click snapshot
 * bufferTime     | collect then emit   | array of values      | batch processing
 */

// ─── Section 5: Schedulers ────────────────────────────────────────────────────

/*
 * Scheduler Table
 *
 * Scheduler                  | When work runs
 * ---------------------------|------------------------------------------------
 * queueScheduler             | synchronous queue; useful for recursive scheduling
 * asapScheduler              | microtask queue (like Promise.resolve)
 * asyncScheduler             | macrotask queue (like setTimeout) — default for interval
 * animationFrameScheduler    | browser requestAnimationFrame
 *
 * Practical rule:
 *   Most applications never need to set schedulers explicitly.
 *   Schedulers are usually implicit — interval uses asyncScheduler internally.
 *
 *   The primary reason to understand schedulers is testing:
 *   VirtualTimeScheduler (Module 14) replaces real time with simulated time,
 *   making time-based operators deterministic and instant in tests.
 */

// observeOn — explicit scheduler for value delivery
function demonstrateObserveOn(): void {
  // Deliver values on the macrotask queue (like setTimeout)
  const sub = of({ count: 42 }).pipe(
    observeOn(asyncScheduler)
  ).subscribe(v => console.log('[observeOn asyncScheduler]', v));
  // Value arrives on the next macrotask tick
  sub.unsubscribe();
}

// ─── Section 6: subscribeOn vs observeOn ─────────────────────────────────────

/*
 * subscribeOn vs observeOn
 *
 * subscribeOn controls WHEN the source subscription starts.
 * observeOn  controls WHEN notifications are delivered downstream.
 *
 *   source$.pipe(
 *     subscribeOn(asyncScheduler),        // start subscribing on next macrotask
 *     observeOn(animationFrameScheduler)  // deliver values on animation frames
 *   );
 *
 * observeOn is the more commonly used of the two. It moves value delivery to a
 * different execution context without changing when the source starts.
 */

function demonstrateSubscribeOn(): void {
  // subscribeOn defers the start of the subscription to the asyncScheduler queue
  const sub = of(1, 2, 3).pipe(
    subscribeOn(asyncScheduler)
  ).subscribe(v => console.log('[subscribeOn]', v));
  sub.unsubscribe();
}

// Browser-only: deliver state changes on every animation frame
// (requestAnimationFrame does not exist in Node.js test environments)
if (typeof document !== 'undefined') {
  of({ frame: 0 }).pipe(
    observeOn(animationFrameScheduler)
  ).subscribe(v => console.log('[animationFrameScheduler]', v));
}

// ─── Run examples ─────────────────────────────────────────────────────────────

demonstrateDebounceTime();
demonstrateThrottleTime();
demonstrateAuditTime();
demonstrateSampleTime();
demonstrateSample();
demonstrateBufferTime();
demonstrateWindowTime();
demonstrateObserveOn();
demonstrateSubscribeOn();
