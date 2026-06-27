// Module 6 — Time, Rate Limiting, and Schedulers: runnable demo

import { interval, fromEvent } from 'rxjs';
import {
  debounceTime, throttleTime, auditTime, sampleTime, bufferTime,
  map, distinctUntilChanged
} from 'rxjs/operators';

// Stub input element for fromEvent examples
const inputElement = document.createElement('input');

// --- debounceTime: wait for silence, then emit the latest value ---
// Each new value resets the timer; only the final quiet value passes.
// Use for: typeahead search
export const searchText$ = fromEvent<InputEvent>(inputElement, 'input').pipe(
  map(event => (event.target as HTMLInputElement).value),
  debounceTime(300),
  distinctUntilChanged()
);

// --- throttleTime: emit first, suppress for the window duration ---
// Use for: double-click protection, high-frequency buttons
export const throttledTicks$ = interval(100).pipe(
  throttleTime(1000)
);

// --- auditTime: open a window, emit latest value when it closes ---
// Use for: resize events, scroll handlers, rendering loops
export const auditedTicks$ = interval(100).pipe(
  auditTime(500)
);

// --- sampleTime: at fixed intervals, emit the latest value if one exists ---
// Use for: dashboards, telemetry, periodic snapshots
export const sampledTicks$ = interval(100).pipe(
  sampleTime(1000)
);

// --- bufferTime: collect source values into arrays at fixed intervals ---
// Emits an empty array if no values arrived in the window.
// Use for: batching analytics events, log shipping, bulk API calls
export const bufferedTicks$ = interval(100).pipe(
  bufferTime(500)
);

/*
 * Schedulers — control when work is queued and executed
 * (JavaScript remains single-threaded; schedulers pick the execution slot):
 *
 *   queueScheduler           synchronous queue; useful for recursive scheduling
 *   asapScheduler            microtask queue (like Promise.resolve)
 *   asyncScheduler           macrotask queue (like setTimeout) — default for interval
 *   animationFrameScheduler  browser requestAnimationFrame
 *
 * Most applications never set schedulers explicitly.
 *
 * VirtualTimeScheduler (Module 13) replaces real time with simulated time,
 * making time-based operators deterministic and instant in tests.
 *
 * observeOn(animationFrameScheduler)  — deliver state on animation frames
 * subscribeOn(asyncScheduler)         — start subscribing on the next macrotask
 */
