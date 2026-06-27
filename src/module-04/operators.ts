// Module 4 — Operators as Behavior Stories | Companion code for RxJS Deep Dive

/*
 * Operator Behavior Notation
 *
 * [{T,a}] -> [{T,b}]
 *
 * map(f):             [{T,a}]       -> [{T,f(a)}]
 * filter(p):          [{T,a}]       -> subset [{T,a}]
 * scan(reducer,seed): [{T,a}]       -> [{T,state}]
 * delay(d):           [{T,a}]       -> [{T+d,a}]
 * debounceTime(d):    burst [{T,a}] -> last quiet value [{T+d,a}]
 */

/*
 * 8-Policy Framework — read every operator through these eight lenses
 *
 * Policy       | Question
 * -------------|------------------------------------------------------------------
 * Source       | What input stream does the operator read?
 * Trigger      | What causes output?
 * Value        | What value is emitted?
 * Cardinality  | How many output values can one input value produce?
 * Time         | When does output happen?
 * Concurrency  | Can multiple inner streams run at the same time?
 * Cancellation | What gets stopped when new values arrive or unsubscribe happens?
 * Termination  | What happens on complete or error?
 */

import {
  of, interval, Subject,
  map, filter, scan, tap, startWith, distinctUntilChanged,
  take, takeUntil, reduce
} from 'rxjs';

// ─── Domain type ──────────────────────────────────────────────────────────────

type User = { name: string; active: boolean };

// ─── Section 4: map ───────────────────────────────────────────────────────────
/*
 * map(project)
 *
 * Source:       a stream of values
 * Trigger:      each source next value
 * Value:        project(value)
 * Cardinality:  one input → one output
 * Time:         same time as source emission
 * Concurrency:  none
 * Cancellation: no inner subscription to cancel
 * Termination:  source complete completes output; source error errors output
 */

function demonstrateMap(): void {
  const users$ = of(
    { name: 'Alice', active: true } satisfies User,
    { name: 'Bob',   active: false } satisfies User
  );
  const names$ = users$.pipe(map(u => u.name));
  names$.subscribe(name => console.log('[map]', name));
}

// ─── Section 5: filter ────────────────────────────────────────────────────────
/*
 * filter(predicate)
 *
 * Source:       a stream of values
 * Trigger:      each source next value
 * Value:        the original value, unchanged
 * Cardinality:  one input → zero or one output
 * Time:         same time as source emission
 * Concurrency:  none
 * Cancellation: none
 * Termination:  source complete completes output; source error errors output
 */

function demonstrateFilter(): void {
  const users$ = of(
    { name: 'Alice', active: true  } satisfies User,
    { name: 'Bob',   active: false } satisfies User,
    { name: 'Carol', active: true  } satisfies User
  );
  const activeUsers$ = users$.pipe(filter(u => u.active));
  activeUsers$.subscribe(u => console.log('[filter]', u));
}

// ─── Section 6: scan ──────────────────────────────────────────────────────────
/*
 * scan(reducer, initialState)
 *
 * Source:       a stream of actions or values
 * Trigger:      each source next value
 * Value:        new accumulated state
 * Cardinality:  one input → one accumulated output
 * Time:         same time as source emission
 * Concurrency:  none
 * Cancellation: none
 * Termination:  source complete completes output; source error errors output
 */

function demonstrateScan(): void {
  // Click counter: map each click to delta 1, accumulate with scan
  const clicks$ = of(null, null, null); // stub for fromEvent clicks
  clicks$.pipe(
    map(() => 1),
    scan((n, d) => n + d, 0)
  ).subscribe(count => console.log('[scan] click count', count));
  // emits: 1, 2, 3
}

// ─── Section 7: tap ───────────────────────────────────────────────────────────
/*
 * tap(sideEffect)
 *
 * Source:       a stream of values
 * Trigger:      each source next value, error, or complete
 * Value:        the original value, unchanged (pass-through)
 * Cardinality:  one input → one output
 * Time:         same time as source emission
 * Concurrency:  none
 * Cancellation: none
 * Termination:  source complete completes output; source error errors output
 *
 * Rule: tap is always safe to add or remove — it never affects downstream values.
 */

function demonstrateTap(): void {
  const users$ = of(
    { name: 'Alice', active: true  } satisfies User,
    { name: 'Bob',   active: false } satisfies User
  );
  users$.pipe(
    tap(v => console.log('[before]', v)),
    filter(u => u.active),
    tap(v => console.log('[after]',  v)),
    map(u => u.name)
  ).subscribe(name => console.log('[result]', name));
}

// ─── Section 8: startWith ─────────────────────────────────────────────────────
/*
 * startWith(initialValue)
 *
 * Source:       a stream of values
 * Trigger:      immediately on subscription, then each source next value
 * Value:        the initial value first, then source values
 * Cardinality:  adds one extra value at the start
 * Time:         initial value emits synchronously before source starts
 * Concurrency:  none
 * Cancellation: none
 * Termination:  source complete completes output; source error errors output
 *
 * startWith is required in the state pattern (scan + startWith + shareReplay)
 * so that late subscribers receive initial state without waiting for the first action.
 */

function demonstrateStartWith(): void {
  const serverStatus$ = new Subject<string>();
  const status$ = serverStatus$.pipe(startWith('connecting'));
  // emits 'connecting' synchronously, then source values
  const sub = status$.subscribe(v => console.log('[startWith]', v));
  serverStatus$.next('online');
  serverStatus$.next('degraded');
  sub.unsubscribe();
  serverStatus$.complete();
}

// ─── Section 9: distinctUntilChanged ─────────────────────────────────────────
/*
 * distinctUntilChanged()
 *
 * Source:       a stream of values
 * Trigger:      each source value that DIFFERS from the previous value
 * Value:        the original value, unchanged
 * Cardinality:  one input → zero or one output
 * Time:         same time as source emission
 * Concurrency:  none
 * Cancellation: none
 * Termination:  source complete completes output; source error errors output
 *
 * Almost always paired with debounceTime for search inputs and with map for state slices.
 */

function demonstrateDistinctUntilChanged(): void {
  of('rx', 'rxjs', 'rxjs', 'rxjs7').pipe(
    distinctUntilChanged()
  ).subscribe(v => console.log('[distinctUntilChanged]', v));
  // emits: 'rx', 'rxjs', 'rxjs7'  — consecutive 'rxjs' duplicate is suppressed
}

// ─── Section 10: take and takeUntil ──────────────────────────────────────────
/*
 * take(count)
 *   Trigger:     each source value up to count
 *   Cardinality: passes at most count values
 *   Termination: completes after count values regardless of source
 *
 * takeUntil(notifier$)
 *   Trigger:     each source value until notifier emits
 *   Cardinality: passes values until notified
 *   Termination: completes when notifier emits its first value
 *
 * takeUntil is the idiomatic way to tie a stream's lifetime to a component
 * or service lifecycle. When destroy$.next() is called, the stream completes
 * and teardown runs automatically.
 */

function demonstrateTake(): void {
  interval(1000).pipe(
    take(3)
  ).subscribe(v => console.log('[take]', v));
  // emits: 0, 1, 2  then completes
}

function demonstrateTakeUntil(): void {
  const destroy$ = new Subject<void>();
  // interval is async — values arrive after this tick; destroy$ ends the stream
  const sub = interval(1000).pipe(
    takeUntil(destroy$)
  ).subscribe(v => console.log('[takeUntil]', v));
  // Simulate component destroy: stream completes, teardown runs automatically
  destroy$.next();
  destroy$.complete();
  sub.unsubscribe(); // safe no-op if stream already completed
}

// ─── Section 11: scan vs reduce ──────────────────────────────────────────────

function demonstrateScanVsReduce(): void {
  // scan: emits 1, 3, 6, 10 as values arrive — state after each action
  of(1, 2, 3, 4).pipe(
    scan((acc, val) => acc + val, 0)
  ).subscribe(v => console.log('[scan]', v));

  // reduce: emits only 10 after the source completes
  of(1, 2, 3, 4).pipe(
    reduce((acc, val) => acc + val, 0)
  ).subscribe(v => console.log('[reduce]', v));

  // In reactive UI work, scan is almost always the right choice:
  // state must be visible after each action, not only at the end.
}

// ─── Section 12: Debugging Pipelines — named-variable technique ───────────────

function demonstrateNamedPipeline(): void {
  const users$ = of(
    { name: 'Alice', active: true  } satisfies User,
    { name: 'Bob',   active: false } satisfies User,
    { name: 'Carol', active: true  } satisfies User
  );
  // Assign intermediate streams to named variables for readable stack traces
  const activeUsers$ = users$.pipe(filter(u => u.active));
  const activeNames$ = activeUsers$.pipe(map(u => u.name));
  activeNames$.subscribe(name => console.log('[named-pipeline]', name));
}

// ─── Run all examples ─────────────────────────────────────────────────────────

demonstrateMap();
demonstrateFilter();
demonstrateScan();
demonstrateTap();
demonstrateStartWith();
demonstrateDistinctUntilChanged();
demonstrateTake();
demonstrateTakeUntil();
demonstrateScanVsReduce();
demonstrateNamedPipeline();
