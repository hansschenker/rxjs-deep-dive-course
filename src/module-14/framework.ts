// Module 14 — Framework Integration | Companion code for RxJS Deep Dive
//
// Framework-specific bindings (Angular decorators, React hooks, Vue composables)
// require their respective packages and cannot be compiled in isolation.
// This file contains the PURE RxJS LOGIC for each framework pattern so it
// can be understood, tested, and adapted to any framework environment.
//
// Separation of responsibilities:
//   RxJS:         events, effects, async workflows, cancellation, state streams
//   UI framework: rendering, template binding, component lifecycle, local view

import {
  Subject,
  of,
  debounceTime,
  switchMap,
  startWith,
  map,
  shareReplay,
  scan,
  distinctUntilChanged,
  takeUntil,
  type Observable,
} from 'rxjs';

// ─── Section 1: Angular-style service (pure RxJS, no decorators) ─────────────

export type SearchState = {
  query:   string;
  results: string[];
  status:  'idle' | 'loading' | 'success' | 'error';
  error:   string | null;
};

export type SearchAction =
  | { type: 'QueryChanged';  query:   string   }
  | { type: 'ResultsLoaded'; results: string[] }
  | { type: 'ErrorOccurred'; error:   string   };

const searchInitialState: SearchState = {
  query:   '',
  results: [],
  status:  'idle',
  error:   null,
};

function searchReducer(state: SearchState, action: SearchAction): SearchState {
  switch (action.type) {
    case 'QueryChanged':
      return { ...state, query: action.query, status: 'loading', error: null };
    case 'ResultsLoaded':
      return { ...state, results: action.results, status: 'success', error: null };
    case 'ErrorOccurred':
      return { ...state, status: 'error', error: action.error, results: [] };
  }
}

/**
 * Angular-style service: owns the Subject + scan + shareReplay pipeline.
 * Components inject this service and derive signal values via toSignal().
 */
export function createSearchService(): {
  dispatch: (action: SearchAction) => void;
  state$:   Observable<SearchState>;
  results$: Observable<string[]>;
} {
  const actions$ = new Subject<SearchAction>();

  const state$ = actions$.pipe(
    scan(searchReducer, searchInitialState),
    startWith(searchInitialState),
    shareReplay({ bufferSize: 1, refCount: true }),
  );

  const results$ = state$.pipe(map(s => s.results));

  return {
    dispatch: (action: SearchAction) => actions$.next(action),
    state$,
    results$,
  };
}

/**
 * Simulates Angular's DestroyRef / takeUntilDestroyed pattern.
 * Call destroy() when the injection context (component or service) is destroyed.
 * Use the destroy$ Subject with takeUntil() to auto-complete any stream.
 */
export function createDestroyRef(): {
  destroy$: Subject<void>;
  destroy:  () => void;
} {
  const destroy$ = new Subject<void>();
  return {
    destroy$,
    destroy: () => { destroy$.next(); destroy$.complete(); },
  };
}

/*
 * Angular component pseudo-code (requires @angular/core, @angular/core/rxjs-interop):
 *
 *   import { Component, signal, inject } from '@angular/core';
 *   import { toObservable, toSignal, takeUntilDestroyed } from '@angular/core/rxjs-interop';
 *   import { HttpClient } from '@angular/common/http';
 *   import { debounceTime, distinctUntilChanged, switchMap, map, startWith, catchError, shareReplay } from 'rxjs';
 *   import { of } from 'rxjs';
 *
 *   @Component({
 *     selector: 'app-search',
 *     template: `
 *       <input [value]="query()" (input)="query.set($any($event.target).value)" />
 *       <div *ngIf="state().status === 'loading'">Loading…</div>
 *       <li *ngFor="let item of state().results">{{ item }}</li>
 *     `
 *   })
 *   export class SearchComponent {
 *     private http = inject(HttpClient);
 *     query = signal('');
 *
 *     private state$ = toObservable(this.query).pipe(
 *       debounceTime(300),
 *       distinctUntilChanged(),
 *       switchMap(q => this.http.get<string[]>(`/api/search?q=${q}`).pipe(
 *         map(results => ({ status: 'success' as const, results })),
 *         startWith({ status: 'loading' as const, results: [] as string[] }),
 *         catchError(err => of({ status: 'error' as const, results: [], error: String(err) }))
 *       )),
 *       takeUntilDestroyed(),        // auto-unsubscribes when component destroys
 *       shareReplay({ bufferSize: 1, refCount: true })
 *     );
 *
 *     state = toSignal(this.state$, { initialValue: { status: 'idle', results: [] } });
 *   }
 *
 * Key Angular interop helpers:
 *   toObservable(signal)                Signal  → Observable (needs injection context)
 *   toSignal(obs$, { initialValue })    Observable → Signal  (auto-manages subscription)
 *   takeUntilDestroyed(destroyRef?)     auto-completes stream when context destroys
 *   async pipe in templates             subscribes and unsubscribes automatically
 */

// ─── Section 2: React-style hook (pure RxJS, no React imports) ───────────────

/**
 * The pure RxJS pattern underlying a useObservable hook.
 *
 * getValue() — returns the latest emitted value (initial until first emission).
 * subscribe() — attaches a callback and returns an unsubscribe function.
 *               Maps to: the cleanup return value of useEffect.
 *
 * State is tracked via closure; no React internals required.
 */
export function createObservableState<T>(
  source$: Observable<T>,
  initial: T,
): {
  getValue:  () => T;
  subscribe: (cb: (value: T) => void) => () => void;
} {
  let current: T = initial;

  return {
    getValue: () => current,
    subscribe: (cb: (value: T) => void) => {
      const sub = source$.subscribe(value => {
        current = value; // update snapshot on every emission
        cb(value);
      });
      return () => sub.unsubscribe();
    },
  };
}

/*
 * React hook implementation (requires 'react' package):
 *
 *   import { useState, useEffect } from 'react';
 *   import type { Observable } from 'rxjs';
 *
 *   function useObservable<T>(source$: Observable<T>, initial: T): T {
 *     const [value, setValue] = useState(initial);
 *
 *     useEffect(() => {
 *       const subscription = source$.subscribe(setValue);
 *       return () => subscription.unsubscribe();   // cleanup on unmount
 *     }, [source$]);
 *
 *     return value;
 *   }
 *
 * The lifecycle rule maps cleanly:
 *   React mounts   → subscribe
 *   React unmounts → unsubscribe (via useEffect cleanup)
 */

// ─── Section 3: Vue-style composable (pure RxJS, no Vue imports) ─────────────

/**
 * The pure RxJS pattern underlying a Vue composable.
 * Owns the subscription lifetime internally.
 *
 * query$   — push search terms here (maps to Vue watchEffect watching a ref)
 * results$ — subscribe for results  (maps to a Vue ref updated via .subscribe)
 * destroy  — call on unmount        (maps to onUnmounted lifecycle hook)
 */
export function createSearchComposable(
  searchFn: (term: string) => Observable<string[]>,
): {
  query$:   Subject<string>;
  results$: Observable<string[]>;
  destroy:  () => void;
} {
  const query$   = new Subject<string>();
  const destroy$ = new Subject<void>();

  const results$ = query$.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    switchMap(searchFn),
    takeUntil(destroy$),
    shareReplay({ bufferSize: 1, refCount: true }),
  );

  return {
    query$,
    results$,
    destroy: () => { destroy$.next(); destroy$.complete(); },
  };
}

/*
 * Vue composable usage (requires 'vue' package):
 *
 *   import { ref, watchEffect, onUnmounted } from 'vue';
 *   import type { Observable } from 'rxjs';
 *   import { createSearchComposable } from './framework';
 *
 *   export function useSearch(searchFn: (q: string) => Observable<string[]>) {
 *     const { query$, results$, destroy } = createSearchComposable(searchFn);
 *
 *     const queryRef   = ref('');
 *     const resultsRef = ref<string[]>([]);
 *
 *     const sub = results$.subscribe(r => { resultsRef.value = r; });
 *
 *     watchEffect(() => { query$.next(queryRef.value); });
 *     onUnmounted(() => { sub.unsubscribe(); destroy(); });
 *
 *     return { queryRef, resultsRef };
 *   }
 */

// ─── Section 4: The lifecycle rule ───────────────────────────────────────────
/*
 * "Subscriptions must be owned by a lifecycle."
 *
 * An unmanaged subscription keeps its source Observable alive and its callback
 * firing long after the component that created it has been destroyed.
 * This causes memory leaks, stale state updates, and hard-to-reproduce bugs.
 *
 * Map the framework lifecycle to RxJS subscription management:
 *
 *   Angular 16+  →  takeUntilDestroyed(destroyRef)    (automatic)
 *   Angular      →  toSignal(obs$)                     (auto-manages subscription)
 *   Angular      →  async pipe in templates             (subscribe + unsubscribe)
 *   React        →  useEffect cleanup: () => sub.unsubscribe()
 *   Vue          →  onUnmounted(() => sub.unsubscribe())
 *   Plain JS     →  createDestroyRef() + takeUntil(destroy$)
 *
 * The pattern: SUBSCRIBE at creation, UNSUBSCRIBE at destruction. Always.
 */

// ─── Demonstrations ───────────────────────────────────────────────────────────

function demonstrateSearchService(): void {
  const service = createSearchService();

  const unsub = service.state$.subscribe(state =>
    console.log('[SearchService]', state.status, state.results),
  );

  service.dispatch({ type: 'QueryChanged',  query:   'rxjs'               });
  service.dispatch({ type: 'ResultsLoaded', results: ['RxJS 7', 'Marbles'] });
  service.dispatch({ type: 'ErrorOccurred', error:   'timeout'             });

  unsub.unsubscribe();
}

function demonstrateObservableState(): void {
  const source$ = of(1, 2, 3);
  const state   = createObservableState(source$, 0);

  const unsub = state.subscribe(v => console.log('[ObservableState] emission:', v));
  console.log('[ObservableState] snapshot after subscribe:', state.getValue());
  unsub();
}

function demonstrateSearchComposable(): void {
  const composable = createSearchComposable(
    term => of([`Result A for ${term}`, `Result B for ${term}`]),
  );

  const sub = composable.results$.subscribe(
    results => console.log('[SearchComposable]', results),
  );

  // Simulate query after debounce window (300 ms)
  composable.query$.next('rxjs');
  setTimeout(() => { sub.unsubscribe(); composable.destroy(); }, 500);
}

demonstrateSearchService();
demonstrateObservableState();
demonstrateSearchComposable();
