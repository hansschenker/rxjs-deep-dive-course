// Module 14 — Framework Integration: runnable demo

import { Observable, Subject, of } from 'rxjs';
import {
  debounceTime, switchMap, startWith, catchError, map, shareReplay
} from 'rxjs/operators';

/*
 * Separation of responsibilities:
 *
 *   RxJS:         events, effects, async workflows, cancellation, state streams
 *   UI framework: rendering, template binding, component lifecycle, local view concerns
 */

// --- React integration ---
// Subscribe inside useEffect; return cleanup to unsubscribe on unmount.
// The hook type signature — actual implementation requires React imports.
export type UseObservableFn = <T>(source$: Observable<T>, initial: T) => T;

/*
 * Full React hook implementation (requires 'react' package):
 *
 *   import { useState, useEffect } from 'react';
 *
 *   function useObservable<T>(source$: Observable<T>, initial: T): T {
 *     const [value, setValue] = useState(initial);
 *     useEffect(() => {
 *       const subscription = source$.subscribe(setValue);
 *       return () => subscription.unsubscribe();
 *     }, [source$]);
 *     return value;
 *   }
 */

// --- Vue useSearch composable ---
// Owns the subscription; exposes reactive refs (query$, results$, status$).
type SearchResult = { id: string; title: string };

function searchApi(q: string): Observable<SearchResult[]> {
  return of([{ id: '1', title: `Result for ${q}` }]);
}

export function useSearch(): {
  query$:   Subject<string>;
  results$: Observable<SearchResult[]>;
  status$:  Observable<string>;
} {
  const query$ = new Subject<string>();

  const state$ = query$.pipe(
    debounceTime(300),
    switchMap(q =>
      searchApi(q).pipe(
        map(results => ({ status: 'success' as const, results })),
        startWith({ status: 'loading' as const, results: [] as SearchResult[] }),
        catchError(() => of({ status: 'failure' as const, results: [] as SearchResult[] }))
      )
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  const results$ = state$.pipe(map(s => s.results));
  const status$  = state$.pipe(map(s => s.status));

  return { query$, results$, status$ };
}

/*
 * Full Vue composable usage:
 *
 *   import { ref, watchEffect, onUnmounted } from 'vue';
 *
 *   export function useSearchComposable() {
 *     const { query$, results$, status$ } = useSearch();
 *     const queryRef  = ref('');
 *     const resultsRef = ref<SearchResult[]>([]);
 *     const statusRef  = ref('idle');
 *
 *     const sub1 = results$.subscribe(r  => { resultsRef.value = r; });
 *     const sub2 = status$.subscribe(s   => { statusRef.value  = s; });
 *
 *     watchEffect(() => { query$.next(queryRef.value); });
 *     onUnmounted(() => { sub1.unsubscribe(); sub2.unsubscribe(); });
 *
 *     return { queryRef, resultsRef, statusRef };
 *   }
 *
 * Angular integration (decorators disabled — shown as comments only):
 *
 *   @Component({ selector: 'app-search', template: `...` })
 *   export class SearchComponent {
 *     private query   = signal('');
 *     private query$  = toObservable(this.query);
 *
 *     private state$ = this.query$.pipe(
 *       debounceTime(300),
 *       distinctUntilChanged(),
 *       switchMap(q =>
 *         http.get<SearchResult[]>(`/api/search?q=${q}`).pipe(
 *           map(results => ({ status: 'success' as const, results })),
 *           startWith({ status: 'loading' as const, results: [] }),
 *           catchError(err => of({ status: 'failure' as const, results: [], error: String(err) }))
 *         )
 *       ),
 *       shareReplay({ bufferSize: 1, refCount: true })
 *     );
 *
 *     state = toSignal(this.state$, { initialValue: { status: 'idle', results: [] } });
 *   }
 *
 * Angular lifecycle helpers:
 *   toObservable(signal)              Signal  → Observable
 *   toSignal(obs$, { initialValue })  Observable → Signal
 *   takeUntilDestroyed(destroyRef)    auto-unsubscribes when injection context destroys
 *   async pipe in templates           subscribes and unsubscribes automatically
 */
