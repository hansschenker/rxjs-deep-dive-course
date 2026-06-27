# Module 14 — Framework Integration

## Goal

Integrate RxJS into frontend frameworks while keeping responsibilities clear.

## 1. RxJS and UI Frameworks

RxJS should not fight the rendering framework.

A good separation is:

```txt
RxJS:
events, effects, async workflows, cancellation, state streams

UI framework:
rendering, template binding, component lifecycle, local view concerns
```

## 2. Angular Signals and RxJS

Signals are good for fine-grained synchronous state at rest.

RxJS is good for asynchronous state in motion.

Architecture:

```txt
DOM / HTTP / WebSocket / Router
        ↓
      RxJS
        ↓
    state stream
        ↓
     toSignal
        ↓
   Angular template
```

## 3. Example

```ts
@Component({
  selector: 'app-search',
  template: `
    <input
      [value]="query()"
      (input)="query.set($any($event.target).value)"
    />

    <div *ngIf="state().status === 'loading'">
      Loading...
    </div>

    <ul>
      <li *ngFor="let item of state().results">
        {{ item.name }}
      </li>
    </ul>
  `
})
export class SearchComponent {
  private http = inject(HttpClient);

  query = signal('');

  private query$ = toObservable(this.query);

  private state$ = this.query$.pipe(
    debounceTime(300),
    distinctUntilChanged(),
    switchMap(query =>
      this.http.get<Result[]>(`/api/search?q=${query}`).pipe(
        map(results => ({
          status: 'success' as const,
          results
        })),
        startWith({
          status: 'loading' as const,
          results: []
        }),
        catchError(error =>
          of({
            status: 'failure' as const,
            results: [],
            error: String(error)
          })
        )
      )
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  state = toSignal(this.state$, {
    initialValue: {
      status: 'idle' as const,
      results: []
    }
  });
}
```

## 4. Angular Service Pattern

The component example above handles simple search. For shared application state, move the stream logic into a service:

```ts
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Subject, merge, scan, startWith, shareReplay } from 'rxjs';
import { switchMap, map, catchError } from 'rxjs';
import { of } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class SearchService {
  private http = inject(HttpClient);
  private actions$ = new Subject<Action>();

  readonly state$ = this.actions$.pipe(
    scan(update, initialState),
    startWith(initialState),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  dispatch(action: Action): void {
    this.actions$.next(action);
  }
}
```

Components inject the service and subscribe via `toSignal`:

```ts
export class SearchComponent {
  private service = inject(SearchService);
  state = toSignal(this.service.state$, { initialValue: initialState });

  onInput(query: string): void {
    this.service.dispatch({ type: 'QueryChanged', query });
  }
}
```

The service owns the subscription lifetime. Components are pure view layers.

## 5. `takeUntilDestroyed`

`takeUntilDestroyed` (Angular 16+) automatically completes a stream when the injection context that created it is destroyed — no manual unsubscribe needed:

```ts
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Injectable({ providedIn: 'root' })
export class NotificationService {
  private destroyRef = inject(DestroyRef);

  readonly notifications$ = interval(5000).pipe(
    switchMap(() => this.http.get<Notification[]>('/api/notifications')),
    takeUntilDestroyed(this.destroyRef)
  );
}
```

In a component (where `DestroyRef` is available from the injection context implicitly):

```ts
export class FeedComponent {
  constructor() {
    this.feed$.pipe(takeUntilDestroyed()).subscribe(this.render.bind(this));
  }
}
```

## 6. Lifecycle Rule

Subscriptions must be owned by a lifecycle.

Options:

* template async pipe (Angular, Vue)
* `toSignal` (Angular)
* `takeUntilDestroyed` (Angular 16+)
* `watchEffect` cleanup return (Vue)
* `useEffect` cleanup return (React)
* framework-specific cleanup APIs

Avoid unmanaged subscriptions in components.

## 7. React Integration

In React, RxJS can be integrated with hooks.

A stream should be subscribed to inside an effect and cleaned up when the component unmounts.

```ts
function useObservable<T>(source$: Observable<T>, initial: T): T {
  const [value, setValue] = useState(initial);

  useEffect(() => {
    const subscription = source$.subscribe(setValue);
    return () => subscription.unsubscribe();
  }, [source$]);

  return value;
}
```

## 8. Vue Integration

In Vue, `watchEffect` runs a side effect and tracks reactive dependencies. Use the cleanup callback to unsubscribe:

```ts
import { ref, watchEffect, onUnmounted } from 'vue';
import { Subject } from 'rxjs';
import { debounceTime, switchMap } from 'rxjs';

export function useSearch() {
  const query = ref('');
  const results = ref<Result[]>([]);
  const status = ref<'idle' | 'loading' | 'success' | 'failure'>('idle');

  const query$ = new Subject<string>();

  const subscription = query$.pipe(
    debounceTime(300),
    switchMap(q =>
      searchApi(q).pipe(
        map(r => ({ status: 'success' as const, results: r })),
        startWith({ status: 'loading' as const, results: [] }),
        catchError(() => of({ status: 'failure' as const, results: [] }))
      )
    )
  ).subscribe(state => {
    status.value = state.status;
    results.value = state.results;
  });

  watchEffect(() => {
    query$.next(query.value);
  });

  onUnmounted(() => subscription.unsubscribe());

  return { query, results, status };
}
```

The composable owns the subscription. The component uses `query`, `results`, and `status` as reactive refs.

## Learning Outcome

The learner should know where RxJS belongs in an application, how to connect it to rendering without leaking subscriptions, and how each major framework's lifecycle maps to RxJS subscription management.

---

