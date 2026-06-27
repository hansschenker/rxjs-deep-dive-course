# Appendix A — Common Anti-Patterns

These are the mistakes that appear most frequently in RxJS codebases. Each has a diagnosis, the harm it causes, and the correct replacement.

---

## A.1 Nested Subscriptions

**Anti-pattern:**

```ts
source$.subscribe(value => {
  inner$(value).subscribe(result => {
    console.log(result);
  });
});
```

**Harm:** The inner subscription is never managed. Every outer emission creates a new inner subscription. Unsubscribing from the outer does not clean up any inner subscriptions — memory and event listeners accumulate indefinitely.

**Fix:** Use a flattening operator. Choose the policy that fits:

```ts
// Cancel previous inner on new outer value (most common for reads)
source$.pipe(switchMap(value => inner$(value))).subscribe(console.log);

// Allow all inner concurrently
source$.pipe(mergeMap(value => inner$(value))).subscribe(console.log);

// Queue inner emissions
source$.pipe(concatMap(value => inner$(value))).subscribe(console.log);

// Ignore new outer while inner is active
source$.pipe(exhaustMap(value => inner$(value))).subscribe(console.log);
```

---

## A.2 Unmanaged Subscriptions

**Anti-pattern:**

```ts
// In a component constructor or ngOnInit — no cleanup
this.data$.subscribe(data => this.render(data));
```

**Harm:** The subscription outlives the component. The callback fires against a destroyed component, causing errors or stale renders. In long-running apps this is a memory leak.

**Fix — option 1:** `takeUntilDestroyed` (Angular 16+):

```ts
this.data$.pipe(takeUntilDestroyed()).subscribe(data => this.render(data));
```

**Fix — option 2:** `takeUntil` with a destroy subject:

```ts
private destroy$ = new Subject<void>();

ngOnInit() {
  this.data$.pipe(takeUntil(this.destroy$)).subscribe(data => this.render(data));
}

ngOnDestroy() {
  this.destroy$.next();
  this.destroy$.complete();
}
```

**Fix — option 3:** Framework binding (`async` pipe, `toSignal`, `useEffect` cleanup).

---

## A.3 `async/await` Inside `.subscribe()`

**Anti-pattern:**

```ts
source$.subscribe(async value => {
  const result = await processAsync(value);
  console.log(result);
});
```

**Harm:** Each emission spawns a floating Promise. The Promise is not connected to the stream — it cannot be cancelled, it cannot be composed, errors are unhandled, and emissions may resolve out of order.

**Fix:** Keep async work inside the stream using a flattening operator:

```ts
source$.pipe(
  switchMap(value => from(processAsync(value)))
).subscribe(console.log);
```

---

## A.4 Subject Used Where a Creation Operator Would Do

**Anti-pattern:**

```ts
const clicks$ = new Subject<MouseEvent>();
document.addEventListener('click', e => clicks$.next(e));
```

**Harm:** The event listener is never removed. When the Subject goes out of scope or the stream is unsubscribed, the listener keeps firing. This is a memory leak and a source of phantom events.

**Fix:** Use `fromEvent` — teardown is automatic on unsubscribe:

```ts
const clicks$ = fromEvent<MouseEvent>(document, 'click');
```

**Rule:** If `fromEvent`, `fromEventPattern`, `interval`, `timer`, `defer`, or `ajax` can model the source — use them. A `Subject` is appropriate only when the values originate from imperative code that has no observable equivalent (e.g., bridging a non-observable callback API or dispatching actions from UI event handlers into a state stream).

---

## A.5 Using `switchMap` for Writes

**Anti-pattern:**

```ts
saveButton$.pipe(
  switchMap(data => saveToServer(data))
).subscribe();
```

**Harm:** If the user clicks save twice rapidly, `switchMap` cancels the first save request mid-flight. The server may have received a partial write. The operation is now in an unknown state.

**Fix:** Choose the policy that matches the write semantics:

```ts
// Ignore additional saves while one is in flight
saveButton$.pipe(exhaustMap(data => saveToServer(data))).subscribe();

// Queue saves sequentially
saveButton$.pipe(concatMap(data => saveToServer(data))).subscribe();
```

**Rule:** `switchMap` is a read policy. It is correct when stale results are worthless (search, autocomplete, live data). It is incorrect for writes, form submissions, or any operation where cancellation mid-flight leaves state inconsistent.

---

## A.6 `.getValue()` on BehaviorSubject Inside a Stream

**Anti-pattern:**

```ts
source$.pipe(
  map(() => store$.getValue().users)
).subscribe(render);
```

**Harm:** The stream reads state synchronously at emission time but does not declare `store$` as a dependency. If `store$` emits between `source$` emissions, the view is stale. This breaks the reactive data flow.

**Fix:** Declare the dependency explicitly with `withLatestFrom` or `combineLatest`:

```ts
source$.pipe(
  withLatestFrom(store$.pipe(map(s => s.users)))
).subscribe(([_, users]) => render(users));
```

---

