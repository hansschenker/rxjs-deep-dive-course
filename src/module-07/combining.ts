// Module 7 — Combining Streams | Companion code for RxJS Deep Dive

import {
  of, Subject, EMPTY,
  combineLatest, zip, forkJoin, merge, concat, race, partition, iif,
  map, withLatestFrom, startWith, delay,
  combineLatestWith, mergeWith, concatWith, zipWith, raceWith
} from 'rxjs';
import type { Observable } from 'rxjs';

// ─── Section 2: combineLatest — any source triggers, combine latest from all ──
/*
 * Policy: whenever any source emits, combine the latest value from every source.
 *         Does not emit until every source has emitted at least once.
 *
 * Each input stream contributes its latest value.
 * Output starts only when all streams have emitted.
 * After that, every new value from any input recomputes the result.
 *
 * Hazard: if one stream never emits, combineLatest never emits.
 * Solution: seed each field stream with startWith(initialValue).
 */

function demonstrateCombineLatest(): void {
  const nameValid$     = of(true).pipe(startWith(false));
  const emailValid$    = of(true).pipe(startWith(false));
  const passwordValid$ = of(true).pipe(startWith(false));

  combineLatest([nameValid$, emailValid$, passwordValid$]).pipe(
    map(([nameV, emailV, passV]) => nameV && emailV && passV)
  ).subscribe(valid => console.log('[combineLatest] formValid', valid));
  // emits false (from startWith seeds), then true (when of(true) fires for all three)
}

// ─── Section 3: withLatestFrom — primary triggers, secondary provides context ─
/*
 * Policy: only the primary stream triggers output.
 *         Secondary streams provide their latest values as context.
 *
 * A form value change alone does not submit.
 * Only a submitClick$ event triggers output.
 */

function demonstrateWithLatestFrom(): void {
  const submitClicks$ = new Subject<void>();
  const formValue$    = new Subject<{ email: string }>();

  const sub = submitClicks$.pipe(
    withLatestFrom(formValue$),
    map(([_click, form]) => form)
  ).subscribe(form => console.log('[withLatestFrom]', form));

  formValue$.next({ email: 'alice@example.com' }); // sets the latest context value
  submitClicks$.next();                              // triggers → { email: 'alice@example.com' }
  formValue$.next({ email: 'bob@example.com' });    // updates context
  submitClicks$.next();                              // triggers → { email: 'bob@example.com' }

  sub.unsubscribe();
  submitClicks$.complete();
  formValue$.complete();
}

// ─── Section 4: zip — pair values by index ────────────────────────────────────
/*
 * Policy: pair values by index; wait until each stream has its next value.
 * zip([a], [b]) -> [(a,b)]
 *
 * Use when order and pairing matter.
 * Hazard: fast streams wait for slow streams.
 */

function demonstrateZip(): void {
  const firstName$ = new Subject<string>();
  const lastName$  = new Subject<string>();

  const sub = zip(firstName$, lastName$).subscribe(
    ([first, last]) => console.log('[zip]', first, last)
  );

  firstName$.next('Jane');
  lastName$.next('Doe');   // pair complete → emits ['Jane', 'Doe']
  firstName$.next('John');
  lastName$.next('Smith'); // pair complete → emits ['John', 'Smith']

  sub.unsubscribe();
  firstName$.complete();
  lastName$.complete();
}

// ─── Section 5: forkJoin — wait for all to complete, emit final values once ───
/*
 * Policy: wait for all sources to complete, then emit their final values once.
 *
 * Use for one-time page loading where all requests must finish before proceeding.
 * Hazard: if one source never completes, forkJoin never emits.
 */

function demonstrateForkJoin(): void {
  const loadUser$     = of({ id: '1', name: 'Alice' }).pipe(delay(0));
  const loadSettings$ = of({ theme: 'dark' }).pipe(delay(0));

  forkJoin({ user: loadUser$, settings: loadSettings$ }).subscribe(
    data => console.log('[forkJoin]', data)
  );
}

// ─── Section 6: merge — forward values from all sources as they arrive ─────────
/*
 * Policy: forward values from all sources as they arrive.
 * No waiting, no pairing — values interleave in arrival order.
 */

function demonstrateMerge(): void {
  const save$   = new Subject<string>();
  const cancel$ = new Subject<string>();
  const route$  = new Subject<string>();

  const sub = merge(save$, cancel$, route$).subscribe(
    action => console.log('[merge]', action)
  );

  save$.next('save');
  cancel$.next('cancel');
  route$.next('/home');

  sub.unsubscribe();
  save$.complete();
  cancel$.complete();
  route$.complete();
}

// ─── Section 7: concat — subscribe each source after the previous completes ───
/*
 * Policy: subscribe to the next source only after the previous source completes.
 * Use for sequential startup workflows where order matters.
 */

function demonstrateConcat(): void {
  const loadConfig$    = of({ apiVersion: '2' }).pipe(delay(0));
  const loadUser$      = of({ name: 'Alice' }).pipe(delay(0));
  const loadDashboard$ = of({ widgets: [] as string[] }).pipe(delay(0));

  concat(loadConfig$, loadUser$, loadDashboard$).subscribe(
    v => console.log('[concat]', v)
  );
}

// ─── Section 8: race — first source to emit wins ──────────────────────────────
/*
 * Policy: the first source to emit wins; all others are unsubscribed.
 */

function demonstrateRace(): void {
  const fast$ = of('fast response');
  const slow$ = of('slow response');

  race(fast$, slow$).subscribe(
    v => console.log('[race]', v)
  );
  // emits: 'fast response' — first in array wins when both are synchronous
}

// ─── Section 9: partition — split one stream into two ─────────────────────────
/*
 * Policy: split one stream into two by a predicate.
 * First stream receives values where predicate is true.
 * Second stream receives values where predicate is false.
 *
 * source:   --a--b--c--d--|
 *           (a,c pass predicate; b,d do not)
 * match$:   --a-----c-----|
 * noMatch$: -----b-----d--|
 */

type AppUser = { name: string; role: 'admin' | 'user' };

function demonstratePartition(): void {
  const users$: Observable<AppUser> = of(
    { name: 'Alice', role: 'admin' as const },
    { name: 'Bob',   role: 'user'  as const },
    { name: 'Carol', role: 'admin' as const }
  );

  const [admins$, regularUsers$] = partition(users$, user => user.role === 'admin');
  admins$.subscribe(u       => console.log('[partition admin]', u.name));
  regularUsers$.subscribe(u => console.log('[partition user]',  u.name));
}

// ─── Section 10: iif — conditional Observable at subscription time ─────────────
/*
 * Policy: choose between two Observables at subscription time based on a condition.
 * The condition is evaluated lazily on each subscription — not when iif is called.
 * This makes iif suitable for dynamic routing between two sources.
 */

function demonstrateIif(): void {
  let isLoggedIn = true;

  const dashboard$ = of('dashboard');
  const result$    = iif(() => isLoggedIn, dashboard$, EMPTY);

  result$.subscribe(v => console.log('[iif loggedIn=true]', v));
  // → 'dashboard'

  isLoggedIn = false;
  iif(() => isLoggedIn, dashboard$, EMPTY).subscribe({
    complete: () => console.log('[iif loggedIn=false] completed via EMPTY')
  });
}

/*
 * Pipeable Operator Variants
 *
 * The static combination operators each have a pipeable counterpart:
 *
 * Static form                       | Pipeable form
 * ----------------------------------|-------------------------------
 * combineLatest([a$, b$])           | a$.pipe(combineLatestWith(b$))
 * merge(a$, b$)                     | a$.pipe(mergeWith(b$))
 * concat(a$, b$)                    | a$.pipe(concatWith(b$))
 * zip(a$, b$)                       | a$.pipe(zipWith(b$))
 * race(a$, b$)                      | a$.pipe(raceWith(b$))
 *
 * Both forms produce identical behavior.
 * Pipeable variants are useful when one source is already the result of a .pipe() chain
 * and adding a static wrapper would reduce readability.
 */

function demonstratePipeableVariants(): void {
  const a$ = of(1, 2);
  const b$ = of(3, 4);

  a$.pipe(combineLatestWith(b$)).subscribe(
    pair => console.log('[combineLatestWith]', pair)
  );
  a$.pipe(mergeWith(b$)).subscribe(
    v => console.log('[mergeWith]', v)
  );
  a$.pipe(concatWith(b$)).subscribe(
    v => console.log('[concatWith]', v)
  );
  a$.pipe(zipWith(b$)).subscribe(
    pair => console.log('[zipWith]', pair)
  );
  a$.pipe(raceWith(b$)).subscribe(
    v => console.log('[raceWith]', v)
  );
}

/*
 * Combining Policy Table
 *
 * Operator         | Trigger           | Waits for all first? | Needs completion? | Use case
 * -----------------|-------------------|----------------------|-------------------|---------------------
 * combineLatest    | any source        | yes                  | no                | live derived state
 * withLatestFrom   | primary source    | secondary needed     | no                | event + state
 * zip              | all next values   | yes                  | no                | ordered pairing
 * forkJoin         | all complete      | yes                  | yes               | page load bundle
 * merge            | any source        | no                   | no                | action streams
 * concat           | previous complete | no                   | yes               | sequential workflows
 * race             | first source      | no                   | no                | first response wins
 * partition        | any source        | no                   | no                | split by predicate
 * iif              | subscription      | no                   | no                | conditional source
 */

// ─── Run all examples ─────────────────────────────────────────────────────────

demonstrateCombineLatest();
demonstrateWithLatestFrom();
demonstrateZip();
demonstrateForkJoin();
demonstrateMerge();
demonstrateConcat();
demonstrateRace();
demonstratePartition();
demonstrateIif();
demonstratePipeableVariants();
