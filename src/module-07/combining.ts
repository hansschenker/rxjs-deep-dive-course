// Module 7 — Combining Streams: runnable demo

import {
  of, combineLatest, zip, forkJoin, merge, concat, race, partition, iif, EMPTY
} from 'rxjs';
import type { Observable } from 'rxjs';
import { map, withLatestFrom, startWith } from 'rxjs/operators';

// --- combineLatest: whenever any source emits, combine latest from all ---
// Waits until every source has emitted at least once.
// Use startWith to unblock sources that may be slow.
const nameValid$     = of(true).pipe(startWith(false));
const emailValid$    = of(true).pipe(startWith(false));
const passwordValid$ = of(true).pipe(startWith(false));

export const formValid$ = combineLatest([nameValid$, emailValid$, passwordValid$]).pipe(
  map(([nameV, emailV, passV]) => nameV && emailV && passV)
);

// --- withLatestFrom: primary stream triggers; secondary provides context ---
// A secondary-stream change alone does not trigger output.
const submitClicks$ = of(null);
const formValue$    = of({ email: 'alice@example.com' });

export const submit$ = submitClicks$.pipe(
  withLatestFrom(formValue$),
  map(([_click, form]) => form)
);

// --- zip: pair values by index; waits for each source to provide its next value ---
const firstName$ = of('Jane');
const lastName$  = of('Doe');
export const pairs$ = zip(firstName$, lastName$);

// --- forkJoin: wait for all sources to complete, emit final values once ---
// If one source never completes, forkJoin never emits.
const loadUserStub$        = of({ id: '1', name: 'Alice' });
const loadSettingsStub$    = of({ theme: 'dark' });
const loadPermissionsStub$ = of(['read', 'write']);

export const pageData$ = forkJoin({
  user:        loadUserStub$,
  settings:    loadSettingsStub$,
  permissions: loadPermissionsStub$
});

// --- merge: forward values from all sources as they arrive ---
const saveClicks$   = of('save');
const cancelClicks$ = of('cancel');
const routeChanges$ = of('/home');

export const userActions$ = merge(saveClicks$, cancelClicks$, routeChanges$);

// --- concat: next source subscribes only after previous completes ---
const loadConfigStub$    = of({ apiVersion: '2' });
const loadDashboardStub$ = of({ widgets: [] as string[] });

export const startup$ = concat(loadConfigStub$, loadUserStub$, loadDashboardStub$);

// --- race: first source to emit wins; all others are unsubscribed ---
const apiRequest$     = of('api response');
const timeoutWarning$ = of('timeout');

export const raceResult$ = race(apiRequest$, timeoutWarning$);

// --- partition: split one stream into two by a predicate ---
type AppUser = { name: string; role: 'admin' | 'user' };
const users$: Observable<AppUser> = of(
  { name: 'Alice', role: 'admin' as const },
  { name: 'Bob',   role: 'user'  as const }
);

const [admins$, regularUsers$] = partition(users$, user => user.role === 'admin');
export { admins$, regularUsers$ };

// --- iif: choose between two Observables at subscription time ---
// The condition is evaluated lazily on each subscription.
function isLoggedIn(): boolean { return true; }
const loadDashboardRoute$ = of('dashboard');

export const conditional$ = iif(() => isLoggedIn(), loadDashboardRoute$, EMPTY);

/*
 * Combining policy table:
 *
 * Operator         | Trigger          | Waits for all? | Needs completion | Use case
 * combineLatest    | any source       | yes (first)    | no               | live derived state
 * withLatestFrom   | primary source   | secondary only | no               | event + state
 * zip              | all next values  | yes            | no               | ordered pairing
 * forkJoin         | all complete     | yes            | yes              | page load bundle
 * merge            | any source       | no             | no               | action streams
 * concat           | previous done    | no             | yes              | sequential workflows
 * race             | first source     | no             | no               | first response wins
 * partition        | any source       | no             | no               | split by predicate
 * iif              | subscription     | no             | no               | conditional source
 */
