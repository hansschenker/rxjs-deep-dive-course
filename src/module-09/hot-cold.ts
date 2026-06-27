// Module 9 — Hot, Cold, and Shared Streams | Companion code for RxJS Deep Dive

import {
  Observable, Subject, BehaviorSubject, ReplaySubject, AsyncSubject, of,
  share, shareReplay, map
} from 'rxjs';

// ─── Section 1: Cold Observable ───────────────────────────────────────────────
// A cold Observable creates a fresh producer for each subscription.
//
// Important correction:
//   Cold does not mean replay.
//   Cold means fresh execution per subscriber.

function demoColdObservable(): void {
  const random$ = new Observable<number>(subscriber => {
    console.log('[cold] producer starts');
    subscriber.next(Math.random());
    subscriber.complete();
  });

  random$.subscribe(v => console.log('[cold] subscriber A:', v));
  random$.subscribe(v => console.log('[cold] subscriber B:', v));
  // Expected:
  //   [cold] producer starts
  //   [cold] subscriber A: 0.123...   ← unique value
  //   [cold] producer starts
  //   [cold] subscriber B: 0.847...   ← different unique value
  //
  // Each subscriber triggers a separate execution — not a replay of the first.
}

// ─── Section 2: Hot Observable ────────────────────────────────────────────────
// A hot source exists independently of any subscription.
// Late subscribers do not receive past values unless replay is explicitly configured.
//
// Examples of hot sources:
//   DOM event (document.addEventListener / fromEvent)
//   WebSocket
//   Subject
//   shared stream (share / shareReplay)
//   global state store

// ─── Section 3: Subject ───────────────────────────────────────────────────────
// A Subject is both Observer and Observable.
// One pushed value fans out to all current subscribers.

function demoSubjectFanout(): void {
  const subject = new Subject<number>();

  subject.subscribe(v => console.log('[subject] A', v));
  subject.subscribe(v => console.log('[subject] B', v));

  subject.next(1);  // fans out: A 1, B 1
  subject.next(2);  // fans out: A 2, B 2
  subject.complete();
  // Expected: A 1, B 1, A 2, B 2
}

function demoLateSubscriberMissesValues(): void {
  // A standard Subject has no memory.
  // Values emitted before subscription are lost.
  const subject = new Subject<number>();

  subject.next(1);  // no subscribers yet — value is discarded

  subject.subscribe(v => console.log('[late-subject]', v));

  subject.next(2);  // subscriber is now present — receives 2
  subject.complete();
  // Expected: [late-subject] 2
  // Value 1 is gone — the subscriber joined too late
}

// ─── Section 4: Subject variants ─────────────────────────────────────────────
// | Type              | Memory policy                  |
// | Subject           | no memory                      |
// | BehaviorSubject   | remembers current value        |
// | ReplaySubject     | remembers configured history   |
// | AsyncSubject      | emits last value on completion |

function demoBehaviorSubject(): void {
  const behavior$ = new BehaviorSubject<number>(0);

  behavior$.next(1);
  behavior$.next(2);

  // Late subscriber immediately receives the current value (2)
  behavior$.subscribe(v => console.log('[behavior] late subscriber:', v));
  // Expected: [behavior] late subscriber: 2  ← current value delivered immediately

  behavior$.next(3);  // late subscriber also receives 3
  behavior$.complete();
}

function demoReplaySubject(): void {
  const replay$ = new ReplaySubject<number>(2);  // buffer last 2 values

  replay$.next(10);
  replay$.next(20);
  replay$.next(30);

  // Late subscriber receives the last 2 buffered values: 20, 30
  replay$.subscribe(v => console.log('[replay-2]', v));
  // Expected: [replay-2] 20, [replay-2] 30

  replay$.complete();
}

function demoAsyncSubject(): void {
  // AsyncSubject — the Subject equivalent of a resolved Promise.
  // Emits exactly one value (the last) when it completes.
  // Replays that value to any late subscriber.

  const async$ = new AsyncSubject<string>();

  async$.subscribe(v => console.log('[async] early subscriber:', v));

  async$.next('computing...');
  async$.next('done');
  async$.complete();
  // Expected: [async] early subscriber: done
  // (only the last value is emitted, and only on completion)

  // Late subscriber also receives 'done' — it replays like a resolved Promise
  async$.subscribe(v => console.log('[async] late subscriber:', v));
  // Expected: [async] late subscriber: done
}

// ─── Section 5: When to use a Subject / anti-pattern ─────────────────────────
/*
 * A Subject is the right tool in exactly two situations:
 *
 *   1. You need to push values imperatively from outside an Observable chain.
 *      Example: a component dispatches actions via subject.next(action).
 *
 *   2. You need to bridge a non-Observable system into RxJS when no creation
 *      operator already fits (fromEvent, fromEventPattern, interval, timer, defer).
 *
 * In all other cases, prefer cold creation operators.
 *
 * Anti-pattern — Subject wrapping an event when fromEvent already exists:
 *
 *   // WRONG — event listener leaks; unsubscribing the Subject does not remove it
 *   const clicks$ = new Subject<MouseEvent>();
 *   document.addEventListener('click', e => clicks$.next(e));
 *
 *   // CORRECT — fromEvent manages setup and teardown automatically
 *   const clicks$ = fromEvent<MouseEvent>(document, 'click');
 *
 * Decision rule:
 *   If fromEvent, fromEventPattern, interval, timer, or defer can model the source,
 *   use them. Reach for Subject only when no creation operator fits.
 */

// ─── Section 6: share ────────────────────────────────────────────────────────
// share() turns one source subscription into shared execution for all current
// subscribers. No replay. Late subscribers see only future values.
// When all subscribers unsubscribe, the source is unsubscribed.

function demoShare(): void {
  // Use a Subject to push values AFTER both subscribers are subscribed,
  // so they receive values concurrently and share the same execution.
  const trigger$ = new Subject<number>();
  let mapCallCount = 0;

  const processed$ = trigger$.pipe(
    map(v => {
      mapCallCount += 1;
      return v * 10;
    }),
    share()
  );

  processed$.subscribe(v => console.log('[share] A:', v));
  processed$.subscribe(v => console.log('[share] B:', v));
  // Both A and B are now subscribed — share() holds a single source subscription

  trigger$.next(1);  // map runs once → A: 10, B: 10
  trigger$.next(2);  // map runs once → A: 20, B: 20
  trigger$.complete();

  console.log('[share] map ran', mapCallCount, 'times for 2 values (not 4) — single execution');
  // Expected: map ran 2 times (not 4) — both subscribers share one source execution
}

// share() and completion: when the source completes, share() tears down and resets.
// A subsequent subscriber triggers a fresh source execution.

function demoShareCompletion(): void {
  // of(1,2,3) completes synchronously.
  // After completion share() resets; the next subscriber gets a fresh execution.
  const shared$ = of(1, 2, 3).pipe(share());

  shared$.subscribe(v => console.log('[share-complete] A:', v));
  // A: 1, 2, 3 — source completes; share() resets

  shared$.subscribe(v => console.log('[share-complete] B:', v));
  // B triggers a fresh execution — B: 1, 2, 3
  // Expected: A 1, A 2, A 3, B 1, B 2, B 3 (two separate executions)
}

// ─── Section 7: shareReplay ───────────────────────────────────────────────────
// shareReplay shares the source AND replays buffered values to late subscribers.
// refCount: true — unsubscribes from source when all subscribers leave.
// This is the standard pattern for cached read models.

function demoShareReplay(): void {
  const source$ = new Subject<string>();

  const cached$ = source$.pipe(
    shareReplay({ bufferSize: 1, refCount: true })
  );

  // Early subscriber
  cached$.subscribe(v => console.log('[shareReplay] early:', v));

  // Push a value — early subscriber receives it; it is also cached
  source$.next('loaded data');

  // Late subscriber — source does NOT restart; receives the buffered value immediately
  cached$.subscribe(v => console.log('[shareReplay] late:', v));
  // Expected: [shareReplay] late: loaded data  ← from buffer, no new source execution

  // Future values go to both
  source$.next('updated data');
  source$.complete();
}

// ─── Run all demos ────────────────────────────────────────────────────────────

demoColdObservable();
demoSubjectFanout();
demoLateSubscriberMissesValues();
demoBehaviorSubject();
demoReplaySubject();
demoAsyncSubject();
demoShare();
demoShareCompletion();
demoShareReplay();
