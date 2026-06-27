import { describe, expect, it } from 'vitest';
import { TestScheduler } from 'rxjs/testing';
import { validSearchText } from '../module-12/custom-operators';

// ---------------------------------------------------------------------------
// Types and reducer – self-contained copy so the test file is independent
// ---------------------------------------------------------------------------

type SearchStatus = 'idle' | 'typing' | 'loading' | 'success' | 'failure';

type SearchState = {
  query: string;
  status: SearchStatus;
  results: string[];
  error: string | undefined;
};

const initialState: SearchState = {
  query: '',
  status: 'idle',
  results: [],
  error: undefined,
};

type SearchAction =
  | { type: 'QueryChanged'; query: string }
  | { type: 'SearchStarted' }
  | { type: 'SearchSucceeded'; results: string[] }
  | { type: 'SearchFailed'; error: string }
  | { type: 'SearchCleared' };

function reducer(state: SearchState, action: SearchAction): SearchState {
  switch (action.type) {
    case 'QueryChanged':
      return {
        ...state,
        query: action.query,
        status: action.query.length > 0 ? 'typing' : 'idle',
      };
    case 'SearchStarted':
      return { ...state, status: 'loading', error: undefined };
    case 'SearchSucceeded':
      return { ...state, status: 'success', results: action.results };
    case 'SearchFailed':
      return { ...state, status: 'failure', results: [], error: action.error };
    case 'SearchCleared':
      return initialState;
  }
}

// ---------------------------------------------------------------------------
// Reducer tests (pure function – no TestScheduler needed)
// ---------------------------------------------------------------------------

describe('Module 15 – Capstone Reducer', () => {
  it('QueryChanged with non-empty query → status: typing', () => {
    const state = reducer(initialState, { type: 'QueryChanged', query: 'rxjs' });
    expect(state.status).toBe('typing');
  });

  it('QueryChanged with empty string → status: idle', () => {
    const state = reducer(initialState, { type: 'QueryChanged', query: '' });
    expect(state.status).toBe('idle');
  });

  it('SearchStarted → status: loading, error: undefined', () => {
    const prior: SearchState = { ...initialState, error: 'previous error' };
    const state = reducer(prior, { type: 'SearchStarted' });
    expect(state.status).toBe('loading');
    expect(state.error).toBeUndefined();
  });

  it('SearchSucceeded → status: success, results populated', () => {
    const results = ['result1', 'result2'];
    const state = reducer(initialState, { type: 'SearchSucceeded', results });
    expect(state.status).toBe('success');
    expect(state.results).toEqual(results);
  });

  it('SearchFailed → status: failure, results: [], error set', () => {
    const state = reducer(initialState, { type: 'SearchFailed', error: 'network error' });
    expect(state.status).toBe('failure');
    expect(state.results).toEqual([]);
    expect(state.error).toBe('network error');
  });

  it('SearchCleared → returns initialState', () => {
    const dirtied: SearchState = {
      query: 'test',
      status: 'success',
      results: ['x'],
      error: undefined,
    };
    const state = reducer(dirtied, { type: 'SearchCleared' });
    expect(state).toEqual(initialState);
  });
});

// ---------------------------------------------------------------------------
// Marble test for validSearchText (requires TestScheduler)
// ---------------------------------------------------------------------------

describe('Module 15 – validSearchText marble test', () => {
  it('short query is filtered', () => {
    const scheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });
    scheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('a-b-c-|', { a: 'hi', b: 'hey', c: 'hello' });
      const result$ = source$.pipe(validSearchText(4, 0));
      expectObservable(result$).toBe('----c-|', { c: 'hello' });
    });
  });
});
