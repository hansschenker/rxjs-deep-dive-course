# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run dev       # Start Vite dev server with HMR
npm run build     # Type-check with tsc, then Vite production build
npm run preview   # Preview the production build locally
```

`vitest` is installed as a dependency but no test script is wired up yet. Run tests with `npx vitest` once test files exist.

## Architecture

**Stack:** Vite + TypeScript (ES2023, strict) + RxJS 7 + VitePress

This is a course platform for teaching reactive programming. The current `src/` contains only the initial Vite boilerplate (`main.ts` entry point, `counter.ts` widget, `style.css`). Each of the 16 course modules will be built here.

**VitePress** (`vitepress@^1.6.4`) is installed for course documentation/content pages alongside the interactive demo app.

## Course Philosophy

The central thesis: *"The domain can change. The RxJS machine stays the same."*

RxJS is treated as a declarative language for values moving over time:
- An Observable is a **lazy description** of a dataflow — nothing runs until subscribed
- Operators **rewire** the stream; user functions **transform values inside** the stream
- Time, cancellation, sharing, errors, and completion are **explicit architectural decisions**

## Course Modules (16 total)

1. Course Setup and Mental Model
2. History and Lineage
3. The Observable Contract
4. Creation and Boundaries
5. Operators as Behavior Stories
6. Flattening Policies
7. Time, Rate Limiting, and Schedulers
8. Combining Streams
9. Error and Recovery Policies
10. Hot, Cold, and Shared Streams
11. State as a Stream
12. TypeScript and Runtime Safety
13. Custom Operators and DSL Design
14. Testing with Virtual Time
15. Framework Integration
16. Capstone Project

## TypeScript Config Notes

- `noUnusedLocals` and `noUnusedParameters` are enforced — remove unused identifiers
- `verbatimModuleSyntax` is on — use `import type` for type-only imports
- No `tsc --emit`; Vite handles transpilation; `tsc` is type-check only
