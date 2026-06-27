---
layout: home

hero:
  name: RxJS Deep Dive
  text: Reactive Architecture, Operator Policies, and State Streams
  tagline: The domain can change. The RxJS machine stays the same.
  actions:
    - theme: brand
      text: Start with Module 0
      link: /modules/00-setup
    - theme: alt
      text: Jump to Capstone
      link: /modules/15-capstone

features:
  - title: Observable as a Language
    details: An Observable is a lazy description of a dataflow. Nothing runs until something subscribes. Operators rewire the stream. User functions transform the values inside.
  - title: 8-Policy Framework
    details: Every operator can be read through eight dimensions — Source, Trigger, Value, Cardinality, Time, Concurrency, Cancellation, and Termination.
  - title: Four Layers
    details: Observable → Operator → Policy → Architecture. Each layer builds on the previous. The same small set of policies appears everywhere.
---

## Course Overview

This course teaches RxJS in 16 modules, from first principles to production architecture.

| Module | Topic |
| --- | --- |
| 0 | [Setup and Mental Model](/modules/00-setup) |
| 1 | [History and Lineage](/modules/01-history) |
| 2 | [The Observable Contract](/modules/02-observable-contract) |
| 3 | [Creation and Boundaries](/modules/03-creation) |
| 4 | [Operators as Behavior Stories](/modules/04-operators) |
| 5 | [Flattening Policies](/modules/05-flattening) |
| 6 | [Time, Rate Limiting, and Schedulers](/modules/06-time) |
| 7 | [Combining Streams](/modules/07-combining) |
| 8 | [Error and Recovery Policies](/modules/08-error) |
| 9 | [Hot, Cold, and Shared Streams](/modules/09-hot-cold) |
| 10 | [State as a Stream](/modules/10-state) |
| 11 | [TypeScript and Runtime Safety](/modules/11-typescript) |
| 12 | [Custom Operators and DSL Design](/modules/12-custom-operators) |
| 13 | [Testing with Virtual Time](/modules/13-testing) |
| 14 | [Framework Integration](/modules/14-framework) |
| 15 | [Capstone Project](/modules/15-capstone) |
