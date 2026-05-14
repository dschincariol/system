# Architecture Decision Records

This directory stores the repository's Architecture Decision Records (ADRs).

Use ADRs for long-lived, cross-cutting decisions that other contributors need to follow. ADRs are not meeting notes and they are not a replacement for the changelog.

## Format

- Filename: `NNNN-short-title.md`
- Numbering: sequential, zero-padded, never reused
- Required sections:
  - `Status`
  - `Date`
  - `Context`
  - `Decision`
  - `Consequences`

Recommended optional fields:

- `Supersedes`
- `Superseded by`

## Status Values

- `Accepted`
  The decision is active guidance.
- `Proposed`
  The decision is recorded but not yet accepted as repo policy.
- `Superseded`
  A later ADR replaced this one.

## When To Add A New ADR

Add an ADR when a change establishes or changes:

- a cross-cutting documentation standard
- a control-plane or ownership boundary
- an API contract format or specification approach
- a configuration or storage source of truth
- a runtime, execution, or operator-safety architecture rule

## When To Update An Existing ADR

Update an ADR when:

- its status changes
- it is superseded by a new ADR
- the wording needs clarification without changing the decision itself

If the decision changes materially, create a new ADR instead of rewriting history in place.

## Current ADRs

- [0001-documentation-architecture.md](0001-documentation-architecture.md)
- [0002-api-specification-choice.md](0002-api-specification-choice.md)
- [0003-docstring-standard.md](0003-docstring-standard.md)
- [0004-decision-log-format.md](0004-decision-log-format.md)
