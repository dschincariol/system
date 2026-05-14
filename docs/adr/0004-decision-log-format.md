# ADR 0004: Decision Log Format

- Status: Accepted
- Date: 2026-04-12

## Context

The repository previously lacked a defined ADR collection and a forward-maintained changelog. At the same time, there is not enough grounded release history in the current repo state to safely reconstruct a deep historical record.

## Decision

The repository will use two lightweight logs:

- ADRs in `docs/adr/` for cross-cutting architectural and governance decisions.
- `CHANGELOG.md` for notable changes from the documented baseline onward.

The ADR format is sequential, file-based, and forward-moving:

- ADRs use zero-padded numeric filenames.
- Accepted ADRs are not rewritten to simulate a different history.
- Materially changed decisions are superseded by new ADRs.

The changelog format is also forward-moving:

- Keep entries under `Unreleased` until a grounded release identifier exists.
- Do not fabricate older version headings.

## Consequences

- Contributors get a durable place to record decisions without pretending to know unsupported history.
- Governance changes become searchable and linkable.
- Historical reconstruction remains possible later, but only when grounded by tags, releases, or other reliable evidence.
