# Codex Staged Migration Program

## Status

Accepted

## Date

2026-04-16

## Context

The repository is large, operationally sensitive, and already partially through a storage and runtime cutover. Broad one-shot prompts have too much surface area to stay accurate, complete, and safe across runtime, data, strategy, execution, and UI boundaries.

The migration work needs a repo-local process that:

- keeps every change bounded
- makes prompt intent auditable after the fact
- separates implementation from verification
- records slice state outside model memory

## Decision

The repository will use a staged Codex migration program with these rules:

- every migration slice runs through a strict `DD -> IMPL -> AUDIT` prompt loop
- each slice is limited to one boundary change
- each slice must define explicit in-scope files, out-of-scope files, verification commands, acceptance criteria, and stop conditions
- implementation prompts may not self-certify completeness
- slice state must be recorded in a repo-local ledger under `docs/handoff/codex_migration/`
- the seeded first slice is `S01`, focused on keeping raw quote evidence and related telemetry off the immediate SQLite live-stream path

## Consequences

- large migrations become slower in prompt count, but safer and easier to audit
- future contributors can resume work from the ledger and prompt pack instead of relying on chat history
- every completed slice should have an implementation prompt, an audit prompt, and recorded verification evidence
- repo-local prompt packs become part of the working handoff material, but they do not replace canonical runtime documentation
