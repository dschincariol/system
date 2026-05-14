# ADR 0001: Documentation Architecture

- Status: Accepted
- Date: 2026-04-12

## Context

The repository already has a strong but growing documentation set: a top-level `README.md`, subsystem READMEs, cross-cutting reference docs under `docs/`, and supplementary planning or handoff material. Without an explicit architecture for those documents, ownership becomes unclear and duplication grows.

## Decision

The repository will use a layered documentation architecture:

- `README.md` is the canonical repository entrypoint.
- `docs/DOCUMENTATION_INDEX.md` is the canonical map of document types and status.
- Subsystem READMEs own local boundaries for the area they document.
- Cross-cutting reference documents under `docs/` own repo-wide contracts such as configuration, data-source control plane behavior, and docstring standards.
- `docs/adr/` stores long-lived architecture decisions.
- `CHANGELOG.md` records notable changes from the documented baseline onward.
- `docs/handoff/` and planning documents remain supplementary and must not override canonical docs.

## Consequences

- Contributors must update the closest owning document instead of adding parallel summaries elsewhere.
- Canonical documents should link to each other instead of repeating the same ownership lists.
- Supplementary documents may explain or plan work, but they do not define runtime truth.
