# Changelog

All notable changes to repository contracts, documentation governance, and operator-relevant behavior should be recorded here.

This changelog starts on 2026-04-12. Earlier repository history has not been retroactively reconstructed because the current repo state does not provide enough grounded release history to do that safely.

## How To Use This File

- Keep new entries under `Unreleased` until there is a grounded release identifier, tag, or versioning rule to attach them to.
- Prefer `Added`, `Changed`, `Fixed`, `Removed`, and `Security` headings.
- Record documentation-governance changes when they alter contributor expectations, canonical references, or external-facing contracts.
- Do not fabricate historical versions.

## [Unreleased]

### Added

- `CONTRIBUTING.md` defining documentation update expectations, ADR triggers, OpenAPI update rules, configuration glossary update rules, and validation expectations.
- `docs/DOCSTRING_STYLE.md` with repository-specific NumPy-style docstring guidance.
- `docs/adr/` with an ADR index and the initial governance ADR set.
- `docs/openapi/` as the canonical home for the incremental OpenAPI source of truth.
- `docs/LICENSING_NOTE.md` documenting that the repository currently has no repo-wide license file.
- `tools/validate_docs.py` for lightweight documentation validation.

### Changed

- `docs/DOCUMENTATION_INDEX.md` updated to include governance, decision-log, licensing, and OpenAPI-baseline docs.
- `docs/DOCS_AUDIT.md` updated to reflect the new governance layer and the remaining documentation gaps.
- `README.md` updated so the canonical documentation set and documentation conventions point at the new governance artifacts.
- `tools/validate_repo.py` now runs documentation validation as part of the canonical repository validation workflow.
- Production handover documentation refreshed for broker configuration, live-execution safety, terminal pre-trade rejection rows, alert lifecycle state, backup evidence, and current storage/schema ownership.
