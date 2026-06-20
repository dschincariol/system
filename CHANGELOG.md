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

- Consolidated readiness evidence API/UI: `/api/operator/readiness_evidence` now normalizes runtime, execution, broker, provider, data-source, governance, production-monitoring, and probe evidence into actionable blocker rows, and the dashboard shows a Readiness Evidence card with broker activation pre-checks.
- Execution diagnostics API/UI: `/api/execution/diagnostics` now aggregates by-symbol TCA, rolling slippage and latency, partial/rejected/suppressed outcomes, LOB/DeepLOB readiness, and learned-slicing audit evidence with explicit stale, unavailable, and shadow-only states without changing live execution authority.
- First-class job catalog API/UI: `/api/jobs/catalog`, richer `/api/jobs` job metadata, backend-owned safety/prerequisite/action policy fields, and a dashboard Job Catalog with filters, grouped rows, latest state, and log links.
- Governance Evidence Center API/UI: `/api/governance/evidence`, evidence drilldowns, and `/api/governance/shadow_capital/scores` now surface promotion evidence, generated-candidate provenance, model-risk controls, production monitoring, and shadow-capital score state without changing promotion or allocation authority.
- Structured-document and graph-feature visibility: `/api/data/feature_visibility`, Data Health panels, and decision attribution metadata now show extraction counts, freshness, confidence, lineage, graph snapshot availability, PIT status, and shadow-only labels without changing live trading authority.
- `CONTRIBUTING.md` defining documentation update expectations, ADR triggers, OpenAPI update rules, configuration glossary update rules, and validation expectations.
- `docs/DOCSTRING_STYLE.md` with repository-specific NumPy-style docstring guidance.
- `docs/adr/` with an ADR index and the initial governance ADR set.
- `docs/openapi/` as the canonical home for the incremental OpenAPI source of truth.
- `docs/LICENSING_NOTE.md` documenting that the repository currently has no repo-wide license file.
- `tools/validate_docs.py` for lightweight documentation validation.
- Shadow-only Chronos time-series foundation encoder features registered as `tsfm.chronos_v2.*`, with PIT metadata, artifact manifest provenance, optional dependency gating, and live-serving rejection for shadow feature contracts.

### Changed

- Operator console high-impact actions now use the shared structured
  confirmation modal and sidecar/API confirmation validation/audit contract
  instead of native browser prompt/confirm flows.
- Non-`sim` broker activation now requires a fresh passing connection test for the same broker; `BROKER_CONNECTION_TEST_MAX_AGE_S` controls the freshness window.
- Dashboard quick job buttons and command-palette job actions now consume backend job catalog safety metadata instead of browser-only unsafe-job name matching.
- `docs/DOCUMENTATION_INDEX.md` updated to include governance, decision-log, licensing, and OpenAPI-baseline docs.
- `docs/DOCS_AUDIT.md` updated to reflect the new governance layer and the remaining documentation gaps.
- `README.md` updated so the canonical documentation set and documentation conventions point at the new governance artifacts.
- `tools/validate_repo.py` now runs documentation validation as part of the canonical repository validation workflow.
- Production handover documentation refreshed for broker configuration, live-execution safety, terminal pre-trade rejection rows, alert lifecycle state, backup evidence, and current storage/schema ownership.
- Production disk retention now caps compose stdout/stderr, tightens file-log rotation to `maxsize 50M` with 10 compressed rotations and `maxage 21`, and surfaces backup accounting retention status plus container mount source in preflight.
