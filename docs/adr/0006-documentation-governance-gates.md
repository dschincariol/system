# ADR 0006: Documentation Governance Gates

- Status: Accepted
- Date: 2026-06-21

## Context

`tools/validate_docs.py` historically validated only structural properties: internal
link targets, ADR numbering, required-file presence, the OpenAPI baseline, and the
`CHANGELOG.md` `[Unreleased]` section. It did not check whether documentation kept pace
with the code it describes. That blind spot let three classes of drift accumulate
silently:

- new `engine/<subsystem>/` packages shipped with no README and no entry in
  `docs/DOCUMENTATION_INDEX.md`, so whole subsystems were undiscoverable;
- environment variables were read in code while never appearing in `.env.example` or
  `docs/REFERENCE_CONFIGURATION_GLOSSARY.md` (a backlog of roughly two thousand
  undocumented variables);
- the high-traffic map/index documents had no machine-checkable freshness signal.

A documentation audit fixed the accumulated gaps, but without enforcement the same gaps
would reappear. Per the ADR triggers in `docs/adr/README.md` and `CONTRIBUTING.md`
(establishing a long-lived documentation/governance standard), this decision is recorded
as an ADR.

## Decision

`tools/validate_docs.py` is extended with three content-coverage gates, kept in the same
print-and-return-nonzero style as the existing checks and run in CI through
`tools/validate_repo.py` (the `docs` sub-validator), which the `Validate` workflow already
executes:

1. **Subsystem-README coverage.** Every `engine/<subsystem>/` directory (excluding
   dunder/hidden directories such as `__pycache__`) must contain a `README.md` and have a
   matching link row in `docs/DOCUMENTATION_INDEX.md`.
2. **Environment-variable coverage.** Every env var read in code (via `os.getenv`,
   `os.environ.get`, or `os.environ[...]` across `engine`, `services`, `routes`, `tools`,
   `boot`, `ops`, `scripts`, and root `*.py`) must be documented in `.env.example`, in
   `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`, or in the frozen
   `docs/config_env_allowlist.txt`. The allowlist tolerates the pre-existing legacy
   backlog so the gate blocks only NEW undocumented variables; it shrinks as variables are
   documented and must never be padded to dodge the gate.
3. **Staleness sentinels.** The map/index documents (`CLAUDE.md`, `MAINTAINER_INDEX.md`,
   `README_DEVELOPER_MAP.md`, `README_ARCHITECTURE.md`, `README_FUNCTION_MAP.md`,
   `Database_Schema.md`) must each carry a `Last verified against code` line.

## Consequences

- New engine subsystems cannot merge without a README and an index row; new environment
  variables cannot merge undocumented; the map/index docs keep an explicit freshness
  marker. The drift classes above fail closed in CI instead of accumulating silently.
- The env-var backlog is frozen, not hidden: `docs/config_env_allowlist.txt` records the
  exact undocumented set and is expected to shrink over time. Documenting a variable and
  deleting its allowlist line is the supported way to reduce it.
- Contributors who add a subsystem, an env var, or who touch a tracked map doc may see the
  validator fail until they update the corresponding documentation — this is the intended
  behavior, and the fixes are mechanical.
