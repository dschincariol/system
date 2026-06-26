# Maintainer Index

This is the shortest canonical read path for engineers working in the repository.

Last verified against code: 2026-06-26

Use [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) for the full documentation map. Use this file when you need the fastest path into code ownership and high-risk surfaces.

## Fastest Read Order

For orientation, read in this order, then stop once you reach the subsystem you will change:

1. [README.md](../README.md)
2. [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md)
3. [start_system.py](../start_system.py) and [dashboard_server.py](../dashboard_server.py)
4. [engine/runtime/job_registry.py](../engine/runtime/job_registry.py) and [engine/runtime/startup_orchestrator.py](../engine/runtime/startup_orchestrator.py)
5. The subsystem README for the area you will change

For the full canonical read order (including the data-source/credential insert and the supporting reference maps), see [README_DEVELOPER_MAP.md](README_DEVELOPER_MAP.md).

## Canonical Subsystem Docs

- Runtime control plane:
  [engine/runtime/README.md](../engine/runtime/README.md)
- Data ingestion and providers:
  [engine/data/README.md](../engine/data/README.md)
- Strategy, models, and portfolio logic:
  [engine/strategy/README.md](../engine/strategy/README.md)
- Execution and broker routing:
  [engine/execution/README.md](../engine/execution/README.md)
- Dashboard and operator APIs:
  [engine/api/README.md](../engine/api/README.md)
- Risk engines:
  [engine/risk/README.md](../engine/risk/README.md)
- Browser terminal:
  [engine/terminal/README.md](../engine/terminal/README.md)
- Browser surfaces:
  [ui/README.md](../ui/README.md)
- Operator launcher and guarded repair layer:
  [boot/README.md](../boot/README.md)
- Sidecar services:
  [services/README.md](../services/README.md)

## Highest-Risk Files

Changes in these files tend to have repo-wide effects. The most central few to keep in mind before any change:

- [engine/runtime/storage.py](../engine/runtime/storage.py) and [engine/runtime/locks.py](../engine/runtime/locks.py)
- [start_system.py](../start_system.py) and [dashboard_server.py](../dashboard_server.py)
- [engine/runtime/job_registry.py](../engine/runtime/job_registry.py)

For the full highest-risk-file list with why-it-is-sensitive notes, see [README_DEVELOPER_MAP.md](README_DEVELOPER_MAP.md) (Critical Infrastructure).

## Task-Based Read Paths

For task-based navigation (fixing startup or runtime stability, changing provider setup or source health, model/strategy, execution, broker activation, alerts, and HTTP/operator surfaces), see the canonical task paths in [README_DEVELOPER_MAP.md](README_DEVELOPER_MAP.md) (Where To Make Common Changes).

Start any task from the subsystem README for the area you will change:

- Runtime control plane: [engine/runtime/README.md](../engine/runtime/README.md)
- Data ingestion and providers: [engine/data/README.md](../engine/data/README.md)
- Strategy and portfolio: [engine/strategy/README.md](../engine/strategy/README.md)
- Execution and broker routing: [engine/execution/README.md](../engine/execution/README.md)
- Dashboard and operator APIs: [engine/api/README.md](../engine/api/README.md)

## Documentation Rules

- Update the relevant subsystem README when behavior or ownership changes.
- Use [DECOMPOSITION_CONVENTIONS.md](DECOMPOSITION_CONVENTIONS.md) before splitting oversized modules; characterize behavior first and keep the original module as a compatibility facade.
- Update [REFERENCE_CONFIGURATION_GLOSSARY.md](REFERENCE_CONFIGURATION_GLOSSARY.md) when environment or secret-management behavior changes.
- Update [REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](REFERENCE_DATA_SOURCE_CONTROL_PLANE.md) when data-source routes, payloads, storage, or lifecycle behavior change.
- Use [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) to decide whether a doc is canonical or supplementary before adding new prose.
- Do not treat `docs/handoff/*`, `docs/archive/*`, or [archive/README_UI_REDESIGN_PLAN.md](archive/README_UI_REDESIGN_PLAN.md) as canonical runtime truth.

## Practical Rules

- Keep startup ownership explicit. Do not split the same responsibility across `start_system.py`, `dashboard_server.py`, and the operator layer without documenting the boundary.
- Treat SQLite coordination code as control-plane infrastructure, not as local utility code.
- Preserve fail-closed behavior in execution and promotion paths unless a change explicitly relaxes that contract.
- Prefer updating documentation in the same change that modifies a contract, not in a later cleanup pass.
