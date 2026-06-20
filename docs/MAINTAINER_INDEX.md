# Maintainer Index

This is the shortest canonical read path for engineers working in the repository.

Use [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) for the full documentation map. Use this file when you need the fastest path into code ownership and high-risk surfaces.

## Canonical Read Order

1. [README.md](../README.md)
2. [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md)
3. [REFERENCE_CONFIGURATION_GLOSSARY.md](REFERENCE_CONFIGURATION_GLOSSARY.md)
4. [start_system.py](../start_system.py)
5. [dashboard_server.py](../dashboard_server.py)
6. [engine/runtime/job_registry.py](../engine/runtime/job_registry.py)
7. [engine/runtime/jobs_manager.py](../engine/runtime/jobs_manager.py)
8. [engine/runtime/startup_orchestrator.py](../engine/runtime/startup_orchestrator.py)
9. The subsystem README for the area you will change

If the change touches provider setup, source lifecycle, or credential storage, insert [REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](REFERENCE_DATA_SOURCE_CONTROL_PLANE.md) before the subsystem README.

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

Changes in these files tend to have repo-wide effects:

- [engine/runtime/storage.py](../engine/runtime/storage.py)
- [engine/runtime/locks.py](../engine/runtime/locks.py)
- [engine/runtime/job_registry.py](../engine/runtime/job_registry.py)
- [engine/runtime/jobs_manager.py](../engine/runtime/jobs_manager.py)
- [engine/runtime/ingestion_runtime.py](../engine/runtime/ingestion_runtime.py)
- [start_system.py](../start_system.py)
- [dashboard_server.py](../dashboard_server.py)
- [routes/data_sources_routes.py](../routes/data_sources_routes.py)
- [services/data_source_manager.py](../services/data_source_manager.py)
- [boot/operator_server.js](../boot/operator_server.js)
- [services/operator_ai/agent.js](../services/operator_ai/agent.js)

## Task-Based Read Paths

If you are fixing startup or runtime stability:

1. [start_system.py](../start_system.py)
2. [dashboard_server.py](../dashboard_server.py)
3. [engine/runtime/startup_orchestrator.py](../engine/runtime/startup_orchestrator.py)
4. [engine/runtime/supervisor.py](../engine/runtime/supervisor.py)
5. [engine/runtime/ingestion_runtime.py](../engine/runtime/ingestion_runtime.py)
6. [engine/api/api_system.py](../engine/api/api_system.py)

If you are changing provider setup or source health:

1. [REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](REFERENCE_DATA_SOURCE_CONTROL_PLANE.md)
2. [services/data_source_manager.py](../services/data_source_manager.py)
3. [routes/data_sources_routes.py](../routes/data_sources_routes.py)
4. [services/credential_encryption.py](../services/credential_encryption.py)
5. [ui/data_sources.html](../ui/data_sources.html)
6. [ui/data_sources.js](../ui/data_sources.js)

If you are changing model or strategy behavior:

1. [engine/strategy/README.md](../engine/strategy/README.md)
2. [engine/strategy/predictor.py](../engine/strategy/predictor.py)
3. The affected model, governance, or portfolio files
4. [engine/execution/README.md](../engine/execution/README.md) if downstream execution assumptions change

If you are changing execution behavior:

1. [engine/execution/README.md](../engine/execution/README.md)
2. [engine/execution/broker_router.py](../engine/execution/broker_router.py)
3. [engine/execution/execution_policy_engine.py](../engine/execution/execution_policy_engine.py)
4. [engine/execution/kill_switch.py](../engine/execution/kill_switch.py)
5. [engine/runtime/gates.py](../engine/runtime/gates.py)

If you are changing HTTP or operator surfaces:

1. [engine/api/README.md](../engine/api/README.md)
2. [engine/api/api_system.py](../engine/api/api_system.py)
3. [engine/api/api_ops.py](../engine/api/api_ops.py)
4. [engine/api/api_jobs.py](../engine/api/api_jobs.py)
5. [engine/api/api_market.py](../engine/api/api_market.py)
6. [dashboard_server.py](../dashboard_server.py)

## Documentation Rules

- Update the relevant subsystem README when behavior or ownership changes.
- Update [REFERENCE_CONFIGURATION_GLOSSARY.md](REFERENCE_CONFIGURATION_GLOSSARY.md) when environment or secret-management behavior changes.
- Update [REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](REFERENCE_DATA_SOURCE_CONTROL_PLANE.md) when data-source routes, payloads, storage, or lifecycle behavior change.
- Use [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) to decide whether a doc is canonical or supplementary before adding new prose.
- Do not treat `docs/handoff/*`, `docs/archive/*`, or [archive/README_UI_REDESIGN_PLAN.md](archive/README_UI_REDESIGN_PLAN.md) as canonical runtime truth.

## Practical Rules

- Keep startup ownership explicit. Do not split the same responsibility across `start_system.py`, `dashboard_server.py`, and the operator layer without documenting the boundary.
- Treat SQLite coordination code as control-plane infrastructure, not as local utility code.
- Preserve fail-closed behavior in execution and promotion paths unless a change explicitly relaxes that contract.
- Prefer updating documentation in the same change that modifies a contract, not in a later cleanup pass.
