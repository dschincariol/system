# Trading System

Production-grade supervised trading system repository with:

- a Python runtime for ingestion, feature generation, model execution, portfolio logic, and broker-facing execution
- a browser dashboard and terminal
- a local operator console and bounded operator-AI repair surface
- deployment, ops, and maintenance tooling

This file is the canonical documentation entrypoint for the repository. For the full documentation map, use [docs/DOCUMENTATION_INDEX.md](docs/DOCUMENTATION_INDEX.md).

## Start Here

Engineer read path:

1. [docs/DOCUMENTATION_INDEX.md](docs/DOCUMENTATION_INDEX.md)
2. [docs/MAINTAINER_INDEX.md](docs/MAINTAINER_INDEX.md)
3. [docs/REFERENCE_CONFIGURATION_GLOSSARY.md](docs/REFERENCE_CONFIGURATION_GLOSSARY.md)
4. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
5. [docs/DATA_CONTRACTS.md](docs/DATA_CONTRACTS.md)
6. [start_system.py](start_system.py)
7. [dashboard_server.py](dashboard_server.py)
8. The subsystem README for the area you are changing

Operator read path:

1. [docs/README_OPERATOR_GUIDE.md](docs/README_OPERATOR_GUIDE.md)
2. [docs/REFERENCE_CONFIGURATION_GLOSSARY.md](docs/REFERENCE_CONFIGURATION_GLOSSARY.md)
3. [docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md)
4. [docs/PRODUCTION_CHECKLIST.md](docs/PRODUCTION_CHECKLIST.md)
5. [boot/README.md](boot/README.md)
6. [ui/README.md](ui/README.md)

## Runtime Topology

- [start_system.py](start_system.py)
  Main supervised runtime entrypoint. Bootstraps the environment, records startup phases, validates imports and runtime health, starts the dashboard server, and supervises ingestion.
- [dashboard_server.py](dashboard_server.py)
  Main HTTP and browser-asset boundary. Serves `ui/`, assembles `engine/api/` route groups, mounts focused route modules such as [routes/data_sources_routes.py](routes/data_sources_routes.py), and exposes a large operator-facing `/api/*` surface.
- [start_ingestion.py](start_ingestion.py)
  Thin wrapper that bootstraps ingestion identity, repairs the DB if needed, and hands off to [engine/runtime/ingestion_runtime.py](engine/runtime/ingestion_runtime.py).
- [engine/runtime/README.md](engine/runtime/README.md)
  Control plane: storage, locks, lifecycle, job registry, job manager, startup orchestration, health, ingestion supervision, and crash recovery.
- [engine/data/README.md](engine/data/README.md)
  Data acquisition and normalization.
- [engine/strategy/README.md](engine/strategy/README.md)
  Features, labels, model lifecycle, prediction, governance, and portfolio intent.
- [engine/execution/README.md](engine/execution/README.md)
  Execution policy, broker routing, kill switches, fills, and attribution.
- [engine/api/README.md](engine/api/README.md)
  HTTP handlers and transport plumbing for dashboard and operator APIs.
- [engine/terminal/README.md](engine/terminal/README.md)
  Browser terminal read APIs, chart/bootstrap flow, and risk-gated terminal order-entry surface.
- [engine/risk/README.md](engine/risk/README.md)
  Portfolio-risk and Monte Carlo risk engines surfaced through the API and execution barrier.

## Canonical Control Surfaces

- Dashboard: [ui/dashboard.html](ui/dashboard.html)
- Data-source management: [ui/data_sources.html](ui/data_sources.html)
- Broker configuration: dashboard broker panel backed by `/api/broker/config`, `/api/broker/test_connection`, and `/api/broker/audit`
- Operator console: [boot/operator_ui.html](boot/operator_ui.html) and [boot/operator_server.js](boot/operator_server.js)
- Browser terminal: [ui/terminal/terminal.html](ui/terminal/terminal.html)

Data-source configuration has one canonical contract:

- provider credentials and source-specific settings live in the `data_sources` table
- stored credentials are encrypted at rest by [services/credential_encryption.py](services/credential_encryption.py)
- [services/data_source_manager.py](services/data_source_manager.py) is the source-of-truth manager
- [routes/data_sources_routes.py](routes/data_sources_routes.py) is the HTTP control-plane surface
- `.env` is bootstrap and system configuration, not the long-lived source of truth for provider credentials

## Canonical Documentation Set

- [docs/DOCUMENTATION_INDEX.md](docs/DOCUMENTATION_INDEX.md)
  Canonical Diataxis map for the repository docs.
- [docs/MAINTAINER_INDEX.md](docs/MAINTAINER_INDEX.md)
  Shortest path for engineers reading or changing the codebase.
- [docs/REFERENCE_CONFIGURATION_GLOSSARY.md](docs/REFERENCE_CONFIGURATION_GLOSSARY.md)
  Configuration and environment contract.
- [docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md)
  Explicit contract for the data-source UI, routes, storage, and runtime lifecycle behavior.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
  Grounded runtime architecture reference assembled from the current entrypoints, APIs, and subsystems.
- [docs/DATA_CONTRACTS.md](docs/DATA_CONTRACTS.md)
  Current payload and persistence contracts that cross module boundaries.
- [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md), [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md), and [docs/PRODUCTION_CHECKLIST.md](docs/PRODUCTION_CHECKLIST.md)
  Canonical operational references for telemetry, failure handling, and production readiness checks.
- [CONTRIBUTING.md](CONTRIBUTING.md), [CHANGELOG.md](CHANGELOG.md), [docs/DOCSTRING_STYLE.md](docs/DOCSTRING_STYLE.md), [docs/adr/README.md](docs/adr/README.md), [docs/openapi/README.md](docs/openapi/README.md), and [docs/LICENSING_NOTE.md](docs/LICENSING_NOTE.md)
  Documentation governance, change tracking, docstring standards, decision records, API-spec location, and current licensing status.
- [docs/README_DATABASE_MAP.md](docs/README_DATABASE_MAP.md)
  Canonical database-family reference until a schema reference is generated directly from code.
- Subsystem READMEs under `engine/`, plus [boot/README.md](boot/README.md), [services/README.md](services/README.md), [ui/README.md](ui/README.md), [ops/README.md](ops/README.md), and [deploy/README.md](deploy/README.md)
  Canonical local entrypoints for subsystem-level ownership.

Supplementary but useful:

- [docs/README_ARCHITECTURE.md](docs/README_ARCHITECTURE.md)
- [docs/README_SEQUENCE_DIAGRAMS.md](docs/README_SEQUENCE_DIAGRAMS.md)
- [docs/README_DEVELOPER_MAP.md](docs/README_DEVELOPER_MAP.md)
- [docs/README_FUNCTION_MAP.md](docs/README_FUNCTION_MAP.md)
- [docs/README_UI_REDESIGN_PLAN.md](docs/README_UI_REDESIGN_PLAN.md)
- `docs/handoff/*`

## Repository Map

- [engine/README.md](engine/README.md)
  Python application map across runtime, data, strategy, execution, research, API, risk, terminal, and legacy jobs.
- [boot/README.md](boot/README.md)
  Local launcher and operator boundary.
- [services/README.md](services/README.md)
  Sidecar services, including the data-source manager and operator AI.
- [ui/README.md](ui/README.md)
  Browser assets, dashboard modules, and the data-source control center.
- [ops/README.md](ops/README.md)
  Ad hoc operational utilities and offline analytics helpers.
- [deploy/README.md](deploy/README.md)
  Hosted-install and service automation.

## Local Bootstrap And Validation

- Copy [.env.example](.env.example) to `.env` for a new workstation.
- On Linux/macOS development machines, run `bash tools/bootstrap_local_toolchain.sh` from the repo root. It creates or updates `.venv` with Python 3.11, installs `requirements.txt`, installs Node.js 20.19.4 with npm 10.8.2 inside `.venv` when needed, runs `npm ci`, and links the `python`, `python3`, `node`, `npm`, and `npx` command names into `$HOME/.local/bin` by default.
- Install Python dependencies with `python -m pip install -r requirements.txt`.
- Use Node.js 20 LTS (`>=20.17.0 <21`) with npm 10.x for the operator UI. The checked-in `.npmrc` enforces this during `npm ci`.
- Install Node dependencies reproducibly with `npm ci`; do not edit or vendor `node_modules/`.
- Prefer `ENGINE_MODE=safe` and `EXECUTION_MODE=safe` until the environment, providers, and operator controls are verified.
- Run `npm run check:ui` after `npm ci` to validate local asset references, dashboard JS syntax, and browser-helper tests before shipping UI changes.
- Run `python tools/validate_repo.py` before merging.
- Run `python tools/validate_repo.py --live` only when a running operator plus engine instance is available and a bounded live smoke test is intended.

Deterministic local gate commands after bootstrap:

```bash
python --version
python tools/git_worktree_triage.py
python -m pytest --version
node --version
npm --version
npm run check:ui
python tools/validate_dependency_lock.py
```

Useful supporting checks:

- [tools/validate_docs.py](tools/validate_docs.py)
- [tools/news_ingestion_selftest.py](tools/news_ingestion_selftest.py)
- [tools/pipeline_smoke_test.py](tools/pipeline_smoke_test.py)
- [tools/research_stress_smoke.py](tools/research_stress_smoke.py)

## Documentation Conventions

- Treat [docs/DOCUMENTATION_INDEX.md](docs/DOCUMENTATION_INDEX.md) as the canonical map.
- When behavior changes, update the relevant subsystem README and any affected reference document in the same change.
- Python public APIs should follow [docs/DOCSTRING_STYLE.md](docs/DOCSTRING_STYLE.md) and remain compatible with Sphinx and Napoleon-style tooling.
- HTTP surfaces should be documented incrementally in [docs/openapi/openapi.yaml](docs/openapi/openapi.yaml) rather than relying only on route lists inside implementation files.
- Architecture decisions that change control-plane boundaries should be captured as ADRs under [docs/adr/](docs/adr/README.md).

## Runtime State

These paths are runtime state, not canonical documentation sources:

- `data/`
- `logs/`
- `__pycache__/`
- `node_modules/`

Use them for diagnostics, not for ownership or contract discovery.
