# Trading System

Production-grade supervised trading system repository with:

- a Python runtime for ingestion, feature generation, model execution, portfolio logic, and broker-facing execution
- a browser dashboard and terminal
- a local operator console and bounded operator-AI repair surface
- deployment, ops, and maintenance tooling

This file is the canonical documentation entrypoint for the repository. For the full documentation map, use [docs/DOCUMENTATION_INDEX.md](docs/DOCUMENTATION_INDEX.md).

## Supported Platform

This repository supports Linux only. Production and local development paths assume a Debian-family Linux host or container with bash, systemd-oriented service assets where applicable, POSIX process semantics, Postgres/Timescale, Redis, Node 20, and Python 3.11. Non-Linux launchers, deployment helpers, host-specific secret APIs, and CI runners are intentionally not supported.

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
5. [docs/DEPENDENCY_PROFILES.md](docs/DEPENDENCY_PROFILES.md)
6. [boot/README.md](boot/README.md)
7. [ui/README.md](ui/README.md)

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
- [docs/README_SEQUENCE_DIAGRAMS.md](docs/README_SEQUENCE_DIAGRAMS.md) and [docs/STATE_MACHINES.md](docs/STATE_MACHINES.md)
  Canonical workflow and state-transition references.
- [docs/DATA_CONTRACTS.md](docs/DATA_CONTRACTS.md)
  Current payload and persistence contracts that cross module boundaries.
- [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md), [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md), and [docs/PRODUCTION_CHECKLIST.md](docs/PRODUCTION_CHECKLIST.md)
  Canonical operational references for telemetry, failure handling, and production readiness checks.
- [docs/DEPENDENCY_PROFILES.md](docs/DEPENDENCY_PROFILES.md)
  CPU/default, NVIDIA CUDA, and reserved AMD/ROCm dependency profile selection, verification, and CPU rollback.
- [docs/CPU_POWER_POLICY.md](docs/CPU_POWER_POLICY.md)
  Boot-enforced CPU performance-profile policy for host `bart`, verifier output,
  revert steps, and ROCm/GPU thermal composition.
- [docs/PRODUCTION_BACKEND_CI.md](docs/PRODUCTION_BACKEND_CI.md), [docs/LIVE_READINESS_CHECKLIST.md](docs/LIVE_READINESS_CHECKLIST.md), and [docs/STAGING_PROD_PREFLIGHT_EVIDENCE.md](docs/STAGING_PROD_PREFLIGHT_EVIDENCE.md)
  Production-backend gate reproduction, live enablement readiness, and staging preflight evidence.
- [CONTRIBUTING.md](CONTRIBUTING.md), [CHANGELOG.md](CHANGELOG.md), [docs/DOCSTRING_STYLE.md](docs/DOCSTRING_STYLE.md), [docs/adr/README.md](docs/adr/README.md), [docs/openapi/README.md](docs/openapi/README.md), and [docs/LICENSING_NOTE.md](docs/LICENSING_NOTE.md)
  Documentation governance, change tracking, docstring standards, decision records, API-spec location, and current licensing status.
- [docs/README_DATABASE_MAP.md](docs/README_DATABASE_MAP.md), [docs/Database_Schema.md](docs/Database_Schema.md), [docs/Audit_Chain_Spec.md](docs/Audit_Chain_Spec.md), and [docs/Secrets_Rotation_Runbook.md](docs/Secrets_Rotation_Runbook.md)
  Canonical database-family, schema-classification, audit-chain, and secrets-rotation references.
- Subsystem READMEs under `engine/`, plus [boot/README.md](boot/README.md), [services/README.md](services/README.md), and [ui/README.md](ui/README.md)
  Canonical local entrypoints for subsystem-level ownership.
- [ops/README.md](ops/README.md) and [deploy/README.md](deploy/README.md)
  Supplementary maps for ad hoc operational utilities and deployment automation.

Supplementary but useful:

- [docs/README_ARCHITECTURE.md](docs/README_ARCHITECTURE.md)
- [docs/SEQUENCE_DIAGRAMS_EXTENDED.md](docs/SEQUENCE_DIAGRAMS_EXTENDED.md)
- [docs/README_DEVELOPER_MAP.md](docs/README_DEVELOPER_MAP.md)
- [docs/README_FUNCTION_MAP.md](docs/README_FUNCTION_MAP.md)
- [docs/archive/README.md](docs/archive/README.md)
- [docs/handoff/README.md](docs/handoff/README.md)
- [docs/codex_prompts/README.md](docs/codex_prompts/README.md)

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
- On Linux/macOS development machines, run `bash tools/bootstrap_local_toolchain.sh` from the repo root. It creates or updates `.venv` with Python 3.11, installs the selected Python dependency profile (`TRADING_DEPENDENCY_PROFILE=cpu` by default), installs Node.js 20.19.4 with npm 10.8.2 inside `.venv` when needed, runs `npm ci`, and links the `python`, `python3`, `node`, `npm`, and `npx` command names into `$HOME/.local/bin` by default.
- Install CPU/default Python dependencies with `TRADING_DEPENDENCY_PROFILE=cpu python -m pip install -r requirements.txt`. Use [docs/DEPENDENCY_PROFILES.md](docs/DEPENDENCY_PROFILES.md) before selecting NVIDIA CUDA or AMD/ROCm profiles.
- Use Node.js 20 LTS (`>=20.17.0 <21`) with npm 10.x for the operator UI. The checked-in `.npmrc` enforces this during `npm ci`.
- Install Node dependencies reproducibly with `npm ci`; do not edit or vendor `node_modules/`.
- Prefer `ENGINE_MODE=safe` and `EXECUTION_MODE=safe` until the environment, providers, and operator controls are verified.
- Run `python tools/check_repo_artifact_hygiene.py --report` before staging broad changes. Local `.env*`, `.venv/`, `node_modules/`, `__pycache__/`, and `var/` state may exist on disk, but only `*.env.example` templates may be tracked.
- Run `npm run check:ui` after `npm ci` to validate local asset references, dashboard JS syntax, browser-helper tests, and the fast chart contract pytest lane before shipping UI changes. The chart lane covers risk chart API shapes, risk chart UI helpers, portfolio backtest chart contracts, and model performance divergence frontend behavior.
- Run `npm run test:ui` when a UI change needs the broader UI pytest allowlist in addition to the browser-helper tests.
- Run `npm run test:py` for the canonical Python test suite. It runs `python -m pytest tests/ -v --tb=short`; `unittest.TestCase` tests are collected and executed by pytest.
- Run `python tools/validate_repo.py` before merging. The validator runs pytest collection before pytest execution and the default startup graph gate is hermetic even when local `.env` points at Postgres, Timescale, or Redis.
- Pytest uses the canonical timeout policy in `pyproject.toml`: every test has a 120 second default timeout through `pytest-timeout` with `timeout_method=thread`. Intentionally slow tests must use a local `@pytest.mark.timeout(<seconds>)` override with a short reason near the test; do not disable timeouts through broad markers or suite-wide command flags.
- Pytest blocks DNS and non-local sockets by default. Local test servers, Postgres, and Redis must bind to loopback or Unix-domain sockets; live broker or market-data tests must be marked `@pytest.mark.live_network` and run explicitly with `TRADING_TEST_ALLOW_LIVE_NETWORK=1`.
- Reproduce the Postgres/Redis production-backend CI gate with [docs/PRODUCTION_BACKEND_CI.md](docs/PRODUCTION_BACKEND_CI.md) when changing runtime storage, Redis cache wrappers, migrations, audit-chain behavior, execution arming, or promotion evidence.
- Run `python tools/validate_repo.py --live` only when a running operator plus engine instance is available and a bounded live smoke test is intended. This mode preserves production dependency requirements for Postgres/Timescale and Redis.

Deterministic local gate commands after bootstrap:

```bash
python --version
python tools/git_worktree_triage.py
python tools/check_repo_artifact_hygiene.py
python -m pytest --version
npm run test:py
node --version
npm --version
npm run check:ui
npm run test:ui
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

- `var/`
- legacy local outputs such as `data/`, `logs/`, `tmp/`, `.run-audit/`, `artifacts/`, and `models/`
- `__pycache__/`
- `node_modules/`

Use them for diagnostics, not for ownership or contract discovery. See
[docs/RUNTIME_STATE.md](docs/RUNTIME_STATE.md) for the ignored local layout.
