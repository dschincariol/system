# Engine Overview

The `engine/` tree contains the Python application code. Most new work lands in one of these subsystems:

- [runtime/README.md](runtime/README.md)
  Boot, lifecycle, storage, jobs, orchestration, and supervision.
- [data/README.md](data/README.md)
  External data adapters, provider routing, ingestion, and source jobs.
- [strategy/README.md](strategy/README.md)
  Features, labels, models, predictions, and portfolio logic.
- [execution/README.md](execution/README.md)
  Broker integrations, routing, execution safety, and attribution.
- [research/README.md](research/README.md)
  Offline stress, fragility, and analysis helpers that consume existing runtime outputs.
- [api/README.md](api/README.md)
  HTTP handlers used by the dashboard and operator.
- [risk/README.md](risk/README.md)
  Risk engines and portfolio risk calculations.
- [terminal/README.md](terminal/README.md)
  Terminal-focused API handlers that back the standalone browser terminal and gated order-entry flow.
- `jobs/`
  Legacy or compatibility job entrypoints outside the runtime/data/strategy/execution grouping.

## High-Value Top-Level Files

- [app.py](app.py)
  General app entry/wiring module.
- [model_registry.py](model_registry.py)
  Registry and lookup logic for stored models.
- [training_guard.py](training_guard.py)
  Training safety and gating logic.

## Working Model

Think of `engine/` as a layered system:

1. `runtime` keeps the process alive and consistent.
2. `data` produces facts.
3. `strategy` converts facts into decisions.
4. `execution` turns allowed decisions into broker actions.
5. `api` exposes all of the above to the UI and operator tooling.

If you change a lower layer, assume upper layers will feel it.
