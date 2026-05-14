# Full Scope Validation Matrix

As of `2026-04-24`, the staged Codex repo migration program is complete, and the current architecture has additional startup, live-ingestion, preflight, compose, dependency, and operator hardening in repo. Docker Desktop is available on this workstation, compose build/up succeeds for the current safe-mode architecture, and production preflight passes locally and inside the runtime container. The actual target deployment is still not production-ready because live smoke does not pass without configured live market-data provider credentials and fresh price ticks.

This document is the signoff matrix for that distinction.

## Decision Rule

Use these three gates in order:

1. `Repo implementation complete`
   - All planned slices `S01-S12` are implemented, audited, and integration-gated.
   - The deterministic repository validation entrypoint passes.
2. `Architecture complete`
   - The repo gate passes.
   - Every target-state architecture item below is either `Complete in repo` and `Proven in deployment`, or it is explicitly marked out of scope.
3. `Production ready`
   - The architecture gate passes.
   - Runtime-coupled smoke checks, failover drills, replay checks, and live trading canary checks pass.

Do not call the original architecture plan complete unless all three gates are satisfied.

## Current Decision

- `Repo implementation complete`: yes
- `Architecture complete`: no
- `Production ready`: no

Current blocking gaps for the current production claim:

- The running local stack does not pass live smoke. `python tools/pipeline_smoke_test.py` reaches the operator and runtime, reconciles proxy-only operator startup, waits 60 seconds for a first price tick, then fails because no tick arrives. Direct `/api/health` inspection reports `ok:false`, `prices_not_ok`, `providers_not_ok`, `providers.total:0`, `providers.healthy:0`, and `prices.last_ts_ms:null`.
- The local compose env intentionally has live providers disabled and no provider credentials: `POLYGON_REST_ENABLED=0`, `POLYGON_WS_ENABLED=0`, `TRADIER_ENABLED=0`, empty `POLYGON_API_KEY`/`POLYGON_KEY`, and empty `TRADIER_API_TOKEN`. Production requires at least one authorized live provider and a passing live smoke run with fresh price ticks.
- `python tools/validate_repo.py --live` now reaches the live smoke phase and exits nonzero after the repo/unit gates pass. The failing gate is `tools/pipeline_smoke_test.py`: it cannot observe a fresh price tick because no live provider is configured.
- Shadow, paper, and limited-capital live canary rollout evidence is not recorded.

Current blocking gaps for the full original target-state plan:

- Production replacement of SQLite with deployed PostgreSQL/Timescale is partially proven for the compose stack by in-container preflight, but the current architecture still keeps SQLite as local runtime state and Timescale as time-series/price/telemetry backing rather than a full shared-state replacement.
- Event backbone deployment for live ingestion and replay is not implemented in repo.
- Redis and object storage are proven reachable in the compose stack by preflight, but training orchestration is only partially represented in repo code.
- Service decomposition into separately deployed bounded services is not implemented.
- Live operational drills and trading rollout evidence are not yet recorded.

## Gate 1: Repo Implementation Complete

| Check | Status | Evidence | Validation |
| --- | --- | --- | --- |
| Slice program `S01-S12` complete | Complete | [SLICE_LEDGER.md](SLICE_LEDGER.md) | Inspect ledger status for `S01-S12` |
| Cross-slice audit after `S01-S03` | Complete | [INT-01-S01-S03.md](integration/INT-01-S01-S03.md) | Recorded integration audit |
| Cross-slice audit after `S09-S12` | Complete | [INT-02-S09-S12.md](integration/INT-02-S09-S12.md) | Recorded integration audit |
| Canonical repo validation entrypoint | Passed in this audit | [tools/validate_repo.py](../../../tools/validate_repo.py) | `python tools/validate_repo.py`: unit phase passed; pytest phase `676 passed, 1 warning, 5 subtests passed`; news ingestion self-test passed |

Additional `2026-04-24` repo-side hardening evidence:

- Docker Desktop was installed and verified with `docker info` and `docker run --rm hello-world`.
- `deploy/compose/.env` was generated locally for this workstation with safe-mode local credentials, provider credentials empty, and `BROKER_NAME=sim`; `.env` remains gitignored and must not be used as a real secret store.
- `deploy/compose/docker-compose.stack.yml` now passes provider, broker, production lock, operator token, and startup import-smoke env contracts into runtime/operator containers.
- `package.json` and `package-lock.json` now use production-audit-clean operator dependencies: `express@4.22.1`, `sqlite3@6.0.1`, and `ws@8.20.0`.
- `engine/runtime/timescale_client.py` no longer self-deadlocks when `start()` is called on an already started live writer.
- `tools/runtime_graph_check.py` now waits for Timescale schema readiness during the sidecar startup check before declaring the sidecar unhealthy.
- `engine/execution/train_size_policy.py` now skips empty datasets only during `PREFLIGHT_SMOKE=1`, preserving non-preflight training failures.
- `start_system.py` now compiles job files during import smoke but imports job modules only when explicitly opted in, avoiding non-critical import side effects on normal startup.
- `engine/runtime/event_log.py` now tracks in-flight background buffer flushes so foreground flush/read paths do not observe a temporarily empty pending buffer while rows are still being written.
- `start_system.py` now imports and starts the authoritative `engine.api.server` path and retries transient SQLite lock contention during startup DB repair.
- `engine/runtime/prod_preflight.py` now reports `status` and `production_ready`, treats warning-only smoke results as non-production-ready, detects SQLite smoke lock contention, and runs smoke checks on an isolated database copy by default.
- `engine/runtime/external_service_readiness.py` now performs protocol-level checks for Timescale/Postgres, Redis, and object storage instead of only validating env shape.
- `boot/operator_server.js` handles proxy-only operator startup deterministically and proxies `/api/execution/barrier` with a runtime-sized timeout.
- `tools/pipeline_smoke_test.py` reconciles proxy-only operator startup and preserves structured HTTP error bodies.
- `engine/data/options_poll.py` treats best-effort ingestion status writes as nonfatal when provider failure handling encounters SQLite contention.
- `deploy/compose/docker-compose.external-services.yml` retries MinIO bucket initialization.
- `engine/runtime/equity_drift.py` suppresses repeat unresolved equity drift alerts across generic alert dedupe bucket boundaries.

Focused validation added or updated:

- `tests/test_prod_preflight_smoke_contracts.py`
- `tests/test_external_service_readiness.py`
- `tests/test_compose_deployment_assets.py`
- `tests/test_operator_ai_context_contract.py`
- `tests/test_pipeline_smoke_contract.py`
- `tests/test_api_server_contract.py`
- `tests/test_startup_health_validation.py`
- `tests/test_options_ingestion_reliability.py`
- `tests/test_equity_drift_integration.py`
- `tests/test_timescale_client_storage_gates.py`
- `tests/test_runtime_graph_check.py`
- `tests/test_train_size_policy_contract.py`
- `tests/test_runtime_reliability_regressions.py`
- `tests/test_ensemble_model_interfaces.py`

Latest focused regression result:

- `python -m pytest tests\test_operator_server_admin_contract_static.py tests\test_compose_deployment_assets.py -q`: `9 passed, 13 subtests passed`
- `python -m pytest tests\test_runtime_reliability_regressions.py::RuntimeReliabilityRegressionTests::test_event_log_and_redundant_init_under_load_have_zero_db_errors -q`: `1 passed`; repeated 5 consecutive runs after the in-flight flush fix.
- `python -m pytest tests\test_ensemble_model_interfaces.py::EnsembleModelInterfaceTests::test_ensemble_member_predictions_execute_in_parallel -q`: `1 passed`; repeated 5 consecutive runs after replacing sleep-based overlap detection with a barrier.

Repo gate signoff criterion:

- `python tools/validate_repo.py` exits `0`
- [SLICE_LEDGER.md](SLICE_LEDGER.md) and both integration audits remain current

## Gate 2: Target-State Architecture Complete

Status legend:

- `Complete in repo`: implemented and verified in code
- `Partial in repo`: seam/interface exists, but deployment or cutover proof is missing
- `Not implemented in repo`: no bounded implementation of the planned target yet

| Original plan item | Status | Repo evidence | Validation | Remaining gap | Signoff criterion |
| --- | --- | --- | --- | --- | --- |
| Hot-path raw evidence, provider health, and ingestion health are deferred off the immediate SQLite path | Complete in repo | [price_router.py](../../../engine/runtime/price_router.py), [timeseries_write_policy.py](../../../engine/runtime/timeseries_write_policy.py), `S01-S02` in [SLICE_LEDGER.md](SLICE_LEDGER.md) | `python -m pytest tests/test_sqlite_contention_relief.py -q`; `python -m pytest tests/test_ingestion_runtime_reliability.py -q` | none at repo level | Slice and tests stay green |
| Live-ingestion schema ownership extracted out of the storage monolith | Complete in repo | [storage_live_ingestion_schema.py](../../../engine/runtime/storage_live_ingestion_schema.py), `S03` in [SLICE_LEDGER.md](SLICE_LEDGER.md) | `python -m pytest tests/test_storage_contracts.py -q` | none at repo level | Schema ownership remains stable outside `storage.py` |
| Timescale-first read routing for price and telemetry | Complete in repo | [price_read_router.py](../../../engine/runtime/price_read_router.py), [telemetry_read_router.py](../../../engine/runtime/telemetry_read_router.py), `S04` | `python -m pytest tests/test_price_migration_validation.py -q`; `python -m pytest tests/test_telemetry_read_routing.py -q` | deployed Timescale parity still needs proof | Production config uses Timescale-backed reads without fallback surprises |
| Live cache boundary with Redis backing | Partial in repo | [live_cache.py](../../../engine/runtime/live_cache.py), `S05` | `python -m pytest tests/test_live_cache.py -q` | Redis is optional and falls back to memory; no proof that Redis is the active production cache | Runtime runs with Redis enabled and healthy in deployment |
| Immutable artifact-store contract with object-storage URIs | Partial in repo | [artifact_store.py](../../../engine/runtime/artifact_store.py), `S06` | `python -m pytest tests/test_model_registry_catalog.py -q`; targeted `test_inference_engine.py` and `test_model_scoring.py` checks from [SLICE_LEDGER.md](SLICE_LEDGER.md) | object-store deployment and artifact mirror operations are not proven in runtime | Artifacts are stored, loaded, and promoted through object storage in deployment |
| Training dataset contract to Parquet/object-storage-friendly bundles | Partial in repo | [dataset_store.py](../../../engine/runtime/dataset_store.py), `S07` | `python -m pytest tests/test_training_dataset_contract.py -q` | bounded provenance bundle exists, but external object-store pipeline and scheduler are not proven | Retraining outputs are materialized to the target store and consumed by scheduled training jobs |
| Live inference isolated behind runtime feature/catalog seams | Complete in repo | [inference_runtime.py](../../../engine/runtime/inference_runtime.py), [inference_engine.py](../../../engine/inference_engine.py), `S08` | `python -m pytest tests/test_inference_runtime.py -q`; `python -m pytest tests/test_inference_engine.py -q` | none at repo level | Inference remains isolated from concrete store modules |
| Durable `order_commands` and `order_events` execution boundary | Complete in repo | [order_command_boundary.py](../../../engine/execution/order_command_boundary.py), `S09` | `python -m pytest tests/test_broker_apply_orders_modes.py -q`; `python -m pytest tests/test_broker_order_idempotency_regressions.py -q` | none at repo level | Boundary remains the canonical durable execution seam |
| Projector-style post-trade reads over durable order/fill events | Complete in repo | [trade_lifecycle_projection.py](../../../engine/runtime/trade_lifecycle_projection.py), [trade_lifecycle.py](../../../engine/runtime/trade_lifecycle.py), [trade_attribution_ledger.py](../../../engine/execution/trade_attribution_ledger.py), `S10` | `python -m pytest tests/test_trade_lifecycle_regressions.py -q`; targeted attribution checks from [SLICE_LEDGER.md](SLICE_LEDGER.md) | none at repo level | Lifecycle and attribution continue to reconstruct from durable events |
| Control-plane server startup owned by `engine.api.server` | Complete in repo | [engine/api/server.py](../../../engine/api/server.py), [docs/ARCHITECTURE.md](../../ARCHITECTURE.md), `S11` | `python -m pytest tests/test_api_server_contract.py -q`; `python -m pytest tests/test_dashboard_route_contracts.py -q` | deployment cutover proof still needs environment evidence | Deployment and docs both treat `engine.api.server` as the startup entrypoint |
| Post-bind orchestration ownership extracted out of `dashboard_server.py` | Partial in repo | [dashboard_runtime_boot.py](../../../engine/runtime/dashboard_runtime_boot.py), `S12` | `python -m pytest tests/test_dashboard_runtime_boot.py -q`; `python -m pytest tests/test_dashboard_route_contracts.py -q` | ownership moved into a runtime helper, but it is still the same process, not a separate deployed service boundary | Runtime boot ownership is deployed and documented at the intended boundary |
| Production shared state moved off SQLite to PostgreSQL/Timescale | Partial in repo | `.env.example` contains `TIMESCALE_*`; [timescale_client.py](../../../engine/runtime/timescale_client.py); [price_read_router.py](../../../engine/runtime/price_read_router.py); compose stack provisions Timescale | `python -m pytest tests/test_timescale_integration_hooks.py -q`; `docker compose ... exec runtime python engine/runtime/prod_preflight.py --json` | compose proves Timescale connectivity/schema for current time-series paths, but repo still uses `DB_PATH`/SQLite as local runtime state rather than full shared-state replacement | Production environment runs with PostgreSQL/Timescale as the primary durable state path |
| Event backbone for live ingestion, replay, and durable fanout | Not implemented in repo | no Redpanda/Kafka deployment or runtime integration artifacts are present in `engine/`, `deploy/`, or `requirements.txt` | repo inspection plus `rg -n -i "redpanda|kafka" docs engine deploy requirements.txt .env.example` | event bus, replay consumers, and deployment assets are missing | Durable event backbone exists in repo and is exercised by integration/load tests |
| Redis as active production cache rather than optional fallback | Partial in repo | [live_cache.py](../../../engine/runtime/live_cache.py); compose stack provisions Redis | `python -m pytest tests/test_live_cache.py -q`; `docker compose ... exec runtime python engine/runtime/prod_preflight.py --json` | compose preflight proves Redis reachability for the current runtime, but production cache ownership still needs live runtime proof under real provider load | deployed runtime uses Redis as the active cache backend and health surfaces show it |
| Object storage as active artifact and dataset system of record | Partial in repo | [artifact_store.py](../../../engine/runtime/artifact_store.py), [dataset_store.py](../../../engine/runtime/dataset_store.py) | targeted `S06-S07` tests plus deployment inspection | no object-store deployment assets or scheduled upload flows are recorded | deployment writes and reads artifacts/datasets from object storage end to end |
| Offline training orchestration with a scheduler such as Prefect | Not implemented in repo | no scheduler/orchestrator implementation is present; only training data contracts were added | repo inspection plus `rg -n -i "prefect" docs engine deploy requirements.txt .env.example` | scheduler flows, deployment, and retraining automation are missing | scheduled retraining flows exist, run, and publish artifacts with validation gates |
| Six bounded runtime services | Not implemented in repo | current repo still centers on one supervised Python runtime plus side processes, documented in [docs/ARCHITECTURE.md](../../ARCHITECTURE.md) | architecture review | service split is still a target-state plan, not a completed repo change | services are separated by deployable boundary and validated independently |
| Vault-backed secret management | Not implemented in repo | current repo uses env vars and encrypted data-source control-plane storage, not Vault | repo inspection plus `rg -n -i "vault" docs engine deploy requirements.txt .env.example` | secret manager integration is absent | secrets are sourced and rotated through the planned secret manager |
| Containerized/runtime packaging for Compose or k3s | Partial in repo | [deploy/compose/docker-compose.external-services.yml](../../../deploy/compose/docker-compose.external-services.yml), [deploy/compose/docker-compose.stack.yml](../../../deploy/compose/docker-compose.stack.yml), [deploy/compose/README.md](../../../deploy/compose/README.md), [deploy/compose/.env.example](../../../deploy/compose/.env.example), [deploy/README.md](../../../deploy/README.md) | `docker compose --env-file deploy/compose/.env -f deploy/compose/docker-compose.external-services.yml -f deploy/compose/docker-compose.stack.yml up -d --build`; in-container preflight | compose packaging covers external dependencies plus the current app runtime and is exercised locally; the operator is intentionally proxy-only in container mode, and there is still no k3s bundle | target deployment packaging exists and is exercised in staging |
| Target observability stack for production rollout | Not implemented in repo | repo has internal health/metrics surfaces, but no Prometheus/Grafana/Loki/OTel deployment bundle | inspect docs and deploy assets | external observability stack is not represented as a target deployment | dashboards, alerts, and telemetry exporters are deployed and exercised |

Architecture gate signoff criterion:

- Every row above is either `Complete in repo` and `Proven in deployment`, or is explicitly removed from scope by an ADR
- No row needed for the target-state claim remains `Partial in repo` or `Not implemented in repo`

## Gate 3: Production Ready

Use the existing production docs as the operator-facing baseline:

- [docs/PRODUCTION_CHECKLIST.md](../../PRODUCTION_CHECKLIST.md)
- [tools/validate_repo.py](../../../tools/validate_repo.py)
- [engine/runtime/prod_preflight.py](../../../engine/runtime/prod_preflight.py)

Required production evidence:

- Deterministic repo validation passes:
  - `python tools/validate_repo.py`
- Production preflight passes on the target environment:
  - `python engine/runtime/prod_preflight.py --json`
- Runtime-coupled live smoke passes on an intentionally running stack:
  - `python tools/validate_repo.py --live`
- Operator health surfaces are healthy:
  - `GET /api/readiness`
  - `GET /api/execution/barrier`
  - `GET /api/operator/provider_telemetry`
  - `GET /api/operator/service_status`
  - `GET /api/operator/support_snapshot`
- Execution safety drills are recorded:
  - duplicate-order/idempotency regression pass
  - kill-switch verification
  - degraded-mode verification
  - restart/recovery drill for the chosen data and event backends
- Trading rollout evidence is recorded:
  - shadow trading pass
  - paper trading pass
  - limited-capital live canary pass

Recorded `2026-04-24` production-readiness validation:

| Command / gate | Result | Evidence |
| --- | --- | --- |
| `python tools/validate_docs.py` | Pass | Documentation validator exits `0` after this matrix update |
| `python tools/runtime_graph_check.py --mode startup` | Pass | `SYSTEM GRAPH VALID` locally/in container |
| `docker compose --env-file deploy/compose/.env -f deploy/compose/docker-compose.external-services.yml -f deploy/compose/docker-compose.stack.yml up -d --build` | Pass | Full stack rebuilt/recreated; runtime and operator containers report healthy |
| `docker compose ... exec runtime python tools/runtime_graph_check.py --mode startup` | Pass | In-container startup graph reports `SYSTEM GRAPH VALID` |
| `python engine/runtime/prod_preflight.py --json` with `PROD_LOCK=1`, `ALLOW_TRAINING=0` | Pass | Returns `ok:true`, `status:"passed"`, `production_ready:true` for local repo context |
| `docker compose ... exec runtime python engine/runtime/prod_preflight.py --json` | Pass | Returns `ok:true`, `status:"passed"`, `production_ready:true`; `timescale_primary`, `timescale_prices`, `live_cache_redis`, and `object_storage` all reachable |
| `npm.cmd audit --omit=dev --audit-level=low` | Pass | `found 0 vulnerabilities` |
| `docker compose ... build operator` | Pass | `npm ci --omit=dev` inside image reports `found 0 vulnerabilities` |
| `python tools/validate_repo.py` | Pass | Exits `0` after the final event-log, dependency, compose, and documentation updates; unit phase completed successfully and repo gates passed |
| Targeted changed-file pytest coverage | Pass | Operator/compose contracts pass; event-log load regression and ensemble parallelism regression each passed 5 consecutive targeted runs |
| `python tools/validate_repo.py --live` | Fail at live smoke | Repo gates and unit phase complete successfully, then `tools/pipeline_smoke_test.py` fails with `health_failed:request_failed` after no fresh price tick is observed; runtime health reports no active providers |
| `python tools/pipeline_smoke_test.py` | Fail | Operator proxy-only startup reconciles; smoke fails after 60 seconds with no price tick; direct health reports `prices_not_ok`, `providers_not_ok`, `providers.total:0`, `providers.healthy:0`, `prices.last_ts_ms:null` |
| Runtime `/api/execution/barrier` through operator sidecar | Pass route/proxy behavior; system not allowed | Proxy route returns a structured barrier result after timeout hardening; live health blockers keep execution disallowed |

Production gate signoff criterion:

- All required production evidence is recorded for the actual deployed target stack
- No blocking runtime, data-integrity, or execution-safety defect is open

## Final Signoff Checklist

The original target-state plan may be called complete only when all of the following are true:

- `python tools/validate_repo.py` passes
- [SLICE_LEDGER.md](SLICE_LEDGER.md) shows `S01-S12` complete
- [INT-01-S01-S03.md](integration/INT-01-S01-S03.md) and [INT-02-S09-S12.md](integration/INT-02-S09-S12.md) are passed
- every required row in the architecture matrix is no longer `Partial in repo` or `Not implemented in repo`
- `python engine/runtime/prod_preflight.py --json` passes on the target environment
- `python tools/validate_repo.py --live` passes on an intentionally running stack
- shadow, paper, and live canary rollout evidence is recorded

Until then, the correct statement is:

- the staged repo migration program is complete
- the full original architecture and production rollout plan is not yet fully complete
