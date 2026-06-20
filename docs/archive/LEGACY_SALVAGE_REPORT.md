# Legacy Salvage Report

Date: 2026-06-20

Scope:

- Legacy tree: `/home/david/gitsandbox/Trading-System-`
- Current tree: `/home/david/gitsandbox/system/system`
- Excluded generated/vendor/runtime material: `.git`, `node_modules`, venvs, databases, logs, and operator state.

Result: 13 old-only source/config files were found. No legacy runtime module was copied back wholesale. Useful behavior was already present in current architecture or was preserved with focused regression tests.

## File Decisions

| Legacy file | Classification | Current equivalent | Decision |
| --- | --- | --- | --- |
| `.vscode/launch.json` | Obsolete | none | Local IDE config only; not source and not portable runtime behavior. |
| `engine/api/api_system_handlers.py` | Duplicate | `engine/api/api_system.py` | Current `api_system.py` already owns health, readiness, system state, repair, and telemetry with richer production readiness and telemetry persistence. Do not restore split handler module. |
| `engine/api/api_voice.py` | Useful, already carried forward | `engine/strategy/llm_explain.py` | The timeout worker helper exists in current code with enable gating plus prompt/response truncation. Added direct tests in `tests/test_llm_explain.py`. |
| `engine/data/jobs/options_poll.py` | Obsolete wrapper | `engine/data/options_poll.py` | Old job imports stale `engine.options.*` paths and writes older schemas directly. Current daemon has provider failover, health, cooldowns, checkpoints, source control-plane integration, and tests. |
| `engine/data/jobs/snapshot_equity.py` | Duplicate wrapper | `engine/runtime/jobs/snapshot_equity.py` | Current job adds lock, heartbeat, daemon/run-once modes, and registry wiring. |
| `engine/data/stream_prices_ibkr.py` | Obsolete | `engine/data/providers/ibkr/daemon_stream.py`, `engine/data/provider_sessions/ibkr_session.py` | Old direct `ibapi` loop writes SQLite tables directly and lacks session manager, reconnect telemetry, source control-plane integration, and lifecycle state. |
| `engine/data/stream_prices_polygon_ws.py` | Duplicate | `engine/jobs/stream_prices_polygon_ws.py` | Current daemon contains the old symbol subscription, write, heartbeat, restart, and provider-health behavior with stronger lifecycle, tracing, source-control, buffering, and failure diagnostics. |
| `engine/runtime/job_graph.py` | Useful behavior, already carried forward | `engine/runtime/supervisor.py`, `tools/runtime_graph_check.py` | Current supervisor implements missing-dependency/cycle validation and dependency-first start order. Added regression tests in `tests/test_runtime_supervisor_dag.py`; do not restore a parallel graph module. |
| `engine/runtime/provider_monitor_job.py` | Duplicate | `engine/runtime/jobs/provider_monitor_job.py` | Current monitor reads provider-health snapshots and ingestion pipeline health, emits heartbeats, and is registered under the canonical jobs path. |
| `engine/runtime/strategy_governance_job.py` | Obsolete placeholder | `engine/strategy/jobs/strategy_governance_job.py` | Old file emits no findings and says schema wiring is future work. Current job owns strategy promotion governance, audit hooks, OPE gate checks, locks, and persistence. |
| `engine/storage.py` | Obsolete compatibility shim | `engine/runtime/storage.py` | Current code intentionally imports canonical runtime storage. `ops/patch_dev_core_imports.py` maps old imports; reintroducing `engine.storage` would preserve stale architecture. |
| `engine/strategy/meta_strategy_layer.py` | Obsolete no-op shim | `engine/runtime/strategy_allocator.py`, `engine/strategy/jobs/trade_pipeline_job.py` | Old function returns `{}` only to satisfy a past import. Current trade pipeline calls `compute_and_persist_strategy_allocations`. |
| `ops/asset_map.py` | Duplicate | `engine/data/asset_map.py` | Current module keeps the same mapping and heuristics with structured warning diagnostics for bad overrides. |

## Salvage Applied

- Added `tests/test_runtime_supervisor_dag.py` to lock in the useful `job_graph.py` guarantees now implemented by `RuntimeSupervisor`.
- Added `tests/test_llm_explain.py` to lock in the useful voice timeout-helper behavior now implemented by `engine.strategy.llm_explain`.

## Not Carried Forward

- No stale `engine.storage` alias was restored.
- No old `engine/data/jobs/*` wrappers were restored.
- No direct legacy IBKR/Polygon stream daemons were restored outside the current provider/session architecture.
- No no-op strategy allocation shim was restored.
