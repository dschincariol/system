# System Audit Layer 1 P0/P1 Triage

Reviewed on 2026-06-15 after running `python tools/system_audit.py`.

## Release Status

The current static audit has **91 findings**, all **P1 `silent_except`** findings of the form `except: <debug/info only, no re-raise>`.

Risk accepted for this release: these remaining handlers are nonfatal cleanup, compatibility, optional-backend, migration, or model-fallback paths that already emit at least debug/info logs and are not the primary failure signal for readiness or operator action.

Acceptance condition: if any residual path begins affecting startup readiness, live execution, operator support snapshots, schema migration correctness, or model promotion outcomes, promote it to warning/error telemetry or convert it to a hard failure.

## Closed Or Classified

| Area | Status |
|---|---|
| Raw SQLite/file DB imports in runtime boundaries | No targeted-test offenders; SQLite fallback remains isolated to `engine/runtime/storage_sqlite.py` test support. |
| Production `print()` telemetry debt | Closed in current audit; CLI stdout is now classified separately from importable production logging. |
| Suspicious literal returns | Closed in current audit; branch-dependent literals and `None` lifecycle returns are no longer treated as placeholders. |
| Stub/no-op contracts | Closed in current audit; intentional cache/Gym no-op hooks have category-scoped inline rationale. |
| Empty `except: pass` handlers | Closed in current audit; reviewed fallback/cleanup paths now carry scoped inline rationale or existing no-op guard acceptance. |

## Residual Counts By Subsystem

| Subsystem | Count | Risk acceptance |
|---|---:|---|
| `engine.runtime` | 35 | Cleanup, storage facade fallback, pool teardown, SQLite test fallback, telemetry flush best-effort paths. |
| `strategy` | 14 | Training/validation fallback paths that do not authorize promotion or execution by themselves. |
| `strategy.models` | 13 | Optional model-library compatibility and cleanup paths; model failures still surface through training/evaluation results. |
| `strategy.ensemble` | 10 | OOS/blender persistence and fallback cleanup; promotion gates remain outside these handlers. |
| `engine.rl` | 7 | Optional RL dependency and portfolio-env fallback paths. |
| `strategy.jobs` | 5 | Job cleanup/fallback paths; job status remains the primary operator signal. |
| `engine.artifacts` | 2 | Best-effort artifact cleanup/persistence fallback. |
| `engine.causal` | 2 | Optional causal dependency fallback. |
| `engine.backtest` | 1 | CPCV optional compatibility fallback. |
| `engine.cache` | 1 | Cache cleanup fallback. |
| `engine.data` | 1 | Optional data dependency fallback. |

## Residual Findings By File

| File | Count | Risk acceptance |
|---|---:|---|
| `engine/strategy/meta_labeling.py` | 8 | Debug/info-only model-data fallback; label/promotion outcomes still depend on explicit returned results. |
| `engine/runtime/storage_sqlite.py` | 7 | Test-only SQLite fallback cleanup and liveness paths. |
| `engine/runtime/storage_pool.py` | 7 | Pool cleanup/teardown fallback; primary storage errors surface before these handlers. |
| `engine/runtime/storage.py` | 6 | Storage facade shutdown fallback; production facade remains Postgres-oriented. |
| `engine/runtime/storage_pg.py` | 6 | Postgres cursor/connection cleanup fallback; query failures still propagate at call sites. |
| `engine/rl/portfolio_env.py` | 4 | RL environment fallback/cleanup; shadow-only training path. |
| `engine/strategy/models/lgbm_regressor.py` | 4 | Optional LightGBM compatibility and cleanup; fit/evaluation failures still surface through job result. |
| `engine/rl/agents.py` | 3 | Optional RL dependency fallback and cleanup. |
| `engine/runtime/telemetry_append_buffer.py` | 3 | Best-effort telemetry buffer shutdown/flush. |
| `engine/strategy/models/lgbm_ranker.py` | 3 | Optional LightGBM ranker compatibility and cleanup. |
| `engine/artifacts/store.py` | 2 | Artifact cleanup fallback. |
| `engine/causal/dowhy_runner.py` | 2 | Optional DoWhy dependency/fallback path. |
| `engine/runtime/observability/pg_stats.py` | 2 | Optional observability query fallback. |
| `engine/runtime/schema/migrations/0007_audit_chain.py` | 2 | Migration compatibility fallback; migration failure still propagates outside these probes. |
| `engine/strategy/model_feature_snapshots.py` | 2 | Feature snapshot fallback; downstream status uses returned result. |
| `engine/backtest/cpcv.py` | 1 | Optional backtest compatibility fallback. |
| `engine/cache/store.py` | 1 | Cache cleanup fallback. |
| `engine/data/options_poll.py` | 1 | Optional dependency/import fallback. |
| `engine/runtime/db_repair.py` | 1 | Repair-step observation fallback. |
| `engine/runtime/first_run.py` | 1 | First-run observation fallback. |
| `engine/strategy/ensemble/blender.py` | 5 | Ensemble cleanup/fallback; scoring and promotion decisions are explicit outputs. |
| `engine/strategy/ensemble/oos_store.py` | 5 | OOS persistence fallback; failure does not authorize promotion. |
| `engine/strategy/gbm_regressor.py` | 1 | Optional GBM cleanup/fallback. |
| `engine/strategy/jobs/causal_scoring.py` | 1 | Job-level fallback; job result remains authoritative. |
| `engine/strategy/jobs/discover_features.py` | 1 | Job-level fallback; job result remains authoritative. |
| `engine/strategy/jobs/embed_news.py` | 1 | Job-level fallback; job result remains authoritative. |
| `engine/strategy/jobs/fill_ensemble_oos_targets.py` | 1 | Job-level fallback; job result remains authoritative. |
| `engine/strategy/jobs/train_ensemble.py` | 1 | Job-level fallback; job result remains authoritative. |
| `engine/strategy/models/patchtst.py` | 5 | Optional torch/model cleanup and compatibility fallback. |
| `engine/strategy/models/xgb_regressor.py` | 1 | Optional XGBoost compatibility fallback. |
| `engine/strategy/promotion_audit.py` | 1 | Audit fallback; promotion gates remain explicit. |
| `engine/strategy/rl_strategy_policy.py` | 1 | Optional RL policy fallback. |
| `engine/strategy/validation.py` | 1 | Validation fallback; returned validation status remains authoritative. |

## Next Hardening Pass

Prioritize promoting the highest-volume runtime storage and model training debug/info handlers to structured warning telemetry where the extra signal would materially improve operator diagnosis.
