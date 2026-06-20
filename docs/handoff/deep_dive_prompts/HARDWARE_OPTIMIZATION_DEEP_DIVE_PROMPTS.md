# Hardware Optimization Deep Dive Prompts

Use these prompts one at a time. Each prompt is scoped to turn the current host review into production code, configuration, documentation, and validation changes without mixing unrelated optimization work.

## Common Preamble

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes. Treat the target host as an AMD Ryzen AI Max+ 395 system with 16 cores / 32 threads, 123 GiB RAM, AMD Radeon 8060S/NPU hardware that is not currently usable by PyTorch, Docker-based runtime services, and a root filesystem under disk pressure. Preserve safe/live trading semantics before performance. Prefer explicit production config over implicit hardware detection where live behavior is at stake.

## Prompt 1 - Fix Disk Pressure, Log Retention, And Backup Accounting

Deep dive and implement disk-retention hardening for the current host. Current evidence: `/` is about 87% full with roughly 60 GiB free, Docker reports large reclaimable image/build/volume usage, local and container runtime logs include multi-GB files, and backup accounting for `/var/backups/trading` needs verification because container and host views disagree.

Requirements:
- Add or tighten Docker logging limits for the compose stack so container stdout/stderr cannot grow unbounded.
- Add logrotate or equivalent retention for repo and container-mounted runtime logs, including ingestion and process stdout logs.
- Ensure retention rules preserve recent operational evidence while compressing or deleting old high-volume logs.
- Add a safe backup accounting check that reports host path, container mount source, apparent size, and retention status for `/var/backups/trading`.
- Add or update an operational cleanup command/runbook that distinguishes safe cache pruning from destructive volume deletion.
- Add preflight or health diagnostics that warn before root disk exhaustion blocks ingestion, database writes, backups, or operator diagnostics.
- Update production docs with retention defaults, cleanup commands, and restore-evidence preservation rules.

Suggested files to inspect:
- `deploy/compose/docker-compose.stack.yml`
- `deploy/compose/docker-compose.external-services.yml`
- `deploy/logrotate/`
- `deploy/README.md`
- `deploy/compose/README.md`
- `ops/backup/`
- `engine/runtime/health.py`
- `engine/runtime/prod_preflight.py`
- `docs/PRODUCTION_CHECKLIST.md`
- `docs/OBSERVABILITY.md`

Done criteria:
- Runtime/container logs have bounded retention in the default Docker deployment.
- Operators can identify disk risk and backup-retention size without shell guesswork.
- Cleanup guidance does not recommend deleting live database, Redis, MinIO, or backup state.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 2 - Make Runtime Hardware Selection Explicitly CPU-First On This AMD Host

Deep dive and implement CPU-first runtime configuration for the current AMD host. Current evidence: the machine has AMD Radeon/NPU hardware, but the user and containers do not have usable accelerator access, ROCm/OpenCL is not configured, and the installed PyTorch build is CUDA-oriented with `cuda_available=False`. Some event-processing jobs still default `TORCH_DEVICE` or related settings to `cuda`.

Requirements:
- Make production Docker/env defaults explicit: `TORCH_DEVICE=cpu`, `EMBED_DEVICE=cpu`, `NLP_DEVICE=cpu`, `FINBERT_DEVICE=cpu`, `TS_FOUNDATION_DEVICE=cpu`, and CUDA feature toggles disabled unless deliberately enabled.
- Replace code defaults that assume CUDA with `cpu` or a documented `auto` mode that only selects accelerator devices after verifying support.
- Ensure event-processing, FinBERT/NLP, PatchTST, iTransformer, temporal, and time-series foundation paths all resolve device selection consistently.
- Add bounded thread defaults suitable for a multi-process live runtime on a 32-thread host, such as 8 compute threads and 2-4 interop threads unless overridden.
- Gate NVIDIA-specific telemetry/import behavior so a non-NVIDIA host does not emit misleading warnings or require NVIDIA-only packages for normal CPU operation.
- Add diagnostics that report resolved device, thread counts, and disabled accelerator reason in health or startup logs.
- Update docs and env examples so operators know this host is CPU-first until AMD accelerator access and a compatible runtime are installed.

Suggested files to inspect:
- `deploy/compose/docker-compose.stack.yml`
- `deploy/compose/.env.example`
- `deploy/env/trading.env.example`
- `engine/data/jobs/process_events.py`
- `engine/data/jobs/process_events_live.py`
- `engine/data/jobs/process_events_enriched.py`
- `engine/data/finbert_sentiment.py`
- `engine/nlp/encoder.py`
- `engine/runtime/torch_threads.py`
- `engine/strategy/models/patchtst.py`
- `engine/strategy/models/itransformer.py`
- `engine/strategy/ts_foundation_encoder.py`
- `requirements.txt`

Done criteria:
- Live/runtime defaults no longer imply CUDA on this AMD host.
- Device resolution is deterministic and observable.
- CPU-thread defaults reduce oversubscription risk without disabling explicit offline overrides.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 3 - Add Docker Resource Isolation For Live Runtime Services

Deep dive and implement resource isolation for the Docker deployment. Current evidence: `trading-runtime`, `trading-timescaledb`, and supporting services have no CPU or memory limits, while the same host also runs IDE processes, tests, Docker, and the operating system. Timescale can consume significant CPU/RAM under load.

Requirements:
- Add production-oriented CPU and memory limits or documented compose profiles for Timescale, runtime, Redis, MinIO, and operator services.
- Leave enough host headroom for the OS, Docker daemon, IDE, test runs, shell sessions, and emergency diagnostics.
- Increase runtime shared memory where needed for PyTorch/data-loader style workloads instead of relying on Docker's small default `/dev/shm`.
- Make limits configurable through env variables or deployment profiles so larger/smaller hosts can tune without editing source compose YAML.
- Ensure resource limits do not silently conflict with Postgres memory settings, Redis maxmemory, or runtime worker/thread defaults.
- Add validation/preflight checks that warn when live services run unbounded on a production host.
- Document recommended limits for the current 16-core/32-thread, 123 GiB RAM machine.

Suggested files to inspect:
- `deploy/compose/docker-compose.stack.yml`
- `deploy/compose/docker-compose.external-services.yml`
- `deploy/compose/.env.example`
- `deploy/compose/README.md`
- `deploy/README.md`
- `engine/runtime/prod_preflight.py`
- `engine/runtime/external_service_readiness.py`
- `ops/server/bootstrap.sh`
- `docs/PRODUCTION_CHECKLIST.md`

Done criteria:
- The default or production compose path can run with bounded service resources.
- Limits are documented and validated, not tribal knowledge.
- Database, Redis, and runtime settings remain internally consistent after limits are applied.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 4 - Align Timescale/Postgres Tuning With Host And Container Limits

Deep dive and implement database tuning that matches the current host and any Docker resource limits. Current evidence: the running Timescale instance has aggressive host-scale settings such as large `shared_buffers` and `effective_cache_size`, while Compose does not enforce matching memory limits. `max_wal_size` is low for write-heavy ingestion and backups.

Requirements:
- Define a single source of truth for production Postgres tuning in the Docker path, analogous to the bare-metal tuning in `ops/server/bootstrap.sh`.
- Make `shared_buffers`, `effective_cache_size`, `work_mem`, `maintenance_work_mem`, parallel workers, autovacuum workers, WAL/checkpoint settings, and IO-cost settings configurable and documented.
- If DB memory is container-limited, tune Postgres for the container limit rather than total host RAM.
- Increase WAL/checkpoint headroom for sustained ingestion without creating unbounded disk risk.
- Validate that configured Postgres memory does not exceed service memory limits or leave insufficient host headroom.
- Add diagnostics showing effective DB settings and how they were derived.
- Update docs with recommended settings for this 123 GiB RAM host and lower-resource fallback guidance.

Suggested files to inspect:
- `deploy/compose/docker-compose.external-services.yml`
- `deploy/compose/.env.example`
- `deploy/compose/README.md`
- `ops/server/bootstrap.sh`
- `ops/server/config/postgres.conf.tmpl`
- `engine/runtime/prod_preflight.py`
- `engine/runtime/staging_prod_preflight.py`
- `docs/archive/Database_Production_Plan.md`
- `docs/README_DATABASE_MAP.md`
- `docs/PRODUCTION_CHECKLIST.md`

Done criteria:
- Docker Timescale tuning is explicit, reproducible, and compatible with service limits.
- Preflight or diagnostics catch unsafe memory/WAL combinations.
- Operators can explain why the configured DB values fit the current machine.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 5 - Optimize Model Scoring Query Path And Required Indexes

Deep dive and optimize the model-scoring database path observed on the running system. Current evidence: active scoring SQL uses a per-prediction latest tracked-prediction lookup with `ORDER BY ... LIMIT 1`, while the running Postgres index set did not show an index matching `(prediction_id, ts_ms DESC, id DESC)` or a clear `model_performance(prediction_id)` index.

Requirements:
- Inspect the current migrations and running schema assumptions for `predictions`, `tracked_predictions`, and `model_performance`.
- Add idempotent migrations for indexes that support unresolved-prediction scoring, including latest tracked prediction by prediction id and model-performance lookup by prediction id.
- Rewrite the scoring query if needed to avoid avoidable correlated work, using `LEFT JOIN LATERAL`, `DISTINCT ON`, or another plan that is explainable and index-backed.
- Preserve correctness for missing tracked predictions, partial scoring state, duplicate prevention, and retry behavior.
- Add tests that assert SQL shape or query behavior for unresolved predictions and that migrations create the required indexes.
- Add optional diagnostics or comments that make the expected Postgres plan discoverable to future maintainers.
- Update database docs with the scoring indexes and why they exist.

Suggested files to inspect:
- `engine/model_scoring.py`
- `engine/runtime/schema/migrations/`
- `engine/runtime/schema/migrator.py`
- `engine/runtime/storage_pg.py`
- `engine/runtime/storage_live_ingestion_schema.py`
- `docs/Database_Schema.md`
- `docs/README_DATABASE_MAP.md`
- `tests/test_storage_pg_runtime_regressions.py`
- `tests/test_db_repair.py`
- `tests/test_model_competition_real_pnl.py`

Done criteria:
- The unresolved-prediction scoring path has the indexes it needs in production migrations.
- The query is demonstrably index-friendly and preserves existing scoring semantics.
- Tests cover both migration/index presence and scoring behavior.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 6 - Tune Ingestion Writers, Queues, And Backpressure For This Host

Deep dive and implement controlled ingestion tuning for the 32-thread, 123 GiB host after database/index safety is addressed. Current evidence: writer pools, async queues, telemetry buffers, and batch sizes are conservative, which is safe but may underuse the machine. Raising all knobs at once could hide database bottlenecks or increase loss windows.

Requirements:
- Inventory all ingestion writer pools, batch sizes, queue sizes, flush intervals, and Redis/Postgres pool settings used by live ingestion.
- Add explicit env-controlled tuning knobs where missing, with safe defaults and production bounds.
- Provide a host profile for this machine that increases Timescale/price/telemetry throughput incrementally without excessive connection multiplication across child processes.
- Add backpressure metrics or diagnostics for queue depth, flush latency, dropped rows, retry counts, and DB write duration.
- Make unsafe queue or pool combinations fail preflight or emit clear warnings.
- Preserve fail-closed behavior for trading gates when ingestion lags or critical prices become stale.
- Add tests for env parsing, bounds, backpressure reporting, and safe defaults.

Suggested files to inspect:
- `engine/runtime/timescale_client.py`
- `engine/runtime/storage_pg_prices.py`
- `engine/runtime/async_writer.py`
- `engine/runtime/telemetry_append_buffer.py`
- `engine/runtime/storage_pool.py`
- `engine/cache/redis_pool.py`
- `engine/runtime/ingestion_runtime.py`
- `engine/runtime/job_registry.py`
- `start_ingestion.py`
- `deploy/compose/.env.example`
- `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`
- `docs/OBSERVABILITY.md`
- `tests/test_ingestion_runtime_reliability.py`
- `tests/test_cache_redis_pool.py`

Done criteria:
- Ingestion throughput knobs are explicit, bounded, and documented.
- Operators can see whether higher throughput is working or only building queues.
- Live trading safety does not depend on ingestion keeping up silently.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 7 - Split Live Runtime From Offline Research And Training Profiles

Deep dive and implement separate resource profiles for live runtime versus offline research/training. Current evidence: the host can support heavier training, TSFresh, LGBM/XGB, and model research workloads, but those workloads should not contend with live ingestion, Timescale, Redis, and execution safety paths unless explicitly isolated.

Requirements:
- Define separate configuration profiles for live/runtime and offline/research/training workloads.
- Keep live defaults conservative: training disabled, optional heavy features off, bounded threads, and low background concurrency.
- Add an offline profile that can intentionally use more CPU/RAM with explicit scheduler, model `n_jobs`, TSFresh, and batch settings.
- Add env-controlled `n_jobs` or worker settings where model families are currently hard-coded to serial execution, while preserving live defaults.
- Make TSFresh parallelism configurable and safe, including symbol-level or feature-level bounds where appropriate.
- Add guardrails so enabling offline training in the live profile requires explicit operator acknowledgement or fails production preflight.
- Document when to run offline jobs, how to avoid live contention, and how to verify resource usage.

Suggested files to inspect:
- `engine/runtime/config.py`
- `engine/runtime/job_registry.py`
- `engine/strategy/tsfresh_features.py`
- `engine/strategy/models/lgbm_regressor.py`
- `engine/strategy/models/xgb_regressor.py`
- `engine/strategy/models/lgbm_ranker.py`
- `engine/strategy/jobs/train_ensemble.py`
- `engine/strategy/jobs/tune_models.py`
- `engine/strategy/pipeline_train_and_eval.py`
- `deploy/compose/docker-compose.stack.yml`
- `deploy/compose/.env.example`
- `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`
- `docs/README_OPERATOR_GUIDE.md`

Done criteria:
- Live and offline workloads have distinct, documented resource profiles.
- Offline parallelism can use the host intentionally without changing live defaults.
- Production preflight catches accidental heavy training/research settings in live mode.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 8 - Split Dependency Profiles By Hardware Backend

Deep dive and implement dependency profiles that match CPU, NVIDIA CUDA, and possible AMD accelerator deployments. Current evidence: the current host is AMD and CPU-only from PyTorch's perspective, but the repo installs CUDA/NVIDIA-oriented packages by default. That increases image size, creates misleading hardware assumptions, and complicates operator diagnostics.

Requirements:
- Split dependency installation paths into a CPU/default runtime profile and optional hardware-specific profiles for NVIDIA CUDA and future AMD/ROCm support.
- Keep live CPU deployment fully functional without NVIDIA-only packages.
- Move `pynvml`, `nvidia-ml-py`, CUDA-specific PyTorch instructions, and NVIDIA diagnostics behind an explicit extra or deployment profile.
- Document that AMD GPU/NPU acceleration requires device permissions, compatible runtime packages, and validated support before enabling accelerator env vars.
- Update Docker/build/install scripts so they install the intended profile explicitly rather than relying on accidental default packages.
- Add tests or static validation that CPU/default installs do not import NVIDIA-only modules on the hot path.
- Update docs with hardware profile selection, verification commands, and rollback to CPU mode.

Suggested files to inspect:
- `requirements.txt`
- `pyproject.toml`
- `deploy/compose/docker-compose.stack.yml`
- `deploy/install_trading_system.sh`
- `deploy/README.md`
- `ops/server/bootstrap.sh`
- `engine/runtime/platform.py`
- `engine/runtime/health.py`
- `engine/runtime/prod_preflight.py`
- `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`
- `docs/PRODUCTION_CHECKLIST.md`
- `README.md`

Done criteria:
- CPU deployment no longer depends on NVIDIA-only runtime packages.
- Hardware-specific acceleration is opt-in and documented.
- Production diagnostics clearly report selected dependency/device profile.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
