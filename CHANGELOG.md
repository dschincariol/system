# Changelog

All notable changes to repository contracts, documentation governance, and operator-relevant behavior should be recorded here.

This changelog starts on 2026-04-12. Earlier repository history has not been retroactively reconstructed because the current repo state does not provide enough grounded release history to do that safely.

## How To Use This File

- Keep new entries under `Unreleased` until there is a grounded release identifier, tag, or versioning rule to attach them to.
- Prefer `Added`, `Changed`, `Fixed`, `Removed`, and `Security` headings.
- Record documentation-governance changes when they alter contributor expectations, canonical references, or external-facing contracts.
- Do not fabricate historical versions.

## [Unreleased]

### Added

- Consolidated readiness evidence API/UI: `/api/operator/readiness_evidence` now normalizes runtime, execution, broker, provider, data-source, governance, production-monitoring, and probe evidence into actionable blocker rows, and the dashboard shows a Readiness Evidence card with broker activation pre-checks.
- Execution diagnostics API/UI: `/api/execution/diagnostics` now aggregates by-symbol TCA, rolling slippage and latency, partial/rejected/suppressed outcomes, LOB/DeepLOB readiness, and learned-slicing audit evidence with explicit stale, unavailable, and shadow-only states without changing live execution authority.
- First-class job catalog API/UI: `/api/jobs/catalog`, richer `/api/jobs` job metadata, backend-owned safety/prerequisite/action policy fields, and a dashboard Job Catalog with filters, grouped rows, latest state, and log links.
- Governance Evidence Center API/UI: `/api/governance/evidence`, evidence drilldowns, and `/api/governance/shadow_capital/scores` now surface promotion evidence, generated-candidate provenance, model-risk controls, production monitoring, and shadow-capital score state without changing promotion or allocation authority.
- Structured-document and graph-feature visibility: `/api/data/feature_visibility`, Data Health panels, and decision attribution metadata now show extraction counts, freshness, confidence, lineage, graph snapshot availability, PIT status, and shadow-only labels without changing live trading authority.
- `CONTRIBUTING.md` defining documentation update expectations, ADR triggers, OpenAPI update rules, configuration glossary update rules, and validation expectations.
- `docs/DOCSTRING_STYLE.md` with repository-specific NumPy-style docstring guidance.
- `docs/adr/` with an ADR index and the initial governance ADR set.
- `docs/openapi/` as the canonical home for the incremental OpenAPI source of truth.
- `docs/LICENSING_NOTE.md` documenting that the repository currently has no repo-wide license file.
- `tools/validate_docs.py` for lightweight documentation validation.
- `docs/OS_MIGRATION_RUNBOOK.md` plus read-only `ops/server/os_migration_preflight.py` and `ops/server/os_migration_postflight.py` gates for moving production host `bart` from Ubuntu 25.10 to a supported LTS with ZFS/Docker/systemd/backup/ROCm evidence.
- Boot-enforced CPU power policy for host `bart`: `trading-cpu-power-policy.service`, an idempotent apply/verify script, and documentation for the performance/EPP trade-off, revert path, and ROCm/GPU thermal composition.
- `docs/MEMORY_PRESSURE_RUNBOOK.md`, `ops/server/memory_pressure_hardening.sh`, and `ops/server/detect_deleted_tmpfs_holders.py` for enforcing swappiness, managed zram/disk swap, ZFS ARC caps, active verification, and read-only deleted `/tmp` holder detection on `bart`.
- Shadow-only Chronos time-series foundation encoder features registered as `tsfm.chronos_v2.*`, with PIT metadata, artifact manifest provenance, optional dependency gating, and live-serving rejection for shadow feature contracts.
- Subsystem READMEs for `engine/jobs/`, `engine/audit/`, `engine/cache/`, `engine/backtest/`, `engine/nlp/`, `engine/causal/`, `engine/artifacts/`, and `engine/rl/`, each linked from `engine/README.md` and `docs/DOCUMENTATION_INDEX.md`.
- `docs/REFERENCE_CONFIGURATION_GLOSSARY.md` section documenting runtime-critical risk knobs (`MC_SIMULATIONS`, gross/net exposure caps, kill-switch drawdown thresholds, `PORTFOLIO_CAR_MAX`, `PORTFOLIO_MAX_POSITIONS`, and the `KILL_DRIFT_*`/`KILL_SLIPPAGE_*`/`PORTFOLIO_ALLOC_*` families) with code-verified defaults.
- `docs/config_env_allowlist.txt` freezing the legacy backlog of environment variables read in code but not yet documented.
- `tools/validate_docs.py` documentation-governance gates — subsystem-README coverage, environment-variable coverage, and staleness sentinels — recorded in `docs/adr/0006-documentation-governance-gates.md`.
- Behavioral money-path pytest coverage for hierarchical allocation, shadow capital scoring, runtime bootstrap safe defaults, execution liquidity, slicing, dual execution, AI advisory, microstructure sizing, and open-order acknowledgement timeouts.
- Tail-risk VaR/CVaR backtesting evidence: a new read-only `GET /api/risk/var_backtest` endpoint and `engine/risk/var_backtesting.py` surface Kupiec POF, Christoffersen independence, and Basel traffic-light exception results from `risk_var_backtest_results` (migration `0079`), with a `risk_var_backtest` runtime job; the route does not change risk caps, sizing, or execution authority.
- Shadow-only GARCH volatility forecasting (`engine/strategy/garch_vol.py`, `garch_vol_forecast` job, migration `0080_garch_vol_forecasts.py`) and a covariance facade (`engine/risk/covariance.py`) for portfolio-risk consumers.
- Shadow-only structured LLM financial-event extraction (`engine/data/llm_event_extraction.py` plus the `llm_event_extraction` job and migration `0082`), projecting shadow-only event feature rows with lineage and confidence; no live trading authority.
- Foundation-model and challenger benchmarking surfaces (`engine/strategy/tsfm_benchmark.py`, `engine/strategy/tsfm_adapters.py`, `engine/nlp/benchmark.py`, the `tsfm_benchmark`/`graph_challenger_benchmark` jobs, and migrations `0081_tsfm_benchmark.py`/`0083_nlp_backend_namespaces.py`), all shadow-only with no live serving authority.
- Critical-alert delivery hardening (GO-R3): `engine/runtime/alerts_notify.py` warns once per process when a CRIT/CRITICAL alert has zero configured notification channels, and `engine/runtime/prod_preflight.py` adds a go-live notification-channel gate that requires at least one enabled channel (`EQ_CRIT_SMTP_HOST`+`EQ_CRIT_EMAIL_TO` or `EQ_CRIT_WEBHOOK_URL`) before live go-live.
- FX live-trading enable gate: `engine/execution/broker_router.py` blocks FX orders with `fx_live_trading_disabled_by_default` unless `FX_LIVE_TRADING_ENABLED=1`, and the live-trading preflight reports the FX enablement entry.
- Nightly soak/chaos CI (`.github/workflows/soak_chaos.yml`) running the safe-mode soak gate and market-session provider-disconnect chaos soak with uploaded evidence, plus the deploy-installer `trading-restore-drill` systemd service/timer (`deploy/systemd/trading-restore-drill.service`/`.timer`) running the weekly Postgres restore drill required by backup evidence.

### Changed

- Operator console high-impact actions now use the shared structured
  confirmation modal and sidecar/API confirmation validation/audit contract
  instead of native browser prompt/confirm flows.
- Non-`sim` broker activation now requires a fresh passing connection test for the same broker; `BROKER_CONNECTION_TEST_MAX_AGE_S` controls the freshness window.
- Dashboard quick job buttons and command-palette job actions now consume backend job catalog safety metadata instead of browser-only unsafe-job name matching.
- `docs/DOCUMENTATION_INDEX.md` updated to include governance, decision-log, licensing, and OpenAPI-baseline docs.
- `docs/DOCS_AUDIT.md` updated to reflect the new governance layer and the remaining documentation gaps.
- `README.md` updated so the canonical documentation set and documentation conventions point at the new governance artifacts.
- `tools/validate_repo.py` now runs documentation validation as part of the canonical repository validation workflow.
- Production handover documentation refreshed for broker configuration, live-execution safety, terminal pre-trade rejection rows, alert lifecycle state, backup evidence, and current storage/schema ownership.
- Production disk retention now caps compose stdout/stderr, tightens file-log rotation to `maxsize 50M` with 10 compressed rotations and `maxage 21`, and surfaces backup accounting retention status plus container mount source in preflight.
- Pytest and `tools/validate_repo.py` now point temporary test scratch at `/var/tmp/trading-system-tests-<uid>/pytest` by default instead of RAM-backed `/tmp`; override with `TRADING_TEST_TMPDIR`.
- `docs/README_FUNCTION_MAP.md` corrected (`boot/operator_server.js` symbol names; `engine/api/api_system.py` ownership of the watchdog/support/provider-telemetry handlers) and expanded with a Risk section plus `execution_ledger`/`model_marketplace`/`champion_manager`/`predictor` entries and a storage-facade re-export note.
- `engine/README.md` and the `strategy`/`execution`/`api`/`risk` subsystem READMEs expanded to cover previously undocumented modules; `engine/jobs/` relabeled from "legacy" to the live price-ingestion path.
- Documentation de-duplicated via cross-links: `docs/Database_Schema.md` marked the authoritative table register, `docs/README_DEVELOPER_MAP.md` made the canonical read-order / highest-risk-file / task-path home, and the readiness and ZFS-migration docs cross-linked to single sources.
- Locked CPU/default Python dependency install now uses `pip install --require-hashes -r requirements.txt` (hash-pinned locks), and `README.md` was updated to match.
- `docs/openapi/openapi.yaml` documents the new read-only `GET /api/risk/var_backtest` route, and the system/data-source sensitive-route descriptions note that query-string token auth is also rejected on non-loopback binds.
- `CLAUDE.md` re-verified against code (count corrected to `~1,500+` Python files and `~140+` registered jobs, shadow feature catalog to `~1,902`, and Phase 2 updated to record the now-present shadow-only graph challenger, TSFM adapters/benchmarking, and structured LLM event extraction); `Last verified against code` bumped to 2026-06-26.

### Fixed

- Operator backup commands corrected from `/opt/trading/app/ops/backup/*` to the real install path `/opt/trading/ops/backup/*` across the disk-retention runbook, observability doc, production checklist, and compose README.
- `docs/DATA_CONTRACTS.md` marks `portfolio_orders.from_side`/`to_side` as required (`TEXT NOT NULL` per migration `0022_portfolio_orders.py`).
- `.env.example` ships `RUNTIME_WORKLOAD_PROFILE=live` (the documented default) instead of `offline`.
- `CLAUDE.md` Python-file count corrected to `~950+`, and the map/index `Last verified against code` dates refreshed.
- `docs/Audit_Chain_Spec.md` drops the non-existent `audit benchmark` CLI example and documents the empty-`prev_hash` sentinel for byte-exact recomputation.

### Removed

- Orphan documented environment variables with no code usage: `MACRO_ENABLED` and `NEWS_FLOW_POLL_SECONDS`.
