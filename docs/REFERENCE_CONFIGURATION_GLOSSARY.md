# Configuration Glossary

This document is the current reference for configuration surfaces in the repository.

It is grounded in:

- [.env.example](../.env.example)
- [engine/runtime/config_schema.py](../engine/runtime/config_schema.py)
- [start_system.py](../start_system.py)
- [start_ingestion.py](../start_ingestion.py)
- [services/credential_encryption.py](../services/credential_encryption.py)
- [services/data_source_manager.py](../services/data_source_manager.py)

## Canonical Configuration Surfaces

| Surface | Canonical For | Notes |
| --- | --- | --- |
| `.env.example` | Baseline catalog of operator-edited environment variables | Starting point for local `.env` files and service-environment templates. |
| `.env` or service environment | Bootstrap and runtime configuration | Used by `start_system.py`, `dashboard_server.py`, `start_ingestion.py`, and many subsystem modules. |
| `data_sources` table | Provider credentials and source-specific settings | Managed through [ui/data_sources.html](../ui/data_sources.html) and [services/data_source_manager.py](../services/data_source_manager.py). |
| `DATA_SOURCE_MASTER_KEY` or `DATA_SOURCE_MASTER_KEY_FILE` | Encryption root for stored provider credentials | Required for production-like deployments; never stored in the database. |
| Runtime-set environment variables | Process identity and resolved paths | Variables such as `TRADING_LOGS`, `TRADING_DATA`, `ENGINE_JOB_NAME`, and `ENGINE_LAUNCHED_BY_SUPERVISOR` are set by the entrypoints and should not be treated as primary operator inputs. |

## Ownership Rules

- Provider credentials should be managed through the data-source control plane, not as long-lived `.env` values.
- `.env` remains canonical for bootstrap settings such as bind host, port, DB path, execution mode, feature toggles, and master-key discovery.
- `engine/runtime/config_schema.py` validates only part of the full environment contract today. Other variables are still consumed directly by entrypoints and subsystem modules.

## Bootstrap, Bind, And Mode

| Variable Or Family | Meaning | Primary Consumers |
| --- | --- | --- |
| `DASHBOARD_HOST`, `DASHBOARD_PORT` | Dashboard bind host and port | [dashboard_server.py](../dashboard_server.py) and browser/operator entrypoints. |
| `DASHBOARD_API_TOKEN` | Token gate for mutating dashboard and control-plane requests | [routes/data_sources_routes.py](../routes/data_sources_routes.py), [ui/data_sources.js](../ui/data_sources.js), and other `/api/*` mutation surfaces. |
| `DB_PATH` | Local data root and legacy compatibility path | Runtime storage is Postgres-backed; `DB_PATH` is still consumed by diagnostics, data-root resolution, and local artifacts in [engine/runtime/config_schema.py](../engine/runtime/config_schema.py). |
| `TS_PG_DSN`, `TS_PG_SCHEMA`, `TS_PG_POOL_TIMEOUT`, `TS_PG_POOL_SIZE` | Runtime Postgres connection, schema, and pool settings | [engine/runtime/storage_pool.py](../engine/runtime/storage_pool.py) and [engine/runtime/storage_pg.py](../engine/runtime/storage_pg.py). |
| `DASHBOARD_STORAGE_REQUEST_TIMEOUT_S`, `DASHBOARD_STORAGE_STARTUP_TIMEOUT_S` | Dashboard storage readiness/request bounds | Used by [dashboard_server.py](../dashboard_server.py) and the HTTP transport to return structured 503 responses instead of letting read handlers stall on Postgres acquisition. |
| `ENGINE_MODE`, `EXECUTION_MODE`, `OPERATOR_MODE` | Runtime and operator operating modes | Read across startup, system-state, API, and UI safety surfaces. Safe mode is the default posture. |
| `DISABLE_LIVE_EXECUTION` | Emergency live-capital kill switch | Read by runtime gates, kill-switch checks, broker routing/adapters, live preflight, and terminal order-entry. Unset or explicit false values (`0`, `false`, `no`, `off`) allow normal live eligibility checks; any other non-empty value blocks live execution. |
| `LIVE_TRADING_CONFIRM`, `LIVE_TRADING_CONFIRM_PHRASE`, `LIVE_TRADING_REQUIRE_CONFIRMATION`, `LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN` | Live-mode confirmation and dashboard-token contract | [engine/runtime/live_trading_preflight.py](../engine/runtime/live_trading_preflight.py). |
| `TRADING_DATA`, `TRADING_LOGS`, `DATA_DIR`, `LOG_DIR` | Resolved runtime data and log directories | [start_system.py](../start_system.py) and [start_ingestion.py](../start_ingestion.py) normalize these before the runtime starts. |

## Startup Validation And Lifecycle

| Variable Or Family | Meaning | Primary Consumers |
| --- | --- | --- |
| `TRADING_STARTUP_HEALTH_TIMEOUT_S`, `TRADING_STARTUP_HEALTH_FAIL_OPEN`, `TRADING_STARTUP_HEALTH_ASYNC_BIND` | Startup health-validation timing and failure posture | [start_system.py](../start_system.py). |
| `TRADING_VALIDATION_TIMEOUT_S` | Timeout for startup validation and preflight work | [start_system.py](../start_system.py). |
| `TRADING_STALE_INGESTION_CLEANUP_TIMEOUT_S`, `TRADING_SKIP_STALE_INGESTION_CLEANUP` | Stale-ingestion cleanup behavior before respawn | [start_system.py](../start_system.py). |
| `TRADING_SKIP_RUNTIME_GRAPH_CHECK` | Skip the runtime graph/import check | [start_system.py](../start_system.py). |
| `TRADING_CHALLENGER_RUNTIME_START_TIMEOUT_S` | Timeout for challenger runtime bootstrap | [start_system.py](../start_system.py). |
| `AUTO_BOOT_DAEMONS`, `AUTO_BOOT_TARGETS`, `AUTO_PIPELINE`, `AUTO_PIPELINE_INCLUDE_EXECUTION` | Automatic startup behavior for daemons and pipeline work | Startup orchestration and job-launch behavior. |

## Storage Backends And External Persistence

| Variable Or Family | Meaning | Primary Consumers |
| --- | --- | --- |
| `TS_PG_*` | Required runtime Postgres storage settings for the main storage facade. Production-like runtimes fail fast if Postgres is unavailable instead of downgrading to local storage. | [engine/runtime/storage_pool.py](../engine/runtime/storage_pool.py), [engine/runtime/storage_pg.py](../engine/runtime/storage_pg.py), and [engine/runtime/runtime_bootstrap.py](../engine/runtime/runtime_bootstrap.py). |
| `DASHBOARD_STORAGE_REQUEST_TIMEOUT_S`, `DASHBOARD_STORAGE_STARTUP_TIMEOUT_S` | Bound dashboard storage acquisition during API requests and startup readiness probes. | [dashboard_server.py](../dashboard_server.py), [engine/api/http_transport.py](../engine/api/http_transport.py). |
| `TIMESCALE_*` | Optional TimescaleDB sidecar integration settings such as DSN, schema, pooling, batching, retry, and timeout behavior | [engine/runtime/timescale_client.py](../engine/runtime/timescale_client.py) and related storage modules. |
| `DB_PATH` | Local data root and compatibility value for diagnostics/artifacts, not a silent fallback for runtime storage. | [engine/runtime/config_schema.py](../engine/runtime/config_schema.py), [engine/runtime/db_guard.py](../engine/runtime/db_guard.py). |

## Data-Source Secrets And Credential Encryption

| Variable Or Family | Meaning | Primary Consumers |
| --- | --- | --- |
| `DATA_SOURCE_MASTER_KEY`, `DATA_SOURCE_MASTER_KEY_FILE` | Preferred encryption root inputs for stored provider credentials | [services/credential_encryption.py](../services/credential_encryption.py). |
| `TRADING_MASTER_KEY`, `TRADING_MASTER_KEY_FILE`, `APP_MASTER_KEY` | Backward-compatible fallback master-key inputs | [services/credential_encryption.py](../services/credential_encryption.py). |
| `POLYGON_API_KEY`, `TRADIER_API_TOKEN`, `FINNHUB_API_KEY`, `FMP_API_KEY`, `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `SEC_USER_AGENT`, `SEC_FROM`, `WEATHER_HTTP_UA`, `IBKR_HOST`, `IBKR_PORT`, `IBKR_CLIENT_ID` | Legacy/bootstrap credential and provider-setting inputs | Imported once by [services/data_source_manager.py](../services/data_source_manager.py) when the DB-backed control plane initializes. After import, the `data_sources` table is the source of truth. |
| `OPENAI_API_KEY` | Key for optional LLM-backed integrations | Used by bounded AI-adjacent features, not by the core execution authority path. |

## Broker, Terminal, And Live-Execution Safety

| Variable Or Family | Meaning | Primary Consumers |
| --- | --- | --- |
| `BROKER`, `BROKER_NAME`, `LIVE_BROKER`, `INTENDED_LIVE_BROKER`, `BROKER_FAILOVER` | Broker identity and live failover chain. In live mode, broker identity must be consistent and the chain must not include `sim`, `paper`, or `sandbox`. | [engine/execution/broker_failover_policy.py](../engine/execution/broker_failover_policy.py), [engine/runtime/live_trading_preflight.py](../engine/runtime/live_trading_preflight.py). |
| `BROKER_BASE_URL`, `ALPACA_BASE_URL`, `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY` | Alpaca endpoint and credential bootstrap inputs. Live preflight rejects paper endpoints for live use. | Broker adapters and [engine/api/api_broker_config.py](../engine/api/api_broker_config.py). |
| `IBKR_HOST`, `IBKR_PORT`, `IBKR_CLIENT_ID` | IBKR gateway connection settings. | [engine/execution/broker_ibkr_gateway.py](../engine/execution/broker_ibkr_gateway.py), [engine/api/api_broker_config.py](../engine/api/api_broker_config.py). |
| `BROKER_TIMEOUT_S`, `BROKER_RETRY_ATTEMPTS`, `BROKER_RETRY_BACKOFF_S`, `BROKER_ROUTER_RETRY_ATTEMPTS`, `BROKER_ROUTER_RETRY_BASE_S`, `BROKER_ROUTER_RETRY_MAX_S` | Broker test and routing retry behavior. Auth/configuration failures remain non-retryable. | [engine/api/api_broker_config.py](../engine/api/api_broker_config.py), [engine/execution/broker_router.py](../engine/execution/broker_router.py). |
| `EXECUTION_PRELIVE_RECONCILE` | Whether pre-live position reconciliation is enabled. Live mode requires it unless an audited break-glass override is supplied. | [engine/runtime/live_execution_control.py](../engine/runtime/live_execution_control.py), [engine/execution/broker_router.py](../engine/execution/broker_router.py). |
| `EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS`, `EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_ACTOR`, `EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_REASON` | Break-glass override contract for disabling pre-live reconciliation in live mode. Actor and reason must be non-placeholder values and accepted overrides are event-logged. | [engine/runtime/live_execution_control.py](../engine/runtime/live_execution_control.py). |
| `TERMINAL_MAX_QTY`, `TERMINAL_MAX_NOTIONAL`, `TERMINAL_PRICE_MAX_AGE_MS`, `TERMINAL_DUPLICATE_WINDOW_MS`, `TERMINAL_SYMBOL_CAPS_JSON` | Backend pre-trade controls for browser-terminal order and flatten intents. Rejections are stored in `terminal_intent_rejections`. | [engine/terminal/api/api_terminal_orders.py](../engine/terminal/api/api_terminal_orders.py). |
| `ALERT_ACK_TIMEOUT_MS`, `ALERT_SHELVE_DEFAULT_MS`, `ALERT_SHELVE_MAX_MS` | Alert acknowledgement and shelving expiry defaults. | [engine/api/api_write.py](../engine/api/api_write.py). |

## Backup Evidence And Restore Drills

| Variable Or Family | Meaning | Primary Consumers |
| --- | --- | --- |
| `PREFLIGHT_REQUIRE_BACKUP_EVIDENCE` | Forces backup evidence to be required even outside live mode. Live mode requires it by default. | [engine/runtime/backup_evidence.py](../engine/runtime/backup_evidence.py), [engine/runtime/prod_preflight.py](../engine/runtime/prod_preflight.py). |
| `BACKUP_EVIDENCE_PATH` | JSON evidence file path. Default is `/var/backups/trading/evidence/latest_backup_restore_evidence.json`. | [engine/runtime/backup_evidence.py](../engine/runtime/backup_evidence.py). |
| `TS_BACKUP_BASE_DIR`, `TS_BACKUP_WAL_DIR`, `TS_RESTORE_DRILL_DIR` | Filesystem fallback locations for base backup, WAL archive, and restore-drill evidence. | [engine/runtime/backup_evidence.py](../engine/runtime/backup_evidence.py), `ops/backup/`. |
| `BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S`, `BACKUP_MAX_AGE_S` | Maximum accepted verified base-backup age. | [engine/runtime/backup_evidence.py](../engine/runtime/backup_evidence.py). |
| `BACKUP_EVIDENCE_WAL_RPO_S`, `BACKUP_EVIDENCE_RPO_S`, `BACKUP_RPO_S` | Maximum accepted WAL archive evidence age. | [engine/runtime/backup_evidence.py](../engine/runtime/backup_evidence.py). |
| `BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S`, `RESTORE_DRILL_MAX_AGE_S` | Maximum accepted restore-drill evidence age. | [engine/runtime/backup_evidence.py](../engine/runtime/backup_evidence.py). |
| `BACKUP_EVIDENCE_RESTORE_RTO_S`, `BACKUP_EVIDENCE_RTO_S`, `RESTORE_RTO_S` | Maximum accepted restore-drill recovery duration. | [engine/runtime/backup_evidence.py](../engine/runtime/backup_evidence.py). |

## Provider Toggles, Polling, And Freshness

| Variable Or Family | Meaning | Primary Consumers |
| --- | --- | --- |
| `IBKR_ENABLED`, `POLYGON_REST_ENABLED`, `POLYGON_WS_ENABLED`, `YFINANCE_ENABLED`, `CCXT_ENABLED`, `TRADIER_ENABLED`, `MACRO_ENABLED` | Compatibility toggles for provider availability and routing | Provider and ingestion paths, often after runtime env projection by [services/data_source_manager.py](../services/data_source_manager.py). |
| `POLL_SECONDS`, `PRICE_MAX_AGE_S` | Base polling cadence and acceptable price staleness | Price-ingestion and health gating. |
| `NEWS_POLL_SECONDS`, `GDELT_POLL_SECONDS`, `SEC_POLL_SECONDS`, `FORM4_POLL_SECONDS`, `CONGRESSIONAL_POLL_SECONDS`, `OPTIONS_POLL_SECONDS`, `EARNINGS_POLL_SECONDS`, `WEATHER_POLL_SECONDS`, `WEATHER_ALERTS_POLL_SECONDS` | Source-specific polling cadences | Ingestion jobs and freshness/health reporting. |

## Universe And Symbol Limits

| Variable Or Family | Meaning | Primary Consumers |
| --- | --- | --- |
| `DEFAULT_SYMBOLS_SEC_TOP_N`, `UNIVERSE_ACTIVE_N`, `UNIVERSE_WATCH_N` | Universe sizing defaults | Universe selection and upstream feed scope. |
| `PROCESS_SYMBOL_LIMIT`, `OPTIONS_SYMBOL_LIMIT`, `OPTIONS_UNDERLYING_LIMIT`, `COMPANY_NEWS_SYMBOL_LIMIT`, `GDELT_SYMBOL_LIMIT`, `SEC_SYMBOL_LIMIT`, `FORM4_SYMBOL_LIMIT`, `MODEL_FEATURE_SNAPSHOT_SYMBOL_LIMIT`, `CALIB_SYMBOL_LIMIT`, `PORTFOLIO_SYMBOL_LIMIT` | Per-job or per-feature symbol caps | Data, strategy, and portfolio jobs that need bounded worksets. |

## Optional Feature, Model, And Risk Families

| Variable Or Family | Meaning | Primary Consumers |
| --- | --- | --- |
| `ENSEMBLE_BLEND_ENABLED`, `ENSEMBLE_BLEND_MODE`, `ENSEMBLE_MAX_WEIGHT`, `ENSEMBLE_MIN_AGREEMENT`, `ENSEMBLE_META_RETRAIN_S` | Opt-in predictor ensemble blending behavior | [engine/runtime/config_schema.py](../engine/runtime/config_schema.py), [engine/strategy/ensemble_blender.py](../engine/strategy/ensemble_blender.py), and predictor paths. |
| `HMM_REGIME_*` | Optional HMM regime layer behavior | Strategy regime-selection paths. |
| `BLACK_LITTERMAN_*` | Optional Black-Litterman expected-return blending | [engine/strategy/black_litterman.py](../engine/strategy/black_litterman.py) and portfolio allocation paths. |
| `USE_FINBERT_SENTIMENT`, `FINBERT_*` | Optional FinBERT enrichment behavior | [engine/data/finbert_sentiment.py](../engine/data/finbert_sentiment.py) and related enrichment jobs. |
| `USE_FORM4_DATA`, `USE_CONGRESSIONAL_TRADE_DATA`, `INGEST_FORM4_ENABLED`, `INGEST_CONGRESSIONAL_ENABLED`, `FORM4_*`, `CONGRESSIONAL_*`, `USE_PIT_UNIVERSE`, `PIT_UNIVERSE_BACKFILL_ENABLED` | Optional alternative-data ingestion and point-in-time universe behavior | Data ingestion, backfills, and universe-building paths. |
| `PORTFOLIO_BACKTEST_USE_EXEC_COSTS`, `ALMGREN_CHRISS_*`, `PORTFOLIO_ALLOCATION_MODE` | Execution-cost realism and allocation overlays | [engine/execution/almgren_chriss.py](../engine/execution/almgren_chriss.py), portfolio backtests, and allocation logic. |
| `GBM_USE_TUNED_HYPERPARAMS`, `GBM_OPTUNA_*` | Optional tuned GBM hyperparameter selection | [engine/strategy/optuna_tuner.py](../engine/strategy/optuna_tuner.py), GBM training jobs, and model-selection paths. |

## Strictly Validated Runtime Variables

| Variable | Meaning |
| --- | --- |
| `ENV` | Canonical environment identity used by `load_runtime_config()`. Accepts `dev`, `prod`, or `test`. |
| `PROD_LOCK` | Prevents unsafe production behavior such as enabling training in a locked production runtime. |
| `ALLOW_TRAINING` | Allows or forbids training work in the current runtime. |
| `SUPERVISOR_ENABLED`, `SUPERVISOR_TICK_S` | Supervisor enablement and heartbeat cadence. |
| `EXEC_DEGRADE_BLOCK`, `EXEC_DEGRADE_WARN_COST_PCT`, `EXEC_DEGRADE_CRIT_COST_PCT` | Execution-degradation thresholds surfaced through system and risk APIs. |

These variables are part of the current contract and are now surfaced in `.env.example` so operators can see the same defaults that `config_schema.py` validates.

## Variables Set By The Entrypoints

These variables are set or normalized by the entrypoints and should be treated as runtime identity, not as the primary operator-facing configuration surface:

- `PYTHONDONTWRITEBYTECODE`
- `PYTHONUNBUFFERED`
- `PYTHONPATH`
- `ENGINE_SUPERVISED`
- `ENGINE_LAUNCHED_BY_SUPERVISOR`
- `ENGINE_JOB_NAME`
- `TRADING_LOGS`
- `TRADING_DATA`

## Operational Rules

- In live-capital-sensitive workflows, prefer explicit safe defaults and fail-closed behavior.
- If a provider credential exists in both `.env` and the `data_sources` table, the DB-backed control plane is the intended long-lived source of truth.
- When adding a new operator-owned configuration variable, update `.env.example`, this glossary, and any relevant subsystem README in the same change.
