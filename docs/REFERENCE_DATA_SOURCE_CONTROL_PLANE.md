# Data-Source Control Plane Reference

This document is the canonical reference for the data-source control plane.

It is grounded in:

- [ui/data_sources.html](../ui/data_sources.html)
- [ui/data_sources.js](../ui/data_sources.js)
- [routes/data_sources_routes.py](../routes/data_sources_routes.py)
- [services/data_source_manager.py](../services/data_source_manager.py)
- [services/credential_encryption.py](../services/credential_encryption.py)

## Scope

The data-source control plane owns:

- source inventory
- shared provider-account credentials, provider identity settings, and source-specific overrides
- encrypted-at-rest credential storage
- source enable and disable actions
- source creation and deletion for custom RSS feeds
- connection testing
- bounded Populate Now storage proof and data-contract verification
- source-specific logs and audit history
- runtime lifecycle reconciliation after source changes

It does not own:

- general runtime bootstrap configuration such as bind host, DB path, or execution mode
- trading authority
- portfolio or execution policy
- structured-document or graph-feature trading authority; Data Health only reads `/api/data/feature_visibility` for operator visibility into optional shadow feature groups

## Canonical Components

| Component | Role |
| --- | --- |
| [ui/data_sources.html](../ui/data_sources.html) | Canonical operator UI for source setup, testing, recovery, and monitoring. |
| [ui/data_sources.js](../ui/data_sources.js) | Browser controller for inventory, detail panes, session token handling, backend-catalog rendering, inline schema validation, and mutations against `/api/data_sources/*`. |
| [routes/data_sources_routes.py](../routes/data_sources_routes.py) | HTTP route module for the data-source control plane. |
| [services/data_source_manager.py](../services/data_source_manager.py) | Source-of-truth manager for storage, env projection, lifecycle reconciliation, testing, enriched source templates, and field validation. |
| [services/credential_encryption.py](../services/credential_encryption.py) | AES-GCM encryption, decryption, and masking for stored credentials. |

## Storage Contract

The manager ensures these tables exist:

| Table | Purpose |
| --- | --- |
| `data_sources` | Canonical source inventory and source configuration. |
| `data_source_provider_accounts` | Shared provider-account credentials inherited by dependent source rows. |
| `data_source_logs` | Source-specific operational log events with credential-bearing detail fields redacted before persistence and again on read. |
| `data_source_audit` | Actor-attributed audit records for create, update, delete, enable, disable, and test actions; detail payloads use the same credential-field sanitizer as source logs. |
| `data_source_populate_evidence` | Latest per-source Populate Now evidence: provider probe summary, row count, storage table, latest timestamp, latency, missing/null counts, duplicate drops, stale/gap status, data contract, and pass/warn/fail status. |
| `runtime_meta` | Stores control-plane readiness and dirty/reload markers such as `data_sources_schema_ready` and `data_sources_dirty`. |

Important columns in `data_sources`:

- `source_key`
- `display_name`
- `source_type`
- `provider_name`
- `job_name`
- `enabled`
- `credentials_enc`
- `settings_json`
- `status`
- `last_error`
- `last_success_ts_ms`
- `last_test_ts_ms`
- `error_count`
- `config_hash`
- `created_ts_ms`
- `updated_ts_ms`

Important columns in `data_source_provider_accounts`:

- `account_key`
- `display_name`
- `provider_name`
- `credentials_enc`
- `key_version`
- `status`
- `last_error`
- `last_test_ts_ms`
- `config_hash`
- `created_ts_ms`
- `updated_ts_ms`

## Log And Audit Redaction

`engine/runtime/data_source_log_store.py` is the production redaction boundary for `data_source_logs.detail_json`. It recursively masks credential-bearing keys before insert and normalizes the stored JSON. The sanitized keys include `credentials`, `credentials_enc`, `api_key`, `api_token`, `client_secret`, `secret`, `token`, and `password`, including separator and camel-case variants. Non-secret status fields, for example `status`, `ok`, `token_required`, and readiness metadata, remain unchanged.

The same sanitizer is applied to data-source audit detail payloads, telemetry read-router responses, and Timescale mirror enqueue paths. `DataSourceManager.initialize()` also runs idempotent cleanup for existing runtime log rows and records the `data_source_logs_sqlite_redaction_v1` runtime marker. That cleanup uses JSON/null semantics (`detail_json IS NOT NULL`) rather than comparing JSON/JSONB values to empty strings, so it is portable across SQLite text storage and Postgres JSONB storage. Empty or invalid legacy text rows normalize to `{}`, valid objects preserve non-secret status fields, and credential-bearing keys are redacted.

Startup schema initialization and legacy log cleanup are separate write transactions. The schema transaction commits first; the cleanup transaction then runs with bounded startup write timeouts and retries through the shared storage transaction helper, so transient lock/deadlock contention can recover without broad table locks. If the primary runtime cleanup cannot complete after its bounded retries, initialization fails with the underlying DB error instead of marking startup healthy. When Timescale telemetry is configured, it attempts the equivalent `data_source_logs_timescale_redaction_v1` cleanup against mirrored `data_source_logs` rows and records the result in runtime metadata; this mirror cleanup remains telemetry-side best effort.

## Built-In Source Templates

These templates are seeded automatically by [services/data_source_manager.py](../services/data_source_manager.py).

| Source Key | Provider | Job Name | Default | Safe Auto-Enable | Credentials / Settings | Storage Tables | Consumers |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `polygon_ws` | `polygon_ws` | `stream_prices_polygon_ws` | On | No | `POLYGON_API_KEY`; WS subscription settings | `prices`, `price_quotes`, `price_quotes_raw`, `price_provider_health` | price router, model snapshots, Data Health |
| `polygon` | `polygon` | `poll_prices` | On | No | `POLYGON_API_KEY` | `prices`, `price_quotes`, `price_quotes_raw`, `price_provider_health` | price router, model snapshots, Data Health |
| `oanda_fx` | `oanda` | `poll_prices` | Off | No | `OANDA_ACCESS_TOKEN` or fallback `OANDA_API_KEY`; `OANDA_ACCOUNT_ID`, `OANDA_ENVIRONMENT`, optional `OANDA_FX_PAIRS` | `prices`, `price_quotes`, `price_quotes_raw`, `price_provider_health` | price router, model snapshots, Data Health |
| `ibkr` | `ibkr` | `stream_prices_ibkr` | Off | No | host, port, client ID, market-data type, currency | `prices`, `price_quotes_raw`, `price_provider_health` | price router, live readiness, Data Health |
| `alpaca_broker_data` | `alpaca` | `alpaca_broker_data_readonly` | Off | No | `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY`, base URL defaulting to paper, optional trade-updates observation settings | `broker_connection_health`, `broker_positions` | live readiness, position reconcile |
| `yfinance` | `yfinance` | `poll_prices` | On | Yes | None | `prices`, `price_provider_health` | price router, model snapshots, Data Health |
| `simulated` | `simulated` | `poll_prices` | Off | Yes | Optional `SIMULATED_MARKET_DATA_SYMBOLS`; no credentials | `prices`, `price_quotes`, `price_quotes_raw`, `price_provider_health` | price router, model snapshots, Data Health, safe/sim validation |
| `ccxt` | `ccxt` | `poll_prices` | On | Yes | `CCXT_EXCHANGE_ID` setting | `prices`, `price_provider_health` | price router, crypto features, Data Health |
| `tradier` | `tradier` | `options_poll` | On | No | `TRADIER_API_TOKEN` | `options_chain`, `options_chain_v2`, `options_symbol_ingestion_state`, `events` | options features, model snapshots, Data Health |
| `polygon_options` | `polygon` | `options_poll` | On | No | `POLYGON_API_KEY` through source or Polygon account | `options_chain`, `options_chain_v2`, `options_symbol_ingestion_state`, `events` | options features, model snapshots, Data Health |
| `reddit` | `reddit` | `poll_social_reddit` | On | No | `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, subreddit/user-agent settings | `events` | sentiment features, news flow, model snapshots |
| `stocktwits` | `stocktwits` | `poll_social_stocktwits` | On | Yes | public endpoint settings | `events` | sentiment features, model snapshots |
| `company_news` | `company_news` | `ingest_now` | On | No | `FINNHUB_API_KEY`, symbol/lookback settings | `events`, `news_event_features`, `news_symbol_features` | news flow, model snapshots, Data Health |
| `transcripts` | `transcripts` | `ingest_now` | On | No | `FMP_API_KEY`, max-items setting | `structured_document_events`, `events` | document features, model snapshots, feature visibility |
| `gdelt` | `gdelt` | `poll_gdelt` | On | Yes | public query settings | `events`, `gdelt_macro_features` | news flow, model snapshots, macro features |
| `sec` | `sec` | `poll_sec_filings` | On | Yes | SEC identity/settings; optional shared SEC account | `structured_document_events`, `events` | document features, model snapshots, feature visibility |
| `form4` | `form4` | `ingest_form4` | Off | No | SEC identity/settings | `events`, `insider_transactions` | insider-flow features, model snapshots |
| `inst_13f` | `inst_13f` | `ingest_13f` | Off | No | SEC identity plus optional Polygon/FMP accounts | `inst_13f_filings`, `inst_13f_holdings`, `inst_13f_cusip_symbol_map`, `inst_13f_symbol_features` | institutional-flow features, model snapshots |
| `congressional_trades` | `congressional_trades` | `ingest_congressional_trades` | Off | No | public source/backfill settings | `congressional_trades`, `events` | legislative-flow features, model snapshots |
| `etf_flows` | `etf_flows` | `ingest_etf_flows` | Off | No | Polygon primary and FMP fallback accounts; cadence setting | `etf_shares_outstanding`, `etf_flow_features` | ETF-flow features, model snapshots |
| `cftc_cot` | `cftc_cot` | `ingest_cftc_cot` | Off | No | keyless CFTC domain, dataset, timeout, contract-map settings | `cftc_cot_positions`, `cot_contract_symbol_map`, `cot_symbol_features` | COT positioning features, model snapshots, feature visibility |
| `finra_short_volume` | `finra_short_volume` | `ingest_finra_short_volume` | Off | No | keyless FINRA file URL, backfill, timeout settings | `finra_short_sale_volume` | short-interest features, model snapshots, feature visibility |
| `finra_short_interest` | `finra_short_interest` | `ingest_finra_short_interest` | Off | No | keyless FINRA Query API URL, limit, page, timeout settings | `finra_short_interest` | short-interest features, model snapshots, feature visibility |
| `crypto_funding` | `crypto_funding` | `ingest_crypto_funding` | Off | No | keyless CCXT funding exchange and market-map settings | `crypto_funding_rates` | crypto-positioning features, model snapshots, feature visibility |
| `quiver_gov` | `quiver_gov` | `ingest_quiver_gov` | Off | No | `QUIVER_API_KEY`, endpoint/auth settings | `quiver_congressional_trades`, `quiver_lobbying_filings`, `quiver_gov_contracts` | legislative-flow features, model snapshots |
| `fundamentals_pit` | `fundamentals_pit` | `ingest_fundamentals_pit` | Off | No | `SIMFIN_API_KEY`, `SHARADAR_API_KEY`, mode/bulk settings | `fundamentals_pit`, `fundamentals_pit_backfill_state`, `fundamentals_pit_symbol_features` | fundamental features, model snapshots |
| `earnings` | `earnings` | `poll_earnings` | On | No | `FMP_API_KEY`, lookahead setting | `events` | calendar features, model snapshots |
| `weather_forecasts` | `weather_forecasts` | `poll_weather_forecasts` | On | Yes | public provider/cadence settings | `events` | weather features, model snapshots |
| `weather_alerts` | `weather_alerts` | `poll_weather_alerts` | On | Yes | public provider/cadence/user-agent settings | `events` | weather features, model snapshots |
| `macro` | `macro` | `poll_macro` | On | Yes | public macro cadence setting; optional FRED account | `factor_registry`, `factor_observations`, `factor_features`, `macro_series_vintages`, `macro_vintage_backfill_state`, `events` | regime features, model snapshots |
| `model_feature_snapshots` | `model_feature_snapshots` | `snapshot_model_features` | On | Yes | internal cadence/bucket settings | `model_feature_snapshots` | model diagnostics, feature visibility |
| `news_flow` | `news_flow` | `process_news_flow` | On | Yes | hashing backend by default; optional OpenAI embeddings account | `news_story_embeddings`, `news_flow_features` | model snapshots, feature visibility |
| `rss_feed` | `rss` | `ingest_now` | Custom | No | feed `name` and `url` settings | `events`, `news_event_features` | news flow, model snapshots |

## Provider Accounts

Shared provider accounts are seeded in `data_source_provider_accounts` and encrypted through the same AES-GCM master-key path as `data_sources.credentials_enc`. Normal API responses expose only `masked_credentials`, `credentials_configured`, `configured_fields`, `credential_fields`, and `used_by`; raw account credentials are available only to internal manager calls using `include_credentials=True`.

| Account Key | Runtime Env Vars | Used By |
| --- | --- | --- |
| `polygon` | `POLYGON_API_KEY` | Polygon REST, Polygon WebSocket, explicit Polygon options, ETF flows, 13F CUSIP lookup |
| `alpaca_data` | `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY` | Read-only Alpaca broker-data catalog entry; not projected to supervised ingestion jobs |
| `fmp` | `FMP_API_KEY` | Transcripts, earnings, ETF fallback, 13F CUSIP fallback |
| `sec_identity` | `SEC_USER_AGENT`, `SEC_FROM` | SEC filings, Form 4, 13F |
| `reddit` | `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET` | Reddit OAuth polling |
| `quiver` | `QUIVER_API_KEY` | Quiver government-flow ingestion |
| `fundamentals_vendors` | `SIMFIN_API_KEY`, `SHARADAR_API_KEY` | PIT fundamentals ingestion |
| `tradier` | `TRADIER_API_TOKEN` | Tradier options ingestion |
| `fred` | `FRED_API_KEY` | Macro vintage ingestion and macro backfills |
| `openai_embeddings` | `OPENAI_API_KEY` | Optional OpenAI-backed embedding jobs |

Effective credential precedence is enforced in manager runtime code: source override, then shared provider account, then allowed external runtime source, then missing. `build_job_environment()` projects one effective value per env var for each job. In strict runtime mode, secret values are written to runtime secret files and projected through `*_FILE`; non-secret SEC identity values are projected as normal env vars because the existing SEC clients read those names directly.

Two provisioning paths are supported:

- File-backed runtime credentials: write one credential per file outside the repo checkout or in the deployment secret mount, mode `0600` when the host filesystem supports it, then point the matching env var at it. Common mappings are `POLYGON_API_KEY_FILE=.../polygon_api_key`, `TRADIER_API_TOKEN_FILE=.../tradier_api_token`, `ALPACA_KEY_ID_FILE=.../alpaca_key_id`, `ALPACA_SECRET_KEY_FILE=.../alpaca_secret_key`, and `OPENAI_API_KEY_FILE=.../openai_api_key`. Missing, unreadable, non-regular, or zero-byte files resolve to a structured missing credential state. They do not crash the control plane and do not trigger a live provider call with an empty credential.
- UI/API managed credentials: after `DATA_SOURCE_MASTER_KEY` or `DATA_SOURCE_MASTER_KEY_FILE` is configured, `POST /api/data_sources/update`, `POST /api/data_sources/test_save`, and `POST /api/data_sources/accounts/update` store submitted credentials in `credentials_enc` as AES-GCM ciphertext with `key_version`. Normal API responses return only masked values and credential presence/status metadata.

## Runnable-State Contract

Every source and every source-managed job has a first-class `runnable_state`:

| State | Meaning |
| --- | --- |
| `off` | The source is disabled or the job is not desired. |
| `enabled-missing-credential` | The source is enabled but at least one required runtime credential is missing or could not be projected. |
| `enabled-credentialed-not-scheduled` | Required credentials are available, but the source is not schedulable, for example `runtime_runnable=false`, or the job is not currently desired. |
| `scheduled-waiting` | The source passed credential projection and its job is desired, but no running or healthy runtime evidence has arrived yet. |
| `running` | The supervised child is running, but health has not promoted it to healthy. |
| `degraded` | Runtime evidence is stale, waiting after an error, or otherwise degraded. |
| `failed` | Runtime evidence reports a failed pipeline or disabled restart loop. |
| `healthy` | The scheduled job has fresh successful pipeline or provider evidence. |

Production code enforces these states in [services/data_source_manager.py](../services/data_source_manager.py):

- `get_desired_ingestion_jobs()` uses the same credential-resolution and projection gate as `build_job_environment()`. Enabled credentialed rows do not add desired jobs until required credentials are available and projectable.
- Keyless feeds remain schedulable when enabled.
- `apply_runtime_environment()` withdraws manager-projected credential env/file keys before rebuilding the current overlay, then clears known credential env/file names before applying the current projection. Disabling or clearing a credential therefore removes stale in-process secrets instead of leaving them available for the next reconciliation.
- `manage_lifecycle()` marks the runtime dirty on every source/provider-account mutation, starts `ingestion_runtime` when newly required, and stops it when no runnable desired jobs remain and a jobs manager is available.
- `get_provider_registry_overrides()` reports provider entries as enabled only when their source is credentialed and desired-eligible.

`GET /api/data_sources` surfaces `status`, `stored_status`, `credential_status`, `needs_credentials`, `runnable_state`, `runnable_state_reason`, `runtime_credentialed`, `runtime_projected`, `runtime_desired_eligible`, `missing_credential_env_vars`, `missing_credentials`, and `job_runnable_state` on source rows. When required credentials are absent, the operator-facing `status` is `needs_credentials` even if the stored row status is an older `configured` or `test_failed` value. The `runtime.jobs` snapshot exposes per-job state keyed by job name. `GET /api/data_sources/logs` includes the current source state in addition to recent log rows, and enable/disable log events include non-secret runnable-state detail.

Provider health and readiness include data-source runnable state for provider-backed feeds. Missing credentials show as explicit provider blockers instead of silently appearing as absent telemetry.

## Template Catalog Shape

`GET /api/data_sources` returns `templates[]` from the backend catalog. The browser UI renders provider setup copy directly from this payload; adding or changing a provider should update [services/data_source_manager.py](../services/data_source_manager.py), not hardcode new provider instructions in JavaScript.

Each template includes:

- identity and routing controls: `template_key`, `source_type`, `provider_name`, `job_name`, `singleton`, `builtin`, `identity_locked`, and `routing_locked`
- mutation policy: `allow_create`, `allow_update`, `allow_delete`, and `supports_test`
- operational metadata: `default_enabled`, `storage_tables`, `consumers`, `safe_to_auto_enable`, and `runtime_runnable`
- `guide`: `category`, `summary`, `needs`, `setup`, `when_enabled`, `docs_url`, `signup_url`, `plan_note`, and `safety_warnings`
- `credential_fields[]` and `setting_fields[]`

Each field object carries:

- `field`
- `env_var` / `env_name`
- `label`
- `help_text`
- `docs_url`
- `signup_url`
- `plan_note`
- `required` and `required_state`
- `secret`
- `validation_hint`
- `validation_regex`
- `placeholder`
- `safety_warning`
- `type` / `input_type`

Secret fields are never echoed back with plaintext values. Normal source responses expose `masked_credentials`, `credentials_configured`, and `credentials_stored`; they do not include raw credential values.

Source responses also expose `credential_resolution[]` for account-linked env vars. Each row reports `env_var`, `mode` (`overridden`, `inherited`, `runtime_external`, or `missing`), account metadata when inherited, and a masked value only.

## Data Health Feature Visibility

The dashboard Data Health screen includes structured-document and graph-feature panels backed by `GET /api/data/feature_visibility`. This is adjacent to the source control plane but does not mutate source configuration.

The route reads existing `structured_document_events`, `graph_relational_snapshots`, and optional `event_log` failure rows to show:

- extraction counts, latest extraction and availability timestamps, low-confidence counts, source lineage, symbol coverage, and event-type coverage
- graph snapshot freshness, observed graph feature ids, relationship coverage, source artifact lineage, and PIT status
- explicit unavailable or stale states when tables, snapshots, or failure telemetry are absent
- shadow-only and `direct_trading_authority=false` labels for structured-document and graph feature groups

Source setup, credential storage, and runtime lifecycle reconciliation remain owned by the data-source control plane. Feature usage remains owned by model-serving, feature-registry, promotion, runtime, and execution gates.

## Identity And Routing Rules

- Built-in sources are singleton records.
- Built-in source identity is locked:
  - `source_type` cannot change
  - `provider_name` cannot change
  - `job_name` cannot change
- Built-in sources cannot be created through the API and cannot be deleted.
- Custom source creation is currently limited to `rss_feed`.
- Custom RSS sources are locked to:
  - `source_type = rss_feed`
  - `provider_name = rss`
  - `job_name = ingest_now`

## Public Source Record Shape

`list_sources()` and the `GET /api/data_sources` route expose DB-backed source records with fields including:

- `source_key`
- `display_name`
- `source_type`
- `provider_name`
- `job_name`
- `enabled`
- `default_enabled`
- `storage_tables`
- `consumers`
- `safe_to_auto_enable`
- `runtime_runnable`
- `runnable_state`
- `runnable_state_reason`
- `credential_required`
- `credential_status`
- `credential_status_reason`
- `needs_credentials`
- `missing_credentials`
- `effective_status`
- `stored_status`
- `runtime_credentialed`
- `runtime_projected`
- `runtime_desired_eligible`
- `missing_credential_env_vars`
- `projected_env_vars`
- `status`
- `last_error`
- `last_success_ts_ms`
- `last_test_ts_ms`
- `error_count`
- `settings`
- `updated_ts_ms`
- `credentials_configured`
- `credentials_stored`
- `credential_error`
- `credential_fields`
- `setting_fields`
- `masked_credentials`
- `credential_resolution`
- `account_keys`
- `template_key`
- `builtin`
- `singleton`
- `can_delete`
- `can_edit_identity`
- `can_edit_routing`
- `supports_test`

Raw credentials are not part of normal route responses.

Each public source row also includes:

- `data_contract`
  Runtime-owned contract for the smallest storage proof: normalized shape,
  required fields, units, symbol namespace, timestamp timezone, point-in-time
  availability semantics, unique key/idempotent upsert, storage table, consumer,
  timestamp field, and freshness TTL.
- `populate_evidence`
  Latest `data_source_populate_evidence` row for the source, sanitized for API
  output and UI display.

## HTTP Routes

| Method | Path | Contract |
| --- | --- | --- |
| `GET` | `/api/data_sources` | Returns `sources`, `templates`, `provider_accounts`, `provider_account_templates`, `runtime`, `auth`, and `desired_ingestion_jobs`. |
| `GET` | `/api/data_sources/logs?source_key=...&limit=...` | Returns source-specific log rows. |
| `POST` | `/api/data_sources/create` | Creates a custom source. Current supported custom type is `rss_feed`. |
| `POST` | `/api/data_sources/update` | Updates an existing source record. |
| `POST` | `/api/data_sources/delete` | Deletes a non-built-in source. |
| `POST` | `/api/data_sources/enable` | Enables an existing source and reconciles desired ingestion jobs. |
| `POST` | `/api/data_sources/disable` | Disables an existing source and reconciles desired ingestion jobs. |
| `POST` | `/api/data_sources/test` | Runs a provider-aware connection test and updates source status. |
| `POST` | `/api/data_sources/populate_now` | Runs a bounded provider-specific one-shot ingestion proof, verifies rows in the expected storage table against the source data contract, records evidence, and returns the latest evidence payload. |
| `POST` | `/api/data_sources/test_save` | Validates and stores source input, clears the ingestion credential cache, runs the provider-aware connection test, and returns the saved source plus test result. |
| `POST` | `/api/data_sources/accounts/update` | Updates encrypted shared provider-account credentials. |

`GET /api/data_sources` also returns:

- `provider_accounts`
  Shared provider-account status and masked credential state.
- `provider_account_templates`
  Backend-owned account schemas and used-by references.
- `auth.token_required`
  Whether clients must send `X-API-Token`
- `auth.actor_required`
  Whether a human-attribution actor should be supplied on mutations

## Mutation Payload Contract

Supported mutation fields are:

- `actor`
- `client_ip`
- `source_key`
- `display_name`
- `enabled`
- `settings`
- `credentials`
- `replace_credentials`
- `clear_credential_fields`

Behavior:

- `settings` must be an object.
- `credentials` must be an object.
- `replace_credentials=true` replaces the stored credential set with only the supplied fields.
- `replace_credentials=false` merges non-empty supplied credential values into the existing set.
- `clear_credential_fields` removes named credential keys from the stored credential set.
- For `rss_feed`, `settings.name` and `settings.url` are required.
- Submitted credential, clear, and setting fields must be declared by the selected template. Unknown submitted fields are rejected before persistence.
- Submitted non-empty values are checked against each field's optional `validation_regex`. Validation errors identify only the field name, not the submitted value.
- Existing rows with legacy extra settings can still be updated when the extra fields are not resubmitted; this preserves backward compatibility while enforcing validation on new payloads.
- Exact masked values returned by the API are ignored as credential submissions and preserve the existing encrypted value instead of persisting mask text.
- Generic masked placeholders such as `***` are rejected and are never encrypted as real credentials.

Provider account updates use:

- `actor`
- `client_ip`
- `account_key`
- `credentials`
- `replace_credentials`
- `clear_credential_fields`

The account update route applies the same field validation, masked-resubmission protection, AES-GCM storage, audit logging, credential-cache clearing, and lifecycle reconciliation as source updates.

`/api/data_sources/test_save` uses the same source payload as update/create plus optional `create=true`. The manager validates fields, encrypts supplied credentials, clears `engine.data._credentials.get_data_credential()` cache, resolves the effective credential through the ingestion path, and then runs the registered provider probe. If encryption fails, for example because the data-source master key is missing, the route returns `saved=false` and does not store the submitted credential payload. If the provider probe does not pass after a successful save, the source is left in `test_failed`, `test_degraded`, or `test_unsupported` with `last_error` set to `<classification>:<message>` rather than retaining a prior healthy status.

## Connection Test Contract

`test_connection()` dispatches through the explicit provider-test registry. Every built-in source key has either a concrete probe handler or an explicit unsupported status. There is no generic successful fall-through for real feeds.

HTTP connection-test routes return structured 4xx/5xx status metadata for expected setup and provider refusals. Missing credentials or missing provider settings return 422, provider credential rejection returns 401, missing provider entitlements return 403, rate limits return 429, and provider transport outages return 503. The payload includes `ok=false`, `classification`, `reason_code`, `provider_reason_code`, `message`, and `meta.status`; credential values are not included.

The source status mapping is:

- `pass` -> `tested`
- `fail` -> `test_failed`
- `degraded` -> `test_degraded`
- `unsupported` -> `test_unsupported`

Only `pass` returns `ok=true`, updates `last_success_ts_ms`, and records audit `success=true`. Degraded and unsupported results are intentionally not counted as successful connection tests.

Connection tests resolve credentials through the same effective path used by ingestion:

1. Source-level encrypted credentials are projected for the source.
2. Shared provider-account credentials are projected when the source inherits an account.
3. Strict/prod runtime projects secret values to files and reads them through `<ENV>_FILE`.
4. Existing external `<ENV>_FILE`, `<ENV>_SECRET`, secret-provider, and compatible plain env values are read by `engine.data._credentials.get_data_credential()`. A configured file path that is missing, empty, unreadable, or not a regular file returns no credential and is surfaced as missing credential metadata.
5. The credential cache is cleared before and after the test so rotations are visible immediately.

Missing-credential test responses include `evidence.missing_env_vars` with exact env var names and `evidence.missing_credentials[]` with the catalog docs, signup URL, plan note, source field, and provider-account options operators can use to obtain or store the credential.

Connection-test responses classify outcomes as `success`, `missing_credentials`, `wrong_credentials`, `provider_unreachable`, `rate_limited`, `entitlement_missing`, `empty_payload`, `malformed_payload`, `policy_blocked`, `degraded_fallback`, `partial_success`, or `unsupported`. HTTP 429 and 503 responses immediately return degraded results with `evidence.stop_testing=true`, sanitized endpoint evidence, retry-after guidance when available, and no further fallback probes for composite providers.

Implemented checks include:

- Polygon REST and WebSocket credentials
- Polygon options snapshot credentials
- Tradier options access
- Finnhub company news
- FMP transcripts and earnings
- Reddit via `praw`
- Stocktwits public trending payload with explicit JSON schema validation and 429 cooldown in the runtime poller
- GDELT article payload with runtime 429/503 cooldown and retry-after honoring
- CFTC COT public reporting API payload
- FINRA short-volume public file payload with header identity, row parsing, and stop-cycle behavior on 401/403/429/503
- FINRA short-interest Query API payload with header identity, JSON shape validation, and degraded empty-payload reporting
- CCXT ticker payload and crypto funding-rate payload
- SEC company ticker payload, required non-placeholder SEC caller identity, Form 4 ownership XML information-document discovery, and 13F Atom feed
- Congressional trades JSON feed with per-source runtime status and malformed/empty-payload detection
- Quiver government-flow payload
- SimFin and Sharadar point-in-time fundamentals, including partial-success degradation when only one selected vendor passes
- FRED observations with ALFRED CSV fallback clearly marked `degraded`; when `FRED_API_KEY` is missing and ALFRED fallback is allowed, runtime reports `fred_api_key_missing_alfred_fallback_used`
- news-flow OpenAI embeddings only when `NEWS_EMBED_BACKEND=openai`; non-external hashing backends return `unsupported`
- weather forecast and NWS alert endpoints; NWS active alerts validate `FeatureCollection` schema and honor 429/503 cooldown
- IBKR read-only authenticated historical-data request through TWS/Gateway, including market-data type selection
- Alpaca read-only account and positions GET probes; the data-source test and Populate Now path never call order, cancel, replace, or flatten paths
- Simulated local prices, which require no external credentials, perform no broker or provider network calls, and return deterministic payload evidence marked `simulated=true`
- custom RSS/Atom feed payload validation plus per-feed runtime status so one failed publisher does not hide successful RSS sources

Optional public-service live smoke is available through `python tools/public_feed_live_smoke.py`. It exits without network access unless `PUBLIC_FEED_LIVE_SMOKE=1` is set explicitly; normal CI and `validate_repo.py` use mocked tests and do not contact public services by default.

If a source type does not have a meaningful active connectivity probe, the registry returns an explicit unsupported result that is not counted as a passing connection test.

Broker-data sources are intentionally separate from execution authority. `engine.data.broker_readonly` owns the static allowlists used by broker data-source probes:

- Alpaca permits only `GET /v2/account` and `GET /v2/positions` from the data-source test and Populate Now paths. `GET /v2/orders`, order submission, cancel, replace, and flatten paths are rejected before any HTTP call. `https://paper-api.alpaca.markets` is the safe default; `https://api.alpaca.markets` is blocked unless `DATA_SOURCE_ALLOW_LIVE_ALPACA_BROKER_DATA=1` is set intentionally for read-only account visibility.
- IBKR permits only `connect(readonly=True)`, `reqMarketDataType`, `qualifyContracts`, `reqHistoricalData`, `isConnected`, and `disconnect` from the data-source test path. A running authenticated TWS/Gateway with market-data or historical-data permissions is required.
- Forbidden broker mutation symbols such as `submit_order`, `submit_market_order`, `submit_limit_order`, `replace_limit_order`, `placeOrder`, `cancelOrder`, `cancel_open_orders`, and `flatten_positions` are rejected by the adapter guard.

`alpaca_broker_data` is `runtime_runnable=false`; enabling it records read-only broker-data guidance and credential status, but `get_desired_ingestion_jobs()` and `build_job_environment()` do not schedule or project Alpaca credentials for any data-source daemon. Runtime projection calls the same broker-source allowlist before a broker row can become desired. Order, cancel, replace, and flatten paths remain owned by broker execution controls and are not reachable from data-source test, enable, lifecycle, or health projection paths.

## Safe/Sim Price Ingestion

Safe and simulated runtime modes have a deterministic local market-data path through the `simulated` provider. It generates bounded quote events from symbol and minute-bucket inputs, labels every row with provider/source `simulated`, and writes through the normal price ingestion router into `prices`, `price_quotes`, `price_quotes_raw`, and `price_provider_health`. This is a simulator feed for validation, not evidence that Polygon, yfinance, CCXT, or any other production provider is healthy.

`SIMULATED_MARKET_DATA_ENABLED=1` enables the provider explicitly. When the stack is in safe/sim/paper execution with a simulated broker, runtime bootstrap and ingestion child selection also allow the simulated feed so acceptance tests can produce fresh rows without real broker, exchange, or paid-provider credentials. `SIMULATED_MARKET_DATA_SYMBOLS` controls the symbols; otherwise the simulator uses a small deterministic equity universe.

Missing credentials for real providers remain explicit `missing_credentials` failures. Transient provider errors and rate limits are classified separately in `poll_prices` job status, and the simulated provider may be appended as a fallback only when safe/sim mode permits it. No production provider success is inferred from simulated rows.

Freshness is observable in the database and API. `GET /api/feeds` includes `price_freshness.status`, `stale`, `age_s`, `max_age_s`, `source`, and `simulated` based on the latest `prices` row. Runtime health exposes the same stale/fresh distinction through the `prices` check, so dashboards can render fresh simulated data differently from stale or missing production data.

## Populate Now Contract

`populate_now()` is a bounded operator action for proving a feed can place at
least one normalized row in the storage table named by its code-defined
contract. It is not a broad backfill. The manager resolves credentials through
the same encrypted/account/env path as ingestion, obeys the source test rate
limit, and stops on provider 429 or 503 responses with a degraded result.

The runtime evidence row records:

- provider evidence, sanitized before API/log output
- row count in the expected storage table for the source filter
- storage table and latest timestamp
- end-to-end latency
- required-field missing/null counts
- duplicate drops detected by the contract unique key
- stale/gap status
- embedded data contract and contract `pass`, `warn`, or `fail`

`GET /api/data_sources` surfaces both `data_contract` and
`populate_evidence`. The Data Sources UI renders the evidence block on the
source detail panel and exposes a `Populate Now` action beside the connection
test button.

Production health is gated by this evidence. A source that otherwise looks
healthy is downgraded to `degraded` with `contract_health_gate` when no
Populate Now row has landed, when the latest evidence row count is zero, or
when the latest contract status is not `pass`. This check lives in
`attach_runtime_states_to_sources()`, so dashboard reads and API clients see
the same enforced state.

## Runtime Coupling

The control plane affects runtime behavior through these methods:

| Method | Effect |
| --- | --- |
| `apply_runtime_environment()` | Projects enabled DB-backed source settings into process environment variables for legacy jobs that still read `os.environ`. |
| `get_desired_ingestion_jobs()` | Computes the daemon jobs that should be running based on enabled, runnable, credential-projectable sources. |
| `manage_lifecycle()` | Marks the runtime dirty, starts `ingestion_runtime` when runnable jobs appear, and stops it when no runnable desired jobs remain and a jobs manager is available. |
| `get_runtime_snapshot()` | Returns `provider_telemetry`, `pipeline_health`, `ingestion_state`, per-job runnable states, and desired jobs for the UI. |

Important consequence:

- changing source configuration can change the desired ingestion job set without editing `.env`
- disabling `polygon_options` removes Polygon from `options_poll`; a generic Polygon REST key no longer implies options ingestion
- default-off alternate-data feeds (`cftc_cot`, FINRA short-volume/short-interest, and `crypto_funding`) project their `INGEST_*_ENABLED` runtime flags only after the corresponding source row is enabled
- changing provider-account credentials clears the ingestion credential cache, marks the control-plane runtime dirty, and changes relevant job config hashes so supervised children can refresh effective env projection

## Operator Rules

- Use [ui/data_sources.html](../ui/data_sources.html) as the single human-facing setup page for source configuration.
- Do not build a second long-lived provider-credential flow in the operator console or dashboard.
- Keep provider guidance, field help, env mapping, validation hints, and safety warnings in the backend catalog so `GET /api/data_sources` remains the UI source of truth.
- Keep the master key outside the database.
- Treat the route and payload shapes above as the control-plane contract until the corresponding OpenAPI paths are added under [openapi/openapi.yaml](openapi/openapi.yaml).
