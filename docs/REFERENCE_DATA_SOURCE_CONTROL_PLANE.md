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
- provider credentials and source-specific settings
- encrypted-at-rest credential storage
- source enable and disable actions
- source creation and deletion for custom RSS feeds
- connection testing
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
| [ui/data_sources.js](../ui/data_sources.js) | Browser controller for inventory, detail panes, session token handling, and mutations against `/api/data_sources/*`. |
| [routes/data_sources_routes.py](../routes/data_sources_routes.py) | HTTP route module for the data-source control plane. |
| [services/data_source_manager.py](../services/data_source_manager.py) | Source-of-truth manager for storage, env projection, lifecycle reconciliation, testing, and source templates. |
| [services/credential_encryption.py](../services/credential_encryption.py) | AES-GCM encryption, decryption, and masking for stored credentials. |

## Storage Contract

The manager ensures these tables exist:

| Table | Purpose |
| --- | --- |
| `data_sources` | Canonical source inventory and source configuration. |
| `data_source_logs` | Source-specific operational log events. |
| `data_source_audit` | Actor-attributed audit records for create, update, delete, enable, disable, and test actions. |
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

## Built-In Source Templates

These templates are seeded automatically by [services/data_source_manager.py](../services/data_source_manager.py).

| Source Key | Source Type | Job Name | Default Enabled | Credentials |
| --- | --- | --- | --- | --- |
| `polygon_ws` | `price_provider` | `stream_prices_polygon_ws` | Yes | `api_key` |
| `polygon` | `price_provider` | `poll_prices` | Yes | `api_key` |
| `ibkr` | `price_provider` | `stream_prices_ibkr` | No | None; host/port/client settings are stored as source settings |
| `yfinance` | `price_provider` | `poll_prices` | Yes | None |
| `ccxt` | `price_provider` | `poll_prices` | Yes | None |
| `tradier` | `options_provider` | `options_poll` | Yes | `api_token` |
| `reddit` | `social_provider` | `poll_social_reddit` | Yes | `client_id`, `client_secret` |
| `stocktwits` | `social_provider` | `poll_social_stocktwits` | Yes | None |
| `company_news` | `news_provider` | `ingest_now` | Yes | `api_key` |
| `transcripts` | `news_provider` | `ingest_now` | Yes | `api_key` |
| `gdelt` | `news_provider` | `poll_gdelt` | Yes | None |
| `sec` | `filings_provider` | `poll_sec_filings` | Yes | None; SEC caller identity is carried through settings and env projection |
| `form4` | `filings_provider` | `ingest_form4` | No | None |
| `congressional_trades` | `legislative_provider` | `ingest_congressional_trades` | No | None |
| `earnings` | `calendar_provider` | `poll_earnings` | Yes | `api_key` |
| `weather_forecasts` | `weather_provider` | `poll_weather_forecasts` | Yes | None |
| `weather_alerts` | `weather_provider` | `poll_weather_alerts` | Yes | None |
| `macro` | `macro_provider` | `poll_macro` | Yes | None |
| `model_feature_snapshots` | `feature_snapshot` | `snapshot_model_features` | Yes | None |
| `rss_feed` | `rss_feed` | `ingest_now` | Custom | None; user supplies `name` and `url` in settings |

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
- `template_key`
- `builtin`
- `singleton`
- `can_delete`
- `can_edit_identity`
- `can_edit_routing`
- `supports_test`

Raw credentials are not part of normal route responses.

## HTTP Routes

| Method | Path | Contract |
| --- | --- | --- |
| `GET` | `/api/data_sources` | Returns `sources`, `templates`, `runtime`, `auth`, and `desired_ingestion_jobs`. |
| `GET` | `/api/data_sources/logs?source_key=...&limit=...` | Returns source-specific log rows. |
| `POST` | `/api/data_sources/create` | Creates a custom source. Current supported custom type is `rss_feed`. |
| `POST` | `/api/data_sources/update` | Updates an existing source record. |
| `POST` | `/api/data_sources/delete` | Deletes a non-built-in source. |
| `POST` | `/api/data_sources/enable` | Enables an existing source and reconciles desired ingestion jobs. |
| `POST` | `/api/data_sources/disable` | Disables an existing source and reconciles desired ingestion jobs. |
| `POST` | `/api/data_sources/test` | Runs a provider-aware connection test and updates source status. |

`GET /api/data_sources` also returns:

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

## Connection Test Contract

`test_connection()` performs provider-aware checks and updates `data_sources.status` to either `tested` or `test_failed`.

Implemented checks include:

- Polygon REST and WebSocket credentials
- Tradier options access
- Finnhub company news
- FMP transcripts and earnings
- Reddit via `praw`
- Stocktwits public endpoint reachability
- GDELT API reachability
- SEC public endpoint reachability
- weather forecast and alert endpoints
- IBKR socket reachability
- custom RSS feed URL reachability

If a source type does not need an active connectivity probe, the manager returns `connection_test_not_required`.

## Runtime Coupling

The control plane affects runtime behavior through these methods:

| Method | Effect |
| --- | --- |
| `apply_runtime_environment()` | Projects enabled DB-backed source settings into process environment variables for legacy jobs that still read `os.environ`. |
| `get_desired_ingestion_jobs()` | Computes the daemon jobs that should be running based on enabled sources. |
| `manage_lifecycle()` | Marks the runtime dirty and optionally starts `ingestion_runtime` when a jobs manager is available. |
| `get_runtime_snapshot()` | Returns `provider_telemetry` and `pipeline_health` summaries for the UI. |

Important consequence:

- changing source configuration can change the desired ingestion job set without editing `.env`

## Operator Rules

- Use [ui/data_sources.html](../ui/data_sources.html) as the single human-facing setup page for source configuration.
- Do not build a second long-lived provider-credential flow in the operator console or dashboard.
- Keep the master key outside the database.
- Treat the route and payload shapes above as the control-plane contract until the corresponding OpenAPI paths are added under [openapi/openapi.yaml](openapi/openapi.yaml).
