# Live Readiness Checklist

Use this checklist before moving from safe/paper operation to live trading.

Canonical production-prep procedure for enabling real trading: see [PRODUCTION_CHECKLIST.md](PRODUCTION_CHECKLIST.md) §6 ("Before Enabling Real Trading"). The items below are the live-trading-specific environment and readiness assertions; where they restate §6, treat §6 as the authoritative procedural source and the entries here as the live-mode requirement checklist.

## Required Environment

- `ENGINE_MODE=live`
- `EXECUTION_MODE=live`
- `LIVE_BROKER`, `BROKER`, `BROKER_NAME`, and every `BROKER_FAILOVER` entry identify the same intended live broker (`ibkr` or `alpaca`).
- `BROKER_FAILOVER` does not include `sim`, `paper`, `sandbox`, or mixed live brokers in live mode.
- `DASHBOARD_API_TOKEN` is set to a non-default secret, and remote `/operator/api/*` bridge reads and mutations require that same dashboard token before the dashboard proxies to the operator sidecar.
- `OPERATOR_API_TOKEN` is set to a generated non-default secret. Direct sidecar protected GET/HEAD/POST and `/ws/operator` access require that token; loopback alone is not authorization and only `/api/operator/ping` is unauthenticated.
- `DATA_SOURCE_MASTER_KEY` or `DATA_SOURCE_MASTER_KEY_FILE` resolves to canonical base64 text for exactly 32 random bytes. Prefer `DATA_SOURCE_MASTER_KEY_FILE` with mode `0600`; generate it with `openssl rand -base64 32 > /var/lib/trading/.data_source_master_key`. Raw text is dev-only and fails live/prod preflight.
- `LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING` is set only in the target host's operator-controlled deployment configuration. Committed examples keep it empty or `0` so live preflight fails closed.
- `DISABLE_LIVE_EXECUTION` is explicitly false (`0`, `false`, `no`, or `off`) only after approved live-enablement signoff. Unset, truthy, and unknown non-empty values are treated as enabled and block live execution.
- `KILL_SWITCH_GLOBAL=1` remains armed for the initial live deployment hold until operator signoff. Real execution still requires audited DB arming through `execution_mode`/`execution_mode_audit`; environment variables must not arm execution. Live preflight recomputes the canonical `execution_mode_audit` row hashes and previous-hash chain before accepting DB arming, and blocks on missing latest signoff rows, missing `prev_hash`, row-hash mismatches, actor/reason/mode/armed tampering, or timestamp order breaks.
- `RULES_AUTO_RESUME` is unset or `0` unless the deployment has explicitly accepted audited automatic recovery for rules-owned rows. Rules auto-resume never clears operator/manual/emergency/startup/preflight/break-glass holds; those require `POST /api/operator/clear_manual_halt` with `CLEAR_MANUAL_HALT` confirmation, actor, source, acknowledgement, and reason.
- `DASHBOARD_HOST` is loopback unless remote access is intentional and token-protected. Token-protected remote access includes the same-origin operator bridge: browsers must supply `X-API-Token` for protected `/operator/api/*` reads and mutations, and the dashboard must forward only the server-side `OPERATOR_API_TOKEN` to the sidecar.
- Alpaca live deployments use `ALPACA_BASE_URL=https://api.alpaca.markets`; the paper endpoint is rejected in live mode. IBKR live deployments set `IBKR_HOST`, `IBKR_PORT`, and `IBKR_CLIENT_ID` explicitly.
- Live AI safety is explicit: `DECISION_ENGINE_ENABLED=1`, `DECISION_MIN_CONFIDENCE`, `DECISION_MIN_ABS_PREDICTION`, `UNCERTAINTY_SIZING_PRODUCTION_POLICY`, `UNCERTAINTY_HIGH_THRESHOLD`, `UNCERTAINTY_HARD_THRESHOLD`, `UNCERTAINTY_MAX_AGE_MS`, `OOD_SUPPRESS_THRESHOLD`, and `OOD_HARD_THRESHOLD` are set before live sizing. Missing values fail live preflight and block risk-increasing live orders.
- Live model serving resolves without fallback for `LIVE_AI_PREFLIGHT_SYMBOLS` and `LIVE_AI_PREFLIGHT_HORIZONS_S`; every resolved model has a readable artifact alias, SHA, or path. Online models must be fitted before live prediction, and RL fallback agents or placeholder policies must remain shadow-only.
- `UNCERTAINTY_SIZING_PRODUCTION_POLICY` is explicitly set before live sizing. Missing values block risk-increasing live orders; valid values are `log_only`, `shrink`, and `strict`.
- `CONFORMAL_MODE`, `OOD_MODE`, and `UNCERTAINTY_SIZING_MODE` reflect the accepted rollout policy for conformal intervals, OOD scores, and model/epistemic uncertainty.
- Options-as-instruments is shadow-only by default: keep `OPTIONS_INSTRUMENTS_MODE=shadow`. Options chain data and options-derived features may run, but live options orders are blocked by `engine.execution.options_readiness`, the broker router, direct Alpaca/IBKR adapter checks, runtime config validation, and live preflight until a reviewed live options broker adapter exists. Any future live enablement must require Greeks, liquidity filters, bid/ask quality, assignment/exercise handling, expiration risk, margin impact, broker support, position limits, and kill-switch integration.

## Storage

- Postgres runtime storage is reachable through `TS_PG_DSN` or the platform default DSN, and `python engine/runtime/prod_preflight.py --json` reports the storage contract as healthy.
- `DB_PATH` points at an absolute local data directory for artifacts, diagnostics, and legacy identity only. It is not the runtime database.
- SQLite is used only for isolated Python tests or explicit legacy compatibility checks, not live control-plane state.
- High-volume price/quote/raw writes use async/Timescale or another append-oriented store when those sidecars are enabled.
- Storage readiness and startup gates are fail closed: unresolved `database_reachable` or `schema_valid` blockers must prevent live enablement.
- `PRICE_ROUTER_BLOCK_SYNC_SQLITE_IN_LIVE=1`
- `PRICE_ROUTER_ALLOW_SYNC_SQLITE_IN_LIVE=0` unless a live exception is explicitly documented.
- Postgres backup, WAL archive, and restore-drill evidence under `ops/backup/` is current enough for the intended RPO/RTO.

## Paper Soak

- Run at least one full market session in paper mode with live data ingestion.
- Confirm terminal order, flatten, and rollback actions work in paper mode.
- Confirm any options intents or hedging overlays remain `execution_target=shadow` or paper-only and never reach a live broker adapter.
- Confirm kill switch activation blocks execution and recovery stays audited.
- Confirm automatic rules recovery clears only `actor=rules_engine` rows with matching `meta_json.trigger`; operator/manual rows remain active until the explicit manual clear endpoint is used.
- Confirm kill-switch cache diagnostics show a fresh bounded snapshot: `cache_fresh=true`, `cache_age_ms <= max_age_ms`, and `source`/`cache_status` present. Stale cache plus unavailable storage must block order flow as `provider_unavailable`, not clear the switch state.
- Confirm broker reconciliation has no stale or orphan positions.
- Confirm dashboard mutation routes require POST and confirmation bodies.

## Validation

- `python tools/validate_repo.py`
- `python tools/validate_dependency_lock.py`
- `python tools/git_worktree_triage.py`
- Targeted execution-gate, dashboard-route, terminal-order, and storage-policy tests pass.

## Manual Sign-Off

- Review active models, champion/challenger status, and latest promotion audit.
- Review provider health and data freshness.
- Review current open orders and broker positions.
- Review current git diff and confirm no generated/runtime files are included in the live change set.
