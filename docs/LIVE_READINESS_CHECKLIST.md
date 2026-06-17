# Live Readiness Checklist

Use this checklist before moving from safe/paper operation to live trading.

## Required Environment

- `ENGINE_MODE=live`
- `EXECUTION_MODE=live`
- `LIVE_BROKER`, `BROKER`, `BROKER_NAME`, and the first `BROKER_FAILOVER` entry identify the same intended live broker (`ibkr` or `alpaca`).
- `BROKER_FAILOVER` does not include `sim`, `paper`, or `sandbox` in live mode.
- `DASHBOARD_API_TOKEN` is set to a non-default secret.
- `LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING`
- `DISABLE_LIVE_EXECUTION` is unset or explicitly false (`0`, `false`, `no`, or `off`). Any other non-empty value is treated as enabled and blocks live execution.
- `KILL_SWITCH_GLOBAL=1` remains armed for the initial live deployment hold until operator signoff. Real execution still requires audited DB arming through `execution_mode`/`execution_mode_audit`; environment variables must not arm execution.
- `DASHBOARD_HOST` is loopback unless remote access is intentional and token-protected.
- Alpaca live deployments use `ALPACA_BASE_URL=https://api.alpaca.markets`; the paper endpoint is rejected in live mode. IBKR live deployments set `IBKR_HOST`, `IBKR_PORT`, and `IBKR_CLIENT_ID` explicitly.

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
- Confirm kill switch activation blocks execution and recovery stays audited.
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
