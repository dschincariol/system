# Live Readiness Checklist

Use this checklist before moving from safe/paper operation to live trading.

## Required Environment

- `ENGINE_MODE=live`
- `DASHBOARD_API_TOKEN` is set to a non-default secret.
- `LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING`
- `DISABLE_LIVE_EXECUTION` is unset or `0`.
- `DASHBOARD_HOST` is loopback unless remote access is intentional and token-protected.

## Storage

- SQLite is used only for control-plane state, configuration, audit records, and small local state.
- High-volume price/quote/raw writes use async/Timescale or another append-oriented store.
- `PRICE_ROUTER_BLOCK_SYNC_SQLITE_IN_LIVE=1`
- `PRICE_ROUTER_ALLOW_SYNC_SQLITE_IN_LIVE=0` unless a live exception is explicitly documented.

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
