# Failure Modes

This document captures the recurrent failure classes that are explicit in the current code paths. It is grounded in `start_system.py`, `dashboard_server.py`, `engine/runtime/gates.py`, `engine/runtime/health.py`, `engine/runtime/ingestion_runtime.py`, `engine/runtime/failure_diagnostics.py`, `engine/execution/kill_switch.py`, `engine/execution/broker_apply_orders.py`, and the operator-facing APIs under `engine/api/api_system.py`.

## Fail-Closed Principle

The runtime is intentionally conservative in safety-critical paths:

- `engine/runtime/gates.py` blocks execution on unknown or critical runtime state.
- `engine/execution/kill_switch.py` adds an execution-specific kill-switch cascade.
- `engine/api/api_system.py` mirrors those states through `/api/execution/barrier`, `/api/readiness`, and `/api/operator/support_snapshot`.

## Common Failure Classes

| Failure class | Primary surfaces | First files to inspect |
| --- | --- | --- |
| Startup or preflight failure | `start_system.py`, `dashboard_server.py`, `/api/readiness`, `/api/operator/preflight_report` | `start_system.py`, `dashboard_server.py`, `engine/runtime/health.py`, `engine/runtime/job_registry.py` |
| Schema or storage failure | `runtime_failure` event-log rows, `/api/operator/support_snapshot`, DB validation output | `engine/runtime/storage.py`, `engine/runtime/db_repair.py`, `engine/runtime/jobs/repair_schema.py` |
| Ingestion runtime not running or stale | `/api/ingestion/status`, `/api/operator/runtime_watchdogs`, `/api/operator/provider_telemetry` | `start_ingestion.py`, `engine/runtime/ingestion_runtime.py`, `engine/runtime/ingestion_status.py` |
| Provider auth or source configuration failure | Data Sources Control Center, `data_source_logs`, `/api/data_sources/logs` | `services/data_source_manager.py`, `routes/data_sources_routes.py`, `services/credential_encryption.py` |
| Execution barrier block | `/api/execution/barrier`, `/api/readiness`, dashboard safety panels | `engine/runtime/gates.py`, `engine/execution/kill_switch.py`, `engine/execution/broker_apply_orders.py` |
| Crash recovery continuity gap | `/api/execution/barrier`, `runtime_meta.crash_recovery_state`, `runtime_metrics` rows for `crash_recovery_*` | `engine/runtime/crash_recovery.py`, `engine/runtime/runtime_bootstrap.py`, `engine/runtime/gates.py` |
| Portfolio risk block | `/api/risk/portfolio`, `/api/risk/monte_carlo`, `/api/execution/barrier` | `engine/risk/portfolio_risk_engine.py`, `engine/risk/monte_carlo_risk_engine.py`, `engine/runtime/risk_state.py` |
| Broker connection or execution-quality degradation | `/api/operator/runtime_watchdogs`, execution metrics APIs, execution barrier reasons | `engine/execution/execution_broker_watchdog.py`, `engine/execution/execution_quality_supervisor.py`, `engine/execution/broker_router.py` |
| Post-trade attribution or reconciliation failure | Attribution quality APIs, `pnl_attribution`, `trade_attribution_ledger`, recent errors | `engine/execution/execution_poll_and_attrib.py`, `engine/execution/execution_ledger.py`, `engine/runtime/trade_lifecycle.py` |
| Critical alert delivery not configured | Production preflight, notification channel status, runtime warning logs | `engine/runtime/prod_preflight.py`, `engine/runtime/alerts_notify.py`, `deploy/env/trading.env.example` |
| WAL archive or storage placement failure | `alerts`, `component_health.storage_wal_guards`, `postgres.wal.alert_state`, production preflight, Timescale compose startup | `deploy/compose/docker-compose.external-services.yml`, `engine/runtime/storage_placement.py`, `engine/strategy/jobs/observability_snapshot.py`, `ops/backup/wal_archive.sh`, `ops/backup/wal_archive_catchup.sh` |

## systemd Watchdog Heartbeat Contract

Production engine and operator units are `Type=notify` services with
`WatchdogSec=60s`. The engine sends `READY=1` only after startup validation and
startup-health validation pass, then sends `WATCHDOG=1` at `<=30s` cadence from
the ingestion watchdog loop or from a fallback systemd-watchdog thread when
`START_INGESTION_WITH_SERVER=0`. The heartbeat is lifecycle-gated and only
pings in `LIVE` or `WARMING_UP`; a degraded, kill-switch, or stuck runtime stops
pinging, so a missed watchdog deadline lets systemd send `SIGABRT` and apply
`Restart=on-failure` within the existing `StartLimitBurst` guard.

The operator sends `READY=1` after the HTTP listener is bound and sends
`WATCHDOG=1` at `<=30s` cadence after its periodic watchdog body completes
without an unrecoverable exception. The engine uses `NotifyAccess=main`; the
operator uses `NotifyAccess=all` so its `/usr/bin/systemd-notify` fallback is
accepted if the native `unix-dgram` module is unavailable on a host. The primary
path for both services is still direct AF_UNIX datagrams from the main process,
including systemd's NUL-prefixed abstract socket form.

Memory cgroups make the supervised app processes preferred victims over
co-located Postgres, Redis, and storage services. The engine has
`MemoryHigh=24G`, `MemoryMax=32G`, and `OOMScoreAdjust=600`; the operator has
`MemoryHigh=4G`, `MemoryMax=6G`, and `OOMScoreAdjust=700`. These limits are
sized against the 128 GiB host budget in
[MEMORY_PRESSURE_RUNBOOK.md](MEMORY_PRESSURE_RUNBOOK.md), including the 48 GiB
ZFS ARC cap plus managed zram and disk swap. To retune them, edit the installed
unit or deploy a systemd drop-in, then run `systemctl daemon-reload` and restart
the affected service. Validate the applied state with `systemctl show`; inactive
units can still show `WatchdogUSec=infinity`, so the go-live gate requires the
engine and operator services to be restarted and active before accepting the
watchdog property. Both long-lived units set `TimeoutStartSec=300s` so normal
startup validation and safe-mode warmup are not killed before readiness can be
reported.

## Critical Alert Delivery

Live go-live requires at least one off-box critical alert channel before
production preflight can pass. Configure either SMTP with both
`EQ_CRIT_SMTP_HOST` and `EQ_CRIT_EMAIL_TO`, or set `EQ_CRIT_WEBHOOK_URL` to a
Slack, Discord, or generic HTTPS webhook. The notification status API and
preflight gate report which channels are configured and enabled.

Operator runbook before go-live:

```bash
# Pick at least one delivery path in deploy/env/trading.env or the host secret/env source.
EQ_CRIT_SMTP_HOST=smtp.example.com
EQ_CRIT_EMAIL_TO=oncall@example.com
# or:
EQ_CRIT_WEBHOOK_URL=https://hooks.example.com/services/...
```

If a CRITICAL runtime alert has zero successful deliveries, the notifier logs a
structured warning once per process. Equity reconciliation CRITICAL alerts are
sent through the same runtime notification path after the alert row is inserted.

## WAL Archive And Storage Placement

Production Compose blocks `timescaledb` behind the one-shot
`storage-placement-preflight` service. That service mounts the same host paths
read-only and runs `engine.runtime.storage_placement`; a root-backed,
`/var/lib/docker`/`/var/lib/containerd`, or non-`zfs` PGDATA, `pg_wal`, Redis,
MinIO, runtime, or backup target exits non-zero before Postgres starts.

`archive_mode` stays `on`, `archive_command` must invoke
`/opt/trading/ops/backup/wal_archive.sh "%p" "%f"`, and
`TS_WAL_ARCHIVE_REQUIRE_MOUNT=1` makes the archive script fail closed if
`/var/backups/trading` is missing or only backed by `/`. Do not replace the
command with `/bin/true` or an inline `cp`; production preflight rejects that as
unaudited PITR.

During runtime, `observability_snapshot` evaluates the same WAL budgets every
cycle. It records `postgres.wal.alert_state`, writes
`component_health.storage_wal_guards`, and emits runtime alerts for rising
`pg_stat_archiver.failed_count`, recent or unrecovered `last_failed_wal`,
excessive `pg_wal` bytes, `.ready` backlog, or low WAL/backup free space. Stable
payloads are fingerprinted so the job does not page every 60s, but changed
segments, backlog, free bytes, or recovery followed by recurrence emit again.

Recovery after restoring the archive mount:

```bash
# operator-run: privileged host-side moves and monitor install
sudo bash ops/server/disk_remediation.sh relocate-backups
sudo bash ops/server/disk_remediation.sh relocate-docker
sudo bash ops/server/disk_remediation.sh install-monitor

# operator-run or container shell: drain already-ready WAL after the mount is healthy
docker compose --env-file deploy/compose/.env -f deploy/compose/docker-compose.external-services.yml \
  exec -u postgres timescaledb /opt/trading/ops/backup/wal_archive_catchup.sh
```

The agent shell may not have `sudo`; privileged ZFS dataset creation, Docker
data-root relocation, mount changes, and host monitor installation are
operator-run steps.

## Crash Recovery Continuity

Boot-time crash recovery writes `runtime_meta.crash_recovery_state` after every
run. For `alpaca` and `ibkr`, recovery must prove broker open orders, recent
broker fills, broker positions, and pre-live reconciliation continuity before
live order authority can be enabled. If any of those reads or reconciliations
fails, `engine/runtime/crash_recovery.py` records a critical gap, emits
`crash_recovery_continuity_gap_total`, `crash_recovery_gap_count`, and
`crash_recovery_continuity_proven=0`, and sets the in-process
`CRASH_RECOVERY_FAIL_CLOSED` fallback.

`engine/runtime/gates.py` consumes that state directly. In live mode it returns
`real_trading_allowed=false`, `allow_execution_pipeline=false`, and a
`critical_crash_recovery_*` reason until a later successful recovery run writes
an `ok` state. Event-log/audit append failures during recovery remain
best-effort telemetry: they are logged, but they do not erase the durable
`crash_recovery_audit` row or hide live-blocking continuity gaps.

## What The Operator APIs Already Provide

The current operator-facing APIs give a bounded first-pass diagnosis without log scraping:

- `/api/execution/barrier`
  The fastest answer to "why is trading blocked?"
- `/api/operator/runtime_watchdogs`
  Job freshness, restart counters, and ingestion watchdog state.
- `/api/operator/provider_telemetry`
  Feed and provider freshness plus active child ownership.
- `/api/operator/support_snapshot`
  Preflight, DB debug state, recent failures, watchdogs, and synthesized diagnostics.

## Event And Failure Logging

`engine/runtime/failure_diagnostics.py` standardizes failure capture:

- `log_failure(...)` records structured failure payloads and can persist them.
- `failure_response(...)` returns API-safe envelopes that include `root_cause_code`, `failure_scope`, and a system-state snapshot.
- persisted failures land in `event_log` as `runtime_failure` events.

When a failure is hard to localize, start with the latest `runtime_failure` event or the latest support snapshot instead of isolated stderr text.
