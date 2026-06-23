# Boot And Operator Layer

The `boot/` directory contains the local launch and operator surface for the system.

Supported platform: Linux only.

## File Roles

- [start_operator.sh](start_operator.sh)
  Shell launcher for Linux environments.
- [operator_server.js](operator_server.js)
  Local Node operator service on port `4001` that proxies dashboard/runtime reads, owns guided controls, and manages launcher/process workflows.
- [operator_ui.html](operator_ui.html)
  Browser UI for operator controls, bootstrap diagnostics, service health, and guided recovery actions.
  It links operators into the main dashboard, repair tooling, and newer control-plane surfaces such as the Data Sources Control Center and terminal.

## Data Source Configuration Boundary

The operator layer is no longer a second source-configuration system.

- Use [ui/data_sources.html](../ui/data_sources.html) as the single source of truth for provider setup, credential storage, testing, enablement, and resets.
- Provider credentials and source-specific settings are stored in the database through [services/data_source_manager.py](../services/data_source_manager.py), not maintained as live `.env` feed config.
- The operator service may still return deprecation responses on old feed-config routes so stale callers can be redirected cleanly.

## Current Operator Repair Surfaces

The boot/operator layer now also owns the guarded repair boundary for operator AI.

- `/api/operator/support_snapshot`
  repair-oriented evidence bundle from the dashboard/runtime API layer
- `/api/operator/ping`
  lightweight sidecar liveness endpoint; the Python dashboard also exposes a same-origin `GET /api/operator/ping` bridge that proxies this sidecar ping and returns `operator_sidecar_unreachable` when the sidecar is down
- `/api/operator/runtime_watchdogs`
  summarized watchdog view for stalled runtime paths
- `/api/operator/provider_telemetry`
  feed/provider telemetry proxy
- `/api/operator/ai/run`
  bounded AI diagnosis run; the returned `result.action` is currently `null`
- `/api/operator/ai/explain`
  explanation-only AI diagnosis path with no action execution
- `/api/operator/ai/patch_preview`
  preview AI-derived patch advice without applying changes
- `/api/operator/ai/apply_patch`
  confirmation-gated patch application path, blocked in live mode
- `/api/operator/ai/rollback_patch`
  confirmation-gated rollback for previously applied operator patches
- `/api/operator/ai/last_patch`
  returns the latest guarded operator patch metadata

## Structured Operator Confirmations

High-impact operator mutations must use structured confirmation payloads instead
of native browser dialog flows. The operator console uses
the shared [../ui/confirmation_modal.mjs](../ui/confirmation_modal.mjs) helper
for live start, guided bootstrap, stop/restart, emergency stop, factory reset,
repair/admin actions, secret changes, backups, updates, feed restarts, and
operator-AI patch actions.

The browser modal is advisory UI. `operator_server.js` remains the authoritative
sidecar gate for its high-impact routes and rejects missing or invalid
confirmation before the mutation runs. Confirmation payloads carry `action_id`,
typed confirmation token, hold duration when required, `consequence_ack`,
`actor`, `source`/`source_surface`, `reason`, `request_id`, and `target`.
Sidecar audit records are appended to
`var/tmp/operator/operator_confirmation_audit.jsonl` by default, with sensitive
values redacted or hashed.

## Operator Bridge And Snapshot Auth

The canonical operator control service is the Node sidecar on port `4001`; the dashboard bridge exists for same-origin browser access. Dashboard `/api/operator/ping` is intentional and public like liveness. Sensitive support snapshots are not public: direct sidecar access requires `X-Operator-Token`, and the sidecar-to-dashboard support-snapshot proxy also needs a configured dashboard API token via `DASHBOARD_API_TOKEN`, `DASHBOARD_API_TOKEN_FILE`, or the configured secret provider. Missing sidecar auth returns `operator_forbidden` with `reason_code=operator_token_required`; missing downstream dashboard auth for the snapshot proxy returns `operator_dashboard_auth_required` without printing token values.

Dashboard health, readiness, barrier, and snapshot proxy calls use bounded timeouts. Dashboard unreachability is reported as degraded proxy metadata (`dashboard_unreachable`, `request_timeout`, or `timed_out=true`) rather than blocking operator readiness indefinitely.

## Maintenance Guidance

- Keep launcher semantics single-owner.
  Avoid hidden duplicate engine starts from both the operator and Python boot paths.
- Reuse existing running operator instances where possible.
- Treat process cleanup and stale launcher state as first-class Linux process-management concerns.
- Keep the operator as a control/proxy layer, not a second business-logic runtime.
  Operator routes should delegate to dashboard/runtime APIs or process controls rather than reimplement trading logic locally.
- Keep the operator out of provider credential ownership.
  Source credentials and source-specific settings should be managed through the Data Sources Control Center, not new `.env` edit paths.
- Keep AI repair flows guarded.
  Patch apply and rollback must remain confirmation-gated and must not bypass live-mode safety rules.
- Keep structured confirmations server-enforced.
  Adding a modal is not enough for dangerous operator routes; add or update the
  sidecar/API confirmation registry and mutation audit payload at the same time.
