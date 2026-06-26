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

## Operator Console Visual Contract

The operator console is styled as the same dark command-center appliance surface
as the dashboard, terminal, data-source, and mobile UIs. `operator_ui.html`
loads [../ui/base.css](../ui/base.css) for shared semantic status tokens and
uses [../ui/state_presenter.js](../ui/state_presenter.js) for loading, empty,
degraded, error, and technical-detail states while keeping page-local CSS for
the sidecar-specific layout.

- Use the shared `--status-ok`, `--status-warn`, `--status-crit`, and
  `--status-info` tokens for pills, dots, issue badges, summaries, and
  degraded/error states.
- Keep high-impact controls visually distinct: Emergency Stop uses a fenced,
  octagonal, full-size incident control with icon and ARIA consequence label;
  factory-reset actions use the critical double-border treatment; restart,
  stop, and update actions remain neutral; read/refresh actions stay
  low-emphasis.
- Keep operator-readable degraded/error copy primary. Raw backend payloads,
  stack details, and route diagnostics belong only under explicit
  `Technical details` disclosures.
- Preserve keyboard focus states on buttons, links, inputs, selects, and
  disclosure summaries.

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

`POST /api/operator/start` and `POST /api/operator/bootstrap` are guarded
state-changing start paths, not read-style diagnostics. Empty payloads are
rejected with `422 confirmation_required` before any start/bootstrap work runs.
Non-live start requires `action_id=operator.start`, `START_OPERATOR`,
`consequence_ack`, actor/source, and reason. Direct bootstrap requires
`action_id=operator.bootstrap`, `BOOTSTRAP_OPERATOR`, `consequence_ack`,
actor/source, and reason; the sidecar proxies it to the dashboard with a
bounded timeout. Start waits for bind/health/telemetry only inside
`OPERATOR_START_REQUEST_TIMEOUT_MS` and returns named top-level failures such as
`preflight_failed`, `bind_timeout`, `backend_unhealthy`, or `start_timeout`
instead of a generic request failure.

The sidecar's direct Start and Restart routes are wrapped by the shared
operator route guard. Filesystem or launcher faults during env/log setup return
JSON `500 internal_server_error` responses and are logged through
`logOperatorCatch` without terminating the operator control plane. Process-level
`unhandledRejection` and `uncaughtException` handlers use the same logging path
and record the sidecar fault in operator state instead of relying on Node's
default unhandled-rejection crash behavior.

## Operator Bridge And Snapshot Auth

The canonical operator control service is the Node sidecar on port `4001`; the dashboard bridge exists for same-origin browser access. Dashboard `/api/operator/ping` is intentional and public like liveness. Sensitive support snapshots are not public: direct sidecar access requires `X-Operator-Token`, and the sidecar-to-dashboard support-snapshot proxy also needs a configured dashboard API token via `DASHBOARD_API_TOKEN`, `DASHBOARD_API_TOKEN_FILE`, or the configured secret provider. Missing sidecar auth returns `operator_forbidden` with `reason_code=operator_token_required`; missing downstream dashboard auth for the snapshot proxy returns `operator_dashboard_auth_required` without printing token values.

The `/operator/` dashboard bridge proxies HTTP API calls only. Browser telemetry uses the sidecar WebSocket at `/ws/operator` directly on the operator origin because the Python dashboard handler is not a WebSocket proxy. The proxied browser asks `/operator/ws_ticket` for a short-lived dashboard-authenticated ticket and sends it as a `Sec-WebSocket-Protocol` marker; the sidecar validates the ticket signature, expiry, and `Origin` before accepting the stream. Raw operator-token subprotocol/query auth remains accepted only for direct sidecar access and backward-compatible tooling.

Dashboard health, readiness, barrier, and snapshot proxy calls use bounded timeouts. Dashboard unreachability is reported as degraded proxy metadata (`dashboard_unreachable`, `request_timeout`, or `timed_out=true`) rather than blocking operator readiness indefinitely.

`/api/operator/proxy/health` remains as a compatibility same-origin bridge to dashboard `/api/health` and is validated against the health payload contract (`ok`, `ts_ms`, and object `db`). System-state routes such as `/api/operator/proxy/system_state` use the separate canonical system-state contract.

The browser operator UI presents health, market, strategy, trading, and service-control failures through designed loading, empty, degraded, and error states. Backend payloads and stack details remain available under explicit `Technical details` disclosures, but raw JSON, stack traces, credential/env names, and internal route diagnostics should not be rendered as primary operator copy.

## Maintenance Guidance

- Keep launcher semantics single-owner.
  Avoid hidden duplicate engine starts from both the operator and Python boot paths.
- Reuse existing running operator instances where possible.
- Treat process cleanup and stale launcher state as first-class Linux process-management concerns.
- Keep the operator as a control/proxy layer, not a second business-logic runtime.
  Operator routes should delegate to dashboard/runtime APIs or process controls rather than reimplement trading logic locally.
- Keep operator API route names snake_case when adding new endpoints.
  Existing camelCase routes may remain as browser compatibility aliases, but they
  should share the same handler as the snake_case route.
- Keep the operator out of provider credential ownership.
  Source credentials and source-specific settings should be managed through the Data Sources Control Center, not new `.env` edit paths.
- Keep AI repair flows guarded.
  Patch apply and rollback must remain confirmation-gated and must not bypass live-mode safety rules.
- Keep structured confirmations server-enforced.
  Adding a modal is not enough for dangerous operator routes; add or update the
  sidecar/API confirmation registry and mutation audit payload at the same time.
