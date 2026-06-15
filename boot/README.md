# Boot And Operator Layer

The `boot/` directory contains the local launch and operator surface for the system.

## File Roles

- [start_operator.bat](start_operator.bat)
  Main Windows launcher used by local users.
- [start_operator.sh](start_operator.sh)
  Shell launcher for non-Windows environments.
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

## Maintenance Guidance

- Keep launcher semantics single-owner.
  Avoid hidden duplicate engine starts from both the operator and Python boot paths.
- Reuse existing running operator instances where possible.
- Treat process cleanup and stale launcher state as first-class concerns on Windows.
- Keep the operator as a control/proxy layer, not a second business-logic runtime.
  Operator routes should delegate to dashboard/runtime APIs or process controls rather than reimplement trading logic locally.
- Keep the operator out of provider credential ownership.
  Source credentials and source-specific settings should be managed through the Data Sources Control Center, not new `.env` edit paths.
- Keep AI repair flows guarded.
  Patch apply and rollback must remain confirmation-gated and must not bypass live-mode safety rules.
