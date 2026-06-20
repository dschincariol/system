# UI Surfacing Deep Dive Implementation Prompts

Use these prompts one at a time. Each prompt is scoped to one repo-wide audit recommendation where backend capability exists but the operator surface, API contract, documentation, or production enforcement is incomplete.

## Common Preamble

You are working in `/home/david/gitsandbox/system/system`. The repo may be dirty; do not revert unrelated user changes. First read `README.md`, `docs/DOCUMENTATION_INDEX.md`, `ui/README.md`, `engine/api/README.md`, and the subsystem README for the code you touch. Preserve existing architecture. Prefer adding small focused modules, typed serializers, and route tests over growing large files. UI policy is advisory; server, API, and runtime gates must remain authoritative. Treat these prompts as design and implementation briefs: inspect the current behavior first, design the production contract, implement it, update tests and docs, and report gaps plainly.

## Prompt 1 - Job Catalog and Backend Job Metadata

Deep dive and implement a first-class job catalog. Current evidence: the backend job registry contains many operational jobs, but the dashboard job console exposes only a small hardcoded subset. `/api/jobs` returns job state and limited registry fields, while the command palette applies client-side unsafe-job regex checks. Operators need a discoverable catalog with backend-owned metadata, safety classification, and clear prerequisites.

Requirements:
- Inventory all registered jobs from `engine/runtime/job_registry.py` and identify which are currently surfaced in `ui/dashboard.html`, `ui/dashboard.js`, and `ui/command_palette.mjs`.
- Extend the backend job serialization contract, either in `/api/jobs` or a new `/api/jobs/catalog`, with registry-owned metadata: id, group, script/module, mode, schedule/cadence if available, stage, owner subsystem, dependencies, required secrets or providers, execution sensitivity, resource class, and operator-facing purpose.
- Replace UI-only unsafe-job classification with a backend safety field that distinguishes read-only, data-refresh, training/research, execution-sensitive, destructive/admin, and unavailable jobs.
- Add server-side confirmation policy for execution-sensitive or destructive job actions if it is missing or inconsistent.
- Build a dashboard Job Catalog surface with search, filters, grouping by workflow, latest run state, last output/log link, prerequisites, and clear disabled states.
- Keep existing quick job buttons working, but source labels and safety state from the backend contract rather than hardcoded UI metadata.
- Add tests for the serializer, safety classification, server-side guarded actions, UI rendering, and command-palette behavior.

Suggested files to inspect:
- `engine/runtime/job_registry.py`
- `engine/api/api_jobs.py`
- `engine/api/http_transport.py`
- `ui/dashboard.html`
- `ui/dashboard.js`
- `ui/command_palette.mjs`
- `tests/test_jobs_manager_registry_enforcement.py`
- `tests/test_dashboard_contract.py`
- `ui/tests/`

Acceptance:
- Every registered job is discoverable through a documented read API and at least one operator UI path.
- Dangerous job starts are guarded by backend policy, not browser regex alone.
- Operators can understand what a job does, why it is disabled, and where to inspect its last result.
- Existing job actions remain backward compatible for current dashboard callers.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 2 - Governance Evidence Center

Deep dive and implement an operator-facing governance evidence center. Current evidence: OPE gating, experiment ledger, net-after-cost labels, learned alpha decay, alpha shrinkage, production monitoring, and shadow capital allocation exist in backend modules, but their evidence is mostly indirect in the dashboard. Some shadow-capital read handlers exist but are not clearly registered as first-class route contracts. Operators need a single place to see whether promotion, generated candidates, and model-risk controls are backed by current evidence.

Requirements:
- Inventory governance and promotion evidence producers, including OPE gate results, experiment ledger rows, net-after-cost label coverage, learned-alpha freshness, alpha shrinkage metadata, production monitoring drift/calibration, shadow-vs-live metrics, and shadow capital scores.
- Add or consolidate documented read APIs for governance evidence, promotion blockers, generated-candidate provenance, and shadow capital scores. Prefer one summary endpoint plus drilldown endpoints where payloads would otherwise become too large.
- Register any existing unmounted shadow-capital handlers or replace them with a clearly named route contract.
- Build a dashboard governance evidence surface in the most appropriate Analyze, Explain, or model lifecycle area. It should show pass/block/unknown state, freshness, sample counts, last update, source artifact, and remediation for missing evidence.
- Make generated-candidate and challenger-promotion blockers explainable without requiring operators to inspect logs or raw files.
- Keep any promotion or allocation action gated by existing production controls. The new UI must explain authority, not create a bypass.
- Add tests for route registration, evidence aggregation, stale or missing evidence states, shadow-capital payload masking if needed, and UI rendering.

Suggested files to inspect:
- `engine/api/api_governance.py`
- `engine/api/api_dashboard_reads.py`
- `engine/api/api_ops.py`
- `dashboard_server.py`
- `engine/strategy/ope_gate.py`
- `engine/strategy/experiment_ledger.py`
- `engine/strategy/net_after_cost_labels.py`
- `engine/strategy/learned_alpha_decay.py`
- `engine/strategy/alpha_shrinkage.py`
- `engine/strategy/production_monitoring.py`
- `engine/runtime/shadow_capital_allocator.py`
- `ui/dashboard.html`
- `ui/dashboard.js`
- `ui/promotion_gate.mjs`
- `docs/DATA_CONTRACTS.md`

Acceptance:
- Operators can see the current governance evidence required for promotion and generated-candidate trust decisions.
- Missing, stale, or insufficient evidence is rendered as a blocker with exact source and remediation.
- Shadow-capital state is available through a documented, registered API route and visible in the UI.
- No promotion or allocation action becomes less restrictive because of the new surface.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 3 - Execution TCA, LOB, and Learned Slicing Diagnostics

Deep dive and implement richer execution diagnostics. Current evidence: execution APIs already expose stats, rolling metrics, by-symbol metrics, and advisories, while LOB simulation and contextual bandit slicing modules exist deeper in the execution subsystem. The dashboard and terminal do not fully surface these capabilities, leaving operators with less visibility into slippage, fill quality, L2 readiness, and learned slicing decisions.

Requirements:
- Inventory available execution analytics routes and payloads, including `/api/execution/stats`, rolling metrics, by-symbol metrics, advisories, broker orders/fills, rejected or suppressed intents, LOB simulation, deepLOB readiness, and contextual bandit slicing outputs.
- Add backend aggregation or serializers where the current data is too raw or inconsistent for UI use. Include clear stale, unavailable, and shadow-only states.
- Extend the dashboard execution screen with by-symbol TCA, rolling slippage, latency, fill quality, partial-fill aggregation, rejected/suppressed order explanations, and implementation-shortfall or VWAP fields where available.
- Add LOB and deepLOB diagnostics: L2 feed freshness, snapshot depth, replay/simulation readiness, calibration status, and warnings for missing or stale market-depth data.
- Add learned slicing diagnostics that show the current policy state, exploration/shadow status, selected action distribution, baseline comparison, and recent suppression reasons without granting new live authority.
- Add terminal or execution blotter drilldowns so an operator can trace an order from intent to fills, rejection, or suppression reason.
- Add tests for execution serializers, stale state, UI table rendering, sorting/filtering, and any new drilldown view model.

Suggested files to inspect:
- `engine/api/api_ops.py`
- `engine/api/api_ops_handlers.py`
- `engine/execution/execution_analytics_engine.py`
- `engine/execution/lob_simulation.py`
- `engine/execution/contextual_bandit_slicer.py`
- `engine/execution/options_readiness.py`
- `ui/dashboard.html`
- `ui/dashboard.js`
- `ui/execution_metrics.js`
- `ui/terminal/terminal.js`
- `tests/test_api_routes_static.py`
- `ui/tests/`

Acceptance:
- Operators can see execution quality by symbol and over time without reading raw API JSON.
- LOB/deepLOB and learned slicing capabilities have explicit readiness, stale, shadow, or unavailable states.
- Rejected, suppressed, and partial-fill outcomes are visible with machine-readable and human-readable reasons.
- Live execution authority remains governed by existing broker, risk, and execution-barrier controls.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 4 - Consolidated Live Readiness Evidence

Deep dive and implement a consolidated live-readiness evidence surface. Current evidence: live readiness depends on options readiness, AI serving safety, broker health, kill-switch state, backup evidence, OPE and experiment-ledger controls, production monitoring, provider telemetry, and liveness/readiness checks. These signals are scattered across APIs and screens, making it hard for an operator to determine why a high-risk action is blocked.

Requirements:
- Inventory all live-readiness and preflight evidence producers, including options readiness, live AI safety, execution barrier, kill switch, broker config/test state, backup evidence, OPE evidence, experiment ledger, production monitoring freshness, provider telemetry, data-source health, and `/healthz` or `/readyz`.
- Add a normalized readiness evidence API, or extend an existing trading-readiness endpoint, with items shaped as id, title, status, severity, blocking, source subsystem, source route or config key, freshness, detail, and remediation.
- Ensure the API fail-closes for missing critical evidence in live or paper modes where the existing runtime requires it.
- Build a dashboard readiness card or panel that groups blockers by category, links to the owning screen, shows last update age, and distinguishes blocked, warning, unavailable, and passing states without color-only encoding.
- Wire high-risk UI actions to read the authoritative readiness evidence where appropriate, so stale or blocking critical evidence disables or requires confirmation before action.
- Add tests for evidence aggregation, missing/stale critical evidence, production-mode severity, UI rendering, and guarded action behavior.

Suggested files to inspect:
- `engine/api/api_system.py`
- `engine/api/api_runtime.py`
- `engine/runtime/live_ai_safety.py`
- `engine/execution/options_readiness.py`
- `engine/runtime/live_trading_preflight.py`
- `engine/runtime/prod_preflight.py`
- `engine/runtime/backup_evidence.py`
- `engine/execution/kill_switch.py`
- `ui/dashboard.html`
- `ui/dashboard.js`
- `ui/operator_overview.js`
- `docs/PRODUCTION_CHECKLIST.md`
- `docs/README_OPERATOR_GUIDE.md`

Acceptance:
- Operators have one authoritative readiness view explaining whether live or paper operation is blocked and why.
- The readiness API includes actionable remediation and source ownership for every blocker.
- Stale or missing critical readiness evidence cannot appear equivalent to a passing state.
- At least one high-risk operator action is demonstrably guarded by the new readiness evidence path where the existing UI lacked that guard.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 5 - Structured Document and Graph Feature Visibility

Deep dive and implement operator visibility for structured document events and graph-relational features. Current evidence: structured document extraction and graph-relational snapshot modules exist as data and strategy capabilities, but the dashboard does not clearly show extraction counts, freshness, lineage, confidence, point-in-time status, or graph feature availability. Operators need to know whether these features are contributing, stale, shadow-only, or unavailable.

Requirements:
- Inventory structured document event outputs, graph-relational snapshots, point-in-time feature metadata, and any model explanation paths that consume these features.
- Add documented read APIs for structured document event counts, latest extraction time, low-confidence counts, source document lineage, symbol coverage, event type coverage, graph snapshot freshness, graph feature availability, and shadow-only/PIT status.
- Extend Data Health with panels for structured documents and graph features. Include freshness, confidence distribution, extraction failures, source lineage, symbol coverage, and explicit unavailable states.
- Extend decision drilldowns or why-modals so any model contribution from structured document or graph feature groups can show lineage, feature availability, confidence, source artifact, and point-in-time validity.
- Label shadow-only features clearly and ensure the UI does not imply they affect live trading if they do not.
- Add tests for route payloads, stale/PIT warnings, shadow-only labels, decision-drilldown rendering, and documentation examples.

Suggested files to inspect:
- `engine/data/structured_document_events.py`
- `engine/strategy/graph_relational.py`
- `engine/api/api_dashboard_reads.py`
- `dashboard_server.py`
- `ui/dashboard.html`
- `ui/dashboard.js`
- `ui/decision_drilldown.mjs`
- `ui/why_modal.js`
- `docs/DATA_CONTRACTS.md`
- `docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md`

Acceptance:
- Operators can inspect structured-document and graph-feature health without reading backend files or logs.
- Feature lineage and point-in-time validity are visible where those features influence explanations.
- Shadow-only capabilities are clearly marked and cannot be mistaken for live authority.
- Missing or stale extraction/graph data produces explicit warnings rather than silent absence.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 6 - API Inventory and OpenAPI Drift Gate

Deep dive and implement an API inventory and OpenAPI drift gate. Current evidence: `docs/openapi/openapi.yaml` states it is incomplete, while the assembled dashboard route table and operator sidecar expose a broader `/api/*` surface. The repo needs a generated route inventory and a validation gate so new routes are documented, classified, or explicitly allowlisted.

Requirements:
- Build a route inventory tool that imports or parses the assembled Python dashboard route specs from `dashboard_server.py` and the `engine/api/*` route modules. Include method, path, handler owner, mutation/read classification, auth requirement if knowable, confirmation requirement if knowable, and source file.
- Include Node operator sidecar routes from `boot/operator_server.js` or add a maintained sidecar route manifest if static parsing is too brittle.
- Generate a deterministic JSON and/or Markdown inventory artifact for review. Avoid including secrets or environment-specific values.
- Compare the generated inventory against `docs/openapi/openapi.yaml` and an explicit allowlist for aliases, deprecated paths, health-only endpoints, or intentionally undocumented internal routes.
- Add a validator that fails when a mutating route is missing OpenAPI coverage, classification, or an explicit allowlist entry.
- Expand OpenAPI coverage for high-value missing routes discovered during the audit, prioritizing jobs, governance evidence, readiness evidence, execution analytics, broker/operator mutations, and terminal mutations.
- Document the route ownership and drift workflow in `docs/openapi/README.md` and wire the validator into the relevant repo validation command.

Suggested files to inspect:
- `dashboard_server.py`
- `engine/api/api_system.py`
- `engine/api/api_jobs.py`
- `engine/api/api_ops.py`
- `engine/api/api_market.py`
- `engine/api/http_transport.py`
- `boot/operator_server.js`
- `docs/openapi/openapi.yaml`
- `docs/openapi/README.md`
- `tools/validate_docs.py`
- `tools/validate_repo.py`
- `tests/test_api_routes_static.py`

Acceptance:
- The repo has a reproducible route inventory for Python dashboard APIs and the operator sidecar.
- New or changed mutating routes cannot silently drift outside OpenAPI or an explicit allowlist.
- Existing aliases and deprecated routes are marked intentionally rather than appearing as accidental gaps.
- Documentation tells future maintainers how to regenerate, review, and validate the inventory.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 7 - Operator Console Structured Confirmation and Audit Alignment

Deep dive and replace native operator-console prompt/confirm flows with structured, auditable confirmation. Current evidence: `boot/operator_ui.html` still uses browser `prompt()` and `confirm()` for live start, guided bootstrap, emergency stop, factory reset, and other high-impact flows. These confirmations are hard to style, test, audit, and align with the dashboard's server-side confirmation contract.

Requirements:
- Inventory every `window.confirm()`, `window.prompt()`, implicit prompt, and high-impact operator mutation in `boot/operator_ui.html`, `boot/operator_server.js`, and related dashboard confirmation code.
- Design and implement an accessible confirmation modal or shared helper with focus management, ARIA roles, Escape/cancel behavior, typed phrase or hold-to-confirm support, required reason where appropriate, consequence text, and disabled submit until valid.
- Replace native prompt/confirm flows for live start, guided bootstrap, emergency stop, factory reset, repair/admin actions, secrets changes, and AI apply-patch actions where applicable.
- Send structured confirmation payloads to the backend or sidecar: action id, typed confirmation or token, hold duration, consequence acknowledgement, actor, source surface, reason, request id, and target.
- Ensure server or sidecar validation is authoritative for high-impact mutations. The modal must not be the only enforcement point.
- Align audit payloads with the existing dashboard mutation audit and confirmation registry where possible.
- Add tests or static checks proving native confirms/prompts are gone from high-impact flows, structured payloads are sent, server-side validation rejects missing confirmation, and keyboard-only modal operation works where practical.

Suggested files to inspect:
- `boot/operator_ui.html`
- `boot/operator_server.js`
- `engine/api/http_transport.py`
- `ui/confirmation_modal.mjs`
- `ui/dashboard.html`
- `ui/dashboard.js`
- `tests/test_operator_server_admin_contract_static.py`
- `tests/test_operator_console_bridge.py`
- `ui/tests/`
- `docs/README_OPERATOR_GUIDE.md`
- `boot/README.md`

Acceptance:
- High-impact operator console actions no longer rely on native browser prompt or confirm flows.
- Structured confirmation payloads are validated in production server or sidecar code before dangerous mutations run.
- Audit records can identify action, actor, source, request id, target, confirmation method, and reason.
- The replacement modal is accessible and does not regress existing legitimate operator workflows.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
