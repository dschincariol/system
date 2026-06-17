# UI World-Class Deep Dive Implementation Prompts

Use these prompts as focused implementation briefs. Each prompt is intentionally scoped to one UI function so an implementation agent can land a defensible change with tests and without broad redesign churn.

## Common Preamble

You are working in `/home/david/gitsandbox/system/system`. First read `ui/README.md`, `docs/README_UI_REDESIGN_PLAN.md`, and the files named in the prompt. Preserve existing architecture and user changes. Prefer small modules over growing `ui/dashboard.js`. UI policy is advisory; server/API/runtime gates remain authoritative. Use current official/primary sources where standards are cited. Before finishing, run `npm run check:ui` plus focused Python/Node tests for changed behavior. Report any tests you could not run.

## 1. Server-Side Confirmation Contract

Implement a unified server-side confirmation contract for high-impact mutations.

Context: `engine/api/http_transport.py` currently requires typed confirmation for only a small explicit set of routes. `POST /api/operator/emergency_stop`, `/api/terminal/order`, `/api/terminal/flatten`, operator stop/restart/autofix, data-source delete/reset, and job actions need consistent server validation rather than browser-only friction.

Research baseline: NN/g confirmation dialogs, GitLab Pajamas destructive actions, ISA-101 HMI control conventions.

Tasks:
- Inventory all non-GET dashboard/API mutation routes and classify them as low, medium, high, or emergency.
- Add a central route confirmation registry with action id, required token, severity, consequence text, optional hold duration, and threshold policy.
- Validate confirmation server-side before handler execution; default-deny high-impact routes if missing or malformed.
- Make emergency stop fast but still validate the already-sent mobile fields: `confirmation`, `confirmation_hold_ms`, `consequence_ack`, actor, and source.
- Add structured mutation audit fields: action id, actor, confirmed, confirmation method, source surface, request id, client ip, and consequence hash.
- Build or reuse an accessible browser modal to replace native `confirm()` for affected dashboard/data-source actions.
- Update mobile and terminal calls to send the new contract fields.

Acceptance:
- Direct API calls without required confirmation are rejected with 422 and audited.
- Confirmed calls still work for mobile emergency stop, terminal flatten, promotion rollback, size-policy train, dangerous jobs, and data-source deletion.
- Existing dashboard token/rate-limit behavior remains intact.
- Tests cover allowed, missing-confirmation, wrong-token, and audit payload cases.

## 2. Terminal Fat-Finger And Pre-Trade Controls

Bring terminal order entry up to market-access safety standards.

Context: `ui/terminal/terminal.js` gates order controls from the execution barrier, but keyboard shortcuts hard-code quantity `100`. `engine/terminal/api/api_terminal_orders.py` validates symbol, side, positive quantity, and the execution gate, but lacks notional caps, price/size bands, duplicate-order protection, and threshold confirmations.

Research baseline: SEC Rule 15c3-5 market-access controls, professional OMS/EMS fat-finger safeguards.

Tasks:
- Make keyboard BUY/SELL shortcuts use the visible quantity field, not hard-coded 100.
- Add an order preview showing side, symbol, qty, latest price age, estimated notional, and execution gate state.
- Add server-side validation for max qty, max notional, stale/missing price, duplicate recent order, and configurable per-symbol caps.
- Add threshold confirmation for orders or flatten requests above configured notional/position limits.
- Persist rejected/suppressed terminal intents with reason codes so the UI can render them in the orders/blotter surface.
- Add tests for keyboard quantity, cap rejection, stale price rejection, duplicate rejection, and high-notional confirmation.

Acceptance:
- No order intent can be written unless server pre-trade controls pass.
- The UI explains exactly why a request is blocked.
- Rejected requests are visible after refresh with a machine-readable reason and human-readable explanation.

## 3. Broker Configuration And Activation UI

Build a first-class broker configuration control plane.

Context: `/api/broker` is read-only account/positions/fills. Broker selection, credentials, failover, timeouts, and connection tests are currently environment-driven or indirect. The data-source control plane is the closest existing pattern.

Research baseline: connector setup flows such as Fivetran setup tests, broker/API key managers, IBKR Client Portal/Gateway configuration patterns.

Tasks:
- Add backend routes: `GET /api/broker/config`, `POST /api/broker/config`, `POST /api/broker/test_connection`, and `GET /api/broker/audit`.
- Store broker credentials through the existing secrets/credential service pattern, not plain `.env` mutation.
- Model active broker, paper/live mode, failover order, base URL/host/port/client id, timeout, retry policy, and last test result.
- Add a dashboard or standalone broker-config UI with select/edit/test/activate/disable flows.
- Require pre-activation test success before switching live broker configuration.
- Surface active broker, connection latency, account freshness, failover state, and credential age in the dashboard/terminal.
- Add audit rows for config changes, tests, activation, deactivation, and failover changes.

Acceptance:
- Operators can inspect and test broker config without restart.
- Live activation is blocked without a passing test and confirmation.
- Secrets are masked in all read APIs.
- Tests cover config validation, test failures, masked reads, activation gating, and audit logging.

## 4. Alert Severity Fidelity

Fix alert severity semantics end to end.

Context: `ui/alerts.js` normalizes `HIGH` to `WARN`, while `ui/alerts_ui.js` contains a separate `HIGH` severity model. This can understate severity in lists, heatmaps, filters, counts, and recommended actions.

Research baseline: ISA-18.2/EEMUA-191 alarm priority consistency and alarm rationalization.

Tasks:
- Preserve `HIGH` as a distinct severity in `ui/alerts.js`.
- Define one shared severity order: `INFO < WARN < HIGH < CRIT`.
- Update heatmap colors/classes, list badges, filters, decision header counts, and incident drawer titles to use the same normalized severity.
- Avoid color-only encoding: add labels/icons/shape or text differences for severity states.
- Add tests covering normalization, sorting, filtering, heatmap scoring, and drawer display for `HIGH`.

Acceptance:
- A backend `HIGH` alert is shown as `HIGH` everywhere.
- `Severity: WARN+` includes `WARN`, `HIGH`, and `CRIT`.
- No UI path downgrades `HIGH` to `WARN`.

## 5. Global Connection And Freshness State

Standardize dashboard freshness and connection-state signaling.

Context: `ui/panel_state.js` and several screens already show staleness, but coverage is uneven. Operators need one reliable answer to "is this data live, stale, retrying, or disconnected?"

Research baseline: high-performance HMI situation awareness, Endsley situation-awareness model, real-time observability dashboard patterns.

Tasks:
- Create a central connection/freshness model that tracks last success, last failure, latency, endpoint group, and stale threshold.
- Add a global banner with states: connected, degraded/retrying, disconnected, and offline/read-only fallback.
- Feed all critical dashboard reads into that model: health, readiness, execution barrier, broker, risk, PnL, alerts, data providers, terminal snapshot.
- Add uniform card metadata: source endpoint, last updated age, latency, and stale reason.
- Make stale critical safety data disable or visibly guard dangerous UI actions.
- Add tests for freshness state transitions and DOM rendering.

Acceptance:
- A failed polling cycle is visible globally and on affected cards.
- Safety-critical cards show last good timestamp and stale age.
- Critical stale state cannot look visually identical to fresh state.

## 6. Accessible Destructive-Action Modal System

Replace native `confirm()`/`prompt()` flows with accessible, consequence-specific modals.

Context: dashboard/data-source/promotion/job flows use native browser confirms and prompts. These are hard to style, audit, validate, and make accessible.

Research baseline: WCAG 2.2, GitLab Pajamas modal/destructive-action patterns, NN/g confirmation guidance.

Tasks:
- Build a reusable `ui/confirmation_modal.mjs` with focus trap, Escape handling, ARIA roles, keyboard support, consequence preview, typed phrase, optional reason, and disabled submit until valid.
- Use it for rollback, dangerous jobs, data-source delete/reset, expert unlock, emergency controls where applicable, and high-notional terminal confirmations.
- Return structured confirmation payloads to API calls so server validation can audit them.
- Ensure destructive and cancel actions are visually separated and not color-only.
- Add Node/browser-helper tests for validation, focus behavior where practical, and payload generation.

Acceptance:
- No high-impact dashboard flow relies only on `window.confirm()` or `window.prompt()`.
- Modal copy restates the action, target, consequence, and reversibility.
- Keyboard-only operation works.

## 7. Alarm Acknowledgement, Shelving, And Escalation

Upgrade alert handling from simple ack/resolve to incident-grade lifecycle management.

Context: alerts support ack/resolve and local snooze fallback, but lack server-side shelving with expiry/reason and ack-timeout re-escalation.

Research baseline: ISA-18.2/EEMUA-191 alarm lifecycle, PagerDuty acknowledgement timeout and escalation behavior.

Tasks:
- Add server-side alert ack state with owner, timestamp, expiry/ack-timeout, reason, and audit trail.
- Add server-side shelving/snooze with required reason, expiry, and severity constraints.
- Re-trigger or re-escalate acknowledged unresolved alerts after timeout.
- Show alert lifecycle in the incident drawer: triggered, acknowledged, shelved, re-triggered, resolved.
- Make severity-aware notification/rate-limit behavior visible.
- Add tests for ack timeout, shelving expiry, audit trail, and UI normalization.

Acceptance:
- Acknowledgement never hides an unresolved alert forever.
- Shelved alerts require reason and expiry and survive refresh.
- The incident drawer explains current lifecycle state and next escalation.

## 8. Accessibility And Non-Color-Only Status Semantics

Make the operator UI robust for keyboard, screen-reader, and color-vision use.

Context: the UI has ARIA live regions and some keyboard affordances, but many status cues rely on red/green/yellow pills and canvas-only chart rendering.

Research baseline: WCAG 2.2 use of color, non-text contrast, status messages, and name/role/value.

Tasks:
- Audit all status pills, heatmaps, risk bands, PnL markers, and alert badges for color-only encoding.
- Add text, icons, patterns, shape, or explicit labels so state survives grayscale/colorblind viewing.
- Adopt an Okabe-Ito-compatible palette for categorical chart/status colors where possible.
- Add accessible names to interactive controls and ensure tab order is logical across dashboard and terminal.
- Add text/table summaries for canvas-only charts.
- Add tests/static checks for key ARIA labels and regression coverage for alert/status labels.

Acceptance:
- Critical/warn/ok states are distinguishable without color.
- Dynamic status changes are announced where operationally important.
- Canvas charts have a meaningful adjacent textual fallback.

## 9. Charting Interactivity And Visualization Depth

Bring analytical charts to professional trading-dashboard quality.

Context: pro charts use Lightweight Charts with crosshair data, but custom Canvas charts in `ui/charts.js` are static and lack legends, tooltips, axis details, keyboard alternatives, and linked investigation workflows.

Research baseline: Cleveland and McGill graphical perception, Munzner nested visualization model, TradingView Lightweight Charts interaction patterns.

Tasks:
- Add legends, axis labels, value labels, and hover/focus tooltips for custom chart components.
- Add accessible table summaries for equity, drawdown, calibration, divergence, replay, and risk charts.
- Add brushing/linking where useful: selected chart window filters related tables or backtest/divergence panels.
- Add uncertainty bands for backtest/risk projections where backend data exists.
- Remove unused `ui/vendor/chart.umd.min.js` only after confirming no runtime references remain.
- Add tests for chart view models and static asset checks.

Acceptance:
- Operators can answer "what point am I looking at?" without reading raw JSON.
- Chart state is inspectable with keyboard/text fallback.
- No dead charting bundle remains unless justified in docs.

## 10. Execution Blotter Transparency And TCA

Upgrade orders/fills from basic visibility to execution-quality analysis.

Context: terminal and dashboard show orders/fills, but rejected/suppressed intents, partial-fill aggregation, arrival price, VWAP, slippage, and implementation shortfall are not first-class UI concepts.

Research baseline: OMS/EMS blotters, transaction cost analysis, implementation shortfall and VWAP benchmarks.

Tasks:
- Extend orders/fills payloads with arrival/decision price, expected price, fill VWAP, implementation shortfall, slippage bps, rejection reason, suppression reason, and lineage ids.
- Aggregate partial fills by client order id while allowing drilldown to fills.
- Add filters for active, rejected, suppressed, filled, partial, canceled, and stale.
- Add TCA columns and summary cards to terminal/dashboard execution screens.
- Make all rejection/suppression reasons human-readable and machine-readable.
- Add tests for aggregation and UI table sorting/filtering.

Acceptance:
- Operators can see why an order did not trade.
- Filled orders show cost relative to arrival/VWAP/expected price.
- Partial fills display both aggregate and child-fill detail.

## 11. Model Governance Transparency

Make model promotion/rollback decisions auditable and explainable at decision time.

Context: promotion gates and rollback exist, but confirmations use native prompts and the UI should show a durable model-card/gate-state snapshot for each decision.

Research baseline: Model Cards for Model Reporting, MLOps registry patterns, human-AI transparency guidance.

Tasks:
- Capture model-card snapshots at promotion/rollback decisions: intended use, data window, metrics, gates, caveats, owner, timestamp, and comparison to champion.
- Store and render "gate state at decision time" in the promotion audit feed.
- Replace rollback/promotion native prompt flows with the accessible confirmation modal.
- Add conflict/staleness badges when model metrics, replay, temporal eval, or execution degradation are stale.
- Add source/timestamp citations to governance UI rows.
- Add tests for snapshot serialization, modal confirmation, and audit rendering.

Acceptance:
- Every promotion/rollback record can be explained without reconstructing current state.
- Stale gate evidence is visible before action.
- UI and backend confirmation tokens remain consistent.

## 12. Operator AI Trust, Grounding, And Feedback

Harden the advisory AI/copilot surface for high-stakes operations.

Context: the copilot is advisory-only, but answers need stronger trust signals: source attribution, timestamps, uncertainty, and feedback capture.

Research baseline: Microsoft HAX Guidelines for Human-AI Interaction, Google PAIR Guidebook, NIST AI RMF.

Tasks:
- Add source and timestamp metadata to every context item passed into the copilot.
- Render citations/source chips in AI answers for status, alerts, model, broker, and risk claims.
- Show uncertainty/confidence or "insufficient evidence" state for every answer.
- Keep "advisory only" framing persistent and non-dismissible near suggested actions.
- Add thumbs up/down plus optional reason and store feedback for audit/tuning.
- Add timeout/slow-answer UX and safe fallback copy.
- Add tests for context citation generation and advisory-only constraints.

Acceptance:
- AI answers identify which live snapshot they are based on.
- The copilot never implies it can trade, patch, or override gates.
- Operator feedback is persisted with answer id, source context ids, and timestamp.

---

# Charting & Decision-Visualization Deep Dive Prompts

Use these prompts with `docs/UI_CHARTING_BEST_IN_CLASS_RECOMMENDATIONS.md`. Each prompt is scoped to
one recommendation from that report. The implementation agent should copy one prompt at a time, keep
changes narrow, and preserve the advisory UI boundary: charts visualize server/runtime decisions;
they never become a control path for sizing, suppression, order placement, promotion, or kill-switch
state.

## Charting Common Preamble

You are working in `/home/david/gitsandbox/system/system`. First read:

- `CLAUDE.md`
- `ui/README.md`
- `docs/README_UI_REDESIGN_PLAN.md`
- `docs/UI_CHARTING_BEST_IN_CLASS_RECOMMENDATIONS.md`
- The exact files named in the prompt

Preserve existing architecture and user changes. Prefer small modules over growing
`ui/dashboard.js`. Use the already-vendored TradingView Lightweight Charts build unless the prompt
explicitly says otherwise. Do not add ECharts, Plotly, Highcharts, or another heavyweight charting
dependency. Use current official/primary sources for standards claims. When changing server/API
behavior, keep runtime gates authoritative and make UI behavior read-only/advisory.

## Mandatory Completion Gate For Every Charting Prompt

After implementation, audit your own work before final response:

- Show exact files changed.
- Explain why each change is required.
- Explain how each fix is enforced in production code rather than only tests/docs.
- Run targeted tests for the changed behavior.
- Run `npm run check:ui` unless the prompt is server-only and you can justify skipping it.
- Run `git status --short --untracked-files=all`.
- Run `python tools/git_worktree_triage.py`.
- Run relevant validators and record exact commands, exit codes, and key output lines. Use these
  defaults unless a narrower validator is clearly more appropriate:
  - UI/static asset changes: `python tools/check_dashboard_ui_contract.py` and
    `python tools/check_local_asset_refs.py`
  - docs-only changes: `python tools/validate_docs.py`
  - API/server contract changes: focused `pytest` for the changed endpoint plus
    `python tools/validate_repo.py` if the route surface or imports changed
  - package/vendor changes: `python tools/validate_dependency_lock.py` and
    `python tools/check_local_asset_refs.py`
- If any requirement is not fully implemented, say `NO-GO` and explain the remaining work.
- In the final response, include a compact audit block with:
  - `Files changed`
  - `Production enforcement`
  - `Tests and validators` with exact exit codes and key output lines
  - `GO` or `NO-GO`

## Charting 1. Delete Dead Chart.js Bundle

Remove the unused Chart.js bundle and make Lightweight Charts the documented canonical charting
dependency.

Context: `ui/vendor/chart.umd.min.js` is a 205 KB Chart.js bundle. It is referenced by no runtime
HTML/script import and conflicts with the recommendation to standardize on Lightweight Charts v5.

Files to read first:
- `ui/vendor/README_charting.txt`
- `ui/dashboard.html`
- `ui/pro_chart_engine.js`
- `ui/terminal/pro_charting.js`
- `tests/test_ui_asset_refs.py`
- `tools/check_local_asset_refs.py`

Tasks:
- Confirm there are no runtime references to `ui/vendor/chart.umd.min.js`.
- Delete `ui/vendor/chart.umd.min.js`.
- Update `ui/vendor/README_charting.txt` to state that Lightweight Charts is the canonical vendored
  charting runtime and that uPlot is only a future dense-time-series fallback if explicitly added.
- Add or update a static asset test so a future Chart.js bundle cannot be reintroduced silently.
- Do not remove `ui/vendor/lightweight-charts.standalone.production.js`.

Acceptance:
- No HTML or JS references the deleted file.
- Local asset checks pass.
- Vendor documentation points implementers to Lightweight Charts, not Chart.js.

## Charting 2. Chart Accessibility Helper

Make all canvas and Lightweight Charts views expose a meaningful accessible summary and table
fallback.

Context: The dashboard canvases have no `role`, `aria-label`, keyboard focus, or adjacent data-table
fallback. WCAG requires color-independent information and programmatic names/roles/values for
meaningful graphics.

Files to read first:
- `ui/dashboard.html`
- `ui/charts.js`
- `ui/market_stress.js`
- `ui/news_panels.js`
- `ui/replay.mjs`
- `ui/pro_chart_engine.js`
- `ui/terminal/pro_charting.js`
- `tests/test_dashboard_ui_contract.py`
- `tests/test_replay_ui_helpers.mjs`

Tasks:
- Create `ui/chart_a11y.js` with production helpers for chart accessible names, one-line takeaways,
  table fallback rendering, and optional keyboard focus metadata.
- Wire the helper into every existing canvas renderer: replay, market stress sparkline, news
  sentiment, calibration, equity drift, performance divergence, portfolio equity, and portfolio
  drawdown.
- Add markup containers next to each chart for a concise summary and a toggleable fallback table.
- Ensure the fallback table is populated from the same normalized series used for drawing, not a
  separate ad hoc data path.
- Ensure failure/no-data states still produce useful text.
- Add static and focused JS tests proving charts receive labels and fallback tables.

Acceptance:
- Every chart has a programmatic label and a readable fallback summary/table.
- The fallback is production-rendered, not just documented.
- Empty/error chart states remain accessible.

## Charting 3. Honest `renderLineChart` And Time Axis

Fix misleading canvas line rendering while the legacy canvas stack is still in use.

Context: `ui/charts.js` clamps out-of-range values to the plot edge, which can make bad or positive
drawdown points look like legitimate boundary values. It also has no x/time axis, and `drawSpark`
has no callers.

Files to read first:
- `ui/charts.js`
- `ui/portfolio_backtest.js`
- `ui/portfolio.js`
- `ui/model_performance_divergence.mjs`
- `ui/market_stress.js`
- `ui/dashboard.html`
- Relevant UI helper tests under `tests/`

Tasks:
- Replace flat-clamping with honest clipping/gap behavior for values outside `yMin`/`yMax`.
- Add optional `{ xValues, fmtX }` or equivalent support so callers with timestamps can render
  2-3 x-axis ticks.
- Update portfolio backtest, equity drift, and performance divergence callers to pass timestamps
  where available.
- Fix `marketStressSparkline` width so it matches sibling dashboard canvases responsively.
- Remove `drawSpark` only if confirmed unused, or rewire it through `chart_a11y.js` if retained.
- Add focused JS tests for clipping, x-axis tick view models, no-data behavior, and the drawdown
  `yMax: 0` case.

Acceptance:
- Out-of-range data is clipped or shown as a gap, never silently flattened.
- Time context is visible for supported series.
- Existing canvas callers still render without throwing.

## Charting 4. Okabe-Ito Palette And Non-Color-Only Status

Replace red/green-only status semantics with colorblind-safe tokens and redundant encodings.

Context: The current UI uses red/green status pairs across pills, alerts, heatmaps, chart markers,
health factors, and mobile badges. This is fragile for color-vision deficiency and violates the
charting report's accessibility direction.

Files to read first:
- `ui/dashboard.html`
- `ui/dashboard_theme.css`
- `ui/styles.tech.css`
- `ui/base.css`
- `ui/alerts.js`
- `ui/alerts_ui.js`
- `ui/decision_bar.js`
- `ui/health_score.js`
- `ui/mobile/mobile.css`
- `ui/mobile/mobile.js`
- `ui/terminal/terminal_theme.css`

Tasks:
- Add shared color/status tokens for neutral, info, ok, warn, high, crit, blocked, and unavailable
  using an Okabe-Ito-compatible palette plus neutral grayscale baseline.
- Update status pills, alert heatmap, chart markers, health score factors, decision bar, mobile
  badges, and terminal chart marker colors to use the tokens.
- Add non-color encodings: text labels, icons/glyphs, outlines, shapes, or patterns where status is
  currently color-only.
- Add `prefers-reduced-motion`, `prefers-contrast`, and `forced-colors` handling for chart/status UI.
- Ensure text contrast and non-text contrast meet WCAG thresholds in normal and high-contrast modes.
- Add tests or static checks for status labels/classes and heatmap non-color semantics.

Acceptance:
- Critical/warn/ok/blocked states remain distinguishable in grayscale.
- Alert heatmap cells include labels or accessible names, not only swatches.
- The same status vocabulary is used across desktop, terminal, and mobile.

## Charting 5. Mobile Plain-English Narrative

Bring the desktop plain-English runtime narrative to mobile.

Context: `ui/runtime_status_summary.js` and `ui/operator_summary.js` already translate raw runtime
state into headline, meaning, and next steps. Mobile independently renders terse labels from the
same primitives.

Files to read first:
- `ui/mobile/mobile.js`
- `ui/mobile/index.html`
- `ui/mobile/mobile.css`
- `ui/mobile/mobile_helpers.mjs`
- `ui/runtime_status_summary.js`
- `tests/test_mobile_ops_helpers.mjs`
- `tests/test_mobile_ops_surface.py`

Tasks:
- Import and use `summarizeRuntimeStatus()` in mobile.
- Render headline, meaning, and next steps in an existing or new `role="status"` region.
- Keep emergency controls and kill-switch confirmation semantics unchanged.
- Add a compact text PnL trend if `/api/pnl` provides enough data; otherwise render a useful
  unavailable state.
- Add mobile tests that prove the summary is derived from shared runtime-status logic and updates
  when health/readiness/barrier state changes.

Acceptance:
- Mobile and desktop produce consistent plain-English operator guidance for the same status inputs.
- Mobile remains functional when one status endpoint fails.
- The narrative is production-rendered, not just available in helpers.

## Charting 6. Decision-Flow Stepper

Render automated decisions as a stage-by-stage visual flow.

Context: `ui/decision_drilldown.mjs` already returns ordered stage rows. The modal currently renders
decision detail mostly as text and tables, so operators cannot see where a decision stopped at a
glance.

Files to read first:
- `ui/decision_drilldown.mjs`
- `ui/dashboard.js`
- `ui/dashboard.html`
- `ui/why_modal.js`
- `tests/test_decision_drilldown.py`
- Any JS tests covering decision modal helpers

Tasks:
- Add a small DOM module, such as `ui/decision_stepper.js`, that renders stages as:
  Signal -> Confidence/Calibration -> Risk/Suppression -> Sizing -> Order -> Fill.
- Use `buildDecisionStageRows()` as the production data source.
- Emphasize the first blocking/suppressed/unavailable stage with icon, label, tone, reason, and
  timestamp.
- Keep stage detail accessible by keyboard and screen reader.
- Add the stepper to the decision modal first; do not overload the live price chart in this prompt.
- Add focused tests for pass, partial, suppressed, blocked, loading, unavailable, and empty stage
  payloads.

Acceptance:
- A non-technical user can tell where the latest decision stopped without reading raw JSON.
- Stepper state is derived from server decision detail payloads.
- The modal still renders useful detail when stage payloads are missing.

## Charting 7. Risk Bullet Bars, Regime Ribbon, And Kill-Switch Lights

Make risk headroom, regime context, and kill-switch state visually scanable.

Context: `/api/risk/portfolio` returns risk history and current caps, but the UI uses only the
latest values in text/stat grids. Kill switches render as appended text in a `<pre>` with a mouse-Y
click hack.

Files to read first:
- `ui/dashboard.js`
- `ui/dashboard.html`
- `ui/kill_switch_ui.js`
- `ui/portfolio.js`
- `engine/api/api_system.py`
- `dashboard_server.py`
- `engine/runtime/regime_stack.py`
- `engine/strategy/regime_stack.py`
- Relevant risk and kill-switch tests under `tests/`

Tasks:
- Create `ui/bullet_bars.js` for accessible bullet bars with value, cap marker, qualitative bands,
  label, status word, and fallback text.
- Render gross exposure, net exposure, vol proxy/target, and drawdown against their caps from
  `/api/risk/portfolio` or canonical `/api/ui/metrics` where available.
- Add a regime ribbon using an existing regime endpoint if sufficient; otherwise add a small read-only
  endpoint that returns macro/asset/micro regime labels and timestamps.
- Replace the kill-switch mouse-Y hack with real per-row status-light buttons/links that open the
  existing explanation modal or recovery hint; do not add mutation authority.
- Add tests for bullet-bar view models, cap thresholds, blocked risk state, regime fallback, and
  kill-switch row activation.

Acceptance:
- Risk headroom is visible as length against a limit, not only a number.
- Kill-switch hints are reachable through real controls with accessible names.
- Regime ribbon and risk bars fail gracefully when data is unavailable.

## Charting 8. Price-Chart Decision And Risk Overlays

Show automated decisions directly on the live price chart.

Context: The pro chart currently shows price, volume, VWAP/EMA, fills/intents, and equity. It does
not draw entry/average-cost/stop/take-profit levels, suppressed/blocked markers, or kill/suppression
windows.

Files to read first:
- `ui/pro_chart_engine.js`
- `ui/terminal/pro_charting.js`
- `engine/terminal/api/api_terminal.py`
- `engine/execution/trade_suppression_engine.py`
- `engine/execution/execution_policy_engine.py`
- `engine/execution/kill_switch.py`
- `engine/execution/trade_attribution_ledger.py`
- Relevant terminal/API tests under `tests/`

Tasks:
- Extend `/api/terminal/markers` or add `/api/terminal/decision_overlays` to expose fills, intents,
  suppressed decisions, blocked decisions, risk caps, and kill/suppression windows with stable
  reason codes and timestamps.
- Use Lightweight Charts `createPriceLine` for entry, average cost, stop, take-profit, max-risk, or
  cap levels where data exists.
- Add distinct marker semantics for filled, intended, suppressed, blocked, and risk-capped events
  using color + shape + text, not color alone.
- Shade or mark drawdown throttle, circuit-breaker, kill-switch, and suppression windows.
- Ensure line/area modes still have a marker anchor series or alternate rendering so markers do not
  disappear.
- Add a legend and accessible summary for overlays.
- Fix O(n)-per-tick VWAP/EMA recompute if overlay additions increase streaming work materially.
- Add focused API tests for overlay payloads and JS tests for overlay normalization/rendering.

Acceptance:
- The chart can explain both traded and not-traded automated decisions.
- Overlay rendering is driven by production API data with reason codes.
- Live streaming remains stable under repeated candle updates.

## Charting 9. Under-Surfaced Timeseries

Chart risk history, Monte-Carlo risk, alpha decay, and regime state that already exist in the system.

Context: `/api/risk/portfolio` returns up to 200 history rows; `/api/risk/monte_carlo` and
`/api/alpha_decay` exist; regime state is persisted in multiple runtime/strategy tables. These are
not consumed by chart UI.

Files to read first:
- `engine/api/api_system.py`
- `dashboard_server.py`
- `ui/dashboard.js`
- `ui/portfolio.js`
- `ui/market_stress.js`
- `engine/runtime/risk_state.py`
- `engine/runtime/alpha_decay_monitor.py`
- `engine/runtime/regime_stack.py`
- `engine/strategy/regime_stack.py`
- Relevant API and risk tests under `tests/`

Tasks:
- Add a risk-over-time chart from `/api/risk/portfolio.history` with gross, net, drawdown, blocked
  state, and timestamped fallback table.
- Add a Monte-Carlo risk view that surfaces VaR/CVaR and available distribution/fan data; if the
  backend only has summary values, render a bullet/bar view and document the missing fan input in
  code comments and `NO-GO` if fan chart was required.
- Add an alpha-decay rolling-Sharpe/half-life chart from `/api/alpha_decay`.
- Add or reuse a read-only regime-stack endpoint for a labeled ribbon over time.
- Keep all new charts lazy-loaded and modular.
- Add API shape tests and JS chart view-model tests.

Acceptance:
- The UI uses more than the first risk history row.
- Monte-Carlo and alpha-decay endpoints have production chart consumers.
- Missing backend detail produces honest unavailable/summary states, not fake precision.

## Charting 10. Portfolio Equity, Underwater Drawdown, Markers, Benchmark, And Promotion Bars

Upgrade portfolio/backtest and promotion visuals from static lines/tables to decision-quality views.

Context: `dashboard.html` already has hidden `portfolioEquityPro` and `portfolioDdPro` containers.
`ui/portfolio_backtest.js` renders equity and drawdown with legacy canvas, while promotion gates are
mostly numeric tables.

Files to read first:
- `ui/portfolio_backtest.js`
- `ui/portfolio.js`
- `ui/promotion_gate.mjs`
- `ui/dashboard.html`
- `ui/pro_chart_engine.js`
- `engine/api/api_governance.py`
- `engine/api/api_dashboard_reads.py`
- `tests/test_portfolio_backtest_contract.py`
- `tests/test_promotion_gate_data.py`

Tasks:
- Render portfolio equity and underwater drawdown with Lightweight Charts using the existing
  `portfolioEquityPro` and `portfolioDdPro` containers.
- Keep the old canvases behind a feature flag or fallback until the pro renderer is stable.
- Add a 6% drawdown throttle reference line, trade/decision markers when available, and hover values.
- Add an optional benchmark overlay if endpoint data exists; if not, expose a clear unavailable
  state and do not fabricate SPY data.
- Render Sharpe, Sortino, Calmar, turnover, and sample count as compact annotations tied to the run.
- Render promotion gate as champion-vs-challenger comparison bars with gate thresholds,
  significance/decision indicators, and stale-data badges.
- Add tests for pro portfolio chart view models, fallback behavior, metric annotations, and promotion
  comparison bar normalization.

Acceptance:
- Portfolio performance cannot be mistaken for outperformance without context.
- Drawdown proximity to throttle is visually explicit.
- Promotion decision state is visible as comparison against gates, not just table cells.

## Charting 11. Overview Screen

Build the at-a-glance non-technical operator home described in the charting report.

Context: The report's flagship recommendation is a simplified Overview screen answering: "Is the
system OK?", "What is it doing right now?", and "Should I trust it / how close to the edge?"

Files to read first:
- `ui/dashboard.html`
- `ui/dashboard.js`
- `ui/view_router.js`
- `ui/health_score.js`
- `ui/runtime_status_summary.js`
- `ui/operator_summary.js`
- `ui/decision_drilldown.mjs`
- `ui/bullet_bars.js` if already implemented
- `ui/market_stress.js`
- `ui/ui_metrics.js`
- Relevant dashboard UI tests

Tasks:
- Add a first-screen Overview layout that is not a marketing page and does not bury the operational
  experience.
- Build three tiles:
  - System status: SAFE / CAUTION / STOP with icon, word, score, freshness, headline, meaning, and
    next step.
  - Current decision: latest decision-flow stepper plus a compact recent-decision mini-timeline.
  - Trust/headroom: risk bullet bars, PnL trend text/sparkline, and market stress top driver.
- Reuse `health_score.js`, `runtime_status_summary.js`, `decision_drilldown.mjs`, and
  `bullet_bars.js`; do not duplicate logic in `dashboard.js`.
- Keep all dangerous controls out of the Overview or visibly guarded by existing policy/read-only
  protections.
- Add accessible labels and fallback text for every visual tile.
- Add focused DOM tests for status mapping, stale data, blocked execution, no-decision state, and
  risk unavailable state.

Acceptance:
- A non-technical operator can answer the three Overview questions within one screen.
- Overview state is derived from production runtime endpoints.
- Overview does not add any mutation/control authority.

## Charting 12. Shared Pro Chart Core

Consolidate duplicated Lightweight Charts code without changing behavior.

Context: `ui/pro_chart_engine.js` and `ui/terminal/pro_charting.js` duplicate chart loading, series
compatibility, teardown, indicators, markers, health ticker, and SSE logic. Dashboard currently
imports preference helpers from terminal code.

Files to read first:
- `ui/pro_chart_engine.js`
- `ui/terminal/pro_charting.js`
- `ui/dashboard.js`
- `ui/terminal/terminal.js`
- `ui/vendor/README_charting.txt`
- Existing pro chart tests or UI static tests

Tasks:
- Extract shared mechanics into `ui/pro_chart_core.js`: library loading, series compatibility,
  chart construction, resize cleanup, marker layer compatibility, indicator helpers, price-line
  helpers, health ticker utilities, and streaming lifecycle primitives where practical.
- Extract persisted preferences into `ui/pro_chart_prefs.js` so dashboard no longer imports prefs
  from terminal charting.
- Keep dashboard-specific and terminal-specific orchestration wrappers thin.
- Preserve public exports used by existing callers or update all callers in one change.
- Add tests for shared helper behavior and static import integrity.
- Do not change visual behavior except for bug fixes explicitly covered by tests.

Acceptance:
- The duplicated pro-chart logic is materially reduced.
- Dashboard and terminal charts still boot and apply overlays.
- Preference state remains backward compatible with existing localStorage keys.

## Charting 13. Uncertainty, Calibration, And Attribution

Make trust and model reasoning visually honest.

Context: `drawCalibration()` is a minimal canvas line. The UI lacks Expected Calibration Error,
bin counts, uncertainty bands, and per-decision feature attribution, even though tree-model SHAP is
feasible for several model families.

Files to read first:
- `ui/charts.js`
- `ui/why_modal.js`
- `ui/dashboard.js`
- `ui/decision_drilldown.mjs`
- `engine/api/drift_explainer.py`
- `engine/strategy/shap_explainer.py`
- `engine/strategy/decision_log.py`
- `engine/execution/trade_attribution_ledger.py`
- Relevant SHAP, decision log, and UI tests

Tasks:
- Upgrade calibration rendering with bin counts, sample counts, ECE, confidence/accuracy labels, and
  a plain-English calibrated/overconfident/underconfident verdict.
- Add uncertainty bands or fan-chart rendering only where backend payloads contain real quantiles or
  distribution samples; otherwise render an honest "uncertainty unavailable" state.
- Add a signed horizontal attribution bar for per-decision "why" using backend-provided feature
  contributions.
- If feature contributions are missing, add a read-only endpoint or payload field that sources them
  from existing SHAP/explainer infrastructure; do not compute expensive SHAP in the browser.
- Ensure the attribution bar has labels, sign, magnitude, fallback table, and non-color encoding.
- Add focused tests for ECE calculation, calibration verdicts, attribution normalization, and
  missing-contribution fallback.

Acceptance:
- Calibration trust is measurable, not just a diagonal line.
- Attribution explains one decision in terms a non-technical operator can read.
- The UI never invents uncertainty or attribution values when backend data is absent.
