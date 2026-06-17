# UI Layer

The `ui/` directory contains browser assets served by the dashboard server.

## Structure

- [dashboard.html](dashboard.html)
  Main dashboard shell and DOM contract for the modular browser UI.
- [dashboard.js](dashboard.js)
  Main dashboard controller that coordinates API reads, refresh loops, panel rendering, and operator-side state.
  It also owns the broker configuration panel for masked config summaries, connection testing, guarded activate/disable actions, and audit-log rendering.
- [dashboard_theme.css](dashboard_theme.css)
  Main dashboard styling.
- [alerts_ui.js](alerts_ui.js)
  Alert-list rendering and alert interaction helpers used by the main dashboard controller.
- [kill_switch_ui.js](kill_switch_ui.js)
  Kill-switch status presentation helpers for dashboard safety surfaces.
- [policy.js](policy.js)
  Browser-side operator/expert interaction policy helpers.
- [read_only_mode.js](read_only_mode.js)
  Read-only safety layer that mirrors backend execution barriers in the browser.
- [data_sources.html](data_sources.html)
  Standalone Data Sources Control Center and the canonical single-page source-management shell for live ingestion/provider configuration.
- [data_sources.js](data_sources.js)
  Browser controller for guided source inventory, plain-language setup, next-action recommendations, CRUD actions, tests, credential resets, and source-specific logs.
- [data_sources.css](data_sources.css)
  Styling for the standalone data-source control-plane experience.
- [portfolio.js](portfolio.js)
  Portfolio panel rendering and portfolio-specific dashboard helpers.
- [bullet_bars.js](bullet_bars.js)
  Accessible risk-headroom bullet bars for exposure, volatility, and drawdown caps.
- [regime_ribbon.js](regime_ribbon.js)
  Regime-context ribbon renderer for macro, asset, and micro regime labels.
- [risk_charts.js](risk_charts.js)
  Lazy-loaded risk, Monte-Carlo, alpha-decay, and regime-history chart renderers for the Positions & Exposure surface.
- [operator_summary.js](operator_summary.js)
  Operator-facing summary card helpers used by the dashboard.
- [operator_overview.js](operator_overview.js)
  Read-only first-screen Overview tiles composed from production runtime, decision, risk, PnL, and stress endpoints.
- [runtime_status_summary.js](runtime_status_summary.js)
  Compact runtime and pipeline-health summary helpers used in newer diagnostics surfaces.
- [runtime_diagnostics.js](runtime_diagnostics.js)
  Richer runtime diagnostics rendering for operator troubleshooting.
- [telemetry_panel.js](telemetry_panel.js)
  Telemetry visualization helpers for runtime/operator diagnostics.
- [execution_metrics.js](execution_metrics.js)
  Execution-metrics panels for slippage, latency, and cost-focused diagnostics.
- [portfolio_backtest.js](portfolio_backtest.js)
  Latest portfolio backtest summary rendering inside dashboard workflows.
- [chart_a11y.js](chart_a11y.js)
  Shared chart accessibility helpers for programmatic labels, one-line summaries, keyboard focus metadata, and table fallbacks.
- [promotion_safety.js](promotion_safety.js)
  Governance and promotion-safety panel helpers.
- [execution_degradation.js](execution_degradation.js)
  Execution degradation and failure-signal UI helpers.
- [decision_bar.js](decision_bar.js)
  Decision summary and decision-state rendering helpers.
- [decision_stepper.js](decision_stepper.js)
  Decision-modal stage flow renderer derived from the drill-down stage payload.
- [decision_attribution.js](decision_attribution.js)
  Signed feature-contribution attribution bar for the decision-modal why view.
- [social_panels.js](social_panels.js)
  Social/news contextual panels rendered in the dashboard.
- [weather_widgets.js](weather_widgets.js)
  Weather-oriented dashboard widgets and supporting browser helpers.
- `terminal/`
  Separate browser terminal UI with charts, watchlists, account reads, fills, and gated order-entry actions.
- `vendor/`
  Third-party bundled assets.

## Maintenance Guidance

- Treat [data_sources.html](data_sources.html) as the canonical UI for provider/source setup.
  Do not reintroduce a second feed-configuration flow in the operator UI or in another dashboard panel.
- Keep UI reads aligned with documented API handlers.
- Keep the broker configuration panel aligned with `/api/broker/config`, `/api/broker/test_connection`, and `/api/broker/audit`.
- Before shipping UI changes, run `npm ci` with Node.js 20 LTS (`>=20.17.0 <21`) and npm 10.x, then run `npm run check:ui`.
  The check validates tracked local asset references, dashboard JS syntax, and browser-helper tests without starting the dashboard or requiring market-data credentials.
- Prefer adding small focused modules over growing `dashboard.js` without bound.
- Treat [dashboard.html](dashboard.html) as a DOM contract.
  If you rename IDs or move structural regions, update the module that reads them in [dashboard.js](dashboard.js).
- Keep client-side policy helpers advisory only.
  Execution authority still lives in backend gates and operator APIs, not in browser-local state.
- Keep operator AI and repair UI text aligned with the actual guarded behavior.
  Browser wording must not imply autonomous patching or trading authority.
- Update this README when adding a new major panel or runtime control surface.
- On Linux/macOS workstations without system Node/npm, run `bash tools/bootstrap_local_toolchain.sh` from the repository root. It installs Node.js 20.19.4 with npm 10.8.2 inside `.venv`, runs `npm ci`, and exposes the `python`, `python3`, `node`, `npm`, and `npx` command names through user-local shims.
