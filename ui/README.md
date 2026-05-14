# UI Layer

The `ui/` directory contains browser assets served by the dashboard server.

## Structure

- [dashboard.html](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\dashboard.html)
  Main dashboard shell and DOM contract for the modular browser UI.
- [dashboard.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\dashboard.js)
  Main dashboard controller that coordinates API reads, refresh loops, panel rendering, and operator-side state.
- [dashboard_theme.css](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\dashboard_theme.css)
  Main dashboard styling.
- [alerts_ui.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\alerts_ui.js)
  Alert-list rendering and alert interaction helpers used by the main dashboard controller.
- [kill_switch_ui.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\kill_switch_ui.js)
  Kill-switch status presentation helpers for dashboard safety surfaces.
- [policy.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\policy.js)
  Browser-side operator/expert interaction policy helpers.
- [read_only_mode.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\read_only_mode.js)
  Read-only safety layer that mirrors backend execution barriers in the browser.
- [data_sources.html](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\data_sources.html)
  Standalone Data Sources Control Center and the canonical single-page source-management shell for live ingestion/provider configuration.
- [data_sources.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\data_sources.js)
  Browser controller for guided source inventory, plain-language setup, next-action recommendations, CRUD actions, tests, credential resets, and source-specific logs.
- [data_sources.css](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\data_sources.css)
  Styling for the standalone data-source control-plane experience.
- [portfolio.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\portfolio.js)
  Portfolio panel rendering and portfolio-specific dashboard helpers.
- [operator_summary.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\operator_summary.js)
  Operator-facing summary card helpers used by the dashboard.
- [runtime_status_summary.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\runtime_status_summary.js)
  Compact runtime and pipeline-health summary helpers used in newer diagnostics surfaces.
- [runtime_diagnostics.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\runtime_diagnostics.js)
  Richer runtime diagnostics rendering for operator troubleshooting.
- [telemetry_panel.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\telemetry_panel.js)
  Telemetry visualization helpers for runtime/operator diagnostics.
- [execution_metrics.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\execution_metrics.js)
  Execution-metrics panels for slippage, latency, and cost-focused diagnostics.
- [portfolio_backtest.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\portfolio_backtest.js)
  Latest portfolio backtest summary rendering inside dashboard workflows.
- [promotion_safety.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\promotion_safety.js)
  Governance and promotion-safety panel helpers.
- [execution_degradation.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\execution_degradation.js)
  Execution degradation and failure-signal UI helpers.
- [decision_bar.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\decision_bar.js)
  Decision summary and decision-state rendering helpers.
- [social_panels.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\social_panels.js)
  Social/news contextual panels rendered in the dashboard.
- [weather_widgets.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\weather_widgets.js)
  Weather-oriented dashboard widgets and supporting browser helpers.
- `terminal/`
  Separate browser terminal UI with charts, watchlists, account reads, fills, and gated order-entry actions.
- `vendor/`
  Third-party bundled assets.

## Maintenance Guidance

- Treat [data_sources.html](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\data_sources.html) as the canonical UI for provider/source setup.
  Do not reintroduce a second feed-configuration flow in the operator UI or in another dashboard panel.
- Keep UI reads aligned with documented API handlers.
- Prefer adding small focused modules over growing `dashboard.js` without bound.
- Treat [dashboard.html](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\dashboard.html) as a DOM contract.
  If you rename IDs or move structural regions, update the module that reads them in [dashboard.js](c:\Users\dschi\Documents\GitHub\Trading-System-\ui\dashboard.js).
- Keep client-side policy helpers advisory only.
  Execution authority still lives in backend gates and operator APIs, not in browser-local state.
- Keep operator AI and repair UI text aligned with the actual guarded behavior.
  Browser wording must not imply autonomous patching or trading authority.
- Update this README when adding a new major panel or runtime control surface.
