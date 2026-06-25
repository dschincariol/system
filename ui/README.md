# UI Layer

The `ui/` directory contains browser assets served by the dashboard server.

## Structure

- [dashboard.html](dashboard.html)
  Main dashboard shell and DOM contract for the modular browser UI.
- [dashboard.js](dashboard.js)
  Main dashboard controller that coordinates API reads, refresh loops, panel rendering, and operator-side state.
  It delegates focused screen controllers such as Data Health to dedicated modules while preserving screen routing and refresh scheduling. It also owns the broker configuration panel for masked config summaries, connection testing, guarded activate/disable actions, and audit-log rendering.
- [api_client.js](api_client.js)
  Shared dashboard API client. It imports a `token` launch parameter into same-origin browser storage, falls back to the stored token on later loads, supports explicit clearing with `clear_token=1`, and attaches `X-API-Token` only to same-origin `/api/*` requests. It also paces same-origin API GET fan-out under the default dashboard token budget while leaving mutations outside the read throttle. It must not place tokens in request URLs or forward them to cross-origin fetches.
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
- [panel_state.js](panel_state.js)
  Shared panel-state and connection/freshness model for dashboard reads. Route loaders should report through the shared fetch client so the global banner, card metadata, and advisory action guards stay consistent.
- [confirmation_modal.mjs](confirmation_modal.mjs)
  Shared accessible confirmation modal for high-impact operator/dashboard
  mutations. It gathers typed tokens, consequence acknowledgement, optional
  hold-to-confirm timing, reason text, request ids, targets, actors, and source
  surface metadata for backend validation and audit.
- [data_sources.html](data_sources.html)
  Standalone Data Sources Control Center and the canonical single-page source-management shell for live ingestion/provider configuration.
- [data_sources.js](data_sources.js)
  Browser controller for guided source inventory, backend-catalog setup metadata, inline field validation, next-action recommendations, CRUD actions, tests, credential resets, and source-specific logs.
- [data_sources.css](data_sources.css)
  Styling for the standalone data-source control-plane experience.
- [data_health.js](data_health.js)
  Data Health screen controller for the dashboard `data` route. It owns the Data Health fetch list, payload normalization, and rendering for ingestion, provider telemetry, runtime signals, and feature-visibility panels while preserving IDs declared in [dashboard.html](dashboard.html).
- [fx_format.js](fx_format.js)
  Pure display helpers for FX pairs. They mirror FX-02's accepted pair spellings for browser formatting only, providing pip-aware prices, pip-distance text, and lot/unit quantity rendering without DOM, network, or credential access.
- [fx_session.js](fx_session.js)
  Pure browser-side mirror of FX-04's 24/5 session boundary model. The default presentation calendar opens Sunday 22:00 UTC and closes Friday 22:00 UTC, with override knobs so the UI can be pinned to backend FX-clock settings.
- [portfolio.js](portfolio.js)
  Portfolio panel rendering and portfolio-specific dashboard helpers.
- [bullet_bars.js](bullet_bars.js)
  Accessible risk-headroom bullet bars for exposure, volatility, and drawdown caps. It re-exports the shared risk-headroom band, status, and default-cap thresholds used by the visual bands and labels.
- [risk_headroom_thresholds.js](risk_headroom_thresholds.js)
  Shared risk-headroom band thresholds, status classification, and default gross/net/drawdown/vol caps used by bullet bars and portfolio-backtest drawdown references.
- [regime_ribbon.js](regime_ribbon.js)
  Regime-context ribbon renderer for macro, asset, and micro regime labels.
- [risk_charts.js](risk_charts.js)
  Lazy-loaded risk, Monte-Carlo, alpha-decay, and regime-history chart renderers for the Positions & Exposure surface.
  Monte-Carlo rendering consumes `/api/risk/monte_carlo` summary metrics, `fan` percentile rows, and final-return `distribution` buckets. When the backend exposes only a legacy summary payload, the fan and distribution canvases are hidden and replaced with explicit unavailable notes.
  Alpha-decay defaults to the highest-severity/relevance chartable strategy, not the strategy with the most rows, and the header selector lets operators switch among returned strategies. Multi-series visuals pass every visible line or band into [chart_a11y.js](chart_a11y.js): portfolio risk exposes gross, net, drawdown, and blocked bands; the Monte-Carlo fan exposes p05, p50, and p95; alpha-decay exposes rolling Sharpe and half-life.
- [market_stress.js](market_stress.js)
  Market-stress panel renderer and history sparkline. The sparkline preserves post-GDELT stress scores above `1.0`, auto-scales the y-axis, and draws warning/critical reference bands from shared threshold helpers.
- [market_stress_thresholds.js](market_stress_thresholds.js)
  Shared warning/critical threshold constants and classification helpers used by market-stress badges, chart bands, runtime summaries, and metric glossary status.
- [news_panels.js](news_panels.js)
  Latest-news table and news-sentiment history renderer. The sentiment chart treats `[-1, 1]` as the expected display scale, draws a neutral zero baseline with positive/negative context bands, clamps out-of-range payload values with visible anomaly markers, and passes raw/displayed sentiment plus data-quality notes into [chart_a11y.js](chart_a11y.js).
- [operator_summary.js](operator_summary.js)
  Operator-facing summary card helpers used by the dashboard.
- [operator_overview.js](operator_overview.js)
  Read-only first-screen Overview tiles composed from production runtime, decision, risk, PnL, and stress endpoints.
- [health_score.js](health_score.js)
  Deterministic top-level health score helper for alerts, runtime, data, and execution. The numeric score is normalized over available factors only, so every render must also show coverage such as `1/4 factors`; partial or low coverage is a trust state, not a passing full-system health claim.
- [runtime_status_summary.js](runtime_status_summary.js)
  Compact runtime and pipeline-health summary helpers used in newer diagnostics surfaces.
- [readiness_evidence.js](readiness_evidence.js)
  Consolidated readiness evidence card renderer and high-risk broker-activation guard backed by `/api/operator/readiness_evidence`.
- [runtime_diagnostics.js](runtime_diagnostics.js)
  Richer runtime diagnostics rendering for operator troubleshooting.
- [telemetry_panel.js](telemetry_panel.js)
  Telemetry visualization helpers for runtime/operator diagnostics.
- [execution_metrics.js](execution_metrics.js)
  Execution-metrics panels for slippage, latency, and cost-focused diagnostics.
- Dashboard execution screen diagnostics
  [dashboard.html](dashboard.html) and [dashboard.js](dashboard.js) render the
  `/api/execution/diagnostics` contract for by-symbol TCA, rolling execution
  quality, partial fills, rejected/suppressed outcomes, intent-to-fill traces,
  LOB/DeepLOB readiness, and learned-slicing diagnostics. These panels are
  read-only; browser state remains advisory and cannot grant execution
  authority.
- [portfolio_backtest.js](portfolio_backtest.js)
  Latest portfolio backtest summary rendering inside dashboard workflows. Its Lightweight Charts runtime loading, chart construction, series compatibility, marker compatibility, and default drawdown cap reference come from shared UI helpers.
- [job_catalog.js](job_catalog.js)
  DOM-light helpers for rendering the dashboard Job Catalog from backend-owned `/api/jobs` or `/api/jobs/catalog` metadata.
- [chart_a11y.js](chart_a11y.js)
  Shared chart accessibility helpers for programmatic labels, one-line summaries, keyboard focus metadata, and table fallbacks.
  Use `seriesFields` for charts that draw multiple numeric series or percentile bands so the accessible summary and table keep rows where any visible series has data. Single-series charts can continue to pass `valueKey`/`valueLabel`.
- [replay.mjs](replay.mjs)
  Historical Replay controller and DPR-aware canvas renderer for `/api/replay/day`. Replay price candles render as OHLC bodies with high/low wicks, bottom time ticks, a compact marker legend, selected-time cursor, event markers, and an accessibility table that exposes open, high, low, close, and volume from the same normalized candle stream used for drawing.
- [decision_overlays.js](decision_overlays.js)
  Shared normalization and legend helpers for automated-decision chart overlays. API decision windows keep `start_ts_ms`/`end_ts_ms` in milliseconds and expose chart-ready `start_s`/`end_s` seconds for Lightweight Charts rendering.
- [pro_chart_core.js](pro_chart_core.js)
  Shared Lightweight Charts construction, indicator, marker, price-line, and decision-window band lifecycle helpers used by dashboard and terminal pro charts.
- [pro_chart_engine.js](pro_chart_engine.js)
  Dashboard pro chart orchestration for live price candles, volume, loaded-window VWAP/EMA, PnL, decision markers, price levels, and shaded decision-window bands.
- [promotion_safety.js](promotion_safety.js)
  Governance and promotion-safety panel helpers.
- [promotion_gate.mjs](promotion_gate.mjs)
  Promotion gate and Governance Evidence Center rendering helpers for challenger comparison, generated-candidate provenance, model-risk controls, production monitoring, and shadow-capital score state.
- [execution_degradation.js](execution_degradation.js)
  Execution degradation and failure-signal UI helpers.
- [model_performance_divergence.mjs](model_performance_divergence.mjs)
  Model performance divergence panel renderer for `/api/model/performance_divergence`. It ranks chartable metrics by status severity, displayed absolute delta, source freshness, and product importance so the default chart highlights the most important comparable divergence instead of the first payload row. The dashboard selector lets operators switch the charted metric while preserving the panel summary, missing-source notes, and row table.
- [decision_bar.js](decision_bar.js)
  Decision summary and decision-state rendering helpers.
- [decision_stepper.js](decision_stepper.js)
  Decision-modal stage flow renderer derived from the drill-down stage payload.
- [decision_attribution.js](decision_attribution.js)
  Signed feature-contribution attribution bar for the decision-modal why view.
- [feature_visibility.js](feature_visibility.js)
  Data Health render helpers for structured-document extraction and graph-relational feature visibility from `/api/data/feature_visibility`.
- [social_panels.js](social_panels.js)
  Social/news contextual panels rendered in the dashboard.
- [weather_widgets.js](weather_widgets.js)
  Weather-oriented dashboard widgets and supporting browser helpers.
- `terminal/`
  Separate browser terminal UI with charts, watchlists, account reads, fills, and gated order-entry actions.
- `vendor/`
  Third-party bundled assets.

## FX Surfacing

- Data Sources marks FX or OANDA-style feeds with an `FX feed` badge and reuses the existing `/api/data_sources/test` action. The FX test-result renderer whitelists only status, ok, latency, detail, and message fields so credential-shaped payload data is not displayed.
- The dashboard Positions & Exposure card reads FX sleeve, leverage, and sizing fields only from existing `/api/ui/metrics`, `/api/portfolio`, `/api/risk/portfolio`, `/api/broker`, and `/api/terminal/positions` payloads. If FX-05 or FX-06 has not surfaced those fields, the card renders `FX data not yet available` rather than inventing zeros.
- The browser terminal uses `fx_format.js` for FX pair prices and lot quantities, and `fx_session.js` for the 24/5 session label. Non-FX symbols stay on the existing terminal formatting path.
- No FX UI helper reads credentials, calls live broker mutation routes, or moves risk/session gate authority into the browser. Backend runtime gates remain authoritative.

## Maintenance Guidance

- Treat [data_sources.html](data_sources.html) as the canonical UI for provider/source setup.
  Do not reintroduce a second feed-configuration flow in the operator UI or in another dashboard panel. Provider-specific setup copy, docs links, plan notes, field help, env-var mapping, validation hints, and safety warnings must come from the backend `templates[]` catalog returned by `/api/data_sources`, not a hardcoded JavaScript provider guide map.
- Keep UI reads aligned with documented API handlers.
- Keep chart accessibility aligned with visible chart data. If a chart draws multiple series, percentile bands, shaded state bands, or pro-chart overlays, wire those values through [chart_a11y.js](chart_a11y.js) `seriesFields`, explicit columns, and a plain-language summary. Do not expose only the primary price or median line when secondary visible data changes the interpretation.
- Keep Historical Replay aligned with `/api/replay/day` OHLC candles. The replay canvas must keep high/low/open/close visible, normalize malformed high/low bounds so wicks always contain the open/close body, keep fills anchored to fill price, keep decisions/orders anchored to nearest candle close when those events have no own price, and keep selected-time/event marker alignment derived from millisecond timestamps.
- Keep the model performance divergence chart default aligned with [model_performance_divergence.mjs](model_performance_divergence.mjs) ranking. Do not fall back to payload order for chart selection; diverged metrics must outrank watch and ok metrics, then larger displayed deltas, fresher sources, and product-critical metrics decide ties.
- Keep pro-chart candles timestamp-safe. [pro_chart_core.js](pro_chart_core.js) rejects candles without an explicit finite positive `t`/`time` value, so missing or zero timestamps cannot create a bogus 1970 chart point.
- Keep pro-chart decision windows sourced from `/api/terminal/decision_overlays`.
  The backend payload uses millisecond timestamps (`start_ts_ms`, nullable `end_ts_ms`), while Lightweight Charts consumes epoch seconds; [decision_overlays.js](decision_overlays.js) owns that conversion, and [pro_chart_core.js](pro_chart_core.js) owns primitive attach/update/detach cleanup for dashboard and terminal charts. Open-ended windows should extend to the latest loaded candle, not wall-clock time.
- Keep pro-chart VWAP labeled as loaded-window VWAP. [decision_overlays.js](decision_overlays.js) computes it by accumulating `close * volume` and volume across the candles currently loaded in the chart plus live tail updates; it does not reset at trading-session boundaries. A true session VWAP needs reliable symbol asset-class, exchange timezone, and session-boundary metadata from the market-data contract before changing the label or reset behavior.
- Keep the Readiness Evidence card aligned with `/api/operator/readiness_evidence`.
  It explains server-side blockers and warning evidence for live/paper operation and is used as an advisory pre-check before broker activation; backend broker config, execution, and runtime gates remain authoritative.
- Keep risk-headroom bullet bars and portfolio drawdown references aligned with [risk_headroom_thresholds.js](risk_headroom_thresholds.js). The default track spans `0.00..1.25` of cap; OK is `<0.85`, Watch is `>=0.85` through exactly `1.00`, and Over is strictly `>1.00`. The default caps are gross `1.00`, net `0.60`, drawdown `0.06`, and vol `0.02`; the portfolio backtest throttle line is the negative drawdown cap because drawdown series are rendered below zero.
- Keep dashboard, terminal, and portfolio Lightweight Charts code on [pro_chart_core.js](pro_chart_core.js) for runtime loading, v4/v5 series compatibility, chart construction, resize cleanup, and marker compatibility. Surface modules may own orchestration and labels, but should not add parallel loader or series shims.
- Keep the news sentiment chart honest about scale and missing data. `/api/news/sentiment` preserves missing sentiment as `null`, not `0.0`; [news_panels.js](news_panels.js) treats null/malformed values as skipped unavailable points, while true numeric zero remains neutral. The same renderer owns the `[-1, 1]` display clamp, anomaly count, raw-value table column, and zero-baseline context rather than assuming the backend has already sanitized the series.
- Keep portfolio risk history reference lines semantically correct. [risk_charts.js](risk_charts.js) draws a zero baseline only when zero is inside the y-domain; it must not use the plot midpoint as a stand-in for zero when net exposure crosses positive and negative values.
- Keep the Governance Evidence Center aligned with `/api/governance/evidence`, its drilldowns, and `/api/governance/shadow_capital/scores`. This surface explains backend authority; it must not imply a promotion or allocation bypass.
- Keep the Data Health screen controller in [data_health.js](data_health.js). [dashboard.js](dashboard.js) should delegate the `data` route refresh to that module, not own Data Health endpoint lists or DOM rendering directly. The static UI contract check enforces the module boundary.
- Keep the Data Health structured-document and graph-feature panels aligned with `/api/data/feature_visibility`. These panels are read-only visibility surfaces; shadow-only structured-document and graph features must be labeled clearly and must not imply live trading authority.
- Keep health-score coverage visible anywhere the health score appears. [health_score.js](health_score.js) treats missing factor groups as unavailable rather than failed and normalizes the score across available factor weights; therefore a `100/100` score with `1/4 factors` means only the available factor is healthy. Low or partial coverage must keep warning/high-visibility styling in both the top health bar and Operator Overview instead of using the same treatment as fully supported stable health.
- Keep the broker configuration panel aligned with `/api/broker/config`, `/api/broker/test_connection`, and `/api/broker/audit`.
- Before shipping UI changes, run `npm ci` with Node.js 20 LTS (`>=20.17.0 <21`) and npm 10.x, then run `npm run check:ui`.
  The check validates tracked local asset references, dashboard JS syntax, browser-helper tests, and the fast chart contract pytest lane without starting the dashboard or requiring market-data credentials. The chart lane covers risk chart API shapes, risk chart UI helpers, portfolio backtest chart contracts, and model performance divergence frontend behavior.
- Run `npm run test:ui` for the broader UI pytest allowlist. Integration-scale backtest, CPCV, HPO, and Optuna suites remain outside the local UI gate and should run through backend/full pytest validation when those subsystems change.
- Prefer adding small focused modules over growing `dashboard.js` without bound.
- Treat [dashboard.html](dashboard.html) as a DOM contract.
  If you rename IDs or move structural regions, update the module that reads them in [dashboard.js](dashboard.js).
- Keep client-side policy helpers advisory only.
  Execution authority still lives in backend gates and operator APIs, not in browser-local state.
- Treat structured 4xx business refusals as displayable application responses, not transport crashes, when a panel explicitly opts into application-level failures. Data-source test/setup flows render `reason_code`/`message` from missing credentials or provider auth refusals; terminal order entry still blocks the action and shows the backend refusal reason.
- Keep structured confirmations advisory in the UI and authoritative on the server.
  New high-impact UI mutations should use [confirmation_modal.mjs](confirmation_modal.mjs)
  or an equivalent shared helper, but the matching sidecar/API route must still
  reject missing or invalid confirmation and include the confirmation metadata in
  mutation audit records.
- Keep job console and command-palette safety state sourced from `/api/jobs` or `/api/jobs/catalog`.
  The backend catalog owns job `safety`, disabled prerequisites, and guarded action policy; browser regexes must not be the authority for dangerous job starts.
- Keep operator AI and repair UI text aligned with the actual guarded behavior.
  Browser wording must not imply autonomous patching or trading authority.
- Update this README when adding a new major panel or runtime control surface.
- On Linux/macOS workstations without system Node/npm, run `bash tools/bootstrap_local_toolchain.sh` from the repository root. It installs Node.js 20.19.4 with npm 10.8.2 inside `.venv`, runs `npm ci`, and exposes the `python`, `python3`, `node`, `npm`, and `npx` command names through user-local shims.
