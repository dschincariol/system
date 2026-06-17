# Charting & Decision-Visualization: Best-in-Class Recommendation Report

**Status:** Advisory recommendation (no code changes). **Date:** 2026-06-16.
**Scope:** Audit of the UI charting/visualization surface, research into best-in-class systems, and a
prioritized roadmap to make this trading system best-in-class at *showing its automated decisions
visually* in a way that is *simple for non-technical users*.

**Two hard goals frame every recommendation:**

1. **Show automated decisions visually** — use charts to convey as much as possible about both the
   *data* and the *system's decision-making*.
2. **Simple for non-technical users** — a person with no trading or statistics background should be
   able to answer *"is the system OK, what is it doing, and should I trust it?"* in about five seconds.

> This report is a plan, not an implementation. The roadmap is advisory until funded. Nothing here
> changes the model-vs-runtime contract: **the model proposes, the runtime owns gates, and the UI
> stays advisory** (per `CLAUDE.md`). Charts make decisions *legible*; they never get order authority.

---

## 1. Executive Summary

The system has **two disconnected chart stacks**.

- The **"pro" stack** (`ui/pro_chart_engine.js` + `ui/terminal/pro_charting.js`) is genuinely
  institutional-grade: it wraps the already-vendored **TradingView Lightweight Charts v5.1.0** with
  live SSE streaming, exponential-backoff reconnect, a magnet crosshair, careful teardown, and a
  freshness ticker. But it drives **only the single live price chart**, the two engines are **~80%
  duplicated**, and it draws **only buy/sell arrows** — `createPriceLine` is never called, so no
  entry/stop/cap levels and no risk or decision overlays ever reach the chart.

- The **"basic" stack** (`ui/charts.js`, 181 lines of static Canvas 2D, plus **four hand-rolled
  polyline copies**) drives **all 8 analytical canvases** on the main dashboard. It has no time axis,
  no tooltips/hover/crosshair/keyboard, **zero accessibility**, and a misleading bug that flattens
  out-of-range data against the plot edge. A **205 KB Chart.js bundle is vendored but completely
  dead** (loaded by no HTML, imported by no JS).

Most importantly for the two goals: **the system barely shows its automated decisions visually.**
Recent decisions, suppression tiers, risk caps, kill switches, the promotion gate, and the regime
stack are all rendered as text, pills, and grids. Meanwhile **rich data the system already computes
and persists is thrown away at the UI**: `/api/risk/portfolio` returns 200 history points but the UI
charts only the latest one; `/api/risk/monte_carlo` (VaR/CVaR) and `/api/alpha_decay` have **zero**
chart consumers; the 3-layer regime stack is never charted. The palette pairs red and green (the
worst colorblind case), and mobile has no charts and not even the plain-English narrative the desktop
already produces.

**The recommended direction** (respecting the no-bundler / vendored-standalone / advisory-UI
constraints) is, in three phases:

- **P0 — make existing charts honest & accessible:** delete the dead bundle, add `role="img"` +
  data-table fallbacks via one shared helper, fix the flat-clamp and add a time axis, introduce an
  Okabe-Ito colorblind-safe palette, and port the plain-English summary to mobile.
- **P1 — make decisions visual** (the headline work, anchored by a **new at-a-glance Overview
  screen**): a left-to-right decision-flow stepper, risk bullet bars, a regime ribbon, price-line
  levels + risk/suppression overlays on the price chart, equity + underwater-drawdown + trade
  markers + benchmark, champion/challenger comparison bars, and charting the under-surfaced
  risk / Monte-Carlo / alpha-decay / regime timeseries.
- **P2 — consolidate:** unify on one Lightweight Charts v5 engine (extracting the duplicated core),
  and add fan-chart uncertainty, calibration-with-ECE, and SHAP-style decision attribution.

Almost every P1 visual can be built from data and components **that already exist** — the gap is
presentation, not plumbing.

---

## 2. Method

This report combines:

- A **code audit** of the full UI charting surface (`ui/charts.js`, `ui/pro_chart_engine.js`,
  `ui/terminal/pro_charting.js`, the 8 dashboard canvases, the decision/risk/governance panels, the
  mobile UI, and the CSS palette), with every claim tied to a `file:line`.
- A **backend data audit** of `engine/api/` and `dashboard_server.py` to find timeseries/decision
  data that exists but is shown as a number or table.
- **Research** into best-in-class charting and decision visualization from primary sources
  (graphical-perception theory, dashboard design, WCAG 2.2, high-performance HMI standards, and the
  TradingView Lightweight Charts documentation). Sources are listed in §8.

Key code claims were independently re-verified (see §3 evidence and §9 appendix).

### 2.1 Codex verification addendum

I re-audited the charting and decision-visualization paths on 2026-06-16 and agree with the
attached report's core findings. Four repo-specific details sharpen the implementation plan:

- `dashboard.html` already contains hidden `portfolioEquityPro` and `portfolioDdPro` containers next
  to the legacy portfolio canvases, but no runtime code uses them. That makes the portfolio
  equity/drawdown view the best low-risk proof point for migrating analytical charts to Lightweight
  Charts panes before touching the live-market chart.
- `pro_chart_engine.js` depends on `terminal/pro_charting.js` for chart preference state while also
  duplicating large parts of its chart lifecycle, series creation, overlay, crosshair, and SSE logic.
  The consolidation target should therefore be two modules, not one: `pro_chart_core.js` for shared
  chart mechanics and `pro_chart_prefs.js` for shared persisted state.
- `/api/terminal/markers` currently builds chart markers only from fills and portfolio order intents.
  It does not expose suppressed, blocked, throttled, risk-capped, or kill-switch windows. Those need
  to be added to a marker/overlay endpoint before the price chart can truthfully show automated
  decisions that did *not* become fills.
- The mobile UI fetches the same health/readiness/status primitives as desktop but independently
  rewrites them into terse labels. Reusing `summarizeRuntimeStatus()` on mobile is a no-regrets P0
  because it improves non-technical comprehension without new backend work or chart code.

---

## 3. Current-State Assessment

### 3.1 Two disconnected chart stacks

| | "Pro" stack | "Basic" stack |
|---|---|---|
| Files | `ui/pro_chart_engine.js`, `ui/terminal/pro_charting.js` | `ui/charts.js` + 4 hand-rolled copies |
| Engine | TradingView Lightweight Charts **v5.1.0** (vendored) | Static Canvas 2D |
| Used by | The single live price chart only | **All 8** dashboard analytical canvases |
| Interactivity | Magnet crosshair, live SSE stream, reconnect, freshness ticker | **None** (no hover/tooltip/axis/keyboard) |
| Decision overlays | Buy/sell arrows only; `createPriceLine` **never called** | None |
| Health | **~80% duplicated** between the two files | Misleading flat-clamp bug; `drawSpark` is dead code |

### 3.2 The "basic" engine is weak and misleading

- `ui/charts.js` (181 lines) exports `drawSpark` (**dead code — zero callers**), `renderLineChart`,
  and `drawCalibration`. `renderLineChart` draws a polyline plus two Y labels: **no x/time axis, no
  legend, no data points, no tooltip**.
- It **silently flattens out-of-range data**: `yFor` clamps the normalized value to `[0,1]`
  (`ui/charts.js:121`), so any point outside the supplied `yMin/yMax` is pinned to the top or bottom
  edge instead of being clipped. This actively misleads — `ui/portfolio_backtest.js` passes `yMax:0`
  for the drawdown chart, so any positive value snaps to the top.
- **Four independent line-drawing implementations** exist: `ui/charts.js`, an inline polyline in
  `ui/market_stress.js:222` (hardcoded `0..1`), an inline polyline in `ui/news_panels.js` (hardcoded
  `-1..1`), and a genuinely richer DPR-aware engine with markers/axis/cursor in `ui/replay.mjs` (the
  best of the basic stack). `marketStressSparkline` also has no `width` attribute, so it defaults to
  300 px while its siblings are 900 px.

### 3.3 A dead 205 KB dependency

`ui/vendor/chart.umd.min.js` is **Chart.js 4.4.1 (205 KB)** and is referenced only by itself and a
docs note — **no HTML `<script>` tag, no JS import**. `ui/dashboard.html` loads exactly three module
scripts (`dashboard.js`, `voice.js`, `copilot.js`). It is pure cruft and a maintenance/security
surface.

### 3.4 Charts have zero accessibility

All 8 `<canvas>` elements in `ui/dashboard.html` (`replayChart`, `marketStressSparkline`,
`newsSentimentCanvas`, `calibCanvas`, `equityDriftCanvas`, `performanceDivergenceChart`,
`portfolioEquityCanvas`, `portfolioDdCanvas`) have **no `role="img"`, no `aria-label`, no `tabindex`,
and no adjacent/inner data-table fallback**. Canvas exposes no semantics by default, so chart content
is invisible to screen readers and keyboard users — a hard WCAG 2.2 failure (SC 1.4.1, 4.1.2).

### 3.5 Decisions are text, not visuals

The system's automated decision-making is almost entirely rendered as pills, tables, and key/value
grids:

- **Recent decisions** and **suppression tiers** (`HARD_BLOCK` / `SOFT_THROTTLE` /
  `SIZE_COMPRESSION`) are text rows.
- **Kill switches** (`ui/kill_switch_ui.js`) are plain text with a fragile mouse-Y click hack instead
  of real per-row buttons.
- The **decision bar** (`ui/decision_bar.js`) is a row of colored text pills.
- **Market stress** (`ui/market_stress.js`) collapses a rich, multi-component signal (VIX / VVIX /
  MOVE / term structure / credit / rates, each with z-scores) into a single colored pill, a text
  table, **a raw JSON dump**, and an axis-less sparkline — with no visual indication of *which
  component is driving the stress*.
- The **promotion gate** is a numeric table, not a champion-vs-challenger comparison.
- The only decision-on-a-timeline visual is the **read-only** `ui/replay.mjs`.

### 3.6 Rich data is collected, then discarded at the UI

| Data (exists in backend) | Endpoint | What the UI does today |
|---|---|---|
| Portfolio risk history (gross/net/vol/drawdown/blocked over time) | `/api/risk/portfolio` returns `history` (≈200 pts) | Uses only `history[0]` (`ui/dashboard.js:5062-5063`); the timeseries is never charted |
| Monte-Carlo VaR / CVaR | `/api/risk/monte_carlo` | **Zero** UI chart consumers |
| Alpha decay / half-life | `/api/alpha_decay` | **Zero** UI chart consumers |
| 3-layer regime stack (macro/asset/micro) | persisted | Never charted |
| Backtest equity **and** drawdown per point, plus Sharpe/Sortino/Calmar | `/api/portfolio/backtest/latest` | Equity + drawdown drawn as two flat static lines; ratios unused |

This is the single biggest, lowest-cost opportunity: the data already flows; only the presentation is
missing.

### 3.7 Not colorblind-safe; no reduced-motion/contrast support

Red (`#ff6b6b` / `#e25555` / `#ff4d6d`) is paired with green (`#2bb673` / `#2ea043` / `#22c55e`) for
crit-vs-ok across pills, the status header, the alert heatmap, and mobile badges — the **worst
dichromat pair**, violating WCAG SC 1.4.1. There are **no Okabe-Ito tokens**, the alert heatmap
swatch is a color-only rectangle with no legend, and **no** CSS declares
`prefers-reduced-motion`, `prefers-contrast`, or `forced-colors`. With ~1 in 12 men having a
color-vision deficiency, a color-only kill-switch or risk state is a real single point of failure.

### 3.8 Mobile is a second-class citizen

`ui/mobile/` has **no charts** and does **not** reuse `summarizeRuntimeStatus` — phone users see only
terse tone words (ok / degraded / kill / live), even though mobile already fetches the same status /
health / readiness inputs the desktop narrative is built from.

### 3.9 What is already strong (build on this)

- `ui/runtime_status_summary.js` + `ui/operator_summary.js` already produce a **plain-English
  headline + meaning + next-steps**.
- `ui/health_score.js` produces a deterministic **0–100 score + badge** from four weighted factors.
- `ui/decision_drilldown.mjs` already returns **ordered, toned decision stages** — it is
  stepper-ready.
- Most status pills already carry **redundant text** (not strictly color-only).
- `ui/replay.mjs` is a DPR-aware engine with markers, an axis, and a selection cursor — a good model
  for what the others could be.

---

## 4. North-Star Vision

**One coherent visual story per operator question, readable in ~5 seconds by a non-expert, where the
system shows *what it decided and why* — not just the numbers.** Concretely:

1. **One analytical engine.** The already-vendored **TradingView Lightweight Charts v5.1.0** drives
   every time-aligned view, using v5 *panes* to stack price + trade-markers, equity, underwater
   drawdown, and exposure as crosshair-synchronized sub-charts.
2. **Every automated decision is visible** on a timeline and in a **decision-flow stepper**
   (Signal → Confidence/Calibration gate → Risk/Suppression gate → Sizing → Order → Fill), with the
   blocking stage emphasized and a plain-language reason on click.
3. **Risk lives in bullet bars** (value + cap marker + ok/watch/over bands), not gauges or grids;
   **regimes are a labeled colored ribbon**; suppression and kill-switch are status lights and
   timeline markers, never bare colored dots.
4. **A "quiet baseline" HMI aesthetic** (per ISA-101): normal state is neutral grayscale, saturated
   amber/red is reserved for deviation, and encoding is always **color + shape/icon + text +
   position** (Okabe-Ito). Every chart has a `role="img"` label, a one-line plain-English takeaway,
   and a toggleable data-table fallback. The same glanceable narrative reaches mobile.
5. **Honest charts:** fan-chart uncertainty bands, a benchmark overlay so a rising curve isn't
   mistaken for outperformance, and calibration with an Expected Calibration Error (ECE) verdict —
   so a non-expert can judge whether to trust the system.

---

## 5. Flagship: The At-a-Glance "Overview" Screen

The single highest-impact move for non-technical users is a **dedicated simplified operator home**
that answers three questions, each as a glanceable tile with a one-line plain-English takeaway, a
freshness age, and a `role="img"` + table fallback. **It is built almost entirely from components
that already exist** (see the reuse map below), so most of the work is composition, not new logic.

### 5.1 Layout mockup

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  OVERVIEW                                              backend 3s ago   [≡]    │
├──────────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────────────────┐   "Is the system OK?"                         │
│  │     ●  SAFE                 │   System is live and trading is allowed.      │
│  │   (green + ✓ + word)        │   Next: nothing needed — monitoring.          │
│  │   Health 92/100             │   [health_score.js + runtime_status_summary]  │
│  └────────────────────────────┘                                               │
│                                                                                │
│  "What is it doing right now?"   (latest decision — where did it stop?)        │
│  ┌─ Signal ─→ Confidence ─→ Risk/Suppress ─→ Sizing ─→ Order ─→ Fill ─┐        │
│  │   ✓ AAPL    ✓ 0.71        ⛔ SOFT_THROTTLE   —        —       —      │        │
│  └────────────────────────────────────────────────────────────────────┘        │
│  "Throttled: drawdown 4.1% > comfort band. Size cut 50%."  [decision_drilldown]│
│  recent: ·····▌··▌·······▌···  (markers on a timeline → click for "why")        │
│                                                                                │
│  "Should I trust it / how close to the edge?"                                  │
│  Gross  ▮▮▮▮▮▮▯▯▯▯ 0.62 / 1.00   ░ ok                                          │
│  Net    ▮▮▮▮▯▯▯▯▯▯ 0.31 / 0.60   ░ ok        PnL today  +1.2% ▲  ▁▂▃▅▄▆        │
│  Vol    ▮▮▮▮▮▮▮▮▯▯ target 0.9x   ▒ watch     Stress  CALM — driven by VIX      │
│  Drawd. ▮▮▮▮▮▮▮▯▯▯ 4.1% / 6.0%   ▒ watch     [bullet_bars.js + market_stress]  │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Component-reuse map

| Tile | Question | Reuses | New |
|---|---|---|---|
| **Status** | Is it OK? | `health_score.js` (score+badge), `runtime_status_summary.js` / `operator_summary.js` (headline/meaning/next-step), execution barrier, kill switch | A big **SAFE / CAUTION / STOP** word (icon + color + word, never color alone) |
| **Decision flow** | What is it doing? | `decision_drilldown.mjs` (ordered toned stages, ts, related counts) | A left-to-right **stepper** DOM with the blocking stage emphasized + a recent-decisions mini-timeline |
| **Risk headroom** | How close to the edge? | `/api/risk/portfolio` (caps), `/api/pnl` | **Bullet bars** (`bullet_bars.js`); PnL sparkline + up/down word; a single labeled stress gauge naming its top driver |

### 5.3 Why this serves both goals

- **Shows decisions visually:** the stepper and timeline make "what did the system decide, and where
  and why did it stop?" a *picture*, not a paragraph — the core supervisor question, answered
  instantly (situation-awareness research, §8).
- **Simple for non-technical users:** semantic words (SAFE/CAUTION/STOP), one screen, plain-English
  takeaways, and no jargon or raw JSON. Bullet bars decode via length (the second-strongest
  perceptual channel) so "how close to the limit" is obvious without reading a number.

---

## 6. Prioritized Roadmap

Effort key: **S** = small, **M** = medium, **L** = large. Priority: **P0** before **P1** before
**P2**.

### Phase 1 — Make existing charts honest & accessible (P0)

| Item | Why | What | Files | Effort |
|---|---|---|---|---|
| Delete dead Chart.js bundle | 205 KB, loaded by nothing; confuses which stack is canonical | Remove the file; note in vendor README that uPlot (MIT) is the only future non-financial fallback | `ui/vendor/chart.umd.min.js`, `ui/vendor/README_charting.txt` | S |
| Chart accessibility helper | All 8 canvases fail WCAG 2.2; data is already in hand at render time | New `ui/chart_a11y.js`: sets `role="img"` + `aria-label` (latest value + range) and renders a toggleable visually-hidden `<table>` + one-line takeaway; wire into every renderer + the 8 `<canvas>` tags | `ui/charts.js`, `ui/dashboard.html`, `ui/market_stress.js`, `ui/news_panels.js` | M |
| Fix `renderLineChart` honesty | Flat-clamp misleads (`charts.js:121`); no time axis on 4+ series | Clip out-of-range points instead of clamping; add a 2–3 tick time axis from per-point `ts`; delete/rewire dead `drawSpark`; fix `marketStressSparkline` width | `ui/charts.js`, `ui/portfolio_backtest.js`, `ui/market_stress.js`, `ui/dashboard.html` | M |
| Okabe-Ito palette + fix color-only | Red/green fails SC 1.4.1; heatmap swatch is color-only | Shared CSS token set (Blue `#0072B2` / Vermillion `#D55E00` / Bluish-green `#009E73` / Orange `#E69F00`); map pills/header/heatmap/mobile badges; add glyph + `aria-label` + legend to the heatmap swatch; add `prefers-reduced-motion/contrast/forced-colors` | `ui/dashboard_theme.css`, `ui/styles.tech.css`, `ui/alerts.js`, `ui/base.css` | M |
| Plain-English narrative on mobile | Phone users see only tone words; the desktop narrative is fully reusable | Import `summarizeRuntimeStatus` into `mobile.js`; render headline + meaning + next-steps in the existing `role="status"` region; add a tiny text PnL trend | `ui/mobile/mobile.js`, `ui/mobile/index.html`, `ui/runtime_status_summary.js` | S |

### Phase 2 — Make decisions visual (P1) — anchored by the Overview screen (§5)

| Item | Why | What | Files | Effort |
|---|---|---|---|---|
| **Decision-flow stepper** | The #1 supervisor question is "where/why did it stop?"; the data already exists | Render `buildDecisionStageRows()` as a left-to-right stepper; each stage a chip with icon+label+color (not color alone) + pass/suppress/block reason + timestamp; emphasize the blocking step | `ui/decision_drilldown.mjs`, `ui/dashboard.js`, `ui/dashboard.html` | M |
| **Risk bullet bars + regime ribbon + kill-switch lights** | Few's research: bullet graphs decode better than gauges/grids; maps to gross 1.0 / net 0.6 / vol / 6% caps | New `ui/bullet_bars.js` (one row per cap from `/api/risk/portfolio`); a labeled regime ribbon; replace the mouse-Y hack with real status-light buttons | `ui/kill_switch_ui.js`, `ui/dashboard.js`, `ui/dashboard.html` | L |
| **Price-chart decision/risk overlays** | The rich risk + decision data never reaches the price chart (only arrows) | Use v5 `createPriceLine` for entry/avg-cost/stop/TP; shade drawdown-throttle / kill-switch / circuit-breaker windows; suppressed (hollow/amber) + blocked (red+lock) markers; distinct Okabe-Ito colors + legend for VWAP/EMA; bind markers to an always-present hidden series so they survive line/area mode | `ui/pro_chart_engine.js`, `ui/terminal/pro_charting.js`, `engine/terminal/api/api_terminal.py` | L |
| **Chart the under-surfaced timeseries** | Mostly zero backend work; directly shows the system's decision-making | Risk-over-time chart from `history[200]`; Monte-Carlo VaR/CVaR bullet/bar + drawdown fan chart; alpha-decay rolling-Sharpe with half-life markers; small regime-stack endpoint for the ribbon | `engine/api/api_system.py`, `dashboard_server.py`, `ui/dashboard.js` | M |
| **Equity + drawdown + markers + benchmark; champion/challenger bars** | Prevents the most common misreadings (no benchmark → false outperformance; flat line → underestimated drawdown) | Migrate equity/backtest canvases to Lightweight Charts panes: equity line + underwater drawdown sub-pane sharing the x-axis, trade markers, a 6%-throttle reference line, an optional SPY benchmark, Sharpe/Sortino/Calmar annotations; render the promotion gate as side-by-side comparison bars with a significance/decision indicator | `ui/portfolio_backtest.js`, `ui/portfolio.js`, `ui/promotion_gate.mjs`, `ui/dashboard.js` | L |

### Phase 3 — Structural consolidation (P2)

| Item | Why | What | Files | Effort |
|---|---|---|---|---|
| **Unify on one engine** | ~80% duplication between the two pro engines; basic stack should retire | Extract shared `ui/pro_chart_core.js` imported by both wrappers (align `_volColor`); standardize all time-aligned views on Lightweight Charts v5; retire `charts.js` from timeseries use | `ui/pro_chart_engine.js`, `ui/terminal/pro_charting.js`, `ui/charts.js` | L |
| **Uncertainty, calibration, attribution** | Honest uncertainty + trustable confidence + the standard "why did it trade this?" explanation | Upgrade `drawCalibration` with bin counts + **ECE** + a plain-English verdict; render model decisions with fan-chart confidence bands; add a signed horizontal **SHAP-style** attribution bar (baseline 0, top ~6 features, color+sign+plain label) to the "why" view; extend the decision payload with per-feature contributions | `ui/charts.js`, `ui/why_modal.js`, `ui/dashboard.js` | L |

### Recommended first implementation slice

Do not start with a large dashboard redesign. The fastest path to visible quality is a narrow slice
that proves the new standards end to end:

1. Add `chart_a11y.js`, fix the `renderLineChart` clamp, add the missing `marketStressSparkline`
   width, and delete the dead Chart.js bundle.
2. Use the existing `portfolioEquityPro` / `portfolioDdPro` containers to render portfolio equity
   and underwater drawdown with Lightweight Charts panes, reference lines, hover values, and a table
   fallback while keeping the old canvases behind a feature flag.
3. Add risk bullet bars from `/api/risk/portfolio.history` on the Positions screen, using the 200
   points already returned by the endpoint.
4. Render a decision-flow stepper from `buildDecisionStageRows()` in the decision modal first; once
   the interaction is stable, promote the latest decision to the new Overview screen.
5. Extend `/api/terminal/markers` or add `/api/terminal/decision_overlays` so suppressed/blocked/
   throttled decisions and kill-switch windows can be drawn on the price chart with clear legends.

That sequence lands honesty, accessibility, one professional analytical chart, and one visual
decision flow before any broad redesign. It also keeps the UI advisory: every visual is derived from
runtime/server state and links back to the underlying row or endpoint.

---

## 7. Risks & Guardrails

- **Advisory boundary.** Decision/risk overlays and the stepper *visualize* server/runtime decisions;
  they must never add a path for the UI to alter sizing, suppression, or kill-switch state
  (`CLAUDE.md` model-vs-runtime contract).
- **Don't grow `dashboard.js`.** It is already ~300 KB. New visuals must land in small modules
  (`chart_a11y.js`, `bullet_bars.js`, `pro_chart_core.js`), per the repo's "prefer small modules"
  rule — otherwise this worsens the very file the team is trying to shrink.
- **Lazy-load the chart lib.** Migrating dashboard canvases to Lightweight Charts means it loads on
  more screens; keep it behind the existing `_ensureLightweightCharts()` lazy path to avoid loading
  189 KB where charts aren't shown.
- **Per-tick performance.** The pro engines already recompute VWAP/EMA O(n) over full history per
  streamed candle; fix the recompute (incremental update or v5 conflation) *before* adding overlays,
  or operator laptops will spike under fast SSE.
- **Backend coupling.** Regime-stack and SHAP attribution need new/extended endpoints that touch the
  high-blast-radius storage facade — follow the migrations + tests discipline; treat them as scoped
  backend tasks, not UI-only.
- **Accessibility can regress.** Make `chart_a11y.js` the mandatory render path and cover it with the
  existing UI contract tests, or future panels will ship color-only/no-fallback again.
- **Palette re-grade is a visible change.** Moving to a quiet-baseline (neutral "ok", saturated only
  on deviation) changes the whole look and may surprise operators used to all-green; roll out behind
  theme tokens and verify 3:1 / 4.5:1 contrast so it doesn't trade one a11y problem for another.

---

## 8. Research Foundation & Sources

### Graphical perception & dashboard design
- Cleveland, W.S. & McGill, R. (1984), *Graphical Perception* — encode the most important quantities
  with **position on a common scale**, then **length**, then angle/slope, area, and color last.
  <http://snoid.sv.vt.edu/~npolys/projects/safas/science.pdf>
- Munzner, T. (2009), *A Nested Model for Visualization Design and Validation*.
  <https://www.cs.ubc.ca/labs/imager/tr/2009/NestedModel/NestedModel.pdf>
- Shneiderman, B. (1996), *The Eyes Have It* — overview first, zoom & filter, details on demand.
  <https://www.cs.umd.edu/~ben/papers/Shneiderman1996eyes.pdf>
- Tufte, E. (1983/2001), *The Visual Display of Quantitative Information* — data-ink ratio,
  sparklines, small multiples. <https://jtr13.github.io/cc19/tuftes-principles-of-data-ink.html>
- Few, S. (2006), *Information Dashboard Design* + *Common Pitfalls in Dashboard Design* — replace
  gauges/dials/pies with **bullet graphs** and sparklines.
  <https://www.perceptualedge.com/articles/Whitepapers/Common_Pitfalls.pdf>

### Accessibility
- W3C WAI — Understanding **SC 1.4.1 Use of Color** (WCAG 2.2).
  <https://www.w3.org/WAI/WCAG22/Understanding/use-of-color.html>
- W3C WAI — Understanding **SC 1.4.11 Non-text Contrast** (≥3:1).
  <https://www.w3.org/WAI/WCAG22/Understanding/non-text-contrast.html>
- W3C WAI — Understanding **SC 4.1.3 Status Messages** (ARIA live regions).
  <https://www.w3.org/WAI/WCAG21/Understanding/status-messages.html>
- W3C — **WCAG 2.2** (incl. SC 4.1.2 Name, Role, Value). <https://www.w3.org/TR/WCAG22/>
- Okabe & Ito **Color Universal Design** palette (Wong 2011, *Nature Methods*) — colorblind-safe
  categorical set. <https://conceptviz.app/blog/okabe-ito-palette-hex-codes-complete-reference>
- TPGi — *Making data visualizations accessible* (hidden data table + plain-language summary).
  <https://www.tpgi.com/making-data-visualizations-accessible/>
- USWDS — *Data visualizations* component guidance.
  <https://designsystem.digital.gov/components/data-visualizations/>

### Decision / AI visualization & high-performance HMI
- ISA-101 — *Human Machine Interfaces for Process Automation Systems* (quiet gray baseline; color for
  abnormal). <https://www.isa.org/standards-and-publications/isa-standards/isa-101-standards>
- Emerson — *Up Your Productivity and Safety with High Performance HMI Design*.
  <https://www.emersonautomationexperts.com/2023/industrial-internet-things/up-your-productivity-and-safety-with-high-performance-hmi-design/>
- ISA-18.2 / EEMUA-191 alarm rationalization — *Alarm management questions everyone asks*.
  <https://www.isa.org/intech-home/2020/march-april/features/alarm-management-questions-that-everyone-asks>
- Endsley situation-awareness model (Perception → Comprehension → Projection).
- NASA — *Glass Cockpit Fact Sheet* (integrated/trend indicators over walls of digits).
  <https://www.nasa.gov/centers/langley/news/factsheets/Glasscockpit_prt.htm>
- SHAP — additive feature-attribution (waterfall/force plots) for per-decision "why".
  <https://shap.readthedocs.io/en/latest/example_notebooks/api_examples/plots/waterfall.html>
- NIST AI RMF 1.0 — transparency, explainability, and interpretability are distinct but mutually
  supporting traits; user-facing explanations should answer what happened, how the system decided,
  and why the decision means something in context.
  <https://nvlpubs.nist.gov/nistpubs/ai/nist.ai.100-1.pdf>

### Charting technology
- TradingView **Lightweight Charts v5** (Apache-2.0, ~35 KB gzip core, financial-first, *panes* for
  synchronized sub-charts). <https://www.tradingview.com/blog/en/tradingview-lightweight-charts-version-5-50837>
  · docs: <https://tradingview.github.io/lightweight-charts/docs/release-notes> · series types:
  <https://tradingview.github.io/lightweight-charts/docs/series-types>
- **uPlot** (MIT, ~50 KB, Canvas) — reserve *only* for dense non-financial small-multiples if ever
  needed. <https://github.com/leeoniya/uPlot>
- **Do not** add ECharts / Plotly / Highcharts — they fight the no-bundler / vendored-standalone
  constraint and duplicate what Lightweight Charts already does for this domain.

---

## 9. Appendix

### 9.1 Per-canvas inventory (main dashboard)

| Canvas id | Renderer | Data source | Interactivity | A11y |
|---|---|---|---|---|
| `replayChart` | `replay.mjs` (own DPR engine) | `/api/replay` candles + events | selection cursor, markers | none |
| `marketStressSparkline` | inline polyline in `market_stress.js` (hardcoded 0..1, missing width) | `/api/market_stress_history` | none | none |
| `newsSentimentCanvas` | inline polyline in `news_panels.js` (hardcoded -1..1) | news sentiment | none | none |
| `calibCanvas` | `drawCalibration` (`charts.js`) | `/api/embed_conf_calib` | none | none |
| `equityDriftCanvas` | `renderLineChart` (`charts.js`) | `/api/equity_drift` | none | none |
| `performanceDivergenceChart` | `renderLineChart` via injection | `/api/model/performance_divergence` | none | none |
| `portfolioEquityCanvas` | `renderLineChart` | `/api/portfolio/backtest/latest` | none | none |
| `portfolioDdCanvas` | `renderLineChart` (passes `yMax:0` → flat-clamp bug) | `/api/portfolio/backtest/latest` | none | none |

### 9.2 "Data that exists but isn't charted" (priority list)

1. **Risk history timeseries** (`/api/risk/portfolio` → `history`, ~200 pts) — only `history[0]` used
   (`dashboard.js:5062-5063`). *Near-zero backend work.*
2. **Monte-Carlo VaR / CVaR** (`/api/risk/monte_carlo`) — zero UI consumers. *Fan-chart candidate.*
3. **Alpha decay / half-life** (`/api/alpha_decay`) — zero UI consumers. *Rolling-Sharpe line.*
4. **Regime stack** (macro/asset/micro) — persisted, never charted. *Ribbon (needs small endpoint).*
5. **Backtest Sharpe/Sortino/Calmar + per-point drawdown** — present in payload, only equity/dd drawn
   as flat lines.
6. **Per-feature decision attribution** — tree models (LightGBM/XGBoost/GBM) are tree-SHAP-amenable;
   payload would need per-feature contributions added.

### 9.3 Verified claims (spot-checks)

- `chart.umd.min.js` referenced by no `<script>` tag and no import. ✔
- Lightweight Charts vendor build is **v5.1.0**. ✔
- `createPriceLine` never called in repo code (only the `priceLineVisible` option appears). ✔
- `drawSpark` has zero callers. ✔
- `riskHistory = asArray(riskPortfolio.history)` then `riskLatest = asObject(riskHistory[0])` —
  full history never charted (`dashboard.js:5062-5063`). ✔
- `alpha_decay` and `monte_carlo` have zero references under `ui/`. ✔
- `ui/mobile/` does not reference `summarizeRuntimeStatus`. ✔
- `ui/dashboard.js` is ~300 KB. ✔

---

*Prepared as an advisory recommendation. If implementation is greenlit, gate every code change behind
`npm run check:ui` plus focused Python/Node tests for the changed behavior, and keep new rendering
code in small modules per `ui/README.md`.*
