# Risk Subsystem

The `engine/risk/` package owns the portfolio-risk engines that feed API reads, execution barriers, and operator diagnostics.

## Files

- [portfolio_risk_engine.py](portfolio_risk_engine.py)
  Additive exposure, drawdown, volatility, and budget checks that write current portfolio-risk state and snapshots.
- [monte_carlo_risk_engine.py](monte_carlo_risk_engine.py)
  Background Monte Carlo refresher that stores stressed portfolio-risk summaries and compact visualization artifacts in `risk_state`.

## `portfolio_risk_engine.py` Surface

The engine in [portfolio_risk_engine.py](portfolio_risk_engine.py) is an additive,
fail-closed overlay called from `engine.strategy.portfolio` before the
`portfolio_risk_gate`. It rescales desired allocations and never rewrites
strategy intent or selection.

Public entrypoint:

- `apply_portfolio_risk_engine(con, desired, state, now_ms) -> (adjusted_desired, info)`
  The single function other subsystems call. It projects live positions plus
  pending orders, applies the throttle/cap/budget stages below in order, runs
  post-cap validation, persists `portfolio_risk_*` runtime state plus a
  `portfolio_risk_snapshots` row and a `risk_events` row on block, and returns
  the post-risk target map alongside the detailed `info` summary. Drawdown
  thresholds are equity fractions, not percentage integers; when disabled
  (`PORTFOLIO_USE_RISK_ENGINE=0`) it returns the input unchanged with
  `{"enabled": False}`.

Knob families (module-level constants read from `PORTFOLIO_RISK_*` env):

- Drawdown throttle — `PORTFOLIO_RISK_DD_THROTTLE_START` (0.06), `DD_THROTTLE_MIN_SCALE`
  (0.35), and `DD_HARD_BLOCK` (0.15). Below start, weights are untouched; from
  start the engine linearly ramps a scale from 1.0 down to the min scale, and at
  the hard-block drawdown it fail-closes the whole evaluation. Drawdown is read
  via `engine.strategy.drawdown_state`; an unavailable reading is itself a block.
- Volatility target — `PORTFOLIO_RISK_VOL_TARGET` (0.020) with `VOL_LOOKBACK`,
  `VOL_FLOOR`/`VOL_CEIL` clamps, an optional `VOL_HARD_BLOCK`, and an optional
  GEX-derived volatility-regime modifier. A correlation-aware portfolio vol proxy
  is compared to the (modifier-adjusted) target and all signed weights are scaled
  down when the proxy exceeds target. Per-symbol vol-adjusted sizing caps
  (`USE_VOL_CAPS`, `SYMBOL_CAP_MAX_W`, `SYMBOL_CAP_MIN_MULT`, `MAX_SYMBOL_GROSS`)
  size each name by `VOL_TARGET / forecast_vol`.
- Correlation clusters — `USE_CORR_CLUSTERS`, `CORR_LOOKBACK`, `CLUSTER_CORR_TH`
  (0.85), `CLUSTER_MAX_GROSS` (0.45), `CLUSTER_MAX_COMPONENTS` (12). Highly
  correlated names are grouped into graph components (|corr| over threshold), and
  each cluster's combined gross is capped by scaling its members proportionally.
- Portfolio gross/net caps — `MAX_GROSS` (1.00) and `MAX_NET` (0.60); gross scales
  all abs weights, net scales signed weights toward zero.
- Budgets — asset-class budgets (`USE_ASSET_CLASS_BUDGETS`,
  `ASSET_CLASS_BUDGETS_JSON`, default EQUITY 1.00 / CRYPTO 0.35 / COMMODITY 0.50 /
  FX 0.50 / RATES 0.60 / UNKNOWN 0.40), strategy budgets (`MAX_STRATEGY_GROSS`
  0.60, `MAX_STRATEGY_NET` 0.40), and an alpha-decay throttle
  (`USE_ALPHA_DECAY_THROTTLE`, `ALPHA_DECAY_FRESH_S`) that rescales per strategy
  from fresh `strategy_metrics`.

The Monte Carlo block knobs (`PORTFOLIO_RISK_MC_*`) and live-gating behavior are
documented in the Monte Carlo sections below.

## API Surfaces

- `GET /api/risk/portfolio`
- `GET /api/risk/monte_carlo`
- `GET /api/execution/barrier`

The execution barrier can incorporate portfolio-risk blocks, so risk changes can affect whether the execution pipeline is currently allowed to run.

## Risk Headroom UI Thresholds

The dashboard bullet bars compare each risk value with the cap exposed by
`GET /api/risk/portfolio` or `/api/ui/metrics`. The shared browser thresholds
are exported from `ui/bullet_bars.js`: OK is `<0.85` of cap, Watch is
`>=0.85` through exactly `1.00`, and Over is strictly `>1.00`. The visual bands
and status labels both use those constants; the exact cap boundary is Watch by
design and only above-cap ratios are labeled Over.

## Configuration Families

- `PORTFOLIO_RISK_*`
- `MC_*`

These variables are consumed directly by the risk engines and should be documented in `.env.example` and `docs/REFERENCE_CONFIGURATION_GLOSSARY.md` when their operator-facing meaning changes.

## Monte Carlo Live Gating

`PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE=1` is the conservative default. In live/prod runtime, the portfolio-risk engine blocks approval when Monte Carlo risk state is missing, unreadable, unparseable, stale beyond `PORTFOLIO_RISK_MC_MAX_AGE_S`, explicitly disabled, or marked `ready=false`/`status=error`. Intentional advisory-only or disabled Monte Carlo behavior must be configured explicitly and is rejected by strict live config validation unless the audited live-risk acceptance override is present.

## Monte Carlo Visualization Contract

`GET /api/risk/monte_carlo` returns the latest persisted `monte_carlo_risk_info` state. Current refresher runs persist:

- summary tail metrics: VaR/CVaR, worst simulated drawdown, drawdown percentiles, and stress-case equivalents;
- `fan`: per-horizon simulated cumulative-return percentiles with `step`, `p05`, `p50`, and `p95`;
- `distribution`: a compact histogram of final simulated cumulative returns with bucket bounds, midpoint `value`, `count`, and `probability`.

The dashboard renders summary bars, a fan chart from `fan`, and a final-return distribution histogram from `distribution`. If an older persisted state only contains summary VaR/CVaR/drawdown fields, the API sets `chart_detail.mode="summary"` and lists the missing `fan_chart` and `distribution` fields so the UI shows an explicit summary-only state instead of an empty chart.
