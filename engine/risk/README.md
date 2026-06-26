# Risk Subsystem

The `engine/risk/` package owns the portfolio-risk engines that feed API reads, execution barriers, and operator diagnostics.

## Files

- [portfolio_risk_engine.py](portfolio_risk_engine.py)
  Additive exposure, drawdown, volatility, and budget checks that write current portfolio-risk state and snapshots.
- [monte_carlo_risk_engine.py](monte_carlo_risk_engine.py)
  Background Monte Carlo refresher that stores stressed portfolio-risk summaries and compact visualization artifacts in `risk_state`.
- [var_backtesting.py](var_backtesting.py)
  VaR/CVaR forecast persistence, exception evidence, Kupiec/Christoffersen tests, and traffic-light summaries.
- [covariance.py](covariance.py)
  Shared covariance-risk facade for aligned price returns, Ledoit-Wolf/OAS
  shrinkage, optional RMT denoising, fallback diagnostics, and serializable
  covariance/correlation matrices.

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
  `{"enabled": False}`. In live mode, disabling the engine only clears
  `portfolio_risk_block` when the independent `PORTFOLIO_NOTIONAL_BACKSTOP`
  is enabled; otherwise it leaves execution hard-blocked.

Knob families (module-level constants read from `PORTFOLIO_RISK_*` env):

- Drawdown throttle — `PORTFOLIO_RISK_DD_THROTTLE_START` (0.06), `DD_THROTTLE_MIN_SCALE`
  (0.35), and `DD_HARD_BLOCK` (0.15). Below start, weights are untouched; from
  start the engine linearly ramps a scale from 1.0 down to the min scale, and at
  the hard-block drawdown it fail-closes the whole evaluation. Drawdown is read
  via `engine.strategy.drawdown_state`; an unavailable reading is itself a block.
- Covariance risk — `RISK_COVARIANCE_METHOD` defaults to `ledoit_wolf`, with
  `oas` and `sample` as explicit alternatives. `RISK_COVARIANCE_MIN_OBS` (60)
  is the aligned-return threshold for shrinkage. If shrinkage cannot run because
  history is thin, symbols are missing, or `sklearn` is unavailable,
  `RISK_COVARIANCE_FALLBACK` (`sample` by default) selects the fallback and the
  diagnostics record `fallback_reason`, `method`, `n_obs`, `n_assets`,
  `shrinkage`, `condition_number`, and covered symbols. `RISK_COVARIANCE_RMT_*`
  gates optional eigenvalue clipping/detoning and stays off unless explicitly
  enabled for larger universes.
- Volatility target — `PORTFOLIO_RISK_VOL_TARGET` (0.020) with `VOL_LOOKBACK`,
  `VOL_FLOOR`/`VOL_CEIL` clamps, an optional `VOL_HARD_BLOCK`, and an optional
  GEX-derived volatility-regime modifier. A correlation-aware portfolio vol proxy
  uses the canonical covariance facade's correlations, is compared to the
  (modifier-adjusted) target, and all signed weights are scaled down when the
  proxy exceeds target. Per-symbol vol-adjusted sizing caps
  (`USE_VOL_CAPS`, `SYMBOL_CAP_MAX_W`, `SYMBOL_CAP_MIN_MULT`, `MAX_SYMBOL_GROSS`)
  size each name by `VOL_TARGET / forecast_vol`. `forecast_vol` is resolved
  centrally through `VOL_FORECAST_SOURCE`: `trailing`, `har`/`har_rv`, `garch`,
  `egarch`, `gjr_garch`, or `blend`. GARCH-family forecasts are persisted in
  `garch_vol_forecasts`; fitted `arch` models are opt-in via
  `GARCH_VOL_USE_ARCH=1`, and dependency-missing or convergence failures fall
  back to deterministic EWMA/trailing forecasts with diagnostics.
- Correlation clusters — `USE_CORR_CLUSTERS`, `CORR_LOOKBACK`, `CLUSTER_CORR_TH`
  (0.85), `CLUSTER_MAX_GROSS` (0.45), `CLUSTER_MAX_COMPONENTS` (12). Highly
  correlated names are grouped into graph components (|corr| over threshold), and
  each cluster's combined gross is capped by scaling its members proportionally.
- Portfolio gross/net caps — `MAX_GROSS` (1.00) and `MAX_NET` (0.60); gross scales
  all abs weights, net scales signed weights toward zero.
- Budgets — asset-class budgets (`USE_ASSET_CLASS_BUDGETS`,
  `ASSET_CLASS_BUDGETS_JSON`, default EQUITY 0.80 when
  `PORTFOLIO_RISK_BIND_EQUITY_BUDGET=1` / CRYPTO 0.35 / COMMODITY 0.50 /
  OPTION 0.20 / FX 0.50 / FUTURES 0.40 / RATES 0.60 / UNKNOWN 0.40), strategy budgets
  (`MAX_STRATEGY_GROSS` 0.60, `MAX_STRATEGY_NET` 0.40), and an alpha-decay throttle
  (`USE_ALPHA_DECAY_THROTTLE`, `ALPHA_DECAY_FRESH_S`) that rescales per strategy
  from fresh `strategy_metrics`.

### EQUITY Asset-Class Classification And Sleeve

In this section, `EQUITY` means the stocks/ETFs asset class. It does not mean
account equity, NAV, cash balance, or deployable capital.

`engine.data.asset_map.asset_class_for_symbol` classifies listed US stocks and
ETFs through a deterministic registry loaded once at module import when
`ASSET_MAP_USE_EQUITY_REGISTRY=1` (default). The registry is seeded from the same
SEC ticker-to-exchange file used by `engine.data.default_symbols`
(`SEC_TICKER_MAP_CACHE`, default `data/sec_company_tickers_exchange.json`).
Only main exchange venues are admitted: `NASDAQ`, `NYSE`, `NYSE ARCA`,
`NYSE AMERICAN`, `AMEX`, and `CBOE`. `OTC` and rows with no exchange stay
`UNKNOWN`, because they have different liquidity and cost assumptions.

The registry branch runs after explicit `ASSET_CLASS_MAP_JSON` overrides and
after the existing crypto, commodity, futures, options, FX, and rates branches.
That ordering makes it a strict upgrade only for symbols that would otherwise be
`UNKNOWN`; FX, crypto, commodity, futures, options, and rates instruments keep
their established rails. With `ASSET_MAP_USE_EQUITY_REGISTRY=0`, the registry is
empty and legacy classification is restored.

This classification is point-in-time stable for a checked-in file snapshot: it
is a pure function of the symbol, environment overrides, and SEC registry file
contents. It does not read the clock, database, network, or broker state, and it
reflects membership in the current file snapshot rather than historical
per-date exchange membership.

Newly discovered or re-upserted symbols flow into storage automatically through
`engine.data.universe.upsert_symbol`, which writes the classifier output to the
existing `symbols.asset_class` column. There is no schema change and no backfill;
historical `UNKNOWN` rows are reclassified only when they are next upserted.

The dedicated EQUITY sleeve is binding by default:
`PORTFOLIO_RISK_BIND_EQUITY_BUDGET=1` sets the default EQUITY asset-class budget
to `0.80`, below `MAX_GROSS` (`1.00`) and above the legacy `UNKNOWN` sleeve
(`0.40`). This gives listed stocks/ETFs their own gross-exposure rail while the
existing gross, per-symbol (`MAX_SYMBOL_GROSS`, default `0.35`), and correlation
cluster (`CLUSTER_MAX_GROSS`, default `0.45`) caps continue to apply. Setting
`PORTFOLIO_RISK_BIND_EQUITY_BUDGET=0` restores the legacy `EQUITY: 1.00`
default, and `PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_JSON` can still override the
effective sleeve table.

### Sector Budgets

`PORTFOLIO_RISK_USE_SECTOR_BUDGETS=1` enables an additive runtime sector budget
stage. It reads sector through `engine.data.quiver_gov.sector_for_symbol`, which
uses the existing `gov_symbol_sector_map`, sector-bearing reference tables
(`security_master`, `securities`, `symbols`/`symbols.meta_json`), and the
checked-in PIT seed at `data/equity_sector_reference.json`. The seed is explicit:
missing symbols stay unresolved (`""`) and are not assigned to a fallback bucket.
`engine.data.jobs.update_universe` seeds matching active-universe symbols into
`gov_symbol_sector_map` and logs resolved/unresolved coverage; Quiver government
ingestion also runs the same idempotent seed path. It does not read
`engine.data.equity_snapshot`; that module tracks account equity/NAV, not GICS
or sector classification.

The default sector gross budget is `PORTFOLIO_RISK_SECTOR_MAX_GROSS=0.30`.
Operators can override individual sectors with
`PORTFOLIO_RISK_SECTOR_BUDGETS_JSON`, for example
`{"ENERGY":0.50,"FINANCIALS":0.25}`. The stage runs after leverage clamps and
before strategy, volatility, and correlation-cluster caps, so sector compression
and cluster compression compose. When a sector is over budget, member target
weights are scaled proportionally and each adjusted row gets a
`reason.sector_budget` blob.

Sector budgeting is intentionally decoupled from the `EQUITY` asset-class
classifier. A discovered single name with a persisted sector is governed even if
the asset map still reports `UNKNOWN`; symbols with no resolvable sector are
left untouched. `engine.data.quiver_gov.sector_coverage_report` reports the
active/listed-equity resolved-vs-unresolved count so operators can see what is
governed before relying on the rail. The post-constraint `sector_within_cap`
check enforces the budget on the final exposure snapshot and blocks with the
existing `post_cap_validation_failed` path if a projected book remains over cap.
The diagnostics `sector_gross_pre`, `sector_gross_post`, and
`sector_budgets_hit` are serialized through the existing `portfolio_risk_info`
state for read-model consumers; no API, route, or UI edit is required.

### EQUITY Leverage And Buying-Power Guard

`PORTFOLIO_RISK_USE_EQUITY_LEVERAGE_CAPS=1` enables the runtime stock/ETF
pre-sizing guard by default. The stage runs after FX leverage caps and before the
strategy-budget stage. It is separate from the FX leverage helper: EQUITY
exposure is capped as aggregate gross stock/ETF weight against account equity
and deployable buying power, not as a per-symbol leverage multiple.

`EQUITY_LEVERAGE_MODE=cash` caps aggregate EQUITY gross at 1.0x account equity.
`EQUITY_LEVERAGE_MODE=reg_t` permits up to the Reg-T initial 2.0x ceiling only
when the `broker_account` schema exposes a valid `buying_power` value.
`EQUITY_LEVERAGE_CAPS_JSON` can lower or override the mode ceilings, for example
`{"cash":0.75,"reg_t":1.5}`. Unknown modes, malformed cap JSON, unavailable
account equity, and Reg-T mode without buying power fail closed.

The guard probes `PRAGMA table_info(broker_account)` before reading buying power
so broker-sim schemas that have `cash` and `equity` but no `buying_power` remain
valid in cash mode and fail closed in Reg-T mode. It consumes
`engine.execution.deployable_capital.compute_deployable_equity` read-only to
derive the deployable base, then writes clamp diagnostics under
`equity_leverage_*` only when it clamps or hard-blocks.
The standalone `equity_deployable_base` helper also fails closed for Reg-T
callers that omit buying power: it returns a zero deployable ceiling with
`unavailable_reason="equity_buying_power_unavailable"` instead of deriving a
2.0x ceiling from account equity alone. The engine still hard-blocks that case
before calling the helper.

### FX Sizing And Risk

FX target weights are runtime portfolio-risk notional fractions, not equity-style
share counts. For an FX symbol such as `EURUSD`, `engine.strategy.fx_sizing`
records base notional, quote notional (`quote = base * pair_rate`), units, lots,
pair rate, and effective leverage on the returned target row under `fx`, with
the enforcement reason under `reason.fx_leverage_cap`. The broker boundary still
owns weight-to-order conversion; the risk engine only makes the FX target
unambiguous for that later FX execution work.

FX instrument semantics come from FX-02 via
`engine.data.universe.get_instrument_metadata`, normalized through the internal
`_fx_instrument` adapter so field spelling differences (`base_ccy` versus
`base_currency`, `leverage_cap` versus `max_leverage`) do not leak into risk
logic. FX exposure bucketing prefers that instrument asset class and falls back
to `asset_class_for_symbol`, so the existing `"FX": 0.50` sleeve binds when FX
metadata is present.

FX leverage enforcement is controlled by `PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS`
(default `1`). The stage runs after asset-class budgets and before correlation
cluster caps. It uses `_last_price(con, symbol)` for the pair rate and clamps each
FX leg to the lesser of the FX-02 instrument leverage cap and the regulatory cap
from `engine.risk.fx_leverage_caps`. That cap table is seeded from FX-00 section
6, defaults to EU/ESMA-style major/minor/exotic caps with a US profile, and can
be overridden with `FX_REGULATORY_LEVERAGE_CAPS_JSON` plus
`FX_LEVERAGE_JURISDICTION`. Missing pair rates are data-unavailable and
fail-closed with `block_reason.type="fx_leverage_hard_block"`.

Currency-pair clustering is controlled by `PORTFOLIO_RISK_FX_CURRENCY_CLUSTERS`
(default `1`). In addition to price-correlation edges, FX pairs sharing a base or
quote currency receive structural graph edges, so pairs such as `EURUSD` and
`GBPUSD` are capped as one cluster even with thin correlation history. Cluster
reason blobs include `fx_shared_currency` for auditability.

The detailed FX fields are stored in the existing `portfolio_risk_info` JSON
state. `engine.api.api_system.api_get_portfolio_risk` already reads and returns
that state, so the FX-08 read-model path can consume these fields without API,
route, or UI changes.

Broker-bound defense in depth:

- `engine.strategy.portfolio_risk_gate.apply_execution_risk_governor(...)` rechecks
  gross and net caps on the final live execution payload before
  `engine.execution.broker_router` can route to Alpaca, IBKR, or any other
  broker adapter. This boundary check uses the shaped orders plus current
  `broker_positions` and pending `broker_order_state` exposure. It resizes
  both target-weight orders and explicit `qty` orders before broker routing,
  suppresses orders with no remaining headroom, and blocks on invalid exposure
  data rather than assuming zero exposure.
- The execution-time gross cap is `EXEC_PORTFOLIO_TOTAL_EXPOSURE_CAP`, falling
  back to `PORTFOLIO_RISK_MAX_GROSS` then `PORTFOLIO_GROSS_CAP`. The execution-time
  net cap is `EXEC_PORTFOLIO_DIRECTION_CONCENTRATION_CAP`, falling back to
  `PORTFOLIO_RISK_MAX_NET` then `PORTFOLIO_MAX_NET_EXPOSURE`.

The Monte Carlo block knobs (`PORTFOLIO_RISK_MC_*`) and live-gating behavior are
documented in the Monte Carlo sections below.

## API Surfaces

- `GET /api/risk/portfolio`
- `GET /api/risk/monte_carlo`
- `GET /api/risk/var_backtest`
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
- `RISK_COVARIANCE_*`

These variables are consumed directly by the risk engines and should be documented in `.env.example` and `docs/REFERENCE_CONFIGURATION_GLOSSARY.md` when their operator-facing meaning changes.

## Monte Carlo Live Gating

`PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE=1` is the conservative default. In live/prod runtime, the portfolio-risk engine blocks approval when Monte Carlo risk state is missing, unreadable, unparseable, stale beyond `PORTFOLIO_RISK_MC_MAX_AGE_S`, explicitly disabled, or marked `ready=false`/`status=error`. Intentional advisory-only or disabled Monte Carlo behavior must be configured explicitly and is rejected by strict live config validation unless the audited live-risk acceptance override is present.

### EQ-10 Cost-Aware Edge Filter And Tail-Risk Rollout

The alert edge filter in `engine.strategy.edge_filter` remains default-off:
`ALERT_USE_EXEC_COST_FILTER=0` and `ALERT_MIN_NET_ABS_Z=0.0` do not reject or
adjust live alerts. `ALERT_EXEC_COST_FILTER_ASSET_CLASSES` defaults empty, which
preserves current behavior for any operator who explicitly enables the filter.
The enable flag, minimum net z threshold, and asset-class scope are read at each
`adjust_expected_z_for_costs` call, so supervised env reloads after module import
take effect in the live alert gate. Set the scope to `EQUITY` only after the
equity asset map is populated enough for the target stock/ETF universe.

Rollout order:

1. Run `python tools/calibrate_edge_filter_min_net_abs_z.py --asset-class EQUITY`
   against paper or shadow `trade_attribution_ledger` data. The tool is
   read-only, emits JSON to stdout, and returns `status:"insufficient_data"`
   with `recommended_min_net_abs_z:null` when usable fills are below
   `--min-fills` instead of fabricating a threshold.
2. Set the operator-chosen `ALERT_MIN_NET_ABS_Z` from calibration, enable
   `ALERT_USE_EXEC_COST_FILTER=1`, and scope with
   `ALERT_EXEC_COST_FILTER_ASSET_CLASSES=EQUITY`.
3. Prove the behavior in paper or shadow. The existing production preflight
   sanity probe exercises `adjust_expected_z_for_costs` when the filter is
   enabled and reports missing-volatility warnings without arming live capital.
4. Set `EQUITY_EXEC_COST_FILTER_REQUIRED_IN_LIVE=1` only when the calibrated
   threshold and paper/shadow evidence are accepted. Strict live validation then
   requires `ALERT_USE_EXEC_COST_FILTER=1` and `ALERT_MIN_NET_ABS_Z > 0`, using
   the existing `LIVE_RISK_THRESHOLD_ACCEPTANCE_OVERRIDE` audit path for any
   deliberate exception.

`UNKNOWN` classification is intentionally not promoted to `EQUITY` by this
rollout. Operators should populate `ASSET_CLASS_MAP_JSON` or the equity
registry inputs for real stocks and ETFs. Including `UNKNOWN` in
`ALERT_EXEC_COST_FILTER_ASSET_CLASSES` is a deliberate broadened scope, not a
default.

This rollout does not lower or default-enable Monte Carlo thresholds. The
existing `PORTFOLIO_RISK_MC_VAR_*`, `PORTFOLIO_RISK_MC_CVAR_*`, drawdown, and
staleness blocks stay the live fail-closed tail-risk authority; EQ-10 only adds
a calibrated execution-cost edge rail that can be required after paper/shadow
evidence exists.

## Monte Carlo Visualization Contract

`GET /api/risk/monte_carlo` returns the latest persisted `monte_carlo_risk_info` state. Current refresher runs persist:

- summary tail metrics: VaR/CVaR, worst simulated drawdown, drawdown percentiles, and stress-case equivalents;
- `simulation`: method metadata for `gaussian`, `student_t`, `historical`, or `filtered_historical`, including fitted/configured Student-t DoF, historical sample size/window, deterministic seed status, and fallback reasons;
- `evt`: optional EVT/POT CVaR overlay metadata with threshold, tail sample size, fitted shape/scale, and whether the EVT estimate was used;
- `fan`: per-horizon simulated cumulative-return percentiles with `step`, `p05`, `p50`, and `p95`;
- `distribution`: a compact histogram of final simulated cumulative returns with bucket bounds, midpoint `value`, `count`, and `probability`.

The dashboard renders summary bars, a fan chart from `fan`, and a final-return distribution histogram from `distribution`. If an older persisted state only contains summary VaR/CVaR/drawdown fields, the API sets `chart_detail.mode="summary"` and lists the missing `fan_chart` and `distribution` fields so the UI shows an explicit summary-only state instead of an empty chart.

## VaR/CVaR Backtesting Contract

Monte Carlo refreshes best-effort upsert each forecast into `risk_var_forecasts` with `forecast_id`, `forecast_ts_ms`, horizon, VaR/CVaR levels, simulation method, and metadata. The `risk_var_backtest` job reads matured forecasts, aligns realized portfolio return from `equity_history` at `forecast_ts_ms + horizon * VAR_BACKTEST_STEP_MS`, and writes `risk_var_backtest_results` rows with:

- forecast id/timestamp, realized timestamp, horizon, confidence level, VaR/CVaR values, realized return/loss, and exception flag;
- Kupiec POF statistic/p-value/status;
- Christoffersen independence statistic/p-value/status;
- rolling exception rate/window and traffic-light status/reason;
- PIT-alignment metadata recording the start equity point, target timestamp, and realized equity point used.

`GET /api/risk/var_backtest` returns these rows with `authority.mode="read_only_risk_model_backtesting"`. Missing tables or no matured forecasts return an explicit empty state and do not affect unrelated dashboard reads. Live risk approval continues to fail closed through the existing Monte Carlo readiness/staleness/threshold gates.
