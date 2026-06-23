# FX Cost Realism

FX backtests and promotion evidence must be net of realistic trading costs. The
offline model adds three FX-specific terms for asset classes whose normalized tag
starts with `FX`:

- pip spread converted to bps with a deterministic reference-mid proxy;
- overnight swap/carry in pips, long/short aware and scaled by hold nights;
- weekend-gap surcharge when a hold crosses the Friday-to-Sunday FX close.

The default spread, swap, and reference-price tables live in
`engine/execution/fx_costs.py`. They are conservative CALIBRATION TODO
placeholders, not broker-calibrated quotes. FX-02 remains the source of truth for
symbol semantics and pip size when its parser/accessor is available; local
`FX_PIP_SIZE` and `FX_REF_PRICE` values are offline fallbacks/proxies only.

## Normalization

Cost lookups normalize `EURUSD`, `EUR/USD`, and `EUR_USD` to the cost-table key
`EUR_USD`. Runtime callers should continue to normalize symbols through FX-02;
`normalize_fx_symbol(...)` is defensive glue for offline reports and tests.

## Knobs

- `FX_PIP_SPREAD_OVERRIDE_JSON`: optional pair-to-pips map, for example
  `{"EUR_USD": 0.7}`.
- `FX_SWAP_PIPS_OVERRIDE_JSON`: optional swap override map. It accepts either
  `{"long": {"EUR_USD": 0.1}, "short": {"EUR_USD": 0.05}}` or per-pair
  objects such as `{"EUR_USD": {"long": 0.1, "short": 0.05}}`.
- `FX_WEEKEND_GAP_BPS`: base weekend-gap surcharge in bps before the per-pair
  risk multiplier.
- `CPCV_FX_COMMISSION_BPS`: explicit FX commission bps for CPCV; spread remains
  the dominant default cost.
- `CPCV_FX_SYMBOL`, `CPCV_FX_NIGHTS`, and `CPCV_FX_CROSSES_WEEKEND`: CPCV
  defaults when an FX cost config omits those fields.

Malformed override JSON is ignored and falls back to defaults.

## Gate Flow

`engine/execution/broker_sim.py` adds FX spread, swap/carry, and weekend-gap bps
inside `_offline_ac_cost_components`. `engine/strategy/cpcv.py` resolves FX cost
configuration with `asset_class.startswith("FX")` and applies the same cost
terms to CPCV return streams. Non-FX cost output shape is unchanged.

`engine/strategy/fx_profitability_report.py` produces a per-pair/per-factor
pass/fail report by calling the existing `run_gated_backtest`, `cpcv_backtest`,
and `passes_promotion_gate` paths. The report is evidence only. It does not
promote, persist promotion state, or call live broker paths. Any FX challenger
still must pass the normal champion/challenger governance path, including
`assess_challenger`, before promotion.

