# Crypto Cost Realism

Crypto offline evaluation now uses deterministic, conservative cost assumptions
instead of falling through to equity defaults.

## Components

- Maker/taker fee: `engine.execution.crypto_costs.fee_bps(...)`.
- Spread: `engine.execution.crypto_costs.spread_bps(...)`, reported as full
  spread and applied as half-spread in return-space cost paths.
- Perpetual funding carry: `funding_carry_bps(...)`, where positive funding is
  a cost to longs and a credit to shorts, scaled by held nights.
- Temporary impact: `AlmgrenChrissCost` has a dedicated `CRYPTO`
  CALIBRATION-TODO coefficient override so crypto no longer silently reuses
  equity impact coefficients.

All tables in `engine/execution/crypto_costs.py` are CALIBRATION-TODO
placeholders. They are intentionally conservative and deterministic for
offline simulation, CPCV, and promotion-gate evidence. They are not broker
quotes and they are not used for live execution.

## Environment Knobs

- `CRYPTO_TAKER_BPS_OVERRIDE_JSON`
- `CRYPTO_MAKER_BPS_OVERRIDE_JSON`
- `CRYPTO_SPREAD_BPS_OVERRIDE_JSON`
- `CRYPTO_FUNDING_BPS_PER_DAY_OVERRIDE_JSON`
- `CRYPTO_FUNDING_OVERRIDE_JSON`
- `CRYPTO_TAKER_BPS`, `CRYPTO_MAKER_BPS`, `CRYPTO_SPREAD_BPS`,
  `CRYPTO_FUNDING_BPS_PER_DAY` for scalar default-root overrides.
- `CPCV_CRYPTO_SYMBOL`, `CPCV_CRYPTO_NIGHTS`, and
  `CPCV_CRYPTO_LIQUIDITY` for CPCV defaults.
- Existing CPCV commission overrides still work:
  `CPCV_CRYPTO_COMMISSION_BPS`, `CPCV_CRYPTO_TAKER_BPS`,
  `CPCV_CRYPTO_MAKER_BPS`.

Malformed JSON overrides are ignored and never logged verbatim.

## Symbol Normalization

There is still no canonical `engine/data/crypto_instrument.py` owner. Until one
exists, cost code uses the local bare-root fallback also used by the earlier
crypto slices: `BTC`, `BTC/USD`, and `BTCUSD` normalize to `BTC`.

## Enforcement

`broker_sim._offline_ac_cost_components` applies crypto fee, half-spread,
temporary impact, and funding carry to offline/gated backtests. `cpcv.py`
applies the same crypto terms in the standard CPCV path so challengers are
measured net of crypto costs before promotion gates inspect the result.

`engine.strategy.crypto_profitability_report.evaluate_crypto_challengers(...)`
is diagnostic-only. It calls the existing `run_gated_backtest`,
`cpcv_backtest`, `compute_pbo`, and `passes_promotion_gate` paths and reports
pass/fail evidence. It does not promote models, write promotion records, or call
live broker adapters. Promotion remains owned by the existing
`champion_manager` and `promotion_guard.assess_challenger` path.

## Known Non-Goals

This slice does not touch live broker paths, UI, schema, or the existing
`broker_sim.py` weight-to-quantity conversion seam. That seam remains
NO-GO-pending-owner for a future sizing/execution owner.
