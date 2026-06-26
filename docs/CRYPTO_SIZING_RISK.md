# Crypto Sizing And Risk

CRYPTO-06 gives crypto a consume-only sizing layer. The sizing helper does not
route orders, authorize trading, or change broker-sim quantity conversion. It
attaches risk metadata that the portfolio risk engine and execution governor can
consume.

## Sizing Module

`engine.strategy.crypto_sizing` mirrors the FX sizing helper with crypto-specific
semantics:

- `normalize_crypto_symbol(...)` maps local variants such as `BTC/USD`,
  `BTCUSD`, `BTCUSDT`, and `XBTUSD` to the bare-root convention used by current
  storage paths. This is a fallback until a canonical `crypto_instrument.py`
  exists.
- `crypto_weight_to_notional(...)` reports USD notional and fractional units.
  Crypto is marked `fractional=true` and carries `min_increment` diagnostics
  instead of assuming whole-share units.
- `clamp_crypto_weight_to_leverage(...)` clamps signed weights to
  `CRYPTO_MAX_LEVERAGE`, default `1.0`.
- If an existing volatility input is supplied by the risk engine, the effective
  crypto cap is also tightened by `CRYPTO_VOL_TARGET / volatility`. The default
  `CRYPTO_VOL_TARGET` is `0.03`; no new data source is introduced.

The module attaches diagnostics under the target row's `crypto` field and
`reason.crypto.sizing`. The metadata is advisory/control context only.

## Portfolio Risk Integration

`engine.risk.portfolio_risk_engine` now has
`PORTFOLIO_RISK_USE_CRYPTO_LEVERAGE_CAPS`, default `1`. For crypto symbols only,
the engine:

- resolves local crypto metadata without requiring a schema change,
- preserves the existing `CRYPTO: 0.35` asset-class budget,
- clamps crypto exposure to the configured leverage/vol cap,
- annotates the target with fractional-unit and notional diagnostics.

Equity and FX sizing paths are unchanged. FX still uses `fx_sizing.py` and
`PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS`; the crypto stage only selects symbols
classified as `CRYPTO`.

## Live Crypto Gate

`engine.strategy.portfolio_risk_gate.apply_execution_risk_governor(...)`
enforces a dedicated live crypto gate before generic exposure caps, and
`engine.execution.broker_router._crypto_order_safety_block(...)` repeats the
live-disable check at the broker routing boundary before broker attempts:

- `CRYPTO_LIVE_TRADING_ENABLED`, default `0`, blocks crypto orders in live mode
  unless explicitly enabled.
- `CRYPTO_NOTIONAL_CAP_USD`, default `10000`, blocks any live crypto order whose
  notional exceeds the cap.

These checks are independent of the global live-execution controls, kill
switches, and broker-specific preflights, which still apply on top. The router
block is defense-in-depth for code paths that reach broker routing without the
portfolio governor; it only applies to non-dry-run crypto batches whose failover
chain contains a live broker. Paper/sim/dry-run orders and non-crypto orders
continue through the existing path.

## Explicit Non-Goals

The broker-sim weight-to-quantity conversion remains unchanged and is still
`NO-GO-pending-owner` for the CRYPTO-02 owner. CRYPTO-06 does not change cost
math, model routing, schema, or grant order authority to sizing helpers.
