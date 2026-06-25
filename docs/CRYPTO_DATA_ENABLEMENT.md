# Crypto Data Enablement

This profile enables crypto market data and perpetual funding data for sim/paper
research only. It does not enable live execution.

## What Flows

- Spot/OHLCV polling uses the existing CCXT price provider in
  `engine/data/provider_registry.py`; `CCXT_ENABLED` controls whether the
  `ccxt` provider is enabled.
- Perpetual funding and basis ingestion uses
  `engine/data/jobs/ingest_crypto_funding.py` and writes to the existing
  `crypto_funding_rates` table.
- Positioning features are computed by
  `engine/data/crypto_positioning.py` and exposed through
  `engine/strategy/feature_registry.py` when `USE_FUNDING_FEATURES=1`.
- Runtime health exposes `crypto_data` and
  `ingestion_sources.crypto_funding` with wired/enabled status, row count, and
  last-row age.

## Sim/Paper Profile

Use `deploy/profiles/crypto_sim.env.example` as the committed example. It sets:

- `ENGINE_MODE=sim`, `EXECUTION_MODE=sim`, `BROKER=sim`
- `DISABLE_LIVE_EXECUTION=1`, `KILL_SWITCH_GLOBAL=1`
- `CCXT_ENABLED=1`
- `INGEST_CRYPTO_FUNDING_ENABLED=1`
- `USE_FUNDING_FEATURES=1`
- `CRYPTO_PERP_MARKETS=` to auto-discover configured crypto symbols

The data-source manager still controls whether `ingest_crypto_funding` is a
desired managed job. Its runtime projection sets
`INGEST_CRYPTO_FUNDING_ENABLED=1` only when the `crypto_funding` source is
enabled in the source control plane.

## Safety Boundary

This is data-only enablement. No order, cancel, replace, flatten, broker
routing, execution-mode, or kill-switch behavior is changed. The profile keeps
execution in sim and live execution disabled.

## Train/Serve Parity

`USE_FUNDING_FEATURES` is read at import time by
`engine/strategy/feature_registry.py`. The feature schema now records
`feature_flags.USE_FUNDING_FEATURES`, and model-load schema checks reject a
train/serve mismatch. A model trained with funding features off must be served
with the flag off; a model trained with funding features on must be served with
the flag on.

## Validation

Run from the repo root:

```bash
python tools/validate_crypto_data.py
```

The validator:

- imports `ccxt` and probes public configured funding/spot endpoints
  defensively;
- skips public probes when `ccxt` is unavailable or the network/provider is
  unavailable;
- runs a mocked funding poller cycle against a temporary SQLite database;
- asserts rows persist to `crypto_funding_rates`;
- asserts the six funding/basis features materialize and do not look ahead.

`PASS` and `SKIP` exit `0`. Integrity failures exit nonzero.

## Still Missing

The current crypto data spine does not ingest on-chain metrics, open interest,
liquidations, or crypto social data. There is also no canonical
`engine/data/crypto_instrument.py`; stored symbols continue to use bare roots
such as `BTC` and `ETH`, consistent with `asset_map.py` and
`crypto_funding_rates`.
