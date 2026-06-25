# Crypto Session Clock

Crypto execution is modeled as 24/7 by default. `engine.execution.crypto_session`
is a pure session module: it does not touch brokers, storage, schema, network,
cost math, or order mutation.

## Defaults

For crypto symbols such as `BTC` and `ETH`, `crypto_session_state(...)` returns:

- `session="open"`
- `is_open=True`
- `in_maintenance_window=False`
- `next_open_ms=None`

This includes weekends. Crypto is not suppressed by the FX weekend clock or by
equity-style UTC session windows.

## Maintenance Knobs

An optional daily UTC maintenance window can be configured with:

- `CRYPTO_MAINTENANCE_ENABLED=1`
- `CRYPTO_MAINTENANCE_START_UTC=HH:MM`
- `CRYPTO_MAINTENANCE_START_HOUR_UTC=0..23`
- `CRYPTO_MAINTENANCE_START_MINUTE_UTC=0..59`
- `CRYPTO_MAINTENANCE_DURATION_MINUTES=0..1440`
- `CRYPTO_MAINTENANCE_SYMBOLS=BTC,ETH` to scope the window to selected roots

The default duration is `0`, so the window is disabled unless explicitly
configured. During the window, `crypto_timing_adjustment(...)` annotates the
decision with `crypto_session_blocked=True` and the execution policy engine
suppresses through the existing audit path. Outside that explicit window,
crypto orders pass through unchanged except for crypto session metadata.

## Feature Sessions

`engine.strategy.feature_registry` keeps existing equity and FX session flag
behavior unchanged. For crypto rows, the base Asia/EU/US session flags are all
set to `1.0` so a 24/7 market is never labeled as out of session by equity-style
hours.

## Ownership Note

There is still no canonical `engine/data/crypto_instrument.py`. Until that owner
exists, crypto symbol normalization is local and intentionally small: root
symbols are normalized from forms such as `BTC`, `BTC/USD`, and `ETHUSDT`.
