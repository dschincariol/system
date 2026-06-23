# Jobs Subsystem

The `engine/jobs/` package holds the live price-ingestion job path consumed by the runtime job manager. Its single daemon is the registered Polygon websocket price streamer, the primary live equity price feed for the system; downstream prediction, risk, and execution read the price tables it populates.

## Files

- [stream_prices_polygon_ws.py](stream_prices_polygon_ws.py)
  Daemon that connects to the Polygon stocks websocket through `engine/data/provider_sessions/polygon_ws_session.py`, subscribes ACTIVE/WATCH symbols routed to the `polygon` provider, and publishes live trade/quote events into the price tables via `engine/runtime/price_router.publish_price_events`. Holds the `stream_prices_polygon_ws` job lock, emits heartbeats, and drives lifecycle state (warming-up/live/degraded) with reconnect, dead-feed, and restart-cooldown handling. The live session parses websocket payloads, validates status/events, coerces numeric fields, and computes quote spread before taking its shared state lock; the lock is reserved for ordered `_last` mutation, per-stream watermarks, duplicate keys, telemetry counters, and pending-event queue updates so flush snapshots are not blocked by per-message CPU work.

## Registration & Launch

Registered in [../runtime/job_registry.py](../runtime/job_registry.py) under job name `stream_prices_polygon_ws` as a `daemon` in the `price_feed` category with `primary_feed: true`, and launched by [../../start_system.py](../../start_system.py). It is the live primary price feed; `stream_prices_ibkr` and `poll_prices` are the secondary/fallback feeds in the same category. This is the live ingestion path, not a legacy one.

## Key Tables / Outputs

Events flow through `publish_price_events`, which writes:

- `price_quotes_raw` — per-provider raw trade/quote rows (provider `polygon_ws`).
- `price_quotes` — consolidated last/bid/ask/spread/volume per symbol.
- `prices` — last trade price, retained for downstream compatibility.

## Configuration Families

- `POLYGON_API_KEY` (required), `POLYGON_WS_ENDPOINT`, `POLYGON_WS_SUBSCRIBE_TRADES`, `POLYGON_WS_SUBSCRIBE_QUOTES`, and the `POLYGON_WS_RECONNECT_*` / `POLYGON_WS_MAX_RECONNECT_ATTEMPTS` knobs.
- `STREAM_PRICES_*` runtime knobs — `STREAM_PRICES_FLUSH_MS` (250), `STREAM_PRICES_HEARTBEAT_S` (2.0), `STREAM_PRICES_MIN_WRITE_INTERVAL_MS` (defaults to flush), `STREAM_PRICES_WS_DEAD_AFTER_MS` (8000), `STREAM_PRICES_WS_RESTART_COOLDOWN_S` (10.0), `STREAM_PRICES_PROVIDER_HEALTH_EVERY_S` (2.0), plus startup-silence and degraded-after thresholds.
- `JOB_LOCK_STALE_AFTER_S` (180) for the shared job lock.
