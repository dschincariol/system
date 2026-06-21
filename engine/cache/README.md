# Cache Subsystem

The `engine/cache/` package is the Redis hot-path cache for a small allowlist of frequently-read runtime tables (kill switch, execution mode/health, broker order state, position baselines, strategy allocations, and feature snapshots). It uses write-through semantics behind the Postgres-backed runtime storage facade and a circuit breaker that falls through to Postgres loaders when Redis is unavailable. The cache is an accelerator only — Postgres remains the source of truth, and every cached value can be rebuilt from a database loader. Consumers are the typed wrappers in `wrappers/`, which the execution, risk, and oversight subsystems call instead of touching Redis directly.

## Files

- [redis_pool.py](redis_pool.py)
  Process-wide singleton `redis.Redis` client factory. Resolves the URL from `TS_REDIS_URL`/`REDIS_URL` (injecting a secret password when configured), sizes the pool via `TS_REDIS_POOL_SIZE`, and routes the startup `ping` through the circuit so a ping or missing-dependency failure opens the circuit instead of aborting the process.
- [store.py](store.py)
  Write-through cache API: `read` (Redis-first, loader-populated on miss), `write_through`/`write_through_many` (Postgres commit first, then Redis update after commit), `prime`, and `invalidate`. All Redis calls go through `cache_circuit().call(...)`; failures degrade to Postgres rather than raising.
- [codec.py](codec.py)
  Canonical payload serialization. Wraps data in a versioned envelope (`{v, ts, data}`, `CURRENT_VERSION = 1`) using msgpack when available and JSON otherwise, and rejects unexpected versions via `UnsupportedCacheVersion` so stale-schema payloads are detected and reloaded.
- [keys.py](keys.py)
  Centralized Redis keyspace builder. Enforces an allowlist of seven hot-path tables (`HOT_PATH_TABLES`) and a configurable prefix (`TS_REDIS_KEY_PREFIX`, default `trading`); unknown tables raise rather than being cached.
- [circuit.py](circuit.py)
  Consecutive-failure `CircuitBreaker` with cooldown probing and the `cache_circuit()` singleton. Defaults are `failure_threshold=3` and `cooldown_s=3.0` (`TS_REDIS_CIRCUIT_FAILURES`/`TS_REDIS_CIRCUIT_COOLDOWN_S`); opening or recovering emits `CACHE_REDIS_UNAVAILABLE`/`CACHE_REDIS_RECOVERED` alerts.
- [wrappers/_common.py](wrappers/_common.py)
  Shared helpers for the typed wrappers: JSON parse/dump, `now_ms`, `after_commit_or_now` (defers cache priming until the supplied connection commits), and `reload_after_codec_version_mismatch` (invalidate, reload from the loader, and re-prime on a version mismatch).
- [wrappers/kill_switch.py](wrappers/kill_switch.py)
  Typed wrapper for `kill_switch_state` (short TTL, default 30s via `KILL_SWITCH_CACHE_TTL_S`). Fail-closed: an unreadable provider or stale/expired snapshot returns a global `provider_unavailable` block, and every write appends a hash-chained `kill_switch_audit` row.
- [wrappers/execution_mode.py](wrappers/execution_mode.py)
  Typed wrapper for the singleton `execution_mode` row (`paper`/`shadow`/`live`, plus an `armed` flag). Arming `live` triggers `assert_live_execution_arming_preflight`, and each change appends a hash-chained `execution_mode_audit` row.
- [wrappers/execution_health.py](wrappers/execution_health.py)
  Typed wrapper for the latest `execution_health_state` row (slippage/latency/routing/broker-failure metrics, 30s TTL).
- [wrappers/broker_order_state.py](wrappers/broker_order_state.py)
  Typed wrapper for `broker_order_state`, addressable by row id, source-order-id+symbol, or latest-by-symbol; a single write primes all applicable key forms via `write_through_many`.
- [wrappers/position_baseline.py](wrappers/position_baseline.py)
  Typed wrapper for per-broker `position_reconcile_baseline` snapshots used by reconciliation.
- [wrappers/strategy_allocations.py](wrappers/strategy_allocations.py)
  Typed wrapper for the latest `strategy_allocations` row per `window_days`.
- [wrappers/feature_snapshots.py](wrappers/feature_snapshots.py)
  Typed wrapper for the latest `model_feature_snapshots` row per symbol and feature-set tag.

## Safety Boundary

Postgres is the source of truth; Redis is a disposable accelerator. The two invariants are enforced consistently across `store.py` and the wrappers:

- **Write-through ordering.** `write_through`/`write_through_many` open a `storage.transaction()` and persist to Postgres *first*; only after the transaction commits is Redis updated. A failure building or setting the cache value never rolls back the committed Postgres write — it logs and `invalidate`s the key so the next read repopulates from the loader. When a wrapper already holds a connection, `after_commit_or_now` defers the Redis prime until that connection commits.
- **Circuit-breaker fallback to Postgres.** Every Redis operation is wrapped by `cache_circuit().call(...)`. After consecutive failures (default 3) the circuit opens; reads then skip Redis and fall through to the database loader, and the cache transparently re-probes after the cooldown. A missing `redis` dependency or a failed startup ping force-opens the circuit so the process keeps running in Postgres fall-through mode.

On a codec version mismatch or decode failure, wrappers invalidate the key and reload from the loader rather than serving stale or unparseable bytes; the kill-switch wrapper additionally fails closed (global block) when the provider cannot be read.

## Configuration Families

- `TS_REDIS_*` — URL, pool size, socket timeouts, key prefix, and circuit thresholds.
- `REDIS_URL` / `REDIS_PASSWORD_SECRET` — fallback URL and secret password source.
- `KILL_SWITCH_CACHE_TTL_S` — kill-switch snapshot freshness budget (clamped to `KILL_SWITCH_MAX_TTL_S`).
