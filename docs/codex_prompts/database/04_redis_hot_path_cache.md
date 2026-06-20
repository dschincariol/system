# Codex DB Prompt 04 — Redis Hot-Path Cache

You are working in a Python systematic trading system. The decision
pipeline reads a small set of state tables on **every** order intent:
the kill switch, execution mode, position baseline, current strategy
allocations, broker order state, the latest feature snapshot for the
symbol. Today those reads go to disk through the storage layer. Now
that the system is moving to Postgres on a single Linux server (prompt
01) with a thin storage layer (prompt 02) and a Timescale schema
(prompt 03), the next single biggest decision-pipeline win is to put
**Redis in front of those reads** with a **write-through** discipline
so the database remains the system of record but reads never touch
disk on the hot path.

Redis runs on the same host (Unix socket from prompt 01). Target
sub-millisecond reads.

## Linux-only note

This is **Linux-only application code** for development, staging, and
production hosts. The Redis client target comes from the
`TS_REDIS_URL` environment variable. Linux default:
`unix:///var/run/redis/trading.sock`. Read
`docs/codex_prompts/database/CROSS_PLATFORM.md` first.

## Goal

1. A `engine/cache/` subpackage hosting a small write-through cache
   API on top of Redis. Transport (Unix socket vs TCP) is determined
   solely by the URL scheme in `TS_REDIS_URL`.
2. Seven hot-path tables wrapped:
   - `kill_switch_state`
   - `execution_mode`
   - `execution_health_state`
   - `broker_order_state`
   - `position_reconcile_baseline`
   - `strategy_allocations`
   - latest `model_feature_snapshots` row per
     `(symbol, feature_group)`
3. **Write-through** semantics: every write hits Postgres first, then
   updates Redis in the same logical operation. Cache is the read
   source; Postgres is the system of record.
4. **Fail-open**: if Redis is unreachable or returns an error, the
   cache layer falls through to Postgres and emits a typed alert. The
   system stays available; the alert tells operators latency degraded.
5. **No stale reads on writer paths.** A writer that just wrote sees
   its own write through the cache.
6. Strict invariant: **the cache never holds anything that is not also
   in Postgres**. Cache loss is never data loss.

## Files to read first (read-only)

- `engine/runtime/storage.py` (post-prompt-02) — storage facade.
- `engine/strategy/portfolio.py` and `engine/strategy/portfolio_risk_gate.py`
  — primary readers of position / strategy / allocation state.
- `engine/execution/broker_router.py` — primary reader of
  `broker_order_state`, `execution_mode`, `kill_switch_state`,
  `execution_health_state`.
- `engine/execution/kill_switch.py` — kill-switch read/write surface.
- `engine/execution/execution_policy_engine.py` — execution mode
  read/write surface.
- `engine/execution/position_reconcile.py` — position baseline
  read/write surface.
- `engine/strategy/predictor.py` — reader of
  `model_feature_snapshots` (latest row per symbol).
- `engine/runtime/locks.py` (post-prompt-02) — for any cross-writer
  ordering needed.
- `engine/runtime/schema/table_classification.py` — to verify the
  seven tables here are correctly classified.

## Files to create

- `engine/cache/__init__.py`
- `engine/cache/redis_pool.py` — singleton `redis.Redis`
  client built from `TS_REDIS_URL` via `redis.Redis.from_url(...)`.
  The URL scheme (`unix://` or `redis://`) selects transport;
  `decode_responses=False`. Connection pool sized by env
  `TS_REDIS_POOL_SIZE` (default 16). Defaults computed by
  `engine/runtime/platform.py` (`unix:///var/run/redis/trading.sock`).
- `engine/cache/circuit.py` — small circuit breaker. After N
  consecutive failures, open for `cooldown_s`; reads fall through to
  Postgres; periodic probe attempts re-close. Emits alerts on
  open / close transitions.
- `engine/cache/codec.py` — serialization. `msgpack` for structured
  rows (faster than JSON, smaller payload). Single canonical encode /
  decode used everywhere.
- `engine/cache/keys.py` — **one** keyspace builder: every key is
  `trading:<table>:<id>` or `trading:<table>:<symbol>:<feature_group>`.
  Predictable, greppable, no key collisions.
- `engine/cache/store.py` — the write-through API:
  - `read(key) -> bytes | None` — Redis first, then loader on miss.
  - `write_through(key, value, *, persist: Callable[[Connection],
    None])` — opens a Postgres transaction, runs `persist`, on commit
    sets the cache, on rollback does nothing to cache.
  - `invalidate(key)` — drop a key (used when a writer outside our
    own write_through path mutates the row).
- `engine/cache/wrappers/__init__.py`
- `engine/cache/wrappers/kill_switch.py` — typed `read_kill_switch()`,
  `set_kill_switch(state, reason, actor)`. Reads from Redis; writes
  through to Postgres + Redis.
- `engine/cache/wrappers/execution_mode.py`
- `engine/cache/wrappers/execution_health.py`
- `engine/cache/wrappers/broker_order_state.py`
- `engine/cache/wrappers/position_baseline.py`
- `engine/cache/wrappers/strategy_allocations.py`
- `engine/cache/wrappers/feature_snapshots.py` — `latest(symbol,
  feature_group)`; key TTL 5 minutes (these get republished on every
  ingestion tick anyway, TTL is just a safety net).
- `tests/test_cache_redis_pool.py`
- `tests/test_cache_circuit.py`
- `tests/test_cache_codec.py`
- `tests/test_cache_write_through.py`
- `tests/test_cache_fail_open.py`
- `tests/test_cache_wrappers_integration.py`

## Files to modify

- `engine/execution/kill_switch.py` — call sites read via
  `cache.wrappers.kill_switch.read_kill_switch()`. Writes call
  `set_kill_switch(...)`.
- `engine/execution/execution_policy_engine.py` — same pattern for
  `execution_mode`.
- `engine/execution/broker_router.py` — read `kill_switch`,
  `execution_mode`, `broker_order_state` from wrappers.
- `engine/execution/position_reconcile.py` — same for
  `position_reconcile_baseline`.
- `engine/strategy/portfolio.py` and
  `engine/strategy/portfolio_risk_gate.py` — read
  `strategy_allocations` via wrapper.
- `engine/strategy/predictor.py` — `feature_snapshots.latest(...)`.

## Implementation plan

1. **Pool.** One process-wide `redis.Redis` instance backed by a
   `redis.ConnectionPool` over the Unix socket. Health-check by
   `PING` at startup; do **not** fail process startup if Redis is
   down — open the circuit, log loudly, proceed in fall-through mode.
2. **Codec.** `msgpack` with a small envelope:
   `{"v": 1, "ts": <unix_ms>, "data": <row>}`. Version byte enables
   future schema changes without re-population.
3. **Keys.** Every wrapper imports its key builder from
   `engine/cache/keys.py`. Conventions enforced by lint test that
   greps for `r.set(` / `r.get(` outside `keys.py`.
4. **Write-through.** The single allowed write path:
   ```python
   with storage.transaction() as tx:
       persist(tx)            # writes to Postgres
   # transaction committed; now update cache
   redis_pool().set(key, encode(value))
   ```
   If the cache `set` fails, log a typed warning and **invalidate**
   the key so the next read re-loads from Postgres. Never leave the
   cache holding a value the database does not.
5. **Read miss.** Read from Postgres, populate cache with
   `SET key value EX 300`, return.
6. **Fail-open.** Every cache call is wrapped:
   ```python
   try:
       v = circuit.call(redis_pool().get, key)
   except CacheUnavailable:
       v = None
   ```
   `None` means "miss — go to Postgres." Alerts fire on circuit
   transitions, not on every miss.
7. **Self-write visibility.** Because write-through updates the cache
   inside the same logical write, a subsequent read inside the same
   process sees its own write immediately. Cross-process writers see
   it as soon as Redis returns from the `SET`.
8. **No background refresh thread.** TTL handles drift; on-write
   cache update handles correctness. Avoid the operational complexity
   of a background refresher.

## Performance targets

- `read_kill_switch()` p50 **< 0.3 ms**, p99 **< 1 ms** on the
  canonical host.
- `feature_snapshots.latest(symbol, fg)` p50 **< 0.5 ms** on a
  cache hit, **< 5 ms** on miss with Postgres warm.
- Write-through write of a state row: **< 5 ms** end-to-end including
  the cache update.
- Circuit-open detection within **3 s** of Redis going down; circuit
  re-close within **5 s** of Redis recovering.

## Acceptance criteria

- [ ] All seven hot-path tables are read through their respective
      wrapper modules in `engine/cache/wrappers/`.
- [ ] No call site outside `engine/cache/` imports `redis` directly
      (lint-tested).
- [ ] Write-through never leaves the cache holding data that isn't in
      Postgres. A test forces a cache `SET` failure after a
      successful Postgres commit and asserts the key is invalidated.
- [ ] Stopping the Redis service in the test harness does not break
      reads or writes; the system continues with Postgres-only,
      latency degrades, and an alert is emitted exactly once per
      transition.
- [ ] Restarting Redis re-closes the circuit within 5 s and reads
      flow back through the cache.
- [ ] Cache encoding is versioned; reading a v1 payload with v2 code
      raises a typed error rather than corrupting state.
- [ ] No call to `r.set(` or `r.get(` in the codebase outside
      `engine/cache/store.py` (enforced by a guard test).
- [ ] All tests pass on Linux with Unix-socket transport when
      `TS_REDIS_URL` is set appropriately.
- [ ] Switching `TS_REDIS_URL` from `unix://...` to `redis://...`
      requires zero source changes; the same Python code uses
      whichever transport the URL specifies.

## Test plan

- `tests/test_cache_redis_pool.py` — pool initializes; `PING` works;
  pool size is honored.
- `tests/test_cache_circuit.py` — N failures opens the circuit; the
  cooldown elapses; a probe re-closes.
- `tests/test_cache_codec.py` — round-trip; version-byte mismatch
  raises.
- `tests/test_cache_write_through.py` — write hits Postgres first
  then cache; rollback does not touch cache; cache `SET` failure
  invalidates.
- `tests/test_cache_fail_open.py` — Redis stopped: reads fall
  through; alert emitted once.
- `tests/test_cache_wrappers_integration.py` — exercise each of the
  seven wrappers end-to-end against a real Redis + Postgres on the
  test host.

Run: `pytest -q tests/test_cache_redis_pool.py tests/test_cache_circuit.py
tests/test_cache_codec.py tests/test_cache_write_through.py
tests/test_cache_fail_open.py tests/test_cache_wrappers_integration.py`

## Out of scope

- Caching of time-series rows (prices, options chains). Those are
  always range-queried; cache hit rate would be low. Use Postgres
  with the `(symbol, ts DESC)` index.
- Distributed Redis (Cluster, Sentinel HA). One Redis on the same
  host with AOF, supervised by systemd, is sufficient for this
  deployment.
- Sub-second cache invalidation across processes by means other than
  the write-through update. Within the design, every writer goes
  through `write_through(...)` and updates the cache itself.
- Caching of audit reads. Audit lookups are not on the hot path.
