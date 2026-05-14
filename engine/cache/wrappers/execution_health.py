"""Redis-cached wrapper for ``execution_health_state``."""

from __future__ import annotations

from typing import Any

from engine.cache import codec, keys, store
from engine.cache.wrappers._common import (
    after_commit_or_now,
    dumps_json,
    now_ms,
    parse_json,
    reload_after_codec_version_mismatch,
)
from engine.runtime import storage

EXECUTION_HEALTH_CODEC_VERSION = 1
EXECUTION_HEALTH_TTL_S = 30


def _row_to_health(row: Any) -> dict[str, Any]:
    extra = parse_json(row[11], {})
    out = {
        "ts_ms": int(row[0] or 0),
        "state": str(row[1] or "unknown"),
        "score": float(row[2] or 0.0),
        "n": int(row[3] or 0),
        "mean_slippage_bps": float(row[4] or 0.0),
        "p95_slippage_bps": float(row[5] or 0.0),
        "mean_latency_ms": float(row[6] or 0.0),
        "p95_latency_ms": float(row[7] or 0.0),
        "routing_failures": int(row[8] or 0),
        "open_due": int(row[9] or 0),
        "broker_failures": int(row[10] or 0),
    }
    if isinstance(extra, dict):
        out.update(extra)
    return out


def _load_latest() -> dict[str, Any] | None:
    con = None
    try:
        con = storage.connect(readonly=True)
        row = con.execute(
            """
            SELECT ts_ms, state, score, n, mean_slippage_bps, p95_slippage_bps,
                   mean_latency_ms, p95_latency_ms, routing_error_rate, open_due,
                   broker_failures, extra_json
            FROM execution_health_state
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
        return _row_to_health(row) if row else None
    except Exception:
        return None
    finally:
        if con is not None:
            con.close()


def read_execution_health() -> dict[str, Any] | None:
    key = keys.execution_health()

    def _loader() -> bytes | None:
        row = _load_latest()
        return codec.encode(row, version=EXECUTION_HEALTH_CODEC_VERSION) if row is not None else None

    raw = store.read(key, _loader, ttl_s=EXECUTION_HEALTH_TTL_S)
    if raw is None:
        return None
    try:
        decoded = codec.decode(raw, expected_version=EXECUTION_HEALTH_CODEC_VERSION)
    except codec.UnsupportedCacheVersion as exc:
        raw = reload_after_codec_version_mismatch(
            key,
            _loader,
            ttl_s=EXECUTION_HEALTH_TTL_S,
            wrapper=__name__,
            expected_version=EXECUTION_HEALTH_CODEC_VERSION,
            error=exc,
        )
        if raw is None:
            return None
        try:
            decoded = codec.decode(raw, expected_version=EXECUTION_HEALTH_CODEC_VERSION)
        except codec.CacheCodecError:
            store.invalidate(key)
            return _load_latest()
    except codec.CacheCodecError:
        store.invalidate(key)
        return _load_latest()
    return dict(decoded or {})


def prime_execution_health(row: dict[str, Any] | None = None) -> None:
    payload = row if row is not None else _load_latest()
    if payload is not None:
        store.prime(
            keys.execution_health(),
            codec.encode(payload, version=EXECUTION_HEALTH_CODEC_VERSION),
            ttl_s=EXECUTION_HEALTH_TTL_S,
        )


def _ensure_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_health_state (
          ts_ms INTEGER NOT NULL,
          state TEXT NOT NULL,
          score REAL,
          n INTEGER,
          mean_slippage_bps REAL,
          p95_slippage_bps REAL,
          mean_latency_ms REAL,
          p95_latency_ms REAL,
          routing_error_rate REAL,
          open_due INTEGER,
          broker_failures INTEGER,
          extra_json TEXT,
          PRIMARY KEY (ts_ms)
        )
        """
    )


def write_execution_health(row: dict[str, Any], *, con: Any | None = None) -> dict[str, Any]:
    payload = dict(row or {})
    ts_ms = int(payload.get("ts_ms") or now_ms())
    payload["ts_ms"] = int(ts_ms)
    extra_json = dumps_json(payload)

    def _persist(db: Any) -> None:
        _ensure_schema(db)
        db.execute(
            """
            INSERT INTO execution_health_state(
              ts_ms, state, score, n, mean_slippage_bps, p95_slippage_bps,
              mean_latency_ms, p95_latency_ms, routing_error_rate, open_due,
              broker_failures, extra_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(ts_ms) DO UPDATE SET
              state=excluded.state,
              score=excluded.score,
              n=excluded.n,
              mean_slippage_bps=excluded.mean_slippage_bps,
              p95_slippage_bps=excluded.p95_slippage_bps,
              mean_latency_ms=excluded.mean_latency_ms,
              p95_latency_ms=excluded.p95_latency_ms,
              routing_error_rate=excluded.routing_error_rate,
              open_due=excluded.open_due,
              broker_failures=excluded.broker_failures,
              extra_json=excluded.extra_json
            """,
            (
                int(ts_ms),
                str(payload.get("state") or "unknown"),
                float(payload.get("score") or 0.0),
                int(payload.get("n") or 0),
                float(payload.get("mean_slippage_bps") or 0.0),
                float(payload.get("p95_slippage_bps") or 0.0),
                float(payload.get("mean_latency_ms") or 0.0),
                float(payload.get("p95_latency_ms") or 0.0),
                float(payload.get("routing_failures") or payload.get("routing_error_rate") or 0.0),
                int(payload.get("open_due") or 0),
                int(payload.get("broker_failures") or 0),
                extra_json,
            ),
        )

    if con is not None:
        _persist(con)
        after_commit_or_now(con, lambda: prime_execution_health(payload))
    else:
        store.write_through(
            keys.execution_health(),
            codec.encode(payload, version=EXECUTION_HEALTH_CODEC_VERSION),
            persist=_persist,
            ttl_s=EXECUTION_HEALTH_TTL_S,
        )
    return payload
