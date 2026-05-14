"""Redis-cached wrapper for ``position_reconcile_baseline``."""

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

POSITION_BASELINE_CODEC_VERSION = 1
POSITION_BASELINE_TTL_S = 300


def _normalize_positions(value: Any) -> dict[str, float]:
    raw = parse_json(value, {}) if not isinstance(value, dict) else value
    out: dict[str, float] = {}
    if not isinstance(raw, dict):
        return out
    for symbol, qty in raw.items():
        sym = str(symbol or "").upper().strip()
        if not sym:
            continue
        try:
            out[sym] = float(qty or 0.0)
        except Exception:
            out[sym] = 0.0
    return out


def _load_baseline(broker: str) -> dict[str, Any] | None:
    broker_key = str(broker or "").strip()
    con = None
    try:
        con = storage.connect(readonly=True)
        row = con.execute(
            "SELECT broker, ts_ms, positions_json FROM position_reconcile_baseline WHERE broker=?",
            (broker_key,),
        ).fetchone()
        if not row:
            return None
        return {
            "broker": str(row[0] or broker_key),
            "ts_ms": int(row[1] or 0),
            "positions": _normalize_positions(row[2]),
        }
    except Exception:
        return None
    finally:
        if con is not None:
            con.close()


def read_position_baseline(broker: str) -> dict[str, Any] | None:
    broker_key = str(broker or "").strip()
    key = keys.position_baseline(broker_key)

    def _loader() -> bytes | None:
        row = _load_baseline(broker_key)
        return codec.encode(row, version=POSITION_BASELINE_CODEC_VERSION) if row is not None else None

    raw = store.read(key, _loader, ttl_s=POSITION_BASELINE_TTL_S)
    if raw is None:
        return None
    try:
        data = codec.decode(raw, expected_version=POSITION_BASELINE_CODEC_VERSION)
    except codec.UnsupportedCacheVersion as exc:
        raw = reload_after_codec_version_mismatch(
            key,
            _loader,
            ttl_s=POSITION_BASELINE_TTL_S,
            wrapper=__name__,
            expected_version=POSITION_BASELINE_CODEC_VERSION,
            error=exc,
        )
        if raw is None:
            return None
        try:
            data = codec.decode(raw, expected_version=POSITION_BASELINE_CODEC_VERSION)
        except codec.CacheCodecError:
            store.invalidate(key)
            return _load_baseline(broker_key)
    except codec.CacheCodecError:
        store.invalidate(key)
        return _load_baseline(broker_key)
    return dict(data or {})


def read_positions(broker: str) -> dict[str, float] | None:
    row = read_position_baseline(broker)
    if row is None:
        return None
    return _normalize_positions(row.get("positions"))


def prime_position_baseline(row: dict[str, Any]) -> None:
    broker = str((row or {}).get("broker") or "").strip()
    if broker:
        store.prime(
            keys.position_baseline(broker),
            codec.encode(row, version=POSITION_BASELINE_CODEC_VERSION),
            ttl_s=POSITION_BASELINE_TTL_S,
        )


def _ensure_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS position_reconcile_baseline (
            broker TEXT PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            positions_json TEXT NOT NULL
        )
        """
    )


def set_position_baseline(
    broker: str,
    positions: dict[str, Any],
    *,
    ts_ms: int | None = None,
    con: Any | None = None,
) -> dict[str, Any]:
    broker_key = str(broker or "").strip()
    payload = {
        "broker": broker_key,
        "ts_ms": int(ts_ms or now_ms()),
        "positions": _normalize_positions(positions),
    }

    def _persist(db: Any) -> None:
        _ensure_schema(db)
        db.execute(
            """
            INSERT INTO position_reconcile_baseline(broker, ts_ms, positions_json)
            VALUES(?,?,?)
            ON CONFLICT(broker) DO UPDATE SET
              ts_ms=excluded.ts_ms,
              positions_json=excluded.positions_json
            """,
            (broker_key, int(payload["ts_ms"]), dumps_json(payload["positions"])),
        )

    if con is not None:
        _persist(con)
        after_commit_or_now(con, lambda: prime_position_baseline(payload))
    else:
        store.write_through(
            keys.position_baseline(broker_key),
            codec.encode(payload, version=POSITION_BASELINE_CODEC_VERSION),
            persist=_persist,
            ttl_s=POSITION_BASELINE_TTL_S,
        )
    return payload
