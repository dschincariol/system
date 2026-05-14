"""Redis-cached wrapper for latest ``model_feature_snapshots`` rows."""

from __future__ import annotations

from typing import Any

from engine.cache import codec, keys, store
from engine.cache.wrappers._common import after_commit_or_now, dumps_json, parse_json, reload_after_codec_version_mismatch
from engine.runtime import storage

FEATURE_SNAPSHOT_CODEC_VERSION = 1
FEATURE_SNAPSHOT_TTL_S = 300


def _row_to_snapshot(row: Any) -> dict[str, Any]:
    return {
        "symbol": str(row[0] or "").upper().strip(),
        "ts_ms": int(row[1] or 0),
        "feature_set_tag": str(row[2] or ""),
        "snapshot_version": int(row[3] or 1),
        "feature_ids": parse_json(row[4], []),
        "vector": parse_json(row[5], []),
        "features": parse_json(row[6], {}),
        "source_timestamps": parse_json(row[7], {}),
        "availability": parse_json(row[8], {}),
        "created_ts_ms": int(row[9] or 0),
    }


def _load_latest(symbol: str, feature_group: str) -> dict[str, Any] | None:
    symbol_key = str(symbol or "").upper().strip()
    group = str(feature_group or "").strip()
    con = None
    try:
        con = storage.connect(readonly=True)
        row = con.execute(
            """
            SELECT
              symbol,
              ts_ms,
              feature_set_tag,
              snapshot_version,
              feature_ids_json,
              vector_json,
              features_json,
              source_timestamps_json,
              availability_json,
              created_ts_ms
            FROM model_feature_snapshots
            WHERE symbol = ?
              AND feature_set_tag = ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (symbol_key, group),
        ).fetchone()
        return _row_to_snapshot(row) if row else None
    except Exception:
        return None
    finally:
        if con is not None:
            con.close()


def latest(symbol: str, feature_group: str) -> dict[str, Any] | None:
    symbol_key = str(symbol or "").upper().strip()
    group = str(feature_group or "").strip()
    key = keys.feature_snapshot(symbol_key, group)

    def _loader() -> bytes | None:
        row = _load_latest(symbol_key, group)
        return codec.encode(row, version=FEATURE_SNAPSHOT_CODEC_VERSION) if row is not None else None

    raw = store.read(key, _loader, ttl_s=FEATURE_SNAPSHOT_TTL_S)
    if raw is None:
        return None
    try:
        data = codec.decode(raw, expected_version=FEATURE_SNAPSHOT_CODEC_VERSION)
    except codec.UnsupportedCacheVersion as exc:
        raw = reload_after_codec_version_mismatch(
            key,
            _loader,
            ttl_s=FEATURE_SNAPSHOT_TTL_S,
            wrapper=__name__,
            expected_version=FEATURE_SNAPSHOT_CODEC_VERSION,
            error=exc,
        )
        if raw is None:
            return None
        try:
            data = codec.decode(raw, expected_version=FEATURE_SNAPSHOT_CODEC_VERSION)
        except codec.CacheCodecError:
            store.invalidate(key)
            return _load_latest(symbol_key, group)
    except codec.CacheCodecError:
        store.invalidate(key)
        return _load_latest(symbol_key, group)
    return dict(data or {})


def prime_feature_snapshot(snapshot: dict[str, Any]) -> None:
    symbol = str((snapshot or {}).get("symbol") or "").upper().strip()
    group = str((snapshot or {}).get("feature_set_tag") or "").strip()
    if symbol and group:
        store.prime(
            keys.feature_snapshot(symbol, group),
            codec.encode(snapshot, version=FEATURE_SNAPSHOT_CODEC_VERSION),
            ttl_s=FEATURE_SNAPSHOT_TTL_S,
        )


def _ensure_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_feature_snapshots (
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          feature_set_tag TEXT NOT NULL,
          snapshot_version INTEGER NOT NULL,
          feature_ids_json TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          features_json TEXT NOT NULL,
          source_timestamps_json TEXT NOT NULL,
          availability_json TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL,
          PRIMARY KEY(symbol, ts_ms, feature_set_tag)
        )
        """
    )


def store_latest(snapshot: dict[str, Any], *, con: Any | None = None) -> dict[str, Any]:
    payload = dict(snapshot or {})
    payload["symbol"] = str(payload.get("symbol") or "").upper().strip()
    payload["feature_set_tag"] = str(payload.get("feature_set_tag") or "").strip()
    if not payload["symbol"] or not payload["feature_set_tag"]:
        raise ValueError("feature_snapshot_symbol_and_group_required")

    def _persist(db: Any) -> None:
        _ensure_schema(db)
        db.execute(
            """
            INSERT INTO model_feature_snapshots(
              symbol, ts_ms, feature_set_tag, snapshot_version,
              feature_ids_json, vector_json, features_json,
              source_timestamps_json, availability_json, created_ts_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, ts_ms, feature_set_tag) DO UPDATE SET
              snapshot_version=excluded.snapshot_version,
              feature_ids_json=excluded.feature_ids_json,
              vector_json=excluded.vector_json,
              features_json=excluded.features_json,
              source_timestamps_json=excluded.source_timestamps_json,
              availability_json=excluded.availability_json,
              created_ts_ms=excluded.created_ts_ms
            """,
            (
                payload["symbol"],
                int(payload.get("ts_ms") or 0),
                payload["feature_set_tag"],
                int(payload.get("snapshot_version") or 1),
                dumps_json(list(payload.get("feature_ids") or [])),
                dumps_json(list(payload.get("vector") or [])),
                dumps_json(dict(payload.get("features") or {})),
                dumps_json(dict(payload.get("source_timestamps") or {})),
                dumps_json(dict(payload.get("availability") or {})),
                int(payload.get("created_ts_ms") or 0),
            ),
        )

    if con is not None:
        _persist(con)
        after_commit_or_now(con, lambda: prime_feature_snapshot(payload))
    else:
        store.write_through(
            keys.feature_snapshot(payload["symbol"], payload["feature_set_tag"]),
            codec.encode(payload, version=FEATURE_SNAPSHOT_CODEC_VERSION),
            persist=_persist,
            ttl_s=FEATURE_SNAPSHOT_TTL_S,
        )
    return payload
