"""Shadow-only temporal heterogeneous graph challenger.

The implementation deliberately avoids heavyweight graph dependencies.  It
materializes PIT-safe graph samples from existing feature snapshots and graph
relationships, then trains a small CPU ridge baseline over target-node features
plus typed relational message aggregates.
"""

from __future__ import annotations

import json
import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import numpy as np

from engine.artifacts.refs import ArtifactRef
from engine.artifacts.store import LocalArtifactStore
from engine.strategy.ensemble import oos_store
from engine.strategy.graph_relational import (
    GRAPH_RELATIONSHIP_TYPES,
    GRAPH_RELATIONAL_EDGE_SCHEMA_VERSION,
    GRAPH_RELATIONAL_FEATURE_IDS,
    GRAPH_RELATIONAL_GRAPH_ID,
    GRAPH_RELATIONAL_GROUP,
    GRAPH_RELATIONAL_NODE_SCHEMA_VERSION,
    GRAPH_RELATIONAL_PREFIX,
    GRAPH_RELATIONAL_SNAPSHOT_VERSION,
    build_graph_relational_snapshot,
    ensure_graph_relational_schema,
    graph_metadata_from_snapshot,
    graph_train_serve_parity,
    load_graph_relational_snapshot,
    store_graph_relational_snapshots,
)
from engine.strategy.model_competition.repository import CompetitionRepository


GRAPH_CHALLENGER_MODEL_NAME = "graph_relational_challenger_v1"
GRAPH_CHALLENGER_MODEL_FAMILY = "graph_relational_challenger"
GRAPH_CHALLENGER_SCORE_SOURCE = "model_oos_predictions"
GRAPH_CHALLENGER_DEFAULT_FEATURE_IDS = (
    "price.last",
    "price.log_ret_5m",
    "price.log_ret_1h",
    "price.rv_20",
    "price.atr_pct_14",
)
GRAPH_CHALLENGER_EDGE_FIELDS = (
    "source_symbol",
    "target_symbol",
    "edge_type",
    "weight",
    "source_ts_ms",
    "availability_ts_ms",
    "source",
)

LOG = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str, allow_nan=False)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if value in (None, "", b"", bytearray()):
        return []
    try:
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _stable_hash(value: Any) -> str:
    import hashlib

    return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def _row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    if hasattr(row, "keys"):
        try:
            return row[key]
        except Exception:
            return default
    try:
        return row[index]
    except Exception:
        return default


def _table_exists(con: Any, table: str) -> bool:
    name = str(table or "").strip()
    if not name or not name.replace("_", "").isalnum():
        return False
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _commit_if_possible(con: Any) -> None:
    commit = getattr(con, "commit", None)
    if callable(commit):
        commit()


def _feature_group_for_id(feature_id: str) -> str:
    fid = str(feature_id or "")
    if fid.startswith(GRAPH_RELATIONAL_PREFIX):
        return GRAPH_RELATIONAL_GROUP
    if fid.startswith("availability."):
        return fid.split(".", 1)[1]
    return fid.split(".", 1)[0] if "." in fid else "custom"


def _max_ts(value: Any, *, prefer_availability: bool = False) -> int:
    if isinstance(value, Mapping):
        best = 0
        for key, item in dict(value).items():
            key_text = str(key or "").lower()
            if prefer_availability:
                wanted = "availability" in key_text or "snapshot" in key_text or "asof" in key_text
            else:
                wanted = key_text.endswith("ts_ms") or key_text.endswith("_ts")
            if wanted:
                best = max(best, _safe_int(item, 0))
            if isinstance(item, Mapping):
                best = max(best, _max_ts(item, prefer_availability=prefer_availability))
        return int(best)
    if isinstance(value, list):
        return max([_max_ts(item, prefer_availability=prefer_availability) for item in value] or [0])
    return 0


def _feature_timestamp_metadata(
    *,
    feature_id: str,
    source_timestamps: Mapping[str, Any],
    fallback_ts_ms: int,
) -> dict[str, Any]:
    group = _feature_group_for_id(str(feature_id))
    group_meta = dict(source_timestamps.get(group) or {})
    source_ts = _max_ts(group_meta, prefer_availability=False) or int(fallback_ts_ms)
    availability_ts = _max_ts(group_meta, prefer_availability=True) or source_ts
    return {
        "feature_id": str(feature_id),
        "feature_group": str(group),
        "source_ts_ms": int(source_ts),
        "availability_ts_ms": int(availability_ts),
    }


def _node_feature_schema(feature_ids: Sequence[str], *, window_count: int, window_stride_ms: int, feature_set_tag: str) -> dict[str, Any]:
    return {
        "schema_version": int(GRAPH_RELATIONAL_NODE_SCHEMA_VERSION),
        "feature_set_tag": str(feature_set_tag or ""),
        "feature_ids": [str(fid) for fid in feature_ids],
        "feature_count": int(len(list(feature_ids or []))),
        "window_count": int(window_count),
        "window_stride_ms": int(window_stride_ms),
        "source": "model_feature_snapshots",
    }


def _edge_feature_schema(edge_types: Sequence[str]) -> dict[str, Any]:
    return {
        "schema_version": int(GRAPH_RELATIONAL_EDGE_SCHEMA_VERSION),
        "edge_types": sorted(str(edge_type) for edge_type in edge_types if str(edge_type or "").strip()),
        "fields": list(GRAPH_CHALLENGER_EDGE_FIELDS),
        "weight_semantics": "relationship_specific_strength",
        "source": "graph_relational_snapshots_or_relationship_sources",
    }


def _ensure_marketplace_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_marketplace_scores (
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_name TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s BIGINT NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          stage TEXT NOT NULL DEFAULT 'challenger',
          score DOUBLE PRECISION NOT NULL DEFAULT 0,
          trades BIGINT NOT NULL DEFAULT 0,
          wins BIGINT NOT NULL DEFAULT 0,
          losses BIGINT NOT NULL DEFAULT 0,
          gross_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
          net_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
          avg_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
          last_signal_ts_ms BIGINT,
          updated_ts_ms BIGINT NOT NULL,
          meta_json JSONB,
          PRIMARY KEY (model_id, model_name, symbol, horizon_s, regime)
        )
        """
    )


def ensure_graph_challenger_schema(con: Any) -> None:
    ensure_graph_relational_schema(con)
    oos_store.ensure_schema(con)
    _ensure_marketplace_schema(con)


def _load_existing_model_feature_snapshot(
    con: Any,
    *,
    symbol: str,
    ts_ms: int,
    feature_set_tag: str,
) -> dict[str, Any] | None:
    try:
        from engine.strategy.model_feature_snapshots import load_model_feature_snapshot

        loaded = load_model_feature_snapshot(
            symbol=str(symbol),
            ts_ms=int(ts_ms),
            feature_set_tag=str(feature_set_tag),
            exact=False,
            con=con,
        )
        if loaded:
            return dict(loaded)
    except Exception:
        LOG.debug("Falling back to direct model_feature_snapshots query.", exc_info=True)
    if not _table_exists(con, "model_feature_snapshots"):
        return None
    try:
        row = con.execute(
            """
            SELECT symbol, ts_ms, feature_set_tag, snapshot_version, feature_ids_json,
                   vector_json, features_json, source_timestamps_json, availability_json, created_ts_ms
            FROM model_feature_snapshots
            WHERE symbol=?
              AND ts_ms <= ?
            ORDER BY ts_ms DESC, created_ts_ms DESC
            LIMIT 1
            """,
            (_symbol(symbol), int(ts_ms)),
        ).fetchone()
    except Exception:
        return None
    if not row:
        return None
    return {
        "symbol": str(_row_get(row, "symbol", 0) or ""),
        "ts_ms": _safe_int(_row_get(row, "ts_ms", 1), 0),
        "feature_set_tag": str(_row_get(row, "feature_set_tag", 2) or ""),
        "snapshot_version": _safe_int(_row_get(row, "snapshot_version", 3), 0),
        "feature_ids": [str(fid) for fid in _json_list(_row_get(row, "feature_ids_json", 4))],
        "vector": [_safe_float(value, 0.0) for value in _json_list(_row_get(row, "vector_json", 5))],
        "features": _json_obj(_row_get(row, "features_json", 6)),
        "source_timestamps": _json_obj(_row_get(row, "source_timestamps_json", 7)),
        "availability": _json_obj(_row_get(row, "availability_json", 8)),
        "created_ts_ms": _safe_int(_row_get(row, "created_ts_ms", 9), 0),
    }


def _node_window(
    con: Any,
    *,
    symbol: str,
    anchor_ts_ms: int,
    window_ts_ms: int,
    feature_ids: Sequence[str],
    feature_set_tag: str,
) -> dict[str, Any]:
    snapshot = _load_existing_model_feature_snapshot(
        con,
        symbol=str(symbol),
        ts_ms=int(window_ts_ms),
        feature_set_tag=str(feature_set_tag),
    )
    feature_values = {str(fid): 0.0 for fid in feature_ids}
    feature_metadata: dict[str, dict[str, Any]] = {}
    availability = False
    reasons: list[str] = []
    source_timestamps: dict[str, Any] = {}
    snapshot_ts_ms = 0
    if snapshot:
        source_timestamps = dict(snapshot.get("source_timestamps") or {})
        snapshot_ts_ms = _safe_int(snapshot.get("ts_ms"), 0)
        raw_features = dict(snapshot.get("features") or {})
        availability = True
        for fid in feature_ids:
            meta = _feature_timestamp_metadata(
                feature_id=str(fid),
                source_timestamps=source_timestamps,
                fallback_ts_ms=snapshot_ts_ms or int(window_ts_ms),
            )
            future = _safe_int(meta.get("source_ts_ms"), 0) > int(anchor_ts_ms) or _safe_int(
                meta.get("availability_ts_ms"),
                0,
            ) > int(anchor_ts_ms)
            if future:
                meta["available"] = False
                meta["excluded_reason"] = "feature_after_anchor"
                reasons.append(f"{fid}:feature_after_anchor")
                feature_values[str(fid)] = 0.0
                availability = False
            else:
                meta["available"] = True
                feature_values[str(fid)] = _safe_float(raw_features.get(str(fid)), 0.0)
            feature_metadata[str(fid)] = meta
    else:
        availability = False
        reasons.append("feature_snapshot_missing")
        for fid in feature_ids:
            feature_metadata[str(fid)] = {
                "feature_id": str(fid),
                "feature_group": _feature_group_for_id(str(fid)),
                "source_ts_ms": None,
                "availability_ts_ms": None,
                "available": False,
                "excluded_reason": "feature_snapshot_missing",
            }
    return {
        "ts_ms": int(window_ts_ms),
        "snapshot_ts_ms": int(snapshot_ts_ms),
        "available": bool(availability),
        "reason_codes": sorted(set(reasons)),
        "features": dict(feature_values),
        "vector": [float(feature_values[str(fid)]) for fid in feature_ids],
        "feature_metadata": feature_metadata,
        "source_timestamps": source_timestamps,
    }


def _normalize_edge(edge: Mapping[str, Any], *, anchor_ts_ms: int) -> dict[str, Any] | None:
    rel = str(edge.get("relationship_type") or edge.get("edge_type") or "").strip()
    if rel not in GRAPH_RELATIONSHIP_TYPES:
        return None
    source_symbol = _symbol(edge.get("source_symbol"))
    target_symbol = _symbol(edge.get("target_symbol"))
    if not source_symbol or not target_symbol or source_symbol == target_symbol:
        return None
    source_ts = _safe_int(edge.get("source_ts_ms"), 0)
    availability_ts = _safe_int(edge.get("availability_ts_ms"), 0)
    if source_ts > int(anchor_ts_ms) or availability_ts <= 0 or availability_ts > int(anchor_ts_ms):
        return None
    return {
        "source_symbol": str(source_symbol),
        "target_symbol": str(target_symbol),
        "edge_type": str(rel),
        "weight": float(_safe_float(edge.get("weight"), 1.0)),
        "source_ts_ms": int(source_ts),
        "availability_ts_ms": int(availability_ts),
        "source": str(edge.get("source") or ""),
        "metadata": dict(edge.get("metadata") or edge.get("meta") or {}),
    }


def _load_graph_snapshot_for_symbol(
    con: Any,
    *,
    symbol: str,
    ts_ms: int,
    peer_symbols: Sequence[str],
) -> tuple[dict[str, Any] | None, bool]:
    snapshot = load_graph_relational_snapshot(symbol=str(symbol), ts_ms=int(ts_ms), con=con)
    if snapshot:
        return dict(snapshot), False
    snapshot = build_graph_relational_snapshot(
        symbol=str(symbol),
        ts_ms=int(ts_ms),
        feature_ids=list(GRAPH_RELATIONAL_FEATURE_IDS),
        peer_symbols=list(peer_symbols),
        con=con,
    )
    return dict(snapshot), True


def _target_from_triple_barrier(con: Any, *, symbol: str, ts_ms: int, horizon_s: int, asof_ts_ms: int | None) -> dict[str, Any] | None:
    if not _table_exists(con, "triple_barrier_labels"):
        return None
    where = "symbol=? AND horizon_s=? AND ts_ms=?"
    params: list[Any] = [_symbol(symbol), int(horizon_s), int(ts_ms)]
    if asof_ts_ms is not None:
        where += " AND COALESCE(vertical_ts_ms, exit_ts_ms, ts_ms) <= ?"
        params.append(int(asof_ts_ms))
    try:
        row = con.execute(
            f"""
            SELECT realized_ret, label, vertical_ts_ms, exit_ts_ms
            FROM triple_barrier_labels
            WHERE {where}
            ORDER BY created_ts_ms DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return None
    return {
        "value": _safe_float(_row_get(row, "realized_ret", 0), 0.0),
        "label": _safe_int(_row_get(row, "label", 1), 0),
        "horizon_s": int(horizon_s),
        "source": "triple_barrier_labels",
        "target_ts_ms": _safe_int(_row_get(row, "vertical_ts_ms", 2), int(ts_ms) + int(horizon_s) * 1000),
        "availability_ts_ms": _safe_int(_row_get(row, "exit_ts_ms", 3), 0) or None,
    }


def _target_from_labels(con: Any, *, symbol: str, ts_ms: int, horizon_s: int, asof_ts_ms: int | None) -> dict[str, Any] | None:
    if not _table_exists(con, "labels"):
        return None
    joins = ""
    event_ts_expr = "l.created_at_ms"
    if _table_exists(con, "events"):
        joins = "LEFT JOIN events e ON e.id = l.event_id"
        event_ts_expr = "COALESCE(e.ts_ms, l.created_at_ms)"
    where = f"UPPER(l.symbol)=? AND l.horizon_s=? AND {event_ts_expr}=?"
    params: list[Any] = [_symbol(symbol), int(horizon_s), int(ts_ms)]
    if asof_ts_ms is not None:
        where += " AND COALESCE(l.created_at_ms, 0) <= ?"
        params.append(int(asof_ts_ms))
    try:
        row = con.execute(
            f"""
            SELECT l.realized_ret, l.impact_z, l.created_at_ms
            FROM labels l
            {joins}
            WHERE {where}
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return None
    value = _safe_float(_row_get(row, "impact_z", 1), _safe_float(_row_get(row, "realized_ret", 0), 0.0))
    return {
        "value": float(value),
        "horizon_s": int(horizon_s),
        "source": "labels",
        "target_ts_ms": int(ts_ms) + int(horizon_s) * 1000,
        "availability_ts_ms": _safe_int(_row_get(row, "created_at_ms", 2), 0) or None,
    }


def _price_at_or_before(con: Any, *, symbol: str, ts_ms: int) -> tuple[int, float] | None:
    if not _table_exists(con, "prices"):
        return None
    try:
        row = con.execute(
            """
            SELECT ts_ms, COALESCE(price, px) AS value
            FROM prices
            WHERE symbol=?
              AND ts_ms <= ?
              AND COALESCE(price, px) IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (_symbol(symbol), int(ts_ms)),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return None
    price = _safe_float(_row_get(row, "value", 1), math.nan)
    if not math.isfinite(price) or price <= 0.0:
        return None
    return _safe_int(_row_get(row, "ts_ms", 0), 0), float(price)


def _target_from_prices(con: Any, *, symbol: str, ts_ms: int, horizon_s: int, asof_ts_ms: int | None) -> dict[str, Any] | None:
    target_ts = int(ts_ms) + int(horizon_s) * 1000
    if asof_ts_ms is not None and target_ts > int(asof_ts_ms):
        return None
    entry = _price_at_or_before(con, symbol=str(symbol), ts_ms=int(ts_ms))
    exit_ = _price_at_or_before(con, symbol=str(symbol), ts_ms=int(target_ts))
    if entry is None or exit_ is None:
        return None
    entry_ts, entry_px = entry
    exit_ts, exit_px = exit_
    if exit_ts < int(ts_ms):
        return None
    try:
        ret = math.log(float(exit_px) / float(entry_px))
    except Exception:
        return None
    return {
        "value": float(ret),
        "horizon_s": int(horizon_s),
        "source": "prices_forward_return",
        "entry_ts_ms": int(entry_ts),
        "target_ts_ms": int(exit_ts),
        "availability_ts_ms": int(exit_ts),
    }


def _target_for_sample(con: Any, *, symbol: str, ts_ms: int, horizon_s: int, asof_ts_ms: int | None) -> dict[str, Any] | None:
    return (
        _target_from_triple_barrier(con, symbol=symbol, ts_ms=ts_ms, horizon_s=horizon_s, asof_ts_ms=asof_ts_ms)
        or _target_from_labels(con, symbol=symbol, ts_ms=ts_ms, horizon_s=horizon_s, asof_ts_ms=asof_ts_ms)
        or _target_from_prices(con, symbol=symbol, ts_ms=ts_ms, horizon_s=horizon_s, asof_ts_ms=asof_ts_ms)
    )


def _sample_timestamps_from_store(
    con: Any,
    *,
    symbols: Sequence[str],
    start_ts_ms: int | None,
    end_ts_ms: int | None,
    max_samples: int,
) -> list[int]:
    symbols_key = sorted(_symbol(sym) for sym in symbols if _symbol(sym))
    params: list[Any] = []
    where: list[str] = []
    if symbols_key:
        where.append("symbol IN (" + ",".join("?" for _ in symbols_key) + ")")
        params.extend(symbols_key)
    if start_ts_ms is not None:
        where.append("ts_ms >= ?")
        params.append(int(start_ts_ms))
    if end_ts_ms is not None:
        where.append("ts_ms <= ?")
        params.append(int(end_ts_ms))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    if _table_exists(con, "model_feature_snapshots"):
        try:
            rows = con.execute(
                f"""
                SELECT DISTINCT ts_ms
                FROM model_feature_snapshots
                {where_sql}
                ORDER BY ts_ms ASC
                LIMIT ?
                """,
                tuple(params + [int(max(1, max_samples))]),
            ).fetchall()
            out = [_safe_int(_row_get(row, "ts_ms", 0), 0) for row in rows or []]
            return [ts for ts in out if ts > 0]
        except Exception:
            LOG.debug("Falling back from model_feature_snapshots timestamp discovery to prices.", exc_info=True)
    if _table_exists(con, "prices"):
        try:
            rows = con.execute(
                f"""
                SELECT DISTINCT ts_ms
                FROM prices
                {where_sql}
                ORDER BY ts_ms ASC
                LIMIT ?
                """,
                tuple(params + [int(max(1, max_samples))]),
            ).fetchall()
            out = [_safe_int(_row_get(row, "ts_ms", 0), 0) for row in rows or []]
            return [ts for ts in out if ts > 0]
        except Exception:
            return []
    return []


def _build_graph_sample(
    con: Any,
    *,
    symbol: str,
    ts_ms: int,
    horizon_s: int,
    seed_symbols: Sequence[str],
    node_feature_ids: Sequence[str],
    feature_set_tag: str,
    window_count: int,
    window_stride_ms: int,
    asof_ts_ms: int | None,
    persist_graph_snapshots: bool,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    target = _target_for_sample(con, symbol=str(symbol), ts_ms=int(ts_ms), horizon_s=int(horizon_s), asof_ts_ms=asof_ts_ms)
    if target is None:
        return None, []

    snapshot, should_persist = _load_graph_snapshot_for_symbol(
        con,
        symbol=str(symbol),
        ts_ms=int(ts_ms),
        peer_symbols=list(seed_symbols),
    )
    snapshots_to_persist = [snapshot] if (persist_graph_snapshots and should_persist and snapshot) else []
    raw_edges = list((snapshot or {}).get("relationships") or [])
    edges = [
        edge
        for edge in (_normalize_edge(edge, anchor_ts_ms=int(ts_ms)) for edge in raw_edges)
        if edge is not None
    ]
    edges.sort(
        key=lambda edge: (
            str(edge.get("edge_type") or ""),
            str(edge.get("source_symbol") or ""),
            str(edge.get("target_symbol") or ""),
            round(_safe_float(edge.get("weight"), 0.0), 12),
        )
    )

    node_symbols = sorted(
        {
            _symbol(symbol),
            *(_symbol(sym) for sym in seed_symbols if _symbol(sym)),
            *(_symbol(edge.get("source_symbol")) for edge in edges),
            *(_symbol(edge.get("target_symbol")) for edge in edges),
        }
    )
    nodes: dict[str, Any] = {}
    max_source_ts = 0
    max_availability_ts = 0
    pit_safe = True
    for node_symbol in node_symbols:
        windows = []
        for window_idx in range(int(window_count)):
            window_ts = int(ts_ms) - int(window_idx) * int(window_stride_ms)
            node_window = _node_window(
                con,
                symbol=str(node_symbol),
                anchor_ts_ms=int(ts_ms),
                window_ts_ms=int(window_ts),
                feature_ids=list(node_feature_ids),
                feature_set_tag=str(feature_set_tag),
            )
            for meta in dict(node_window.get("feature_metadata") or {}).values():
                if not bool((meta or {}).get("available", True)):
                    continue
                source_ts = _safe_int((meta or {}).get("source_ts_ms"), 0)
                availability_ts = _safe_int((meta or {}).get("availability_ts_ms"), 0)
                max_source_ts = max(max_source_ts, source_ts)
                max_availability_ts = max(max_availability_ts, availability_ts)
                if source_ts > int(ts_ms) or availability_ts > int(ts_ms):
                    pit_safe = False
            windows.append(node_window)
        nodes[str(node_symbol)] = {"symbol": str(node_symbol), "windows": windows}

    for edge in edges:
        max_source_ts = max(max_source_ts, _safe_int(edge.get("source_ts_ms"), 0))
        max_availability_ts = max(max_availability_ts, _safe_int(edge.get("availability_ts_ms"), 0))

    edge_types = sorted({str(edge.get("edge_type") or "") for edge in edges if str(edge.get("edge_type") or "")})
    node_schema = _node_feature_schema(
        list(node_feature_ids),
        window_count=int(window_count),
        window_stride_ms=int(window_stride_ms),
        feature_set_tag=str(feature_set_tag),
    )
    edge_schema = _edge_feature_schema(edge_types)
    graph_meta = graph_metadata_from_snapshot(snapshot or {})
    graph_meta.update(
        {
            "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
            "snapshot_version": int(GRAPH_RELATIONAL_SNAPSHOT_VERSION),
            "edge_types": list(edge_types),
            "node_feature_schema": dict(node_schema),
            "edge_feature_schema": dict(edge_schema),
            "max_source_ts_ms": int(max_source_ts or ts_ms),
            "max_availability_ts_ms": int(max_availability_ts or ts_ms),
            "snapshot_available": bool(snapshot),
            "pit_safe": bool(pit_safe and max_source_ts <= int(ts_ms) and max_availability_ts <= int(ts_ms)),
            "stage": "shadow",
            "direct_trading_authority": False,
        }
    )
    graph_meta["train_serve_parity"] = graph_train_serve_parity(graph_meta, graph_meta)
    metadata = {
        "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
        "snapshot_version": int(GRAPH_RELATIONAL_SNAPSHOT_VERSION),
        "edge_types": list(edge_types),
        "max_source_ts_ms": int(max_source_ts or ts_ms),
        "max_availability_ts_ms": int(max_availability_ts or ts_ms),
        "node_feature_schema": dict(node_schema),
        "edge_feature_schema": dict(edge_schema),
        "train_serve_parity": dict(graph_meta["train_serve_parity"]),
        "pit_safe": bool(graph_meta["pit_safe"]),
        "shadow_only": True,
        "direct_trading_authority": False,
        "missing_edge_fallback": bool(not edges),
        "source": "graph_challenger_dataset_builder",
    }
    sample_id = _stable_hash(
        {
            "symbol": _symbol(symbol),
            "ts_ms": int(ts_ms),
            "horizon_s": int(horizon_s),
            "node_symbols": list(node_symbols),
            "edge_hash": _stable_hash(edges),
            "target_source": target.get("source"),
        }
    )[:24]
    return (
        {
            "sample_id": str(sample_id),
            "symbol": _symbol(symbol),
            "target_symbol": _symbol(symbol),
            "ts_ms": int(ts_ms),
            "horizon_s": int(horizon_s),
            "node_symbols": list(node_symbols),
            "nodes": nodes,
            "edges": edges,
            "edge_types": list(edge_types),
            "target": dict(target),
            "target_value": float(target.get("value") or 0.0),
            "metadata": metadata,
            "graph_metadata": graph_meta,
        },
        snapshots_to_persist,
    )


def build_graph_challenger_dataset(
    *,
    con: Any,
    symbols: Sequence[str],
    sample_ts_ms: Sequence[int] | None = None,
    start_ts_ms: int | None = None,
    end_ts_ms: int | None = None,
    horizons_s: Sequence[int] = (300,),
    node_feature_ids: Sequence[str] | None = None,
    feature_set_tag: str = "",
    window_count: int = 3,
    window_stride_ms: int = 300_000,
    asof_ts_ms: int | None = None,
    max_samples: int = 512,
    persist_graph_snapshots: bool = False,
) -> dict[str, Any]:
    ensure_graph_relational_schema(con)
    symbols_key = sorted(dict.fromkeys(_symbol(sym) for sym in symbols if _symbol(sym)))
    feature_ids = [str(fid) for fid in list(node_feature_ids or GRAPH_CHALLENGER_DEFAULT_FEATURE_IDS)]
    tag = str(feature_set_tag or "graph_challenger_node_v1")
    timestamps = [
        int(ts)
        for ts in list(sample_ts_ms or [])
        if int(ts) > 0 and (start_ts_ms is None or int(ts) >= int(start_ts_ms)) and (end_ts_ms is None or int(ts) <= int(end_ts_ms))
    ]
    if not timestamps:
        timestamps = _sample_timestamps_from_store(
            con,
            symbols=symbols_key,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            max_samples=int(max_samples),
        )
    timestamps = sorted(dict.fromkeys(timestamps))[: int(max(1, max_samples))]
    horizon_values = sorted(dict.fromkeys(int(h) for h in horizons_s if int(h) > 0)) or [300]
    samples: list[dict[str, Any]] = []
    graph_snapshots_to_persist: list[dict[str, Any]] = []
    for ts in timestamps:
        for horizon in horizon_values:
            for symbol in symbols_key:
                sample, snapshots_to_persist = _build_graph_sample(
                    con,
                    symbol=str(symbol),
                    ts_ms=int(ts),
                    horizon_s=int(horizon),
                    seed_symbols=symbols_key,
                    node_feature_ids=feature_ids,
                    feature_set_tag=tag,
                    window_count=int(max(1, window_count)),
                    window_stride_ms=int(max(1, window_stride_ms)),
                    asof_ts_ms=asof_ts_ms,
                    persist_graph_snapshots=bool(persist_graph_snapshots),
                )
                if sample is not None:
                    samples.append(sample)
                    graph_snapshots_to_persist.extend(snapshots_to_persist)
    if graph_snapshots_to_persist:
        store_graph_relational_snapshots(graph_snapshots_to_persist, con=con)
    edge_types = sorted({rel for sample in samples for rel in list(sample.get("edge_types") or [])})
    node_schema = _node_feature_schema(
        feature_ids,
        window_count=int(max(1, window_count)),
        window_stride_ms=int(max(1, window_stride_ms)),
        feature_set_tag=tag,
    )
    edge_schema = _edge_feature_schema(edge_types)
    max_source_ts = max([_safe_int((sample.get("metadata") or {}).get("max_source_ts_ms"), 0) for sample in samples] or [0])
    max_availability_ts = max([_safe_int((sample.get("metadata") or {}).get("max_availability_ts_ms"), 0) for sample in samples] or [0])
    metadata = {
        "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
        "snapshot_version": int(GRAPH_RELATIONAL_SNAPSHOT_VERSION),
        "symbols": list(symbols_key),
        "horizons_s": list(horizon_values),
        "sample_count": int(len(samples)),
        "edge_types": list(edge_types),
        "max_source_ts_ms": int(max_source_ts),
        "max_availability_ts_ms": int(max_availability_ts),
        "node_feature_schema": dict(node_schema),
        "edge_feature_schema": dict(edge_schema),
        "pit_safe": bool(all(bool((sample.get("metadata") or {}).get("pit_safe")) for sample in samples)),
        "shadow_only": True,
        "direct_trading_authority": False,
    }
    dataset_fingerprint = _stable_hash(
        {
            "metadata": metadata,
            "samples": [
                {
                    "sample_id": sample.get("sample_id"),
                    "target": sample.get("target_value"),
                    "edges": sample.get("edges"),
                    "nodes": {
                        sym: [window.get("vector") for window in list((node.get("windows") or []))]
                        for sym, node in sorted(dict(sample.get("nodes") or {}).items())
                    },
                }
                for sample in samples
            ],
        }
    )
    dataset_id = f"graph_challenger-{dataset_fingerprint[:16]}"
    metadata["dataset_id"] = str(dataset_id)
    metadata["fingerprint"] = str(dataset_fingerprint)
    return {
        "dataset_id": str(dataset_id),
        "fingerprint": str(dataset_fingerprint),
        "samples": samples,
        "node_feature_schema": node_schema,
        "edge_feature_schema": edge_schema,
        "metadata": metadata,
    }


def _sample_feature_vector(sample: Mapping[str, Any], *, node_feature_ids: Sequence[str], edge_types: Sequence[str], use_graph: bool) -> tuple[list[float], list[str]]:
    sample_dict = dict(sample or {})
    target_symbol = _symbol(sample_dict.get("target_symbol") or sample_dict.get("symbol"))
    nodes = dict(sample_dict.get("nodes") or {})
    target_node = dict(nodes.get(target_symbol) or {})
    target_windows = list(target_node.get("windows") or [])
    values: list[float] = []
    names: list[str] = []
    for window_idx, window in enumerate(target_windows):
        vector = list((window or {}).get("vector") or [])
        for fid_idx, fid in enumerate(node_feature_ids):
            values.append(_safe_float(vector[fid_idx] if fid_idx < len(vector) else 0.0, 0.0))
            names.append(f"target:w{int(window_idx)}:{fid}")
    if not use_graph:
        return values, names

    latest_vectors = {
        _symbol(sym): list((list((node or {}).get("windows") or [{}])[0] or {}).get("vector") or [])
        for sym, node in nodes.items()
    }
    inbound_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in list(sample_dict.get("edges") or []):
        source = _symbol((edge or {}).get("source_symbol"))
        target = _symbol((edge or {}).get("target_symbol"))
        if source == target_symbol:
            neighbor = target
        elif target == target_symbol:
            neighbor = source
        else:
            continue
        if neighbor and neighbor != target_symbol:
            inbound_by_type[str((edge or {}).get("edge_type") or "")].append({**dict(edge or {}), "neighbor": neighbor})

    for edge_type in edge_types:
        typed_edges = inbound_by_type.get(str(edge_type), [])
        weight_sum = sum(abs(_safe_float(edge.get("weight"), 0.0)) for edge in typed_edges)
        values.append(float(len(typed_edges)))
        names.append(f"edge_count:{edge_type}")
        values.append(float(weight_sum))
        names.append(f"edge_weight_sum:{edge_type}")
        for fid_idx, fid in enumerate(node_feature_ids):
            acc = 0.0
            denom = 0.0
            for edge in typed_edges:
                neighbor = _symbol(edge.get("neighbor"))
                vector = latest_vectors.get(neighbor, [])
                weight = abs(_safe_float(edge.get("weight"), 0.0))
                acc += weight * _safe_float(vector[fid_idx] if fid_idx < len(vector) else 0.0, 0.0)
                denom += weight
            values.append(float(acc / denom) if denom > 0.0 else 0.0)
            names.append(f"message:{edge_type}:{fid}")
    return values, names


def _design_matrix(samples: Sequence[Mapping[str, Any]], *, node_feature_ids: Sequence[str], edge_types: Sequence[str], use_graph: bool) -> tuple[np.ndarray, list[str]]:
    rows: list[list[float]] = []
    names: list[str] = []
    for sample in samples:
        row, row_names = _sample_feature_vector(sample, node_feature_ids=node_feature_ids, edge_types=edge_types, use_graph=use_graph)
        if not names:
            names = row_names
        rows.append(row)
    if not rows:
        return np.zeros((0, 0), dtype=np.float64), []
    return np.asarray(rows, dtype=np.float64), names


def _fit_ridge(X: np.ndarray, y: np.ndarray, *, ridge_lambda: float) -> dict[str, Any]:
    if X.size == 0 or y.size == 0:
        return {"intercept": 0.0, "weights": [], "x_mean": [], "x_scale": [], "ridge_lambda": float(ridge_lambda)}
    x_mean = X.mean(axis=0)
    x_scale = X.std(axis=0)
    x_scale[x_scale < 1e-12] = 1.0
    Xn = (X - x_mean) / x_scale
    design = np.column_stack([np.ones(Xn.shape[0]), Xn])
    penalty = np.eye(design.shape[1], dtype=np.float64) * float(max(0.0, ridge_lambda))
    penalty[0, 0] = 0.0
    try:
        coef = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    except Exception:
        coef = np.linalg.lstsq(design.T @ design + penalty, design.T @ y, rcond=None)[0]
    return {
        "intercept": float(coef[0]),
        "weights": [float(v) for v in coef[1:]],
        "x_mean": [float(v) for v in x_mean],
        "x_scale": [float(v) for v in x_scale],
        "ridge_lambda": float(ridge_lambda),
    }


def _predict_ridge(model: Mapping[str, Any], X: np.ndarray) -> np.ndarray:
    if X.size == 0:
        return np.zeros((0,), dtype=np.float64)
    weights = np.asarray(list(model.get("weights") or []), dtype=np.float64)
    if weights.size == 0:
        return np.zeros((X.shape[0],), dtype=np.float64) + _safe_float(model.get("intercept"), 0.0)
    mean = np.asarray(list(model.get("x_mean") or [0.0] * X.shape[1]), dtype=np.float64)
    scale = np.asarray(list(model.get("x_scale") or [1.0] * X.shape[1]), dtype=np.float64)
    scale[scale < 1e-12] = 1.0
    return _safe_float(model.get("intercept"), 0.0) + ((X - mean) / scale) @ weights


def _metrics(predictions: Sequence[float], targets: Sequence[float]) -> dict[str, Any]:
    preds = np.asarray(list(predictions or []), dtype=np.float64)
    y = np.asarray(list(targets or []), dtype=np.float64)
    if preds.size == 0 or y.size == 0:
        return {"n": 0, "mae": 0.0, "rmse": 0.0, "directional_accuracy": 0.0}
    err = preds - y
    direction = np.sign(preds) == np.sign(y)
    return {
        "n": int(y.size),
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(math.sqrt(float(np.mean(err * err)))),
        "directional_accuracy": float(np.mean(direction.astype(np.float64))),
    }


def train_graph_challenger_models(
    dataset: Mapping[str, Any],
    *,
    holdout_fraction: float = 0.25,
    ridge_lambda: float = 1.0,
) -> dict[str, Any]:
    samples = [dict(sample) for sample in list((dataset or {}).get("samples") or [])]
    samples.sort(key=lambda sample: (int(sample.get("ts_ms") or 0), str(sample.get("symbol") or ""), int(sample.get("horizon_s") or 0)))
    if not samples:
        raise ValueError("graph_challenger_dataset_empty")
    node_schema = dict((dataset or {}).get("node_feature_schema") or {})
    edge_schema = dict((dataset or {}).get("edge_feature_schema") or {})
    node_feature_ids = [str(fid) for fid in list(node_schema.get("feature_ids") or [])]
    edge_types = [str(edge_type) for edge_type in list(edge_schema.get("edge_types") or [])]
    n = len(samples)
    split_idx = max(1, int(round(float(n) * (1.0 - float(holdout_fraction)))))
    if n > 1:
        split_idx = min(split_idx, n - 1)
    train_samples = samples[:split_idx]
    eval_samples = samples[split_idx:] if split_idx < n else samples
    y_train = np.asarray([_safe_float(sample.get("target_value"), 0.0) for sample in train_samples], dtype=np.float64)
    y_eval = np.asarray([_safe_float(sample.get("target_value"), 0.0) for sample in eval_samples], dtype=np.float64)

    X_train_base, base_feature_names = _design_matrix(
        train_samples,
        node_feature_ids=node_feature_ids,
        edge_types=edge_types,
        use_graph=False,
    )
    X_eval_base, _ = _design_matrix(
        eval_samples,
        node_feature_ids=node_feature_ids,
        edge_types=edge_types,
        use_graph=False,
    )
    X_train_graph, graph_feature_names = _design_matrix(
        train_samples,
        node_feature_ids=node_feature_ids,
        edge_types=edge_types,
        use_graph=True,
    )
    X_eval_graph, _ = _design_matrix(
        eval_samples,
        node_feature_ids=node_feature_ids,
        edge_types=edge_types,
        use_graph=True,
    )
    baseline_model = _fit_ridge(X_train_base, y_train, ridge_lambda=float(ridge_lambda))
    graph_model = _fit_ridge(X_train_graph, y_train, ridge_lambda=float(ridge_lambda))
    baseline_pred = _predict_ridge(baseline_model, X_eval_base)
    graph_pred = _predict_ridge(graph_model, X_eval_graph)
    baseline_metrics = _metrics(baseline_pred.tolist(), y_eval.tolist())
    graph_metrics = _metrics(graph_pred.tolist(), y_eval.tolist())
    ablation = {
        "baseline_node_only": dict(baseline_metrics),
        "graph_message_passing": dict(graph_metrics),
        "incremental_mae": float(_safe_float(baseline_metrics.get("mae"), 0.0) - _safe_float(graph_metrics.get("mae"), 0.0)),
        "incremental_rmse": float(_safe_float(baseline_metrics.get("rmse"), 0.0) - _safe_float(graph_metrics.get("rmse"), 0.0)),
        "comparison": "graph_message_passing_minus_node_only",
    }
    horizon_values = sorted({int(sample.get("horizon_s") or 0) for sample in samples if int(sample.get("horizon_s") or 0) > 0})
    horizon = int(horizon_values[0]) if len(horizon_values) == 1 else 0
    model_payload = {
        "model_name": GRAPH_CHALLENGER_MODEL_NAME,
        "model_family": GRAPH_CHALLENGER_MODEL_FAMILY,
        "horizon_s": int(horizon),
        "stage": "shadow",
        "direct_trading_authority": False,
        "baseline_model": baseline_model,
        "graph_model": graph_model,
        "baseline_feature_names": list(base_feature_names),
        "graph_feature_names": list(graph_feature_names),
        "node_feature_schema": dict(node_schema),
        "edge_feature_schema": dict(edge_schema),
        "graph_metadata": dict((dataset or {}).get("metadata") or {}),
        "ablation": dict(ablation),
        "train_sample_ids": [str(sample.get("sample_id") or "") for sample in train_samples],
        "eval_sample_ids": [str(sample.get("sample_id") or "") for sample in eval_samples],
    }
    predictions = []
    for sample, base_pred, graph_value in zip(eval_samples, baseline_pred.tolist(), graph_pred.tolist()):
        predictions.append(
            {
                "sample_id": str(sample.get("sample_id") or ""),
                "symbol": str(sample.get("symbol") or ""),
                "horizon_s": int(sample.get("horizon_s") or 0),
                "ts_ms": int(sample.get("ts_ms") or 0),
                "prediction": float(graph_value),
                "baseline_prediction": float(base_pred),
                "target": _safe_float(sample.get("target_value"), 0.0),
            }
        )
    return {
        "model": model_payload,
        "predictions": predictions,
        "ablation": ablation,
        "train_count": int(len(train_samples)),
        "eval_count": int(len(eval_samples)),
    }


def _artifact_ref_from_sha(sha256: str) -> ArtifactRef:
    return ArtifactRef(
        sha256=str(sha256),
        size=0,
        content_type="application/json",
        kind="model",
        created_ts=datetime.now(timezone.utc),
        metadata={},
    )


def save_graph_challenger_artifact(
    *,
    model_result: Mapping[str, Any],
    dataset: Mapping[str, Any],
    artifact_store: LocalArtifactStore | None = None,
) -> dict[str, Any]:
    store = artifact_store or LocalArtifactStore()
    model = dict(model_result.get("model") or {})
    horizon = int(model.get("horizon_s") or 0)
    alias = f"model:{GRAPH_CHALLENGER_MODEL_FAMILY}:{horizon or 'multi'}:current"
    payload = {
        "artifact_version": 1,
        "created_ts_ms": _now_ms(),
        "model": model,
        "dataset_metadata": dict((dataset or {}).get("metadata") or {}),
        "shadow_only": True,
        "direct_trading_authority": False,
    }
    payload_bytes = _json_dumps(payload).encode("utf-8")
    ref = store.put(
        payload_bytes,
        content_type="application/json",
        kind="model",
        alias=str(alias),
        metadata={
            "model_name": GRAPH_CHALLENGER_MODEL_NAME,
            "model_family": GRAPH_CHALLENGER_MODEL_FAMILY,
            "horizon_s": int(horizon),
            "dataset_id": str((dataset or {}).get("dataset_id") or ""),
            "shadow_only": True,
            "direct_trading_authority": False,
        },
    )
    return {
        "artifact_sha256": str(ref.sha256),
        "artifact_alias": str(alias),
        "artifact_uri": str(ref.uri),
        "size_bytes": int(ref.size),
        "payload": payload,
    }


def load_graph_challenger_artifact(
    ref_or_alias: str,
    *,
    artifact_store: LocalArtifactStore | None = None,
) -> dict[str, Any]:
    store = artifact_store or LocalArtifactStore()
    ref_text = str(ref_or_alias or "").strip()
    if not ref_text:
        raise ValueError("graph_challenger_artifact_ref_required")
    if ref_text.startswith("artifact:"):
        ref_text = ref_text.split(":", 1)[1]
    if len(ref_text) == 64 and all(ch in "0123456789abcdefABCDEF" for ch in ref_text):
        ref = _artifact_ref_from_sha(ref_text.lower())
    else:
        resolved = store.resolve(ref_text)
        if resolved is None:
            raise FileNotFoundError(f"graph_challenger_artifact_alias_not_found:{ref_text}")
        ref = resolved
    return _json_obj(store.get_bytes(ref, verify=True).decode("utf-8"))


def _write_oos_predictions(con: Any, *, predictions: Sequence[Mapping[str, Any]], run_id: str) -> int:
    rows = [
        {
            "symbol": str(row.get("symbol") or ""),
            "horizon": int(row.get("horizon_s") or 0),
            "family": GRAPH_CHALLENGER_MODEL_FAMILY,
            "ts": int(row.get("ts_ms") or 0),
            "run_id": str(run_id),
            "prediction": float(row.get("prediction") or 0.0),
            "target": float(row.get("target") or 0.0),
        }
        for row in predictions
        if str(row.get("symbol") or "").strip() and int(row.get("horizon_s") or 0) > 0
    ]
    return int(oos_store.upsert_oos_predictions(rows, con=con, ensure=True))


def _persist_run_row(
    con: Any,
    *,
    run_id: str,
    model_result: Mapping[str, Any],
    dataset: Mapping[str, Any],
    artifact: Mapping[str, Any],
    oos_count: int,
) -> None:
    ensure_graph_challenger_schema(con)
    model = dict(model_result.get("model") or {})
    graph_metadata = dict(model.get("graph_metadata") or (dataset or {}).get("metadata") or {})
    node_schema = dict(model.get("node_feature_schema") or (dataset or {}).get("node_feature_schema") or {})
    edge_schema = dict(model.get("edge_feature_schema") or (dataset or {}).get("edge_feature_schema") or {})
    parity = graph_train_serve_parity(graph_metadata, graph_metadata)
    graph_metadata["train_serve_parity"] = dict(parity)
    con.execute(
        """
        INSERT OR REPLACE INTO graph_challenger_runs(
          run_id, model_name, model_family, horizon_s, stage, artifact_sha256, artifact_alias,
          oos_prediction_count, graph_metadata_json, node_feature_schema_json, edge_feature_schema_json,
          train_serve_parity_json, benchmark_json, created_ts_ms
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(run_id),
            GRAPH_CHALLENGER_MODEL_NAME,
            GRAPH_CHALLENGER_MODEL_FAMILY,
            int(model.get("horizon_s") or 0),
            "shadow",
            str(artifact.get("artifact_sha256") or ""),
            str(artifact.get("artifact_alias") or ""),
            int(oos_count),
            _json_dumps(graph_metadata),
            _json_dumps(node_schema),
            _json_dumps(edge_schema),
            _json_dumps(parity),
            _json_dumps(dict(model_result.get("ablation") or {})),
            _now_ms(),
        ),
    )


def _write_marketplace_rows(
    con: Any,
    *,
    run_id: str,
    model_result: Mapping[str, Any],
    dataset: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> int:
    _ensure_marketplace_schema(con)
    predictions = [dict(row) for row in list(model_result.get("predictions") or [])]
    by_key: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in predictions:
        by_key[(str(row.get("symbol") or "").upper().strip(), int(row.get("horizon_s") or 0))].append(row)
    repo = CompetitionRepository(con)
    writes = 0
    graph_metadata = dict(((model_result.get("model") or {}).get("graph_metadata") if isinstance(model_result.get("model"), Mapping) else {}) or (dataset or {}).get("metadata") or {})
    graph_metadata["train_serve_parity"] = graph_train_serve_parity(graph_metadata, graph_metadata)
    ablation = dict(model_result.get("ablation") or {})
    for (symbol, horizon), rows in sorted(by_key.items()):
        if not symbol or horizon <= 0:
            continue
        targets = [_safe_float(row.get("target"), 0.0) for row in rows]
        preds = [_safe_float(row.get("prediction"), 0.0) for row in rows]
        metrics = _metrics(preds, targets)
        wins = sum(1 for pred, target in zip(preds, targets) if math.copysign(1.0, pred or 0.0) == math.copysign(1.0, target or 0.0))
        score = _safe_float(ablation.get("incremental_mae"), 0.0)
        meta = {
            "score_source": GRAPH_CHALLENGER_SCORE_SOURCE,
            "model_family": GRAPH_CHALLENGER_MODEL_FAMILY,
            "model_kind": "shadow_graph_challenger",
            "run_id": str(run_id),
            "artifact_sha256": str(artifact.get("artifact_sha256") or ""),
            "artifact_alias": str(artifact.get("artifact_alias") or ""),
            "graph_relational": dict(graph_metadata),
            "graph_metadata": dict(graph_metadata),
            "node_feature_schema": dict((dataset or {}).get("node_feature_schema") or {}),
            "edge_feature_schema": dict((dataset or {}).get("edge_feature_schema") or {}),
            "benchmark": {"symbol_metrics": dict(metrics), "ablation": dict(ablation)},
            "shadow_only": True,
            "direct_trading_authority": False,
        }
        repo.upsert_marketplace_score(
            {
                "model_id": GRAPH_CHALLENGER_MODEL_FAMILY,
                "model_name": GRAPH_CHALLENGER_MODEL_NAME,
                "symbol": str(symbol),
                "horizon_s": int(horizon),
                "regime": "global",
                "stage": "shadow",
                "score": float(score),
                "trades": int(len(rows)),
                "wins": int(wins),
                "losses": int(max(0, len(rows) - wins)),
                "gross_pnl": 0.0,
                "net_pnl": 0.0,
                "avg_confidence": 0.0,
                "last_signal_ts_ms": max([_safe_int(row.get("ts_ms"), 0) for row in rows] or [0]),
            },
            meta=meta,
            updated_ts_ms=_now_ms(),
            update_pnl_on_conflict=False,
        )
        writes += 1
    return int(writes)


def run_graph_challenger_benchmark(
    *,
    con: Any,
    symbols: Sequence[str],
    sample_ts_ms: Sequence[int] | None = None,
    start_ts_ms: int | None = None,
    end_ts_ms: int | None = None,
    horizons_s: Sequence[int] = (300,),
    node_feature_ids: Sequence[str] | None = None,
    feature_set_tag: str = "",
    window_count: int = 3,
    window_stride_ms: int = 300_000,
    holdout_fraction: float = 0.25,
    ridge_lambda: float = 1.0,
    asof_ts_ms: int | None = None,
    max_samples: int = 512,
    artifact_store: LocalArtifactStore | None = None,
) -> dict[str, Any]:
    dataset = build_graph_challenger_dataset(
        con=con,
        symbols=list(symbols),
        sample_ts_ms=sample_ts_ms,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        horizons_s=list(horizons_s),
        node_feature_ids=node_feature_ids,
        feature_set_tag=str(feature_set_tag or "graph_challenger_node_v1"),
        window_count=int(window_count),
        window_stride_ms=int(window_stride_ms),
        asof_ts_ms=asof_ts_ms,
        max_samples=int(max_samples),
        persist_graph_snapshots=True,
    )
    model_result = train_graph_challenger_models(
        dataset,
        holdout_fraction=float(holdout_fraction),
        ridge_lambda=float(ridge_lambda),
    )
    run_id = "graph-challenger-" + _stable_hash(
        {
            "dataset_id": dataset.get("dataset_id"),
            "model_name": GRAPH_CHALLENGER_MODEL_NAME,
            "node_feature_schema": dataset.get("node_feature_schema"),
            "edge_feature_schema": dataset.get("edge_feature_schema"),
            "ablation": model_result.get("ablation"),
        }
    )[:16]
    artifact = save_graph_challenger_artifact(
        model_result=model_result,
        dataset=dataset,
        artifact_store=artifact_store,
    )
    oos_count = _write_oos_predictions(con, predictions=list(model_result.get("predictions") or []), run_id=str(run_id))
    _persist_run_row(
        con,
        run_id=str(run_id),
        model_result=model_result,
        dataset=dataset,
        artifact=artifact,
        oos_count=int(oos_count),
    )
    marketplace_rows = _write_marketplace_rows(
        con,
        run_id=str(run_id),
        model_result=model_result,
        dataset=dataset,
        artifact=artifact,
    )
    _commit_if_possible(con)
    return {
        "ok": True,
        "run_id": str(run_id),
        "model_name": GRAPH_CHALLENGER_MODEL_NAME,
        "model_family": GRAPH_CHALLENGER_MODEL_FAMILY,
        "stage": "shadow",
        "shadow_only": True,
        "direct_trading_authority": False,
        "dataset": {k: v for k, v in dataset.items() if k != "samples"},
        "sample_count": int(len(list(dataset.get("samples") or []))),
        "train_count": int(model_result.get("train_count") or 0),
        "eval_count": int(model_result.get("eval_count") or 0),
        "oos_prediction_count": int(oos_count),
        "marketplace_rows": int(marketplace_rows),
        "artifact": {k: v for k, v in artifact.items() if k != "payload"},
        "ablation": dict(model_result.get("ablation") or {}),
        "graph_metadata": dict((model_result.get("model") or {}).get("graph_metadata") or {}),
    }


__all__ = [
    "GRAPH_CHALLENGER_MODEL_FAMILY",
    "GRAPH_CHALLENGER_MODEL_NAME",
    "build_graph_challenger_dataset",
    "ensure_graph_challenger_schema",
    "load_graph_challenger_artifact",
    "run_graph_challenger_benchmark",
    "save_graph_challenger_artifact",
    "train_graph_challenger_models",
]
