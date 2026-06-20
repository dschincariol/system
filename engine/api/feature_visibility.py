"""Read-only visibility serializers for structured-doc and graph features."""

from __future__ import annotations

import json
import math
import time
from typing import Any, Mapping, Sequence

from engine.api.api_read import _table_exists
from engine.api.internal_access import db_connect
from engine.data.structured_document_events import (
    EXTRACTOR_NAME,
    EXTRACTOR_VERSION,
    STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS,
)
from engine.strategy.feature_pit import FEATURE_PIT_POLICIES, evaluate_group_policy
from engine.strategy.graph_relational import (
    GRAPH_RELATIONAL_FEATURE_IDS,
    GRAPH_RELATIONAL_GRAPH_ID,
    GRAPH_RELATIONAL_GROUP,
    GRAPH_RELATIONAL_SNAPSHOT_VERSION,
    graph_relational_features_enabled,
)

STRUCTURED_DOC_FEATURE_GROUP = "structured_doc_events_v1"
STRUCTURED_DOC_PIT_GROUP = "structured_doc_events"
STRUCTURED_DOC_PREFIX = "structured_doc_events_v1."
GRAPH_RELATIONAL_PREFIX = "graph.relational_v1."
LOW_CONFIDENCE_THRESHOLD = 0.60
DEFAULT_LINEAGE_LIMIT = 12


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


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


def _row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if hasattr(row, "keys"):
        try:
            return row[key]
        except Exception:
            return default
    try:
        return row[index]
    except Exception:
        return default


def _norm_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _age_ms(now_ms: int, ts_ms: Any) -> int | None:
    ts = _safe_int(ts_ms, 0)
    if ts <= 0:
        return None
    return int(max(0, int(now_ms) - int(ts)))


def _query_one(con: Any, sql: str, params: Sequence[Any] = ()) -> Any | None:
    try:
        return con.execute(sql, tuple(params)).fetchone()
    except Exception:
        return None


def _query_all(con: Any, sql: str, params: Sequence[Any] = ()) -> list[Any]:
    try:
        return list(con.execute(sql, tuple(params)).fetchall() or [])
    except Exception:
        return []


def _status_from_pit(detail: Mapping[str, Any], *, empty_status: str = "unavailable") -> str:
    reasons = {str(code) for code in list((detail or {}).get("reason_codes") or [])}
    if "feature_stale" in reasons:
        return "stale"
    if reasons:
        return empty_status
    return "available"


def _pit_detail(group: str, source_meta: Mapping[str, Any], *, now_ms: int, available: bool) -> dict[str, Any]:
    return evaluate_group_policy(
        group=str(group),
        source_meta=dict(source_meta or {}),
        anchor_ts_ms=int(now_ms),
        available=bool(available),
    )


def _group_rows(
    con: Any,
    *,
    table: str,
    column: str,
    latest_column: str = "availability_ts_ms",
    where_sql: str = "",
    params: Sequence[Any] = (),
    limit: int = 30,
) -> list[dict[str, Any]]:
    where = f"WHERE {where_sql}" if where_sql else ""
    rows = _query_all(
        con,
        f"""
        SELECT {column}, COUNT(*) AS row_count, MAX({latest_column}) AS latest_ts_ms
        FROM {table}
        {where}
        GROUP BY {column}
        ORDER BY row_count DESC, latest_ts_ms DESC
        LIMIT ?
        """,
        (*tuple(params), int(max(1, limit))),
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        value = str(_row_get(row, column, 0) or "").strip()
        if not value:
            value = "unknown"
        out.append(
            {
                str(column): value,
                "count": _safe_int(_row_get(row, "row_count", 1), 0),
                "latest_ts_ms": _safe_int(_row_get(row, "latest_ts_ms", 2), 0) or None,
            }
        )
    return out


def _structured_where(symbol: str) -> tuple[str, tuple[Any, ...]]:
    sym = _norm_symbol(symbol)
    if not sym:
        return "", ()
    return "symbol = ?", (sym,)


def _structured_confidence_distribution(con: Any, where_sql: str, params: Sequence[Any]) -> dict[str, Any]:
    where = f"WHERE {where_sql}" if where_sql else ""
    row = _query_one(
        con,
        f"""
        SELECT
          SUM(CASE WHEN extraction_confidence < 0.40 THEN 1 ELSE 0 END) AS very_low,
          SUM(CASE WHEN extraction_confidence >= 0.40 AND extraction_confidence < 0.60 THEN 1 ELSE 0 END) AS low,
          SUM(CASE WHEN extraction_confidence >= 0.60 AND extraction_confidence < 0.80 THEN 1 ELSE 0 END) AS medium,
          SUM(CASE WHEN extraction_confidence >= 0.80 THEN 1 ELSE 0 END) AS high
        FROM structured_document_events
        {where}
        """,
        tuple(params),
    )
    buckets = [
        {"label": "very_low", "min": 0.0, "max": 0.4, "count": _safe_int(_row_get(row, "very_low", 0), 0)},
        {"label": "low", "min": 0.4, "max": 0.6, "count": _safe_int(_row_get(row, "low", 1), 0)},
        {"label": "medium", "min": 0.6, "max": 0.8, "count": _safe_int(_row_get(row, "medium", 2), 0)},
        {"label": "high", "min": 0.8, "max": 1.0, "count": _safe_int(_row_get(row, "high", 3), 0)},
    ]
    return {"buckets": buckets}


def _structured_lineage_rows(
    con: Any,
    *,
    symbol: str = "",
    feature_id: str = "",
    decision_ts_ms: int | None = None,
    limit: int = DEFAULT_LINEAGE_LIMIT,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    sym = _norm_symbol(symbol)
    if sym:
        where.append("symbol = ?")
        params.append(sym)
    fid = str(feature_id or "").strip()
    if fid and fid not in {
        "structured_doc_events_v1.event_count_30d",
        "structured_doc_events_v1.latest_event_age_days",
    }:
        where.append("feature_id = ?")
        params.append(fid)
    if decision_ts_ms is not None and int(decision_ts_ms) > 0:
        where.append("availability_ts_ms <= ?")
        params.append(int(decision_ts_ms))
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = _query_all(
        con,
        f"""
        SELECT source_document_id, source_event_id, symbol, document_type, source,
               event_type, event_ts_ms, availability_ts_ms, extraction_confidence,
               feature_id, evidence, created_ts_ms, pit_metadata_json
        FROM structured_document_events
        {where_sql}
        ORDER BY availability_ts_ms DESC, id DESC
        LIMIT ?
        """,
        (*tuple(params), int(max(1, limit))),
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        source_document_id = str(_row_get(row, "source_document_id", 0) or "")
        item = {
            "source_document_id": source_document_id,
            "source_event_id": _safe_int(_row_get(row, "source_event_id", 1), 0) or None,
            "symbol": str(_row_get(row, "symbol", 2) or ""),
            "document_type": str(_row_get(row, "document_type", 3) or "unknown"),
            "source": str(_row_get(row, "source", 4) or "unknown"),
            "event_type": str(_row_get(row, "event_type", 5) or ""),
            "event_ts_ms": _safe_int(_row_get(row, "event_ts_ms", 6), 0) or None,
            "availability_ts_ms": _safe_int(_row_get(row, "availability_ts_ms", 7), 0) or None,
            "extraction_confidence": _safe_float(_row_get(row, "extraction_confidence", 8), 0.0),
            "feature_id": str(_row_get(row, "feature_id", 9) or ""),
            "evidence": str(_row_get(row, "evidence", 10) or "")[:500],
            "created_ts_ms": _safe_int(_row_get(row, "created_ts_ms", 11), 0) or None,
            "pit_metadata": _json_obj(_row_get(row, "pit_metadata_json", 12)),
            "source_artifact": f"structured_document_events:{source_document_id}" if source_document_id else "",
        }
        out.append(item)
    return out


def _structured_failure_summary(con: Any) -> dict[str, Any]:
    if not _table_exists(con, "event_log"):
        return {
            "available": False,
            "count": None,
            "latest_ts_ms": None,
            "source": "event_log",
            "reason": "event_log table unavailable; extraction failure telemetry cannot be counted from persisted state.",
        }
    row = _query_one(
        con,
        """
        SELECT COUNT(*) AS count, MAX(ts_ms) AS latest_ts_ms
        FROM event_log
        WHERE event_type = 'runtime_failure'
          AND entity_id = 'STRUCTURED_DOCUMENT_EVENT_EXTRACTION_FAILED'
        """,
    )
    return {
        "available": True,
        "count": _safe_int(_row_get(row, "count", 0), 0),
        "latest_ts_ms": _safe_int(_row_get(row, "latest_ts_ms", 1), 0) or None,
        "source": "event_log",
        "reason": "",
    }


def structured_document_visibility(
    con: Any,
    *,
    now_ms: int,
    symbol: str = "",
    low_confidence_threshold: float = LOW_CONFIDENCE_THRESHOLD,
    lineage_limit: int = DEFAULT_LINEAGE_LIMIT,
) -> dict[str, Any]:
    warnings: list[str] = []
    policy = FEATURE_PIT_POLICIES[STRUCTURED_DOC_PIT_GROUP]
    base = {
        "available": False,
        "status": "unavailable",
        "shadow_only": True,
        "direct_trading_authority": False,
        "feature_group": STRUCTURED_DOC_FEATURE_GROUP,
        "pit_group": STRUCTURED_DOC_PIT_GROUP,
        "feature_ids": list(STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS),
        "extractor_name": EXTRACTOR_NAME,
        "extractor_version": EXTRACTOR_VERSION,
        "freshness_ttl_ms": int(policy.freshness_ttl_ms),
        "counts": {
            "events": 0,
            "source_documents": 0,
            "symbols": 0,
            "low_confidence": 0,
        },
        "latest_extraction_ts_ms": None,
        "latest_availability_ts_ms": None,
        "latest_event_ts_ms": None,
        "confidence": {
            "low_confidence_threshold": float(low_confidence_threshold),
            "low_confidence_count": 0,
            "buckets": [],
        },
        "coverage": {"symbols": [], "event_types": [], "document_types": [], "sources": []},
        "lineage": {"source_documents": []},
        "extraction_failures": {"available": False, "count": None, "latest_ts_ms": None, "source": "event_log"},
        "pit_status": {},
        "warnings": warnings,
    }
    if not _table_exists(con, "structured_document_events"):
        warnings.append("structured_document_events table unavailable")
        base["pit_status"] = _pit_detail(STRUCTURED_DOC_PIT_GROUP, {}, now_ms=now_ms, available=False)
        return base

    where_sql, params = _structured_where(symbol)
    where = f"WHERE {where_sql}" if where_sql else ""
    row = _query_one(
        con,
        f"""
        SELECT COUNT(*) AS event_count,
               COUNT(DISTINCT source_document_id) AS source_document_count,
               COUNT(DISTINCT symbol) AS symbol_count,
               SUM(CASE WHEN extraction_confidence < ? THEN 1 ELSE 0 END) AS low_confidence_count,
               MAX(created_ts_ms) AS latest_created_ts_ms,
               MAX(availability_ts_ms) AS latest_availability_ts_ms,
               MAX(event_ts_ms) AS latest_event_ts_ms
        FROM structured_document_events
        {where}
        """,
        (float(low_confidence_threshold), *tuple(params)),
    )
    event_count = _safe_int(_row_get(row, "event_count", 0), 0)
    source_document_count = _safe_int(_row_get(row, "source_document_count", 1), 0)
    symbol_count = _safe_int(_row_get(row, "symbol_count", 2), 0)
    low_count = _safe_int(_row_get(row, "low_confidence_count", 3), 0)
    latest_created = _safe_int(_row_get(row, "latest_created_ts_ms", 4), 0)
    latest_availability = _safe_int(_row_get(row, "latest_availability_ts_ms", 5), 0)
    latest_event = _safe_int(_row_get(row, "latest_event_ts_ms", 6), 0)

    source_meta = {
        "latest_event_ts_ms": latest_event or None,
        "latest_availability_ts_ms": latest_availability or None,
    }
    pit = _pit_detail(STRUCTURED_DOC_PIT_GROUP, source_meta, now_ms=now_ms, available=event_count > 0)
    status = "empty" if event_count <= 0 else _status_from_pit(pit, empty_status="unavailable")
    if status == "stale":
        warnings.append("structured document extractions are stale under the PIT freshness policy")
    if event_count <= 0:
        warnings.append("no structured document events are available")
    if low_count > 0:
        warnings.append(f"{low_count} structured document events are below confidence threshold {low_confidence_threshold:.2f}")

    confidence = _structured_confidence_distribution(con, where_sql, params)
    confidence["low_confidence_threshold"] = float(low_confidence_threshold)
    confidence["low_confidence_count"] = int(low_count)

    base.update(
        {
            "available": bool(event_count > 0 and status == "available"),
            "status": status,
            "counts": {
                "events": int(event_count),
                "source_documents": int(source_document_count),
                "symbols": int(symbol_count),
                "low_confidence": int(low_count),
            },
            "latest_extraction_ts_ms": latest_created or None,
            "latest_availability_ts_ms": latest_availability or None,
            "latest_event_ts_ms": latest_event or None,
            "latest_availability_age_ms": _age_ms(now_ms, latest_availability),
            "confidence": confidence,
            "coverage": {
                "symbols": _group_rows(
                    con,
                    table="structured_document_events",
                    column="symbol",
                    latest_column="availability_ts_ms",
                    where_sql=where_sql,
                    params=params,
                    limit=50,
                ),
                "event_types": _group_rows(
                    con,
                    table="structured_document_events",
                    column="event_type",
                    latest_column="availability_ts_ms",
                    where_sql=where_sql,
                    params=params,
                    limit=30,
                ),
                "document_types": _group_rows(
                    con,
                    table="structured_document_events",
                    column="document_type",
                    latest_column="availability_ts_ms",
                    where_sql=where_sql,
                    params=params,
                    limit=10,
                ),
                "sources": _group_rows(
                    con,
                    table="structured_document_events",
                    column="source",
                    latest_column="availability_ts_ms",
                    where_sql=where_sql,
                    params=params,
                    limit=20,
                ),
            },
            "lineage": {
                "source_documents": _structured_lineage_rows(
                    con,
                    symbol=symbol,
                    limit=lineage_limit,
                ),
            },
            "extraction_failures": _structured_failure_summary(con),
            "pit_status": pit,
        }
    )
    if not bool(base["extraction_failures"].get("available")):
        warnings.append(str(base["extraction_failures"].get("reason") or "extraction failure telemetry unavailable"))
    return base


def _latest_graph_rows(con: Any, *, symbol: str = "", limit: int = DEFAULT_LINEAGE_LIMIT) -> list[dict[str, Any]]:
    where = ""
    params: list[Any] = []
    sym = _norm_symbol(symbol)
    if sym:
        where = "WHERE symbol = ?"
        params.append(sym)
    rows = _query_all(
        con,
        f"""
        SELECT symbol, ts_ms, graph_id, snapshot_version, feature_ids_json,
               features_json, edge_counts_json, relationships_json,
               source_timestamps_json, availability_json, metadata_json, created_ts_ms
        FROM graph_relational_snapshots
        {where}
        ORDER BY ts_ms DESC, created_ts_ms DESC
        LIMIT ?
        """,
        (*tuple(params), int(max(1, limit))),
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        feature_ids = [str(fid) for fid in _json_list(_row_get(row, "feature_ids_json", 4))]
        features = _json_obj(_row_get(row, "features_json", 5))
        edge_counts = {str(k): _safe_int(v, 0) for k, v in _json_obj(_row_get(row, "edge_counts_json", 6)).items()}
        relationships = _json_list(_row_get(row, "relationships_json", 7))
        source_timestamps = _json_obj(_row_get(row, "source_timestamps_json", 8))
        availability = _json_obj(_row_get(row, "availability_json", 9))
        metadata = _json_obj(_row_get(row, "metadata_json", 10))
        ts_ms = _safe_int(_row_get(row, "ts_ms", 1), 0)
        max_source = _safe_int(metadata.get("max_source_ts_ms") or source_timestamps.get("max_source_ts_ms"), 0)
        max_availability = _safe_int(
            metadata.get("max_availability_ts_ms") or source_timestamps.get("max_availability_ts_ms"),
            0,
        )
        pit_safe = _safe_bool(metadata.get("pit_safe"), False) and (
            (max_source <= 0 or max_source <= ts_ms) and (max_availability <= 0 or max_availability <= ts_ms)
        )
        relationship_hash = str(metadata.get("relationship_hash") or source_timestamps.get("relationship_hash") or "")
        out.append(
            {
                "symbol": str(_row_get(row, "symbol", 0) or ""),
                "ts_ms": ts_ms or None,
                "graph_id": str(_row_get(row, "graph_id", 2) or ""),
                "snapshot_version": _safe_int(_row_get(row, "snapshot_version", 3), 0),
                "feature_ids": feature_ids,
                "features": features,
                "edge_counts": edge_counts,
                "relationship_count": int(sum(edge_counts.values()) or len(relationships or [])),
                "source_timestamps": source_timestamps,
                "availability": availability,
                "metadata": metadata,
                "created_ts_ms": _safe_int(_row_get(row, "created_ts_ms", 11), 0) or None,
                "pit_safe": bool(pit_safe),
                "stage": str(metadata.get("stage") or "shadow"),
                "direct_trading_authority": _safe_bool(metadata.get("direct_trading_authority"), False),
                "relationship_hash": relationship_hash,
                "source_artifact": (
                    f"graph_relational_snapshots:{str(_row_get(row, 'symbol', 0) or '')}:{ts_ms}:{str(_row_get(row, 'graph_id', 2) or '')}"
                    if ts_ms > 0
                    else ""
                ),
            }
        )
    return out


def graph_feature_visibility(
    con: Any,
    *,
    now_ms: int,
    symbol: str = "",
    lineage_limit: int = DEFAULT_LINEAGE_LIMIT,
) -> dict[str, Any]:
    warnings: list[str] = []
    policy = FEATURE_PIT_POLICIES[GRAPH_RELATIONAL_GROUP]
    base = {
        "available": False,
        "status": "unavailable",
        "enabled": bool(graph_relational_features_enabled()),
        "shadow_only": True,
        "direct_trading_authority": False,
        "feature_group": GRAPH_RELATIONAL_GROUP,
        "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
        "snapshot_version": GRAPH_RELATIONAL_SNAPSHOT_VERSION,
        "feature_ids": list(GRAPH_RELATIONAL_FEATURE_IDS),
        "freshness_ttl_ms": int(policy.freshness_ttl_ms),
        "feature_availability": {
            "expected_feature_count": len(GRAPH_RELATIONAL_FEATURE_IDS),
            "observed_feature_count": 0,
            "missing_feature_ids": list(GRAPH_RELATIONAL_FEATURE_IDS),
        },
        "snapshot_freshness": {},
        "coverage": {"symbols": [], "relationship_types": []},
        "snapshots": [],
        "pit_status": {},
        "warnings": warnings,
    }
    if not _table_exists(con, "graph_relational_snapshots"):
        warnings.append("graph_relational_snapshots table unavailable")
        base["pit_status"] = _pit_detail(GRAPH_RELATIONAL_GROUP, {}, now_ms=now_ms, available=False)
        return base

    where_sql = ""
    params: list[Any] = []
    sym = _norm_symbol(symbol)
    if sym:
        where_sql = "WHERE symbol = ?"
        params.append(sym)
    row = _query_one(
        con,
        f"""
        SELECT COUNT(*) AS snapshot_count,
               COUNT(DISTINCT symbol) AS symbol_count,
               MAX(ts_ms) AS latest_snapshot_ts_ms,
               MAX(created_ts_ms) AS latest_created_ts_ms
        FROM graph_relational_snapshots
        {where_sql}
        """,
        tuple(params),
    )
    snapshot_count = _safe_int(_row_get(row, "snapshot_count", 0), 0)
    symbol_count = _safe_int(_row_get(row, "symbol_count", 1), 0)
    latest_snapshot_ts_ms = _safe_int(_row_get(row, "latest_snapshot_ts_ms", 2), 0)
    latest_created_ts_ms = _safe_int(_row_get(row, "latest_created_ts_ms", 3), 0)
    latest_rows = _latest_graph_rows(con, symbol=symbol, limit=lineage_limit)
    latest = latest_rows[0] if latest_rows else {}
    source_meta = dict((latest or {}).get("metadata") or {})
    source_meta.update(dict((latest or {}).get("source_timestamps") or {}))
    pit = _pit_detail(GRAPH_RELATIONAL_GROUP, source_meta, now_ms=now_ms, available=snapshot_count > 0)
    status = "empty" if snapshot_count <= 0 else _status_from_pit(pit, empty_status="unavailable")
    if status == "stale":
        warnings.append("graph relational snapshots are stale under the PIT freshness policy")
    if snapshot_count <= 0:
        warnings.append("no graph relational snapshots are available")
    if not bool(base["enabled"]):
        warnings.append("USE_GRAPH_RELATIONAL_FEATURES is disabled; graph features are unavailable unless precomputed snapshots exist")
    if latest_rows and not bool((latest or {}).get("pit_safe")):
        warnings.append("latest graph snapshot is not point-in-time valid")
    if any(bool(row.get("direct_trading_authority")) for row in latest_rows):
        warnings.append("graph snapshot metadata unexpectedly claims direct trading authority")
    if any(str(row.get("stage") or "").lower() != "shadow" for row in latest_rows):
        warnings.append("graph snapshot metadata contains non-shadow stage values")

    relationship_totals: dict[str, int] = {}
    observed_features: set[str] = set()
    pit_valid_count = 0
    pit_invalid_count = 0
    shadow_only_count = 0
    for snap in latest_rows:
        observed_features.update(str(fid) for fid in list(snap.get("feature_ids") or []))
        for rel, count in dict(snap.get("edge_counts") or {}).items():
            relationship_totals[str(rel)] = relationship_totals.get(str(rel), 0) + _safe_int(count, 0)
        if bool(snap.get("pit_safe")):
            pit_valid_count += 1
        else:
            pit_invalid_count += 1
        if str(snap.get("stage") or "shadow").lower() == "shadow":
            shadow_only_count += 1

    expected_features = set(GRAPH_RELATIONAL_FEATURE_IDS)
    observed_expected = sorted(expected_features & observed_features)
    missing_features = sorted(expected_features - observed_features)

    base.update(
        {
            "available": bool(snapshot_count > 0 and status == "available" and bool((latest or {}).get("pit_safe", True))),
            "status": "shadow_only" if snapshot_count > 0 and status == "available" else status,
            "counts": {
                "snapshots": int(snapshot_count),
                "symbols": int(symbol_count),
            },
            "latest_snapshot_ts_ms": latest_snapshot_ts_ms or None,
            "latest_created_ts_ms": latest_created_ts_ms or None,
            "latest_snapshot_age_ms": _age_ms(now_ms, latest_snapshot_ts_ms),
            "feature_availability": {
                "expected_feature_count": len(GRAPH_RELATIONAL_FEATURE_IDS),
                "observed_feature_count": len(observed_expected),
                "observed_feature_ids": observed_expected,
                "missing_feature_ids": missing_features,
            },
            "snapshot_freshness": {
                "latest_snapshot_ts_ms": latest_snapshot_ts_ms or None,
                "latest_created_ts_ms": latest_created_ts_ms or None,
                "age_ms": _age_ms(now_ms, latest_snapshot_ts_ms),
                "freshness_ttl_ms": int(policy.freshness_ttl_ms),
                "stale": "feature_stale" in set(str(code) for code in list(pit.get("reason_codes") or [])),
            },
            "coverage": {
                "symbols": _group_rows(
                    con,
                    table="graph_relational_snapshots",
                    column="symbol",
                    latest_column="ts_ms",
                    where_sql=("symbol = ?" if sym else ""),
                    params=([sym] if sym else []),
                    limit=50,
                ),
                "relationship_types": [
                    {"relationship_type": rel, "count": int(count)}
                    for rel, count in sorted(relationship_totals.items(), key=lambda item: (-int(item[1]), item[0]))
                ],
            },
            "snapshots": latest_rows,
            "pit_status": {
                **dict(pit),
                "latest_snapshot_pit_safe": bool((latest or {}).get("pit_safe", False)),
                "pit_valid_snapshot_count": int(pit_valid_count),
                "pit_invalid_snapshot_count": int(pit_invalid_count),
                "shadow_only_snapshot_count": int(shadow_only_count),
            },
        }
    )
    return base


def build_feature_visibility(
    *,
    con: Any = None,
    now_ms: int | None = None,
    symbol: str = "",
    low_confidence_threshold: float = LOW_CONFIDENCE_THRESHOLD,
    lineage_limit: int = DEFAULT_LINEAGE_LIMIT,
) -> dict[str, Any]:
    owns = False
    if con is None:
        con = db_connect()
        owns = True
    anchor = int(now_ms or _now_ms())
    try:
        structured = structured_document_visibility(
            con,
            now_ms=anchor,
            symbol=symbol,
            low_confidence_threshold=low_confidence_threshold,
            lineage_limit=lineage_limit,
        )
        graph = graph_feature_visibility(
            con,
            now_ms=anchor,
            symbol=symbol,
            lineage_limit=lineage_limit,
        )
        warnings = [*list(structured.get("warnings") or []), *list(graph.get("warnings") or [])]
        ready = bool(structured.get("available") or graph.get("available") or graph.get("status") == "shadow_only")
        return {
            "ok": True,
            "ts_ms": int(anchor),
            "structured_documents": structured,
            "graph_features": graph,
            "explanation_paths": {
                "decision_detail_route": "/api/ui/decision",
                "attribution_sources": [
                    "decision.explain.prediction_explanation",
                    "decision.extra.prediction_explanation",
                    "alert.explain_json",
                    "trade_attribution_ledger[].signal_json",
                    "trade_attribution_ledger[].decision_json",
                    "portfolio_orders[].explain_json",
                    "prediction_explanations",
                ],
                "feature_prefixes": [STRUCTURED_DOC_PREFIX, GRAPH_RELATIONAL_PREFIX],
            },
            "meta": {
                "ready": ready,
                "status": "warning" if warnings else ("available" if ready else "unavailable"),
                "warnings": warnings,
                "feature_visibility_version": 1,
                "symbol": _norm_symbol(symbol) or None,
            },
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception:
                # no-op-guard: allow - read connection cleanup is best-effort.
                pass


def _structured_feature_visibility(
    con: Any,
    *,
    feature_id: str,
    symbol: str,
    decision_ts_ms: int,
) -> dict[str, Any]:
    base = {
        "family": "structured_documents",
        "feature_group": STRUCTURED_DOC_FEATURE_GROUP,
        "pit_group": STRUCTURED_DOC_PIT_GROUP,
        "shadow_only": True,
        "direct_trading_authority": False,
        "feature_available": False,
        "status": "unavailable",
        "point_in_time_valid": False,
        "confidence": {"max": None, "low_confidence_count": 0},
        "lineage": [],
        "source_artifact": "",
        "pit_status": {},
        "warnings": [],
    }
    if not symbol or decision_ts_ms <= 0:
        base["warnings"].append("decision symbol or timestamp unavailable")
        return base
    if not _table_exists(con, "structured_document_events"):
        base["warnings"].append("structured_document_events table unavailable")
        return base
    lineage = _structured_lineage_rows(
        con,
        symbol=symbol,
        feature_id=feature_id,
        decision_ts_ms=int(decision_ts_ms),
        limit=5,
    )
    latest_availability = max((_safe_int(row.get("availability_ts_ms"), 0) for row in lineage), default=0)
    latest_event = max((_safe_int(row.get("event_ts_ms"), 0) for row in lineage), default=0)
    max_confidence = max((_safe_float(row.get("extraction_confidence"), 0.0) for row in lineage), default=math.nan)
    low_count = sum(1 for row in lineage if _safe_float(row.get("extraction_confidence"), 0.0) < LOW_CONFIDENCE_THRESHOLD)
    source_meta = {
        "latest_event_ts_ms": latest_event or None,
        "latest_availability_ts_ms": latest_availability or None,
    }
    pit = evaluate_group_policy(
        group=STRUCTURED_DOC_PIT_GROUP,
        source_meta=source_meta,
        anchor_ts_ms=int(decision_ts_ms),
        available=bool(lineage),
    )
    status = "shadow_only" if bool(lineage) and bool(pit.get("ok")) else _status_from_pit(pit)
    if not lineage:
        base["warnings"].append("no PIT-eligible structured document lineage found for this feature")
    if "feature_stale" in set(str(code) for code in list(pit.get("reason_codes") or [])):
        base["warnings"].append("structured document lineage is stale at the decision timestamp")
    base.update(
        {
            "feature_available": bool(lineage and pit.get("ok")),
            "status": status,
            "point_in_time_valid": bool(lineage and pit.get("ok")),
            "confidence": {
                "max": (None if math.isnan(max_confidence) else float(max_confidence)),
                "low_confidence_count": int(low_count),
                "threshold": float(LOW_CONFIDENCE_THRESHOLD),
            },
            "lineage": lineage,
            "source_artifact": str((lineage[0] or {}).get("source_artifact") or "") if lineage else "",
            "pit_status": pit,
        }
    )
    return base


def _graph_feature_visibility(
    con: Any,
    *,
    feature_id: str,
    symbol: str,
    decision_ts_ms: int,
) -> dict[str, Any]:
    base = {
        "family": "graph_features",
        "feature_group": GRAPH_RELATIONAL_GROUP,
        "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
        "shadow_only": True,
        "direct_trading_authority": False,
        "feature_available": False,
        "status": "unavailable",
        "point_in_time_valid": False,
        "confidence": {"max": None},
        "lineage": [],
        "source_artifact": "",
        "pit_status": {},
        "warnings": [],
    }
    if not symbol or decision_ts_ms <= 0:
        base["warnings"].append("decision symbol or timestamp unavailable")
        return base
    if not _table_exists(con, "graph_relational_snapshots"):
        base["warnings"].append("graph_relational_snapshots table unavailable")
        return base
    rows = _query_all(
        con,
        """
        SELECT symbol, ts_ms, graph_id, snapshot_version, feature_ids_json,
               features_json, edge_counts_json, relationships_json,
               source_timestamps_json, availability_json, metadata_json, created_ts_ms
        FROM graph_relational_snapshots
        WHERE symbol = ?
          AND graph_id = ?
          AND ts_ms <= ?
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        (_norm_symbol(symbol), GRAPH_RELATIONAL_GRAPH_ID, int(decision_ts_ms)),
    )
    if not rows:
        base["warnings"].append("no graph relational snapshot exists at or before the decision timestamp")
        return base
    snap = _latest_graph_rows_from_query_rows(rows)[0]
    feature_ids = {str(fid) for fid in list(snap.get("feature_ids") or [])}
    source_meta = dict(snap.get("metadata") or {})
    source_meta.update(dict(snap.get("source_timestamps") or {}))
    pit = evaluate_group_policy(
        group=GRAPH_RELATIONAL_GROUP,
        source_meta=source_meta,
        anchor_ts_ms=int(decision_ts_ms),
        available=bool(snap.get("pit_safe")),
    )
    available = bool(feature_id in feature_ids and snap.get("pit_safe") and pit.get("ok"))
    if feature_id not in feature_ids:
        base["warnings"].append("graph feature id not present in snapshot feature contract")
    if not bool(snap.get("pit_safe")):
        base["warnings"].append("graph snapshot is not point-in-time safe")
    if "feature_stale" in set(str(code) for code in list(pit.get("reason_codes") or [])):
        base["warnings"].append("graph snapshot is stale at the decision timestamp")
    if not bool(graph_relational_features_enabled()):
        base["warnings"].append("USE_GRAPH_RELATIONAL_FEATURES is disabled; feature remains shadow-only")
    value = _safe_float(dict(snap.get("features") or {}).get(feature_id), 0.0)
    base.update(
        {
            "feature_available": bool(available),
            "status": "shadow_only" if available else _status_from_pit(pit),
            "point_in_time_valid": bool(available),
            "value": float(value),
            "lineage": [snap],
            "source_artifact": str(snap.get("source_artifact") or ""),
            "pit_status": {
                **dict(pit),
                "snapshot_ts_ms": snap.get("ts_ms"),
                "relationship_hash": str(snap.get("relationship_hash") or ""),
            },
        }
    )
    return base


def _latest_graph_rows_from_query_rows(rows: Sequence[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        feature_ids = [str(fid) for fid in _json_list(_row_get(row, "feature_ids_json", 4))]
        features = _json_obj(_row_get(row, "features_json", 5))
        edge_counts = {str(k): _safe_int(v, 0) for k, v in _json_obj(_row_get(row, "edge_counts_json", 6)).items()}
        relationships = _json_list(_row_get(row, "relationships_json", 7))
        source_timestamps = _json_obj(_row_get(row, "source_timestamps_json", 8))
        availability = _json_obj(_row_get(row, "availability_json", 9))
        metadata = _json_obj(_row_get(row, "metadata_json", 10))
        ts_ms = _safe_int(_row_get(row, "ts_ms", 1), 0)
        max_source = _safe_int(metadata.get("max_source_ts_ms") or source_timestamps.get("max_source_ts_ms"), 0)
        max_availability = _safe_int(
            metadata.get("max_availability_ts_ms") or source_timestamps.get("max_availability_ts_ms"),
            0,
        )
        pit_safe = _safe_bool(metadata.get("pit_safe"), False) and (
            (max_source <= 0 or max_source <= ts_ms) and (max_availability <= 0 or max_availability <= ts_ms)
        )
        symbol = str(_row_get(row, "symbol", 0) or "")
        graph_id = str(_row_get(row, "graph_id", 2) or "")
        out.append(
            {
                "symbol": symbol,
                "ts_ms": ts_ms or None,
                "graph_id": graph_id,
                "snapshot_version": _safe_int(_row_get(row, "snapshot_version", 3), 0),
                "feature_ids": feature_ids,
                "features": features,
                "edge_counts": edge_counts,
                "relationship_count": int(sum(edge_counts.values()) or len(relationships or [])),
                "source_timestamps": source_timestamps,
                "availability": availability,
                "metadata": metadata,
                "created_ts_ms": _safe_int(_row_get(row, "created_ts_ms", 11), 0) or None,
                "pit_safe": bool(pit_safe),
                "stage": str(metadata.get("stage") or "shadow"),
                "direct_trading_authority": _safe_bool(metadata.get("direct_trading_authority"), False),
                "relationship_hash": str(metadata.get("relationship_hash") or source_timestamps.get("relationship_hash") or ""),
                "source_artifact": f"graph_relational_snapshots:{symbol}:{ts_ms}:{graph_id}" if ts_ms > 0 else "",
            }
        )
    return out


def feature_visibility_for_feature(
    con: Any,
    *,
    feature_id: str,
    symbol: str,
    decision_ts_ms: int,
) -> dict[str, Any] | None:
    fid = str(feature_id or "").strip()
    if fid.startswith(STRUCTURED_DOC_PREFIX):
        return _structured_feature_visibility(
            con,
            feature_id=fid,
            symbol=symbol,
            decision_ts_ms=int(decision_ts_ms or 0),
        )
    if fid.startswith(GRAPH_RELATIONAL_PREFIX):
        return _graph_feature_visibility(
            con,
            feature_id=fid,
            symbol=symbol,
            decision_ts_ms=int(decision_ts_ms or 0),
        )
    return None


def annotate_attribution_feature_visibility(
    attribution: Mapping[str, Any],
    *,
    con: Any,
    decision: Mapping[str, Any] | None,
) -> dict[str, Any]:
    out = dict(attribution or {})
    rows = []
    groups: dict[str, int] = {}
    symbol = _norm_symbol((decision or {}).get("symbol"))
    decision_ts_ms = _safe_int((decision or {}).get("ts_ms"), 0)
    for raw in list(out.get("top_features") or []):
        item = dict(raw or {})
        feature_id = str(item.get("feature_id") or item.get("feature") or item.get("name") or "")
        visibility = feature_visibility_for_feature(
            con,
            feature_id=feature_id,
            symbol=symbol,
            decision_ts_ms=int(decision_ts_ms),
        )
        if visibility is not None:
            item["feature_visibility"] = visibility
            family = str(visibility.get("family") or "unknown")
            groups[family] = groups.get(family, 0) + 1
        rows.append(item)
    out["top_features"] = rows
    out["feature_visibility_summary"] = {
        "annotated_feature_count": int(sum(groups.values())),
        "groups": groups,
        "shadow_only": bool(groups),
        "direct_trading_authority": False if groups else None,
    }
    return out


__all__ = [
    "LOW_CONFIDENCE_THRESHOLD",
    "annotate_attribution_feature_visibility",
    "build_feature_visibility",
    "feature_visibility_for_feature",
    "graph_feature_visibility",
    "structured_document_visibility",
]
