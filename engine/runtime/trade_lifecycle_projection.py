"""Projection helpers over durable order/fill events with legacy-table fallback."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple


def _table_exists(con, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _safe_json_loads(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _prefer_primary(primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(secondary or {})
    for key, value in dict(primary or {}).items():
        if value not in (None, "", [], {}):
            out[key] = value
        elif key not in out:
            out[key] = value
    return out


def _fetch_event_rows(
    con,
    *,
    client_order_id: Optional[str] = None,
    source_alert_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not _table_exists(con, "order_events"):
        return []
    if client_order_id in (None, "") and source_alert_id is None:
        return []

    rows = []
    if client_order_id not in (None, "") or source_alert_id is not None:
        clauses: List[str] = []
        params: List[Any] = []
        if client_order_id not in (None, ""):
            clauses.append("json_extract(payload_json, '$.client_order_id') = ?")
            params.append(str(client_order_id))
        if source_alert_id is not None:
            clauses.append("CAST(json_extract(payload_json, '$.source_alert_id') AS INTEGER) = ?")
            params.append(int(source_alert_id))
        sql = f"""
            SELECT id, ts_ms, command_id, batch_id, correlation_id, event_type, mode, broker, status, payload_json
            FROM order_events
            WHERE {" OR ".join(clauses)}
            ORDER BY ts_ms ASC, id ASC
        """
        try:
            rows = con.execute(sql, tuple(params)).fetchall() or []
        except Exception:
            rows = []

    if not rows:
        raw_rows = con.execute(
            """
            SELECT id, ts_ms, command_id, batch_id, correlation_id, event_type, mode, broker, status, payload_json
            FROM order_events
            ORDER BY ts_ms ASC, id ASC
            """
        ).fetchall() or []
        filtered = []
        for row in raw_rows:
            payload = _safe_json_loads(row[9]) if len(row) > 9 else None
            payload_client_order_id = None if not isinstance(payload, dict) else str(payload.get("client_order_id") or "").strip()
            payload_source_alert_id = None if not isinstance(payload, dict) else _safe_int(payload.get("source_alert_id"))
            if client_order_id not in (None, "") and payload_client_order_id == str(client_order_id):
                filtered.append(row)
                continue
            if source_alert_id is not None and payload_source_alert_id == int(source_alert_id):
                filtered.append(row)
        rows = filtered

    out: List[Dict[str, Any]] = []
    for row in rows:
        payload = _safe_json_loads(row[9])
        out.append(
            {
                "id": _safe_int(row[0]),
                "ts_ms": _safe_int(row[1]),
                "command_id": (str(row[2]) if row[2] not in (None, "") else None),
                "batch_id": _safe_int(row[3]),
                "correlation_id": (str(row[4]) if row[4] not in (None, "") else None),
                "event_type": str(row[5] or ""),
                "mode": str(row[6] or ""),
                "broker": str(row[7] or ""),
                "status": str(row[8] or ""),
                "payload": (payload if isinstance(payload, dict) else {}),
            }
        )
    return out


def _project_execution_order(event_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if str(event_row.get("event_type") or "") != "order_submit":
        return None
    payload = dict(event_row.get("payload") or {})
    client_order_id = str(payload.get("client_order_id") or "").strip()
    if not client_order_id:
        return None
    return {
        "client_order_id": client_order_id,
        "portfolio_orders_id": _safe_int(payload.get("portfolio_orders_id")),
        "source_alert_id": _safe_int(payload.get("source_alert_id")),
        "prediction_id": _safe_int(payload.get("prediction_id")),
        "model_id": str(payload.get("model_id") or "baseline"),
        "model_version": (str(payload.get("model_version")) if payload.get("model_version") not in (None, "") else None),
        "symbol": str(payload.get("symbol") or ""),
        "qty": _safe_float(payload.get("qty")),
        "submit_ts_ms": _safe_int(payload.get("submit_ts_ms")),
        "broker": str(payload.get("broker") or event_row.get("broker") or ""),
        "status": str(event_row.get("status") or payload.get("status") or "submitted"),
        "extra_json": dict(payload),
    }


def _project_fill(event_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if str(event_row.get("event_type") or "") != "fill":
        return None
    payload = dict(event_row.get("payload") or {})
    client_order_id = str(payload.get("client_order_id") or "").strip()
    if not client_order_id:
        return None
    return {
        "client_order_id": client_order_id,
        "fill_id": (str(payload.get("fill_id")) if payload.get("fill_id") not in (None, "") else None),
        "broker": str(payload.get("broker") or event_row.get("broker") or ""),
        "model_id": str(payload.get("model_id") or "baseline"),
        "model_version": (str(payload.get("model_version")) if payload.get("model_version") not in (None, "") else None),
        "symbol": str(payload.get("symbol") or ""),
        "portfolio_orders_id": _safe_int(payload.get("portfolio_orders_id")),
        "source_alert_id": _safe_int(payload.get("source_alert_id")),
        "prediction_id": _safe_int(payload.get("prediction_id")),
        "ts_ms": _safe_int(payload.get("fill_ts_ms") or payload.get("ts_ms")),
        "submit_ts_ms": _safe_int(payload.get("submit_ts_ms")),
        "fill_ts_ms": _safe_int(payload.get("fill_ts_ms")),
        "fill_qty": _safe_float(payload.get("fill_qty")),
        "fill_px": _safe_float(payload.get("fill_px")),
        "expected_px": _safe_float(payload.get("expected_px")),
        "mid_px": _safe_float(payload.get("mid_px")),
        "bid_px": _safe_float(payload.get("bid_px")),
        "ask_px": _safe_float(payload.get("ask_px")),
        "spread_bps": _safe_float(payload.get("spread_bps")),
        "slippage_bps": _safe_float(payload.get("slippage_bps")),
        "fill_latency_ms": _safe_int(payload.get("fill_latency_ms") or payload.get("latency_ms")),
        "fees": _safe_float(payload.get("fees")),
        "liquidity": (str(payload.get("liquidity")) if payload.get("liquidity") not in (None, "") else None),
        "extra_json": dict(payload),
    }


def _row_to_dict(row, columns: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not row:
        return out
    for idx, name in enumerate(columns):
        value = row[idx]
        if name.endswith("_json") and value not in (None, ""):
            parsed = _safe_json_loads(value)
            out[name] = parsed if parsed is not None else value
            continue
        out[name] = value
    return out


def _read_execution_orders_raw(
    con,
    *,
    client_order_id: Optional[str] = None,
    source_alert_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not _table_exists(con, "execution_orders"):
        return []
    columns = [
        "client_order_id",
        "portfolio_orders_id",
        "source_alert_id",
        "prediction_id",
        "model_id",
        "model_version",
        "symbol",
        "qty",
        "submit_ts_ms",
        "broker",
        "status",
        "extra_json",
    ]
    if client_order_id not in (None, ""):
        rows = con.execute(
            f"""
            SELECT {", ".join(columns)}
            FROM execution_orders
            WHERE client_order_id=?
            ORDER BY submit_ts_ms ASC, client_order_id ASC
            """,
            (str(client_order_id),),
        ).fetchall()
        return [_row_to_dict(row, columns) for row in (rows or [])]
    if source_alert_id is not None:
        rows = con.execute(
            f"""
            SELECT {", ".join(columns)}
            FROM execution_orders
            WHERE source_alert_id=?
            ORDER BY submit_ts_ms ASC, client_order_id ASC
            """,
            (int(source_alert_id),),
        ).fetchall()
        return [_row_to_dict(row, columns) for row in (rows or [])]
    return []


def _read_execution_fills_raw(con, client_order_ids: List[str]) -> List[Dict[str, Any]]:
    if not client_order_ids or not _table_exists(con, "execution_fills"):
        return []
    columns = [
        "client_order_id",
        "fill_id",
        "broker",
        "model_id",
        "model_version",
        "symbol",
        "portfolio_orders_id",
        "source_alert_id",
        "prediction_id",
        "ts_ms",
        "submit_ts_ms",
        "fill_ts_ms",
        "fill_qty",
        "fill_px",
        "expected_px",
        "mid_px",
        "bid_px",
        "ask_px",
        "spread_bps",
        "slippage_bps",
        "fill_latency_ms",
        "fees",
        "liquidity",
        "extra_json",
    ]
    placeholders = ",".join(["?"] * len(client_order_ids))
    rows = con.execute(
        f"""
        SELECT {", ".join(columns)}
        FROM execution_fills
        WHERE client_order_id IN ({placeholders})
        ORDER BY fill_ts_ms ASC, id ASC
        """,
        tuple(str(item) for item in client_order_ids),
    ).fetchall()
    return [_row_to_dict(row, columns) for row in (rows or [])]


def _fill_key(fill_row: Dict[str, Any]) -> Tuple[str, str]:
    client_order_id = str(fill_row.get("client_order_id") or "")
    fill_id = str(fill_row.get("fill_id") or fill_row.get("fill_ts_ms") or "")
    return (client_order_id, fill_id)


def _fetch_command_rows(
    con,
    *,
    batch_ids: List[int],
    correlation_ids: List[str],
) -> List[Dict[str, Any]]:
    if not _table_exists(con, "order_commands"):
        return []
    clauses: List[str] = []
    params: List[Any] = []
    if batch_ids:
        placeholders = ",".join(["?"] * len(batch_ids))
        clauses.append(f"batch_id IN ({placeholders})")
        params.extend(int(item) for item in batch_ids)
    if correlation_ids:
        placeholders = ",".join(["?"] * len(correlation_ids))
        clauses.append(f"correlation_id IN ({placeholders})")
        params.extend(str(item) for item in correlation_ids)
    if not clauses:
        return []
    rows = con.execute(
        f"""
        SELECT command_id, ts_ms, updated_ts_ms, batch_id, payload_ts_ms, correlation_id, mode, broker,
               payload_source, status, real_order_count, shadow_order_count, blocked_order_count, command_json, result_json
        FROM order_commands
        WHERE {" OR ".join(clauses)}
        ORDER BY ts_ms ASC, command_id ASC
        """,
        tuple(params),
    ).fetchall() or []
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "command_id": str(row[0] or ""),
                "ts_ms": _safe_int(row[1]),
                "updated_ts_ms": _safe_int(row[2]),
                "batch_id": _safe_int(row[3]),
                "payload_ts_ms": _safe_int(row[4]),
                "correlation_id": (str(row[5]) if row[5] not in (None, "") else None),
                "mode": str(row[6] or ""),
                "broker": str(row[7] or ""),
                "payload_source": str(row[8] or ""),
                "status": str(row[9] or ""),
                "real_order_count": _safe_int(row[10]),
                "shadow_order_count": _safe_int(row[11]),
                "blocked_order_count": _safe_int(row[12]),
                "command_json": (_safe_json_loads(row[13]) if row[13] not in (None, "") else {}),
                "result_json": (_safe_json_loads(row[14]) if row[14] not in (None, "") else {}),
            }
        )
    return out


def read_trade_lifecycle_projection(
    con,
    *,
    source_alert_id: Optional[int] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    event_rows = _fetch_event_rows(
        con,
        client_order_id=client_order_id,
        source_alert_id=source_alert_id,
    )

    projected_orders: Dict[str, Dict[str, Any]] = {}
    projected_fills: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in event_rows:
        order_row = _project_execution_order(row)
        if order_row:
            projected_orders[str(order_row.get("client_order_id") or "")] = order_row
        fill_row = _project_fill(row)
        if fill_row:
            projected_fills[_fill_key(fill_row)] = fill_row

    raw_orders = _read_execution_orders_raw(
        con,
        client_order_id=client_order_id,
        source_alert_id=source_alert_id,
    )

    merged_orders: Dict[str, Dict[str, Any]] = {}
    for client_id, projected in projected_orders.items():
        merged_orders[str(client_id)] = dict(projected)
    for raw in raw_orders:
        client_id = str(raw.get("client_order_id") or "")
        merged_orders[client_id] = _prefer_primary(dict(raw), merged_orders.get(client_id, {}))

    client_order_ids = sorted(
        {
            str(client_order_id)
            for client_order_id in (
                list(merged_orders.keys())
                + ([str(client_order_id)] if client_order_id not in (None, "") else [])
            )
            if client_order_id
        }
    )
    raw_fills = _read_execution_fills_raw(con, client_order_ids)

    merged_fills: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for key, projected in projected_fills.items():
        merged_fills[key] = dict(projected)
    for raw in raw_fills:
        key = _fill_key(raw)
        merged_fills[key] = _prefer_primary(dict(raw), merged_fills.get(key, {}))

    batch_ids = sorted(
        {
            int(batch_id)
            for batch_id in (
                [row.get("batch_id") for row in event_rows]
                + [row.get("portfolio_orders_id") for row in merged_orders.values()]
            )
            if batch_id not in (None, "")
        }
    )
    correlation_ids = sorted(
        {
            str(correlation_id)
            for correlation_id in (
                [row.get("correlation_id") for row in event_rows]
                + client_order_ids
            )
            if correlation_id not in (None, "")
        }
    )
    command_rows = _fetch_command_rows(
        con,
        batch_ids=batch_ids,
        correlation_ids=correlation_ids,
    )

    execution_orders = sorted(
        list(merged_orders.values()),
        key=lambda item: (
            _safe_int(item.get("submit_ts_ms")) or 0,
            str(item.get("client_order_id") or ""),
        ),
    )
    fills = sorted(
        list(merged_fills.values()),
        key=lambda item: (
            _safe_int(item.get("fill_ts_ms")) or 0,
            str(item.get("fill_id") or ""),
            str(item.get("client_order_id") or ""),
        ),
    )

    return {
        "order_events": list(event_rows),
        "order_commands": list(command_rows),
        "execution_orders": execution_orders,
        "fills": fills,
    }


def find_latest_execution_order_projection(
    con,
    *,
    source_alert_id: int,
    model_id: str,
    symbol: str,
) -> Optional[Dict[str, Any]]:
    projection = read_trade_lifecycle_projection(
        con,
        source_alert_id=int(source_alert_id),
    )
    target_model_id = str(model_id or "").strip() or "baseline"
    target_symbol = str(symbol or "").strip().upper()
    candidates = [
        dict(item)
        for item in list(projection.get("execution_orders") or [])
        if str(item.get("model_id") or "").strip() == target_model_id
        and str(item.get("symbol") or "").strip().upper() == target_symbol
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            _safe_int(item.get("submit_ts_ms")) or 0,
            str(item.get("client_order_id") or ""),
        ),
        reverse=True,
    )
    return dict(candidates[0])
