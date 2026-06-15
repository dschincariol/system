from __future__ import annotations

"""Read-only historical day replay aggregation."""

import json
from datetime import date as _date, datetime, time as _time, timedelta, timezone
from math import ceil
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from engine.api.api_market import _build_candles_from_rows, _tf_to_ms
from engine.api.http_parsing import qs as _qs
from engine.runtime.storage import connect_ro
from engine.runtime.storage_pool import is_storage_acquisition_error, storage_unavailable_payload


ROUTE_SPECS_REPLAY = [
    ("GET", "/api/replay/day", "api_get_replay_day"),
]

_DEFAULT_TZ = "America/New_York"
_MAX_POINTS = 5000
_MAX_EVENTS = 5000
_MAX_SOURCE_ROWS = 100_000


def _parse_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    return max(int(minimum), min(int(maximum), out))


def _add_gap(
    gaps: List[Dict[str, Any]],
    *,
    stream: str,
    code: str,
    message: str,
    severity: str = "warn",
    **extra: Any,
) -> None:
    gap = {
        "stream": str(stream),
        "code": str(code),
        "message": str(message),
        "severity": str(severity),
    }
    gap.update({k: v for k, v in extra.items() if v is not None})
    gaps.append(gap)


def _parse_day_range(q: Dict[str, str], gaps: List[Dict[str, Any]]) -> Tuple[str, str, int, int]:
    tz_name = str(q.get("tz") or _DEFAULT_TZ).strip() or _DEFAULT_TZ
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo(_DEFAULT_TZ)
        _add_gap(
            gaps,
            stream="meta",
            code="invalid_timezone",
            message=f"Unsupported timezone {tz_name}; using {_DEFAULT_TZ}.",
            severity="info",
        )
        tz_name = _DEFAULT_TZ

    raw_date = str(q.get("date") or "").strip()
    if raw_date:
        try:
            day = _date.fromisoformat(raw_date)
        except Exception:
            day = datetime.now(tz).date()
            _add_gap(
                gaps,
                stream="meta",
                code="invalid_date",
                message=f"Unsupported replay date {raw_date}; using {day.isoformat()}.",
                severity="warn",
            )
    else:
        day = datetime.now(tz).date()

    start = datetime.combine(day, _time.min, tzinfo=tz)
    end = start + timedelta(days=1)
    return day.isoformat(), tz_name, int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _table_exists(con: Any, name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(name),),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_columns(con: Any, name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({name})").fetchall() or []
    except Exception:
        return set()
    out: set[str] = set()
    for row in rows:
        try:
            out.add(str(row[1]))
        except Exception:
            try:
                out.add(str(row["name"]))
            except Exception:
                continue
    return out


def _expr(cols: set[str], name: str, alias: str, default_sql: str = "NULL") -> str:
    if name in cols:
        return f"{name} AS {alias}"
    return f"{default_sql} AS {alias}"


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        out = int(value)
    except Exception:
        return None
    return out if out > 0 else None


def _parse_json_obj(raw: Any, gaps: List[Dict[str, Any]], *, stream: str, field: str, row_id: Any) -> Dict[str, Any]:
    if raw in (None, ""):
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(str(raw))
    except Exception:
        _add_gap(
            gaps,
            stream=stream,
            code="malformed_json",
            message=f"Could not parse {field} on {stream} row {row_id}.",
            severity="warn",
            row_id=row_id,
            field=field,
        )
        return {"raw": str(raw)}
    return dict(parsed) if isinstance(parsed, dict) else {"value": parsed}


def _bounded_rows(rows: Sequence[Any], max_points: int) -> List[Any]:
    items = list(rows or [])
    if len(items) <= int(max_points):
        return items
    step = max(1, int(ceil(len(items) / float(max_points))))
    sampled = items[::step]
    if sampled and items[-1] is not sampled[-1]:
        sampled[-1] = items[-1]
    return sampled[: int(max_points)]


def _source_row_limit(max_points: int) -> int:
    return max(500, min(_MAX_SOURCE_ROWS, int(max_points) * 50))


def _query_price_bars(
    con: Any,
    *,
    symbol: str,
    tf_ms: int,
    start_ts_ms: int,
    end_ts_ms: int,
    max_points: int,
    gaps: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if not _table_exists(con, "price_bars"):
        return [], None
    cols = _table_columns(con, "price_bars")
    required = {"ts_ms", "symbol", "o", "h", "l", "c"}
    if not required.issubset(cols):
        _add_gap(
            gaps,
            stream="price",
            code="price_bars_malformed",
            message="price_bars exists but lacks required OHLC columns.",
        )
        return [], "price_bars"
    tf_s = int(max(1, round(tf_ms / 1000)))
    where = ["symbol=?", "ts_ms>=?", "ts_ms<?"]
    params: List[Any] = [symbol, int(start_ts_ms), int(end_ts_ms)]
    if "tf_s" in cols:
        where.append("tf_s=?")
        params.append(tf_s)
    params.append(_source_row_limit(max_points))
    rows = con.execute(
        f"""
        SELECT ts_ms, o, h, l, c, {'v' if 'v' in cols else 'NULL'} AS v
        FROM price_bars
        WHERE {' AND '.join(where)}
        ORDER BY ts_ms ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall() or []
    candles = []
    malformed = 0
    for row in rows:
        ts_ms = _to_int(row[0])
        o = _to_float(row[1])
        h = _to_float(row[2])
        low = _to_float(row[3])
        c = _to_float(row[4])
        if ts_ms is None or o is None or h is None or low is None or c is None:
            malformed += 1
            continue
        v = _to_float(row[5]) or 0.0
        candles.append(
            {
                "ts": int(ts_ms),
                "ts_ms": int(ts_ms),
                "t": int(ts_ms // 1000),
                "time": int(ts_ms // 1000),
                "open": float(o),
                "high": float(h),
                "low": float(low),
                "close": float(c),
                "volume": float(v),
                "source": "price_bars",
            }
        )
    if malformed:
        _add_gap(
            gaps,
            stream="price",
            code="malformed_price_bars",
            message=f"Skipped {malformed} malformed price_bars rows.",
            malformed_count=malformed,
        )
    return _bounded_rows(candles, max_points), "price_bars"


def _query_price_snapshots(
    con: Any,
    *,
    symbol: str,
    tf_ms: int,
    start_ts_ms: int,
    end_ts_ms: int,
    max_points: int,
    gaps: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    candidates = (
        ("price_quotes", "last", "volume"),
        ("price_quotes_raw", "last", "volume"),
        ("prices", "price", None),
        ("prices", "px", None),
    )
    source_tables = [name for name, _price, _vol in candidates if _table_exists(con, name)]
    for table, price_col, volume_col in candidates:
        if not _table_exists(con, table):
            continue
        cols = _table_columns(con, table)
        if "ts_ms" not in cols or "symbol" not in cols or price_col not in cols:
            continue
        vol_sql = volume_col if volume_col and volume_col in cols else "NULL"
        rows = con.execute(
            f"""
            SELECT ts_ms, {price_col} AS price, {vol_sql} AS volume
            FROM {table}
            WHERE symbol=?
              AND ts_ms>=?
              AND ts_ms<?
            ORDER BY ts_ms ASC
            LIMIT ?
            """,
            (symbol, int(start_ts_ms), int(end_ts_ms), _source_row_limit(max_points)),
        ).fetchall() or []
        parsed_rows: List[Tuple[int, Optional[float], Optional[float]]] = []
        malformed = 0
        for row in rows:
            ts_ms = _to_int(row[0])
            price = _to_float(row[1])
            volume = _to_float(row[2])
            if ts_ms is None or price is None:
                malformed += 1
                continue
            parsed_rows.append((int(ts_ms), float(price), volume))
        if malformed:
            _add_gap(
                gaps,
                stream="price",
                code="malformed_price_rows",
                message=f"Skipped {malformed} malformed {table} rows.",
                malformed_count=malformed,
                source_table=table,
            )
        if not parsed_rows:
            continue
        candles = _build_candles_from_rows(parsed_rows, tf_ms=int(tf_ms))
        for candle in candles:
            candle["time"] = int(candle.get("t") or (int(candle.get("ts_ms") or 0) // 1000))
            candle["source"] = table
        return _bounded_rows(candles, max_points), table
    return [], (source_tables[0] if source_tables else None)


def _query_candles(
    con: Any,
    *,
    symbol: str,
    tf: str,
    start_ts_ms: int,
    end_ts_ms: int,
    max_points: int,
    gaps: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not symbol:
        _add_gap(gaps, stream="price", code="missing_symbol", message="Price replay requires a symbol.")
        return [], {"source": None, "ready": False}
    tf_ms = _tf_to_ms(tf)
    candles, source = _query_price_bars(
        con,
        symbol=symbol,
        tf_ms=tf_ms,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        max_points=max_points,
        gaps=gaps,
    )
    if not candles:
        candles, source = _query_price_snapshots(
            con,
            symbol=symbol,
            tf_ms=tf_ms,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            max_points=max_points,
            gaps=gaps,
        )
    if not candles:
        code = "price_table_missing" if not source else "no_price_data"
        message = "No supported price table is available." if not source else f"No {symbol} price data for the selected day."
        _add_gap(gaps, stream="price", code=code, message=message, source_table=source)
    return candles, {"source": source, "ready": bool(candles), "tf_ms": int(tf_ms)}


def _append_where_filter(
    where: List[str],
    params: List[Any],
    *,
    cols: set[str],
    column: str,
    value: str,
    stream: str,
    gaps: List[Dict[str, Any]],
) -> bool:
    if not value:
        return True
    if column not in cols:
        _add_gap(
            gaps,
            stream=stream,
            code="filter_unsupported",
            message=f"{stream} source lacks {column}; filtered rows from that source were skipped.",
            severity="info",
            column=column,
        )
        return False
    where.append(f"{column}=?")
    params.append(str(value))
    return True


def _query_decisions(
    con: Any,
    *,
    symbol: str,
    model_id: str,
    start_ts_ms: int,
    end_ts_ms: int,
    limit: int,
    gaps: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not _table_exists(con, "decision_log"):
        _add_gap(gaps, stream="decisions", code="decision_log_missing", message="decision_log table is unavailable.")
        return [], {"source": None, "ready": False}
    cols = _table_columns(con, "decision_log")
    if "ts_ms" not in cols:
        _add_gap(gaps, stream="decisions", code="decision_log_malformed", message="decision_log lacks ts_ms.")
        return [], {"source": "decision_log", "ready": False}
    where = ["ts_ms>=?", "ts_ms<?"]
    params: List[Any] = [int(start_ts_ms), int(end_ts_ms)]
    if not _append_where_filter(where, params, cols=cols, column="symbol", value=symbol, stream="decisions", gaps=gaps):
        return [], {"source": "decision_log", "ready": False}
    if model_id:
        model_col = "model_name" if "model_name" in cols else ("model_id" if "model_id" in cols else "")
        if not model_col:
            _add_gap(gaps, stream="decisions", code="model_filter_unsupported", message="decision_log lacks a model column.")
            return [], {"source": "decision_log", "ready": False}
        where.append(f"{model_col}=?")
        params.append(model_id)
    params.append(int(limit))
    rows = con.execute(
        f"""
        SELECT
          {_expr(cols, 'id', 'id', 'NULL')},
          ts_ms AS ts_ms,
          {_expr(cols, 'symbol', 'symbol', "''")},
          {_expr(cols, 'model_name', 'model_name', "''")},
          {_expr(cols, 'model_kind', 'model_kind', "''")},
          {_expr(cols, 'horizon_s', 'horizon_s', 'NULL')},
          {_expr(cols, 'predicted_z', 'predicted_z', 'NULL')},
          {_expr(cols, 'confidence', 'confidence', 'NULL')},
          {_expr(cols, 'extra_json', 'extra_json', 'NULL')},
          {_expr(cols, 'explain_json', 'explain_json', 'NULL')},
          {_expr(cols, 'components_json', 'components_json', 'NULL')}
        FROM decision_log
        WHERE {' AND '.join(where)}
        ORDER BY ts_ms ASC, id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall() or []
    out: List[Dict[str, Any]] = []
    malformed = 0
    for row in rows:
        ts_ms = _to_int(row[1])
        if ts_ms is None:
            malformed += 1
            continue
        extra = _parse_json_obj(row[8], gaps, stream="decisions", field="extra_json", row_id=row[0])
        explain = _parse_json_obj(row[9], gaps, stream="decisions", field="explain_json", row_id=row[0])
        components = _parse_json_obj(row[10], gaps, stream="decisions", field="components_json", row_id=row[0])
        predicted_z = _to_float(row[6])
        inferred = "LONG" if (predicted_z or 0.0) > 0 else ("SHORT" if (predicted_z or 0.0) < 0 else "")
        label = str(
            extra.get("decision")
            or extra.get("action")
            or extra.get("side")
            or extra.get("signal")
            or inferred
            or "decision"
        )
        out.append(
            {
                "id": row[0],
                "ts_ms": int(ts_ms),
                "t": int(ts_ms // 1000),
                "symbol": str(row[2] or ""),
                "model_name": str(row[3] or ""),
                "model_kind": str(row[4] or ""),
                "horizon_s": _to_int(row[5]),
                "predicted_z": predicted_z,
                "confidence": _to_float(row[7]),
                "label": label,
                "extra": extra,
                "explain": explain,
                "components": components,
                "source_table": "decision_log",
            }
        )
    if malformed:
        _add_gap(gaps, stream="decisions", code="malformed_decision_rows", message=f"Skipped {malformed} malformed decision rows.")
    if not out:
        _add_gap(gaps, stream="decisions", code="no_decisions", message="No decision records for the selected filters.", severity="info")
    return out, {"source": "decision_log", "ready": bool(out)}


def _query_portfolio_orders(
    con: Any,
    *,
    symbol: str,
    model_id: str,
    start_ts_ms: int,
    end_ts_ms: int,
    limit: int,
    gaps: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not _table_exists(con, "portfolio_orders"):
        return []
    cols = _table_columns(con, "portfolio_orders")
    if "ts_ms" not in cols:
        _add_gap(gaps, stream="orders", code="portfolio_orders_malformed", message="portfolio_orders lacks ts_ms.")
        return []
    where = ["ts_ms>=?", "ts_ms<?"]
    params: List[Any] = [int(start_ts_ms), int(end_ts_ms)]
    if not _append_where_filter(where, params, cols=cols, column="symbol", value=symbol, stream="orders", gaps=gaps):
        return []
    if model_id and not _append_where_filter(where, params, cols=cols, column="model_id", value=model_id, stream="orders", gaps=gaps):
        return []
    params.append(int(limit))
    rows = con.execute(
        f"""
        SELECT
          {_expr(cols, 'id', 'id', 'NULL')},
          ts_ms AS ts_ms,
          {_expr(cols, 'model_id', 'model_id', "''")},
          {_expr(cols, 'symbol', 'symbol', "''")},
          {_expr(cols, 'action', 'action', "''")},
          {_expr(cols, 'from_side', 'from_side', "''")},
          {_expr(cols, 'to_side', 'to_side', "''")},
          {_expr(cols, 'delta_weight', 'delta_weight', 'NULL')},
          {_expr(cols, 'source_alert_id', 'source_alert_id', 'NULL')}
        FROM portfolio_orders
        WHERE {' AND '.join(where)}
        ORDER BY ts_ms ASC, id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall() or []
    out: List[Dict[str, Any]] = []
    for row in rows:
        ts_ms = _to_int(row[1])
        if ts_ms is None:
            continue
        out.append(
            {
                "id": row[0],
                "ts_ms": int(ts_ms),
                "t": int(ts_ms // 1000),
                "model_id": str(row[2] or ""),
                "symbol": str(row[3] or ""),
                "action": str(row[4] or ""),
                "from_side": str(row[5] or ""),
                "to_side": str(row[6] or ""),
                "delta_weight": _to_float(row[7]),
                "source_alert_id": row[8],
                "source_table": "portfolio_orders",
                "kind": "intent",
            }
        )
    return out


def _query_broker_orders(
    con: Any,
    *,
    symbol: str,
    model_id: str,
    start_ts_ms: int,
    end_ts_ms: int,
    limit: int,
    gaps: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not _table_exists(con, "broker_order_state"):
        return []
    if model_id:
        _add_gap(
            gaps,
            stream="orders",
            code="broker_orders_model_filter_unsupported",
            message="broker_order_state does not carry model_id; broker order rows were skipped for this model filter.",
            severity="info",
        )
        return []
    cols = _table_columns(con, "broker_order_state")
    ts_col = "created_ts_ms" if "created_ts_ms" in cols else ("updated_ts_ms" if "updated_ts_ms" in cols else "")
    if not ts_col:
        _add_gap(gaps, stream="orders", code="broker_order_state_malformed", message="broker_order_state lacks a timestamp column.")
        return []
    where = [f"{ts_col}>=?", f"{ts_col}<?"]
    params: List[Any] = [int(start_ts_ms), int(end_ts_ms)]
    if not _append_where_filter(where, params, cols=cols, column="symbol", value=symbol, stream="orders", gaps=gaps):
        return []
    params.append(int(limit))
    rows = con.execute(
        f"""
        SELECT
          {_expr(cols, 'id', 'id', 'NULL')},
          {ts_col} AS ts_ms,
          {_expr(cols, 'source_order_id', 'source_order_id', "''")},
          {_expr(cols, 'symbol', 'symbol', "''")},
          {_expr(cols, 'state', 'state', "''")},
          {_expr(cols, 'updated_ts_ms', 'updated_ts_ms', 'NULL')},
          {_expr(cols, 'meta_json', 'meta_json', 'NULL')}
        FROM broker_order_state
        WHERE {' AND '.join(where)}
        ORDER BY {ts_col} ASC, id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall() or []
    out: List[Dict[str, Any]] = []
    for row in rows:
        ts_ms = _to_int(row[1])
        if ts_ms is None:
            continue
        out.append(
            {
                "id": row[0],
                "ts_ms": int(ts_ms),
                "t": int(ts_ms // 1000),
                "source_order_id": str(row[2] or ""),
                "symbol": str(row[3] or ""),
                "state": str(row[4] or ""),
                "updated_ts_ms": _to_int(row[5]),
                "meta": _parse_json_obj(row[6], gaps, stream="orders", field="meta_json", row_id=row[0]),
                "source_table": "broker_order_state",
                "kind": "broker",
            }
        )
    return out


def _query_orders(
    con: Any,
    *,
    symbol: str,
    model_id: str,
    start_ts_ms: int,
    end_ts_ms: int,
    limit: int,
    gaps: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    tables = {
        "portfolio_orders": _table_exists(con, "portfolio_orders"),
        "broker_order_state": _table_exists(con, "broker_order_state"),
    }
    if not any(tables.values()):
        _add_gap(gaps, stream="orders", code="order_tables_missing", message="No supported order tables are available.")
        return [], {"tables": tables, "ready": False}
    if not all(tables.values()):
        _add_gap(gaps, stream="orders", code="partial_order_sources", message="Only part of the order lifecycle is available.", severity="info", tables=tables)
    rows = _query_portfolio_orders(
        con,
        symbol=symbol,
        model_id=model_id,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        limit=limit,
        gaps=gaps,
    )
    rows.extend(
        _query_broker_orders(
            con,
            symbol=symbol,
            model_id=model_id,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=limit,
            gaps=gaps,
        )
    )
    rows.sort(key=lambda item: int(item.get("ts_ms") or 0))
    if not rows:
        _add_gap(gaps, stream="orders", code="no_orders", message="No order records for the selected filters.", severity="info")
    return rows[:limit], {"tables": tables, "ready": bool(rows)}


def _query_fills_table(
    con: Any,
    table: str,
    *,
    symbol: str,
    model_id: str,
    start_ts_ms: int,
    end_ts_ms: int,
    limit: int,
    gaps: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not _table_exists(con, table):
        return []
    cols = _table_columns(con, table)
    if table == "execution_fills":
        ts_col = "fill_ts_ms" if "fill_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else "")
        qty_col = "fill_qty" if "fill_qty" in cols else ("qty" if "qty" in cols else "")
        px_col = "fill_px" if "fill_px" in cols else ("px" if "px" in cols else ("price" if "price" in cols else ""))
    elif table in {"broker_fills", "broker_fills_v2"}:
        ts_col = "ts_ms" if "ts_ms" in cols else ""
        qty_col = "qty" if "qty" in cols else ""
        px_col = "px" if "px" in cols else ("price" if "price" in cols else "")
    else:
        ts_col = "ts_ms" if "ts_ms" in cols else ("ts" if "ts" in cols else "")
        qty_col = "qty" if "qty" in cols else ""
        px_col = "price" if "price" in cols else ("px" if "px" in cols else "")
    if not ts_col or not qty_col or not px_col:
        _add_gap(gaps, stream="fills", code="fill_table_malformed", message=f"{table} lacks fill timestamp, quantity, or price columns.", source_table=table)
        return []
    where = [f"{ts_col}>=?", f"{ts_col}<?"]
    params: List[Any] = [int(start_ts_ms), int(end_ts_ms)]
    if not _append_where_filter(where, params, cols=cols, column="symbol", value=symbol, stream="fills", gaps=gaps):
        return []
    if model_id and "model_id" in cols:
        where.append("model_id=?")
        params.append(model_id)
    elif model_id:
        _add_gap(
            gaps,
            stream="fills",
            code="fills_model_filter_unsupported",
            message=f"{table} does not carry model_id; rows were skipped for this model filter.",
            severity="info",
            source_table=table,
        )
        return []
    params.append(int(limit))
    rows = con.execute(
        f"""
        SELECT
          {_expr(cols, 'id', 'id', 'NULL')},
          {ts_col} AS ts_ms,
          {_expr(cols, 'symbol', 'symbol', "''")},
          {qty_col} AS qty,
          {px_col} AS price,
          {_expr(cols, 'source_order_id', 'source_order_id', 'NULL')},
          {_expr(cols, 'client_order_id', 'client_order_id', 'NULL')},
          {_expr(cols, 'broker', 'broker', 'NULL')},
          {_expr(cols, 'note', 'note', 'NULL')},
          {_expr(cols, 'explain_json', 'explain_json', 'NULL')}
        FROM {table}
        WHERE {' AND '.join(where)}
        ORDER BY {ts_col} ASC, id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall() or []
    out: List[Dict[str, Any]] = []
    malformed = 0
    for row in rows:
        ts_ms = _to_int(row[1])
        qty = _to_float(row[3])
        price = _to_float(row[4])
        if ts_ms is None or qty is None or price is None:
            malformed += 1
            continue
        side = "BUY" if qty > 0 else ("SELL" if qty < 0 else "")
        out.append(
            {
                "id": row[0],
                "ts_ms": int(ts_ms),
                "t": int(ts_ms // 1000),
                "symbol": str(row[2] or ""),
                "qty": float(qty),
                "side": side,
                "price": float(price),
                "source_order_id": row[5],
                "client_order_id": row[6],
                "broker": row[7],
                "note": row[8],
                "explain": _parse_json_obj(row[9], gaps, stream="fills", field="explain_json", row_id=row[0]),
                "source_table": table,
            }
        )
    if malformed:
        _add_gap(gaps, stream="fills", code="malformed_fill_rows", message=f"Skipped {malformed} malformed {table} rows.", source_table=table)
    return out


def _query_fills(
    con: Any,
    *,
    symbol: str,
    model_id: str,
    start_ts_ms: int,
    end_ts_ms: int,
    limit: int,
    gaps: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    candidate_tables = ("execution_fills", "broker_fills_v2", "broker_fills", "trades")
    present = {table: _table_exists(con, table) for table in candidate_tables}
    if not any(present.values()):
        _add_gap(gaps, stream="fills", code="fill_tables_missing", message="No supported fill/trade tables are available.")
        return [], {"tables": present, "ready": False}
    if not (present.get("execution_fills") or present.get("broker_fills_v2") or present.get("broker_fills")):
        _add_gap(gaps, stream="fills", code="partial_fill_sources", message="Only legacy trade rows are available; broker fill coverage is missing.", severity="info", tables=present)
    rows: List[Dict[str, Any]] = []
    for table in candidate_tables:
        rows.extend(
            _query_fills_table(
                con,
                table,
                symbol=symbol,
                model_id=model_id,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                limit=limit,
                gaps=gaps,
            )
        )
    rows.sort(key=lambda item: int(item.get("ts_ms") or 0))
    if not rows:
        _add_gap(gaps, stream="fills", code="no_fills", message="No fill records for the selected filters.", severity="info")
    return rows[:limit], {"tables": present, "ready": bool(rows)}


def _query_risk(
    con: Any,
    *,
    start_ts_ms: int,
    end_ts_ms: int,
    limit: int,
    gaps: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    risk: List[Dict[str, Any]] = []
    pnl: List[Dict[str, Any]] = []
    sources = {
        "portfolio_risk_snapshots": _table_exists(con, "portfolio_risk_snapshots"),
        "equity_history": _table_exists(con, "equity_history"),
        "broker_account": _table_exists(con, "broker_account"),
    }
    if sources["portfolio_risk_snapshots"]:
        cols = _table_columns(con, "portfolio_risk_snapshots")
        if "ts_ms" in cols:
            rows = con.execute(
                f"""
                SELECT
                  ts_ms,
                  {_expr(cols, 'gross', 'gross', 'NULL')},
                  {_expr(cols, 'net', 'net', 'NULL')},
                  {_expr(cols, 'vol_proxy', 'vol_proxy', 'NULL')},
                  {_expr(cols, 'drawdown', 'drawdown', 'NULL')},
                  {_expr(cols, 'blocked', 'blocked', '0')},
                  {_expr(cols, 'info_json', 'info_json', 'NULL')}
                FROM portfolio_risk_snapshots
                WHERE ts_ms>=?
                  AND ts_ms<?
                ORDER BY ts_ms ASC
                LIMIT ?
                """,
                (int(start_ts_ms), int(end_ts_ms), int(limit)),
            ).fetchall() or []
            for row in rows:
                ts_ms = _to_int(row[0])
                if ts_ms is None:
                    continue
                risk.append(
                    {
                        "ts_ms": int(ts_ms),
                        "t": int(ts_ms // 1000),
                        "gross": _to_float(row[1]),
                        "net": _to_float(row[2]),
                        "vol_proxy": _to_float(row[3]),
                        "drawdown": _to_float(row[4]),
                        "blocked": bool(int(row[5] or 0)),
                        "info": _parse_json_obj(row[6], gaps, stream="risk", field="info_json", row_id=ts_ms),
                        "source_table": "portfolio_risk_snapshots",
                    }
                )
        else:
            _add_gap(gaps, stream="risk", code="risk_snapshots_malformed", message="portfolio_risk_snapshots lacks ts_ms.")
    if not risk:
        code = "risk_history_missing" if not sources["portfolio_risk_snapshots"] else "no_risk_snapshots"
        msg = "No risk history table is available." if not sources["portfolio_risk_snapshots"] else "No risk snapshots for the selected day."
        _add_gap(gaps, stream="risk", code=code, message=msg)

    if sources["equity_history"]:
        cols = _table_columns(con, "equity_history")
        if {"ts_ms", "equity"}.issubset(cols):
            rows = con.execute(
                """
                SELECT ts_ms, equity
                FROM equity_history
                WHERE ts_ms>=?
                  AND ts_ms<?
                ORDER BY ts_ms ASC
                LIMIT ?
                """,
                (int(start_ts_ms), int(end_ts_ms), int(limit)),
            ).fetchall() or []
            for row in rows:
                ts_ms = _to_int(row[0])
                equity = _to_float(row[1])
                if ts_ms is None or equity is None:
                    continue
                pnl.append(
                    {
                        "ts_ms": int(ts_ms),
                        "t": int(ts_ms // 1000),
                        "equity": float(equity),
                        "source_table": "equity_history",
                    }
                )
    if sources["broker_account"]:
        cols = _table_columns(con, "broker_account")
        ts_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else "")
        if ts_col:
            rows = con.execute(
                f"""
                SELECT
                  {ts_col} AS ts_ms,
                  {_expr(cols, 'equity', 'equity', 'NULL')},
                  {_expr(cols, 'cash', 'cash', 'NULL')},
                  {_expr(cols, 'day_pnl', 'day_pnl', 'NULL')},
                  {_expr(cols, 'unrealized_pnl', 'unrealized_pnl', 'NULL')},
                  {_expr(cols, 'realized_pnl', 'realized_pnl', 'NULL')}
                FROM broker_account
                WHERE {ts_col}>=?
                  AND {ts_col}<?
                ORDER BY {ts_col} ASC
                LIMIT ?
                """,
                (int(start_ts_ms), int(end_ts_ms), int(limit)),
            ).fetchall() or []
            for row in rows:
                ts_ms = _to_int(row[0])
                if ts_ms is None:
                    continue
                pnl.append(
                    {
                        "ts_ms": int(ts_ms),
                        "t": int(ts_ms // 1000),
                        "equity": _to_float(row[1]),
                        "cash": _to_float(row[2]),
                        "day_pnl": _to_float(row[3]),
                        "unrealized_pnl": _to_float(row[4]),
                        "realized_pnl": _to_float(row[5]),
                        "source_table": "broker_account",
                    }
                )
    pnl.sort(key=lambda item: int(item.get("ts_ms") or 0))
    if not pnl:
        _add_gap(gaps, stream="pnl", code="pnl_history_missing", message="No PnL/equity history is available.", severity="info")
    return risk, pnl[:limit], {"tables": sources, "ready": bool(risk or pnl)}


def _counts(**streams: Iterable[Any]) -> Dict[str, int]:
    return {name: int(len(list(values or []))) for name, values in streams.items()}


def api_get_replay_day(parsed: Any, _ctx=None) -> Dict[str, Any]:
    q = _qs(parsed)
    initial_gaps: List[Dict[str, Any]] = []
    day, tz_name, start_ts_ms, end_ts_ms = _parse_day_range(q, initial_gaps)
    symbol = str(q.get("symbol") or "").strip().upper()
    model_id = str(q.get("model_id") or q.get("model") or "").strip()
    tf = str(q.get("tf") or "1m").strip() or "1m"
    max_points = _parse_int(q.get("max_points") or q.get("limit") or "1500", 1500, minimum=50, maximum=_MAX_POINTS)
    event_limit = _parse_int(q.get("event_limit") or "1000", 1000, minimum=1, maximum=_MAX_EVENTS)

    gaps: List[Dict[str, Any]] = list(initial_gaps)
    con = None
    try:
        con = connect_ro()
        candles, price_source = _query_candles(
            con,
            symbol=symbol,
            tf=tf,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            max_points=max_points,
            gaps=gaps,
        )
        decisions, decision_source = _query_decisions(
            con,
            symbol=symbol,
            model_id=model_id,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=event_limit,
            gaps=gaps,
        )
        orders, order_source = _query_orders(
            con,
            symbol=symbol,
            model_id=model_id,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=event_limit,
            gaps=gaps,
        )
        fills, fill_source = _query_fills(
            con,
            symbol=symbol,
            model_id=model_id,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=event_limit,
            gaps=gaps,
        )
        risk, pnl, risk_source = _query_risk(
            con,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=event_limit,
            gaps=gaps,
        )
    except Exception as exc:
        if is_storage_acquisition_error(exc):
            return storage_unavailable_payload(endpoint="/api/replay/day", error=exc)
        raise
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _add_gap(
                gaps,
                stream="storage",
                code="connection_close_failed",
                message="Replay database connection close failed after query.",
                severity="warn",
                error_type=type(e).__name__,
            )

    counts = _counts(
        candles=candles,
        decisions=decisions,
        orders=orders,
        fills=fills,
        risk=risk,
        pnl=pnl,
    )
    ready = any(counts.values())
    if not ready:
        _add_gap(gaps, stream="replay", code="no_data_for_date", message="No replay data for the selected day and filters.")

    return {
        "ok": True,
        "mode": "replay",
        "read_only": True,
        "schema_version": 1,
        "date": day,
        "tz": tz_name,
        "symbol": symbol or None,
        "model_id": model_id or None,
        "tf": tf,
        "range": {
            "start_ts_ms": int(start_ts_ms),
            "end_ts_ms": int(end_ts_ms),
            "start_iso": datetime.fromtimestamp(start_ts_ms / 1000, timezone.utc).isoformat(),
            "end_iso": datetime.fromtimestamp(end_ts_ms / 1000, timezone.utc).isoformat(),
        },
        "filters": {
            "date": day,
            "symbol": symbol or None,
            "model_id": model_id or None,
            "tf": tf,
            "max_points": int(max_points),
            "event_limit": int(event_limit),
        },
        "candles": candles,
        "decisions": decisions,
        "orders": orders,
        "fills": fills,
        "risk": risk,
        "pnl": pnl,
        "streams": {
            "candles": candles,
            "decisions": decisions,
            "orders": orders,
            "fills": fills,
            "risk": risk,
            "pnl": pnl,
        },
        "gaps": gaps,
        "meta": {
            "ready": bool(ready),
            "read_only": True,
            "partial": bool(gaps),
            "counts": counts,
            "sources": {
                "price": price_source,
                "decisions": decision_source,
                "orders": order_source,
                "fills": fill_source,
                "risk": risk_source,
            },
        },
    }
