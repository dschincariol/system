"""Read-only execution diagnostics serializers for operator UI surfaces."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import json
import math
import re
import time
from typing import Any, Iterable, Mapping

from engine.execution.contextual_bandit_slicer import (
    POLICY_NAME,
    POLICY_SCOPE,
    evaluate_against_baselines,
    learned_execution_enabled,
)
from engine.execution.lob_simulation import (
    deeplob_shadow_enabled,
    l2_data_quality_snapshot,
    latency_assumption_snapshot,
    lob_deeplob_readiness_snapshot,
    simulator_calibration_snapshot,
)
from engine.runtime.storage import connect_ro


FRESH_MS = 5 * 60_000
STALE_MS = 30 * 60_000
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class SourceState:
    """Availability metadata for a route/table backing an execution surface."""

    route: str
    state: str
    payload: str
    description: str
    table: str | None = None
    rows: int | None = None
    latest_ts_ms: int | None = None
    age_ms: int | None = None
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out = {
            "route": self.route,
            "state": self.state,
            "payload": self.payload,
            "description": self.description,
        }
        if self.table is not None:
            out["table"] = self.table
        if self.rows is not None:
            out["rows"] = int(self.rows)
        if self.latest_ts_ms is not None:
            out["latest_ts_ms"] = int(self.latest_ts_ms)
        if self.age_ms is not None:
            out["age_ms"] = int(self.age_ms)
        if self.reason:
            out["reason"] = self.reason
        return out


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value is None or value == "":
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _safe_int(value: Any, default: int | None = 0) -> int | None:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _humanize_reason(reason: Any) -> str:
    text = str(reason or "").strip()
    if not text:
        return "Reason unavailable."
    return text.replace("_", " ").replace(":", ": ")


def _ident(name: str) -> str:
    text = str(name or "").strip()
    if not _IDENT_RE.match(text):
        raise ValueError(f"invalid SQL identifier: {text!r}")
    return text


def _table_exists(con: Any, table: str) -> bool:
    name = _ident(table)
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (name,),
        ).fetchone()
        if row:
            return True
    except Exception:
        # no-op-guard: allow - SQLite probe failed; try PostgreSQL metadata next.
        pass
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ANY (current_schemas(false))
              AND table_name=?
            LIMIT 1
            """,
            (name,),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_columns(con: Any, table: str) -> set[str]:
    name = _ident(table)
    try:
        rows = con.execute(f"PRAGMA table_info({_ident(name)})").fetchall() or []
        cols = {str(row[1]) for row in rows if row and len(row) > 1 and row[1]}
        if cols:
            return cols
    except Exception:
        # no-op-guard: allow - SQLite PRAGMA failed; try PostgreSQL metadata next.
        pass
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ANY (current_schemas(false))
              AND table_name=?
            """,
            (name,),
        ).fetchall() or []
        return {str(row[0]) for row in rows if row and row[0]}
    except Exception:
        return set()


def _fetchall(con: Any, sql: str, params: Iterable[Any] = ()) -> list[Any]:
    try:
        return list(con.execute(sql, tuple(params)).fetchall() or [])
    except Exception:
        return []


def _fetchone(con: Any, sql: str, params: Iterable[Any] = ()) -> Any:
    try:
        return con.execute(sql, tuple(params)).fetchone()
    except Exception:
        return None


def _scalar(con: Any, sql: str, params: Iterable[Any] = (), default: Any = None) -> Any:
    row = _fetchone(con, sql, params)
    if not row:
        return default
    try:
        return row[0]
    except Exception:
        return default


def _column(cols: set[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None


def _state_from_latest(latest_ts_ms: Any, now_ms: int, *, rows: int = 0) -> tuple[str, int | None, str | None]:
    latest = _safe_int(latest_ts_ms, None)
    if not latest:
        if rows > 0:
            return "available", None, "timestamp_unavailable"
        return "unavailable", None, "no_rows"
    age = max(0, int(now_ms) - int(latest))
    if age > STALE_MS:
        return "stale", age, "latest_row_stale"
    return "fresh", age, None


def _source_state(
    con: Any,
    *,
    route: str,
    payload: str,
    description: str,
    table: str,
    ts_candidates: tuple[str, ...],
    now_ms: int,
) -> SourceState:
    if not _table_exists(con, table):
        return SourceState(
            route=route,
            state="unavailable",
            payload=payload,
            description=description,
            table=table,
            rows=0,
            reason=f"{table}_missing",
        )

    table_sql = _ident(table)
    cols = _table_columns(con, table)
    rows = int(_safe_int(_scalar(con, f"SELECT COUNT(*) FROM {table_sql}", default=0), 0) or 0)
    ts_col = _column(cols, *ts_candidates)
    latest = None
    if ts_col:
        latest = _safe_int(_scalar(con, f"SELECT MAX({_ident(ts_col)}) FROM {table_sql}", default=None), None)
    state, age_ms, reason = _state_from_latest(latest, now_ms, rows=rows)
    return SourceState(
        route=route,
        state=state,
        payload=payload,
        description=description,
        table=table,
        rows=rows,
        latest_ts_ms=latest,
        age_ms=age_ms,
        reason=reason,
    )


def _first_existing_table(con: Any, names: Iterable[str]) -> str | None:
    for name in names:
        if _table_exists(con, name):
            return str(name)
    return None


def _inventory(con: Any, now_ms: int) -> dict[str, Any]:
    fill_table = _first_existing_table(con, ("execution_fills", "broker_fills_v2", "broker_fills"))
    order_table = _first_existing_table(con, ("execution_orders", "broker_order_state", "portfolio_orders"))
    routes: list[SourceState] = []

    if order_table:
        routes.append(
            _source_state(
                con,
                route="/api/execution/stats",
                payload="orders/fills/metrics summary",
                description="Execution order and fill counts with freshness metadata.",
                table=order_table,
                ts_candidates=("submit_ts_ms", "updated_ts_ms", "created_ts_ms", "ts_ms"),
                now_ms=now_ms,
            )
        )
    else:
        routes.append(
            SourceState(
                route="/api/execution/stats",
                state="unavailable",
                payload="orders/fills/metrics summary",
                description="Execution order and fill counts with freshness metadata.",
                reason="execution_order_tables_missing",
            )
        )

    for route, payload, description in (
        ("/api/execution/metrics", "aggregate slippage, fees, latency, fill detection", "Aggregate execution cost and latency metrics."),
        ("/api/execution/metrics/rolling", "24h/7d rolling execution metrics", "Rolling execution cost and latency windows."),
        ("/api/execution/metrics/by_symbol", "symbol-level execution TCA", "By-symbol fill count, slippage, fee, and latency metrics."),
        ("/api/execution/metrics/by_confidence", "execution costs by confidence bucket", "Confidence-bucket execution cost diagnostics."),
    ):
        if fill_table:
            routes.append(
                _source_state(
                    con,
                    route=route,
                    payload=payload,
                    description=description,
                    table=fill_table,
                    ts_candidates=("fill_ts_ms", "ts_ms"),
                    now_ms=now_ms,
                )
            )
        else:
            routes.append(
                SourceState(
                    route=route,
                    state="unavailable",
                    payload=payload,
                    description=description,
                    reason="execution_fill_tables_missing",
                )
            )

    route_tables = (
        ("/api/execution/advisories", "execution_ai_advisory", ("ts_ms",), "advisory rows", "Execution AI advisory rows and operator action state."),
        ("/api/terminal/orders", "broker_order_state", ("updated_ts_ms", "created_ts_ms"), "broker orders", "Broker-side order state for terminal and blotter views."),
        ("/api/terminal/fills", fill_table or "broker_fills", ("fill_ts_ms", "ts_ms"), "broker fills", "Broker fill rows used by terminal and chart overlays."),
        ("/api/terminal/orders", "terminal_intent_rejections", ("ts_ms",), "rejected terminal intents", "Rejected terminal intents with reason codes."),
        ("/api/audit/records?table=trade_attribution_ledger", "trade_attribution_ledger", ("ts_ms",), "suppressed intents", "Trade attribution rows carrying suppression reasons."),
        ("/api/execution/diagnostics", "market_microstructure_signals", ("ts_ms",), "LOB and DeepLOB readiness", "L2 freshness and DeepLOB shadow-readiness diagnostics."),
        ("/api/execution/diagnostics", "execution_policy_audit", ("ts_ms",), "learned slicing evidence", "Execution policy audit rows carrying learned-slicing decisions."),
    )
    for route, table, ts_cols, payload, description in route_tables:
        if not table:
            continue
        routes.append(
            _source_state(
                con,
                route=route,
                payload=payload,
                description=description,
                table=str(table),
                ts_candidates=tuple(ts_cols),
                now_ms=now_ms,
            )
        )

    return {
        "routes": [item.as_dict() for item in routes],
        "summary": {
            "available": sum(1 for item in routes if item.state in {"fresh", "available"}),
            "fresh": sum(1 for item in routes if item.state == "fresh"),
            "stale": sum(1 for item in routes if item.state == "stale"),
            "unavailable": sum(1 for item in routes if item.state == "unavailable"),
        },
    }


def _metric_from_payload(payload: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _safe_float(payload.get(key), None)
        if value is not None:
            return float(value)
    return None


def _recent_execution_metric_rows(con: Any, limit: int) -> list[dict[str, Any]]:
    if not _table_exists(con, "execution_metrics"):
        return []
    rows = _fetchall(
        con,
        """
        SELECT ts_ms, client_order_id, broker, symbol, submit_qty, filled_qty,
               ref_px, expected_px, fill_px, fill_vwap, spread_bps,
               slippage_bps, fill_latency_ms, fees, m2m_pnl
        FROM execution_metrics
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    out = []
    for row in rows:
        out.append(
            {
                "ts_ms": _safe_int(row[0], 0),
                "client_order_id": str(row[1] or ""),
                "broker": str(row[2] or ""),
                "symbol": str(row[3] or "").upper(),
                "submit_qty": _safe_float(row[4], None),
                "filled_qty": _safe_float(row[5], None),
                "ref_px": _safe_float(row[6], None),
                "expected_px": _safe_float(row[7], None),
                "fill_px": _safe_float(row[8], None),
                "fill_vwap": _safe_float(row[9], None),
                "spread_bps": _safe_float(row[10], None),
                "slippage_bps": _safe_float(row[11], None),
                "fill_latency_ms": _safe_float(row[12], None),
                "fees": _safe_float(row[13], None),
                "m2m_pnl": _safe_float(row[14], None),
            }
        )
    return out


def _recent_execution_fill_rows(con: Any, limit: int) -> list[dict[str, Any]]:
    if _table_exists(con, "execution_fills"):
        rows = _fetchall(
            con,
            """
            SELECT client_order_id, broker, symbol, portfolio_orders_id, source_alert_id,
                   submit_ts_ms, fill_ts_ms, fill_qty, fill_px, expected_px, mid_px,
                   spread_bps, slippage_bps, fill_latency_ms, fees, extra_json
            FROM execution_fills
            ORDER BY fill_ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        out = []
        for row in rows:
            extra = _json_obj(row[15])
            out.append(
                {
                    "source": "execution_fills",
                    "client_order_id": str(row[0] or ""),
                    "broker": str(row[1] or ""),
                    "symbol": str(row[2] or "").upper(),
                    "portfolio_orders_id": _safe_int(row[3], None),
                    "source_alert_id": _safe_int(row[4], None),
                    "submit_ts_ms": _safe_int(row[5], None),
                    "ts_ms": _safe_int(row[6], 0),
                    "fill_qty": _safe_float(row[7], None),
                    "fill_px": _safe_float(row[8], None),
                    "expected_px": _safe_float(row[9], None),
                    "mid_px": _safe_float(row[10], None),
                    "spread_bps": _safe_float(row[11], None),
                    "slippage_bps": _safe_float(row[12], None),
                    "fill_latency_ms": _safe_float(row[13], None),
                    "fees": _safe_float(row[14], None),
                    "implementation_shortfall_bps": _metric_from_payload(
                        extra,
                        "implementation_shortfall_bps",
                        "shortfall_bps",
                        "arrival_slippage_bps",
                    ),
                    "vwap_px": _metric_from_payload(extra, "vwap_px", "fill_vwap", "vwap"),
                    "extra": extra,
                }
            )
        return out

    fills_table = _first_existing_table(con, ("broker_fills_v2", "broker_fills"))
    if not fills_table:
        return []
    cols = _table_columns(con, fills_table)
    ts_col = _column(cols, "ts_ms", "fill_ts_ms")
    qty_col = _column(cols, "qty", "fill_qty")
    px_col = _column(cols, "px", "fill_px", "price")
    if not ts_col or not qty_col or not px_col:
        return []
    explain_col = _column(cols, "explain_json", "extra_json", "raw_json")
    source_order_col = _column(cols, "source_order_id", "client_order_id")
    note_col = _column(cols, "note")
    rows = _fetchall(
        con,
        f"""
        SELECT {_ident(ts_col)}, symbol, {_ident(qty_col)}, {_ident(px_col)},
               {(_ident(source_order_col) if source_order_col else "NULL")},
               {(_ident(note_col) if note_col else "NULL")},
               {(_ident(explain_col) if explain_col else "NULL")}
        FROM {_ident(fills_table)}
        ORDER BY {_ident(ts_col)} DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    out = []
    for row in rows:
        extra = _json_obj(row[6])
        out.append(
            {
                "source": fills_table,
                "client_order_id": str(row[4] or ""),
                "broker": str(extra.get("broker") or extra.get("source") or ""),
                "symbol": str(row[1] or "").upper(),
                "portfolio_orders_id": _safe_int(row[4], None),
                "source_alert_id": _safe_int(extra.get("source_alert_id"), None),
                "submit_ts_ms": _safe_int(extra.get("submit_ts_ms"), None),
                "ts_ms": _safe_int(row[0], 0),
                "fill_qty": _safe_float(row[2], None),
                "fill_px": _safe_float(row[3], None),
                "expected_px": _metric_from_payload(extra, "expected_px", "expected_price", "ref_px"),
                "mid_px": _metric_from_payload(extra, "mid_px", "arrival_mid_px"),
                "spread_bps": _metric_from_payload(extra, "spread_bps"),
                "slippage_bps": _metric_from_payload(extra, "slippage_bps", "slippage"),
                "fill_latency_ms": _metric_from_payload(extra, "fill_latency_ms", "latency_ms"),
                "fees": _metric_from_payload(extra, "fees", "commission"),
                "implementation_shortfall_bps": _metric_from_payload(
                    extra,
                    "implementation_shortfall_bps",
                    "shortfall_bps",
                    "arrival_slippage_bps",
                ),
                "vwap_px": _metric_from_payload(extra, "vwap_px", "fill_vwap", "vwap"),
                "note": str(row[5] or ""),
                "extra": extra,
            }
        )
    return out


def _avg(values: Iterable[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


def _sum(values: Iterable[float | None]) -> float:
    return float(sum(float(value) for value in values if value is not None and math.isfinite(float(value))))


def _fill_quality_by_symbol(con: Any) -> dict[str, dict[str, float | None]]:
    out: dict[str, dict[str, float | None]] = {}
    if _table_exists(con, "execution_policy_feedback"):
        rows = _fetchall(
            con,
            """
            SELECT symbol, AVG(fill_quality_score), AVG(slippage_error_bps),
                   AVG(latency_error_ms), COUNT(*), MAX(ts_ms)
            FROM execution_policy_feedback
            GROUP BY symbol
            """
        )
        for row in rows:
            sym = str(row[0] or "").upper()
            out[sym] = {
                "avg_fill_quality_score": _safe_float(row[1], None),
                "avg_slippage_error_bps": _safe_float(row[2], None),
                "avg_latency_error_ms": _safe_float(row[3], None),
                "quality_sample_n": _safe_int(row[4], 0),
                "quality_latest_ts_ms": _safe_int(row[5], None),
            }
    if _table_exists(con, "execution_fill_quality"):
        rows = _fetchall(
            con,
            """
            SELECT symbol, AVG(total_cost_bps), AVG(spread_capture_bps), MAX(ts_ms)
            FROM execution_fill_quality
            GROUP BY symbol
            """
        )
        for row in rows:
            sym = str(row[0] or "").upper()
            out.setdefault(sym, {})
            out[sym].update(
                {
                    "avg_total_cost_bps": _safe_float(row[1], None),
                    "avg_spread_capture_bps": _safe_float(row[2], None),
                    "fill_quality_latest_ts_ms": _safe_int(row[3], None),
                }
            )
    return out


def _build_by_symbol_tca(con: Any, recent_fills: list[dict[str, Any]], metric_rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    quality = _fill_quality_by_symbol(con)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    grouped_fills: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_rows = metric_rows or recent_fills
    for row in source_rows:
        sym = str(row.get("symbol") or "").upper()
        if sym:
            grouped[sym].append(row)
    for row in recent_fills:
        sym = str(row.get("symbol") or "").upper()
        if sym:
            grouped_fills[sym].append(row)

    out = []
    for sym, rows in grouped.items():
        fill_meta_rows = grouped_fills.get(sym, [])
        vwap_rows = rows + fill_meta_rows
        fill_qty_values = [
            abs(float(_safe_float(row.get("filled_qty"), _safe_float(row.get("fill_qty"), 0.0)) or 0.0))
            for row in rows
        ]
        latest = max((_safe_int(row.get("ts_ms"), 0) or 0) for row in rows)
        q = quality.get(sym, {})
        out.append(
            {
                "symbol": sym,
                "n_fills": len(rows),
                "filled_qty": float(sum(fill_qty_values)),
                "avg_slippage_bps": _avg(row.get("slippage_bps") for row in rows),
                "avg_latency_ms": _avg(row.get("fill_latency_ms") for row in rows),
                "avg_spread_bps": _avg(row.get("spread_bps") for row in rows),
                "total_fees": _sum(row.get("fees") for row in rows),
                "avg_fill_quality_score": q.get("avg_fill_quality_score"),
                "avg_slippage_error_bps": q.get("avg_slippage_error_bps"),
                "avg_latency_error_ms": q.get("avg_latency_error_ms"),
                "avg_total_cost_bps": q.get("avg_total_cost_bps"),
                "avg_spread_capture_bps": q.get("avg_spread_capture_bps"),
                "avg_implementation_shortfall_bps": _avg(row.get("implementation_shortfall_bps") for row in fill_meta_rows),
                "avg_vwap_px": _avg(row.get("fill_vwap") for row in vwap_rows) or _avg(row.get("vwap_px") for row in vwap_rows),
                "latest_ts_ms": latest or None,
            }
        )
    out.sort(key=lambda row: (abs(float(row.get("avg_slippage_bps") or 0.0)), int(row.get("n_fills") or 0)), reverse=True)
    return out[: max(1, int(limit))]


def _build_rolling_tca(rows: list[dict[str, Any]], now_ms: int) -> list[dict[str, Any]]:
    windows = (
        ("5m", 5 * 60_000),
        ("1h", 60 * 60_000),
        ("24h", 24 * 60 * 60_000),
        ("7d", 7 * 24 * 60 * 60_000),
    )
    out = []
    for label, window_ms in windows:
        subset = [row for row in rows if (_safe_int(row.get("ts_ms"), 0) or 0) >= int(now_ms - window_ms)]
        latest = max((_safe_int(row.get("ts_ms"), 0) or 0) for row in subset) if subset else None
        out.append(
            {
                "window": label,
                "window_ms": int(window_ms),
                "state": "available" if subset else "unavailable",
                "n_fills": len(subset),
                "avg_slippage_bps": _avg(row.get("slippage_bps") for row in subset),
                "avg_latency_ms": _avg(row.get("fill_latency_ms") for row in subset),
                "avg_fill_quality_score": _avg(row.get("fill_quality_score") for row in subset),
                "total_fees": _sum(row.get("fees") for row in subset),
                "avg_implementation_shortfall_bps": _avg(row.get("implementation_shortfall_bps") for row in subset),
                "latest_ts_ms": latest,
                "age_ms": (max(0, int(now_ms - latest)) if latest else None),
            }
        )
    return out


def _partial_fill_rows(con: Any, limit: int) -> list[dict[str, Any]]:
    if not (_table_exists(con, "execution_orders") and _table_exists(con, "execution_fills")):
        return []
    rows = _fetchall(
        con,
        """
        SELECT
          o.client_order_id,
          o.portfolio_orders_id,
          o.source_alert_id,
          o.broker,
          o.symbol,
          o.qty,
          o.status,
          o.submit_ts_ms,
          COALESCE(SUM(ABS(f.fill_qty)), 0.0) AS filled_qty,
          COUNT(f.client_order_id) AS fill_count,
          MAX(f.fill_ts_ms) AS latest_fill_ts_ms,
          CASE
            WHEN SUM(ABS(f.fill_qty)) > 0
            THEN SUM(ABS(f.fill_qty) * f.fill_px) / SUM(ABS(f.fill_qty))
            ELSE NULL
          END AS fill_vwap,
          AVG(f.slippage_bps) AS avg_slippage_bps,
          AVG(f.fill_latency_ms) AS avg_latency_ms
        FROM execution_orders o
        LEFT JOIN execution_fills f
          ON f.client_order_id = o.client_order_id
        GROUP BY
          o.client_order_id, o.portfolio_orders_id, o.source_alert_id,
          o.broker, o.symbol, o.qty, o.status, o.submit_ts_ms
        ORDER BY COALESCE(MAX(f.fill_ts_ms), o.submit_ts_ms) DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    out = []
    for row in rows:
        ordered = abs(float(_safe_float(row[5], 0.0) or 0.0))
        filled = abs(float(_safe_float(row[8], 0.0) or 0.0))
        fill_ratio = min(1.0, filled / ordered) if ordered > 0 else None
        status = str(row[6] or "").lower()
        is_partial = bool(fill_ratio is not None and 0.0 < fill_ratio < 0.999999) or "partial" in status
        if not is_partial and filled <= 0.0:
            continue
        out.append(
            {
                "client_order_id": str(row[0] or ""),
                "portfolio_orders_id": _safe_int(row[1], None),
                "source_alert_id": _safe_int(row[2], None),
                "broker": str(row[3] or ""),
                "symbol": str(row[4] or "").upper(),
                "ordered_qty": ordered,
                "filled_qty": filled,
                "remaining_qty": max(0.0, ordered - filled),
                "fill_ratio": fill_ratio,
                "fill_count": _safe_int(row[9], 0),
                "status": str(row[6] or "unknown"),
                "submit_ts_ms": _safe_int(row[7], None),
                "latest_fill_ts_ms": _safe_int(row[10], None),
                "fill_vwap": _safe_float(row[11], None),
                "avg_slippage_bps": _safe_float(row[12], None),
                "avg_latency_ms": _safe_float(row[13], None),
                "state": "partial" if is_partial else "filled",
            }
        )
    return out


def _recent_rejections(con: Any, limit: int) -> list[dict[str, Any]]:
    if not _table_exists(con, "terminal_intent_rejections"):
        return []
    rows = _fetchall(
        con,
        """
        SELECT id, ts_ms, symbol, side, qty, reason_code, reason, source, detail_json
        FROM terminal_intent_rejections
        ORDER BY ts_ms DESC, id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    out = []
    for row in rows:
        out.append(
            {
                "id": _safe_int(row[0], None),
                "ts_ms": _safe_int(row[1], 0),
                "symbol": str(row[2] or "").upper(),
                "side": str(row[3] or "").upper(),
                "qty": _safe_float(row[4], None),
                "reason_code": str(row[5] or "rejected"),
                "reason": str(row[6] or _humanize_reason(row[5])),
                "source": str(row[7] or "terminal"),
                "detail": _json_obj(row[8]),
                "state": "rejected",
            }
        )
    return out


def _recent_suppressions(con: Any, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if _table_exists(con, "trade_attribution_ledger"):
        cols = _table_columns(con, "trade_attribution_ledger")
        select_parts = [
            "id" if "id" in cols else "NULL",
            "ts_ms" if "ts_ms" in cols else "NULL",
            "source_alert_id" if "source_alert_id" in cols else "NULL",
            "symbol" if "symbol" in cols else "NULL",
            "suppression_reason" if "suppression_reason" in cols else "NULL",
            "decision_json" if "decision_json" in cols else "NULL",
            "order_id" if "order_id" in cols else "NULL",
        ]
        where_clause = "suppression_reason IS NOT NULL AND TRIM(COALESCE(suppression_reason, '')) <> ''" if "suppression_reason" in cols else "0=1"
        rows = _fetchall(
            con,
            f"""
            SELECT {", ".join(select_parts)}
            FROM trade_attribution_ledger
            WHERE {where_clause}
            ORDER BY {("ts_ms" if "ts_ms" in cols else "1")} DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        for row in rows:
            decision = _json_obj(row[5])
            reason_code = str(row[4] or decision.get("blocked_by") or decision.get("reason") or "suppressed")
            out.append(
                {
                    "id": _safe_int(row[0], None),
                    "ts_ms": _safe_int(row[1], 0),
                    "source_alert_id": _safe_int(row[2], None),
                    "symbol": str(row[3] or "").upper(),
                    "reason_code": reason_code,
                    "reason": _humanize_reason(reason_code),
                    "decision": decision,
                    "portfolio_orders_id": _safe_int(row[6], None),
                    "state": "suppressed",
                    "source": "trade_attribution_ledger",
                }
            )

    if _table_exists(con, "execution_policy_audit"):
        cols = _table_columns(con, "execution_policy_audit")
        if "suppression_state" in cols:
            rows = _fetchall(
                con,
                """
                SELECT id, ts_ms, source_alert_id, symbol, suppression_state, decision_json,
                       portfolio_orders_batch_id
                FROM execution_policy_audit
                WHERE suppression_state IS NOT NULL
                  AND TRIM(COALESCE(suppression_state, '')) NOT IN ('', 'NONE', 'ALLOW')
                ORDER BY ts_ms DESC, id DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            for row in rows:
                decision = _json_obj(row[5])
                reason_code = str(decision.get("blocked_by") or decision.get("reason") or row[4] or "suppressed")
                out.append(
                    {
                        "id": _safe_int(row[0], None),
                        "ts_ms": _safe_int(row[1], 0),
                        "source_alert_id": _safe_int(row[2], None),
                        "symbol": str(row[3] or "").upper(),
                        "reason_code": reason_code,
                        "reason": _humanize_reason(reason_code),
                        "decision": decision,
                        "portfolio_orders_id": _safe_int(row[6], None),
                        "state": "suppressed",
                        "source": "execution_policy_audit",
                    }
                )

    out.sort(key=lambda row: int(row.get("ts_ms") or 0), reverse=True)
    deduped = []
    seen: set[tuple[Any, ...]] = set()
    for row in out:
        key = (row.get("source"), row.get("id"), row.get("ts_ms"), row.get("reason_code"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped[: int(limit)]


def _lob_diagnostics(con: Any, *, symbol: str | None, now_ms: int) -> dict[str, Any]:
    sym = str(symbol or "").upper().strip() or None
    l2 = l2_data_quality_snapshot(con, symbol=sym, ts_ms=now_ms)
    latency = latency_assumption_snapshot(con, ts_ms=now_ms)
    calibration = simulator_calibration_snapshot(con, symbol=sym, ts_ms=now_ms)
    readiness = lob_deeplob_readiness_snapshot(con, symbol=sym, ts_ms=now_ms, require_enabled=True)
    warnings = []
    for source in (l2, latency, calibration, readiness):
        warnings.extend(str(item) for item in list(source.get("blockers") or []))
    warnings = list(dict.fromkeys(warnings))

    if bool(l2.get("ok")):
        l2_state = "fresh"
    elif "l2_stale" in warnings:
        l2_state = "stale"
    else:
        l2_state = "unavailable"

    if not bool(readiness.get("enabled")):
        deeplob_state = "shadow_disabled"
    elif bool(readiness.get("ok")):
        deeplob_state = "shadow_ready"
    else:
        deeplob_state = "shadow_blocked"

    return {
        "state": "ready" if not warnings else ("stale" if l2_state == "stale" else "blocked"),
        "symbol": sym,
        "l2_feed": {
            "state": l2_state,
            "freshness_state": l2_state,
            "latest_ts_ms": l2.get("latest_ts_ms"),
            "age_ms": l2.get("age_ms"),
            "max_age_ms": l2.get("max_age_ms"),
            "sample_n": l2.get("sample_n", 0),
            "required_rows": l2.get("required_rows"),
            "snapshot_depth": {
                "top_depth_rows": l2.get("top_depth_rows", 0),
                "avg_top_depth_qty": l2.get("avg_top_depth_qty"),
                "avg_spread_bps": l2.get("avg_spread_bps"),
                "median_interval_ms": l2.get("median_interval_ms"),
            },
            "reason": l2.get("reason"),
        },
        "simulation": {
            "state": "ready" if bool(calibration.get("ok")) else "unavailable",
            "replay_ready": bool(l2.get("ok")) and bool(calibration.get("ok")),
            "calibration_status": "ready" if bool(calibration.get("ok")) else str(calibration.get("reason") or "unavailable"),
            **dict(calibration),
        },
        "latency": dict(latency),
        "deeplob": {
            "state": deeplob_state,
            "enabled": bool(deeplob_shadow_enabled()),
            "shadow_only": True,
            "readiness": dict(readiness),
            "constraints": {
                "portfolio_selection_allowed": False,
                "portfolio_sizing_allowed": False,
                "broker_routing_allowed": False,
                "execution_timing_only": True,
            },
        },
        "warnings": warnings,
    }


def _learned_policy_rows(con: Any, limit: int) -> list[dict[str, Any]]:
    if not _table_exists(con, "execution_policy_audit"):
        return []
    cols = _table_columns(con, "execution_policy_audit")
    select_parts = [
        "id" if "id" in cols else "NULL",
        "ts_ms" if "ts_ms" in cols else "NULL",
        "symbol" if "symbol" in cols else "NULL",
        "side" if "side" in cols else "NULL",
        "qty" if "qty" in cols else "NULL",
        "policy_json" if "policy_json" in cols else "NULL",
        "decision_json" if "decision_json" in cols else "NULL",
        "suppression_state" if "suppression_state" in cols else "NULL",
        "source_alert_id" if "source_alert_id" in cols else "NULL",
        "portfolio_orders_batch_id" if "portfolio_orders_batch_id" in cols else "NULL",
    ]
    order = "ts_ms DESC" if "ts_ms" in cols else "1 DESC"
    rows = _fetchall(
        con,
        f"""
        SELECT {", ".join(select_parts)}
        FROM execution_policy_audit
        ORDER BY {order}
        LIMIT ?
        """,
        (int(limit),),
    )
    out = []
    for row in rows:
        policy = _json_obj(row[5])
        decision = _json_obj(row[6])
        learned = _json_obj(policy.get("learned_execution"))
        if not learned:
            learned = _json_obj(decision.get("learned_execution"))
        out.append(
            {
                "id": _safe_int(row[0], None),
                "ts_ms": _safe_int(row[1], 0),
                "symbol": str(row[2] or "").upper(),
                "side": str(row[3] or "").upper(),
                "qty": _safe_float(row[4], None),
                "policy_json": policy,
                "decision_json": decision,
                "learned_execution": learned,
                "suppression_state": str(row[7] or ""),
                "source_alert_id": _safe_int(row[8], None),
                "portfolio_orders_id": _safe_int(row[9], None),
            }
        )
    return out


def _learned_slicing_diagnostics(con: Any, *, limit: int, now_ms: int) -> dict[str, Any]:
    rows = _learned_policy_rows(con, limit)
    enabled_env = bool(learned_execution_enabled({}))
    action_counts: Counter[str] = Counter()
    recent_decisions = []
    baseline_samples = []
    suppressions = []
    requested = 0
    applied = 0
    blocked = 0
    latest_ts = None

    for row in rows:
        learned = dict(row.get("learned_execution") or {})
        latest_ts = max(latest_ts or 0, int(row.get("ts_ms") or 0)) if row.get("ts_ms") else latest_ts
        if learned:
            if bool(learned.get("enabled")):
                requested += 1
            if bool(learned.get("applied")):
                applied += 1
                action_id = str(learned.get("action_id") or "unknown")
                action_counts[action_id] += 1
            elif bool(learned.get("enabled")):
                blocked += 1
                reason = str(learned.get("reason") or "learned_execution_not_applied")
                suppressions.append(
                    {
                        "ts_ms": row.get("ts_ms"),
                        "symbol": row.get("symbol"),
                        "reason_code": reason,
                        "reason": _humanize_reason(reason),
                        "source": "execution_policy_audit",
                    }
                )
            if learned.get("context") and learned.get("constraints"):
                baseline_samples.append(
                    {
                        "symbol": row.get("symbol"),
                        "side": row.get("side"),
                        "qty": row.get("qty"),
                        "context": dict(learned.get("context") or {}),
                        "constraints": dict(learned.get("constraints") or {}),
                    }
                )
            recent_decisions.append(
                {
                    "ts_ms": row.get("ts_ms"),
                    "symbol": row.get("symbol"),
                    "side": row.get("side"),
                    "enabled": bool(learned.get("enabled")),
                    "applied": bool(learned.get("applied")),
                    "action_id": learned.get("action_id"),
                    "reason": learned.get("reason"),
                    "parameters": dict(learned.get("parameters") or {}),
                    "source_alert_id": row.get("source_alert_id"),
                    "portfolio_orders_id": row.get("portfolio_orders_id"),
                }
            )

    baseline = {"state": "unavailable", "reason": "no_recent_learned_context", "summary": {}}
    if baseline_samples:
        try:
            report = evaluate_against_baselines(baseline_samples[: min(50, len(baseline_samples))])
            baseline = {
                "state": "available",
                "reason": "ok",
                "summary": dict(report.get("summary") or {}),
                "sample_n": len(baseline_samples[: min(50, len(baseline_samples))]),
            }
        except Exception as exc:
            baseline = {"state": "unavailable", "reason": f"baseline_evaluation_failed:{type(exc).__name__}", "summary": {}}

    if not _table_exists(con, "execution_policy_audit"):
        state = "unavailable"
        reason = "execution_policy_audit_missing"
    elif not rows:
        state = "unavailable"
        reason = "no_policy_audit_rows"
    elif requested == 0:
        state = "disabled" if not enabled_env else "available"
        reason = "no_recent_learned_slicing_requests"
    elif applied > 0:
        state = "active"
        reason = "recent_learned_slicing_applied"
    else:
        state = "shadow_or_blocked"
        reason = "recent_learned_slicing_not_applied"

    return {
        "state": state,
        "reason": reason,
        "policy": {
            "name": POLICY_NAME,
            "scope": POLICY_SCOPE,
            "enabled_by_env": enabled_env,
            "requested_recent": int(requested),
            "applied_recent": int(applied),
            "blocked_recent": int(blocked),
            "latest_ts_ms": latest_ts,
            "age_ms": (max(0, int(now_ms - latest_ts)) if latest_ts else None),
        },
        "authority": {
            "live_authority_granted": False,
            "asset_selection_allowed": False,
            "side_selection_allowed": False,
            "broker_selection_allowed": False,
            "portfolio_sizing_allowed": False,
            "execution_parameters_only": True,
        },
        "exploration": {
            "state": "unavailable",
            "reason": "no_exploration_mode_configured",
        },
        "shadow": {
            "state": "not_shadow_only",
            "reason": "learned slicer is bounded by execution policy when enabled; it is not a new live authority.",
        },
        "selected_action_distribution": [
            {"action_id": action, "count": int(count)}
            for action, count in action_counts.most_common()
        ],
        "baseline_comparison": baseline,
        "recent_suppression_reasons": suppressions[: int(limit)],
        "recent_decisions": recent_decisions[: int(limit)],
    }


def _trace_rows(
    *,
    partials: list[dict[str, Any]],
    rejections: list[dict[str, Any]],
    suppressions: list[dict[str, Any]],
    recent_fills: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    rows = []
    for row in partials:
        fill_ratio = _safe_float(row.get("fill_ratio"), None)
        rows.append(
            {
                "kind": "partial_fill",
                "symbol": row.get("symbol"),
                "client_order_id": row.get("client_order_id"),
                "portfolio_orders_id": row.get("portfolio_orders_id"),
                "source_alert_id": row.get("source_alert_id"),
                "intent": "execution order submitted",
                "route": row.get("broker") or "broker unavailable",
                "fill": f"{float(row.get('filled_qty') or 0.0):g}/{float(row.get('ordered_qty') or 0.0):g}",
                "outcome": "partial fill" if row.get("state") == "partial" else "filled",
                "reason_code": "partial_fill" if row.get("state") == "partial" else "filled",
                "reason": (
                    f"{(fill_ratio or 0.0) * 100.0:.1f}% filled"
                    if fill_ratio is not None
                    else "Fill ratio unavailable"
                ),
                "ts_ms": row.get("latest_fill_ts_ms") or row.get("submit_ts_ms"),
            }
        )
    for row in rejections:
        rows.append(
            {
                "kind": "rejected_intent",
                "symbol": row.get("symbol"),
                "client_order_id": None,
                "portfolio_orders_id": None,
                "source_alert_id": None,
                "intent": f"{row.get('side') or ''} {row.get('qty') or ''}".strip() or "terminal intent",
                "route": "not routed",
                "fill": "none",
                "outcome": "rejected",
                "reason_code": row.get("reason_code"),
                "reason": row.get("reason"),
                "ts_ms": row.get("ts_ms"),
            }
        )
    for row in suppressions:
        rows.append(
            {
                "kind": "suppressed_intent",
                "symbol": row.get("symbol"),
                "client_order_id": None,
                "portfolio_orders_id": row.get("portfolio_orders_id"),
                "source_alert_id": row.get("source_alert_id"),
                "intent": "portfolio intent",
                "route": "not routed",
                "fill": "none",
                "outcome": "suppressed",
                "reason_code": row.get("reason_code"),
                "reason": row.get("reason"),
                "ts_ms": row.get("ts_ms"),
            }
        )
    for row in recent_fills:
        rows.append(
            {
                "kind": "filled_order",
                "symbol": row.get("symbol"),
                "client_order_id": row.get("client_order_id"),
                "portfolio_orders_id": row.get("portfolio_orders_id"),
                "source_alert_id": row.get("source_alert_id"),
                "intent": "execution order",
                "route": row.get("broker") or row.get("source") or "broker",
                "fill": f"{row.get('fill_qty') or 0:g} @ {row.get('fill_px') or 0:g}",
                "outcome": "filled",
                "reason_code": "fill_recorded",
                "reason": "Broker fill recorded.",
                "ts_ms": row.get("ts_ms"),
            }
        )
    rows.sort(key=lambda row: int(row.get("ts_ms") or 0), reverse=True)
    return rows[: int(limit)]


def build_execution_diagnostics(
    *,
    con: Any | None = None,
    limit: int = 50,
    symbol: str | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Build the read-only execution diagnostics payload used by operators."""

    owns_connection = con is None
    active_con = con or connect_ro()
    limit_i = max(1, min(500, int(limit or 50)))
    now = int(now_ms if now_ms is not None else _now_ms())
    sym = str(symbol or "").upper().strip()
    try:
        recent_metrics = _recent_execution_metric_rows(active_con, limit_i * 10)
        recent_fills = _recent_execution_fill_rows(active_con, limit_i * 10)
        tca_source = recent_metrics or recent_fills
        partials = _partial_fill_rows(active_con, limit_i)
        rejections = _recent_rejections(active_con, limit_i)
        suppressions = _recent_suppressions(active_con, limit_i)
        if sym:
            recent_fills = [row for row in recent_fills if str(row.get("symbol") or "").upper() == sym]
            recent_metrics = [row for row in recent_metrics if str(row.get("symbol") or "").upper() == sym]
            tca_source = [row for row in tca_source if str(row.get("symbol") or "").upper() == sym]
            partials = [row for row in partials if str(row.get("symbol") or "").upper() == sym]
            rejections = [row for row in rejections if str(row.get("symbol") or "").upper() == sym]
            suppressions = [row for row in suppressions if str(row.get("symbol") or "").upper() == sym]

        by_symbol = _build_by_symbol_tca(active_con, recent_fills, recent_metrics, limit_i)
        rolling = _build_rolling_tca(tca_source, now)
        latest_fill_ts = max((_safe_int(row.get("ts_ms"), 0) or 0) for row in tca_source) if tca_source else None
        tca_state, fill_age_ms, tca_reason = _state_from_latest(latest_fill_ts, now, rows=len(tca_source))

        return {
            "ok": True,
            "ts_ms": now,
            "state": tca_state if tca_state != "unavailable" else "partial",
            "symbol": sym or None,
            "inventory": _inventory(active_con, now),
            "tca": {
                "state": tca_state,
                "reason": tca_reason or "ok",
                "latest_fill_ts_ms": latest_fill_ts,
                "latest_fill_age_ms": fill_age_ms,
                "by_symbol": by_symbol,
                "rolling": rolling,
                "partial_fills": partials,
                "recent_fills": recent_fills[:limit_i],
            },
            "order_flow": {
                "state": "available" if (partials or rejections or suppressions or recent_fills) else "unavailable",
                "partial_fills": partials,
                "rejected_intents": rejections,
                "suppressed_intents": suppressions,
                "outcome_counts": {
                    "partial_fills": len([row for row in partials if row.get("state") == "partial"]),
                    "filled_orders": len([row for row in partials if row.get("state") == "filled"]),
                    "rejected_intents": len(rejections),
                    "suppressed_intents": len(suppressions),
                },
            },
            "lob": _lob_diagnostics(active_con, symbol=sym or None, now_ms=now),
            "learned_slicing": _learned_slicing_diagnostics(active_con, limit=limit_i, now_ms=now),
            "drilldowns": _trace_rows(
                partials=partials,
                rejections=rejections,
                suppressions=suppressions,
                recent_fills=recent_fills,
                limit=limit_i,
            ),
        }
    finally:
        if owns_connection:
            try:
                active_con.close()
            except Exception:
                # no-op-guard: allow - connection cleanup best-effort only.
                pass


__all__ = ["build_execution_diagnostics"]
