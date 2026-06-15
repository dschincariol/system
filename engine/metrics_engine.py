"""Prediction feedback loop and model performance aggregation."""

from __future__ import annotations

import json
import logging
import math
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_gauge
from engine.runtime.storage import connect, init_db, run_write_txn

LOG = get_logger("engine.metrics_engine")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.metrics_engine",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_binary_flag(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        numeric = int(value)
    except Exception:
        try:
            numeric = int(float(str(value).strip()))
        except Exception:
            return None
    return numeric if numeric in (0, 1) else None


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
        except Exception:
            return {}
        return dict(obj) if isinstance(obj, dict) else {}
    return {}


def _table_exists(con, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _table_columns(con, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
    except Exception:
        return set()
    return {str(row[1] or "").strip().lower() for row in rows}


def _has_column(con, table_name: str, column_name: str) -> bool:
    return str(column_name or "").strip().lower() in _table_columns(con, table_name)


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_model_id(value: Any) -> str:
    text = str(value or "").strip()
    return text or "baseline"


def _sign(value: Any) -> int:
    numeric = _safe_float(value, 0.0)
    if numeric > 0.0:
        return 1
    if numeric < 0.0:
        return -1
    return 0


def _prediction_match_score(
    row_model_id: str,
    row_model_name: str,
    *,
    target_model_id: str,
    target_model_name: str,
) -> int:
    if target_model_id and row_model_id == target_model_id:
        return 0
    if target_model_name and row_model_name == target_model_name:
        return 1
    return 2


def _load_alert_row(con, cache: Dict[int, Dict[str, Any]], source_alert_id: Optional[int]) -> Dict[str, Any]:
    sid = _safe_int(source_alert_id, 0)
    if sid <= 0:
        return {}
    cached = cache.get(sid)
    if cached is not None:
        return dict(cached)
    if not _table_exists(con, "alerts"):
        cache[sid] = {}
        return {}

    columns = _table_columns(con, "alerts")
    select_cols = ["event_id", "symbol", "horizon_s", "explain_json"]
    for optional in ("prediction_id", "model_name", "model_id", "model_version"):
        if optional in columns:
            select_cols.append(optional)

    row = con.execute(
        f"SELECT {', '.join(select_cols)} FROM alerts WHERE id=? LIMIT 1",
        (int(sid),),
    ).fetchone()
    if not row:
        cache[sid] = {}
        return {}

    out: Dict[str, Any] = {}
    for idx, name in enumerate(select_cols):
        out[name] = row[idx]
    cache[sid] = dict(out)
    return out


def _resolve_prediction_row(
    con,
    *,
    explicit_prediction_id: Optional[int],
    event_id: Optional[int],
    symbol: str,
    horizon_s: Optional[int],
    model_id: str,
    model_name: str,
) -> Dict[str, Any]:
    if not _table_exists(con, "predictions"):
        return {}

    if _safe_int(explicit_prediction_id, 0) > 0:
        row = con.execute(
            """
            SELECT id, ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                   model_name, model_id, model_version
            FROM predictions
            WHERE id=?
            LIMIT 1
            """,
            (int(explicit_prediction_id),),
        ).fetchone()
        if row:
            return {
                "id": int(row[0]),
                "ts_ms": int(row[1]),
                "event_id": int(row[2]),
                "symbol": str(row[3]),
                "horizon_s": int(row[4]),
                "predicted_z": float(row[5]),
                "confidence": float(row[6]),
                "model_name": (str(row[7]).strip() if row[7] not in (None, "") else None),
                "model_id": _normalize_model_id(row[8]),
                "model_version": (str(row[9]).strip() if row[9] not in (None, "") else None),
            }

    event_id_i = _safe_int(event_id, 0)
    horizon_s_i = _safe_int(horizon_s, 0)
    symbol_u = _normalize_symbol(symbol)
    if event_id_i <= 0 or horizon_s_i <= 0 or not symbol_u:
        return {}

    rows = con.execute(
        """
        SELECT id, ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
               model_name, model_id, model_version
        FROM predictions
        WHERE event_id=? AND UPPER(TRIM(symbol))=? AND horizon_s=?
        ORDER BY ts_ms DESC, id DESC
        """,
        (int(event_id_i), str(symbol_u), int(horizon_s_i)),
    ).fetchall()
    if not rows:
        return {}

    target_model_id = _normalize_model_id(model_id) if str(model_id or "").strip() else ""
    target_model_name = str(model_name or "").strip()
    best = min(
        rows,
        key=lambda row: (
            _prediction_match_score(
                _normalize_model_id(row[8]),
                str(row[7] or "").strip(),
                target_model_id=target_model_id,
                target_model_name=target_model_name,
            ),
            -_safe_int(row[1], 0),
            -_safe_int(row[0], 0),
        ),
    )
    return {
        "id": int(best[0]),
        "ts_ms": int(best[1]),
        "event_id": int(best[2]),
        "symbol": str(best[3]),
        "horizon_s": int(best[4]),
        "predicted_z": float(best[5]),
        "confidence": float(best[6]),
        "model_name": (str(best[7]).strip() if best[7] not in (None, "") else None),
        "model_id": _normalize_model_id(best[8]),
        "model_version": (str(best[9]).strip() if best[9] not in (None, "") else None),
    }


def _resolve_realized_z(
    con,
    cache: Dict[Tuple[int, str, int], Optional[float]],
    *,
    event_id: Optional[int],
    symbol: str,
    horizon_s: Optional[int],
) -> Optional[float]:
    event_id_i = _safe_int(event_id, 0)
    horizon_s_i = _safe_int(horizon_s, 0)
    symbol_u = _normalize_symbol(symbol)
    if event_id_i <= 0 or horizon_s_i <= 0 or not symbol_u:
        return None

    key = (int(event_id_i), str(symbol_u), int(horizon_s_i))
    if key in cache:
        return cache[key]

    value: Optional[float] = None
    if _table_exists(con, "labels_exec"):
        row = con.execute(
            """
            SELECT net_z
            FROM labels_exec
            WHERE event_id=? AND symbol=? AND horizon_s=?
            LIMIT 1
            """,
            (int(event_id_i), str(symbol_u), int(horizon_s_i)),
        ).fetchone()
        if row and row[0] is not None:
            value = float(row[0])

    if value is None and _table_exists(con, "labels"):
        row = con.execute(
            """
            SELECT impact_z
            FROM labels
            WHERE event_id=? AND symbol=? AND horizon_s=?
            LIMIT 1
            """,
            (int(event_id_i), str(symbol_u), int(horizon_s_i)),
        ).fetchone()
        if row and row[0] is not None:
            value = float(row[0])

    cache[key] = value
    return value


def init_metrics_db() -> None:
    """Ensure prediction-feedback and model-performance tables exist."""
    init_db()


def refresh_prediction_feedback(
    *,
    snapshot_ts_ms: Optional[int] = None,
    con=None,
) -> Dict[str, Any]:
    """Materialize prediction-feedback rows against realized outcomes."""
    if con is None:
        init_metrics_db()

    result = {
        "ok": True,
        "snapshot_ts_ms": None,
        "rows_seen": 0,
        "feedback_upserted": 0,
        "links_backfilled": 0,
        "unresolved_predictions": 0,
    }

    def _write(conw):
        if not _table_exists(conw, "pnl_attribution"):
            result["ok"] = False
            result["status"] = "missing_pnl_attribution"
            return result

        effective_ts = _safe_int(snapshot_ts_ms, 0)
        if effective_ts <= 0:
            row = conw.execute("SELECT MAX(ts_ms) FROM pnl_attribution").fetchone()
            effective_ts = _safe_int((row or [None])[0], 0)
        if effective_ts <= 0:
            result["ok"] = False
            result["status"] = "no_pnl_attribution"
            return result

        pnl_has_prediction_id = _has_column(conw, "pnl_attribution", "prediction_id")
        prediction_select = "prediction_id," if pnl_has_prediction_id else "NULL AS prediction_id,"
        rows = conw.execute(
            f"""
            SELECT
              ts_ms,
              source_alert_id,
              model_id,
              model_version,
              symbol,
              pnl,
              realized_pnl,
              unrealized_pnl,
              fees,
              slippage_bps,
              {prediction_select}
              extra_json
            FROM pnl_attribution
            WHERE ts_ms=?
            ORDER BY source_alert_id ASC, symbol ASC
            """,
            (int(effective_ts),),
        ).fetchall()
        result["snapshot_ts_ms"] = int(effective_ts)
        result["rows_seen"] = int(len(rows or []))

        alert_cache: Dict[int, Dict[str, Any]] = {}
        realized_z_cache: Dict[Tuple[int, str, int], Optional[float]] = {}
        alerts_has_prediction_id = _has_column(conw, "alerts", "prediction_id")
        execution_orders_has_prediction_id = _has_column(conw, "execution_orders", "prediction_id")

        for (
            resolution_ts_ms,
            source_alert_id,
            attribution_model_id,
            attribution_model_version,
            symbol,
            net_pnl,
            realized_pnl,
            unrealized_pnl,
            fees,
            slippage_bps,
            explicit_prediction_id,
            extra_json,
        ) in rows or []:
            symbol_u = _normalize_symbol(symbol)
            if not symbol_u:
                continue

            alert_row = _load_alert_row(conw, alert_cache, _safe_int(source_alert_id, 0))
            extra = _safe_json_dict(extra_json)
            explain = _safe_json_dict(alert_row.get("explain_json"))
            resolved_model_id = _normalize_model_id(
                attribution_model_id
                or alert_row.get("model_id")
                or extra.get("model_id")
                or explain.get("model_id")
            )
            resolved_model_name = str(
                alert_row.get("model_name")
                or extra.get("model_name")
                or explain.get("model_name")
                or resolved_model_id
            ).strip()
            resolved_horizon_s = _safe_int(
                alert_row.get("horizon_s")
                or extra.get("horizon_s")
                or explain.get("horizon_s"),
                0,
            )
            resolved_event_id = _safe_int(
                alert_row.get("event_id")
                or extra.get("event_id")
                or explain.get("event_id"),
                0,
            )
            prediction_row = _resolve_prediction_row(
                conw,
                explicit_prediction_id=(
                    explicit_prediction_id
                    or alert_row.get("prediction_id")
                    or extra.get("prediction_id")
                ),
                event_id=resolved_event_id,
                symbol=symbol_u,
                horizon_s=resolved_horizon_s,
                model_id=resolved_model_id,
                model_name=resolved_model_name,
            )
            if not prediction_row:
                result["unresolved_predictions"] = int(result["unresolved_predictions"]) + 1
                continue

            prediction_id = int(prediction_row["id"])
            if _safe_int(source_alert_id, 0) > 0 and alerts_has_prediction_id:
                cur = conw.execute(
                    """
                    UPDATE alerts
                    SET prediction_id=?
                    WHERE id=?
                      AND (prediction_id IS NULL OR prediction_id <> ?)
                    """,
                    (int(prediction_id), int(source_alert_id), int(prediction_id)),
                )
                result["links_backfilled"] = int(result["links_backfilled"]) + int(getattr(cur, "rowcount", 0) or 0)

            if pnl_has_prediction_id:
                cur = conw.execute(
                    """
                    UPDATE pnl_attribution
                    SET prediction_id=?
                    WHERE ts_ms=? AND source_alert_id=? AND COALESCE(NULLIF(TRIM(model_id), ''), 'baseline')=? AND UPPER(TRIM(symbol))=?
                      AND (prediction_id IS NULL OR prediction_id <> ?)
                    """,
                    (
                        int(prediction_id),
                        int(resolution_ts_ms),
                        int(_safe_int(source_alert_id, 0)),
                        str(resolved_model_id),
                        str(symbol_u),
                        int(prediction_id),
                    ),
                )
                result["links_backfilled"] = int(result["links_backfilled"]) + int(getattr(cur, "rowcount", 0) or 0)

            if execution_orders_has_prediction_id and _safe_int(source_alert_id, 0) > 0:
                cur = conw.execute(
                    """
                    UPDATE execution_orders
                    SET prediction_id=?
                    WHERE source_alert_id=?
                      AND COALESCE(NULLIF(TRIM(model_id), ''), 'baseline')=?
                      AND UPPER(TRIM(symbol))=?
                      AND (prediction_id IS NULL OR prediction_id <> ?)
                    """,
                    (
                        int(prediction_id),
                        int(source_alert_id),
                        str(resolved_model_id),
                        str(symbol_u),
                        int(prediction_id),
                    ),
                )
                result["links_backfilled"] = int(result["links_backfilled"]) + int(getattr(cur, "rowcount", 0) or 0)

            realized_z = _resolve_realized_z(
                conw,
                realized_z_cache,
                event_id=prediction_row.get("event_id"),
                symbol=prediction_row.get("symbol") or symbol_u,
                horizon_s=prediction_row.get("horizon_s") or resolved_horizon_s,
            )
            prediction_correct = (
                None if realized_z is None else int(_sign(prediction_row.get("predicted_z")) == _sign(realized_z))
            )
            pnl_correct = int(_sign(prediction_row.get("predicted_z")) == _sign(net_pnl))

            client_order_ids = list(extra.get("realized_trade_client_order_ids") or extra.get("client_order_ids") or [])
            trade_count = _safe_int(
                extra.get("realized_trade_count"),
                len([value for value in client_order_ids if str(value or "").strip()]),
            )
            meta = {
                "client_order_ids": [str(value) for value in client_order_ids if str(value or "").strip()],
                "prediction_id": int(prediction_id),
                "source_alert_id": (_safe_int(source_alert_id, 0) if _safe_int(source_alert_id, 0) > 0 else None),
                "total_cost": _safe_float(extra.get("total_cost"), _safe_float(fees, 0.0) + _safe_float(extra.get("slippage_cost"), 0.0)),
                "slippage_cost": _safe_float(extra.get("slippage_cost"), 0.0),
                "position_size": _safe_float(extra.get("position_size"), 0.0),
                "avg_price": extra.get("avg_price"),
                "last_price": extra.get("last_px"),
            }

            conw.execute(
                """
                INSERT INTO model_prediction_feedback(
                  prediction_id, prediction_ts_ms, resolution_ts_ms, event_id, source_alert_id,
                  model_id, model_name, model_version, symbol, horizon_s, predicted_z, confidence,
                  realized_z, realized_pnl, unrealized_pnl, net_pnl, fees, slippage_bps,
                  trade_count, prediction_correct, pnl_correct, meta_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(prediction_id) DO UPDATE SET
                  prediction_ts_ms=excluded.prediction_ts_ms,
                  resolution_ts_ms=excluded.resolution_ts_ms,
                  event_id=excluded.event_id,
                  source_alert_id=excluded.source_alert_id,
                  model_id=excluded.model_id,
                  model_name=excluded.model_name,
                  model_version=excluded.model_version,
                  symbol=excluded.symbol,
                  horizon_s=excluded.horizon_s,
                  predicted_z=excluded.predicted_z,
                  confidence=excluded.confidence,
                  realized_z=excluded.realized_z,
                  realized_pnl=excluded.realized_pnl,
                  unrealized_pnl=excluded.unrealized_pnl,
                  net_pnl=excluded.net_pnl,
                  fees=excluded.fees,
                  slippage_bps=excluded.slippage_bps,
                  trade_count=excluded.trade_count,
                  prediction_correct=excluded.prediction_correct,
                  pnl_correct=excluded.pnl_correct,
                  meta_json=excluded.meta_json
                """,
                (
                    int(prediction_id),
                    int(prediction_row["ts_ms"]),
                    int(_safe_int(resolution_ts_ms, 0)),
                    int(_safe_int(prediction_row.get("event_id"), 0)),
                    (int(source_alert_id) if _safe_int(source_alert_id, 0) > 0 else None),
                    str(resolved_model_id),
                    str(resolved_model_name or prediction_row.get("model_name") or resolved_model_id),
                    (
                        str(
                            attribution_model_version
                            or prediction_row.get("model_version")
                            or alert_row.get("model_version")
                            or ""
                        ).strip()
                        or None
                    ),
                    str(symbol_u),
                    int(_safe_int(prediction_row.get("horizon_s"), resolved_horizon_s)),
                    float(_safe_float(prediction_row.get("predicted_z"), 0.0)),
                    float(_safe_float(prediction_row.get("confidence"), 0.0)),
                    (float(realized_z) if realized_z is not None else None),
                    float(_safe_float(realized_pnl, 0.0)),
                    float(_safe_float(unrealized_pnl, 0.0)),
                    float(_safe_float(net_pnl, 0.0)),
                    float(_safe_float(fees, 0.0)),
                    (float(slippage_bps) if slippage_bps is not None else None),
                    int(max(0, trade_count)),
                    prediction_correct,
                    int(pnl_correct),
                    json.dumps(meta, separators=(",", ":"), sort_keys=True),
                ),
            )
            result["feedback_upserted"] = int(result["feedback_upserted"]) + 1

        return result

    if con is not None:
        return dict(_write(con))
    return dict(run_write_txn(_write))


def _stddev(values: Iterable[float]) -> float:
    samples = [float(_safe_float(value, 0.0)) for value in values]
    n = len(samples)
    if n <= 1:
        return 0.0
    mean = sum(samples) / float(n)
    variance = sum((value - mean) ** 2 for value in samples) / float(n)
    return math.sqrt(max(0.0, float(variance)))


def _sharpe(values: Iterable[float]) -> float:
    samples = [float(_safe_float(value, 0.0)) for value in values]
    n = len(samples)
    if n <= 1:
        return 0.0
    sd = _stddev(samples)
    if sd <= 1e-12:
        return 0.0
    mean = sum(samples) / float(n)
    return float((mean / sd) * math.sqrt(float(n)))


def _max_drawdown(values: Iterable[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += float(_safe_float(value, 0.0))
        peak = max(float(peak), float(cumulative))
        max_drawdown = max(float(max_drawdown), float(peak - cumulative))
    return float(max_drawdown)


def compute_model_performance_stats(*, con=None) -> Dict[str, Any]:
    """Aggregate model-performance rows into summary statistics."""
    if con is None:
        init_metrics_db()

    result = {"ok": True, "groups_written": 0, "feedback_rows": 0}
    accuracy_metrics: List[Tuple[float, Dict[str, Any]]] = []

    def _write(conw):
        if not _table_exists(conw, "model_prediction_feedback"):
            result["ok"] = False
            result["status"] = "missing_model_prediction_feedback"
            return result

        rows = conw.execute(
            """
            SELECT
              prediction_id,
              resolution_ts_ms,
              model_id,
              model_name,
              symbol,
              horizon_s,
              confidence,
              realized_pnl,
              net_pnl,
              trade_count,
              prediction_correct,
              pnl_correct
            FROM model_prediction_feedback
            ORDER BY resolution_ts_ms ASC, prediction_id ASC
            """
        ).fetchall()
        result["feedback_rows"] = int(len(rows or []))
        conw.execute("DELETE FROM model_performance_stats")
        if not rows:
            return result

        grouped: Dict[Tuple[str, str, str, int], List[Dict[str, Any]]] = {}
        for (
            _prediction_id,
            resolution_ts_ms,
            model_id,
            model_name,
            symbol,
            horizon_s,
            confidence,
            realized_pnl,
            net_pnl,
            trade_count,
            prediction_correct,
            pnl_correct,
        ) in rows or []:
            record = {
                "resolution_ts_ms": _safe_int(resolution_ts_ms, 0),
                "model_id": _normalize_model_id(model_id),
                "model_name": str(model_name or model_id or "baseline").strip() or "baseline",
                "symbol": _normalize_symbol(symbol),
                "horizon_s": _safe_int(horizon_s, 0),
                "confidence": _safe_float(confidence, 0.0),
                "realized_pnl": _safe_float(realized_pnl, 0.0),
                "net_pnl": _safe_float(net_pnl, 0.0),
                "trade_count": max(0, _safe_int(trade_count, 0)),
                "prediction_correct": prediction_correct,
                "pnl_correct": pnl_correct,
            }
            model_key = ("model", record["model_id"], "*", 0)
            detail_key = ("model_symbol_horizon", record["model_id"], record["symbol"], record["horizon_s"])
            grouped.setdefault(model_key, []).append(record)
            grouped.setdefault(detail_key, []).append(record)

        now_ms = _now_ms()
        for (scope, model_id, symbol, horizon_s), items in grouped.items():
            if not items:
                continue
            model_name = str(items[-1].get("model_name") or model_id)
            pnl_series = [float(item.get("net_pnl") or 0.0) for item in items]
            realized_series = [float(item.get("realized_pnl") or 0.0) for item in items]
            label_accuracy_values = [
                int(flag)
                for item in items
                for flag in (_safe_binary_flag(item.get("prediction_correct")),)
                if flag is not None
            ]
            pnl_accuracy_values = [
                int(flag)
                for item in items
                for flag in (_safe_binary_flag(item.get("pnl_correct")),)
                if flag is not None
            ]
            accuracy = (
                (sum(label_accuracy_values) / float(len(label_accuracy_values)))
                if label_accuracy_values
                else ((sum(pnl_accuracy_values) / float(len(pnl_accuracy_values))) if pnl_accuracy_values else None)
            )
            pnl_accuracy = (
                (sum(pnl_accuracy_values) / float(len(pnl_accuracy_values))) if pnl_accuracy_values else None
            )
            win_rate = (
                sum(1 for value in pnl_series if float(value) > 0.0) / float(len(pnl_series))
                if pnl_series
                else None
            )
            avg_confidence = (
                sum(float(item.get("confidence") or 0.0) for item in items) / float(len(items))
                if items
                else None
            )
            metrics = {
                "scope": str(scope),
                "prediction_count": int(len(items)),
                "resolved_count": int(len(items)),
                "trade_count": int(sum(int(item.get("trade_count") or 0) for item in items)),
                "label_accuracy": (
                    (sum(label_accuracy_values) / float(len(label_accuracy_values)))
                    if label_accuracy_values
                    else None
                ),
                "pnl_accuracy": pnl_accuracy,
                "win_rate": win_rate,
                "sharpe": float(_sharpe(pnl_series)),
                "max_drawdown": float(_max_drawdown(pnl_series)),
                "sum_realized_pnl": float(sum(realized_series)),
                "sum_net_pnl": float(sum(pnl_series)),
                "avg_net_pnl": float(sum(pnl_series) / float(len(pnl_series))) if pnl_series else 0.0,
                "avg_confidence": avg_confidence,
            }
            conw.execute(
                """
                INSERT INTO model_performance_stats(
                  scope, model_id, model_name, symbol, horizon_s, prediction_count,
                  resolved_count, trade_count, accuracy, pnl_accuracy, win_rate,
                  sharpe, max_drawdown, sum_realized_pnl, sum_net_pnl, avg_confidence,
                  updated_ts_ms, metrics_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scope, model_id, symbol, horizon_s) DO UPDATE SET
                  model_name=excluded.model_name,
                  prediction_count=excluded.prediction_count,
                  resolved_count=excluded.resolved_count,
                  trade_count=excluded.trade_count,
                  accuracy=excluded.accuracy,
                  pnl_accuracy=excluded.pnl_accuracy,
                  win_rate=excluded.win_rate,
                  sharpe=excluded.sharpe,
                  max_drawdown=excluded.max_drawdown,
                  sum_realized_pnl=excluded.sum_realized_pnl,
                  sum_net_pnl=excluded.sum_net_pnl,
                  avg_confidence=excluded.avg_confidence,
                  updated_ts_ms=excluded.updated_ts_ms,
                  metrics_json=excluded.metrics_json
                """,
                (
                    str(scope),
                    str(model_id),
                    str(model_name),
                    str(symbol),
                    int(horizon_s),
                    int(len(items)),
                    int(len(items)),
                    int(metrics["trade_count"]),
                    (float(accuracy) if accuracy is not None else None),
                    (float(pnl_accuracy) if pnl_accuracy is not None else None),
                    (float(win_rate) if win_rate is not None else None),
                    float(metrics["sharpe"]),
                    float(metrics["max_drawdown"]),
                    float(metrics["sum_realized_pnl"]),
                    float(metrics["sum_net_pnl"]),
                    (float(avg_confidence) if avg_confidence is not None else None),
                    int(now_ms),
                    json.dumps(metrics, separators=(",", ":"), sort_keys=True),
                ),
            )
            if accuracy is not None:
                accuracy_metrics.append(
                    (
                        float(accuracy),
                        {
                            "scope": str(scope),
                            "model_id": str(model_id),
                            "symbol": str(symbol or "*"),
                            "horizon_s": int(horizon_s),
                        },
                    )
                )
            result["groups_written"] = int(result["groups_written"]) + 1

        return result

    if con is not None:
        out = dict(_write(con))
    else:
        out = dict(run_write_txn(_write))

    for accuracy, tags in accuracy_metrics:
        emit_gauge(
            "prediction_accuracy",
            float(accuracy),
            component="engine.metrics_engine",
            extra_tags=dict(tags),
        )
    return out


def refresh_feedback_loop(
    *,
    snapshot_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Refresh prediction feedback and derived performance stats together."""
    feedback = refresh_prediction_feedback(snapshot_ts_ms=snapshot_ts_ms)
    stats = compute_model_performance_stats()
    return {
        "ok": bool(feedback.get("ok", True) and stats.get("ok", True)),
        "feedback": feedback,
        "stats": stats,
    }


def list_prediction_feedback(
    *,
    model_id: Optional[str] = None,
    symbol: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """List recent prediction-feedback rows for one model or symbol."""
    init_metrics_db()
    con = connect(readonly=True)
    try:
        where: List[str] = []
        params: List[Any] = []
        if str(model_id or "").strip():
            where.append("model_id=?")
            params.append(_normalize_model_id(model_id))
        if str(symbol or "").strip():
            where.append("symbol=?")
            params.append(_normalize_symbol(symbol))
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = con.execute(
            f"""
            SELECT
              prediction_id, prediction_ts_ms, resolution_ts_ms, event_id, source_alert_id,
              model_id, model_name, model_version, symbol, horizon_s, predicted_z,
              confidence, realized_z, realized_pnl, unrealized_pnl, net_pnl,
              fees, slippage_bps, trade_count, prediction_correct, pnl_correct, meta_json
            FROM model_prediction_feedback
            {where_sql}
            ORDER BY resolution_ts_ms DESC, prediction_id DESC
            LIMIT ?
            """,
            tuple(params + [max(1, int(limit))]),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
                    "prediction_id": int(row[0]),
                    "prediction_ts_ms": int(row[1]),
                    "resolution_ts_ms": int(row[2] or 0),
                    "event_id": int(row[3]),
                    "source_alert_id": (_safe_int(row[4], 0) if row[4] is not None else None),
                    "model_id": str(row[5]),
                    "model_name": str(row[6] or ""),
                    "model_version": (str(row[7]).strip() if row[7] not in (None, "") else None),
                    "symbol": str(row[8]),
                    "horizon_s": int(row[9]),
                    "predicted_z": float(row[10]),
                    "confidence": float(row[11]),
                    "realized_z": (float(row[12]) if row[12] is not None else None),
                    "realized_pnl": (float(row[13]) if row[13] is not None else None),
                    "unrealized_pnl": (float(row[14]) if row[14] is not None else None),
                    "net_pnl": (float(row[15]) if row[15] is not None else None),
                    "fees": (float(row[16]) if row[16] is not None else None),
                    "slippage_bps": (float(row[17]) if row[17] is not None else None),
                    "trade_count": int(row[18] or 0),
                    "prediction_correct": _safe_binary_flag(row[19]),
                    "pnl_correct": _safe_binary_flag(row[20]),
                    "meta": _safe_json_dict(row[21]),
                }
            )
        return out
    finally:
        con.close()


def get_model_performance_stats(
    *,
    scope: str = "model",
    model_id: Optional[str] = None,
    symbol: Optional[str] = None,
    horizon_s: Optional[int] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Return aggregated performance statistics for the requested scope."""
    init_metrics_db()
    con = connect(readonly=True)
    try:
        where = ["scope=?"]
        params: List[Any] = [str(scope)]
        if str(model_id or "").strip():
            where.append("model_id=?")
            params.append(_normalize_model_id(model_id))
        if str(symbol or "").strip():
            where.append("symbol=?")
            params.append(_normalize_symbol(symbol))
        if horizon_s is not None:
            where.append("horizon_s=?")
            params.append(int(horizon_s))
        rows = con.execute(
            f"""
            SELECT
              scope, model_id, model_name, symbol, horizon_s, prediction_count,
              resolved_count, trade_count, accuracy, pnl_accuracy, win_rate, sharpe,
              max_drawdown, sum_realized_pnl, sum_net_pnl, avg_confidence,
              updated_ts_ms, metrics_json
            FROM model_performance_stats
            WHERE {' AND '.join(where)}
            ORDER BY sum_net_pnl DESC, updated_ts_ms DESC, model_id ASC
            LIMIT ?
            """,
            tuple(params + [max(1, int(limit))]),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
                    "scope": str(row[0]),
                    "model_id": str(row[1]),
                    "model_name": str(row[2]),
                    "symbol": str(row[3]),
                    "horizon_s": int(row[4]),
                    "prediction_count": int(row[5]),
                    "resolved_count": int(row[6]),
                    "trade_count": int(row[7]),
                    "accuracy": (float(row[8]) if row[8] is not None else None),
                    "pnl_accuracy": (float(row[9]) if row[9] is not None else None),
                    "win_rate": (float(row[10]) if row[10] is not None else None),
                    "sharpe": (float(row[11]) if row[11] is not None else None),
                    "max_drawdown": (float(row[12]) if row[12] is not None else None),
                    "sum_realized_pnl": float(row[13] or 0.0),
                    "sum_net_pnl": float(row[14] or 0.0),
                    "avg_confidence": (float(row[15]) if row[15] is not None else None),
                    "updated_ts_ms": int(row[16]),
                    "metrics": _safe_json_dict(row[17]),
                }
            )
        return out
    finally:
        con.close()


class MetricsEngine:
    """Small facade around prediction-feedback and performance-stat APIs."""

    def refresh_feedback_loop(self, *, snapshot_ts_ms: Optional[int] = None) -> Dict[str, Any]:
        """Refresh feedback rows and performance stats in one call."""
        return refresh_feedback_loop(snapshot_ts_ms=snapshot_ts_ms)

    def refresh_prediction_feedback(self, *, snapshot_ts_ms: Optional[int] = None) -> Dict[str, Any]:
        """Materialize prediction feedback rows against realized outcomes."""
        return refresh_prediction_feedback(snapshot_ts_ms=snapshot_ts_ms)

    def compute_model_performance_stats(self) -> Dict[str, Any]:
        """Rebuild aggregated model-performance statistics."""
        return compute_model_performance_stats()

    def list_prediction_feedback(
        self,
        *,
        model_id: Optional[str] = None,
        symbol: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List recent prediction-feedback rows for one model or symbol."""
        return list_prediction_feedback(model_id=model_id, symbol=symbol, limit=limit)

    def get_model_performance_stats(
        self,
        *,
        scope: str = "model",
        model_id: Optional[str] = None,
        symbol: Optional[str] = None,
        horizon_s: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return aggregated performance statistics for the requested scope."""
        return get_model_performance_stats(
            scope=scope,
            model_id=model_id,
            symbol=symbol,
            horizon_s=horizon_s,
            limit=limit,
        )


DEFAULT_ENGINE = MetricsEngine()


__all__ = [
    "MetricsEngine",
    "DEFAULT_ENGINE",
    "init_metrics_db",
    "refresh_prediction_feedback",
    "compute_model_performance_stats",
    "refresh_feedback_loop",
    "list_prediction_feedback",
    "get_model_performance_stats",
]
