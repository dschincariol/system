"""Net-after-cost label artifact helpers.

This module centralizes the durable label artifact used by training,
evaluation, and promotion.  The artifact is keyed by the original prediction
event, keeps the prediction timestamp separate from the computed timestamp, and
records the execution-cost evidence used to convert gross forward return into a
net return.
"""

from __future__ import annotations

import json
import math
import time
from typing import Any, Dict, Iterable, Mapping, Optional


TABLE_NAME = "net_after_cost_labels"


def now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _looks_like_sqlite(con) -> bool:
    module_name = str(getattr(con, "__class__", type(con)).__module__ or "").lower()
    class_name = str(getattr(con, "__class__", type(con)).__name__ or "").lower()
    return "sqlite" in module_name or "sqlite" in class_name


def _columns(con, table_name: str) -> set[str]:
    if _looks_like_sqlite(con):
        try:
            rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
            return {str(row[1] or "").strip() for row in rows if len(row) > 1}
        except Exception:
            return set()
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ANY (current_schemas(false))
              AND table_name = ?
            """,
            (str(table_name),),
        ).fetchall() or []
        return {str(row[0] or "").strip() for row in rows}
    except Exception:
        return set()


def _table_exists(con, table_name: str) -> bool:
    storage_lookup_available = True
    try:
        from engine.runtime.storage import table_exists as _storage_table_exists

        return bool(_storage_table_exists(con, str(table_name)))
    except Exception:
        storage_lookup_available = False
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (str(table_name),),
        ).fetchone()
        if row:
            return True
    except Exception:
        if not storage_lookup_available and _looks_like_sqlite(con):
            return False
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _alter_add_column_if_missing(con, table_name: str, column_name: str, ddl: str) -> None:
    if str(column_name) in _columns(con, table_name):
        return
    try:
        con.execute(f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS {column_name} {ddl}")
    except Exception:
        # SQLite accepts broad type names but not IF NOT EXISTS for ADD COLUMN.
        try:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")
        except Exception:
            return


def ensure_net_after_cost_labels_schema(con) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
          event_id BIGINT NOT NULL,
          prediction_id BIGINT,
          source_alert_id BIGINT,
          symbol TEXT NOT NULL,
          horizon_s BIGINT NOT NULL,
          label_ts_ms BIGINT NOT NULL,
          entry_ts_ms BIGINT,
          exit_ts_ms BIGINT,
          computed_at_ts_ms BIGINT NOT NULL,
          model_name TEXT,
          model_id TEXT,
          model_version TEXT,
          model_family TEXT NOT NULL DEFAULT 'unknown',
          regime TEXT NOT NULL DEFAULT 'global',
          confidence DOUBLE PRECISION,
          confidence_raw DOUBLE PRECISION,
          confidence_metadata_json JSONB,
          side BIGINT NOT NULL,
          realized BIGINT NOT NULL DEFAULT 0,
          gross_return DOUBLE PRECISION NOT NULL,
          realized_forward_return DOUBLE PRECISION NOT NULL,
          execution_cost_return DOUBLE PRECISION NOT NULL,
          net_return DOUBLE PRECISION NOT NULL,
          fees_bps DOUBLE PRECISION NOT NULL DEFAULT 0,
          slippage_bps DOUBLE PRECISION NOT NULL DEFAULT 0,
          spread_bps DOUBLE PRECISION NOT NULL DEFAULT 0,
          borrow_bps DOUBLE PRECISION NOT NULL DEFAULT 0,
          financing_bps DOUBLE PRECISION NOT NULL DEFAULT 0,
          total_cost_bps DOUBLE PRECISION NOT NULL DEFAULT 0,
          fees_cost DOUBLE PRECISION,
          slippage_cost DOUBLE PRECISION,
          spread_cost DOUBLE PRECISION,
          borrow_cost DOUBLE PRECISION,
          financing_cost DOUBLE PRECISION,
          total_cost DOUBLE PRECISION,
          source TEXT NOT NULL,
          order_count BIGINT NOT NULL DEFAULT 0,
          fill_count BIGINT NOT NULL DEFAULT 0,
          label_metadata_json JSONB,
          PRIMARY KEY (event_id, symbol, horizon_s, label_ts_ms)
        )
        """
    )
    for column_name, ddl in (
        ("prediction_id", "BIGINT"),
        ("source_alert_id", "BIGINT"),
        ("model_family", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("regime", "TEXT NOT NULL DEFAULT 'global'"),
        ("confidence_metadata_json", "JSONB"),
        ("realized_forward_return", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("execution_cost_return", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("borrow_bps", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("financing_bps", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("borrow_cost", "DOUBLE PRECISION"),
        ("financing_cost", "DOUBLE PRECISION"),
        ("order_count", "BIGINT NOT NULL DEFAULT 0"),
        ("fill_count", "BIGINT NOT NULL DEFAULT 0"),
        ("label_metadata_json", "JSONB"),
    ):
        _alter_add_column_if_missing(con, TABLE_NAME, column_name, ddl)
    con.execute(f"CREATE INDEX IF NOT EXISTS idx_net_after_cost_labels_ts ON {TABLE_NAME}(label_ts_ms)")
    con.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_net_after_cost_labels_model_ts
          ON {TABLE_NAME}(model_family, model_name, symbol, horizon_s, label_ts_ms)
        """
    )
    con.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_net_after_cost_labels_realized_net
          ON {TABLE_NAME}(realized, net_return, computed_at_ts_ms)
        """
    )
    con.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_net_after_cost_labels_prediction
          ON {TABLE_NAME}(prediction_id)
        """
    )


def infer_model_family(*values: Any) -> str:
    text = " ".join(str(value or "") for value in values).strip().lower()
    if not text:
        return "unknown"
    if "patchtst" in text:
        return "patchtst"
    if "temporal" in text:
        return "temporal"
    if "lightgbm" in text or "lgbm" in text:
        return "lightgbm"
    if "xgboost" in text or "xgb" in text:
        return "xgboost"
    if "gbm" in text:
        return "gbm"
    if "embed" in text or "ridge" in text:
        return "embed"
    if "regime" in text:
        return "regime_stats"
    if ":" in text:
        return text.split(":", 1)[0] or "unknown"
    if "." in text:
        return text.split(".", 1)[0] or "unknown"
    if "_" in text:
        return text.split("_", 1)[0] or "unknown"
    return text[:64] or "unknown"


def extract_regime(*payloads: Any) -> str:
    for payload in payloads:
        obj = _json_dict(payload)
        candidates: list[Any] = [
            obj.get("regime"),
            obj.get("regime_label"),
            obj.get("market_regime"),
            obj.get("volatility_regime"),
        ]
        for nested_key in ("model_intent", "intent", "signal", "regime_vector", "hmm_regime"):
            nested = obj.get(nested_key)
            if isinstance(nested, dict):
                candidates.extend(
                    [
                        nested.get("regime"),
                        nested.get("regime_label"),
                        nested.get("market_regime"),
                        nested.get("volatility_regime"),
                    ]
                )
        for value in candidates:
            text = str(value or "").strip()
            if text:
                return text
    return "global"


def load_prediction_label_context(
    con,
    *,
    event_id: int,
    symbol: str,
    horizon_s: int,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "event_id": int(event_id),
        "symbol": str(symbol or "").upper().strip(),
        "horizon_s": int(horizon_s),
    }
    if _table_exists(con, "predictions"):
        try:
            row = con.execute(
                """
                SELECT id, ts_ms, predicted_z, confidence, confidence_raw, prediction_strength,
                       model_name, model_id, model_version, volatility_regime, trend_regime, liquidity_regime
                FROM predictions
                WHERE event_id=? AND UPPER(TRIM(symbol))=UPPER(TRIM(?)) AND horizon_s=?
                ORDER BY ts_ms DESC, id DESC
                LIMIT 1
                """,
                (int(event_id), str(symbol), int(horizon_s)),
            ).fetchone()
        except Exception:
            row = None
        if row:
            out.update(
                {
                    "prediction_id": _safe_int(row[0], 0) or None,
                    "label_ts_ms": _safe_int(row[1], 0),
                    "predicted_z": _safe_float(row[2], 0.0),
                    "confidence": _safe_float(row[3], 0.0),
                    "confidence_raw": _safe_float(row[4], _safe_float(row[3], 0.0)),
                    "prediction_strength": _safe_float(row[5], 0.0),
                    "model_name": str(row[6] or ""),
                    "model_id": str(row[7] or ""),
                    "model_version": str(row[8] or ""),
                    "volatility_regime": str(row[9] or ""),
                    "trend_regime": str(row[10] or ""),
                    "liquidity_regime": str(row[11] or ""),
                }
            )

    prediction_id = out.get("prediction_id")
    if _table_exists(con, "alerts"):
        try:
            if prediction_id:
                row = con.execute(
                    """
                    SELECT id, confidence, explain_json, detail_json, model_name, model_id, model_version
                    FROM alerts
                    WHERE prediction_id=?
                    ORDER BY ts_ms DESC, id DESC
                    LIMIT 1
                    """,
                    (int(prediction_id),),
                ).fetchone()
            else:
                row = con.execute(
                    """
                    SELECT id, confidence, explain_json, detail_json, model_name, model_id, model_version
                    FROM alerts
                    WHERE event_id=? AND UPPER(TRIM(symbol))=UPPER(TRIM(?)) AND horizon_s=?
                    ORDER BY ts_ms DESC, id DESC
                    LIMIT 1
                    """,
                    (int(event_id), str(symbol), int(horizon_s)),
                ).fetchone()
        except Exception:
            row = None
        if row:
            out["source_alert_id"] = _safe_int(row[0], 0) or None
            if row[1] is not None:
                out["confidence"] = _safe_float(row[1], _safe_float(out.get("confidence"), 0.0))
            out["alert_explain_json"] = row[2]
            out["alert_detail_json"] = row[3]
            for key, idx in (("model_name", 4), ("model_id", 5), ("model_version", 6)):
                if not str(out.get(key) or "").strip() and row[idx] not in (None, ""):
                    out[key] = str(row[idx])

    out["regime"] = extract_regime(
        out.get("alert_explain_json"),
        out.get("alert_detail_json"),
        {
            "volatility_regime": out.get("volatility_regime"),
            "trend_regime": out.get("trend_regime"),
            "liquidity_regime": out.get("liquidity_regime"),
        },
    )
    out["model_family"] = infer_model_family(out.get("model_name"), out.get("model_id"))
    return out


def _iter_json_dicts(values: Iterable[Any]) -> Iterable[Dict[str, Any]]:
    for value in values:
        obj = _json_dict(value)
        if obj:
            yield obj
            for nested_key in ("extra", "costs", "pnl_attribution", "execution", "execution_quality", "carry"):
                nested = obj.get(nested_key)
                if isinstance(nested, dict):
                    yield dict(nested)


def extract_borrow_financing_costs(payloads: Iterable[Any], *, notional: float = 0.0) -> Dict[str, Any]:
    borrow_cost = 0.0
    financing_cost = 0.0
    borrow_bps = 0.0
    financing_bps = 0.0
    available = False
    for obj in _iter_json_dicts(payloads):
        borrow_value = None
        for key in ("borrow_cost", "borrow_fee", "borrow_fees", "stock_borrow_cost", "short_borrow_cost"):
            if obj.get(key) not in (None, ""):
                borrow_value = obj.get(key)
                break
        financing_value = None
        for key in ("financing_cost", "funding_cost", "carry_cost", "margin_interest", "interest_cost"):
            if obj.get(key) not in (None, ""):
                financing_value = obj.get(key)
                break
        if borrow_value is not None:
            borrow_cost += max(0.0, _safe_float(borrow_value, 0.0))
            available = True
        if financing_value is not None:
            financing_cost += max(0.0, _safe_float(financing_value, 0.0))
            available = True
        if obj.get("borrow_bps") not in (None, ""):
            borrow_bps = max(borrow_bps, max(0.0, _safe_float(obj.get("borrow_bps"), 0.0)))
            available = True
        if obj.get("financing_bps") not in (None, ""):
            financing_bps = max(financing_bps, max(0.0, _safe_float(obj.get("financing_bps"), 0.0)))
            available = True
    if notional > 0.0:
        if borrow_bps <= 0.0 and borrow_cost > 0.0:
            borrow_bps = float(borrow_cost) / float(notional) * 10000.0
        if financing_bps <= 0.0 and financing_cost > 0.0:
            financing_bps = float(financing_cost) / float(notional) * 10000.0
    return {
        "borrow_cost": float(borrow_cost),
        "financing_cost": float(financing_cost),
        "borrow_bps": float(borrow_bps),
        "financing_bps": float(financing_bps),
        "available": bool(available),
    }


def load_execution_trace(
    con,
    *,
    event_id: int,
    symbol: str,
    horizon_s: int,
    label_ts_ms: int,
    exit_ts_ms: int,
    prediction_id: Optional[int] = None,
    source_alert_id: Optional[int] = None,
) -> Dict[str, Any]:
    del event_id, horizon_s
    sym = str(symbol or "").upper().strip()
    trace: Dict[str, Any] = {
        "order_count": 0,
        "fill_count": 0,
        "order_ids": [],
        "fill_ids": [],
        "fees_cost": 0.0,
        "slippage_cost": 0.0,
        "spread_cost": 0.0,
        "total_cost": 0.0,
        "fees_bps": None,
        "slippage_bps": None,
        "spread_bps": None,
        "json_payloads": [],
    }
    order_ids: list[str] = []
    order_payloads: list[Any] = []
    if _table_exists(con, "execution_orders"):
        try:
            rows = con.execute(
                """
                SELECT client_order_id, source_alert_id, prediction_id, model_id, model_version,
                       submit_ts_ms, spread_bps, extra_json
                FROM execution_orders
                WHERE UPPER(TRIM(symbol))=UPPER(TRIM(?))
                  AND submit_ts_ms BETWEEN ? AND ?
                  AND (
                    (? IS NOT NULL AND prediction_id=?)
                    OR (? IS NOT NULL AND source_alert_id=?)
                    OR (? IS NULL AND ? IS NULL)
                  )
                ORDER BY submit_ts_ms ASC, client_order_id ASC
                """,
                (
                    sym,
                    int(label_ts_ms) - 60_000,
                    int(exit_ts_ms) + 60_000,
                    prediction_id,
                    prediction_id,
                    source_alert_id,
                    source_alert_id,
                    prediction_id,
                    source_alert_id,
                ),
            ).fetchall() or []
        except Exception:
            rows = []
        for client_order_id, *_rest, spread_bps, extra_json in rows:
            if client_order_id not in (None, ""):
                order_ids.append(str(client_order_id))
            order_payloads.append(extra_json)
            if spread_bps is not None:
                trace["spread_bps"] = _safe_float(spread_bps, 0.0)
        trace["order_count"] = int(len(order_ids))
        trace["order_ids"] = list(order_ids)

    fill_payloads: list[Any] = []
    if _table_exists(con, "execution_fills"):
        try:
            rows = con.execute(
                """
                SELECT fill_id, client_order_id, fill_ts_ms, fill_qty, fill_px, expected_px,
                       mid_px, bid_px, ask_px, spread_bps, slippage_bps, fees, raw_json, extra_json
                FROM execution_fills
                WHERE UPPER(TRIM(symbol))=UPPER(TRIM(?))
                  AND fill_ts_ms BETWEEN ? AND ?
                  AND (
                    (? IS NOT NULL AND prediction_id=?)
                    OR (? IS NOT NULL AND source_alert_id=?)
                    OR (? IS NULL AND ? IS NULL)
                  )
                ORDER BY fill_ts_ms ASC, id ASC
                """,
                (
                    sym,
                    int(label_ts_ms) - 60_000,
                    int(exit_ts_ms) + 60_000,
                    prediction_id,
                    prediction_id,
                    source_alert_id,
                    source_alert_id,
                    prediction_id,
                    source_alert_id,
                ),
            ).fetchall() or []
        except Exception:
            rows = []
        notional = 0.0
        fees = 0.0
        slippage_weighted = 0.0
        slippage_weight = 0.0
        spread_weighted = 0.0
        spread_weight = 0.0
        for fill_id, client_order_id, _fill_ts, qty, px, _expected_px, _mid_px, _bid_px, _ask_px, spread_bps, slippage_bps, fee, raw_json, extra_json in rows:
            q = abs(_safe_float(qty, 0.0))
            p = abs(_safe_float(px, 0.0))
            fill_notional = q * p
            notional += fill_notional
            fees += max(0.0, _safe_float(fee, 0.0))
            if fill_id not in (None, ""):
                trace["fill_ids"].append(str(fill_id))
            elif client_order_id not in (None, ""):
                trace["fill_ids"].append(str(client_order_id))
            if slippage_bps is not None:
                sl = max(0.0, _safe_float(slippage_bps, 0.0))
                slippage_weighted += sl * max(1e-12, q)
                slippage_weight += max(1e-12, q)
                trace["slippage_cost"] += fill_notional * sl / 10000.0
            if spread_bps is not None:
                sp = max(0.0, _safe_float(spread_bps, 0.0))
                spread_weighted += sp * max(1e-12, q)
                spread_weight += max(1e-12, q)
                trace["spread_cost"] += fill_notional * sp / 10000.0
            fill_payloads.extend([raw_json, extra_json])
        trace["fill_count"] = int(len(rows))
        trace["fees_cost"] = float(fees)
        if notional > 0.0:
            trace["fees_bps"] = float(fees) / float(notional) * 10000.0
        if slippage_weight > 0.0:
            trace["slippage_bps"] = float(slippage_weighted) / float(slippage_weight)
        if spread_weight > 0.0:
            trace["spread_bps"] = float(spread_weighted) / float(spread_weight)
        trace["notional"] = float(notional)

    pnl_payloads: list[Any] = []
    if _table_exists(con, "pnl_attribution"):
        try:
            rows = con.execute(
                """
                SELECT fees, slippage_bps, realized_pnl, unrealized_pnl, extra_json
                FROM pnl_attribution
                WHERE UPPER(TRIM(symbol))=UPPER(TRIM(?))
                  AND (
                    (? IS NOT NULL AND prediction_id=?)
                    OR (? IS NOT NULL AND source_alert_id=?)
                    OR (? IS NULL AND ? IS NULL)
                  )
                ORDER BY ts_ms DESC
                LIMIT 10
                """,
                (
                    sym,
                    prediction_id,
                    prediction_id,
                    source_alert_id,
                    source_alert_id,
                    prediction_id,
                    source_alert_id,
                ),
            ).fetchall() or []
        except Exception:
            rows = []
        for fees, slippage_bps, _realized_pnl, _unrealized_pnl, extra_json in rows:
            if trace["fees_cost"] <= 0.0 and fees is not None:
                trace["fees_cost"] = max(0.0, _safe_float(fees, 0.0))
            if trace["slippage_bps"] is None and slippage_bps is not None:
                trace["slippage_bps"] = max(0.0, _safe_float(slippage_bps, 0.0))
            pnl_payloads.append(extra_json)

    trace["json_payloads"] = list(order_payloads) + list(fill_payloads) + list(pnl_payloads)
    trace["total_cost"] = (
        _safe_float(trace.get("fees_cost"), 0.0)
        + _safe_float(trace.get("slippage_cost"), 0.0)
        + _safe_float(trace.get("spread_cost"), 0.0)
    )
    return trace


def build_net_after_cost_label(
    *,
    event_id: int,
    symbol: str,
    horizon_s: int,
    label_ts_ms: int,
    side: int,
    gross_return: float,
    net_return: float,
    realized_forward_return: float,
    source: str,
    realized: int,
    entry_ts_ms: Optional[int] = None,
    exit_ts_ms: Optional[int] = None,
    computed_at_ts_ms: Optional[int] = None,
    costs: Optional[Mapping[str, Any]] = None,
    context: Optional[Mapping[str, Any]] = None,
    execution_trace: Optional[Mapping[str, Any]] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    ctx = dict(context or {})
    cost = dict(costs or {})
    trace = dict(execution_trace or {})
    cost_return = max(0.0, _safe_float(gross_return, 0.0) - _safe_float(net_return, 0.0))
    trace_payloads = list(trace.get("json_payloads") or [])
    borrow_financing = extract_borrow_financing_costs(
        [ctx.get("alert_explain_json"), ctx.get("alert_detail_json"), dict(metadata or {})] + trace_payloads,
        notional=_safe_float(trace.get("notional"), 0.0),
    )
    total_cost_bps = max(
        _safe_float(cost.get("total_cost_bps"), 0.0),
        _safe_float(trace.get("total_cost_bps"), 0.0),
        float(cost_return) * 10000.0,
    )
    fees_bps = max(_safe_float(cost.get("fees_bps"), 0.0), _safe_float(trace.get("fees_bps"), 0.0))
    slippage_bps = max(_safe_float(cost.get("slippage_bps"), 0.0), _safe_float(trace.get("slippage_bps"), 0.0))
    spread_bps = max(_safe_float(cost.get("spread_bps"), 0.0), _safe_float(trace.get("spread_bps"), 0.0))
    borrow_bps = _safe_float(borrow_financing.get("borrow_bps"), 0.0)
    financing_bps = _safe_float(borrow_financing.get("financing_bps"), 0.0)
    if borrow_bps > 0.0 or financing_bps > 0.0:
        total_cost_bps = max(float(total_cost_bps), fees_bps + slippage_bps + spread_bps + borrow_bps + financing_bps)

    confidence = ctx.get("confidence")
    confidence_raw = ctx.get("confidence_raw")
    confidence_metadata = {
        "predicted_z": ctx.get("predicted_z"),
        "confidence": confidence,
        "confidence_raw": confidence_raw,
        "prediction_strength": ctx.get("prediction_strength"),
        "prediction_id": ctx.get("prediction_id"),
        "source_alert_id": ctx.get("source_alert_id"),
        "volatility_regime": ctx.get("volatility_regime"),
        "trend_regime": ctx.get("trend_regime"),
        "liquidity_regime": ctx.get("liquidity_regime"),
    }
    label_metadata = {
        **dict(metadata or {}),
        "timestamp_safe": True,
        "cost_evidence": {
            "borrow_financing_available": bool(borrow_financing.get("available")),
            "execution_trace_available": bool(trace.get("order_count") or trace.get("fill_count")),
            "order_count": _safe_int(trace.get("order_count"), 0),
            "fill_count": _safe_int(trace.get("fill_count"), 0),
        },
        "execution_trace": {
            "order_ids": list(trace.get("order_ids") or [])[:50],
            "fill_ids": list(trace.get("fill_ids") or [])[:50],
            "notional": trace.get("notional"),
        },
    }

    return {
        "event_id": int(event_id),
        "prediction_id": ctx.get("prediction_id"),
        "source_alert_id": ctx.get("source_alert_id"),
        "symbol": str(symbol or "").upper().strip(),
        "horizon_s": int(horizon_s),
        "label_ts_ms": int(label_ts_ms),
        "entry_ts_ms": (None if entry_ts_ms is None else int(entry_ts_ms)),
        "exit_ts_ms": (None if exit_ts_ms is None else int(exit_ts_ms)),
        "computed_at_ts_ms": int(computed_at_ts_ms if computed_at_ts_ms is not None else now_ms()),
        "model_name": str(ctx.get("model_name") or ""),
        "model_id": str(ctx.get("model_id") or ""),
        "model_version": str(ctx.get("model_version") or ""),
        "model_family": str(ctx.get("model_family") or infer_model_family(ctx.get("model_name"), ctx.get("model_id"))),
        "regime": str(ctx.get("regime") or "global"),
        "confidence": (None if confidence is None else _safe_float(confidence, 0.0)),
        "confidence_raw": (None if confidence_raw is None else _safe_float(confidence_raw, 0.0)),
        "confidence_metadata_json": _json_dumps(confidence_metadata),
        "side": int(1 if int(side or 0) >= 0 else -1),
        "realized": int(1 if int(realized or 0) else 0),
        "gross_return": _safe_float(gross_return, 0.0),
        "realized_forward_return": _safe_float(realized_forward_return, 0.0),
        "execution_cost_return": float(cost_return),
        "net_return": _safe_float(net_return, 0.0),
        "fees_bps": float(fees_bps),
        "slippage_bps": float(slippage_bps),
        "spread_bps": float(spread_bps),
        "borrow_bps": float(borrow_bps),
        "financing_bps": float(financing_bps),
        "total_cost_bps": float(total_cost_bps),
        "fees_cost": trace.get("fees_cost"),
        "slippage_cost": trace.get("slippage_cost"),
        "spread_cost": trace.get("spread_cost"),
        "borrow_cost": float(borrow_financing.get("borrow_cost") or 0.0),
        "financing_cost": float(borrow_financing.get("financing_cost") or 0.0),
        "total_cost": (
            _safe_float(trace.get("total_cost"), 0.0)
            + float(borrow_financing.get("borrow_cost") or 0.0)
            + float(borrow_financing.get("financing_cost") or 0.0)
        ),
        "source": str(source or "unknown"),
        "order_count": _safe_int(trace.get("order_count"), 0),
        "fill_count": _safe_int(trace.get("fill_count"), 0),
        "label_metadata_json": _json_dumps(label_metadata),
    }


_UPSERT_COLUMNS = (
    "event_id",
    "prediction_id",
    "source_alert_id",
    "symbol",
    "horizon_s",
    "label_ts_ms",
    "entry_ts_ms",
    "exit_ts_ms",
    "computed_at_ts_ms",
    "model_name",
    "model_id",
    "model_version",
    "model_family",
    "regime",
    "confidence",
    "confidence_raw",
    "confidence_metadata_json",
    "side",
    "realized",
    "gross_return",
    "realized_forward_return",
    "execution_cost_return",
    "net_return",
    "fees_bps",
    "slippage_bps",
    "spread_bps",
    "borrow_bps",
    "financing_bps",
    "total_cost_bps",
    "fees_cost",
    "slippage_cost",
    "spread_cost",
    "borrow_cost",
    "financing_cost",
    "total_cost",
    "source",
    "order_count",
    "fill_count",
    "label_metadata_json",
)


def upsert_net_after_cost_label(con, artifact: Mapping[str, Any]) -> None:
    ensure_net_after_cost_labels_schema(con)
    values = [artifact.get(column) for column in _UPSERT_COLUMNS]
    placeholders = ",".join("?" for _ in _UPSERT_COLUMNS)
    updates = ",".join(
        f"{column}=excluded.{column}"
        for column in _UPSERT_COLUMNS
        if column not in {"event_id", "symbol", "horizon_s"}
    )
    con.execute(
        f"""
        INSERT INTO {TABLE_NAME}({",".join(_UPSERT_COLUMNS)})
        VALUES ({placeholders})
        ON CONFLICT(event_id, symbol, horizon_s, label_ts_ms) DO UPDATE SET {updates}
        """,
        tuple(values),
    )


def net_cost_evidence_summary(
    con,
    *,
    model_name: str | None = None,
    model_family: str | None = None,
    lookback_days: int = 90,
    min_ts_ms: int | None = None,
) -> Dict[str, Any]:
    if not _table_exists(con, TABLE_NAME):
        return {"available": False, "n": 0}
    cutoff = int(min_ts_ms if min_ts_ms is not None else now_ms() - int(lookback_days) * 86_400_000)
    predicates = ["label_ts_ms >= ?", "realized=1"]
    params: list[Any] = [int(cutoff)]
    if model_name:
        predicates.append("model_name=?")
        params.append(str(model_name))
    if model_family:
        predicates.append("model_family=?")
        params.append(str(model_family))
    try:
        row = con.execute(
            f"""
            SELECT COUNT(1), AVG(net_return), AVG(gross_return), AVG(execution_cost_return),
                   AVG(total_cost_bps), SUM(CASE WHEN total_cost_bps IS NOT NULL THEN 1 ELSE 0 END)
            FROM {TABLE_NAME}
            WHERE {" AND ".join(predicates)}
            """,
            tuple(params),
        ).fetchone()
    except Exception:
        return {"available": False, "n": 0}
    n = _safe_int((row or [0])[0], 0)
    cost_n = _safe_int((row or [0, 0, 0, 0, 0, 0])[5], 0)
    return {
        "available": bool(n > 0 and cost_n > 0),
        "n": int(n),
        "cost_evidence_n": int(cost_n),
        "avg_net_return": (None if not row or row[1] is None else _safe_float(row[1], 0.0)),
        "avg_gross_return": (None if not row or row[2] is None else _safe_float(row[2], 0.0)),
        "avg_execution_cost_return": (None if not row or row[3] is None else _safe_float(row[3], 0.0)),
        "avg_total_cost_bps": (None if not row or row[4] is None else _safe_float(row[4], 0.0)),
    }
