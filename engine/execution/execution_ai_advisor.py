"""
Advisory-only execution guidance.

This module produces read-side guidance for shaped execution intents but does
not gate or modify order submission.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, connect_ro, init_db, run_write_txn

LOG = get_logger("engine.execution.execution_ai_advisor")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.execution.execution_ai_advisor",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if out != out:
            return float(default)
        return float(out)
    except Exception as e:
        _warn_nonfatal("EXECUTION_AI_ADVISOR_SAFE_FLOAT_FAILED", e, once_key="safe_float", value_type=type(value).__name__)
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal("EXECUTION_AI_ADVISOR_SAFE_INT_FAILED", e, once_key="safe_int", value_type=type(value).__name__)
        return int(default)


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, separators=(",", ":"), sort_keys=True)
    except Exception as e:
        _warn_nonfatal("EXECUTION_AI_ADVISOR_JSON_DUMPS_FAILED", e, once_key="json_dumps", value_type=type(value).__name__)
        return "{}"


def _json_loads(text: Any, default: Any) -> Any:
    try:
        out = json.loads(text or "null")
        return out if out is not None else default
    except Exception as e:
        _warn_nonfatal("EXECUTION_AI_ADVISOR_JSON_LOADS_FAILED", e, once_key="json_loads", value_type=type(text).__name__)
        return default


def _historical_execution_snapshot(symbol: str, broker: str, lookback_n: int = 120) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    br = str(broker or "").strip().lower()
    if not sym:
        return {
            "sample_n": 0,
            "avg_slippage_bps": None,
            "p95_slippage_bps": None,
            "avg_latency_ms": None,
            "source": None,
        }

    con = connect_ro()
    try:
        if br and br != "unknown":
            try:
                row = con.execute(
                    """
                    SELECT
                      COUNT(*),
                      AVG(slippage_bps),
                      AVG(fill_latency_ms)
                    FROM execution_analytics
                    WHERE symbol = ?
                      AND (broker = ? OR broker IS NULL)
                      AND slippage_bps IS NOT NULL
                    """,
                    (sym, br),
                ).fetchone()
                sample_n = int((row or [0])[0] or 0)
                if sample_n > 0:
                    p95_row = con.execute(
                        """
                        SELECT slippage_bps
                        FROM execution_analytics
                        WHERE symbol = ?
                          AND (broker = ? OR broker IS NULL)
                          AND slippage_bps IS NOT NULL
                        ORDER BY slippage_bps ASC
                        LIMIT 1 OFFSET ?
                        """,
                        (sym, br, max(0, int(round((sample_n - 1) * 0.95)))),
                    ).fetchone()
                    return {
                        "sample_n": sample_n,
                        "avg_slippage_bps": (float(row[1]) if row and row[1] is not None else None),
                        "p95_slippage_bps": (float(p95_row[0]) if p95_row and p95_row[0] is not None else None),
                        "avg_latency_ms": (float(row[2]) if row and row[2] is not None else None),
                        "source": "execution_analytics",
                    }
            except Exception as e:
                _warn_nonfatal(
                    "EXECUTION_AI_ADVISOR_ANALYTICS_QUERY_FAILED",
                    e,
                    once_key="historical_execution_analytics_query",
                    symbol=str(sym),
                    broker=str(br),
                )

        try:
            rows = con.execute(
                """
                SELECT slippage_bps, fill_latency_ms
                FROM execution_fills
                WHERE symbol = ?
                  AND slippage_bps IS NOT NULL
                ORDER BY fill_ts_ms DESC, id DESC
                LIMIT ?
                """,
                (sym, int(max(20, min(500, int(lookback_n))))),
            ).fetchall() or []
        except Exception:
            rows = []

        slips = []
        latencies = []
        for slippage_bps, fill_latency_ms in rows:
            if slippage_bps is not None:
                slips.append(float(slippage_bps))
            if fill_latency_ms is not None:
                latencies.append(float(fill_latency_ms))

        slips.sort()
        sample_n = len(slips)
        p95_slip = slips[min(sample_n - 1, max(0, int(round((sample_n - 1) * 0.95))))] if sample_n else None
        avg_slip = (sum(slips) / float(sample_n)) if sample_n else None
        avg_latency = (sum(latencies) / float(len(latencies))) if latencies else None
        return {
            "sample_n": int(sample_n),
            "avg_slippage_bps": avg_slip,
            "p95_slippage_bps": p95_slip,
            "avg_latency_ms": avg_latency,
            "source": "execution_fills" if sample_n else None,
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_AI_ADVISOR_CLOSE_FAILED",
                e,
                once_key="historical_execution_snapshot_close",
                symbol=str(sym),
                broker=str(br),
            )


def _estimate_expected_slippage_bps(order: Dict[str, Any], history: Optional[Dict[str, Any]] = None) -> float:
    preset = order.get("expected_slippage_bps")
    if preset is not None:
        return round(max(0.0, _safe_float(preset, 0.0)), 3)

    overrides = order.get("epe_broker_sim_overrides")
    if not isinstance(overrides, dict):
        overrides = {}

    slip = float(overrides.get("extra_slippage_bps") or 0.0)
    if str(order.get("order_type") or "").upper() == "MARKET":
        slip += 2.0
    if str(order.get("aggressiveness") or "").upper() == "AGGRESSIVE":
        slip += 1.5
    if str(order.get("aggressiveness") or "").upper() == "NEUTRAL":
        slip += 0.6

    volatility = abs(_safe_float(order.get("volatility"), 0.0))
    slip += min(6.0, volatility * 100.0)

    confidence = _safe_float(order.get("confidence"), 0.0)
    if confidence >= 0.8:
        slip += 0.4

    hist = history if isinstance(history, dict) else {}
    avg_hist_slip = hist.get("avg_slippage_bps")
    p95_hist_slip = hist.get("p95_slippage_bps")
    avg_hist_latency = hist.get("avg_latency_ms")
    sample_n = _safe_int(hist.get("sample_n"), 0)

    if sample_n >= 5 and avg_hist_slip is not None:
        slip = max(float(slip), float(avg_hist_slip))
    if sample_n >= 10 and p95_hist_slip is not None:
        slip = max(float(slip), float(p95_hist_slip) * 0.65)
    if sample_n >= 5 and avg_hist_latency is not None and float(avg_hist_latency) >= 8000.0:
        slip += min(2.5, float(avg_hist_latency) / 8000.0)

    return round(max(0.0, slip), 3)


def _advisory_for_order(
    order: Dict[str, Any],
    *,
    ts_ms: int,
    batch_id: Optional[int],
    portfolio_orders_id: Optional[int],
    payload_source: str,
    execution_mode: str,
    broker: str,
) -> Dict[str, Any]:
    symbol = str(order.get("symbol") or "").strip().upper() or "UNKNOWN"
    side = str(order.get("to_side") or order.get("side") or "").strip().upper()
    order_type = str(order.get("order_type") or "").strip().upper()
    aggressiveness = str(order.get("aggressiveness") or "").strip().upper()
    alpha = _safe_float(order.get("epe_alpha_remaining"), 0.0)
    regime_compat = _safe_float(order.get("regime_compatibility"), 1.0)
    confidence = _safe_float(order.get("confidence"), 0.0)
    history = _historical_execution_snapshot(symbol=symbol, broker=broker)
    expected_slippage_bps = _estimate_expected_slippage_bps(order, history)

    urgency = "low"
    recommendation = "advisory_ok"
    rationale: List[str] = []

    if order_type == "MARKET" or aggressiveness == "AGGRESSIVE":
        urgency = "high"
        recommendation = "review_before_send"
        rationale.append("Aggressive execution increases slippage risk.")
    elif aggressiveness == "NEUTRAL":
        urgency = "medium"
        recommendation = "monitor_fill_quality"
        rationale.append("Neutral execution may need closer monitoring.")

    if alpha < 0.25:
        urgency = "high"
        recommendation = "review_before_send"
        rationale.append("Low remaining alpha suggests limited timing edge.")
    elif alpha < 0.45 and urgency == "low":
        urgency = "medium"
        rationale.append("Alpha decay suggests moderate urgency.")

    if regime_compat < 0.55:
        urgency = "medium" if urgency == "low" else urgency
        recommendation = "consider_smaller_slice"
        rationale.append("Regime compatibility is weak for this order.")

    if expected_slippage_bps >= 3.5:
        urgency = "high"
        recommendation = "review_before_send"
        rationale.append("Expected slippage is elevated for current microstructure.")
    elif expected_slippage_bps >= 1.5 and urgency == "low":
        urgency = "medium"
        recommendation = "monitor_fill_quality"
        rationale.append("Expected slippage is above passive baseline.")

    sample_n = _safe_int(history.get("sample_n"), 0)
    avg_hist_slip = history.get("avg_slippage_bps")
    p95_hist_slip = history.get("p95_slippage_bps")
    avg_hist_latency = history.get("avg_latency_ms")
    if sample_n >= 5 and avg_hist_slip is not None:
        rationale.append(
            f"Recent {symbol} executions averaged {float(avg_hist_slip):.2f} bps slippage over {int(sample_n)} fills."
        )
    if sample_n >= 10 and p95_hist_slip is not None and float(p95_hist_slip) >= 4.0:
        urgency = "high" if urgency != "high" else urgency
        recommendation = "review_before_send"
        rationale.append("Tail slippage has been elevated in recent fills.")
    if sample_n >= 5 and avg_hist_latency is not None and float(avg_hist_latency) >= 12000.0:
        urgency = "high" if urgency == "medium" else urgency
        recommendation = "monitor_fill_quality" if recommendation == "advisory_ok" else recommendation
        rationale.append("Recent fill latency has been slow for this execution path.")

    if confidence >= 0.8 and expected_slippage_bps < 1.5 and regime_compat >= 0.75 and urgency == "low":
        rationale.append("Execution profile looks aligned with signal quality.")

    if not rationale:
        rationale.append("No unusual execution risk flags detected.")

    features = {
        "alpha_remaining": round(alpha, 4),
        "regime_compatibility": round(regime_compat, 4),
        "confidence": round(confidence, 4),
        "volatility": round(_safe_float(order.get("volatility"), 0.0), 6),
        "tse_state": str(order.get("tse_state") or ""),
        "tse_action": str(order.get("tse_action") or ""),
        "capital_mode": str(order.get("capital_mode") or ""),
        "history_sample_n": int(sample_n),
        "history_avg_slippage_bps": (round(float(avg_hist_slip), 4) if avg_hist_slip is not None else None),
        "history_p95_slippage_bps": (round(float(p95_hist_slip), 4) if p95_hist_slip is not None else None),
        "history_avg_latency_ms": (round(float(avg_hist_latency), 2) if avg_hist_latency is not None else None),
        "history_source": history.get("source"),
    }
    advisory_blob = {
        "rationale": rationale,
        "expected_slippage_bps": expected_slippage_bps,
        "broker_overrides": order.get("epe_broker_sim_overrides") if isinstance(order.get("epe_broker_sim_overrides"), dict) else {},
        "source_alert_id": order.get("source_alert_id"),
        "history": history,
    }

    return {
        "ts_ms": int(ts_ms),
        "batch_id": (int(batch_id) if batch_id is not None else None),
        "portfolio_orders_id": (int(portfolio_orders_id) if portfolio_orders_id is not None else None),
        "payload_source": str(payload_source or ""),
        "execution_mode": str(execution_mode or ""),
        "broker": str(broker or ""),
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "aggressiveness": aggressiveness,
        "urgency": urgency,
        "recommendation": recommendation,
        "expected_slippage_bps": expected_slippage_bps,
        "confidence": round(confidence, 4),
        "rationale": " ".join(rationale),
        "features_json": _json_dumps(features),
        "advisory_json": _json_dumps(advisory_blob),
    }


def persist_execution_advisories(
    *,
    shaped_payload: List[Dict[str, Any]],
    batch_id: Optional[int] = None,
    portfolio_orders_id: Optional[int] = None,
    payload_source: str = "",
    execution_mode: str = "",
    broker: str = "",
    ts_ms: Optional[int] = None,
) -> List[Dict[str, Any]]:
    init_db()
    now_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
    advisories = [
        _advisory_for_order(
            dict(order or {}),
            ts_ms=now_ms,
            batch_id=batch_id,
            portfolio_orders_id=portfolio_orders_id,
            payload_source=payload_source,
            execution_mode=execution_mode,
            broker=broker,
        )
        for order in list(shaped_payload or [])
        if isinstance(order, dict) and str(order.get("symbol") or "").strip()
    ]
    if not advisories:
        return []

    con = connect()
    try:
        for item in advisories:
            cur = con.execute(
                """
                INSERT INTO execution_ai_advisory(
                  ts_ms, batch_id, portfolio_orders_id, payload_source, execution_mode, broker,
                  symbol, side, order_type, aggressiveness, urgency, recommendation,
                  expected_slippage_bps, confidence, rationale, features_json, advisory_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(item["ts_ms"]),
                    item["batch_id"],
                    item["portfolio_orders_id"],
                    item["payload_source"],
                    item["execution_mode"],
                    item["broker"],
                    item["symbol"],
                    item["side"],
                    item["order_type"],
                    item["aggressiveness"],
                    item["urgency"],
                    item["recommendation"],
                    float(item["expected_slippage_bps"]),
                    float(item["confidence"]),
                    item["rationale"],
                    item["features_json"],
                    item["advisory_json"],
                ),
            )
            item["advisory_id"] = int(cur.lastrowid or 0)
        con.commit()
        return advisories
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_AI_ADVISOR_CLOSE_FAILED",
                e,
                once_key="persist_execution_advisories_close",
            )


def list_execution_advisories(limit: int = 20) -> Dict[str, Any]:
    init_db()
    limit = max(1, min(200, int(limit or 20)))
    con = connect_ro()
    try:
        rows = con.execute(
            """
            SELECT
              a.id, a.ts_ms, a.batch_id, a.portfolio_orders_id, a.payload_source,
              a.execution_mode, a.broker, a.symbol, a.side, a.order_type,
              a.aggressiveness, a.urgency, a.recommendation, a.expected_slippage_bps,
              a.confidence, a.approved, a.rejected, a.rationale, a.features_json,
              a.advisory_json, act.action, act.actor, act.note, act.ts_ms
            FROM execution_ai_advisory a
            LEFT JOIN (
              SELECT x1.advisory_id, x1.action, x1.actor, x1.note, x1.ts_ms
              FROM execution_ai_advisory_actions x1
              JOIN (
                SELECT advisory_id, MAX(ts_ms) AS ts_ms
                FROM execution_ai_advisory_actions
                GROUP BY advisory_id
              ) x2
                ON x2.advisory_id = x1.advisory_id
               AND x2.ts_ms = x1.ts_ms
            ) act
              ON act.advisory_id = a.id
            ORDER BY a.ts_ms DESC, a.id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []

        items = []
        for row in rows:
            items.append(
                {
                    "advisory_id": int(row[0] or 0),
                    "ts_ms": int(row[1] or 0),
                    "batch_id": (int(row[2]) if row[2] is not None else None),
                    "portfolio_orders_id": (int(row[3]) if row[3] is not None else None),
                    "payload_source": str(row[4] or ""),
                    "execution_mode": str(row[5] or ""),
                    "broker": str(row[6] or ""),
                    "symbol": str(row[7] or ""),
                    "side": str(row[8] or ""),
                    "order_type": str(row[9] or ""),
                    "aggressiveness": str(row[10] or ""),
                    "urgency": str(row[11] or ""),
                    "recommendation": str(row[12] or ""),
                    "expected_slippage_bps": float(row[13] or 0.0),
                    "confidence": float(row[14] or 0.0),
                    "approved": bool(int(row[15] or 0)),
                    "rejected": bool(int(row[16] or 0)),
                    "rationale": str(row[17] or ""),
                    "features": _json_loads(row[18], {}),
                    "advisory": _json_loads(row[19], {}),
                    "last_action": {
                        "action": (str(row[20]) if row[20] is not None else None),
                        "actor": (str(row[21]) if row[21] is not None else None),
                        "note": (str(row[22]) if row[22] is not None else None),
                        "ts_ms": (int(row[23]) if row[23] is not None else None),
                    },
                }
            )

        summary = {
            "count": int(len(items)),
            "high_urgency": int(sum(1 for item in items if str(item.get("urgency") or "").lower() == "high")),
            "approved": int(sum(1 for item in items if bool(item.get("approved")))),
            "rejected": int(sum(1 for item in items if bool(item.get("rejected")))),
        }
        return {"ok": True, "items": items, "summary": summary}
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_AI_ADVISOR_CLOSE_FAILED",
                e,
                once_key="list_execution_advisories_close",
                limit=int(limit),
            )


def record_execution_advisory_action(
    *,
    advisory_id: int,
    action: str,
    actor: str = "operator",
    note: str = "",
    detail: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    act = str(action or "").strip().lower()
    if act not in {"approve", "reject", "note"}:
        raise ValueError("invalid_action")

    now_ms = int(time.time() * 1000)

    def _write(con) -> Dict[str, Any]:
        con.execute(
            """
            INSERT INTO execution_ai_advisory_actions(
              ts_ms, advisory_id, action, actor, note, detail_json
            )
            VALUES (?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                int(advisory_id),
                act,
                str(actor or "operator"),
                str(note or ""),
                _json_dumps(detail or {}),
            ),
        )
        if act == "approve":
            con.execute(
                "UPDATE execution_ai_advisory SET approved = 1, rejected = 0 WHERE id = ?",
                (int(advisory_id),),
            )
        elif act == "reject":
            con.execute(
                "UPDATE execution_ai_advisory SET approved = 0, rejected = 1 WHERE id = ?",
                (int(advisory_id),),
            )
        return {"ok": True, "advisory_id": int(advisory_id), "action": act, "ts_ms": int(now_ms)}

    return dict(run_write_txn(_write) or {"ok": False, "error": "write_failed"})
