"""
FILE: trade_attribution_ledger.py

Execution subsystem module for `trade_attribution_ledger`.
"""

# dev_core/trade_attribution_ledger.py

import json
import logging
import time
from typing import Any, Dict, Optional

from engine.audit.chain import append_chain_row
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db, run_write_txn
from engine.runtime.trade_lifecycle_projection import find_latest_execution_order_projection


def _now_ms() -> int:
    return int(time.time() * 1000)


LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: Optional[str] = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _safe_json_loads(s: Optional[str]) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception as e:
        _warn_nonfatal(
            "trade_attribution_safe_json_load_failed",
            "TRADE_ATTRIBUTION_SAFE_JSON_LOAD_FAILED",
            e,
            warn_key=f"trade_attribution_safe_json_load:{str(s)[:80]}",
            raw_preview=str(s)[:200],
        )
        return None


def _normalize_model_id(model_id: Any) -> str:
    mid = str(model_id or "").strip()
    return mid or "baseline"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _table_has_column(con, table_name: str, column_name: str) -> bool:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
    except Exception as e:
        _warn_nonfatal(
            "trade_attribution_table_has_column_failed",
            "TRADE_ATTRIBUTION_TABLE_HAS_COLUMN_FAILED",
            e,
            warn_key=f"trade_attribution_table_has_column:{table_name}:{column_name}",
            table_name=str(table_name),
            column_name=str(column_name),
        )
        return False
    target = str(column_name or "").strip().lower()
    return any(str(row[1] or "").strip().lower() == target for row in rows if row and len(row) > 1)


def _pick_model_from_explain(explain: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(explain, dict):
        return {}
    # Common patterns in this codebase:
    # - model_name / model_kind / model_ts_ms
    # - model / model_meta nested objects
    m: Dict[str, Any] = {}
    for k in ("model_name", "model_kind", "model_ts_ms", "model_version", "horizon_s"):
        if k in explain and explain.get(k) is not None:
            m[k] = explain.get(k)
    if explain.get("model_id") is not None:
        m["model_id"] = _normalize_model_id(explain.get("model_id"))
    if "model" in explain and isinstance(explain.get("model"), dict):
        for k, v in (explain.get("model") or {}).items():
            if v is not None:
                m[f"model.{k}"] = v
    if "model_meta" in explain and isinstance(explain.get("model_meta"), dict):
        for k, v in (explain.get("model_meta") or {}).items():
            if v is not None:
                m[f"model_meta.{k}"] = v
    return m


def _extract_model_from_execution_order(extra: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(extra, dict):
        return {}
    out: Dict[str, Any] = {}
    source_model = extra.get("source_model")
    if isinstance(source_model, dict):
        for key in ("model_id", "model_name", "model_kind", "model_ts_ms", "model_version", "horizon_s", "regime", "market_regime"):
            val = source_model.get(key)
            if val is not None:
                out[key] = (_normalize_model_id(val) if key == "model_id" else val)
    for key in ("model_id", "model_name", "model_kind", "model_ts_ms", "model_version", "horizon_s", "regime", "market_regime"):
        val = extra.get(key)
        if val is not None and out.get(key) is None:
            out[key] = (_normalize_model_id(val) if key == "model_id" else val)
    for key in ("model", "strategy", "meta", "original_order", "explain"):
        nested = extra.get(key)
        if isinstance(nested, dict):
            nested_out = _extract_model_from_execution_order(nested)
            for nk, nv in nested_out.items():
                if nv is not None and out.get(nk) is None:
                    out[nk] = nv
    return out


def _merge_model_json(primary: Optional[Dict[str, Any]], secondary: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for src in (secondary, primary):
        if not isinstance(src, dict):
            continue
        for key, value in src.items():
            if value is not None:
                out[key] = (_normalize_model_id(value) if key == "model_id" else value)
    out["model_id"] = _normalize_model_id(out.get("model_id"))
    return out


def _pick_regime_vector_from_explain(explain: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(explain, dict):
        return {}
    # common patterns:
    # - regime / current_regime
    # - regime_vector / regime_vec
    out: Dict[str, Any] = {}
    for k in ("regime", "current_regime", "regime_label"):
        if k in explain and explain.get(k) is not None:
            out[k] = explain.get(k)
    for k in ("market_regime", "market_regime_label"):
        if k in explain and explain.get(k) is not None:
            out[k] = explain.get(k)
    if "market_regime_snapshot" in explain and isinstance(explain.get("market_regime_snapshot"), dict):
        out["market_regime_snapshot"] = explain.get("market_regime_snapshot")
    for k in ("regime_vector", "regime_vec", "regime_features"):
        if k in explain and isinstance(explain.get(k), dict):
            out[k] = explain.get(k)
    return out


def ensure_trade_attribution_ready() -> None:
    init_db()
    # storage.init_db() now ensures table exists; nothing else required.


def log_suppression(
    *,
    source_alert_id: Optional[int],
    symbol: str,
    suppression_reason: str,
    signal_json: Optional[Dict[str, Any]] = None,
    model_json: Optional[Dict[str, Any]] = None,
    regime_vector_json: Optional[Dict[str, Any]] = None,
    execution_policy_json: Optional[Dict[str, Any]] = None,
    decision_json: Optional[Dict[str, Any]] = None,
) -> None:
    ensure_trade_attribution_ready()
    con = connect(readonly=False)
    try:
        # Suppressed trades are still first-class attribution events so later
        # governance can compare executed vs suppressed opportunity sets.
        ts = _now_ms()
        append_chain_row(
            "trade_attribution_ledger",
            {
                "ts_ms": int(ts),
                "source_alert_id": int(source_alert_id) if source_alert_id is not None else None,
                "model_id": _normalize_model_id((model_json or {}).get("model_id")),
                "symbol": str(symbol or "").strip().upper(),
                "signal_json": signal_json or {},
                "model_json": model_json or {},
                "regime_vector_json": regime_vector_json or {},
                "execution_policy_json": execution_policy_json or {},
                "suppression_reason": str(suppression_reason or "").strip(),
                "pnl": None,
                "fees": None,
                "slippage_bps": None,
                "decision_json": decision_json or {},
                "created_ts_ms": int(ts),
            },
            con,
        )
        con.commit()
    finally:
        con.close()


def upsert_from_pnl_attribution_snapshot(snapshot_ts_ms: int) -> Dict[str, Any]:
    """
    Reads a specific ts_ms from pnl_attribution, enriches with:
      - alerts.explain_json (signal/model/regime hints)
      - portfolio_orders.explain_json + id (source_order_id / execution intent)
      - execution_policy_audit.decision_json (execution policy decision record)
    Writes rows into trade_attribution_ledger so every $ is explainable.
    This is the main bridge from raw pnl rows to operator-facing causal context.
    """
    ensure_trade_attribution_ready()
    con = connect(readonly=False)
    try:
        pts = int(snapshot_ts_ms or 0)
        if pts is None:
            return {"ok": False, "status": "no_pnl_attribution"}

        rows = con.execute(
            """
            SELECT
              p.ts_ms,
              p.source_alert_id,
              p.model_id,
              p.model_version,
              p.symbol,
              p.pnl,
              p.fees,
              p.slippage_bps,
              p.position_size,
              p.avg_price,
              p.realized_pnl,
              p.unrealized_pnl,
              p.extra_json
            FROM pnl_attribution p
            WHERE p.ts_ms = ?
            """,
            (int(pts),),
        ).fetchall()

        n = 0
        for ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees, slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json in rows or []:
            sid = int(source_alert_id)
            mid = _normalize_model_id(model_id)
            sym = str(symbol or "").strip().upper()
            pnl_extra = _safe_json_loads(extra_json) if extra_json else None
            slippage_cost = 0.0
            if isinstance(pnl_extra, dict):
                try:
                    slippage_cost = float(pnl_extra.get("slippage_cost") or 0.0)
                except Exception:
                    slippage_cost = 0.0
            canonical_realized = float(realized_pnl or 0.0)
            canonical_unrealized = float(unrealized_pnl or 0.0)
            canonical_total = float(
                canonical_realized
                + canonical_unrealized
                - float(fees or 0.0)
                - float(slippage_cost)
            )

            signal_json: Dict[str, Any] = {
                "source_alert_id": sid,
                "model_id": mid,
                "model_version": (str(model_version).strip() if model_version not in (None, "") else None),
                "pnl_attribution": {
                    "position_size": (float(position_size) if position_size is not None else None),
                    "avg_price": (float(avg_price) if avg_price is not None else None),
                    "realized_pnl": float(canonical_realized),
                    "unrealized_pnl": float(canonical_unrealized),
                    "total_pnl": float(canonical_total),
                    "extra": pnl_extra,
                },
            }
            execution_quality = (
                (pnl_extra or {}).get("execution_quality")
                if isinstance(pnl_extra, dict)
                else None
            )
            if isinstance(execution_quality, dict):
                signal_json["pnl_attribution"]["execution_quality"] = {
                    "expected_price": (
                        _safe_float(execution_quality.get("expected_price"))
                        if execution_quality.get("expected_price") is not None
                        else None
                    ),
                    "fill_price": (
                        _safe_float(execution_quality.get("fill_price"))
                        if execution_quality.get("fill_price") is not None
                        else None
                    ),
                    "slippage": (
                        _safe_float(execution_quality.get("slippage"))
                        if execution_quality.get("slippage") is not None
                        else None
                    ),
                    "abs_slippage": (
                        _safe_float(execution_quality.get("abs_slippage"))
                        if execution_quality.get("abs_slippage") is not None
                        else None
                    ),
                    "avg_latency_ms": (
                        _safe_float(execution_quality.get("avg_latency_ms"))
                        if execution_quality.get("avg_latency_ms") is not None
                        else None
                    ),
                    "max_latency_ms": (
                        _safe_float(execution_quality.get("max_latency_ms"))
                        if execution_quality.get("max_latency_ms") is not None
                        else None
                    ),
                }
            model_json: Dict[str, Any] = {}
            regime_vec: Dict[str, Any] = {}
            execution_order_model: Dict[str, Any] = {}
            try:
                a = con.execute(
                    """
                    SELECT ts_ms, event_title, symbol, horizon_s, expected_z, confidence,
                           severity, rule_id, explain_json
                    FROM alerts
                    WHERE id=?
                    """,
                    (int(sid),),
                ).fetchone()
                if a:
                    ax = _safe_json_loads(a[8]) if a[8] else None
                    signal_json.update(
                        {
                            "alert_ts_ms": int(a[0] or 0),
                            "event_title": str(a[1] or ""),
                            "symbol": str(a[2] or sym),
                            "horizon_s": int(a[3] or 0),
                            "expected_z": float(a[4] or 0.0),
                            "confidence": float(a[5] or 0.0),
                            "severity": str(a[6] or ""),
                            "rule_id": str(a[7] or ""),
                        }
                    )
                    if isinstance(ax, dict):
                        model_json = _pick_model_from_explain(ax)
                        regime_vec = _pick_regime_vector_from_explain(ax)
                        signal_json["alert_explain"] = ax
            except Exception as e:
                _warn_nonfatal(
                    "trade_attribution_alert_context_load_failed",
                    "TRADE_ATTRIBUTION_ALERT_CONTEXT_LOAD_FAILED",
                    e,
                    warn_key=f"trade_attribution_alert_context_load_failed:{sid}:{mid}:{sym}",
                    source_alert_id=int(sid),
                    model_id=str(mid),
                    symbol=str(sym),
                )

            try:
                eo = find_latest_execution_order_projection(
                    con,
                    source_alert_id=int(sid),
                    model_id=str(mid),
                    symbol=str(sym),
                )
                if eo:
                    exo = eo.get("extra_json") if isinstance(eo, dict) else None
                    if isinstance(exo, dict):
                        execution_order_model = _extract_model_from_execution_order(exo)
                        signal_json["execution_order"] = {
                            "client_order_id": str(eo.get("client_order_id") or ""),
                            "submit_ts_ms": int(eo.get("submit_ts_ms") or 0),
                        }
                        if execution_order_model.get("regime") and not regime_vec.get("regime"):
                            regime_vec["regime"] = execution_order_model.get("regime")
                        if execution_order_model.get("market_regime") and not regime_vec.get("market_regime"):
                            regime_vec["market_regime"] = execution_order_model.get("market_regime")
                        if execution_order_model.get("horizon_s") is not None and signal_json.get("horizon_s") is None:
                            signal_json["horizon_s"] = execution_order_model.get("horizon_s")
            except Exception as e:
                _warn_nonfatal(
                    "trade_attribution_execution_order_context_load_failed",
                    "TRADE_ATTRIBUTION_EXECUTION_ORDER_CONTEXT_LOAD_FAILED",
                    e,
                    warn_key=f"trade_attribution_execution_order_context_load_failed:{sid}:{mid}:{sym}",
                    source_alert_id=int(sid),
                    model_id=str(mid),
                    symbol=str(sym),
                )

            try:
                po = None
                if _table_has_column(con, "portfolio_orders", "source_alert_id"):
                    po = con.execute(
                        """
                        SELECT id, ts_ms, action, from_side, to_side, from_weight, to_weight, delta_weight, explain_json
                        FROM portfolio_orders
                        WHERE source_alert_id=? AND model_id=? AND symbol=?
                        ORDER BY ts_ms DESC, id DESC
                        LIMIT 1
                        """,
                        (int(sid), mid, sym),
                    ).fetchone()
                if po:
                    signal_json.update(
                        {
                            "source_order_id": int(po[0]),
                            "portfolio_order_ts_ms": int(po[1] or 0),
                            "action": str(po[2] or ""),
                            "from_side": str(po[3] or ""),
                            "to_side": str(po[4] or ""),
                            "from_weight": float(po[5] or 0.0),
                            "to_weight": float(po[6] or 0.0),
                            "delta_weight": float(po[7] or 0.0),
                        }
                    )
                    px = _safe_json_loads(po[8]) if po[8] else None
                    if isinstance(px, dict):
                        signal_json["portfolio_explain"] = px
                        model_json = _merge_model_json(
                            execution_order_model,
                            model_json,
                        )
                        model_json = _merge_model_json(
                            _pick_model_from_explain(px),
                            model_json,
                        )
                        if not regime_vec:
                            regime_vec = _pick_regime_vector_from_explain(px)

                        strategy_obj = px.get("strategy")
                        if (
                            isinstance(strategy_obj, dict)
                            and strategy_obj.get("name")
                            and not model_json.get("model_name")
                        ):
                            model_json["model_name"] = str(strategy_obj.get("name"))

                        execution_obj = px.get("execution")
                        if isinstance(execution_obj, dict):
                            strategy_alloc = execution_obj.get("strategy_alloc")
                            if (
                                isinstance(strategy_alloc, dict)
                                and len(strategy_alloc) == 1
                                and not model_json.get("model_name")
                            ):
                                only_key = next(iter(strategy_alloc.keys()), None)
                                if only_key:
                                    model_json["model_name"] = str(only_key)
            except Exception as e:
                _warn_nonfatal(
                    "trade_attribution_portfolio_order_context_load_failed",
                    "TRADE_ATTRIBUTION_PORTFOLIO_ORDER_CONTEXT_LOAD_FAILED",
                    e,
                    warn_key=f"trade_attribution_portfolio_order_context_load_failed:{sid}:{mid}:{sym}",
                    source_alert_id=int(sid),
                    model_id=str(mid),
                    symbol=str(sym),
                )

            model_json = _merge_model_json(execution_order_model, model_json)

            execution_policy_json: Dict[str, Any] = {}
            decision_json: Dict[str, Any] = {}
            try:
                e = None
                if not _table_has_column(con, "execution_policy_audit", "source_alert_id"):
                    e = None
                elif _table_has_column(con, "execution_policy_audit", "model_id"):
                    e = con.execute(
                        """
                        SELECT decision_json
                        FROM execution_policy_audit
                        WHERE source_alert_id=? AND model_id=?
                        ORDER BY ts_ms DESC
                        LIMIT 1
                        """,
                        (int(sid), mid),
                    ).fetchone()
                else:
                    e = con.execute(
                        """
                        SELECT decision_json
                        FROM execution_policy_audit
                        WHERE source_alert_id=?
                        ORDER BY ts_ms DESC
                        LIMIT 1
                        """,
                        (int(sid),),
                    ).fetchone()
                if e and e[0]:
                    dj = _safe_json_loads(e[0])
                    if isinstance(dj, dict):
                        execution_policy_json = dj
                        decision_json = dj
                        if not model_json:
                            strategy_name = dj.get("strategy_name") or dj.get(
                                "model_name"
                            )
                            if isinstance(strategy_name, str) and strategy_name.strip():
                                model_json["model_name"] = str(strategy_name).strip()
            except Exception as e:
                _warn_nonfatal(
                    "trade_attribution_execution_policy_context_load_failed",
                    "TRADE_ATTRIBUTION_EXECUTION_POLICY_CONTEXT_LOAD_FAILED",
                    e,
                    warn_key=f"trade_attribution_execution_policy_context_load_failed:{sid}:{mid}",
                    source_alert_id=int(sid),
                    model_id=str(mid),
                )

            append_chain_row(
                "trade_attribution_ledger",
                {
                    "ts_ms": int(ts_ms),
                    "source_alert_id": int(sid),
                    "model_id": mid,
                    "symbol": sym,
                    "signal_json": signal_json or {},
                    "model_json": model_json or {},
                    "regime_vector_json": regime_vec or {},
                    "execution_policy_json": execution_policy_json or {},
                    "suppression_reason": None,
                    "pnl": float(canonical_total),
                    "fees": float(fees or 0.0),
                    "slippage_bps": float(slippage_bps) if slippage_bps is not None else None,
                    "expected_price": (
                        _safe_float(execution_quality.get("expected_price"))
                        if isinstance(execution_quality, dict)
                        and execution_quality.get("expected_price") is not None
                        else None
                    ),
                    "fill_price": (
                        _safe_float(execution_quality.get("fill_price"))
                        if isinstance(execution_quality, dict)
                        and execution_quality.get("fill_price") is not None
                        else None
                    ),
                    "execution_slippage": (
                        _safe_float(execution_quality.get("slippage"))
                        if isinstance(execution_quality, dict)
                        and execution_quality.get("slippage") is not None
                        else None
                    ),
                    "execution_latency_ms": (
                        _safe_float(execution_quality.get("avg_latency_ms"))
                        if isinstance(execution_quality, dict)
                        and execution_quality.get("avg_latency_ms") is not None
                        else None
                    ),
                    "decision_json": decision_json or {},
                    "created_ts_ms": int(_now_ms()),
                },
                con,
            )
            n += 1

        con.commit()
        out = {"ok": True, "snapshot_ts_ms": int(pts), "rows_upserted": int(n)}
        try:
            out["completeness"] = attribution_completeness_snapshot()
        except Exception as e:
            _warn_nonfatal(
                "trade_attribution_completeness_snapshot_failed",
                "TRADE_ATTRIBUTION_COMPLETENESS_SNAPSHOT_FAILED",
                e,
                warn_key="trade_attribution_completeness_snapshot_failed",
                snapshot_ts_ms=int(pts),
            )
        return out
    finally:
        con.close()


def upsert_from_latest_pnl_attribution_snapshot() -> Dict[str, Any]:
    ensure_trade_attribution_ready()
    con = connect(readonly=False)
    try:
        r = con.execute("SELECT MAX(ts_ms) FROM pnl_attribution").fetchone()
        pts = int(r[0]) if r and r[0] is not None else None
    finally:
        con.close()
    if pts is None:
        return {"ok": False, "status": "no_pnl_attribution"}
    return upsert_from_pnl_attribution_snapshot(int(pts))


def rebuild_historical_trade_attribution(
    *,
    limit_snapshots: int = 50,
    max_snapshot_age_ms: Optional[int] = None,
) -> Dict[str, Any]:
    ensure_trade_attribution_ready()
    con = connect(readonly=True)
    try:
        sql = """
            SELECT DISTINCT ts_ms
            FROM pnl_attribution
            {where_clause}
            ORDER BY ts_ms DESC
            LIMIT ?
        """
        params = [max(1, int(limit_snapshots or 50))]
        where_clause = ""
        if max_snapshot_age_ms is not None and int(max_snapshot_age_ms) > 0:
            cutoff_ts_ms = _now_ms() - int(max_snapshot_age_ms)
            where_clause = "WHERE ts_ms >= ?"
            params = [int(cutoff_ts_ms), max(1, int(limit_snapshots or 50))]
        rows = con.execute(sql.format(where_clause=where_clause), tuple(params)).fetchall()
        snapshots = [_now for (_now,) in rows or [] if _now is not None]
    finally:
        con.close()

    rebuilt = 0
    rows_upserted = 0
    last_snapshot_ts_ms = 0
    for snapshot_ts_ms in snapshots:
        result = upsert_from_pnl_attribution_snapshot(int(snapshot_ts_ms))
        if not bool(result.get("ok")):
            continue
        rebuilt += 1
        rows_upserted += int(result.get("rows_upserted") or 0)
        last_snapshot_ts_ms = max(last_snapshot_ts_ms, int(result.get("snapshot_ts_ms") or 0))

    out = {
        "ok": True,
        "snapshots_rebuilt": int(rebuilt),
        "rows_upserted": int(rows_upserted),
        "last_snapshot_ts_ms": int(last_snapshot_ts_ms),
    }
    try:
        out["completeness"] = attribution_completeness_snapshot()
    except Exception as e:
        _warn_nonfatal(
            "trade_attribution_rebuild_completeness_snapshot_failed",
            "TRADE_ATTRIBUTION_REBUILD_COMPLETENESS_SNAPSHOT_FAILED",
            e,
            warn_key="trade_attribution_rebuild_completeness_snapshot_failed",
            snapshots_rebuilt=int(rebuilt),
        )
    return out


def attribution_completeness_snapshot(limit: int = 5000) -> Dict[str, Any]:
    ensure_trade_attribution_ready()
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT model_json, regime_vector_json, execution_policy_json
            FROM trade_attribution_ledger
            WHERE suppression_reason IS NULL
              AND pnl IS NOT NULL
            ORDER BY ts_ms DESC, id DESC
            LIMIT ?
            """,
            (max(1, int(limit or 5000)),),
        ).fetchall()

        total = len(rows or [])
        model_present = 0
        authoritative_model_present = 0
        regime_present = 0
        policy_present = 0

        for model_json_raw, regime_json_raw, policy_json_raw in rows or []:
            model_json = _safe_json_loads(model_json_raw)
            regime_json = _safe_json_loads(regime_json_raw)
            policy_json = _safe_json_loads(policy_json_raw)

            if isinstance(model_json, dict) and model_json:
                model_present += 1
                if (
                    str(model_json.get("model_name") or "").strip()
                    and (
                        str(model_json.get("model_kind") or "").strip()
                        or model_json.get("model_ts_ms") not in (None, "")
                    )
                ):
                    authoritative_model_present += 1

            if isinstance(regime_json, dict) and regime_json:
                regime_present += 1
            if isinstance(policy_json, dict) and policy_json:
                policy_present += 1

        denom = float(max(1, total))
        return {
            "ok": True,
            "rows": int(total),
            "model_present": int(model_present),
            "authoritative_model_present": int(authoritative_model_present),
            "regime_present": int(regime_present),
            "policy_present": int(policy_present),
            "model_present_ratio": float(model_present) / denom,
            "authoritative_model_present_ratio": float(authoritative_model_present) / denom,
            "regime_present_ratio": float(regime_present) / denom,
            "policy_present_ratio": float(policy_present) / denom,
        }
    finally:
        con.close()


def _ensure_suppression_opportunity_tables(con) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS suppression_opportunity (
          ts_ms INTEGER NOT NULL,
          ledger_id INTEGER NOT NULL,
          source_alert_id INTEGER,
          symbol TEXT NOT NULL,
          suppression_reason TEXT NOT NULL,

          equity REAL,
          to_weight REAL,
          expected_z REAL,
          confidence REAL,
          volatility REAL,

          expected_alpha_pnl REAL,
          meta_json TEXT,

          PRIMARY KEY (ts_ms, ledger_id)
        );

        CREATE INDEX IF NOT EXISTS idx_supp_opp_ts
          ON suppression_opportunity(ts_ms);

        CREATE INDEX IF NOT EXISTS idx_supp_opp_alert
          ON suppression_opportunity(source_alert_id);

        CREATE INDEX IF NOT EXISTS idx_supp_opp_symbol_ts
          ON suppression_opportunity(symbol, ts_ms);
        """
    )


def _latest_equity(con, ts_ms: int) -> float:
    try:
        r = con.execute(
            """
            SELECT equity
            FROM equity_history
            WHERE ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (int(ts_ms),),
        ).fetchone()
        if not r:
            return 0.0
        return float(r[0] or 0.0)
    except Exception as e:
        _warn_nonfatal(
            "trade_attribution_latest_equity_failed",
            "TRADE_ATTRIBUTION_LATEST_EQUITY_FAILED",
            e,
            warn_key="trade_attribution_latest_equity_failed",
            ts_ms=int(ts_ms),
        )
        return 0.0


def _alert_expected_meta(con, alert_id: int) -> Dict[str, Any]:
    """
    Best-effort:
      expected_z, confidence, volatility (from alerts.explain_json)
    """
    out: Dict[str, Any] = {"expected_z": 0.0, "confidence": 0.0, "volatility": 0.0}
    try:
        r = con.execute(
            """
            SELECT expected_z, confidence, explain_json
            FROM alerts
            WHERE id=?
            """,
            (int(alert_id),),
        ).fetchone()
        if not r:
            return out

        out["expected_z"] = float(r[0] or 0.0)
        out["confidence"] = float(r[1] or 0.0)

        ex = _safe_json_loads(r[2]) if r[2] else None
        if isinstance(ex, dict):
            for k in ("volatility", "vol", "sigma", "realized_vol"):
                if k in ex and ex.get(k) is not None:
                    try:
                        out["volatility"] = float(ex.get(k) or 0.0)
                        break
                    except Exception as e:
                        _warn_nonfatal(
                            "trade_attribution_alert_volatility_parse_failed",
                            "TRADE_ATTRIBUTION_ALERT_VOLATILITY_PARSE_FAILED",
                            e,
                            warn_key="trade_attribution_alert_volatility_parse_failed",
                            alert_id=int(alert_id),
                            key=str(k),
                            value=ex.get(k),
                        )
        return out
    except Exception as e:
        _warn_nonfatal(
            "trade_attribution_alert_expected_meta_failed",
            "TRADE_ATTRIBUTION_ALERT_EXPECTED_META_FAILED",
            e,
            warn_key=f"trade_attribution_alert_expected_meta:{alert_id}",
            alert_id=int(alert_id),
        )
        return out


def suppression_opportunity_snapshot(lookback_ms: int = 86400000) -> Dict[str, Any]:
    """
    Computes counterfactual opportunity cost for suppressed intents:
      expected_alpha_pnl ~= equity * to_weight * (expected_z * volatility)

    Sources:
      - trade_attribution_ledger (suppression_reason != NULL)
      - portfolio_orders (to_weight via source_order_id in signal_json, fallback by source_alert_id)
      - alerts (expected_z/confidence + volatility from explain_json)
      - equity_history (equity at ts_ms)
    Writes into suppression_opportunity.
    """
    ensure_trade_attribution_ready()
    con = connect(readonly=False)
    try:
        now = _now_ms()
        rows = (
            con.execute(
                """
            SELECT id, ts_ms, source_alert_id, symbol, suppression_reason, signal_json
            FROM trade_attribution_ledger
            WHERE suppression_reason IS NOT NULL
              AND ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT 5000
            """,
                (int(now - int(lookback_ms)),),
            ).fetchall()
            or []
        )

        def _write(txn_con):
            wrote = 0
            for (
                rid,
                ts_ms,
                source_alert_id,
                symbol,
                suppression_reason,
                signal_json,
            ) in rows:
                ledger_id = int(rid)
                ts_i = int(ts_ms or 0)
                sym = str(symbol or "").strip().upper()
                reason = str(suppression_reason or "").strip()
                sid = int(source_alert_id) if source_alert_id is not None else None

                sig = _safe_json_loads(signal_json) if signal_json else None
                source_order_id = None
                if isinstance(sig, dict) and sig.get("source_order_id") is not None:
                    try:
                        source_order_id = _safe_int(sig.get("source_order_id"))
                    except Exception:
                        source_order_id = None

                to_weight = None

                if source_order_id is not None:
                    try:
                        po = txn_con.execute(
                            """
                            SELECT to_weight
                            FROM portfolio_orders
                            WHERE id=?
                            """,
                            (int(source_order_id),),
                        ).fetchone()
                        if po and po[0] is not None:
                            to_weight = float(po[0] or 0.0)
                    except Exception as e:
                        _warn_nonfatal(
                            "trade_attribution_source_order_weight_lookup_failed",
                            "TRADE_ATTRIBUTION_SOURCE_ORDER_WEIGHT_LOOKUP_FAILED",
                            e,
                            warn_key=f"trade_attribution_source_order_weight_lookup_failed:{source_order_id}",
                            source_order_id=int(source_order_id),
                            symbol=str(sym),
                        )

                if to_weight is None and sid is not None:
                    try:
                        po = txn_con.execute(
                            """
                            SELECT to_weight
                            FROM portfolio_orders
                            WHERE source_alert_id=? AND symbol=?
                            ORDER BY ts_ms DESC, id DESC
                            LIMIT 1
                            """,
                            (int(sid), sym),
                        ).fetchone()
                        if po and po[0] is not None:
                            to_weight = float(po[0] or 0.0)
                    except Exception as e:
                        _warn_nonfatal(
                            "trade_attribution_alert_weight_lookup_failed",
                            "TRADE_ATTRIBUTION_ALERT_WEIGHT_LOOKUP_FAILED",
                            e,
                            warn_key=f"trade_attribution_alert_weight_lookup_failed:{sid}:{sym}",
                            source_alert_id=int(sid),
                            symbol=str(sym),
                        )

                if to_weight is None:
                    to_weight = 0.0

                equity = _latest_equity(txn_con, ts_i)

                expected_z = 0.0
                confidence = 0.0
                volatility = 0.0
                if sid is not None:
                    em = _alert_expected_meta(txn_con, sid)
                    expected_z = float(em.get("expected_z") or 0.0)
                    confidence = float(em.get("confidence") or 0.0)
                    volatility = float(em.get("volatility") or 0.0)

                expected_ret = float(expected_z) * float(volatility)
                expected_alpha_pnl = (
                    float(equity) * float(to_weight) * float(expected_ret)
                )

                meta = {
                    "source_order_id": source_order_id,
                    "signal_json": sig if isinstance(sig, dict) else None,
                }

                txn_con.execute(
                    """
                    INSERT OR REPLACE INTO suppression_opportunity(
                      ts_ms, ledger_id, source_alert_id, symbol, suppression_reason,
                      equity, to_weight, expected_z, confidence, volatility,
                      expected_alpha_pnl, meta_json
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(ts_i),
                        int(ledger_id),
                        (int(sid) if sid is not None else None),
                        sym,
                        reason,
                        float(equity),
                        float(to_weight),
                        float(expected_z),
                        float(confidence),
                        float(volatility),
                        float(expected_alpha_pnl),
                        json.dumps(meta, separators=(",", ":"), sort_keys=True),
                    ),
                )
                wrote += 1
            return wrote

        wrote = run_write_txn(_write)
        return {"ok": True, "rows_written": int(wrote), "ts_ms": int(now)}
    finally:
        con.close()


def suppression_cost_snapshot(lookback_ms: int = 86400000) -> Dict[str, Any]:
    """
    Measures opportunity cost of suppression:
    compares suppressed signals vs executed pnl.
    """
    con = connect(readonly=True)
    try:
        now = _now_ms()

        executed = con.execute(
            """
            SELECT SUM(
                     COALESCE(
                       CAST(json_extract(signal_json, '$.pnl_attribution.total_pnl') AS REAL),
                       COALESCE(CAST(json_extract(signal_json, '$.pnl_attribution.realized_pnl') AS REAL), 0.0)
                       + COALESCE(CAST(json_extract(signal_json, '$.pnl_attribution.unrealized_pnl') AS REAL), 0.0)
                       - COALESCE(fees, 0.0)
                       - COALESCE(CAST(json_extract(signal_json, '$.pnl_attribution.extra.slippage_cost') AS REAL), 0.0)
                     )
                   )
            FROM trade_attribution_ledger
            WHERE suppression_reason IS NULL
              AND ts_ms >= ?
            """,
            (now - int(lookback_ms),),
        ).fetchone()[0]

        suppressed = con.execute(
            """
            SELECT COUNT(1)
            FROM trade_attribution_ledger
            WHERE suppression_reason IS NOT NULL
              AND ts_ms >= ?
            """,
            (now - int(lookback_ms),),
        ).fetchone()[0]

        return {
            "executed_pnl": float(executed or 0.0),
            "suppressed_count": int(suppressed or 0),
            "ts_ms": int(now),
        }
    finally:
        con.close()
