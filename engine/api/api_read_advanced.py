"""
FILE: api_read_advanced.py

HTTP/API handlers for read advanced endpoints.
"""

"""
Advanced Read-Only API Endpoints
Moved from dashboard_server to enforce layer isolation.
"""

import json
import logging
import time
from typing import Any

from engine.api.api_read import _table_exists
from engine.api.internal_access import db_connect
from engine.api.sql_identifiers import require_allowed_table_name, sql_identifier
from engine.runtime.state_cache import cache_get_or_load
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    connect_ro_direct,
    fetch_recent_audit_records,
    fetch_recent_backtest_cpcv_runs,
    fetch_decision_detail,
    fetch_prediction_explanations,
    fetch_recent_drift_retrain_events,
    fetch_recent_decisions,
    fetch_recent_hypothesis_registry,
    fetch_recent_promotion_statistical_evidence,
)

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.api.api_read_advanced",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def _broker_fills_table(con) -> str:
    # Advanced reads keep legacy table fallback logic local so callers can
    # request semantic data without caring about physical table names.
    if _table_exists(con, "broker_fills_v2"):
        return require_allowed_table_name("broker_fills_v2")
    return require_allowed_table_name("broker_fills")


def _safe_json_obj(payload: Any) -> dict:
    if isinstance(payload, dict):
        return dict(payload)
    if not isinstance(payload, str) or not payload.strip():
        return {}
    try:
        obj = json.loads(payload)
    except Exception as e:
        _warn_nonfatal(
            "API_READ_ADVANCED_JSON_PARSE_FAILED",
            e,
            once_key="api_read_advanced_safe_json_obj",
            payload_preview=str(payload)[:120],
        )
        return {}
    return dict(obj) if isinstance(obj, dict) else {}


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception as e:
        _warn_nonfatal(
            "API_READ_ADVANCED_FLOAT_PARSE_FAILED",
            e,
            once_key="api_read_advanced_float_parse_failed",
            value_preview=str(value)[:120],
        )
        return None


def _shadow_book_snapshot(con, model_id: str) -> dict:
    mid = str(model_id or "").strip()
    if not mid:
        return {}
    book_key = f"shadow:{mid}"
    if (not _table_exists(con, "broker_shadow_account")) or (not _table_exists(con, "broker_shadow_positions")):
        return {}

    account_row = con.execute(
        """
        SELECT cash, equity, updated_ts_ms
        FROM broker_shadow_account
        WHERE book_key=?
        LIMIT 1
        """,
        (book_key,),
    ).fetchone()
    if not account_row:
        return {}

    pos_rows = con.execute(
        """
        SELECT symbol, qty, avg_px, updated_ts_ms
        FROM broker_shadow_positions
        WHERE book_key=?
        ORDER BY ABS(qty) DESC, symbol ASC
        """,
        (book_key,),
    ).fetchall() or []

    return {
        "book_key": str(book_key),
        "account": {
            "cash": float(account_row[0] or 0.0),
            "equity": float(account_row[1] or 0.0),
            "updated_ts_ms": int(account_row[2] or 0),
        },
        "positions": [
            {
                "symbol": str(r[0] or ""),
                "qty": float(r[1] or 0.0),
                "avg_px": float(r[2] or 0.0),
                "updated_ts_ms": int(r[3] or 0),
            }
            for r in pos_rows
        ],
    }


def _diagnostic_rows(con, table: str, sql: str, schema_errors: list[dict[str, Any]]):
    if not _table_exists(con, table):
        return []
    try:
        return con.execute(sql).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "API_READ_ADVANCED_DIAGNOSTIC_QUERY_FAILED",
            e,
            once_key=f"diagnostic_query_failed:{table}",
            table=str(table),
        )
        schema_errors.append(
            {
                "table": str(table),
                "error": str(e),
                "code": "diagnostic_query_failed",
            }
        )
        return []


# --------------------------------------------------
# MODEL DIAGNOSTICS
# --------------------------------------------------

def get_model_diagnostics():
    # This endpoint mixes direct SQL with nested storage helper calls that may
    # open and close their own read handles. Use a dedicated read-only handle
    # here so sibling helper reads cannot invalidate the connection mid-request.
    con = connect_ro_direct()
    try:
        # These diagnostics are read-only aggregations for operator/model
        # inspection and should never mutate training/runtime state.
        out = {}
        schema_errors: list[dict[str, Any]] = []

        rows = _diagnostic_rows(
            con,
            "model_stats_regime",
            """
            SELECT symbol, horizon_s, regime, n, mean_impact_z
            FROM model_stats_regime
            ORDER BY symbol, horizon_s, regime
            """,
            schema_errors,
        )

        priors = {}
        for sym, h, reg, n, mean_z in rows:
            priors.setdefault(f"{sym}:{h}", []).append({
                "regime": reg,
                "n": int(n),
                "mean_z": float(mean_z),
            })
        out["regime_priors"] = priors

        rows = _diagnostic_rows(
            con,
            "model_stats",
            """
            SELECT symbol, horizon_s, n, mean_impact_z
            FROM model_stats
            ORDER BY symbol, horizon_s
            """,
            schema_errors,
        )

        out["global_priors"] = [
            {"symbol": r[0], "horizon_s": r[1], "n": int(r[2]), "mean_z": float(r[3])}
            for r in rows
        ]

        rows = _diagnostic_rows(
            con,
            "spillover_beta",
            """
            SELECT target_symbol, driver_symbol, horizon_s, n, beta
            FROM spillover_beta
            ORDER BY target_symbol, horizon_s, n DESC
            """,
            schema_errors,
        )

        spill = {}
        for tgt, drv, h, n, beta in rows:
            spill.setdefault(f"{tgt}:{h}", []).append({
                "driver": drv,
                "n": int(n),
                "beta": float(beta),
            })
        out["spillovers"] = spill
        out["promotion_hypotheses"] = fetch_recent_hypothesis_registry(limit=50)
        out["promotion_statistical_evidence"] = fetch_recent_promotion_statistical_evidence(limit=50)
        out["promotion_cpcv_runs"] = fetch_recent_backtest_cpcv_runs(limit=20)
        out["drift_retrain_events"] = fetch_recent_drift_retrain_events(limit=20)
        try:
            row = con.execute(
                "SELECT value FROM runtime_meta WHERE key=?",
                ("drift_retrain_status",),
            ).fetchone()
            out["drift_retrain_status"] = _safe_json_obj((row[0] if row else "") or "{}")
        except Exception:
            out["drift_retrain_status"] = {}

        try:
            ensemble_weights_rows = con.execute(
                """
                SELECT created_ts, mode, regime, weights_json,
                       LENGTH(meta_blob), meta_artifact_sha256, meta_artifact_alias
                FROM ensemble_blend_weights
                ORDER BY created_ts DESC, id DESC
                LIMIT 10
                """
            ).fetchall() if _table_exists(con, "ensemble_blend_weights") else []
        except Exception:
            try:
                ensemble_weights_rows = con.execute(
                    """
                    SELECT created_ts, mode, regime, weights_json, LENGTH(meta_blob), '', ''
                    FROM ensemble_blend_weights
                    ORDER BY created_ts DESC, id DESC
                    LIMIT 10
                    """
                ).fetchall() if _table_exists(con, "ensemble_blend_weights") else []
            except Exception:
                ensemble_weights_rows = []
        out["ensemble_current_weights"] = [
            {
                "created_ts": int(row[0] or 0),
                "mode": str(row[1] or ""),
                "regime": (str(row[2]) if row[2] is not None and str(row[2]).strip() else None),
                "weights": _safe_json_obj(row[3]),
                "has_meta_blob": bool(int(row[4] or 0) > 0 or str(row[5] or "").strip()),
                "meta_artifact_sha256": str(row[5] or ""),
                "meta_artifact_alias": str(row[6] or ""),
            }
            for row in ensemble_weights_rows or []
        ]

        ensemble_prediction_rows = _diagnostic_rows(
            con,
            "ensemble_predictions",
            """
            SELECT symbol, ts, blended_prediction, family_preds_json, weights_json, agreement
            FROM ensemble_predictions
            ORDER BY ts DESC, id DESC
            LIMIT 25
            """,
            schema_errors,
        )
        out["ensemble_recent_predictions"] = [
            {
                "symbol": str(row[0] or ""),
                "ts": int(row[1] or 0),
                "blended_prediction": float(row[2] or 0.0),
                "family_preds": _safe_json_obj(row[3]),
                "weights": _safe_json_obj(row[4]),
                "agreement": float(row[5] or 0.0),
            }
            for row in ensemble_prediction_rows or []
        ]
        out["prediction_explanations"] = fetch_prediction_explanations(limit=25)

        ensemble_perf_rows = _diagnostic_rows(
            con,
            "ensemble_family_performance",
            """
            SELECT window_start_ts, window_end_ts, family, n_predictions, realized_sharpe, hit_rate
            FROM ensemble_family_performance
            ORDER BY window_end_ts DESC, id DESC
            LIMIT 50
            """,
            schema_errors,
        )
        out["ensemble_family_performance"] = [
            {
                "window_start_ts": int(row[0] or 0),
                "window_end_ts": int(row[1] or 0),
                "family": str(row[2] or ""),
                "n_predictions": int(row[3] or 0),
                "realized_sharpe": (float(row[4]) if row[4] is not None else None),
                "hit_rate": (float(row[5]) if row[5] is not None else None),
            }
            for row in ensemble_perf_rows or []
        ]
        if schema_errors:
            out["diagnostics_status"] = "schema_error"
            out["schema_errors"] = list(schema_errors)

        return out
    finally:
        con.close()


# --------------------------------------------------
# TEMPORAL MODELS
# --------------------------------------------------

def get_temporal_models(limit: int = 20):
    limit = max(1, min(5000, int(limit or 20)))
    con = db_connect()
    try:
        try:
            rows = con.execute(
                """
                SELECT model_name, window, input_dim, ts_ms, metrics_json,
                       LENGTH(weights) as weights_bytes
                FROM temporal_models
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        except Exception:
            rows = []

        out = []
        for r in rows or []:
            try:
                mj = json.loads(r[4] or "{}")
            except Exception:
                mj = {}

            out.append({
                "model_name": str(r[0] or ""),
                "window": int(r[1] or 0),
                "input_dim": int(r[2] or 0),
                "ts_ms": int(r[3] or 0),
                "weights_bytes": int(r[5] or 0),
                "metrics": mj,
            })

        return {"ok": True, "rows": out}
    finally:
        con.close()


# --------------------------------------------------
# PORTFOLIO BACKTEST READ
# --------------------------------------------------

def get_latest_portfolio_backtest():
    con = db_connect()
    try:
        if (not _table_exists(con, "portfolio_bt_runs")) or (not _table_exists(con, "portfolio_bt_points")):
            return {
                "ok": False,
                "error": "no portfolio backtest runs",
                "run": None,
                "meta": {"ready": False, "count": 0, "status": 200},
            }

        row = con.execute(
            """
            SELECT id, ts_ms, start_ts_ms, end_ts_ms, metrics_json
            FROM portfolio_bt_runs
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        ).fetchone()

        if not row:
            return {
                "ok": False,
                "error": "no portfolio backtest runs",
                "run": None,
                "meta": {"ready": False, "count": 0, "status": 200},
            }

        run_id, ts_ms, start_ts_ms, end_ts_ms, metrics_json = row

        try:
            metrics = json.loads(metrics_json or "{}")
        except Exception:
            metrics = {}

        pts = con.execute(
            """
            SELECT ts_ms, ret, equity, drawdown, detail_json
            FROM portfolio_bt_points
            WHERE run_id = ?
            ORDER BY ts_ms ASC
            """,
            (int(run_id),),
        ).fetchall() or []

        points = []
        for r in pts:
            try:
                detail = json.loads(r[4] or "{}")
            except Exception:
                detail = {}
            points.append({
                "ts_ms": int(r[0] or 0),
                "ret": float(r[1] or 0.0),
                "equity": float(r[2] or 0.0),
                "drawdown": float(r[3] or 0.0),
                "detail": detail,
            })

        return {
            "ok": True,
            "run": {
                "id": int(run_id),
                "ts_ms": int(ts_ms or 0),
                "start_ts_ms": int(start_ts_ms or 0),
                "end_ts_ms": int(end_ts_ms or 0),
                "metrics": metrics,
                "points": points,
            },
            "meta": {
                "ready": True,
                "count": int(len(points)),
            },
        }
    finally:
        con.close()


def get_temporal_shadow_eval(limit: int = 200):
    limit = max(1, min(5000, int(limit or 200)))
    con = db_connect()
    try:
        if not _table_exists(con, "temporal_shadow_eval"):
            return []

        try:
            rows = con.execute(
                """
                SELECT
                  symbol,
                  COALESCE(key_type, 'symbol') AS key_type,
                  COALESCE(key, symbol) AS key,
                  horizon_s,
                  ts_ms,
                  n,
                  rmse,
                  baseline_rmse,
                  directional_acc,
                  baseline_directional_acc,
                  COALESCE(rmse_improvement, 0.0) AS rmse_improvement,
                  COALESCE(diracc_delta, 0.0) AS diracc_delta,
                  COALESCE(capital_efficiency, json_extract(detail_json, '$.capital_efficiency')) AS capital_efficiency,
                  COALESCE(drawdown_contribution, json_extract(detail_json, '$.drawdown_contribution')) AS drawdown_contribution,
                  COALESCE(avg_slippage_impact, json_extract(detail_json, '$.avg_slippage_impact')) AS avg_slippage_impact,
                  pass_all,
                  detail_json
                FROM temporal_shadow_eval
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall() or []
        except Exception as e:
            _warn_nonfatal("API_READ_ADVANCED_TEMPORAL_SHADOW_EVAL_QUERY_FAILED", e, once_key="temporal_shadow_eval_query_failed")
            return []

        out = []
        for row in rows:
            try:
                out.append(
                    {
                        "symbol": str(row[0] or ""),
                        "key_type": str(row[1] or "symbol"),
                        "key": str(row[2] or ""),
                        "horizon_s": int(row[3] or 0),
                        "ts_ms": int(row[4] or 0),
                        "n": int(row[5] or 0),
                        "rmse": float(row[6] or 0.0),
                        "baseline_rmse": float(row[7] or 0.0),
                        "directional_acc": float(row[8] or 0.0),
                        "baseline_directional_acc": float(row[9] or 0.0),
                        "rmse_improvement": float(row[10] or 0.0),
                        "diracc_delta": float(row[11] or 0.0),
                        "capital_efficiency": float(row[12] or 0.0),
                        "drawdown_contribution": float(row[13] or 0.0),
                        "avg_slippage_impact": float(row[14] or 0.0),
                        "pass_all": bool(int(row[15] or 0)),
                        "detail": _safe_json_obj(row[16]),
                    }
                )
            except Exception as e:
                _warn_nonfatal(
                    "API_READ_ADVANCED_TEMPORAL_SHADOW_EVAL_ROW_FAILED",
                    e,
                    once_key="temporal_shadow_eval_row_failed",
                )
                continue
        return out
    finally:
        con.close()

# --------------------------------------------------
# PORTFOLIO SNAPSHOT (dashboard)
# --------------------------------------------------

def get_portfolio_snapshot(
    limit_state: int = 200,
    intents_window_ms: int = 2500,
    intents_max_rows: int = 5000,
    model_id: str = "",
):
    model_filter = str(model_id or "").strip()
    cache_key = f"{int(limit_state)}:{int(intents_window_ms)}:{int(intents_max_rows)}:{model_filter}"

    def _load():
        con = db_connect()
        try:
            # This endpoint merges current portfolio state with recent intent
            # history so the dashboard can explain both "what we hold" and
            # "what we just tried to do."
            has_state = _table_exists(con, "portfolio_state")
            has_orders_table = _table_exists(con, "portfolio_orders")

            if (not has_state) and (not has_orders_table):
                return {
                    "ok": True,
                    "meta": {"ready": False, "reason": "portfolio_tables_missing"},
                    "state": [],
                    "orders": [],
                }

            # State
            if has_state:
                try:
                    if model_filter:
                        st_rows = con.execute(
                            """
                            SELECT model_id, symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json
                            FROM portfolio_state
                            WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                            ORDER BY ABS(weight) DESC, updated_ts_ms DESC
                            LIMIT ?
                            """,
                            (str(model_filter), int(limit_state)),
                        ).fetchall() or []
                    else:
                        st_rows = con.execute(
                            """
                            SELECT model_id, symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json
                            FROM portfolio_state
                            ORDER BY ABS(weight) DESC, updated_ts_ms DESC
                            LIMIT ?
                            """,
                            (int(limit_state),),
                        ).fetchall() or []
                except Exception:
                    st_rows = []
            else:
                st_rows = []

            state_symbols = []
            for r in st_rows:
                try:
                    symbol = str(r[1] or "").strip().upper()
                except Exception:
                    symbol = ""
                if symbol and symbol not in state_symbols:
                    state_symbols.append(symbol)

            broker_positions_by_symbol = {}
            if state_symbols and _table_exists(con, "broker_positions"):
                try:
                    cols = {
                        str(row[1])
                        for row in (con.execute("PRAGMA table_info(broker_positions)").fetchall() or [])
                        if row and len(row) > 1 and row[1]
                    }
                    ts_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else None)
                    select_cols = [
                        "symbol",
                        ("qty" if "qty" in cols else "NULL") + " AS qty",
                        ("side" if "side" in cols else "NULL") + " AS side",
                        ("unrealized_pnl" if "unrealized_pnl" in cols else "NULL") + " AS unrealized_pnl",
                        ("realized_pnl" if "realized_pnl" in cols else "NULL") + " AS realized_pnl",
                        (f"{ts_col}" if ts_col else "NULL") + " AS snapshot_ts_ms",
                    ]
                    placeholders = ",".join("?" for _ in state_symbols)
                    rows = con.execute(
                        f"""
                        SELECT {", ".join(select_cols)}
                        FROM broker_positions
                        WHERE UPPER(TRIM(symbol)) IN ({placeholders})
                        ORDER BY COALESCE({ts_col}, 0) DESC, symbol ASC
                        """ if ts_col else f"""
                        SELECT {", ".join(select_cols)}
                        FROM broker_positions
                        WHERE UPPER(TRIM(symbol)) IN ({placeholders})
                        ORDER BY symbol ASC
                        """,
                        tuple(state_symbols),
                    ).fetchall() or []
                    for row in rows:
                        try:
                            symbol, qty, live_side, unrealized_pnl, realized_pnl, snapshot_ts_ms = row
                        except Exception as e:
                            _warn_nonfatal(
                                "API_READ_ADVANCED_BROKER_POSITION_ROW_PARSE_FAILED",
                                e,
                                once_key="api_read_advanced_broker_position_row_parse_failed",
                                row_preview=str(row)[:160],
                            )
                            continue
                        symbol_key = str(symbol or "").strip().upper()
                        if not symbol_key or symbol_key in broker_positions_by_symbol:
                            continue
                        broker_positions_by_symbol[symbol_key] = {
                            "qty": _float_or_none(qty),
                            "side": str(live_side or "").strip(),
                            "unrealized_pnl": _float_or_none(unrealized_pnl),
                            "realized_pnl": _float_or_none(realized_pnl),
                            "snapshot_ts_ms": int(snapshot_ts_ms or 0) if snapshot_ts_ms is not None else None,
                        }
                except Exception as e:
                    _warn_nonfatal(
                        "API_READ_ADVANCED_PORTFOLIO_BROKER_ENRICH_FAILED",
                        e,
                        once_key="portfolio_broker_enrich_failed",
                    )
                    broker_positions_by_symbol = {}

            state = []
            for r in st_rows:
                try:
                    model_id, symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json = r
                except Exception as e:
                    _warn_nonfatal("API_READ_ADVANCED_PORTFOLIO_STATE_ROW_FAILED", e, once_key="portfolio_state_row_failed")
                    continue
                try:
                    ex = json.loads(explain_json or "{}") if explain_json else {}
                except Exception:
                    ex = {}
                broker_live = broker_positions_by_symbol.get(str(symbol or "").strip().upper(), {})
                state.append(
                    {
                        "symbol": str(symbol or ""),
                        "model_id": str(model_id or "baseline"),
                        "side": str(side or ""),
                        "size": broker_live.get("qty"),
                        "weight": float(weight or 0.0),
                        "unrealized_pnl": broker_live.get("unrealized_pnl"),
                        "realized_pnl": broker_live.get("realized_pnl"),
                        "opened_ts_ms": int(opened_ts_ms or 0),
                        "updated_ts_ms": int(updated_ts_ms or 0),
                        "source_alert_id": (int(source_alert_id) if source_alert_id is not None else None),
                        "explain": ex if isinstance(ex, dict) else {},
                    }
                )

            # Orders
            intents_res = {"ok": True, "batch_id": None, "batch_ts_ms": None, "intents": []}
            orders = []

            try:
                from engine.strategy.portfolio_execution_intents import load_latest_execution_intents
                intents_res = load_latest_execution_intents(
                    con,
                    window_ms=int(intents_window_ms),
                    max_rows=int(intents_max_rows),
                )
                if isinstance(intents_res, dict):
                    orders = intents_res.get("intents") or []
                    if model_filter:
                        orders = [
                            o for o in list(orders or [])
                            if str((o or {}).get("model_id") or "baseline").strip() == str(model_filter)
                        ]
            except Exception:
                intents_res = {"ok": True, "batch_id": None, "batch_ts_ms": None, "intents": []}
                orders = []

            if (not orders) and has_orders_table:
                try:
                    if model_filter:
                        ord_rows = con.execute(
                            """
                            SELECT ts_ms, model_id, symbol, action, from_side, to_side,
                                   from_weight, to_weight, delta_weight, source_alert_id, explain_json
                            FROM portfolio_orders
                            WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                            ORDER BY ts_ms DESC
                            LIMIT ?
                            """,
                            (str(model_filter), int(max(1, min(int(intents_max_rows), 5000)))),
                        ).fetchall() or []
                    else:
                        ord_rows = con.execute(
                            """
                            SELECT ts_ms, model_id, symbol, action, from_side, to_side,
                                   from_weight, to_weight, delta_weight, source_alert_id, explain_json
                            FROM portfolio_orders
                            ORDER BY ts_ms DESC
                            LIMIT ?
                            """,
                            (int(max(1, min(int(intents_max_rows), 5000))),),
                        ).fetchall() or []
                except Exception:
                    ord_rows = []

                for r in ord_rows:
                    try:
                        ts_ms, model_id, symbol, action, from_side, to_side, from_weight, to_weight, delta_weight, source_alert_id, explain_json = r
                    except Exception as e:
                        _warn_nonfatal("API_READ_ADVANCED_PORTFOLIO_ORDER_ROW_FAILED", e, once_key="portfolio_order_row_failed")
                        continue
                    try:
                        ex = json.loads(explain_json or "{}") if explain_json else {}
                    except Exception:
                        ex = {}
                    orders.append(
                        {
                        "ts_ms": int(ts_ms or 0),
                        "model_id": str(model_id or "baseline"),
                        "symbol": str(symbol or ""),
                            "action": str(action or ""),
                            "from_side": str(from_side or ""),
                            "to_side": str(to_side or ""),
                            "from_weight": float(from_weight or 0.0),
                            "to_weight": float(to_weight or 0.0),
                            "delta_weight": float(delta_weight or 0.0),
                            "source_alert_id": (int(source_alert_id) if source_alert_id is not None else None),
                            "explain": ex if isinstance(ex, dict) else {},
                        }
                    )

            return {
                "ok": True,
                "meta": {
                    "ready": bool(has_state or has_orders_table),
                    "model_id": str(model_filter),
                    "has_state": bool(has_state),
                    "has_orders_table": bool(has_orders_table),
                    "orders_batch_id": intents_res.get("batch_id") if isinstance(intents_res, dict) else None,
                    "orders_batch_ts_ms": intents_res.get("batch_ts_ms") if isinstance(intents_res, dict) else None,
                },
                "state": state,
                "orders": orders or [],
                "execution_book": (_shadow_book_snapshot(con, model_filter) if model_filter else {}),
            }
        finally:
            con.close()

    return cache_get_or_load("portfolio_snapshot", cache_key, _load, ttl_s=0.75)


# --------------------------------------------------
# EXECUTION METRICS
# --------------------------------------------------

def get_execution_metrics_rolling(model_id: str = ""):
    model_filter = str(model_id or "").strip()
    con = db_connect()
    try:
        now_ms = int(time.time() * 1000)
        day_ms = 24 * 60 * 60 * 1000
        week_ms = 7 * day_ms

        if _table_exists(con, "execution_fills"):
            def _q(since_ms):
                try:
                    if model_filter:
                        return con.execute(
                            """
                            SELECT
                              COUNT(*)                           AS n_fills,
                              SUM(COALESCE(slippage_bps, 0.0))  AS total_slippage_bps,
                              SUM(COALESCE(fees, 0.0))          AS total_fees,
                              AVG(slippage_bps)                 AS avg_slippage_bps,
                              AVG(fill_latency_ms)              AS avg_time_to_fill_ms,
                              AVG(spread_bps)                   AS avg_spread_at_entry_bps,
                              AVG(expected_px)                  AS avg_expected_fill_price,
                              AVG(fill_px)                      AS avg_actual_fill_price
                            FROM execution_fills
                            WHERE fill_ts_ms >= ?
                              AND COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                            """,
                            (int(since_ms), str(model_filter)),
                        ).fetchone()
                    return con.execute(
                        """
                        SELECT
                          COUNT(*)                           AS n_fills,
                          SUM(COALESCE(slippage_bps, 0.0))  AS total_slippage_bps,
                          SUM(COALESCE(fees, 0.0))          AS total_fees,
                          AVG(slippage_bps)                 AS avg_slippage_bps,
                          AVG(fill_latency_ms)              AS avg_time_to_fill_ms,
                          AVG(spread_bps)                   AS avg_spread_at_entry_bps,
                          AVG(expected_px)                  AS avg_expected_fill_price,
                          AVG(fill_px)                      AS avg_actual_fill_price
                        FROM execution_fills
                        WHERE fill_ts_ms >= ?
                        """,
                        (int(since_ms),),
                    ).fetchone()
                except Exception as e:
                    _warn_nonfatal(
                        "API_READ_ADVANCED_EXECUTION_METRICS_QUERY_FAILED",
                        e,
                        once_key=f"execution_metrics_query:{model_filter or 'all'}",
                        model_id=str(model_filter or ""),
                        since_ms=int(since_ms),
                    )
                    row = None
                    return row

            r_24h = _q(now_ms - day_ms)
            r_7d = _q(now_ms - week_ms)

            def _row(r):
                return {
                    "n_fills": int(r[0] or 0) if r else 0,
                    "total_slippage_bps": float(r[1] or 0.0) if r else 0.0,
                    "total_fees": float(r[2] or 0.0) if r else 0.0,
                    "avg_slippage_bps": float(r[3] or 0.0) if r else 0.0,
                    "avg_time_to_fill_ms": float(r[4] or 0.0) if r else 0.0,
                    "avg_spread_at_entry_bps": float(r[5] or 0.0) if r else 0.0,
                    "avg_expected_fill_price": float(r[6] or 0.0) if r else 0.0,
                    "avg_actual_fill_price": float(r[7] or 0.0) if r else 0.0,
                    "total_slippage": float(r[1] or 0.0) if r else 0.0,
                    "total_cost": float(r[2] or 0.0) if r else 0.0,
                    "avg_slippage": float(r[3] or 0.0) if r else 0.0,
                }

            return {"ok": True, "model_id": (str(model_filter) if model_filter else None), "last_24h": _row(r_24h), "last_7d": _row(r_7d)}

        fills_table = _broker_fills_table(con)
        fills_sql = sql_identifier(fills_table)

        def _q(since_ms):
            try:
                return con.execute(
                    f"""
                    SELECT
                      COUNT(*)        AS n_fills,
                      SUM(slippage)   AS total_slippage,
                      SUM(fees)       AS total_fees,
                      SUM(total_cost) AS total_cost,
                      AVG(slippage)   AS avg_slippage
                    FROM {fills_sql}
                    WHERE ts_ms >= ?
                    """,
                    (int(since_ms),),
                ).fetchone()
            except Exception as e:
                _warn_nonfatal(
                    "API_READ_ADVANCED_EXECUTION_METRICS_LEGACY_QUERY_FAILED",
                    e,
                    once_key=f"execution_metrics_legacy_query:{fills_table}",
                    table=str(fills_table),
                    since_ms=int(since_ms),
                )
                row = None
                return row

        r_24h = _q(now_ms - day_ms)
        r_7d  = _q(now_ms - week_ms)

        def _row(r):
            return {
                "n_fills": int(r[0] or 0) if r else 0,
                "total_slippage": float(r[1] or 0.0) if r else 0.0,
                "total_fees": float(r[2] or 0.0) if r else 0.0,
                "total_cost": float(r[3] or 0.0) if r else 0.0,
                "avg_slippage": float(r[4] or 0.0) if r else 0.0,
            }

        return {"ok": True, "model_id": (str(model_filter) if model_filter else None), "last_24h": _row(r_24h), "last_7d": _row(r_7d)}
    finally:
        con.close()


def get_execution_metrics_by_symbol(limit: int = 50, model_id: str = ""):
    limit = max(1, min(500, int(limit or 50)))
    model_filter = str(model_id or "").strip()
    con = db_connect()
    try:
        if _table_exists(con, "execution_fills"):
            try:
                if model_filter:
                    rows = con.execute(
                            """
                            SELECT
                              symbol,
                              COUNT(*)                           AS n_fills,
                              SUM(COALESCE(slippage_bps, 0.0))  AS total_slippage_bps,
                              SUM(COALESCE(fees, 0.0))          AS total_fees,
                              AVG(slippage_bps)                 AS avg_slippage_bps,
                              AVG(fill_latency_ms)              AS avg_time_to_fill_ms,
                              AVG(spread_bps)                   AS avg_spread_at_entry_bps,
                              AVG(expected_px)                  AS avg_expected_fill_price,
                              AVG(fill_px)                      AS avg_actual_fill_price
                            FROM execution_fills
                            WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                            GROUP BY symbol
                            ORDER BY total_fees DESC, total_slippage_bps DESC, symbol ASC
                            LIMIT ?
                            """,
                            (str(model_filter), limit),
                        ).fetchall()
                else:
                    rows = con.execute(
                            """
                            SELECT
                              symbol,
                              COUNT(*)                           AS n_fills,
                              SUM(COALESCE(slippage_bps, 0.0))  AS total_slippage_bps,
                              SUM(COALESCE(fees, 0.0))          AS total_fees,
                              AVG(slippage_bps)                 AS avg_slippage_bps,
                              AVG(fill_latency_ms)              AS avg_time_to_fill_ms,
                              AVG(spread_bps)                   AS avg_spread_at_entry_bps,
                              AVG(expected_px)                  AS avg_expected_fill_price,
                              AVG(fill_px)                      AS avg_actual_fill_price
                            FROM execution_fills
                            GROUP BY symbol
                            ORDER BY total_fees DESC, total_slippage_bps DESC, symbol ASC
                            LIMIT ?
                            """,
                            (limit,),
                        ).fetchall()
            except Exception:
                rows = []

            return {
                "ok": True,
                "model_id": (str(model_filter) if model_filter else None),
                "symbols": [
                    {
                        "symbol": r[0],
                        "n_fills": int(r[1] or 0),
                        "total_slippage_bps": float(r[2] or 0.0),
                        "total_fees": float(r[3] or 0.0),
                        "avg_slippage_bps": float(r[4] or 0.0),
                        "avg_time_to_fill_ms": float(r[5] or 0.0),
                        "avg_spread_at_entry_bps": float(r[6] or 0.0),
                        "avg_expected_fill_price": float(r[7] or 0.0),
                        "avg_actual_fill_price": float(r[8] or 0.0),
                        "total_slippage": float(r[2] or 0.0),
                        "total_cost": float(r[3] or 0.0),
                        "avg_slippage": float(r[4] or 0.0),
                    }
                    for r in rows
                ],
            }

        fills_table = _broker_fills_table(con)
        fills_sql = sql_identifier(fills_table)

        try:
            rows = con.execute(
                f"""
                SELECT
                  symbol,
                  COUNT(*)        AS n_fills,
                  SUM(slippage)   AS total_slippage,
                  SUM(fees)       AS total_fees,
                  SUM(total_cost) AS total_cost,
                  AVG(slippage)   AS avg_slippage
                FROM {fills_sql}
                GROUP BY symbol
                ORDER BY total_cost DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        except Exception:
            rows = []

        return {
            "ok": True,
            "symbols": [
                {
                    "symbol": r[0],
                    "n_fills": int(r[1] or 0),
                    "total_slippage": float(r[2] or 0.0),
                    "total_fees": float(r[3] or 0.0),
                    "total_cost": float(r[4] or 0.0),
                    "avg_slippage": float(r[5] or 0.0),
                }
                for r in rows
            ],
        }
    finally:
        con.close()


def get_execution_cost_by_confidence(model_id: str = ""):
    model_filter = str(model_id or "").strip()
    con = db_connect()
    try:
        if _table_exists(con, "execution_fills"):
            try:
                if model_filter:
                    rows = con.execute(
                        """
                        SELECT
                          CAST(COALESCE(json_extract(extra_json, '$.confidence'), json_extract(extra_json, '$.signal.confidence')) * 10 AS INTEGER) AS bucket,
                          COUNT(*) AS n_fills,
                          SUM(COALESCE(fees, 0.0)) AS total_cost,
                          AVG(COALESCE(fees, 0.0)) AS avg_cost
                        FROM execution_fills
                        WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                          AND COALESCE(json_extract(extra_json, '$.confidence'), json_extract(extra_json, '$.signal.confidence')) IS NOT NULL
                        GROUP BY bucket
                        ORDER BY bucket ASC
                        """,
                        (str(model_filter),),
                    ).fetchall()
                else:
                    rows = con.execute(
                        """
                        SELECT
                          CAST(COALESCE(json_extract(extra_json, '$.confidence'), json_extract(extra_json, '$.signal.confidence')) * 10 AS INTEGER) AS bucket,
                          COUNT(*) AS n_fills,
                          SUM(COALESCE(fees, 0.0)) AS total_cost,
                          AVG(COALESCE(fees, 0.0)) AS avg_cost
                        FROM execution_fills
                        WHERE COALESCE(json_extract(extra_json, '$.confidence'), json_extract(extra_json, '$.signal.confidence')) IS NOT NULL
                        GROUP BY bucket
                        ORDER BY bucket ASC
                        """
                    ).fetchall()
            except Exception:
                rows = []

            buckets = []
            for b, n, tc, ac in rows:
                lo = max(0.0, min(0.9, (int(b) or 0) / 10.0))
                hi = lo + 0.1
                buckets.append({
                    "conf_lo": lo,
                    "conf_hi": hi,
                    "n_fills": int(n or 0),
                    "total_cost": float(tc or 0.0),
                    "avg_cost": float(ac or 0.0),
                })
            return {"ok": True, "model_id": (str(model_filter) if model_filter else None), "buckets": buckets}

        fills_table = _broker_fills_table(con)
        fills_sql = sql_identifier(fills_table)

        try:
            rows = con.execute(
                f"""
                SELECT
                  CAST(confidence * 10 AS INTEGER) AS bucket,
                  COUNT(*)        AS n_fills,
                  SUM(total_cost) AS total_cost,
                  AVG(total_cost) AS avg_cost
                FROM {fills_sql}
                WHERE confidence IS NOT NULL
                GROUP BY bucket
                ORDER BY bucket ASC
                """
            ).fetchall()
        except Exception:
            rows = []

        buckets = []
        for b, n, tc, ac in rows:
            lo = max(0.0, min(0.9, (int(b) or 0) / 10.0))
            hi = lo + 0.1
            buckets.append({
                "conf_lo": lo,
                "conf_hi": hi,
                "n_fills": int(n or 0),
                "total_cost": float(tc or 0.0),
                "avg_cost": float(ac or 0.0),
            })

        return {"ok": True, "model_id": (str(model_filter) if model_filter else None), "buckets": buckets}
    finally:
        con.close()


# --------------------------------------------------
# SOCIAL READS
# --------------------------------------------------

def get_social_features(symbol: str, limit: int = 200):
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {"ok": True, "rows": []}

    limit = max(1, min(5000, int(limit or 200)))

    con = db_connect()
    try:
        if not _table_exists(con, "social_features"):
            return {"ok": True, "rows": []}

        try:
            rows = con.execute(
                """
                SELECT
                  bucket_ts_ms,
                  bucket_sec,

                  mention_count,
                  unique_authors,
                  new_author_ratio,
                  engagement_now,

                  sentiment_mean,
                  sentiment_dispersion,

                  mention_rate_z,
                  bot_likelihood_mean,
                  promo_likelihood_mean,
                  manip_risk,
                  attention_shock,

                  cross_platform_confirm
                FROM social_features
                WHERE symbol = ?
                ORDER BY bucket_ts_ms DESC
                LIMIT ?
                """,
                (sym, int(limit)),
            ).fetchall()
        except Exception:
            rows = []

        out = []
        for r in rows or []:
            try:
                out.append({
                    "bucket_ts_ms": int(r[0] or 0),
                    "bucket_sec": int(r[1] or 0),

                    "mention_count": int(r[2] or 0),
                    "unique_authors": int(r[3] or 0),
                    "new_author_ratio": float(r[4] or 0.0),
                    "engagement_now": float(r[5] or 0.0),

                    "sentiment_mean": float(r[6] or 0.0),
                    "sentiment_dispersion": float(r[7] or 0.0),

                    "mention_rate_z": float(r[8] or 0.0),
                    "bot_likelihood_mean": float(r[9] or 0.0),
                    "promo_likelihood_mean": float(r[10] or 0.0),
                    "manip_risk": float(r[11] or 0.0),
                    "attention_shock": float(r[12] or 0.0),

                    "cross_platform_confirm": float(r[13] or 0.0),
                })
            except Exception as e:
                _warn_nonfatal("API_READ_ADVANCED_EXECUTION_COST_BUCKET_ROW_FAILED", e, once_key="execution_cost_bucket_row_failed")
                continue

        return {"ok": True, "symbol": sym, "rows": out}
    finally:
        con.close()


def get_social_regimes(symbol: str, limit: int = 200):
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {"ok": True, "rows": []}

    limit = max(1, min(5000, int(limit or 200)))

    con = db_connect()
    try:
        if not _table_exists(con, "social_regimes"):
            return {"ok": True, "rows": []}

        try:
            rows = con.execute(
                """
                SELECT
                  bucket_ts_ms,
                  bucket_sec,
                  regime,
                  regime_conf,
                  features_json
                FROM social_regimes
                WHERE symbol = ?
                ORDER BY bucket_ts_ms DESC
                LIMIT ?
                """,
                (sym, int(limit)),
            ).fetchall()
        except Exception:
            rows = []

        out = []
        for r in rows or []:
            try:
                out.append({
                    "bucket_ts_ms": int(r[0] or 0),
                    "bucket_sec": int(r[1] or 0),
                    "regime": str(r[2] or ""),
                    "regime_conf": float(r[3] or 0.0),
                    "features": (json.loads(r[4]) if (r[4] or "").strip() else None),
                })
            except Exception as e:
                _warn_nonfatal("API_READ_ADVANCED_SOCIAL_FEATURE_ROW_FAILED", e, once_key="social_feature_row_failed")
                continue

        return {"ok": True, "symbol": sym, "rows": out}
    finally:
        con.close()


def get_social_blocks(limit: int = 200):
    limit = max(1, min(2000, int(limit or 200)))

    con = db_connect()
    try:
        table = None
        for t in ("decision_log", "decisions", "trade_decisions"):
            if _table_exists(con, t):
                table = require_allowed_table_name(t)
                break

        if not table:
            return {"ok": True, "rows": []}

        rows = []
        try:
            table_sql = sql_identifier(table)
            rows = con.execute(
                f"""
                SELECT ts_ms, symbol, reason_json
                FROM {table_sql}
                WHERE json_extract(reason_json, '$.social_gate_block') = 1
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        except Exception:
            try:
                rows = con.execute(
                    f"""
                    SELECT ts_ms, symbol, reason_json
                    FROM {table_sql}
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()
            except Exception:
                rows = []

        out = []
        for r in rows or []:
            try:
                out.append({
                    "ts_ms": int(r[0] or 0),
                    "symbol": str(r[1] or ""),
                    "reason": (json.loads(r[2]) if (r[2] or "").strip() else {}),
                })
            except Exception as e:
                _warn_nonfatal("API_READ_ADVANCED_SHADOW_EVAL_ROW_FAILED", e, once_key="shadow_eval_row_failed")
                continue

        return {"ok": True, "table": table, "rows": out}
    finally:
        con.close()

# ----------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------

def get_validation_rows():
    try:
        from engine.strategy.validation import get_validation_scores as _get_validation_scores
        return {"ok": True, "rows": _get_validation_scores()}
    except Exception as e:
        _warn_nonfatal("API_READ_ADVANCED_SOCIAL_FEATURES_FAILED", e)
        error_payload = {"ok": False, "error": str(e)}
        return error_payload


# ----------------------------------------------------------------------
# Shadow Capital Allocation
# ----------------------------------------------------------------------

def get_shadow_capital_scores(limit: int = 50, regime: str = "global"):
    try:
        from engine.runtime.shadow_capital_allocator import (
            get_shadow_capital_scores as _impl,
        )
        return _impl(limit=limit, regime=regime)
    except Exception as e:
        _warn_nonfatal("API_READ_ADVANCED_PORTFOLIO_SNAPSHOT_FAILED", e)
        error_payload = {"ok": False, "error": str(e)}
        return error_payload


def run_shadow_capital_scores(window_s: int = 86400, regime: str = "global"):
    try:
        from engine.runtime.shadow_capital_allocator import (
            compute_and_persist_shadow_capital_scores as _run,
        )
        return _run(window_s=window_s, regime=regime)
    except Exception as e:
        _warn_nonfatal("API_READ_ADVANCED_EXECUTION_METRICS_FAILED", e)
        error_payload = {"ok": False, "error": str(e)}
        return error_payload

# ----------------------------------------------------------------------
# Size Policy
# ----------------------------------------------------------------------

def get_size_policy():
    from engine.api.internal_access import db_connect as _db_connect
    from engine.strategy.size_policy import load_latest_size_policy

    con = _db_connect()
    try:
        policy = load_latest_size_policy(con)
        if not policy:
            return {"ok": True, "policy": None, "points": []}

        return {
            "ok": True,
            "policy": {
                "id": int(policy["policy_id"]),
                "ts_ms": int(policy["ts_ms"]),
                "lookback_days": int(policy.get("lookback_days") or 0),
                "buckets": int(policy.get("buckets") or 0),
                "method": str(policy["method"]),
                "params": dict(policy.get("params") or {}),
                "metrics": dict(policy.get("metrics") or {}),
            },
            "points": list(policy.get("points") or []),
        }
    finally:
        con.close()


# ----------------------------------------------------------------------
# Decisions UI
# ----------------------------------------------------------------------

_DECISION_DETAIL_TABLES = {
    "alerts",
    "decision_log",
    "portfolio_orders",
    "execution_policy_audit",
    "execution_orders",
    "execution_fills",
    "trade_attribution_ledger",
}


def _decision_quote_ident(name: str) -> str:
    name = str(name or "").strip()
    if name not in _DECISION_DETAIL_TABLES:
        raise ValueError(f"unsupported_decision_detail_identifier:{name}")
    return '"' + name.replace('"', '""') + '"'


def _decision_table_columns(con, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({_decision_quote_ident(table)})").fetchall() or []
        return {str(row[1] or "").strip() for row in rows if row and len(row) > 1}
    except Exception as e:
        _warn_nonfatal(
            "API_READ_ADVANCED_DECISION_DETAIL_COLUMNS_FAILED",
            e,
            once_key=f"decision_detail_columns:{table}",
            table=str(table),
        )
        return set()


def _decision_row_to_dict(cursor, row) -> dict[str, Any]:
    if not row:
        return {}
    columns = [str(item[0]) for item in (getattr(cursor, "description", None) or [])]
    out: dict[str, Any] = {}
    if hasattr(row, "keys"):
        try:
            out = {str(key): row[key] for key in row.keys()}
        except Exception:
            out = {}
    if not out:
        try:
            out = {
                str(columns[idx]): row[idx]
                for idx in range(min(len(columns), len(row)))
            }
        except Exception:
            out = {}
    return _decision_decode_json_fields(out)


def _decision_decode_json_fields(row: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in dict(row or {}).items():
        if isinstance(value, str) and (
            key.endswith("_json")
            or key in {
                "payload",
                "payload_excerpt",
                "detail_json",
                "reason_json",
                "decision_json",
                "signal_json",
                "model_json",
                "raw_json",
                "meta_json",
            }
        ):
            try:
                out[str(key)] = json.loads(value) if value.strip() else {}
                continue
            except Exception as e:
                _warn_nonfatal(
                    "API_READ_ADVANCED_DECISION_JSON_DECODE_FAILED",
                    e,
                    once_key=f"decision_json_decode:{key}",
                    field=str(key),
                    value_excerpt=str(value)[:256],
                )
        out[str(key)] = value
    return out


def _decision_select_one(
    con,
    table: str,
    where_sql: str,
    params: tuple[Any, ...],
    *,
    order_sql: str = "",
) -> dict[str, Any] | None:
    if not _table_exists(con, table):
        return None
    try:
        cur = con.execute(
            f"SELECT * FROM {_decision_quote_ident(table)} WHERE {where_sql} {order_sql} LIMIT 1",
            tuple(params),
        )
        row = cur.fetchone()
        return _decision_row_to_dict(cur, row) if row else None
    except Exception as e:
        _warn_nonfatal(
            "API_READ_ADVANCED_DECISION_DETAIL_SELECT_ONE_FAILED",
            e,
            once_key=f"decision_detail_select_one:{table}:{where_sql}",
            table=str(table),
        )
        return None


def _decision_select_many(
    con,
    table: str,
    where_sql: str,
    params: tuple[Any, ...],
    *,
    order_sql: str = "ORDER BY ts_ms DESC",
    limit: int = 8,
) -> list[dict[str, Any]]:
    if not _table_exists(con, table):
        return []
    try:
        cur = con.execute(
            f"SELECT * FROM {_decision_quote_ident(table)} WHERE {where_sql} {order_sql} LIMIT ?",
            tuple(params) + (max(1, min(50, int(limit or 8))),),
        )
        rows = cur.fetchall() or []
        return [_decision_row_to_dict(cur, row) for row in rows]
    except Exception as e:
        _warn_nonfatal(
            "API_READ_ADVANCED_DECISION_DETAIL_SELECT_MANY_FAILED",
            e,
            once_key=f"decision_detail_select_many:{table}:{where_sql}",
            table=str(table),
        )
        return []


def _decision_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        out = int(value)
        return out if out > 0 else None
    except Exception as e:
        _warn_nonfatal(
            "API_READ_ADVANCED_DECISION_INT_PARSE_FAILED",
            e,
            once_key="decision_int",
            value_type=type(value).__name__,
        )
        return None


def _decision_first(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _decision_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception as e:
            _warn_nonfatal(
                "API_READ_ADVANCED_DECISION_DICT_PARSE_FAILED",
                e,
                once_key="decision_dict",
                value_type=type(value).__name__,
            )
            return {}
    return {}


def _normalize_decision_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not record:
        return None
    out = _decision_decode_json_fields(dict(record))
    decision_id = _decision_first(out.get("decision_id"), out.get("id"))
    if decision_id is not None:
        out["decision_id"] = decision_id
    explain = _decision_dict(_decision_first(out.get("explain"), out.get("explain_json")))
    extra = _decision_dict(_decision_first(out.get("extra"), out.get("extra_json")))
    features = _decision_first(out.get("features_json"), out.get("features"))
    components = _decision_first(out.get("components_json"), out.get("component_vector"))
    if explain:
        out["explain"] = explain
    if extra:
        out["extra"] = extra
    if features is not None:
        out["features"] = features
    if components is not None:
        out["components"] = components
    out.setdefault("source_ts_ms", out.get("ts_ms"))
    out.setdefault("source_timestamp_ms", out.get("ts_ms"))
    if out.get("model_version") in (None, ""):
        model_version = _decision_first(
            extra.get("model_version"),
            explain.get("model_version"),
            _decision_dict(explain.get("model")).get("version"),
        )
        if model_version not in (None, ""):
            out["model_version"] = model_version
    return out


def _format_decision_confidence(value: Any) -> str:
    try:
        n = float(value)
    except Exception as e:
        _warn_nonfatal(
            "API_READ_ADVANCED_DECISION_CONFIDENCE_FORMAT_FAILED",
            e,
            once_key="decision_confidence",
            value_type=type(value).__name__,
        )
        return "unavailable"
    if not (n == n):
        return "unavailable"
    if 0 <= n <= 1:
        return f"{round(n * 100)}%"
    return f"{n:.4g}"


def _stage(
    key: str,
    label: str,
    *,
    status: str,
    summary: str,
    ts_ms: Any = None,
    data: Any = None,
    unavailable_reason: str = "",
) -> dict[str, Any]:
    return {
        "key": str(key),
        "label": str(label),
        "status": str(status or "unavailable"),
        "summary": str(summary or ""),
        "ts_ms": _decision_int(ts_ms),
        "data": data if data is not None else {},
        "unavailable_reason": str(unavailable_reason or ""),
    }


def _lookup_alert_for_decision(con, decision: dict[str, Any] | None, source_alert_id: int | None) -> dict[str, Any] | None:
    if source_alert_id:
        return _decision_select_one(con, "alerts", "id=?", (int(source_alert_id),))
    if not decision:
        return None
    event_id = _decision_int(decision.get("event_id"))
    symbol = str(decision.get("symbol") or "").strip()
    horizon_s = _decision_int(decision.get("horizon_s"))
    if event_id is not None and symbol and horizon_s is not None:
        return _decision_select_one(
            con,
            "alerts",
            "event_id=? AND symbol=? AND horizon_s=?",
            (int(event_id), str(symbol), int(horizon_s)),
            order_sql="ORDER BY ts_ms DESC",
        )
    if event_id is not None:
        return _decision_select_one(
            con,
            "alerts",
            "event_id=?",
            (int(event_id),),
            order_sql="ORDER BY ts_ms DESC",
        )
    return None


def _lookup_decision_for_lineage(
    con,
    *,
    source_alert_id: int | None,
    portfolio_order: dict[str, Any] | None,
    alert: dict[str, Any] | None,
) -> dict[str, Any] | None:
    source = alert or {}
    if not source and source_alert_id:
        source = _decision_select_one(con, "alerts", "id=?", (int(source_alert_id),)) or {}
    event_id = _decision_int(source.get("event_id"))
    symbol = str(_decision_first(source.get("symbol"), (portfolio_order or {}).get("symbol")) or "").strip()
    horizon_s = _decision_int(source.get("horizon_s"))
    if event_id is not None and symbol and horizon_s is not None:
        return _decision_select_one(
            con,
            "decision_log",
            "event_id=? AND symbol=? AND horizon_s=?",
            (int(event_id), str(symbol), int(horizon_s)),
            order_sql="ORDER BY ts_ms DESC",
        )
    if event_id is not None:
        return _decision_select_one(
            con,
            "decision_log",
            "event_id=?",
            (int(event_id),),
            order_sql="ORDER BY ts_ms DESC",
        )
    return None


def _decision_related_rows(
    con,
    *,
    decision: dict[str, Any] | None,
    alert: dict[str, Any] | None,
    source_alert_id: int | None,
    portfolio_order_id: int | None,
    ledger_id: int | None,
    client_order_id: str | None,
) -> dict[str, Any]:
    related: dict[str, Any] = {
        "alert": alert or None,
        "portfolio_orders": [],
        "execution_policy_audit": [],
        "execution_orders": [],
        "fills": [],
        "trade_attribution_ledger": [],
    }

    if source_alert_id:
        related["portfolio_orders"] = _decision_select_many(
            con,
            "portfolio_orders",
            "source_alert_id=?",
            (int(source_alert_id),),
            limit=8,
        )
        related["execution_policy_audit"] = _decision_select_many(
            con,
            "execution_policy_audit",
            "source_alert_id=?",
            (int(source_alert_id),),
            limit=8,
        )
        related["execution_orders"] = _decision_select_many(
            con,
            "execution_orders",
            "source_alert_id=?",
            (int(source_alert_id),),
            order_sql="ORDER BY submit_ts_ms DESC",
            limit=8,
        )
        related["trade_attribution_ledger"] = _decision_select_many(
            con,
            "trade_attribution_ledger",
            "source_alert_id=?",
            (int(source_alert_id),),
            limit=8,
        )
        fill_cols = _decision_table_columns(con, "execution_fills")
        if "source_alert_id" in fill_cols:
            related["fills"] = _decision_select_many(
                con,
                "execution_fills",
                "source_alert_id=?",
                (int(source_alert_id),),
                order_sql="ORDER BY fill_ts_ms DESC",
                limit=8,
            )

    if portfolio_order_id:
        if not related["portfolio_orders"]:
            order = _decision_select_one(con, "portfolio_orders", "id=?", (int(portfolio_order_id),))
            if order:
                related["portfolio_orders"] = [order]
        policy_cols = _decision_table_columns(con, "execution_policy_audit")
        if not related["execution_policy_audit"] and "portfolio_orders_batch_id" in policy_cols:
            related["execution_policy_audit"] = _decision_select_many(
                con,
                "execution_policy_audit",
                "portfolio_orders_batch_id=?",
                (int(portfolio_order_id),),
                limit=8,
            )
        order_cols = _decision_table_columns(con, "execution_orders")
        if not related["execution_orders"] and "portfolio_orders_id" in order_cols:
            related["execution_orders"] = _decision_select_many(
                con,
                "execution_orders",
                "portfolio_orders_id=?",
                (int(portfolio_order_id),),
                order_sql="ORDER BY submit_ts_ms DESC",
                limit=8,
            )

    if ledger_id:
        ledger = _decision_select_one(con, "trade_attribution_ledger", "id=?", (int(ledger_id),))
        if ledger:
            if not related["trade_attribution_ledger"]:
                related["trade_attribution_ledger"] = [ledger]
            elif not any(_decision_int(row.get("id")) == int(ledger_id) for row in related["trade_attribution_ledger"]):
                related["trade_attribution_ledger"].insert(0, ledger)

    if client_order_id:
        if not related["execution_orders"]:
            related["execution_orders"] = _decision_select_many(
                con,
                "execution_orders",
                "client_order_id=?",
                (str(client_order_id),),
                order_sql="ORDER BY submit_ts_ms DESC",
                limit=8,
            )
        fill_cols = _decision_table_columns(con, "execution_fills")
        if not related["fills"] and "client_order_id" in fill_cols:
            related["fills"] = _decision_select_many(
                con,
                "execution_fills",
                "client_order_id=?",
                (str(client_order_id),),
                order_sql="ORDER BY fill_ts_ms DESC",
                limit=8,
            )

    return related


def _build_decision_stages(decision: dict[str, Any] | None, related: dict[str, Any]) -> list[dict[str, Any]]:
    alert = related.get("alert") or {}
    portfolio_orders = list(related.get("portfolio_orders") or [])
    policy_rows = list(related.get("execution_policy_audit") or [])
    execution_orders = list(related.get("execution_orders") or [])
    fills = list(related.get("fills") or [])
    ledger_rows = list(related.get("trade_attribution_ledger") or [])
    suppressed = [row for row in ledger_rows if row.get("suppression_reason") not in (None, "")]

    stages: list[dict[str, Any]] = []
    if alert:
        stages.append(_stage(
            "source",
            "Source signal",
            status="available",
            summary=str(_decision_first(alert.get("event_title"), alert.get("title"), alert.get("message"), "Alert signal recorded.")),
            ts_ms=alert.get("ts_ms"),
            data=alert,
        ))
    elif decision:
        stages.append(_stage(
            "source",
            "Source signal",
            status="partial",
            summary=f"Decision row is present; upstream alert row was not linked. Event id: {decision.get('event_id', 'unavailable')}.",
            ts_ms=decision.get("ts_ms"),
            data={"event_id": decision.get("event_id"), "symbol": decision.get("symbol")},
            unavailable_reason="source_alert_unavailable",
        ))
    else:
        stages.append(_stage(
            "source",
            "Source signal",
            status="unavailable",
            summary="No decision or upstream alert record was available for this drill-down.",
            unavailable_reason="missing_decision_source",
        ))

    if decision:
        model_version = _decision_first(decision.get("model_version"), decision.get("model_ts_ms"), "version unavailable")
        stages.append(_stage(
            "model",
            "Model decision",
            status="available",
            summary=(
                f"{_decision_first(decision.get('model_name'), 'model unavailable')} "
                f"({model_version}) confidence {_format_decision_confidence(decision.get('confidence'))}"
            ),
            ts_ms=decision.get("ts_ms"),
            data={
                "decision_id": decision.get("decision_id"),
                "model_name": decision.get("model_name"),
                "model_kind": decision.get("model_kind"),
                "model_version": decision.get("model_version"),
                "model_ts_ms": decision.get("model_ts_ms"),
                "predicted_z": decision.get("predicted_z"),
                "confidence": decision.get("confidence"),
                "confidence_raw": decision.get("confidence_raw"),
                "prediction_strength": decision.get("prediction_strength"),
                "features_hash": decision.get("features_hash"),
                "feature_set_tag": decision.get("feature_set_tag"),
            },
        ))
    else:
        stages.append(_stage(
            "model",
            "Model decision",
            status="unavailable",
            summary="No decision_log row was found for this identifier.",
            unavailable_reason="decision_log_row_missing",
        ))

    if portfolio_orders:
        first_order = portfolio_orders[0]
        stages.append(_stage(
            "portfolio",
            "Portfolio intent",
            status="available",
            summary=(
                f"{_decision_first(first_order.get('action'), 'intent')} "
                f"{_decision_first(first_order.get('symbol'), '')} "
                f"delta {_decision_first(first_order.get('delta_weight'), 'unavailable')}"
            ).strip(),
            ts_ms=first_order.get("ts_ms"),
            data={"count": len(portfolio_orders), "rows": portfolio_orders},
        ))
    else:
        stages.append(_stage(
            "portfolio",
            "Portfolio intent",
            status="unavailable",
            summary="No linked portfolio order intent is available.",
            unavailable_reason="portfolio_order_unavailable",
        ))

    if policy_rows:
        first_policy = policy_rows[0]
        decision_json = _decision_dict(first_policy.get("decision_json"))
        blocked_by = _decision_first(decision_json.get("blocked_by"), first_policy.get("decision"), first_policy.get("suppression_state"))
        stages.append(_stage(
            "policy",
            "Risk and policy checks",
            status="available",
            summary=str(blocked_by or "Execution policy audit row recorded."),
            ts_ms=first_policy.get("ts_ms"),
            data={"count": len(policy_rows), "rows": policy_rows},
        ))
    elif suppressed:
        first_suppressed = suppressed[0]
        stages.append(_stage(
            "policy",
            "Risk and policy checks",
            status="suppressed",
            summary=str(first_suppressed.get("suppression_reason") or "Suppressed by execution policy."),
            ts_ms=first_suppressed.get("ts_ms"),
            data={"count": len(suppressed), "rows": suppressed},
        ))
    elif decision and (_decision_first(decision.get("risk_impact"), decision.get("rule_id")) is not None):
        stages.append(_stage(
            "policy",
            "Risk and policy checks",
            status="partial",
            summary=str(_decision_first(decision.get("risk_impact"), decision.get("rule_id"), "Decision risk fields are present.")),
            ts_ms=decision.get("ts_ms"),
            data={"risk_impact": decision.get("risk_impact"), "rule_id": decision.get("rule_id")},
            unavailable_reason="execution_policy_audit_unavailable",
        ))
    else:
        stages.append(_stage(
            "policy",
            "Risk and policy checks",
            status="unavailable",
            summary="No linked risk, suppression, or execution-policy audit row is available.",
            unavailable_reason="policy_audit_unavailable",
        ))

    if execution_orders:
        first_exec = execution_orders[0]
        stages.append(_stage(
            "route",
            "Route",
            status="available",
            summary=(
                f"{_decision_first(first_exec.get('broker'), 'broker unavailable')} "
                f"{_decision_first(first_exec.get('status'), first_exec.get('state'), 'status unavailable')}"
            ),
            ts_ms=_decision_first(first_exec.get("submit_ts_ms"), first_exec.get("updated_ts_ms"), first_exec.get("ts_ms")),
            data={"count": len(execution_orders), "rows": execution_orders},
        ))
    elif suppressed:
        stages.append(_stage(
            "route",
            "Route",
            status="suppressed",
            summary="No broker route was created because the trade was suppressed.",
            ts_ms=suppressed[0].get("ts_ms"),
            data={"suppression_reason": suppressed[0].get("suppression_reason")},
        ))
    else:
        stages.append(_stage(
            "route",
            "Route",
            status="unavailable",
            summary="No linked execution order or route was found.",
            unavailable_reason="execution_route_unavailable",
        ))

    if fills:
        first_fill = fills[0]
        stages.append(_stage(
            "outcome",
            "Outcome",
            status="executed",
            summary=(
                f"Fill recorded at {_decision_first(first_fill.get('fill_px'), first_fill.get('px'), 'price unavailable')} "
                f"for {_decision_first(first_fill.get('fill_qty'), first_fill.get('qty'), 'size unavailable')}"
            ),
            ts_ms=_decision_first(first_fill.get("fill_ts_ms"), first_fill.get("ts_ms")),
            data={"count": len(fills), "rows": fills},
        ))
    elif suppressed:
        first_suppressed = suppressed[0]
        stages.append(_stage(
            "outcome",
            "Outcome",
            status="suppressed",
            summary=str(first_suppressed.get("suppression_reason") or "Trade did not execute because it was suppressed."),
            ts_ms=first_suppressed.get("ts_ms"),
            data=first_suppressed,
        ))
    elif ledger_rows:
        first_ledger = ledger_rows[0]
        stages.append(_stage(
            "outcome",
            "Outcome",
            status="available",
            summary=str(_decision_first(first_ledger.get("pnl"), first_ledger.get("decision_json"), "Attribution row recorded.")),
            ts_ms=first_ledger.get("ts_ms"),
            data={"count": len(ledger_rows), "rows": ledger_rows},
        ))
    else:
        stages.append(_stage(
            "outcome",
            "Outcome",
            status="unavailable",
            summary="No fill, suppression, or attribution outcome has been linked yet.",
            unavailable_reason="outcome_unavailable",
        ))

    return stages


def get_recent_decisions(limit: int = 25):
    limit = max(1, min(250, int(limit or 25)))
    rows = fetch_recent_decisions(limit=limit) or []
    normalized_rows = [_normalize_decision_record(row) or row for row in rows]
    return {
        "ok": True,
        "ts_ms": int(time.time() * 1000),
        "decisions": normalized_rows,
        "meta": {"ready": True, "count": int(len(normalized_rows))},
    }


def get_decision_detail(
    decision_id: int = 0,
    *,
    source_alert_id: int | None = None,
    portfolio_order_id: int | None = None,
    ledger_id: int | None = None,
    client_order_id: str | None = None,
):
    decision_id_value = _decision_int(decision_id)
    detail = fetch_decision_detail(int(decision_id_value)) if decision_id_value else None
    detail = _normalize_decision_record(detail)

    source_alert_id_value = _decision_int(source_alert_id)
    portfolio_order_id_value = _decision_int(portfolio_order_id)
    ledger_id_value = _decision_int(ledger_id)
    client_order_id_value = str(client_order_id or "").strip() or None

    con = None
    related: dict[str, Any] = {
        "alert": None,
        "portfolio_orders": [],
        "execution_policy_audit": [],
        "execution_orders": [],
        "fills": [],
        "trade_attribution_ledger": [],
    }
    try:
        con = db_connect()
        portfolio_order = None
        ledger_row = None
        execution_order = None
        if portfolio_order_id_value:
            portfolio_order = _decision_select_one(con, "portfolio_orders", "id=?", (int(portfolio_order_id_value),))
            source_alert_id_value = _decision_int(_decision_first(source_alert_id_value, (portfolio_order or {}).get("source_alert_id")))
        if ledger_id_value:
            ledger_row = _decision_select_one(con, "trade_attribution_ledger", "id=?", (int(ledger_id_value),))
            source_alert_id_value = _decision_int(_decision_first(source_alert_id_value, (ledger_row or {}).get("source_alert_id")))
        if client_order_id_value:
            execution_order = _decision_select_one(
                con,
                "execution_orders",
                "client_order_id=?",
                (str(client_order_id_value),),
                order_sql="ORDER BY submit_ts_ms DESC",
            )
            source_alert_id_value = _decision_int(_decision_first(source_alert_id_value, (execution_order or {}).get("source_alert_id")))
            portfolio_order_id_value = _decision_int(_decision_first(portfolio_order_id_value, (execution_order or {}).get("portfolio_orders_id")))

        alert = _lookup_alert_for_decision(con, detail, source_alert_id_value)
        if alert and source_alert_id_value is None:
            source_alert_id_value = _decision_int(alert.get("id"))
        if detail is None:
            detail = _normalize_decision_record(
                _lookup_decision_for_lineage(
                    con,
                    source_alert_id=source_alert_id_value,
                    portfolio_order=portfolio_order,
                    alert=alert,
                )
            )
        if detail and alert is None:
            alert = _lookup_alert_for_decision(con, detail, source_alert_id_value)
            if alert and source_alert_id_value is None:
                source_alert_id_value = _decision_int(alert.get("id"))

        related = _decision_related_rows(
            con,
            decision=detail,
            alert=alert,
            source_alert_id=source_alert_id_value,
            portfolio_order_id=portfolio_order_id_value,
            ledger_id=ledger_id_value,
            client_order_id=client_order_id_value,
        )
        if portfolio_order and not related.get("portfolio_orders"):
            related["portfolio_orders"] = [portfolio_order]
        if ledger_row and not related.get("trade_attribution_ledger"):
            related["trade_attribution_ledger"] = [ledger_row]
        if execution_order and not related.get("execution_orders"):
            related["execution_orders"] = [execution_order]
    except Exception as e:
        _warn_nonfatal(
            "API_READ_ADVANCED_DECISION_DETAIL_AGGREGATE_FAILED",
            e,
            once_key="decision_detail_aggregate_failed",
        )
    finally:
        if con is not None:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal(
                    "API_READ_ADVANCED_DECISION_DETAIL_CLOSE_FAILED",
                    e,
                    once_key="decision_detail_close_failed",
                )

    stages = _build_decision_stages(detail, related)
    if not detail and not any(
        related.get(key)
        for key in ("alert", "portfolio_orders", "execution_policy_audit", "execution_orders", "fills", "trade_attribution_ledger")
    ):
        return {"ok": False, "error": "decision_not_found", "decision": None}
    return {
        "ok": True,
        "ts_ms": int(time.time() * 1000),
        "decision": detail,
        "stages": stages,
        "related": related,
        "meta": {
            "detail_version": 1,
            "decision_id": _decision_int((detail or {}).get("decision_id")),
            "source_alert_id": source_alert_id_value,
            "portfolio_order_id": portfolio_order_id_value,
            "ledger_id": ledger_id_value,
            "client_order_id": client_order_id_value,
        },
    }


def get_audit_records(table: str, limit: int = 100, from_id: int | None = None, to_id: int | None = None):
    limit = max(1, min(10000, int(limit or 100)))
    table = require_allowed_table_name(table)
    rows = fetch_recent_audit_records(table, limit=limit, from_id=from_id, to_id=to_id) or []
    return {
        "ok": True,
        "ts_ms": int(time.time() * 1000),
        "table": str(table),
        "records": rows,
        "meta": {"ready": True, "count": int(len(rows))},
    }
