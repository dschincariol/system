"""
Trade lifecycle trace utility.

Builds an auditable view across the main trade lifecycle tables starting from
`source_alert_id` or `client_order_id`.
"""

import json
import sys
from typing import Any, Dict, List, Optional

from engine.runtime.storage import connect, init_db
from engine.runtime.trade_lifecycle_projection import read_trade_lifecycle_projection


def _table_exists(con, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _table_columns(con, table_name: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
    return {str(row[1] or "").strip() for row in rows if row and len(row) > 1}


def _select_present_columns(con, table_name: str, requested: List[str]) -> List[str]:
    available = _table_columns(con, table_name)
    return [name for name in requested if name in available]


def _row_to_dict(row, columns: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not row:
        return out
    for idx, name in enumerate(columns):
        value = row[idx]
        if name.endswith("_json") and value not in (None, ""):
            try:
                out[name] = json.loads(value)
                continue
            except Exception as e:
                sys.stderr.write(f"[engine.runtime.trade_lifecycle] json_parse_failed column={name}: {type(e).__name__}: {e}\n")
                sys.stderr.flush()
        out[name] = value
    return out


def _fetch_one(con, sql: str, params: tuple, columns: List[str]) -> Optional[Dict[str, Any]]:
    row = con.execute(sql, params).fetchone()
    if not row:
        return None
    return _row_to_dict(row, columns)


def _fetch_all(con, sql: str, params: tuple, columns: List[str]) -> List[Dict[str, Any]]:
    rows = con.execute(sql, params).fetchall()
    return [_row_to_dict(row, columns) for row in (rows or [])]


def trace_trade_lifecycle(
    *,
    source_alert_id: Optional[int] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    init_db()
    report: Dict[str, Any] = {
        "ok": True,
        "anchor": {
            "source_alert_id": (int(source_alert_id) if source_alert_id is not None else None),
            "client_order_id": (str(client_order_id) if client_order_id not in (None, "") else None),
        },
        "steps": {},
        "breaks": [],
        "approximations": [],
    }

    con = connect(readonly=True)
    try:
        projection = read_trade_lifecycle_projection(
            con,
            source_alert_id=(int(source_alert_id) if source_alert_id is not None else None),
            client_order_id=(str(client_order_id) if client_order_id not in (None, "") else None),
        )
        order_events = list(projection.get("order_events") or [])
        order_commands = list(projection.get("order_commands") or [])
        execution_orders = list(projection.get("execution_orders") or [])
        report["steps"]["order_events"] = order_events
        report["steps"]["order_commands"] = order_commands

        order = None
        if client_order_id not in (None, ""):
            order = next(
                (
                    item
                    for item in execution_orders
                    if str(item.get("client_order_id") or "") == str(client_order_id)
                ),
                None,
            )
        report["steps"]["execution_order"] = order
        if order and source_alert_id is None and order.get("source_alert_id") is not None:
            source_alert_id = int(order["source_alert_id"])
            report["anchor"]["source_alert_id"] = int(source_alert_id)

        if not execution_orders and source_alert_id is not None:
            projection = read_trade_lifecycle_projection(
                con,
                source_alert_id=int(source_alert_id),
                client_order_id=(str(client_order_id) if client_order_id not in (None, "") else None),
            )
            order_events = list(projection.get("order_events") or [])
            order_commands = list(projection.get("order_commands") or [])
            execution_orders = list(projection.get("execution_orders") or [])
            report["steps"]["order_events"] = order_events
            report["steps"]["order_commands"] = order_commands
            if order is None and client_order_id not in (None, ""):
                order = next(
                    (
                        item
                        for item in execution_orders
                        if str(item.get("client_order_id") or "") == str(client_order_id)
                    ),
                    None,
                )
                report["steps"]["execution_order"] = order
            if order is None and source_alert_id is not None:
                order = next(
                    (
                        item
                        for item in execution_orders
                        if item.get("source_alert_id") == int(source_alert_id)
                    ),
                    None,
                )
                if report["steps"].get("execution_order") is None:
                    report["steps"]["execution_order"] = order

        report["steps"]["execution_orders"] = execution_orders

        portfolio_orders = []
        primary_portfolio_order: Optional[Dict[str, Any]] = None
        if _table_exists(con, "portfolio_orders"):
            portfolio_order_columns = _select_present_columns(
                con,
                "portfolio_orders",
                [
                    "id",
                    "ts_ms",
                    "model_id",
                    "symbol",
                    "action",
                    "from_side",
                    "to_side",
                    "from_weight",
                    "to_weight",
                    "delta_weight",
                    "source_alert_id",
                    "prediction_id",
                    "explain_json",
                ],
            )
            portfolio_order_ids = sorted(
                {
                    int(item.get("portfolio_orders_id"))
                    for item in execution_orders
                    if item.get("portfolio_orders_id") not in (None, "")
                }
            )
            if portfolio_order_ids:
                placeholders = ",".join(["?"] * len(portfolio_order_ids))
                portfolio_orders = _fetch_all(
                    con,
                    f"""
                    SELECT {", ".join(portfolio_order_columns)}
                    FROM portfolio_orders
                    WHERE id IN ({placeholders})
                    ORDER BY ts_ms ASC, id ASC
                    """,
                    tuple(portfolio_order_ids),
                    portfolio_order_columns,
                )
            elif source_alert_id is not None:
                portfolio_orders = _fetch_all(
                    con,
                    f"""
                    SELECT {", ".join(portfolio_order_columns)}
                    FROM portfolio_orders
                    WHERE source_alert_id=?
                    ORDER BY ts_ms ASC, id ASC
                    """,
                    (int(source_alert_id),),
                    portfolio_order_columns,
                )
            if not portfolio_orders and order and order.get("prediction_id") not in (None, "") and "prediction_id" in set(portfolio_order_columns):
                portfolio_orders = _fetch_all(
                    con,
                    f"""
                    SELECT {", ".join(portfolio_order_columns)}
                    FROM portfolio_orders
                    WHERE prediction_id=?
                    ORDER BY ts_ms ASC, id ASC
                    """,
                    (int(order["prediction_id"]),),
                    portfolio_order_columns,
                )
        if portfolio_orders:
            order_portfolio_id = int(order.get("portfolio_orders_id")) if order and order.get("portfolio_orders_id") not in (None, "") else None
            if order_portfolio_id is not None:
                primary_portfolio_order = next(
                    (item for item in portfolio_orders if int(item.get("id") or 0) == int(order_portfolio_id)),
                    None,
                )
            if primary_portfolio_order is None:
                primary_portfolio_order = portfolio_orders[0]

        if source_alert_id is None and primary_portfolio_order and primary_portfolio_order.get("source_alert_id") not in (None, ""):
            source_alert_id = int(primary_portfolio_order["source_alert_id"])
            report["anchor"]["source_alert_id"] = int(source_alert_id)

        alert = None
        if source_alert_id is not None and _table_exists(con, "alerts"):
            alert_columns = _select_present_columns(
                con,
                "alerts",
                [
                    "id",
                    "ts_ms",
                    "event_id",
                    "prediction_id",
                    "event_title",
                    "symbol",
                    "horizon_s",
                    "expected_z",
                    "confidence",
                    "severity",
                    "rule_id",
                    "model_name",
                    "model_id",
                    "model_version",
                    "explain_json",
                ],
            )
            alert = _fetch_one(
                con,
                f"""
                SELECT {", ".join(alert_columns)}
                FROM alerts
                WHERE id=?
                LIMIT 1
                """,
                (int(source_alert_id),),
                alert_columns,
            )
        report["steps"]["alert"] = alert

        if source_alert_id is not None and not alert:
            report["breaks"].append("missing_alert")

        event_id = alert.get("event_id") if alert else None
        prediction_id = (
            (order.get("prediction_id") if order else None)
            or (primary_portfolio_order.get("prediction_id") if primary_portfolio_order else None)
            or (alert.get("prediction_id") if alert else None)
        )
        symbol = (
            (alert.get("symbol") if alert else None)
            or (order.get("symbol") if order else None)
            or (primary_portfolio_order.get("symbol") if primary_portfolio_order else None)
        )
        horizon_s = alert.get("horizon_s") if alert else None
        model_name = (
            (alert.get("model_name") if alert else None)
            or ((order or {}).get("extra_json") or {}).get("model_name")
        )
        model_id = (
            (order.get("model_id") if order else None)
            or (alert.get("model_id") if alert else None)
            or (primary_portfolio_order.get("model_id") if primary_portfolio_order else None)
        )

        prediction = None
        prediction_history: List[Dict[str, Any]] = []
        decision = None
        if _table_exists(con, "predictions") and (
            prediction_id not in (None, "")
            or (symbol and horizon_s is not None)
        ):
            prediction_columns = _select_present_columns(
                con,
                "predictions",
                [
                    "id",
                    "ts_ms",
                    "event_id",
                    "symbol",
                    "horizon_s",
                    "predicted_z",
                    "confidence",
                    "confidence_raw",
                    "prediction_strength",
                    "model_name",
                    "model_id",
                    "model_version",
                ],
            )
            if prediction_id not in (None, ""):
                prediction = _fetch_one(
                    con,
                    f"""
                    SELECT {", ".join(prediction_columns)}
                    FROM predictions
                    WHERE id=?
                    LIMIT 1
                    """,
                    (int(prediction_id),),
                    prediction_columns,
                )
            if prediction is None and event_id is not None:
                prediction = _fetch_one(
                    con,
                    f"""
                    SELECT {", ".join(prediction_columns)}
                    FROM predictions
                    WHERE event_id=? AND symbol=? AND horizon_s=?
                    LIMIT 1
                    """,
                    (int(event_id), str(symbol), int(horizon_s)),
                    prediction_columns,
                )
            elif prediction is None and alert:
                report["approximations"].append("prediction_resolved_without_alert_event_id")
                params = [str(symbol), int(horizon_s), int(alert.get("ts_ms") or 0)]
                sql = f"""
                    SELECT {", ".join(prediction_columns)}
                    FROM predictions
                    WHERE symbol=? AND horizon_s=? AND ts_ms <= ?
                """
                if model_id not in (None, ""):
                    sql += " AND COALESCE(NULLIF(TRIM(model_id), ''), '') = ?"
                    params.append(str(model_id))
                elif model_name not in (None, ""):
                    sql += " AND COALESCE(NULLIF(TRIM(model_name), ''), '') = ?"
                    params.append(str(model_name))
                sql += " ORDER BY ts_ms DESC LIMIT 1"
                prediction = _fetch_one(
                    con,
                    sql,
                    tuple(params),
                    prediction_columns,
                )
        if prediction:
            if event_id in (None, "") and prediction.get("event_id") not in (None, ""):
                event_id = prediction.get("event_id")
            if symbol in (None, "") and prediction.get("symbol") not in (None, ""):
                symbol = prediction.get("symbol")
            if horizon_s in (None, "") and prediction.get("horizon_s") not in (None, ""):
                horizon_s = prediction.get("horizon_s")
            if model_name in (None, "") and prediction.get("model_name") not in (None, ""):
                model_name = prediction.get("model_name")
            if model_id in (None, "") and prediction.get("model_id") not in (None, ""):
                model_id = prediction.get("model_id")
        report["steps"]["prediction"] = prediction

        if _table_exists(con, "prediction_history") and symbol and horizon_s is not None:
            if event_id is not None:
                prediction_history = _fetch_all(
                    con,
                    """
                    SELECT id, ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                           confidence_raw, prediction_strength, model_name, model_id, model_version
                    FROM prediction_history
                    WHERE event_id=? AND symbol=? AND horizon_s=?
                    ORDER BY ts_ms ASC, id ASC
                    """,
                    (int(event_id), str(symbol), int(horizon_s)),
                    [
                        "id",
                        "ts_ms",
                        "event_id",
                        "symbol",
                        "horizon_s",
                        "predicted_z",
                        "confidence",
                        "confidence_raw",
                        "prediction_strength",
                        "model_name",
                        "model_id",
                        "model_version",
                    ],
                )
            elif prediction and prediction.get("event_id") is not None:
                prediction_history = _fetch_all(
                    con,
                    """
                    SELECT id, ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                           confidence_raw, prediction_strength, model_name, model_id, model_version
                    FROM prediction_history
                    WHERE event_id=? AND symbol=? AND horizon_s=?
                    ORDER BY ts_ms ASC, id ASC
                    """,
                    (int(prediction["event_id"]), str(symbol), int(horizon_s)),
                    [
                        "id",
                        "ts_ms",
                        "event_id",
                        "symbol",
                        "horizon_s",
                        "predicted_z",
                        "confidence",
                        "confidence_raw",
                        "prediction_strength",
                        "model_name",
                        "model_id",
                        "model_version",
                    ],
                )
        report["steps"]["prediction_history"] = prediction_history

        if _table_exists(con, "decision_log") and symbol and horizon_s is not None:
            if event_id is not None:
                decision = _fetch_one(
                    con,
                    """
                    SELECT ts_ms, event_id, symbol, horizon_s, predicted_z, confidence, model_name,
                           model_kind, model_ts_ms, features_hash, features_json, explain_json, extra_json
                    FROM decision_log
                    WHERE event_id=? AND symbol=? AND horizon_s=?
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """,
                    (int(event_id), str(symbol), int(horizon_s)),
                    [
                        "ts_ms",
                        "event_id",
                        "symbol",
                        "horizon_s",
                        "predicted_z",
                        "confidence",
                        "model_name",
                        "model_kind",
                        "model_ts_ms",
                        "features_hash",
                        "features_json",
                        "explain_json",
                        "extra_json",
                    ],
                )
            elif alert:
                report["approximations"].append("decision_resolved_without_alert_event_id")
                decision = _fetch_one(
                    con,
                    """
                    SELECT ts_ms, event_id, symbol, horizon_s, predicted_z, confidence, model_name,
                           model_kind, model_ts_ms, features_hash, features_json, explain_json, extra_json
                    FROM decision_log
                    WHERE symbol=? AND horizon_s=? AND ts_ms <= ?
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """,
                    (str(symbol), int(horizon_s), int(alert.get("ts_ms") or 0)),
                    [
                        "ts_ms",
                        "event_id",
                        "symbol",
                        "horizon_s",
                        "predicted_z",
                        "confidence",
                        "model_name",
                        "model_kind",
                        "model_ts_ms",
                        "features_hash",
                        "features_json",
                        "explain_json",
                        "extra_json",
                    ],
                )
        report["steps"]["decision"] = decision

        report["steps"]["portfolio_orders"] = portfolio_orders

        client_order_ids = [
            str(item.get("client_order_id"))
            for item in execution_orders
            if item.get("client_order_id") not in (None, "")
        ]

        fills: List[Dict[str, Any]] = list(projection.get("fills") or [])
        report["steps"]["fills"] = fills

        resolved_model_id = model_id or ((fills[0].get("model_id") if fills else None) or None)
        resolved_symbol = symbol or ((fills[0].get("symbol") if fills else None) or None)
        if resolved_model_id and resolved_symbol and _table_exists(con, "model_position_state"):
            report["steps"]["position"] = _fetch_one(
                con,
                """
                SELECT model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms
                FROM model_position_state
                WHERE model_id=? AND symbol=?
                LIMIT 1
                """,
                (str(resolved_model_id), str(resolved_symbol)),
                ["model_id", "symbol", "net_qty", "avg_entry_price", "realized_pnl", "last_update_ts_ms"],
            )
        else:
            report["steps"]["position"] = None

        if source_alert_id is not None and resolved_symbol and _table_exists(con, "pnl_attribution"):
            if resolved_model_id:
                pnl = _fetch_one(
                    con,
                    """
                    SELECT ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                           slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                    FROM pnl_attribution
                    WHERE source_alert_id=? AND model_id=? AND symbol=?
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """,
                    (int(source_alert_id), str(resolved_model_id), str(resolved_symbol)),
                    [
                        "ts_ms",
                        "source_alert_id",
                        "model_id",
                        "model_version",
                        "symbol",
                        "pnl",
                        "fees",
                        "slippage_bps",
                        "position_size",
                        "avg_price",
                        "realized_pnl",
                        "unrealized_pnl",
                        "extra_json",
                    ],
                )
            else:
                pnl = _fetch_one(
                    con,
                    """
                    SELECT ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                           slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                    FROM pnl_attribution
                    WHERE source_alert_id=? AND symbol=?
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """,
                    (int(source_alert_id), str(resolved_symbol)),
                    [
                        "ts_ms",
                        "source_alert_id",
                        "model_id",
                        "model_version",
                        "symbol",
                        "pnl",
                        "fees",
                        "slippage_bps",
                        "position_size",
                        "avg_price",
                        "realized_pnl",
                        "unrealized_pnl",
                        "extra_json",
                    ],
                )
        else:
            pnl = None
        report["steps"]["pnl_attribution"] = pnl

        marketplace = None
        if _table_exists(con, "model_marketplace_scores") and resolved_symbol and horizon_s is not None:
            if resolved_model_id:
                marketplace = _fetch_one(
                    con,
                    """
                    SELECT model_id, model_name, symbol, horizon_s, regime, stage, score,
                           trades, wins, losses, gross_pnl, net_pnl, avg_confidence,
                           last_signal_ts_ms, updated_ts_ms, meta_json
                    FROM model_marketplace_scores
                    WHERE model_id=? AND symbol=? AND horizon_s=?
                    ORDER BY updated_ts_ms DESC
                    LIMIT 1
                    """,
                    (str(resolved_model_id), str(resolved_symbol), int(horizon_s)),
                    [
                        "model_id",
                        "model_name",
                        "symbol",
                        "horizon_s",
                        "regime",
                        "stage",
                        "score",
                        "trades",
                        "wins",
                        "losses",
                        "gross_pnl",
                        "net_pnl",
                        "avg_confidence",
                        "last_signal_ts_ms",
                        "updated_ts_ms",
                        "meta_json",
                    ],
                )
            elif model_name:
                marketplace = _fetch_one(
                    con,
                    """
                    SELECT model_id, model_name, symbol, horizon_s, regime, stage, score,
                           trades, wins, losses, gross_pnl, net_pnl, avg_confidence,
                           last_signal_ts_ms, updated_ts_ms, meta_json
                    FROM model_marketplace_scores
                    WHERE model_name=? AND symbol=? AND horizon_s=?
                    ORDER BY updated_ts_ms DESC
                    LIMIT 1
                    """,
                    (str(model_name), str(resolved_symbol), int(horizon_s)),
                    [
                        "model_id",
                        "model_name",
                        "symbol",
                        "horizon_s",
                        "regime",
                        "stage",
                        "score",
                        "trades",
                        "wins",
                        "losses",
                        "gross_pnl",
                        "net_pnl",
                        "avg_confidence",
                        "last_signal_ts_ms",
                        "updated_ts_ms",
                        "meta_json",
                    ],
                )
        report["steps"]["marketplace_score"] = marketplace

        metrics = None
        if _table_exists(con, "model_metrics") and model_name and resolved_symbol and horizon_s is not None:
            metrics = _fetch_one(
                con,
                """
                SELECT model_name, symbol, horizon_s, n, ts_ms, metrics_json
                FROM model_metrics
                WHERE model_name=? AND symbol=? AND horizon_s=?
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (str(model_name), str(resolved_symbol), int(horizon_s)),
                ["model_name", "symbol", "horizon_s", "n", "ts_ms", "metrics_json"],
            )
        report["steps"]["model_metrics"] = metrics

        required = {
            "prediction": report["steps"].get("prediction"),
            "decision": report["steps"].get("decision"),
            "alert": report["steps"].get("alert"),
            "portfolio_orders": report["steps"].get("portfolio_orders"),
            "execution_orders": report["steps"].get("execution_orders"),
            "fills": report["steps"].get("fills"),
            "position": report["steps"].get("position"),
        }
        for step_name, value in required.items():
            missing = value is None or (isinstance(value, list) and len(value) == 0)
            if missing:
                report["breaks"].append(f"missing_{step_name}")

        if fills and not pnl:
            report["approximations"].append("pnl_attribution_not_yet_materialized")
        if pnl and not marketplace:
            report["approximations"].append("marketplace_score_not_yet_materialized")
        if alert and alert.get("event_id") in (None, ""):
            report["approximations"].append("alert_missing_event_id")

        report["ok"] = len(report["breaks"]) == 0
        return report
    finally:
        con.close()
