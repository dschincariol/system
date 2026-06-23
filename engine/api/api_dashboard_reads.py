"""
FILE: api_dashboard_reads.py

HTTP/API handlers for dashboard reads endpoints.
"""

"""
Dashboard Read Endpoints (moved out of dashboard_server)

Goal:
- dashboard_server.py is orchestration only
- ALL DB reads and dev_core reads live behind engine/api
"""

import logging

from engine.api.http_parsing import qs as _qs
from engine.api.api_read import _table_exists
from engine.api.internal_access import db_connect
from engine.api.sql_identifiers import require_allowed_table_name
from engine.runtime.failure_diagnostics import failure_response, log_failure
from engine.runtime.price_read_router import fetch_price_rows
from engine.runtime.state_cache import cache_get_or_load

from engine.api.api_read_advanced import (
    get_audit_records,
    get_decision_detail,
    get_model_diagnostics,
    get_temporal_models,
    get_latest_portfolio_backtest,
    get_portfolio_snapshot,
    get_recent_decisions,
    get_execution_metrics_by_symbol,
    get_execution_cost_by_confidence,
    get_social_features,
    get_social_regimes,
    get_social_blocks,
    get_validation_rows,
    get_shadow_capital_scores,
    run_shadow_capital_scores,
    get_size_policy,
)
from engine.api.feature_visibility import (
    LOW_CONFIDENCE_THRESHOLD,
    build_feature_visibility,
)

LOG = logging.getLogger(__name__)


def _parse_int(value, default, minimum=None, maximum=None):
    try:
        out = int(value)
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(int(minimum), out)
    if maximum is not None:
        out = min(int(maximum), out)
    return out

# ------------------------------
# Handlers (HTTP signatures)
# build_handler calls:
#   GET: handler(parsed, ctx)
#   POST: handler(parsed, body, ctx)
# ------------------------------

def api_get_model_diagnostics(_parsed, _ctx=None):
    # Dashboard read handlers stay thin and delegate to advanced read helpers
    # so UI wiring remains separate from query implementation.
    return {"ok": True, "data": get_model_diagnostics()}

def api_get_temporal_models(parsed, ctx):
    try:
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "20") or "20", 20, 1, 5000)
        return get_temporal_models(limit=limit)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_temporal_models_failed",
            code="API_DASHBOARD_TEMPORAL_MODELS_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=ctx,
        )

def api_get_latest_portfolio_backtest(_parsed, _ctx=None):
    return get_latest_portfolio_backtest()


def api_get_portfolio(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        limit_state = _parse_int(qs.get("limit_state", "200") or "200", 200, 1, 5000)
        intents_window_ms = _parse_int(qs.get("intents_window_ms", "2500") or "2500", 2500, 0)
        intents_max_rows = _parse_int(qs.get("intents_max_rows", "5000") or "5000", 5000, 1, 50000)
        model_id = str(qs.get("model_id", "") or "").strip()
        return get_portfolio_snapshot(
            limit_state=limit_state,
            intents_window_ms=intents_window_ms,
            intents_max_rows=intents_max_rows,
            model_id=model_id,
        )
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_portfolio_failed",
            code="API_DASHBOARD_PORTFOLIO_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )


def api_get_recent_decisions(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "25") or "25", 25, 1, 250)
        return get_recent_decisions(limit=limit)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_recent_decisions_failed",
            code="API_DASHBOARD_RECENT_DECISIONS_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )


def api_get_decision_detail(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        decision_id = _parse_int(qs.get("id", "0") or "0", 0, 0)
        source_alert_id = _parse_int(qs.get("source_alert_id", "0") or "0", 0, 0)
        portfolio_order_id = _parse_int(qs.get("portfolio_order_id", "0") or "0", 0, 0)
        ledger_id = _parse_int(qs.get("ledger_id", "0") or "0", 0, 0)
        client_order_id = str(qs.get("client_order_id", "") or "").strip()
        if (
            decision_id <= 0
            and source_alert_id <= 0
            and portfolio_order_id <= 0
            and ledger_id <= 0
            and not client_order_id
        ):
            return {"ok": False, "error": "missing_id"}
        return get_decision_detail(
            decision_id,
            source_alert_id=(source_alert_id if source_alert_id > 0 else None),
            portfolio_order_id=(portfolio_order_id if portfolio_order_id > 0 else None),
            ledger_id=(ledger_id if ledger_id > 0 else None),
            client_order_id=(client_order_id or None),
        )
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_decision_detail_failed",
            code="API_DASHBOARD_DECISION_DETAIL_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )


def api_get_feature_visibility(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        symbol = str(qs.get("symbol", "") or "").strip().upper()
        limit = _parse_int(qs.get("limit", "12") or "12", 12, 1, 100)
        try:
            threshold = float(qs.get("low_confidence_threshold", "") or LOW_CONFIDENCE_THRESHOLD)
        except Exception:
            threshold = LOW_CONFIDENCE_THRESHOLD
        return build_feature_visibility(
            symbol=symbol,
            low_confidence_threshold=float(threshold),
            lineage_limit=limit,
        )
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_feature_visibility_failed",
            code="API_DASHBOARD_FEATURE_VISIBILITY_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )


def api_get_audit_records(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        table = str(qs.get("table", "") or "").strip()
        if not table:
            return {"ok": False, "error": "missing_table"}
        try:
            table = require_allowed_table_name(table)
        except (AssertionError, ValueError) as e:
            log_failure(
                LOG,
                event="api_dashboard_reads_unauthorized_table",
                code="API_DASHBOARD_READS_UNAUTHORIZED_TABLE",
                message=str(e),
                error=e,
                level=logging.WARNING,
                component="engine.api.api_dashboard_reads",
                extra={"table": str(table)},
                persist=False,
            )
            return {
                "ok": False,
                "error": "unauthorized_table",
                "reason": "unauthorized_table",
                "detail": str(e),
                "meta": {"status": 400},
            }
        limit = _parse_int(qs.get("limit", "100") or "100", 100, 1, 10000)
        from_id_raw = qs.get("from_id", "") or ""
        to_id_raw = qs.get("to_id", "") or ""
        from_id = _parse_int(from_id_raw, 0, 1) if from_id_raw else None
        to_id = _parse_int(to_id_raw, 0, 1) if to_id_raw else None
        return get_audit_records(table=table, limit=limit, from_id=from_id, to_id=to_id)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_audit_records_failed",
            code="API_DASHBOARD_AUDIT_RECORDS_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )

def api_get_prices(parsed, _ctx=None):
    qs = _qs(parsed)
    symbol = str(qs.get("symbol", "") or "").strip().upper()
    limit = _parse_int(qs.get("limit", "200") or "200", 200, 1, 5000)

    def _load():
        data = fetch_price_rows(symbol=symbol, limit=int(limit))
        # The chart layer expects OHLCV-shaped rows even when only last-price
        # snapshots are available, so we synthesize flat candles here.
        candles = [
            {
                "ts": int(d["ts_ms"] or 0),
                "ts_ms": int(d["ts_ms"] or 0),
                "open": float(d["price"] if d["price"] is not None else d["px"] or 0.0),
                "high": float(d["price"] if d["price"] is not None else d["px"] or 0.0),
                "low": float(d["price"] if d["price"] is not None else d["px"] or 0.0),
                "close": float(d["price"] if d["price"] is not None else d["px"] or 0.0),
                "volume": 0.0,
            }
            for d in reversed(data)
            if d.get("ts_ms")
            and (d.get("price") is not None or d.get("px") is not None)
        ]
        return {
            "ok": True,
            "error": None,
            "symbol": symbol or None,
            "meta": {"ready": bool(data), "count": int(len(data))},
            "candles": candles,
            "data": data,
            "rows": data,
        }

    return cache_get_or_load("api_dashboard_prices", f"{symbol}:{int(limit)}", _load, ttl_s=0.75)


def api_get_trades(parsed, _ctx=None):
    qs = _qs(parsed)
    symbol = str(qs.get("symbol", "") or "").strip().upper()
    model_id = str(qs.get("model_id", "") or "").strip()
    limit = _parse_int(qs.get("limit", "200") or "200", 200, 1, 5000)

    con = db_connect()
    try:
        rows = []

        if _table_exists(con, "execution_fills"):
            if symbol:
                if model_id:
                    rows = con.execute(
                            """
                            SELECT
                                id,
                                symbol,
                                CASE WHEN COALESCE(fill_qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                                ABS(COALESCE(fill_qty, 0)) AS qty,
                                fill_px AS price,
                                fill_ts_ms AS ts_ms,
                                client_order_id,
                                broker,
                                'execution_fills' AS source_table
                            FROM execution_fills
                            WHERE symbol = ?
                              AND COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                            ORDER BY fill_ts_ms DESC, id DESC
                            LIMIT ?
                            """,
                            (symbol, str(model_id), int(limit)),
                        ).fetchall() or []
                else:
                    rows = con.execute(
                            """
                            SELECT
                                id,
                                symbol,
                                CASE WHEN COALESCE(fill_qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                                ABS(COALESCE(fill_qty, 0)) AS qty,
                                fill_px AS price,
                                fill_ts_ms AS ts_ms,
                                client_order_id,
                                broker,
                                'execution_fills' AS source_table
                            FROM execution_fills
                            WHERE symbol = ?
                            ORDER BY fill_ts_ms DESC, id DESC
                            LIMIT ?
                            """,
                            (symbol, int(limit)),
                        ).fetchall() or []
            else:
                if model_id:
                    rows = con.execute(
                            """
                            SELECT
                                id,
                                symbol,
                                CASE WHEN COALESCE(fill_qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                                ABS(COALESCE(fill_qty, 0)) AS qty,
                                fill_px AS price,
                                fill_ts_ms AS ts_ms,
                                client_order_id,
                                broker,
                                'execution_fills' AS source_table
                            FROM execution_fills
                            WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                            ORDER BY fill_ts_ms DESC, id DESC
                            LIMIT ?
                            """,
                            (str(model_id), int(limit)),
                        ).fetchall() or []
                else:
                    rows = con.execute(
                            """
                            SELECT
                                id,
                                symbol,
                                CASE WHEN COALESCE(fill_qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                                ABS(COALESCE(fill_qty, 0)) AS qty,
                                fill_px AS price,
                                fill_ts_ms AS ts_ms,
                                client_order_id,
                                broker,
                                'execution_fills' AS source_table
                            FROM execution_fills
                            ORDER BY fill_ts_ms DESC, id DESC
                            LIMIT ?
                            """,
                            (int(limit),),
                        ).fetchall() or []
        elif _table_exists(con, "broker_fills"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT
                        id,
                        symbol,
                        CASE WHEN COALESCE(qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                        ABS(COALESCE(qty, 0)) AS qty,
                        px AS price,
                        ts_ms,
                        source_order_id,
                        note,
                        'broker_fills' AS source_table
                    FROM broker_fills
                    WHERE symbol = ?
                    ORDER BY ts_ms DESC, id DESC
                    LIMIT ?
                    """,
                    (symbol, int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT
                        id,
                        symbol,
                        CASE WHEN COALESCE(qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                        ABS(COALESCE(qty, 0)) AS qty,
                        px AS price,
                        ts_ms,
                        source_order_id,
                        note,
                        'broker_fills' AS source_table
                    FROM broker_fills
                    ORDER BY ts_ms DESC, id DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []
        elif _table_exists(con, "trades"):
            cols = {
                str(r[1])
                for r in (con.execute("PRAGMA table_info(trades)").fetchall() or [])
                if r and len(r) > 1 and r[1]
            }
            ts_col = "ts_ms" if "ts_ms" in cols else ("ts" if "ts" in cols else None)
            if ts_col:
                if symbol:
                    rows = con.execute(
                        f"""
                        SELECT
                            id,
                            symbol,
                            side,
                            qty,
                            price,
                            {ts_col} AS ts_ms,
                            NULL AS ref,
                            NULL AS note,
                            'trades' AS source_table
                        FROM trades
                        WHERE symbol = ?
                        ORDER BY {ts_col} DESC, id DESC
                        LIMIT ?
                        """,
                        (symbol, int(limit)),
                    ).fetchall() or []
                else:
                    rows = con.execute(
                        f"""
                        SELECT
                            id,
                            symbol,
                            side,
                            qty,
                            price,
                            {ts_col} AS ts_ms,
                            NULL AS ref,
                            NULL AS note,
                            'trades' AS source_table
                        FROM trades
                        ORDER BY {ts_col} DESC, id DESC
                        LIMIT ?
                        """,
                        (int(limit),),
                    ).fetchall() or []

        data = [
            {
                "id": int(r[0] or 0),
                "symbol": str(r[1] or ""),
                "side": str(r[2] or ""),
                "qty": float(r[3] or 0.0),
                "price": float(r[4] or 0.0),
                "ts_ms": int(r[5] or 0),
                "ref": (str(r[6]) if r[6] is not None else None),
                "note": (str(r[7]) if r[7] is not None else None),
                "source_table": str(r[8] or ""),
            }
            for r in rows
        ]
        markers = [
            {
                "ts": int((d["ts_ms"] or 0) // 1000),
                "ts_ms": int(d["ts_ms"] or 0),
                "symbol": str(d["symbol"] or ""),
                "side": str(d["side"] or ""),
                "price": float(d["price"] or 0.0),
                "size": float(d["qty"] or 0.0),
            }
            for d in reversed(data)
            if d.get("ts_ms")
        ]
        return {
            "ok": True,
            "error": None,
            "symbol": symbol or None,
            "model_id": model_id or None,
            "meta": {"ready": bool(data), "count": int(len(data))},
            "markers": markers,
            "data": data,
            "rows": data,
        }
    finally:
        con.close()


def api_get_trades_legacy_table(_parsed, _ctx=None):
    from engine.runtime.storage import connect

    con = connect(readonly=True)
    try:
        cols = {
            str(r[1])
            for r in (con.execute("PRAGMA table_info(trades)").fetchall() or [])
            if r and len(r) > 1 and r[1]
        }
        ts_col = "ts_ms" if "ts_ms" in cols else ("ts" if "ts" in cols else None)
        if not ts_col:
            return {"ok": True, "data": [], "rows": []}

        rows = con.execute(
            f"""
            SELECT id, symbol, side, qty, price, {ts_col} AS ts_ms
            FROM trades
            ORDER BY {ts_col} DESC
            LIMIT 200
            """
        ).fetchall()

        data = [
            {
                "id": r["id"],
                "symbol": r["symbol"],
                "side": r["side"],
                "qty": r["qty"],
                "price": r["price"],
                "ts_ms": r["ts_ms"],
            }
            for r in rows
        ]

        return {
            "ok": True,
            "data": data,
            "rows": data,
        }
    finally:
        try:
            con.close()
        except Exception as e:
            log_failure(
                LOG,
                event="api_dashboard_reads_close_failed",
                code="API_DASHBOARD_READS_CLOSE_FAILED",
                message="API dashboard reads connection close failed.",
                error=e,
                level=logging.WARNING,
                component="engine.api.api_dashboard_reads",
                persist=False,
            )

def api_get_execution_metrics_by_symbol(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "50") or "50", 50, 1, 5000)
        return get_execution_metrics_by_symbol(limit=limit)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_execution_metrics_by_symbol_failed",
            code="API_DASHBOARD_EXECUTION_METRICS_BY_SYMBOL_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )

def api_get_execution_cost_by_confidence(_parsed, _ctx=None):
    return get_execution_cost_by_confidence()

def api_get_social_features(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        symbol = str(qs.get("symbol", "") or "").strip()
        if not symbol:
            return {"ok": False, "error": "missing_symbol"}
        limit = _parse_int(qs.get("limit", "200") or "200", 200, 1, 5000)
        return get_social_features(symbol=symbol, limit=limit)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_social_features_failed",
            code="API_DASHBOARD_SOCIAL_FEATURES_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )

def api_get_social_regimes(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        symbol = str(qs.get("symbol", "") or "").strip()
        if not symbol:
            return {"ok": False, "error": "missing_symbol"}
        limit = _parse_int(qs.get("limit", "200") or "200", 200, 1, 5000)
        return get_social_regimes(symbol=symbol, limit=limit)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_social_regimes_failed",
            code="API_DASHBOARD_SOCIAL_REGIMES_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )

def api_get_social_blocks(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "200") or "200", 200, 1, 5000)
        return get_social_blocks(limit=limit)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_social_blocks_failed",
            code="API_DASHBOARD_SOCIAL_BLOCKS_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )

def api_get_validation(_parsed, _ctx=None):
    return get_validation_rows()

def api_get_size_policy(_parsed, _ctx=None):
    return get_size_policy()

def api_get_shadow_capital_scores(parsed, _ctx=None):
    try:
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "50") or "50", 50, 1, 5000)
        regime = str(qs.get("regime", "global") or "global").strip() or "global"
        return get_shadow_capital_scores(limit=limit, regime=regime)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_dashboard_reads_shadow_capital_scores_failed",
            code="API_DASHBOARD_SHADOW_CAPITAL_SCORES_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_dashboard_reads",
            ctx=_ctx,
        )


def api_post_shadow_capital_run(parsed, body, _ctx=None):
    qs = _qs(parsed)
    window_s = qs.get("window_s", "")
    regime = qs.get("regime", "")

    if isinstance(body, dict):
        if not window_s:
            window_s = body.get("window_s", "")
        if not regime:
            regime = body.get("regime", "")

    try:
        window_s = int(window_s or 86400)
    except Exception:
        window_s = 86400

    regime = str(regime or "global").strip() or "global"

    return run_shadow_capital_scores(window_s=window_s, regime=regime)
