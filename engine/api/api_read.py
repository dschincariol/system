"""
FILE: api_read.py

HTTP/API handlers for read endpoints.
"""

"""
Read-only API layer.

All DB reads previously in dashboard_server.py now live here.
No runtime orchestration.
No supervisor logic.
Pure read-only DB queries.
"""

import json
import time
import logging
from typing import Any
from engine.api.sql_identifiers import require_allowed_table_name, sql_identifier
from engine.runtime.storage import connect_ro as _db_connect
from engine.runtime.state_cache import cache_get_or_load
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.telemetry_read_router import fetch_provider_health_rows

LOG = get_logger("engine.api.api_read")
_WARNED_KEYS = set()


def _warn_nonfatal(code: str, error: BaseException, *, warn_key: str | None = None, **extra):
    if warn_key and warn_key in _WARNED_KEYS:
        return
    log_failure(
        LOG,
        event="api_read_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.api.api_read",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_KEYS.add(warn_key)


def _table_exists(con, name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "API_READ_TABLE_EXISTS_FAILED",
            e,
            warn_key=f"table_exists:{name}",
            table=str(name),
        )
        return False


def _table_cols(con, name: str):
    try:
        table_name = require_allowed_table_name(name)
        rows = con.execute(
            f"PRAGMA table_info({sql_identifier(table_name)})"
        ).fetchall() or []
        return [str(r[1]) for r in rows]
    except Exception as e:
        _warn_nonfatal(
            "API_READ_TABLE_COLS_FAILED",
            e,
            warn_key=f"table_cols:{name}",
            table=str(name),
        )
        return []


def _extract_confidence_metrics_from_explain(explain: dict) -> dict:
    explain = explain if isinstance(explain, dict) else {}
    engine_blob: dict[str, Any] = dict(explain.get("confidence_engine") or {}) if isinstance(explain.get("confidence_engine"), dict) else {}
    model_intent: dict[str, Any] = dict(explain.get("model_intent") or {}) if isinstance(explain.get("model_intent"), dict) else {}

    def _pick_float(*values):
        for value in values:
            try:
                out = float(value)
            except Exception as e:
                _warn_nonfatal(
                    "API_READ_PICK_FLOAT_FAILED",
                    e,
                    warn_key=f"pick_float:{type(value).__name__}:{value!r}",
                    value_type=type(value).__name__,
                )
                continue
            if out == out:
                return float(out)
        return None

    return {
        "confidence_raw": _pick_float(
            engine_blob.get("raw_confidence"),
            explain.get("confidence_raw"),
            model_intent.get("confidence_raw"),
        ),
        "prediction_strength": _pick_float(
            engine_blob.get("prediction_strength"),
            explain.get("prediction_strength"),
            model_intent.get("prediction_strength"),
            explain.get("score"),
        ),
    }


def _load_alert_state_maps(con, alert_ids: list[int]) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
    ids = []
    for alert_id in alert_ids or []:
        try:
            ids.append(int(alert_id))
        except Exception as e:
            _warn_nonfatal(
                "API_READ_ALERT_ID_INPUT_PARSE_FAILED",
                e,
                warn_key="api_read_alert_id_input_parse_failed",
                alert_id=str(alert_id),
            )
            continue
    ids = list(dict.fromkeys(ids))
    if not ids:
        return {}, {}

    placeholders = ",".join("?" for _ in ids)
    ack_map: dict[int, dict[str, Any]] = {}
    resolution_map: dict[int, dict[str, Any]] = {}

    if _table_exists(con, "alert_acks"):
        try:
            rows = con.execute(
                f"""
                SELECT alert_id, acked_ts_ms, acked_by, source
                FROM alert_acks
                WHERE alert_id IN ({placeholders})
                """,
                tuple(ids),
            ).fetchall() or []
            for row in rows:
                try:
                    alert_id = int(row[0])
                except Exception as e:
                    _warn_nonfatal(
                        "API_READ_ALERT_ACK_ROW_PARSE_FAILED",
                        e,
                        warn_key="api_read_alert_ack_row_parse_failed",
                        row_preview=str(row)[:120],
                    )
                    continue
                ack_map[alert_id] = {
                    "acked": True,
                    "acked_ts_ms": int(row[1] or 0) if row[1] is not None else None,
                    "acked_by": str(row[2] or ""),
                    "ack_source": str(row[3] or ""),
                }
        except Exception as e:
            _warn_nonfatal(
                "API_READ_ALERT_ACKS_FAILED",
                e,
                warn_key="api_read_alert_acks_failed",
            )

    if _table_exists(con, "alert_resolutions"):
        try:
            rows = con.execute(
                f"""
                SELECT alert_id, resolved_ts_ms, resolved_by, reason, source
                FROM alert_resolutions
                WHERE alert_id IN ({placeholders})
                """,
                tuple(ids),
            ).fetchall() or []
            for row in rows:
                try:
                    alert_id = int(row[0])
                except Exception as e:
                    _warn_nonfatal(
                        "API_READ_ALERT_RESOLUTION_ROW_PARSE_FAILED",
                        e,
                        warn_key="api_read_alert_resolution_row_parse_failed",
                        row_preview=str(row)[:120],
                    )
                    continue
                resolution_map[alert_id] = {
                    "resolved": True,
                    "status": "resolved",
                    "resolved_ts_ms": int(row[1] or 0) if row[1] is not None else None,
                    "resolved_by": str(row[2] or ""),
                    "resolved_reason": str(row[3] or ""),
                    "resolve_source": str(row[4] or ""),
                }
        except Exception as e:
            _warn_nonfatal(
                "API_READ_ALERT_RESOLUTIONS_FAILED",
                e,
                warn_key="api_read_alert_resolutions_failed",
            )

    return ack_map, resolution_map


# ------------------------------
# ALERTS
# ------------------------------

def get_alerts():
    def _load():
        con = _db_connect()
        try:
            # Read endpoints use short-lived cached snapshots so the dashboard
            # does not hammer SQLite on every poll.
            rows = con.execute("""
                SELECT
                  id, ts_ms, severity, symbol, horizon_s,
                  expected_z, confidence, event_title, rule_id, explain_json
                FROM alerts
                ORDER BY ts_ms DESC
                LIMIT 50
            """).fetchall()
            ack_map, resolution_map = _load_alert_state_maps(
                con,
                [int(r[0]) for r in rows if r and r[0] is not None],
            )

            crit_n = 0
            high_n = 0
            warn_n = 0

            out_rows = []
            for r in rows:
                sev = r[2]
                if sev == "CRIT":
                    crit_n += 1
                elif sev == "HIGH":
                    high_n += 1
                elif sev == "WARN":
                    warn_n += 1

                explain_json = r[9] or "{}"
                try:
                    explain = json.loads(explain_json)
                    if not isinstance(explain, dict):
                        explain = {}
                except Exception:
                    explain = {}
                conf_metrics = _extract_confidence_metrics_from_explain(explain)

                try:
                    alert_id = int(r[0])
                except Exception:
                    alert_id = 0
                alert_row = {
                    "id": r[0],
                    "ts_ms": r[1],
                    "severity": sev,
                    "symbol": r[3],
                    "horizon_s": r[4],
                    "expected_z": r[5],
                    "confidence": r[6],
                    "confidence_raw": conf_metrics.get("confidence_raw"),
                    "prediction_strength": conf_metrics.get("prediction_strength"),
                    "event_title": r[7],
                    "rule_id": r[8],
                    "explain_json": explain_json,
                    "status": "active",
                    "acked": False,
                    "acked_by": "",
                    "acked_ts_ms": None,
                    "resolved": False,
                    "resolved_by": "",
                    "resolved_ts_ms": None,
                    "resolved_reason": "",
                }
                ack_state = ack_map.get(alert_id, {})
                if ack_state:
                    alert_row.update(ack_state)
                resolution_state = resolution_map.get(alert_id, {})
                if resolution_state:
                    alert_row.update(resolution_state)
                out_rows.append(alert_row)

            return {
                "ok": True,
                "rows": out_rows,
                "summary": {
                    "crit": crit_n,
                    "high": high_n,
                    "warn": warn_n,
                    "total": len(out_rows),
                },
            }
        finally:
            con.close()

    return cache_get_or_load("api_read", "alerts", _load, ttl_s=0.75)


# ------------------------------
# EXECUTION METRICS
# ------------------------------

def get_execution_metrics(model_id: str = ""):
    model_filter = str(model_id or "").strip()
    def _load():
        con = _db_connect()
        try:
            if _table_exists(con, "execution_fills"):
                # Prefer canonical execution_fills/execution_orders when they
                # exist, but keep a legacy fallback below for older schemas.
                if model_filter:
                    row = con.execute(
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
                        WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                        """,
                        (str(model_filter),),
                    ).fetchone()

                    detail_rows = con.execute(
                        """
                        SELECT
                          f.symbol,
                          f.expected_px,
                          f.fill_px,
                          f.slippage_bps,
                          f.fill_latency_ms,
                          f.spread_bps,
                          f.fees,
                          f.extra_json,
                          o.extra_json
                        FROM execution_fills f
                        LEFT JOIN execution_orders o
                          ON o.client_order_id = f.client_order_id
                        WHERE COALESCE(NULLIF(TRIM(f.model_id), ''), 'baseline') = ?
                        ORDER BY f.fill_ts_ms DESC, f.id DESC
                        LIMIT 5000
                        """,
                        (str(model_filter),),
                    ).fetchall()
                else:
                    row = con.execute(
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
                        """
                    ).fetchone()

                    detail_rows = con.execute(
                        """
                        SELECT
                          f.symbol,
                          f.expected_px,
                          f.fill_px,
                          f.slippage_bps,
                          f.fill_latency_ms,
                          f.spread_bps,
                          f.fees,
                          f.extra_json,
                          o.extra_json
                        FROM execution_fills f
                        LEFT JOIN execution_orders o
                          ON o.client_order_id = f.client_order_id
                        ORDER BY f.fill_ts_ms DESC, f.id DESC
                        LIMIT 5000
                        """
                    ).fetchall()

                by_strategy = {}
                for sym, expected_px, fill_px, slippage_bps, fill_latency_ms, spread_bps, fees, fill_extra_json, order_extra_json in detail_rows or []:
                    fill_extra = {}
                    order_extra = {}

                    try:
                        fill_extra = json.loads(fill_extra_json or "{}")
                        if not isinstance(fill_extra, dict):
                            fill_extra = {}
                    except Exception:
                        fill_extra = {}

                    try:
                        order_extra = json.loads(order_extra_json or "{}")
                        if not isinstance(order_extra, dict):
                            order_extra = {}
                    except Exception:
                        order_extra = {}

                    strategy_name = (
                        fill_extra.get("strategy_name")
                        or order_extra.get("strategy_name")
                        or (fill_extra.get("strategy") or {}).get("name") if isinstance(fill_extra.get("strategy"), dict) else None
                        or (order_extra.get("strategy") or {}).get("name") if isinstance(order_extra.get("strategy"), dict) else None
                        or ((fill_extra.get("explain") or {}).get("strategy") or {}).get("name") if isinstance((fill_extra.get("explain") or {}).get("strategy"), dict) else None
                        or ((order_extra.get("explain") or {}).get("strategy") or {}).get("name") if isinstance((order_extra.get("explain") or {}).get("strategy"), dict) else None
                        or "UNKNOWN"
                    )
                    strategy_name = str(strategy_name).strip() or "UNKNOWN"

                    bucket = by_strategy.setdefault(
                        strategy_name,
                        {
                            "strategy_name": strategy_name,
                            "n_fills": 0,
                            "total_fees": 0.0,
                            "_slippage_sum": 0.0,
                            "_slippage_n": 0,
                            "_latency_sum": 0.0,
                            "_latency_n": 0,
                            "_spread_sum": 0.0,
                            "_spread_n": 0,
                            "_expected_sum": 0.0,
                            "_expected_n": 0,
                            "_actual_sum": 0.0,
                            "_actual_n": 0,
                            "symbols": set(),
                        },
                    )

                    bucket["n_fills"] += 1
                    bucket["total_fees"] += float(fees or 0.0)
                    if sym:
                        bucket["symbols"].add(str(sym))

                    if slippage_bps is not None:
                        bucket["_slippage_sum"] += float(slippage_bps)
                        bucket["_slippage_n"] += 1
                    if fill_latency_ms is not None:
                        bucket["_latency_sum"] += float(fill_latency_ms)
                        bucket["_latency_n"] += 1
                    if spread_bps is not None:
                        bucket["_spread_sum"] += float(spread_bps)
                        bucket["_spread_n"] += 1
                    if expected_px is not None:
                        bucket["_expected_sum"] += float(expected_px)
                        bucket["_expected_n"] += 1
                    if fill_px is not None:
                        bucket["_actual_sum"] += float(fill_px)
                        bucket["_actual_n"] += 1

                strategy_rows = []
                for _, bucket in sorted(by_strategy.items(), key=lambda kv: (-kv[1]["n_fills"], kv[0])):
                    strategy_rows.append(
                        {
                            "strategy_name": bucket["strategy_name"],
                            "n_fills": int(bucket["n_fills"]),
                            "symbols": sorted(bucket["symbols"]),
                            "avg_slippage_bps": (bucket["_slippage_sum"] / bucket["_slippage_n"]) if bucket["_slippage_n"] else 0.0,
                            "avg_time_to_fill_ms": (bucket["_latency_sum"] / bucket["_latency_n"]) if bucket["_latency_n"] else 0.0,
                            "avg_spread_at_entry_bps": (bucket["_spread_sum"] / bucket["_spread_n"]) if bucket["_spread_n"] else 0.0,
                            "avg_expected_fill_price": (bucket["_expected_sum"] / bucket["_expected_n"]) if bucket["_expected_n"] else 0.0,
                            "avg_actual_fill_price": (bucket["_actual_sum"] / bucket["_actual_n"]) if bucket["_actual_n"] else 0.0,
                            "total_fees": float(bucket["total_fees"]),
                        }
                    )

                return {
                    "ok": True,
                    "model_id": (str(model_filter) if model_filter else None),
                    "n_fills": int((row or [0])[0] or 0),
                    "total_slippage_bps": float((row or [0, 0])[1] or 0.0),
                    "total_fees": float((row or [0, 0, 0])[2] or 0.0),
                    "avg_slippage_bps": float((row or [0, 0, 0, 0])[3] or 0.0),
                    "avg_time_to_fill_ms": float((row or [0, 0, 0, 0, 0])[4] or 0.0),
                    "avg_spread_at_entry_bps": float((row or [0, 0, 0, 0, 0, 0])[5] or 0.0),
                    "avg_expected_fill_price": float((row or [0, 0, 0, 0, 0, 0, 0])[6] or 0.0),
                    "avg_actual_fill_price": float((row or [0, 0, 0, 0, 0, 0, 0, 0])[7] or 0.0),
                    "by_strategy": strategy_rows,
                    "total_slippage": float((row or [0, 0])[1] or 0.0),
                    "total_cost": float((row or [0, 0, 0])[2] or 0.0),
                    "avg_slippage": float((row or [0, 0, 0, 0])[3] or 0.0),
                }

            table = (
                require_allowed_table_name("broker_fills_v2")
                if _table_exists(con, "broker_fills_v2")
                else require_allowed_table_name("broker_fills")
            )
            table_sql = sql_identifier(table)

            row = con.execute(
                f"""
                SELECT
                  COUNT(*),
                  SUM(slippage),
                  SUM(fees),
                  SUM(total_cost),
                  AVG(slippage)
                FROM {table_sql}
                """
            ).fetchone()

            return {
                "ok": True,
                "model_id": (str(model_filter) if model_filter else None),
                "n_fills": int(row[0] or 0),
                "total_slippage": float(row[1] or 0.0),
                "total_fees": float(row[2] or 0.0),
                "total_cost": float(row[3] or 0.0),
                "avg_slippage": float(row[4] or 0.0),
            }
        finally:
            con.close()

    return cache_get_or_load("api_read", f"execution_metrics:{model_filter}", _load, ttl_s=1.0)


def get_feed_status():
    def _load():
        con = _db_connect()
        now_ms = int(time.time() * 1000)

        try:
            providers = []
            for row in fetch_provider_health_rows():
                ts_ms = row.get("ts_ms")
                age_s = None if ts_ms is None else round((now_ms - int(ts_ms)) / 1000.0, 1)
                providers.append(
                    {
                        "provider": str(row.get("provider") or ""),
                        "ok": bool(row.get("ok")),
                        "ts_ms": (int(ts_ms) if ts_ms is not None else None),
                        "age_s": age_s,
                        "latency_ms": (
                            float(row.get("latency_ms"))
                            if row.get("latency_ms") is not None
                            else None
                        ),
                        "n_symbols": (
                            int(row.get("n_symbols"))
                            if row.get("n_symbols") is not None
                            else None
                        ),
                        "error": (
                            str(row.get("error"))
                            if row.get("error") is not None
                            else None
                        ),
                    }
                )

            feed_jobs = []
            if _table_exists(con, "job_locks"):
                rows = con.execute(
                    """
                    SELECT job_name, heartbeat_ts_ms, owner, pid
                    FROM job_locks
                    WHERE job_name IN ('provider_monitor', 'poll_prices', 'stream_prices_polygon_ws')
                    ORDER BY job_name ASC
                    """
                ).fetchall() or []

                for job_name, heartbeat_ts_ms, owner, pid in rows:
                    age_s = None if heartbeat_ts_ms is None else round((now_ms - int(heartbeat_ts_ms)) / 1000.0, 1)
                    feed_jobs.append(
                        {
                            "job_name": str(job_name or ""),
                            "heartbeat_ts_ms": (int(heartbeat_ts_ms) if heartbeat_ts_ms is not None else None),
                            "heartbeat_age_s": age_s,
                            "owner": str(owner or ""),
                            "pid": (int(pid) if pid is not None else None),
                        }
                    )

            healthy_providers = len([p for p in providers if p.get("ok")])
            stale_providers = len([p for p in providers if (p.get("age_s") is not None and p.get("age_s") > 120.0)])

            return {
                "ok": True,
                "ts_ms": now_ms,
                "summary": {
                    "providers_total": len(providers),
                    "providers_healthy": healthy_providers,
                    "providers_stale": stale_providers,
                    "jobs_total": len(feed_jobs),
                },
                "providers": providers,
                "jobs": feed_jobs,
            }
        finally:
            con.close()

    return cache_get_or_load("api_read", "feed_status", _load, ttl_s=1.0)


def get_execution_stats(model_id: str = ""):
    model_filter = str(model_id or "").strip()
    def _load():
        con = _db_connect()
        now_ms = int(time.time() * 1000)

        try:
            order_table = "execution_orders" if _table_exists(con, "execution_orders") else None

            fills_table = None
            fills_ts_col = None
            if _table_exists(con, "execution_fills"):
                fills_table = require_allowed_table_name("execution_fills")
                fills_ts_col = "fill_ts_ms"
            elif _table_exists(con, "broker_fills_v2"):
                fills_table = require_allowed_table_name("broker_fills_v2")
                fills_ts_col = "ts_ms" if "ts_ms" in _table_cols(con, "broker_fills_v2") else "fill_ts_ms"
            elif _table_exists(con, "broker_fills"):
                fills_table = require_allowed_table_name("broker_fills")
                fills_ts_col = "ts_ms" if "ts_ms" in _table_cols(con, "broker_fills") else "fill_ts_ms"

            metrics_table = "execution_metrics" if _table_exists(con, "execution_metrics") else None

            orders_total = 0
            orders_24h = 0
            last_order_ts_ms = None
            order_status = {}

            if order_table:
                if model_filter:
                    row = con.execute(
                        """
                        SELECT COUNT(*), MAX(submit_ts_ms)
                        FROM execution_orders
                        WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                        """,
                        (str(model_filter),),
                    ).fetchone()
                else:
                    row = con.execute(
                        """
                        SELECT COUNT(*), MAX(submit_ts_ms)
                        FROM execution_orders
                        """
                    ).fetchone()
                orders_total = int((row or [0, None])[0] or 0)
                last_order_ts_ms = (int((row or [0, None])[1]) if (row and row[1] is not None) else None)

                if model_filter:
                    row = con.execute(
                        """
                        SELECT COUNT(*)
                        FROM execution_orders
                        WHERE submit_ts_ms >= ?
                          AND COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                        """,
                        (int(now_ms - 86400_000), str(model_filter)),
                    ).fetchone()
                else:
                    row = con.execute(
                        """
                        SELECT COUNT(*)
                        FROM execution_orders
                        WHERE submit_ts_ms >= ?
                        """,
                        (int(now_ms - 86400_000),),
                    ).fetchone()
                orders_24h = int((row or [0])[0] or 0)

                if model_filter:
                    rows = con.execute(
                        """
                        SELECT status, COUNT(*)
                        FROM execution_orders
                        WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                        GROUP BY status
                        ORDER BY COUNT(*) DESC
                        """,
                        (str(model_filter),),
                    ).fetchall() or []
                else:
                    rows = con.execute(
                        """
                        SELECT status, COUNT(*)
                        FROM execution_orders
                        GROUP BY status
                        ORDER BY COUNT(*) DESC
                        """
                    ).fetchall() or []
                order_status = {
                    str(status or "unknown"): int(n or 0)
                    for status, n in rows
                }

            fills_total = 0
            fills_24h = 0
            last_fill_ts_ms = None

            if fills_table and fills_ts_col:
                fills_sql = sql_identifier(fills_table)
                if model_filter and fills_table == "execution_fills":
                    row = con.execute(
                        f"""
                        SELECT COUNT(*), MAX({fills_ts_col})
                        FROM {fills_sql}
                        WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                        """,
                        (str(model_filter),),
                    ).fetchone()
                else:
                    row = con.execute(
                        f"""
                        SELECT COUNT(*), MAX({fills_ts_col})
                        FROM {fills_sql}
                        """
                    ).fetchone()
                fills_total = int((row or [0, None])[0] or 0)
                last_fill_ts_ms = (int((row or [0, None])[1]) if (row and row[1] is not None) else None)

                if model_filter and fills_table == "execution_fills":
                    row = con.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {fills_sql}
                        WHERE {fills_ts_col} >= ?
                          AND COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                        """,
                        (int(now_ms - 86400_000), str(model_filter)),
                    ).fetchone()
                else:
                    row = con.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {fills_sql}
                        WHERE {fills_ts_col} >= ?
                        """,
                        (int(now_ms - 86400_000),),
                    ).fetchone()
                fills_24h = int((row or [0])[0] or 0)

            metrics_summary = {
                "avg_slippage_bps": None,
                "sum_fees": None,
                "sum_m2m_pnl": None,
                "sum_realized_pnl": None,
                "sum_unrealized_pnl": None,
                "sum_total_pnl": None,
            }

            if metrics_table:
                if model_filter and _table_exists(con, "execution_orders"):
                    row = con.execute(
                        """
                        SELECT AVG(m.slippage_bps), SUM(m.fees), SUM(m.m2m_pnl)
                        FROM execution_metrics m
                        JOIN execution_orders o
                          ON o.client_order_id = m.client_order_id
                        WHERE COALESCE(NULLIF(TRIM(o.model_id), ''), 'baseline') = ?
                        """,
                        (str(model_filter),),
                    ).fetchone()
                else:
                    row = con.execute(
                        """
                        SELECT AVG(slippage_bps), SUM(fees), SUM(m2m_pnl)
                        FROM execution_metrics
                        """
                    ).fetchone()
                metrics_summary: dict[str, float | None] = {
                    "avg_slippage_bps": (float(row[0]) if row and row[0] is not None else None),
                    "sum_fees": (float(row[1]) if row and row[1] is not None else None),
                    "sum_m2m_pnl": (float(row[2]) if row and row[2] is not None else None),
                }
            if _table_exists(con, "pnl_attribution"):
                if model_filter:
                    normalized_model_filter = str(model_filter or "").strip() or "baseline"
                    if normalized_model_filter == "baseline":
                        model_filter_sql = "(model_id = ? OR NULLIF(TRIM(model_id), '') IS NULL)"
                    else:
                        model_filter_sql = "model_id = ?"
                    row = con.execute(
                        f"""
                        SELECT
                          COALESCE(SUM(COALESCE(realized_pnl, 0.0)), 0.0),
                          COALESCE(SUM(COALESCE(unrealized_pnl, 0.0)), 0.0),
                          COALESCE(SUM(COALESCE(realized_pnl, 0.0) + COALESCE(unrealized_pnl, 0.0) - COALESCE(fees, 0.0) - COALESCE(json_extract(extra_json, '$.slippage_cost'), 0.0)), 0.0)
                        FROM pnl_attribution
                        WHERE ts_ms = (
                          SELECT MAX(ts_ms)
                          FROM pnl_attribution
                          WHERE {model_filter_sql}
                        )
                          AND {model_filter_sql}
                        """,
                        (normalized_model_filter, normalized_model_filter),
                    ).fetchone()
                else:
                    row = con.execute(
                        """
                        SELECT
                          COALESCE(SUM(COALESCE(realized_pnl, 0.0)), 0.0),
                          COALESCE(SUM(COALESCE(unrealized_pnl, 0.0)), 0.0),
                          COALESCE(SUM(COALESCE(realized_pnl, 0.0) + COALESCE(unrealized_pnl, 0.0) - COALESCE(fees, 0.0) - COALESCE(json_extract(extra_json, '$.slippage_cost'), 0.0)), 0.0)
                        FROM pnl_attribution
                        WHERE ts_ms = (SELECT MAX(ts_ms) FROM pnl_attribution)
                        """
                    ).fetchone()
                metrics_summary["sum_realized_pnl"] = (
                    float(row[0]) if row and row[0] is not None else None
                )
                metrics_summary["sum_unrealized_pnl"] = (
                    float(row[1]) if row and row[1] is not None else None
                )
                metrics_summary["sum_total_pnl"] = (
                    float(row[2]) if row and row[2] is not None else None
                )

            return {
                "ok": True,
                "model_id": (str(model_filter) if model_filter else None),
                "ts_ms": now_ms,
                "orders": {
                    "table": order_table,
                    "total": orders_total,
                    "last_24h": orders_24h,
                    "last_order_ts_ms": last_order_ts_ms,
                    "last_order_age_s": (round((now_ms - int(last_order_ts_ms)) / 1000.0, 1) if last_order_ts_ms else None),
                    "status": order_status,
                },
                "fills": {
                    "table": fills_table,
                    "total": fills_total,
                    "last_24h": fills_24h,
                    "last_fill_ts_ms": last_fill_ts_ms,
                    "last_fill_age_s": (round((now_ms - int(last_fill_ts_ms)) / 1000.0, 1) if last_fill_ts_ms else None),
                },
                "metrics": metrics_summary,
            }
        finally:
            con.close()

    return cache_get_or_load("api_read", f"execution_stats:{model_filter}", _load, ttl_s=1.0)


# ------------------------------
# MODEL REGISTRY
# ------------------------------

def get_model_registry(limit: int = 50):
    limit = max(1, min(500, int(limit or 50)))

    con = _db_connect()
    try:
        rows = con.execute(
            """
            SELECT model_name, model_kind, model_ts_ms, stage, regime, metrics_json, created_ts_ms, note
            FROM model_registry
            ORDER BY created_ts_ms DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        out = []
        for r in rows:
            try:
                metrics = json.loads(r[5] or "{}")
            except Exception:
                metrics = {}
            out.append({
                "model_name": str(r[0] or ""),
                "model_kind": r[1],
                "model_ts_ms": int(r[2]),
                "stage": r[3],
                "regime": str(r[4] or "global"),
                "metrics": metrics,
                "created_ts_ms": int(r[6]),
                "note": r[7],
            })

        champion = next((row for row in out if str(row.get("stage") or "") == "champion"), None)
        challenger = next((row for row in out if str(row.get("stage") or "") == "challenger"), None)
        return {
            "ok": True,
            "rows": out,
            "history": out,
            "champion": champion or {},
            "challenger": challenger or {},
        }
    finally:
        con.close()


def get_model_lifecycle_summary(limit: int = 6):
    try:
        from engine.strategy.model_lifecycle import get_lifecycle_summary

        return get_lifecycle_summary(limit=max(1, min(50, int(limit or 6))))
    except Exception as e:
        _warn_nonfatal(
            "API_READ_MODEL_LIFECYCLE_SUMMARY_FAILED",
            e,
            warn_key="model_lifecycle_summary",
            limit=int(limit or 6),
        )
        return {"ok": False, "error": str(e), "families": {}}

# ============================================================
# ADDITIONAL READ ENDPOINTS (moved from dashboard_server)
# ============================================================

def get_confidence_mass():
    con = _db_connect()
    try:
        try:
            rows = con.execute(
                """
                SELECT confidence
                FROM predictions
                ORDER BY ts_ms DESC
                LIMIT 2000
                """
            ).fetchall()
        except Exception:
            rows = []

        vals = []
        for r in rows:
            try:
                vals.append(float(r[0]))
            except Exception as e:
                _warn_nonfatal("API_READ_CONFIDENCE_PARSE_FAILED", e, warn_key="confidence_parse")

        bins = [0] * 10
        for v in vals:
            v = max(0.0, min(1.0, float(v)))
            idx = int(min(9, max(0, int(v * 10.0))))
            bins[idx] += 1

        return {
            "ok": True,
            "n": int(len(vals)),
            "bins": [
                {"lo": i / 10.0, "hi": (i + 1) / 10.0, "count": int(bins[i])}
                for i in range(10)
            ],
        }
    finally:
        con.close()


def get_temporal_eval(limit: int = 50):
    limit = max(1, min(5000, int(limit or 50)))
    con = _db_connect()
    try:
        if _table_exists(con, "temporal_model_eval"):
            rows = con.execute(
                """
                SELECT horizon_s, n_eval AS n, rmse, directional_acc, ts_ms
                FROM temporal_model_eval
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        elif _table_exists(con, "temporal_eval"):
            rows = con.execute(
                """
                SELECT horizon_s, n, rmse, directional_acc, ts_ms
                FROM temporal_eval
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        else:
            rows = []

        return {
            "ok": True,
            "rows": [
                {
                    "horizon_s": int(r[0] or 0),
                    "n": int(r[1] or 0),
                    "rmse": float(r[2] or 0.0),
                    "directional_acc": float(r[3] or 0.0),
                    "ts_ms": int(r[4] or 0),
                }
                for r in rows
            ],
        }
    finally:
        con.close()


def get_embed_model_eval(limit: int = 500):
    limit = max(1, min(5000, int(limit or 500)))
    con = _db_connect()
    try:
        try:
            rows = con.execute(
                """
                SELECT key_type, key, horizon_s, model_kind, ts_ms,
                       n_train, n_eval, rmse, spearman, directional_acc
                FROM embed_model_eval
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        except Exception:
            rows = []

        out = []
        for r in rows:
            out.append({
                "key_type": str(r[0] or ""),
                "key": str(r[1] or ""),
                "horizon_s": int(r[2] or 0),
                "model_kind": str(r[3] or ""),
                "ts_ms": int(r[4] or 0),
                "n_train": int(r[5] or 0),
                "n_eval": int(r[6] or 0),
                "rmse": float(r[7] or 0.0),
                "spearman": float(r[8] or 0.0),
                "directional_acc": float(r[9] or 0.0),
            })

        return {"ok": True, "rows": out}
    finally:
        con.close()


def get_embed_conf_calib(horizon_s: int, model_kind: str, limit: int = 200):
    limit = max(2, min(5000, int(limit or 200)))
    hs = int(horizon_s or 0)
    mk = str(model_kind or "").strip().lower()
    if mk not in ("ridge", "mlp"):
        mk = "ridge"

    con = _db_connect()
    try:
        try:
            row = con.execute(
                """
                SELECT ts_ms, conf_k, n_points, x_json, y_json
                FROM embed_conf_calib
                WHERE horizon_s=? AND model_kind=?
                """,
                (hs, mk),
            ).fetchone()
        except Exception:
            row = None

        if not row:
            return {"ok": True, "curve": None}

        ts_ms, conf_k, n_points, xj, yj = row
        xs = json.loads(xj or "[]")
        ys = json.loads(yj or "[]")

        curve = []
        for i in range(min(len(xs), len(ys))):
            curve.append({"x": float(xs[i]), "y": float(ys[i])})

        return {
            "ok": True,
            "horizon_s": hs,
            "model_kind": mk,
            "ts_ms": int(ts_ms or 0),
            "conf_k": float(conf_k or 0.0),
            "n_points": int(n_points or len(curve)),
            "curve": curve,
        }
    finally:
        con.close()
