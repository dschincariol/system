# CREATE NEW FILE: dev_core/dashboard_weather_widgets.py
"""
Dashboard helper queries for weather + impact widgets.

This file is backend-agnostic: it only reads SQLite and returns JSON-ready dicts.
You can import it from whatever dashboard server you already have.

Functions:
- get_weather_snapshot_for_symbol(symbol, ts_ms)
- get_weather_effect_summary(ts_ms=None)
- get_weather_alert_summary(ts_ms=None)
"""

import logging
import time
import json
from typing import Dict, Any, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.data.weather_features import get_weather_feature_snapshot

LOG = get_logger("engine.runtime.dashboard_weather_widgets")


def _utc_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event="dashboard_weather_widgets_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.dashboard_weather_widgets",
        extra=extra or None,
        persist=False,
    )


def _table_columns(con, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
        return {str(row[1] or "").strip() for row in rows}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_WEATHER_WIDGETS_TABLE_COLUMNS_FAILED", e, table=str(table_name))
        return set()


def _optional_metric(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def get_weather_snapshot_for_symbol(symbol: str, ts_ms: Optional[int] = None) -> Dict[str, Any]:
    if ts_ms is None:
        ts_ms = _utc_ms()
    # Keep the response JSON-ready so the dashboard layer can forward it
    # directly without additional translation.
    wx = get_weather_feature_snapshot(symbol=str(symbol), ts_ms=int(ts_ms)) or {}
    return {
        "ts_ms": int(ts_ms),
        "symbol": str(symbol).upper(),
        "wx": dict(wx),
    }


def get_weather_effect_summary(ts_ms: Optional[int] = None) -> Dict[str, Any]:
    if ts_ms is None:
        ts_ms = _utc_ms()

    con = connect()
    try:
        columns = _table_columns(con, "model_weather_effect")
        base_spearman_expr = "base_spearman" if "base_spearman" in columns else "NULL AS base_spearman"
        wx_spearman_expr = "wx_spearman" if "wx_spearman" in columns else "NULL AS wx_spearman"
        spearman_delta_expr = "spearman_delta" if "spearman_delta" in columns else "NULL AS spearman_delta"
        rows = con.execute(
            f"""
            SELECT horizon_s, ts_ms,
                   base_rmse, wx_rmse, rmse_delta,
                   {base_spearman_expr}, {wx_spearman_expr}, {spearman_delta_expr},
                   n_eval
            FROM model_weather_effect
            WHERE key_type='global' AND key='global'
              AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 50
            """,
            (int(ts_ms),),
        ).fetchall() or []

        # Keep only the newest row per horizon so the widget reflects the
        # current weather-effect estimate rather than a historical series.
        # latest per horizon
        best = {}
        for r in rows:
            h = int(r[0])
            if h in best:
                continue
            best[h] = {
                "horizon_s": h,
                "ts_ms": int(r[1]),
                "base_rmse": float(r[2] or 0.0),
                "wx_rmse": float(r[3] or 0.0),
                "rmse_delta": float(r[4] or 0.0),
                "base_spearman": _optional_metric(r[5]),
                "wx_spearman": _optional_metric(r[6]),
                "spearman_delta": _optional_metric(r[7]),
                "n_eval": int(r[8] or 0),
            }

        out = [best[k] for k in sorted(best.keys())]
        return {
            "ts_ms": int(ts_ms),
            "series": out,
            "meta": {
                "ready": bool(out),
                "status": 200,
                "missing_columns": sorted(
                    col
                    for col in ("base_spearman", "wx_spearman", "spearman_delta")
                    if col not in columns
                ),
            },
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_WEATHER_WIDGETS_CLOSE_FAILED", e, operation="get_weather_effect_summary")


def get_weather_alert_summary(ts_ms: Optional[int] = None) -> Dict[str, Any]:
    if ts_ms is None:
        ts_ms = _utc_ms()

    con = connect()
    try:
        # Treat no-expiry alerts as active for a bounded window so widgets stay
        # informative without keeping stale alerts around forever.
        # active alerts (expires_ts==0 means unknown -> treat as active for 24h)
        min_issued = int(ts_ms) - 7 * 24 * 3600 * 1000
        rows = con.execute(
            """
            SELECT provider, alert_id, issued_ts, effective_ts, expires_ts,
                   event, severity, urgency, certainty,
                   area_desc, affected_regions, headline
            FROM weather_alerts
            WHERE issued_ts >= ?
            ORDER BY issued_ts DESC
            LIMIT 200
            """,
            (int(min_issued),),
        ).fetchall() or []

        out = []
        for r in rows:
            try:
                issued = int(r[2] or 0)
                expires = int(r[4] or 0)
                active = (issued <= int(ts_ms)) and (
                    (expires == 0 and int(ts_ms) <= issued + 24 * 3600 * 1000) or (expires > 0 and int(ts_ms) <= expires)
                )
                if not active:
                    continue

                out.append({
                    "provider": str(r[0] or ""),
                    "alert_id": str(r[1] or ""),
                    "issued_ts": issued,
                    "effective_ts": int(r[3] or 0),
                    "expires_ts": expires,
                    "event": str(r[5] or ""),
                    "severity": str(r[6] or ""),
                    "urgency": str(r[7] or ""),
                    "certainty": str(r[8] or ""),
                    "area_desc": str(r[9] or ""),
                    "affected_regions": json.loads(r[10]) if r[10] else [],
                    "headline": str(r[11] or ""),
                })
            except Exception:
                LOG.warning("dashboard_weather_widgets alert_row_parse_failed", exc_info=True)
                continue

        return {"ts_ms": int(ts_ms), "active": out}
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_WEATHER_WIDGETS_CLOSE_FAILED", e, operation="get_weather_alert_summary")
