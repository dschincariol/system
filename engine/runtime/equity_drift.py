"""
FILE: equity_drift.py

Runtime helpers for broker-vs-backtest equity drift.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect as _db_connect


LOG = get_logger("runtime.equity_drift")

EQUITY_DRIFT_WARN_PCT = 0.03
EQUITY_DRIFT_CRIT_PCT = 0.10
EQUITY_DRIFT_WARN_ABS = 2500.0
EQUITY_DRIFT_CRIT_ABS = 10000.0
EQUITY_DRIFT_SUSTAINED_WINDOW = 5
EQUITY_DRIFT_SUSTAINED_MIN_WARN = 3
EQUITY_DRIFT_SUSTAINED_MIN_CRIT = 2


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.equity_drift",
        extra=extra or None,
        persist=False,
    )


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("EQUITY_DRIFT_TABLE_EXISTS_FAILED", e, table_name=str(table_name))
        return False


def _table_columns(con, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row[1]) for row in (rows or []) if len(row) > 1 and row[1]}
    except Exception as e:
        _warn_nonfatal("EQUITY_DRIFT_TABLE_COLUMNS_FAILED", e, table_name=str(table_name))
        return set()


def classify_equity_diff(
    diff_pct: float | None,
    diff_abs: float | None,
    warn_pct: float = EQUITY_DRIFT_WARN_PCT,
    crit_pct: float = EQUITY_DRIFT_CRIT_PCT,
    warn_abs: float = EQUITY_DRIFT_WARN_ABS,
    crit_abs: float = EQUITY_DRIFT_CRIT_ABS,
):
    if diff_pct is None and diff_abs is None:
        return ("UNKNOWN", "no diff computed")

    ap = abs(float(diff_pct or 0.0))
    aa = abs(float(diff_abs or 0.0))

    if ap >= float(crit_pct) or aa >= float(crit_abs):
        return ("CRIT", "equity diff exceeds CRIT threshold")
    if ap >= float(warn_pct) or aa >= float(warn_abs):
        return ("WARN", "equity diff exceeds WARN threshold")

    return ("OK", "equity diff within tolerance")


def detect_sustained_equity_drift(
    con,
    window: int,
    min_warn: int,
    min_crit: int,
):
    if not _table_exists(con, "equity_drift"):
        return None
    try:
        rows = con.execute(
            """
            SELECT level
            FROM equity_drift
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(window),),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "EQUITY_DRIFT_SUSTAINED_READ_FAILED",
            e,
            window=int(window),
            min_warn=int(min_warn),
            min_crit=int(min_crit),
        )
        return None

    if not rows:
        return None

    levels = [str(r[0] or "").strip().upper() for r in rows]
    crit_n = sum(1 for level in levels if level == "CRIT")
    warn_n = sum(1 for level in levels if level == "WARN")

    if crit_n >= int(min_crit):
        return "CRIT"
    if warn_n >= int(min_warn):
        return "WARN"
    return None


def get_latest_broker_equity(con) -> tuple[Optional[float], int]:
    if not _table_exists(con, "broker_account"):
        return (None, 0)

    cols = _table_columns(con, "broker_account")
    if "equity" not in cols:
        return (None, 0)

    order_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else None)
    if not order_col:
        return (None, 0)

    try:
        row = con.execute(
            f"""
            SELECT equity, {order_col}
            FROM broker_account
            ORDER BY {order_col} DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return (None, 0)
        return (float(row[0] or 0.0), int(row[1] or 0))
    except Exception as e:
        _warn_nonfatal("EQUITY_DRIFT_BROKER_EQUITY_READ_FAILED", e)
        return (None, 0)


def get_latest_backtest_equity(con) -> Optional[Dict[str, Any]]:
    if (not _table_exists(con, "portfolio_bt_runs")) or (not _table_exists(con, "portfolio_bt_points")):
        return None

    try:
        run_row = con.execute(
            """
            SELECT id, ts_ms
            FROM portfolio_bt_runs
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
        if not run_row:
            return None

        pt = con.execute(
            """
            SELECT ts_ms, equity
            FROM portfolio_bt_points
            WHERE run_id = ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (int(run_row[0]),),
        ).fetchone()
        if not pt:
            return None

        return {
            "run_id": int(run_row[0]),
            "run_ts_ms": int(run_row[1] or 0),
            "backtest_ts_ms": int(pt[0] or 0),
            "backtest_equity": float(pt[1] or 0.0),
        }
    except Exception as e:
        _warn_nonfatal("EQUITY_DRIFT_BACKTEST_EQUITY_READ_FAILED", e)
        return None


def build_equity_drift_row(
    *,
    ts_ms: int,
    broker_equity: float,
    backtest_reference: Dict[str, Any],
) -> Dict[str, Any]:
    bt_equity = float(backtest_reference.get("backtest_equity") or 0.0)
    diff_equity = float(broker_equity) - float(bt_equity)
    base = abs(float(bt_equity)) if abs(float(bt_equity)) > 1e-9 else 1.0
    diff_equity_pct = float(diff_equity) / float(base)
    level, reason = classify_equity_diff(diff_equity_pct, diff_equity)

    detail = {
        "backtest_run_id": int(backtest_reference.get("run_id") or 0),
        "backtest_ts_ms": int(backtest_reference.get("backtest_ts_ms") or 0),
        "backtest_run_ts_ms": int(backtest_reference.get("run_ts_ms") or 0),
    }
    return {
        "ts_ms": int(ts_ms),
        "broker_equity": float(broker_equity),
        "backtest_equity": float(bt_equity),
        "diff_equity": float(diff_equity),
        "diff_equity_pct": float(diff_equity_pct),
        "level": str(level),
        "reason": str(reason),
        "backtest_run_id": int(backtest_reference.get("run_id") or 0),
        "backtest_ts_ms": int(backtest_reference.get("backtest_ts_ms") or 0),
        "detail_json": json.dumps(detail, separators=(",", ":"), sort_keys=True),
    }


def get_current_equity_drift(con) -> Dict[str, Any]:
    broker_equity, broker_ts_ms = get_latest_broker_equity(con)
    backtest_reference = get_latest_backtest_equity(con)

    if broker_equity is None or not backtest_reference:
        return {
            "ok": True,
            "resolved": False,
            "acked": False,
            "equity_diff_level": "UNKNOWN",
            "reason": "missing_broker_or_backtest_equity",
            "diff_equity": None,
            "diff_equity_pct": None,
            "broker_equity": broker_equity,
            "backtest_equity": (
                None if not backtest_reference else float(backtest_reference.get("backtest_equity") or 0.0)
            ),
            "broker_ts_ms": int(broker_ts_ms or 0),
            "backtest_ts_ms": (
                0 if not backtest_reference else int(backtest_reference.get("backtest_ts_ms") or 0)
            ),
        }

    row = build_equity_drift_row(
        ts_ms=max(int(broker_ts_ms or 0), int(backtest_reference.get("backtest_ts_ms") or 0)),
        broker_equity=float(broker_equity),
        backtest_reference=backtest_reference,
    )
    return {
        "ok": True,
        "resolved": False,
        "acked": False,
        "equity_diff_level": str(row["level"]),
        "reason": str(row["reason"]),
        "diff_equity": float(row["diff_equity"]),
        "diff_equity_pct": float(row["diff_equity_pct"]),
        "broker_equity": float(row["broker_equity"]),
        "backtest_equity": float(row["backtest_equity"]),
        "broker_ts_ms": int(broker_ts_ms or 0),
        "backtest_ts_ms": int(row["backtest_ts_ms"] or 0),
    }


def _latest_equity_drift_row(con) -> Optional[Dict[str, Any]]:
    if not _table_exists(con, "equity_drift"):
        return None
    try:
        row = con.execute(
            """
            SELECT
                ts_ms,
                broker_equity,
                backtest_equity,
                diff_equity,
                diff_equity_pct,
                level,
                reason,
                backtest_run_id,
                backtest_ts_ms,
                detail_json
            FROM equity_drift
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        return {
            "ts_ms": int(row[0] or 0),
            "broker_equity": float(row[1] or 0.0),
            "backtest_equity": float(row[2] or 0.0),
            "diff_equity": float(row[3] or 0.0),
            "diff_equity_pct": float(row[4] or 0.0),
            "level": str(row[5] or ""),
            "reason": str(row[6] or ""),
            "backtest_run_id": int(row[7] or 0),
            "backtest_ts_ms": int(row[8] or 0),
            "detail_json": str(row[9] or "{}"),
        }
    except Exception as e:
        _warn_nonfatal("EQUITY_DRIFT_LATEST_ROW_READ_FAILED", e)
        return None


def _alert_severity_for_level(level: str) -> Optional[str]:
    level_u = str(level or "").strip().upper()
    if level_u == "WARN":
        return "WARN"
    if level_u == "CRIT":
        return "CRIT"
    return None


def _active_equity_alert_exists(con, *, rule_id: str, severity: str) -> bool:
    if not _table_exists(con, "alerts"):
        return False

    cols = _table_columns(con, "alerts")
    if not {"rule_id", "severity", "symbol"}.issubset(cols):
        return False

    status_filter = ""
    params: list[Any] = [
        str(rule_id or "").strip(),
        str(severity or "").strip().upper(),
        "PORTFOLIO",
    ]
    if "status" in cols:
        status_filter = """
              AND LOWER(COALESCE(status, 'open')) NOT IN (
                'closed',
                'resolved',
                'dismissed',
                'suppressed'
              )
        """

    try:
        row = con.execute(
            f"""
            SELECT 1
            FROM alerts
            WHERE rule_id=?
              AND UPPER(TRIM(severity))=?
              AND UPPER(TRIM(symbol))=?
              {status_filter}
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "EQUITY_DRIFT_ACTIVE_ALERT_READ_FAILED",
            e,
            rule_id=str(rule_id or ""),
            severity=str(severity or ""),
        )
        return False


def emit_equity_drift_alerts(*, current_row: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        from engine.runtime.alerts import emit_runtime_alert
    except Exception as e:
        _warn_nonfatal("EQUITY_DRIFT_ALERT_IMPORT_FAILED", e)
        return {"ok": False, "emitted": [], "reason": f"import_failed:{type(e).__name__}"}

    read_con = None
    try:
        read_con = _db_connect(readonly=True)
        row = dict(current_row or {})
        if not row:
            row = _latest_equity_drift_row(read_con) or {}
        if not row:
            return {"ok": True, "emitted": [], "reason": "no_current_row"}

        level = str(row.get("level") or "").strip().upper()
        severity = _alert_severity_for_level(level)
        if severity is None:
            return {"ok": True, "emitted": [], "reason": "equity_diff_within_tolerance"}

        sustained_level = detect_sustained_equity_drift(
            read_con,
            window=EQUITY_DRIFT_SUSTAINED_WINDOW,
            min_warn=EQUITY_DRIFT_SUSTAINED_MIN_WARN,
            min_crit=EQUITY_DRIFT_SUSTAINED_MIN_CRIT,
        )

        try:
            detail = json.loads(str(row.get("detail_json") or "{}"))
            if not isinstance(detail, dict):
                detail = {}
        except Exception:
            detail = {}

        base_explain = {
            "type": "equity_drift",
            "broker_equity": float(row.get("broker_equity") or 0.0),
            "backtest_equity": float(row.get("backtest_equity") or 0.0),
            "diff_equity": float(row.get("diff_equity") or 0.0),
            "diff_equity_pct": float(row.get("diff_equity_pct") or 0.0),
            "equity_diff_level": str(level),
            "reason": str(row.get("reason") or ""),
            "backtest_run_id": int(row.get("backtest_run_id") or 0),
            "backtest_ts_ms": int(row.get("backtest_ts_ms") or 0),
            "detail": dict(detail),
        }

        emitted: list[Dict[str, Any]] = []
        if _active_equity_alert_exists(read_con, rule_id="EQUITY_RECON", severity=severity):
            emitted.append({"rule_id": "EQUITY_RECON", "inserted": False, "reason": "active_alert_exists"})
        else:
            recon = emit_runtime_alert(
                event_title=f"Equity reconciliation {severity}: broker vs backtest mismatch",
                symbol="PORTFOLIO",
                horizon_s=0,
                expected_z=abs(float(row.get("diff_equity_pct") or 0.0)),
                confidence=1.0,
                severity=severity,
                rule_id="EQUITY_RECON",
                explain=dict(base_explain),
                detail=dict(detail),
                source="equity_drift",
                dedupe_scope=str(level),
                ts_ms=int(row.get("ts_ms") or 0),
                return_details=True,
            )
            emitted.append({"rule_id": "EQUITY_RECON", **dict(recon or {})})

        sustained_severity = _alert_severity_for_level(str(sustained_level or ""))
        if sustained_severity is not None:
            sustained_explain = dict(base_explain)
            sustained_explain.update(
                {
                    "type": "equity_drift_sustained",
                    "sustained_level": str(sustained_level),
                    "window": int(EQUITY_DRIFT_SUSTAINED_WINDOW),
                    "min_warn": int(EQUITY_DRIFT_SUSTAINED_MIN_WARN),
                    "min_crit": int(EQUITY_DRIFT_SUSTAINED_MIN_CRIT),
                }
            )
            if _active_equity_alert_exists(
                read_con,
                rule_id="EQUITY_DRIFT_SUSTAINED",
                severity=sustained_severity,
            ):
                emitted.append(
                    {
                        "rule_id": "EQUITY_DRIFT_SUSTAINED",
                        "inserted": False,
                        "reason": "active_alert_exists",
                    }
                )
            else:
                sustained = emit_runtime_alert(
                    event_title=f"Sustained equity drift {sustained_severity}: broker vs backtest mismatch persists",
                    symbol="PORTFOLIO",
                    horizon_s=0,
                    expected_z=abs(float(row.get("diff_equity_pct") or 0.0)),
                    confidence=1.0,
                    severity=sustained_severity,
                    rule_id="EQUITY_DRIFT_SUSTAINED",
                    explain=sustained_explain,
                    detail=dict(detail),
                    source="equity_drift",
                    dedupe_scope=str(sustained_level),
                    ts_ms=int(row.get("ts_ms") or 0),
                    return_details=True,
                )
                emitted.append({"rule_id": "EQUITY_DRIFT_SUSTAINED", **dict(sustained or {})})

        return {"ok": True, "emitted": emitted, "reason": ""}
    except Exception as e:
        _warn_nonfatal("EQUITY_DRIFT_ALERT_EMIT_FAILED", e)
        return {"ok": False, "emitted": [], "reason": str(e)}
    finally:
        if read_con is not None:
            try:
                read_con.close()
            except Exception as e:
                _warn_nonfatal("EQUITY_DRIFT_ALERT_CLOSE_FAILED", e)


def sync_equity_drift_from_history(con, *, upto_ts_ms: Optional[int] = None) -> Dict[str, Any]:
    if not _table_exists(con, "equity_history"):
        return {"ok": True, "written": 0, "reason": "no_equity_history"}

    backtest_reference = get_latest_backtest_equity(con)
    if not backtest_reference:
        return {"ok": True, "written": 0, "reason": "no_backtest_reference"}

    last_ts = 0
    if _table_exists(con, "equity_drift"):
        try:
            row = con.execute("SELECT MAX(ts_ms) FROM equity_drift").fetchone()
            last_ts = int((row[0] or 0) if row else 0)
        except Exception as e:
            _warn_nonfatal("EQUITY_DRIFT_LATEST_TS_READ_FAILED", e)
            last_ts = 0

    query = """
        SELECT ts_ms, equity
        FROM equity_history
        WHERE ts_ms > ?
    """
    params: list[Any] = [int(last_ts)]
    if upto_ts_ms is not None:
        query += " AND ts_ms <= ?"
        params.append(int(upto_ts_ms))
    query += " ORDER BY ts_ms ASC"

    written = 0
    latest_row: Optional[Dict[str, Any]] = None
    try:
        rows = con.execute(query, tuple(params)).fetchall()
        for ts_ms, broker_equity in rows or []:
            row = build_equity_drift_row(
                ts_ms=int(ts_ms or 0),
                broker_equity=float(broker_equity or 0.0),
                backtest_reference=backtest_reference,
            )
            con.execute(
                """
                INSERT OR REPLACE INTO equity_drift (
                    ts_ms,
                    broker_equity,
                    backtest_equity,
                    diff_equity,
                    diff_equity_pct,
                    level,
                    reason,
                    backtest_run_id,
                    backtest_ts_ms,
                    detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(row["ts_ms"]),
                    float(row["broker_equity"]),
                    float(row["backtest_equity"]),
                    float(row["diff_equity"]),
                    float(row["diff_equity_pct"]),
                    str(row["level"]),
                    str(row["reason"]),
                    int(row["backtest_run_id"]),
                    int(row["backtest_ts_ms"]),
                    str(row["detail_json"]),
                ),
            )
            latest_row = row
            written += 1
    except Exception as e:
        _warn_nonfatal("EQUITY_DRIFT_SYNC_FAILED", e, upto_ts_ms=upto_ts_ms)
        return {"ok": False, "written": written, "reason": str(e)}

    return {
        "ok": True,
        "written": int(written),
        "latest_row": latest_row,
        "backtest_run_id": int(backtest_reference.get("run_id") or 0),
        "backtest_ts_ms": int(backtest_reference.get("backtest_ts_ms") or 0),
    }
