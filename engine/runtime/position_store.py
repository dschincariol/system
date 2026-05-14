"""
FILE: position_store.py

Runtime subsystem module for `position_store`.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("engine.runtime.position_store")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="position_store_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.position_store",
        extra=extra or None,
        persist=False,
    )


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name or ""),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("POSITION_STORE_TABLE_EXISTS_FAILED", e, table_name=str(table_name or ""))
        return False


def _json_dict(value) -> dict:
    try:
        if isinstance(value, dict):
            return dict(value)
        if value in (None, ""):
            return {}
        parsed = json.loads(value)
        return dict(parsed) if isinstance(parsed, dict) else {}
    except Exception as e:
        _warn_nonfatal("POSITION_STORE_JSON_DICT_FAILED", e, value_type=type(value).__name__)
        return {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception as e:
        _warn_nonfatal("POSITION_STORE_SAFE_FLOAT_FAILED", e, value_repr=repr(value), default=float(default))
        return float(default)


def _latest_broker_account(con, book_key: Optional[str] = None) -> dict:
    if book_key and _table_exists(con, "broker_shadow_account"):
        try:
            row = con.execute(
                """
                SELECT
                  updated_ts_ms,
                  equity,
                  cash,
                  NULL AS day_pnl,
                  NULL AS unrealized_pnl,
                  NULL AS realized_pnl,
                  NULL AS extra_json
                FROM broker_shadow_account
                WHERE book_key=?
                LIMIT 1
                """,
                (str(book_key),),
            ).fetchone()
        except Exception as e:
            _warn_nonfatal("POSITION_STORE_SHADOW_ACCOUNT_FETCH_FAILED", e, book_key=str(book_key or ""))
            row = None
        if row:
            return {
                "ts_ms": int(row[0] or 0),
                "equity": float(row[1] or 0.0) if row[1] is not None else None,
                "cash": float(row[2] or 0.0) if row[2] is not None else None,
                "day_pnl": None,
                "unrealized_pnl": None,
                "realized_pnl": None,
                "extra": {},
            }
    if not _table_exists(con, "broker_account"):
        return {}
    try:
        row = con.execute(
            """
            SELECT
              ts_ms,
              equity,
              cash,
              day_pnl,
              unrealized_pnl,
              realized_pnl,
              extra_json
            FROM broker_account
            ORDER BY COALESCE(updated_ts_ms, ts_ms) DESC, ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception as e:
        _warn_nonfatal("POSITION_STORE_BROKER_ACCOUNT_FETCH_FAILED", e)
        row = None
    if not row:
        return {}

    extra = _json_dict(row[6] if len(row) > 6 else None)
    return {
        "ts_ms": int(row[0] or 0),
        "equity": float(row[1] or 0.0) if row[1] is not None else None,
        "cash": float(row[2] or 0.0) if row[2] is not None else None,
        "day_pnl": float(row[3] or 0.0) if row[3] is not None else None,
        "unrealized_pnl": float(row[4] or 0.0) if row[4] is not None else None,
        "realized_pnl": float(row[5] or 0.0) if row[5] is not None else None,
        "extra": extra,
    }


def _last_price(con, symbol: str) -> float | None:
    for sql in (
        "SELECT price FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
        "SELECT px FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
    ):
        try:
            row = con.execute(sql, (str(symbol),)).fetchone()
        except Exception as e:
            _warn_nonfatal("POSITION_STORE_LAST_PRICE_FETCH_FAILED", e, symbol=str(symbol), sql=str(sql))
            row = None
        if row and row[0] is not None:
            try:
                return float(row[0])
            except Exception as e:
                _warn_nonfatal("POSITION_STORE_LAST_PRICE_PARSE_FAILED", e, symbol=str(symbol))
                continue
    return None


def _sum_broker_positions(con, book_key: Optional[str] = None) -> dict[str, float] | None:
    try:
        if book_key and _table_exists(con, "broker_shadow_positions"):
            rows = con.execute(
                """
                SELECT symbol, qty, avg_px
                FROM broker_shadow_positions
                WHERE book_key=?
                """,
                (str(book_key),),
            ).fetchall() or []
            unrealized = 0.0
            realized = 0.0
            for symbol, qty, avg_px in rows:
                px = _last_price(con, str(symbol or ""))
                if px is None:
                    continue
                unrealized += (float(px) - float(avg_px or 0.0)) * float(qty or 0.0)
            return {
                "unrealized": float(unrealized),
                "realized": float(realized),
            }

        if not _table_exists(con, "broker_positions"):
            return None
        row = con.execute(
            """
            SELECT
              COALESCE(SUM(COALESCE(unrealized_pnl, 0.0)), 0.0),
              COALESCE(SUM(COALESCE(realized_pnl, 0.0)), 0.0)
            FROM broker_positions
            WHERE ts_ms = (
              SELECT MAX(ts_ms) FROM broker_positions
            )
            """
        ).fetchone()
        if not row:
            return None
        return {
            "unrealized": float(row[0] or 0.0),
            "realized": float(row[1] or 0.0),
        }
    except Exception as e:
        _warn_nonfatal("POSITION_STORE_BROKER_POSITIONS_SUM_FAILED", e, book_key=str(book_key or ""))
        return None


def _sum_execution_fills_fees(con, model_id: Optional[str] = None) -> float | None:
    if not _table_exists(con, "execution_fills"):
        return None
    try:
        if model_id:
            row = con.execute(
                """
                SELECT
                  COALESCE(SUM(COALESCE(fees, 0.0) + COALESCE(commission, 0.0)), 0.0)
                FROM execution_fills
                WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                """,
                (str(model_id),),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT
                  COALESCE(SUM(COALESCE(fees, 0.0) + COALESCE(commission, 0.0)), 0.0)
                FROM execution_fills
                """
            ).fetchone()
        if not row:
            return None
        return float(row[0] or 0.0)
    except Exception as e:
        _warn_nonfatal("POSITION_STORE_EXECUTION_FEES_SUM_FAILED", e, model_id=str(model_id or ""))
        return None


def _latest_pnl_attribution_snapshot(con, model_id: Optional[str] = None) -> dict | None:
    if not _table_exists(con, "pnl_attribution"):
        return None
    try:
        if model_id:
            normalized_model_id = str(model_id or "").strip() or "baseline"
            if normalized_model_id == "baseline":
                model_filter_sql = "(model_id = ? OR NULLIF(TRIM(model_id), '') IS NULL)"
            else:
                model_filter_sql = "model_id = ?"
            row = con.execute(
                f"""
                SELECT
                  ts_ms,
                  COUNT(DISTINCT COALESCE(NULLIF(TRIM(model_id), ''), 'baseline')),
                  COALESCE(SUM(COALESCE(realized_pnl, 0.0)), 0.0),
                  COALESCE(SUM(COALESCE(unrealized_pnl, 0.0)), 0.0),
                  COALESCE(SUM(COALESCE(fees, 0.0)), 0.0),
                  COALESCE(SUM(COALESCE(json_extract(extra_json, '$.slippage_cost'), 0.0)), 0.0)
                FROM pnl_attribution
                WHERE ts_ms = (
                  SELECT MAX(ts_ms)
                  FROM pnl_attribution
                  WHERE {model_filter_sql}
                )
                  AND {model_filter_sql}
                GROUP BY ts_ms
                """,
                (normalized_model_id, normalized_model_id),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT
                  ts_ms,
                  COUNT(DISTINCT COALESCE(NULLIF(TRIM(model_id), ''), 'baseline')),
                  COALESCE(SUM(COALESCE(realized_pnl, 0.0)), 0.0),
                  COALESCE(SUM(COALESCE(unrealized_pnl, 0.0)), 0.0),
                  COALESCE(SUM(COALESCE(fees, 0.0)), 0.0),
                  COALESCE(SUM(COALESCE(json_extract(extra_json, '$.slippage_cost'), 0.0)), 0.0)
                FROM pnl_attribution
                WHERE ts_ms = (SELECT MAX(ts_ms) FROM pnl_attribution)
                GROUP BY ts_ms
                """
            ).fetchone()
        if not row or row[0] is None:
            return None
        return {
            "ts_ms": int(row[0] or 0),
            "model_count": int(row[1] or 0),
            "realized": float(row[2] or 0.0),
            "unrealized": float(row[3] or 0.0),
            "fees": float(row[4] or 0.0),
            "slippage": float(row[5] or 0.0),
            "net_total": float(row[2] or 0.0) + float(row[3] or 0.0) - float(row[4] or 0.0) - float(row[5] or 0.0),
        }
    except Exception as e:
        _warn_nonfatal("POSITION_STORE_PNL_ATTRIBUTION_SNAPSHOT_FAILED", e, model_id=str(model_id or ""))
        return None


def _latest_model_position_snapshot(con, model_id: Optional[str] = None) -> dict | None:
    if not _table_exists(con, "model_position_state"):
        return None
    try:
        if model_id:
            rows = con.execute(
                """
                SELECT model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms
                FROM model_position_state
                WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                ORDER BY symbol ASC
                """,
                (str(model_id),),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms
                FROM model_position_state
                ORDER BY model_id ASC, symbol ASC
                """
            ).fetchall()
        if not rows:
            return None

        realized = 0.0
        unrealized = 0.0
        latest_ts_ms = 0
        model_ids = set()
        for row_model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms in rows or []:
            mid = str(row_model_id or "").strip() or "baseline"
            sym = str(symbol or "").upper().strip()
            qty = float(net_qty or 0.0)
            avg_px = float(avg_entry_price or 0.0)
            realized += float(realized_pnl or 0.0)
            latest_ts_ms = max(latest_ts_ms, int(last_update_ts_ms or 0))
            model_ids.add(mid)
            if not sym or abs(qty) <= 1e-12 or avg_px <= 0.0:
                continue
            last_px = _last_price(con, sym)
            if last_px is None:
                continue
            unrealized += (float(last_px) - float(avg_px)) * float(qty)

        return {
            "ts_ms": int(latest_ts_ms),
            "model_count": int(len(model_ids)),
            "realized": float(realized),
            "unrealized": float(unrealized),
            "total": float(realized + unrealized),
        }
    except Exception as e:
        _warn_nonfatal("POSITION_STORE_MODEL_POSITION_SNAPSHOT_FAILED", e, model_id=str(model_id or ""))
        return None


def _latest_equity_baseline(con) -> float | None:
    if not _table_exists(con, "equity_history"):
        return None
    try:
        first = con.execute(
            "SELECT equity FROM equity_history ORDER BY ts_ms ASC LIMIT 1"
        ).fetchone()
        if not first or first[0] is None:
            return None
        return float(first[0])
    except Exception as e:
        _warn_nonfatal("POSITION_STORE_EQUITY_BASELINE_FAILED", e)
        return None


def _canonical_pnl_snapshot(con, model_id: Optional[str] = None) -> dict[str, Any] | None:
    mid = str(model_id or "").strip() or None
    canonical = _latest_model_position_snapshot(con, model_id=mid)
    attrib = _latest_pnl_attribution_snapshot(con, model_id=mid)
    if not canonical and not attrib:
        return None

    realized_source = canonical.get("realized") if canonical is not None else (attrib or {}).get("realized")
    unrealized_source = canonical.get("unrealized") if canonical is not None else (attrib or {}).get("unrealized")
    realized = _safe_float(realized_source)
    unrealized = _safe_float(unrealized_source)
    fees = _safe_float((attrib or {}).get("fees"))
    slippage = _safe_float((attrib or {}).get("slippage"))
    total = float(realized + unrealized - fees - slippage)
    ts_ms = max(
        int((canonical or {}).get("ts_ms") or 0),
        int((attrib or {}).get("ts_ms") or 0),
    )
    model_count = max(
        int((canonical or {}).get("model_count") or 0),
        int((attrib or {}).get("model_count") or 0),
    )
    return {
        "model_id": str(mid or ""),
        "model_count": int(model_count),
        "total": float(total),
        "unrealized": float(unrealized),
        "realized": float(realized),
        "day_pnl": float(total),
        "daily_pnl": float(total),
        "total_pnl": float(total),
        "fees": float(fees),
        "slippage": float(slippage),
        "equity": 0.0,
        "cash": 0.0,
        "ts_ms": int(ts_ms),
        "source": "canonical",
    }


def get_pnl_snapshot(model_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns the canonical live PnL snapshot.

    Production callers should only consume fill-derived attribution and
    model-position state. Broker/account fallbacks live in the diagnostic helper
    below so the truth path stays explicit.
    """
    con = connect(readonly=True)
    try:
        mid = str(model_id or "").strip() or None
        canonical = _canonical_pnl_snapshot(con, model_id=mid)
        if canonical:
            return canonical
        return {
            "model_id": str(mid or ""),
            "total": 0.0,
            "unrealized": 0.0,
            "realized": 0.0,
            "day_pnl": 0.0,
            "daily_pnl": 0.0,
            "total_pnl": 0.0,
            "equity": 0.0,
            "cash": 0.0,
            "ts_ms": 0,
            "source": "missing",
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("POSITION_STORE_CLOSE_FAILED", e, operation="get_pnl_snapshot", model_id=str(model_id or ""))


def get_pnl_snapshot_diagnostic(model_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Best-effort runtime diagnostic view.

    This is intentionally separate from `get_pnl_snapshot()` so operators can
    inspect broker/account fallbacks without polluting the production truth path.
    """
    con = connect(readonly=True)
    try:
        mid = str(model_id or "").strip() or None
        book_key = f"shadow:{mid}" if mid else None
        canonical = _canonical_pnl_snapshot(con, model_id=mid)
        account = _latest_broker_account(con, book_key=book_key if mid else None)
        pos = _sum_broker_positions(con, book_key=book_key if mid else None) or {}
        attrib = _latest_pnl_attribution_snapshot(con, model_id=mid) or {}
        fee_total = _sum_execution_fills_fees(con, model_id=mid)
        equity_baseline = _latest_equity_baseline(con)
        extra = dict(account.get("extra") or {})

        if canonical:
            out = dict(canonical)
            out["equity"] = float(account.get("equity") or 0.0) if account.get("equity") is not None else 0.0
            out["cash"] = float(account.get("cash") or 0.0)
            out["source"] = "canonical+diagnostic"
            return out

        unrealized = account.get("unrealized_pnl")
        if unrealized is None:
            unrealized = extra.get("unrealized_pnl")
        if unrealized is None:
            unrealized = attrib.get("unrealized")
        if unrealized is None:
            unrealized = pos.get("unrealized")
        unrealized = float(unrealized or 0.0)

        realized = account.get("realized_pnl")
        if realized is None:
            realized = extra.get("realized_pnl")
        if realized is None and "day_realized_pnl" in extra:
            realized = extra.get("day_realized_pnl")
        if realized is None:
            realized = attrib.get("realized")
        if realized is None:
            realized = pos.get("realized")
        realized = float(realized or 0.0)

        day_pnl = account.get("day_pnl")
        if day_pnl is None:
            day_pnl = extra.get("day_pnl")
        if day_pnl is None:
            day_pnl = extra.get("daily_pnl")
        if day_pnl is None:
            day_pnl = realized + unrealized - float(attrib.get("fees") or 0.0)
        day_pnl = float(day_pnl or 0.0)

        total = None
        equity = account.get("equity")
        if equity is not None and equity_baseline is not None:
            total = float(equity) - float(equity_baseline)
        elif "total_pnl" in extra:
            total = float(extra.get("total_pnl") or 0.0)
        elif "pnl_total" in extra:
            total = float(extra.get("pnl_total") or 0.0)
        elif attrib:
            total = float(realized + unrealized - float(attrib.get("fees") or 0.0))
        elif fee_total is not None:
            total = float(realized + unrealized - float(fee_total or 0.0))
        else:
            total = float(realized + unrealized)

        return {
            "model_id": str(mid or ""),
            "total": float(total or 0.0),
            "unrealized": float(unrealized),
            "realized": float(realized),
            "day_pnl": float(day_pnl),
            "daily_pnl": float(day_pnl),
            "total_pnl": float(total or 0.0),
            "equity": float(equity or 0.0) if equity is not None else 0.0,
            "cash": float(account.get("cash") or 0.0),
            "ts_ms": int(account.get("ts_ms") or attrib.get("ts_ms") or 0),
            "source": "diagnostic_fallback",
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("POSITION_STORE_CLOSE_FAILED", e, operation="get_pnl_snapshot_diagnostic", model_id=str(model_id or ""))
