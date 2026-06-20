"""Read-mostly API handlers that back the browser trading terminal.

These endpoints expose watchlists, snapshots, positions, orders, fills, equity,
and chart markers while degrading safely to empty payloads when optional tables
or newer runtime state are not available.
"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from typing import Any, Dict, List, Optional

from engine.api.http_parsing import qs as _qs
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.gates import execution_gate_snapshot
from engine.runtime.storage import connect_ro

try:
    from engine.cache.wrappers.execution_mode import read_execution_mode as _get_execution_mode
except Exception:
    _get_execution_mode = None  # type: ignore

try:
    from engine.cache.wrappers.kill_switch import read_kill_switch as _kill_switch_snapshot
except Exception:
    _kill_switch_snapshot = None  # type: ignore

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
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
        component="engine.terminal.api.api_terminal",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


ROUTE_SPECS_TERMINAL = [
    ("GET", "/api/terminal/watchlist", "api_get_terminal_watchlist"),
    ("GET", "/api/terminal/snapshot",  "api_get_terminal_snapshot"),
    ("GET", "/api/terminal/positions", "api_get_terminal_positions"),
    ("GET", "/api/terminal/orders",    "api_get_terminal_orders"),
    ("GET", "/api/terminal/fills",     "api_get_terminal_fills"),
    ("GET", "/api/terminal/equity",    "api_get_terminal_equity"),
    ("GET", "/api/terminal/markers",   "api_get_terminal_markers"),
    ("GET", "/api/terminal/decision_overlays", "api_get_terminal_decision_overlays"),
]


def _table_exists(con, name: str) -> bool:
    try:
        r = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(name),),
        ).fetchone()
        return bool(r)
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_TABLE_EXISTS_FAILED", e, once_key=f"table_exists_{name}", table=str(name))
        return False


def _rows_to_dicts(rows) -> List[Dict[str, Any]]:
    out = []
    if not rows:
        return out
    for r in rows:
        try:
            # Storage rows support dict-style conversion.
            out.append(dict(r))
        except Exception:
            try:
                # fallback: mapping-ish
                out.append({k: r[k] for k in r.keys()})
            except Exception:
                out.append({"raw": str(r)})
    return out


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        n = float(value)
        if not math.isfinite(n):
            return default
        return float(n)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _table_columns(con, name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({str(name)})").fetchall() or []
        return {str(r[1]) for r in rows if r and len(r) > 1 and r[1]}
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_TABLE_COLUMNS_FAILED", e, once_key=f"table_columns_{name}", table=str(name))
        return set()


def _column_expr(cols: set[str], candidates: List[str], alias: str, default_sql: str = "NULL") -> str:
    for col in candidates:
        if str(col) in cols:
            return f"{str(col)} AS {str(alias)}"
    return f"{default_sql} AS {str(alias)}"


def _json_loads(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _stable_reason_code(value: Any, fallback: str = "unknown") -> str:
    raw = str(value or fallback or "unknown").strip().lower()
    if ":" in raw:
        raw = raw.split(":", 1)[0]
    raw = raw.replace(">=", "_gte_").replace("<=", "_lte_").replace(">", "_gt_").replace("<", "_lt_")
    raw = re.sub(r"[^a-z0-9_]+", "_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    return raw or str(fallback or "unknown")


def _side_from_payload(*payloads: Any) -> str:
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key in ("side", "to_side", "action"):
            raw = str(payload.get(key) or "").upper().strip()
            if raw in ("BUY", "LONG"):
                return "BUY"
            if raw in ("SELL", "SHORT"):
                return "SELL"
        qty = _safe_float(payload.get("qty"), None)
        if qty is not None and abs(qty) > 1e-12:
            return "BUY" if qty > 0 else "SELL"
        weight = _safe_float(payload.get("to_weight"), None)
        if weight is not None and abs(weight) > 1e-12:
            return "BUY" if weight > 0 else "SELL"
    return ""


def _find_number(payload: Any, keys: set[str], *, depth: int = 0) -> Optional[float]:
    if depth > 5:
        return None
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_norm = str(key or "").strip().lower()
            if key_norm in keys:
                found = _safe_float(value, None)
                if found is not None and found > 0:
                    return found
        for value in payload.values():
            found = _find_number(value, keys, depth=depth + 1)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_number(item, keys, depth=depth + 1)
            if found is not None:
                return found
    return None


def _add_marker(markers: List[Dict[str, Any]], **kwargs: Any) -> None:
    ts_ms = _safe_int(kwargs.get("ts_ms"), 0)
    ts = _safe_int(kwargs.get("t") or kwargs.get("ts"), int(ts_ms // 1000) if ts_ms > 0 else 0)
    if ts <= 0 and ts_ms > 0:
        ts = int(ts_ms // 1000)
    if ts <= 0:
        return
    kind = str(kwargs.get("kind") or "event").strip().lower()
    markers.append({
        "ts": int(ts),
        "t": int(ts),
        "ts_ms": int(ts_ms if ts_ms > 0 else ts * 1000),
        "symbol": str(kwargs.get("symbol") or "").upper().strip(),
        "kind": kind,
        "side": str(kwargs.get("side") or "").upper().strip(),
        "size": _safe_float(kwargs.get("size"), 0.0),
        "qty": _safe_float(kwargs.get("qty"), 0.0),
        "price": _safe_float(kwargs.get("price"), None),
        "px": _safe_float(kwargs.get("price"), None),
        "reason_code": _stable_reason_code(kwargs.get("reason_code"), kind),
        "reason": str(kwargs.get("reason") or kwargs.get("reason_code") or "").strip(),
        "text": str(kwargs.get("text") or "").strip(),
        "source": str(kwargs.get("source") or "").strip(),
        "source_id": kwargs.get("source_id"),
        "source_alert_id": kwargs.get("source_alert_id"),
    })


def _add_price_line(price_lines: List[Dict[str, Any]], *, price: Any, kind: str, title: str, reason_code: str, ts_ms: int = 0, source: str = "") -> None:
    px = _safe_float(price, None)
    if px is None or px <= 0:
        return
    dedupe_key = (str(kind), round(float(px), 8), str(title))
    for existing in price_lines:
        if existing.get("_dedupe_key") == dedupe_key:
            return
    price_lines.append({
        "kind": str(kind),
        "price": float(px),
        "title": str(title)[:36],
        "reason_code": _stable_reason_code(reason_code, kind),
        "ts_ms": int(ts_ms or 0),
        "source": str(source or ""),
        "_dedupe_key": dedupe_key,
    })


def _add_price_lines_from_payload(price_lines: List[Dict[str, Any]], payload: Any, *, ts_ms: int, source: str) -> None:
    if not isinstance(payload, (dict, list)):
        return
    for keys, kind, title, reason_code in (
        ({"entry_price", "entry_px", "expected_price", "expected_px"}, "entry", "Entry", "entry_level"),
        ({"avg_price", "avg_entry_price", "average_cost", "avg_cost"}, "average_cost", "Average cost", "average_cost"),
        ({"stop", "stop_px", "stop_price", "stop_loss", "stop_loss_px", "stop_loss_price"}, "stop", "Stop", "stop_level"),
        ({"take_profit", "take_profit_px", "take_profit_price", "target_price", "target_px"}, "take_profit", "Take profit", "take_profit_level"),
        ({"max_risk_price", "max_risk_px", "risk_price"}, "max_risk", "Max risk", "max_risk_level"),
        ({"cap_price", "cap_px", "risk_cap_price", "risk_cap_px"}, "cap", "Risk cap", "risk_cap_level"),
    ):
        found = _find_number(payload, keys)
        if found is not None:
            _add_price_line(price_lines, price=found, kind=kind, title=title, reason_code=reason_code, ts_ms=ts_ms, source=source)


def _classify_suppression(reason: str, decision: Dict[str, Any], policy: Dict[str, Any]) -> str:
    text = f"{reason} {decision.get('blocked_by') or ''} {policy.get('blocked_by') or ''}".lower()
    scale = _safe_float(decision.get("scale"), _safe_float(policy.get("scale"), None))
    if "risk_cap" in text or "size_compression" in text or "scaled_to_zero" in text:
        return "risk_capped"
    if scale is not None and scale < 0.999 and any(token in text for token in ("meta_label", "ood", "conformal", "tse", "risk", "scale")):
        return "risk_capped"
    if any(token in text for token in ("kill_switch", "hard_block", "blocked", "missing_", "future_signal", "conformal_interval", "ood_hard")):
        return "blocked"
    return "suppressed"


def _add_window(windows: List[Dict[str, Any]], markers: List[Dict[str, Any]], *, kind: str, start_ts_ms: Any, end_ts_ms: Any = None, reason_code: str, reason: str = "", label: str = "", source: str = "") -> None:
    start = _safe_int(start_ts_ms, 0)
    if start <= 0:
        return
    end = _safe_int(end_ts_ms, 0)
    if end <= start:
        end = 0
    window = {
        "kind": str(kind),
        "start_ts_ms": int(start),
        "end_ts_ms": int(end) if end > 0 else None,
        "reason_code": _stable_reason_code(reason_code, kind),
        "reason": str(reason or reason_code or ""),
        "label": str(label or kind.replace("_", " ")),
        "source": str(source or ""),
    }
    dedupe = (window["kind"], window["start_ts_ms"], window.get("end_ts_ms"), window["reason_code"], window["source"])
    for existing in windows:
        if (
            existing.get("kind"),
            existing.get("start_ts_ms"),
            existing.get("end_ts_ms"),
            existing.get("reason_code"),
            existing.get("source"),
        ) == dedupe:
            return
    windows.append(window)
    _add_marker(
        markers,
        ts_ms=start,
        kind=kind,
        text=("KILL" if "kill" in kind else "CB" if "circuit" in kind else "DD" if "drawdown" in kind else "TSE"),
        reason_code=window["reason_code"],
        reason=window["reason"],
        source=source,
    )
    if end > 0:
        _add_marker(
            markers,
            ts_ms=end,
            kind=kind,
            text="END",
            reason_code=f"{window['reason_code']}_ended",
            reason=window["reason"],
            source=source,
        )



def _close_ro_connection(con: Any) -> None:
    try:
        con.close()
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_CONNECTION_CLOSE_FAILED", e, once_key="terminal_ro_connection_close")


def _coerce_ts_ms(value: Any, default: int) -> int:
    try:
        n = int(value or 0)
    except Exception:
        return int(default)
    return n if n > 0 else int(default)


def _dedupe_strings(values: List[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _terminal_execution_barrier_snapshot() -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    try:
        if _get_execution_mode is None or _kill_switch_snapshot is None:
            raise RuntimeError("execution_gate_providers_missing")
        gate = execution_gate_snapshot(
            get_execution_mode_fn=_get_execution_mode,
            kill_switches=(_kill_switch_snapshot() or {}),
        )
        if not isinstance(gate, dict):
            raise TypeError(f"execution_gate_snapshot returned {type(gate).__name__}")
    except Exception as e:
        _warn_nonfatal(
            "API_TERMINAL_EXECUTION_BARRIER_FAILED",
            e,
            once_key="terminal_execution_barrier_failed",
        )
        return {
            "ok": False,
            "real_trading_allowed": False,
            "real_trading_blocked": True,
            "blocked": True,
            "blocking_reasons": ["execution_barrier_unavailable"],
            "reason": "execution_barrier_unavailable",
            "mode": "unknown",
            "execution_mode": "unknown",
            "gate_status": "execution_barrier_unavailable",
            "allowed": False,
            "allow_execution": False,
            "allow_execution_pipeline": False,
            "allow_simulation": False,
            "armed": None,
            "source": "terminal_snapshot:error",
            "runtime_state": "UNKNOWN",
            "severity": "CRITICAL",
            "ts_ms": now_ms,
            "updated_ts_ms": now_ms,
        }

    real_allowed = bool(gate.get("real_trading_allowed", False))
    reason = str(gate.get("reason") or ("real_trading_allowed" if real_allowed else "execution_blocked"))
    reason_values: List[Any] = []
    if not real_allowed:
        reason_values.append(reason)
        for key in ("blocking_reasons", "severity_reasons", "reason_codes"):
            raw = gate.get(key)
            if isinstance(raw, list):
                reason_values.extend(raw)
            elif raw not in (None, ""):
                reason_values.append(raw)

    mode = str(gate.get("mode") or gate.get("execution_mode") or "unknown")
    updated_ts_ms = _coerce_ts_ms(gate.get("updated_ts_ms") or gate.get("ts_ms"), now_ms)

    return {
        "ok": bool(gate.get("ok", True)),
        "real_trading_allowed": real_allowed,
        "real_trading_blocked": not real_allowed,
        "blocked": not real_allowed,
        "blocking_reasons": _dedupe_strings(reason_values),
        "reason": reason,
        "mode": mode,
        "execution_mode": mode,
        "gate_status": reason,
        "allowed": bool(gate.get("allowed", False)),
        "allow_execution": bool(gate.get("allow_execution", False)),
        "allow_execution_pipeline": bool(gate.get("allow_execution_pipeline", False)),
        "allow_simulation": bool(gate.get("allow_simulation", False)),
        "armed": gate.get("armed"),
        "source": str(gate.get("source") or ""),
        "runtime_state": str(gate.get("runtime_state") or ""),
        "severity": str(gate.get("severity") or ("OK" if real_allowed else "CRITICAL")),
        "ts_ms": updated_ts_ms,
        "updated_ts_ms": updated_ts_ms,
    }


def api_get_terminal_watchlist(_parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
    try:
        # Prefer symbols with freshest market data
        symbols: List[str] = []

        watchlist_sources = [
            (
                "price_quotes",
                """
                SELECT symbol, MAX(COALESCE(last_update_ts_ms, last_quote_ts_ms, last_trade_ts_ms, ts_ms)) AS last_ts
                  FROM price_quotes
                 GROUP BY symbol
                 ORDER BY last_ts DESC
                 LIMIT 200
                """,
                "API_TERMINAL_WATCHLIST_PRICE_QUOTES_READ_FAILED",
            ),
            (
                "price_quotes_raw",
                """
                SELECT symbol, MAX(COALESCE(quote_ts_ms, trade_ts_ms, event_ts_ms, ingest_ts_ms, ts_ms)) AS last_ts
                  FROM price_quotes_raw
                 GROUP BY symbol
                 ORDER BY last_ts DESC
                 LIMIT 200
                """,
                "API_TERMINAL_WATCHLIST_PRICE_QUOTES_RAW_READ_FAILED",
            ),
            (
                "price_bars",
                """
                SELECT symbol, MAX(ts_ms) AS last_ts
                  FROM price_bars
                 GROUP BY symbol
                 ORDER BY last_ts DESC
                 LIMIT 200
                """,
                "API_TERMINAL_WATCHLIST_PRICE_BARS_READ_FAILED",
            ),
        ]

        for table_name, sql, warn_code in watchlist_sources:
            if symbols or not _table_exists(con, table_name):
                continue
            try:
                rows = con.execute(sql).fetchall()
                for r in rows or []:
                    try:
                        s = str(r[0] or "").strip().upper()
                        if s and s not in symbols:
                            symbols.append(s)
                    except Exception as e:
                        _warn_nonfatal(
                            "API_TERMINAL_WATCHLIST_SYMBOL_PARSE_FAILED",
                            e,
                            once_key=f"watchlist_symbol_parse_{table_name}",
                            table=table_name,
                        )
            except Exception as e:
                _warn_nonfatal(warn_code, e, once_key=f"watchlist_read_{table_name}", table=table_name)

        # Fallback: portfolio_state symbols
        # The terminal should stay usable even if higher-fidelity market tables are
        # missing, so the API degrades to whatever read model is still available.
        if not symbols and _table_exists(con, "portfolio_state"):
            try:
                rows = con.execute(
                    """
                    SELECT DISTINCT symbol
                      FROM portfolio_state
                     WHERE symbol IS NOT NULL AND symbol != ''
                     ORDER BY updated_ts_ms DESC
                     LIMIT 200
                    """
                ).fetchall()
                for r in rows or []:
                    try:
                        s = str(r[0] or "").strip().upper()
                        if s and s not in symbols:
                            symbols.append(s)
                    except Exception as e:
                        _warn_nonfatal("API_TERMINAL_WATCHLIST_PORTFOLIO_SYMBOL_PARSE_FAILED", e, once_key="watchlist_portfolio_symbol_parse")
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_WATCHLIST_PORTFOLIO_READ_FAILED", e, once_key="watchlist_portfolio_read")

        return {"ok": True, "symbols": symbols}
    finally:
        _close_ro_connection(con)


def api_get_terminal_positions(_parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
    try:
        rows = []

        if _table_exists(con, "broker_positions"):
            try:
                cols = {
                    str(r[1])
                    for r in (con.execute("PRAGMA table_info(broker_positions)").fetchall() or [])
                    if r and len(r) > 1 and r[1]
                }
                ts_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else "0")

                rows = con.execute(
                    f"""
                    SELECT symbol, qty, avg_px, {ts_col} AS updated_ts_ms
                      FROM broker_positions
                     ORDER BY {ts_col} DESC
                     LIMIT 500
                    """
                ).fetchall()
            except Exception:
                rows = []

        return {"ok": True, "rows": _rows_to_dicts(rows)}
    finally:
        _close_ro_connection(con)


def api_get_terminal_orders(parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
    try:
        q = _qs(parsed)
        limit_s = (q.get("limit") or "500").strip()
        try:
            limit = max(1, min(5000, int(limit_s)))
        except Exception:
            limit = 500

        # Merge broker-facing state with portfolio intent rows. They represent
        # different stages of the order lifecycle, so the API returns both instead
        # of pretending there is a single authoritative table.
        # Merge: broker_order_state + portfolio_orders (best effort)
        out = {"broker": [], "portfolio": [], "rejected": []}

        if _table_exists(con, "broker_order_state"):
            try:
                rows = con.execute(
                    """
                    SELECT id, source_order_id, symbol, state, created_ts_ms, updated_ts_ms, ttl_ms, meta_json
                      FROM broker_order_state
                     ORDER BY updated_ts_ms DESC
                     LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()
                out["broker"] = _rows_to_dicts(rows)
            except Exception:
                out["broker"] = []

        if _table_exists(con, "portfolio_orders"):
            try:
                rows = con.execute(
                    """
                    SELECT id, ts_ms, model_id, symbol, action, from_side, to_side,
                           from_weight, to_weight, delta_weight, source_alert_id
                      FROM portfolio_orders
                     ORDER BY ts_ms DESC
                     LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()
                out["portfolio"] = _rows_to_dicts(rows)
            except Exception:
                out["portfolio"] = []

        if _table_exists(con, "terminal_intent_rejections"):
            try:
                rows = con.execute(
                    """
                    SELECT id, ts_ms, symbol, side, qty, reason_code, reason, source, detail_json
                      FROM terminal_intent_rejections
                     ORDER BY ts_ms DESC
                     LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []
                rejected = []
                for row in rows:
                    try:
                        rejected.append({
                            "id": row[0],
                            "ts_ms": int(row[1] or 0),
                            "updated_ts_ms": int(row[1] or 0),
                            "symbol": str(row[2] or ""),
                            "side": str(row[3] or ""),
                            "qty": float(row[4] or 0.0),
                            "state": "REJECTED",
                            "action": "REJECTED",
                            "reason_code": str(row[5] or "rejected"),
                            "reason": str(row[6] or "Terminal request rejected."),
                            "source": str(row[7] or "terminal"),
                            "detail_json": str(row[8] or "{}"),
                        })
                    except Exception as e:
                        _warn_nonfatal("API_TERMINAL_REJECTION_ROW_PARSE_FAILED", e, once_key="terminal_rejection_row")
                out["rejected"] = rejected
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_REJECTIONS_READ_FAILED", e, once_key="terminal_rejections_read")
                out["rejected"] = []

        return {"ok": True, "data": out}
    finally:
        _close_ro_connection(con)


def api_get_terminal_fills(parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
    try:
        q = _qs(parsed)
        limit_s = (q.get("limit") or "1000").strip()
        symbol = (q.get("symbol") or "").strip().upper()

        try:
            limit = max(1, min(20000, int(limit_s)))
        except Exception:
            limit = 1000

        rows = []
        fills_table = "broker_fills_v2" if _table_exists(con, "broker_fills_v2") else ("broker_fills" if _table_exists(con, "broker_fills") else None)

        if fills_table:
            try:
                if symbol:
                    rows = con.execute(
                        f"""
                        SELECT id, ts_ms, symbol, qty, px, source_order_id, note, explain_json
                          FROM {fills_table}
                         WHERE symbol=?
                         ORDER BY ts_ms DESC
                         LIMIT ?
                        """,
                        (str(symbol), int(limit)),
                    ).fetchall()
                else:
                    rows = con.execute(
                        f"""
                        SELECT id, ts_ms, symbol, qty, px, source_order_id, note, explain_json
                          FROM {fills_table}
                         ORDER BY ts_ms DESC
                         LIMIT ?
                        """,
                        (int(limit),),
                    ).fetchall()
            except Exception:
                rows = []

        return {"ok": True, "rows": _rows_to_dicts(rows)}
    finally:
        _close_ro_connection(con)


def api_get_terminal_equity(parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
    try:
        q = _qs(parsed)

        limit_s = (q.get("limit") or "2000").strip()
        try:
            limit = max(10, min(50000, int(limit_s)))
        except Exception:
            limit = 2000

        account = None
        series = []

        if _table_exists(con, "broker_account"):
            try:
                cols = {
                    str(r[1])
                    for r in (con.execute("PRAGMA table_info(broker_account)").fetchall() or [])
                    if r and len(r) > 1 and r[1]
                }
                cash_col = "cash" if "cash" in cols else ("buying_power" if "buying_power" in cols else "0")
                equity_col = "equity" if "equity" in cols else cash_col
                ts_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else "0")

                r = con.execute(
                    f"SELECT {cash_col} AS cash, {equity_col} AS equity, {ts_col} AS updated_ts_ms FROM broker_account ORDER BY {ts_col} DESC LIMIT 1"
                ).fetchone()
                if r:
                    account = {"cash": float(r[0] or 0.0), "equity": float(r[1] or 0.0), "updated_ts_ms": int(r[2] or 0)}
            except Exception:
                account = None

        # Account is the current snapshot; equity_history is the chartable time
        # series. The UI can render one without the other, so both are optional.
        if _table_exists(con, "equity_history"):
            try:
                rows = con.execute(
                    """
                    SELECT ts_ms, equity
                      FROM equity_history
                     ORDER BY ts_ms DESC
                     LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()
                rows = list(reversed(rows or []))
                for r in rows:
                    try:
                        ts_ms = int(r[0] or 0)
                        equity = float(r[1] or 0.0)
                        series.append({
                            "ts_ms": ts_ms,
                            "t": int(ts_ms // 1000),
                            "v": equity,
                            "equity": equity,
                        })
                    except Exception as e:
                        _warn_nonfatal("API_TERMINAL_EQUITY_SERIES_ROW_PARSE_FAILED", e, once_key="equity_series_row_parse")
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_EQUITY_SERIES_READ_FAILED", e, once_key="equity_series_read")
                series = []

        return {
            "ok": True,
            "account": account,
            "series": series,
            "meta": {
                "ready": bool(account or series),
                "count": int(len(series)),
            },
        }
    finally:
        _close_ro_connection(con)


def _append_fill_overlays(con, symbol: str, markers: List[Dict[str, Any]], price_lines: List[Dict[str, Any]], limit: int) -> None:
    fills_table = "broker_fills_v2" if _table_exists(con, "broker_fills_v2") else ("broker_fills" if _table_exists(con, "broker_fills") else None)
    if not fills_table:
        return
    cols = _table_columns(con, fills_table)
    if "symbol" not in cols:
        return
    ts_col = "ts_ms" if "ts_ms" in cols else ("fill_ts_ms" if "fill_ts_ms" in cols else None)
    if not ts_col:
        return
    try:
        select = ", ".join([
            _column_expr(cols, [ts_col], "ts_ms", "0"),
            _column_expr(cols, ["qty", "fill_qty"], "qty", "0"),
            _column_expr(cols, ["px", "price", "fill_px"], "px", "NULL"),
            _column_expr(cols, ["side"], "side", "NULL"),
            _column_expr(cols, ["source_order_id", "portfolio_orders_id"], "source_order_id", "NULL"),
            _column_expr(cols, ["source_alert_id"], "source_alert_id", "NULL"),
            _column_expr(cols, ["explain_json", "extra_json", "raw_json"], "meta_json", "NULL"),
        ])
        rows = con.execute(
            f"""
            SELECT {select}
              FROM {fills_table}
             WHERE UPPER(symbol)=?
             ORDER BY {ts_col} DESC
             LIMIT ?
            """,
            (str(symbol), int(limit)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_FILL_OVERLAYS_READ_FAILED", e, once_key="fill_overlays_read")
        return

    latest_fill_px = None
    latest_fill_ts_ms = 0
    for row in _rows_to_dicts(rows):
        try:
            ts_ms = _safe_int(row.get("ts_ms"), 0)
            qty = _safe_float(row.get("qty"), 0.0) or 0.0
            px = _safe_float(row.get("px"), None)
            side_raw = str(row.get("side") or "").upper().strip()
            side = side_raw if side_raw in ("BUY", "SELL") else ("BUY" if qty > 0 else "SELL")
            _add_marker(
                markers,
                ts_ms=ts_ms,
                symbol=symbol,
                kind="filled",
                side=side,
                qty=qty,
                size=abs(qty),
                price=px,
                text=("FILL B" if side == "BUY" else "FILL S"),
                reason_code="fill_executed",
                source=fills_table,
                source_id=row.get("source_order_id"),
                source_alert_id=row.get("source_alert_id"),
            )
            meta = _json_loads(row.get("meta_json"))
            _add_price_lines_from_payload(price_lines, meta, ts_ms=ts_ms, source=fills_table)
            if px is not None and ts_ms >= latest_fill_ts_ms:
                latest_fill_px = px
                latest_fill_ts_ms = ts_ms
        except Exception as e:
            _warn_nonfatal("API_TERMINAL_FILL_OVERLAY_BUILD_FAILED", e, once_key="fill_overlay_build")
    if latest_fill_px is not None:
        _add_price_line(price_lines, price=latest_fill_px, kind="entry", title="Last fill", reason_code="last_fill_price", ts_ms=latest_fill_ts_ms, source=fills_table)


def _append_intent_overlays(con, symbol: str, markers: List[Dict[str, Any]], price_lines: List[Dict[str, Any]], limit: int) -> None:
    if not _table_exists(con, "portfolio_orders"):
        return
    cols = _table_columns(con, "portfolio_orders")
    if "symbol" not in cols or "ts_ms" not in cols:
        return
    try:
        select = ", ".join([
            _column_expr(cols, ["id"], "id", "NULL"),
            _column_expr(cols, ["ts_ms"], "ts_ms", "0"),
            _column_expr(cols, ["action"], "action", "NULL"),
            _column_expr(cols, ["from_side"], "from_side", "NULL"),
            _column_expr(cols, ["to_side"], "to_side", "NULL"),
            _column_expr(cols, ["delta_weight"], "delta_weight", "0"),
            _column_expr(cols, ["to_weight"], "to_weight", "0"),
            _column_expr(cols, ["source_alert_id"], "source_alert_id", "NULL"),
            _column_expr(cols, ["explain_json"], "explain_json", "NULL"),
        ])
        rows = con.execute(
            f"""
            SELECT {select}
              FROM portfolio_orders
             WHERE UPPER(symbol)=?
             ORDER BY ts_ms DESC
             LIMIT ?
            """,
            (str(symbol), int(limit)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_INTENT_OVERLAYS_READ_FAILED", e, once_key="intent_overlays_read")
        return

    for row in _rows_to_dicts(rows):
        try:
            ts_ms = _safe_int(row.get("ts_ms"), 0)
            dw = _safe_float(row.get("delta_weight"), 0.0) or 0.0
            tw = _safe_float(row.get("to_weight"), 0.0) or 0.0
            side = _side_from_payload(row) or str(row.get("action") or "INTENT").upper()
            explain = _json_loads(row.get("explain_json"))
            px = _find_number(explain, {"expected_price", "expected_px", "entry_price", "entry_px"}) if explain is not None else None
            _add_marker(
                markers,
                ts_ms=ts_ms,
                symbol=symbol,
                kind="intended",
                side=side,
                qty=tw if abs(tw) > 0 else dw,
                size=abs(dw if abs(dw) > 0 else tw),
                price=px,
                text="INTENT",
                reason_code="portfolio_intent",
                source="portfolio_orders",
                source_id=row.get("id"),
                source_alert_id=row.get("source_alert_id"),
            )
            _add_price_lines_from_payload(price_lines, explain, ts_ms=ts_ms, source="portfolio_orders")
        except Exception as e:
            _warn_nonfatal("API_TERMINAL_INTENT_OVERLAY_BUILD_FAILED", e, once_key="intent_overlay_build")


def _append_attribution_overlays(con, symbol: str, markers: List[Dict[str, Any]], price_lines: List[Dict[str, Any]], limit: int) -> None:
    if not _table_exists(con, "trade_attribution_ledger"):
        return
    cols = _table_columns(con, "trade_attribution_ledger")
    if "symbol" not in cols or "ts_ms" not in cols or "suppression_reason" not in cols:
        return
    try:
        select = ", ".join([
            _column_expr(cols, ["id"], "id", "NULL"),
            _column_expr(cols, ["ts_ms"], "ts_ms", "0"),
            _column_expr(cols, ["source_alert_id"], "source_alert_id", "NULL"),
            _column_expr(cols, ["model_id"], "model_id", "'baseline'"),
            _column_expr(cols, ["signal_json"], "signal_json", "NULL"),
            _column_expr(cols, ["execution_policy_json"], "execution_policy_json", "NULL"),
            _column_expr(cols, ["decision_json"], "decision_json", "NULL"),
            _column_expr(cols, ["suppression_reason"], "suppression_reason", "NULL"),
            _column_expr(cols, ["expected_price"], "expected_price", "NULL"),
            _column_expr(cols, ["fill_price"], "fill_price", "NULL"),
        ])
        rows = con.execute(
            f"""
            SELECT {select}
              FROM trade_attribution_ledger
             WHERE UPPER(symbol)=?
               AND suppression_reason IS NOT NULL
               AND TRIM(suppression_reason) != ''
             ORDER BY ts_ms DESC
             LIMIT ?
            """,
            (str(symbol), int(limit)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_ATTRIBUTION_OVERLAYS_READ_FAILED", e, once_key="attribution_overlays_read")
        return

    for row in _rows_to_dicts(rows):
        try:
            ts_ms = _safe_int(row.get("ts_ms"), 0)
            reason = str(row.get("suppression_reason") or "").strip()
            signal = _json_loads(row.get("signal_json")) or {}
            policy = _json_loads(row.get("execution_policy_json")) or {}
            decision = _json_loads(row.get("decision_json")) or {}
            kind = _classify_suppression(reason, decision if isinstance(decision, dict) else {}, policy if isinstance(policy, dict) else {})
            side = _side_from_payload(signal if isinstance(signal, dict) else {}, decision if isinstance(decision, dict) else {}, policy if isinstance(policy, dict) else {})
            qty = _safe_float((signal or {}).get("qty") if isinstance(signal, dict) else None, None)
            weight = _safe_float((signal or {}).get("to_weight") if isinstance(signal, dict) else None, 0.0) or 0.0
            px = _safe_float(row.get("expected_price"), _safe_float(row.get("fill_price"), None))
            if px is None:
                px = _find_number([signal, decision, policy], {"expected_price", "expected_px", "entry_price", "entry_px"})
            _add_marker(
                markers,
                ts_ms=ts_ms,
                symbol=symbol,
                kind=kind,
                side=side,
                qty=(qty if qty is not None else weight),
                size=abs(qty if qty is not None else weight),
                price=px,
                text=("CAP" if kind == "risk_capped" else "BLOCK" if kind == "blocked" else "SUPP"),
                reason_code=_stable_reason_code(reason, kind),
                reason=reason,
                source="trade_attribution_ledger",
                source_id=row.get("id"),
                source_alert_id=row.get("source_alert_id"),
            )
            _add_price_lines_from_payload(price_lines, [signal, decision, policy], ts_ms=ts_ms, source="trade_attribution_ledger")
        except Exception as e:
            _warn_nonfatal("API_TERMINAL_ATTRIBUTION_OVERLAY_BUILD_FAILED", e, once_key="attribution_overlay_build")


def _append_policy_risk_cap_overlays(con, symbol: str, markers: List[Dict[str, Any]], price_lines: List[Dict[str, Any]], limit: int) -> None:
    if _table_exists(con, "execution_policy_audit"):
        cols = _table_columns(con, "execution_policy_audit")
        if "symbol" in cols and "ts_ms" in cols:
            try:
                select = ", ".join([
                    _column_expr(cols, ["id"], "id", "NULL"),
                    _column_expr(cols, ["ts_ms"], "ts_ms", "0"),
                    _column_expr(cols, ["side"], "side", "NULL"),
                    _column_expr(cols, ["qty"], "qty", "0"),
                    _column_expr(cols, ["decision_json"], "decision_json", "NULL"),
                    _column_expr(cols, ["policy_json"], "policy_json", "NULL"),
                    _column_expr(cols, ["suppression_state"], "suppression_state", "NULL"),
                    _column_expr(cols, ["source_alert_id"], "source_alert_id", "NULL"),
                ])
                rows = con.execute(
                    f"""
                    SELECT {select}
                      FROM execution_policy_audit
                     WHERE UPPER(symbol)=?
                     ORDER BY ts_ms DESC
                     LIMIT ?
                    """,
                    (str(symbol), int(limit)),
                ).fetchall()
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_POLICY_CAP_OVERLAYS_READ_FAILED", e, once_key="policy_cap_overlays_read")
                rows = []
            for row in _rows_to_dicts(rows):
                try:
                    decision = _json_loads(row.get("decision_json")) or {}
                    policy = _json_loads(row.get("policy_json")) or {}
                    size_scale = _safe_float((decision or {}).get("size_scale"), _safe_float((policy or {}).get("size_scale"), 1.0))
                    tse_obj = decision.get("tse") if isinstance(decision, dict) else None
                    tse_state = tse_obj.get("state") if isinstance(tse_obj, dict) else ""
                    suppression_state = str(row.get("suppression_state") or tse_state or "").upper().strip()
                    if not (size_scale is not None and size_scale < 0.999) and suppression_state not in ("SIZE_COMPRESSION", "SOFT_THROTTLE"):
                        continue
                    ts_ms = _safe_int(row.get("ts_ms"), 0)
                    reason = "execution_policy_size_scaled"
                    if suppression_state:
                        reason = f"trade_suppression_{suppression_state.lower()}"
                    side = _side_from_payload(row, decision if isinstance(decision, dict) else {}, policy if isinstance(policy, dict) else {})
                    qty = _safe_float(row.get("qty"), 0.0) or 0.0
                    px = _find_number([decision, policy], {"expected_price", "expected_px", "entry_price", "entry_px"})
                    _add_marker(
                        markers,
                        ts_ms=ts_ms,
                        symbol=symbol,
                        kind="risk_capped",
                        side=side,
                        qty=qty,
                        size=abs(qty),
                        price=px,
                        text="CAP",
                        reason_code=reason,
                        reason=reason,
                        source="execution_policy_audit",
                        source_id=row.get("id"),
                        source_alert_id=row.get("source_alert_id"),
                    )
                    _add_price_lines_from_payload(price_lines, [decision, policy], ts_ms=ts_ms, source="execution_policy_audit")
                except Exception as e:
                    _warn_nonfatal("API_TERMINAL_POLICY_CAP_OVERLAY_BUILD_FAILED", e, once_key="policy_cap_overlay_build")

    if not _table_exists(con, "execution_orders"):
        return
    cols = _table_columns(con, "execution_orders")
    if "symbol" not in cols or "submit_ts_ms" not in cols or "extra_json" not in cols:
        return
    try:
        select = ", ".join([
            _column_expr(cols, ["client_order_id"], "client_order_id", "NULL"),
            _column_expr(cols, ["submit_ts_ms"], "submit_ts_ms", "0"),
            _column_expr(cols, ["qty"], "qty", "0"),
            _column_expr(cols, ["ref_px"], "ref_px", "NULL"),
            _column_expr(cols, ["expected_px"], "expected_px", "NULL"),
            _column_expr(cols, ["mid_px"], "mid_px", "NULL"),
            _column_expr(cols, ["source_alert_id"], "source_alert_id", "NULL"),
            _column_expr(cols, ["extra_json"], "extra_json", "NULL"),
        ])
        rows = con.execute(
            f"""
            SELECT {select}
              FROM execution_orders
             WHERE UPPER(symbol)=?
             ORDER BY submit_ts_ms DESC
             LIMIT ?
            """,
            (str(symbol), int(limit)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_EXECUTION_ORDER_CAP_OVERLAYS_READ_FAILED", e, once_key="execution_order_cap_overlays_read")
        return
    for row in _rows_to_dicts(rows):
        try:
            extra = _json_loads(row.get("extra_json")) or {}
            caps = extra.get("portfolio_risk_caps") if isinstance(extra, dict) else None
            if not isinstance(caps, dict):
                continue
            scale = _safe_float(caps.get("scale"), 1.0)
            if not bool(caps.get("scaled")) and not (scale is not None and scale < 0.999):
                continue
            ts_ms = _safe_int(row.get("submit_ts_ms"), 0)
            qty = _safe_float(row.get("qty"), 0.0) or 0.0
            px = _safe_float(row.get("ref_px"), _safe_float(row.get("expected_px"), _safe_float(row.get("mid_px"), None)))
            side = "BUY" if qty > 0 else "SELL"
            _add_marker(
                markers,
                ts_ms=ts_ms,
                symbol=symbol,
                kind="risk_capped",
                side=side,
                qty=qty,
                size=abs(qty),
                price=px,
                text="CAP",
                reason_code="portfolio_risk_cap_scaled",
                reason=f"portfolio risk cap scale {scale}",
                source="execution_orders",
                source_id=row.get("client_order_id"),
                source_alert_id=row.get("source_alert_id"),
            )
            caps_payload = caps.get("caps") if isinstance(caps.get("caps"), dict) else {}
            symbol_cap = _safe_float(caps_payload.get("symbol_concentration_cap"), None)
            if symbol_cap is not None and abs(qty) > 1e-12:
                _add_price_line(
                    price_lines,
                    price=float(symbol_cap) / abs(float(qty)),
                    kind="cap",
                    title="Symbol cap",
                    reason_code="symbol_concentration_cap",
                    ts_ms=ts_ms,
                    source="execution_orders",
                )
            _add_price_lines_from_payload(price_lines, extra, ts_ms=ts_ms, source="execution_orders")
        except Exception as e:
            _warn_nonfatal("API_TERMINAL_EXECUTION_ORDER_CAP_OVERLAY_BUILD_FAILED", e, once_key="execution_order_cap_overlay_build")


def _append_position_price_lines(con, symbol: str, price_lines: List[Dict[str, Any]]) -> None:
    if _table_exists(con, "broker_positions"):
        cols = _table_columns(con, "broker_positions")
        if "symbol" in cols:
            ts_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else "0")
            avg_col = "avg_px" if "avg_px" in cols else ("avg_price" if "avg_price" in cols else None)
            qty_col = "qty" if "qty" in cols else None
            if avg_col:
                try:
                    row = con.execute(
                        f"""
                        SELECT {avg_col} AS avg_px, {qty_col or '0'} AS qty, {ts_col} AS ts_ms
                          FROM broker_positions
                         WHERE UPPER(symbol)=?
                         ORDER BY {ts_col} DESC
                         LIMIT 1
                        """,
                        (str(symbol),),
                    ).fetchone()
                    if row:
                        d = dict(row) if hasattr(row, "keys") else {"avg_px": row[0], "qty": row[1], "ts_ms": row[2]}
                        if abs(_safe_float(d.get("qty"), 0.0) or 0.0) > 1e-12:
                            _add_price_line(price_lines, price=d.get("avg_px"), kind="average_cost", title="Average cost", reason_code="broker_average_cost", ts_ms=_safe_int(d.get("ts_ms"), 0), source="broker_positions")
                except Exception as e:
                    _warn_nonfatal("API_TERMINAL_POSITION_PRICE_LINE_FAILED", e, once_key="position_price_line")
    if _table_exists(con, "model_position_state"):
        cols = _table_columns(con, "model_position_state")
        if {"symbol", "avg_entry_price"}.issubset(cols):
            try:
                row = con.execute(
                    """
                    SELECT avg_entry_price, net_qty, last_update_ts_ms
                      FROM model_position_state
                     WHERE UPPER(symbol)=?
                       AND ABS(COALESCE(net_qty, 0)) > 0
                     ORDER BY last_update_ts_ms DESC
                     LIMIT 1
                    """,
                    (str(symbol),),
                ).fetchone()
                if row:
                    _add_price_line(price_lines, price=row[0], kind="average_cost", title="Model avg cost", reason_code="model_average_cost", ts_ms=_safe_int(row[2], 0), source="model_position_state")
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_MODEL_POSITION_PRICE_LINE_FAILED", e, once_key="model_position_price_line")


def _append_window_overlays(con, markers: List[Dict[str, Any]], windows: List[Dict[str, Any]]) -> None:
    if _table_exists(con, "kill_switch_audit"):
        cols = _table_columns(con, "kill_switch_audit")
        if {"ts_ms", "enabled", "scope", "key"}.issubset(cols):
            try:
                select = ", ".join([
                    _column_expr(cols, ["ts_ms"], "ts_ms", "0"),
                    _column_expr(cols, ["enabled"], "enabled", "0"),
                    _column_expr(cols, ["scope"], "scope", "'global'"),
                    _column_expr(cols, ["key"], "key", "'global'"),
                    _column_expr(cols, ["reason"], "reason", "NULL"),
                    _column_expr(cols, ["meta_json"], "meta_json", "NULL"),
                ])
                rows = con.execute(
                    f"SELECT {select} FROM kill_switch_audit ORDER BY ts_ms ASC LIMIT 500"
                ).fetchall()
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_KILL_SWITCH_AUDIT_WINDOWS_READ_FAILED", e, once_key="kill_switch_audit_windows_read")
                rows = []
            active: Dict[str, Dict[str, Any]] = {}
            for row in _rows_to_dicts(rows):
                key = f"{row.get('scope') or 'global'}:{row.get('key') or 'global'}"
                enabled = _safe_int(row.get("enabled"), 0) == 1
                if enabled:
                    active[key] = row
                    continue
                prev = active.pop(key, None)
                if prev:
                    _add_window(
                        windows,
                        markers,
                        kind="kill_switch_window",
                        start_ts_ms=prev.get("ts_ms"),
                        end_ts_ms=row.get("ts_ms"),
                        reason_code=f"kill_switch_{prev.get('scope') or 'global'}",
                        reason=str(prev.get("reason") or ""),
                        label="Kill switch",
                        source="kill_switch_audit",
                    )
            for prev in active.values():
                _add_window(
                    windows,
                    markers,
                    kind="kill_switch_window",
                    start_ts_ms=prev.get("ts_ms"),
                    end_ts_ms=None,
                    reason_code=f"kill_switch_{prev.get('scope') or 'global'}",
                    reason=str(prev.get("reason") or ""),
                    label="Kill switch",
                    source="kill_switch_audit",
                )

    if _table_exists(con, "kill_switch_state"):
        cols = _table_columns(con, "kill_switch_state")
        if {"enabled", "scope", "key"}.issubset(cols):
            try:
                select = ", ".join([
                    _column_expr(cols, ["scope"], "scope", "'global'"),
                    _column_expr(cols, ["key"], "key", "'global'"),
                    _column_expr(cols, ["enabled"], "enabled", "0"),
                    _column_expr(cols, ["reason"], "reason", "NULL"),
                    _column_expr(cols, ["meta_json"], "meta_json", "NULL"),
                    _column_expr(cols, ["created_ts_ms"], "created_ts_ms", "0"),
                    _column_expr(cols, ["updated_ts_ms"], "updated_ts_ms", "0"),
                ])
                rows = con.execute(f"SELECT {select} FROM kill_switch_state WHERE enabled=1 LIMIT 100").fetchall()
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_KILL_SWITCH_STATE_WINDOWS_READ_FAILED", e, once_key="kill_switch_state_windows_read")
                rows = []
            for row in _rows_to_dicts(rows):
                meta = _json_loads(row.get("meta_json")) or {}
                _add_window(
                    windows,
                    markers,
                    kind="kill_switch_window",
                    start_ts_ms=_safe_int(row.get("created_ts_ms"), 0) or _safe_int(row.get("updated_ts_ms"), 0),
                    end_ts_ms=(meta.get("until_ts_ms") if isinstance(meta, dict) else None),
                    reason_code=f"kill_switch_{row.get('scope') or 'global'}",
                    reason=str(row.get("reason") or ""),
                    label="Kill switch",
                    source="kill_switch_state",
                )

    if _table_exists(con, "trade_suppression_audit"):
        cols = _table_columns(con, "trade_suppression_audit")
        if {"ts_ms", "state", "action"}.issubset(cols):
            try:
                select = ", ".join([
                    _column_expr(cols, ["ts_ms"], "ts_ms", "0"),
                    _column_expr(cols, ["state"], "state", "'NONE'"),
                    _column_expr(cols, ["action"], "action", "'NONE'"),
                    _column_expr(cols, ["reason"], "reason", "NULL"),
                    _column_expr(cols, ["hard_block"], "hard_block", "0"),
                ])
                rows = con.execute(f"SELECT {select} FROM trade_suppression_audit ORDER BY ts_ms ASC LIMIT 500").fetchall()
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_SUPPRESSION_WINDOWS_READ_FAILED", e, once_key="suppression_windows_read")
                rows = []
            current = None
            for row in _rows_to_dicts(rows):
                state = str(row.get("state") or row.get("action") or "NONE").upper().strip()
                active = state not in ("", "NONE", "NORMAL")
                if active and current is None:
                    current = row
                    continue
                if active and current is not None and str(current.get("state") or "").upper() == state:
                    continue
                if current is not None:
                    cur_state = str(current.get("state") or current.get("action") or "").upper()
                    _add_window(
                        windows,
                        markers,
                        kind=("suppression_hard_block_window" if _safe_int(current.get("hard_block"), 0) == 1 or cur_state == "HARD_BLOCK" else "suppression_window"),
                        start_ts_ms=current.get("ts_ms"),
                        end_ts_ms=row.get("ts_ms"),
                        reason_code=f"trade_suppression_{cur_state.lower()}",
                        reason=str(current.get("reason") or ""),
                        label="Trade suppression",
                        source="trade_suppression_audit",
                    )
                    current = row if active else None
            if current is not None:
                cur_state = str(current.get("state") or current.get("action") or "").upper()
                _add_window(
                    windows,
                    markers,
                    kind=("suppression_hard_block_window" if _safe_int(current.get("hard_block"), 0) == 1 or cur_state == "HARD_BLOCK" else "suppression_window"),
                    start_ts_ms=current.get("ts_ms"),
                    end_ts_ms=None,
                    reason_code=f"trade_suppression_{cur_state.lower()}",
                    reason=str(current.get("reason") or ""),
                    label="Trade suppression",
                    source="trade_suppression_audit",
                )

    if _table_exists(con, "portfolio_risk_snapshots"):
        cols = _table_columns(con, "portfolio_risk_snapshots")
        if {"ts_ms", "blocked"}.issubset(cols):
            try:
                select = ", ".join([
                    _column_expr(cols, ["ts_ms"], "ts_ms", "0"),
                    _column_expr(cols, ["blocked"], "blocked", "0"),
                    _column_expr(cols, ["drawdown"], "drawdown", "NULL"),
                    _column_expr(cols, ["info_json"], "info_json", "NULL"),
                ])
                rows = con.execute(f"SELECT {select} FROM portfolio_risk_snapshots ORDER BY ts_ms ASC LIMIT 500").fetchall()
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_PORTFOLIO_RISK_WINDOWS_READ_FAILED", e, once_key="portfolio_risk_windows_read")
                rows = []
            current = None
            for row in _rows_to_dicts(rows):
                active = _safe_int(row.get("blocked"), 0) == 1
                if active and current is None:
                    current = row
                    continue
                if not active and current is not None:
                    info = _json_loads(current.get("info_json")) or {}
                    reason = (info.get("block_reason") if isinstance(info, dict) else None) or {}
                    reason_code = _stable_reason_code((reason or {}).get("type") if isinstance(reason, dict) else "portfolio_risk_blocked", "portfolio_risk_blocked")
                    _add_window(
                        windows,
                        markers,
                        kind="drawdown_throttle_window",
                        start_ts_ms=current.get("ts_ms"),
                        end_ts_ms=row.get("ts_ms"),
                        reason_code=reason_code,
                        reason=json.dumps(reason, separators=(",", ":"), sort_keys=True) if isinstance(reason, dict) and reason else reason_code,
                        label="Drawdown throttle",
                        source="portfolio_risk_snapshots",
                    )
                    current = None
            if current is not None:
                info = _json_loads(current.get("info_json")) or {}
                reason = (info.get("block_reason") if isinstance(info, dict) else None) or {}
                reason_code = _stable_reason_code((reason or {}).get("type") if isinstance(reason, dict) else "portfolio_risk_blocked", "portfolio_risk_blocked")
                _add_window(
                    windows,
                    markers,
                    kind="drawdown_throttle_window",
                    start_ts_ms=current.get("ts_ms"),
                    end_ts_ms=None,
                    reason_code=reason_code,
                    reason=json.dumps(reason, separators=(",", ":"), sort_keys=True) if isinstance(reason, dict) and reason else reason_code,
                    label="Drawdown throttle",
                    source="portfolio_risk_snapshots",
                )

    if _table_exists(con, "risk_events"):
        cols = _table_columns(con, "risk_events")
        if {"ts_ms", "trigger_type"}.issubset(cols):
            try:
                select = ", ".join([
                    _column_expr(cols, ["ts_ms"], "ts_ms", "0"),
                    _column_expr(cols, ["trigger_type"], "trigger_type", "'risk_event'"),
                    _column_expr(cols, ["reason"], "reason", "NULL"),
                ])
                rows = con.execute(f"SELECT {select} FROM risk_events ORDER BY ts_ms DESC LIMIT 200").fetchall()
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_RISK_EVENT_WINDOWS_READ_FAILED", e, once_key="risk_event_windows_read")
                rows = []
            for row in _rows_to_dicts(rows):
                trigger = _stable_reason_code(row.get("trigger_type"), "risk_event")
                kind = "circuit_breaker_window" if "circuit" in trigger else ("drawdown_throttle_window" if "drawdown" in trigger else "risk_event_window")
                _add_window(
                    windows,
                    markers,
                    kind=kind,
                    start_ts_ms=row.get("ts_ms"),
                    end_ts_ms=None,
                    reason_code=trigger,
                    reason=str(row.get("reason") or trigger),
                    label=("Circuit breaker" if kind == "circuit_breaker_window" else "Risk event"),
                    source="risk_events",
                )


def _decision_overlay_summary(markers: List[Dict[str, Any]], windows: List[Dict[str, Any]], price_lines: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    for marker in markers:
        kind = str(marker.get("kind") or "event")
        counts[kind] = int(counts.get(kind, 0)) + 1
    text_parts = []
    for kind, label in (
        ("filled", "filled"),
        ("intended", "intended"),
        ("suppressed", "suppressed"),
        ("blocked", "blocked"),
        ("risk_capped", "risk-capped"),
    ):
        if counts.get(kind):
            text_parts.append(f"{counts[kind]} {label}")
    if windows:
        text_parts.append(f"{len(windows)} windows")
    if price_lines:
        text_parts.append(f"{len(price_lines)} price levels")
    return {
        "counts": counts,
        "text": ", ".join(text_parts) if text_parts else "No automated decision overlays available for this symbol.",
    }


def _build_terminal_decision_overlay_payload(con, symbol: str, *, limit: int = 2000) -> Dict[str, Any]:
    markers: List[Dict[str, Any]] = []
    price_lines: List[Dict[str, Any]] = []
    windows: List[Dict[str, Any]] = []

    _append_fill_overlays(con, symbol, markers, price_lines, limit)
    _append_intent_overlays(con, symbol, markers, price_lines, limit)
    _append_attribution_overlays(con, symbol, markers, price_lines, limit)
    _append_policy_risk_cap_overlays(con, symbol, markers, price_lines, limit)
    _append_position_price_lines(con, symbol, price_lines)
    _append_window_overlays(con, markers, windows)

    try:
        markers.sort(key=lambda m: (int(m.get("ts") or m.get("t") or 0), str(m.get("kind") or "")))
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_DECISION_MARKERS_SORT_FAILED", e, once_key="decision_markers_sort")
    try:
        windows.sort(key=lambda w: int(w.get("start_ts_ms") or 0))
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_DECISION_WINDOWS_SORT_FAILED", e, once_key="decision_windows_sort")

    for line in price_lines:
        line.pop("_dedupe_key", None)

    summary = _decision_overlay_summary(markers, windows, price_lines)
    legend = [
        {"kind": "filled", "label": "Filled", "shape": "arrow", "text": "FILL B/S", "color": "#56B4E9/#D55E00"},
        {"kind": "intended", "label": "Intended", "shape": "circle", "text": "INTENT", "color": "#009E73"},
        {"kind": "suppressed", "label": "Suppressed", "shape": "square", "text": "SUPP", "color": "#E69F00"},
        {"kind": "blocked", "label": "Blocked", "shape": "arrowDown", "text": "BLOCK", "color": "#73B7E6"},
        {"kind": "risk_capped", "label": "Risk capped", "shape": "arrowUp", "text": "CAP", "color": "#CC79A7"},
    ]
    return {
        "ok": True,
        "symbol": symbol,
        "markers": markers,
        "price_lines": price_lines,
        "windows": windows,
        "legend": legend,
        "summary": summary,
        "meta": {
            "ready": True,
            "count": int(len(markers)),
            "markers_count": int(len(markers)),
            "price_lines_count": int(len(price_lines)),
            "windows_count": int(len(windows)),
            "reason_codes": sorted({str(m.get("reason_code") or "") for m in markers if m.get("reason_code")}),
        },
    }


def _terminal_decision_overlay_response(parsed: Any) -> Dict[str, Any]:
    con = connect_ro()
    try:
        q = _qs(parsed)
        symbol = (q.get("symbol") or "").strip().upper()
        if not symbol:
            return {"ok": False, "error": "missing_symbol", "markers": [], "price_lines": [], "windows": [], "meta": {"ready": False}}
        try:
            limit = max(1, min(5000, int((q.get("limit") or "2000").strip())))
        except Exception:
            limit = 2000
        return _build_terminal_decision_overlay_payload(con, symbol, limit=int(limit))
    finally:
        _close_ro_connection(con)


def api_get_terminal_markers(parsed: Any, _ctx=None) -> Dict[str, Any]:
    return _terminal_decision_overlay_response(parsed)


def api_get_terminal_decision_overlays(parsed: Any, _ctx=None) -> Dict[str, Any]:
    return _terminal_decision_overlay_response(parsed)


def api_get_terminal_snapshot(parsed: Any, _ctx=None) -> Dict[str, Any]:
    # Single call for terminal boot
    started_at = time.perf_counter()

    barrier = _terminal_execution_barrier_snapshot()
    watch = api_get_terminal_watchlist(parsed, _ctx)
    pos = api_get_terminal_positions(parsed, _ctx)
    ords = api_get_terminal_orders(parsed, _ctx)
    fills = api_get_terminal_fills(parsed, _ctx)
    eq = api_get_terminal_equity(parsed, _ctx)

    return {
        "ok": True,
        "ts_ms": int(time.time() * 1000),
        "latency_ms": int((time.perf_counter() - started_at) * 1000),
        "watchlist": watch.get("symbols") if isinstance(watch, dict) else [],
        "positions": (pos.get("rows") if isinstance(pos, dict) else []),
        "orders": (ords.get("data") if isinstance(ords, dict) else {"broker": [], "portfolio": []}),
        "fills": (fills.get("rows") if isinstance(fills, dict) else []),
        "equity": {"account": eq.get("account"), "series": eq.get("series")} if isinstance(eq, dict) else {"account": None, "series": []},
        "execution_barrier": barrier,
    }
