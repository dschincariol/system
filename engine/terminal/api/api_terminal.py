"""Read-mostly API handlers that back the browser trading terminal.

These endpoints expose watchlists, snapshots, positions, orders, fills, equity,
and chart markers while degrading safely to empty payloads when optional tables
or newer runtime state are not available.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List

from engine.api.http_parsing import qs as _qs
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.gates import execution_gate_snapshot
from engine.runtime.storage import connect_ro

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
        gate = execution_gate_snapshot()
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


def api_get_terminal_positions(_parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
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


def api_get_terminal_orders(parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
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
    out = {"broker": [], "portfolio": []}

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

    return {"ok": True, "data": out}


def api_get_terminal_fills(parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
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


def api_get_terminal_equity(parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
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


def api_get_terminal_markers(parsed: Any, _ctx=None) -> Dict[str, Any]:
    con = connect_ro()
    q = _qs(parsed)

    symbol = (q.get("symbol") or "").strip().upper()
    if not symbol:
        return {"ok": False, "error": "missing_symbol", "markers": [], "meta": {"ready": False}}

    # Markers are a UI convenience layer built from fills/orders rather than a
    # dedicated canonical table. Missing markers should never break the terminal.
    markers: List[Dict[str, Any]] = []
    fills_table = "broker_fills_v2" if _table_exists(con, "broker_fills_v2") else ("broker_fills" if _table_exists(con, "broker_fills") else None)

    if fills_table:
        try:
            rows = con.execute(
                f"""
                SELECT ts_ms, qty, px
                  FROM {fills_table}
                 WHERE symbol=?
                 ORDER BY ts_ms DESC
                 LIMIT 2000
                """,
                (str(symbol),),
            ).fetchall()
            for r in rows or []:
                try:
                    ts_ms = int(r[0] or 0)
                    ts = int(ts_ms // 1000)
                    qty = float(r[1] or 0.0)
                    px = float(r[2] or 0.0)
                    side = "BUY" if qty > 0 else "SELL"
                    markers.append({
                        "ts": ts,
                        "ts_ms": ts_ms,
                        "t": ts,
                        "symbol": symbol,
                        "kind": "fill",
                        "side": side,
                        "size": abs(qty),
                        "qty": qty,
                        "price": px,
                        "px": px,
                        "text": side,
                    })
                except Exception as e:
                    _warn_nonfatal("API_TERMINAL_FILL_MARKER_BUILD_FAILED", e, once_key="fill_marker_build")
        except Exception as e:
            _warn_nonfatal("API_TERMINAL_FILL_MARKERS_READ_FAILED", e, once_key="fill_markers_read")

    if _table_exists(con, "portfolio_orders"):
        try:
            rows = con.execute(
                """
                SELECT ts_ms, action, from_side, to_side, delta_weight, source_alert_id
                  FROM portfolio_orders
                 WHERE symbol=?
                 ORDER BY ts_ms DESC
                 LIMIT 2000
                """,
                (str(symbol),),
            ).fetchall()
            for r in rows or []:
                try:
                    ts_ms = int(r[0] or 0)
                    ts = int(ts_ms // 1000)
                    action = str(r[1] or "")
                    to_side = str(r[3] or "")
                    dw = float(r[4] or 0.0)
                    sid = r[5]
                    side = (to_side or action or "INTENT").upper()
                    markers.append({
                        "ts": ts,
                        "ts_ms": ts_ms,
                        "t": ts,
                        "symbol": symbol,
                        "kind": "intent",
                        "side": side,
                        "size": abs(dw),
                        "delta_weight": dw,
                        "price": None,
                        "source_alert_id": sid,
                        "text": "INTENT",
                    })
                except Exception as e:
                    _warn_nonfatal("API_TERMINAL_INTENT_MARKER_BUILD_FAILED", e, once_key="intent_marker_build")
        except Exception as e:
            _warn_nonfatal("API_TERMINAL_INTENT_MARKERS_READ_FAILED", e, once_key="intent_markers_read")

    try:
        markers.sort(key=lambda m: int(m.get("ts") or m.get("t") or 0))
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_MARKERS_SORT_FAILED", e, once_key="markers_sort")

    return {
        "ok": True,
        "symbol": symbol,
        "markers": markers,
        "meta": {
            "ready": True,
            "count": int(len(markers)),
        },
    }


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
