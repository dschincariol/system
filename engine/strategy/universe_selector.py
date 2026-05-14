"""
FILE: universe_selector.py

Strategy-facing universe selection helpers.
"""

from __future__ import annotations

import json
import os
from typing import Any, List

from engine.data.universe import get_active_symbols
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect


MODEL_UNIVERSE_LOOKBACK_S = int(os.environ.get("MODEL_UNIVERSE_LOOKBACK_S", "21600"))
LOG = get_logger("engine.strategy.universe_selector")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.strategy.universe_selector",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_json_obj(raw) -> dict:
    if isinstance(raw, dict):
        return dict(raw)
    if not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw or "{}")
    except Exception as e:
        _warn_nonfatal("UNIVERSE_SELECTOR_JSON_PARSE_FAILED", e, once_key="safe_json_obj_parse")
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _extract_model_universe_hint(explain_json: str) -> bool:
    explain = _safe_json_obj(explain_json)
    if not explain:
        return False
    candidates = [explain]
    for key in ("model_intent", "model_output", "portfolio_decision", "trade_decision", "decision", "signal"):
        val = explain.get(key)
        if isinstance(val, dict):
            candidates.append(dict(val))
    for item in candidates:
        if not isinstance(item, dict):
            continue
        for key in ("include_in_universe", "universe_include", "promote_symbol"):
            val = item.get(key)
            if isinstance(val, bool):
                return val
            if val is not None and str(val).strip().lower() in ("1", "true", "yes", "y", "on"):
                return True
        for key in ("universe_score", "universe_rank", "rank"):
            try:
                return _safe_float(item.get(key)) > 0.0
            except Exception as e:
                _warn_nonfatal("UNIVERSE_SELECTOR_HINT_PARSE_FAILED", e, once_key=f"hint_parse_{key}", key=str(key))
        for key in ("target_weight", "portfolio_weight", "position_size"):
            try:
                return abs(_safe_float(item.get(key))) > 0.0
            except Exception as e:
                _warn_nonfatal("UNIVERSE_SELECTOR_HINT_PARSE_FAILED", e, once_key=f"hint_parse_{key}", key=str(key))
    return False


def _load_recent_model_symbols(con, limit: int) -> List[str]:
    if int(limit or 0) <= 0:
        return []
    cutoff_ms = None
    if MODEL_UNIVERSE_LOOKBACK_S > 0:
        import time
        cutoff_ms = int(time.time() * 1000) - int(MODEL_UNIVERSE_LOOKBACK_S) * 1000
    try:
        if cutoff_ms is None:
            rows = con.execute(
                """
                SELECT symbol, explain_json
                FROM alerts
                ORDER BY ts_ms DESC
                LIMIT 500
                """
            ).fetchall() or []
        else:
            rows = con.execute(
                """
                SELECT symbol, explain_json
                FROM alerts
                WHERE ts_ms >= ?
                ORDER BY ts_ms DESC
                LIMIT 500
                """,
                (int(cutoff_ms),),
            ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("UNIVERSE_SELECTOR_MODEL_SYMBOL_LOAD_FAILED", e, once_key="load_recent_model_symbols")
        return []

    out: List[str] = []
    seen = set()
    for symbol, explain_json in rows:
        sym = str(symbol or "").upper().strip()
        if not sym or sym in seen:
            continue
        if not _extract_model_universe_hint(str(explain_json or "{}")):
            continue
        seen.add(sym)
        out.append(sym)
        if len(out) >= int(limit):
            break
    return out


def select_active_universe(limit: int = 25) -> List[str]:
    """
    Return the current trading universe for strategy-side consumers.

    This is intentionally a thin adapter over the canonical symbols table so
    challenger/champion logic does not fork universe ownership.
    """
    con = connect()
    try:
        symbols = get_active_symbols(con, limit=max(1, int(limit or 0)))
        if len(symbols or []) < max(1, int(limit or 0)):
            extra = _load_recent_model_symbols(con, max(1, int(limit or 0)) - len(symbols or []))
            if extra:
                symbols = list(symbols or []) + list(extra)
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("UNIVERSE_SELECTOR_CLOSE_FAILED", e, once_key="select_active_universe_close")

    clean = []
    seen = set()
    for symbol in symbols or []:
        sym = str(symbol or "").upper().strip()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        clean.append(sym)
    return clean
