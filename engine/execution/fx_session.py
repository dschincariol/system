"""FX execution-session state derived from the canonical FX clock.

FX-04 owns ``engine.data.prices.fx_clock`` as the session-boundary source of
truth. This module derives weekend-open/closed state from that clock and only
adds execution-specific rollover timing annotations. The default FX week opens
Sunday 17:00 America/New_York and closes Friday 17:00 America/New_York, roughly
21:00 UTC during US daylight time and 22:00 UTC during US standard time.

No broker, database, network, order, swap, or P&L operation is performed here.
FX-07 owns spread/swap/carry cost accounting.
"""

from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta, timezone
import os
from typing import Any, Dict
from zoneinfo import ZoneInfo

try:
    from engine.data.prices.fx_clock import (
        DEFAULT_FX_WEEK_CLOSE_HOUR_ET,
        fx_forward_eval_ms as _fx_forward_eval_ms,
        fx_market_closed as _fx_market_closed,
    )

    _HAS_CANONICAL_FX_CLOCK = True
except Exception:  # pragma: no cover - only used if FX-04 is absent.
    DEFAULT_FX_WEEK_CLOSE_HOUR_ET = 17
    _fx_forward_eval_ms = None
    _fx_market_closed = None
    _HAS_CANONICAL_FX_CLOCK = False

_UTC = timezone.utc
_FALLBACK_ET = timezone(timedelta(hours=-5), name="ET_FIXED")


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.environ.get(name, str(default)) or str(default)).strip())
    except Exception:
        value = int(default)
    return max(int(minimum), min(int(maximum), int(value)))


def _ny_tz():
    try:
        return ZoneInfo("America/New_York")
    except Exception:  # pragma: no cover - defensive fallback only.
        return _FALLBACK_ET


def _dt_from_ms(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=_UTC)


def _ms_from_dt(value: datetime) -> int:
    return int(value.astimezone(_UTC).timestamp() * 1000)


def _is_fx_pair(symbol: str) -> bool:
    try:
        from engine.data.fx_instrument import parse_fx_symbol

        parsed = parse_fx_symbol(symbol)
        return bool(parsed is not None and parsed.base_ccy and parsed.quote_ccy and str(parsed.instrument_kind or "") == "fx_spot")
    except Exception:
        # no-op-guard: allow - FX-02 parser absence falls through to asset-map fallback.
        pass

    try:
        from engine.data.asset_map import asset_class_for_symbol

        text = str(symbol or "").upper().strip().replace("/", "").replace("_", "")
        return bool(asset_class_for_symbol(text) == "FX" and len(text) == 6 and text.isalpha() and text[:3] != text[3:])
    except Exception:
        return False


def _fallback_closed_utc(ts_ms: int) -> bool:
    dt = _dt_from_ms(ts_ms)
    close_day = _env_int("FX_WEEK_CLOSE_DAY_UTC", 4, 0, 6)
    open_day = _env_int("FX_WEEK_OPEN_DAY_UTC", 6, 0, 6)
    close_hour = _env_int("FX_WEEK_CLOSE_HOUR_UTC", 22, 0, 23)
    open_hour = _env_int("FX_WEEK_OPEN_HOUR_UTC", 22, 0, 23)
    weekday = int(dt.weekday())
    hour = int(dt.hour)
    if weekday == close_day and hour >= close_hour:
        return True
    if close_day < open_day and close_day < weekday < open_day:
        return True
    if weekday == open_day and hour < open_hour:
        return True
    return False


def _fallback_next_open_utc(ts_ms: int) -> int | None:
    if not _fallback_closed_utc(ts_ms):
        return None
    dt = _dt_from_ms(ts_ms)
    open_day = _env_int("FX_WEEK_OPEN_DAY_UTC", 6, 0, 6)
    open_hour = _env_int("FX_WEEK_OPEN_HOUR_UTC", 22, 0, 23)
    days = (open_day - int(dt.weekday())) % 7
    candidate = datetime.combine((dt + timedelta(days=days)).date(), dt_time(hour=open_hour, tzinfo=_UTC))
    if candidate <= dt:
        candidate += timedelta(days=7)
    return _ms_from_dt(candidate)


def _market_closed(ts_ms: int) -> bool:
    if _HAS_CANONICAL_FX_CLOCK and _fx_market_closed is not None:
        return bool(_fx_market_closed(int(ts_ms)))
    return _fallback_closed_utc(int(ts_ms))


def _next_open_ms(ts_ms: int) -> int | None:
    if not _market_closed(ts_ms):
        return None
    if _HAS_CANONICAL_FX_CLOCK and _fx_forward_eval_ms is not None:
        return int(_fx_forward_eval_ms(int(ts_ms), 0))
    return _fallback_next_open_utc(int(ts_ms))


def _rollover_window(now_ms: int) -> tuple[bool, int, int]:
    tz = _ny_tz()
    local = _dt_from_ms(now_ms).astimezone(tz)
    default_hour = _env_int("FX_WEEK_CLOSE_HOUR_ET", int(DEFAULT_FX_WEEK_CLOSE_HOUR_ET), 0, 23)
    hour = _env_int("FX_ROLLOVER_HOUR_ET", default_hour, 0, 23)
    minute = _env_int("FX_ROLLOVER_START_MINUTE_ET", 0, 0, 59)
    duration_min = _env_int("FX_ROLLOVER_DURATION_MINUTES", 60, 0, 240)
    start = datetime.combine(local.date(), dt_time(hour=hour, minute=minute, tzinfo=tz))
    end = start + timedelta(minutes=int(duration_min))
    return bool(duration_min > 0 and start <= local < end), _ms_from_dt(start), _ms_from_dt(end)


def fx_session_state(symbol: str, now_ms: int) -> Dict[str, Any]:
    """Return pure FX execution-session state for ``symbol`` at ``now_ms``."""

    is_fx = _is_fx_pair(symbol)
    if not is_fx:
        return {
            "is_fx": False,
            "session": "open",
            "is_open": True,
            "in_rollover_window": False,
            "next_open_ms": None,
            "canonical_clock": bool(_HAS_CANONICAL_FX_CLOCK),
        }

    closed = _market_closed(int(now_ms))
    in_rollover, rollover_start_ms, rollover_end_ms = _rollover_window(int(now_ms))
    if closed:
        return {
            "is_fx": True,
            "session": "weekend_closed",
            "is_open": False,
            "in_rollover_window": False,
            "next_open_ms": _next_open_ms(int(now_ms)),
            "canonical_clock": bool(_HAS_CANONICAL_FX_CLOCK),
        }
    return {
        "is_fx": True,
        "session": "rollover" if in_rollover else "open",
        "is_open": True,
        "in_rollover_window": bool(in_rollover),
        "next_open_ms": None,
        "rollover_start_ms": int(rollover_start_ms),
        "rollover_end_ms": int(rollover_end_ms),
        "canonical_clock": bool(_HAS_CANONICAL_FX_CLOCK),
    }


def fx_timing_adjustment(symbol: str, now_ms: int, base_decision: Dict[str, Any]) -> Dict[str, Any]:
    """Return an FX-session-adjusted execution decision.

    Non-FX decisions are returned unchanged. Weekend-closed FX orders are marked
    for policy-layer suppression. Rollover FX orders are biased toward passive
    limit timing with extra delay; no swap/carry cost is calculated here.
    """

    state = fx_session_state(symbol, now_ms)
    if not bool(state.get("is_fx")):
        return dict(base_decision or {})

    out = dict(base_decision or {})
    out["fx_session"] = dict(state)
    if not bool(state.get("is_open")):
        out["fx_session_blocked"] = True
        out["fx_session_reason"] = "weekend_closed"
        return out

    if bool(state.get("in_rollover_window")):
        rollover_delay_ms = _env_int("FX_ROLLOVER_ENTRY_DELAY_MS", 60000, 0, 600000)
        rollover_chunk_pct = float(_env_int("FX_ROLLOVER_CHUNK_PCT_BPS", 1500, 100, 10000)) / 10000.0
        try:
            current_delay = int(out.get("entry_delay_ms") or 0)
        except Exception:
            current_delay = 0
        current_chunk = out.get("chunk_pct")
        try:
            chunk_pct = float(current_chunk)
        except Exception:
            chunk_pct = 1.0
        out["fx_rollover_timing_bias"] = True
        out["fx_session_blocked"] = False
        out["order_type"] = "LIMIT"
        out["aggressiveness"] = "PASSIVE"
        out["execution_policy"] = "passive"
        out["entry_strategy"] = "rollover_passive_limit"
        out["entry_delay_ms"] = int(max(current_delay, rollover_delay_ms))
        out["chunk_pct"] = float(min(max(0.01, chunk_pct), rollover_chunk_pct))
        out["latency_mult"] = float(max(float(out.get("latency_mult") or 1.0), 1.5))
    return out


__all__ = ["fx_session_state", "fx_timing_adjustment"]
