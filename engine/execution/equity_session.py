"""Pure US equity regular-session clock and execution timing helper.

This module models NYSE/Nasdaq regular trading hours for execution safety:
09:30 ET inclusive to 16:00 ET exclusive, with exchange holidays and 13:00 ET
early closes on seeded half-days. It performs no broker, database, network,
order, cost, schema, or P&L operation.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, Iterable, Set
from zoneinfo import ZoneInfo

LOG = logging.getLogger(__name__)
_WARNED_TZ_FALLBACK = False
_UTC = timezone.utc
_FALLBACK_ET = timezone(timedelta(hours=-5), name="ET_FIXED")
_TZ_NAME = "America/New_York"

DEFAULT_EQUITY_RTH_OPEN_HOUR_ET = 9
DEFAULT_EQUITY_RTH_OPEN_MIN_ET = 30
DEFAULT_EQUITY_RTH_CLOSE_HOUR_ET = 16
DEFAULT_EQUITY_RTH_CLOSE_MIN_ET = 0
DEFAULT_EQUITY_HALFDAY_CLOSE_HOUR_ET = 13
DEFAULT_EQUITY_HALFDAY_CLOSE_MIN_ET = 0

# Seeded from NYSE/ICE holiday and early-close calendars:
# https://www.nyse.com/markets/hours-calendars
# https://ir.theice.com/press/news-details/2024/NYSE-Group-Announces-2025-2026-and-2027-Holiday-and-Early-Closings-Calendar/default.aspx
# https://ir.theice.com/press/news-details/2025/NYSE-Group-Announces-2026-2027-and-2028-Holiday-and-Early-Closings-Calendar/default.aspx
_SEEDED_HOLIDAYS: frozenset[tuple[int, int, int]] = frozenset(
    {
        (2025, 1, 1),
        (2025, 1, 20),
        (2025, 2, 17),
        (2025, 4, 18),
        (2025, 5, 26),
        (2025, 6, 19),
        (2025, 7, 4),
        (2025, 9, 1),
        (2025, 11, 27),
        (2025, 12, 25),
        (2026, 1, 1),
        (2026, 1, 19),
        (2026, 2, 16),
        (2026, 4, 3),
        (2026, 5, 25),
        (2026, 6, 19),
        (2026, 7, 3),
        (2026, 9, 7),
        (2026, 11, 26),
        (2026, 12, 25),
        (2027, 1, 1),
        (2027, 1, 18),
        (2027, 2, 15),
        (2027, 3, 26),
        (2027, 5, 31),
        (2027, 6, 18),
        (2027, 7, 5),
        (2027, 9, 6),
        (2027, 11, 25),
        (2027, 12, 24),
        (2028, 1, 17),
        (2028, 2, 21),
        (2028, 4, 14),
        (2028, 5, 29),
        (2028, 6, 19),
        (2028, 7, 4),
        (2028, 9, 4),
        (2028, 11, 23),
        (2028, 12, 25),
    }
)
_SEEDED_HALFDAYS: frozenset[tuple[int, int, int]] = frozenset(
    {
        (2025, 7, 3),
        (2025, 11, 28),
        (2025, 12, 24),
        (2026, 11, 27),
        (2026, 12, 24),
        (2027, 11, 26),
        (2028, 7, 3),
        (2028, 11, 24),
    }
)
_SEEDED_YEARS: frozenset[int] = frozenset(
    {year for year, _month, _day in set(_SEEDED_HOLIDAYS).union(set(_SEEDED_HALFDAYS))}
)


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.environ.get(str(name), str(default)) or str(default)).strip())
    except Exception:
        value = int(default)
    return max(int(minimum), min(int(maximum), int(value)))


def _settings() -> tuple[int, int, int, int, int, int]:
    open_hour = _env_int("EQUITY_RTH_OPEN_HOUR_ET", DEFAULT_EQUITY_RTH_OPEN_HOUR_ET, 0, 23)
    open_min = _env_int("EQUITY_RTH_OPEN_MIN_ET", DEFAULT_EQUITY_RTH_OPEN_MIN_ET, 0, 59)
    close_hour = _env_int("EQUITY_RTH_CLOSE_HOUR_ET", DEFAULT_EQUITY_RTH_CLOSE_HOUR_ET, 0, 23)
    close_min = _env_int("EQUITY_RTH_CLOSE_MIN_ET", DEFAULT_EQUITY_RTH_CLOSE_MIN_ET, 0, 59)
    half_hour = _env_int("EQUITY_HALFDAY_CLOSE_HOUR_ET", DEFAULT_EQUITY_HALFDAY_CLOSE_HOUR_ET, 0, 23)
    half_min = _env_int("EQUITY_HALFDAY_CLOSE_MIN_ET", DEFAULT_EQUITY_HALFDAY_CLOSE_MIN_ET, 0, 59)
    return open_hour, open_min, close_hour, close_min, half_hour, half_min


def _ny_tz():
    global _WARNED_TZ_FALLBACK
    try:
        return ZoneInfo(_TZ_NAME)
    except Exception as exc:  # pragma: no cover - defensive fallback only.
        if not _WARNED_TZ_FALLBACK:
            LOG.log(logging.WARNING, "equity_session_zoneinfo_unavailable_fixed_offset_fallback: %s", exc)
            _WARNED_TZ_FALLBACK = True
        return _FALLBACK_ET


def _dt_from_ms(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=_UTC)


def _ms_from_dt(value: datetime) -> int:
    return int(value.astimezone(_UTC).timestamp() * 1000)


def _coerce_date_tuple(value: Any) -> tuple[int, int, int] | None:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = date.fromisoformat(text[:10])
            return int(parsed.year), int(parsed.month), int(parsed.day)
        except Exception:
            return None
    if isinstance(value, (list, tuple)) and len(value) >= 3:
        try:
            year = int(value[0])
            month = int(value[1])
            day = int(value[2])
            return int(date(year, month, day).year), int(month), int(day)
        except Exception:
            return None
    return None


def _json_date_set(env_name: str) -> Set[tuple[int, int, int]]:
    raw = str(os.environ.get(env_name, "") or "").strip()
    if not raw:
        return set()
    try:
        parsed = json.loads(raw)
    except Exception:
        return set()

    items: Iterable[Any]
    if isinstance(parsed, dict):
        items = list(parsed.keys())
    elif isinstance(parsed, list):
        items = parsed
    else:
        return set()

    out: Set[tuple[int, int, int]] = set()
    for item in items:
        coerced = _coerce_date_tuple(item)
        if coerced is not None:
            out.add(coerced)
    return out


def _holiday_set() -> Set[tuple[int, int, int]]:
    return set(_SEEDED_HOLIDAYS).union(_json_date_set("EQUITY_MARKET_HOLIDAYS_JSON"))


def _halfday_set() -> Set[tuple[int, int, int]]:
    return set(_SEEDED_HALFDAYS).union(_json_date_set("EQUITY_MARKET_HALFDAYS_JSON"))


def _covered_years() -> Set[int]:
    dates = _holiday_set().union(_halfday_set())
    return set(_SEEDED_YEARS).union({int(year) for year, _month, _day in dates})


def _date_key(value: date) -> tuple[int, int, int]:
    return int(value.year), int(value.month), int(value.day)


def _is_equity_symbol(symbol: str) -> bool:
    try:
        from engine.data.asset_map import asset_class_for_symbol

        return bool(asset_class_for_symbol(str(symbol or "").strip().upper()) == "EQUITY")
    except Exception:
        return False


def _local_open_close(local_day: date, *, half_day: bool, tz: Any) -> tuple[datetime, datetime]:
    open_hour, open_min, close_hour, close_min, half_hour, half_min = _settings()
    open_dt = datetime.combine(local_day, dt_time(hour=int(open_hour), minute=int(open_min), tzinfo=tz))
    if half_day:
        close_dt = datetime.combine(local_day, dt_time(hour=int(half_hour), minute=int(half_min), tzinfo=tz))
    else:
        close_dt = datetime.combine(local_day, dt_time(hour=int(close_hour), minute=int(close_min), tzinfo=tz))
    if close_dt <= open_dt:
        close_dt = open_dt
    return open_dt, close_dt


def _is_trading_day(local_day: date) -> bool:
    if int(local_day.weekday()) >= 5:
        return False
    return _date_key(local_day) not in _holiday_set()


def _next_open_after(local_dt: datetime, *, tz: Any) -> int | None:
    probe = local_dt.date()
    for offset in range(0, 370):
        day = probe + timedelta(days=offset)
        if not _is_trading_day(day):
            continue
        half_day = _date_key(day) in _halfday_set()
        open_dt, close_dt = _local_open_close(day, half_day=half_day, tz=tz)
        if local_dt < close_dt:
            return _ms_from_dt(open_dt if local_dt <= open_dt else local_dt)
    return None


def _unknown_year_policy() -> str:
    return str(os.environ.get("EQUITY_SESSION_UNKNOWN_YEAR_POLICY", "open_rth") or "open_rth").strip().lower()


def _neutral_state(symbol: str) -> Dict[str, Any]:
    return {
        "is_equity": False,
        "symbol": str(symbol or "").strip().upper(),
        "session": "regular",
        "is_open": True,
        "is_half_day": False,
        "minutes_to_close": None,
        "next_open_ms": None,
        "tz": _TZ_NAME,
        "holiday_table_covered": True,
    }


def equity_session_state(symbol: str, now_ms: int) -> Dict[str, Any]:
    """Return pure US equity RTH state for ``symbol`` at ``now_ms``."""

    normalized = str(symbol or "").strip().upper()
    if not _is_equity_symbol(normalized):
        return _neutral_state(normalized)

    tz = _ny_tz()
    local = _dt_from_ms(int(now_ms)).astimezone(tz)
    local_day = local.date()
    key = _date_key(local_day)
    holidays = _holiday_set()
    halfdays = _halfday_set()
    covered = int(local_day.year) in _covered_years()
    half_day = key in halfdays
    open_dt, close_dt = _local_open_close(local_day, half_day=half_day, tz=tz)

    base: Dict[str, Any] = {
        "is_equity": True,
        "symbol": normalized,
        "is_half_day": bool(half_day),
        "minutes_to_close": None,
        "next_open_ms": None,
        "tz": _TZ_NAME,
        "holiday_table_covered": bool(covered),
        "rth_open_ms": _ms_from_dt(open_dt),
        "rth_close_ms": _ms_from_dt(close_dt),
        "unknown_year_policy": _unknown_year_policy(),
    }

    if int(local.weekday()) >= 5:
        return {
            **base,
            "session": "closed_weekend",
            "is_open": False,
            "next_open_ms": _next_open_after(local, tz=tz),
        }
    if key in holidays:
        return {
            **base,
            "session": "closed_holiday",
            "is_open": False,
            "next_open_ms": _next_open_after(local, tz=tz),
        }
    if local < open_dt:
        return {
            **base,
            "session": "pre_market",
            "is_open": False,
            "next_open_ms": _ms_from_dt(open_dt),
        }
    if local >= close_dt:
        return {
            **base,
            "session": "after_hours",
            "is_open": False,
            "next_open_ms": _next_open_after(local + timedelta(milliseconds=1), tz=tz),
        }

    minutes_to_close = int(max(0.0, (close_dt - local).total_seconds()) // 60)
    return {
        **base,
        "session": "regular",
        "is_open": True,
        "minutes_to_close": int(minutes_to_close),
        "next_open_ms": None,
    }


def equity_timing_adjustment(symbol: str, now_ms: int, base_decision: Dict[str, Any]) -> Dict[str, Any]:
    """Return an equity-session-adjusted execution decision.

    Non-equity decisions are returned unchanged. Closed-session equities are
    marked for policy-layer suppression. Near-close and half-day equities are
    biased toward passive limit timing without mutating ``base_decision``.
    """

    state = equity_session_state(symbol, now_ms)
    if not bool(state.get("is_equity")):
        return dict(base_decision or {})

    fail_closed_unknown = _unknown_year_policy() in {"fail_closed", "closed", "block"}
    uncovered_open_policy = (not bool(state.get("holiday_table_covered"))) and not fail_closed_unknown
    if uncovered_open_policy:
        return dict(base_decision or {})

    out = dict(base_decision or {})
    out["equity_session"] = dict(state)
    out["equity_session_blocked"] = False
    out["equity_out_of_session_mark"] = bool((not bool(state.get("is_open"))) and not uncovered_open_policy)
    if (not bool(state.get("holiday_table_covered"))) and fail_closed_unknown:
        out["equity_session_blocked"] = True
        out["equity_session_reason"] = "holiday_table_uncovered"
        return out
    if not bool(state.get("is_open")):
        out["equity_session_blocked"] = True
        out["equity_session_reason"] = str(state.get("session") or "closed")
        return out

    near_close_min = _env_int("EQUITY_NEAR_CLOSE_BIAS_MIN", 10, 0, 240)
    minutes_to_close = state.get("minutes_to_close")
    in_near_close = minutes_to_close is not None and int(minutes_to_close) <= int(near_close_min)
    if bool(state.get("is_half_day")) or bool(in_near_close):
        out["equity_session_timing_bias"] = True
        out["equity_session_reason"] = "half_day" if bool(state.get("is_half_day")) else "near_close"
        out["order_type"] = "LIMIT"
        out["aggressiveness"] = "PASSIVE"
        out["execution_policy"] = "passive"
        out["entry_strategy"] = "equity_session_passive_limit"
        try:
            current_delay = int(out.get("entry_delay_ms") or 0)
        except Exception:
            current_delay = 0
        try:
            current_chunk = float(out.get("chunk_pct") or 1.0)
        except Exception:
            current_chunk = 1.0
        try:
            current_latency_mult = float(out.get("latency_mult") or 1.0)
        except Exception:
            current_latency_mult = 1.0
        out["entry_delay_ms"] = int(max(current_delay, 60_000))
        out["chunk_pct"] = float(min(max(0.05, current_chunk), 0.20))
        out["latency_mult"] = float(max(current_latency_mult, 1.5))
    return out


__all__ = ["equity_session_state", "equity_timing_adjustment"]
