"""Canonical futures session-boundary clock for data, labels, and later execution.

This module is the single source of truth for futures market-open,
market-closed, settlement, and daily maintenance boundaries in the futures
enablement workstream. FUT-09 execution-session logic should derive from this
module, or configure identical ``FUT_*`` env knobs, so data, labels, execution,
and UI clocks do not diverge.

The default Globex week boundary is America/Chicago 16:00 CT Friday close
through 17:00 CT Sunday reopen. The daily maintenance break is 16:00-17:00 CT
Monday through Thursday. Those local CT boundaries correspond to different UTC
hours across US daylight and standard time; this module uses real
``ZoneInfo("America/Chicago")`` DST rules and only falls back to a fixed offset
if zoneinfo is unavailable.

Holiday closures are intentionally not baked in. The default holiday set is
empty, and operators can refresh it at runtime through ``refresh_holidays`` or
provide comma-separated ISO dates via ``FUT_HOLIDAYS_CT``. TODO(FUT-04): wire a
sourced CME holiday schedule refresh before relying on holiday-specific closes.
"""

from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta, timezone
import logging
import os
from zoneinfo import ZoneInfo

LOG = logging.getLogger(__name__)
_WARNED_TZ_FALLBACK = False
_UTC = timezone.utc
_FALLBACK_CT = timezone(timedelta(hours=-6), name="CT_FIXED")

DEFAULT_SESSION_CALENDAR = "CME_GLOBEX_24x5"
DEFAULT_FUT_WEEK_CLOSE_DAY_CT = 4  # Friday, datetime.weekday()
DEFAULT_FUT_WEEK_OPEN_DAY_CT = 6  # Sunday, datetime.weekday()
DEFAULT_FUT_WEEK_CLOSE_HOUR_CT = 16
DEFAULT_FUT_WEEK_OPEN_HOUR_CT = 17
DEFAULT_FUT_MAINT_START_CT = "16:00"
DEFAULT_FUT_MAINT_END_CT = "17:00"
DEFAULT_FUT_SETTLE_HOUR_CT = 15
DEFAULT_FUT_SETTLE_MINUTE_CT = 15

_HOLIDAYS_CT: set[date] = set()


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.environ.get(str(name), str(default)) or str(default)).strip())
    except Exception:
        value = int(default)
    return max(int(minimum), min(int(maximum), int(value)))


def _parse_clock(value: str, default: str) -> tuple[int, int]:
    text = str(os.environ.get(value, default) or default).strip()
    try:
        if ":" in text:
            hour_s, minute_s = text.split(":", 1)
            hour, minute = int(hour_s), int(minute_s)
        else:
            hour, minute = int(text), 0
    except Exception:
        hour_s, minute_s = str(default).split(":", 1)
        hour, minute = int(hour_s), int(minute_s)
    return max(0, min(23, int(hour))), max(0, min(59, int(minute)))


def _settings() -> tuple[int, int, int, int, tuple[int, int], tuple[int, int]]:
    close_day = _env_int("FUT_WEEK_CLOSE_DAY_CT", DEFAULT_FUT_WEEK_CLOSE_DAY_CT, 0, 6)
    open_day = _env_int("FUT_WEEK_OPEN_DAY_CT", DEFAULT_FUT_WEEK_OPEN_DAY_CT, 0, 6)
    close_hour = _env_int("FUT_WEEK_CLOSE_HOUR_CT", DEFAULT_FUT_WEEK_CLOSE_HOUR_CT, 0, 23)
    open_hour = _env_int("FUT_WEEK_OPEN_HOUR_CT", DEFAULT_FUT_WEEK_OPEN_HOUR_CT, 0, 23)
    maint_start = _parse_clock("FUT_MAINT_START_CT", DEFAULT_FUT_MAINT_START_CT)
    maint_end = _parse_clock("FUT_MAINT_END_CT", DEFAULT_FUT_MAINT_END_CT)
    return close_day, open_day, close_hour, open_hour, maint_start, maint_end


def _ct_tz():
    global _WARNED_TZ_FALLBACK
    try:
        return ZoneInfo("America/Chicago")
    except Exception as exc:  # pragma: no cover - defensive fallback only.
        if not _WARNED_TZ_FALLBACK:
            LOG.log(logging.WARNING, "futures_sessions_zoneinfo_unavailable_fixed_offset_fallback: %s", exc)
            _WARNED_TZ_FALLBACK = True
        return _FALLBACK_CT


def _dt_from_ms(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=_UTC)


def _ms_from_dt(value: datetime) -> int:
    return int(value.astimezone(_UTC).timestamp() * 1000)


def _week_boundary(local_dt: datetime, weekday: int, hour: int) -> datetime:
    base = local_dt.date()
    delta_days = int(weekday) - int(local_dt.weekday())
    day = base + timedelta(days=delta_days)
    return datetime.combine(day, dt_time(hour=int(hour), tzinfo=local_dt.tzinfo))


def _weekend_gap_for_week(local_dt: datetime) -> tuple[datetime, datetime]:
    close_day, open_day, close_hour, open_hour, _maint_start, _maint_end = _settings()
    close_dt = _week_boundary(local_dt, close_day, close_hour)
    open_dt = _week_boundary(local_dt, open_day, open_hour)
    if open_dt <= close_dt:
        open_dt += timedelta(days=7)
    return close_dt, open_dt


def _maintenance_gap_for_day(local_dt: datetime) -> tuple[datetime, datetime] | None:
    if int(local_dt.weekday()) not in {0, 1, 2, 3}:
        return None
    _close_day, _open_day, _close_hour, _open_hour, maint_start, maint_end = _settings()
    start = datetime.combine(
        local_dt.date(),
        dt_time(hour=int(maint_start[0]), minute=int(maint_start[1]), tzinfo=local_dt.tzinfo),
    )
    end = datetime.combine(
        local_dt.date(),
        dt_time(hour=int(maint_end[0]), minute=int(maint_end[1]), tzinfo=local_dt.tzinfo),
    )
    if end <= start:
        end += timedelta(days=1)
    return start, end


def _env_holidays() -> set[date]:
    out: set[date] = set()
    raw = str(os.environ.get("FUT_HOLIDAYS_CT", "") or "").strip()
    if not raw:
        return out
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        try:
            out.add(date.fromisoformat(text))
        except Exception:
            continue
    return out


def refresh_holidays(dates: set[date] | list[date] | tuple[date, ...] | None = None) -> set[date]:
    """Replace the in-memory CT holiday set and return the normalized values."""

    global _HOLIDAYS_CT
    values = dates if dates is not None else _env_holidays()
    normalized: set[date] = set()
    for value in values or ():
        if isinstance(value, date):
            normalized.add(value)
        else:
            try:
                normalized.add(date.fromisoformat(str(value)))
            except Exception:
                continue
    _HOLIDAYS_CT = set(normalized)
    return set(_HOLIDAYS_CT)


def holidays_ct() -> set[date]:
    """Return the current refreshable CT holiday set plus env-provided dates."""

    return set(_HOLIDAYS_CT) | _env_holidays()


def _holiday_gap_for_day(local_dt: datetime) -> tuple[datetime, datetime] | None:
    if local_dt.date() not in holidays_ct():
        return None
    start = datetime.combine(local_dt.date(), dt_time(0, 0, tzinfo=local_dt.tzinfo))
    return start, start + timedelta(days=1)


def _surrounding_closed_gaps(start_ms: int, end_ms: int) -> list[tuple[int, int]]:
    tz = _ct_tz()
    start_local = _dt_from_ms(min(int(start_ms), int(end_ms))).astimezone(tz)
    gaps: list[tuple[int, int]] = []
    for day_offset in range(-2, 10):
        probe = start_local + timedelta(days=day_offset)
        maint = _maintenance_gap_for_day(probe)
        if maint is not None:
            gaps.append((_ms_from_dt(maint[0]), _ms_from_dt(maint[1])))
        holiday = _holiday_gap_for_day(probe)
        if holiday is not None:
            gaps.append((_ms_from_dt(holiday[0]), _ms_from_dt(holiday[1])))
    for week_offset in range(-1, 3):
        probe = start_local + timedelta(days=7 * week_offset)
        gap_start, gap_end = _weekend_gap_for_week(probe)
        gaps.append((_ms_from_dt(gap_start), _ms_from_dt(gap_end)))
    return sorted(set(gaps))


def futures_market_closed(ts_ms: int, session_calendar: str = DEFAULT_SESSION_CALENDAR) -> bool:
    """Return True when the canonical futures data/session clock is closed."""

    del session_calendar
    ts = int(ts_ms)
    for gap_start, gap_end in _surrounding_closed_gaps(ts, ts):
        if int(gap_start) <= ts < int(gap_end):
            return True
    return False


def is_maintenance_break(ts_ms: int) -> bool:
    """Return True during the Mon-Thu 16:00-17:00 CT maintenance break."""

    local = _dt_from_ms(int(ts_ms)).astimezone(_ct_tz())
    gap = _maintenance_gap_for_day(local)
    if gap is None:
        return False
    return _ms_from_dt(gap[0]) <= int(ts_ms) < _ms_from_dt(gap[1])


def settlement_ts_for_day(ts_ms: int, session_calendar: str = "CME_EQUITY") -> int:
    """Return the CT settlement timestamp for the local session date."""

    del session_calendar
    local = _dt_from_ms(int(ts_ms)).astimezone(_ct_tz())
    hour = _env_int("FUT_SETTLE_HOUR_CT", DEFAULT_FUT_SETTLE_HOUR_CT, 0, 23)
    minute = _env_int("FUT_SETTLE_MINUTE_CT", DEFAULT_FUT_SETTLE_MINUTE_CT, 0, 59)
    settle = datetime.combine(local.date(), dt_time(hour=hour, minute=minute, tzinfo=local.tzinfo))
    return _ms_from_dt(settle)


def next_session_open_ms(ts_ms: int) -> int:
    """Return ``ts_ms`` if open, otherwise the next canonical futures reopen."""

    ts = int(ts_ms)
    if not futures_market_closed(ts):
        return ts
    ends = [int(gap_end) for gap_start, gap_end in _surrounding_closed_gaps(ts, ts) if int(gap_start) <= ts < int(gap_end)]
    if ends:
        return max(ends)
    return ts


def futures_window_spans_closed_gap(start_ms: int, end_ms: int) -> bool:
    """Return True when the naive window overlaps any canonical closed gap."""

    start = min(int(start_ms), int(end_ms))
    end = max(int(start_ms), int(end_ms))
    if start == end:
        return futures_market_closed(start)
    for gap_start, gap_end in _surrounding_closed_gaps(start, end):
        if start < int(gap_end) and end >= int(gap_start):
            return True
    return False


def futures_session_flags(ts_ms: int) -> tuple[float, float, float]:
    """Return Asia/EU/US-style base flags using the canonical Globex clock."""

    if futures_market_closed(int(ts_ms)):
        return 0.0, 0.0, 0.0
    local = _dt_from_ms(int(ts_ms)).astimezone(_ct_tz())
    hour = int(local.hour)
    asia = 1.0 if hour >= 17 or hour < 2 else 0.0
    eu = 1.0 if 2 <= hour < 8 else 0.0
    us = 1.0 if 8 <= hour < 16 else 0.0
    return asia, eu, us
