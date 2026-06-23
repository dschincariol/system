"""Canonical FX session-boundary clock for label and downstream session users.

This module is the single source of truth for FX market-open and market-closed
boundaries in the FX enablement workstream. FX-06 execution-session logic and
FX-08 UI session display should derive from this module, or configure identical
``FX_WEEK_*`` env knobs, so label, execution, and UI clocks do not diverge.

The default week boundary is America/New_York 17:00 ET: closed from Friday
17:00 ET inclusive through Sunday 17:00 ET exclusive. That corresponds to
roughly Friday/Sunday 21:00 UTC during US daylight time and 22:00 UTC during
US standard time. Daily 17:00 ET rollover is treated as open-market time here;
only the weekend gap removes price-formation time from forward windows.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, time as dt_time, timedelta, timezone
from zoneinfo import ZoneInfo

LOG = logging.getLogger(__name__)
_WARNED_TZ_FALLBACK = False
_UTC = timezone.utc
_FALLBACK_ET = timezone(timedelta(hours=-5), name="ET_FIXED")

DEFAULT_FX_WEEK_CLOSE_DAY_ET = 4  # Friday, datetime.weekday()
DEFAULT_FX_WEEK_OPEN_DAY_ET = 6  # Sunday, datetime.weekday()
DEFAULT_FX_WEEK_CLOSE_HOUR_ET = 17
DEFAULT_FX_WEEK_OPEN_HOUR_ET = 17


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.environ.get(str(name), str(default)) or str(default)).strip())
    except Exception:
        value = int(default)
    return max(int(minimum), min(int(maximum), int(value)))


def _settings() -> tuple[int, int, int, int]:
    close_day = _env_int("FX_WEEK_CLOSE_DAY_ET", DEFAULT_FX_WEEK_CLOSE_DAY_ET, 0, 6)
    open_day = _env_int("FX_WEEK_OPEN_DAY_ET", DEFAULT_FX_WEEK_OPEN_DAY_ET, 0, 6)
    close_hour = _env_int("FX_WEEK_CLOSE_HOUR_ET", DEFAULT_FX_WEEK_CLOSE_HOUR_ET, 0, 23)
    open_hour = _env_int("FX_WEEK_OPEN_HOUR_ET", DEFAULT_FX_WEEK_OPEN_HOUR_ET, 0, 23)
    return close_day, open_day, close_hour, open_hour


def _ny_tz():
    global _WARNED_TZ_FALLBACK
    try:
        return ZoneInfo("America/New_York")
    except Exception as exc:  # pragma: no cover - defensive fallback only.
        if not _WARNED_TZ_FALLBACK:
            LOG.log(logging.WARNING, "fx_clock_zoneinfo_unavailable_fixed_offset_fallback: %s", exc)
            _WARNED_TZ_FALLBACK = True
        return _FALLBACK_ET


def _dt_from_ms(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=_UTC)


def _ms_from_dt(value: datetime) -> int:
    return int(value.astimezone(_UTC).timestamp() * 1000)


def _week_boundary(local_dt: datetime, weekday: int, hour: int) -> datetime:
    base = local_dt.date()
    delta_days = int(weekday) - int(local_dt.weekday())
    day = base + timedelta(days=delta_days)
    return datetime.combine(day, dt_time(hour=int(hour), tzinfo=local_dt.tzinfo))


def _closed_gap_for_week(local_dt: datetime) -> tuple[datetime, datetime]:
    close_day, open_day, close_hour, open_hour = _settings()
    close_dt = _week_boundary(local_dt, close_day, close_hour)
    open_dt = _week_boundary(local_dt, open_day, open_hour)
    if open_dt <= close_dt:
        open_dt += timedelta(days=7)
    return close_dt, open_dt


def _surrounding_closed_gaps(start_ms: int, end_ms: int) -> list[tuple[int, int]]:
    tz = _ny_tz()
    start_local = _dt_from_ms(min(int(start_ms), int(end_ms))).astimezone(tz)
    gaps: list[tuple[int, int]] = []
    for week_offset in range(-1, 3):
        probe = start_local + timedelta(days=7 * week_offset)
        gap_start, gap_end = _closed_gap_for_week(probe)
        gaps.append((_ms_from_dt(gap_start), _ms_from_dt(gap_end)))
    return gaps


def fx_market_closed(ts_ms: int) -> bool:
    """Return True from Friday 17:00 ET inclusive until Sunday 17:00 ET reopen.

    The Sunday reopen is exclusive: at or after Sunday 17:00 ET the FX market is
    considered open by this label/session clock.
    """

    ts = int(ts_ms)
    for gap_start, gap_end in _surrounding_closed_gaps(ts, ts):
        if int(gap_start) <= ts < int(gap_end):
            return True
    return False


def _next_gap_start_after(ts_ms: int) -> int:
    ts = int(ts_ms)
    candidates = [gap_start for gap_start, _gap_end in _surrounding_closed_gaps(ts, ts) if gap_start > ts]
    if candidates:
        return int(min(candidates))
    tz = _ny_tz()
    local = _dt_from_ms(ts).astimezone(tz) + timedelta(days=7)
    gap_start, _gap_end = _closed_gap_for_week(local)
    return _ms_from_dt(gap_start)


def _next_open_after_closed(ts_ms: int) -> int:
    ts = int(ts_ms)
    for gap_start, gap_end in sorted(_surrounding_closed_gaps(ts, ts)):
        if int(gap_start) <= ts < int(gap_end):
            return int(gap_end)
        if ts < int(gap_start):
            break
    return ts


def fx_forward_eval_ms(start_ts_ms: int, horizon_ms: int) -> int:
    """Return ``horizon_ms`` of open FX-market time after ``start_ts_ms``.

    Weekend gaps are skipped. The daily 17:00 ET rollover is not treated as a
    market-wide close in this helper; it remains open-market time for labels.
    """

    remaining = max(0, int(horizon_ms or 0))
    current = int(start_ts_ms)
    if fx_market_closed(current):
        current = _next_open_after_closed(current)
    if remaining <= 0:
        return int(current)
    while remaining > 0:
        next_close = _next_gap_start_after(current)
        open_ms = max(0, int(next_close) - int(current))
        if remaining <= open_ms:
            return int(current + remaining)
        remaining -= open_ms
        current = _next_open_after_closed(int(next_close))
    return int(current)


def fx_window_spans_closed_gap(start_ts_ms: int, end_ts_ms: int) -> bool:
    """Return True when the naive window overlaps the weekend closed gap."""

    start = min(int(start_ts_ms), int(end_ts_ms))
    end = max(int(start_ts_ms), int(end_ts_ms))
    if start == end:
        return fx_market_closed(start)
    for gap_start, gap_end in _surrounding_closed_gaps(start, end):
        if start < int(gap_end) and end >= int(gap_start):
            return True
    return False
