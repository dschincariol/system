from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from engine.execution.equity_session import equity_session_state

NY = ZoneInfo("America/New_York")


def _ms_et(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=NY).timestamp() * 1000)


def _dt_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def test_spring_forward_week_uses_new_york_rth_boundaries() -> None:
    state = equity_session_state("SPY", _ms_et(2026, 3, 9, 10, 0))

    assert state["session"] == "regular"
    assert _dt_utc(int(state["rth_open_ms"])).hour == 13
    assert _dt_utc(int(state["rth_open_ms"])).minute == 30
    assert _dt_utc(int(state["rth_close_ms"])).hour == 20
    assert _dt_utc(int(state["rth_close_ms"])).minute == 0


def test_fall_back_week_uses_new_york_rth_boundaries() -> None:
    state = equity_session_state("SPY", _ms_et(2026, 11, 2, 10, 0))

    assert state["session"] == "regular"
    assert _dt_utc(int(state["rth_open_ms"])).hour == 14
    assert _dt_utc(int(state["rth_open_ms"])).minute == 30
    assert _dt_utc(int(state["rth_close_ms"])).hour == 21
    assert _dt_utc(int(state["rth_close_ms"])).minute == 0
