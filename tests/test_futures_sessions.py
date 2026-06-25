from __future__ import annotations

from datetime import date, datetime, timezone
import importlib
from zoneinfo import ZoneInfo


CT = ZoneInfo("America/Chicago")


def _ms_ct(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=CT).timestamp() * 1000)


def _dt_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _reload_sessions():
    return importlib.reload(importlib.import_module("engine.data.calendar.futures_sessions"))


def test_globex_closed_boundaries_and_maintenance_break(monkeypatch) -> None:
    for name in ("FUT_WEEK_CLOSE_HOUR_CT", "FUT_WEEK_OPEN_HOUR_CT", "FUT_MAINT_START_CT", "FUT_MAINT_END_CT"):
        monkeypatch.delenv(name, raising=False)
    mod = _reload_sessions()

    assert mod.futures_market_closed(_ms_ct(2026, 1, 9, 16, 0))
    assert mod.futures_market_closed(_ms_ct(2026, 1, 10, 12, 0))
    assert mod.futures_market_closed(_ms_ct(2026, 1, 11, 16, 59))
    assert not mod.futures_market_closed(_ms_ct(2026, 1, 11, 17, 0))
    assert mod.futures_market_closed(_ms_ct(2026, 1, 5, 16, 30))
    assert mod.is_maintenance_break(_ms_ct(2026, 1, 5, 16, 30))
    assert not mod.futures_market_closed(_ms_ct(2026, 1, 5, 15, 30))
    assert not mod.futures_market_closed(_ms_ct(2026, 1, 5, 17, 0))


def test_futures_window_next_open_and_settlement() -> None:
    mod = _reload_sessions()

    assert mod.futures_window_spans_closed_gap(_ms_ct(2026, 1, 9, 15, 30), _ms_ct(2026, 1, 11, 17, 30))
    assert not mod.futures_window_spans_closed_gap(_ms_ct(2026, 1, 6, 10, 0), _ms_ct(2026, 1, 6, 11, 0))
    assert mod.next_session_open_ms(_ms_ct(2026, 1, 9, 16, 30)) == _ms_ct(2026, 1, 11, 17, 0)
    assert mod.next_session_open_ms(_ms_ct(2026, 1, 5, 16, 30)) == _ms_ct(2026, 1, 5, 17, 0)
    assert mod.settlement_ts_for_day(_ms_ct(2026, 1, 5, 9, 0), "CME_EQUITY") == _ms_ct(2026, 1, 5, 15, 15)


def test_real_chicago_dst_offsets_are_used() -> None:
    mod = _reload_sessions()
    standard_close = _ms_ct(2026, 1, 9, 16, 0)
    dst_close = _ms_ct(2026, 3, 13, 16, 0)
    dst_reopen = _ms_ct(2026, 3, 8, 17, 0)

    assert _dt_utc(standard_close).hour == 22
    assert _dt_utc(dst_close).hour == 21
    assert _dt_utc(dst_reopen).hour == 22
    assert not mod.futures_market_closed(dst_reopen)


def test_env_override_boundaries(monkeypatch) -> None:
    monkeypatch.setenv("FUT_WEEK_CLOSE_HOUR_CT", "15")
    monkeypatch.setenv("FUT_WEEK_OPEN_HOUR_CT", "18")
    monkeypatch.setenv("FUT_MAINT_START_CT", "15:30")
    monkeypatch.setenv("FUT_MAINT_END_CT", "16:30")
    mod = _reload_sessions()

    assert mod.futures_market_closed(_ms_ct(2026, 1, 9, 15, 0))
    assert mod.futures_market_closed(_ms_ct(2026, 1, 11, 17, 30))
    assert not mod.futures_market_closed(_ms_ct(2026, 1, 11, 18, 0))
    assert mod.is_maintenance_break(_ms_ct(2026, 1, 5, 16, 0))
    assert not mod.is_maintenance_break(_ms_ct(2026, 1, 5, 16, 30))


def test_refreshable_holiday_set(monkeypatch) -> None:
    monkeypatch.delenv("FUT_HOLIDAYS_CT", raising=False)
    mod = _reload_sessions()

    assert not mod.futures_market_closed(_ms_ct(2026, 1, 6, 12, 0))
    mod.refresh_holidays([date(2026, 1, 6)])
    try:
        assert mod.futures_market_closed(_ms_ct(2026, 1, 6, 12, 0))
    finally:
        mod.refresh_holidays([])


def test_feature_registry_uses_futures_globex_clock_for_session_flags() -> None:
    from engine.strategy import feature_registry

    ts_ms = _ms_ct(2026, 6, 22, 16, 30)
    assert feature_registry._session_flags(ts_ms, asset_class="FUTURES") == (0.0, 0.0, 0.0)
    assert feature_registry._session_flags(ts_ms, asset_class="EQUITY") == (0.0, 0.0, 1.0)
