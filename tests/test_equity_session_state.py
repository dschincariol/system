from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from engine.execution import equity_session

NY = ZoneInfo("America/New_York")


def _ms_et(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=NY).timestamp() * 1000)


def test_regular_pre_after_weekend_holiday_and_halfday_states(monkeypatch) -> None:
    monkeypatch.delenv("EQUITY_MARKET_HOLIDAYS_JSON", raising=False)
    monkeypatch.delenv("EQUITY_MARKET_HALFDAYS_JSON", raising=False)
    monkeypatch.delenv("EQUITY_SESSION_UNKNOWN_YEAR_POLICY", raising=False)

    regular = equity_session.equity_session_state("SPY", _ms_et(2026, 6, 24, 10, 0))
    pre = equity_session.equity_session_state("SPY", _ms_et(2026, 6, 24, 9, 29))
    after = equity_session.equity_session_state("SPY", _ms_et(2026, 6, 24, 16, 1))
    weekend = equity_session.equity_session_state("SPY", _ms_et(2026, 6, 27, 10, 0))
    holiday = equity_session.equity_session_state("SPY", _ms_et(2026, 7, 3, 10, 0))
    halfday_open = equity_session.equity_session_state("SPY", _ms_et(2026, 11, 27, 12, 30))
    halfday_closed = equity_session.equity_session_state("SPY", _ms_et(2026, 11, 27, 13, 1))

    assert regular["session"] == "regular"
    assert regular["is_open"] is True
    assert regular["minutes_to_close"] == 360
    assert pre["session"] == "pre_market"
    assert pre["is_open"] is False
    assert after["session"] == "after_hours"
    assert after["is_open"] is False
    assert weekend["session"] == "closed_weekend"
    assert holiday["session"] == "closed_holiday"
    assert halfday_open["session"] == "regular"
    assert halfday_open["is_half_day"] is True
    assert halfday_open["is_open"] is True
    assert halfday_open["minutes_to_close"] == 30
    assert halfday_closed["session"] == "after_hours"
    assert halfday_closed["is_half_day"] is True


def test_env_overrides_and_purity(monkeypatch) -> None:
    monkeypatch.setenv("EQUITY_RTH_OPEN_HOUR_ET", "10")
    monkeypatch.setenv("EQUITY_RTH_OPEN_MIN_ET", "15")
    monkeypatch.setenv("EQUITY_MARKET_HOLIDAYS_JSON", json.dumps(["2026-06-24"]))

    closed = equity_session.equity_session_state("SPY", _ms_et(2026, 6, 24, 11, 0))
    assert closed["session"] == "closed_holiday"

    monkeypatch.setenv("EQUITY_MARKET_HOLIDAYS_JSON", "[]")
    before_override_open = equity_session.equity_session_state("SPY", _ms_et(2026, 6, 24, 10, 14))
    at_override_open = equity_session.equity_session_state("SPY", _ms_et(2026, 6, 24, 10, 15))
    first = equity_session.equity_session_state("SPY", _ms_et(2026, 6, 24, 10, 15))
    second = equity_session.equity_session_state("SPY", _ms_et(2026, 6, 24, 10, 15))

    assert before_override_open["session"] == "pre_market"
    assert at_override_open["session"] == "regular"
    assert first == second


def test_non_equity_symbol_returns_neutral_state(monkeypatch) -> None:
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)

    state = equity_session.equity_session_state("BTC", _ms_et(2026, 7, 3, 10, 0))

    assert state["is_equity"] is False
    assert state["session"] == "regular"
    assert state["is_open"] is True
    assert state["next_open_ms"] is None
