from __future__ import annotations

import importlib
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.data.futures_roll import ensure_futures_roll_tables

pytestmark = pytest.mark.safety_critical

CT = ZoneInfo("America/Chicago")


def _ms_ct(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=CT).timestamp() * 1000)


def test_futures_order_blocks_maintenance_delivery_and_allows_open_session(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = importlib.reload(importlib.import_module("engine.execution.broker_ibkr_gateway"))
    monkeypatch.setenv("FUTURES_DELIVERY_BLOCK_WINDOW_MS", str(2 * 24 * 60 * 60 * 1000))

    maintenance = gateway.futures_order_block("ES.c.0", ts_ms=_ms_ct(2026, 1, 5, 16, 30))
    assert maintenance is not None
    assert maintenance["status"] == "futures_maintenance_break_blocked"

    expiry = gateway.futures_order_block(
        "ES.c.0",
        ts_ms=_ms_ct(2026, 1, 5, 15, 30),
        order={"expiry_ts_ms": _ms_ct(2026, 1, 6, 15, 30)},
    )
    assert expiry is not None
    assert expiry["status"] == "futures_delivery_window_blocked"

    allowed = gateway.futures_order_block(
        "ES.c.0",
        ts_ms=_ms_ct(2026, 1, 5, 15, 30),
        order={"expiry_ts_ms": _ms_ct(2026, 1, 20, 15, 30)},
    )
    assert allowed is None


def test_futures_order_blocks_roll_calendar_window_and_allows_outside_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = importlib.reload(importlib.import_module("engine.execution.broker_ibkr_gateway"))
    monkeypatch.setenv("FUTURES_LIVE_TRADING_ENABLED", "0")
    monkeypatch.setenv("FUTURES_ROLL_BLOCK_WINDOW_MS", str(60 * 60 * 1000))

    roll_ts = _ms_ct(2026, 1, 6, 15, 30)
    con = sqlite3.connect(":memory:")
    try:
        ensure_futures_roll_tables(con)
        con.execute(
            """
            INSERT INTO futures_roll_calendar(
                root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            ("ES", roll_ts, "ESZ25", "ESH26", 1.0025, "unit_test", roll_ts),
        )

        blocked = gateway.futures_order_block("ES.c.0", ts_ms=roll_ts, con=con)
        assert blocked is not None
        assert blocked["status"] == "futures_roll_window_blocked"
        assert blocked["reason"] == "futures_order_inside_roll_window"
        assert blocked["root"] == "ES"
        assert blocked["symbol"] == "ES.C.0"
        assert blocked["roll_ts_ms"] == roll_ts
        assert blocked["from_contract"] == "ESZ25"
        assert blocked["to_contract"] == "ESH26"

        outside = gateway.futures_order_block("ES.c.0", ts_ms=_ms_ct(2026, 1, 7, 15, 30), con=con)
        assert outside is None
    finally:
        con.close()
