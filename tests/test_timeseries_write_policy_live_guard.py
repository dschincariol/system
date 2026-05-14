from __future__ import annotations

from engine.runtime.timeseries_write_policy import TimeseriesWritePolicy


def _policy(**overrides):
    values = {
        "sqlite_write_enabled": True,
        "sqlite_prices_enabled": True,
        "sqlite_quotes_enabled": True,
        "sqlite_raw_enabled": False,
        "require_async_during_cutover": True,
        "sync_provider_aux_sqlite_write_enabled": False,
        "block_sync_sqlite_in_live": True,
        "allow_sync_sqlite_in_live": False,
    }
    values.update(overrides)
    return TimeseriesWritePolicy(**values)


def test_live_mode_blocks_sync_sqlite_price_surfaces():
    state = _policy().validate_high_volume_runtime(engine_mode="live")

    assert state["ok"] is False
    assert state["reason"] == "live_high_volume_sqlite_sync_write_blocked"
    assert state["active_sqlite_surfaces"] == ["prices", "quotes"]


def test_live_mode_allows_non_sqlite_high_volume_surfaces():
    state = _policy(
        sqlite_write_enabled=False,
        sqlite_prices_enabled=False,
        sqlite_quotes_enabled=False,
        sqlite_raw_enabled=False,
    ).validate_high_volume_runtime(engine_mode="live")

    assert state["ok"] is True


def test_paper_mode_allows_sqlite_for_workstation_development():
    state = _policy().validate_high_volume_runtime(engine_mode="paper")

    assert state["ok"] is True
