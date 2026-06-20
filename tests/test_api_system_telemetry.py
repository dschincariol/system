from engine.api import api_system


def test_telemetry_fills_table_uses_storage_table_exists(monkeypatch):
    calls = []

    def _table_exists(_con, table):
        calls.append(table)
        return table == "broker_fills"

    monkeypatch.setattr(api_system, "table_exists", _table_exists)

    assert api_system._telemetry_fills_table(object()) == "broker_fills"
    assert calls == ["broker_fills_v2", "broker_fills"]


def test_telemetry_fills_table_returns_none_when_no_fill_table(monkeypatch):
    monkeypatch.setattr(api_system, "table_exists", lambda _con, _table: False)

    assert api_system._telemetry_fills_table(object()) is None
