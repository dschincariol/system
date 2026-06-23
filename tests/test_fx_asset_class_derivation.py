from __future__ import annotations

import importlib
import inspect


def _reload_asset_map():
    import engine.data.asset_map as asset_map

    return importlib.reload(asset_map)


def test_asset_class_for_symbol_uses_fx_parser_for_pairs_and_dxy(monkeypatch) -> None:
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)
    asset_map = _reload_asset_map()

    for symbol in ("EURUSD", "USDJPY", "GBPUSD", "DXY", "EURGBP", "AUDUSD"):
        result = asset_map.asset_class_for_symbol(symbol)
        assert isinstance(result, str)
        assert result == "FX"


def test_asset_class_for_symbol_preserves_non_fx_classifications(monkeypatch) -> None:
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)
    asset_map = _reload_asset_map()

    expected = {
        "SPY": "EQUITY",
        "BTC": "CRYPTO",
        "OIL": "COMMODITY",
        "TLT": "RATES",
        "ZZZ": "UNKNOWN",
    }
    for symbol, asset_class in expected.items():
        result = asset_map.asset_class_for_symbol(symbol)
        assert isinstance(result, str)
        assert result == asset_class


def test_asset_class_override_still_wins_over_fx_branch(monkeypatch) -> None:
    monkeypatch.setenv("ASSET_CLASS_MAP_JSON", '{"EURUSD":"CUSTOM","DXY":"INDEX"}')
    asset_map = _reload_asset_map()

    assert asset_map.asset_class_for_symbol("EURUSD") == "CUSTOM"
    assert asset_map.asset_class_for_symbol("DXY") == "INDEX"


def test_asset_class_signature_is_unchanged(monkeypatch) -> None:
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)
    asset_map = _reload_asset_map()

    signature = inspect.signature(asset_map.asset_class_for_symbol)
    assert list(signature.parameters) == ["symbol"]
    assert asset_map.asset_class_for_symbol("EURUSD") == "FX"
