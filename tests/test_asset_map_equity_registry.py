from __future__ import annotations

import importlib
import inspect
import json
import uuid


def _write_sec_registry(path):
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [1, "NVIDIA CORP", "NVDA", "Nasdaq"],
            [2, "XYZ CORP", "XYZ", "NYSE"],
            [3, "OTC CORP", "OTCZZ", "OTC"],
            [4, "NO EXCHANGE CORP", "NULZZ", None],
            [5, "FX COLLISION", "EURUSD", "Nasdaq"],
            [6, "CRYPTO COLLISION", "ETH", "NYSE"],
            [7, "COMMODITY COLLISION", "GC", "NYSE"],
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _reload_asset_map():
    import engine.data.asset_map as asset_map

    return importlib.reload(asset_map)


def test_equity_registry_classifies_allowed_exchanges_only(monkeypatch, tmp_path) -> None:
    registry_path = tmp_path / "sec_company_tickers_exchange.json"
    _write_sec_registry(registry_path)
    monkeypatch.setenv("SEC_TICKER_MAP_CACHE", str(registry_path))
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)
    monkeypatch.delenv("ASSET_MAP_USE_EQUITY_REGISTRY", raising=False)

    asset_map = _reload_asset_map()

    assert asset_map.asset_class_for_symbol("NVDA") == "EQUITY"
    assert asset_map.asset_class_for_symbol("XYZ") == "EQUITY"
    assert asset_map.asset_class_for_symbol("OTCZZ") == "UNKNOWN"
    assert asset_map.asset_class_for_symbol("NULZZ") == "UNKNOWN"


def test_equity_registry_preserves_earlier_asset_class_branches(monkeypatch, tmp_path) -> None:
    registry_path = tmp_path / "sec_company_tickers_exchange.json"
    _write_sec_registry(registry_path)
    monkeypatch.setenv("SEC_TICKER_MAP_CACHE", str(registry_path))
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)
    monkeypatch.delenv("ASSET_MAP_USE_EQUITY_REGISTRY", raising=False)

    asset_map = _reload_asset_map()

    expected = {
        "EURUSD": "FX",
        "USDJPY": "FX",
        "DXY": "FX",
        "BTC": "CRYPTO",
        "ETH": "CRYPTO",
        "OIL": "COMMODITY",
        "GC": "COMMODITY",
        "TLT": "RATES",
        "SPY": "EQUITY",
        "QQQ": "EQUITY",
    }
    for symbol, asset_class in expected.items():
        assert asset_map.asset_class_for_symbol(symbol) == asset_class


def test_equity_registry_flag_off_restores_legacy_unknown(monkeypatch, tmp_path) -> None:
    registry_path = tmp_path / "sec_company_tickers_exchange.json"
    _write_sec_registry(registry_path)
    monkeypatch.setenv("SEC_TICKER_MAP_CACHE", str(registry_path))
    monkeypatch.setenv("ASSET_MAP_USE_EQUITY_REGISTRY", "0")
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)

    asset_map = _reload_asset_map()

    assert asset_map.asset_class_for_symbol("NVDA") == "UNKNOWN"


def test_equity_registry_override_precedence(monkeypatch, tmp_path) -> None:
    registry_path = tmp_path / "sec_company_tickers_exchange.json"
    _write_sec_registry(registry_path)
    monkeypatch.setenv("SEC_TICKER_MAP_CACHE", str(registry_path))
    monkeypatch.setenv("ASSET_CLASS_MAP_JSON", '{"NVDA":"CUSTOM"}')
    monkeypatch.delenv("ASSET_MAP_USE_EQUITY_REGISTRY", raising=False)

    asset_map = _reload_asset_map()

    assert asset_map.asset_class_for_symbol("NVDA") == "CUSTOM"


def test_equity_registry_fail_safe_on_garbage_payload(monkeypatch, tmp_path, caplog) -> None:
    canary = f"canary-{uuid.uuid4()}"
    registry_path = tmp_path / "garbage-sec.json"
    registry_path.write_text(f"not-json {canary}", encoding="utf-8")
    monkeypatch.setenv("SEC_TICKER_MAP_CACHE", str(registry_path))
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)
    monkeypatch.delenv("ASSET_MAP_USE_EQUITY_REGISTRY", raising=False)

    asset_map = _reload_asset_map()

    assert asset_map.asset_class_for_symbol("NVDA") == "UNKNOWN"
    assert asset_map.asset_class_for_symbol("SPY") == "EQUITY"
    assert canary not in caplog.text


def test_equity_registry_signature_is_unchanged(monkeypatch, tmp_path) -> None:
    registry_path = tmp_path / "sec_company_tickers_exchange.json"
    _write_sec_registry(registry_path)
    monkeypatch.setenv("SEC_TICKER_MAP_CACHE", str(registry_path))
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)
    monkeypatch.delenv("ASSET_MAP_USE_EQUITY_REGISTRY", raising=False)
    asset_map = _reload_asset_map()

    assert list(inspect.signature(asset_map.asset_class_for_symbol).parameters) == ["symbol"]
