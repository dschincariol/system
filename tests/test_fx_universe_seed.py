from __future__ import annotations

import importlib
import json


FX_MAJORS = {"EURUSD", "USDJPY", "GBPUSD", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD"}


def _reload_default_symbols():
    import engine.data.default_symbols as default_symbols

    return importlib.reload(default_symbols)


def test_fx_seed_symbols_are_opt_in(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_SYMBOLS_INCLUDE_SEEDS", "1")
    monkeypatch.delenv("DEFAULT_SYMBOLS", raising=False)
    monkeypatch.delenv("FX_PAIRS_ENABLED", raising=False)
    monkeypatch.delenv("OANDA_FX_PAIRS", raising=False)
    default_symbols = _reload_default_symbols()

    default_set = set(default_symbols.load_default_symbols())
    assert FX_MAJORS.isdisjoint(default_set)

    monkeypatch.setenv("FX_PAIRS_ENABLED", "1")
    default_symbols = _reload_default_symbols()
    enabled_set = set(default_symbols.load_default_symbols())
    assert FX_MAJORS.issubset(enabled_set)


def test_oanda_fx_pairs_override_seeds_explicit_pairs(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_SYMBOLS_INCLUDE_SEEDS", "1")
    monkeypatch.delenv("DEFAULT_SYMBOLS", raising=False)
    monkeypatch.delenv("FX_PAIRS_ENABLED", raising=False)
    monkeypatch.setenv("OANDA_FX_PAIRS", "EUR_USD,GBPUSD")
    default_symbols = _reload_default_symbols()

    symbols = set(default_symbols.load_default_symbols())

    assert {"EURUSD", "GBPUSD"}.issubset(symbols)
    assert "USDJPY" not in symbols


def test_fx_pair_oanda_instrument_round_trip_and_metadata() -> None:
    default_symbols = _reload_default_symbols()

    assert default_symbols.fx_pair_to_oanda_instrument("EURUSD") == "EUR_USD"
    assert default_symbols.oanda_instrument_to_fx_pair("EUR_USD") == "EURUSD"
    meta = default_symbols.default_symbol_metadata("EURUSD")
    assert meta["price_provider"] == "oanda"
    assert meta["oanda_instrument"] == "EUR_USD"


class _Cursor:
    def __init__(self, rows) -> None:
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)


class _SymbolConnection:
    def __init__(self) -> None:
        self.closed = False

    def execute(self, sql, params=()):
        text = " ".join(str(sql).split())
        if "FROM symbols WHERE status IN ('ACTIVE','WATCH')" in text:
            return _Cursor(
                [
                    (
                        "EURUSD",
                        json.dumps({"price_provider": "oanda", "oanda_instrument": "EUR_USD"}),
                    )
                ]
            )
        if "FROM symbols ORDER BY updated_ts_ms" in text:
            return _Cursor([])
        raise AssertionError(f"unexpected SQL: {text}")

    def close(self):
        self.closed = True


def test_poll_prices_builds_oanda_symbol_map_from_meta(monkeypatch) -> None:
    from engine.data import poll_prices

    con = _SymbolConnection()
    monkeypatch.setenv("FORCE_FACTOR_PROXY_TICKERS", "0")
    monkeypatch.setattr(poll_prices, "load_default_symbols", lambda: [])
    monkeypatch.setattr(poll_prices, "filter_symbol_mapping_for_shard", lambda mapping, _shard: dict(mapping))
    monkeypatch.setattr(poll_prices, "connect", lambda *args, **kwargs: con)

    universe = poll_prices._load_active_symbol_universe()

    assert universe.oanda_map == {"EURUSD": "EUR_USD"}
    assert poll_prices._provider_symbol_map_for_cycle("oanda", universe) == {"EURUSD": "EUR_USD"}
    assert "EURUSD" in set(universe.oanda_map)
