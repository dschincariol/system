from __future__ import annotations

from engine.data.fx_instrument import is_fx_symbol, parse_fx_symbol


def test_parse_fx_spot_metadata() -> None:
    eurusd = parse_fx_symbol("EURUSD")

    assert eurusd is not None
    assert eurusd.symbol == "EURUSD"
    assert eurusd.asset_class == "FX"
    assert eurusd.instrument_kind == "fx_spot"
    assert eurusd.base_ccy == "EUR"
    assert eurusd.quote_ccy == "USD"
    assert eurusd.pip_size == 0.0001
    assert eurusd.contract_size == 100000.0
    assert eurusd.pnl_ccy == "USD"
    assert eurusd.leverage_cap == 20.0
    assert eurusd.session_calendar == "FX_24x5"
    assert eurusd.source == "parser"


def test_parse_jpy_quote_uses_jpy_pip() -> None:
    usdjpy = parse_fx_symbol("USDJPY")

    assert usdjpy is not None
    assert usdjpy.symbol == "USDJPY"
    assert usdjpy.pip_size == 0.01
    assert usdjpy.pnl_ccy == "JPY"


def test_parse_friendly_variants_normalize_to_canonical_symbol() -> None:
    canonical = parse_fx_symbol("EURUSD")
    slash = parse_fx_symbol("EUR/USD")
    underscore = parse_fx_symbol("eur_usd")

    assert canonical is not None
    assert slash is not None
    assert underscore is not None
    assert slash.to_dict() == canonical.to_dict()
    assert underscore.to_dict() == canonical.to_dict()
    assert slash.symbol == "EURUSD"
    assert underscore.symbol == "EURUSD"


def test_dxy_is_fx_index_special_case() -> None:
    dxy = parse_fx_symbol("DXY")

    assert dxy is not None
    assert dxy.symbol == "DXY"
    assert dxy.instrument_kind == "fx_index"
    assert dxy.base_ccy is None
    assert dxy.quote_ccy == "USD"
    assert dxy.pnl_ccy == "USD"
    assert is_fx_symbol("DXY") is True


def test_non_pairs_do_not_parse() -> None:
    for symbol in ("SPY", "BTC", "GOOGLE", "", None):
        assert parse_fx_symbol(symbol) is None
        assert is_fx_symbol(symbol) is False


def test_is_fx_symbol_agrees_with_parser() -> None:
    for symbol in ("EURUSD", "USDJPY", "EUR/GBP", "AUD_USD", "DXY", "SPY", "GOOGLE"):
        assert is_fx_symbol(symbol) is (parse_fx_symbol(symbol) is not None)
