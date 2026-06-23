from __future__ import annotations

import json

from engine.risk.fx_leverage_caps import effective_leverage_cap, regulatory_leverage_cap


def test_regulatory_caps_seeded_from_fx00_section_6(monkeypatch):
    monkeypatch.delenv("FX_REGULATORY_LEVERAGE_CAPS_JSON", raising=False)
    monkeypatch.delenv("FX_LEVERAGE_JURISDICTION", raising=False)

    # FX-00 §6 documents the EU/ESMA retail major-pair cap around 30:1.
    assert regulatory_leverage_cap("EURUSD", jurisdiction="EU") == 30.0
    # FX-00 §6 documents the US/NFA major-pair cap around 50:1.
    assert regulatory_leverage_cap("EURUSD", jurisdiction="US") == 50.0
    assert regulatory_leverage_cap("EURTRY", jurisdiction="EU") == 10.0


def test_effective_cap_uses_min_instrument_and_regulatory(monkeypatch):
    monkeypatch.delenv("FX_REGULATORY_LEVERAGE_CAPS_JSON", raising=False)
    monkeypatch.delenv("FX_LEVERAGE_JURISDICTION", raising=False)

    assert effective_leverage_cap("EURUSD", {"leverage_cap": 12.0}) == 12.0
    assert effective_leverage_cap("EURUSD", {"leverage_cap": 50.0}) == 30.0
    assert effective_leverage_cap("EURTRY", {"leverage_cap": 100.0}) == 10.0
    assert effective_leverage_cap("EURUSD", None) == 30.0


def test_env_override_and_jurisdiction_selector(monkeypatch):
    override = {"US": {"major": 45, "minor": 18, "exotic": 8}, "EURUSD": 40}
    monkeypatch.setenv("FX_REGULATORY_LEVERAGE_CAPS_JSON", json.dumps(override))
    monkeypatch.setenv("FX_LEVERAGE_JURISDICTION", "US")

    assert regulatory_leverage_cap("EURUSD") == 40.0
    assert regulatory_leverage_cap("USDSEK") == 18.0
    assert regulatory_leverage_cap("EURTRY") == 8.0
    assert effective_leverage_cap("EURUSD", {"leverage_cap": 50.0}) == 40.0
