from __future__ import annotations

import importlib


EXPECTED_FX_MACRO_SERIES = {
    "DFII10": {
        "factor_id": "macro.us_real_yield_10y",
        "cadence": "daily",
        "name": "US 10Y TIPS Real Yield",
    },
    "DTWEXBGS": {
        "factor_id": "macro.usd_broad_index",
        "cadence": "daily",
        "name": "Trade Weighted U.S. Dollar Broad Index",
    },
    "ECBDFR": {
        "factor_id": "macro.ecb_policy_rate",
        "cadence": "daily",
        "name": "ECB Deposit Facility Rate",
    },
    "IUDSOIA": {
        "factor_id": "macro.uk_sonia_rate",
        "cadence": "daily",
        "name": "UK SONIA Interest Rate",
    },
    # Verified in the FX research dossier as the Japan short-rate input:
    # Interest Rates: Immediate Rates (< 24 Hours): Call Money/Interbank
    # Rate: Total for Japan, FRED series IRSTCI01JPM156N. It is monthly, so
    # downstream FX-03 loaders must handle cadence-aware lagging.
    "IRSTCI01JPM156N": {
        "factor_id": "macro.japan_call_money_rate",
        "cadence": "monthly",
        "name": "Japan Call Money Interbank Rate",
    },
}


def test_fx_macro_series_specs_are_registered_as_raw_fred_graph_rows() -> None:
    import engine.data.factor_ingestion as factor_ingestion

    mod = importlib.reload(factor_ingestion)
    specs = list(mod.MACRO_SERIES_SPECS)
    by_source = {spec.source_series_id: spec for spec in specs}

    for source_series_id, expected in EXPECTED_FX_MACRO_SERIES.items():
        assert source_series_id in by_source
        spec = by_source[source_series_id]
        assert spec.factor_id == expected["factor_id"]
        assert spec.cadence == expected["cadence"]
        assert spec.name == expected["name"]
        assert spec.family == "macro"
        assert spec.applies_to == "fx"
        assert spec.symbol_topic == "fx"
        assert spec.download_mode == "fred_graph"

    factor_ids = [spec.factor_id for spec in specs]
    assert len(factor_ids) == len(set(factor_ids))


def test_researched_japan_short_rate_id_cannot_be_silently_omitted() -> None:
    import engine.data.factor_ingestion as factor_ingestion

    mod = importlib.reload(factor_ingestion)
    by_source = {spec.source_series_id: spec for spec in mod.MACRO_SERIES_SPECS}
    japan_spec = by_source.get("IRSTCI01JPM156N")

    assert japan_spec is not None, (
        "FX research names IRSTCI01JPM156N as the Japan short-rate input; "
        "if it becomes unavailable, replace this with an explicit xfail/TODO "
        "that names the data-source dependency instead of silently omitting it."
    )
    assert japan_spec.factor_id == "macro.japan_call_money_rate"
    assert japan_spec.applies_to == "fx"
    assert japan_spec.cadence == "monthly"
