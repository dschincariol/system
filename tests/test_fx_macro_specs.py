from __future__ import annotations

import importlib


def test_fx_macro_series_specs_are_registered_as_raw_fred_graph_rows() -> None:
    import engine.data.factor_ingestion as factor_ingestion

    mod = importlib.reload(factor_ingestion)
    specs = list(mod.MACRO_SERIES_SPECS)
    by_source = {spec.source_series_id: spec for spec in specs}

    for source_series_id in ("DFII10", "DTWEXBGS", "ECBDFR", "IUDSOIA"):
        assert source_series_id in by_source
        spec = by_source[source_series_id]
        assert spec.family == "macro"
        assert spec.applies_to == "fx"
        assert spec.symbol_topic == "fx"
        assert spec.download_mode == "fred_graph"

    factor_ids = [spec.factor_id for spec in specs]
    assert len(factor_ids) == len(set(factor_ids))
