from __future__ import annotations

import importlib


def test_default_serving_feature_count_matches_docs(monkeypatch) -> None:
    # Pins the CLAUDE.md / docs/README_ARCHITECTURE.md "Current feature
    # inventory" claim: default serving is 111, not the full registry catalog.
    monkeypatch.delenv("USE_SYMBOL_SNAPSHOT_FEATURES", raising=False)
    import engine.strategy.feature_registry as feature_registry

    feature_registry = importlib.reload(feature_registry)
    feature_registry.invalidate_feature_registry_cache()

    default_ids = feature_registry.default_feature_ids()
    expected = feature_registry.expected_columns()
    registered = feature_registry.registered_feature_ids(include_shadow=False)

    assert len(feature_registry.BASE_FEATURE_IDS) == 8
    assert len(feature_registry.UNIFIED_SYMBOL_FEATURE_IDS) == 103
    assert len(default_ids) == 111
    assert len(expected) == 111
    assert expected == default_ids
    assert set(default_ids).issubset(set(registered))
    assert len(registered) > len(default_ids)
