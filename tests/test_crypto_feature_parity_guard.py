from __future__ import annotations

import importlib
import json
import uuid

import pytest


CRYPTO_FEATURE_IDS = {
    "funding_rate_now",
    "funding_z_30d",
    "funding_extreme_flag",
    "funding_cum_3d",
    "perp_basis_pct",
    "basis_z_30d",
}


def _reload_feature_registry(monkeypatch, *, funding_enabled: bool):
    monkeypatch.setenv("USE_FUNDING_FEATURES", "1" if funding_enabled else "0")
    import engine.strategy.feature_registry as feature_registry

    return importlib.reload(feature_registry)


def _schema(feature_registry, feature_ids):
    ids = list(feature_ids or [])
    return {
        "feature_ids": list(ids),
        "feature_set_tag": feature_registry.feature_set_tag_from_ids(list(ids)),
        "feature_count": len(ids),
        "feature_flags": feature_registry.feature_schema_flags(list(ids)),
    }


def test_use_funding_features_changes_feature_fingerprint_and_parity_guard_fails_closed(monkeypatch, caplog) -> None:
    canary = f"crypto-parity-canary-{uuid.uuid4().hex}"
    monkeypatch.setenv("CRYPTO_PERP_MARKETS", canary)

    off_registry = _reload_feature_registry(monkeypatch, funding_enabled=False)
    off_ids = off_registry.default_feature_ids()
    off_schema = _schema(off_registry, off_ids)
    off_fingerprint = json.dumps(
        {"ids": off_ids, "tag": off_schema["feature_set_tag"], "flags": off_schema["feature_flags"]},
        sort_keys=True,
    )

    on_registry = _reload_feature_registry(monkeypatch, funding_enabled=True)
    on_ids = on_registry.default_feature_ids()
    on_schema = _schema(on_registry, on_ids)
    on_fingerprint = json.dumps(
        {"ids": on_ids, "tag": on_schema["feature_set_tag"], "flags": on_schema["feature_flags"]},
        sort_keys=True,
    )

    assert off_fingerprint != on_fingerprint
    assert off_schema["feature_flags"]["USE_FUNDING_FEATURES"] is False
    assert on_schema["feature_flags"]["USE_FUNDING_FEATURES"] is True
    assert not (CRYPTO_FEATURE_IDS & set(off_ids))
    assert CRYPTO_FEATURE_IDS.issubset(set(on_ids))

    with pytest.raises(ValueError) as exc:
        on_registry.assert_feature_schema_runtime_parity(
            off_schema,
            current_schema=on_schema,
            context="feature_schema_drift",
            model_name="crypto_feature_parity_test",
        )

    message = str(exc.value)
    assert "USE_FUNDING_FEATURES" in message
    assert "artifact=False" in message
    assert "current=True" in message
    assert canary not in message
    assert canary not in caplog.text

    on_registry.assert_feature_schema_runtime_parity(
        on_schema,
        current_schema=on_schema,
        context="feature_schema_drift",
        model_name="crypto_feature_parity_test",
    )
