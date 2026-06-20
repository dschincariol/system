from __future__ import annotations

import importlib
import os
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _price_con(anchor: int, *, rows: int = 16) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE prices(ts_ms INTEGER, symbol TEXT, price REAL, px REAL, source TEXT)")
    start = int(anchor) - int(rows * 60_000)
    for idx in range(rows):
        ts_ms = start + int(idx * 60_000)
        price = 100.0 + float(idx)
        con.execute(
            "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?, ?, ?, ?, ?)",
            (int(ts_ms), "AAPL", float(price), float(price), "unit"),
        )
    return con


def test_chronos_feature_ids_are_registered_shadow_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("USE_TS_FOUNDATION_FEATURES", raising=False)
    feature_registry, tsfe = _reload("engine.strategy.feature_registry", "engine.strategy.ts_foundation_encoder")

    fid = tsfe.TS_FOUNDATION_CHRONOS_FEATURE_IDS[0]
    assert fid in feature_registry.registered_feature_ids(include_shadow=True)
    assert fid not in feature_registry.default_feature_ids()
    assert feature_registry.feature_stage(fid) == feature_registry.FEATURE_STAGE_SHADOW
    assert fid in feature_registry.shadow_feature_ids([fid])

    groups = feature_registry.list_groups()
    meta = groups[tsfe.TS_FOUNDATION_CHRONOS_GROUP]
    assert meta["stage"] == feature_registry.FEATURE_STAGE_SHADOW
    assert meta["encoder_mode"] == "frozen"
    assert meta["direct_trading_authority"] is False
    assert meta["availability_timestamp_field"] == "encoder_artifact_created_ts_ms"


def test_chronos_device_is_cpu_first(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TS_FOUNDATION_DEVICE", raising=False)
    monkeypatch.delenv("TORCH_DEVICE", raising=False)
    monkeypatch.delenv("RUNTIME_HARDWARE_PROFILE", raising=False)
    (tsfe,) = _reload("engine.strategy.ts_foundation_encoder")

    assert tsfe.chronos_device() == "cpu"


def test_chronos_encoder_features_carry_artifact_and_pit_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USE_TS_FOUNDATION_FEATURES", "1")
    monkeypatch.setenv("TS_FOUNDATION_CONTEXT_ROWS", "8")
    monkeypatch.setenv("TS_FOUNDATION_MIN_CONTEXT_ROWS", "4")
    monkeypatch.setenv("TS_FOUNDATION_EMBEDDING_DIM", "4")
    monkeypatch.setenv("TS_FOUNDATION_REQUIRE_ARTIFACT_PERSISTENCE", "0")
    tsfe, snapshots = _reload("engine.strategy.ts_foundation_encoder", "engine.strategy.model_feature_snapshots")

    anchor = 1_700_000_000_000
    feature_ids = tsfe.get_chronos_feature_ids(4)
    con = _price_con(anchor, rows=12)
    monkeypatch.setattr(
        tsfe,
        "_encoder_artifact_metadata",
        lambda **_kwargs: {
            "artifact_alias": "feature_encoder:unit",
            "artifact_sha256": "a" * 64,
            "artifact_created_ts_ms": anchor - 10_000,
            "encoder_artifact_created_ts_ms": anchor - 10_000,
            "artifact_kind": "feature_encoder_manifest",
            "artifact_persisted": True,
        },
    )
    monkeypatch.setattr(tsfe, "_load_chronos_pipeline", lambda **_kwargs: object())
    monkeypatch.setattr(tsfe, "_call_embed", lambda *_args, **_kwargs: np.asarray([1.0, 2.0, 3.0, 4.0], dtype=np.float32))

    snap = snapshots.build_model_feature_snapshot(
        symbol="AAPL",
        ts_ms=anchor,
        feature_ids=feature_ids,
        con=con,
    )

    assert snap["availability"][tsfe.TS_FOUNDATION_CHRONOS_GROUP] is True
    assert [snap["features"][fid] for fid in feature_ids] == [1.0, 2.0, 3.0, 4.0]
    source_meta = snap["source_timestamps"][tsfe.TS_FOUNDATION_CHRONOS_GROUP]
    assert source_meta["artifact_alias"] == "feature_encoder:unit"
    assert source_meta["artifact_sha256"] == "a" * 64
    assert source_meta["model_family"] == "chronos"
    assert source_meta["frozen_encoder"] is True
    assert source_meta["direct_trading_authority"] is False
    assert snap["feature_metadata"][tsfe.TS_FOUNDATION_CHRONOS_GROUP]["lag_policy"] == (
        "price_history_asof_and_frozen_encoder_artifact_available"
    )
    assert snap["pit_controls"][tsfe.TS_FOUNDATION_CHRONOS_GROUP]["ok"] is True


def test_chronos_encoder_future_artifact_is_zeroed_by_pit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USE_TS_FOUNDATION_FEATURES", "1")
    monkeypatch.setenv("TS_FOUNDATION_CONTEXT_ROWS", "8")
    monkeypatch.setenv("TS_FOUNDATION_MIN_CONTEXT_ROWS", "4")
    monkeypatch.setenv("TS_FOUNDATION_EMBEDDING_DIM", "2")
    monkeypatch.setenv("TS_FOUNDATION_REQUIRE_ARTIFACT_PERSISTENCE", "0")
    tsfe, snapshots = _reload("engine.strategy.ts_foundation_encoder", "engine.strategy.model_feature_snapshots")

    anchor = 1_700_000_000_000
    feature_ids = tsfe.get_chronos_feature_ids(2)
    con = _price_con(anchor, rows=12)
    monkeypatch.setattr(
        tsfe,
        "_encoder_artifact_metadata",
        lambda **_kwargs: {
            "artifact_alias": "feature_encoder:future",
            "artifact_sha256": "b" * 64,
            "artifact_created_ts_ms": anchor + 1,
            "encoder_artifact_created_ts_ms": anchor + 1,
            "artifact_kind": "feature_encoder_manifest",
            "artifact_persisted": True,
        },
    )
    monkeypatch.setattr(tsfe, "_load_chronos_pipeline", lambda **_kwargs: object())
    monkeypatch.setattr(tsfe, "_call_embed", lambda *_args, **_kwargs: np.asarray([9.0, 8.0], dtype=np.float32))

    snap = snapshots.build_model_feature_snapshot(
        symbol="AAPL",
        ts_ms=anchor,
        feature_ids=feature_ids,
        con=con,
    )

    assert snap["availability"][tsfe.TS_FOUNDATION_CHRONOS_GROUP] is False
    assert all(float(snap["features"][fid]) == 0.0 for fid in feature_ids)
    reason_codes = snap["pit_controls"][tsfe.TS_FOUNDATION_CHRONOS_GROUP]["reason_codes"]
    assert "availability_after_decision" in reason_codes


def test_chronos_pipeline_freeze_failures_are_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    (tsfe,) = _reload("engine.strategy.ts_foundation_encoder")
    calls: list[tuple[str, str]] = []

    class BadParam:
        def requires_grad_(self, _value: bool) -> None:
            raise RuntimeError("param freeze failed")

    class BadPipeline:
        def eval(self) -> None:
            raise RuntimeError("eval failed")

        def parameters(self) -> list[BadParam]:
            return [BadParam()]

    monkeypatch.setattr(tsfe, "_warn_nonfatal", lambda code, error, **_extra: calls.append((code, str(error))))
    pipeline = BadPipeline()

    assert tsfe._freeze_pipeline(pipeline) is pipeline
    assert calls == [
        ("TS_FOUNDATION_PIPELINE_EVAL_FREEZE_FAILED", "eval failed"),
        ("TS_FOUNDATION_PIPELINE_PARAM_FREEZE_FAILED", "param freeze failed"),
    ]


def test_to_numpy_coerce_failure_is_reported_and_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    (tsfe,) = _reload("engine.strategy.ts_foundation_encoder")
    calls: list[tuple[str, str]] = []

    class ArrayFallback:
        def to_numpy(self) -> np.ndarray:
            raise RuntimeError("to_numpy failed")

        def __array__(self, dtype=None, copy=None) -> np.ndarray:
            del copy
            return np.asarray([1.25, 2.5], dtype=dtype or np.float32)

    monkeypatch.setattr(tsfe, "_warn_nonfatal", lambda code, error, **_extra: calls.append((code, str(error))))

    arr = tsfe._coerce_numeric_array(ArrayFallback())

    assert arr.tolist() == [1.25, 2.5]
    assert calls == [("TS_FOUNDATION_TO_NUMPY_COERCE_FAILED", "to_numpy failed")]


def test_live_model_contract_rejects_shadow_foundation_features(monkeypatch: pytest.MonkeyPatch) -> None:
    feature_registry, live_ai_safety, predictor, tsfe = _reload(
        "engine.strategy.feature_registry",
        "engine.runtime.live_ai_safety",
        "engine.strategy.predictor",
        "engine.strategy.ts_foundation_encoder",
    )
    fid = tsfe.TS_FOUNDATION_CHRONOS_FEATURE_IDS[0]

    import engine.model_registry as model_registry
    import engine.strategy.model_config as model_config

    monkeypatch.setattr(model_registry, "get_model_spec", lambda *_args, **_kwargs: {"feature_ids": [fid]})
    monkeypatch.setattr(model_config, "get_model_config", lambda *_args, **_kwargs: {})
    snapshot = live_ai_safety.model_feature_contract_snapshot("unit_live_model")
    assert snapshot["ok"] is False
    assert snapshot["reason"] == "live_model_shadow_feature_contract"
    assert snapshot["shadow_feature_ids"] == [fid]

    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setattr(predictor, "is_active_model_name", lambda _name: True)
    monkeypatch.setattr(predictor, "get_model_spec", lambda *_args, **_kwargs: {"feature_ids": [fid]})
    monkeypatch.setattr(predictor, "get_model_config", lambda *_args, **_kwargs: {})
    with pytest.raises(ValueError, match="live_model_serving_shadow_features_forbidden"):
        predictor._resolve_active_model("AAPL", 300, forced_model_name="unit_live_model")

    assert feature_registry.shadow_feature_ids([fid]) == [fid]


def test_runtime_config_rejects_invalid_ts_foundation_dim() -> None:
    with patch.dict(
        os.environ,
        {
            "ENV": "dev",
            "TS_FOUNDATION_EMBEDDING_DIM": "0",
        },
        clear=True,
    ):
        from engine.runtime.config_schema import ConfigError, load_runtime_config

        with pytest.raises(ConfigError, match="TS_FOUNDATION_EMBEDDING_DIM"):
            load_runtime_config()
