from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _series(rows: int = 40) -> tuple[tuple[int, float], ...]:
    start = 1_700_000_000_000
    return tuple((start + idx * 60_000, 100.0 + idx * 0.5 + (idx % 3) * 0.1) for idx in range(rows))


def test_fake_adapter_interface_consistency() -> None:
    from engine.strategy.tsfm_adapters import FakeDeterministicTSFMAdapter, TSFMSeriesContext

    context = TSFMSeriesContext(
        symbol="AAPL",
        timestamps_ms=tuple(ts for ts, _value in _series(12)),
        values=tuple(value for _ts, value in _series(12)),
        asof_ts_ms=_series(12)[-1][0],
        asset_class="equity",
    )
    adapter = FakeDeterministicTSFMAdapter()

    forecast = adapter.forecast(context, horizon=3)
    embedding = adapter.embed(context, dim=5)
    description = adapter.describe()

    assert forecast.backend == "fake"
    assert len(forecast.horizon_path) == 3
    assert {"0.1", "0.5", "0.9"}.issubset(forecast.quantiles)
    assert len(embedding.feature_ids) == 5
    assert len(embedding.values) == 5
    assert description["stage"] == "shadow"
    assert description["direct_trading_authority"] is False


def test_chronos_adapter_gracefully_skips_when_dependency_path_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    from engine.strategy import ts_foundation_encoder as tsfe
    from engine.strategy.tsfm_adapters import Chronos2Adapter, TSFMAdapterConfig, TSFMAdapterUnavailable, TSFMSeriesContext

    monkeypatch.setattr(
        tsfe,
        "_load_chronos_pipeline",
        lambda **_kwargs: (_ for _ in ()).throw(ImportError("chronos missing")),
    )
    context = TSFMSeriesContext(
        symbol="AAPL",
        timestamps_ms=tuple(ts for ts, _value in _series(8)),
        values=tuple(value for _ts, value in _series(8)),
        asof_ts_ms=_series(8)[-1][0],
    )
    adapter = Chronos2Adapter(
        TSFMAdapterConfig(
            backend="chronos",
            model_id="amazon/chronos-2",
            local_files_only=True,
            horizon=2,
        )
    )

    assert adapter.describe()["model_id"] == "amazon/chronos-2"
    assert adapter.describe()["local_files_only"] is True
    with pytest.raises(TSFMAdapterUnavailable, match="chronos_forecast_unavailable"):
        adapter.forecast(context, horizon=2)


def test_tsfm_feature_ids_are_shadow_and_live_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    feature_registry = importlib.reload(importlib.import_module("engine.strategy.feature_registry"))
    live_ai_safety = importlib.reload(importlib.import_module("engine.runtime.live_ai_safety"))
    from engine.strategy.tsfm_adapters import TSFM_SHADOW_GROUP, tsfm_feature_ids_for_backend

    fid = tsfm_feature_ids_for_backend("timesfm", embedding_dim=20)[-1]
    assert feature_registry.feature_stage(fid) == feature_registry.FEATURE_STAGE_SHADOW
    assert fid not in feature_registry.default_feature_ids()
    assert fid in feature_registry.shadow_feature_ids([fid])
    assert feature_registry.list_groups()[TSFM_SHADOW_GROUP]["stage"] == feature_registry.FEATURE_STAGE_SHADOW

    import engine.model_registry as model_registry
    import engine.strategy.model_config as model_config

    monkeypatch.setattr(model_registry, "get_model_spec", lambda *_args, **_kwargs: {"feature_ids": [fid]})
    monkeypatch.setattr(model_config, "get_model_config", lambda *_args, **_kwargs: {})
    snapshot = live_ai_safety.model_feature_contract_snapshot("unit_tsfm_model")
    assert snapshot["ok"] is False
    assert snapshot["reason"] == "live_model_shadow_feature_contract"
    assert snapshot["shadow_feature_ids"] == [fid]


def test_pit_window_rejects_future_or_non_pit_targets() -> None:
    from engine.strategy.tsfm_benchmark import build_pit_window

    bad_series = ((1_000, 100.0), (2_000, 101.0), (1_500, 102.0))
    with pytest.raises(ValueError, match="tsfm_pit_target_not_after_asof"):
        build_pit_window(
            symbol="AAPL",
            asset_class="equity",
            task="forecast",
            series=bad_series,
            context_end_index=1,
            context_length=2,
            horizon=1,
        )


def test_benchmark_persists_oos_rows_baselines_artifacts_and_risk_inputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "runtime.sqlite"))
    monkeypatch.setenv("TS_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))

    from engine.strategy.tsfm_benchmark import TSFMBenchmarkConfig, run_tsfm_benchmark

    con = sqlite3.connect(":memory:")
    config = TSFMBenchmarkConfig(
        run_id="unit-tsfm",
        symbols=("AAPL",),
        adapters=("fake",),
        baselines=("trailing", "har", "garch", "lightgbm", "patchtst", "itransformer"),
        tasks=("forecast", "realized_volatility"),
        horizon=2,
        context_length=8,
        min_context=6,
        max_eval_points=3,
        embedding_dim=4,
        fallback="skip",
        require_artifact_persistence=True,
        series_by_symbol={"AAPL": _series(18)},
    )

    summary = run_tsfm_benchmark(config, con=con)

    assert summary["status"] == "completed"
    assert summary["stage"] == "shadow"
    assert summary["artifact"]["artifact_persisted"] is True
    assert summary["metrics"]["tsfm.fake"]["n"] > 0
    assert summary["baselines"]["lightgbm"]["status_counts"]["unavailable"] > 0
    assert summary["risk_inputs_written"] > 0

    oos_count = con.execute(
        "SELECT COUNT(*) FROM model_oos_predictions WHERE run_id=? AND family=?",
        ("unit-tsfm", "tsfm.fake"),
    ).fetchone()[0]
    assert int(oos_count) > 0

    row_count = con.execute("SELECT COUNT(*) FROM tsfm_benchmark_rows WHERE run_id=?", ("unit-tsfm",)).fetchone()[0]
    assert int(row_count) == summary["rows_written"]

    feature_snapshot_raw = con.execute(
        "SELECT feature_snapshot_json FROM tsfm_benchmark_rows WHERE family='tsfm.fake' AND status='ok' LIMIT 1"
    ).fetchone()[0]
    feature_snapshot = json.loads(feature_snapshot_raw)
    assert feature_snapshot["stage"] == "shadow"
    assert feature_snapshot["feature_ids"]

    model_row = con.execute(
        "SELECT stage, live_ready, meta_json FROM model_versions WHERE model_name='tsfm.fake' LIMIT 1"
    ).fetchone()
    assert model_row[0] == "shadow"
    assert int(model_row[1]) == 0
    assert json.loads(model_row[2])["score_source"] == "model_oos_predictions"

    marketplace_row = con.execute(
        "SELECT stage, meta_json FROM model_marketplace_scores WHERE model_name='tsfm.fake' LIMIT 1"
    ).fetchone()
    assert marketplace_row[0] == "shadow"
    marketplace_meta = json.loads(marketplace_row[1])
    assert marketplace_meta["promotion_authority"] == "shadow_only_oos_no_execution_authority"
    assert marketplace_meta["net_cost_evidence_available"] is False


def test_tsfm_zero_shot_oos_cannot_promote_without_normal_evidence() -> None:
    from engine.strategy import champion_manager

    candidate = {
        "model_name": "tsfm.fake",
        "score": 10.0,
        "meta": {
            "score_source": "model_oos_predictions",
            "model_family": "tsfm",
            "zero_shot": True,
            "net_cost_evidence_available": False,
            "net_cost_label_count": 0,
        },
    }

    assert champion_manager._score_source_is_competition_candidate(candidate["meta"]) is True
    assert champion_manager._candidate_is_live_promotable(candidate) is False
