from __future__ import annotations

import importlib
import math
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

EQUITY_ONLY_IDS = [
    "options_symbol.iv_rank",
    "social.mention_rate_z",
    "social_regime.mania_score",
]


def _reload_feature_registry(monkeypatch: pytest.MonkeyPatch, *, enabled: bool):
    monkeypatch.setenv("USE_FUTURES_FEATURES", "1" if enabled else "0")
    return importlib.reload(importlib.import_module("engine.strategy.feature_registry"))


def _reload_sqlite_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, enabled: bool):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "futures_features.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_DB_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
    monkeypatch.setenv("PRICE_READ_BACKEND", "sqlite")
    monkeypatch.setenv("TRADING_FAILURE_DIAGNOSTICS_PERSIST", "0")
    storage_sqlite = importlib.reload(importlib.import_module("engine.runtime.storage_sqlite"))
    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    storage.init_db()
    storage_sqlite.init_db()
    return _reload_feature_registry(monkeypatch, enabled=enabled)


def test_futures_features_default_off_preserves_non_futures_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    fr = _reload_feature_registry(monkeypatch, enabled=False)
    requested_equity = list(EQUITY_ONLY_IDS) + list(fr.FUT_FEATURE_IDS) + ["base.source_credibility"]
    requested_fx = list(fr.FX_FEATURE_IDS) + list(fr.FUT_FEATURE_IDS) + ["base.source_credibility"]

    assert "futures" not in fr.FEATURE_GROUPS
    assert "futures_cot" not in fr.FEATURE_GROUPS
    assert all(not fid.startswith("fut.") for fid in fr.registered_feature_ids())
    assert all(not fid.startswith("fut.") for fid in fr.expected_columns())
    assert fr.resolve_feature_ids(requested_equity, asset_class="EQUITY") == fr.resolve_feature_ids(
        [fid for fid in requested_equity if not fid.startswith("fut.")],
        asset_class="EQUITY",
    )
    assert fr.resolve_feature_ids(requested_fx, asset_class="FX") == fr.resolve_feature_ids(
        [fid for fid in requested_fx if not fid.startswith("fut.")],
        asset_class="FX",
    )


def test_futures_feature_group_registers_with_train_serve_parity(monkeypatch: pytest.MonkeyPatch) -> None:
    fr = _reload_feature_registry(monkeypatch, enabled=True)
    requested = list(fr.FUT_FEATURE_IDS) + list(fr.FX_FEATURE_IDS) + list(EQUITY_ONLY_IDS) + [
        "base.source_credibility"
    ]
    train_cols = fr.expected_columns(requested, asset_class="FUTURES")
    serve_cols = fr.resolve_feature_ids(requested, asset_class="FUTURES")

    assert fr.FEATURE_GROUPS["futures"] == list(fr.FUT_FEATURE_IDS)
    assert fr.FEATURE_GROUPS["futures_cot"] == list(fr.FUTURES_COT_FEATURE_IDS)
    assert [fid for fid in train_cols if fid.startswith("fut.")] == list(fr.FUT_FEATURE_IDS)
    assert train_cols == serve_cols
    assert all(fid not in train_cols for fid in EQUITY_ONLY_IDS)
    assert all(not fid.startswith("fx.") for fid in train_cols)
    registered = fr.registered_feature_ids()
    assert len(registered) == len(set(registered))
    assert all(fid in registered for fid in fr.FUT_FEATURE_IDS)
    assert "futures" in fr.feature_set_tag_from_ids(list(fr.FUT_FEATURE_IDS)).split("+")


def test_futures_loaders_return_bounded_zeros_without_data(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fr = _reload_sqlite_runtime(monkeypatch, tmp_path, enabled=True)
    snap = fr.compute_feature_snapshot(
        event={"ts_ms": 0, "title": "roll update", "body": "", "source": "unit-test"},
        symbol="ES.c.0",
        feature_ids=list(fr.FUT_FEATURE_IDS),
    )

    assert list(snap.keys()) == list(fr.FUT_FEATURE_IDS)
    assert all(math.isfinite(float(value)) for value in snap.values())
    assert all(abs(float(value)) <= 10.0 for value in snap.values())
    assert all(float(value) == 0.0 for value in snap.values())
