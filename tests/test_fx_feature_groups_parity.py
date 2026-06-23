from __future__ import annotations

import importlib
import json
import math
import os
import sqlite3
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from engine.strategy import feature_registry

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTERED_EQUITY_ONLY = [
    "options_symbol.iv_rank",
    "social.mention_rate_z",
    "social_regime.mania_score",
]
DEFAULT_COLUMNS_SNAPSHOT = tuple(feature_registry.expected_columns())


@pytest.fixture(autouse=True)
def _fresh_feature_registry_module():
    importlib.reload(feature_registry)
    yield


def _write_test_secrets(secret_dir: Path) -> None:
    secret_dir.mkdir(parents=True, exist_ok=True)
    for name, value in {
        "master_key": "test-master-key",
        "pg_password_app": "test-app-password",
        "pg_password_ingest": "test-ingest-password",
        "pg_password_reader": "test-reader-password",
    }.items():
        (secret_dir / name).write_text(value, encoding="utf-8")


def _json_obj_line(stdout: str) -> dict:
    for line in reversed(str(stdout or "").splitlines()):
        text = line.strip()
        if text.startswith("{"):
            return dict(json.loads(text))
    raise AssertionError(f"no JSON object in subprocess stdout: {stdout!r}")


def _reload_runtime_for_sqlite(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "fx_features.db"))
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
    return importlib.reload(importlib.import_module("engine.strategy.feature_registry"))


def test_fx_groups_register_with_stable_order() -> None:
    ids = list(feature_registry.FX_FEATURE_IDS)
    assert ids
    assert len(ids) == len(set(ids))
    assert feature_registry.FEATURE_GROUPS["fx"] == ids
    assert feature_registry.FEATURE_GROUP_METADATA["fx"]["feature_ids"] == ids
    assert ids == (
        list(feature_registry.FX_RATE_FEATURE_IDS)
        + list(feature_registry.FX_CARRY_FEATURE_IDS)
        + list(feature_registry.FX_DXY_FEATURE_IDS)
        + list(feature_registry.FX_CROSS_FEATURE_IDS)
        + list(feature_registry.FX_COT_FEATURE_IDS)
        + list(feature_registry.FX_MOMENTUM_FEATURE_IDS)
        + list(feature_registry.FX_EVENT_FEATURE_IDS)
    )


def test_fx_snapshot_round_trip_tagging() -> None:
    assert feature_registry._feature_uses_symbol_snapshot("fx.rate_diff_2y") is True
    fx_tag = feature_registry.feature_set_tag_from_ids(["fx.rate_diff_2y", "fx.carry_annualized"])
    equity_tag = feature_registry.feature_set_tag_from_ids(["price.last"])
    assert "fx" in fx_tag.split("+")
    assert fx_tag != equity_tag


def test_fx_train_and_serve_schema_are_asset_class_gated() -> None:
    requested = (
        list(feature_registry.FX_FEATURE_IDS)
        + list(DEFAULT_REGISTERED_EQUITY_ONLY)
        + ["base.source_credibility"]
    )
    train_cols = feature_registry.expected_columns(requested, asset_class="FX")
    serve_cols = feature_registry.resolve_feature_ids(requested, asset_class="FX")

    assert train_cols == serve_cols
    assert train_cols == feature_registry.resolve_feature_ids(requested, asset_class="FX")
    assert [fid for fid in train_cols if fid.startswith("fx.")] == list(feature_registry.FX_FEATURE_IDS)
    assert all(fid not in train_cols for fid in DEFAULT_REGISTERED_EQUITY_ONLY)


def test_default_registered_equity_only_ids_are_absent_for_fx() -> None:
    resolved = feature_registry.resolve_feature_ids(
        list(DEFAULT_REGISTERED_EQUITY_ONLY) + list(feature_registry.FX_FEATURE_IDS),
        asset_class="FX",
    )
    assert all(fid not in resolved for fid in DEFAULT_REGISTERED_EQUITY_ONLY)
    assert all(not fid.startswith(("options_symbol.", "social.", "social_regime.")) for fid in resolved)
    assert [fid for fid in resolved if fid.startswith("fx.")] == list(feature_registry.FX_FEATURE_IDS)


def test_opt_in_equity_groups_are_also_gated_for_fx(tmp_path: Path) -> None:
    secret_dir = tmp_path / "secrets"
    _write_test_secrets(secret_dir)
    script = (
        "import json; "
        "from engine.strategy import feature_registry as fr; "
        "fid = fr.INSIDER_FEATURE_IDS[0] if fr.INSIDER_FEATURE_IDS else ''; "
        "resolved = fr.resolve_feature_ids([fid] + list(fr.FX_FEATURE_IDS), asset_class='FX'); "
        "print(json.dumps({'fid': fid, 'registered': bool(fid and fid in fr.registered_feature_ids()), 'resolved': resolved}))"
    )
    env = dict(os.environ)
    env.update(
        {
            "USE_INSIDER_FEATURES": "1",
            "TS_SECRETS_PROVIDER": "plaintext",
            "TS_DEV_SECRETS_DIR": str(secret_dir),
            "TS_PG_DSN": "host=127.0.0.1 port=1 dbname=postgres user=postgres password=test",
            "DB_PATH": str(tmp_path / "insider.db"),
            "TRADING_FAILURE_DIAGNOSTICS_PERSIST": "0",
        }
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(REPO_ROOT),
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    payload = _json_obj_line(result.stdout)
    if not payload.get("fid") or not payload.get("registered"):
        pytest.skip("insider opt-in group did not register in subprocess")
    assert payload["fid"] not in payload["resolved"]
    assert any(str(fid).startswith("fx.") for fid in payload["resolved"])


def test_equity_and_none_asset_class_paths_remain_compatible() -> None:
    requested = list(feature_registry.FX_FEATURE_IDS) + list(DEFAULT_REGISTERED_EQUITY_ONLY)
    current_default = feature_registry.expected_columns()
    equity = feature_registry.resolve_feature_ids(requested, asset_class="EQUITY")

    assert all(fid in equity for fid in DEFAULT_REGISTERED_EQUITY_ONLY)
    assert not any(fid.startswith("fx.") for fid in equity)
    assert feature_registry.expected_columns(asset_class=None) == current_default
    assert feature_registry.expected_columns() == current_default
    assert DEFAULT_COLUMNS_SNAPSHOT
    assert feature_registry.resolve_feature_ids(requested) == feature_registry.resolve_feature_ids(
        requested,
        asset_class=None,
    )
    assert len(current_default) >= 100


def test_fx_resolvers_never_raise_without_fx01_data(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fr = _reload_runtime_for_sqlite(monkeypatch, tmp_path)
    canary = "CANARY-" + uuid.uuid4().hex
    monkeypatch.setenv("TS_PG_PASSWORD", canary)

    snap = fr.compute_feature_snapshot(
        event={"ts_ms": 0, "title": "rate decision", "body": "", "source": "unit-test"},
        symbol="EURUSD",
        feature_ids=list(fr.FX_FEATURE_IDS),
    )

    assert list(snap.keys()) == list(fr.FX_FEATURE_IDS)
    assert all(math.isfinite(float(value)) for value in snap.values())
    assert all(float(value) == 0.0 for value in snap.values())
    payload = json.dumps(snap, sort_keys=True)
    assert canary not in payload
    assert canary not in caplog.text


def test_fx_cot_mapping_uses_existing_eurusd_6e_contract() -> None:
    from engine.data import cftc_cot

    con = sqlite3.connect(":memory:")
    cftc_cot.ensure_cot_tables(con)
    cftc_cot.seed_default_cot_mappings(con)

    contracts = dict(cftc_cot.cot_target_contracts_for_symbol(con, "EURUSD"))
    assert contracts["6E"] == 1.0
    features, meta, available = cftc_cot.resolve_cot_features(
        con,
        symbol="EURUSD",
        ts_ms=1_700_000_000_000,
    )
    assert set(features) == set(cftc_cot.COT_FEATURE_IDS)
    assert meta["contracts"] == ["6E"]
    assert available is False
    assert all(float(value) == 0.0 for value in features.values())
