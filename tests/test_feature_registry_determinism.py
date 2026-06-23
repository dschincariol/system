from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

from engine.strategy.feature_registry import expected_columns

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_expected_columns_order_is_stable_across_repeated_calls() -> None:
    first = expected_columns()
    assert first
    for _ in range(100):
        assert expected_columns() == first


def _write_test_secrets(secret_dir) -> None:
    secret_dir.mkdir(parents=True, exist_ok=True)
    for name, value in {
        "master_key": "test-master-key",
        "pg_password_app": "test-app-password",
        "pg_password_ingest": "test-ingest-password",
        "pg_password_reader": "test-reader-password",
    }.items():
        (secret_dir / name).write_text(value, encoding="utf-8")


def _json_line(stdout: str) -> list[str]:
    for line in reversed(str(stdout or "").splitlines()):
        text = line.strip()
        if text.startswith("["):
            return list(json.loads(text))
    raise AssertionError(f"no JSON list in subprocess stdout: {stdout!r}")


def test_expected_columns_order_is_stable_across_hash_seeds(tmp_path) -> None:
    secret_dir = tmp_path / "secrets"
    _write_test_secrets(secret_dir)
    script = (
        "import json; "
        "from engine.strategy.feature_registry import expected_columns; "
        "print(json.dumps(expected_columns()))"
    )
    outputs = []
    for seed in ("1", "77", "12345"):
        env = dict(os.environ)
        env.update(
            {
                "PYTHONHASHSEED": seed,
                "TS_SECRETS_PROVIDER": "plaintext",
                "TS_DEV_SECRETS_DIR": str(secret_dir),
                "TS_PG_DSN": "host=127.0.0.1 port=1 dbname=postgres user=postgres password=test",
                "DB_PATH": str(tmp_path / f"runtime_{seed}.db"),
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
        outputs.append(_json_line(result.stdout))

    assert outputs[0]
    assert outputs[1:] == [outputs[0], outputs[0]]


def test_registered_feature_ids_cache_is_ttl_bounded_and_invalidatable(monkeypatch) -> None:
    import engine.strategy.feature_registry as feature_registry
    from engine.strategy.discovery import registry as discovery_registry

    feature_registry.invalidate_feature_registry_cache()
    old_ttl = float(feature_registry.FEATURE_REGISTRY_CACHE_TTL_S)
    records = [
        SimpleNamespace(
            feature_id="unit.live.cache_a",
            stage=feature_registry.FEATURE_STAGE_LIVE,
        )
    ]
    calls: list[str | None] = []

    def _fake_list_registered_features(*, stage=None, limit=5000):
        calls.append(stage)
        return [
            record
            for record in list(records)
            if stage is None or str(record.stage) == str(stage)
        ][: int(limit)]

    try:
        feature_registry.FEATURE_REGISTRY_CACHE_TTL_S = 30.0
        monkeypatch.setattr(discovery_registry, "list_registered_features", _fake_list_registered_features)

        first = feature_registry.registered_feature_ids(include_shadow=False)
        second = feature_registry.registered_feature_ids(include_shadow=False)

        assert "unit.live.cache_a" in first
        assert second == first
        assert calls == [feature_registry.FEATURE_STAGE_LIVE]

        records.append(
            SimpleNamespace(
                feature_id="unit.live.cache_b",
                stage=feature_registry.FEATURE_STAGE_LIVE,
            )
        )
        assert "unit.live.cache_b" not in feature_registry.registered_feature_ids(include_shadow=False)

        feature_registry.invalidate_feature_registry_cache()
        refreshed = feature_registry.registered_feature_ids(include_shadow=False)
        assert "unit.live.cache_b" in refreshed
        assert calls == [feature_registry.FEATURE_STAGE_LIVE, feature_registry.FEATURE_STAGE_LIVE]

        allowlist = feature_registry._registered_feature_allowlist(include_shadow=False)
        assert isinstance(allowlist, frozenset)
        assert "unit.live.cache_b" in allowlist
    finally:
        feature_registry.FEATURE_REGISTRY_CACHE_TTL_S = old_ttl
        feature_registry.invalidate_feature_registry_cache()


def test_registered_feature_ids_cache_refreshes_after_ttl(monkeypatch) -> None:
    import engine.strategy.feature_registry as feature_registry
    from engine.strategy.discovery import registry as discovery_registry

    feature_registry.invalidate_feature_registry_cache()
    old_ttl = float(feature_registry.FEATURE_REGISTRY_CACHE_TTL_S)
    now = [100.0]
    records = [
        SimpleNamespace(
            feature_id="unit.live.ttl_a",
            stage=feature_registry.FEATURE_STAGE_LIVE,
        )
    ]
    calls: list[str | None] = []

    def _fake_list_registered_features(*, stage=None, limit=5000):
        calls.append(stage)
        return [
            record
            for record in list(records)
            if stage is None or str(record.stage) == str(stage)
        ][: int(limit)]

    try:
        feature_registry.FEATURE_REGISTRY_CACHE_TTL_S = 1.0
        monkeypatch.setattr(feature_registry.time, "monotonic", lambda: now[0])
        monkeypatch.setattr(discovery_registry, "list_registered_features", _fake_list_registered_features)

        assert "unit.live.ttl_a" in feature_registry.registered_feature_ids(include_shadow=False)
        records.append(
            SimpleNamespace(
                feature_id="unit.live.ttl_b",
                stage=feature_registry.FEATURE_STAGE_LIVE,
            )
        )
        assert "unit.live.ttl_b" not in feature_registry.registered_feature_ids(include_shadow=False)

        now[0] += 1.1
        refreshed = feature_registry.registered_feature_ids(include_shadow=False)

        assert "unit.live.ttl_b" in refreshed
        assert calls == [feature_registry.FEATURE_STAGE_LIVE, feature_registry.FEATURE_STAGE_LIVE]
    finally:
        feature_registry.FEATURE_REGISTRY_CACHE_TTL_S = old_ttl
        feature_registry.invalidate_feature_registry_cache()


def test_registered_feature_ids_cache_is_separated_by_stage_and_shadow(monkeypatch) -> None:
    import engine.strategy.feature_registry as feature_registry
    from engine.strategy.discovery import registry as discovery_registry

    feature_registry.invalidate_feature_registry_cache()
    old_ttl = float(feature_registry.FEATURE_REGISTRY_CACHE_TTL_S)
    shadow_builtin = feature_registry.TS_FOUNDATION_CHRONOS_FEATURE_IDS[0]
    records = [
        SimpleNamespace(
            feature_id="unit.live.stage",
            stage=feature_registry.FEATURE_STAGE_LIVE,
        ),
        SimpleNamespace(
            feature_id="unit.shadow.stage",
            stage=feature_registry.FEATURE_STAGE_SHADOW,
        ),
    ]
    calls: list[str | None] = []

    def _fake_list_registered_features(*, stage=None, limit=5000):
        calls.append(stage)
        return [
            record
            for record in list(records)
            if stage is None or str(record.stage) == str(stage)
        ][: int(limit)]

    try:
        feature_registry.FEATURE_REGISTRY_CACHE_TTL_S = 30.0
        monkeypatch.setattr(discovery_registry, "list_registered_features", _fake_list_registered_features)

        live_ids = feature_registry.registered_feature_ids(include_shadow=False)
        shadow_ids = feature_registry.registered_feature_ids(
            include_shadow=True,
            stage=feature_registry.FEATURE_STAGE_SHADOW,
        )
        live_allowlist = feature_registry._registered_feature_allowlist(
            include_shadow=True,
            stage=feature_registry.FEATURE_STAGE_LIVE,
        )

        assert "unit.live.stage" in live_ids
        assert "unit.shadow.stage" not in live_ids
        assert shadow_builtin not in live_ids
        assert "unit.shadow.stage" in shadow_ids
        assert shadow_builtin in shadow_ids
        assert "unit.live.stage" not in shadow_ids
        assert isinstance(live_allowlist, frozenset)
        assert "unit.live.stage" in live_allowlist
        assert "unit.shadow.stage" not in live_allowlist
        assert calls == [
            feature_registry.FEATURE_STAGE_LIVE,
            feature_registry.FEATURE_STAGE_SHADOW,
        ]
    finally:
        feature_registry.FEATURE_REGISTRY_CACHE_TTL_S = old_ttl
        feature_registry.invalidate_feature_registry_cache()
