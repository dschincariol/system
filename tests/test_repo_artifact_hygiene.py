from __future__ import annotations

import pytest

from tools import check_repo_artifact_hygiene as hygiene


def _violation(path: str) -> str | None:
    violation = hygiene.artifact_violation_for_path(path)
    return violation.reason if violation is not None else None


def test_allows_legitimate_tracked_sources_and_env_templates() -> None:
    allowed = [
        ".env.example",
        "deploy/compose/.env.example",
        "deploy/env/trading.env.example",
        "deploy/env/staging-prod-preflight.env.example",
        "engine/artifacts/store.py",
        "engine/strategy/models/base_model.py",
        "data/equity_sector_reference.json",
        "data/model_configs.json",
        "data/sec_company_tickers_exchange.json",
    ]

    assert hygiene.tracked_artifact_violations(allowed) == []


def test_blocks_generated_dependency_and_runtime_paths() -> None:
    blocked = {
        ".venv/bin/python": "virtual environment",
        "node_modules/pkg/index.js": "node dependency",
        "ui/node_modules/pkg/index.js": "node dependency",
        "engine/__pycache__/x.cpython-311.pyc": "python bytecode",
        ".pytest_cache/v/cache/nodeids": "pytest cache",
        "var/log/engine.log": "runtime state",
        "logs/runtime.log": "runtime log",
        "tmp/operator.state.json": "temporary runtime",
        "data/runtime/trading.db": "runtime data",
    }

    for path, expected_reason in blocked.items():
        assert expected_reason in (_violation(path) or "")


@pytest.mark.parametrize(
    ("path", "expected_reason"),
    [
        ("engine/__pycache__/module.cpython-311.pyc", "python bytecode cache"),
        ("ui/node_modules/pkg/index.js", "node dependency directory"),
        ("artifacts/reports/latest.json", "generated artifact output"),
        ("model-artifacts/champion/model.bin", "generated model artifact output"),
        ("deploy/env/prod.env", "local environment, secret, or runtime data path"),
        ("data/secrets/operator-token", "local environment, secret, or runtime data path"),
        ("engine/runtime/state.sqlite3", "generated runtime/cache file suffix"),
        ("engine/runtime/service.log.1", "generated runtime/cache file suffix"),
    ],
)
def test_planted_offender_matrix_exercises_every_detector_table(path: str, expected_reason: str) -> None:
    violation = hygiene.artifact_violation_for_path(path)

    assert violation is not None
    assert violation.path == path
    assert violation.reason == expected_reason


def test_cli_fails_when_tracked_planted_offenders_are_reported(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    planted = [
        ".env",
        "artifacts/generated-model.json",
        "ui/node_modules/pkg/index.js",
        "engine/runtime/local.sqlite",
    ]
    monkeypatch.setattr(hygiene, "tracked_files", lambda root=hygiene.ROOT: planted)

    exit_code = hygiene.main([])
    output = capsys.readouterr().out

    assert exit_code == 1
    for path in planted:
        assert path in output
    assert "Remove these from the index" in output


def test_blocks_local_env_and_secret_material_but_not_templates() -> None:
    blocked = [
        ".env",
        ".env.local",
        ".env.codex-sim-paper.bak",
        "deploy/compose/.env",
        "deploy/compose/.env.local",
        "deploy/env/trading.env",
        "deploy/env/trading.local",
        "data/secrets/dashboard_api_token",
        "data/.data_source_master_key",
    ]

    for path in blocked:
        assert _violation(path) is not None

    assert _violation(".env.example") is None
    assert _violation("deploy/compose/.env.example") is None
    assert _violation("deploy/env/trading.env.example") is None
