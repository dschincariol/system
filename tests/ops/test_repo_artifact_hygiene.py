from __future__ import annotations

import pytest

from tools import check_repo_artifact_hygiene as hygiene


@pytest.mark.parametrize(
    ("path", "expected_reason"),
    [
        (".env", "local environment, secret, or runtime data path"),
        (".env.production", "local environment, secret, or runtime data path"),
        (".venv/bin/python", "local Python virtual environment"),
        ("engine/__pycache__/module.cpython-311.pyc", "python bytecode cache"),
        ("ui/node_modules/pkg/index.js", "node dependency directory"),
        ("data/runtime/trading.db", "local environment, secret, or runtime data path"),
        ("deploy/compose/.env", "local environment, secret, or runtime data path"),
        ("engine/runtime/service.log.7", "generated runtime/cache file suffix"),
    ],
)
def test_artifact_hygiene_classifies_planted_offenders(path: str, expected_reason: str) -> None:
    violation = hygiene.artifact_violation_for_path(path)

    assert violation is not None
    assert violation.path == path
    assert violation.reason == expected_reason


@pytest.mark.parametrize(
    "path",
    [
        ".env.example",
        "service.env.example",
        "deploy/compose/.env.example",
        "deploy/env/trading.env.example",
        "deploy/env/staging-prod-preflight.env.example",
    ],
)
def test_artifact_hygiene_allows_tracked_env_templates(path: str) -> None:
    assert hygiene.artifact_violation_for_path(path) is None


def test_artifact_hygiene_detector_matrix_blocks_all_planted_offenders_and_allows_templates() -> None:
    planted = [
        ".env",
        ".env.production",
        ".venv/bin/python",
        "engine/__pycache__/module.cpython-311.pyc",
        "ui/node_modules/pkg/index.js",
        "data/runtime/trading.db",
        "deploy/compose/.env",
        "engine/runtime/service.log.7",
    ]
    allowed = [
        ".env.example",
        "service.env.example",
        "deploy/compose/.env.example",
        "deploy/env/trading.env.example",
    ]

    violations = hygiene.tracked_artifact_violations([*planted, *allowed])

    assert {violation.path for violation in violations} == set(planted)
    assert not ({violation.path for violation in violations} & set(allowed))
