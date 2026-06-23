from __future__ import annotations

from pathlib import Path

from tools import validate_dependency_lock


ROOT = Path(__file__).resolve().parents[1]


def test_dependency_lock_validator_accepts_checked_in_manifests() -> None:
    assert validate_dependency_lock.main(["--strict"]) == 0


def test_runtime_manifests_exclude_dev_test_tools() -> None:
    runtime_paths = [
        ROOT / "requirements.in",
        ROOT / "requirements-base.txt",
        ROOT / "requirements.txt",
        ROOT / "requirements.lock.txt",
        ROOT / "requirements-nvidia-cuda.txt",
        ROOT / "requirements-amd-rocm.txt",
        ROOT / "requirements-amd-rocm-full.txt",
    ]

    for path in runtime_paths:
        names = set(validate_dependency_lock._requirements_entries(path))
        assert not (names & validate_dependency_lock.DEV_TOOL_REQUIREMENTS), path


def test_dev_manifest_declares_and_locks_validation_tools() -> None:
    dev_roots = validate_dependency_lock._requirements_entries(ROOT / "requirements-dev.in")
    dev_lock = validate_dependency_lock._requirements_entries(ROOT / "requirements-dev.lock.txt")

    for tool in validate_dependency_lock.DEV_TOOL_REQUIREMENTS:
        assert tool in dev_roots
        assert "==" in dev_roots[tool] or "===" in dev_roots[tool]
        assert tool in dev_lock


def test_install_manifests_apply_expected_constraints() -> None:
    runtime_includes, runtime_constraints = validate_dependency_lock._manifest_refs(ROOT / "requirements.txt")
    dev_includes, dev_constraints = validate_dependency_lock._manifest_refs(ROOT / "requirements-dev.txt")

    assert (ROOT / "requirements.in").resolve() in runtime_includes
    assert (ROOT / "requirements.lock.txt").resolve() in runtime_constraints
    assert (ROOT / "requirements-dev.in").resolve() in dev_includes
    assert (ROOT / "requirements-dev.lock.txt").resolve() in dev_constraints
