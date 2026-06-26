from __future__ import annotations

import json
from pathlib import Path

from pytest import CaptureFixture, MonkeyPatch

from tools import validate_dependency_lock


ROOT = Path(__file__).resolve().parents[1]
ROCM_DIVERGENCE_REASON = "ROCm image carries a validated profile-specific runtime pin"


def _pin_lines(*, numpy: str = "2.1.2", pydantic: str = "2.13.4") -> list[str]:
    return [
        "huggingface-hub==0.36.2",
        "joblib==1.5.3",
        "lightgbm==4.5.0",
        "llvmlite==0.47.0",
        "numba==0.65.1",
        f"numpy=={numpy}",
        "pandas==3.0.3",
        "pyarrow==19.0.1",
        f"pydantic=={pydantic}",
        "pydantic-core==2.46.4",
        "safetensors==0.8.0",
        "scikit-learn==1.5.2",
        "scipy==1.17.1",
        "sentence-transformers==3.0.1",
        "SQLAlchemy==2.0.51",
        "statsmodels==0.14.6",
        "tokenizers==0.22.2",
        "transformers==4.57.6",
        "",
    ]


def _write_requirements(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_shared_pin_fixture(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    *,
    amd_numpy: str = "2.1.2",
    amd_lock_pydantic: str = "2.13.4",
) -> None:
    monkeypatch.setattr(validate_dependency_lock, "ROOT", tmp_path)
    monkeypatch.setattr(
        validate_dependency_lock,
        "SHARED_PIN_SOURCES",
        (
            validate_dependency_lock.SharedPinSource("base", "requirements-base.txt"),
            validate_dependency_lock.SharedPinSource("cpu", "requirements.txt"),
            validate_dependency_lock.SharedPinSource("cpu", "requirements.lock.txt"),
            validate_dependency_lock.SharedPinSource("nvidia-cuda", "requirements-nvidia-cuda.txt"),
            validate_dependency_lock.SharedPinSource("nvidia-cuda", "requirements-nvidia-cuda.lock.txt"),
            validate_dependency_lock.SharedPinSource("amd-rocm", "requirements-amd-rocm-full.txt"),
            validate_dependency_lock.SharedPinSource("amd-rocm", "requirements-amd-rocm.lock.txt"),
        ),
    )
    _write_requirements(
        tmp_path / "requirements-base.txt",
        [
            "joblib==1.5.3",
            "lightgbm==4.5.0",
            "numpy==2.1.2",
            "pandas==3.0.3",
            "pyarrow==19.0.1",
            "scikit-learn==1.5.2",
            "scipy==1.17.1",
            "sentence-transformers==3.0.1",
            "transformers==4.57.6",
            "",
        ],
    )
    _write_requirements(tmp_path / "requirements.txt", ["-r requirements-base.txt", ""])
    _write_requirements(tmp_path / "requirements.lock.txt", _pin_lines())
    _write_requirements(tmp_path / "requirements-nvidia-cuda.txt", ["-r requirements-base.txt", ""])
    _write_requirements(tmp_path / "requirements-nvidia-cuda.lock.txt", _pin_lines())
    _write_requirements(
        tmp_path / "requirements-amd-rocm-full.txt",
        [
            "joblib==1.5.3",
            "lightgbm==4.5.0",
            f"numpy=={amd_numpy}",
            "pandas==3.0.3",
            "pyarrow==19.0.1",
            "scikit-learn==1.5.2",
            "scipy==1.17.1",
            "sentence-transformers==3.0.1",
            "transformers==4.57.6",
            "",
        ],
    )
    _write_requirements(
        tmp_path / "requirements-amd-rocm.lock.txt",
        _pin_lines(numpy=amd_numpy, pydantic=amd_lock_pydantic),
    )


def _empty_report(*_args: object, **_kwargs: object) -> tuple[list[str], list[str]]:
    return [], []


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
    nvidia_includes, nvidia_constraints = validate_dependency_lock._manifest_refs(
        ROOT / "requirements-nvidia-cuda.txt"
    )
    rocm_includes, rocm_constraints = validate_dependency_lock._manifest_refs(
        ROOT / "requirements-amd-rocm-full.txt"
    )

    assert (ROOT / "requirements.in").resolve() in runtime_includes
    assert (ROOT / "requirements.lock.txt").resolve() in runtime_constraints
    assert (ROOT / "requirements-dev.in").resolve() in dev_includes
    assert (ROOT / "requirements-dev.lock.txt").resolve() in dev_constraints
    assert (ROOT / "requirements-base.txt").resolve() in nvidia_includes
    assert (ROOT / "requirements-nvidia-cuda.lock.txt").resolve() in nvidia_constraints
    assert rocm_includes == []
    assert (ROOT / "requirements-amd-rocm.lock.txt").resolve() in rocm_constraints


def test_lock_files_require_hashes_for_all_package_entries() -> None:
    for lock_name in (
        "requirements.lock.txt",
        "requirements-dev.lock.txt",
        "requirements-nvidia-cuda.lock.txt",
        "requirements-amd-rocm.lock.txt",
    ):
        assert not validate_dependency_lock._lock_requirements_missing_hashes(ROOT / lock_name)


def test_gpu_profile_lock_constraints_are_required(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(validate_dependency_lock, "ROOT", tmp_path)
    manifest = tmp_path / "requirements-nvidia-cuda.txt"
    lock = tmp_path / "requirements-nvidia-cuda.lock.txt"
    manifest.write_text("torch==2.4.1+cu121\n", encoding="utf-8")
    lock.write_text(
        "torch==2.4.1+cu121 \\\n"
        "    --hash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
        encoding="utf-8",
    )

    errors, warnings = validate_dependency_lock._runtime_profile_lock_constraint_report(
        {"requirements-nvidia-cuda.txt": "requirements-nvidia-cuda.lock.txt"}
    )
    assert warnings == []
    assert "requirements_profile_missing_lock_constraint:requirements-nvidia-cuda.txt" in errors

    manifest.write_text(
        "-c requirements-nvidia-cuda.lock.txt\n"
        "torch==2.4.1+cu121\n",
        encoding="utf-8",
    )
    errors, warnings = validate_dependency_lock._runtime_profile_lock_constraint_report(
        {"requirements-nvidia-cuda.txt": "requirements-nvidia-cuda.lock.txt"}
    )
    assert errors == []
    assert warnings == []


def test_shared_scientific_pin_report_accepts_identical_profile_pins(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    _write_shared_pin_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(validate_dependency_lock, "SHARED_PIN_ALLOWLIST", ())

    errors, warnings = validate_dependency_lock._shared_scientific_pin_report()

    assert errors == []
    assert warnings == []


def test_shared_scientific_pin_report_allowlists_documented_divergence(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    _write_shared_pin_fixture(tmp_path, monkeypatch, amd_numpy="2.4.6")
    monkeypatch.setattr(
        validate_dependency_lock,
        "SHARED_PIN_ALLOWLIST",
        (
            validate_dependency_lock.SharedPinAllowlistEntry(
                "numpy",
                "amd-rocm",
                "2.4.6",
                ROCM_DIVERGENCE_REASON,
            ),
        ),
    )

    errors, warnings = validate_dependency_lock._shared_scientific_pin_report()

    assert errors == []
    assert warnings == [
        "cross_profile_pin_allowlisted:requirements-amd-rocm-full.txt:numpy:"
        "reference=requirements-base.txt:2.1.2:"
        "profile=amd-rocm:actual=2.4.6:expected=2.4.6:"
        f"reason={ROCM_DIVERGENCE_REASON}",
        "cross_profile_pin_allowlisted:requirements-amd-rocm.lock.txt:numpy:"
        "reference=requirements-base.txt:2.1.2:"
        "profile=amd-rocm:actual=2.4.6:expected=2.4.6:"
        f"reason={ROCM_DIVERGENCE_REASON}",
    ]


def test_shared_scientific_pin_report_rejects_unjustified_allowlist_reason(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    _write_shared_pin_fixture(tmp_path, monkeypatch, amd_numpy="2.4.6")
    monkeypatch.setattr(
        validate_dependency_lock,
        "SHARED_PIN_ALLOWLIST",
        (
            validate_dependency_lock.SharedPinAllowlistEntry(
                "numpy",
                "amd-rocm",
                "2.4.6",
                "unverified base-version availability on ROCm image; pending confirmation",
            ),
        ),
    )

    errors, warnings = validate_dependency_lock._shared_scientific_pin_report()

    assert warnings == [
        "cross_profile_pin_allowlisted:requirements-amd-rocm-full.txt:numpy:"
        "reference=requirements-base.txt:2.1.2:"
        "profile=amd-rocm:actual=2.4.6:expected=2.4.6:"
        "reason=unverified base-version availability on ROCm image; pending confirmation",
        "cross_profile_pin_allowlisted:requirements-amd-rocm.lock.txt:numpy:"
        "reference=requirements-base.txt:2.1.2:"
        "profile=amd-rocm:actual=2.4.6:expected=2.4.6:"
        "reason=unverified base-version availability on ROCm image; pending confirmation",
    ]
    assert (
        "shared_pin_allowlist_unjustified:amd-rocm:numpy:"
        "reason=unverified base-version availability on ROCm image; pending confirmation"
    ) in errors


def test_shared_scientific_pin_allowlist_expected_version_is_enforced(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    _write_shared_pin_fixture(tmp_path, monkeypatch, amd_numpy="2.4.7")
    monkeypatch.setattr(
        validate_dependency_lock,
        "SHARED_PIN_ALLOWLIST",
        (
            validate_dependency_lock.SharedPinAllowlistEntry(
                "numpy",
                "amd-rocm",
                "2.4.6",
                ROCM_DIVERGENCE_REASON,
            ),
        ),
    )

    errors, warnings = validate_dependency_lock._shared_scientific_pin_report()

    assert warnings == []
    assert (
        "cross_profile_pin_mismatch:requirements-amd-rocm-full.txt:numpy:"
        "reference=requirements-base.txt:2.1.2:"
        "profile=amd-rocm:actual=2.4.7:allowlist_expected=2.4.6"
    ) in errors


def test_shared_scientific_pin_mismatch_returns_nonzero(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    _write_shared_pin_fixture(tmp_path, monkeypatch, amd_lock_pydantic="2.99.0")
    monkeypatch.setattr(validate_dependency_lock, "SHARED_PIN_ALLOWLIST", ())
    expected_error = (
        "cross_profile_pin_mismatch:requirements-amd-rocm.lock.txt:pydantic:"
        "reference=requirements.lock.txt:2.13.4:"
        "profile=amd-rocm:actual=2.99.0"
    )

    errors, warnings = validate_dependency_lock._shared_scientific_pin_report()

    assert expected_error in errors
    assert warnings == []

    for report_name in (
        "_requirements_report",
        "_install_manifest_report",
        "_runtime_profile_lock_constraint_report",
        "_lock_file_report",
        "_dev_runtime_separation_report",
        "_tabular_challenger_optional_dependency_report",
        "_profile_requirements_report",
        "_pyproject_report",
        "_npm_lock_report",
        "_ci_workflow_report",
    ):
        monkeypatch.setattr(validate_dependency_lock, report_name, _empty_report)

    assert validate_dependency_lock.main(["--json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert expected_error in payload["errors"]
    assert payload["shared_scientific_pins"]["packages"]
    assert any(
        source["path"] == "requirements-amd-rocm.lock.txt"
        for source in payload["shared_scientific_pins"]["sources"]
    )


def test_lock_hash_validator_rejects_missing_hash(tmp_path: Path) -> None:
    lock_path = tmp_path / "requirements.lock.txt"
    lock_path.write_text(
        "\n".join(
            [
                "--index-url https://pypi.org/simple",
                "example-package==1.0.0",
                "hashed-package==2.0.0 \\",
                "    --hash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "",
            ]
        ),
        encoding="utf-8",
    )

    assert validate_dependency_lock._lock_requirements_missing_hashes(lock_path) == [
        f"{lock_path}:2:example-package"
    ]


def test_strict_lock_report_rejects_missing_hash(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(validate_dependency_lock, "ROOT", tmp_path)
    input_path = tmp_path / "requirements.in"
    lock_path = tmp_path / "requirements.lock.txt"
    input_path.write_text("example-package==1.0.0\n", encoding="utf-8")
    lock_path.write_text("example-package==1.0.0\n", encoding="utf-8")

    errors, _warnings = validate_dependency_lock._lock_file_report(
        lock_path,
        input_path,
        require_hashes=True,
    )

    assert "requirements_lock_missing_hashes:requirements.lock.txt:1:example-package" in errors
