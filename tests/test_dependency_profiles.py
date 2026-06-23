from __future__ import annotations

import ast
import os
import platform
import subprocess
import sys
import tomllib
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
CPU_HOT_PATHS = (
    "start_system.py",
    "engine/runtime/platform.py",
    "engine/runtime/health.py",
    "engine/runtime/prod_preflight.py",
    "engine/runtime/hardware.py",
    "engine/data/jobs/process_events.py",
    "engine/data/jobs/process_events_live.py",
    "engine/data/jobs/process_events_enriched.py",
    "engine/data/jobs/process_events_shadow.py",
)


def _requirement_names(path: Path, seen: set[Path] | None = None) -> set[str]:
    seen = seen or set()
    path = path.resolve()
    if path in seen:
        return set()
    seen.add(path)
    names: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-r "):
            names.update(_requirement_names((path.parent / line.split(maxsplit=1)[1]).resolve(), seen))
            continue
        if line.startswith("-"):
            continue
        name = line.split(";", 1)[0].split(" @ ", 1)[0].split("[", 1)[0]
        for marker in ("==", "~=", ">=", "<=", ">", "<", "==="):
            name = name.split(marker, 1)[0]
        names.add(name.replace("_", "-").lower())
    return names


def _requirement_lines(path: Path) -> set[str]:
    lines: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        lines.add(line)
    return lines


def _rocm_python_markers_supported() -> bool:
    return sys.version_info[:2] >= (3, 12) and platform.system() == "Linux"


def test_cpu_default_requirements_exclude_nvidia_only_packages() -> None:
    names = _requirement_names(REPO / "requirements.txt")

    assert "torch" in names
    assert "xgboost-cpu" in names
    assert "xgboost" not in names
    assert not {name for name in names if name == "pynvml" or name.startswith("nvidia-")}
    assert "psycopg" in names
    assert "psycopg2" not in names
    assert "psycopg2-binary" not in names


def test_nvidia_profile_contains_nvidia_diagnostics_packages() -> None:
    names = _requirement_names(REPO / "requirements-nvidia-cuda.txt")

    assert {"torch", "pynvml", "nvidia-ml-py"}.issubset(names)


def test_cpu_hot_paths_have_no_module_level_nvidia_imports() -> None:
    for rel in CPU_HOT_PATHS:
        tree = ast.parse((REPO / rel).read_text(encoding="utf-8"), filename=rel)
        for node in tree.body:
            imported: list[str] = []
            if isinstance(node, ast.Import):
                imported = [alias.name.split(".", 1)[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported = [node.module.split(".", 1)[0]]
            assert not (set(imported) & {"pynvml", "nvidia_smi"}), rel


def test_amd_rocm_profile_contains_rocm_runtime_packages() -> None:
    names = _requirement_names(REPO / "requirements-amd-rocm.txt")

    assert {"torch", "torchaudio", "triton", "xgboost"}.issubset(names)


def test_pyproject_amd_rocm_extra_matches_requirements_profile() -> None:
    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    extra = set(project["project"]["optional-dependencies"]["amd-rocm"])

    assert extra == _requirement_lines(REPO / "requirements-amd-rocm.txt")


def test_dependency_profile_resolver_enforces_amd_rocm_python_markers() -> None:
    env = dict(os.environ)
    env["TRADING_DEPENDENCY_PROFILE"] = "amd-rocm"
    env.pop("TRADING_REQUIREMENTS_FILE", None)
    proc = subprocess.run(
        ["bash", "deploy/bin/resolve_python_requirements.sh", str(REPO)],
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    if _rocm_python_markers_supported():
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip().endswith("requirements-amd-rocm.txt")
    else:
        assert proc.returncode == 65
        assert "amd_rocm_python_runtime_unsupported" in proc.stderr
        assert "required_python=>=3.12" in proc.stderr or "required_platform=Linux" in proc.stderr


def test_dependency_profile_resolver_rejects_unvalidated_amd_rocm_stub(tmp_path) -> None:
    stub = tmp_path / "requirements-amd-rocm.txt"
    stub.write_text("# future AMD/ROCm profile marker only\n", encoding="utf-8")
    env = dict(os.environ)
    env["TRADING_DEPENDENCY_PROFILE"] = "amd-rocm"
    env["TRADING_REQUIREMENTS_FILE"] = str(stub)

    proc = subprocess.run(
        ["bash", "deploy/bin/resolve_python_requirements.sh", str(REPO)],
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert proc.returncode == 65
    assert "amd_rocm_profile_not_validated" in proc.stderr


def test_dependency_profile_resolver_selects_cpu_and_nvidia_profiles() -> None:
    for profile, expected in (
        ("cpu", "requirements.txt"),
        ("nvidia-cuda", "requirements-nvidia-cuda.txt"),
    ):
        env = dict(os.environ)
        env["TRADING_DEPENDENCY_PROFILE"] = profile
        env.pop("TRADING_REQUIREMENTS_FILE", None)
        proc = subprocess.run(
            ["bash", "deploy/bin/resolve_python_requirements.sh", str(REPO)],
            cwd=REPO,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip().endswith(expected)


def test_ci_rocm_profile_job_builds_profile_and_excludes_gpu_marker_by_default() -> None:
    workflow = (REPO / ".github" / "workflows" / "validate.yml").read_text(encoding="utf-8")

    assert "rocm-profile:" in workflow
    assert "requirements-amd-rocm.txt" in workflow
    assert "--build-arg TRADING_DEPENDENCY_PROFILE=amd-rocm" in workflow
    assert "--build-arg TRADING_REQUIREMENTS_FILE=requirements-amd-rocm.txt" in workflow
    assert "amd_rocm_python_runtime_unsupported" in workflow
    assert '-m "not requires_rocm"' in workflow
