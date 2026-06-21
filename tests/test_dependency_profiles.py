from __future__ import annotations

import ast
import os
import subprocess
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
        if not line or line.startswith("#") or line.startswith("--"):
            continue
        if line.startswith("-r "):
            names.update(_requirement_names((path.parent / line.split(maxsplit=1)[1]).resolve(), seen))
            continue
        name = line.split(";", 1)[0].split("[", 1)[0]
        for marker in ("==", "~=", ">=", "<=", ">", "<", "==="):
            name = name.split(marker, 1)[0]
        names.add(name.replace("_", "-").lower())
    return names


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


def test_dependency_profile_resolver_blocks_unvalidated_rocm_without_override() -> None:
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

    assert proc.returncode == 64
    assert "amd_rocm_dependency_profile_not_validated" in proc.stderr


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
