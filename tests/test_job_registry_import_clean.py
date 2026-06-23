from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from engine.runtime.job_registry import ALLOWED_JOBS

REPO_ROOT = Path(__file__).resolve().parents[1]


def _import_script_path(script_path: str) -> None:
    path = (REPO_ROOT / script_path).resolve()
    module_name = "_allowed_job_import_clean_" + "_".join(path.relative_to(REPO_ROOT).with_suffix("").parts)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load module spec for {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)


def test_allowed_job_registry_modules_import_cleanly() -> None:
    errors: list[str] = []

    for job_name, spec in ALLOWED_JOBS.items():
        if not isinstance(spec, (tuple, list)) or not spec:
            errors.append(f"{job_name}: invalid spec {spec!r}")
            continue

        script_path = str(spec[0] or "").strip()
        if not script_path.endswith(".py"):
            continue

        try:
            _import_script_path(script_path)
        except Exception as exc:  # pragma: no cover - assertion message is the useful output
            module_name = ".".join(Path(script_path).with_suffix("").parts)
            errors.append(f"{job_name}: {module_name}: {type(exc).__name__}: {exc}")

    assert errors == []
