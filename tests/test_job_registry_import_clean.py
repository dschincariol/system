from __future__ import annotations

import importlib
from pathlib import Path

from engine.runtime.job_registry import ALLOWED_JOBS


def test_allowed_job_registry_modules_import_cleanly() -> None:
    errors: list[str] = []

    for job_name, spec in ALLOWED_JOBS.items():
        if not isinstance(spec, (tuple, list)) or not spec:
            errors.append(f"{job_name}: invalid spec {spec!r}")
            continue

        script_path = str(spec[0] or "").strip()
        if not script_path.endswith(".py"):
            continue

        module_name = ".".join(Path(script_path).with_suffix("").parts)
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # pragma: no cover - assertion message is the useful output
            errors.append(f"{job_name}: {module_name}: {type(exc).__name__}: {exc}")

    assert errors == []
