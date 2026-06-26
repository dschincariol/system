from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest


def test_validate_job_registry_paths_compiles_with_read_only_source_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from engine.runtime import job_registry

    repo_root = tmp_path / "repo"
    job_dir = repo_root / "engine" / "runtime" / "jobs"
    job_dir.mkdir(parents=True)
    job_file = job_dir / "unit_job.py"
    job_file.write_text("def main():\n    return None\n", encoding="utf-8")

    monkeypatch.setattr(
        job_registry,
        "ALLOWED_JOBS",
        {"unit_job": ("engine/runtime/jobs/unit_job.py", "oneshot", None, {"fallback_feed": True})},
    )
    monkeypatch.setattr(job_registry, "PIPELINE_ORDER", [])
    monkeypatch.setattr(job_registry, "JOB_ORDER", [])
    monkeypatch.setattr(job_registry, "QUARANTINED_JOB_FILES", set())
    monkeypatch.setattr(job_registry, "_source_allowed_job_duplicates", lambda: [])

    chmod_paths = [job_file, job_dir, job_dir.parent, job_dir.parent.parent, repo_root]
    previous_modes = {path: stat.S_IMODE(path.stat().st_mode) for path in chmod_paths}
    try:
        for path in chmod_paths:
            path.chmod(0o555 if path.is_dir() else 0o444)

        result = job_registry.validate_job_registry_paths(repo_root=repo_root)

        assert result == {"ok": True, "errors": []}
        assert not (job_dir / "__pycache__").exists()
    finally:
        for path, mode in reversed(previous_modes.items()):
            if path.exists():
                os.chmod(path, mode)
