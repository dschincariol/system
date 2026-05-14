from __future__ import annotations

import threading

import pytest

from engine.runtime import jobs_manager
from engine.runtime.job_registry import enforce_registered_job_path


def _manual_manager(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    script = root / "engine" / "runtime" / "jobs" / "rogue_unregistered.py"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text("def main():\n    return None\n", encoding="utf-8")
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    monkeypatch.setattr(jobs_manager, "_PROJECT_ROOT", str(root))
    monkeypatch.setattr(jobs_manager, "_ENGINE_DIR", str(root / "engine"))
    monkeypatch.setattr(jobs_manager, "_LOG_DIR", str(log_dir))
    monkeypatch.setattr(jobs_manager, "_acquire_lock", lambda *args, **kwargs: True)
    monkeypatch.setattr(jobs_manager, "_release_lock", lambda *args, **kwargs: None)
    monkeypatch.setattr(jobs_manager, "_write_job_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(jobs_manager, "_job_launch_trace_append", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        jobs_manager.JobManager,
        "_resource_admission",
        lambda self, job: {"ok": True, "profile": {}, "scheduler_state": {}},
    )

    manager = object.__new__(jobs_manager.JobManager)
    manager._lock = threading.Lock()
    manager._jobs = {
        "rogue": jobs_manager.JobState(
            "rogue",
            "engine/runtime/jobs/rogue_unregistered.py",
            "oneshot",
        )
    }
    manager._preflight_fn = None
    manager._get_kill_switches_fn = None
    manager._get_execution_mode_fn = None
    return manager


def test_jobs_manager_blocks_unregistered_job_launch(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TS_ALLOW_UNREGISTERED_JOBS", raising=False)
    monkeypatch.delenv("TS_ENV", raising=False)
    manager = _manual_manager(tmp_path, monkeypatch)

    with pytest.raises(PermissionError, match="unregistered_job: engine/runtime/jobs/rogue_unregistered.py"):
        manager.start("rogue")


def test_unregistered_job_dev_bypass_is_ignored_in_production(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TS_ALLOW_UNREGISTERED_JOBS", "1")
    monkeypatch.setenv("TS_ENV", "production")
    manager = _manual_manager(tmp_path, monkeypatch)

    with pytest.raises(PermissionError, match="unregistered_job: engine/runtime/jobs/rogue_unregistered.py"):
        manager.start("rogue")


def test_unregistered_job_dev_bypass_allows_local_path(tmp_path, monkeypatch) -> None:
    root = tmp_path / "repo"
    script = root / "engine" / "runtime" / "jobs" / "rogue_unregistered.py"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text("def main():\n    return None\n", encoding="utf-8")
    monkeypatch.setenv("TS_ALLOW_UNREGISTERED_JOBS", "1")
    monkeypatch.setenv("TS_ENV", "development")

    assert (
        enforce_registered_job_path(script, repo_root=root)
        == "engine/runtime/jobs/rogue_unregistered.py"
    )
