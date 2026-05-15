from __future__ import annotations

import threading

import pytest

from engine.runtime import jobs_manager
from engine.runtime import job_registry
from engine.runtime.job_registry import enforce_registered_job_path


def test_job_history_write_is_best_effort(monkeypatch) -> None:
    def fail_history(*args, **kwargs):
        raise TimeoutError("history pool timeout")

    monkeypatch.setattr(jobs_manager, "_write_job_history_impl", fail_history)

    jobs_manager._write_job_history("poll_prices", "exit", "process exited", 1)


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


def test_registry_includes_safe_bootstrap_job_files() -> None:
    expected = {
        "backtest_walk_forward": "engine/strategy/jobs/backtest_walk_forward.py",
        "portfolio_backtest": "engine/strategy/jobs/portfolio_backtest.py",
        "calibrate_price_confidence": "engine/data/jobs/calibrate_price_confidence.py",
        "strategy_kill_drift_monitor": "engine/strategy/jobs/kill_drift_monitor.py",
        "strategy_kill_health_monitor": "engine/strategy/jobs/kill_health_monitor.py",
        "strategy_kill_slippage_monitor": "engine/strategy/jobs/kill_slippage_monitor.py",
        "train_drawdown_policy": "engine/strategy/jobs/train_drawdown_policy.py",
        "recalibrate_confidence": "engine/strategy/jobs/recalibrate_confidence.py",
        "shadow_train": "engine/strategy/jobs/shadow_train_job.py",
        "strategy_governance": "engine/strategy/jobs/strategy_governance_job.py",
        "trade_pipeline": "engine/strategy/jobs/trade_pipeline_job.py",
        "compute_exec_labels": "engine/execution/jobs/compute_exec_labels.py",
        "compute_exec_labels_from_fills": "engine/execution/jobs/compute_exec_labels_from_fills.py",
        "compute_exec_z": "engine/execution/jobs/compute_exec_z.py",
    }

    for name, script in expected.items():
        spec = job_registry.ALLOWED_JOBS.get(name)
        assert spec is not None
        assert spec[0] == script

    result = job_registry.validate_runtime_architecture(import_check=False)
    assert result["ok"] is True


def test_registry_validation_still_flags_untracked_job_files(monkeypatch) -> None:
    registered = job_registry._registered_job_script_paths()
    rogue = "engine/runtime/jobs/rogue_untracked.py"
    monkeypatch.setattr(
        job_registry,
        "_discover_repo_job_files",
        lambda _repo_root=None: set(registered) | {rogue},
    )

    result = job_registry.validate_job_registry_paths(import_check=False)

    assert result["ok"] is False
    assert f"untracked_job_file:{rogue}" in result["errors"]


def test_job_manager_list_jobs_can_timeout_when_manager_lock_is_busy() -> None:
    manager = object.__new__(jobs_manager.JobManager)
    manager._lock = threading.Lock()
    manager._jobs = {}
    manager._lock.acquire()
    try:
        with pytest.raises(TimeoutError, match="jobs_manager_lock_timeout"):
            manager.list_jobs(timeout_s=0.01, include_persisted=False)
    finally:
        manager._lock.release()
