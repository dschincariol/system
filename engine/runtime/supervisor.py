from __future__ import annotations
"""
Unified Runtime Supervisor (DAG-hardened)

Preserves:
- register_job()
- start()
- stop()
- restart()
- stop_all()
- status()
- heartbeat()
- daemon auto-restart (only when NOT delegating to JobManager)
- restart_count tracking
- exit code tracking
- monitor loop (only when NOT delegating)

Adds (structural / no behavior change by default):
- deterministic_start() with strict DAG enforcement (cycle + missing deps)
- optional dependency enforcement on start() via ENV gate
- optional delegation to JobManager (preferred; single process launcher)
- restart backoff + crash-loop guard (only when NOT delegating)

ENV (all optional):
  SUPERVISOR_ENFORCE_DEPS_ON_START=0|1
  SUPERVISOR_MONITOR_WHEN_DELEGATING=0|1
  SUPERVISOR_RESTART_BASE_DELAY_MS=2000
  SUPERVISOR_RESTART_MAX_DELAY_MS=30000
  SUPERVISOR_RESTART_WINDOW_S=120
  SUPERVISOR_RESTART_MAX_IN_WINDOW=5
  SUPERVISOR_MONITOR_PERIOD_S=2.0
"""

"""
FILE: supervisor.py

Runtime subsystem module for `supervisor`.
"""

import json
import os
import subprocess
import threading
import time
import sys
from collections import deque
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.job_registry import (
    ALLOWED_JOBS,
    JOB_ORDER,
    PIPELINE_ORDER,
    validate_runtime_architecture,
)
from engine.runtime.log_retention import rotate_log_if_needed
from engine.runtime.logging import get_logger
from engine.runtime.platform import default_local_log_dir

LOG = get_logger("runtime_supervisor")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: Optional[str] = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=30,
        component="runtime_supervisor",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)

def _write_supervisor_analysis(snapshot: Dict[str, Any]) -> None:
    try:
        from engine.runtime.runtime_meta import meta_set
        meta_set("supervisor_analysis", json.dumps(snapshot, separators=(",", ":"), sort_keys=True))
    except Exception as e:
        _warn_nonfatal(
            "supervisor_analysis_persist_failed",
            "SUPERVISOR_ANALYSIS_PERSIST_FAILED",
            e,
            warn_key="supervisor_analysis_persist_failed",
        )

def _build_supervisor_analysis_from_jobs(jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Shape a compact diagnostics payload for UI/operator consumers rather than
    # internal scheduling logic.
    now_ms = int(time.time() * 1000)
    rows = [dict(row or {}) for row in (jobs or [])]
    failed_jobs = [str(r.get("name") or "") for r in rows if str(r.get("status") or "").upper() == "FAILED"]
    restarting_jobs = [str(r.get("name") or "") for r in rows if int(r.get("restart_count") or 0) > 0]
    loop_jobs = [
        {
            "job": str(r.get("name") or ""),
            "restart_count": int(r.get("restart_count") or 0),
            "restart_window_exhaustions": int(r.get("restart_window_exhaustions") or 0),
            "failed_reason": str(r.get("failed_reason") or ""),
            "last_exit_code": r.get("last_exit_code", r.get("exit_code")),
        }
        for r in rows
        if int(r.get("restart_count") or 0) > 1 or int(r.get("restart_window_exhaustions") or 0) > 0
    ]
    crash_cause = ""
    if failed_jobs:
        first_failed = next((r for r in rows if str(r.get("name") or "") == failed_jobs[0]), {})
        crash_cause = str(first_failed.get("failed_reason") or first_failed.get("last_error") or first_failed.get("last_exit_code") or "").strip()

    return {
        "ok": len(failed_jobs) == 0 and len(loop_jobs) == 0,
        "restart_loops_detected": len(loop_jobs) > 0,
        "restart_loops": loop_jobs,
        "failed_jobs": failed_jobs,
        "restarting_jobs": restarting_jobs,
        "crash_cause": crash_cause,
        "exit_patterns": [
            {
                "job": str(r.get("name") or ""),
                "last_exit_code": r.get("last_exit_code", r.get("exit_code")),
                "failed_reason": str(r.get("failed_reason") or r.get("last_error") or ""),
                "restart_count": int(r.get("restart_count") or 0),
            }
            for r in rows
            if r.get("last_exit_code", r.get("exit_code")) is not None or str(r.get("failed_reason") or r.get("last_error") or "").strip()
        ][:100],
        "ts_ms": now_ms,
    }


class JobSpec:
    def __init__(self, name: str, script: str, daemon: bool = False):
        self.name = name
        self.script = script
        self.daemon = daemon


class JobState:
    def __init__(self, spec: JobSpec):
        self.spec = spec
        self.process: Optional[subprocess.Popen] = None
        self.last_heartbeat_ts: float = 0.0
        self.last_start_ts: float = 0.0
        self.last_exit_code: Optional[int] = None
        self.restart_count: int = 0
        self.failed_reason: Optional[str] = None
        self._restart_window_exhaustions: int = 0

        self._restart_ts: Deque[float] = deque(maxlen=256)
        self._next_restart_allowed_ts: float = 0.0
        self._current_delay_ms: int = 0


def _default_deps_from_pipeline(pipeline: List[str]) -> Dict[str, List[str]]:
    deps: Dict[str, List[str]] = {}
    prev: Optional[str] = None
    for name in pipeline or []:
        if prev is None:
            deps.setdefault(name, [])
        else:
            deps.setdefault(name, []).append(prev)
        prev = name
    return deps


def _now() -> float:
    return time.time()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _job_log_dir() -> Path:
    log_dir = Path(
        os.environ.get("TRADING_LOGS")
        or os.environ.get("LOG_DIR")
        or str(default_local_log_dir().resolve())
    )
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _job_log_paths(name: str) -> tuple[str, str]:
    safe = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in str(name))
    log_dir = _job_log_dir()
    return (
        str((log_dir / f"{safe}.stdout.log").resolve()),
        str((log_dir / f"{safe}.stderr.log").resolve()),
    )


def _apply_ingestion_shard_env(env: Dict[str, str]) -> None:
    from engine.runtime.ingestion_shards import canonical_shard_env, current_ingestion_shard

    env.update(canonical_shard_env(current_ingestion_shard()))


class RuntimeSupervisor:
    def __init__(self, jobs=None):
        self._jobs: Dict[str, JobState] = {}
        self._lock = threading.Lock()

        # When a JobsManager delegate is present, this class becomes a façade
        # over that owner while preserving the older supervisor API.
        self._delegate = jobs
        self._deps = _default_deps_from_pipeline(list(PIPELINE_ORDER or []))

        self._enforce_deps_on_start = os.environ.get("SUPERVISOR_ENFORCE_DEPS_ON_START", "0") == "1"
        self._monitor_when_delegating = os.environ.get("SUPERVISOR_MONITOR_WHEN_DELEGATING", "0") == "1"

        self._restart_base_delay_ms = int(os.environ.get("SUPERVISOR_RESTART_BASE_DELAY_MS", "2000"))
        self._restart_max_delay_ms = int(os.environ.get("SUPERVISOR_RESTART_MAX_DELAY_MS", "30000"))
        self._restart_window_s = int(os.environ.get("SUPERVISOR_RESTART_WINDOW_S", "120"))
        self._restart_max_in_window = int(os.environ.get("SUPERVISOR_RESTART_MAX_IN_WINDOW", "5"))
        self._monitor_period_s = float(os.environ.get("SUPERVISOR_MONITOR_PERIOD_S", "2.0"))

        self._monitor_thread = None
        self._stop_event = threading.Event()
        # The internal monitor loop is only needed when we own process lifecycles
        # directly, or when explicitly requested while delegating.
        if self._delegate is None or self._monitor_when_delegating:
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                daemon=True,
                name="runtime_supervisor_monitor",
            )
            self._monitor_thread.start()

    def register_job(self, name: str, script: str, daemon: bool = False):
        with self._lock:
            if name in self._jobs:
                raise ValueError(f"Job already registered: {name}")
            spec = JobSpec(name=name, script=script, daemon=daemon)
            self._jobs[name] = JobState(spec)

    def allowed_jobs(self) -> Dict[str, Any]:
        return dict(ALLOWED_JOBS)

    def job_order(self) -> List[str]:
        return list(JOB_ORDER or [])

    def pipeline_order(self) -> List[str]:
        return list(PIPELINE_ORDER or [])

    def status(self) -> Dict[str, Any]:
        if self._delegate is not None:
            try:
                try:
                    jobs = self._delegate.list_jobs(
                        timeout_s=max(0.05, float(os.environ.get("API_JOB_LIST_TIMEOUT_S", "0.5"))),
                        include_persisted=False,
                    )
                except TypeError:
                    jobs = self._delegate.list_jobs()
                analysis = _build_supervisor_analysis_from_jobs(jobs if isinstance(jobs, list) else [])
                _write_supervisor_analysis(analysis)
                return {"ok": True, "delegated": True, "jobs": jobs, "supervisor_analysis": analysis}
            except TimeoutError as e:
                _warn_nonfatal(
                    "supervisor_delegate_status_timeout",
                    "SUPERVISOR_DELEGATE_STATUS_TIMEOUT",
                    e,
                    warn_key="supervisor_delegate_status_timeout",
                )
                analysis = {
                    "ok": False,
                    "restart_loops_detected": False,
                    "restart_loops": [],
                    "failed_jobs": [],
                    "restarting_jobs": [],
                    "crash_cause": str(e),
                    "exit_patterns": [],
                    "ts_ms": int(time.time() * 1000),
                }
                return {
                    "ok": False,
                    "delegated": True,
                    "error": "jobs_list_timeout",
                    "jobs": [],
                    "supervisor_analysis": analysis,
                }
            except Exception as e:
                _warn_nonfatal(
                    "supervisor_delegate_status_failed",
                    "SUPERVISOR_DELEGATE_STATUS_FAILED",
                    e,
                    warn_key="supervisor_delegate_status_failed",
                )
                analysis = {
                    "ok": False,
                    "restart_loops_detected": False,
                    "restart_loops": [],
                    "failed_jobs": [],
                    "restarting_jobs": [],
                    "crash_cause": str(e),
                    "exit_patterns": [],
                    "ts_ms": int(time.time() * 1000),
                }
                _write_supervisor_analysis(analysis)
                return {"ok": False, "delegated": True, "error": str(e), "jobs": [], "supervisor_analysis": analysis}

        out = {}
        with self._lock:
            for name, state in self._jobs.items():
                proc = state.process
                running = False
                pid = None
                try:
                    if proc is not None and proc.poll() is None:
                        running = True
                        pid = proc.pid
                except Exception:
                    running = False
                    pid = None

                status = "STOPPED"
                if state.failed_reason:
                    status = "FAILED"
                elif running:
                    status = "RUNNING"
                elif state._next_restart_allowed_ts and state._next_restart_allowed_ts > _now():
                    status = "STARTING"
                elif state.last_exit_code is not None:
                    status = "FAILED" if int(state.last_exit_code) != 0 else "STOPPED"

                out[name] = {
                    "name": name,
                    "status": status,
                    "running": running,
                    "pid": pid,
                    "last_start_ts": state.last_start_ts,
                    "last_exit_code": state.last_exit_code,
                    "restart_count": state.restart_count,
                    "restart_window_exhaustions": int(state._restart_window_exhaustions or 0),
                    "failed_reason": state.failed_reason,
                    "last_heartbeat_ts": state.last_heartbeat_ts,
                }
        analysis = _build_supervisor_analysis_from_jobs(list(out.values()))
        _write_supervisor_analysis(analysis)
        return {"ok": True, "delegated": False, "jobs": out, "supervisor_analysis": analysis}

    def heartbeat(self, name: str) -> None:
        if self._delegate is not None:
            return
        with self._lock:
            state = self._require(name)
            state.last_heartbeat_ts = _now()

    def start(self, name: str) -> Dict[str, Any]:
        if self._delegate is not None:
            if self._enforce_deps_on_start:
                return self.start_with_deps(name, strict=True)
            try:
                old = os.environ.get("ENGINE_SUPERVISED")
                old_shard = {
                    "INGESTION_SHARD_INDEX": os.environ.get("INGESTION_SHARD_INDEX"),
                    "INGESTION_SHARD_COUNT": os.environ.get("INGESTION_SHARD_COUNT"),
                }
                os.environ["ENGINE_SUPERVISED"] = "1"
                shard_env: Dict[str, str] = {}
                _apply_ingestion_shard_env(shard_env)
                os.environ.update(shard_env)
                try:
                    return self._delegate.start(name)
                finally:
                    if old is None:
                        try:
                            del os.environ["ENGINE_SUPERVISED"]
                        except Exception as e:
                            _warn_nonfatal(
                                "supervisor_delegate_env_cleanup_failed",
                                "SUPERVISOR_DELEGATE_ENV_CLEANUP_FAILED",
                                e,
                                warn_key="supervisor_delegate_env_cleanup_failed",
                                env_key="ENGINE_SUPERVISED",
                                job=str(name),
                            )
                    else:
                        os.environ["ENGINE_SUPERVISED"] = old
                    for key, value in old_shard.items():
                        if value is None:
                            try:
                                del os.environ[key]
                            except Exception as e:
                                _warn_nonfatal(
                                    "supervisor_delegate_env_cleanup_failed",
                                    "SUPERVISOR_DELEGATE_ENV_CLEANUP_FAILED",
                                    e,
                                    warn_key=f"supervisor_delegate_env_cleanup_failed:{key}",
                                    env_key=str(key),
                                    job=str(name),
                                )
                        else:
                            os.environ[key] = value
            except Exception as e:
                _warn_nonfatal(
                    "supervisor_delegate_start_failed",
                    "SUPERVISOR_DELEGATE_START_FAILED",
                    e,
                    warn_key=f"supervisor_delegate_start_failed:{name}",
                    job=str(name),
                )
                return {"ok": False, "error": str(e)}

        if self._enforce_deps_on_start:
            return self.start_with_deps(name, strict=True)

        # In non-delegated mode the supervisor launches child processes directly
        # and tracks local restart/backoff state in JobState.
        with self._lock:
            state = self._require(name)

            if state.process and state.process.poll() is None:
                return {"ok": True, "already_running": True}

            env = os.environ.copy()
            env["ENGINE_SUPERVISED"] = "1"
            env["ENGINE_LAUNCHED_BY_SUPERVISOR"] = "1"
            env["ENGINE_JOB_NAME"] = str(name)
            _apply_ingestion_shard_env(env)
            try:
                spec = ALLOWED_JOBS.get(str(name))
                meta = dict(spec[3] if isinstance(spec, (tuple, list)) and len(spec) >= 4 and isinstance(spec[3], dict) else {})
                env["ENGINE_PROCESS_ROLE"] = str(meta.get("resource_class") or "")
                from engine.runtime.thread_policy import apply_cpu_thread_policy_to_env

                apply_cpu_thread_policy_to_env(env, role=env.get("ENGINE_PROCESS_ROLE") or None)
            except Exception as e:
                _warn_nonfatal(
                    "supervisor_thread_policy_failed",
                    "SUPERVISOR_THREAD_POLICY_FAILED",
                    e,
                    warn_key=f"supervisor_thread_policy_failed:{name}",
                    job=str(name),
                )

            repo_root = str(_repo_root())
            existing_pp = env.get("PYTHONPATH", "")
            parts = [p for p in existing_pp.split(os.pathsep) if p]
            if repo_root not in parts:
                parts.insert(0, repo_root)
            env["PYTHONPATH"] = os.pathsep.join(parts)

            script_path = str((_repo_root() / state.spec.script).resolve())
            stdout_path, stderr_path = _job_log_paths(name)

            rotate_log_if_needed(stdout_path)
            rotate_log_if_needed(stderr_path)
            stdout_fh = open(stdout_path, "ab")
            stderr_fh = open(stderr_path, "ab")

            try:
                state.process = subprocess.Popen(
                    [sys.executable, "-u", script_path],
                    cwd=repo_root,
                    stdout=stdout_fh,
                    stderr=stderr_fh,
                    env=env,
                    close_fds=(not sys.platform.startswith("win")),
                    creationflags=(subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0),
                )
            except Exception:
                stdout_fh.close()
                stderr_fh.close()
                LOG.exception(
                    "SUPERVISOR_START_FAILED",
                    extra={"job": str(name), "script_path": str(script_path)},
                )
                raise
            finally:
                try:
                    stdout_fh.close()
                except Exception as e:
                    _warn_nonfatal(
                        "supervisor_stdout_log_close_failed",
                        "SUPERVISOR_STDOUT_LOG_CLOSE_FAILED",
                        e,
                        warn_key=f"supervisor_stdout_log_close_failed:{name}",
                        job=str(name),
                        log_path=str(stdout_path),
                    )
                try:
                    stderr_fh.close()
                except Exception as e:
                    _warn_nonfatal(
                        "supervisor_stderr_log_close_failed",
                        "SUPERVISOR_STDERR_LOG_CLOSE_FAILED",
                        e,
                        warn_key=f"supervisor_stderr_log_close_failed:{name}",
                        job=str(name),
                        log_path=str(stderr_path),
                    )

            state.last_start_ts = _now()
            state.last_exit_code = None
            state.failed_reason = None

            LOG.info(
                "SUPERVISOR_JOB_STARTED",
                extra={
                    "job": str(name),
                    "pid": int(state.process.pid),
                    "stdout_log": str(stdout_path),
                    "stderr_log": str(stderr_path),
                },
            )

            return {"ok": True}
    def stop(self, name: str) -> Dict[str, Any]:
        if self._delegate is not None:
            try:
                return self._delegate.stop(name)
            except Exception as e:
                _warn_nonfatal(
                    "supervisor_delegate_stop_failed",
                    "SUPERVISOR_DELEGATE_STOP_FAILED",
                    e,
                    warn_key=f"supervisor_delegate_stop_failed:{name}",
                    job=str(name),
                )
                return {"ok": False, "error": str(e)}

        with self._lock:
            state = self._require(name)
            if state.process and state.process.poll() is None:
                state.process.terminate()
                try:
                    state.process.wait(timeout=5)
                except Exception as wait_error:
                    try:
                        state.process.kill()
                        try:
                            state.process.wait(timeout=5)
                        except Exception as second_wait_error:
                            _warn_nonfatal(
                                "supervisor_stop_wait_after_kill_failed",
                                "SUPERVISOR_STOP_WAIT_AFTER_KILL_FAILED",
                                second_wait_error,
                                warn_key=f"supervisor_stop_wait_after_kill_failed:{name}",
                                job=str(name),
                            )
                    except Exception as kill_error:
                        _warn_nonfatal(
                            "supervisor_stop_kill_failed",
                            "SUPERVISOR_STOP_KILL_FAILED",
                            kill_error,
                            warn_key=f"supervisor_stop_kill_failed:{name}",
                            job=str(name),
                            wait_error=str(wait_error),
                        )
            state.process = None
            return {"ok": True}

    def restart(self, name: str) -> Dict[str, Any]:
        stop_result = self.stop(name)
        if isinstance(stop_result, dict) and not stop_result.get("ok"):
            return stop_result
        return self.start(name)

    def stop_all(
        self,
        *,
        drain_before_kill: Optional[Callable[..., Dict[str, Any]]] = None,
        drain_deadline_s: Optional[float] = None,
    ) -> Dict[str, Any]:
        if self._delegate is not None:
            try:
                try:
                    result = self._delegate.stop_all(
                        drain_before_kill=drain_before_kill,
                        drain_deadline_s=drain_deadline_s,
                    )
                except TypeError as exc:
                    message = str(exc)
                    if (
                        "drain_before_kill" not in message
                        and "drain_deadline_s" not in message
                        and "unexpected keyword" not in message
                        and "got an unexpected" not in message
                    ):
                        raise
                    result = self._delegate.stop_all()
                if isinstance(result, dict):
                    return result
                return {"ok": True}
            except Exception as e:
                _warn_nonfatal(
                    "supervisor_delegate_stop_all_failed",
                    "SUPERVISOR_DELEGATE_STOP_ALL_FAILED",
                    e,
                    warn_key="supervisor_delegate_stop_all_failed",
                )
                return {"ok": False, "error": str(e)}

        self._stop_event.set()

        names: List[str]
        errors: List[str] = []
        with self._lock:
            names = list(self._jobs.keys())

        if drain_before_kill is not None:
            states: list[tuple[str, JobState, subprocess.Popen[Any]]] = []
            with self._lock:
                for name in names:
                    state = self._jobs.get(name)
                    proc = state.process if state is not None else None
                    if state is not None and proc is not None and proc.poll() is None:
                        states.append((name, state, proc))

            for name, state, proc in states:
                try:
                    proc.terminate()
                    LOG.info(
                        "SUPERVISOR_JOB_TERMINATE_SENT",
                        extra={"job": str(name), "pid": int(getattr(proc, "pid", 0) or 0)},
                    )
                except Exception as e:
                    errors.append(f"{name}:terminate:{e}")
                    state.failed_reason = f"terminate_failed:{e}"

            drain_snapshot: Dict[str, Any] = {}
            deadline = time.monotonic() + max(0.0, float(drain_deadline_s or 0.0))
            try:
                drain_snapshot = dict(
                    drain_before_kill(
                        reason="runtime_supervisor_stop_all_pre_sigkill",
                        deadline_s=max(0.0, float(deadline) - time.monotonic()),
                    )
                    or {}
                )
            except Exception as e:
                errors.append(f"drain_before_kill:{e}")
                _warn_nonfatal(
                    "supervisor_stop_all_drain_before_kill_failed",
                    "SUPERVISOR_STOP_ALL_DRAIN_BEFORE_KILL_FAILED",
                    e,
                    warn_key="supervisor_stop_all_drain_before_kill_failed",
                )

            for name, state, proc in states:
                try:
                    if proc.poll() is None:
                        proc.kill()
                        try:
                            proc.wait(timeout=5)
                        except Exception as kill_wait_err:
                            _warn_nonfatal(
                                "supervisor_stop_all_wait_after_kill_failed",
                                "SUPERVISOR_STOP_ALL_WAIT_AFTER_KILL_FAILED",
                                kill_wait_err,
                                warn_key=f"supervisor_stop_all_wait_after_kill_failed:{name}",
                                job=str(name),
                            )
                    state.last_exit_code = proc.poll()
                    state.process = None
                except Exception as e:
                    errors.append(f"{name}:kill:{e}")
                    _warn_nonfatal(
                        "supervisor_stop_all_kill_failed",
                        "SUPERVISOR_STOP_ALL_KILL_FAILED",
                        e,
                        warn_key=f"supervisor_stop_all_kill_failed:{name}",
                        job=str(name),
                    )

            if self._monitor_thread is not None and self._monitor_thread.is_alive():
                try:
                    self._monitor_thread.join(timeout=max(1.0, float(self._monitor_period_s) * 2.0))
                except Exception as e:
                    errors.append(f"monitor_join:{e}")

            return {"ok": len(errors) == 0, "errors": errors, "drain": drain_snapshot}

        for name in names:
            try:
                self.stop(name)
            except Exception as e:
                errors.append(f"{name}:{e}")

        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            try:
                self._monitor_thread.join(timeout=max(1.0, float(self._monitor_period_s) * 2.0))
            except Exception as e:
                errors.append(f"monitor_join:{e}")

        return {"ok": len(errors) == 0, "errors": errors}

    def deterministic_start(
        self,
        targets: List[str],
        *,
        include_deps: bool = True,
        strict: bool = True,
    ) -> Dict[str, Any]:
        targets = [str(x).strip() for x in (targets or []) if str(x).strip()]

        v = self.validate_graph(strict=strict)
        if not v.get("ok"):
            return {"ok": False, "order": [], "started": [], "errors": list(v.get("errors") or [])}

        if include_deps:
            try:
                resolved = self._topo_expand(targets, strict=strict)
            except Exception as e:
                _warn_nonfatal(
                    "supervisor_topo_expand_failed",
                    "SUPERVISOR_TOPO_EXPAND_FAILED",
                    e,
                    warn_key=f"supervisor_topo_expand_failed:{','.join(targets)}:{strict}",
                    targets=list(targets),
                    strict=bool(strict),
                )
                return {"ok": False, "order": [], "started": [], "errors": [str(e)]}
        else:
            resolved = targets

        started = []
        errors = []
        ok = True

        for name in resolved:
            if not self._is_known_job(name):
                if strict:
                    ok = False
                    errors.append(f"not_registered:{name}")
                    return {"ok": False, "order": resolved, "started": started, "errors": errors}
                continue

            r = self.start(name)
            if not r.get("ok"):
                ok = False
                errors.append(f"start_failed:{name}:{r.get('error') or ''}".strip(":"))
                if strict:
                    return {"ok": False, "order": resolved, "started": started, "errors": errors}
            else:
                started.append(name)

        return {"ok": ok, "order": resolved, "started": started, "errors": errors}

    def start_with_deps(self, name: str, *, strict: bool = True) -> Dict[str, Any]:
        return self.deterministic_start([name], include_deps=True, strict=strict)

    def validate_graph(self, *, strict: bool = True) -> Dict[str, Any]:
        known = set(ALLOWED_JOBS.keys())
        errors: List[str] = list(
            (validate_runtime_architecture(repo_root=_repo_root(), import_check=False).get("errors") or [])
        )

        for n in (PIPELINE_ORDER or []):
            if n not in known:
                errors.append(f"pipeline_unknown_job:{n}")

        seen: Set[str] = set()
        visiting: Set[str] = set()

        def dfs(n: str):
            if n in seen:
                return
            if n in visiting:
                errors.append(f"dependency_cycle:{n}")
                return
            visiting.add(n)
            for d in (self._deps.get(n) or []):
                if d not in known:
                    errors.append(f"missing_dep:{n}->{d}")
                    continue
                dfs(d)
            visiting.remove(n)
            seen.add(n)

        for n in list(known):
            dfs(n)

        ok = len(errors) == 0
        if strict and not ok:
            return {"ok": False, "errors": errors}
        return {"ok": True, "errors": errors}

    def _topo_expand(self, targets: List[str], strict: bool = True) -> List[str]:
        order_index = {name: i for i, name in enumerate(list(JOB_ORDER or []))}
        known = set(ALLOWED_JOBS.keys())
        seen: Set[str] = set()
        out: List[str] = []

        def visit(n: str, stack: Set[str]):
            if n in seen:
                return
            if n in stack:
                if strict:
                    raise RuntimeError(f"dependency_cycle:{n}")
                return

            stack.add(n)
            for d in (self._deps.get(n) or []):
                if d not in known:
                    if strict:
                        raise RuntimeError(f"missing_dep:{n}->{d}")
                    continue
                visit(d, stack)
            stack.remove(n)
            seen.add(n)
            out.append(n)

        for t in targets:
            if t not in known and strict:
                raise RuntimeError(f"not_registered:{t}")
            visit(t, set())

        prioritized = []
        deferred = []

        for n in out:
            if n in order_index:
                prioritized.append(n)
            else:
                deferred.append(n)

        prioritized.sort(key=lambda n: order_index.get(n, 10**9))
        return prioritized + deferred

    def _is_known_job(self, name: str) -> bool:
        try:
            return str(name) in set(ALLOWED_JOBS.keys())
        except Exception as e:
            _warn_nonfatal(
                "supervisor_known_job_check_failed",
                "SUPERVISOR_KNOWN_JOB_CHECK_FAILED",
                e,
                warn_key="supervisor_known_job_check_failed",
            )
            return False

    def _require(self, name: str) -> JobState:
        if name not in self._jobs:
            raise ValueError(f"Job not registered: {name}")
        return self._jobs[name]

    def _monitor_loop(self):
        while not self._stop_event.wait(timeout=self._monitor_period_s):
            if self._delegate is not None and not self._monitor_when_delegating:
                continue

            if self._delegate is not None:
                continue

            with self._lock:
                for name, state in self._jobs.items():
                    if not state.process:
                        continue

                    exit_code = None
                    try:
                        exit_code = state.process.poll()
                    except Exception:
                        LOG.exception("SUPERVISOR_POLL_FAILED", extra={"job": str(name)})
                        exit_code = None

                    if exit_code is None:
                        continue

                    state.last_exit_code = exit_code
                    state.process = None
                    _write_supervisor_analysis(_build_supervisor_analysis_from_jobs([
                        {
                            "name": str(job_name),
                            "status": "FAILED" if int(state.last_exit_code or 0) != 0 else "STOPPED",
                            "last_exit_code": state.last_exit_code,
                            "restart_count": int(state.restart_count),
                            "restart_window_exhaustions": int(state._restart_window_exhaustions or 0),
                            "failed_reason": str(state.failed_reason or ""),
                        }
                        for job_name, state in self._jobs.items()
                    ]))

                    if not state.spec.daemon:
                        continue

                    now = _now()

                    while state._restart_ts and (now - state._restart_ts[0]) > float(self._restart_window_s):
                        state._restart_ts.popleft()

                    if len(state._restart_ts) >= int(self._restart_max_in_window):
                        state._restart_window_exhaustions = int(state._restart_window_exhaustions or 0) + 1
                        max_cooldowns = int(os.environ.get("SUPERVISOR_RESTART_MAX_COOLDOWNS", "3"))
                        if state._restart_window_exhaustions >= max_cooldowns:
                            state.failed_reason = (
                                f"restart_limit_exhausted windows={state._restart_window_exhaustions} "
                                f"max_in_window={self._restart_max_in_window} "
                                f"window_s={self._restart_window_s}"
                            )
                            LOG.error(
                                "SUPERVISOR_PERMANENT_FAILURE",
                                extra={
                                    "job": str(name),
                                    "restart_count": int(len(state._restart_ts)),
                                    "restart_window_s": float(self._restart_window_s),
                                    "restart_window_exhaustions": int(state._restart_window_exhaustions),
                                    "last_exit_code": state.last_exit_code,
                                    "failed_reason": str(state.failed_reason),
                                },
                            )
                            _write_supervisor_analysis(_build_supervisor_analysis_from_jobs([
                                {
                                    "name": str(job_name),
                                    "status": "FAILED" if int(st.last_exit_code or 0) != 0 or str(st.failed_reason or "") else "STOPPED",
                                    "last_exit_code": st.last_exit_code,
                                    "restart_count": int(st.restart_count),
                                    "restart_window_exhaustions": int(st._restart_window_exhaustions or 0),
                                    "failed_reason": str(st.failed_reason or ""),
                                }
                                for job_name, st in self._jobs.items()
                            ]))
                            continue

                        LOG.error(
                            "SUPERVISOR_RESTART_GUARD_TRIGGERED",
                            extra={
                                "job": str(name),
                                "restart_count": int(len(state._restart_ts)),
                                "restart_window_s": float(self._restart_window_s),
                                "restart_window_exhaustions": int(state._restart_window_exhaustions),
                                "last_exit_code": state.last_exit_code,
                            },
                        )
                        _write_supervisor_analysis(_build_supervisor_analysis_from_jobs([
                            {
                                "name": str(job_name),
                                "status": "FAILED" if int(st.last_exit_code or 0) != 0 or str(st.failed_reason or "") else "STOPPED",
                                "last_exit_code": st.last_exit_code,
                                "restart_count": int(st.restart_count),
                                "restart_window_exhaustions": int(st._restart_window_exhaustions or 0),
                                "failed_reason": str(st.failed_reason or ""),
                            }
                            for job_name, st in self._jobs.items()
                        ]))
                        continue

                    if now < state._next_restart_allowed_ts:
                        LOG.warning(
                            "SUPERVISOR_RESTART_DELAY_ACTIVE",
                            extra={
                                "job": str(name),
                                "next_restart_allowed_ts": float(state._next_restart_allowed_ts),
                                "last_exit_code": state.last_exit_code,
                            },
                        )
                        continue

                    if state._current_delay_ms <= 0:
                        state._current_delay_ms = int(self._restart_base_delay_ms)
                    else:
                        state._current_delay_ms = min(
                            int(self._restart_max_delay_ms),
                            int(state._current_delay_ms * 2),
                        )

                    delay_s = float(state._current_delay_ms) / 1000.0
                    state._next_restart_allowed_ts = now + delay_s
                    state._restart_ts.append(now)
                    state.restart_count += 1

                    def _restart_later(job_name: str, restart_delay_s: float):
                        if self._stop_event.wait(timeout=restart_delay_s):
                            return
                        with self._lock:
                            st = self._jobs.get(job_name)
                            if not st:
                                return
                            if self._stop_event.is_set():
                                return
                            if st.process is not None:
                                return
                            try:
                                env = os.environ.copy()
                                env["ENGINE_SUPERVISED"] = "1"
                                env["ENGINE_LAUNCHED_BY_SUPERVISOR"] = "1"
                                env["ENGINE_JOB_NAME"] = str(job_name)
                                _apply_ingestion_shard_env(env)
                                try:
                                    spec = ALLOWED_JOBS.get(str(job_name))
                                    meta = dict(
                                        spec[3]
                                        if isinstance(spec, (tuple, list))
                                        and len(spec) >= 4
                                        and isinstance(spec[3], dict)
                                        else {}
                                    )
                                    env["ENGINE_PROCESS_ROLE"] = str(meta.get("resource_class") or "")
                                    from engine.runtime.thread_policy import apply_cpu_thread_policy_to_env

                                    apply_cpu_thread_policy_to_env(env, role=env.get("ENGINE_PROCESS_ROLE") or None)
                                except Exception as e:
                                    _warn_nonfatal(
                                        "supervisor_restart_thread_policy_failed",
                                        "SUPERVISOR_RESTART_THREAD_POLICY_FAILED",
                                        e,
                                        warn_key=f"supervisor_restart_thread_policy_failed:{job_name}",
                                        job=str(job_name),
                                    )

                                repo_root = str(_repo_root())
                                existing_pp = env.get("PYTHONPATH", "")
                                parts = [p for p in existing_pp.split(os.pathsep) if p]
                                if repo_root not in parts:
                                    parts.insert(0, repo_root)
                                env["PYTHONPATH"] = os.pathsep.join(parts)

                                script_path = str((_repo_root() / st.spec.script).resolve())
                                stdout_path, stderr_path = _job_log_paths(job_name)

                                rotate_log_if_needed(stdout_path)
                                rotate_log_if_needed(stderr_path)
                                stdout_fh = open(stdout_path, "ab")
                                stderr_fh = open(stderr_path, "ab")

                                try:
                                    st.process = subprocess.Popen(
                                        [sys.executable, "-u", script_path],
                                        cwd=repo_root,
                                        stdout=stdout_fh,
                                        stderr=stderr_fh,
                                        env=env,
                                        close_fds=(not sys.platform.startswith("win")),
                                        creationflags=(
                                            subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
                                        ),
                                    )
                                except Exception:
                                    stdout_fh.close()
                                    stderr_fh.close()
                                    raise
                                finally:
                                    try:
                                        stdout_fh.close()
                                    except Exception as e:
                                        _warn_nonfatal(
                                            "supervisor_restart_stdout_log_close_failed",
                                            "SUPERVISOR_RESTART_STDOUT_LOG_CLOSE_FAILED",
                                            e,
                                            warn_key=f"supervisor_restart_stdout_log_close_failed:{job_name}",
                                            job=str(job_name),
                                        )
                                    try:
                                        stderr_fh.close()
                                    except Exception as e:
                                        _warn_nonfatal(
                                            "supervisor_restart_stderr_log_close_failed",
                                            "SUPERVISOR_RESTART_STDERR_LOG_CLOSE_FAILED",
                                            e,
                                            warn_key=f"supervisor_restart_stderr_log_close_failed:{job_name}",
                                            job=str(job_name),
                                        )

                                st.last_start_ts = _now()
                                st.last_exit_code = None
                                st.failed_reason = None
                                LOG.warning(
                                    "SUPERVISOR_JOB_RESTARTED",
                                    extra={
                                        "job": str(job_name),
                                        "pid": int(st.process.pid),
                                        "delay_s": float(restart_delay_s),
                                        "stdout_log": str(stdout_path),
                                        "stderr_log": str(stderr_path),
                                    },
                                )
                            except Exception as e:
                                _warn_nonfatal(
                                    "supervisor_restart_failed",
                                    "SUPERVISOR_RESTART_FAILED",
                                    e,
                                    warn_key=f"supervisor_restart_failed:{job_name}",
                                    job=str(job_name),
                                )
                                return

                    threading.Thread(
                        target=_restart_later,
                        args=(name, delay_s),
                        daemon=True,
                    ).start()
