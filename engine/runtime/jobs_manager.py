"""
FILE: jobs_manager.py

Runtime subsystem module for `jobs_manager`.
"""

# jobs_manager.py
import json
import os
import sys
import time
import threading
import subprocess
from collections import deque
from typing import Any, Callable, Deque, Dict, Optional

from engine.runtime.job_registry import (
    ALLOWED_JOBS,
    JOB_ORDER,
    enforce_registered_job_path,
    is_market_data_job,
    is_offline_workload_job,
)

from engine.runtime.config import (
    AUTO_RESTART_DAEMONS,
    DAEMON_RESTART_BASE_DELAY_MS,
    DAEMON_RESTART_MAX_DELAY_MS,
    DAEMON_RESTART_WINDOW_S,
    DAEMON_RESTART_MAX_IN_WINDOW,
    DAEMON_WATCHDOG_PERIOD_S,
    PREFLIGHT_ENABLE,
    PREFLIGHT_BLOCK_JOBS,
    RESOURCE_SCHEDULER_BACKGROUND_MAX,
    RESOURCE_SCHEDULER_ENABLE,
    RESOURCE_SCHEDULER_EXECUTION_MAX,
    RESOURCE_SCHEDULER_GLOBAL_MAX,
    RESOURCE_SCHEDULER_INFERENCE_MAX,
    RESOURCE_SCHEDULER_REPLAY_MAX,
    RESOURCE_SCHEDULER_TRAINING_MAX,
)
from engine.runtime.platform import default_local_log_dir

_DAEMON_STALL_AFTER_MS = int(os.environ.get("DAEMON_STALL_AFTER_MS", "120000"))
_ALLOW_DAEMON_PERMANENT_FAILURE = str(
    os.environ.get("DAEMON_ALLOW_PERMANENT_FAILURE", "0")
).strip().lower() in ("1", "true", "yes", "on")

from engine.runtime.gates import execution_gate_snapshot
from engine.runtime.job_registry import validate_job_registry_paths
from engine.runtime.lifecycle_state import set_state, WARMING_UP, DEGRADED
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.log_retention import rotate_log_if_needed
from engine.runtime.logging import get_logger, log_event
from engine.runtime.runtime_meta import meta_get
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.tracing import trace_event
from engine.runtime.storage import (
    PG_LIVENESS_DB_ENABLED,
    _pid_is_running,
    connect_liveness_ro_direct as _connect_liveness_ro_direct,
    connect_ro_direct as _connect_ro_direct,
)

LOG = get_logger("runtime.jobs_manager")
log_event(
    LOG,
    20,
    "jobs_manager_loaded",
    component="engine.runtime.jobs_manager",
    extra={"path": __file__},
)


def _warn_nonfatal(code: str, error: Exception, *, persist: bool = True, **extra) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.jobs_manager",
        include_health=False,
        persist=bool(persist),
        extra=extra or None,
    )


def _append_job_log_safe(job: "JobState", message: str, *, code: str, **extra) -> None:
    try:
        job.append_log(str(message))
    except Exception as append_err:
        _warn_nonfatal(code, append_err, job=str(getattr(job, "name", "")), message=str(message), **extra)


def _lock_release_timeout_s() -> float:
    try:
        return max(0.01, min(5.0, float(os.environ.get("JOBS_MANAGER_LOCK_RELEASE_TIMEOUT_S", "0.25") or 0.25)))
    except Exception as e:
        _warn_nonfatal("JOBS_MANAGER_LOCK_RELEASE_TIMEOUT_PARSE_FAILED", e)
        return 0.25


def _release_lock_best_effort(
    lock_name: str,
    *,
    job: str,
    scope: str,
    deadline: Optional[float] = None,
) -> bool:
    """Release a runtime lock without letting lock-storage failures block shutdown."""
    remaining_s = _lock_release_timeout_s()
    if deadline is not None:
        remaining_s = min(remaining_s, max(0.0, float(deadline) - time.monotonic()))
    if remaining_s <= 0.0:
        _warn_nonfatal(
            "JOBS_MANAGER_RUNTIME_LOCK_RELEASE_SKIPPED",
            TimeoutError("lock release deadline exhausted"),
            persist=False,
            job=str(job),
            lock_name=str(lock_name),
            scope=str(scope),
        )
        return False

    done = threading.Event()
    result: Dict[str, Any] = {}

    def _runner() -> None:
        try:
            _release_lock(lock_name)
            result["ok"] = True
        except Exception as exc:
            result["error"] = exc
        finally:
            done.set()

    thread = threading.Thread(
        target=_runner,
        name=f"job_lock_release_{str(job)}",
        daemon=True,
    )
    try:
        thread.start()
    except Exception as exc:
        _warn_nonfatal(
            "JOBS_MANAGER_RUNTIME_LOCK_RELEASE_THREAD_FAILED",
            exc,
            persist=False,
            job=str(job),
            lock_name=str(lock_name),
            scope=str(scope),
        )
        return False

    if not done.wait(timeout=max(0.0, float(remaining_s))):
        _warn_nonfatal(
            "JOBS_MANAGER_RUNTIME_LOCK_RELEASE_TIMEOUT",
            TimeoutError(f"lock release timed out after {remaining_s:.3f}s"),
            persist=False,
            job=str(job),
            lock_name=str(lock_name),
            scope=str(scope),
            timeout_s=float(remaining_s),
        )
        return False

    error = result.get("error")
    if isinstance(error, Exception):
        _warn_nonfatal(
            "JOBS_MANAGER_RUNTIME_LOCK_RELEASE_FAILED",
            error,
            persist=False,
            job=str(job),
            lock_name=str(lock_name),
            scope=str(scope),
        )
        return False
    return True

# ---------------------------------------------------
# PATHS (robust against wrong CWD)
# jobs_manager.py lives at: engine/runtime/jobs_manager.py
# project root is 1 level up from engine/
# ---------------------------------------------------
_ENGINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_PROJECT_ROOT = os.path.abspath(os.path.join(_ENGINE_DIR, ".."))
_LOG_DIR = os.path.abspath(
    os.environ.get("TRADING_LOGS")
    or os.environ.get("LOG_DIR")
    or str(default_local_log_dir())
)
os.makedirs(_LOG_DIR, exist_ok=True)

def _job_launch_trace_append(payload: Dict) -> None:
    # Launch diagnostics are persisted asynchronously because start/stop paths
    # should not block on runtime_meta writes.
    snapshot = dict(payload or {})

    def _runner() -> None:
        try:
            from engine.runtime.runtime_meta import meta_get, meta_set

            raw = str(meta_get("job_launch_trace", "[]") or "[]").strip() or "[]"
            trace = json.loads(raw)
            if not isinstance(trace, list):
                trace = []
            trace.append(snapshot)
            trace = trace[-500:]
            meta_set("job_launch_trace", json.dumps(trace, separators=(",", ":"), sort_keys=True))
        except Exception as e:
            log_event(
                LOG,
                40,
                "job_launch_trace_append_failed",
                component="engine.runtime.jobs_manager",
                extra={"error": f"{type(e).__name__}: {e}", "payload": snapshot},
            )

    try:
        threading.Thread(
            target=_runner,
            name="job_launch_trace_append",
            daemon=True,
        ).start()
    except Exception as e:
        log_event(
            LOG,
            40,
            "job_launch_trace_thread_start_failed",
            component="engine.runtime.jobs_manager",
            extra={"error": f"{type(e).__name__}: {e}", "payload": snapshot},
        )


def _job_log_path(job_name: str) -> str:
    safe = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in str(job_name or ""))
    return os.path.join(_LOG_DIR, f"{safe}.combined.log")


def _script_module_name(script_path: str) -> str:
    try:
        rel = os.path.relpath(os.path.abspath(script_path), _PROJECT_ROOT)
    except Exception as e:
        _warn_nonfatal("JOBS_MANAGER_SCRIPT_MODULE_NAME_FAILED", e, script_path=str(script_path))
        return ""

    root, ext = os.path.splitext(rel)
    if str(ext or "").lower() != ".py":
        return ""

    parts = [part for part in root.split(os.sep) if part]
    if not parts:
        return ""
    if any(not str(part).isidentifier() for part in parts):
        return ""
    return ".".join(parts)

# ------------------------------
# SQLITE LOCKS + JOB HISTORY (single source of truth)
# ------------------------------
from engine.runtime.locks import (
    ensure_job_locks as _ensure_job_locks,
    acquire_lock as _acquire_lock,
    heartbeat_lock as _heartbeat_lock,
    read_lock as _read_lock,
    release_lock as _release_lock,
    ensure_job_history as _ensure_job_history,
    write_job_history as _write_job_history_impl,
    read_job_history as _read_job_history,
)


def _job_history_write_timeout_s() -> float:
    try:
        return max(0.05, float(os.environ.get("JOBS_MANAGER_HISTORY_WRITE_TIMEOUT_S", "0.25") or 0.25))
    except Exception as e:
        _warn_nonfatal("JOBS_MANAGER_HISTORY_WRITE_TIMEOUT_PARSE_FAILED", e)
        return 0.25


def _write_job_history(job_name: str, action: str, detail: str, exit_code) -> None:
    try:
        from engine.runtime.storage_pool import storage_acquire_timeout_override

        timeout_ctx = storage_acquire_timeout_override(_job_history_write_timeout_s())
    except Exception:
        from contextlib import nullcontext

        timeout_ctx = nullcontext()

    try:
        with timeout_ctx:
            _write_job_history_impl(job_name, action, detail, exit_code)
    except Exception as e:
        _warn_nonfatal(
            "JOBS_MANAGER_JOB_HISTORY_WRITE_FAILED",
            e,
            job=str(job_name),
            action=str(action),
        )


# ------------------------------
# PUBLIC READ HELPERS (API)
# ------------------------------

def get_job_log(job_name: str, tail: int = 200) -> str:
    jm = _GLOBAL_JOB_MANAGER.get()
    if not jm:
        return ""
    job = jm.get(job_name)
    if not job:
        return ""
    return job.tail(int(tail or 0))


def get_job_history(job_name: str, limit: int = 200) -> list:
    return _read_job_history(job_name, limit=limit)


# ------------------------------
# JOB STATE / MANAGER
# ------------------------------

class JobState:
    def __init__(self, name: str, script: str, mode: str, group: Optional[str] = None):
        self.name = name
        self.script = script
        self.mode = mode
        self.group = group
        self.meta: Dict[str, object] = {}
        self.proc: Optional[subprocess.Popen] = None
        self.started_at_ms: Optional[int] = None
        self.exited_at_ms: Optional[int] = None
        self.exit_code: Optional[int] = None
        self.log: Deque[str] = deque(maxlen=4000)
        # start()/stop() may append logs while already holding the job lock,
        # so this must be re-entrant to avoid self-deadlocking.
        self._lock = threading.RLock()

        self.stop_requested: bool = False
        self.restart_attempts_window: Deque[int] = deque(maxlen=50)
        self.next_restart_ms: int = 0
        self._restart_in_flight: bool = False
        self.last_start_args: Optional[list] = None
        self.last_start_cwd: Optional[str] = None
        self.failed_reason: Optional[str] = None
        self.restart_window_exhaustions: int = 0

        # DEBUG TRACKING
        self.last_error: Optional[str] = None
        self.last_success_ts: Optional[int] = None

        # For oneshot jobs, we hold a cross-process lock "job:<name>"
        self._oneshot_lock_name: Optional[str] = None

    def to_dict(self, *, include_persisted: bool = True) -> Dict:
        with self._lock:
            # This is the canonical in-memory job snapshot exposed to APIs and
            # dashboards, so it merges process state with persisted heartbeat
            # state from the runtime lock table and heartbeat rows for
            # externally supervised daemons.
            running = self.proc is not None and self.proc.poll() is None
            pid = None
            try:
                pid = int(self.proc.pid) if self.proc is not None else None
            except Exception:
                pid = None

            hb = {}
            hb_ts_ms = 0
            hb_age_s = None
            stale = False

            if include_persisted:
                try:
                    lock_heartbeat = _read_runtime_lock(self.name) or {}
                    persisted_heartbeat = _read_job_heartbeat(self.name) or {}
                    hb = dict(lock_heartbeat)
                    if persisted_heartbeat:
                        # Persisted job_heartbeats are the daemon liveness source of
                        # truth. A fresher lock row can exist even when the last
                        # supervised heartbeat is stale, so do not let lock
                        # freshness mask a stale daemon.
                        hb["owner"] = persisted_heartbeat.get("owner") or hb.get("owner")
                        hb["pid"] = persisted_heartbeat.get("pid") or hb.get("pid")
                        hb["heartbeat_ts_ms"] = int(persisted_heartbeat.get("heartbeat_ts_ms") or 0)
                        if persisted_heartbeat.get("_heartbeat_source"):
                            hb["_heartbeat_source"] = persisted_heartbeat.get("_heartbeat_source")
                    hb_ts_ms = int(hb.get("heartbeat_ts_ms") or 0)
                    if hb_ts_ms > 0:
                        hb_age_s = round((int(time.time() * 1000) - hb_ts_ms) / 1000.0, 1)
                        stale = bool(hb_age_s > (float(_DAEMON_STALL_AFTER_MS) / 1000.0))
                except Exception:
                    hb = {}
                    hb_ts_ms = 0
                    hb_age_s = None
                    stale = False

            if (not running) and pid is None:
                persisted_pid = int(hb.get("pid") or 0) if hb.get("pid") is not None else 0
                if persisted_pid > 0 and hb_ts_ms > 0 and (not stale) and _pid_is_running(persisted_pid):
                    running = True
                    pid = persisted_pid

            status = "STOPPED"
            if running:
                status = "RUNNING"
            elif self.failed_reason:
                status = "FAILED"
            elif self._restart_in_flight or int(self.next_restart_ms or 0) > int(time.time() * 1000):
                status = "STARTING"
            elif self.exit_code is not None:
                status = "FAILED" if int(self.exit_code) != 0 else "STOPPED"

            return {
                "name": self.name,
                "script": self.script,
                "mode": self.mode,
                "group": self.group,
                "category": self.group,
                "status": status,
                "running": bool(running),
                "pid": pid,
                "started_at_ms": self.started_at_ms,
                "exited_at_ms": self.exited_at_ms,
                "exit_code": self.exit_code,
                "failed_reason": self.failed_reason,
                "last_error": self.last_error,
                "last_success_ts": self.last_success_ts,
                "log_path": _job_log_path(self.name),
                "log_lines": len(self.log),
                "stop_requested": bool(self.stop_requested),
                "next_restart_ms": int(self.next_restart_ms or 0),
                "restart_count": int(len(self.restart_attempts_window)),
                "restart_window_exhaustions": int(self.restart_window_exhaustions or 0),
                "heartbeat_ts_ms": int(hb_ts_ms or 0),
                "heartbeat_age_s": hb_age_s,
                "stale": bool(stale),
                "heartbeat_missing": bool(hb_ts_ms == 0),
                "lock_owner": hb.get("owner"),
                "heartbeat_source": str(hb.get("_heartbeat_source") or "job_locks"),
            }

    def append_log(self, line: str) -> None:
        # Keep a small in-memory tail for API reads while also writing the full
        # stream to the per-job combined log file on disk.
        text = str(line).rstrip("\n")
        with self._lock:
            self.log.append(text)
        try:
            log_path = _job_log_path(self.name)
            rotate_log_if_needed(log_path)
            with open(log_path, "a", encoding="utf-8", errors="replace") as fh:
                fh.write(text + "\n")
        except Exception as e:
            log_event(
                LOG,
                40,
                "job_log_write_failed",
                component="engine.runtime.jobs_manager",
                extra={"job": str(self.name), "error": f"{type(e).__name__}: {e}"},
            )

    def tail(self, n: int) -> str:
        with self._lock:
            if n <= 0:
                return ""
            return "\n".join(list(self.log)[-n:])


# ------------------------------
# GLOBAL JOB MANAGER HANDLE
# ------------------------------

class _GlobalJobManager:
    def __init__(self):
        self._jm = None

    def set(self, jm):
        self._jm = jm

    def get(self):
        return self._jm


_GLOBAL_JOB_MANAGER = _GlobalJobManager()

def get_all_job_states():
    jm = _GLOBAL_JOB_MANAGER.get()
    if not jm:
        return {}
    try:
        return {j["name"]: j for j in jm.list_jobs()}
    except Exception as e:
        _warn_nonfatal("JOBS_MANAGER_GET_ALL_JOB_STATES_FAILED", e)
        return {}


def _runtime_lock_candidates(job_name: str) -> list[str]:
    name = str(job_name or "").strip()
    if not name:
        return []
    return [name, f"job:{name}"]


def _read_runtime_lock(job_name: str) -> Dict:
    for lock_name in _runtime_lock_candidates(job_name):
        try:
            row = _read_lock(lock_name) or {}
        except Exception:
            row = {}
        if row:
            out = dict(row)
            out["_lock_name"] = str(lock_name)
            return out
    return {}


def _read_job_heartbeat(job_name: str) -> Dict:
    candidates = _runtime_lock_candidates(job_name)
    if not candidates:
        return {}

    con = None
    try:
        con = _connect_liveness_ro_direct() if bool(PG_LIVENESS_DB_ENABLED) else _connect_ro_direct()
        placeholders = ",".join("?" for _ in candidates)
        params = tuple(candidates) + (str(job_name),)
        row = con.execute(
            f"""
            SELECT job_name, owner, pid, ts_ms
            FROM job_heartbeats
            WHERE job_name IN ({placeholders})
            ORDER BY CASE WHEN job_name = ? THEN 0 ELSE 1 END, ts_ms DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
        if not row:
            return {}
        return {
            "job_name": str(row[0] or ""),
            "owner": str(row[1] or ""),
            "pid": (int(row[2]) if row[2] is not None else None),
            "heartbeat_ts_ms": (int(row[3]) if row[3] is not None else 0),
            "_heartbeat_source": "job_heartbeats",
        }
    except Exception as e:
        _warn_nonfatal("JOBS_MANAGER_READ_JOB_HEARTBEAT_FAILED", e, job=str(job_name))
        return {}
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as close_err:
            _warn_nonfatal(
                "JOBS_MANAGER_READ_JOB_HEARTBEAT_CLOSE_FAILED",
                close_err,
                job=str(job_name),
            )


class JobManager:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(
        self,
        preflight_fn=None,
        get_kill_switches_fn=None,
        get_execution_mode_fn=None,
    ):
        self._jobs: Dict[str, JobState] = {}

        for name, value in ALLOWED_JOBS.items():
            # Supported formats:
            # (script, mode)
            # (script, mode, group)
            # (script, mode, group, meta)

            script = None
            mode = None
            group = None
            meta = {}

            if isinstance(value, (list, tuple)):
                if len(value) == 2:
                    script, mode = value
                elif len(value) == 3:
                    script, mode, group = value
                elif len(value) >= 4:
                    script, mode, group, meta = value[0], value[1], value[2], value[3]
                else:
                    continue
            else:
                continue

            js = JobState(name, script, mode, group)
            js.meta = dict(meta or {})
            self._jobs[name] = js

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._watchdog_thread: Optional[threading.Thread] = None
        self._preflight_fn = preflight_fn

        # Execution gating providers (injected by runtime / dashboard)
        # If not provided, execution jobs fail-closed by default (safer).
        self._get_kill_switches_fn = get_kill_switches_fn
        self._get_execution_mode_fn = get_execution_mode_fn

        _GLOBAL_JOB_MANAGER.set(self)

        registry_check = validate_job_registry_paths(_PROJECT_ROOT, import_check=False)
        if not registry_check.get("ok"):
            raise RuntimeError(
                "invalid_job_registry: " + "; ".join(registry_check.get("errors") or [])
            )

        # Ensure DB coordination tables exist early (fail closed)
        try:
            _ensure_job_locks()
        except Exception as e:
            log_event(
                LOG,
                40,
                "ensure_job_locks_failed",
                component="engine.runtime.jobs_manager",
                extra={"error": f"{type(e).__name__}: {e}"},
            )
            raise
        try:
            _ensure_job_history()
        except Exception as e:
            log_event(
                LOG,
                40,
                "ensure_job_history_failed",
                component="engine.runtime.jobs_manager",
                extra={"error": f"{type(e).__name__}: {e}"},
            )
            raise

        self._publish_resource_scheduler_state()

        # -------------------------------------------------
        # WATCHDOG BOOTSTRAP (inline, no dynamic binding)
        # -------------------------------------------------
        if not getattr(self, "_watchdog_started", False):
            self._watchdog_started = True
            self._watchdog_thread = threading.Thread(
                target=self._daemon_watchdog_loop,
                daemon=True,
                name="jobs_manager_watchdog",
            )
            self._watchdog_thread.start()

        # NOTE:
        # Do NOT auto-start daemons here.
        # Deterministic boot is handled by RuntimeSupervisor
        # inside dashboard_server.run_server().

    def start_initial_daemons(self):
        """
        Start all daemon jobs once at boot.
        NOTE: deterministic boot should be handled by RuntimeSupervisor.
        This is kept only for backwards compatibility.
        """

        order = ["ingestion_runtime"] + [
            j for j in (JOB_ORDER or []) if j != "ingestion_runtime"
        ]

        # ensure the supervised market-data runtime owns provider startup
        if "ingestion_runtime" in ALLOWED_JOBS:
            try:
                self.start("ingestion_runtime")
            except Exception as e:
                log_event(
                    LOG,
                    40,
                    "start_initial_daemons_ingestion_failed",
                    component="engine.runtime.jobs_manager",
                    extra={"job": "ingestion_runtime", "error": f"{type(e).__name__}: {e}"},
                )
                raise

        if not order:
            # fallback: stable deterministic order
            order = sorted(list(self._jobs.keys()))

        for name in order:
            job = self.get(name)
            if not job:
                continue
            if job.mode != "daemon":
                continue

            # Never stall-restart ingestion_runtime (it supervises feeds)
            if job.name == "ingestion_runtime":
                continue
            try:
                self.start(name)
            except Exception as e:
                log_event(
                    LOG,
                    40,
                    "start_initial_daemons_job_failed",
                    component="engine.runtime.jobs_manager",
                    extra={"job": str(name), "error": f"{type(e).__name__}: {e}"},
                )
                raise

    def list_jobs(self, *, timeout_s: float | None = None, include_persisted: bool = True):
        acquired = False
        if timeout_s is None:
            self._lock.acquire()
            acquired = True
        else:
            acquired = self._lock.acquire(timeout=max(0.0, float(timeout_s)))
            if not acquired:
                raise TimeoutError("jobs_manager_lock_timeout")

        try:
            ordered_jobs = []
            seen = set()
            for name in JOB_ORDER:
                if name in self._jobs:
                    ordered_jobs.append(self._jobs[name])
                    seen.add(name)

            for name in sorted(self._jobs):
                if name in seen:
                    continue
                ordered_jobs.append(self._jobs[name])
        finally:
            if acquired:
                self._lock.release()

        return [
            self._serialize_job_state(job, include_persisted=bool(include_persisted))
            for job in ordered_jobs
        ]

    def get(self, name: str) -> Optional[JobState]:
        with self._lock:
            return self._jobs.get(name)

    def _resource_profile(self, job: JobState) -> Dict:
        meta = dict(getattr(job, "meta", {}) or {})
        resource_class = str(meta.get("resource_class") or "").strip().lower()
        stage = str(meta.get("pipeline_stage") or "").strip().lower()
        name = str(getattr(job, "name", "") or "").strip().lower()

        if not resource_class:
            if meta.get("execution") is True:
                resource_class = "execution"
            elif name.startswith("process_events") or stage == "process":
                resource_class = "inference"
            elif "replay" in name:
                resource_class = "replay"
            elif stage in {"embed_train", "model_train", "temporal_train"} or name.startswith("train_"):
                resource_class = "training"
            else:
                resource_class = "background"

        default_priority = {
            "execution": 100,
            "inference": 90,
            "training": 40,
            "replay": 30,
            "background": 10,
        }
        default_slots = {
            "execution": 1,
            "inference": 1,
            "training": 1,
            "replay": 1,
            "background": 0,
        }

        try:
            resource_priority = int(meta.get("resource_priority") or default_priority.get(resource_class, 10))
        except Exception:
            resource_priority = int(default_priority.get(resource_class, 10))

        try:
            slot_cost = int(meta.get("slot_cost") or default_slots.get(resource_class, 0))
        except Exception:
            slot_cost = int(default_slots.get(resource_class, 0))

        return {
            "resource_class": str(resource_class),
            "resource_priority": int(resource_priority),
            "slot_cost": max(0, int(slot_cost)),
        }

    def _serialize_job_state(self, job: JobState, *, include_persisted: bool = True) -> Dict:
        out = dict(job.to_dict(include_persisted=bool(include_persisted)) or {})
        out.update(self._resource_profile(job))
        out["resource_managed"] = bool(int(out.get("slot_cost") or 0) > 0)
        return out

    def _resource_class_limit(self, resource_class: str) -> int:
        key = str(resource_class or "").strip().lower()
        limits = {
            "execution": int(RESOURCE_SCHEDULER_EXECUTION_MAX),
            "inference": int(RESOURCE_SCHEDULER_INFERENCE_MAX),
            "training": int(RESOURCE_SCHEDULER_TRAINING_MAX),
            "replay": int(RESOURCE_SCHEDULER_REPLAY_MAX),
            "background": int(RESOURCE_SCHEDULER_BACKGROUND_MAX),
        }
        return int(limits.get(key, 0))

    def _resource_scheduler_state(self) -> Dict:
        with self._lock:
            jobs = list(self._jobs.values())

        class_slots: Dict[str, int] = {}
        running_jobs = []
        total_slots = 0

        for job in jobs:
            profile = self._resource_profile(job)
            slot_cost = int(profile.get("slot_cost") or 0)
            if slot_cost <= 0 or not self.is_running(job.name):
                continue

            resource_class = str(profile.get("resource_class") or "background")
            class_slots[resource_class] = int(class_slots.get(resource_class, 0) + slot_cost)
            total_slots += slot_cost
            running_jobs.append(
                {
                    "job": str(job.name),
                    "mode": str(job.mode),
                    "resource_class": resource_class,
                    "resource_priority": int(profile.get("resource_priority") or 0),
                    "slot_cost": int(slot_cost),
                }
            )

        running_jobs.sort(
            key=lambda row: (
                -int(row.get("resource_priority") or 0),
                str(row.get("resource_class") or ""),
                str(row.get("job") or ""),
            )
        )

        return {
            "enabled": bool(RESOURCE_SCHEDULER_ENABLE),
            "global_limit": int(RESOURCE_SCHEDULER_GLOBAL_MAX),
            "class_limits": {
                "execution": int(RESOURCE_SCHEDULER_EXECUTION_MAX),
                "inference": int(RESOURCE_SCHEDULER_INFERENCE_MAX),
                "training": int(RESOURCE_SCHEDULER_TRAINING_MAX),
                "replay": int(RESOURCE_SCHEDULER_REPLAY_MAX),
                "background": int(RESOURCE_SCHEDULER_BACKGROUND_MAX),
            },
            "global_slots_used": int(total_slots),
            "class_slots_used": class_slots,
            "running_jobs": running_jobs,
            "ts_ms": int(time.time() * 1000),
        }

    def _publish_resource_scheduler_state(self) -> None:
        try:
            from engine.runtime.runtime_meta import meta_set

            meta_set(
                "resource_scheduler_state",
                json.dumps(self._resource_scheduler_state(), separators=(",", ":"), sort_keys=True),
            )
        except Exception as e:
            log_event(
                LOG,
                40,
                "resource_scheduler_state_publish_failed",
                component="engine.runtime.jobs_manager",
                extra={"error": f"{type(e).__name__}: {e}"},
            )

    def _resource_admission(self, job: JobState) -> Dict:
        profile = self._resource_profile(job)
        slot_cost = int(profile.get("slot_cost") or 0)
        if not RESOURCE_SCHEDULER_ENABLE or slot_cost <= 0:
            return {"ok": True, "profile": profile, "scheduler_state": self._resource_scheduler_state()}

        scheduler_state = self._resource_scheduler_state()
        resource_class = str(profile.get("resource_class") or "background")
        class_used = int((scheduler_state.get("class_slots_used") or {}).get(resource_class, 0))
        total_used = int(scheduler_state.get("global_slots_used") or 0)
        class_limit = int(self._resource_class_limit(resource_class))
        global_limit = int(RESOURCE_SCHEDULER_GLOBAL_MAX)

        reasons = []
        if global_limit > 0 and (total_used + slot_cost) > global_limit:
            reasons.append(
                f"global_slots_exceeded used={total_used} request={slot_cost} limit={global_limit}"
            )
        if class_limit > 0 and (class_used + slot_cost) > class_limit:
            reasons.append(
                f"class_slots_exceeded class={resource_class} used={class_used} request={slot_cost} limit={class_limit}"
            )

        blockers = list(scheduler_state.get("running_jobs") or [])
        if reasons:
            blockers = [
                row
                for row in blockers
                if str(row.get("resource_class") or "") == resource_class
                or (global_limit > 0 and (total_used + slot_cost) > global_limit)
            ]

        return {
            "ok": len(reasons) == 0,
            "profile": profile,
            "scheduler_state": scheduler_state,
            "reasons": reasons,
            "blockers": blockers,
        }

    # -------------------------------------------------
    # Compatibility API for dashboard job log/history
    # ctx["JOBS"] is a JobManager in api_handlers
    # -------------------------------------------------
    def get_job_log(self, name: str, tail: int = 200) -> Dict:
        job = self.get(name)
        if not job:
            return {"ok": False, "error": "job_not_found", "job": str(name)}

        try:
            text = job.tail(int(tail or 0))
            lines = text.split("\n") if text else []
            return {"ok": True, "job": str(name), "lines": lines}
        except Exception as e:
            _warn_nonfatal("JOBS_MANAGER_GET_JOB_LOG_FAILED", e, job=str(name), tail=int(tail or 0))
            return {"ok": False, "error": "job_log_exception", "detail": str(e), "job": str(name)}

    def get_job_history(self, name: str, limit: int = 200) -> Dict:
        try:
            rows = _read_job_history(str(name or ""), limit=int(limit or 0))
            return {"ok": True, "job": str(name), "rows": rows}
        except Exception as e:
            _warn_nonfatal("JOBS_MANAGER_GET_JOB_HISTORY_FAILED", e, job=str(name), limit=int(limit or 0))
            return {"ok": False, "error": "job_history_exception", "detail": str(e), "job": str(name)}

    def is_running(self, name: str) -> bool:
        j = self.get(name)
        if not j:
            return False
        p = j.proc
        if not p:
            return False
        try:
            return p.poll() is None
        except Exception as e:
            _warn_nonfatal("JOBS_MANAGER_IS_RUNNING_CHECK_FAILED", e, job=str(name))
            return False

    def start(self, name: str) -> Dict:
        # start() is the main ownership boundary for job launches: it enforces
        # registry rules, execution gates, daemon exclusivity, and oneshot locks.
        job = self.get(name)
        if not job:
            _job_launch_trace_append({
                "job": str(name),
                "attempted": True,
                "spawned": False,
                "failed": True,
                "entry_valid": False,
                "error": f"unknown job: {name}",
                "ts_ms": int(time.time() * 1000),
            })
            return {"ok": False, "error": f"unknown job: {name}"}

        if (
            PREFLIGHT_ENABLE
            and PREFLIGHT_BLOCK_JOBS
            and self._preflight_fn
            and getattr(job, "meta", {}).get("execution") is True
        ):
            # Optional preflight blocking is only applied to execution jobs,
            # because launching those in an unhealthy state is especially costly.
            p = self._preflight_fn()
            if not p.get("ok"):
                return {"ok": False, "error": "preflight_failed", "notes": p.get("notes", [])}

        # --------------------------------------------------
        # HARD EXECUTION GATE (cannot be bypassed anywhere)
        # --------------------------------------------------
        if getattr(job, "meta", {}).get("execution") is True:
            fail_open = os.environ.get("EXECUTION_GATE_FAIL_OPEN_IF_NO_PROVIDERS", "0") == "1"

            if not self._get_kill_switches_fn or not self._get_execution_mode_fn:
                if not fail_open:
                    return {
                        "ok": False,
                        "error": "execution_blocked_gate_providers_missing",
                        "job": str(name),
                    }
            else:
                gate = execution_gate_snapshot(
                    system_state=self._get_execution_mode_fn() if self._get_execution_mode_fn else None,
                    kill_switches=self._get_kill_switches_fn() if self._get_kill_switches_fn else None,
                    execution_degraded=False,
                )

                if (not gate.get("ok")) or (not gate.get("allowed")):
                    return {
                        "ok": False,
                        "error": "execution_blocked",
                        "job": str(name),
                        "gate": gate,
                    }

        if is_offline_workload_job(job.name):
            blocked_workload_result = None
            try:
                from engine.runtime.workload_profiles import assert_offline_work_allowed

                profile_ack = assert_offline_work_allowed(job_name=str(job.name))
            except RuntimeError as e:
                detail = str(e)
                _write_job_history(job.name, "start_blocked_workload_profile", detail, None)
                _job_launch_trace_append({
                    "job": str(job.name),
                    "attempted": True,
                    "spawned": False,
                    "failed": True,
                    "entry_valid": True,
                    "workload_profile_denied": True,
                    "error": detail,
                    "ts_ms": int(time.time() * 1000),
                })
                log_event(
                    LOG,
                    30,
                    "job_start_blocked_workload_profile",
                    component="engine.runtime.jobs_manager",
                    extra={"job": str(job.name), "error": detail},
                )
                blocked_workload_result = {
                    "ok": False,
                    "error": "offline_training_live_profile_ack_required",
                    "job": str(job.name),
                    "detail": detail,
                }
            except Exception as e:
                detail = f"{type(e).__name__}: {e}"
                _write_job_history(job.name, "start_blocked_workload_profile_error", detail, None)
                blocked_workload_result = {
                    "ok": False,
                    "error": "workload_profile_guard_failed",
                    "job": str(job.name),
                    "detail": detail,
                }
            else:
                if bool(profile_ack.get("required")):
                    log_event(
                        LOG,
                        30,
                        "job_start_offline_work_acknowledged_in_live_profile",
                        component="engine.runtime.jobs_manager",
                        extra={
                            "job": str(job.name),
                            "audit": dict(profile_ack.get("audit") or {}),
                            "enabled_settings": list(profile_ack.get("enabled_settings") or []),
                        },
                    )
            if blocked_workload_result is not None:
                return blocked_workload_result

        admission = self._resource_admission(job)
        if not admission.get("ok"):
            detail = "; ".join(str(x) for x in (admission.get("reasons") or []))
            _write_job_history(
                job.name,
                "start_blocked_resource_scheduler",
                detail,
                None,
            )
            _job_launch_trace_append({
                "job": str(job.name),
                "attempted": True,
                "spawned": False,
                "failed": True,
                "entry_valid": True,
                "resource_scheduler_denied": True,
                "resource_profile": dict(admission.get("profile") or {}),
                "resource_reasons": list(admission.get("reasons") or []),
                "resource_blockers": list(admission.get("blockers") or []),
                "ts_ms": int(time.time() * 1000),
            })
            log_event(
                LOG,
                30,
                "job_start_blocked_resource_scheduler",
                component="engine.runtime.jobs_manager",
                extra={
                    "job": str(job.name),
                    "reasons": list(admission.get("reasons") or []),
                    "profile": dict(admission.get("profile") or {}),
                },
            )
            self._publish_resource_scheduler_state()
            return {
                "ok": False,
                "error": "resource_scheduler_denied",
                "job": str(job.name),
                "scheduler": admission,
            }

        with job._lock:
            job.stop_requested = False

            if job.proc and job.proc.poll() is None:
                return {"ok": True, "status": "already_running"}

            if job.mode == "daemon":
                # Only enforce exclusivity within the same daemon group (e.g. price_feed).
                # If job.group is None, do not enforce exclusivity.
                if getattr(job, "group", None):
                    for j in self._jobs.values():
                        try:
                            if (
                                j is not job
                                and j.mode == "daemon"
                                and getattr(j, "group", None) == getattr(job, "group", None)
                                and j.proc
                                and j.proc.poll() is None
                            ):
                                job.append_log(f"[server] stopping conflicting daemon: {j.name}")

                                j.stop_requested = True
                                j.next_restart_ms = 0
                                j._restart_in_flight = False

                                try:
                                    j.proc.terminate()
                                    try:
                                        j.proc.wait(timeout=3)
                                    except Exception as wait_err:
                                        _warn_nonfatal("JOBS_MANAGER_CONFLICTING_DAEMON_WAIT_FAILED", wait_err, job=str(j.name), replacement_job=str(job.name))
                                        try:
                                            j.proc.kill()
                                            try:
                                                j.proc.wait(timeout=5)
                                            except Exception as kill_wait_err:
                                                _warn_nonfatal("JOBS_MANAGER_CONFLICTING_DAEMON_KILL_WAIT_FAILED", kill_wait_err, job=str(j.name), replacement_job=str(job.name))
                                        except Exception as kill_err:
                                            _warn_nonfatal("JOBS_MANAGER_CONFLICTING_DAEMON_KILL_FAILED", kill_err, job=str(j.name), replacement_job=str(job.name))
                                except Exception as term_err:
                                    _warn_nonfatal("JOBS_MANAGER_CONFLICTING_DAEMON_TERMINATE_FAILED", term_err, job=str(j.name), replacement_job=str(job.name))

                                j.exit_code = j.proc.poll()
                                j.exited_at_ms = int(time.time() * 1000)

                                _write_job_history(
                                    j.name,
                                    "group_replaced",
                                    f"replaced by {job.name}",
                                    j.exit_code,
                                )
                        except Exception as e:
                            _warn_nonfatal("JOBS_MANAGER_GROUP_REPLACEMENT_FAILED", e, job=str(job.name), conflicting_job=str(getattr(j, "name", "")))

            if job.mode == "oneshot":
                # Oneshots rely on a cross-process lock so concurrent control
                # paths collapse to one active run.
                lock_name = f"job:{job.name}"
                if not _acquire_lock(lock_name, ttl_ms=10 * 60 * 1000):
                    return {"ok": False, "error": f"job locked: {job.name}"}
                job._oneshot_lock_name = lock_name

            job.stop_requested = False
            job._restart_in_flight = False
            job.next_restart_ms = 0
            job.exited_at_ms = None
            job.exit_code = None
            job.failed_reason = None

            py = sys.executable

            # Resolve scripts from the project root (robust even if CWD is wrong)
            script_rel = str(job.script or "")
            script_path = os.path.abspath(os.path.join(_PROJECT_ROOT, script_rel))

            # fallback path resolution
            if not os.path.exists(script_path):
                alt = os.path.abspath(os.path.join(_ENGINE_DIR, script_rel))
                if os.path.exists(alt):
                    script_path = alt

            try:
                script_rel = enforce_registered_job_path(script_path, repo_root=_PROJECT_ROOT)
            except PermissionError as e:
                if job.mode == "oneshot":
                    lock_name = getattr(job, "_oneshot_lock_name", None) or f"job:{job.name}"
                    _release_lock(lock_name)
                    job._oneshot_lock_name = None
                job.append_log(f"[server] {e}")
                _write_job_history(job.name, "start_blocked_unregistered_job", str(e), None)
                _job_launch_trace_append({
                    "job": str(job.name),
                    "attempted": True,
                    "spawned": False,
                    "failed": True,
                    "entry_valid": False,
                    "error": str(e),
                    "ts_ms": int(time.time() * 1000),
                })
                raise

            module_name = _script_module_name(script_path)
            if module_name:
                args = [py, "-u", "-m", module_name]
            else:
                args = [py, "-u", script_path]

            if not os.path.exists(script_path):
                if job.mode == "oneshot":
                    _release_lock(f"job:{job.name}")
                job.append_log(
                    f"[server] script not found: {script_path} "
                    f"(from {script_rel}) project_root={_PROJECT_ROOT}"
                )
                _write_job_history(
                    job.name,
                    "start_failed",
                    f"script not found: {script_path} (from {script_rel})",
                    None,
                )
                _job_launch_trace_append({
                    "job": str(job.name),
                    "attempted": True,
                    "spawned": False,
                    "failed": True,
                    "entry_valid": False,
                    "entry": str(script_rel),
                    "script_path": str(script_path),
                    "error": f"script not found: {script_path}",
                    "ts_ms": int(time.time() * 1000),
                })
                return {"ok": False, "error": f"script not found: {script_path}"}

            job.append_log(f"[server] starting: {args}")
            _write_job_history(job.name, "start", f"{args}", None)
            _job_launch_trace_append({
                "job": str(job.name),
                "attempted": True,
                "spawned": False,
                "failed": False,
                "entry_valid": True,
                "entry": str(script_rel),
                "script_path": str(script_path),
                "cwd": str(_PROJECT_ROOT),
                "args": list(args),
                "ts_ms": int(time.time() * 1000),
            })

            job.started_at_ms = int(time.time() * 1000)
            job.last_start_args = list(args)
            job.last_start_cwd = str(_PROJECT_ROOT)

            env = dict(os.environ)
            env["ENGINE_LAUNCHED_BY_SUPERVISOR"] = "1"
            env["ENGINE_SUPERVISED"] = "1"
            env["ENGINE_JOB_NAME"] = str(job.name)
            try:
                profile = self._resource_profile(job)
                env["ENGINE_PROCESS_ROLE"] = str(profile.get("resource_class") or "background")
                from engine.runtime.thread_policy import apply_cpu_thread_policy_to_env

                apply_cpu_thread_policy_to_env(env, role=env["ENGINE_PROCESS_ROLE"])
            except Exception as env_err:
                _warn_nonfatal("JOBS_MANAGER_THREAD_POLICY_FAILED", env_err, job=str(job.name))

            # ensure engine imports work in subprocess
            existing_pp = env.get("PYTHONPATH", "")
            parts = [p for p in existing_pp.split(os.pathsep) if p]

            if _PROJECT_ROOT not in parts:
                parts.insert(0, _PROJECT_ROOT)

            if _ENGINE_DIR not in parts:
                insert_at = 1 if parts and parts[0] == _PROJECT_ROOT else 0
                parts.insert(insert_at, _ENGINE_DIR)

            env["PYTHONPATH"] = os.pathsep.join(parts)
            try:
                from services.data_source_manager import (
                    apply_safe_no_credential_runtime_environment,
                    safe_no_credential_market_data_mode,
                )

                if safe_no_credential_market_data_mode():
                    apply_safe_no_credential_runtime_environment(env)
            except Exception as env_err:
                _warn_nonfatal("JOBS_MANAGER_SAFE_ENV_SANITIZE_FAILED", env_err, job=str(job.name))

            try:
                job.proc = subprocess.Popen(
                    args,
                    cwd=str(_PROJECT_ROOT),
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                    creationflags=(subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0),
                )
            except Exception as e:
                if job.mode == "oneshot":
                    try:
                        _release_lock(f"job:{job.name}")
                    except Exception as release_err:
                        _warn_nonfatal("JOBS_MANAGER_ONESHOT_RELEASE_FAILED", release_err, job=str(job.name), scope="spawn_failed_cleanup")
                job.append_log(f"[server] spawn failed: {e}")
                _write_job_history(job.name, "start_failed", f"spawn failed: {e}", None)

                try:
                    if is_market_data_job(job.name):
                        set_state(DEGRADED, f"{job.name}_spawn_failed:{type(e).__name__}:{e}")
                except Exception as state_err:
                    _warn_nonfatal("JOBS_MANAGER_SET_STATE_FAILED", state_err, job=str(job.name), scope="spawn_failed")

                _job_launch_trace_append({
                    "job": str(job.name),
                    "attempted": True,
                    "spawned": False,
                    "failed": True,
                    "entry_valid": True,
                    "entry": str(script_rel),
                    "script_path": str(script_path),
                    "cwd": str(_PROJECT_ROOT),
                    "args": list(args),
                    "error": f"spawn failed: {e}",
                    "ts_ms": int(time.time() * 1000),
                })
                return {"ok": False, "error": f"spawn failed: {e}"}

            # best-effort heartbeat stamp (locks.py is single source of truth)
            try:
                _heartbeat_lock(f"job:{job.name}")
            except Exception as hb_err:
                _warn_nonfatal("JOBS_MANAGER_HEARTBEAT_LOCK_FAILED", hb_err, job=str(job.name), scope="post_spawn")

            if job.mode == "oneshot" and job._oneshot_lock_name:
                threading.Thread(
                    target=self._oneshot_lock_heartbeat_loop,
                    args=(job,),
                    daemon=True,
                ).start()

            threading.Thread(target=self._pump_output, args=(job,), daemon=True).start()

            start_grace_ms = int(os.environ.get("JOB_START_GRACE_MS", "250"))
            if start_grace_ms > 0:
                time.sleep(max(0.0, float(start_grace_ms) / 1000.0))

            early_rc = None
            pid_alive = False
            try:
                if job.proc is not None:
                    early_rc = job.proc.poll()
                    pid_alive = bool(job.proc.pid and _pid_is_running(int(job.proc.pid)))
            except Exception:
                early_rc = None
                pid_alive = False

            if early_rc is not None and job.mode == "oneshot" and int(early_rc) == 0:
                job.exited_at_ms = int(time.time() * 1000)
                job.exit_code = 0
                job.last_success_ts = job.exited_at_ms
                job.append_log("[server] completed rc=0 during start grace")
                _write_job_history(job.name, "exit", "process completed during start grace", job.exit_code)

                if job._oneshot_lock_name:
                    try:
                        _release_lock(job._oneshot_lock_name)
                    except Exception as release_err:
                        _warn_nonfatal("JOBS_MANAGER_ONESHOT_RELEASE_FAILED", release_err, job=str(job.name), scope="completed_during_start_grace")
                    job._oneshot_lock_name = None

                emit_counter(
                    "job_start",
                    1,
                    component="engine.runtime.jobs_manager",
                    job=job.name,
                )
                emit_gauge(
                    "job_health",
                    1.0,
                    component="engine.runtime.jobs_manager",
                    job=job.name,
                    extra_tags={"metric_scope": "job_running"},
                )
                _job_launch_trace_append({
                    "job": str(job.name),
                    "attempted": True,
                    "spawned": True,
                    "failed": False,
                    "entry_valid": True,
                    "entry": str(script_rel),
                    "script_path": str(script_path),
                    "cwd": str(_PROJECT_ROOT),
                    "pid": int(getattr(job.proc, "pid", 0) or 0),
                    "exit_code": 0,
                    "completed_during_start_grace": True,
                    "ts_ms": int(time.time() * 1000),
                })
                self._publish_resource_scheduler_state()
                return {"ok": True, "exit_code": 0}

            if early_rc is not None or not pid_alive:
                if early_rc is None:
                    early_rc = -1
                job.exited_at_ms = int(time.time() * 1000)
                job.exit_code = int(early_rc)
                if not pid_alive and int(job.exit_code) == -1:
                    job.append_log("[server] start failed: process pid not alive after spawn grace")
                    _write_job_history(job.name, "start_failed", "process pid not alive after spawn grace", job.exit_code)
                else:
                    job.append_log(f"[server] start failed: exited rc={job.exit_code}")
                    job.last_error = f"start_failed rc={job.exit_code}"
                    _write_job_history(job.name, "start_failed", f"process exited rc={job.exit_code}", job.exit_code)

                _job_launch_trace_append({
                    "job": str(job.name),
                    "attempted": True,
                    "spawned": False,
                    "failed": True,
                    "entry_valid": True,
                    "entry": str(script_rel),
                    "script_path": str(script_path),
                    "cwd": str(_PROJECT_ROOT),
                    "pid": int(getattr(job.proc, "pid", 0) or 0),
                    "exit_code": int(job.exit_code),
                    "error": "process pid not alive after spawn grace" if (not pid_alive and int(job.exit_code) == -1) else f"process exited rc={job.exit_code}",
                    "ts_ms": int(time.time() * 1000),
                })

                try:
                    if job.proc is not None and job.proc.poll() is None:
                        job.proc.kill()
                        try:
                            job.proc.wait(timeout=1)
                        except Exception as kill_wait_err:
                            _warn_nonfatal("JOBS_MANAGER_START_FAIL_KILL_WAIT_FAILED", kill_wait_err, job=str(job.name))
                except Exception as kill_err:
                    _warn_nonfatal("JOBS_MANAGER_START_FAIL_KILL_FAILED", kill_err, job=str(job.name))

                if job.mode == "oneshot":
                    try:
                        _release_lock(f"job:{job.name}")
                    except Exception as release_err:
                        _warn_nonfatal("JOBS_MANAGER_ONESHOT_RELEASE_FAILED", release_err, job=str(job.name), scope="start_failed_cleanup")
                    job._oneshot_lock_name = None

                emit_counter(
                    "job_failure",
                    1,
                    component="engine.runtime.jobs_manager",
                    job=job.name,
                    extra_tags={"failure_type": "start_exit"},
                )
                emit_gauge(
                    "job_health",
                    0.0,
                    component="engine.runtime.jobs_manager",
                    job=job.name,
                    extra_tags={"metric_scope": "job_running"},
                )
                trace_event(
                    "job_start_failed",
                    component="engine.runtime.jobs_manager",
                    entity_type="job",
                    entity_id=str(job.name),
                    payload={
                        "script": str(job.script),
                        "mode": str(job.mode),
                        "group": str(job.group or ""),
                        "exit_code": int(job.exit_code),
                        "pid_alive": bool(pid_alive),
                    },
                    job=job.name,
                )
                log_event(
                    LOG,
                    40,
                    "job_start_failed",
                    component="engine.runtime.jobs_manager",
                    extra={
                        "job": str(job.name),
                        "script": str(job.script),
                        "mode": str(job.mode),
                        "group": str(job.group or ""),
                        "exit_code": int(job.exit_code),
                        "pid_alive": bool(pid_alive),
                    },
                )
                try:
                    if is_market_data_job(job.name):
                        set_state(DEGRADED, f"{job.name}_start_failed_rc={job.exit_code}")
                except Exception as state_err:
                    _warn_nonfatal("JOBS_MANAGER_SET_STATE_FAILED", state_err, job=str(job.name), scope="start_failed_rc")

                _job_launch_trace_append({
                    "job": str(job.name),
                    "attempted": True,
                    "spawned": False,
                    "failed": True,
                    "entry_valid": True,
                    "entry": str(script_rel),
                    "script_path": str(script_path),
                    "cwd": str(_PROJECT_ROOT),
                    "pid": int(getattr(job.proc, "pid", 0) or 0),
                    "exit_code": int(job.exit_code),
                    "error": f"process exited rc={job.exit_code}",
                    "ts_ms": int(time.time() * 1000),
                })

                self._publish_resource_scheduler_state()
                return {"ok": False, "error": f"process exited rc={job.exit_code}", "exit_code": int(job.exit_code)}

            emit_counter(
                "job_start",
                1,
                component="engine.runtime.jobs_manager",
                job=job.name,
            )

            try:
                if job.name == "ingestion_runtime" and not str(meta_get("first_price_ts_ms", "") or "").strip():
                    set_state(WARMING_UP, "ingestion_runtime_boot_confirmed")
            except Exception as state_err:
                _warn_nonfatal("JOBS_MANAGER_SET_STATE_FAILED", state_err, job=str(job.name), scope="ingestion_runtime_boot_confirmed")
            emit_gauge(
                "job_health",
                1.0,
                component="engine.runtime.jobs_manager",
                job=job.name,
                extra_tags={"metric_scope": "job_running"},
            )
            trace_event(
                "job_start",
                component="engine.runtime.jobs_manager",
                entity_type="job",
                entity_id=str(job.name),
                payload={
                    "script": str(job.script),
                    "mode": str(job.mode),
                    "group": str(job.group or ""),
                    "pid": int(job.proc.pid) if job.proc is not None else None,
                },
                job=job.name,
            )
            log_event(
                LOG,
                20,
                "job_start",
                component="engine.runtime.jobs_manager",
                extra={
                    "job": str(job.name),
                    "script": str(job.script),
                    "mode": str(job.mode),
                    "group": str(job.group or ""),
                    "pid": int(job.proc.pid) if job.proc is not None else None,
                },
            )
            try:
                if is_market_data_job(job.name) and not str(meta_get("first_price_ts_ms", "") or "").strip():
                    set_state(WARMING_UP, f"{job.name}_started_awaiting_first_price_tick")
            except Exception as state_err:
                _warn_nonfatal("JOBS_MANAGER_SET_STATE_FAILED", state_err, job=str(job.name), scope="started_awaiting_first_price_tick")

            job.last_success_ts = int(time.time() * 1000)

            _job_launch_trace_append({
                "job": str(job.name),
                "attempted": True,
                "spawned": True,
                "failed": False,
                "entry_valid": True,
                "entry": str(script_rel),
                "script_path": str(script_path),
                "cwd": str(_PROJECT_ROOT),
                "pid": int(getattr(job.proc, "pid", 0) or 0),
                "ts_ms": int(time.time() * 1000),
            })

            self._publish_resource_scheduler_state()
            return {"ok": True, "status": "started"}

    def stop(self, name: str) -> Dict:
        job = self.get(name)
        if not job:
            _job_launch_trace_append({
                "job": str(name),
                "attempted": True,
                "spawned": False,
                "failed": True,
                "entry_valid": False,
                "error": f"unknown job: {name}",
                "ts_ms": int(time.time() * 1000),
            })
            return {"ok": False, "error": f"unknown job: {name}"}

        with job._lock:
            job.stop_requested = True
            job.next_restart_ms = 0

            if not job.proc or job.proc.poll() is not None:
                for lock_name in _runtime_lock_candidates(job.name):
                    try:
                        _release_lock(lock_name)
                    except Exception as release_err:
                        _warn_nonfatal("JOBS_MANAGER_RUNTIME_LOCK_RELEASE_FAILED", release_err, job=str(job.name), lock_name=str(lock_name), scope="stop_not_running")
                _write_job_history(job.name, "stop", "not_running", None)
                emit_counter(
                    "job_stop",
                    1,
                    component="engine.runtime.jobs_manager",
                    job=job.name,
                )
                emit_gauge(
                    "job_health",
                    0.0,
                    component="engine.runtime.jobs_manager",
                    job=job.name,
                    extra_tags={"metric_scope": "job_running"},
                )
                trace_event(
                    "job_stop",
                    component="engine.runtime.jobs_manager",
                    entity_type="job",
                    entity_id=str(job.name),
                    payload={"status": "not_running"},
                    job=job.name,
                )
                self._publish_resource_scheduler_state()
                return {"ok": True, "status": "not_running"}

            job.append_log("[server] stopping...")
            _write_job_history(job.name, "stop", "terminate()", None)

            try:
                job.proc.terminate()
                try:
                    job.proc.wait(timeout=3)
                except Exception as wait_err:
                    _warn_nonfatal("JOBS_MANAGER_STOP_WAIT_FAILED", wait_err, job=str(job.name))
                    try:
                        job.proc.kill()
                        try:
                            job.proc.wait(timeout=5)
                        except Exception as kill_wait_err:
                            _warn_nonfatal("JOBS_MANAGER_STOP_KILL_WAIT_FAILED", kill_wait_err, job=str(job.name))
                    except Exception as kill_err:
                        _warn_nonfatal("JOBS_MANAGER_STOP_KILL_FAILED", kill_err, job=str(job.name))
            except Exception as e:
                job.append_log(f"[server] terminate error: {e}")
                _write_job_history(job.name, "stop_failed", str(e), None)
                _warn_nonfatal("JOBS_MANAGER_STOP_FAILED", e, job=str(job.name))
                return {"ok": False, "error": str(e)}

        for lock_name in _runtime_lock_candidates(job.name):
            try:
                _release_lock(lock_name)
            except Exception as release_err:
                _warn_nonfatal("JOBS_MANAGER_RUNTIME_LOCK_RELEASE_FAILED", release_err, job=str(job.name), lock_name=str(lock_name), scope="stop_completed")

        emit_counter(
            "job_stop",
            1,
            component="engine.runtime.jobs_manager",
            job=job.name,
        )
        trace_event(
            "job_stop",
            component="engine.runtime.jobs_manager",
            entity_type="job",
            entity_id=str(job.name),
            payload={"status": "terminate_sent"},
            job=job.name,
        )
        log_event(
            LOG,
            20,
            "job_stop",
            component="engine.runtime.jobs_manager",
            extra={"job": str(job.name), "status": "terminate_sent"},
        )
        self._publish_resource_scheduler_state()
        return {"ok": True, "status": "terminate_sent"}

    def stop_all(
        self,
        *,
        drain_before_kill: Optional[Callable[..., Dict[str, Any]]] = None,
        drain_deadline_s: Optional[float] = None,
    ) -> Dict:
        stopped = []
        errors = []
        self._stop_event.set()

        with self._lock:
            jobs = list(self._jobs.values())

        if drain_before_kill is not None:
            stop_budget_s = float(drain_deadline_s) if drain_deadline_s is not None else 5.0
            stop_deadline = time.monotonic() + max(0.0, stop_budget_s)
            running: list[tuple[JobState, subprocess.Popen]] = []
            for job in jobs:
                try:
                    with job._lock:
                        job.stop_requested = True
                        job.next_restart_ms = 0
                        p = job.proc
                    if not p or p.poll() is not None:
                        for lock_name in _runtime_lock_candidates(job.name):
                            _release_lock_best_effort(
                                lock_name,
                                job=str(job.name),
                                scope="stop_all_not_running",
                                deadline=stop_deadline,
                            )
                        _write_job_history(job.name, "stop", "not_running", None)
                        stopped.append(job.name)
                        continue

                    with job._lock:
                        job.append_log("[server] stopping...")
                    _write_job_history(job.name, "stop", "terminate()", None)
                    p.terminate()
                    running.append((job, p))
                    stopped.append(job.name)
                except Exception as e:
                    errors.append(f"{job.name}: terminate failed: {e}")
                    _warn_nonfatal("JOBS_MANAGER_STOP_ALL_TERMINATE_FAILED", e, job=str(job.name))

            drain_snapshot: Dict[str, Any] = {}
            try:
                drain_snapshot = dict(
                    drain_before_kill(
                        reason="jobs_manager_stop_all_pre_sigkill",
                        deadline_s=max(0.0, float(stop_deadline) - time.monotonic()),
                    )
                    or {}
                )
            except Exception as e:
                errors.append(f"drain_before_kill: {e}")
                _warn_nonfatal("JOBS_MANAGER_STOP_ALL_DRAIN_BEFORE_KILL_FAILED", e)

            for job, p in running:
                try:
                    if p.poll() is None:
                        p.kill()
                        try:
                            p.wait(timeout=min(5.0, max(0.0, float(stop_deadline) - time.monotonic())))
                        except Exception as kill_wait_err:
                            _warn_nonfatal("JOBS_MANAGER_STOP_ALL_KILL_WAIT_FAILED", kill_wait_err, job=str(job.name))
                        with job._lock:
                            job.append_log("[server] hard-kill (kill())")
                            _write_job_history(job.name, "stop_hard_kill", "kill()", None)
                    with job._lock:
                        job.exit_code = p.poll()
                        job.exited_at_ms = int(time.time() * 1000)
                except Exception as e:
                    errors.append(f"{job.name}: kill failed: {e}")
                    _warn_nonfatal("JOBS_MANAGER_STOP_ALL_KILL_FAILED", e, job=str(job.name))

            for job in jobs:
                for lock_name in _runtime_lock_candidates(job.name):
                    _release_lock_best_effort(
                        lock_name,
                        job=str(job.name),
                        scope="stop_all_completed",
                        deadline=stop_deadline,
                    )
                emit_counter(
                    "job_stop",
                    1,
                    component="engine.runtime.jobs_manager",
                    job=job.name,
                )
                trace_event(
                    "job_stop",
                    component="engine.runtime.jobs_manager",
                    entity_type="job",
                    entity_id=str(job.name),
                    payload={"status": "terminate_sent"},
                    job=job.name,
                )

            if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
                try:
                    self._watchdog_thread.join(
                        timeout=min(
                            max(1.0, float(DAEMON_WATCHDOG_PERIOD_S) * 2.0),
                            max(0.0, float(stop_deadline) - time.monotonic()),
                        )
                    )
                except Exception as e:
                    errors.append(f"watchdog_join: {e}")

            return {"ok": len(errors) == 0, "stopped": stopped, "errors": errors, "drain": drain_snapshot}

        for job in jobs:
            try:
                self.stop(job.name)
                stopped.append(job.name)
            except Exception as e:
                errors.append(f"{job.name}: {e}")

        deadline = time.time() + 3.0
        for job in jobs:
            try:
                with job._lock:
                    p = job.proc
                if not p:
                    continue
                while time.time() < deadline:
                    if p.poll() is not None:
                        break
                    time.sleep(0.05)
                if p.poll() is None:
                    try:
                        p.kill()
                        try:
                            p.wait(timeout=5)
                        except Exception as kill_wait_err:
                            _warn_nonfatal("JOBS_MANAGER_STOP_ALL_KILL_WAIT_FAILED", kill_wait_err, job=str(job.name))
                        with job._lock:
                            job.append_log("[server] hard-kill (kill())")
                            _write_job_history(job.name, "stop_hard_kill", "kill()", None)
                    except Exception as e:
                        errors.append(f"{job.name}: kill failed: {e}")
            except Exception as e:
                errors.append(f"{job.name}: wait/kill error: {e}")

        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            try:
                self._watchdog_thread.join(timeout=max(1.0, float(DAEMON_WATCHDOG_PERIOD_S) * 2.0))
            except Exception as e:
                errors.append(f"watchdog_join: {e}")

        return {"ok": len(errors) == 0, "stopped": stopped, "errors": errors}

    def _oneshot_lock_heartbeat_loop(self, job: JobState):
        loop_errors = 0
        max_loop_errors = int(os.environ.get("ONESHOT_HEARTBEAT_MAX_ERRORS", "20"))

        while True:
            try:
                with job._lock:
                    proc = job.proc
                    lock_name = getattr(job, "_oneshot_lock_name", None)

                if not proc or not lock_name:
                    return

                rc = proc.poll()
                if rc is not None:
                    return

                try:
                    _heartbeat_lock(lock_name)
                except Exception as e:
                    job.append_log(f"[server] oneshot lock heartbeat failed: {type(e).__name__}: {e}")
                    log_event(
                        LOG,
                        40,
                        "oneshot_lock_heartbeat_failed",
                        component="engine.runtime.jobs_manager",
                        extra={"job": str(job.name), "lock_name": str(lock_name), "error": f"{type(e).__name__}: {e}"},
                    )
                loop_errors = 0
            except Exception as e:
                loop_errors += 1
                try:
                    job.append_log(f"[server] oneshot heartbeat loop error: {type(e).__name__}: {e}")
                except Exception as append_err:
                    _warn_nonfatal("JOBS_MANAGER_JOB_APPEND_LOG_FAILED", append_err, job=str(job.name), scope="oneshot_heartbeat_loop_error")
                log_event(
                    LOG,
                    40,
                    "oneshot_lock_heartbeat_loop_error",
                    component="engine.runtime.jobs_manager",
                    extra={
                        "job": str(job.name),
                        "error": f"{type(e).__name__}: {e}",
                        "loop_errors": int(loop_errors),
                        "max_loop_errors": int(max_loop_errors),
                    },
                )
                if loop_errors >= max_loop_errors:
                    try:
                        job.append_log(
                            f"[server] oneshot heartbeat loop aborting after {loop_errors} consecutive errors"
                        )
                    except Exception as append_err:
                        _warn_nonfatal("JOBS_MANAGER_JOB_APPEND_LOG_FAILED", append_err, job=str(job.name), scope="oneshot_heartbeat_loop_abort")
                    return

            time.sleep(5.0)

    def _pump_output(self, job: JobState):
        proc = job.proc
        if not proc:
            return

        # For oneshot jobs, keep the lock alive while the process runs
        lock_name = getattr(job, "_oneshot_lock_name", None)

        def _finalize_exit(rc):
            with job._lock:
                job.proc = None
                job.exited_at_ms = int(time.time() * 1000)
                job.exit_code = int(rc) if rc is not None else None
                job._restart_in_flight = False
                job.append_log(f"[server] exited rc={job.exit_code}")
                if int(job.exit_code or 0) != 0:
                    job.last_error = f"runtime_exit rc={job.exit_code}"
                _write_job_history(job.name, "exit", "process exited", job.exit_code)

            runtime_ms = 0
            try:
                if job.started_at_ms and job.exited_at_ms:
                    runtime_ms = max(0, int(job.exited_at_ms) - int(job.started_at_ms))
            except Exception:
                runtime_ms = 0

            emit_counter(
                "job_exit",
                1,
                component="engine.runtime.jobs_manager",
                job=job.name,
                extra_tags={"exit_code": int(job.exit_code) if job.exit_code is not None else "none"},
            )
            emit_gauge(
                "job_health",
                0.0,
                component="engine.runtime.jobs_manager",
                job=job.name,
                extra_tags={"metric_scope": "job_running"},
            )
            emit_timing(
                "job_runtime_ms",
                int(runtime_ms),
                component="engine.runtime.jobs_manager",
                job=job.name,
            )
            trace_event(
                "job_exit",
                component="engine.runtime.jobs_manager",
                entity_type="job",
                entity_id=str(job.name),
                payload={
                    "exit_code": int(job.exit_code) if job.exit_code is not None else None,
                    "runtime_ms": int(runtime_ms),
                },
                job=job.name,
            )

            try:
                if is_market_data_job(job.name) and int(job.exit_code or 0) != 0 and not bool(job.stop_requested):
                    set_state(DEGRADED, f"{job.name}_runtime_exit_rc={job.exit_code}")
                    log_event(
                        LOG,
                        40,
                        "critical_job_runtime_exit",
                        component="engine.runtime.jobs_manager",
                        extra={
                            "job": str(job.name),
                            "exit_code": int(job.exit_code or 0),
                        },
                    )
            except Exception as e:
                _warn_nonfatal("JOBS_MANAGER_RUNTIME_EXIT_STATE_UPDATE_FAILED", e, job=str(job.name), exit_code=int(job.exit_code or 0))

            for runtime_lock_name in _runtime_lock_candidates(job.name):
                try:
                    _release_lock(runtime_lock_name)
                except Exception as release_err:
                    _warn_nonfatal("JOBS_MANAGER_RUNTIME_LOCK_RELEASE_FAILED", release_err, job=str(job.name), lock_name=str(runtime_lock_name), scope="finalize_exit")

            if lock_name:
                try:
                    _release_lock(lock_name)
                except Exception as release_err:
                    _warn_nonfatal("JOBS_MANAGER_ONESHOT_RELEASE_FAILED", release_err, job=str(job.name), lock_name=str(lock_name), scope="finalize_exit")
                with job._lock:
                    job._oneshot_lock_name = None

            self._publish_resource_scheduler_state()

        def _drain_stream(stream, prefix: str = ""):
            if not stream:
                return
            try:
                for line in stream:
                    if not line:
                        break
                    if prefix:
                        job.append_log(f"{prefix}{line}")
                    else:
                        job.append_log(line)
            except Exception as e:
                job.append_log(f"[server] {prefix.strip() or 'log'} pump error: {e}")
            finally:
                try:
                    stream.close()
                except Exception as close_err:
                    _warn_nonfatal("JOBS_MANAGER_STREAM_CLOSE_FAILED", close_err, job=str(job.name), stream_prefix=str(prefix or "stdout"))

        stderr_thread = None
        if proc.stderr:
            stderr_thread = threading.Thread(
                target=_drain_stream,
                args=(proc.stderr, "[stderr] "),
                daemon=True,
                name=f"{job.name}_stderr_pump",
            )
            stderr_thread.start()

        if not proc.stdout:
            try:
                rc = proc.wait(timeout=float(os.environ.get("JOB_NO_STDOUT_WAIT_TIMEOUT_S", "60")))
            except Exception as wait_err:
                _warn_nonfatal("JOBS_MANAGER_NO_STDOUT_WAIT_FAILED", wait_err, job=str(job.name))
                rc = proc.poll()
                if rc is None:
                    try:
                        proc.terminate()
                    except Exception as terminate_err:
                        _warn_nonfatal("JOBS_MANAGER_NO_STDOUT_TERMINATE_FAILED", terminate_err, job=str(job.name))
                    try:
                        rc = proc.wait(timeout=5)
                    except Exception as post_term_wait_err:
                        _warn_nonfatal("JOBS_MANAGER_NO_STDOUT_POST_TERMINATE_WAIT_FAILED", post_term_wait_err, job=str(job.name))
                        try:
                            proc.kill()
                        except Exception as kill_err:
                            _warn_nonfatal("JOBS_MANAGER_NO_STDOUT_KILL_FAILED", kill_err, job=str(job.name))
                        rc = proc.poll()
            if stderr_thread is not None:
                try:
                    stderr_thread.join(timeout=1.0)
                except Exception as join_err:
                    _warn_nonfatal("JOBS_MANAGER_STDERR_JOIN_FAILED", join_err, job=str(job.name), scope="no_stdout")
            _finalize_exit(rc)
            return

        try:
            _drain_stream(proc.stdout)
        finally:
            try:
                rc = proc.poll()
                if rc is None:
                    rc = proc.wait(timeout=1)
            except Exception as wait_err:
                _warn_nonfatal("JOBS_MANAGER_FINAL_WAIT_FAILED", wait_err, job=str(job.name))
                rc = proc.poll()
            if stderr_thread is not None:
                try:
                    stderr_thread.join(timeout=1.0)
                except Exception as join_err:
                    _warn_nonfatal("JOBS_MANAGER_STDERR_JOIN_FAILED", join_err, job=str(job.name), scope="stdout_drain")
            _finalize_exit(rc)

            if job.mode == "oneshot":
                try:
                    _release_lock(f"job:{job.name}")
                except Exception as release_err:
                    _warn_nonfatal("JOBS_MANAGER_ONESHOT_RELEASE_FAILED", release_err, job=str(job.name), scope="stdout_drain_finalize")

    def _daemon_watchdog_loop(self):
        while not self._stop_event.wait(timeout=DAEMON_WATCHDOG_PERIOD_S):
            try:
                if AUTO_RESTART_DAEMONS:
                    self._check_and_restart_daemons()
            except Exception as e:
                log_event(
                    LOG,
                    40,
                    "daemon_watchdog_loop_error",
                    component="engine.runtime.jobs_manager",
                    extra={"error": f"{type(e).__name__}: {e}"},
                )

    def _check_and_restart_daemons(self):
        now = int(time.time() * 1000)
        with self._lock:
            jobs = list(self._jobs.values())

        for job in jobs:
            if job.mode != "daemon":
                continue

            # Never stall-restart ingestion_runtime (it supervises feeds)
            if job.name == "ingestion_runtime":
                continue

            with job._lock:
                if job.stop_requested:
                    continue

                if job.proc and self.is_running(job.name):
                    # observe worker heartbeat only; do not mutate worker lock ownership here
                    try:
                        emit_counter(
                            "job_heartbeat",
                            1,
                            component="engine.runtime.jobs_manager",
                            job=job.name,
                        )
                        emit_gauge(
                            "job_health",
                            1.0,
                            component="engine.runtime.jobs_manager",
                            job=job.name,
                            extra_tags={"metric_scope": "watchdog"},
                        )
                    except Exception as metrics_err:
                        _warn_nonfatal("JOBS_MANAGER_WATCHDOG_METRICS_FAILED", metrics_err, job=str(job.name))

                    # stall detection: restart when heartbeat goes stale OR never appears
                    try:
                        row = _read_runtime_lock(job.name) or {}
                        hb = int(row.get("heartbeat_ts_ms") or 0)
                        stall_reason = None

                        if (
                            hb > 0
                            and job.started_at_ms
                            and (now - job.started_at_ms) > int(_DAEMON_STALL_AFTER_MS)
                            and (now - hb) > int(_DAEMON_STALL_AFTER_MS)
                        ):
                            stall_reason = f"hb_age_ms={now-hb}"

                        elif (
                            hb <= 0
                            and job.started_at_ms
                            and (now - job.started_at_ms) > int(_DAEMON_STALL_AFTER_MS)
                        ):
                            stall_reason = f"no_heartbeat_since_start age_ms={now - int(job.started_at_ms or 0)}"

                        if stall_reason:
                            job.append_log(
                                f"[server] daemon stall detected; {stall_reason}; forcing restart"
                            )
                            _write_job_history(
                                job.name,
                                "autorestart_stall_detected",
                                str(stall_reason),
                                None,
                            )
                            emit_counter(
                                "job_restart_count",
                                1,
                                component="engine.runtime.jobs_manager",
                                job=job.name,
                                extra_tags={"restart_reason": "stall_detected"},
                            )
                            trace_event(
                                "job_restart_scheduled",
                                component="engine.runtime.jobs_manager",
                                entity_type="job",
                                entity_id=str(job.name),
                                payload={"restart_reason": "stall_detected", "detail": str(stall_reason)},
                                job=job.name,
                            )

                            # Force-kill without setting stop_requested (so watchdog can restart)
                            try:
                                p = job.proc
                            except Exception as proc_err:
                                _warn_nonfatal("JOBS_MANAGER_WATCHDOG_PROC_READ_FAILED", proc_err, job=str(job.name))
                                p = None

                            try:
                                if p and p.poll() is None:
                                    try:
                                        p.terminate()
                                    except Exception as terminate_err:
                                        _warn_nonfatal("JOBS_MANAGER_WATCHDOG_TERMINATE_FAILED", terminate_err, job=str(job.name))

                                    # short wait then kill
                                    deadline = time.time() + 2.0
                                    while time.time() < deadline:
                                        if p.poll() is not None:
                                            break
                                        time.sleep(0.05)

                                    if p.poll() is None:
                                        try:
                                            p.kill()
                                            try:
                                                p.wait(timeout=5)
                                            except Exception as kill_wait_err:
                                                _warn_nonfatal("JOBS_MANAGER_WATCHDOG_KILL_WAIT_FAILED", kill_wait_err, job=str(job.name))
                                        except Exception as kill_err:
                                            _warn_nonfatal("JOBS_MANAGER_WATCHDOG_KILL_FAILED", kill_err, job=str(job.name))
                            except Exception as restart_err:
                                _warn_nonfatal("JOBS_MANAGER_WATCHDOG_RESTART_ENFORCEMENT_FAILED", restart_err, job=str(job.name), stall_reason=str(stall_reason))

                            # allow restart path to proceed
                            job.exit_code = -9
                            job.exited_at_ms = now
                    except Exception as e:
                        job.append_log(f"[server] watchdog stall-check error: {type(e).__name__}: {e}")
                        log_event(
                            LOG,
                            40,
                            "daemon_stall_check_failed",
                            component="engine.runtime.jobs_manager",
                            extra={"job": str(job.name), "error": f"{type(e).__name__}: {e}"},
                        )

                    continue

                if not job.started_at_ms:
                    continue

                # --------------------------------------------------
                # HARD EXECUTION GATE: never auto-restart execution jobs
                # unless the execution gate is explicitly OK.
                # Fail-closed by default.
                # --------------------------------------------------
                if getattr(job, "meta", {}).get("execution") is True:
                    gate = execution_gate_snapshot(
                        system_state=self._get_execution_mode_fn() if self._get_execution_mode_fn else None,
                        kill_switches=self._get_kill_switches_fn() if self._get_kill_switches_fn else None,
                        execution_degraded=False,
                    )
                    if (not gate.get("ok")) or (not gate.get("allowed")):
                        job.append_log(
                            f"[server] auto-restart blocked (execution gated): {gate.get('reason') or gate}"
                        )
                        _write_job_history(
                            job.name,
                            "autorestart_blocked_execution_gated",
                            str(gate),
                            job.exit_code,
                        )
                        # stop further restart attempts until an operator manually starts
                        job.stop_requested = True
                        continue

                if job.next_restart_ms and now < job.next_restart_ms:
                    continue

                window_start = now - (DAEMON_RESTART_WINDOW_S * 1000)
                while job.restart_attempts_window and job.restart_attempts_window[0] < window_start:
                    job.restart_attempts_window.popleft()

                if len(job.restart_attempts_window) >= DAEMON_RESTART_MAX_IN_WINDOW:
                    cooldown_ms = int(
                        os.environ.get(
                            "DAEMON_RESTART_COOLDOWN_AFTER_WINDOW_MS",
                            str(max(300000, int(DAEMON_RESTART_WINDOW_S) * 1000)),
                        )
                    )
                    max_cooldowns = int(os.environ.get("DAEMON_RESTART_MAX_COOLDOWNS", "3"))

                    oldest = int(job.restart_attempts_window[0]) if job.restart_attempts_window else now
                    retry_at_ms = int(oldest + cooldown_ms)

                    if now < retry_at_ms:
                        job.next_restart_ms = max(int(job.next_restart_ms or 0), retry_at_ms)
                        job.append_log(
                            f"[server] auto-restart cooling down until {retry_at_ms}; "
                            f"too many restarts in {DAEMON_RESTART_WINDOW_S}s"
                        )
                        _write_job_history(
                            job.name,
                            "autorestart_cooldown",
                            f"retry_at_ms={retry_at_ms}",
                            job.exit_code,
                        )
                        continue

                    job.restart_window_exhaustions = int(job.restart_window_exhaustions or 0) + 1
                    if job.restart_window_exhaustions >= max_cooldowns and _ALLOW_DAEMON_PERMANENT_FAILURE:
                        job.failed_reason = (
                            f"restart_limit_exhausted windows={job.restart_window_exhaustions} "
                            f"max_in_window={DAEMON_RESTART_MAX_IN_WINDOW} "
                            f"window_s={DAEMON_RESTART_WINDOW_S}"
                        )
                        job.stop_requested = True
                        job._restart_in_flight = False
                        job.next_restart_ms = 0
                        job.append_log(f"[server] permanent failure: {job.failed_reason}")
                        _write_job_history(
                            job.name,
                            "autorestart_permanent_failure",
                            str(job.failed_reason),
                            job.exit_code,
                        )

                        try:
                            if is_market_data_job(job.name):
                                set_state(DEGRADED, f"{job.name}_permanent_failure")
                        except Exception as state_err:
                            _warn_nonfatal("JOBS_MANAGER_SET_STATE_FAILED", state_err, job=str(job.name), scope="permanent_failure")
                        emit_counter(
                            "job_failure",
                            1,
                            component="engine.runtime.jobs_manager",
                            job=job.name,
                            extra_tags={"failure_type": "restart_limit_exhausted"},
                        )
                        trace_event(
                            "job_permanent_failure",
                            component="engine.runtime.jobs_manager",
                            entity_type="job",
                            entity_id=str(job.name),
                            payload={"reason": str(job.failed_reason)},
                            job=job.name,
                        )
                        continue

                    if job.restart_window_exhaustions >= max_cooldowns:
                        retry_after_ms = int(
                            os.environ.get(
                                "DAEMON_RESTART_SURVIVAL_COOLDOWN_MS",
                                str(max(cooldown_ms, 900000)),
                            )
                        )
                        job.next_restart_ms = max(int(job.next_restart_ms or 0), now + max(1000, retry_after_ms))
                        job._restart_in_flight = False
                        job.restart_attempts_window.clear()
                        job.append_log(
                            "[server] restart budget exhausted; entering survival cooldown "
                            f"for {retry_after_ms}ms instead of permanent failure"
                        )
                        _write_job_history(
                            job.name,
                            "autorestart_survival_cooldown",
                            f"retry_after_ms={retry_after_ms}",
                            job.exit_code,
                        )
                        emit_counter(
                            "job_restart_count",
                            1,
                            component="engine.runtime.jobs_manager",
                            job=job.name,
                            extra_tags={"restart_reason": "survival_cooldown"},
                        )
                        trace_event(
                            "job_restart_survival_cooldown",
                            component="engine.runtime.jobs_manager",
                            entity_type="job",
                            entity_id=str(job.name),
                            payload={"retry_after_ms": int(retry_after_ms)},
                            job=job.name,
                        )
                        continue

                    job.restart_attempts_window.clear()
                    job.next_restart_ms = 0

                attempt_n = len(job.restart_attempts_window)
                delay = DAEMON_RESTART_BASE_DELAY_MS * (2 ** attempt_n)
                delay = min(int(delay), int(DAEMON_RESTART_MAX_DELAY_MS))
                delay = max(250, delay)
                job.next_restart_ms = now + delay

                # mark restart thread as pending (prevents duplicate threads)
                if job._restart_in_flight:
                    continue
                job._restart_in_flight = True

                job.append_log(f"[server] daemon crashed; scheduling restart in {delay}ms")
                _write_job_history(job.name, "autorestart_scheduled", f"delay_ms={delay}", job.exit_code)
                emit_counter(
                    "job_restart_count",
                    1,
                    component="engine.runtime.jobs_manager",
                    job=job.name,
                    extra_tags={"restart_reason": "daemon_crash"},
                )
                trace_event(
                    "job_restart_scheduled",
                    component="engine.runtime.jobs_manager",
                    entity_type="job",
                    entity_id=str(job.name),
                    payload={
                        "delay_ms": int(delay),
                        "exit_code": int(job.exit_code) if job.exit_code is not None else None,
                    },
                    job=job.name,
                )

                # record attempt at schedule-time to avoid thread storms
                job.restart_attempts_window.append(int(time.time() * 1000))
                if len(job.restart_attempts_window) > 1:
                    job.restart_attempts_window = deque(
                        list(job.restart_attempts_window)[-DAEMON_RESTART_MAX_IN_WINDOW:],
                        maxlen=50,
                    )

            def _restart_later(jref: JobState, delay_ms: int):
                try:
                    # Restart scheduling is deferred to a thread so the watchdog
                    # loop stays responsive while backoff timers are waiting.
                    time.sleep(delay_ms / 1000.0)

                    with jref._lock:
                        if jref.stop_requested:
                            jref._restart_in_flight = False
                            return
                        if self.is_running(jref.name):
                            jref._restart_in_flight = False
                            return

                    res = self.start(jref.name)
                    if not res.get("ok"):
                        with jref._lock:
                            jref._restart_in_flight = False
                            jref.append_log(f"[server] auto-restart failed: {res.get('error')}")
                            _write_job_history(
                                jref.name,
                                "autorestart_failed",
                                str(res.get("error") or ""),
                                jref.exit_code,
                            )
                        emit_counter(
                            "job_failure",
                            1,
                            component="engine.runtime.jobs_manager",
                            job=jref.name,
                            extra_tags={"failure_type": "autorestart_failed"},
                        )
                        trace_event(
                            "job_restart_failed",
                            component="engine.runtime.jobs_manager",
                            entity_type="job",
                            entity_id=str(jref.name),
                            payload={"error": str(res.get("error") or "")},
                            job=jref.name,
                        )
                        return

                    with jref._lock:
                        jref.next_restart_ms = 0
                        jref._restart_in_flight = False
                        jref.append_log("[server] auto-restart: started")
                        _write_job_history(jref.name, "autorestart_started", "started", None)

                    emit_counter(
                        "job_start",
                        1,
                        component="engine.runtime.jobs_manager",
                        job=jref.name,
                        extra_tags={"start_reason": "autorestart"},
                    )
                    trace_event(
                        "job_restart_started",
                        component="engine.runtime.jobs_manager",
                        entity_type="job",
                        entity_id=str(jref.name),
                        payload={"status": "started"},
                        job=jref.name,
                    )
                except Exception as e:
                    with jref._lock:
                        jref._restart_in_flight = False
                        jref.append_log(f"[server] auto-restart thread error: {type(e).__name__}: {e}")
                        _write_job_history(
                            jref.name,
                            "autorestart_thread_error",
                            f"{type(e).__name__}: {e}",
                            jref.exit_code,
                        )
                    log_event(
                        LOG,
                        40,
                        "job_restart_thread_error",
                        component="engine.runtime.jobs_manager",
                        extra={"job": str(jref.name), "error": f"{type(e).__name__}: {e}"},
                    )

            t = threading.Thread(
                target=_restart_later,
                args=(job, delay),
                daemon=True,
            )
            t.start()
