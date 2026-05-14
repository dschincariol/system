"""
FILE: orchestrator.py

Runtime subsystem module for `orchestrator`.
"""

"""
Runtime Orchestrator

Extracted from dashboard_server.py

Owns:
- run_pipeline
- auto pipeline loop
- auto challenger loop
- auto size policy loop

No HTTP logic.
No API logic.
Pure runtime orchestration.
"""

import json
import os
import time
from typing import Dict, Optional, Callable
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.job_registry import ALLOWED_JOBS, PIPELINE_ORDER, get_price_feed_jobs
from engine.runtime.storage import connect as _db_connect
from engine.runtime.gates import execution_gate_snapshot, is_execution_job
from engine.runtime.logging import get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.runtime_meta import meta_get
from engine.runtime.tracing import trace_event

LOG = get_logger("runtime.orchestrator")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: object) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=30,
        component="engine.runtime.orchestrator",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _isolated_ingestion_enabled() -> bool:
    return str(os.environ.get("START_INGESTION_WITH_SERVER", "1")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

class RuntimeOrchestrator:
    def __init__(
        self,
        jobs,
        acquire_lock,
        release_lock,
        auto_pipeline_include_execution: bool,
        auto_pipeline_log: bool,
        auto_pipeline_interval_s: float,
        auto_pipeline_start_delay_s: float,
        auto_challenger_log: bool,
        auto_challenger_interval_s: float,
        auto_challenger_start_delay_s: float,
        auto_challenger_min_drift: float,
        auto_size_policy_log: bool,
        auto_size_policy_interval_s: float,
        auto_size_policy_start_delay_s: float,
        # injected (keeps orchestrator API-free)
        get_kill_switches: Optional[Callable[[], dict]] = None,
        get_execution_mode: Optional[Callable[[], dict]] = None,
    ):
        self.JOBS = jobs
        self._acquire_lock = acquire_lock
        self._release_lock = release_lock

        self.AUTO_PIPELINE_INCLUDE_EXECUTION = auto_pipeline_include_execution
        self.AUTO_PIPELINE_LOG = auto_pipeline_log
        self.AUTO_PIPELINE_INTERVAL_S = auto_pipeline_interval_s
        self.AUTO_PIPELINE_START_DELAY_S = auto_pipeline_start_delay_s

        self.AUTO_CHALLENGER_LOG = auto_challenger_log
        self.AUTO_CHALLENGER_INTERVAL_S = auto_challenger_interval_s
        self.AUTO_CHALLENGER_START_DELAY_S = auto_challenger_start_delay_s
        self.AUTO_CHALLENGER_MIN_DRIFT = auto_challenger_min_drift

        self.AUTO_SIZE_POLICY_LOG = auto_size_policy_log
        self.AUTO_SIZE_POLICY_INTERVAL_S = auto_size_policy_interval_s
        self.AUTO_SIZE_POLICY_START_DELAY_S = auto_size_policy_start_delay_s

        self._get_kill_switches = get_kill_switches or (lambda: {})
        self._get_execution_mode = get_execution_mode or (lambda: {})

    # ---------------------------------------------------
    # Helpers
    # ---------------------------------------------------

    def _is_job_running(self, name: str) -> bool:
        j = self.JOBS.get(name)
        if not j:
            return False
        p = getattr(j, "proc", None)
        if not p:
            return False
        try:
            return p.poll() is None
        except Exception as e:
            _warn_nonfatal("orchestrator_job_poll_failed", "ORCHESTRATOR_JOB_POLL_FAILED", e, warn_key=f"is_job_running:{name}", job=str(name))
            return False

    def _any_price_feed_running(self) -> bool:
        for name in ["ingestion_runtime"] + list(get_price_feed_jobs()):
            if self._is_job_running(name):
                return True
        try:
            raw = str(meta_get("ingestion_state", "") or "").strip()
            if raw:
                state = json.loads(raw)
                if isinstance(state, dict):
                    if bool(state.get("running")):
                        return True
                    children = state.get("children")
                    if isinstance(children, dict):
                        for child in children.values():
                            if isinstance(child, dict) and bool(child.get("running")):
                                return True
        except Exception as exc:
            _warn_nonfatal(
                "runtime_orchestrator_ingestion_state_parse_failed",
                "RUNTIME_ORCHESTRATOR_INGESTION_STATE_PARSE_FAILED",
                exc,
                warn_key="runtime_orchestrator_ingestion_state_parse_failed",
            )
        return False

    def _wait_job_exit(self, name: str, poll_s: float = 0.25) -> Dict:
        """
        Wait for a one-shot job to exit.
        Daemons are not waited on.
        """
        job = self.JOBS.get(name)
        if not job:
            return {"ok": False, "error": f"job_missing:{name}"}

        # Daemons are supervised elsewhere; waiting here only makes sense for
        # oneshot jobs where a clean exit is the success condition.
        try:
            if str(getattr(job, "mode", "") or "").lower() == "daemon":
                return {"ok": True, "daemon": True}
        except Exception as exc:
            _warn_nonfatal(
                "runtime_orchestrator_job_mode_read_failed",
                "RUNTIME_ORCHESTRATOR_JOB_MODE_READ_FAILED",
                exc,
                warn_key="runtime_orchestrator_job_mode_read_failed",
                job=str(name),
            )

        deadline = time.time() + 3600  # 1h safety timeout
        while True:
            if time.time() > deadline:
                return {"ok": False, "error": f"{name} timeout"}
            time.sleep(poll_s)         
            job = self.JOBS.get(name)
            if not job:
                return {"ok": False, "error": f"{name} disappeared"}
            p = getattr(job, "proc", None)
            if not p:
                try:
                    exit_code = getattr(job, "exit_code", None)
                except Exception:
                    exit_code = None
                if exit_code not in (0, None):
                    return {"ok": False, "error": f"{name} exited rc={exit_code}"}
                return {"ok": True, "rc": exit_code}
            try:
                rc = p.poll()
            except Exception:
                rc = None
            if rc is not None:
                try:
                    exit_code = getattr(job, "exit_code", rc)
                except Exception:
                    exit_code = rc
                if exit_code not in (0, None):
                    return {"ok": False, "error": f"{name} exited rc={exit_code}"}
                return {"ok": True, "rc": exit_code}

    # ---------------------------------------------------
    # PIPELINE
    # ---------------------------------------------------

    def _validate_pipeline_plan(self, include_execution: bool) -> Dict:
        # Validate the whole plan before starting anything so bad registry
        # entries fail fast instead of half-running a pipeline.
        planned = []
        known = set(ALLOWED_JOBS.keys())

        for name in PIPELINE_ORDER:
            if name not in known:
                return {
                    "ok": False,
                    "error": "pipeline_unknown_job",
                    "job": name,
                    "results": [],
                }

            spec = ALLOWED_JOBS.get(name)
            mode = ""
            if isinstance(spec, (list, tuple)) and len(spec) >= 2:
                mode = str(spec[1] or "").strip().lower()

            if mode != "oneshot":
                return {
                    "ok": False,
                    "error": "pipeline_non_oneshot_job",
                    "job": name,
                    "mode": mode,
                    "results": [],
                }

            if (not include_execution) and is_execution_job(name):
                continue

            planned.append(name)

        for name in planned:
            if not self.JOBS.get(name):
                return {
                    "ok": False,
                    "error": "job_not_registered",
                    "job": name,
                    "results": [],
                }

        return {"ok": True, "planned": planned}

    def run_pipeline(self, include_execution: bool = False):
        """
        Runs jobs in PIPELINE_ORDER.
        include_execution=False will skip execution jobs.

        Fail closed:
        - every referenced pipeline job must be registered
        - execution jobs require an affirmative execution gate
        - only one pipeline run may execute at a time
        """
        if not self._acquire_lock("pipeline", ttl_ms=60 * 60 * 1000):
            emit_counter(
                "job_failure",
                1,
                component="engine.runtime.orchestrator",
                job="pipeline",
                extra_tags={"failure_type": "pipeline_locked"},
            )
            return {
                "ok": False,
                "error": "pipeline_locked",
                "results": [],
            }

        started_ms = int(time.time() * 1000)

        try:
            plan_check = self._validate_pipeline_plan(include_execution=include_execution)
            if not plan_check.get("ok"):
                return plan_check

            planned = list(plan_check.get("planned") or [])

            if include_execution:
                try:
                    wants_exec = any(is_execution_job(n) for n in planned)
                except Exception:
                    wants_exec = False

                if wants_exec:
                    # Re-check the execution gate here so pipeline execution
                    # steps cannot bypass current kill-switch state.
                    snap = execution_gate_snapshot(
                        system_state=self._get_execution_mode() if self._get_execution_mode else None,
                        kill_switches=self._get_kill_switches() if self._get_kill_switches else None,
                        execution_degraded=False,
                    )
                    if (not snap.get("ok")) or (not (snap.get("allowed") or snap.get("allow_execution"))):
                        return {
                            "ok": False,
                            "error": "execution_gate_blocked",
                            "gate": snap,
                            "results": [],
                        }

            results = []
            for name in planned:
                try:
                    res = self.JOBS.start(name)

                    if not isinstance(res, dict) or not res.get("ok"):
                        results.append({"job": name, "ok": False, "result": res})
                        return {"ok": False, "results": results}

                    wait = self._wait_job_exit(name)
                    if not wait.get("ok"):
                        results.append({"job": name, "ok": False, "result": wait})
                        return {"ok": False, "results": results}

                    results.append({"job": name, "ok": True})
                    emit_counter(
                        "job_start",
                        1,
                        component="engine.runtime.orchestrator",
                        job=name,
                        extra_tags={"start_reason": "pipeline_step"},
                    )
                    trace_event(
                        "feature_pipeline",
                        component="engine.runtime.orchestrator",
                        entity_type="job",
                        entity_id=str(name),
                        payload={"include_execution": bool(include_execution)},
                        job=name,
                    )

                except Exception as e:
                    _warn_nonfatal("orchestrator_job_start_failed", "ORCHESTRATOR_JOB_START_FAILED", e, warn_key=f"start:{name}", job=str(name))
                    results.append({"job": name, "ok": False, "error": str(e)})
                    return {"ok": False, "results": results}

            dur_ms = int(time.time() * 1000) - int(started_ms)
            emit_timing(
                "job_runtime_ms",
                int(dur_ms),
                component="engine.runtime.orchestrator",
                job="pipeline",
            )
            emit_gauge(
                "job_health",
                1.0,
                component="engine.runtime.orchestrator",
                job="pipeline",
                extra_tags={"metric_scope": "pipeline_run"},
            )
            trace_event(
                "feature_pipeline",
                component="engine.runtime.orchestrator",
                entity_type="job",
                entity_id="pipeline",
                payload={
                    "include_execution": bool(include_execution),
                    "jobs_run": [str(r.get("job")) for r in results],
                    "duration_ms": int(dur_ms),
                },
                job="pipeline",
            )
            log_event(
                LOG,
                20,
                "pipeline_complete",
                component="engine.runtime.orchestrator",
                extra={
                    "job": "pipeline",
                    "include_execution": bool(include_execution),
                    "duration_ms": int(dur_ms),
                    "jobs_run": [str(r.get("job")) for r in results],
                },
            )
            return {"ok": True, "results": results}
        finally:
            try:
                self._release_lock("pipeline")
            except Exception as exc:
                _warn_nonfatal(
                    "runtime_orchestrator_pipeline_release_lock_failed",
                    "RUNTIME_ORCHESTRATOR_PIPELINE_RELEASE_LOCK_FAILED",
                    exc,
                    warn_key="runtime_orchestrator_pipeline_release_lock_failed",
                )


    # ---------------------------------------------------
    # AUTO PIPELINE
    # ---------------------------------------------------

    def auto_pipeline_loop(self):
        # The auto loops intentionally use simple polling so operators can reason
        # about cadence and disable behavior with env flags alone.
        time.sleep(max(0.0, float(self.AUTO_PIPELINE_START_DELAY_S)))

        while True:
            try:
                # safety: keep prices flowing without replacing a healthy primary feed
                if not self._any_price_feed_running():
                    if _isolated_ingestion_enabled():
                        if self.AUTO_PIPELINE_LOG:
                            log_event(
                                LOG,
                                20,
                                "auto_pipeline_poll_prices_skip_isolated_ingestion",
                                component="engine.runtime.orchestrator",
                                extra={"job": "poll_prices"},
                            )
                    else:
                        res = self.JOBS.start("poll_prices")
                        if self.AUTO_PIPELINE_LOG:
                            log_event(
                                LOG,
                                20,
                                "auto_pipeline_poll_prices_start",
                                component="engine.runtime.orchestrator",
                                extra={"job": "poll_prices", "result": res},
                            )

                res = self.run_pipeline(include_execution=bool(self.AUTO_PIPELINE_INCLUDE_EXECUTION))
                emit_counter(
                    "job_heartbeat",
                    1,
                    component="engine.runtime.orchestrator",
                    job="auto_pipeline_loop",
                )
                if self.AUTO_PIPELINE_LOG:
                    log_event(
                        LOG,
                        20,
                        "auto_pipeline_run",
                        component="engine.runtime.orchestrator",
                        extra={"job": "pipeline", "result": res},
                    )

            except Exception as e:
                emit_counter(
                    "job_failure",
                    1,
                    component="engine.runtime.orchestrator",
                    job="auto_pipeline_loop",
                    extra_tags={"failure_type": "exception"},
                )
                if self.AUTO_PIPELINE_LOG:
                    log_event(
                        LOG,
                        40,
                        "auto_pipeline_error",
                        component="engine.runtime.orchestrator",
                        extra={"job": "auto_pipeline_loop", "error": str(e)},
                    )

            time.sleep(max(5.0, float(self.AUTO_PIPELINE_INTERVAL_S)))

    # ---------------------------------------------------
    # CHALLENGER
    # ---------------------------------------------------

    def _max_drift_ratio(self) -> float:
        con = _db_connect()
        try:
            try:
                r = con.execute("SELECT MAX(drift_ratio) FROM model_drift").fetchone()
                val = float(r[0] or 0.0) if r else 0.0
            except Exception:
                val = 0.0
            return val
        finally:
            con.close()

    def _run_challenger_job_wait(self) -> Dict:
        if not self.JOBS.get("pipeline_train_and_eval"):
            return {
                "ok": False,
                "error": "job_not_registered",
                "job": "pipeline_train_and_eval",
            }

        if not self._acquire_lock("challenger", ttl_ms=30 * 60 * 1000):
            return {"ok": False, "error": "challenger locked (already running?)"}

        try:
            res = self.JOBS.start("pipeline_train_and_eval")
            if not isinstance(res, dict) or not res.get("ok"):
                return res if isinstance(res, dict) else {"ok": False, "error": "start_failed"}

            wait_res = self._wait_job_exit("pipeline_train_and_eval")
            if not wait_res.get("ok"):
                return wait_res

            return {"ok": True}
        finally:
            try:
                self._release_lock("challenger")
            except Exception as exc:
                _warn_nonfatal(
                    "runtime_orchestrator_challenger_release_lock_failed",
                    "RUNTIME_ORCHESTRATOR_CHALLENGER_RELEASE_LOCK_FAILED",
                    exc,
                    warn_key="runtime_orchestrator_challenger_release_lock_failed",
                )

    def auto_challenger_loop(self):
        time.sleep(max(0.0, float(self.AUTO_CHALLENGER_START_DELAY_S)))

        while True:
            try:
                if self.AUTO_CHALLENGER_MIN_DRIFT > 0.0:
                    md = self._max_drift_ratio()
                    if md < self.AUTO_CHALLENGER_MIN_DRIFT:
                        if self.AUTO_CHALLENGER_LOG:
                            log_event(
                                LOG,
                                20,
                                "auto_challenger_skip",
                                component="engine.runtime.orchestrator",
                                extra={"job": "pipeline_train_and_eval", "max_drift": float(md)},
                            )
                    else:
                        out = self._run_challenger_job_wait()
                        emit_counter(
                            "job_heartbeat",
                            1,
                            component="engine.runtime.orchestrator",
                            job="auto_challenger_loop",
                        )
                        if self.AUTO_CHALLENGER_LOG:
                            log_event(
                                LOG,
                                20,
                                "auto_challenger_result",
                                component="engine.runtime.orchestrator",
                                extra={"job": "pipeline_train_and_eval", "result": out},
                            )
                else:
                    out = self._run_challenger_job_wait()
                    emit_counter(
                        "job_heartbeat",
                        1,
                        component="engine.runtime.orchestrator",
                        job="auto_challenger_loop",
                    )
                    if self.AUTO_CHALLENGER_LOG:
                        log_event(
                            LOG,
                            20,
                            "auto_challenger_result",
                            component="engine.runtime.orchestrator",
                            extra={"job": "train_and_eval_challenger", "result": out},
                        )

            except Exception as e:
                emit_counter(
                    "job_failure",
                    1,
                    component="engine.runtime.orchestrator",
                    job="auto_challenger_loop",
                    extra_tags={"failure_type": "exception"},
                )
                if self.AUTO_CHALLENGER_LOG:
                    log_event(
                        LOG,
                        40,
                        "auto_challenger_error",
                        component="engine.runtime.orchestrator",
                        extra={"job": "auto_challenger_loop", "error": str(e)},
                    )

            time.sleep(max(30.0, float(self.AUTO_CHALLENGER_INTERVAL_S)))

    # ---------------------------------------------------
    # SIZE POLICY
    # ---------------------------------------------------

    def auto_size_policy_loop(self):
        time.sleep(max(0.0, float(self.AUTO_SIZE_POLICY_START_DELAY_S)))

        while True:
            try:
                if self.AUTO_SIZE_POLICY_LOG:
                    log_event(
                        LOG,
                        20,
                        "auto_size_policy_tick",
                        component="engine.runtime.orchestrator",
                        extra={"job": "train_size_policy"},
                    )

                if not self.JOBS.get("train_size_policy"):
                    if self.AUTO_SIZE_POLICY_LOG:
                        log_event(
                            LOG,
                            20,
                            "auto_size_policy_skip",
                            component="engine.runtime.orchestrator",
                            extra={"job": "train_size_policy", "reason": "not_registered"},
                        )
                elif not self._acquire_lock("train_size_policy", ttl_ms=30 * 60 * 1000):
                    if self.AUTO_SIZE_POLICY_LOG:
                        log_event(
                            LOG,
                            20,
                            "auto_size_policy_skip",
                            component="engine.runtime.orchestrator",
                            extra={"job": "train_size_policy", "reason": "locked"},
                        )
                else:
                    try:
                        res = self.JOBS.start("train_size_policy")
                        emit_counter(
                            "job_heartbeat",
                            1,
                            component="engine.runtime.orchestrator",
                            job="auto_size_policy_loop",
                        )
                        if self.AUTO_SIZE_POLICY_LOG:
                            log_event(
                                LOG,
                                20,
                                "auto_size_policy_result",
                                component="engine.runtime.orchestrator",
                                extra={"job": "train_size_policy", "result": res},
                            )

                        wait_res = self._wait_job_exit("train_size_policy")
                        if self.AUTO_SIZE_POLICY_LOG and not wait_res.get("ok"):
                            log_event(
                                LOG,
                                40,
                                "auto_size_policy_wait_error",
                                component="engine.runtime.orchestrator",
                                extra={"job": "train_size_policy", "result": wait_res},
                            )

                    finally:
                        try:
                            self._release_lock("train_size_policy")
                        except Exception as exc:
                            _warn_nonfatal(
                                "runtime_orchestrator_train_size_policy_release_lock_failed",
                                "RUNTIME_ORCHESTRATOR_TRAIN_SIZE_POLICY_RELEASE_LOCK_FAILED",
                                exc,
                                warn_key="runtime_orchestrator_train_size_policy_release_lock_failed",
                            )

            except Exception as e:
                emit_counter(
                    "job_failure",
                    1,
                    component="engine.runtime.orchestrator",
                    job="auto_size_policy_loop",
                    extra_tags={"failure_type": "exception"},
                )
                if self.AUTO_SIZE_POLICY_LOG:
                    log_event(
                        LOG,
                        40,
                        "auto_size_policy_error",
                        component="engine.runtime.orchestrator",
                        extra={"job": "auto_size_policy_loop", "error": str(e)},
                    )

            time.sleep(max(300.0, float(self.AUTO_SIZE_POLICY_INTERVAL_S)))
