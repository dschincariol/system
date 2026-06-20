"""
FILE: startup_orchestrator.py

Runtime subsystem module for `startup_orchestrator`.
"""

from __future__ import annotations

import os
import json
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.platform import default_local_db_dir, default_local_log_dir, use_local_runtime_defaults
from engine.runtime.storage import connect
from engine.runtime.health import run_preflight
from engine.runtime.db_repair import repair as repair_db
from engine.runtime.runtime_meta import meta_set
from engine.runtime.ipc import market_data_status
from engine.runtime.test_isolation import running_python_tests


class StartupOrchestrator:
    def __init__(
        self,
        *,
        jobs,
        supervisor=None,
        health_fn: Optional[Callable[[], Dict]] = None,
        log=None,
    ):
        self.jobs = jobs
        self.supervisor = supervisor
        self.health_fn = health_fn
        self.log = log or get_logger("startup_orchestrator")
        self.job_lock_stale_after_s = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
        self.preflight_timeout_s = max(1.0, float(os.environ.get("STARTUP_PREFLIGHT_TIMEOUT_S", "5")))
        self.health_timeout_s = max(1.0, float(os.environ.get("STARTUP_HEALTH_TIMEOUT_S", "5")))
        self.daemon_start_timeout_s = max(1.0, float(os.environ.get("STARTUP_DAEMON_START_TIMEOUT_S", "10")))
        self.oneshot_start_timeout_s = max(1.0, float(os.environ.get("STARTUP_ONESHOT_START_TIMEOUT_S", "15")))
        self._warned_nonfatal_keys: set[str] = set()
        self._progress_persist_lock = threading.Lock()
        self._progress_persist_inflight = False

    def _isolated_ingestion_enabled(self) -> bool:
        raw = str(os.environ.get("START_INGESTION_WITH_SERVER", "0") or "").strip().lower()
        return raw in ("1", "true", "yes", "on")

    def _warn_nonfatal(self, code: str, error: Exception, *, once_key: str | None = None, **extra) -> None:
        key = str(once_key or "")
        if key:
            if key in self._warned_nonfatal_keys:
                return
            self._warned_nonfatal_keys.add(key)
        log_failure(
            self.log,
            event=str(code).lower(),
            code=str(code),
            message=str(error),
            error=error,
            level=30,
            component="engine.runtime.startup_orchestrator",
            extra=extra or None,
            include_health=False,
            persist=False,
        )

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _step(self, steps: List[Dict], step_id: str, ok: bool, label: str, detail=None) -> Dict:
        item = {
            "id": str(step_id),
            "ok": bool(ok),
            "label": str(label),
            "detail": detail,
            "ts_ms": self._now_ms(),
        }
        steps.append(item)
        try:
            self.log.info("startup_orchestrator_step id=%s ok=%s", str(step_id), bool(ok))
        except Exception as e:
            self._warn_nonfatal(
                "STARTUP_ORCHESTRATOR_STEP_LOG_FAILED",
                e,
                once_key=f"step_log:{step_id}",
                step_id=str(step_id),
                ok=bool(ok),
            )
        self._persist_progress_async(steps)
        return item

    def _persist_progress(self, steps: List[Dict], *, final: Optional[Dict] = None) -> None:
        # Mirror startup progress into runtime_meta so API/UI readers can inspect
        # boot state without needing access to this process's memory.
        payload = {
            "ts_ms": self._now_ms(),
            "steps": list(steps or []),
        }
        if isinstance(final, dict):
            payload["final"] = dict(final)
        try:
            meta_set(
                "startup_orchestrator_progress",
                json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str),
                best_effort=True,
            )
        except Exception as e:
            self._warn_nonfatal(
                "STARTUP_ORCHESTRATOR_PROGRESS_PERSIST_FAILED",
                e,
                once_key="progress_persist",
            )

    def _persist_progress_async(self, steps: List[Dict], *, final: Optional[Dict] = None) -> None:
        # Progress persistence is best-effort and asynchronous so boot never
        # blocks on metadata writes.
        snapshot_steps = list(steps or [])
        snapshot_final = dict(final) if isinstance(final, dict) else None
        if running_python_tests():
            self._persist_progress(snapshot_steps, final=snapshot_final)
            return
        with self._progress_persist_lock:
            if self._progress_persist_inflight:
                return
            self._progress_persist_inflight = True

        def _runner() -> None:
            try:
                self._persist_progress(snapshot_steps, final=snapshot_final)
            finally:
                with self._progress_persist_lock:
                    self._progress_persist_inflight = False

        try:
            threading.Thread(
                target=_runner,
                name="startup_orchestrator_progress",
                daemon=True,
            ).start()
        except Exception as e:
            self._warn_nonfatal(
                "STARTUP_ORCHESTRATOR_PROGRESS_THREAD_START_FAILED",
                e,
                once_key="progress_thread_start",
            )

    def _db_counts(self) -> Dict:
        # Cheap readiness snapshot used during boot, not a full integrity check.
        con = connect(readonly=True)
        try:
            out = {}
            for table in [
                "symbols",
                "symbol_universe",
                "prices",
                "price_quotes",
                "price_provider_health",
                "events",
                "social_features",
                "weather_forecast_region_daily",
                "weather_alerts",
                "predictions",
                "alerts",
                "labels",
                "model_stats",
                "model_stats_regime",
                "model_metrics",
                "model_registry",
                "validation_scores",
            ]:
                try:
                    out[table] = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
                except Exception:
                    out[table] = 0

            try:
                cutoff_ms = int(time.time() * 1000 - (120 * 1000))
                out["fresh_price_provider_health"] = int(
                    con.execute(
                        """
                        SELECT COUNT(*)
                        FROM price_provider_health
                        WHERE ts_ms >= ?
                        """,
                        (cutoff_ms,),
                    ).fetchone()[0]
                    or 0
                )
            except Exception:
                out["fresh_price_provider_health"] = 0

            try:
                stale_after_ms = int(float(os.environ.get("PROCESS_EVENTS_PRICE_MAX_AGE_S", "180")) * 1000.0)
                cutoff_ms = int(time.time() * 1000 - stale_after_ms)
                fresh_row = con.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT symbol), MAX(ts_ms)
                    FROM prices
                    WHERE COALESCE(price, px) IS NOT NULL
                      AND ts_ms >= ?
                    """,
                    (cutoff_ms,),
                ).fetchone() or (0, 0, None)
                out["fresh_prices"] = int(fresh_row[0] or 0)
                out["fresh_price_symbols"] = int(fresh_row[1] or 0)
                out["last_price_ts_ms"] = int(fresh_row[2] or 0)
            except Exception:
                out["fresh_prices"] = 0
                out["fresh_price_symbols"] = 0
                out["last_price_ts_ms"] = 0

            return out
        finally:
            try:
                con.close()
            except Exception as e:
                self._warn_nonfatal("STARTUP_ORCHESTRATOR_DB_COUNTS_CLOSE_FAILED", e)

    def _event_pipeline_prices_ready(self) -> bool:
        stale_after_ms = int(float(os.environ.get("PROCESS_EVENTS_PRICE_MAX_AGE_S", "180")) * 1000.0)
        min_rows = int(os.environ.get("PROCESS_EVENTS_MIN_FRESH_PRICE_ROWS", "3"))
        min_symbol_coverage = float(os.environ.get("PROCESS_EVENTS_MIN_FRESH_SYMBOL_COVERAGE", "0.60"))
        counts = self._db_counts()

        active_symbols = int(counts.get("symbols") or 0)
        required_symbols = int(min_rows)
        if active_symbols > 0:
            required_symbols = max(required_symbols, int(max(1, active_symbols * min_symbol_coverage)))

        ipc_state = market_data_status(max_age_ms=stale_after_ms)
        ipc_ready = bool(
            ipc_state.get("ok")
            and bool(ipc_state.get("running"))
            and int(ipc_state.get("last_price_ts_ms") or 0) >= int(time.time() * 1000 - stale_after_ms)
            and int(ipc_state.get("fresh_rows") or 0) >= min_rows
            and int(ipc_state.get("fresh_symbols") or 0) >= required_symbols
        )

        db_ready = bool(
            int(counts.get("fresh_prices") or 0) >= min_rows
            and int(counts.get("fresh_price_symbols") or 0) >= required_symbols
            and int(counts.get("last_price_ts_ms") or 0) >= int(time.time() * 1000 - stale_after_ms)
        )

        return bool(ipc_ready or db_ready)

    def _call_with_timeout(self, fn: Callable[[], Dict], timeout_s: float, fallback: Dict) -> Dict:
        # Slow health or preflight calls are isolated in a thread so startup
        # remains bounded even if one dependency stalls.
        started_ts_ms = self._now_ms()
        started = time.perf_counter()
        result: Dict[str, Dict] = {"value": dict(fallback or {})}
        error: Dict[str, Optional[Exception]] = {"value": None}

        def _runner():
            try:
                value = fn()
                result["value"] = value if isinstance(value, dict) else dict(fallback or {})
            except Exception as e:
                error["value"] = e

        t = threading.Thread(target=_runner, name="startup_orchestrator_call", daemon=True)
        t.start()
        t.join(timeout=max(0.1, float(timeout_s)))

        elapsed_ms = int(round((time.perf_counter() - started) * 1000.0))
        finished_ts_ms = self._now_ms()

        if t.is_alive():
            timed_out = dict(fallback or {})
            timed_out["timed_out"] = True
            timed_out.setdefault("error", f"timeout_after_{float(timeout_s):.1f}s")
            timed_out.setdefault("started_ts_ms", int(started_ts_ms))
            timed_out["finished_ts_ms"] = int(finished_ts_ms)
            timed_out["duration_ms"] = int(elapsed_ms)
            timed_out["input_valid"] = True
            timed_out["output_valid"] = False
            return timed_out

        if error["value"] is not None:
            failed = dict(fallback or {})
            failed["error"] = str(error["value"])
            failed.setdefault("started_ts_ms", int(started_ts_ms))
            failed["finished_ts_ms"] = int(finished_ts_ms)
            failed["duration_ms"] = int(elapsed_ms)
            failed["input_valid"] = True
            failed["output_valid"] = False
            return failed

        value = result["value"]
        output_valid = isinstance(value, dict)
        out = dict(value) if output_valid else dict(fallback or {})
        if not output_valid:
            out.setdefault("error", "invalid_result_type")
            out["result_type"] = type(value).__name__
        out.setdefault("started_ts_ms", int(started_ts_ms))
        out["finished_ts_ms"] = int(finished_ts_ms)
        out.setdefault("duration_ms", int(elapsed_ms))
        out.setdefault("input_valid", True)
        out.setdefault("output_valid", bool(output_valid))
        return out

    def _safe_preflight(self) -> Dict:
        return self._call_with_timeout(
            run_preflight,
            self.preflight_timeout_s,
            {
                "ok": False,
                "notes": [f"preflight_timeout_after_{self.preflight_timeout_s:.1f}s"],
                "timed_out": True,
                "tables_ok": True,
                "health_ok": False,
            },
        )

    def _clear_stale_lock(self, job_name: str) -> Dict:
        # Only clear a oneshot lock if the owning job is not running, the PID is
        # dead, and the heartbeat is older than the configured staleness window.
        con = connect(readonly=False)
        try:
            lock_names = [str(job_name), f"job:{str(job_name)}"]
            row = None
            selected_lock_name = str(job_name)
            for candidate in lock_names:
                row = con.execute(
                    "SELECT owner, pid, heartbeat_ts_ms FROM job_locks WHERE job_name=?",
                    (str(candidate),),
                ).fetchone()
                if row:
                    selected_lock_name = str(candidate)
                    break

            if not row:
                return {"ok": True, "cleared": False, "reason": "no_lock"}

            owner = str(row[0] or "")
            pid = int(row[1] or 0)
            hb_ms = int(row[2] or 0)
            age_ms = self._now_ms() - hb_ms
            threshold_ms = int(self.job_lock_stale_after_s) * 1000

            running = False
            try:
                job = self.jobs.get(job_name)
                running = bool(job and job.to_dict().get("running"))
            except Exception:
                running = False

            pid_running = False
            try:
                from engine.runtime.storage import _pid_is_running
                pid_running = bool(_pid_is_running(pid))
            except Exception:
                pid_running = False

            if running:
                return {"ok": True, "cleared": False, "reason": "running", "age_ms": age_ms, "pid": pid}

            if pid_running:
                return {
                    "ok": True,
                    "cleared": False,
                    "reason": "pid_running",
                    "age_ms": age_ms,
                    "pid": pid,
                    "owner": owner,
                }

            if age_ms < threshold_ms:
                return {"ok": True, "cleared": False, "reason": "fresh_lock", "age_ms": age_ms}

            con.execute("DELETE FROM job_locks WHERE job_name=?", (str(selected_lock_name),))
            con.execute("DELETE FROM job_heartbeats WHERE job_name IN (?, ?)", (str(job_name), str(selected_lock_name)))
            con.commit()
            return {
                "ok": True,
                "cleared": True,
                "age_ms": age_ms,
                "pid": pid,
                "owner": owner,
                "lock_name": str(selected_lock_name),
            }
        finally:
            try:
                con.close()
            except Exception as e:
                self._warn_nonfatal("STARTUP_ORCHESTRATOR_CLEAR_STALE_LOCK_CLOSE_FAILED", e)

    def _start_daemon(self, name: str) -> Dict:
        try:
            rows = self.jobs.list_jobs() or []
            for row in rows:
                if str((row or {}).get("name") or "") != str(name):
                    continue
                if bool((row or {}).get("running")):
                    return {"ok": True, "status": "already_running", "source": "jobs_list"}
        except Exception as e:
            self._warn_nonfatal(
                "STARTUP_ORCHESTRATOR_LIST_JOBS_FAILED",
                e,
                once_key=f"list_jobs:{name}",
                job_name=str(name),
            )

        supervisor = self.supervisor
        if supervisor is not None:
            return self._call_with_timeout(
                lambda: supervisor.deterministic_start([name], include_deps=True, strict=False),
                self.daemon_start_timeout_s,
                {"ok": False, "error": f"daemon_start_timeout:{name}", "timed_out": True},
            )
        return self._call_with_timeout(
            lambda: self.jobs.start(name),
            self.daemon_start_timeout_s,
            {"ok": False, "error": f"daemon_start_timeout:{name}", "timed_out": True},
        )

    def _start_oneshot(self, name: str) -> Dict:
        try:
            self._clear_stale_lock(name)
        except Exception as e:
            self._warn_nonfatal(
                "STARTUP_ORCHESTRATOR_CLEAR_STALE_LOCK_FAILED",
                e,
                once_key=f"clear_stale_lock:{name}",
                job_name=str(name),
            )
        out = self._call_with_timeout(
            lambda: self.jobs.start(name),
            self.oneshot_start_timeout_s,
            {
                "ok": False,
                "error": f"oneshot_start_timeout:{name}",
                "job": str(name),
                "timed_out": True,
            },
        )
        if isinstance(out, dict):
            out.setdefault("job", str(name))
        return out if isinstance(out, dict) else {"ok": False, "error": f"oneshot_start_invalid:{name}", "job": str(name)}

    def _wait_until(self, fn: Callable[[], bool], timeout_s: float, sleep_s: float = 1.5) -> bool:
        deadline = time.time() + float(timeout_s)
        while time.time() < deadline:
            try:
                if fn():
                    return True
            except Exception as e:
                self._warn_nonfatal(
                    "STARTUP_ORCHESTRATOR_WAIT_UNTIL_CHECK_FAILED",
                    e,
                    once_key="wait_until_check",
                )
            time.sleep(float(sleep_s))
        return False

    def _health_snapshot(self) -> Dict:
        # Health reads use the same bounded wrapper as preflight so boot cannot
        # wedge indefinitely on an unhealthy dependency.
        if not self.health_fn:
            return {"ok": False, "error": "health_fn_missing"}
        snap = self._call_with_timeout(
            self.health_fn,
            self.health_timeout_s,
            {
                "ok": False,
                "error": f"health_timeout_after_{self.health_timeout_s:.1f}s",
                "timed_out": True,
            },
        )
        return snap if isinstance(snap, dict) else {"ok": False, "error": "invalid_health_snapshot"}

    def _runtime_health_snapshot(self, snap: Optional[Dict] = None) -> Dict:
        raw = dict(snap or self._health_snapshot() or {})
        nested = raw.get("health")
        if isinstance(nested, dict):
            return dict(nested)
        return raw

    def _backend_ready(self) -> bool:
        snap = self._runtime_health_snapshot()
        if not isinstance(snap, dict):
            return False
        raw_validation = snap.get("startup_validation")
        startup_validation = dict(raw_validation) if isinstance(raw_validation, dict) else {}
        return bool(startup_validation.get("ok"))

    def run(self, mode: str = "safe") -> Dict:
        # This sequence is intentionally linear and fail-closed so source jobs
        # come up before downstream event/label/model stages.
        # Ensure required runtime directories exist. Explicit deployment paths
        # still win; local defaults live under the ignored var/ tree.
        try:
            dirs: list[Path] = []
            data_dir = os.environ.get("TRADING_DATA") or os.environ.get("DATA_DIR")
            log_dir = os.environ.get("TRADING_LOGS") or os.environ.get("LOG_DIR")
            if not data_dir and use_local_runtime_defaults():
                data_dir = str(default_local_db_dir())
            if not log_dir and use_local_runtime_defaults():
                log_dir = str(default_local_log_dir())
            if data_dir:
                dirs.append(Path(data_dir).expanduser())
            if log_dir:
                dirs.append(Path(log_dir).expanduser())
            db_path = os.environ.get("DB_PATH")
            if db_path:
                dirs.append(Path(db_path).expanduser().parent)
            for directory in dirs:
                directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._warn_nonfatal("STARTUP_ORCHESTRATOR_MAKEDIRS_FAILED", e, once_key="makedirs")

        # ensure schema exists before any startup step
        try:
            repair_db()
        except Exception as e:
            self._warn_nonfatal("STARTUP_ORCHESTRATOR_REPAIR_DB_FAILED", e, once_key="repair_db")

        steps: List[Dict] = []

        self._step(
            steps,
            "preflight_deferred",
            True,
            "Skip duplicated preflight/bootstrap in startup orchestrator",
            {"reason": "handled_by_start_system_and_dashboard"},
        )

        counts0 = self._db_counts()
        self._step(steps, "db_counts_initial", True, "Read database state", counts0)

        self._step(
            steps,
            "daemon_status_skipped",
            True,
            "Skip synchronous daemon status query",
            {"reason": "jobs_list_may_block_during_boot"},
        )

        health_ok = self._wait_until(self._backend_ready, 45, 1.5)
        self._step(steps, "health_ready", health_ok, "Verify backend health", self._health_snapshot())
        if not health_ok:
            return {"ok": False, "mode": str(mode), "steps": steps, "counts": self._db_counts()}

        counts1 = self._db_counts()
        if int(counts1.get("symbols") or 0) <= 0 or int(counts1.get("symbol_universe") or 0) <= 0:
            res = self._start_oneshot("update_universe")
            self._step(steps, "update_universe", bool(res.get("ok", True)), "Run universe build", res)

            if not bool(res.get("ok", True)):
                retry = self._clear_stale_lock("update_universe")
                self._step(steps, "update_universe_lock_fix", bool(retry.get("ok", True)), "Clear stale universe lock", retry)
                res = self._start_oneshot("update_universe")
                self._step(steps, "update_universe_retry", bool(res.get("ok", True)), "Retry universe build", res)

            symbols_ok = self._wait_until(
                lambda: int(self._db_counts().get("symbols") or 0) > 0 and int(self._db_counts().get("symbol_universe") or 0) > 0,
                30,
                1.5,
            )
            self._step(steps, "symbols_ready", symbols_ok, "Verify symbol universe", self._db_counts())
            if not symbols_ok:
                return {"ok": False, "mode": str(mode), "steps": steps, "counts": self._db_counts()}
        else:
            self._step(steps, "symbols_ready", True, "Verify symbol universe", counts1)

        # let ingestion_runtime own streaming provider startup; only force polling fallback here
        counts_feed = self._db_counts()

        if int(counts_feed.get("fresh_price_provider_health") or 0) == 0:
            self._step(
                steps,
                "force_streaming_feed",
                True,
                "Streaming feeds are owned by ingestion_runtime",
                {"reason": "skip_direct_stream_start"},
            )

        prices_ok = self._wait_until(self._event_pipeline_prices_ready, 45, 2.0)
        self._step(
            steps,
            "prices_ready",
            prices_ok,
            "Verify fresh prices",
            {
                "counts": self._db_counts(),
                "health": self._health_snapshot(),
            },
        )

        if not prices_ok:
            counts_now = self._db_counts()

            if int(counts_now.get("fresh_price_provider_health") or 0) == 0:
                if self._isolated_ingestion_enabled():
                    fallback = {"ok": True, "reason": "isolated_ingestion_owns_poll_prices"}
                else:
                    try:
                        fallback = self._start_daemon("poll_prices")
                    except Exception as e:
                        fallback = {"ok": False, "error": str(e)}
            else:
                fallback = {"ok": True, "reason": "fresh_provider_health_exists"}

            self._step(
                steps,
                "start_poll_prices",
                bool(fallback.get("ok", True)),
                "Start fallback price feed",
                fallback,
            )

            prices_ok = self._wait_until(self._event_pipeline_prices_ready, 45, 2.0)
            self._step(
                steps,
                "prices_ready_retry",
                prices_ok,
                "Re-check fresh prices",
                {
                    "counts": self._db_counts(),
                    "health": self._health_snapshot(),
                },
            )
            if not prices_ok:
                return {"ok": False, "mode": str(mode), "steps": steps, "counts": self._db_counts()}

        pipeline_jobs = [
            ("poll_gdelt", "Poll structured news sources", None),
            ("poll_sec_filings", "Poll SEC filings", None),
            ("poll_earnings", "Poll earnings calendar", None),
            ("ingest_now", "Run ingest pipeline", lambda c: int(c.get("events") or 0) > 0),
            ("process_events", "Run event pipeline", lambda c: int(c.get("predictions") or 0) > 0 or int(c.get("alerts") or 0) > 0),
            ("label_due_events", "Run label pipeline", lambda c: int(c.get("labels") or 0) > 0),
            ("compute_drift", "Run drift pipeline", None),
            ("train_embed_models", "Train embed model", None),
            ("train_model_v2", "Train first model", lambda c: int(c.get("model_stats") or 0) > 0 or int(c.get("model_stats_regime") or 0) > 0),
            ("validate_now", "Validate first model", lambda c: int(c.get("model_metrics") or 0) > 0 or int(c.get("validation_scores") or 0) > 0),
            ("process_events", "Re-run event pipeline with trained priors", lambda c: int(c.get("predictions") or 0) > 0 or int(c.get("alerts") or 0) > 0),
        ]

        for job_name, label, ready_fn in pipeline_jobs:
            if str(job_name) == "process_events":
                prices_ready = self._wait_until(self._event_pipeline_prices_ready, 30, 2.0)
                self._step(
                    steps,
                    "process_events_prices_ready",
                    prices_ready,
                    "Verify fresh prices for event pipeline",
                    {
                        "counts": self._db_counts(),
                        "health": self._health_snapshot(),
                    },
                )
                if not prices_ready:
                    return {"ok": False, "mode": str(mode), "steps": steps, "counts": self._db_counts()}

            self._step(
                steps,
                f"{job_name}_starting",
                True,
                f"Start {job_name}",
                {"job": str(job_name)},
            )
            res = self._start_oneshot(job_name)
            self._step(steps, job_name, bool(res.get("ok", True)), label, res)

            if not bool(res.get("ok", True)):
                retry = self._clear_stale_lock(job_name)
                self._step(steps, f"{job_name}_lock_fix", bool(retry.get("ok", True)), f"Clear stale lock for {job_name}", retry)
                res = self._start_oneshot(job_name)
                self._step(steps, f"{job_name}_retry", bool(res.get("ok", True)), f"Retry {job_name}", res)
                if not bool(res.get("ok", True)):
                    return {"ok": False, "mode": str(mode), "steps": steps, "counts": self._db_counts()}

            if ready_fn is not None:
                ready = self._wait_until(lambda: bool(ready_fn(self._db_counts())), 60, 2.0)
                self._step(steps, f"{job_name}_ready", ready, f"Verify {job_name}", self._db_counts())
                if not ready:
                    return {"ok": False, "mode": str(mode), "steps": steps, "counts": self._db_counts()}


        # if prices exist ensure lifecycle transitions
        try:
            counts_life = self._db_counts()
            if int(counts_life.get("prices") or 0) > 0:
                from engine.runtime.lifecycle_state import set_state, LIVE
                set_state(LIVE, "prices_detected_startup")
        except Exception as e:
            self._warn_nonfatal(
                "STARTUP_ORCHESTRATOR_SET_LIFECYCLE_LIVE_FAILED",
                e,
                once_key="set_lifecycle_live",
            )

        # final safety: guarantee a feed exists if prices still missing
        try:
            counts_final_feed = self._db_counts()
            if int(counts_final_feed.get("prices") or 0) == 0 and not self._isolated_ingestion_enabled():
                self._start_daemon("poll_prices")
        except Exception as e:
            self._warn_nonfatal(
                "STARTUP_ORCHESTRATOR_FINAL_FEED_GUARD_FAILED",
                e,
                once_key="final_feed_guard",
            )

        final_counts = self._db_counts()
        final_health = self._health_snapshot()
        final_ok = bool(
            int(final_counts.get("symbols") or 0) > 0
            and int(final_counts.get("prices") or 0) > 0
            and int(final_counts.get("events") or 0) > 0
            and int(final_counts.get("labels") or 0) > 0
            and (
                int(final_counts.get("model_registry") or 0) > 0
                or int(final_counts.get("model_metrics") or 0) > 0
            )
        )

        self._step(
            steps,
            "final_state",
            final_ok,
            "Verify startup readiness",
            {
                "counts": final_counts,
                "health": final_health,
            },
        )

        final_preflight = {}
        try:
            final_preflight = self._safe_preflight()
        except Exception as e:
            final_preflight = {"ok": False, "error": str(e)}

        out = {
            "ok": final_ok and bool(final_preflight.get("ok", True)),
            "mode": str(mode),
            "steps": steps,
            "counts": final_counts,
            "health": final_health,
            "preflight": final_preflight,
            "ready": bool(final_ok),
        }
        self._persist_progress_async(steps, final=out)
        return out
