"""
FILE: dashboard_server.py

HTTP dashboard server and browser-asset host for the trading system.

This module binds the main UI/API process, serves `/ui/*` assets, exposes
dashboard-facing read/write endpoints, and bridges the runtime/strategy/
execution subsystems into a single operator-facing surface.
"""

# dashboard_server.py
"""
Local UI Dashboard + Job Console

UI:
  http://localhost:8000/ui/dashboard.html

APIs:
  /api/jobs
  /api/embed_model_eval
  /api/embed_conf_calib
  /api/jobs/start?name=<job>
  /api/jobs/stop?name=<job>
  /api/jobs/log?name=<job>&tail=<n>
  /api/jobs/history?name=<job>&limit=<n>
  /api/alerts
  /api/validation
  /api/health
  /api/pipeline/run
  /api/model/diagnostics
  /api/confidence_massv
"""
import json
import importlib
import os
import sys
import threading
import time
import traceback
from contextlib import nullcontext
from typing import Any, Optional, cast
from urllib import error as urllib_error, request as urllib_request

# ------------------------------------------------------------------
# Ensure imports work no matter what the working directory is.
# dashboard_server.py lives at repo root, so use this file's directory.
# ------------------------------------------------------------------
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_ENGINE_DIR = os.path.join(_BASE_DIR, "engine")

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_BASE_DIR, ".env"))
except Exception as e:
    try:
        sys.stderr.write(f"[dashboard_server] dotenv_load_failed: {type(e).__name__}: {e}\n")
        sys.stderr.flush()
    except Exception:
        traceback.print_exc()

try:
    from dashboard_config import (
        COPILOT_LLM_ENDPOINT,
        COPILOT_LLM_MODEL,
        COPILOT_LLM_TIMEOUT_S,
    )
except Exception:
    COPILOT_LLM_ENDPOINT = os.environ.get("COPILOT_LLM_ENDPOINT", "").strip()
    COPILOT_LLM_MODEL = os.environ.get("COPILOT_LLM_MODEL", "").strip()
    try:
        COPILOT_LLM_TIMEOUT_S = max(1.0, float(os.environ.get("COPILOT_LLM_TIMEOUT_S", "8.0")))
    except Exception:
        COPILOT_LLM_TIMEOUT_S = 8.0

try:
    import psutil
except Exception as e:
    psutil = None
    try:
        sys.stderr.write(f"[dashboard_server] psutil_import_failed: {type(e).__name__}: {e}\n")
        sys.stderr.flush()
    except Exception:
        traceback.print_exc()

try:
    pass
except Exception as e:
    raise RuntimeError(f"dashboard_server_config_schema_import_failed: {type(e).__name__}: {e}") from e

from engine.runtime.logging import get_logger
from engine.runtime.failure_diagnostics import log_failure
log = get_logger("dashboard")

_PRIMARY_BOOTSTRAP_DONE = str(os.environ.get("ENGINE_PRIMARY_BOOTSTRAP_DONE", "")).strip().lower() in ("1", "true", "yes", "on")


def _warn_nonfatal(code: str, error: Exception, **extra) -> None:
    log_failure(
        log,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="dashboard_server",
        include_health=False,
        persist=True,
        extra=extra or None,
    )


try:
    from services.data_source_manager import get_manager as _get_data_source_manager
except Exception as e:
    _get_data_source_manager = None
    log_failure(
        log,
        event="dashboard_server_data_source_manager_init_failed",
        code="DASHBOARD_SERVER_DATA_SOURCE_MANAGER_INIT_FAILED",
        message=str(e),
        error=e,
        level=30,
        component="dashboard_server",
        include_health=False,
        persist=True,
    )


def _safe_print(*args, **kwargs) -> None:
    kwargs.setdefault("flush", True)
    try:
        print(*args, **kwargs)
    except OSError as e:
        try:
            log.warning("dashboard_server_safe_print_failed: %s: %s", type(e).__name__, e)
        except Exception:
            traceback.print_exc()
    except Exception as e:
        log_failure(
            log,
            event="dashboard_server_safe_print_failed",
            code="DASHBOARD_SERVER_SAFE_PRINT_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="dashboard_server",
            include_health=False,
            persist=True,
            extra={"args": [str(arg) for arg in args[:10]]},
        )

def _env_int(key: str, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    raw = os.environ.get(key)
    try:
        value = int(float(str(raw if raw is not None else default).strip()))
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value

def _env_float(key: str, default: float, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    raw = os.environ.get(key)
    try:
        value = float(str(raw if raw is not None else default).strip())
    except Exception:
        value = float(default)
    if minimum is not None:
        value = max(float(minimum), value)
    if maximum is not None:
        value = min(float(maximum), value)
    return value


_safe_print("[dashboard_server] module_import_start")

try:
    from engine.runtime.event_log import append_event
except Exception as e:
    append_event = None
    log_failure(
        log,
        event="dashboard_server_event_log_import_failed",
        code="DASHBOARD_SERVER_EVENT_LOG_IMPORT_FAILED",
        message=str(e),
        error=e,
        level=30,
        component="dashboard_server",
        include_health=False,
        persist=True,
    )

from engine.api.http_transport import build_handler, run_http_server
from engine.api.http_parsing import qs as _qs
from engine.runtime.shutdown import runtime_shutdown



# ------------------------------------------------------------------
# DB HEALTH
# ------------------------------------------------------------------
from engine.runtime.storage import DB_PATH

def _db_health_snapshot():
    result = {
        "ok": True,
        "db_path": str(DB_PATH),
        "cwd": os.getcwd(),
        "base_dir": _BASE_DIR,
        "exists": DB_PATH.exists(),
        "size_bytes": 0,
        "wal_bytes": 0,
        "integrity": "unknown",
        "error": None,
        "tables": [],
        "row_counts": {},
    }
    try:
        if DB_PATH.exists():
            result["size_bytes"] = DB_PATH.stat().st_size
            wal_path = DB_PATH.with_suffix(DB_PATH.suffix + "-wal")
            if wal_path.exists():
                result["wal_bytes"] = wal_path.stat().st_size

        # Health uses a read-only connection so dashboard inspection cannot become
        # another writer competing with the runtime for SQLite locks.
        from engine.runtime.storage import connect_ro

        con = connect_ro()
        try:
            row = con.execute("PRAGMA quick_check;").fetchone()
            if row:
                result["integrity"] = str(row[0])
                if str(row[0]).lower() != "ok":
                    result["ok"] = False
        finally:
            con.close()

    except Exception as e:
        result["ok"] = False
        result["error"] = str(e)

    return result

def api_get_db_health(_parsed, _ctx=None):
    snap = _db_health_snapshot()
    ts_ms = int(time.time() * 1000)
    snap["ts"] = ts_ms
    snap["ts_ms"] = ts_ms

    try:
        from engine.api.api_system import api_get_system_state
        snap["system_snapshot"] = api_get_system_state(None, {
            "JOBS": JOBS,
            "SUPERVISOR": SUPERVISOR,
            "API_HANDLERS": API_HANDLERS,
        })
    except Exception as e:
        snap["system_snapshot_error"] = str(e)

    try:
        from engine.runtime.health import get_health_snapshot
        snap["runtime_health"] = get_health_snapshot()
    except Exception as e:
        snap["runtime_health_error"] = str(e)

    try:
        from engine.api.api_system import _recent_runtime_errors
        snap["recent_errors"] = _recent_runtime_errors(limit=10)
    except Exception:
        snap["recent_errors"] = []

    # add table + row counts
    try:
        con = _dashboard_db_connect()
        try:
            cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cur.fetchall()]
            snap["tables"] = tables
            rc = {}
            for t in tables:
                try:
                    row = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
                    rc[t] = int(row[0]) if row else 0
                except Exception:
                    rc[t] = None
            snap["row_counts"] = rc
        finally:
            con.close()
    except Exception as e:
        snap["ok"] = False
        snap["error"] = str(e)

    try:
        from engine.runtime.storage import get_db_debug_snapshot
        snap["storage_debug"] = get_db_debug_snapshot()
    except Exception as e:
        snap["storage_debug_error"] = str(e)

    return snap

# API-layer only access (no direct dev_core access from dashboard)
from engine.api.internal_access import (
    get_execution_mode as _exec_mode_get,
)
from engine.api.api_relevance import api_get_relevance_stats

_safe_print("[dashboard_server] importing runtime_bootstrap")
from engine.runtime.runtime_bootstrap import bootstrap_runtime
from engine.runtime.dashboard_runtime_boot import (
    run_post_bind_boot as _run_dashboard_post_bind_boot,
    run_post_bind_boot_safe as _run_dashboard_post_bind_boot_safe,
)
from engine.runtime.startup_gates import assert_prebind_startup_gates
_safe_print("[dashboard_server] importing startup_orchestrator")
from engine.runtime.startup_orchestrator import StartupOrchestrator

# `dashboard_server` is the HTTP/UI boundary plus post-bind orchestration hooks.
# Under `start_system`, it should cooperate with the primary supervisor instead of
# re-owning the whole runtime lifecycle.

try:
    from engine.api.api_handlers import (
        api_get_kill_switches as _api_get_kill_switches_impl,
        api_get_job_log as _api_get_job_log_impl,
        api_get_job_history as _api_get_job_history_impl,
    )

except Exception:
    _api_get_kill_switches_impl = None
    _api_get_job_log_impl = None
    _api_get_job_history_impl = None

try:
    from engine.execution.kill_switch import snapshot as _kill_switch_snapshot_impl
except Exception:
    _kill_switch_snapshot_impl = None

from engine.runtime.job_registry import (
    ALLOWED_JOBS,
    get_boot_jobs,
    get_price_feed_jobs,
    validate_runtime_architecture,
)
from engine.runtime.supervisor import RuntimeSupervisor
_safe_print("[dashboard_server] importing jobs_manager")
from engine.runtime.jobs_manager import JobManager
_safe_print("[dashboard_server] importing orchestrator")
from engine.runtime.orchestrator import RuntimeOrchestrator

from engine.runtime.health import (
    get_health_snapshot,
    get_kill_switch_snapshot_readonly,
    run_preflight,
    get_schema_audit,
)
from engine.runtime.runtime_meta import meta_get



from engine.runtime.locks import (
    acquire_lock,
    release_lock,
    write_job_history,
)

from engine.runtime.guards import (
    auto_rollback_loop,
)

from engine.runtime.lifecycle import (
    start_lifecycle_monitor,
    mark_shutdown,
)

try:
    from engine.runtime.lifecycle_state import (
        set_state,
        mark_clean_shutdown,
        mark_crash_shutdown,
        mark_dashboard_bound,
        BOOTING,
        WARMING_UP,
        DEGRADED,
        SHUTTING_DOWN,
    )
except Exception as e:
    raise RuntimeError(f"dashboard_server_lifecycle_state_import_failed: {type(e).__name__}: {e}") from e

# ------------------------------------------------------------------
# Jobs API handlers (branch-safe import)
# ------------------------------------------------------------------
# ------------------------------------------------------------------
# Jobs API handlers (current branch)
# ------------------------------------------------------------------
from engine.api.api_jobs import (
    api_get_jobs,
    api_post_pipeline_run as _api_jobs_post_pipeline_run,
    api_post_job_start,
    api_post_job_stop,
)
# ------------------------------------------------------
# CONFIG (auto-restart guards)
# ------------------------------------------------------
from engine.runtime.config import (
    AUTO_SIZE_POLICY,
    AUTO_SIZE_POLICY_INTERVAL_S,
    AUTO_SIZE_POLICY_START_DELAY_S,
    AUTO_SIZE_POLICY_LOG,

    AUTO_PIPELINE,
    AUTO_PIPELINE_INTERVAL_S,
    AUTO_PIPELINE_START_DELAY_S,
    AUTO_PIPELINE_LOG,

    AUTO_CHALLENGER,
    AUTO_CHALLENGER_INTERVAL_S,
    AUTO_CHALLENGER_START_DELAY_S,
    AUTO_CHALLENGER_LOG,
    AUTO_CHALLENGER_MIN_DRIFT,

    AUTO_PIPELINE_INCLUDE_EXECUTION,
)

# # ----------------------------------------
# # STRUCTURAL SCHEMA AUDIT (tables + columns)
# # ----------------------------------------
# # This audits *structure* only (existence + required columns).
# # Optional tables are included but do not fail ok unless required=True.
# SCHEMA_EXPECTATIONS = {
#     # core ingest
#     "prices": {"required": True, "cols": ["ts_ms", "symbol", "price"]},
#     "events": {"required": True, "cols": ["id", "ts_ms"]},
#     "labels": {"required": True, "cols": ["event_id", "label", "ts_ms"]},
#     "predictions": {"required": False, "cols": ["event_id", "ts_ms", "predicted_z"]},

#     # ops / UI
#     "alerts": {"required": True, "cols": ["id", "ts_ms", "severity", "symbol", "horizon_s"]},
#     "job_history": {"required": True, "cols": ["id", "ts_ms", "job_name", "event"]},
#     "job_locks": {"required": True, "cols": ["job_name", "owner", "pid", "acquired_ts_ms", "heartbeat_ts_ms"]},
#     "risk_state": {"required": False, "cols": ["key", "value", "updated_ts_ms"]},

#     # model + promotion
#     "model_stats_regime": {"required": False, "cols": ["symbol", "horizon_s", "regime", "n", "mean_impact_z"]},
#     "model_stats": {"required": False, "cols": ["symbol", "horizon_s", "n", "mean_impact_z"]},
#     "spillover_beta": {"required": False, "cols": ["target_symbol", "driver_symbol", "horizon_s", "n", "beta"]},
#     "model_registry": {"required": False, "cols": ["model_name", "stage", "model_kind", "model_ts_ms", "created_ts_ms"]},
#     "model_promotion_audit": {"required": False, "cols": ["ts_ms", "model_name", "key", "decision"]},
#     "validation_points": {"required": False, "cols": ["ts_ms", "model_name", "rmse", "n"]},

#     # portfolio
#     "portfolio_state": {"required": True, "cols": ["ts_ms"]},
#     "portfolio_orders": {"required": True, "cols": ["ts_ms"]},
#     "portfolio_bt_runs": {"required": True, "cols": ["id", "ts_ms", "start_ts_ms", "end_ts_ms"]},
#     "portfolio_bt_points": {"required": True, "cols": ["run_id", "ts_ms", "equity", "drawdown"]},

#     # broker/execution
#     "broker_account": {"required": True, "cols": ["ts_ms"]},
#     "broker_positions": {"required": True, "cols": ["ts_ms", "symbol"]},
#     "broker_meta": {"required": True, "cols": ["key", "value"]},
#     "broker_fills_v2": {"required": False, "cols": ["ts_ms", "symbol"]},
#     "broker_fills": {"required": False, "cols": ["ts_ms", "symbol"]},

#     # dashboard-only tables created here
#     "alert_acks": {"required": False, "cols": ["alert_id", "acked_ts_ms"]},
#     "alert_resolutions": {"required": False, "cols": ["alert_id", "resolved_ts_ms"]},
#     "equity_drift": {"required": False, "cols": ["ts_ms", "diff_equity", "diff_equity_pct", "level"]},

#     # size policy
#     "size_policy": {"required": False, "cols": ["id", "ts_ms", "lookback_days", "buckets", "method"]},
#     "size_policy_points": {"required": False, "cols": ["policy_id", "bucket_idx", "conf_lo", "conf_hi", "factor"]},
# }

def api_get_schema_audit(_parsed):
    return get_schema_audit()

def api_post_repair_schema(_parsed, _body=None, _ctx=None):
    if not _repair_schema_run:
        return {"ok": False, "error": "repair_schema_unavailable"}

    try:
        result = _repair_schema_run()

        if isinstance(result, dict):
            return result

        return {"ok": False, "error": "repair_schema_invalid_result", "result": str(result)}

    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_REPAIR_SCHEMA_FAILED", e, endpoint="api_post_repair_schema")
        return {"ok": False, "error": str(e)}

# ------------------------------------------------------
# CRIT notifications (email / webhook)
# ------------------------------------------------------
EQ_CRIT_EMAIL_TO = os.environ.get("EQ_CRIT_EMAIL_TO", "")   # comma-separated
EQ_CRIT_EMAIL_FROM = os.environ.get("EQ_CRIT_EMAIL_FROM", "alerts@localhost")
EQ_CRIT_SMTP_HOST = os.environ.get("EQ_CRIT_SMTP_HOST", "")
EQ_CRIT_SMTP_PORT = _env_int("EQ_CRIT_SMTP_PORT", 25, minimum=1, maximum=65535)

EQ_CRIT_WEBHOOK_URL = os.environ.get("EQ_CRIT_WEBHOOK_URL", "")
EQ_CRIT_WEBHOOK_TIMEOUT_S = _env_float("EQ_CRIT_WEBHOOK_TIMEOUT_S", 4.0, minimum=0.1, maximum=60.0)

# ------------------------------------------------------
# Broker ↔ Backtest equity reconciliation thresholds (NEW)
# ------------------------------------------------------
EQ_DIFF_WARN_PCT = _env_float("EQ_DIFF_WARN_PCT", 0.01, minimum=0.0, maximum=1.0)   # 1%
EQ_DIFF_CRIT_PCT = _env_float("EQ_DIFF_CRIT_PCT", 0.03, minimum=0.0, maximum=1.0)   # 3%
EQ_DIFF_WARN_ABS = _env_float("EQ_DIFF_WARN_ABS", 50.0, minimum=0.0, maximum=1_000_000.0)
EQ_DIFF_CRIT_ABS = _env_float("EQ_DIFF_CRIT_ABS", 250.0, minimum=0.0, maximum=1_000_000.0)
EQ_DIFF_ALERT_COOLDOWN_S = _env_int("EQ_DIFF_ALERT_COOLDOWN_S", 300, minimum=0, maximum=86400)

# Auto-resolve hysteresis (must be LOWER than WARN/CRIT to avoid flapping)
EQ_DIFF_RESOLVE_PCT = _env_float("EQ_DIFF_RESOLVE_PCT", 0.006, minimum=0.0, maximum=1.0)  # 0.6%
EQ_DIFF_RESOLVE_ABS = _env_float("EQ_DIFF_RESOLVE_ABS", 30.0, minimum=0.0, maximum=1_000_000.0)
EQ_DIFF_RESOLVE_LOOKBACK_S = _env_int("EQ_DIFF_RESOLVE_LOOKBACK_S", 86400, minimum=0, maximum=604800)  # 24h

# Sustained equity drift detection
EQ_DRIFT_SUSTAINED_WINDOW = _env_int("EQ_DRIFT_SUSTAINED_WINDOW", 5, minimum=1, maximum=1000)
EQ_DRIFT_SUSTAINED_MIN_WARN = _env_int("EQ_DRIFT_SUSTAINED_MIN_WARN", 3, minimum=1, maximum=EQ_DRIFT_SUSTAINED_WINDOW)
EQ_DRIFT_SUSTAINED_MIN_CRIT = _env_int("EQ_DRIFT_SUSTAINED_MIN_CRIT", 3, minimum=1, maximum=EQ_DRIFT_SUSTAINED_WINDOW)

# Job history retention
JOB_HISTORY_MAX_ROWS = _env_int("JOB_HISTORY_MAX_ROWS", 5000, minimum=100, maximum=1000000)

# ------------------------------------------------------
# RELEVANCE STATS CONFIG (NEW)
# ------------------------------------------------------

ENABLE_RELEVANCE_STATS = os.environ.get("ENABLE_RELEVANCE_STATS", "1") == "1"
RELEVANCE_STATS_CACHE_TTL_S = _env_int("RELEVANCE_STATS_CACHE_TTL_S", 60, minimum=1, maximum=3600)
RELEVANCE_STATS_TIMEOUT_S = _env_float("RELEVANCE_STATS_TIMEOUT_S", 5.0, minimum=0.1, maximum=60.0)

# ---------------------------------------------------
# RUNTIME ORCHESTRATION
# ---------------------------------------------------
JOBS: Optional[JobManager] = None
SUPERVISOR: Optional[RuntimeSupervisor] = None
ORCHESTRATOR: Optional[RuntimeOrchestrator] = None


def _get_kill_switches_snapshot() -> dict:
    return dict(get_kill_switch_snapshot_readonly() or {})


def _ensure_runtime_orchestration():
    global JOBS, SUPERVISOR, ORCHESTRATOR

    if JOBS is None:
        JOBS = JobManager(
            preflight_fn=run_preflight,
            get_kill_switches_fn=_get_kill_switches_snapshot,
            get_execution_mode_fn=lambda: (_exec_mode_get() or {}),
        )

    if SUPERVISOR is None:
        SUPERVISOR = RuntimeSupervisor(jobs=JOBS)

    if ORCHESTRATOR is None:
        ORCHESTRATOR = RuntimeOrchestrator(
            jobs=JOBS,
            acquire_lock=acquire_lock,
            release_lock=release_lock,
            auto_pipeline_include_execution=AUTO_PIPELINE_INCLUDE_EXECUTION,
            auto_pipeline_log=AUTO_PIPELINE_LOG,
            auto_pipeline_interval_s=AUTO_PIPELINE_INTERVAL_S,
            auto_pipeline_start_delay_s=AUTO_PIPELINE_START_DELAY_S,
            auto_challenger_log=AUTO_CHALLENGER_LOG,
            auto_challenger_interval_s=AUTO_CHALLENGER_INTERVAL_S,
            auto_challenger_start_delay_s=AUTO_CHALLENGER_START_DELAY_S,
            auto_challenger_min_drift=AUTO_CHALLENGER_MIN_DRIFT,
            auto_size_policy_log=AUTO_SIZE_POLICY_LOG,
            auto_size_policy_interval_s=AUTO_SIZE_POLICY_INTERVAL_S,
            auto_size_policy_start_delay_s=AUTO_SIZE_POLICY_START_DELAY_S,
            get_kill_switches=_get_kill_switches_snapshot,
            get_execution_mode=lambda: (_exec_mode_get() or {}),
        )

    return JOBS, SUPERVISOR, ORCHESTRATOR


def _jobs_manager() -> JobManager:
    jobs, _, _ = _ensure_runtime_orchestration()
    return jobs


def _runtime_supervisor() -> RuntimeSupervisor:
    _, supervisor, _ = _ensure_runtime_orchestration()
    return supervisor


def _runtime_orchestrator() -> RuntimeOrchestrator:
    _, _, orchestrator = _ensure_runtime_orchestration()
    return orchestrator


def _qs_value(parsed, key: str, default: str = "") -> str:
    return str(_qs(parsed, key, default) or default)


def _qs_dict(parsed) -> dict[str, str]:
    raw = _qs(parsed)
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    return {}


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}

_AUTO_PIPELINE_THREAD_STARTED = False
_AUTO_CHALLENGER_THREAD_STARTED = False
_AUTO_SIZE_POLICY_THREAD_STARTED = False
_STARTUP_ORCHESTRATOR_THREAD_STARTED = False

# -------------            -- ------------------------------------------------------
# SERVER LIFECYCLE (status + graceful shutdown)
# -------------            -- ------------------------------------------------------
SERVER_SHUTDOWN_TOKEN = os.environ.get("SERVER_SHUTDOWN_TOKEN", "").strip()

# Optional API token for any mutating endpoints (start/stop jobs, pipeline run, training mode, etc).
# - If empty: mutating endpoints are allowed ONLY from localhost.
# - If set: token is required for ALL mutating endpoints (local + remote).
DASHBOARD_API_TOKEN = os.environ.get("DASHBOARD_API_TOKEN", "").strip()

SERVER_STARTED_AT_MS = int(time.time() * 1000)
_default_crash_path = os.path.join(_BASE_DIR, "logs", "crash_analytics.jsonl")
CRASH_LOG_PATH = os.environ.get("CRASH_LOG_PATH", _default_crash_path)
_SERVER_STOP_EVENT = threading.Event()
_SERVER_BACKGROUND_THREADS = []
_SERVER_SHUTDOWN_LOCK = threading.Lock()
_SERVER_SHUTDOWN_DONE = False


def _start_background_thread(name, target, args=()):
    def _runner():
        try:
            target(*args)
        except Exception:
            log.exception("background thread crashed: %s", name)
            raise

    thread = threading.Thread(
        target=_runner,
        name=str(name),
        daemon=True,
    )
    thread.start()
    _SERVER_BACKGROUND_THREADS.append(thread)
    return thread

# Ensure absolute path (systemd-safe)
if not os.path.isabs(CRASH_LOG_PATH):
    CRASH_LOG_PATH = os.path.join(_BASE_DIR, CRASH_LOG_PATH)

# Ensure logs directory exists (production safe)
try:
    os.makedirs(os.path.dirname(CRASH_LOG_PATH), exist_ok=True)
except Exception as e:
    log_failure(
        log,
        event="dashboard_server_crash_log_dir_create_failed",
        code="DASHBOARD_SERVER_CRASH_LOG_DIR_CREATE_FAILED",
        message=str(e),
        error=e,
        level=30,
        component="dashboard_server",
        include_health=False,
        persist=True,
        extra={"crash_log_path": str(CRASH_LOG_PATH)},
    )

def _write_crash_analytics(exit_code, err: str = "", tb: str = ""):
    try:
        os.makedirs(os.path.dirname(CRASH_LOG_PATH), exist_ok=True)
    except Exception as e:
        log_failure(
            log,
            event="dashboard_server_crash_analytics_dir_create_failed",
            code="DASHBOARD_SERVER_CRASH_ANALYTICS_DIR_CREATE_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="dashboard_server",
            include_health=False,
            persist=True,
            extra={"crash_log_path": str(CRASH_LOG_PATH)},
        )

    try:
        payload = {
            "ts_ms": int(time.time() * 1000),
            "exit_code": int(exit_code),
            "uptime_s": int((int(time.time() * 1000) - SERVER_STARTED_AT_MS) / 1000),
            "error": str(err or ""),
            "traceback": str(tb or ""),
        }
        with open(CRASH_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception as e:
        log_failure(
            log,
            event="dashboard_server_write_crash_analytics_failed",
            code="DASHBOARD_SERVER_WRITE_CRASH_ANALYTICS_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="dashboard_server",
            include_health=False,
            persist=True,
            extra={"exit_code": int(exit_code), "error": str(err or "")},
        )


# HTTP bind
def _require_env(key: str) -> str:
    value = str(os.environ.get(key, "") or "").strip()
    if not value:
        raise RuntimeError(f"missing required env: {key}")
    return value


_db_path_env = _require_env("DB_PATH")
if not os.path.isabs(os.path.abspath(_db_path_env)):
    os.environ["DB_PATH"] = os.path.abspath(_db_path_env)

host = str(os.environ.get("DASHBOARD_HOST", "127.0.0.1") or "").strip() or "127.0.0.1"

port = _env_int("DASHBOARD_PORT", _env_int("PORT", 8000, minimum=1, maximum=65535), minimum=1, maximum=65535)

# ---------------------------------------------------
# SUPERVISOR AUTO BOOT (deterministic, ENV-gated)
# ---------------------------------------------------
def _env_bool(key: str, default: bool = False) -> bool:
    v = os.environ.get(key)
    if v is None:
        return bool(default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

# Default ON for production-safe deterministic boot
# Force deterministic auto-boot in shadow/live
_mode = _require_env("ENGINE_MODE").strip().lower()

try:
    from engine.runtime.live_trading_preflight import assert_dashboard_security_config
    _DASHBOARD_SECURITY_PREFLIGHT = assert_dashboard_security_config(
        engine_mode=_mode,
        dashboard_host=host,
        dashboard_api_token=DASHBOARD_API_TOKEN,
        live_confirm=os.environ.get("LIVE_TRADING_CONFIRM", ""),
    )
except Exception as e:
    raise RuntimeError(str(e)) from e

# Deterministic boot policy:
# - live/shadow: always auto-boot
# - safe/dev/paper: allow env control
if _mode in ("shadow", "live"):
    AUTO_BOOT_DAEMONS = True
elif _mode in ("safe", "dev", "development", "paper"):
    AUTO_BOOT_DAEMONS = _env_bool("AUTO_BOOT_DAEMONS", True)
else:
    raise RuntimeError(f"invalid ENGINE_MODE: {_mode}")

AUTO_BOOT_TARGETS = [
    x.strip() for x in os.environ.get("AUTO_BOOT_TARGETS", "").split(",")
    if x.strip()
]

if "stream_prices_polygon_ws" in AUTO_BOOT_TARGETS and not str(os.environ.get("POLYGON_API_KEY", "") or "").strip():
    raise RuntimeError("POLYGON_API_KEY is required when AUTO_BOOT_TARGETS includes stream_prices_polygon_ws")

# ---------------------------------------------------
# OPERATOR UI COMPATIBILITY HELPERS
# ---------------------------------------------------
_OPERATOR_PRICE_JOB_CANDIDATES = tuple(
    name for name in get_price_feed_jobs()
    if name in ALLOWED_JOBS
)

_OPERATOR_LOG_PATH = os.environ.get(
    "OPERATOR_LOG_PATH",
    os.path.join(_BASE_DIR, "logs", "runtime.log"),
)

_OPERATOR_STDERR_LOG_PATH = os.environ.get(
    "OPERATOR_STDERR_LOG_PATH",
    os.path.join(_BASE_DIR, "boot", "engine_stderr.log"),
)

_OPERATOR_CONSOLE_PREFIX = "/operator"
_OPERATOR_UI_HTML_PATH = os.path.join(_BASE_DIR, "boot", "operator_ui.html")
_OPERATOR_PROXY_TIMEOUT_S = _env_float("OPERATOR_PROXY_TIMEOUT_S", 6.0, minimum=0.5, maximum=60.0)
_OPERATOR_PROXY_MAX_BODY_BYTES = _env_int(
    "OPERATOR_PROXY_MAX_BODY_BYTES",
    1048576,
    minimum=1024,
    maximum=10485760,
)


def _operator_sidecar_host_port() -> tuple[str, int]:
    host = str(os.environ.get("OPERATOR_BIND_HOST", "127.0.0.1") or "127.0.0.1").strip()
    if host in ("", "0.0.0.0", "::", "[::]"):
        host = "127.0.0.1"
    port = _env_int("OPERATOR_PORT", 4001, minimum=1, maximum=65535)
    return host, int(port)


def _operator_sidecar_base_url() -> str:
    host, port = _operator_sidecar_host_port()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{int(port)}"


def _operator_sidecar_ws_url() -> str:
    return _operator_sidecar_base_url().replace("http://", "ws://", 1) + "/ws/operator"


def _operator_sidecar_status_payload(timeout_s: Optional[float] = None) -> dict:
    timeout = float(timeout_s if timeout_s is not None else min(_OPERATOR_PROXY_TIMEOUT_S, 2.0))
    base_url = _operator_sidecar_base_url()
    started = time.time()
    payload = {
        "ok": False,
        "reachable": False,
        "service": "node_operator_sidecar",
        "base_url": base_url,
        "direct_url": base_url + "/",
        "same_origin_url": "/operator/",
        "compatibility_routes": ["/operator/", "/operator_ui.html"],
        "http_proxy_prefix": "/operator/api/",
        "action": "Start or restart the Node operator sidecar if reachable is false.",
        "websocket": {
            "proxy_enabled": False,
            "direct_url": _operator_sidecar_ws_url(),
            "deferred_reason": (
                "Python SimpleHTTPRequestHandler does not safely implement "
                "WebSocket upgrade proxying; the bridged UI uses HTTP proxy "
                "paths plus the existing direct sidecar WebSocket when available."
            ),
        },
        "meta": {"status": 200},
        "ts_ms": int(time.time() * 1000),
    }

    try:
        req = urllib_request.Request(
            base_url + "/api/operator/ping",
            headers={"Accept": "application/json"},
            method="GET",
        )
        with urllib_request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 0) or resp.getcode() or 0)
            raw = resp.read(8192)
        elapsed_ms = int((time.time() - started) * 1000)
        body = {}
        if raw:
            try:
                body = json.loads(raw.decode("utf-8", errors="replace"))
            except Exception:
                body = {"raw": raw.decode("utf-8", errors="replace")[:500]}
        reachable = 200 <= status < 400
        payload.update({
            "ok": bool(reachable),
            "reachable": bool(reachable),
            "status": status,
            "latency_ms": elapsed_ms,
            "ping": body,
            "error": None if reachable else f"http_{status}",
        })
    except Exception as e:
        payload.update({
            "ok": False,
            "reachable": False,
            "status": 0,
            "latency_ms": int((time.time() - started) * 1000),
            "error": "operator_sidecar_unreachable",
            "detail": f"{type(e).__name__}: {e}",
        })

    return payload


def api_get_operator_sidecar_status(_parsed=None, _ctx=None):
    return _operator_sidecar_status_payload()


def _wrap_operator_console_routes(BaseHandler):
    """Add same-origin operator-console compatibility routes to a handler.

    The Node operator service remains the owner of the operator API/control
    plane. This wrapper only exposes `/operator/` from the dashboard origin and
    proxies `/operator/api/*` HTTP calls to the existing sidecar.
    """

    class OperatorConsoleCompatHandler(BaseHandler):
        def _operator_send_bytes(self, status: int, body: bytes, content_type: str, headers: Optional[dict] = None):
            try:
                self.send_response(int(status))
                self.send_header("Content-Type", str(content_type or "application/octet-stream"))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Operator-Console-Bridge", "1")
                for name, value in dict(headers or {}).items():
                    lower = str(name).lower()
                    if lower in ("connection", "transfer-encoding", "content-length"):
                        continue
                    self.send_header(str(name), str(value))
                self.send_header("Content-Length", str(len(body or b"")))
                self.end_headers()
                if body:
                    self.wfile.write(body)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                return

        def _operator_send_json(self, status: int, obj: dict, headers: Optional[dict] = None):
            body = json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")
            self._operator_send_bytes(status, body, "application/json; charset=utf-8", headers)

        def _operator_serve_ui(self):
            try:
                with open(_OPERATOR_UI_HTML_PATH, "rb") as f:
                    body = f.read()
            except Exception as e:
                self._operator_send_json(404, {
                    "ok": False,
                    "error": "operator_ui_missing",
                    "path": _OPERATOR_UI_HTML_PATH,
                    "detail": f"{type(e).__name__}: {e}",
                })
                return True

            self._operator_send_bytes(200, body, "text/html; charset=utf-8")
            return True

        def _operator_proxy_target(self, parsed):
            path = str(parsed.path or "")
            if path == "/operator/api/operator_summary":
                target_path = "/api/operator_summary"
            elif path.startswith("/operator/api/"):
                target_path = path[len("/operator"):]
            else:
                return ""

            target = _operator_sidecar_base_url() + target_path
            if parsed.query:
                target += "?" + str(parsed.query)
            return target

        def _operator_proxy_http(self, parsed):
            target = self._operator_proxy_target(parsed)
            if not target:
                return False

            method = str(self.command or "GET").upper()
            data = None
            if method not in ("GET", "HEAD"):
                try:
                    content_length = int(self.headers.get("Content-Length") or "0")
                except Exception:
                    content_length = 0
                if content_length > _OPERATOR_PROXY_MAX_BODY_BYTES:
                    self._operator_send_json(413, {
                        "ok": False,
                        "error": "operator_proxy_body_too_large",
                        "max_bytes": _OPERATOR_PROXY_MAX_BODY_BYTES,
                    })
                    return True
                data = self.rfile.read(max(0, content_length)) if content_length > 0 else b""

            headers = {"Accept": self.headers.get("Accept") or "application/json"}
            content_type = self.headers.get("Content-Type")
            if content_type:
                headers["Content-Type"] = content_type

            try:
                req = urllib_request.Request(target, data=data, headers=headers, method=method)
                with urllib_request.urlopen(req, timeout=_OPERATOR_PROXY_TIMEOUT_S) as resp:
                    status = int(getattr(resp, "status", 0) or resp.getcode() or 200)
                    body = resp.read()
                    response_headers = dict(resp.headers.items())
            except urllib_error.HTTPError as e:
                status = int(getattr(e, "code", 502) or 502)
                body = e.read()
                response_headers = dict(e.headers.items()) if getattr(e, "headers", None) else {}
            except Exception as e:
                self._operator_send_json(503, {
                    "ok": False,
                    "error": "operator_sidecar_unavailable",
                    "detail": f"{type(e).__name__}: {e}",
                    "action": (
                        "Start or restart the Node operator sidecar and retry the "
                        "same-origin /operator console action."
                    ),
                    "sidecar": _operator_sidecar_status_payload(timeout_s=0.75),
                })
                return True

            content_type = response_headers.pop("Content-Type", "application/json; charset=utf-8")
            self._operator_send_bytes(status, body or b"", content_type, response_headers)
            return True

        def _handle_operator_console_compat(self):
            from urllib.parse import urlparse

            parsed = urlparse(self.path)
            path = str(parsed.path or "")

            if path in ("/operator", "/operator/", "/operator_ui.html"):
                return self._operator_serve_ui()

            if path == "/operator/status":
                self._operator_send_json(200, _operator_sidecar_status_payload())
                return True

            if path == "/operator/ws/operator":
                self._operator_send_json(426, {
                    "ok": False,
                    "error": "websocket_proxy_deferred",
                    "sidecar_ws_url": _operator_sidecar_ws_url(),
                    "action": "Use the direct sidecar WebSocket URL until dashboard WebSocket proxying is implemented.",
                    "detail": (
                        "Use the existing Node operator WebSocket directly; "
                        "the Python dashboard bridge proxies HTTP only."
                    ),
                }, headers={"Upgrade": "websocket"})
                return True

            return self._operator_proxy_http(parsed)

        def do_GET(self):
            if self._handle_operator_console_compat():
                return
            return super().do_GET()

        def do_POST(self):
            if self._handle_operator_console_compat():
                return
            return super().do_POST()

    OperatorConsoleCompatHandler.__name__ = f"OperatorConsoleCompat{getattr(BaseHandler, '__name__', 'Handler')}"
    return OperatorConsoleCompatHandler


def _shutdown_runtime_once(reason: str = "") -> None:
    global _SERVER_SHUTDOWN_DONE

    with _SERVER_SHUTDOWN_LOCK:
        if _SERVER_SHUTDOWN_DONE:
            return
        _SERVER_SHUTDOWN_DONE = True

    _SERVER_STOP_EVENT.set()

    try:
        from engine.runtime.storage_pool import storage_acquire_timeout_override

        timeout_ctx = storage_acquire_timeout_override(_dashboard_storage_request_timeout_s())
    except Exception:
        timeout_ctx = nullcontext()

    try:
        with timeout_ctx:
            mark_shutdown()
    except Exception as e:
        log.exception("dashboard_server_mark_shutdown_failed: %s", e)

    try:
        from engine.runtime.storage_pool import storage_acquire_timeout_override

        timeout_ctx = storage_acquire_timeout_override(_dashboard_storage_request_timeout_s())
    except Exception:
        timeout_ctx = nullcontext()

    try:
        with timeout_ctx:
            if mark_clean_shutdown:
                mark_clean_shutdown()
            elif set_state:
                set_state(SHUTTING_DOWN, reason or "clean_shutdown")
    except Exception as e:
        log.exception("dashboard_server_mark_clean_shutdown_failed: %s", e)

    try:
        runtime_shutdown(JOBS=JOBS, SUPERVISOR=SUPERVISOR)
    except Exception as e:
        log.error("dashboard_server runtime_shutdown error: %s", e)


def _request_httpd_shutdown(reason: str = "") -> None:
    def _shutdown_httpd() -> None:
        try:
            if _HTTPD:
                _HTTPD.shutdown()
        except Exception as e:
            log.exception("dashboard_server_httpd_shutdown_failed: %s", e)

    _start_background_thread(
        f"httpd_shutdown_{str(reason or 'signal')[:32]}",
        _shutdown_httpd,
        (),
    )


def _tail_text_file(path: str, limit_bytes: int = 65536) -> str:
    try:
        if not path or not os.path.exists(path):
            return ""
        with open(path, "rb") as f:
            try:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                start = max(0, size - int(limit_bytes))
                f.seek(start, os.SEEK_SET)
            except Exception as e:
                log_failure(
                    log,
                    event="dashboard_server_tail_text_seek_failed",
                    code="DASHBOARD_SERVER_TAIL_TEXT_SEEK_FAILED",
                    message=str(e),
                    error=e,
                    level=30,
                    component="dashboard_server",
                    include_health=False,
                    persist=True,
                    extra={"path": str(path), "limit_bytes": int(limit_bytes)},
                )
                f.seek(0, os.SEEK_SET)
            data = f.read()
        return data.decode("utf-8", errors="replace")
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_LOG_READ_FAILED", e, path=str(path), limit_bytes=int(limit_bytes))
        return f"[log_read_error] {e}"

def _operator_running_jobs():
    try:
        rows = _jobs_manager().list_jobs() or []
        return [r for r in rows if bool(r.get("running"))]
    except Exception as e:
        log_failure(
            log,
            event="dashboard_server_operator_running_jobs_failed",
            code="DASHBOARD_SERVER_OPERATOR_RUNNING_JOBS_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="dashboard_server",
            include_health=False,
            persist=True,
        )
        return []

def _operator_price_running():
    try:
        from engine.runtime.ipc import market_data_status

        snap = market_data_status(
            max_age_ms=int(
                float(os.environ.get("HEALTH_PRICES_MAX_AGE_S", "120")) * 1000.0
            )
        )
        if snap.get("ok") and snap.get("running"):
            return True
    except Exception as e:
        log_failure(
            log,
            event="dashboard_server_operator_price_running_market_data_failed",
            code="DASHBOARD_SERVER_OPERATOR_PRICE_RUNNING_MARKET_DATA_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="dashboard_server",
            include_health=False,
            persist=True,
        )

    try:
        for row in _operator_running_jobs():
            name = str(row.get("name") or "")
            group = str(row.get("group") or "")
            if name in _OPERATOR_PRICE_JOB_CANDIDATES:
                return True
            if group == "price_feed":
                return True
    except Exception as e:
        log_failure(
            log,
            event="dashboard_server_operator_price_running_jobs_scan_failed",
            code="DASHBOARD_SERVER_OPERATOR_PRICE_RUNNING_JOBS_SCAN_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="dashboard_server",
            include_health=False,
            persist=True,
        )
    return False

def _operator_status_payload():
    rows = []
    try:
        rows = _jobs_manager().list_jobs() or []
    except Exception as e:
        log_failure(
            log,
            event="dashboard_server_operator_status_jobs_list_failed",
            code="DASHBOARD_SERVER_OPERATOR_STATUS_JOBS_LIST_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="dashboard_server",
            include_health=False,
            persist=True,
        )
        rows = []

    running = [r for r in rows if bool(r.get("running"))]
    mode = str(os.environ.get("ENGINE_MODE", "safe") or "safe")

    health = {}
    full_snapshot = {}
    try:
        full_snapshot = api_get_system_state(None, {
            "JOBS": JOBS,
            "SUPERVISOR": SUPERVISOR,
            "API_HANDLERS": API_HANDLERS,
        }) or {}
    except Exception:
        full_snapshot = {}

    try:
        health = api_get_health(None, {
            "JOBS": JOBS,
            "SUPERVISOR": SUPERVISOR,
            "API_HANDLERS": API_HANDLERS,
        }) or {}
    except Exception:
        health = {}

    health_ok = bool(isinstance(health, dict) and health.get("ok"))
    ingestion = dict((health or {}).get("ingestion") or {})
    services = dict((health or {}).get("services") or {})
    engine_service = dict((services or {}).get("engine") or {})
    engine_running = bool(engine_service.get("running"))
    ingestion_ok = bool(ingestion.get("ok"))
    price_feed_running = bool(ingestion.get("running")) or bool(ingestion.get("job_visible"))

    if running and health_ok and engine_running and ingestion_ok and price_feed_running:
        status = "RUNNING"
    elif running or engine_running or price_feed_running:
        status = "DEGRADED"
    elif health_ok:
        status = "STARTING"
    else:
        status = "STOPPED"

    last_exit_code = None
    restart_attempts = 0
    for row in rows:
        restart_attempts += int(row.get("restart_count") or 0)
        if last_exit_code is None and row.get("exit_code") is not None:
            last_exit_code = row.get("exit_code")

    return {
        "ok": bool(health_ok and running and engine_running and ingestion_ok and price_feed_running),
        "status": status,
        "lastMode": mode,
        "lastExitCode": last_exit_code,
        "restartAttempts": restart_attempts,
        "lastHealthyAt": (int(health.get("ts_ms") or 0) if health_ok else None),
        "runningJobs": [str(r.get("name") or "") for r in running],
        "snapshot": full_snapshot,
    }

def _operator_preflight_steps():
    try:
        p = run_preflight() or {}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_OPERATOR_PREFLIGHT_FAILED", e)
        return {
            "ok": False,
            "checks": [
                {
                    "id": "preflight",
                    "label": "Preflight exception",
                    "ok": False,
                    "details": str(e),
                }
            ],
            "notes": [str(e)],
        }

    notes = list(p.get("notes") or [])
    checks = []

    checks.append({
        "id": "preflight",
        "label": "Preflight",
        "ok": bool(p.get("ok")),
        "details": "\n".join(str(x) for x in notes) if notes else "Preflight completed.",
    })

    return {
        "ok": bool(p.get("ok")),
        "checks": checks,
        "notes": notes,
        "raw": p,
    }

def _operator_start_targets(mode: str):
    static_targets = list(AUTO_BOOT_TARGETS) if AUTO_BOOT_TARGETS else []

    if "ingestion_runtime" in static_targets:
        static_targets = [
            name
            for name in static_targets
            if name not in _OPERATOR_PRICE_JOB_CANDIDATES
        ]
        static_targets.append("ingestion_runtime")

    static_targets = list(dict.fromkeys([x for x in static_targets if x]))
    if static_targets:
        return static_targets

    targets = []

    if "ingestion_runtime" in ALLOWED_JOBS:
        targets.append("ingestion_runtime")
        return targets

    for candidate in _OPERATOR_PRICE_JOB_CANDIDATES:
        if candidate in ALLOWED_JOBS:
            targets.append(candidate)

    return targets


def _dashboard_auto_boot_static_targets(raw_targets, *, ingestion_enabled: bool) -> list[str]:
    static_targets = list(dict.fromkeys([str(x).strip() for x in (raw_targets or []) if str(x).strip()]))
    if ingestion_enabled:
        blocked = set(_OPERATOR_PRICE_JOB_CANDIDATES)
        blocked.add("ingestion_runtime")
        return [name for name in static_targets if name not in blocked]
    if "ingestion_runtime" in static_targets:
        static_targets = [
            name
            for name in static_targets
            if name not in _OPERATOR_PRICE_JOB_CANDIDATES
        ]
        static_targets.append("ingestion_runtime")
    return static_targets


def _dashboard_auto_boot_price_candidates(*, ingestion_enabled: bool) -> list[str]:
    if ingestion_enabled:
        return []

    preferred_price = [
        x.strip()
        for x in os.environ.get(
            "AUTO_BOOT_PRICE_TARGET",
            "stream_prices_polygon_ws,poll_prices",
        ).split(",")
        if x.strip()
    ]

    candidates = [p for p in preferred_price if p in ALLOWED_JOBS]

    def _script_exists(job_name):
        script_rel = ""
        try:
            spec = ALLOWED_JOBS.get(job_name)
            if not isinstance(spec, (tuple, list)) or not spec:
                return False
            script_rel = str(spec[0] or "").strip()
            if not script_rel:
                return False
            script_abs = os.path.abspath(os.path.join(_BASE_DIR, script_rel))
            if os.path.exists(script_abs):
                return True
            engine_abs = os.path.abspath(os.path.join(_ENGINE_DIR, script_rel))
            return os.path.exists(engine_abs)
        except Exception as e:
            _warn_nonfatal(
                "DASHBOARD_SERVER_SCRIPT_CANDIDATE_RESOLVE_FAILED",
                e,
                script_rel=str(script_rel),
            )
            return False

    candidates = [p for p in candidates if _script_exists(p)]
    if candidates:
        return candidates
    return [
        p for p in _OPERATOR_PRICE_JOB_CANDIDATES
        if p in ALLOWED_JOBS and _script_exists(p)
    ]

def _operator_start_impl(mode: str):
    mode = str(mode or os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
    os.environ["ENGINE_MODE"] = mode

    pre = _operator_preflight_steps()
    steps = list(pre.get("checks") or [])

    if not pre.get("ok"):
        return {
            "ok": False,
            "mode": mode,
            "steps": steps,
            "error": "preflight_failed",
        }

    targets = _operator_start_targets(mode)
    if not targets:
        steps.append({
            "id": "targets",
            "label": "Resolve boot targets",
            "ok": False,
            "details": "No operator boot targets found in ALLOWED_JOBS.",
        })
        return {
            "ok": False,
            "mode": mode,
            "steps": steps,
            "error": "no_boot_targets",
        }

    try:
        result = _runtime_supervisor().deterministic_start(
            targets,
            include_deps=True,
            strict=False,
        )
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_DETERMINISTIC_START_FAILED", e, endpoint="api_post_operator_start")
        steps.append({
            "id": "start",
            "label": "Deterministic start",
            "ok": False,
            "details": str(e),
        })
        return {
            "ok": False,
            "mode": mode,
            "steps": steps,
            "error": str(e),
        }

    steps.append({
        "id": "start",
        "label": "Deterministic start",
        "ok": bool(result.get("ok")),
        "details": json.dumps(result, default=str),
    })

    return {
        "ok": bool(result.get("ok")),
        "mode": mode,
        "steps": steps,
        "result": result,
    }

# If no explicit targets provided, default to price feed WS only when
# ingestion is not being launched as a sibling process.
if AUTO_BOOT_DAEMONS:
    if not AUTO_BOOT_TARGETS:
        if "ingestion_runtime" in ALLOWED_JOBS:
            AUTO_BOOT_TARGETS = ["ingestion_runtime"]
        elif str(os.environ.get("START_INGESTION_WITH_SERVER", "1")).strip().lower() not in ("1", "true", "yes", "on"):
            AUTO_BOOT_TARGETS = list(_OPERATOR_PRICE_JOB_CANDIDATES[:1])

_HTTPD = None  # set in run_server()
_DASHBOARD_HTTP_BOUND = False
_LOCAL_META_CACHE: dict[str, Any] = {}

def _snapshot_json_default(value):
    try:
        return str(value)
    except Exception as e:
        _warn_nonfatal(
            "DASHBOARD_SERVER_SNAPSHOT_JSON_DEFAULT_FAILED",
            e,
            value_type=type(value).__name__,
        )
        snapshot_repr = repr(value)
        return snapshot_repr

def _meta_set_json(key: str, payload, *, best_effort: bool = False) -> None:
    _LOCAL_META_CACHE[str(key)] = payload
    if not bool(globals().get("_DASHBOARD_HTTP_BOUND", False)):
        return
    ready_fn = globals().get("_dashboard_storage_known_ready")
    if callable(ready_fn) and not bool(ready_fn()):
        return
    try:
        from engine.runtime.runtime_meta import meta_set
        try:
            from engine.runtime.storage_pool import storage_acquire_timeout_override

            timeout_fn = globals().get("_dashboard_storage_request_timeout_s")
            timeout_s = timeout_fn() if callable(timeout_fn) else 0.5
            timeout_ctx = storage_acquire_timeout_override(timeout_s)
        except Exception:
            timeout_ctx = nullcontext()
        with timeout_ctx:
            meta_set(
                str(key),
                json.dumps(payload, default=_snapshot_json_default, separators=(",", ":"), sort_keys=True),
                best_effort=bool(best_effort),
            )
    except Exception as e:
        log.warning("dashboard_server_meta_set_json_failed key=%s error=%s", str(key), e)

def _meta_get_json(key: str, default):
    if str(key) in _LOCAL_META_CACHE:
        value = _LOCAL_META_CACHE.get(str(key))
        return value if isinstance(value, type(default)) else default
    if not bool(globals().get("_DASHBOARD_HTTP_BOUND", False)):
        return default
    ready_fn = globals().get("_dashboard_storage_known_ready")
    if callable(ready_fn) and not bool(ready_fn()):
        return default
    try:
        from engine.runtime.runtime_meta import meta_get
        try:
            from engine.runtime.storage_pool import storage_acquire_timeout_override

            timeout_fn = globals().get("_dashboard_storage_request_timeout_s")
            timeout_s = timeout_fn() if callable(timeout_fn) else 0.5
            timeout_ctx = storage_acquire_timeout_override(timeout_s)
        except Exception:
            timeout_ctx = nullcontext()
        with timeout_ctx:
            raw = str(meta_get(str(key), "") or "").strip()
        if not raw:
            return default
        value = json.loads(raw)
        _LOCAL_META_CACHE[str(key)] = value
        return value if isinstance(value, type(default)) else default
    except Exception as e:
        if _is_dashboard_storage_unavailable_error(e):
            log.warning("dashboard_server_meta_get_json_storage_unavailable key=%s error=%s", str(key), e)
        else:
            _warn_nonfatal("DASHBOARD_SERVER_META_GET_JSON_FAILED", e, key=str(key))
        return default

def _update_startup_trace(phase: str, *, status: str = "started", detail: str = "", extra: dict | None = None) -> None:
    trace = _meta_get_json("startup_trace", {
        "phase": "BOOT",
        "phases": [],
        "first_failure": {},
        "import_errors": [],
        "ts_ms": 0,
    })
    now_ms = int(time.time() * 1000)
    trace["phase"] = str(phase)
    trace.setdefault("phases", []).append({
        "phase": str(phase),
        "status": str(status),
        "detail": str(detail or ""),
        "ts_ms": now_ms,
        "extra": dict(extra or {}),
    })
    trace["ts_ms"] = now_ms
    _meta_set_json("startup_trace", trace, best_effort=True)

def _record_startup_failure(phase: str, exc: BaseException, *, module: str = "", file_path: str = "") -> None:
    trace = _meta_get_json("startup_trace", {
        "phase": "BOOT",
        "phases": [],
        "first_failure": {},
        "import_errors": [],
        "ts_ms": 0,
    })
    if trace.get("first_failure"):
        return
    tb = traceback.extract_tb(exc.__traceback__) if getattr(exc, "__traceback__", None) else []
    leaf = tb[-1] if tb else None
    trace["phase"] = str(phase)
    trace["first_failure"] = {
        "phase": str(phase),
        "type": type(exc).__name__,
        "error": str(exc),
        "module": str(module or (leaf.name if leaf else "")),
        "file": str(file_path or (leaf.filename if leaf else "")),
        "line": int((leaf.lineno if leaf else 0) or 0),
        "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-12000:],
        "ts_ms": int(time.time() * 1000),
    }
    trace["ts_ms"] = int(time.time() * 1000)
    _meta_set_json("startup_trace", trace, best_effort=True)

_BOOT_DIAGNOSTICS = {
    "ok": True,
    "auto_repair_attempted": False,
    "storage": {
        "checked": False,
        "ok": None,
        "status": "unknown",
        "storage": "postgres",
        "backend": "postgres",
        "degraded": False,
        "detail": "storage_not_checked",
        "ts_ms": 0,
    },
    "api_dependencies": {
        "started": False,
        "ok": None,
        "detail": "",
        "missing_route_handlers": [],
        "ts_ms": 0,
    },
    "prebind_gates": None,
    "startup_preflight": None,
    "startup_repair": None,
    "startup_orchestrator": None,
    "post_bind_boot": {
        "started": False,
        "ok": None,
        "error": "",
        "ts_ms": 0,
    },
    "ts_ms": 0,
}
# ---------------------------------------------------
# UI CONSOLE LIFECYCLE ENDPOINTS
# ---------------------------------------------------
def api_get_training_status(_parsed, _ctx=None):
    """
    UI calls /api/training_status.
    Source of truth is engine.training_guard.get_training_status() when available.
    """
    try:
        from engine.training_guard import get_training_status as _get_training_status
        out = _get_training_status()
        if isinstance(out, dict):
            allowed = bool(out.get("allowed"))
            mode = str(out.get("mode") or "")
            out.setdefault("ok", bool(allowed and mode == "enabled"))
            return out
        return {"ok": False, "error": "invalid_training_status", "raw": str(out)}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_TRAINING_STATUS_FAILED", e, endpoint="api_get_training_status")
        return {"ok": False, "error": str(e)}


_COPILOT_MAX_QUESTION_CHARS = 600
_COPILOT_MAX_HISTORY_ITEMS = 6
_COPILOT_MAX_HISTORY_CHARS = 320
_COPILOT_MAX_ACTIONS = 4
_COPILOT_CONTEXT_JSON_CHARS = 18000


def _copilot_text(value: Any, max_chars: int = 240) -> str:
    text = " ".join(str(value if value is not None else "").split())
    if len(text) <= max_chars:
        return text
    return text[: max(0, int(max_chars) - 1)].rstrip() + "…"


def _copilot_persona(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    return "fund_manager" if token == "fund_manager" else "operations"


def _copilot_view(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in {
        "overview",
        "operate",
        "explain",
        "analyze",
        "data",
        "positions",
        "execution",
    } else "overview"


def _copilot_compact(value: Any, *, depth: int = 0, max_depth: int = 3) -> Any:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return round(float(value), 6)
    if isinstance(value, str):
        return _copilot_text(value, 240)
    if depth >= max_depth:
        return _copilot_text(value, 240)
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for idx, (k, v) in enumerate(value.items()):
            if idx >= 18:
                break
            out[str(k)] = _copilot_compact(v, depth=depth + 1, max_depth=max_depth)
        return out
    if isinstance(value, (list, tuple, set)):
        return [
            _copilot_compact(v, depth=depth + 1, max_depth=max_depth)
            for v in list(value)[:8]
        ]
    return _copilot_text(value, 240)


def _copilot_history(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for item in value[-_COPILOT_MAX_HISTORY_ITEMS:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        if role not in ("user", "assistant"):
            continue
        text = _copilot_text(item.get("text") or item.get("content") or "", _COPILOT_MAX_HISTORY_CHARS)
        if not text:
            continue
        out.append({"role": role, "text": text})
    return out


def _copilot_active_alert(body: dict[str, Any], ctx=None) -> dict[str, Any]:
    raw = _json_dict(body.get("active_incident") or body.get("active_alert"))
    alert_id = 0
    try:
        alert_id = int(raw.get("id") or 0)
    except Exception:
        alert_id = 0

    if alert_id > 0:
        try:
            fetched = api_get_alert_by_id({"id": str(alert_id)}, ctx)
            if isinstance(fetched, dict) and fetched.get("ok"):
                return _copilot_compact(fetched, max_depth=3)
        except Exception as e:
            _warn_nonfatal(
                "DASHBOARD_SERVER_COPILOT_ALERT_CONTEXT_FAILED",
                e,
                endpoint="api_post_copilot_ask",
                alert_id=alert_id,
            )

    return _copilot_compact(raw, max_depth=3) if raw else {}


def _copilot_call_read(handler_name: str, ctx=None, parsed=None) -> dict[str, Any]:
    fn = API_HANDLERS.get(handler_name)
    if not callable(fn):
        return {"ok": False, "error": f"{handler_name}_unavailable"}
    try:
        out = fn(parsed, ctx)
        if isinstance(out, dict):
            return out
        return {"ok": True, "data": _copilot_text(out, 400)}
    except Exception as e:
        _warn_nonfatal(
            "DASHBOARD_SERVER_COPILOT_READ_FAILED",
            e,
            endpoint="api_post_copilot_ask",
            handler=handler_name,
        )
        return {"ok": False, "error": str(e)}


def _copilot_server_context(ctx=None) -> dict[str, Any]:
    raw = {
        "health": _copilot_call_read("api_get_health", ctx),
        "readiness": _copilot_call_read("api_get_readiness", ctx),
        "system_state": _copilot_call_read("api_get_system_state", ctx),
        "execution_barrier": _copilot_call_read("api_get_execution_barrier", ctx),
        "market_stress": _copilot_call_read("api_get_market_stress", ctx),
        "training_status": _copilot_call_read("api_get_training_status", ctx),
        "promotion_status": _copilot_call_read("api_get_promotion_status", ctx),
    }
    return _copilot_compact(raw, max_depth=3)


def _copilot_suggested_actions(
    *,
    active_view: str,
    active_alert: dict[str, Any],
    server_context: dict[str, Any],
) -> list[str]:
    suggestions: list[str] = []

    if active_alert:
        suggestions.append(
            "Review the incident drawer facts, severity, rule id, expected move, and confidence before comparing it with related alerts."
        )

    system_state = _json_dict(server_context.get("system_state"))
    system_token = _copilot_text(system_state.get("state") or "", 40).upper()
    if system_token and system_token not in {"OK", "READY", "RUNNING", "HEALTHY"}:
        suggestions.append("Review the top-level health summary and operator summary together to isolate which subsystem is degraded.")

    readiness = _json_dict(server_context.get("readiness"))
    if readiness.get("ready") is False or readiness.get("ok") is False:
        suggestions.append("Review readiness and startup status details before trusting the current dashboard state as fully ready.")

    barrier = _json_dict(server_context.get("execution_barrier"))
    if barrier.get("allowed") is False:
        suggestions.append("Review the execution barrier card and operator execution pill to confirm why execution is currently blocked or degraded.")

    by_view = {
        "overview": "Review the health summary, decision bar, and active alerts together before drilling into any single panel.",
        "operate": "Review operator summary, readiness, and execution barrier together to confirm the current operating condition.",
        "explain": "Review the incident drawer, Why modal, and decision bar so the alert is read in the same context the dashboard uses.",
        "analyze": "Review model metrics, portfolio backtest, and execution metrics together before drawing performance conclusions.",
        "data": "Review the system-status header, ingestion details, and market-data freshness signals for the current degradation source.",
        "positions": "Review portfolio state, broker snapshot, and equity reconciliation details together before trusting current exposure.",
        "execution": "Review execution confidence buckets, execution advisory details, and the barrier state together.",
    }
    suggestions.append(by_view.get(active_view, by_view["overview"]))

    out: list[str] = []
    seen = set()
    for item in suggestions:
        text = _copilot_text(item, 180)
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= _COPILOT_MAX_ACTIONS:
            break
    return out


def _copilot_prompt_payload(
    *,
    question: str,
    persona: str,
    active_view: str,
    active_alert: dict[str, Any],
    visible_state: dict[str, Any],
    history: list[dict[str, str]],
    server_context: dict[str, Any],
) -> dict[str, Any]:
    return {
        "persona": persona,
        "active_view": active_view,
        "question": question,
        "history": history,
        "active_alert": active_alert,
        "visible_state": visible_state,
        "server_context": server_context,
    }


def _copilot_extract_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = [_copilot_extract_text(item) for item in value[:8]]
        text = "\n".join(part for part in parts if part)
        return text
    if isinstance(value, dict):
        for key in ("answer", "output_text", "text", "content"):
            text = _copilot_extract_text(value.get(key))
            if text:
                return text
        choices = value.get("choices")
        if isinstance(choices, list) and choices:
            text = _copilot_extract_text(choices[0])
            if text:
                return text
        output = value.get("output")
        if isinstance(output, list) and output:
            text = _copilot_extract_text(output[0])
            if text:
                return text
        message = value.get("message")
        if isinstance(message, dict):
            text = _copilot_extract_text(message)
            if text:
                return text
    return ""


def _copilot_normalize_answer(value: Any) -> str:
    raw = _copilot_extract_text(value)
    lines = [line.strip() for line in str(raw or "").splitlines()]
    text = "\n".join(line for line in lines if line)
    if len(text) <= 1600:
        return text
    return text[:1599].rstrip() + "…"


def _copilot_ask_llm(prompt_payload: dict[str, Any]) -> str:
    endpoint = str(COPILOT_LLM_ENDPOINT or "").strip()
    if not endpoint:
        return ""

    system_prompt = (
        "You are a read-only copilot for a production trading dashboard. "
        "Explain only from the provided context. "
        "Do not suggest executing trades, restarting services, toggling kill switches, "
        "editing configuration, acknowledging alerts, or resolving alerts. "
        "Keep the answer concise, plain text only, and operator-safe. "
        "If the context is incomplete, say what is missing."
    )
    payload = {
        "model": str(COPILOT_LLM_MODEL or "").strip(),
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(prompt_payload, separators=(",", ":"), default=str)[:_COPILOT_CONTEXT_JSON_CHARS],
            },
        ],
        "input": prompt_payload,
        "question": prompt_payload.get("question", ""),
    }
    if not payload["model"]:
        payload.pop("model", None)

    req = urllib_request.Request(
        endpoint,
        data=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=float(COPILOT_LLM_TIMEOUT_S or 8.0)) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            raw = str(e)
        _warn_nonfatal(
            "DASHBOARD_SERVER_COPILOT_HTTP_ERROR",
            e,
            endpoint="api_post_copilot_ask",
            upstream=endpoint,
            body=_copilot_text(raw, 400),
        )
        return ""
    except Exception as e:
        _warn_nonfatal(
            "DASHBOARD_SERVER_COPILOT_UPSTREAM_FAILED",
            e,
            endpoint="api_post_copilot_ask",
            upstream=endpoint,
        )
        return ""

    try:
        parsed = json.loads(raw) if raw else {}
    except Exception:
        parsed = {"answer": raw}
    return _copilot_normalize_answer(parsed)


def api_post_copilot_ask(_parsed, body=None, _ctx=None):
    """
    Read-only dashboard copilot.

    The endpoint accepts lightweight UI context and returns text-only guidance.
    It must never trigger runtime mutations or return executable actions.
    """
    payload = body if isinstance(body, dict) else {}
    if payload.get("__body_error__"):
        return {
            "ok": False,
            "answer": "Copilot request could not be read safely. Try a shorter question.",
            "suggested_actions": [
                "Review the health summary, active alerts, and execution barrier directly in the dashboard.",
            ],
            "error": str(payload.get("__body_error__")),
        }

    question = _copilot_text(payload.get("question") or "", _COPILOT_MAX_QUESTION_CHARS)
    active_view = _copilot_view(payload.get("active_view"))
    persona = _copilot_persona(payload.get("persona"))
    visible_state = _copilot_compact(_json_dict(payload.get("visible_state")), max_depth=3)
    history = _copilot_history(payload.get("history"))
    active_alert = _copilot_active_alert(payload, _ctx)
    server_context = _copilot_server_context(_ctx)
    suggested_actions = _copilot_suggested_actions(
        active_view=active_view,
        active_alert=active_alert,
        server_context=server_context,
    )

    if not question:
        return {
            "ok": False,
            "answer": "Ask a question such as “why is health degraded?” or “explain this alert.”",
            "suggested_actions": suggested_actions,
        }

    if not str(COPILOT_LLM_ENDPOINT or "").strip():
        return {
            "ok": False,
            "answer": "AI copilot is unavailable because no read-only model endpoint is configured for this dashboard.",
            "suggested_actions": suggested_actions,
        }

    prompt_payload = _copilot_prompt_payload(
        question=question,
        persona=persona,
        active_view=active_view,
        active_alert=active_alert,
        visible_state=visible_state,
        history=history,
        server_context=server_context,
    )
    answer = _copilot_ask_llm(prompt_payload)
    if not answer:
        return {
            "ok": False,
            "answer": "AI copilot is temporarily unavailable. Review the suggested dashboard panels directly.",
            "suggested_actions": suggested_actions,
        }

    return {
        "ok": True,
        "answer": answer,
        "suggested_actions": suggested_actions,
    }


def _publish_boot_diagnostics() -> None:
    _BOOT_DIAGNOSTICS["ts_ms"] = int(time.time() * 1000)
    _LOCAL_META_CACHE["dashboard_boot_diagnostics"] = dict(_BOOT_DIAGNOSTICS)
    if not bool(globals().get("_DASHBOARD_HTTP_BOUND", False)):
        return
    ready_fn = globals().get("_dashboard_storage_known_ready")
    if callable(ready_fn) and not bool(ready_fn()):
        return
    try:
        from engine.runtime.runtime_meta import meta_set
        try:
            from engine.runtime.storage_pool import storage_acquire_timeout_override

            timeout_ctx = storage_acquire_timeout_override(_dashboard_storage_request_timeout_s())
        except Exception:
            timeout_ctx = nullcontext()
        with timeout_ctx:
            meta_set(
                "dashboard_boot_diagnostics",
                json.dumps(_BOOT_DIAGNOSTICS, separators=(",", ":"), sort_keys=True, default=str),
                best_effort=True,
            )
    except Exception as e:
        log.warning("dashboard_server_boot_diagnostics_publish_failed: %s", e)


_STORAGE_REQUIRED_ROUTE_PATHS = frozenset(
    {
        "/api/db/health",
        "/api/operator/db_schema",
        "/api/data_sources",
        "/api/data_sources/logs",
        "/api/ui/metrics",
        "/api/replay/day",
        "/api/model/performance_divergence",
        "/api/operator/status",
        "/api/market/candles",
        "/api/market/stream",
        "/api/portfolio",
        "/api/portfolio/backtest",
        "/api/prices",
        "/api/trades",
        "/api/broker",
        "/api/pnl",
        "/api/pnl/summary",
        "/api/risk/summary",
        "/api/risk/portfolio",
        "/api/execution_metrics",
        "/api/execution/metrics",
        "/api/execution/stats",
        "/api/execution_overlays",
        "/api/execution/overlays",
        "/api/promotion/audit",
        "/api/promotion_audit",
        "/api/causal/scores",
        "/api/model_metrics",
        "/api/model/metrics",
        "/api/models/status",
        "/api/terminal/watchlist",
        "/api/terminal/snapshot",
        "/api/terminal/positions",
        "/api/terminal/orders",
        "/api/terminal/fills",
        "/api/terminal/equity",
        "/api/terminal/markers",
    }
)


def _dashboard_storage_request_timeout_s() -> float:
    try:
        return max(0.05, float(os.environ.get("DASHBOARD_STORAGE_REQUEST_TIMEOUT_S", "0.5") or 0.5))
    except Exception:
        return 0.5


def _dashboard_storage_startup_timeout_s() -> float:
    try:
        return max(0.05, float(os.environ.get("DASHBOARD_STORAGE_STARTUP_TIMEOUT_S", "2.0") or 2.0))
    except Exception:
        return 2.0


def _dashboard_strict_runtime_storage_required() -> bool:
    try:
        from engine.runtime.config_schema import get_runtime_safety_context

        safety = get_runtime_safety_context()
        return bool((safety or {}).get("strict_runtime"))
    except Exception:
        env = str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "").strip().lower()
        mode = str(os.environ.get("ENGINE_MODE") or "").strip().lower()
        supervised = str(os.environ.get("ENGINE_SUPERVISED") or "").strip().lower() in ("1", "true", "yes", "on")
        return bool(supervised or env in ("prod", "production") or mode in ("live", "shadow", "paper"))


def _dashboard_storage_readiness_probe(*, force: bool = False, startup: bool = False) -> dict[str, Any]:
    try:
        from engine.runtime.storage_pool import probe_storage_readiness

        snapshot = probe_storage_readiness(
            timeout_s=(
                _dashboard_storage_startup_timeout_s()
                if startup
                else _dashboard_storage_request_timeout_s()
            ),
            max_age_s=0.0 if force else 2.0,
            force=force,
        )
    except Exception as error:
        snapshot = {
            "checked": True,
            "ok": False,
            "status": "unavailable",
            "storage": "postgres",
            "backend": "postgres",
            "degraded": True,
            "detail": "postgres_readiness_probe_exception",
            "error": f"{type(error).__name__}: {error}",
            "error_type": type(error).__name__,
            "required": _dashboard_strict_runtime_storage_required(),
            "ts_ms": int(time.time() * 1000),
        }

    _BOOT_DIAGNOSTICS["storage"] = dict(snapshot or {})
    _publish_boot_diagnostics()
    return dict(snapshot or {})


def _raise_if_dashboard_storage_required_unavailable() -> None:
    if not _dashboard_strict_runtime_storage_required():
        return
    snapshot = _dashboard_storage_readiness_probe(force=True, startup=True)
    if bool(snapshot.get("ok")):
        return
    detail = str(snapshot.get("error") or snapshot.get("detail") or "runtime_storage_unavailable")
    raise RuntimeError(f"runtime_storage_unavailable: postgres required but not ready: {detail}")


def _dashboard_storage_unavailable_payload(endpoint: str, error: BaseException | None = None) -> dict[str, Any]:
    try:
        from engine.runtime.storage_pool import storage_readiness_snapshot, storage_unavailable_payload

        return storage_unavailable_payload(
            endpoint=endpoint,
            error=error,
            readiness=storage_readiness_snapshot(),
        )
    except Exception:
        return {
            "ok": False,
            "error": "storage_unavailable",
            "detail": f"{type(error).__name__}: {error}" if error is not None else "runtime_storage_unavailable",
            "endpoint": str(endpoint or ""),
            "meta": {"status": 503, "retryable": True, "ts_ms": int(time.time() * 1000)},
        }


def _is_dashboard_storage_unavailable_error(error: BaseException) -> bool:
    try:
        from engine.runtime.storage_pool import is_storage_acquisition_error

        return bool(is_storage_acquisition_error(error))
    except Exception:
        return "couldn't get a connection" in str(error or "").lower()


def _dashboard_storage_known_unavailable() -> bool:
    try:
        from engine.runtime.storage_pool import storage_readiness_snapshot

        snapshot = storage_readiness_snapshot()
        return bool(snapshot.get("checked") and snapshot.get("ok") is False)
    except Exception:
        return False


def _dashboard_storage_known_ready() -> bool:
    try:
        from engine.runtime.storage_pool import storage_readiness_snapshot

        snapshot = storage_readiness_snapshot()
        return bool(snapshot.get("checked") and snapshot.get("ok") is True)
    except Exception:
        return False


def _prewarm_health_cache(handler_ctx) -> None:
    try:
        api_get_health(None, handler_ctx)
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_HEALTH_CACHE_PREWARM_FAILED", e)


def _run_preflight_bounded(timeout_s=None) -> dict:
    timeout_s = max(1.0, float(timeout_s or os.environ.get("STARTUP_PREFLIGHT_TIMEOUT_S", "5")))
    result = {
        "value": {
            "ok": False,
            "notes": [f"preflight_timeout_after_{timeout_s:.1f}s"],
            "timed_out": True,
            "tables_ok": True,
            "health_ok": False,
        }
    }
    error: dict[str, Exception | None] = {"value": None}

    def _runner():
        try:
            value = run_preflight()
            if isinstance(value, dict):
                result["value"] = value
            else:
                result["value"] = {"ok": False, "error": "invalid_preflight_result", "raw": str(value)}
        except Exception as e:
            error["value"] = e

    t = threading.Thread(target=_runner, name="dashboard_preflight", daemon=True)
    t.start()
    t.join(timeout=timeout_s)
    if t.is_alive():
        return dict(result["value"])
    if error["value"] is not None:
        return {"ok": False, "error": str(error["value"]), "notes": [str(error["value"])]}
    return dict(result["value"] or {})


def _is_timeout_only_preflight(result: dict | None) -> bool:
    if not isinstance(result, dict) or bool(result.get("ok")):
        return False
    if not bool(result.get("timed_out")) or not bool(result.get("tables_ok")):
        return False
    if str(result.get("error") or "").strip():
        return False
    notes = result.get("notes") or []
    if not isinstance(notes, list) or not notes:
        return False
    meaningful_notes = [str(note).strip() for note in notes if str(note).strip()]
    if not meaningful_notes:
        return False
    return all(note.startswith("preflight_timeout_after_") for note in meaningful_notes)

def api_get_server_status(_parsed, _ctx=None):
    """Return basic process metadata for the dashboard HTTP server.

    Parameters
    ----------
    _parsed : Any
        Accepted for handler signature compatibility and ignored.
    _ctx : Any, optional
        Unused request context placeholder.

    Returns
    -------
    dict
        Mapping with ``ok``, ``ts_ms``, ``uptime_s``, ``host``, and ``port``.
        ``ts_ms`` is epoch milliseconds and ``uptime_s`` is integer wall-clock
        uptime in seconds.
    """
    now_ms = int(time.time() * 1000)
    uptime_s = int((now_ms - SERVER_STARTED_AT_MS) / 1000)
    return {
        "ok": True,
        "ts_ms": now_ms,
        "uptime_s": uptime_s,
        "host": host,
        "port": port,
    }

def api_post_server_shutdown(_parsed, _body=None, _ctx=None):
    """Request an in-process dashboard shutdown.

    Parameters
    ----------
    _parsed : Any
        Accepted for handler signature compatibility and ignored.
    _body : Any, optional
        Unused request body placeholder.
    _ctx : Any, optional
        Unused request context placeholder.

    Returns
    -------
    dict
        ``{"ok": True}`` when shutdown was requested successfully. Failures
        return ``{"ok": False, "error": "server_shutdown_failed", ...}``.

    Side Effects
    ------------
    Triggers runtime shutdown hooks and calls ``HTTPServer.shutdown()`` on the
    active dashboard server instance.
    """
    _shutdown_runtime_once("api_server_shutdown")
    try:
        if _HTTPD:
            _HTTPD.shutdown()
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_SHUTDOWN_FAILED", e, endpoint="api_post_server_shutdown")
        return {"ok": False, "error": "server_shutdown_failed", "detail": str(e)}
    return {"ok": True}

# ------------------------------------------------------
# DIAGNOSTICS / METRICS

# -------------            -- ------------------------------------------------------
def _normalize_explain_json(val) -> str:
    """
    Ensure explain_json is always a JSON string.
    - If None/empty: returns "{}"
    - If already JSON text: returns as-is
    - If bytes: decodes utf-8
    - Otherwise: returns JSON-encoded wrapper
    """
    if val is None:
        return "{}"
    try:
        if isinstance(val, (bytes, bytearray)):
            val = val.decode("utf-8", errors="replace")
    except Exception as e:
        log_failure(
            log,
            event="dashboard_server_snapshot_decode_failed",
            code="DASHBOARD_SERVER_SNAPSHOT_DECODE_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="dashboard_server",
            include_health=False,
            persist=True,
        )

    s = str(val).strip()
    if not s:
        return "{}"

    # if it parses, return original text (keeps exact content)
    try:
        json.loads(s)
        return s
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_EXPLAIN_JSON_PARSE_FAILED", e)
        return json.dumps({"raw": s})

# -------------            -- ------------------------------------------------------
# HTTP HANDLER
# -------------            -- ------------------------------------------------------


# ------------------------------
# ROUTE SPECS (split into files)
# ------------------------------
try:
    from engine.api.api_system import ROUTE_SPECS_SYSTEM
except Exception:
    ROUTE_SPECS_SYSTEM = []

ROUTE_SPECS_SYSTEM = list(ROUTE_SPECS_SYSTEM or [])

try:
    from engine.api.api_jobs import ROUTE_SPECS_JOBS
except Exception:
    ROUTE_SPECS_JOBS = []

try:
    from engine.api.api_ops import ROUTE_SPECS_OPS
except Exception:
    ROUTE_SPECS_OPS = []

try:
    from engine.api.api_market import ROUTE_SPECS_MARKET
except Exception:
    ROUTE_SPECS_MARKET = []

try:
    from engine.api.api_replay import ROUTE_SPECS_REPLAY
except Exception:
    ROUTE_SPECS_REPLAY = []

try:
    from routes.data_sources_routes import ROUTE_SPECS_DATA_SOURCES
except Exception:
    ROUTE_SPECS_DATA_SOURCES = []

try:
    from engine.api.api_ui_metrics import (
        ROUTE_SPECS_UI_METRICS,
        build_ui_metrics_snapshot,
    )
except Exception:
    ROUTE_SPECS_UI_METRICS = []
    build_ui_metrics_snapshot = None

# (optional) terminal route modules are consolidated via ROUTE_SPECS_TERMINAL_ALL below

# -------------------------------------------------------------------
# FALLBACK ROUTES (UI hard-dep)
# If ROUTE_SPECS_* failed to import or are incomplete, the UI will 404.
# These map paths -> handler keys in API_HANDLERS below.
# -------------------------------------------------------------------
_FALLBACK_ROUTE_SPECS = [
    {"method": "GET",  "path": "/api/db/health",        "handler": "api_get_db_health"},

    # OPERATOR (required by ui/dashboard.js snapshot bundle)
    {"method": "GET", "path": "/api/operator_summary", "handler": "api_get_operator_summary"},
    {"method": "GET",  "path": "/api/operator/sidecar_status",  "handler": "api_get_operator_sidecar_status"},
    {"method": "GET",  "path": "/api/operator/status",            "handler": "api_get_operator_status"},
    {"method": "GET",  "path": "/api/operator/bootstrap",         "handler": "api_get_operator_bootstrap_status"},
    {"method": "GET",  "path": "/api/operator/bootstrapStatus",   "handler": "api_get_operator_bootstrap_status"},
    {"method": "GET",  "path": "/api/operator/readiness",         "handler": "api_get_readiness"},
    {"method": "GET",  "path": "/api/operator/health",            "handler": "api_get_health"},
    {"method": "GET",  "path": "/api/operator/logs",              "handler": "api_get_operator_logs"},
    {"method": "GET",  "path": "/api/operator/stderr_tail",       "handler": "api_get_operator_stderr_tail"},
    {"method": "GET",  "path": "/api/operator/db_schema",         "handler": "api_get_schema_audit"},
    {"method": "GET",  "path": "/api/operator/snapshot",          "handler": "api_get_support_snapshot"},
    {"method": "GET",  "path": "/api/operator/market_data",       "handler": "api_get_operator_market_data"},
    {"method": "GET",  "path": "/api/operator/strategy_decisions","handler": "api_get_operator_strategy_decisions"},
    {"method": "GET",  "path": "/api/operator/preflight",         "handler": "api_get_operator_preflight"},

    {"method": "POST", "path": "/api/operator/start",             "handler": "api_post_operator_start"},
    {"method": "POST", "path": "/api/operator/bootstrap",         "handler": "api_post_operator_bootstrap"},
    {"method": "POST", "path": "/api/operator/stop",              "handler": "api_post_operator_stop"},
    {"method": "POST", "path": "/api/operator/restart",           "handler": "api_post_operator_restart"},
    {"method": "POST", "path": "/api/operator/restart_engine",    "handler": "api_post_operator_restart"},
    {"method": "POST", "path": "/api/operator/restart_feeds",     "handler": "api_post_operator_restart_feeds"},
    {"method": "POST", "path": "/api/operator/emergency_stop",    "handler": "api_post_operator_emergency_stop"},
    {"method": "POST", "path": "/api/operator/autofix",           "handler": "api_post_operator_autofix"},
    {"method": "POST", "path": "/api/operator/clearLastError",    "handler": "api_post_operator_clear_last_error"},
    {"method": "GET",  "path": "/api/operator/institutionalCheck","handler": "api_get_operator_institutional_check"},


    {"method": "GET",  "path": "/api/training_status",       "handler": "api_get_training_status"},
    {"method": "GET",  "path": "/api/pnl",                   "handler": "api_get_pnl"},
    {"method": "POST", "path": "/api/repair_schema",         "handler": "api_post_repair_schema"},
    {"method": "GET",  "path": "/api/status",                "handler": "api_get_status"},
    {"method": "GET",  "path": "/api/liveness",             "handler": "api_get_liveness"},
    {"method": "GET",  "path": "/api/system/config",         "handler": "api_get_runtime_config"},
    {"method": "GET",  "path": "/api/supervisor/status",     "handler": "api_get_supervisor_status"},
    {"method": "GET",  "path": "/api/ingestion/status",      "handler": "api_get_ingestion_status"},
    {"method": "GET",  "path": "/api/risk/portfolio",        "handler": "api_get_portfolio_risk"},
    {"method": "GET",  "path": "/api/market/session",        "handler": "api_get_market_session"},
    {"method": "GET",  "path": "/api/pnl/summary",           "handler": "api_get_pnl_summary"},
    {"method": "GET",  "path": "/api/risk/summary",          "handler": "api_get_risk_summary"},
    {"method": "GET",  "path": "/api/models/status",         "handler": "api_get_models_status"},
    {"method": "POST", "path": "/api/models/promote",        "handler": "api_post_models_promote"},

    # JOBS
    {"method": "GET",  "path": "/api/jobs",               "handler": "api_get_jobs"},
    {"method": "POST", "path": "/api/jobs/start",         "handler": "api_post_job_start"},
    {"method": "POST", "path": "/api/jobs/stop",          "handler": "api_post_job_stop"},
    {"method": "GET",  "path": "/api/jobs/log",           "handler": "api_get_job_log"},
    {"method": "GET",  "path": "/api/jobs/history",       "handler": "api_get_job_history"},
    {"method": "POST", "path": "/api/pipeline/run",       "handler": "api_post_pipeline_run"},

    # OPS
    {"method": "GET", "path": "/api/alerts",                          "handler": "api_get_alerts"},
    {"method": "GET", "path": "/api/notifications/status",            "handler": "api_get_notifications_status"},
    {"method": "POST", "path": "/api/notifications/test",             "handler": "api_post_notifications_test"},
    {"method": "GET", "path": "/api/alerts/timeline",                 "handler": "api_get_alerts"},
    {"method": "GET", "path": "/api/feeds",                           "handler": "api_get_feeds"},
    {"method": "GET", "path": "/api/validation",                      "handler": "api_get_validation"},
    {"method": "GET", "path": "/api/model/diagnostics",               "handler": "api_get_model_diagnostics"},
    {"method": "GET", "path": "/api/model_registry",                  "handler": "api_get_model_registry"},
    {"method": "GET", "path": "/api/model/registry",                  "handler": "api_get_model_registry"},
    {"method": "GET", "path": "/api/embed_model_eval",                "handler": "api_get_embed_model_eval"},
    {"method": "GET", "path": "/api/embed_conf_calib",                "handler": "api_get_embed_conf_calib"},
    {"method": "GET", "path": "/api/temporal_eval",                   "handler": "api_get_temporal_eval"},
    {"method": "GET", "path": "/api/temporal/eval",                   "handler": "api_get_temporal_eval"},
    {"method": "GET", "path": "/api/temporal_models",                 "handler": "api_get_temporal_models"},
    {"method": "GET", "path": "/api/temporal/models",                 "handler": "api_get_temporal_models"},
    {"method": "GET", "path": "/api/backtest/portfolio/latest",       "handler": "api_get_latest_portfolio_backtest"},
    {"method": "GET", "path": "/api/portfolio/backtest/latest",       "handler": "api_get_latest_portfolio_backtest"},
    {"method": "GET", "path": "/api/execution_metrics",               "handler": "api_get_execution_metrics"},
    {"method": "GET", "path": "/api/execution/metrics",               "handler": "api_get_execution_metrics"},
    {"method": "GET", "path": "/api/execution/stats",                 "handler": "api_get_execution_stats"},
    {"method": "GET", "path": "/api/execution_metrics/rolling",       "handler": "api_get_execution_metrics_rolling"},
    {"method": "GET", "path": "/api/execution/metrics/rolling",       "handler": "api_get_execution_metrics_rolling"},
    {"method": "GET", "path": "/api/execution_metrics/by_symbol",     "handler": "api_get_execution_metrics_by_symbol"},
    {"method": "GET", "path": "/api/execution/metrics/by_symbol",     "handler": "api_get_execution_metrics_by_symbol"},
    {"method": "GET", "path": "/api/execution_metrics/by_confidence", "handler": "api_get_execution_cost_by_confidence"},
    {"method": "GET", "path": "/api/execution/metrics/by_confidence", "handler": "api_get_execution_cost_by_confidence"},
    {"method": "GET", "path": "/api/confidence_mass",                 "handler": "api_get_confidence_mass"},
    {"method": "GET", "path": "/api/social/features",                 "handler": "api_get_social_features"},
    {"method": "GET", "path": "/api/social/regimes",                  "handler": "api_get_social_regimes"},
    {"method": "GET", "path": "/api/social/blocks",                   "handler": "api_get_social_blocks"},
    {"method": "GET", "path": "/api/relevance_stats",                 "handler": "api_get_relevance_stats"},
    {"method": "GET", "path": "/api/relevance/stats",                 "handler": "api_get_relevance_stats"},
    {"method": "POST","path": "/api/champion/rollback",               "handler": "api_post_rollback"},

    # EXECUTION / PORTFOLIO / PROMOTION (UI hard-deps)
    {"method": "GET", "path": "/api/market_stress",                   "handler": "api_get_market_stress"},
    {"method": "GET", "path": "/api/market_stress_history",           "handler": "api_get_market_stress_history"},
    {"method": "GET", "path": "/api/portfolio",                       "handler": "api_get_portfolio"},
    {"method": "GET", "path": "/api/portfolio/backtest",              "handler": "api_get_portfolio_backtest"},
    {"method": "GET",  "path": "/api/prices",                          "handler": "api_get_prices"},
    {"method": "GET",  "path": "/api/trades",                          "handler": "api_get_trades"},
    {"method": "GET", "path": "/api/broker",                          "handler": "api_get_broker"},
    {"method": "GET", "path": "/api/strategy/status",                 "handler": "api_get_strategy_status"},
    {"method": "GET", "path": "/api/strategy_metrics",                "handler": "api_get_strategy_metrics"},
    {"method": "GET", "path": "/api/strategy/metrics",                "handler": "api_get_strategy_metrics"},
    {"method": "GET", "path": "/api/alpha_decay",                     "handler": "api_get_alpha_decay"},
    {"method": "GET", "path": "/api/reconcile/broker_backtest",       "handler": "api_get_reconcile_broker_backtest"},
    {"method": "GET", "path": "/api/equity_drift",                    "handler": "api_get_equity_drift"},
    {"method": "GET", "path": "/api/temporal_shadow_eval",            "handler": "api_get_temporal_shadow_eval"},
    {"method": "GET", "path": "/api/temporal/shadow_eval",            "handler": "api_get_temporal_shadow_eval"},
    {"method": "GET", "path": "/api/promotion_audit",                 "handler": "api_get_promotion_audit"},
    {"method": "GET", "path": "/api/promotion/audit",                 "handler": "api_get_promotion_audit"},
    {"method": "GET", "path": "/api/causal/scores",                   "handler": "api_get_causal_scores"},
    {"method": "GET", "path": "/api/promotion/status",                "handler": "api_get_promotion_status"},
    {"method": "GET", "path": "/api/governance/summary",              "handler": "api_get_governance_summary"},

    # UI hard-deps present in ui/dashboard.js but missing from ROUTE_SPECS_* in this repo
    {"method": "GET",  "path": "/api/system/kill_switches",           "handler": "api_get_kill_switches"},  # alias
    {"method": "GET",  "path": "/api/alerts/by_id",                   "handler": "api_get_alert_by_id"},
    {"method": "POST", "path": "/api/alerts/{id}/ack",                "handler": "api_post_alert_ack"},
    {"method": "POST", "path": "/api/alerts/{id}/resolve",            "handler": "api_post_alert_resolve"},
    {"method": "GET",  "path": "/api/ui/decisions",                   "handler": "api_get_recent_decisions"},
    {"method": "GET",  "path": "/api/ui/decision",                    "handler": "api_get_decision_detail"},
    {"method": "GET",  "path": "/api/audit/records",                  "handler": "api_get_audit_records"},
    {"method": "POST", "path": "/api/ui/interaction",                 "handler": "api_post_ui_interaction"},
    {"method": "POST", "path": "/api/copilot/ask",                    "handler": "api_post_copilot_ask"},
    {"method": "GET",  "path": "/api/promotion/explain",              "handler": "api_get_promotion_explain"},
    {"method": "POST", "path": "/api/promotion/enable",               "handler": "api_post_promotion_enable"},
    {"method": "POST", "path": "/api/system/fix",                     "handler": "api_post_system_fix"},
    {"method": "GET",  "path": "/api/size_policy",                    "handler": "api_get_size_policy"},
    {"method": "GET",  "path": "/api/strategy/size_policy",           "handler": "api_get_size_policy"},
    {"method": "POST", "path": "/api/size_policy/train",              "handler": "api_post_size_policy_train"},
    {"method": "POST", "path": "/api/strategy/size_policy/train",     "handler": "api_post_size_policy_train"},
    {"method": "GET",  "path": "/api/model_metrics",                  "handler": "api_get_model_metrics"},
    {"method": "GET",  "path": "/api/model/metrics",                  "handler": "api_get_model_metrics"},
    {"method": "GET",  "path": "/api/execution_overlays",             "handler": "api_get_execution_overlays"},
    {"method": "GET",  "path": "/api/execution/overlays",             "handler": "api_get_execution_overlays"},
    {"method": "GET",  "path": "/api/crash_analytics",                "handler": "api_get_crash_analytics"},
    {"method": "GET",  "path": "/api/news/latest",                    "handler": "api_get_news_latest"},
    {"method": "GET",  "path": "/api/news/sentiment",                 "handler": "api_get_news_sentiment"},

    # MARKET (live candles from Polygon feed via DB)
    {"method": "GET",  "path": "/api/market/candles",                 "handler": "api_get_market_candles"},
    {"method": "GET",  "path": "/api/market/stream",                  "handler": "api_get_market_stream"},

    # TERMINAL
    {"method": "GET",  "path": "/api/terminal/watchlist",             "handler": "api_get_terminal_watchlist"},
    {"method": "GET",  "path": "/api/terminal/snapshot",              "handler": "api_get_terminal_snapshot"},
    {"method": "GET",  "path": "/api/terminal/positions",             "handler": "api_get_terminal_positions"},
    {"method": "GET",  "path": "/api/terminal/orders",                "handler": "api_get_terminal_orders"},
    {"method": "GET",  "path": "/api/terminal/fills",                 "handler": "api_get_terminal_fills"},
    {"method": "GET",  "path": "/api/terminal/equity",                "handler": "api_get_terminal_equity"},
    {"method": "GET",  "path": "/api/terminal/markers",               "handler": "api_get_terminal_markers"},

    {"method": "POST", "path": "/api/terminal/order",                 "handler": "api_post_terminal_order"},
    {"method": "POST", "path": "/api/terminal/flatten",               "handler": "api_post_terminal_flatten"},
]

# -------------------------------------------------------------------
# FORCE-MERGE ALL ROUTES (SYSTEM + JOBS + OPS + FALLBACK)
# - Always keep first occurrence of (method,path)
# - Always normalize to dict {method,path,handler}
# -------------------------------------------------------------------

# Load terminal routes if module exists
try:
    from engine.terminal.api import ROUTE_SPECS_TERMINAL_ALL
    _terminal_routes = list(ROUTE_SPECS_TERMINAL_ALL)
except Exception:
    _terminal_routes = []

_RAW_ROUTE_SPECS = (
    list(ROUTE_SPECS_SYSTEM)
    + list(ROUTE_SPECS_JOBS)
    + list(ROUTE_SPECS_OPS)
    + list(ROUTE_SPECS_MARKET)
    + list(ROUTE_SPECS_REPLAY)
    + list(ROUTE_SPECS_DATA_SOURCES)
    + list(ROUTE_SPECS_UI_METRICS)
    + list(_terminal_routes)
    + list(_FALLBACK_ROUTE_SPECS)
)

def _normalize_route_specs(route_specs):
    seen = set()
    out = []

    for r in route_specs:
        if isinstance(r, dict):
            method = str(r.get("method", "")).upper().strip()
            path = str(r.get("path", "")).strip()
            handler = str(r.get("handler", "")).strip()
        elif isinstance(r, tuple) and len(r) >= 3:
            method = str(r[0]).upper().strip()
            path = str(r[1]).strip()
            handler = str(r[2]).strip()
        else:
            continue

        if not method or not path or not handler:
            continue

        key = (method, path)
        if key in seen:
            continue

        seen.add(key)
        out.append({
            "method": method,
            "path": path,
            "handler": handler,
        })

    return out


_HANDLER_SIGNATURE_FALLBACK_WARNED: set[tuple[str, int, int]] = set()


def _call_with_typeerror_fallbacks(handler_name: str, fn, *arg_variants):
    last_error = None
    for idx, args in enumerate(arg_variants):
        try:
            return fn(*args)
        except TypeError as e:
            last_error = e
            if idx >= len(arg_variants) - 1:
                raise
            warn_key = (str(handler_name), int(len(args)), int(len(arg_variants[idx + 1])))
            if warn_key not in _HANDLER_SIGNATURE_FALLBACK_WARNED:
                _warn_nonfatal(
                    "DASHBOARD_SERVER_HANDLER_SIGNATURE_FALLBACK",
                    e,
                    handler=str(handler_name),
                    attempted_arg_count=int(len(args)),
                    fallback_arg_count=int(len(arg_variants[idx + 1])),
                )
                _HANDLER_SIGNATURE_FALLBACK_WARNED.add(warn_key)
            continue
    raise last_error if last_error is not None else TypeError(f"{handler_name} has no compatible signature")


ROUTE_SPECS = _normalize_route_specs(_RAW_ROUTE_SPECS)

def api_get_kill_switches(parsed, ctx=None):
    """Delegate kill-switch state retrieval to the packaged API handler.

    Parameters
    ----------
    parsed : Any
        Parsed request object. When ``None``, a minimal shim is created so
        lifecycle probes can still query the handler.
    ctx : dict, optional
        Optional request context forwarded to the implementation when accepted.

    Returns
    -------
    dict
        Implementation-defined kill-switch payload. If no implementation is
        available, returns ``{"ok": False, "error": "kill_switches_unavailable"}``.

    Notes
    -----
    This wrapper exists to keep the dashboard entrypoint compatible with older
    handler signatures while preserving the central kill-switch implementation.
    """
    if not _api_get_kill_switches_impl:
        return {"ok": False, "error": "kill_switches_unavailable"}
    # Some callers pass None (lifecycle monitor). Provide a minimal parsed shim.
    if parsed is None:
        class _P:  # tiny shim
            query = ""
        parsed = _P()

    # Prefer passing ctx if the impl supports it
    return _call_with_typeerror_fallbacks(
        "api_get_kill_switches",
        _api_get_kill_switches_impl,
        (parsed, ctx),
        (parsed, {}),
    )


def api_get_job_log(parsed, body=None, ctx=None):
    if not _api_get_job_log_impl:
        return {"ok": False, "error": "job_log_unavailable"}

    try:
        ctx = ctx or {}
        if "JOBS" not in ctx:
            ctx["JOBS"] = JOBS

        return _call_with_typeerror_fallbacks(
            "api_get_job_log",
            _api_get_job_log_impl,
            (parsed, body, ctx),
            (parsed, ctx),
            (parsed,),
        )

    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_JOB_LOG_FAILED", e, endpoint="api_get_job_log")
        return {"ok": False, "error": "job_log_exception", "detail": str(e)}

def api_get_job_history(parsed, body=None, ctx=None):
    if not _api_get_job_history_impl:
        return {"ok": False, "error": "job_history_unavailable"}

    try:
        ctx = ctx or {}
        if "JOBS" not in ctx:
            ctx["JOBS"] = JOBS

        return _call_with_typeerror_fallbacks(
            "api_get_job_history",
            _api_get_job_history_impl,
            (parsed, body, ctx),
            (parsed, ctx),
            (parsed,),
        )
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_JOB_HISTORY_FAILED", e, endpoint="api_get_job_history")
        return {"ok": False, "error": "job_history_exception", "detail": str(e)}

# ------------------------------
# JOBS + PIPELINE
# ------------------------------
def api_post_pipeline_run(parsed, _body=None, _ctx=None):
    try:
        if isinstance(_body, dict):
            return _api_jobs_post_pipeline_run(parsed, _body, _ctx)
        q = _qs_dict(parsed)
        include_execution = str(q.get("include_execution") or "").strip().lower() in ("1", "true", "yes", "y", "on")
        return _runtime_orchestrator().run_pipeline(include_execution=include_execution)
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_PIPELINE_RUN_FAILED", e, endpoint="api_post_pipeline_run")
        return {"ok": False, "error": str(e)}

def api_get_supervisor_status_local(_parsed=None, _body=None, _ctx=None):
    return api_get_supervisor_status(_parsed, _ctx)
# ---------------------------------------------------
# OPS HANDLERS
# - api_ops_handlers.py in this repo does NOT define many "NEW UI hard-deps"
#   referenced by older dashboard_server variants.
# - Import the ones that exist; provide safe stubs for missing to avoid startup crash.
# ---------------------------------------------------

def _unavailable(name: str):
    def _fn(*_a, **_k):
        return {"ok": False, "error": "unavailable", "name": name}
    return _fn

_OPS_HANDLER_NAMES = [
    "api_get_alerts",
    "api_get_notifications_status",
    "api_get_feeds",
    "api_get_validation",
    "api_get_model_diagnostics",
    "api_get_model_registry",
    "api_get_model_performance_divergence",
    "api_get_embed_model_eval",
    "api_get_embed_conf_calib",
    "api_get_temporal_eval",
    "api_get_temporal_models",
    "api_get_latest_portfolio_backtest",
    "api_get_execution_metrics",
    "api_get_execution_stats",
    "api_get_execution_metrics_rolling",
    "api_get_execution_metrics_by_symbol",
    "api_get_execution_cost_by_confidence",
    "api_get_execution_advisories",
    "api_get_social_features",
    "api_get_social_regimes",
    "api_get_social_blocks",
    "api_get_news_latest",
    "api_get_news_sentiment",
    "api_get_human_alignment_summary",
    "api_get_weather_snapshot",
    "api_get_weather_alerts",
    "api_get_weather_effect",
    "api_get_confidence_mass",
    "api_post_notifications_test",
    "api_post_execution_advisory_action",
    "api_post_rollback",
]
_OPS_REQUIRED_HANDLERS = {
    "api_get_alerts",
    "api_get_validation",
    "api_get_model_registry",
    "api_get_latest_portfolio_backtest",
    "api_get_execution_metrics",
}
_OPS_IMPORT_ERRORS = {}

# Predeclare dynamically loaded ops handlers so static analysis can resolve
# the names that are populated below through `globals()`.
api_get_alerts = _unavailable("api_get_alerts")
api_get_notifications_status = _unavailable("api_get_notifications_status")
api_get_feeds = _unavailable("api_get_feeds")
api_get_validation = _unavailable("api_get_validation")
api_get_model_diagnostics = _unavailable("api_get_model_diagnostics")
api_get_model_registry = _unavailable("api_get_model_registry")
api_get_model_performance_divergence = _unavailable("api_get_model_performance_divergence")
api_get_embed_model_eval = _unavailable("api_get_embed_model_eval")
api_get_embed_conf_calib = _unavailable("api_get_embed_conf_calib")
api_get_temporal_eval = _unavailable("api_get_temporal_eval")
api_get_temporal_models = _unavailable("api_get_temporal_models")
api_get_latest_portfolio_backtest = _unavailable("api_get_latest_portfolio_backtest")
api_get_execution_metrics = _unavailable("api_get_execution_metrics")
api_get_execution_stats = _unavailable("api_get_execution_stats")
api_get_execution_metrics_rolling = _unavailable("api_get_execution_metrics_rolling")
api_get_execution_metrics_by_symbol = _unavailable("api_get_execution_metrics_by_symbol")
api_get_execution_cost_by_confidence = _unavailable("api_get_execution_cost_by_confidence")
api_get_execution_advisories = _unavailable("api_get_execution_advisories")
api_get_social_features = _unavailable("api_get_social_features")
api_get_social_regimes = _unavailable("api_get_social_regimes")
api_get_social_blocks = _unavailable("api_get_social_blocks")
api_get_human_alignment_summary = _unavailable("api_get_human_alignment_summary")
api_get_weather_snapshot = _unavailable("api_get_weather_snapshot")
api_get_weather_alerts = _unavailable("api_get_weather_alerts")
api_get_weather_effect = _unavailable("api_get_weather_effect")
api_get_confidence_mass = _unavailable("api_get_confidence_mass")
api_post_notifications_test = _unavailable("api_post_notifications_test")
api_post_execution_advisory_action = _unavailable("api_post_execution_advisory_action")
api_post_rollback = _unavailable("api_post_rollback")

try:
    _ops_module = importlib.import_module("engine.api.api_ops_handlers")
except Exception as e:
    raise RuntimeError(f"dashboard_server_api_ops_handlers_import_failed: {type(e).__name__}: {e}") from e

for _handler_name in _OPS_HANDLER_NAMES:
    _handler = getattr(_ops_module, _handler_name, None)
    if callable(_handler):
        globals()[_handler_name] = _handler
        continue

    _OPS_IMPORT_ERRORS[_handler_name] = "missing_or_not_callable"
    if _handler_name in _OPS_REQUIRED_HANDLERS:
        raise RuntimeError(f"dashboard_server_required_handler_missing:{_handler_name}")

    globals()[_handler_name] = _unavailable(_handler_name)
    try:
        log.warning("dashboard_server_optional_handler_unavailable: %s", _handler_name)
    except Exception:
        traceback.print_exc()

# Optional (missing in this repo): keep names defined so API_HANDLERS can reference them safely.

try:
    from engine.api.api_dashboard_reads import (
        api_get_portfolio as _impl_api_get_portfolio,
        api_get_latest_portfolio_backtest as _impl_api_get_latest_portfolio_backtest,
        api_get_recent_decisions as _impl_api_get_recent_decisions,
        api_get_decision_detail as _impl_api_get_decision_detail,
        api_get_audit_records as _impl_api_get_audit_records,
    )
except Exception:
    _impl_api_get_portfolio = None
    _impl_api_get_latest_portfolio_backtest = None
    _impl_api_get_recent_decisions = None
    _impl_api_get_decision_detail = None
    _impl_api_get_audit_records = None

def api_get_portfolio(parsed, _ctx=None):
    if _impl_api_get_portfolio:
        return _call_with_typeerror_fallbacks(
            "api_get_portfolio",
            _impl_api_get_portfolio,
            (parsed, {}),
            (parsed,),
            tuple(),
        )
    return {
        "ok": False,
        "error": "portfolio_handler_unavailable",
        "meta": {"ready": False, "reason": "portfolio_handler_unavailable"},
        "state": [],
        "orders": [],
    }

def api_get_portfolio_backtest(parsed, _ctx=None):
    if _impl_api_get_latest_portfolio_backtest:
        return _call_with_typeerror_fallbacks(
            "api_get_portfolio_backtest",
            _impl_api_get_latest_portfolio_backtest,
            (parsed, {}),
            (parsed,),
            tuple(),
        )
    return {
        "ok": False,
        "error": "portfolio_backtest_handler_unavailable",
        "data": [],
        "rows": [],
    }


def api_get_recent_decisions(parsed, _ctx=None):
    if _impl_api_get_recent_decisions:
        return _call_with_typeerror_fallbacks(
            "api_get_recent_decisions",
            _impl_api_get_recent_decisions,
            (parsed, {}),
            (parsed,),
            tuple(),
        )
    return {"ok": True, "decisions": [], "meta": {"ready": False, "count": 0}}


def api_get_decision_detail(parsed, _ctx=None):
    if _impl_api_get_decision_detail:
        return _call_with_typeerror_fallbacks(
            "api_get_decision_detail",
            _impl_api_get_decision_detail,
            (parsed, {}),
            (parsed,),
            tuple(),
        )
    return {"ok": False, "error": "decision_handler_unavailable", "decision": None}


def api_get_audit_records(parsed, _ctx=None):
    if _impl_api_get_audit_records:
        return _call_with_typeerror_fallbacks(
            "api_get_audit_records",
            _impl_api_get_audit_records,
            (parsed, {}),
            (parsed,),
            tuple(),
        )
    return {"ok": False, "error": "audit_records_handler_unavailable", "records": []}


def api_post_ui_interaction(_parsed, body=None, _ctx=None):
    try:
        from engine.runtime.storage import log_alert_interaction, log_decision_view
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_UI_INTERACTION_STORAGE_IMPORT_FAILED", e)
        return {"ok": False, "error": f"interaction_storage_unavailable:{e}"}

    payload = body if isinstance(body, dict) else {}
    interaction_type = str(payload.get("interaction_type") or "").strip()
    if not interaction_type:
        return {"ok": False, "error": "missing_interaction_type"}

    detail = payload.get("detail")
    if detail is not None and not isinstance(detail, dict):
        detail = {"value": str(detail)}

    try:
        alert_id = payload.get("alert_id")
        alert_id = int(alert_id) if alert_id is not None else None
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_UI_INTERACTION_ALERT_ID_INVALID", e, alert_id=payload.get("alert_id"))
        return {"ok": False, "error": "invalid_alert_id"}

    try:
        decision_id = payload.get("decision_id")
        decision_id = int(decision_id) if decision_id is not None else None
    except Exception as e:
        _warn_nonfatal(
            "DASHBOARD_SERVER_UI_INTERACTION_DECISION_ID_INVALID",
            e,
            decision_id=payload.get("decision_id"),
        )
        return {"ok": False, "error": "invalid_decision_id"}

    if alert_id is None and decision_id is None:
        return {"ok": False, "error": "missing_subject_id"}

    actor = str(payload.get("actor") or "operator").strip() or "operator"
    session_id = str(payload.get("session_id") or "").strip() or None
    source = str(payload.get("source") or "dashboard").strip() or "dashboard"

    try:
        interaction_id = log_alert_interaction(
            interaction_type=interaction_type,
            alert_id=alert_id,
            decision_id=decision_id,
            actor=actor,
            session_id=session_id,
            source=source,
            detail=detail if isinstance(detail, dict) else {},
        )
        view_id = None
        if decision_id is not None and interaction_type == "decision_open":
            view_id = log_decision_view(
                decision_id=decision_id,
                actor=actor,
                session_id=session_id,
                source=source,
                detail=detail if isinstance(detail, dict) else {},
            )
        return {"ok": True, "interaction_id": interaction_id, "view_id": view_id}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_UI_INTERACTION_FAILED", e, endpoint="api_post_ui_interaction")
        return {"ok": False, "error": str(e)}


def api_get_prices(parsed, _ctx=None):
    con = None
    symbol = ""
    try:
        qs = _qs_dict(parsed)
        symbol = str(qs.get("symbol", "") or "").strip().upper()
        limit = max(1, min(5000, int(qs.get("limit", "200") or "200")))

        con = _dashboard_db_connect()
        rows = []

        if _dashboard_table_exists(con, "prices"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT
                        ts_ms,
                        symbol,
                        COALESCE(price, px) AS price,
                        px,
                        source
                    FROM prices
                    WHERE symbol = ?
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (symbol, int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT
                        ts_ms,
                        symbol,
                        COALESCE(price, px) AS price,
                        px,
                        source
                    FROM prices
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []
        elif _dashboard_table_exists(con, "price_quotes"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT
                        ts_ms,
                        symbol,
                        last AS price,
                        last AS px,
                        'price_quotes' AS source
                    FROM price_quotes
                    WHERE symbol = ?
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (symbol, int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT
                        ts_ms,
                        symbol,
                        last AS price,
                        last AS px,
                        'price_quotes' AS source
                    FROM price_quotes
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []
        elif _dashboard_table_exists(con, "price_quotes_raw"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT
                        ts_ms,
                        symbol,
                        last AS price,
                        last AS px,
                        'price_quotes_raw' AS source
                    FROM price_quotes_raw
                    WHERE symbol = ?
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (symbol, int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT
                        ts_ms,
                        symbol,
                        last AS price,
                        last AS px,
                        'price_quotes_raw' AS source
                    FROM price_quotes_raw
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []

        data = [
            {
                "ts_ms": int(r[0] or 0),
                "symbol": str(r[1] or ""),
                "price": (float(r[2]) if r[2] is not None else None),
                "px": (float(r[3]) if r[3] is not None else None),
                "source": (str(r[4]) if r[4] is not None else None),
            }
            for r in rows
        ]
        candles = [
            {
                "ts": int(d["ts_ms"] or 0),
                "ts_ms": int(d["ts_ms"] or 0),
                "open": float(d["price"] if d["price"] is not None else d["px"] or 0.0),
                "high": float(d["price"] if d["price"] is not None else d["px"] or 0.0),
                "low": float(d["price"] if d["price"] is not None else d["px"] or 0.0),
                "close": float(d["price"] if d["price"] is not None else d["px"] or 0.0),
                "volume": 0.0,
            }
            for d in reversed(data)
            if d.get("ts_ms")
            and (d.get("price") is not None or d.get("px") is not None)
        ]
        return {
            "ok": True,
            "error": None,
            "symbol": symbol or None,
            "meta": {"ready": bool(data), "count": int(len(data))},
            "candles": candles,
            "data": data,
            "rows": data,
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_TRADES_FAILED", e, endpoint="api_get_trades")
        return {
            "ok": False,
            "error": str(e),
            "symbol": symbol or None,
            "meta": {"ready": False, "count": 0},
            "candles": [],
            "data": [],
            "rows": [],
        }
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_trades")


def api_get_trades(parsed, _ctx=None):
    con = None
    symbol = ""
    try:
        qs = _qs_dict(parsed)
        symbol = str(qs.get("symbol", "") or "").strip().upper()
        limit = max(1, min(5000, int(qs.get("limit", "200") or "200")))

        con = _dashboard_db_connect()
        rows = []

        if _dashboard_table_exists(con, "execution_fills"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT
                        id,
                        symbol,
                        CASE WHEN COALESCE(fill_qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                        ABS(COALESCE(fill_qty, 0)) AS qty,
                        fill_px AS price,
                        fill_ts_ms AS ts_ms,
                        client_order_id,
                        broker,
                        'execution_fills' AS source_table
                    FROM execution_fills
                    WHERE symbol = ?
                    ORDER BY fill_ts_ms DESC, id DESC
                    LIMIT ?
                    """,
                    (symbol, int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT
                        id,
                        symbol,
                        CASE WHEN COALESCE(fill_qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                        ABS(COALESCE(fill_qty, 0)) AS qty,
                        fill_px AS price,
                        fill_ts_ms AS ts_ms,
                        client_order_id,
                        broker,
                        'execution_fills' AS source_table
                    FROM execution_fills
                    ORDER BY fill_ts_ms DESC, id DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []
        elif _dashboard_table_exists(con, "broker_fills_v2"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT
                        id,
                        symbol,
                        CASE WHEN COALESCE(qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                        ABS(COALESCE(qty, 0)) AS qty,
                        px AS price,
                        ts_ms,
                        source_order_id,
                        note,
                        'broker_fills_v2' AS source_table
                    FROM broker_fills_v2
                    WHERE symbol = ?
                    ORDER BY ts_ms DESC, id DESC
                    LIMIT ?
                    """,
                    (symbol, int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT
                        id,
                        symbol,
                        CASE WHEN COALESCE(qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                        ABS(COALESCE(qty, 0)) AS qty,
                        px AS price,
                        ts_ms,
                        source_order_id,
                        note,
                        'broker_fills_v2' AS source_table
                    FROM broker_fills_v2
                    ORDER BY ts_ms DESC, id DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []
        elif _dashboard_table_exists(con, "broker_fills"):
            if symbol:
                rows = con.execute(
                    """
                    SELECT
                        id,
                        symbol,
                        CASE WHEN COALESCE(qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                        ABS(COALESCE(qty, 0)) AS qty,
                        px AS price,
                        ts_ms,
                        source_order_id,
                        note,
                        'broker_fills' AS source_table
                    FROM broker_fills
                    WHERE symbol = ?
                    ORDER BY ts_ms DESC, id DESC
                    LIMIT ?
                    """,
                    (symbol, int(limit)),
                ).fetchall() or []
            else:
                rows = con.execute(
                    """
                    SELECT
                        id,
                        symbol,
                        CASE WHEN COALESCE(qty, 0) >= 0 THEN 'BUY' ELSE 'SELL' END AS side,
                        ABS(COALESCE(qty, 0)) AS qty,
                        px AS price,
                        ts_ms,
                        source_order_id,
                        note,
                        'broker_fills' AS source_table
                    FROM broker_fills
                    ORDER BY ts_ms DESC, id DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall() or []

        data = [
            {
                "id": int(r[0] or 0),
                "symbol": str(r[1] or ""),
                "side": str(r[2] or ""),
                "qty": float(r[3] or 0.0),
                "price": float(r[4] or 0.0),
                "ts_ms": int(r[5] or 0),
                "ref": (str(r[6]) if r[6] is not None else None),
                "note": (str(r[7]) if r[7] is not None else None),
                "source_table": str(r[8] or ""),
            }
            for r in rows
        ]
        markers = [
            {
                "ts": int((d["ts_ms"] or 0) // 1000),
                "ts_ms": int(d["ts_ms"] or 0),
                "symbol": str(d["symbol"] or ""),
                "side": str(d["side"] or ""),
                "price": float(d["price"] or 0.0),
                "size": float(d["qty"] or 0.0),
            }
            for d in reversed(data)
            if d.get("ts_ms")
        ]
        return {
            "ok": True,
            "error": None,
            "symbol": symbol or None,
            "meta": {"ready": bool(data), "count": int(len(data))},
            "markers": markers,
            "data": data,
            "rows": data,
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_TRADES_FETCH_FAILED", e, endpoint="api_get_trades")
        error_payload = {
            "ok": False,
            "error": str(e),
            "symbol": symbol or None,
            "meta": {"ready": False, "count": 0},
            "markers": [],
            "data": [],
            "rows": [],
        }
        return error_payload
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_trades")


def api_get_trades_legacy_table(_parsed, _ctx=None):
    try:
        from engine.runtime.storage import connect

        con = connect(readonly=True)
        try:
            cols = {
                str(r[1])
                for r in (con.execute("PRAGMA table_info(trades)").fetchall() or [])
                if r and len(r) > 1 and r[1]
            }
            ts_col = "ts_ms" if "ts_ms" in cols else ("ts" if "ts" in cols else None)
            if not ts_col:
                return {"ok": True, "data": [], "rows": []}

            rows = con.execute(
                f"""
                SELECT id, symbol, side, qty, price, {ts_col} AS ts_ms
                FROM trades
                ORDER BY {ts_col} DESC
                LIMIT 200
                """
            ).fetchall()

            data = [
                {
                    "id": r["id"],
                    "symbol": r["symbol"],
                    "side": r["side"],
                    "qty": r["qty"],
                    "price": r["price"],
                    "ts_ms": r["ts_ms"],
                }
                for r in rows
            ]

            return {
                "ok": True,
                "data": data,
                "rows": data,
            }
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_trades_legacy_table")
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_TRADES_LEGACY_FAILED", e, endpoint="api_get_trades_legacy_table")
        return {"ok": False, "error": str(e), "data": [], "rows": []}

def _dashboard_db_connect():
    try:
        from engine.api.internal_access import db_connect
        return db_connect(readonly=True)
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_DB_CONNECT_IMPORT_FAILED", e, module="engine.api.internal_access")
        from engine.runtime.storage import connect as db_connect
        return db_connect(readonly=True)


def _dashboard_table_exists(con, table_name):
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "DASHBOARD_SERVER_TABLE_EXISTS_FAILED",
            e,
            table_name=str(table_name),
        )
        exists = False
        return exists


def _dashboard_table_columns(con, table_name):
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
        return [str(r[1]) for r in rows if len(r) > 1]
    except Exception as e:
        _warn_nonfatal(
            "DASHBOARD_SERVER_TABLE_COLUMNS_FAILED",
            e,
            table_name=str(table_name),
        )
        columns: list[str] = []
        return columns


def _dashboard_parse_json(value, default=None):
    if default is None:
        default = {}
    try:
        if value is None or value == "":
            return default
        if isinstance(value, (dict, list)):
            return value
        return json.loads(value)
    except Exception as e:
        _warn_nonfatal(
            "DASHBOARD_SERVER_PARSE_JSON_FAILED",
            e,
            value_type=type(value).__name__,
        )
        parsed_default = default
        return parsed_default


def api_get_market_stress(_parsed=None, _ctx=None):
    try:
        from engine.strategy.market_stress import get_market_stress_snapshot
        stress = get_market_stress_snapshot() or {}
        return {"ok": True, "stress": stress}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_MARKET_STRESS_FAILED", e, endpoint="api_get_market_stress")
        return {"ok": False, "error": str(e), "stress": {}}


def api_get_market_stress_history(parsed, _ctx=None):
    limit_s = _qs_value(parsed, "limit", "60")
    try:
        limit = max(10, min(240, int(limit_s)))
    except Exception:
        limit = 60

    con = None
    try:
        from engine.strategy.market_stress import get_market_stress_snapshot

        con = _dashboard_db_connect()
        series = []

        if not _dashboard_table_exists(con, "prices"):
            return {"ok": True, "series": []}

        rows = con.execute(
            """
            SELECT DISTINCT ts_ms
            FROM prices
            WHERE symbol = 'VIX'
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []

        ts_values = [int(r[0]) for r in rows if r and r[0] is not None]
        ts_values.reverse()

        for ts_ms in ts_values:
            try:
                stress = get_market_stress_snapshot(con=con, ts_ms=int(ts_ms)) or {}
                series.append({
                    "ts_ms": int(ts_ms),
                    "stress_score": float(stress.get("stress_score", 0.0) or 0.0),
                })
            except Exception as e:
                _warn_nonfatal(
                    "DASHBOARD_SERVER_MARKET_STRESS_HISTORY_ROW_FAILED",
                    e,
                    endpoint="api_get_market_stress_history",
                    ts_ms=int(ts_ms),
                )
                continue

        return {"ok": True, "series": series}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_MARKET_STRESS_HISTORY_FAILED", e, endpoint="api_get_market_stress_history")
        return {"ok": False, "error": str(e), "series": []}
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_market_stress")


def api_get_broker(parsed, _ctx=None):
    try:
        eq = api_get_terminal_equity(parsed, _ctx)
        pos = api_get_terminal_positions(parsed, _ctx)
        fills = api_get_terminal_fills(parsed, _ctx)

        account = {}
        if isinstance(eq, dict):
            account = dict(eq.get("account") or {})

        positions = []
        if isinstance(pos, dict):
            for r in (pos.get("rows") or []):
                positions.append({
                    "symbol": str(r.get("symbol") or ""),
                    "qty": float(r.get("qty") or 0.0),
                    "avg_px": float(r.get("avg_px") or 0.0),
                    "updated_ts_ms": int(r.get("updated_ts_ms") or 0),
                })

        fills_out = []
        if isinstance(fills, dict):
            for r in (fills.get("rows") or []):
                fills_out.append({
                    "id": r.get("id"),
                    "ts_ms": int(r.get("ts_ms") or 0),
                    "symbol": str(r.get("symbol") or ""),
                    "qty": float(r.get("qty") or 0.0),
                    "px": float(r.get("px") or 0.0),
                    "order_id": r.get("source_order_id"),
                    "note": r.get("note"),
                })

        return {
            "ok": True,
            "account": account,
            "positions": positions,
            "fills": fills_out,
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_BROKER_STATUS_FAILED", e, endpoint="api_get_broker_status")
        return {"ok": False, "error": str(e), "account": {}, "positions": [], "fills": []}


def api_get_strategy_status(_parsed=None, _ctx=None):
    con = None
    try:
        con = _dashboard_db_connect()
        rows = []

        rows.append({"key": "engine_mode", "value": str(os.environ.get("ENGINE_MODE", "safe"))})

        if _dashboard_table_exists(con, "strategy_registry"):
            reg_rows = con.execute(
                """
                SELECT strategy_name, enabled, stage, updated_ts_ms
                FROM strategy_registry
                ORDER BY strategy_name ASC
                """
            ).fetchall() or []

            rows.append({"key": "registered_strategies", "value": str(len(reg_rows))})

            enabled_n = sum(1 for r in reg_rows if int(r[1] or 0) == 1)
            rows.append({"key": "enabled_strategies", "value": str(enabled_n)})

            for strategy_name, enabled, stage, updated_ts_ms in reg_rows[:20]:
                rows.append({
                    "key": f"strategy:{strategy_name}",
                    "value": f"enabled={int(enabled or 0)} stage={stage or ''} updated_ts_ms={int(updated_ts_ms or 0)}",
                })

        if _dashboard_table_exists(con, "strategy_allocations"):
            alloc = con.execute(
                """
                SELECT ts_ms, window_days, allocations_json
                FROM strategy_allocations
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
            if alloc:
                alloc_json = _json_dict(_dashboard_parse_json(alloc[2], {}))
                rows.append({"key": "latest_allocation_ts_ms", "value": str(int(alloc[0] or 0))})
                rows.append({"key": "latest_allocation_window_days", "value": str(int(alloc[1] or 0))})
                rows.append({"key": "latest_allocation_keys", "value": ",".join(sorted(list((alloc_json or {}).keys()))[:20])})

        if _dashboard_table_exists(con, "strategy_metrics"):
            metric_row = con.execute(
                """
                SELECT MAX(ts_ms)
                FROM strategy_metrics
                """
            ).fetchone()
            rows.append({"key": "latest_strategy_metrics_ts_ms", "value": str(int((metric_row or [0])[0] or 0))})

        if _dashboard_table_exists(con, "alpha_decay_runtime_history"):
            runtime_row = con.execute(
                """
                SELECT ts_ms, status, min_throttle_mult, severe_count, warn_count
                FROM alpha_decay_runtime_history
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
            if runtime_row:
                rows.append({"key": "alpha_decay_runtime_ts_ms", "value": str(int(runtime_row[0] or 0))})
                rows.append({"key": "alpha_decay_status", "value": str(runtime_row[1] or "ok")})
                rows.append({"key": "alpha_decay_min_throttle_mult", "value": str(float(runtime_row[2] or 1.0))})
                rows.append({"key": "alpha_decay_severe_count", "value": str(int(runtime_row[3] or 0))})
                rows.append({"key": "alpha_decay_warn_count", "value": str(int(runtime_row[4] or 0))})

        if _dashboard_table_exists(con, "alpha_decay_strategy_metrics"):
            decay_rows = con.execute(
                """
                SELECT strategy_name, severity, throttle_mult, rolling_sharpe, structural_break_z, ts_ms
                FROM alpha_decay_strategy_metrics
                WHERE ts_ms IN (
                  SELECT MAX(ts_ms)
                  FROM alpha_decay_strategy_metrics
                  GROUP BY strategy_name
                )
                ORDER BY severity DESC, strategy_name ASC
                LIMIT 20
                """
            ).fetchall() or []

            rows.append({"key": "alpha_decay_strategy_rows", "value": str(len(decay_rows))})

            for strategy_name, severity, throttle_mult, rolling_sharpe, structural_break_z, ts_ms in decay_rows:
                rows.append({
                    "key": f"alpha_decay:{strategy_name}",
                    "value": (
                        f"severity={severity or 'ok'} "
                        f"throttle_mult={float(throttle_mult or 1.0)} "
                        f"rolling_sharpe={float(rolling_sharpe or 0.0)} "
                        f"structural_break_z={float(structural_break_z or 0.0)} "
                        f"ts_ms={int(ts_ms or 0)}"
                    ),
                })

        return {"ok": True, "rows": rows}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_BROKER_FAILED", e, endpoint="api_get_broker")
        return {"ok": False, "error": str(e), "rows": []}
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_broker")


def api_get_strategy_metrics(parsed, _ctx=None):
    limit_s = _qs_value(parsed, "limit", "50")
    try:
        limit = max(1, min(500, int(limit_s)))
    except Exception:
        limit = 50

    con = None
    try:
        con = _dashboard_db_connect()
        if not _dashboard_table_exists(con, "strategy_metrics"):
            return []

        cols = set(_dashboard_table_columns(con, "strategy_metrics"))
        strategy_col = "strategy_name" if "strategy_name" in cols else ("strategy" if "strategy" in cols else None)
        ts_col = "ts_ms" if "ts_ms" in cols else None
        metrics_col = "metrics_json" if "metrics_json" in cols else None
        if not strategy_col or not ts_col or not metrics_col:
            return []
        window_expr = "window_days" if "window_days" in cols else "0 AS window_days"
        rows = con.execute(
            f"""
            SELECT {strategy_col} AS strategy_name, {window_expr}, {ts_col} AS ts_ms, {metrics_col} AS metrics_json
            FROM strategy_metrics
            ORDER BY {ts_col} DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []

        out = []
        for strategy_name, window_days, ts_ms, metrics_json in rows:
            try:
                m = _json_dict(_dashboard_parse_json(metrics_json, {}))
                out.append({
                    "strategy": str(strategy_name or ""),
                    "window_days": int(window_days or 0),
                    "net_calmar": float(m.get("net_calmar", m.get("calmar", 0.0)) or 0.0),
                    "sharpe": float(m.get("sharpe", 0.0) or 0.0),
                    "turnover": float(m.get("turnover", m.get("turnover_daily", 0.0)) or 0.0),
                    "alpha_decay_rolling_sharpe": float(m.get("alpha_decay_rolling_sharpe", 0.0) or 0.0),
                    "alpha_decay_half_life_buckets": (
                        None
                        if m.get("alpha_decay_half_life_buckets") is None
                        else float(m.get("alpha_decay_half_life_buckets", 0.0) or 0.0)
                    ),
                    "alpha_decay_structural_break_z": float(m.get("alpha_decay_structural_break_z", 0.0) or 0.0),
                    "alpha_decay_severity": str(m.get("alpha_decay_severity", "ok") or "ok"),
                    "alpha_decay_severity_score": float(m.get("alpha_decay_severity_score", 0.0) or 0.0),
                    "alpha_decay_throttle_mult": float(m.get("alpha_decay_throttle_mult", 1.0) or 1.0),
                    "alpha_decay_n_obs": int(m.get("alpha_decay_n_obs", 0) or 0),
                    "ts_ms": int(ts_ms or 0),
                })
            except Exception as e:
                _warn_nonfatal(
                    "DASHBOARD_SERVER_STRATEGY_METRICS_ROW_FAILED",
                    e,
                    endpoint="api_get_strategy_metrics",
                    strategy_name=str(strategy_name or ""),
                )
                continue
        return out
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_STRATEGY_METRICS_FAILED", e, endpoint="api_get_strategy_metrics")
        return []
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_strategy_metrics")

def api_get_alpha_decay(parsed, _ctx=None):
    limit_s = _qs_value(parsed, "limit", "50")
    try:
        limit = max(1, min(500, int(limit_s)))
    except Exception:
        limit = 50

    con = None
    try:
        con = _dashboard_db_connect()

        runtime = {}
        if _dashboard_table_exists(con, "alpha_decay_runtime_history"):
            row = con.execute(
                """
                SELECT ts_ms, status, min_throttle_mult, severe_count, warn_count, detail_json
                FROM alpha_decay_runtime_history
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
            if row:
                runtime = {
                    "ts_ms": int(row[0] or 0),
                    "status": str(row[1] or "ok"),
                    "min_throttle_mult": float(row[2] or 1.0),
                    "severe_count": int(row[3] or 0),
                    "warn_count": int(row[4] or 0),
                    "detail": _dashboard_parse_json(row[5], {}),
                }

        strategies = []
        if _dashboard_table_exists(con, "alpha_decay_strategy_metrics"):
            rows = con.execute(
                """
                SELECT m.strategy_name,
                       m.ts_ms,
                       m.window_days,
                       m.bucket_s,
                       m.rolling_sharpe,
                       m.half_life_buckets,
                       m.half_life_seconds,
                       m.structural_break_z,
                       m.severity,
                       m.severity_score,
                       m.throttle_mult,
                       m.n_obs,
                       m.detail_json
                FROM alpha_decay_strategy_metrics m
                JOIN (
                  SELECT strategy_name, MAX(ts_ms) AS ts_ms
                  FROM alpha_decay_strategy_metrics
                  GROUP BY strategy_name
                ) t
                ON t.strategy_name=m.strategy_name AND t.ts_ms=m.ts_ms
                ORDER BY m.ts_ms DESC, m.strategy_name ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall() or []

            for row in rows:
                strategies.append({
                    "strategy": str(row[0] or ""),
                    "ts_ms": int(row[1] or 0),
                    "window_days": int(row[2] or 0),
                    "bucket_s": int(row[3] or 0),
                    "rolling_sharpe": float(row[4] or 0.0),
                    "half_life_buckets": (None if row[5] is None else float(row[5] or 0.0)),
                    "half_life_seconds": (None if row[6] is None else float(row[6] or 0.0)),
                    "structural_break_z": float(row[7] or 0.0),
                    "severity": str(row[8] or "ok"),
                    "severity_score": float(row[9] or 0.0),
                    "throttle_mult": float(row[10] or 1.0),
                    "n_obs": int(row[11] or 0),
                    "detail": _dashboard_parse_json(row[12], {}),
                })

        return {
            "ok": True,
            "runtime": runtime,
            "strategies": strategies,
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_ALPHA_DECAY_FAILED", e, endpoint="api_get_alpha_decay")
        return {"ok": False, "error": str(e), "runtime": {}, "strategies": []}
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_alpha_decay")

def api_get_reconcile_broker_backtest(_parsed=None, _ctx=None):
    con = None
    try:
        con = _dashboard_db_connect()
        from engine.runtime.equity_drift import get_current_equity_drift

        return get_current_equity_drift(con)
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_RECONCILE_BROKER_BACKTEST_FAILED", e, endpoint="api_get_reconcile_broker_backtest")
        return {
            "ok": False,
            "error": str(e),
            "resolved": False,
            "acked": False,
            "equity_diff_level": "UNKNOWN",
            "reason": str(e),
        }
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_reconcile_broker_backtest")


def api_get_equity_drift(parsed, _ctx=None):
    limit_s = _qs_value(parsed, "limit", "500")
    try:
        limit = max(1, min(5000, int(limit_s)))
    except Exception:
        limit = 500

    con = None
    try:
        con = _dashboard_db_connect()
        points = []

        if _dashboard_table_exists(con, "equity_drift"):
            cur = con.execute(
                """
                SELECT *
                FROM equity_drift
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            rows = cur.fetchall() or []
            cols = [d[0] for d in (cur.description or [])]

            for row in reversed(rows):
                rec = dict(zip(cols, row))
                diff_pct = rec.get("diff_equity_pct")
                if diff_pct is None:
                    diff_pct = rec.get("diff_pct")
                diff_abs = rec.get("diff_equity")
                if diff_abs is None:
                    diff_abs = rec.get("diff_abs")

                points.append({
                    "ts_ms": int(rec.get("ts_ms") or 0),
                    "diff_equity_pct": float(diff_pct or 0.0),
                    "diff_equity": float(diff_abs or 0.0),
                    "level": str(rec.get("level") or ""),
                    "broker_equity": float(rec.get("broker_equity") or rec.get("equity_live") or 0.0),
                    "backtest_equity": float(rec.get("backtest_equity") or rec.get("equity_bt") or 0.0),
                })

        return {"ok": True, "points": points}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_EQUITY_DRIFT_FAILED", e, endpoint="api_get_equity_drift")
        return {"ok": False, "error": str(e), "points": []}
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_equity_drift")


def api_get_temporal_shadow_eval(parsed, _ctx=None):
    limit_s = _qs_value(parsed, "limit", "200")
    try:
        limit = max(1, min(5000, int(limit_s)))
    except Exception:
        limit = 200

    con = None
    try:
        con = _dashboard_db_connect()
        if not _dashboard_table_exists(con, "temporal_shadow_eval"):
            return []

        rows = con.execute(
            """
            SELECT
              symbol,
              COALESCE(key_type, 'symbol') AS key_type,
              COALESCE(key, symbol) AS key,
              horizon_s,
              ts_ms,
              n,
              rmse,
              baseline_rmse,
              directional_acc,
              baseline_directional_acc,
              COALESCE(rmse_improvement, 0.0) AS rmse_improvement,
              COALESCE(diracc_delta, 0.0) AS diracc_delta,
              COALESCE(capital_efficiency, json_extract(detail_json, '$.capital_efficiency')) AS capital_efficiency,
              COALESCE(drawdown_contribution, json_extract(detail_json, '$.drawdown_contribution')) AS drawdown_contribution,
              COALESCE(avg_slippage_impact, json_extract(detail_json, '$.avg_slippage_impact')) AS avg_slippage_impact,
              pass_all,
              detail_json
            FROM temporal_shadow_eval
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []

        out = []
        for r in rows:
            out.append({
                "symbol": str(r[0] or ""),
                "key_type": str(r[1] or "symbol"),
                "key": str(r[2] or ""),
                "horizon_s": int(r[3] or 0),
                "ts_ms": int(r[4] or 0),
                "n": int(r[5] or 0),
                "rmse": float(r[6] or 0.0),
                "baseline_rmse": float(r[7] or 0.0),
                "directional_acc": float(r[8] or 0.0),
                "baseline_directional_acc": float(r[9] or 0.0),
                "rmse_improvement": float(r[10] or 0.0),
                "diracc_delta": float(r[11] or 0.0),
                "capital_efficiency": float(r[12] or 0.0),
                "drawdown_contribution": float(r[13] or 0.0),
                "avg_slippage_impact": float(r[14] or 0.0),
                "pass_all": bool(int(r[15] or 0)),
                "detail": _dashboard_parse_json(r[16], {}),
            })
        return out
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_PROMOTION_CANDIDATES_FAILED", e, endpoint="api_get_temporal_shadow_eval")
        return []
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_temporal_shadow_eval")


def api_get_promotion_audit(parsed, _ctx=None):
    limit_s = _qs_value(parsed, "limit", "200")
    try:
        limit = max(1, min(5000, int(limit_s)))
    except Exception:
        limit = 200

    con = None
    try:
        con = _dashboard_db_connect()
        if not _dashboard_table_exists(con, "model_promotion_audit"):
            return []

        rows = con.execute(
            """
            SELECT ts_ms, actor, action, model_name, regime, reason_json
            FROM model_promotion_audit
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []

        out = []
        for r in rows:
            reason = _dashboard_parse_json(r[5], {})
            out.append({
                "ts_ms": int(r[0] or 0),
                "actor": str(r[1] or ""),
                "action": str(r[2] or ""),
                "model_name": str(r[3] or ""),
                "regime": ("" if r[4] is None else str(r[4])),
                "reason": reason,
                "causal_scores": reason.get("causal_scores", {}) if isinstance(reason, dict) else {},
            })
        return out
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_PROMOTION_AUDIT_FAILED", e, endpoint="api_get_promotion_audit")
        return []
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_promotion_audit")


def api_get_causal_scores(parsed, _ctx=None):
    limit_s = _qs_value(parsed, "limit", "200")
    feature = str(_qs_value(parsed, "feature", "") or "").strip()
    target = str(_qs_value(parsed, "target", "") or "").strip()
    window = str(_qs_value(parsed, "window", "") or "").strip()
    try:
        limit = max(1, min(5000, int(limit_s)))
    except Exception:
        limit = 200

    con = None
    try:
        con = _dashboard_db_connect()
        if not _dashboard_table_exists(con, "causal_scores"):
            return []

        where = []
        params = []
        if feature:
            where.append("feature=?")
            params.append(str(feature))
        if target:
            where.append("target=?")
            params.append(str(target))
        if window:
            where.append("window=?")
            params.append(str(window))
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        rows = con.execute(
            f"""
            SELECT cs.feature, cs.target, cs.window, cs.ts, cs.granger_p,
                   cs.granger_lag, cs.dowhy_effect, cs.dowhy_p, cs.score, cs.decision
            FROM causal_scores cs
            JOIN (
                SELECT feature, target, window, MAX(ts) AS ts
                FROM causal_scores
                {where_sql}
                GROUP BY feature, target, window
            ) latest
              ON latest.feature=cs.feature
             AND latest.target=cs.target
             AND latest.window=cs.window
             AND latest.ts=cs.ts
            ORDER BY cs.ts DESC, cs.score ASC, cs.feature ASC
            LIMIT ?
            """,
            tuple(params + [int(limit)]),
        ).fetchall() or []

        return [
            {
                "feature": str(row[0] or ""),
                "target": str(row[1] or ""),
                "window": str(row[2] or ""),
                "ts": int(row[3] or 0),
                "granger_p": row[4],
                "granger_lag": int(row[5] or 0),
                "dowhy_effect": row[6],
                "dowhy_p": row[7],
                "score": row[8],
                "decision": str(row[9] or ""),
            }
            for row in rows
        ]
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_CAUSAL_SCORES_FAILED", e, endpoint="api_get_causal_scores")
        return []
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_causal_scores")

from engine.api.api_system import (
    api_get_health,
    api_get_liveness,
    api_get_status,
    api_get_system_state,
    api_get_competition_view,
    api_get_replay_freshness,
    api_get_attribution_quality,
    api_get_readiness,
    api_get_runtime_health,
    api_get_trading_readiness,
    api_get_preflight_report,
    api_get_runtime_watchdogs,
    api_get_service_status,
    api_get_provider_telemetry,
    api_get_supervisor_diagnostics,
    api_get_support_snapshot,
    api_post_self_repair,
    api_get_telemetry,
    api_get_telemetry_history,
    api_get_execution_barrier,
    api_get_monte_carlo_risk,
    api_get_drift_explainer,
    api_get_supervisor_status,
    api_get_runtime_config,
    api_get_ingestion_status,
    api_get_portfolio_risk,
)

from engine.api.api_operator_handlers import (
    api_get_operator_summary,
    api_get_operator_status,
    api_get_operator_bootstrap_status,
    api_get_operator_preflight,
    api_post_operator_start,
    api_post_operator_bootstrap,
    api_post_operator_stop,
    api_post_operator_restart,
    api_post_operator_restart_feeds,
    api_post_operator_emergency_stop,
    api_post_operator_autofix,
    api_post_operator_clear_last_error,
    api_get_operator_logs,
    api_get_operator_stderr_tail,
    api_get_operator_market_data,
    api_get_operator_strategy_decisions,
    api_get_operator_institutional_check,
)

try:
    from engine.api.api_market import (
        api_get_market_candles,
        api_get_market_stream,
    )
except Exception:
    api_get_market_candles = _unavailable("api_get_market_candles")
    api_get_market_stream = _unavailable("api_get_market_stream")

try:
    from engine.api.api_replay import api_get_replay_day
except Exception:
    api_get_replay_day = _unavailable("api_get_replay_day")

try:
    from engine.terminal.api.api_terminal import (
        api_get_terminal_watchlist,
        api_get_terminal_snapshot,
        api_get_terminal_positions,
        api_get_terminal_orders,
        api_get_terminal_fills,
        api_get_terminal_equity,
        api_get_terminal_markers,
    )
except Exception:
    api_get_terminal_watchlist = _unavailable("api_get_terminal_watchlist")
    api_get_terminal_snapshot = _unavailable("api_get_terminal_snapshot")
    api_get_terminal_positions = _unavailable("api_get_terminal_positions")
    api_get_terminal_orders = _unavailable("api_get_terminal_orders")
    api_get_terminal_fills = _unavailable("api_get_terminal_fills")
    api_get_terminal_equity = _unavailable("api_get_terminal_equity")
    api_get_terminal_markers = _unavailable("api_get_terminal_markers")

try:
    from engine.terminal.api.api_terminal_orders import (
        api_post_terminal_order,
        api_post_terminal_flatten,
    )
except Exception:
    api_post_terminal_order = _unavailable("api_post_terminal_order")
    api_post_terminal_flatten = _unavailable("api_post_terminal_flatten")

# ---- SCHEMA REPAIR (Operator controlled) ----
_repair_schema_run = None

try:
    from engine.runtime.jobs.repair_schema import run as _repair_schema_run
except Exception:
    _repair_schema_run = None



def api_get_pnl(_parsed, _ctx=None):
    try:
        model_id = str(_qs(_parsed, "model_id", "") or "").strip()
        from engine.runtime.position_store import get_pnl_snapshot
        data = get_pnl_snapshot(model_id=model_id or None) or {}
        return {
            "ok": True,
            "error": None,
            "meta": {
                "ready": bool(data),
                "count": int(len(data)) if isinstance(data, dict) else 0,
            },
            "data": data,
            "total": data.get("total"),
            "unrealized": data.get("unrealized"),
            "realized": data.get("realized"),
            "model_id": model_id or None,
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_NEWS_SENTIMENT_FAILED", e, endpoint="api_get_news_sentiment")
        return {
            "ok": False,
            "error": str(e),
            "meta": {"ready": False, "count": 0},
            "data": {},
            "total": None,
            "unrealized": None,
            "realized": None,
        }


def api_get_news_latest(parsed, _ctx=None):
    con = None
    try:
        qs = _qs_dict(parsed)
        limit = max(1, min(200, int(qs.get("limit", "50") or "50")))

        con = _dashboard_db_connect()
        if not _dashboard_table_exists(con, "events"):
            return {
                "ok": True,
                "error": None,
                "meta": {"ready": False, "count": 0, "reason": "events_table_missing"},
                "items": [],
            }

        rows = con.execute(
            """
            SELECT ts_ms, source, title, symbol, meta_json
            FROM events
            WHERE COALESCE(event_type, 'news') = 'news'
            ORDER BY ts_ms DESC, id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []

        items = []
        for ts_ms, source, title, symbol, meta_json in rows:
            meta = _dashboard_parse_json(meta_json, {})
            resolved_symbol = str(symbol or "").strip().upper()
            if (not resolved_symbol) and isinstance(meta, dict):
                resolved_symbol = str(
                    meta.get("symbol")
                    or meta.get("ticker")
                    or meta.get("asset")
                    or meta.get("instrument")
                    or ""
                ).strip().upper()
            items.append(
                {
                    "ts_ms": int(ts_ms or 0),
                    "symbol": resolved_symbol,
                    "title": str(title or ""),
                    "source": str(source or ""),
                }
            )

        return {
            "ok": True,
            "error": None,
            "meta": {"ready": bool(items), "count": int(len(items))},
            "items": items,
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_NEWS_SENTIMENT_SERIES_FAILED", e, endpoint="api_get_news_sentiment_series")
        return {
            "ok": False,
            "error": str(e),
            "meta": {"ready": False, "count": 0},
            "items": [],
        }
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_news_sentiment")


def api_get_news_sentiment(parsed, _ctx=None):
    con = None
    try:
        qs = _qs_dict(parsed)
        limit = max(1, min(500, int(qs.get("limit", "200") or "200")))

        con = _dashboard_db_connect()
        if not _dashboard_table_exists(con, "social_features"):
            return {
                "ok": True,
                "error": None,
                "meta": {"ready": False, "count": 0, "reason": "social_features_table_missing"},
                "series": [],
            }

        rows = con.execute(
            """
            SELECT bucket_ts_ms, AVG(sentiment_mean) AS sentiment
            FROM social_features
            GROUP BY bucket_ts_ms
            ORDER BY bucket_ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []

        rows = list(reversed(rows))
        series = [
            {
                "ts_ms": int(r[0] or 0),
                "sentiment": float(r[1] or 0.0),
            }
            for r in rows
        ]

        return {
            "ok": True,
            "error": None,
            "meta": {"ready": bool(series), "count": int(len(series))},
            "series": series,
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_NEWS_SENTIMENT_SERIES_FAILED", e, endpoint="api_get_news_sentiment_series")
        return {
            "ok": False,
            "error": str(e),
            "meta": {"ready": False, "count": 0},
            "series": [],
        }
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_news_sentiment_series")


# ---------------------------------------------------
# UI hard-deps missing from ROUTE_SPECS_* in this repo
# ---------------------------------------------------




def _alert_id_from_request(parsed, body=None):
    value = _qs_value(parsed, "id", "")
    if not value and isinstance(body, dict):
        candidate = body.get("alert_id", body.get("id"))
        if candidate is not None:
            value = str(candidate)
    try:
        return int(str(value).strip())
    except Exception as e:
        _warn_nonfatal(
            "DASHBOARD_SERVER_ALERT_ID_PARSE_FAILED",
            e,
            value=str(value),
        )
        return 0


def _dashboard_load_alert_state(con, alert_id: int) -> dict[str, Any]:
    state: dict[str, Any] = {
        "status": "active",
        "acked": False,
        "acked_by": "",
        "acked_ts_ms": None,
        "ack_source": "",
        "resolved": False,
        "resolved_by": "",
        "resolved_ts_ms": None,
        "resolved_reason": "",
        "resolve_source": "",
    }
    alert_key = int(alert_id or 0)
    if alert_key <= 0:
        return state

    if _dashboard_table_exists(con, "alert_acks"):
        try:
            row = con.execute(
                """
                SELECT acked_ts_ms, acked_by, source
                FROM alert_acks
                WHERE alert_id = ?
                LIMIT 1
                """,
                (alert_key,),
            ).fetchone()
            if row:
                state.update({
                    "acked": True,
                    "acked_ts_ms": int(row[0] or 0) if row[0] is not None else None,
                    "acked_by": str(row[1] or ""),
                    "ack_source": str(row[2] or ""),
                })
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_ALERT_ACK_STATE_FAILED", e, alert_id=alert_key)

    if _dashboard_table_exists(con, "alert_resolutions"):
        try:
            row = con.execute(
                """
                SELECT resolved_ts_ms, resolved_by, reason, source
                FROM alert_resolutions
                WHERE alert_id = ?
                LIMIT 1
                """,
                (alert_key,),
            ).fetchone()
            if row:
                state.update({
                    "status": "resolved",
                    "resolved": True,
                    "resolved_ts_ms": int(row[0] or 0) if row[0] is not None else None,
                    "resolved_by": str(row[1] or ""),
                    "resolved_reason": str(row[2] or ""),
                    "resolve_source": str(row[3] or ""),
                })
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_ALERT_RESOLUTION_STATE_FAILED", e, alert_id=alert_key)

    return state


def api_post_alert_ack(parsed, body=None, _ctx=None):
    alert_id = _alert_id_from_request(parsed, body)
    if alert_id <= 0:
        return {"ok": False, "error": "missing_id"}
    payload = body if isinstance(body, dict) else {}
    actor = str(payload.get("actor") or payload.get("who") or "operator").strip() or "operator"
    source = str(payload.get("source") or "dashboard").strip() or "dashboard"
    try:
        from engine.api.api_write import ack_alert

        return ack_alert(alert_id, who=actor, source=source)
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_ALERT_ACK_FAILED", e, alert_id=alert_id)
        return {"ok": False, "error": str(e)}


def api_post_alert_resolve(parsed, body=None, _ctx=None):
    alert_id = _alert_id_from_request(parsed, body)
    if alert_id <= 0:
        return {"ok": False, "error": "missing_id"}
    payload = body if isinstance(body, dict) else {}
    actor = str(payload.get("actor") or payload.get("who") or "operator").strip() or "operator"
    reason = str(payload.get("reason") or "").strip()
    source = str(payload.get("source") or "dashboard").strip() or "dashboard"
    try:
        from engine.api.api_write import resolve_alert

        return resolve_alert(alert_id, who=actor, reason=reason, source=source)
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_ALERT_RESOLVE_FAILED", e, alert_id=alert_id)
        return {"ok": False, "error": str(e)}


def api_get_alert_by_id(parsed, _ctx=None):
    alert_id = _qs_value(parsed, "id", "")
    if not alert_id:
        return {"ok": False, "error": "missing_id"}

    try:
        from engine.api.internal_access import db_connect
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_ALERT_BY_ID_DB_IMPORT_FAILED", e, endpoint="api_get_alert_by_id")
        return {"ok": False, "error": "db_unavailable"}

    conn = None
    try:
        conn = db_connect(readonly=True)
        cur = conn.cursor()
        cur.execute("SELECT * FROM alerts WHERE id = ? LIMIT 1", (alert_id,))
        row = cur.fetchone()
        if not row:
            return {"ok": False, "error": "not_found", "id": alert_id}

        cols = [d[0] for d in cur.description] if cur.description else []
        out = dict(zip(cols, row)) if cols else {"row": row}
        try:
            explain = json.loads(out.get("explain_json") or "{}")
            if not isinstance(explain, dict):
                explain = {}
        except Exception:
            explain = {}
        explain = _json_dict(explain)
        engine_blob = _json_dict(explain.get("confidence_engine"))
        model_intent = _json_dict(explain.get("model_intent"))

        def _pick_float(*values):
            for value in values:
                try:
                    num = float(value)
                except Exception as e:
                    _warn_nonfatal(
                        "DASHBOARD_SERVER_ALERT_BY_ID_FLOAT_PARSE_FAILED",
                        e,
                        value=repr(value),
                    )
                    continue
                if num == num:
                    return float(num)
            return None

        out["confidence_raw"] = _pick_float(
            engine_blob.get("raw_confidence"),
            explain.get("confidence_raw"),
            model_intent.get("confidence_raw"),
        )
        out["prediction_strength"] = _pick_float(
            engine_blob.get("prediction_strength"),
            explain.get("prediction_strength"),
            model_intent.get("prediction_strength"),
            explain.get("score"),
        )
        prediction_explanation = _json_dict(explain.get("prediction_explanation"))
        if prediction_explanation:
            out["prediction_explanation"] = prediction_explanation
            out["explanation_type"] = str(prediction_explanation.get("explanation_type") or "")
            out["top_explanation_features"] = [
                dict(item)
                for item in list(prediction_explanation.get("top_features") or [])[:10]
                if isinstance(item, dict)
            ]
        out.update(_dashboard_load_alert_state(conn, int(alert_id)))
        return {"ok": True, "id": alert_id, "alert": out}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_ALERT_DETAIL_FAILED", e, endpoint="api_get_alert_detail")
        return {"ok": False, "error": str(e)}
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_alert_detail")


def api_get_promotion_status(_parsed=None, _ctx=None):
    try:
        from engine.api.api_governance import get_promotion_status as _get_status
        from engine.strategy.promotion_guard import promotion_allowed as _promotion_allowed
        from engine.model_registry import get_stage_latest as _get_stage_latest
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_PROMOTION_STATUS_IMPORT_FAILED", e)
        return {"ok": False, "error": f"promotion_status_unavailable:{e}"}

    try:
        status = _get_status() or {}

        allowed = False
        reason = {}
        try:
            pa = _promotion_allowed()
            if isinstance(pa, tuple) and len(pa) >= 2:
                allowed = bool(pa[0])
                reason = pa[1] or {}
            else:
                allowed = bool(pa)
                reason = {}
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_PROMOTION_GUARD_FAILED", e)
            allowed = False
            reason = {"blockers": ["promotion_guard_error"]}

        champion = None
        try:
            champion = _get_stage_latest("embed_regressor", "champion")
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_CHAMPION_LOOKUP_FAILED", e)
            champion = None

        enabled = bool(status.get("enabled", True))

        return {
            "ok": True,
            "enabled": enabled,
            "allowed": bool(allowed),
            "training_allowed": bool(allowed),
            "promotion_enabled_db": "1" if enabled else "0",
            "updated_ts_ms": int(status.get("updated_ts_ms") or 0),
            "reason": reason or {},
            "current_champion": champion,
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_PROMOTION_STATUS_FAILED", e)
        return {"ok": False, "error": str(e)}


def api_get_promotion_explain(_parsed=None, _ctx=None):
    try:
        from engine.api.api_governance import get_promotion_explain as _impl
        return _impl()
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_PROMOTION_EXPLAIN_FAILED", e)
        return {"ok": False, "error": f"promotion_explain_unavailable:{e}"}


def api_get_governance_summary(_parsed=None, _ctx=None):
    try:
        from engine.api.api_governance import get_governance_summary as _impl
        return _impl()
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_GOVERNANCE_SUMMARY_FAILED", e)
        return {"ok": False, "error": f"governance_summary_unavailable:{e}"}


def _confirmation_error(body: Any, expected: str) -> Optional[dict[str, Any]]:
    payload = _json_dict(body)
    actual = str(payload.get("confirm") or "").strip()
    if actual == str(expected):
        return None
    return {
        "ok": False,
        "error": "confirmation_required",
        "required_confirm": str(expected),
        "http_status": 422,
    }


def api_post_promotion_enable(parsed=None, _body=None, _ctx=None):
    denied = _confirmation_error(_body, "PROMOTION")
    if denied:
        return denied
    try:
        from engine.api.api_write import set_promotion_enabled
        body = _json_dict(_body)
        on = str(body.get("on") if "on" in body else _qs_value(parsed, "on", "1"))
        return set_promotion_enabled(on)
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_PROMOTION_ENABLE_FAILED", e)
        return {"ok": False, "error": str(e)}


def api_get_promotion_enable(_parsed=None, _ctx=None):
    return {"ok": False, "error": "method_not_allowed", "http_status": 405}


def api_post_system_fix(_parsed=None, _body=None, _ctx=None):
    denied = _confirmation_error(_body, "SYSTEM_FIX")
    if denied:
        return denied
    actions = []

    try:
        if _repair_schema_run is not None:
            try:
                repair_result = _repair_schema_run()
                actions.append({
                    "step": "repair_schema",
                    "ok": True,
                    "result": repair_result,
                })
            except Exception as e:
                actions.append({
                    "step": "repair_schema",
                    "ok": False,
                    "error": str(e),
                })
        else:
            actions.append({
                "step": "repair_schema",
                "ok": False,
                "error": "repair_schema_unavailable",
            })

        if "train_size_policy" in ALLOWED_JOBS:
            try:
                train_result = _jobs_manager().start("train_size_policy")
                actions.append({
                    "step": "train_size_policy",
                    "ok": bool((train_result or {}).get("ok")),
                    "started": bool((train_result or {}).get("ok")),
                    "result": train_result,
                })
            except Exception as e:
                actions.append({
                    "step": "train_size_policy",
                    "ok": False,
                    "error": str(e),
                })

        return {
            "ok": all(bool(a.get("ok")) for a in actions if a.get("step") != "train_size_policy") if actions else True,
            "actions": actions,
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_SYSTEM_FIX_FAILED", e, endpoint="api_post_system_fix")
        error_payload = {"ok": False, "error": str(e), "actions": actions}
        return error_payload


def api_get_system_fix(_parsed=None, _ctx=None):
    return {"ok": False, "error": "method_not_allowed", "http_status": 405}


def api_get_champion_rollback(parsed, _ctx=None):
    return {"ok": False, "error": "method_not_allowed", "http_status": 405}


def api_get_size_policy(parsed, _ctx=None):
    try:
        from engine.api.api_dashboard_reads import api_get_size_policy as _impl
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_SIZE_POLICY_IMPORT_FAILED", e)
        return {"ok": False, "error": "size_policy_unavailable"}

    return _call_with_typeerror_fallbacks(
        "api_get_size_policy",
        _impl,
        (parsed, {}),
        (parsed,),
        tuple(),
    )


def api_post_size_policy_train(_parsed, _body=None, _ctx=None):
    denied = _confirmation_error(_body, "TRAIN_SIZE_POLICY")
    if denied:
        return denied
    # Kick off existing job if registered.
    try:
        name = "train_size_policy"
        if name not in ALLOWED_JOBS:
            return {"ok": False, "error": "job_not_registered", "job": name}
        # Start via JobManager directly (same process)
        result = _jobs_manager().start(name)
        if isinstance(result, dict):
            result.setdefault("job", name)
            return result
        return {"ok": True, "job": name, "started": True}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_START_JOB_FAILED", e, endpoint="api_start_job")
        return {"ok": False, "error": str(e)}


def api_get_model_metrics(_parsed, _ctx=None):
    try:
        from engine.strategy.validation import get_model_metrics
        data = get_model_metrics()
        return {"ok": True, "data": data}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_MODEL_METRICS_FAILED", e, endpoint="api_get_model_metrics")
        return {"ok": False, "error": str(e)}


def api_get_execution_overlays(_parsed, _ctx=None):
    con = None
    try:
        con = _dashboard_db_connect()
        out = {
            "ok": True,
            "rows": [],
            "sources": {},
        }

        if _dashboard_table_exists(con, "execution_analytics"):
            row = con.execute(
                """
                SELECT COUNT(*), MAX(ts_ms)
                FROM execution_analytics
                """
            ).fetchone()
            out["sources"]["execution_analytics"] = {
                "rows": int((row or [0, 0])[0] or 0),
                "last_ts_ms": int((row or [0, 0])[1] or 0),
            }

        if _dashboard_table_exists(con, "execution_orders"):
            row = con.execute(
                """
                SELECT COUNT(*), MAX(submit_ts_ms)
                FROM execution_orders
                """
            ).fetchone()
            out["sources"]["execution_orders"] = {
                "rows": int((row or [0, 0])[0] or 0),
                "last_ts_ms": int((row or [0, 0])[1] or 0),
            }

        if _dashboard_table_exists(con, "broker_order_state"):
            row = con.execute(
                """
                SELECT COUNT(*), MAX(updated_ts_ms)
                FROM broker_order_state
                """
            ).fetchone()
            out["sources"]["broker_order_state"] = {
                "rows": int((row or [0, 0])[0] or 0),
                "last_ts_ms": int((row or [0, 0])[1] or 0),
            }

        return out
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_OPERATOR_LOGS_FAILED", e, endpoint="api_get_operator_logs")
        return {"ok": False, "error": str(e), "rows": [], "sources": {}}
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_operator_logs")

def api_get_crash_analytics(parsed, _ctx=None):
    # Reads CRASH_LOG_PATH jsonl written by _write_crash_analytics
    limit_s = _qs_value(parsed, "limit", "100")
    try:
        limit = max(1, min(10000, int(limit_s)))
    except Exception:
        limit = 100

    try:
        if not os.path.exists(CRASH_LOG_PATH):
            return {"ok": True, "rows": [], "path": CRASH_LOG_PATH}
        rows = []
        with open(CRASH_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    rows.append({"raw": line})
        if len(rows) > limit:
            rows = rows[-limit:]
        return {"ok": True, "rows": rows, "path": CRASH_LOG_PATH}
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_CRASH_LOG_FAILED", e, endpoint="api_get_crash_log")
        return {"ok": False, "error": str(e), "path": CRASH_LOG_PATH}

def api_post_self_heal(_parsed, _body=None, _ctx=None):
    ctx = _ctx or {
        "JOBS": JOBS,
        "SUPERVISOR": SUPERVISOR,
        "ORCHESTRATOR": ORCHESTRATOR,
        "API_HANDLERS": API_HANDLERS,
    }
    return api_post_self_repair(_parsed, _body, ctx)
# ------------------------------------------------------
# OPERATOR SUMMARY (Human readable system explanation)
# ------------------------------------------------------

def api_get_market_session(_parsed=None, _ctx=None):
    try:
        now = time.localtime()
        weekday = int(now.tm_wday)  # Mon=0
        hhmm = now.tm_hour * 100 + now.tm_min

        if weekday >= 5:
            state = "CLOSED"
        elif 930 <= hhmm < 1600:
            state = "OPEN"
        else:
            state = "CLOSED"

        return {
            "ok": True,
            "state": state,
            "ts_ms": int(time.time() * 1000),
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_MARKET_SESSION_FAILED", e, endpoint="api_get_market_session")
        return {"ok": False, "error": str(e), "state": "UNKNOWN"}


def api_get_pnl_summary(_parsed=None, _ctx=None):
    try:
        pnl = api_get_pnl(_parsed, _ctx)
        data = (pnl or {}).get("data") or {}
        return {
            "ok": bool((pnl or {}).get("ok")),
            "day_pnl": data.get("day_pnl", data.get("total", 0.0)),
            "daily_pnl": data.get("daily_pnl", data.get("total", 0.0)),
            "total_pnl": data.get("total", 0.0),
            "realized": data.get("realized", 0.0),
            "unrealized": data.get("unrealized", 0.0),
            "ts_ms": int(time.time() * 1000),
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_PNL_SUMMARY_FAILED", e, endpoint="api_get_pnl_summary")
        return {"ok": False, "error": str(e)}


def api_get_risk_summary(_parsed=None, _ctx=None):
    try:
        mc = api_get_monte_carlo_risk(_parsed, _ctx)
        barrier = api_get_execution_barrier(_parsed, _ctx)

        gross_exposure = 0.0
        net_exposure = 0.0
        max_drawdown_pct = 0.0

        if isinstance(mc, dict):
            gross_exposure = float(mc.get("gross_exposure", 0.0) or 0.0)
            net_exposure = float(mc.get("net_exposure", 0.0) or 0.0)
            max_drawdown_pct = float(mc.get("max_drawdown_pct", 0.0) or 0.0)

        return {
            "ok": True,
            "gross_exposure": gross_exposure,
            "net_exposure": net_exposure,
            "max_drawdown_pct": max_drawdown_pct,
            "execution_barrier": barrier,
            "ts_ms": int(time.time() * 1000),
        }
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_RISK_SUMMARY_FAILED", e, endpoint="api_get_risk_summary")
        return {"ok": False, "error": str(e)}


def _capture_ui_metrics_source(name: str, handler, parsed=None, ctx=None):
    try:
        return handler(parsed, ctx)
    except Exception as e:
        if _is_dashboard_storage_unavailable_error(e):
            return _dashboard_storage_unavailable_payload(str(name), e)
        _warn_nonfatal(
            "DASHBOARD_SERVER_UI_METRICS_SOURCE_FAILED",
            e,
            endpoint=str(name),
        )
        return {"ok": False, "error": str(e), "source": str(name)}


def api_get_ui_metrics(parsed=None, _ctx=None):
    try:
        if build_ui_metrics_snapshot is None:
            return {
                "ok": False,
                "error": "ui_metrics_builder_unavailable",
                "schema_version": 1,
            }
        storage_snapshot = dict(_BOOT_DIAGNOSTICS.get("storage") or {})
        if bool(storage_snapshot.get("checked")) and storage_snapshot.get("ok") is False:
            return _dashboard_storage_unavailable_payload("/api/ui/metrics")
        return build_ui_metrics_snapshot(
            pnl=_capture_ui_metrics_source("/api/pnl", api_get_pnl, parsed, _ctx),
            pnl_summary=_capture_ui_metrics_source("/api/pnl/summary", api_get_pnl_summary, parsed, _ctx),
            portfolio=_capture_ui_metrics_source("/api/portfolio", api_get_portfolio, parsed, _ctx),
            risk_summary=_capture_ui_metrics_source("/api/risk/summary", api_get_risk_summary, parsed, _ctx),
            portfolio_risk=_capture_ui_metrics_source("/api/risk/portfolio", api_get_portfolio_risk, parsed, _ctx),
            broker=_capture_ui_metrics_source("/api/broker", api_get_broker, parsed, _ctx),
            terminal_positions=_capture_ui_metrics_source(
                "/api/terminal/positions",
                api_get_terminal_positions,
                parsed,
                _ctx,
            ),
            now_ms=int(time.time() * 1000),
        )
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_UI_METRICS_FAILED", e, endpoint="api_get_ui_metrics")
        return {
            "ok": False,
            "error": str(e),
            "schema_version": 1,
            "pnl": {},
            "exposure": {},
            "positions": {},
            "account": {},
            "risk": {},
            "sources": {},
            "summary": {"degraded": True, "missing_sources": ["ui_metrics"], "stale_sources": []},
        }


def api_get_models_status(_parsed=None, _ctx=None):
    try:
        con = _dashboard_db_connect()
        try:
            model_registry_n = 0
            model_metrics_n = 0

            if _dashboard_table_exists(con, "model_registry"):
                row = con.execute("SELECT COUNT(*) FROM model_registry").fetchone()
                model_registry_n = int((row or [0])[0] or 0)

            if _dashboard_table_exists(con, "model_metrics"):
                row = con.execute("SELECT COUNT(*) FROM model_metrics").fetchone()
                model_metrics_n = int((row or [0])[0] or 0)

            promotion_ready = model_registry_n > 0 and model_metrics_n > 0

            return {
                "ok": True,
                "promotion_ready": bool(promotion_ready),
                "promotion_state": "ready" if promotion_ready else "idle",
                "model_registry_count": model_registry_n,
                "model_metrics_count": model_metrics_n,
                "ts_ms": int(time.time() * 1000),
            }
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("DASHBOARD_SERVER_API_CON_CLOSE_FAILED", e, endpoint="api_get_ui_status")
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_MODELS_STATUS_FAILED", e, endpoint="api_get_models_status")
        return {"ok": False, "error": str(e)}


def api_post_models_promote(_parsed=None, _body=None, _ctx=None):
    try:
        return api_post_promotion_enable(_parsed, _body, _ctx)
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_UI_STATUS_FAILED", e, endpoint="api_get_ui_status")
        return {"ok": False, "error": str(e)}

try:
    from routes.data_sources_routes import (
        api_get_data_source_logs,
        api_get_data_sources,
        api_post_data_source_create,
        api_post_data_source_delete,
        api_post_data_source_disable,
        api_post_data_source_enable,
        api_post_data_source_test,
        api_post_data_source_update,
    )
except Exception:
    api_get_data_sources = None
    api_get_data_source_logs = None
    api_post_data_source_create = None
    api_post_data_source_update = None
    api_post_data_source_delete = None
    api_post_data_source_enable = None
    api_post_data_source_disable = None
    api_post_data_source_test = None

API_HANDLERS = {
    # SYSTEM
    "api_get_kill_switches": api_get_kill_switches,
    "api_get_health": api_get_health,
    "api_get_liveness": api_get_liveness,
    "api_get_status": api_get_status,
    "api_get_system_state": api_get_system_state,
    "api_get_competition_view": api_get_competition_view,
    "api_get_replay_freshness": api_get_replay_freshness,
    "api_get_attribution_quality": api_get_attribution_quality,
    "api_get_readiness": api_get_readiness,
    "api_get_runtime_health": api_get_runtime_health,
    "api_get_trading_readiness": api_get_trading_readiness,
    "api_get_preflight_report": api_get_preflight_report,
    "api_get_runtime_watchdogs": api_get_runtime_watchdogs,
    "api_get_service_status": api_get_service_status,
    "api_get_provider_telemetry": api_get_provider_telemetry,
    "api_get_supervisor_diagnostics": api_get_supervisor_diagnostics,
    "api_get_support_snapshot": api_get_support_snapshot,
    "api_get_telemetry": api_get_telemetry,
    "api_get_telemetry_history": api_get_telemetry_history,
    "api_get_runtime_config": api_get_runtime_config,
    "api_get_supervisor_status": api_get_supervisor_status,
    "api_get_ingestion_status": api_get_ingestion_status,
    "api_get_portfolio_risk": api_get_portfolio_risk,
    "api_get_pnl": api_get_pnl,
    "api_post_repair_schema": api_post_repair_schema,
    "api_post_self_repair": api_post_self_repair,
    "api_post_self_heal": api_post_self_heal,
    "api_get_execution_barrier": api_get_execution_barrier,
    "api_get_db_health": api_get_db_health,
    "api_get_monte_carlo_risk": api_get_monte_carlo_risk,
    "api_get_drift_explainer": api_get_drift_explainer,

    # UI console lifecycle
    "api_get_operator_summary": api_get_operator_summary,
    "api_get_operator_sidecar_status": api_get_operator_sidecar_status,
    "api_get_operator_status": api_get_operator_status,
    "api_get_operator_bootstrap_status": api_get_operator_bootstrap_status,
    "api_get_operator_preflight": api_get_operator_preflight,
    "api_get_operator_logs": api_get_operator_logs,
    "api_get_operator_stderr_tail": api_get_operator_stderr_tail,
    "api_get_operator_market_data": api_get_operator_market_data,
    "api_get_operator_strategy_decisions": api_get_operator_strategy_decisions,
    "api_get_operator_institutional_check": api_get_operator_institutional_check,
    "api_get_schema_audit": api_get_schema_audit,
    "api_post_operator_start": api_post_operator_start,
    "api_post_operator_bootstrap": api_post_operator_bootstrap,
    "api_post_operator_stop": api_post_operator_stop,
    "api_post_operator_restart": api_post_operator_restart,
    "api_post_operator_restart_feeds": api_post_operator_restart_feeds,
    "api_post_operator_emergency_stop": api_post_operator_emergency_stop,
    "api_post_operator_autofix": api_post_operator_autofix,
    "api_post_operator_clear_last_error": api_post_operator_clear_last_error,
    "api_get_server_status": api_get_server_status,
    "api_get_training_status": api_get_training_status,
    "api_post_server_shutdown": api_post_server_shutdown,

    # JOBS
    "api_get_jobs": api_get_jobs,
    "api_post_job_start": api_post_job_start,
    "api_post_job_stop": api_post_job_stop,
    "api_post_pipeline_run": api_post_pipeline_run,
    "api_get_job_log": api_get_job_log,
    "api_get_job_history": api_get_job_history,

    # DATA SOURCES
    "api_get_data_sources": api_get_data_sources,
    "api_get_data_source_logs": api_get_data_source_logs,
    "api_post_data_source_create": api_post_data_source_create,
    "api_post_data_source_update": api_post_data_source_update,
    "api_post_data_source_delete": api_post_data_source_delete,
    "api_post_data_source_enable": api_post_data_source_enable,
    "api_post_data_source_disable": api_post_data_source_disable,
    "api_post_data_source_test": api_post_data_source_test,

    # OPS
    "api_get_alerts": api_get_alerts,
    "api_get_notifications_status": api_get_notifications_status,
    "api_get_feeds": api_get_feeds,
    "api_get_validation": api_get_validation,
    "api_get_model_diagnostics": api_get_model_diagnostics,
    "api_get_model_registry": api_get_model_registry,
    "api_get_model_performance_divergence": api_get_model_performance_divergence,
    "api_get_embed_model_eval": api_get_embed_model_eval,
    "api_get_embed_conf_calib": api_get_embed_conf_calib,
    "api_get_temporal_eval": api_get_temporal_eval,
    "api_get_temporal_models": api_get_temporal_models,
    "api_get_latest_portfolio_backtest": api_get_latest_portfolio_backtest,
    "api_get_execution_metrics": api_get_execution_metrics,
    "api_get_execution_stats": api_get_execution_stats,
    "api_get_execution_metrics_rolling": api_get_execution_metrics_rolling,
    "api_get_execution_metrics_by_symbol": api_get_execution_metrics_by_symbol,
    "api_get_execution_cost_by_confidence": api_get_execution_cost_by_confidence,
    "api_get_execution_advisories": api_get_execution_advisories,
    "api_get_social_features": api_get_social_features,
    "api_get_social_regimes": api_get_social_regimes,
    "api_get_social_blocks": api_get_social_blocks,
    "api_get_news_latest": api_get_news_latest,
    "api_get_news_sentiment": api_get_news_sentiment,
    "api_get_human_alignment_summary": api_get_human_alignment_summary,
    "api_get_weather_snapshot": api_get_weather_snapshot,
    "api_get_weather_alerts": api_get_weather_alerts,
    "api_get_weather_effect": api_get_weather_effect,
    "api_get_confidence_mass": api_get_confidence_mass,
    "api_post_notifications_test": api_post_notifications_test,
    "api_get_relevance_stats": api_get_relevance_stats,
    "api_post_execution_advisory_action": api_post_execution_advisory_action,
    "api_post_rollback": api_post_rollback,

    # UI hard-deps (aliases + additional endpoints)
    "api_get_alert_by_id": api_get_alert_by_id,
    "api_post_alert_ack": api_post_alert_ack,
    "api_post_alert_resolve": api_post_alert_resolve,
    "api_get_recent_decisions": api_get_recent_decisions,
    "api_get_decision_detail": api_get_decision_detail,
    "api_get_audit_records": api_get_audit_records,
    "api_post_ui_interaction": api_post_ui_interaction,
    "api_post_copilot_ask": api_post_copilot_ask,
    "api_get_governance_summary": api_get_governance_summary,
    "api_get_promotion_status": api_get_promotion_status,
    "api_get_promotion_explain": api_get_promotion_explain,
    "api_post_promotion_enable": api_post_promotion_enable,
    "api_get_promotion_enable": api_get_promotion_enable,
    "api_post_system_fix": api_post_system_fix,
    "api_get_system_fix": api_get_system_fix,
    "api_get_size_policy": api_get_size_policy,
    "api_post_size_policy_train": api_post_size_policy_train,
    "api_get_champion_rollback": api_get_champion_rollback,
    "api_get_model_metrics": api_get_model_metrics,
    "api_get_execution_overlays": api_get_execution_overlays,
    "api_get_crash_analytics": api_get_crash_analytics,

    # MARKET
    "api_get_market_candles": api_get_market_candles,
    "api_get_market_stream": api_get_market_stream,
    "api_get_replay_day": api_get_replay_day,

    # TERMINAL
    "api_get_terminal_watchlist": api_get_terminal_watchlist,
    "api_get_terminal_snapshot": api_get_terminal_snapshot,
    "api_get_terminal_positions": api_get_terminal_positions,
    "api_get_terminal_orders": api_get_terminal_orders,
    "api_get_terminal_fills": api_get_terminal_fills,
    "api_get_terminal_equity": api_get_terminal_equity,
    "api_get_terminal_markers": api_get_terminal_markers,

    "api_post_terminal_order": api_post_terminal_order,
    "api_post_terminal_flatten": api_post_terminal_flatten,

    # MISSING OPS / EXECUTION / PORTFOLIO (kept for compatibility; safe stubs if absent)
    "api_get_market_stress": api_get_market_stress,
    "api_get_market_stress_history": api_get_market_stress_history,
    "api_get_portfolio": api_get_portfolio,
    "api_get_portfolio_backtest": api_get_portfolio_backtest,
    "api_get_prices": api_get_prices,
    "api_get_trades": api_get_trades,
    "api_get_broker": api_get_broker,
    "api_get_strategy_status": api_get_strategy_status,
    "api_get_strategy_metrics": api_get_strategy_metrics,
    "api_get_alpha_decay": api_get_alpha_decay,
    "api_get_reconcile_broker_backtest": api_get_reconcile_broker_backtest,
    "api_get_equity_drift": api_get_equity_drift,
    "api_get_temporal_shadow_eval": api_get_temporal_shadow_eval,
    "api_get_promotion_audit": api_get_promotion_audit,
    "api_get_causal_scores": api_get_causal_scores,

    "api_get_market_session": api_get_market_session,
    "api_get_pnl_summary": api_get_pnl_summary,
    "api_get_risk_summary": api_get_risk_summary,
    "api_get_ui_metrics": api_get_ui_metrics,
    "api_get_models_status": api_get_models_status,
    "api_post_models_promote": api_post_models_promote,
}

ROUTE_SPECS = [
    route
    for route in ROUTE_SPECS
    if str(route.get("handler") or "").strip() in API_HANDLERS
]

# ------------------------------------------------------
# SERVER
# ------------------------------------------------------
def _run_dashboard_control_plane():
    """Bind and run the dashboard HTTP server and operator runtime surface.

    Returns
    -------
    None
        This function blocks in the server loop until shutdown is requested.

    Raises
    ------
    Exception
        Propagates startup validation, bind, bootstrap, or serve-loop failures.

    Notes
    -----
    Startup is staged and fail-closed. Runtime architecture validation runs
    before the server is exposed, while heavier runtime bootstrap work is
    deferred until after a successful bind.

    Side Effects
    ------------
    Loads environment variables, mutates lifecycle state, binds the HTTP
    socket, starts runtime jobs/services, and records startup diagnostics.
    """
    _safe_print("[dashboard_server] run_server_enter")
    _update_startup_trace("JOB_REGISTRATION", status="started", detail="dashboard_server.run_server_enter")
    global _HTTPD, _DASHBOARD_HTTP_BOUND
    _DASHBOARD_HTTP_BOUND = False

    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(_BASE_DIR, ".env"))
    except Exception as e:
        log.exception("dashboard_server_dotenv_load_failed: %s", e)

    # Lifecycle state writes are DB-backed; defer them until after the socket is
    # bound so dependency outages do not prevent static dashboard access.

    # ---------------------------------------------------
    # Runtime architecture validation (direct dashboard_server.py boot)
    # ---------------------------------------------------
    try:
        arch_check = validate_runtime_architecture(repo_root=_BASE_DIR)
        if not arch_check.get("ok"):
            raise RuntimeError(
                "runtime_architecture_invalid: "
                + "; ".join(arch_check.get("errors") or [])
            )
        _update_startup_trace("JOB_REGISTRATION", status="ok", detail="runtime_architecture_valid", extra={"errors": list(arch_check.get("errors") or [])})
    except Exception as e:
        _record_startup_failure("JOB_REGISTRATION", e, module="dashboard_server.validate_runtime_architecture", file_path=__file__)
        _update_startup_trace("JOB_REGISTRATION", status="failed", detail=str(e))
        raise

    _update_startup_trace(
        "JOB_REGISTRATION",
        status="ok",
        detail="storage_readiness_deferred_until_after_bind",
    )

    # ---------------------------------------------------
    # RUNTIME BOOTSTRAP (DB + coordination tables)
    # ---------------------------------------------------
    # DEFERRED — bootstrap moved post-bind
    boot = {
        "ok": True,
        "deferred_until_after_bind": True,
        "steps": [],
        "errors": [],
        "ts_ms": int(time.time() * 1000),
    }

    # ---------------------------------------------------
    def _post_bind_boot_safe():
        return _run_dashboard_post_bind_boot_safe(sys.modules[__name__], handler_ctx)



    missing_route_handlers = []
    for route in ROUTE_SPECS:
        handler_name = str(route.get("handler") or "").strip()
        if not handler_name:
            missing_route_handlers.append({
                "method": route.get("method"),
                "path": route.get("path"),
                "handler": handler_name,
                "reason": "blank_handler",
            })
            continue
        if handler_name not in API_HANDLERS or not callable(API_HANDLERS.get(handler_name)):
            missing_route_handlers.append({
                "method": route.get("method"),
                "path": route.get("path"),
                "handler": handler_name,
                "reason": "handler_not_registered",
            })

    _BOOT_DIAGNOSTICS["api_dependencies"] = {
        "started": True,
        "ok": len(missing_route_handlers) == 0,
        "detail": "ok" if not missing_route_handlers else "route_handler_registration_failed",
        "missing_route_handlers": list(missing_route_handlers[:50]),
        "route_count": int(len(ROUTE_SPECS)),
        "handler_count": int(len(API_HANDLERS)),
        "ts_ms": int(time.time() * 1000),
    }
    _publish_boot_diagnostics()

    if missing_route_handlers:
        raise RuntimeError(
            "route_handler_registration_failed: "
            + json.dumps(missing_route_handlers[:50], default=str)
        )

    prebind_gates = assert_prebind_startup_gates(
        repo_root=_BASE_DIR,
        host=host,
        port=port,
        require_ui_assets=True,
        api_dependencies=dict(_BOOT_DIAGNOSTICS.get("api_dependencies") or {}),
    )
    _BOOT_DIAGNOSTICS["prebind_gates"] = dict(prebind_gates or {})
    _publish_boot_diagnostics()

    handler_ctx = {
        "JOBS": JOBS,
        "SUPERVISOR": SUPERVISOR,
        "ORCHESTRATOR": ORCHESTRATOR,
        "ALLOWED_JOBS": ALLOWED_JOBS,
        "API_HANDLERS": API_HANDLERS,
        "STORAGE_REQUIRED_PATHS": list(_STORAGE_REQUIRED_ROUTE_PATHS),
        "STORAGE_REQUEST_TIMEOUT_S": _dashboard_storage_request_timeout_s(),
        "STORAGE_READINESS_CACHE_S": 2.0,

        # operator handler dependencies
        "qs": _qs,
        "_operator_status_payload": _operator_status_payload,
        "_operator_preflight_steps": _operator_preflight_steps,
        "_operator_start_impl": _operator_start_impl,
        "_boot_diagnostics": lambda: dict(_BOOT_DIAGNOSTICS),
        "_tail_text_file": _tail_text_file,
        "_OPERATOR_LOG_PATH": _OPERATOR_LOG_PATH,
        "_OPERATOR_STDERR_LOG_PATH": _OPERATOR_STDERR_LOG_PATH,
    }

    _safe_print("[dashboard_server] build_handler_begin")
    HandlerCls = build_handler(
        ROUTE_SPECS=ROUTE_SPECS,
        API_HANDLERS=API_HANDLERS,
        dashboard_api_token=DASHBOARD_API_TOKEN,
        ctx=handler_ctx,
        static_dir=os.path.join(_BASE_DIR, "ui"),
    )
    HandlerCls = _wrap_operator_console_routes(HandlerCls)
    _safe_print("[dashboard_server] build_handler_ok")

    if HandlerCls is None or not callable(HandlerCls):
        e = RuntimeError(
            "dashboard_handler_construction_failed: "
            f"build_handler returned {type(HandlerCls).__name__}"
        )
        _record_startup_failure("RUNNING", e, module="dashboard_server.build_handler", file_path=__file__)
        _update_startup_trace(
            "RUNNING",
            status="failed",
            detail=str(e),
            extra={
                "host": str(host),
                "port": int(port),
                "route_count": int(len(ROUTE_SPECS)),
                "handler_count": int(len(API_HANDLERS)),
            },
        )
        raise e

    _safe_print(f"[dashboard_server] bind_begin host={host} port={port}")
    log.info("dashboard_server_bind_begin host=%s port=%s", host, port)
    _update_startup_trace("RUNNING", status="started", detail="run_http_server_bind", extra={"host": str(host), "port": int(port)})
    try:
        _HTTPD = run_http_server(host, port, HandlerCls)
        _DASHBOARD_HTTP_BOUND = True
        _safe_print(f"[dashboard_server] bind_ok host={host} port={port}")
    except Exception as e:
        _record_startup_failure("RUNNING", e, module="dashboard_server.run_http_server", file_path=__file__)
        _update_startup_trace("RUNNING", status="failed", detail=f"dashboard_bind_failed:{e}", extra={"host": str(host), "port": int(port)})
        raise

    if not _HTTPD:
        e = RuntimeError(
            "run_http_server returned None (bind failure) "
            f"host={host} port={port} handler={getattr(HandlerCls, '__name__', type(HandlerCls).__name__)}"
        )
        _record_startup_failure("RUNNING", e, module="dashboard_server.run_http_server", file_path=__file__)
        _update_startup_trace(
            "RUNNING",
            status="failed",
            detail=str(e),
            extra={
                "host": str(host),
                "port": int(port),
                "handler_name": getattr(HandlerCls, "__name__", type(HandlerCls).__name__),
            },
        )
        raise e

    _update_startup_trace("RUNNING", status="ok", detail="dashboard_bound", extra={"host": str(host), "port": int(port)})
    log.info("dashboard socket bound at http://%s:%s/ui/dashboard.html (startup pending)", host, port)

    try:
        if mark_dashboard_bound and _dashboard_storage_known_ready():
            mark_dashboard_bound(f"http://{host}:{port}")
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_MARK_DASHBOARD_BOUND_FAILED", e, scope="dashboard_bound")

    try:
        _publish_boot_diagnostics()
        if set_state and _dashboard_storage_known_ready():
            try:
                from engine.runtime.storage_pool import storage_acquire_timeout_override

                timeout_ctx = storage_acquire_timeout_override(_dashboard_storage_request_timeout_s())
            except Exception:
                timeout_ctx = nullcontext()
            with timeout_ctx:
                if not str(meta_get("first_price_ts_ms", "") or "").strip():
                    set_state(WARMING_UP, "dashboard_bound_awaiting_first_price_tick")
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_BOUND_STATE_UPDATE_FAILED", e, scope="dashboard_bound")

    try:
        if append_event and _dashboard_storage_known_ready():
            try:
                from engine.runtime.storage_pool import storage_acquire_timeout_override

                timeout_ctx = storage_acquire_timeout_override(_dashboard_storage_request_timeout_s())
            except Exception:
                timeout_ctx = nullcontext()
            with timeout_ctx:
                append_event(
                    event_type="dashboard_server_bound",
                    event_source="dashboard_server",
                    entity_type="runtime",
                    entity_id="dashboard_server",
                    payload={
                        "host": str(host),
                        "port": int(port),
                        "engine_mode": str(os.environ.get("ENGINE_MODE", "safe") or "safe"),
                        "ts_ms": int(time.time() * 1000),
                    },
                    ts_ms=int(time.time() * 1000),
                    best_effort=True,
                )
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_BOUND_EVENT_APPEND_FAILED", e, scope="dashboard_server_bound")

    def _post_bind_boot():
        return _run_dashboard_post_bind_boot(sys.modules[__name__], handler_ctx)

    _start_background_thread("health_cache_prewarm", _prewarm_health_cache, (handler_ctx,))
    _start_background_thread("post_bind_boot", _post_bind_boot_safe)

    try:
        import signal

        def _shutdown(_sig=None, _frame=None):
            try:
                if append_event:
                    try:
                        from engine.runtime.storage_pool import storage_acquire_timeout_override

                        timeout_ctx = storage_acquire_timeout_override(_dashboard_storage_request_timeout_s())
                    except Exception:
                        timeout_ctx = nullcontext()
                    with timeout_ctx:
                        append_event(
                            event_type="dashboard_server_shutdown_signal",
                            event_source="dashboard_server",
                            entity_type="runtime",
                            entity_id="dashboard_server",
                            payload={
                                "signal": str(_sig) if _sig is not None else "",
                                "ts_ms": int(time.time() * 1000),
                            },
                            ts_ms=int(time.time() * 1000),
                            best_effort=True,
                        )
            except Exception as e:
                log.exception("dashboard_server_shutdown_append_event_failed: %s", e)
            _shutdown_runtime_once(f"signal={_sig}" if _sig is not None else "signal")
            _request_httpd_shutdown(f"signal={_sig}" if _sig is not None else "signal")

        try:
            signal.signal(signal.SIGINT, _shutdown)
        except Exception as e:
            log.exception("dashboard_server_sigint_handler_register_failed: %s", e)
        try:
            signal.signal(signal.SIGTERM, _shutdown)
        except Exception as e:
            log.exception("dashboard_server_sigterm_handler_register_failed: %s", e)
    except Exception as e:
        log.exception("dashboard_server_signal_setup_failed: %s", e)
        raise
    if not _HTTPD:
        raise RuntimeError("HTTP server failed to start")

    try:
        log.info("dashboard_server_serve_forever_enter host=%s port=%s", host, port)
        _HTTPD.serve_forever()
        log.info("dashboard_server_serve_forever_returned host=%s port=%s", host, port)
    finally:
        _shutdown_runtime_once("serve_forever_exit")

        try:
            if _HTTPD:
                _HTTPD.server_close()
        except Exception as e:
            _warn_nonfatal("DASHBOARD_SERVER_CLOSE_FAILED", e, scope="serve_forever_exit")


def run_server():
    from engine.api.server import run_server as _run_server

    return _run_server(dashboard_module=sys.modules[__name__])

def stop_server():
    """Stop the dashboard server and trigger runtime shutdown hooks.

    Returns
    -------
    None

    Side Effects
    ------------
    Requests runtime shutdown exactly once and, when present, calls
    ``HTTPServer.shutdown()`` on the active server instance.
    """
    global _HTTPD
    _shutdown_runtime_once("stop_server")
    try:
        if _HTTPD:
            _HTTPD.shutdown()
    except Exception as e:
        _warn_nonfatal("DASHBOARD_SERVER_SHUTDOWN_FAILED", e, scope="stop_server")



if __name__ == "__main__":
    try:
        run_server()
    except Exception as e:
        try:
            if mark_crash_shutdown:
                mark_crash_shutdown(str(e))
            elif set_state:
                set_state(DEGRADED, "dashboard_crash")
        except Exception as mark_err:
            _warn_nonfatal("DASHBOARD_SERVER_CRASH_MARK_FAILED", mark_err, scope="__main__")

        try:
            import traceback as _tb
            _write_crash_analytics(exit_code=1, err=str(e), tb=_tb.format_exc())
        except Exception:
            _write_crash_analytics(exit_code=1)
        log.exception("dashboard_server crashed")
        raise
