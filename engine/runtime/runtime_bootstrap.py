from __future__ import annotations
"""
Runtime bootstrap (idempotent)

Purpose:
- Centralize DB + coordination table bootstrap
- Keep dashboard_server.py / bootstrap_server.py thin

Notes:
- NO job starts here
- NO schema creation beyond coordination tables
"""

"""
FILE: runtime_bootstrap.py

Runtime subsystem module for `runtime_bootstrap`.
"""

import os
import sys
import time
import json
import logging

# ensure project root is always importable
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _is_missing_optional_module(error: ModuleNotFoundError, module_name: str) -> bool:
    return getattr(error, "name", "") == module_name or f"No module named '{module_name}'" in str(error)

_SAFE_NO_CREDENTIAL_BOOTSTRAP_ENV = {
    "POLYGON_REST_ENABLED": "0",
    "POLYGON_WS_ENABLED": "0",
    "IBKR_ENABLED": "0",
    "CCXT_ENABLED": "0",
    "TRADIER_ENABLED": "0",
    "YFINANCE_ENABLED": "1",
    "LIVE_PRICE_PROVIDER_CHAIN": "yfinance",
    "OPTIONS_PROVIDER_CHAIN": "",
}
_BOOTSTRAP_CREDENTIAL_RUNTIME_ENV_KEYS = (
    "ALPACA_API_KEY",
    "ALPACA_KEY_ID",
    "ALPACA_OAUTH_TOKEN",
    "ALPACA_SECRET",
    "ALPACA_SECRET_KEY",
    "ANTHROPIC_API_KEY",
    "BINANCE_API_KEY",
    "BINANCE_SECRET",
    "BINANCE_SECRET_KEY",
    "CCXT_API_KEY",
    "CCXT_PASSWORD",
    "CCXT_SECRET",
    "COINBASE_API_KEY",
    "COINBASE_API_SECRET",
    "COINBASE_SECRET",
    "FINNHUB_API_KEY",
    "FMP_API_KEY",
    "GROQ_API_KEY",
    "IBKR_CLIENT_ID",
    "IBKR_HOST",
    "IBKR_PASSWORD",
    "IBKR_PORT",
    "IBKR_USERNAME",
    "KRAKEN_API_KEY",
    "KRAKEN_PRIVATE_KEY",
    "OANDA_ACCESS_TOKEN",
    "OANDA_API_KEY",
    "OPENAI_API_KEY",
    "POLYGON_API_KEY",
    "POLYGON_KEY",
    "QUIVER_API_KEY",
    "REDDIT_CLIENT_ID",
    "REDDIT_CLIENT_SECRET",
    "SHARADAR_API_KEY",
    "SIMFIN_API_KEY",
    "TRADIER_API_TOKEN",
)


def _bootstrap_env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name), "")
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _bootstrap_redact(value):
    try:
        from engine.api.redaction import redact_api_payload, redact_string

        if isinstance(value, str):
            return redact_string(value)
        return redact_api_payload(value)
    except Exception as e:
        logging.log(
            logging.WARNING,
            "runtime_bootstrap_redact_failed error=%s",
            f"{type(e).__name__}: {e}",
        )
        return value


def _safe_no_credential_bootstrap_mode() -> bool:
    if _bootstrap_env_flag("ALLOW_CREDENTIAL_DATA_PROVIDERS_IN_SAFE", False):
        return False
    mode = str(os.environ.get("ENGINE_MODE") or "safe").strip().lower()
    execution_mode = str(os.environ.get("EXECUTION_MODE") or "safe").strip().lower()
    broker = str(os.environ.get("BROKER") or "sim").strip().lower()
    broker_name = str(os.environ.get("BROKER_NAME") or broker or "sim").strip().lower()
    if mode != "safe" or execution_mode not in {"safe", "paper", "sim-paper", "sim_paper"}:
        return False
    if broker != "sim" or broker_name != "sim":
        return False
    return bool(
        _bootstrap_env_flag("DISABLE_LIVE_EXECUTION", True)
        and _bootstrap_env_flag("KILL_SWITCH_GLOBAL", True)
    )


def _apply_safe_no_credential_bootstrap_environment() -> None:
    if not _safe_no_credential_bootstrap_mode():
        return
    for key in _BOOTSTRAP_CREDENTIAL_RUNTIME_ENV_KEYS:
        os.environ.pop(str(key), None)
    for key, value in _SAFE_NO_CREDENTIAL_BOOTSTRAP_ENV.items():
        os.environ[str(key)] = str(value)


def _bootstrap_stderr_event(event: str, error: BaseException, **extra) -> None:
    payload = {
        "event": str(event),
        "component": "engine.runtime.runtime_bootstrap",
        "error_type": type(error).__name__,
        "error_message": _bootstrap_redact(str(error)),
        "extra": _bootstrap_redact(dict(extra or {})),
        "ts_ms": int(time.time() * 1000),
    }
    try:
        logging.getLogger("engine.runtime.bootstrap.early").log(
            logging.WARNING,
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
        )
    except Exception as e:
        logging.log(
            logging.WARNING,
            "runtime_bootstrap_early_log_failed event=%s error=%s",
            str(event),
            f"{type(e).__name__}: {e}",
        )
        try:
            os.write(
                2,
                (
                    f"[engine.runtime.runtime_bootstrap] runtime_bootstrap_early_log_failed "
                    f"event={event} error={type(e).__name__}: {e}\n"
                ).encode("utf-8", errors="replace"),
            )
        except Exception:
            logging.log(
                logging.WARNING,
                "runtime_bootstrap_early_log_failed event=%s error=%s",
                str(event),
                f"{type(e).__name__}: {e}",
            )
        return

def _dashboard_route_contract_introspection_enabled() -> bool:
    return str(os.environ.get("DASHBOARD_ROUTE_CONTRACT_INTROSPECTION", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


if not _dashboard_route_contract_introspection_enabled():
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(_PROJECT_ROOT, ".env"), override=False)
    except ModuleNotFoundError as e:
        if not _is_missing_optional_module(e, "dotenv"):
            _bootstrap_stderr_event("runtime_bootstrap_dotenv_load_failed", e)
    except Exception as e:
        _bootstrap_stderr_event("runtime_bootstrap_dotenv_load_failed", e)

from engine.runtime.config_schema import ConfigError, get_runtime_safety_context, load_runtime_config
from engine.runtime.platform import default_data_root

# Ensure DB_PATH exists before importing any storage-backed modules.
_db_path = str(os.environ.get("DB_PATH") or "").strip()
_runtime_safety = get_runtime_safety_context()
_strict_runtime = bool(_runtime_safety.get("strict_runtime"))

if not _db_path:
    if _strict_runtime:
        raise RuntimeError("DB_PATH is required in supervised/prod runtime bootstrap")
    default_db = str(default_data_root())
    try:
        os.makedirs(default_db, exist_ok=True)
    except Exception as e:
        _bootstrap_stderr_event(
            "runtime_bootstrap_default_db_dir_create_failed",
            e,
            default_db=default_db,
        )
    os.environ["DB_PATH"] = default_db

_apply_safe_no_credential_bootstrap_environment()

try:
    load_runtime_config()
except ConfigError as e:
    safe_error = str(_bootstrap_redact(str(e)))
    os.environ["RUNTIME_CONFIG_ERROR"] = safe_error
    if _strict_runtime:
        raise RuntimeError(f"runtime config invalid: {safe_error}") from None

from engine.runtime.logging import get_logger
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import init_db as _init_db
from engine.runtime.storage import connect as _db_connect
from engine.runtime.db_guard import ensure_db_ok


LOG = get_logger("runtime.bootstrap")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.runtime_bootstrap",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


# ----------------------------------------------------------------------
# HEALTH HELPERS
# ----------------------------------------------------------------------

def _db_table_counts():
    # This diagnostic snapshot is used by health/debug endpoints; missing tables
    # degrade gracefully instead of failing the whole bootstrap helper.
    counts = {}
    con = None
    try:
        con = _db_connect(readonly=True)
        for table in (
            "symbols",
            "prices",
            "price_quotes",
            "price_provider_health",
            "events",
            "labels",
            "model_metrics",
            "model_registry",
            "alerts",
            "job_locks",
            "job_history",
            "portfolio_state",
        ):
            try:
                row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[str(table)] = int((row or [0])[0] or 0)
            except Exception as e:
                _warn_nonfatal(
                    "runtime_bootstrap_db_table_count_query_failed",
                    "RUNTIME_BOOTSTRAP_DB_TABLE_COUNT_QUERY_FAILED",
                    e,
                    warn_key=f"runtime_bootstrap_db_table_count:{table}",
                    table=str(table),
                )
                counts[str(table)] = None
        return counts
    except Exception as e:
        _warn_nonfatal(
            "runtime_bootstrap_db_table_counts_failed",
            "RUNTIME_BOOTSTRAP_DB_TABLE_COUNTS_FAILED",
            e,
            warn_key="runtime_bootstrap_db_table_counts_failed",
        )
        return counts
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal(
                "runtime_bootstrap_db_table_counts_close_failed",
                "RUNTIME_BOOTSTRAP_DB_TABLE_COUNTS_CLOSE_FAILED",
                e,
                warn_key="runtime_bootstrap_db_table_counts_close_failed",
            )


def _recent_runtime_errors(limit: int = 10):
    # Prefer alert-style runtime errors when present, but fall back to
    # job_history so older schema variants still surface recent failures.
    rows_out = []
    con = None
    try:
        con = _db_connect(readonly=True)
        try:
            rows = con.execute(
                """
                SELECT ts_ms, severity, message
                FROM alerts
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall() or []

            for row in rows:
                rows_out.append({
                    "ts_ms": int(row[0] or 0),
                    "severity": str(row[1] or ""),
                    "message": str(row[2] or ""),
                })

        except Exception:
            rows = con.execute(
                """
                SELECT ts_ms, job_name, event, detail, exit_code
                FROM job_history
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall() or []

            for row in rows:
                rows_out.append({
                    "ts_ms": int(row[0] or 0),
                    "severity": "WARN",
                    "job_name": str(row[1] or ""),
                    "event": str(row[2] or ""),
                    "message": str(row[3] or ""),
                    "exit_code": row[4],
                })

        try:
            try:
                from engine.runtime.event_log import flush_event_log_buffer

                flush_event_log_buffer(max_batches=64)
            except Exception as e:
                _warn_nonfatal(
                    "runtime_bootstrap_recent_runtime_errors_flush_failed",
                    "RUNTIME_BOOTSTRAP_RECENT_RUNTIME_ERRORS_FLUSH_FAILED",
                    e,
                    warn_key="runtime_bootstrap_recent_runtime_errors_flush_failed",
                )
            rows = con.execute(
                """
                SELECT ts_ms, payload_json
                FROM event_log
                WHERE event_type='runtime_failure'
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall() or []
            for row in rows:
                try:
                    payload = json.loads(row[1] or "{}")
                except Exception:
                    payload = {}
                if not isinstance(payload, dict):
                    payload = {}
                rows_out.append(
                    {
                        "ts_ms": int(row[0] or 0),
                        "severity": "ERROR",
                        "code": str(payload.get("root_cause_code") or ""),
                        "message": str(payload.get("error_message") or ""),
                    }
                )
            rows_out.sort(key=lambda item: int((item or {}).get("ts_ms") or 0), reverse=True)
            rows_out = rows_out[: int(limit)]
        except Exception as e:
            _warn_nonfatal(
                "runtime_bootstrap_recent_runtime_failure_query_failed",
                "RUNTIME_BOOTSTRAP_RECENT_RUNTIME_FAILURE_QUERY_FAILED",
                e,
                warn_key="runtime_bootstrap_recent_runtime_failure_query_failed",
                limit=int(limit),
            )

        return rows_out

    except Exception as e:
        _warn_nonfatal(
            "runtime_bootstrap_recent_runtime_errors_failed",
            "RUNTIME_BOOTSTRAP_RECENT_RUNTIME_ERRORS_FAILED",
            e,
            warn_key=f"runtime_bootstrap_recent_runtime_errors:{limit}",
            limit=int(limit),
        )
        return rows_out
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal(
                "runtime_bootstrap_recent_runtime_errors_close_failed",
                "RUNTIME_BOOTSTRAP_RECENT_RUNTIME_ERRORS_CLOSE_FAILED",
                e,
                warn_key="runtime_bootstrap_recent_runtime_errors_close_failed",
            )


def _build_runtime_health(_parsed=None, ctx=None):
    return {
        "ok": True,
        "db_tables": _db_table_counts(),
        "recent_errors": _recent_runtime_errors(),
        "ts_ms": int(time.time() * 1000),
    }

try:
    from engine.runtime.first_run import bootstrap_first_run
    from engine.runtime.locks import _ensure_job_locks, _ensure_job_history
    from engine.runtime.health import run_preflight
    from engine.runtime.db_repair import repair as repair_db
except Exception:
    sys.path.insert(0, _PROJECT_ROOT)
    from engine.runtime.first_run import bootstrap_first_run
    from engine.runtime.locks import _ensure_job_locks, _ensure_job_history
    from engine.runtime.health import run_preflight
    from engine.runtime.db_repair import repair as repair_db


def bootstrap_runtime(log=None) -> dict:
    """
    Bootstraps runtime prerequisites.
    Safe to call multiple times.
    """
    log = log or get_logger("runtime.bootstrap")
    boot_ts_ms = int(time.time() * 1000)

    out = {
        "ok": True,
        "init_db": False,
        "job_locks": False,
        "job_history": False,
        "event_log": False,
        "crash_recovery": None,
        "startup_preflight": None,
        "startup_repair": None,
        "errors": [],
        "steps": [],
        "ts_ms": boot_ts_ms,
    }

    def _step(name: str, ok: bool, detail=None) -> None:
        item = {
            "name": str(name),
            "ok": bool(ok),
            "detail": detail,
            "ts_ms": int(time.time() * 1000),
        }
        out["steps"].append(item)
        if ok:
            log.info("bootstrap_step_ok %s", name, extra={"event": "bootstrap_step_ok", "extra_json": item})
        else:
            log.error("bootstrap_step_failed %s", name, extra={"event": "bootstrap_step_failed", "extra_json": item})

    log.info(
        "runtime_bootstrap_start",
        extra={
            "event": "runtime_bootstrap_start",
            "extra_json": {
                "db_path": str(os.environ.get("DB_PATH") or ""),
                "engine_mode": str(os.environ.get("ENGINE_MODE", "safe") or "safe"),
                "ts_ms": int(boot_ts_ms),
            },
        },
    )

    # Hard DB bootstrap is fail-fast because every later runtime subsystem
    # depends on schema, coordination tables, and DB guard state.
    try:
        fr = bootstrap_first_run(mode=str(os.environ.get("ENGINE_MODE", "safe") or "safe"))
        if not isinstance(fr, dict) or not fr.get("ok"):
            raise RuntimeError(f"bootstrap_first_run failed: {fr}")
        _step("bootstrap_first_run", True, fr)

        db_guard = ensure_db_ok()
        if not isinstance(db_guard, dict) or not db_guard.get("ok"):
            raise RuntimeError(f"ensure_db_ok failed: {db_guard}")
        _step("db_guard", True, db_guard)

        last_err = None
        # The most common startup failure here is transient SQLite contention, so
        # init_db gets a few bounded retries.
        for _ in range(3):
            try:
                _init_db()
                out["init_db"] = True
                break
            except Exception as e:
                last_err = e
                time.sleep(0.5)

        if not out["init_db"]:
            raise RuntimeError(f"init_db retry failed: {last_err}")

        _step("init_db", True, {"retries": 3, "db_guard": db_guard})
    except Exception as e:
        out["ok"] = False
        out["errors"].append(f"init_db:{e}")
        _step("init_db", False, str(e))
        _warn_nonfatal(
            "runtime_bootstrap_init_db_failed",
            "RUNTIME_BOOTSTRAP_INIT_DB_FAILED",
            e,
            warn_key="runtime_bootstrap_init_db_failed",
            partial_state=dict(out),
        )
        return out  # fail-fast

    # Event-log bootstrap is separate so later failures can still be recorded
    # into a persistent audit trail.
    try:
        from engine.runtime.event_log import init_event_log, append_event

        init_event_log()
        out["event_log"] = True
        _step("event_log", True, None)

        try:
            append_event(
                event_type="engine_start",
                event_source="runtime.bootstrap",
                entity_type="runtime",
                entity_id="bootstrap",
                payload={
                    "engine_mode": str(os.environ.get("ENGINE_MODE", "safe") or "safe"),
                    "db_path": str(os.environ.get("DB_PATH") or ""),
                    "boot_ts_ms": int(boot_ts_ms),
                },
                ts_ms=int(boot_ts_ms),
            )
        except Exception as e:
            _warn_nonfatal(
                "runtime_bootstrap_engine_start_event_append_failed",
                "RUNTIME_BOOTSTRAP_ENGINE_START_EVENT_APPEND_FAILED",
                e,
                warn_key="runtime_bootstrap_engine_start_event_append_failed",
                boot_ts_ms=int(boot_ts_ms),
            )
    except Exception as e:
        out["ok"] = False
        out["errors"].append(f"event_log:{e}")
        _step("event_log", False, str(e))

    # ---------------------------------------------------
    # Cross-process coordination tables (idempotent)
    # ---------------------------------------------------
    try:
        _ensure_job_locks()
        out["job_locks"] = True
        _step("job_locks", True, None)
    except Exception as e:
        out["ok"] = False
        out["errors"].append(f"ensure_job_locks:{e}")
        _step("job_locks", False, str(e))

    try:
        _ensure_job_history()
        out["job_history"] = True
        _step("job_history", True, None)
    except Exception as e:
        out["ok"] = False
        out["errors"].append(f"ensure_job_history:{e}")
        _step("job_history", False, str(e))

    # ---------------------------------------------------
    # Crash recovery replay
    # ---------------------------------------------------
    try:
        from engine.runtime.crash_recovery import replay_boot_recovery
        rec = replay_boot_recovery(log=log)
        out["crash_recovery"] = rec
        if isinstance(rec, dict) and not rec.get("ok", True):
            out["ok"] = False
            out["errors"].append(f"crash_recovery:{rec}")
            _step("crash_recovery", False, rec)
        else:
            _step("crash_recovery", True, rec)
    except Exception as e:
        out["crash_recovery"] = {"ok": False, "error": str(e)}
        out["errors"].append(f"crash_recovery:{e}")
        _step("crash_recovery", False, str(e))

    # ---------------------------------------------------
    # STARTUP PREFLIGHT + AUTO REPAIR
    # ---------------------------------------------------
    try:
        pre = run_preflight()
        out["startup_preflight"] = pre
        _step("startup_preflight_initial", bool(pre.get("ok")), pre)

        if not bool(pre.get("ok")):
            repair_steps = {
                "bootstrap_first_run": None,
                "repair_db": None,
                "stale_job_locks_cleared": [],
                "post_preflight": None,
            }

            try:
                repair_steps["bootstrap_first_run"] = bootstrap_first_run(
                    mode=str(os.environ.get("ENGINE_MODE", "safe") or "safe")
                )
                _step(
                    "startup_repair_bootstrap_first_run",
                    bool((repair_steps["bootstrap_first_run"] or {}).get("ok")),
                    repair_steps["bootstrap_first_run"],
                )
            except Exception as e:
                repair_steps["bootstrap_first_run"] = {"ok": False, "error": str(e)}
                _step("startup_repair_bootstrap_first_run", False, str(e))

            try:
                repair_steps["repair_db"] = repair_db()
                _step(
                    "startup_repair_db",
                    bool((repair_steps["repair_db"] or {}).get("ok")),
                    repair_steps["repair_db"],
                )
            except Exception as e:
                repair_steps["repair_db"] = {"ok": False, "error": str(e)}
                _step("startup_repair_db", False, str(e))

            try:
                from engine.runtime.storage import connect as _db_connect
                stale_after_s = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
                threshold_ms = int(time.time() * 1000) - (stale_after_s * 1000)

                con = _db_connect()
                try:
                    from engine.runtime.storage import _pid_is_running

                    rows = con.execute(
                        """
                        SELECT job_name, owner, pid, heartbeat_ts_ms
                        FROM job_locks
                        WHERE heartbeat_ts_ms IS NULL OR heartbeat_ts_ms < ?
                        ORDER BY job_name ASC
                        """,
                        (int(threshold_ms),),
                    ).fetchall() or []

                    cleared = []
                    skipped = []
                    for row in rows:
                        job_name = str(row[0] or "")
                        owner = str(row[1] or "")
                        pid = int(row[2] or 0)
                        if not job_name:
                            continue
                        if _pid_is_running(pid):
                            skipped.append({"job_name": job_name, "owner": owner, "pid": pid})
                            continue
                        con.execute("DELETE FROM job_locks WHERE job_name=?", (job_name,))
                        try:
                            con.execute("DELETE FROM job_heartbeats WHERE job_name=?", (job_name,))
                        except Exception as e:
                            _warn_nonfatal(
                                "runtime_bootstrap_stale_job_heartbeat_delete_failed",
                                "RUNTIME_BOOTSTRAP_STALE_JOB_HEARTBEAT_DELETE_FAILED",
                                e,
                                warn_key=f"runtime_bootstrap_stale_job_heartbeat_delete_failed:{job_name}",
                                job_name=str(job_name),
                                pid=int(pid),
                            )
                        cleared.append(job_name)

                    con.commit()
                    repair_steps["stale_job_locks_cleared"] = cleared
                    repair_steps["stale_job_locks_skipped_running"] = skipped
                    _step(
                        "startup_repair_clear_stale_locks",
                        True,
                        {"cleared": cleared, "count": len(cleared), "skipped_running": skipped},
                    )
                finally:
                    try:
                        con.close()
                    except Exception as e:
                        _warn_nonfatal(
                            "runtime_bootstrap_clear_stale_locks_close_failed",
                            "RUNTIME_BOOTSTRAP_CLEAR_STALE_LOCKS_CLOSE_FAILED",
                            e,
                            warn_key="runtime_bootstrap_clear_stale_locks_close_failed",
                        )
            except Exception as e:
                _step("startup_repair_clear_stale_locks", False, str(e))

            try:
                repair_steps["post_preflight"] = run_preflight()
                out["startup_preflight"] = repair_steps["post_preflight"]
                _step(
                    "startup_preflight_after_repair",
                    bool((repair_steps["post_preflight"] or {}).get("ok")),
                    repair_steps["post_preflight"],
                )
            except Exception as e:
                repair_steps["post_preflight"] = {"ok": False, "error": str(e)}
                _step("startup_preflight_after_repair", False, str(e))

            out["startup_repair"] = repair_steps

            if not bool((repair_steps.get("post_preflight") or {}).get("ok")):
                out["ok"] = False
                out["errors"].append("startup_preflight_failed_after_repair")
    except Exception as e:
        out["ok"] = False
        out["errors"].append(f"startup_preflight:{e}")
        _step("startup_preflight_exception", False, str(e))

    log.info(
        "runtime_bootstrap_done",
        extra={
            "event": "runtime_bootstrap_done",
            "extra_json": {
                "ok": bool(out.get("ok")),
                "errors": list(out.get("errors") or []),
                "steps": list(out.get("steps") or []),
                "startup_preflight": out.get("startup_preflight"),
                "startup_repair": out.get("startup_repair"),
                "ts_ms": int(time.time() * 1000),
            },
        },
    )
    return out
