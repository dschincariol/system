# FILE: engine/runtime/first_run.py
# REPLACE ENTIRE FILE WITH THIS EXACT CONTENT

from __future__ import annotations

import logging
import time

from engine.runtime.db_guard import ensure_db_ok, resolve_db_path
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.jobs.repair_schema import run as repair_schema
from engine.runtime.lifecycle_state import set_state, SCHEMA_REPAIR, WARMING_UP
from engine.runtime.logging import get_logger
from engine.runtime.runtime_meta import meta_set
from engine.runtime.storage import (
    _has_column,
    _safe_commit,
    connect_rw_direct,
    get_db_validation_snapshot,
)

LOG = get_logger("engine.runtime.first_run")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _db_path() -> str:
    return str(resolve_db_path())


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: object) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.first_run",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _log_bootstrap_step(step: str, status: str, started_ts_ms: int, **extra: object) -> None:
    payload = {
        "step": str(step),
        "status": str(status),
        "duration_ms": max(0, int(time.time() * 1000) - int(started_ts_ms)),
        "ts_ms": int(time.time() * 1000),
    }
    payload.update(extra or {})
    try:
        LOG.info(
            "first_run_bootstrap_step %s %s",
            str(step),
            str(status),
            extra={
                "event": "first_run_bootstrap_step",
                "extra_json": payload,
            },
        )
    except Exception:
        LOG.info(
            "first_run_bootstrap_step step=%s status=%s duration_ms=%s extra=%s",
            str(step),
            str(status),
            int(payload.get("duration_ms") or 0),
            repr(payload),
        )


def _seed_minimum_rows(db_path: str) -> dict:
    """
    Non-technical deterministic boot:
    - Ensure some core rows exist so UI has something to read
    - Seed a minimal symbol universe so live data jobs have deterministic defaults
    """
    out = {"ok": True, "seeded": [], "error": None}
    now_ms = int(time.time() * 1000)
    conn = None

    try:
        conn = connect_rw_direct()
        cur = conn.cursor()

        # Seed the smallest useful dashboard/runtime rows so first boot has
        # deterministic state even before jobs populate real values.
        # portfolio_equity_state (schema in repair_schema.py uses ts_ms PRIMARY KEY)
        try:
            n = cur.execute("SELECT COUNT(*) FROM portfolio_equity_state").fetchone()
            if int(n[0] or 0) == 0:
                cols = {"ts_ms", "equity"}
                if _has_column(conn, "portfolio_equity_state", "drawdown"):
                    cols.add("drawdown")
                if _has_column(conn, "portfolio_equity_state", "updated_ts_ms"):
                    cols.add("updated_ts_ms")

                ordered_cols = [col for col in ("ts_ms", "equity", "drawdown", "updated_ts_ms") if col in cols]
                values = {
                    "ts_ms": int(now_ms),
                    "equity": 0.0,
                    "drawdown": 0.0,
                    "updated_ts_ms": int(now_ms),
                }
                cur.execute(
                    "INSERT INTO portfolio_equity_state ({}) VALUES ({})".format(
                        ", ".join(ordered_cols),
                        ", ".join("?" for _ in ordered_cols),
                    ),
                    tuple(values[col] for col in ordered_cols),
                )
                out["seeded"].append("portfolio_equity_state")
        except Exception as exc:
            _warn_nonfatal(
                "first_run_seed_portfolio_equity_state_failed",
                "FIRST_RUN_SEED_PORTFOLIO_EQUITY_STATE_FAILED",
                exc,
            )

        # broker_account (schema in repair_schema.py uses ts_ms PRIMARY KEY)
        try:
            n = cur.execute("SELECT COUNT(*) FROM broker_account").fetchone()
            if int(n[0] or 0) == 0:
                cur.execute(
                    "INSERT INTO broker_account (ts_ms, equity, buying_power) VALUES (?, ?, ?)",
                    (now_ms, 0.0, 0.0),
                )
                out["seeded"].append("broker_account")
        except Exception as exc:
            _warn_nonfatal(
                "first_run_seed_broker_account_failed",
                "FIRST_RUN_SEED_BROKER_ACCOUNT_FAILED",
                exc,
            )

        # Ensure the runtime has a small default symbol universe so ingestion
        # and warmup jobs have something deterministic to work on.
        # symbols: ensure the runtime has at least a small starter universe
        try:
            n = cur.execute("SELECT COUNT(*) FROM symbols").fetchone()
            if int(n[0] or 0) == 0:
                default_symbols = [
                    str(s).strip().upper()
                    for s in __import__("os").environ.get("DEFAULT_SYMBOLS", "SPY,QQQ,IWM,DIA").split(",")
                    if str(s).strip()
                ]
                seeded = 0
                for sym in default_symbols:
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO symbols(
                            symbol, asset_class, status, score, created_ts_ms, updated_ts_ms, meta_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            sym,
                            "EQUITY",
                            "WATCH",
                            0.5,
                            now_ms,
                            now_ms,
                            '{"price_provider":"polygon_ws"}',
                        ),
                    )
                    seeded += 1
                if seeded > 0:
                    out["seeded"].append("symbols")
                    out["seeded_symbols"] = default_symbols
        except Exception as exc:
            _warn_nonfatal(
                "first_run_seed_symbols_failed",
                "FIRST_RUN_SEED_SYMBOLS_FAILED",
                exc,
            )

        _safe_commit(conn)
        conn.close()
        return out

    except Exception as e:
        try:
            if conn is not None:
                conn.rollback()
        except Exception as exc:
            _warn_nonfatal(
                "first_run_seed_rollback_failed",
                "FIRST_RUN_SEED_ROLLBACK_FAILED",
                exc,
                warn_key="first_run_seed_rollback_failed",
            )
        try:
            if conn is not None:
                conn.close()
        except Exception as exc:
            _warn_nonfatal(
                "first_run_seed_close_failed",
                "FIRST_RUN_SEED_CLOSE_FAILED",
                exc,
                warn_key="first_run_seed_close_failed",
            )
        out["ok"] = False
        out["error"] = str(e)
        return out


def bootstrap_first_run(mode: str = "safe", *, assume_prior_db_repair: bool = False) -> dict:
    """
    Deterministic boot pipeline:
    1) DB guard (creates file, quarantines corruption)
    2) Schema repair (idempotent)
    3) Seed minimal rows (idempotent)
    """
    out = {
        "ok": True,
        "mode": mode,
        "assume_prior_db_repair": bool(assume_prior_db_repair),
        "fast_path_used": False,
        "db_guard": None,
        "schema": None,
        "seed": None,
        "validation": None,
    }
    structural_validation = None
    fast_path_ok = False
    overall_started_ts_ms = int(time.time() * 1000)

    _log_bootstrap_step(
        "bootstrap_first_run",
        "started",
        overall_started_ts_ms,
        mode=str(mode),
        assume_prior_db_repair=bool(assume_prior_db_repair),
    )

    try:
        set_state(SCHEMA_REPAIR, "first_run_bootstrap")
    except Exception as exc:
        _warn_nonfatal(
            "first_run_schema_repair_state_failed",
            "FIRST_RUN_SCHEMA_REPAIR_STATE_FAILED",
            exc,
            warn_key="first_run_schema_repair_state_failed",
        )

    if assume_prior_db_repair:
        started_ts_ms = int(time.time() * 1000)
        try:
            structural_validation = dict(
                get_db_validation_snapshot(include_quick_check=False, strict=True) or {}
            )
            fast_path_ok = bool(structural_validation.get("ok"))
            _log_bootstrap_step(
                "pre_validation",
                "ok" if fast_path_ok else "failed",
                started_ts_ms,
                quick_check_skipped=True,
                missing_tables=list(structural_validation.get("missing_tables") or []),
                missing_indexes=list(structural_validation.get("missing_indexes") or []),
                missing_columns=dict(structural_validation.get("missing_columns") or {}),
                schema_version=structural_validation.get("schema_version"),
                expected_schema_version=structural_validation.get("expected_schema_version"),
            )
        except Exception as exc:
            fast_path_ok = False
            structural_validation = {
                "ok": False,
                "error": f"pre_validation_failed:{type(exc).__name__}:{exc}",
                "quick_check": "skipped",
                "quick_check_skipped": True,
            }
            _warn_nonfatal(
                "first_run_pre_validation_failed",
                "FIRST_RUN_PRE_VALIDATION_FAILED",
                exc,
                warn_key="first_run_pre_validation_failed",
            )
            _log_bootstrap_step(
                "pre_validation",
                "failed",
                started_ts_ms,
                quick_check_skipped=True,
                error=str(structural_validation.get("error") or ""),
            )

    # DB guard is first because every later step assumes the path is writable
    # and not obviously corrupt.
    started_ts_ms = int(time.time() * 1000)
    if fast_path_ok:
        g = {
            "ok": True,
            "db_path": _db_path(),
            "action": "reused_prior_db_repair",
            "error": None,
            "skipped": True,
        }
    else:
        g = ensure_db_ok()
    out["db_guard"] = g
    _log_bootstrap_step(
        "db_guard",
        "ok" if bool(g.get("ok")) else "failed",
        started_ts_ms,
        skipped=bool(g.get("skipped")),
        action=str(g.get("action") or ""),
        error=str(g.get("error") or ""),
    )
    if not g.get("ok"):
        out["ok"] = False
        return out

    # Schema repair is authoritative for boot-time migrations.
    started_ts_ms = int(time.time() * 1000)
    if fast_path_ok:
        out["fast_path_used"] = True
        s = {
            "ok": True,
            "db_path": _db_path(),
            "schema_version": structural_validation.get("schema_version"),
            "expected_schema_version": structural_validation.get("expected_schema_version"),
            "required_tables": list(structural_validation.get("required_tables") or []),
            "quick_check": "reused_prior_db_repair",
            "quick_check_skipped": True,
            "skipped": True,
        }
    else:
        s = repair_schema()
    out["schema"] = s
    _log_bootstrap_step(
        "schema_repair",
        "ok" if bool(s.get("ok")) else "failed",
        started_ts_ms,
        skipped=bool(s.get("skipped")),
        schema_version=s.get("schema_version"),
        expected_schema_version=s.get("expected_schema_version"),
        error=str(s.get("error") or ""),
    )
    if not s.get("ok"):
        out["ok"] = False

    db_path = _db_path()
    started_ts_ms = int(time.time() * 1000)
    if db_path:
        out["seed"] = _seed_minimum_rows(db_path)
    else:
        out["seed"] = {"ok": False, "error": "DB_PATH_missing_after_schema"}
    _log_bootstrap_step(
        "seed_minimum_rows",
        "ok" if bool((out.get("seed") or {}).get("ok")) else "failed",
        started_ts_ms,
        db_path=str(db_path or ""),
        seeded=list((out.get("seed") or {}).get("seeded") or []),
        error=str((out.get("seed") or {}).get("error") or ""),
    )
    if not bool((out.get("seed") or {}).get("ok")):
        out["ok"] = False

    started_ts_ms = int(time.time() * 1000)
    try:
        if fast_path_ok:
            validation = dict(structural_validation or {})
            validation["integrity_source"] = "prior_db_repair"
            validation["validation_mode"] = "structural_only"
        else:
            validation = dict(get_db_validation_snapshot(strict=True) or {})
            validation["integrity_source"] = "bootstrap_first_run"
            validation["validation_mode"] = "full"
        out["validation"] = validation
        _log_bootstrap_step(
            "db_validation",
            "ok" if bool(validation.get("ok")) else "failed",
            started_ts_ms,
            validation_mode=str(validation.get("validation_mode") or ""),
            quick_check=str(validation.get("quick_check") or ""),
            quick_check_skipped=bool(validation.get("quick_check_skipped")),
            missing_tables=list(validation.get("missing_tables") or []),
            missing_indexes=list(validation.get("missing_indexes") or []),
            missing_columns=dict(validation.get("missing_columns") or {}),
        )
        if not bool(validation.get("ok")):
            out["ok"] = False
    except Exception as exc:
        out["ok"] = False
        out["validation"] = {
            "ok": False,
            "error": f"db_validation_failed:{type(exc).__name__}:{exc}",
        }
        _warn_nonfatal(
            "first_run_db_validation_failed",
            "FIRST_RUN_DB_VALIDATION_FAILED",
            exc,
            warn_key="first_run_db_validation_failed",
        )
        _log_bootstrap_step(
            "db_validation",
            "failed",
            started_ts_ms,
            error=str(out["validation"].get("error") or ""),
        )

    if out.get("ok"):
        started_ts_ms = int(time.time() * 1000)
        try:
            # Warmup markers are cleared/set here so the rest of runtime can tell
            # that schema is ready but first market data has not arrived yet.
            meta_set("first_price_ts_ms", "")
            meta_set("price_provider_active", "")
            meta_set("warmup_started_ts_ms", str(int(time.time() * 1000)))
            set_state(WARMING_UP, "schema_ready_awaiting_first_price_tick")
            _log_bootstrap_step(
                "warmup_markers",
                "ok",
                started_ts_ms,
            )
        except Exception as exc:
            _warn_nonfatal(
                "first_run_warmup_markers_failed",
                "FIRST_RUN_WARMUP_MARKERS_FAILED",
                exc,
                warn_key="first_run_warmup_markers_failed",
            )
            _log_bootstrap_step(
                "warmup_markers",
                "failed",
                started_ts_ms,
                error=str(exc),
            )

    _log_bootstrap_step(
        "bootstrap_first_run",
        "ok" if bool(out.get("ok")) else "failed",
        overall_started_ts_ms,
        fast_path_used=bool(out.get("fast_path_used")),
    )
    return out
