# FILE: trade_pipeline_job.py
# REPLACE ENTIRE FILE WITH THIS (copy/paste)

"""
Full Autonomous Trade Pipeline Job

Flow:
  1) Universe Discovery
  2) Meta Strategy Allocation
  3) Portfolio Rebalance (writes portfolio_orders)
  3b) Regime Scaling Snapshot (audit trail)
  4) Risk Filter
  5) Broker Apply (paper/shadow/live via broker_apply_orders)
  6) Execution mode snapshot
  7) Divergence check (if dual enabled)
  8) Stage audit logging

Safety:
  - Job lock enforced
  - Each stage auditable
  - Hard abort on failure
  - No partial stage leakage (stage audits are committed immediately)
  - Time budget enforcement (global + per-stage)
"""

import json
import os
import sys
import time
import traceback
from typing import Dict, Any, Tuple, Callable, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
)

from engine.data.universe_discovery import discover_universe_once
from engine.execution.execution_mode import get_execution_mode
from engine.execution.kill_switch import execution_allowed
from engine.runtime.logging import get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.tracing import trace_event


JOB_NAME = "trade_pipeline"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))

ENABLE_DUAL = os.environ.get("EXECUTION_DUAL_ENABLE", "0") == "1"

# ---- Time Budget Enforcement ----
# Global pipeline deadline from job start; if exceeded, abort before execution.
PIPELINE_MAX_DURATION_MS = int(os.environ.get("PIPELINE_MAX_DURATION_MS", "5000"))
# Any single stage exceeding this will hard-fail (prevents runaway steps).
PIPELINE_STAGE_BUDGET_MS = int(os.environ.get("PIPELINE_STAGE_BUDGET_MS", "2500"))
LOG = get_logger("strategy.trade_pipeline")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=30,
        component="engine.strategy.jobs.trade_pipeline_job",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _print(obj: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(obj, sort_keys=True) + "\n")
    sys.stdout.flush()


def _ensure_schema(con) -> None:
    init_db()


def _audit(con, ts_ms: int, stage: str, ok: bool, dur_ms: int, detail: Dict[str, Any]) -> None:
    emit_timing(
        "strategy_latency",
        int(dur_ms),
        component="engine.strategy.jobs.trade_pipeline_job",
        job=JOB_NAME,
        strategy=str(stage),
    )

    emit_gauge(
        "job_health",
        1.0 if ok else 0.0,
        component="engine.strategy.jobs.trade_pipeline_job",
        job=JOB_NAME,
        strategy=str(stage),
    )

    trace_event(
        "strategy_signal",
        component="engine.strategy.jobs.trade_pipeline_job",
        entity_type="pipeline_stage",
        entity_id=str(stage),
        payload={"ok": bool(ok), "duration_ms": int(dur_ms), **dict(detail or {})},
        job=JOB_NAME,
        strategy=str(stage),
        ts_ms=int(ts_ms),
    )

    con.execute(
        """
        INSERT OR REPLACE INTO pipeline_stage_audit
        (ts_ms, stage, ok, duration_ms, detail_json)
        VALUES (?,?,?,?,?)
        """,
        (
            int(ts_ms),
            str(stage),
            1 if ok else 0,
            int(dur_ms),
            json.dumps(detail or {}),
        ),
    )
    # Ensure stage results persist even if later stages fail
    try:
        con.commit()
    except Exception as exc:
        _warn_nonfatal(
            "trade_pipeline_stage_audit_commit_failed",
            "TRADE_PIPELINE_STAGE_AUDIT_COMMIT_FAILED",
            exc,
            warn_key="trade_pipeline_stage_audit_commit_failed",
            stage=str(stage),
        )


def _deadline_exceeded(deadline_ms: int) -> bool:
    try:
        return _now_ms() > int(deadline_ms)
    except Exception as exc:
        _warn_nonfatal(
            "trade_pipeline_deadline_check_failed",
            "TRADE_PIPELINE_DEADLINE_CHECK_FAILED",
            exc,
            warn_key=f"trade_pipeline_deadline_check:{deadline_ms}",
            deadline_ms=deadline_ms,
        )
        return False


def _run_stage(
    con,
    ts_ms: int,
    stage: str,
    fn: Callable[[], Any],
    *,
    deadline_ms: Optional[int] = None,
) -> Tuple[bool, Any]:
    start = _now_ms()

    # Global deadline pre-check
    if deadline_ms is not None and start > int(deadline_ms):
        _audit(con, ts_ms, stage, False, 0, {"error": "pipeline_deadline_exceeded_pre"})
        return False, {"error": "pipeline_deadline_exceeded_pre"}

    try:
        result = fn()
        dur = _now_ms() - start

        # Per-stage time budget
        if dur > int(PIPELINE_STAGE_BUDGET_MS):
            _audit(
                con,
                ts_ms,
                stage,
                False,
                dur,
                {"error": "stage_time_budget_exceeded", "duration_ms": int(dur), "budget_ms": int(PIPELINE_STAGE_BUDGET_MS)},
            )
            return False, {"error": "stage_time_budget_exceeded"}

        # Global deadline post-check (do not continue to next stages if exceeded)
        if deadline_ms is not None and _now_ms() > int(deadline_ms):
            _audit(
                con,
                ts_ms,
                stage,
                False,
                dur,
                {"error": "pipeline_deadline_exceeded_post", "duration_ms": int(dur)},
            )
            return False, {"error": "pipeline_deadline_exceeded_post"}

        _audit(con, ts_ms, stage, True, dur, result if isinstance(result, dict) else {})
        return True, result

    except Exception as e:
        dur = _now_ms() - start
        _audit(
            con,
            ts_ms,
            stage,
            False,
            dur,
            {"error": str(e), "trace": traceback.format_exc()},
        )
        _warn_nonfatal(
            "trade_pipeline_stage_failed",
            "TRADE_PIPELINE_STAGE_FAILED",
            e,
            warn_key=f"trade_pipeline_stage_failed:{stage}",
            stage=str(stage),
        )
        return False, {"error": str(e)}


def _circuit_breaker_tripped() -> bool:
    try:
        from engine.execution.circuit_breaker import check_circuit_breaker
    except Exception as exc:
        _warn_nonfatal(
            "trade_pipeline_circuit_breaker_import_failed",
            "TRADE_PIPELINE_CIRCUIT_BREAKER_IMPORT_FAILED",
            exc,
            warn_key="trade_pipeline_circuit_breaker_import_failed",
        )
        return False

    try:
        return bool(check_circuit_breaker())
    except Exception as exc:
        _warn_nonfatal(
            "trade_pipeline_circuit_breaker_check_failed",
            "TRADE_PIPELINE_CIRCUIT_BREAKER_CHECK_FAILED",
            exc,
            warn_key="trade_pipeline_circuit_breaker_check_failed",
        )
        return False


def main() -> int:
    if _circuit_breaker_tripped():
        _print({"ok": True, "status": "circuit_breaker_tripped", "job": JOB_NAME})
        return 0

    con = None
    ts_ms = _now_ms()
    pipeline_deadline_ms = int(ts_ms + int(PIPELINE_MAX_DURATION_MS))

    try:
        init_db()
        con = connect()
        _ensure_schema(con)

        if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
            _audit(con, ts_ms, "job_lock", True, 0, {"status": "locked_out"})
            _print({"ok": True, "status": "locked_out", "job": JOB_NAME})
            return 0

        _audit(con, ts_ms, "job_lock", True, 0, {"status": "acquired", "owner": OWNER, "pid": PID})

        # ----------- Time Budget Snapshot -----------
        _audit(
            con,
            ts_ms,
            "time_budget",
            True,
            0,
            {
                "pipeline_max_duration_ms": int(PIPELINE_MAX_DURATION_MS),
                "stage_budget_ms": int(PIPELINE_STAGE_BUDGET_MS),
                "deadline_ms": int(pipeline_deadline_ms),
            },
        )

        # ----------- Kill Switch Pre-Check -----------
        allow, reason, meta = execution_allowed(con=con, symbol=None, regime=None)
        _audit(con, ts_ms, "kill_switch_precheck", bool(allow), 0, {"reason": reason, "meta": meta})
        if not allow:
            _print({"ok": False, "status": "blocked", "reason": reason})
            return 0

        # ----------- Global Deadline Pre-Check -----------
        if _deadline_exceeded(pipeline_deadline_ms):
            _audit(con, ts_ms, "deadline_precheck", False, 0, {"error": "pipeline_deadline_exceeded_pre"})
            _print({"ok": False, "status": "abort", "job": JOB_NAME, "stage": "deadline_precheck"})
            return 2
        _audit(con, ts_ms, "deadline_precheck", True, 0, {"ok": True})

        # ----------- 1. Universe Discovery -----------
        ok, uni = _run_stage(
            con,
            ts_ms,
            "universe_discovery",
            lambda: discover_universe_once(con=con, ts_ms=ts_ms),
            deadline_ms=pipeline_deadline_ms,
        )
        if not ok:
            _print({"ok": False, "status": "abort", "job": JOB_NAME, "stage": "universe_discovery"})
            return 2

        # ----------- 2. Meta Strategy Allocation -----------
        from engine.runtime.strategy_allocator import compute_and_persist_strategy_allocations

        ok, alloc = _run_stage(
            con,
            ts_ms,
            "meta_allocation",
            lambda: compute_and_persist_strategy_allocations(con, now_ms=ts_ms),
            deadline_ms=pipeline_deadline_ms,
        )
        if not ok:
            _print({"ok": False, "status": "abort", "job": JOB_NAME, "stage": "meta_allocation"})
            return 2

        # ----------- 3. Portfolio Rebalance (writes portfolio_orders) -----------
        trace_event(
            "feature_pipeline",
            component="engine.strategy.jobs.trade_pipeline_job",
            entity_type="pipeline_stage",
            entity_id="portfolio_rebalance",
            payload={
                "universe_symbols": int(len((uni or {}).get("symbols") or [])),
                "allocator_ok": bool((alloc or {}).get("ok")),
                "allocator_count": int(len((alloc or {}).get("allocations") or {})),
            },
            job=JOB_NAME,
            strategy="portfolio_rebalance",
        )
        emit_counter(
            "market_data_event",
            1,
            component="engine.strategy.jobs.trade_pipeline_job",
            job=JOB_NAME,
            extra_tags={"event_type": "feature_pipeline"},
        )
        from engine.strategy.portfolio import compute_rebalance

        ok, pr = _run_stage(
            con,
            ts_ms,
            "portfolio_rebalance",
            lambda: compute_rebalance(),
            deadline_ms=pipeline_deadline_ms,
        )
        if not ok:
            _print({"ok": False, "status": "abort", "job": JOB_NAME, "stage": "portfolio_rebalance"})
            return 2

        # ----------- 3b. Regime Scaling Snapshot (read-only audit trail) -----------
        try:
            from engine.strategy.regime_size import regime_capital_scale
            _rs = regime_capital_scale(
                con=con,
                anchor=str(os.environ.get("PORTFOLIO_REGIME_ANCHOR", "SPY")).strip().upper(),
            )
        except Exception as exc:
            _warn_nonfatal(
                "trade_pipeline_regime_capital_scale_failed",
                "TRADE_PIPELINE_REGIME_CAPITAL_SCALE_FAILED",
                exc,
                warn_key="trade_pipeline_regime_capital_scale_failed",
            )
            _rs = {"ok": False}

        try:
            _audit(
                con,
                ts_ms,
                "regime_capital_scale",
                True,
                0,
                (_rs if isinstance(_rs, dict) else {"ok": True}),
            )
        except Exception as exc:
            _warn_nonfatal(
                "trade_pipeline_regime_scale_audit_failed",
                "TRADE_PIPELINE_REGIME_SCALE_AUDIT_FAILED",
                exc,
                warn_key="trade_pipeline_regime_scale_audit_failed",
            )

        # ----------- 4. Risk Filter -----------
        from engine.runtime.risk_state import evaluate_risk_guards

        ok, _ = _run_stage(
            con,
            ts_ms,
            "risk_filter",
            lambda: evaluate_risk_guards(),
            deadline_ms=pipeline_deadline_ms,
        )
        if not ok:
            _print({"ok": False, "status": "abort", "job": JOB_NAME, "stage": "risk_filter"})
            return 2

        # ----------- Global Deadline Gate BEFORE Execution -----------
        # Institutional rule: never start live/paper sends if the pipeline is late.
        if _deadline_exceeded(pipeline_deadline_ms):
            _audit(con, ts_ms, "execution_gate_deadline", False, 0, {"error": "pipeline_deadline_exceeded_before_execution"})
            _print({"ok": False, "status": "abort", "job": JOB_NAME, "stage": "execution_gate_deadline"})
            return 2
        _audit(con, ts_ms, "execution_gate_deadline", True, 0, {"ok": True})

        # ----------- 5. Execution -----------
        import engine.execution.broker_apply_orders as broker_apply_orders  # uses existing entry logic

        ok, exec_res = _run_stage(
            con,
            ts_ms,
            "execution",
            lambda: broker_apply_orders.main(),
            deadline_ms=pipeline_deadline_ms,
        )
        if not ok:
            _print({"ok": False, "status": "abort", "job": JOB_NAME, "stage": "execution"})
            return 2

        # ----------- 6. Execution Mode Snapshot -----------
        try:
            mode = get_execution_mode()
        except Exception as exc:
            _warn_nonfatal(
                "trade_pipeline_mode_snapshot_failed",
                "TRADE_PIPELINE_MODE_SNAPSHOT_FAILED",
                exc,
                warn_key="trade_pipeline_mode_snapshot_failed",
            )
            mode = {"mode": "unknown"}
        _audit(con, ts_ms, "mode_snapshot", True, 0, mode if isinstance(mode, dict) else {"mode": str(mode)})

        # ----------- 7. Divergence Check (optional dual) -----------
        if not ENABLE_DUAL:
            _audit(con, ts_ms, "divergence_check", True, 0, {"status": "skipped", "reason": "EXECUTION_DUAL_ENABLE!=1"})
        else:
            def _dual_check():
                # If dual is enabled, require an implementation; fail hard if missing.
                from engine.execution.dual_execution import check_dual_divergence
                return check_dual_divergence(con=con, ts_ms=ts_ms, exec_result=exec_res)

            ok, _ = _run_stage(con, ts_ms, "divergence_check", _dual_check, deadline_ms=pipeline_deadline_ms)
            if not ok:
                _print({"ok": False, "status": "abort", "job": JOB_NAME, "stage": "divergence_check"})
                return 2

        _audit(
            con,
            ts_ms,
            "complete",
            True,
            0,
            {
                "job": JOB_NAME,
                "ts_ms": ts_ms,
                "owner": OWNER,
                "pid": PID,
                "alloc": alloc if isinstance(alloc, dict) else {},
            },
        )

        log_event(
            LOG,
            20,
            "trade_pipeline_complete",
            component="engine.strategy.jobs.trade_pipeline_job",
            extra={
                "job": JOB_NAME,
                "ts_ms": int(ts_ms),
                "execution_mode": mode,
            },
        )
        _print(
            {
                "ok": True,
                "status": "complete",
                "job": JOB_NAME,
                "ts_ms": ts_ms,
                "execution_mode": mode,
            }
        )
        return 0

    except Exception as e:
        try:
            if con is not None:
                _audit(con, ts_ms, "fatal", False, 0, {"error": str(e), "trace": traceback.format_exc()})
        except Exception as exc:
            _warn_nonfatal(
                "trade_pipeline_fatal_audit_failed",
                "TRADE_PIPELINE_FATAL_AUDIT_FAILED",
                exc,
                warn_key="trade_pipeline_fatal_audit_failed",
            )
        _print({"ok": False, "status": "fatal", "error": str(e)})
        return 2

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal(
                "trade_pipeline_release_job_lock_failed",
                "TRADE_PIPELINE_RELEASE_JOB_LOCK_FAILED",
                exc,
                warn_key="trade_pipeline_release_job_lock_failed",
            )
        try:
            if con is not None:
                con.close()
        except Exception as exc:
            _warn_nonfatal(
                "trade_pipeline_close_failed",
                "TRADE_PIPELINE_CLOSE_FAILED",
                exc,
                warn_key="trade_pipeline_close_failed",
            )


if __name__ == "__main__":
    raise SystemExit(main())
