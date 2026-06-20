# FILE: process_events_shadow.py
"""
SHADOW worker (research-only / background heavy compute)

Responsibilities:
- Do NOT emit live alerts (default)
- Run expensive shadow predictors / evaluation
- Optional clustering, regime experiments, backtests, temporal models
- Uses dedicated low-priority CUDA stream (shadow)

This file is where you put:
- temporal shadow prediction
- experimental models
- training jobs that must not starve live inference

IMPORTANT:
- If you want training here, keep it on _SHADOW_STREAM
- Consider also lowering CPU priority / using separate machine for training
"""

import os
import time
import json
import logging
from typing import Any, cast

import numpy as np
import torch
from engine.data.default_symbols import load_default_symbols
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.hardware import (
    apply_cpu_first_runtime_defaults,
    log_runtime_hardware_diagnostics,
    resolve_torch_device,
    torch_device_is_cuda,
)
from engine.runtime.torch_threads import configure_torch_thread_pools

# -----------------------------------------------------------------------------
# ENV defaults
# -----------------------------------------------------------------------------
apply_cpu_first_runtime_defaults()
_TORCH_DEVICE_RESOLUTION = resolve_torch_device(torch, env_var="TORCH_DEVICE")
_CUDA_RUNTIME_ENABLED = torch_device_is_cuda(torch, _TORCH_DEVICE_RESOLUTION)

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [process_events_shadow] %(message)s",
)
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.process_events_shadow",
        extra=extra or None,
        include_health=False,
        persist=False,
    )

_thread_config = configure_torch_thread_pools(torch)
if _thread_config.get("reason") == "failed":
    _warn_nonfatal(
        "PROCESS_EVENTS_SHADOW_TORCH_THREAD_CONFIG_FAILED",
        _thread_config["error"],
        once_key="torch_thread_config",
        cpu_threads=int(_thread_config.get("cpu_threads") or 0),
        interop_threads=int(_thread_config.get("interop_threads") or 0),
    )
log_runtime_hardware_diagnostics(LOGGER, torch_module=torch, component="engine.data.jobs.process_events_shadow")

# -----------------------------------------------------------------------------
# CUDA streams (shadow-focused)
# -----------------------------------------------------------------------------
_LIVE_STREAM = None
_SHADOW_STREAM = None
if _CUDA_RUNTIME_ENABLED:
    try:
        _LIVE_STREAM = torch.cuda.default_stream()
        _SHADOW_STREAM = torch.cuda.Stream(priority=1)
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_SHADOW_CUDA_STREAM_INIT_FAILED", e, once_key="cuda_stream_init")
        _LIVE_STREAM = None
        _SHADOW_STREAM = None

# -----------------------------------------------------------------------------
# Project imports
# -----------------------------------------------------------------------------
from engine.runtime.storage import (
    connect,
    connect_ro,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)

from engine.data.universe import get_active_symbols
from engine.strategy.rules_engine import evaluate_rules
from engine.execution.kill_switch import execution_allowed
from engine.strategy.model_config import experimental_models_enabled

# Optional heavy subsystems
try:
    from engine.strategy.temporal_predictor import predict_temporal_shadow_for_event
except Exception:
    predict_temporal_shadow_for_event = None

try:
    from engine.strategy.clustering import assign_cluster
except Exception:
    assign_cluster = None

# -----------------------------------------------------------------------------
# Runtime config
# -----------------------------------------------------------------------------
JOB_NAME = "process_events_shadow"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

DEFAULT_SYMBOLS = load_default_symbols(extra=["OIL"])
HORIZONS = [300, 3600]

# Shadow behavior toggles
SHADOW_EMIT_ALERTS = os.environ.get("SHADOW_EMIT_ALERTS", "0") == "1"
MAX_EVENTS_PER_PASS = int(os.environ.get("SHADOW_MAX_EVENTS_PER_PASS", "25"))
ENABLE_EXPERIMENTAL_MODELS = experimental_models_enabled()

# -----------------------------------------------------------------------------
# Ensure schemas exist
# -----------------------------------------------------------------------------
init_db()


def _require_fresh_prices_or_exit(job_name: str) -> None:
    now_ms = int(time.time() * 1000)
    stale_after_ms = int(float(os.environ.get("PROCESS_EVENTS_PRICE_MAX_AGE_S", "180")) * 1000.0)
    min_rows = int(os.environ.get("PROCESS_EVENTS_MIN_FRESH_PRICE_ROWS", "3"))
    min_symbol_coverage = float(os.environ.get("PROCESS_EVENTS_MIN_FRESH_SYMBOL_COVERAGE", "0.60"))

    con = connect_ro()
    try:
        row = con.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT symbol), MAX(ts_ms)
            FROM prices
            WHERE COALESCE(price, px) IS NOT NULL
              AND ts_ms >= ?
            """,
            (int(now_ms - stale_after_ms),),
        ).fetchone() or (0, 0, None)

        active_row = con.execute(
            """
            SELECT COUNT(*)
            FROM symbols
            WHERE status IN ('ACTIVE','WATCH')
            """
        ).fetchone() or (0,)
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_SHADOW_FRESH_PRICE_CHECK_CLOSE_FAILED", e, once_key="fresh_price_check_close")

    fresh_n = int(row[0] or 0)
    fresh_symbols = int(row[1] or 0)
    last_ts_ms = int(row[2] or 0)
    active_symbols = int(active_row[0] or 0)

    required_symbols = int(min_rows)
    if active_symbols > 0:
        required_symbols = max(required_symbols, int(max(1, active_symbols * min_symbol_coverage)))

    try:
        from engine.runtime.ipc import market_data_status

        # Shadow work accepts the supervisor's global market-data snapshot as the
        # source of truth so experimental jobs do not diverge from the runtime's
        # own view of whether feeds are healthy enough to proceed.
        snap = market_data_status(max_age_ms=stale_after_ms)
        if (
            snap.get("ok")
            and snap.get("running")
            and int(snap.get("last_price_ts_ms") or 0) >= int(now_ms - stale_after_ms)
            and int(snap.get("fresh_rows") or 0) >= min_rows
            and int(snap.get("fresh_symbols") or 0) >= required_symbols
        ):
            return
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_SHADOW_MARKET_DATA_STATUS_READ_FAILED", e, once_key="market_data_status_read")

    if fresh_n < min_rows or fresh_symbols < required_symbols:
        logging.error(
            "%s blocked: insufficient fresh prices fresh_n=%s fresh_symbols=%s required_symbols=%s active_symbols=%s last_ts_ms=%s stale_after_ms=%s",
            job_name,
            fresh_n,
            fresh_symbols,
            required_symbols,
            active_symbols,
            last_ts_ms,
            stale_after_ms,
        )
        raise SystemExit(3)


def main() -> None:
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    last_hb_s = 0.0

    try:
        try:
            evaluate_rules()
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_SHADOW_EVALUATE_RULES_FAILED", e, once_key="evaluate_rules")

        # Shadow respects the same kill switch so research workloads do not keep
        # burning GPU/CPU after the runtime has intentionally halted trading.
        # Shadow respects kill-switch too (don’t waste cycles if disabled)
        allow0, _, _ = execution_allowed(symbol=None, regime=None)
        if not allow0:
            _warn_nonfatal(
                "PROCESS_EVENTS_SHADOW_KILL_SWITCH_BLOCKED",
                RuntimeError("shadow blocked by kill switch"),
                once_key="kill_switch_blocked",
            )
            return
        if not ENABLE_EXPERIMENTAL_MODELS:
            logging.info("shadow experimental models disabled; exiting")
            return

        _require_fresh_prices_or_exit(JOB_NAME)

        # Shadow experiments still stay scoped to the canonical active/watch
        # universe. If you widen this, do it deliberately and document why.
        # Symbols
        conu = connect_ro()
        try:
            try:
                symbols = get_active_symbols(conu, limit=int(os.environ.get("PROCESS_SYMBOL_LIMIT", "2000")))
            except Exception:
                symbols = []
            if not symbols:
                symbols = list(DEFAULT_SYMBOLS)
            symbols = list(dict.fromkeys(symbols))
        finally:
            try:
                conu.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_SHADOW_SYMBOLS_CLOSE_FAILED", e, once_key="symbols_close")

        # Shadow code assumes the canonical ingest/enrichment path already built
        # embeddings. That keeps expensive experimental work layered on top of,
        # rather than entangled with, the production feature pipeline.
        # Pull recent events that already have embeddings (shadow wants embedded)
        con = connect_ro()
        try:
            rows = con.execute(
                """
                SELECT e.id, e.ts_ms, e.title, emb.dim, emb.vec
                FROM events e
                JOIN event_embeddings emb ON emb.event_id = e.id
                ORDER BY e.ts_ms DESC
                LIMIT ?
                """,
                (int(MAX_EVENTS_PER_PASS),),
            ).fetchall()
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_SHADOW_EVENTS_CLOSE_FAILED", e, once_key="events_close")

        if not rows:
            logging.info("no embedded events for shadow")
            return

        conw = connect(readonly=False)
        try:
            for (eid, ts_ms, title, dim, blob) in rows:
                now_s = time.time()
                if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                    try:
                        touch_job_lock(JOB_NAME, OWNER, PID)
                        put_job_heartbeat(
                            JOB_NAME,
                            OWNER,
                            PID,
                            extra_json=json.dumps(
                                {"event_id": int(eid), "event_ts_ms": int(ts_ms or 0)},
                                separators=(",", ":"),
                                sort_keys=True,
                            ),
                        )
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_SHADOW_HEARTBEAT_FAILED", e, once_key="heartbeat")
                    last_hb_s = now_s

                if not predict_temporal_shadow_for_event and not assign_cluster:
                    continue

                vec = None
                try:
                    d = int(dim or 0)
                    if d > 0 and blob:
                        a = np.frombuffer(blob, dtype=np.float32)
                        if a.size == d:
                            vec = a
                except Exception:
                    vec = None

                # Optional clustering (shadow)
                if assign_cluster and vec is not None:
                    try:
                        if _SHADOW_STREAM is not None and _CUDA_RUNTIME_ENABLED:
                            with torch.cuda.stream(cast(Any, _SHADOW_STREAM)):
                                _ = assign_cluster(event_id=int(eid), ts_ms=int(ts_ms or 0), title=(title or ""), vec=vec)
                        else:
                            _ = assign_cluster(event_id=int(eid), ts_ms=int(ts_ms or 0), title=(title or ""), vec=vec)
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_SHADOW_ASSIGN_CLUSTER_FAILED", e, once_key=f"assign_cluster:{int(eid)}", event_id=int(eid))

                # Temporal shadow predictor (shadow stream)
                if predict_temporal_shadow_for_event:
                    try:
                        if _SHADOW_STREAM is not None and _CUDA_RUNTIME_ENABLED:
                            with torch.cuda.stream(cast(Any, _SHADOW_STREAM)):
                                _ = predict_temporal_shadow_for_event(
                                    conw,
                                    event_id=int(eid),
                                    ts_ms=int(ts_ms or 0),
                                    symbols=symbols,
                                    horizons=HORIZONS,
                                )
                        else:
                            _ = predict_temporal_shadow_for_event(
                                conw,
                                event_id=int(eid),
                                ts_ms=int(ts_ms or 0),
                                symbols=symbols,
                                horizons=HORIZONS,
                            )
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_SHADOW_TEMPORAL_PREDICT_FAILED", e, once_key=f"temporal_predict:{int(eid)}", event_id=int(eid))

            conw.commit()
        finally:
            try:
                conw.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_SHADOW_WRITE_CLOSE_FAILED", e, once_key="write_close")

        logging.info("SHADOW COMPLETE n_events=%s", len(rows))

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_SHADOW_RELEASE_JOB_LOCK_FAILED", e, once_key="release_job_lock")


if __name__ == "__main__":
    main()
