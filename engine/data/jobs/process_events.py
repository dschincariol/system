"""
FILE: process_events.py

Job entrypoint or scheduled task for `process_events`.
"""

"""
Process unembedded events:
- embed
- predict expected impact
- store predictions
- emit alerts (with explainability + relevance)

Design goals:
- Ensure core DB schema exists before use
- Avoid holding SQLite write locks during embedding compute
- Include rich explainability payloads
- Heartbeats + job locks for production safety
- Dynamic symbol universe (ACTIVE + WATCH from symbols table)
- Preserve legacy behavior: optional alert confidence downweighting
"""

import re
import time
import os
import json
import random
import logging
from typing import Dict, Any, List, Optional, Tuple, cast

import numpy as np
try:
    from sentence_transformers import SentenceTransformer as _SentenceTransformer
    _SENTENCE_TRANSFORMER_CLS: Any = _SentenceTransformer
    _SENTENCE_TRANSFORMERS_IMPORT_ERROR = None
except Exception as _sentence_transformers_import_error:
    _SENTENCE_TRANSFORMERS_IMPORT_ERROR = _sentence_transformers_import_error

    class _SentenceTransformerStub:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError(f"sentence_transformers_unavailable:{_SENTENCE_TRANSFORMERS_IMPORT_ERROR}")
    _SENTENCE_TRANSFORMER_CLS = _SentenceTransformerStub
import torch
from pathlib import Path
from engine.data.default_symbols import load_default_symbols
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.hardware import (
    apply_cpu_first_runtime_defaults,
    log_runtime_hardware_diagnostics,
    nvidia_telemetry_enabled,
    resolve_torch_device,
    torch_device_is_cuda,
)
from engine.runtime.torch_threads import configure_torch_thread_pools
from engine.strategy.model_config import configured_model_horizons, experimental_models_enabled
from engine.strategy.microstructure_signals import (
    apply_microstructure_confidence,
    load_latest_microstructure_context,
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
        component="engine.data.jobs.process_events",
        extra=extra or None,
        include_health=False,
        persist=False,
    )

# -----------------------------
# Optional accelerator stream separation + pinned async H->D
# -----------------------------
# This file is the canonical enriched event-processing path. The live/shadow
# variants split out narrower workflows, but this module carries the richest
# end-to-end explainability and ingestion behavior.
_LIVE_STREAM = None
_SHADOW_STREAM = None

# CPU-first runtime defaults. CUDA/NVIDIA behavior is opt-in via explicit env.
apply_cpu_first_runtime_defaults()
_TORCH_DEVICE_RESOLUTION = resolve_torch_device(torch, env_var="TORCH_DEVICE")
_CUDA_RUNTIME_ENABLED = torch_device_is_cuda(torch, _TORCH_DEVICE_RESOLUTION)
_NVIDIA_TELEMETRY_ENABLED = nvidia_telemetry_enabled(torch)

# GPU feedback throttling config
GPU_THROTTLE_ENABLE = os.environ.get("GPU_THROTTLE_ENABLE", "0") == "1" and _NVIDIA_TELEMETRY_ENABLED
GPU_UTIL_MAX = float(os.environ.get("GPU_UTIL_MAX", "92"))          # %
GPU_MEM_MAX = float(os.environ.get("GPU_MEM_MAX", "92"))            # %
GPU_THROTTLE_SLEEP_S = float(os.environ.get("GPU_THROTTLE_SLEEP_S", "0.05"))

# Pinned H->D config
PINNED_ENABLE = os.environ.get("PINNED_ENABLE", "0") == "1" and _CUDA_RUNTIME_ENABLED
PINNED_PREFETCH = os.environ.get("PINNED_PREFETCH", "0") == "1" and _CUDA_RUNTIME_ENABLED
PINNED_DEVICE = os.environ.get("PINNED_DEVICE", _TORCH_DEVICE_RESOLUTION.resolved).strip()
PINNED_DTYPE = torch.float32

_thread_config = configure_torch_thread_pools(torch)
if _thread_config.get("reason") == "failed":
    _warn_nonfatal(
        "PROCESS_EVENTS_TORCH_THREAD_CONFIG_FAILED",
        _thread_config["error"],
        once_key="torch_thread_config",
        cpu_threads=int(_thread_config.get("cpu_threads") or 0),
        interop_threads=int(_thread_config.get("interop_threads") or 0),
    )
log_runtime_hardware_diagnostics(LOGGER, torch_module=torch, component="engine.data.jobs.process_events")

# Initialize CUDA streams only when an explicit runtime device resolved to CUDA.
if _CUDA_RUNTIME_ENABLED:
    try:
        _LIVE_STREAM = torch.cuda.default_stream()
        _SHADOW_STREAM = torch.cuda.Stream(priority=1)
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_CUDA_STREAM_INIT_FAILED", e, once_key="cuda_stream_init")
        _LIVE_STREAM = None
        _SHADOW_STREAM = None


def _gpu_stats() -> Dict[str, float]:
    """
    Returns {util: %, mem: %, mem_used_mb, mem_total_mb}.
    Best-effort:
      1) pynvml
      2) nvidia-smi
      3) torch memory only (no util)
    """
    if not _NVIDIA_TELEMETRY_ENABLED:
        return {"util": 0.0, "mem": 0.0, "mem_used_mb": 0.0, "mem_total_mb": 0.0}

    # 1) NVML
    try:
        import pynvml  # type: ignore
        pynvml.nvmlInit()
        h = pynvml.nvmlDeviceGetHandleByIndex(0)
        util = float(pynvml.nvmlDeviceGetUtilizationRates(h).gpu)
        memi = pynvml.nvmlDeviceGetMemoryInfo(h)
        used = float(memi.used) / (1024.0 * 1024.0)
        total = float(memi.total) / (1024.0 * 1024.0)
        memp = 100.0 * used / total if total > 1e-9 else 0.0
        return {"util": util, "mem": memp, "mem_used_mb": used, "mem_total_mb": total}
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_GPU_STATS_NVML_FAILED", e, once_key="gpu_stats_nvml")

    # 2) nvidia-smi
    try:
        import subprocess
        out = subprocess.check_output(
            [
                "nvidia-smi",
                "--query-gpu=utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            timeout=1.0,
        ).decode("utf-8", errors="ignore").strip()
        # "12, 456, 8192"
        parts = [p.strip() for p in out.split(",")]
        util = float(parts[0])
        used = float(parts[1])
        total = float(parts[2]) if float(parts[2]) > 1e-9 else 1.0
        memp = 100.0 * used / total
        return {"util": util, "mem": memp, "mem_used_mb": used, "mem_total_mb": total}
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_GPU_STATS_NVIDIA_SMI_FAILED", e, once_key="gpu_stats_nvidia_smi")

    # 3) torch memory only
    try:
        total = float(torch.cuda.get_device_properties(0).total_memory) / (1024.0 * 1024.0)
        used = float(torch.cuda.memory_allocated(0)) / (1024.0 * 1024.0)
        memp = 100.0 * used / total if total > 1e-9 else 0.0
        return {"util": 0.0, "mem": memp, "mem_used_mb": used, "mem_total_mb": total}
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_GPU_STATS_TORCH_FAILED", e, once_key="gpu_stats_torch")
        stats = {"util": 0.0, "mem": 0.0, "mem_used_mb": 0.0, "mem_total_mb": 0.0}
        return stats


def _gpu_throttle_if_needed() -> None:
    if not GPU_THROTTLE_ENABLE or not _NVIDIA_TELEMETRY_ENABLED:
        return
    try:
        s = _gpu_stats()
        if s.get("util", 0.0) >= GPU_UTIL_MAX or s.get("mem", 0.0) >= GPU_MEM_MAX:
            time.sleep(max(0.0, float(GPU_THROTTLE_SLEEP_S)))
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_GPU_THROTTLE_FAILED", e, once_key="gpu_throttle")
        return None


def _pinned_prefetch_to_device(vec_np: np.ndarray) -> Optional["torch.Tensor"]:
    """
    Crash-safe, best-effort pinned H->D prefetch.
    Returns device tensor (cuda) if successful, else None.

    This is intentionally optional: it improves overlap if your predictor can accept
    a torch.Tensor directly (recommended patch), otherwise it still warms the copy path.
    """
    if not PINNED_ENABLE or not _CUDA_RUNTIME_ENABLED:
        return None
    try:
        # Ensure contiguous float32 CPU buffer
        a = np.asarray(vec_np, dtype=np.float32, order="C")
        t = torch.from_numpy(a)
        if t.device.type != "cpu":
            t = t.cpu()
        t = t.pin_memory()  # pinned
        # async copy on live stream
        stream = _LIVE_STREAM if _LIVE_STREAM is not None else torch.cuda.default_stream()
        with torch.cuda.stream(stream):
            d = t.to(device=PINNED_DEVICE, dtype=PINNED_DTYPE, non_blocking=True)
        return d
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_PINNED_PREFETCH_FAILED", e, once_key="pinned_prefetch")
        out = None
        return out

# ------            -- ------------------------------------------------------
# In-memory cache for recent embeddings (novelty acceleration)
# ------            -- ------------------------------------------------------
# This is an optimization only; correctness must not depend on cache hits.

_RECENT_EMB_CACHE: List[np.ndarray] = []
_RECENT_EMB_CACHE_MAX = int(os.environ.get("NOVELTY_CACHE_MAX", "500"))

from engine.runtime.storage import (
    connect_ro,
    _new_connection,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    get_job_checkpoint,
    put_job_checkpoint,
)
from engine.runtime.metrics import emit_snapshot

from engine.strategy.predictor import predict_runtime_event
from engine.runtime.alerts import emit_alert
from engine.runtime.event_bus import publish_event
from engine.strategy.validation import store_prediction
from engine.strategy.decision_log import log_decision, hash_feature_vector
from engine.strategy.confidence_adjust import get_adjusted_confidence
from engine.strategy.confidence_engine import apply_confidence_payload, describe_signal_confidence
from engine.strategy.model_intent import build_model_intent
from engine.data.universe import get_active_symbols
from engine.strategy.model_v2 import get_current_regime
from engine.strategy.news_domain import extract_domain, is_domain_blocked, domain_conf_multiplier
from engine.strategy.rules_engine import evaluate_rules
from engine.execution.kill_switch import execution_allowed
from engine.runtime.ipc import market_data_status

# ------            -- ------------------------------------------------------
# Optional subsystems (shadow-safe)
# ------            -- ------------------------------------------------------
# Optional imports are allowed to fail so event processing can still run in a
# reduced-but-functional mode if auxiliary modeling features are unavailable.

try:
    from engine.strategy.temporal_predictor import predict_temporal_shadow_for_event
except Exception:
    predict_temporal_shadow_for_event = None

try:
    from engine.strategy.shadow import shadow_predict
except Exception:
    shadow_predict = None

try:
    from engine.strategy.clustering import assign_cluster
except Exception:
    assign_cluster = None

# ------            -- ------------------------------------------------------
# Runtime config
# ------            -- ------------------------------------------------------

# If symbols table is empty, fallback to a conservative seed set.
DEFAULT_SYMBOLS = load_default_symbols(extra=["OIL"])

HORIZONS = configured_model_horizons(default=[300, 3600])
ENABLE_EXPERIMENTAL_MODELS = experimental_models_enabled()
ENABLE_SHADOW_PREDICTIONS = ENABLE_EXPERIMENTAL_MODELS and (
    os.environ.get("ENABLE_SHADOW_PREDICTIONS", "0") == "1"
)

JOB_NAME = "process_events"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

# Novelty scoring (embedding-based)
NOVELTY_LOOKBACK = int(os.environ.get("NOVELTY_LOOKBACK", "200"))
NOVELTY_MIN_EVENTS = int(os.environ.get("NOVELTY_MIN_EVENTS", "8"))
NOVELTY_MIN_SCORE = float(os.environ.get("NOVELTY_MIN_SCORE", "0.20"))  # currently explain-only / future gating

# Tradability proxy parameters (explain-only)
RET_SCALE_PER_Z = float(os.environ.get("RET_SCALE_PER_Z", "0.0025"))  # per 1.0 z at 1h (0.25% default)
COST_BPS = float(os.environ.get("COST_BPS", "6.0"))                   # 6 bps default

# ------            -- ------------------------------------------------------
# Feature 4: Kill-switch on execution cost spikes (spread-based)
# ------            -- ------------------------------------------------------
EXEC_COST_SPIKE_BPS = float(os.environ.get("EXEC_COST_SPIKE_BPS", "45.0"))  # trigger kill if avg spread_bps >= this
EXEC_COST_SPIKE_WINDOW_S = int(os.environ.get("EXEC_COST_SPIKE_WINDOW_S", "120"))  # lookback window
EXEC_COST_SPIKE_MIN_N = int(os.environ.get("EXEC_COST_SPIKE_MIN_N", "8"))          # minimum samples required
EXEC_COST_SPIKE_SYMBOL_LIMIT = int(os.environ.get("EXEC_COST_SPIKE_SYMBOL_LIMIT", "50"))  # top symbols to sample

# Preserve legacy alert behavior (old file): downweight alert confidence if expected_ret_net < 0
ALERT_DOWNWEIGHT_NEG_NET = os.environ.get("ALERT_DOWNWEIGHT_NEG_NET", "1") == "1"
ALERT_DOWNWEIGHT_MULT = float(os.environ.get("ALERT_DOWNWEIGHT_MULT", "0.75"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
DISCOVER_SYMBOLS = os.environ.get("DISCOVER_SYMBOLS", "1") == "1"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [process_events] %(message)s",
)

TRACE_STEPS = os.environ.get("PROCESS_EVENTS_TRACE_STEPS", "0") == "1"

# ------            -- ------------------------------------------------------
# Helpers
# ------            -- ------------------------------------------------------

def _sleep_with_jitter(seconds: float) -> None:
    if seconds <= 0:
        return
    j = seconds * 0.2
    time.sleep(max(0.05, seconds + random.uniform(-j, j)))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_SAFE_INT_FAILED",
            e,
            once_key="safe_int",
            value=repr(value)[:120],
        )
        return int(default)


def _latency_summary(values: List[int]) -> Dict[str, float]:
    cleaned = sorted(int(max(0, int(v))) for v in values if v is not None)
    if not cleaned:
        return {}
    count = len(cleaned)
    p95_index = min(count - 1, max(0, int(round((count - 1) * 0.95))))
    return {
        "count": float(count),
        "avg_ms": float(round(sum(cleaned) / count, 3)),
        "p95_ms": float(cleaned[p95_index]),
        "max_ms": float(cleaned[-1]),
    }


def _emit_latency_snapshot(stage_values: Dict[str, List[int]], *, event_count: int, prediction_count: int) -> None:
    metrics: Dict[str, float] = {}
    for stage_name, values in dict(stage_values or {}).items():
        summary = _latency_summary(list(values or []))
        if not summary:
            continue
        metric_prefix = f"pipeline_latency.{stage_name}"
        metrics[f"{metric_prefix}.count"] = float(summary["count"])
        metrics[f"{metric_prefix}.avg_ms"] = float(summary["avg_ms"])
        metrics[f"{metric_prefix}.p95_ms"] = float(summary["p95_ms"])
        metrics[f"{metric_prefix}.max_ms"] = float(summary["max_ms"])

    if not metrics:
        return

    metrics["pipeline_latency.events_scanned"] = float(max(0, int(event_count)))
    metrics["pipeline_latency.predictions_written"] = float(max(0, int(prediction_count)))
    emit_snapshot(
        metrics,
        tags={
            "component": "engine.data.jobs.process_events",
            "job": JOB_NAME,
        },
    )


def _trace_step(event_id: int, step: str, **extra: Any) -> None:
    if not TRACE_STEPS:
        return
    payload = " ".join(f"{k}={v}" for k, v in extra.items())
    msg = f"TRACE event_id={int(event_id)} step={step}"
    if payload:
        msg = f"{msg} {payload}"
    logging.info(msg)


def _trace_main(step: str, **extra: Any) -> None:
    if not TRACE_STEPS:
        return
    payload = " ".join(f"{k}={v}" for k, v in extra.items())
    msg = f"TRACE_MAIN step={step}"
    if payload:
        msg = f"{msg} {payload}"
    logging.info(msg)


def _features_hash(vec: np.ndarray) -> str:
    return str(hash_feature_vector(vec.tolist()) or "")


def _alert_details_dict(details: Any) -> Dict[str, Any]:
    return dict(details) if isinstance(details, dict) else {}


def _cosine_max_sim(vec: np.ndarray, mat: np.ndarray) -> float:
    """
    vec: (d,) float32
    mat: (n,d) float32
    returns max cosine similarity in [-1,1]
    """
    if mat is None or mat.size == 0:
        return 0.0
    v = vec.astype(np.float32, copy=False)
    m = mat.astype(np.float32, copy=False)

    vnorm = float(np.linalg.norm(v))
    if vnorm <= 1e-12:
        return 0.0

    mn = np.linalg.norm(m, axis=1)
    good = mn > 1e-12
    if not np.any(good):
        return 0.0

    sims = (m[good] @ v) / (mn[good] * vnorm)
    if sims.size == 0:
        return 0.0
    return float(np.max(sims))

def _compute_novelty(con, event_id: int, vec: np.ndarray, lookback: int) -> float:
    """
    Novelty = 1 - max cosine similarity to recent embedded events.
    Uses in-memory cache first, DB as fallback.
    """
    try:
        # --- Fast path: in-memory cache ---
        if len(_RECENT_EMB_CACHE) >= max(1, int(NOVELTY_MIN_EVENTS)):
            mat = np.vstack(_RECENT_EMB_CACHE[-lookback:]).astype(np.float32, copy=False)
            max_sim = _cosine_max_sim(vec, mat)
            novelty = 1.0 - max_sim
            if novelty == novelty:
                return float(max(0.0, min(1.0, novelty)))
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_NOVELTY_CACHE_FAILED", e, once_key="novelty_cache_fast_path", event_id=int(event_id))

    # --- Fallback: DB lookup ---
    try:
        rows = con.execute(
            """
            SELECT emb.dim, emb.vec
            FROM event_embeddings emb
            JOIN events e ON e.id = emb.event_id
            WHERE emb.event_id != ?
            ORDER BY e.ts_ms DESC
            LIMIT ?
            """,
            (int(event_id), int(lookback)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_NOVELTY_QUERY_FAILED",
            e,
            once_key=f"process_events_novelty_query_failed:{event_id}",
            event_id=int(event_id),
        )
        novelty = 0.0
        return novelty

    if not rows or len(rows) < max(1, int(NOVELTY_MIN_EVENTS)):
        return 0.0

    try:
        mats: List[np.ndarray] = []
        d0 = int(rows[0][0] or 0)
        if d0 <= 0:
            return 0.0

        for dim, blob in rows:
            if int(dim or 0) != d0 or not blob:
                continue
            a = np.frombuffer(blob, dtype=np.float32)
            if a.size == d0:
                mats.append(a)

        if len(mats) < max(1, int(NOVELTY_MIN_EVENTS)):
            return 0.0

        mat = np.vstack(mats).astype(np.float32, copy=False)
        max_sim = _cosine_max_sim(vec, mat)
        novelty = 1.0 - max_sim
        if novelty != novelty:
            return 0.0
        return float(max(0.0, min(1.0, novelty)))
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_NOVELTY_COMPUTE_FAILED",
            e,
            once_key=f"process_events_novelty_compute_failed:{event_id}",
            event_id=int(event_id),
        )
        novelty = 0.0
        return novelty

def _update_event_meta_json(con, event_id: int, meta: Dict[str, Any]) -> None:
    """
    Best-effort UPDATE events.meta_json. Fail-soft if column doesn't exist.
    """
    try:
        con.execute(
            "UPDATE events SET meta_json=? WHERE id=?",
            (json.dumps(meta or {}, separators=(",", ":"), sort_keys=True), int(event_id)),
        )
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_META_UPDATE_FAILED", e, once_key="event_meta_update", event_id=int(event_id))


def _ensure_live_write_connection(con, cur):
    try:
        con.execute("SELECT 1")
        return con, cur
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_REOPEN_WRITE_CONNECTION_FAILED",
            e,
            once_key="reopen_write_connection",
        )
        con2 = _new_connection(readonly=False)
        return con2, con2.cursor()


def _exec_cost_context(con, symbol: str) -> Dict[str, Any]:
    try:
        row = con.execute(
            """
            SELECT ts_ms, last, bid, ask, spread, source
            FROM price_quotes
            WHERE symbol=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()

        if not row:
            return {}

        ts_ms, last, bid, ask, spread, src = row

        if spread is None and bid is not None and ask is not None:
            spread = float(ask) - float(bid)

        spread_bps = None
        if last and spread and last > 1e-9:
            spread_bps = 10000.0 * float(spread) / float(last)

        return {
            "ts_ms": int(ts_ms),
            "last": float(last) if last is not None else None,
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "spread": float(spread) if spread is not None else None,
            "spread_bps": float(spread_bps) if spread_bps is not None else None,
            "source": src,
        }
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_EXEC_COST_CONTEXT_FAILED",
            e,
            once_key=f"process_events_exec_cost_context_failed:{symbol}",
            symbol=str(symbol),
        )
        context = {}
        return context

def _detect_exec_cost_spike(con) -> Dict[str, Any]:
    """
    Returns:
      {
        "spike": bool,
        "avg_spread_bps": float|None,
        "p90_spread_bps": float|None,
        "n": int,
        "window_s": int,
        "cutoff_ts_ms": int,
        "symbols": [..]  # sampled symbols
      }
    """
    now_ms = int(time.time() * 1000)
    cutoff = int(now_ms - int(EXEC_COST_SPIKE_WINDOW_S) * 1000)

    # Sample most recently updated ACTIVE/WATCH symbols (bounded)
    syms = []
    try:
        rows = con.execute(
            """
            SELECT symbol
            FROM symbols
            WHERE status IN ('ACTIVE','WATCH')
            ORDER BY updated_ts_ms DESC
            LIMIT ?
            """,
            (int(max(1, EXEC_COST_SPIKE_SYMBOL_LIMIT)),),
        ).fetchall()
        for r in rows or []:
            if r and r[0]:
                syms.append(str(r[0]))
    except Exception:
        syms = []

    if not syms:
        return {
            "spike": False,
            "avg_spread_bps": None,
            "p90_spread_bps": None,
            "n": 0,
            "window_s": int(EXEC_COST_SPIKE_WINDOW_S),
            "cutoff_ts_ms": int(cutoff),
            "symbols": [],
        }

    spreads = []
    try:
        q = ",".join(["?"] * len(syms))
        rows = con.execute(
            f"""
            SELECT last, bid, ask, spread
            FROM price_quotes
            WHERE ts_ms >= ?
              AND symbol IN ({q})
            """,
            (int(cutoff), *syms),
        ).fetchall()

        for last, bid, ask, spr in rows or []:
            try:
                last = float(last) if last is not None else None
                if last is None or last <= 1e-9:
                    continue

                if spr is None and bid is not None and ask is not None:
                    spr = float(ask) - float(bid)
                if spr is None:
                    continue

                spr = float(spr)
                sbps = 10000.0 * spr / last
                if sbps == sbps and sbps >= 0.0:
                    spreads.append(float(sbps))
            except Exception as e:
                _warn_nonfatal(
                    "PROCESS_EVENTS_EXEC_COST_SPIKE_ROW_PARSE_FAILED",
                    e,
                    once_key="process_events_exec_cost_spike_row_parse_failed",
                    row_repr=repr((last, bid, ask, spr)),
                )
                continue
    except Exception:
        spreads = []

    n = int(len(spreads))
    if n < int(max(1, EXEC_COST_SPIKE_MIN_N)):
        return {
            "spike": False,
            "avg_spread_bps": (float(np.mean(spreads)) if spreads else None),
            "p90_spread_bps": (float(np.percentile(spreads, 90)) if spreads else None),
            "n": int(n),
            "window_s": int(EXEC_COST_SPIKE_WINDOW_S),
            "cutoff_ts_ms": int(cutoff),
            "symbols": list(syms),
        }

    avg_bps = float(np.mean(spreads))
    p90_bps = float(np.percentile(spreads, 90))

    spike = avg_bps >= float(EXEC_COST_SPIKE_BPS)

    return {
        "spike": bool(spike),
        "avg_spread_bps": float(avg_bps),
        "p90_spread_bps": float(p90_bps),
        "n": int(n),
        "window_s": int(EXEC_COST_SPIKE_WINDOW_S),
        "cutoff_ts_ms": int(cutoff),
        "symbols": list(syms),
    }

def _options_context(con, symbol: str) -> Dict[str, float | int | None]:
    """
    Returns lightweight IV/OI context for explain + confidence shaping.
    """
    try:
        row = con.execute(
            """
            SELECT AVG(iv), SUM(open_interest)
            FROM options_chain
            WHERE symbol=?
              AND ts_ms >= ?
            """,
            (str(symbol), int(time.time() * 1000) - 3600_000),
        ).fetchone()
        if not row:
            return {}
        return {
            "avg_iv": float(row[0]) if row[0] is not None else None,
            "open_interest": int(row[1]) if row[1] is not None else None,
        }
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_OPTIONS_CONTEXT_FAILED",
            e,
            once_key=f"process_events_options_context_failed:{symbol}",
            symbol=str(symbol),
        )
        context = {}
        return context

def _options_anomaly(con, symbol: str) -> Dict[str, float]:
    """
    Compute anomaly signals from options_chain:
      - iv_ratio_1h_24h
      - oi_delta_1h_24h
    """
    now_ms = int(time.time() * 1000)
    h1 = now_ms - 3600_000
    h24 = now_ms - 24 * 3600_000

    try:
        r1 = con.execute(
            """
            SELECT AVG(iv), AVG(open_interest)
            FROM options_chain
            WHERE symbol=? AND ts_ms >= ?
            """,
            (str(symbol), int(h1)),
        ).fetchone()
        r24 = con.execute(
            """
            SELECT AVG(iv), AVG(open_interest)
            FROM options_chain
            WHERE symbol=? AND ts_ms >= ?
            """,
            (str(symbol), int(h24)),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_OPTIONS_ANOMALY_QUERY_FAILED",
            e,
            once_key=f"process_events_options_anomaly_query_failed:{symbol}",
            symbol=str(symbol),
        )
        out = {}
        return out

    try:
        iv1 = float(r1[0]) if r1 and r1[0] is not None else None
        iv24 = float(r24[0]) if r24 and r24[0] is not None else None
        oi1 = float(r1[1]) if r1 and r1[1] is not None else None
        oi24 = float(r24[1]) if r24 and r24[1] is not None else None
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_OPTIONS_ANOMALY_PARSE_FAILED",
            e,
            once_key=f"process_events_options_anomaly_parse_failed:{symbol}",
            symbol=str(symbol),
        )
        out = {}
        return out

    out = {}

    if iv1 is not None and iv24 is not None and iv24 > 1e-12:
        out["iv_ratio_1h_24h"] = float(iv1 / iv24)

    if oi1 is not None and oi24 is not None:
        out["oi_delta_1h_24h"] = float(oi1 - oi24)

    return out

def _earnings_context(con, symbol: str) -> Dict[str, Any]:
    """
    Returns next earnings date if within lookahead window.
    """
    try:
        look_days = int(os.environ.get("EARNINGS_EVENT_LOOKAHEAD_DAYS", "10"))
    except Exception:
        look_days = 10

    try:
        # date strings are YYYY-MM-DD; SQLite text compares correctly
        row = con.execute(
            """
            SELECT earnings_date, time_of_day, eps_est, revenue_est, source
            FROM earnings_calendar
            WHERE symbol=?
              AND earnings_date >= date('now')
              AND earnings_date <= date('now', ?)
            ORDER BY earnings_date ASC
            LIMIT 1
            """,
            (str(symbol), f"+{int(look_days)} day"),
        ).fetchone()
        if not row:
            return {}
        return {
            "earnings_date": str(row[0]),
            "time_of_day": (str(row[1]) if row[1] is not None else None),
            "eps_est": (float(row[2]) if row[2] is not None else None),
            "revenue_est": (float(row[3]) if row[3] is not None else None),
            "source": (str(row[4]) if row[4] is not None else None),
        }
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_EARNINGS_CONTEXT_FAILED",
            e,
            once_key=f"process_events_earnings_context_failed:{symbol}",
            symbol=str(symbol),
        )
        context = {}
        return context


def _sec_filing_context(con, symbol: str) -> Dict[str, Any]:
    """
    Returns most recent filing in last N days (default 3).
    """
    try:
        look_days = int(os.environ.get("SEC_FILING_LOOKBACK_DAYS", "3"))
    except Exception:
        look_days = 3

    try:
        row = con.execute(
            """
            SELECT form, filed_date, accession, primary_doc_url, source
            FROM sec_filings
            WHERE symbol=?
              AND filed_date >= date('now', ?)
            ORDER BY filed_date DESC
            LIMIT 1
            """,
            (str(symbol), f"-{int(look_days)} day"),
        ).fetchone()
        if not row:
            return {}
        return {
            "form": str(row[0]),
            "filed_date": str(row[1]),
            "accession": str(row[2]),
            "primary_doc_url": (str(row[3]) if row[3] is not None else None),
            "source": (str(row[4]) if row[4] is not None else None),
        }
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_SEC_FILING_CONTEXT_FAILED",
            e,
            once_key=f"process_events_sec_filing_context_failed:{symbol}",
            symbol=str(symbol),
        )
        context = {}
        return context


def _insider_context(con, symbol: str) -> Dict[str, Any]:
    try:
        look_days = int(os.environ.get("FORM4_CONTEXT_LOOKBACK_DAYS", "30"))
    except Exception:
        look_days = 30

    cutoff_ms = int(time.time() * 1000) - (int(look_days) * 24 * 3600 * 1000)
    try:
        row = con.execute(
            """
            SELECT transaction_type, direction, value, shares, insider_name, insider_role,
                   filing_accession, transaction_date, filing_date, source
            FROM insider_transactions
            WHERE symbol=?
              AND COALESCE(transaction_ts_ms, filing_ts_ms, ingested_ts_ms, 0) >= ?
            ORDER BY COALESCE(transaction_ts_ms, filing_ts_ms, ingested_ts_ms) DESC
            LIMIT 1
            """,
            (str(symbol), int(cutoff_ms)),
        ).fetchone()
        if not row:
            return {}
        return {
            "transaction_type": str(row[0]) if row[0] is not None else None,
            "direction": str(row[1]) if row[1] is not None else None,
            "value": float(row[2]) if row[2] is not None else None,
            "shares": float(row[3]) if row[3] is not None else None,
            "insider_name": str(row[4]) if row[4] is not None else None,
            "insider_role": str(row[5]) if row[5] is not None else None,
            "filing_accession": str(row[6]) if row[6] is not None else None,
            "transaction_date": str(row[7]) if row[7] is not None else None,
            "filing_date": str(row[8]) if row[8] is not None else None,
            "source": str(row[9]) if row[9] is not None else None,
        }
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_INSIDER_CONTEXT_FAILED",
            e,
            once_key=f"process_events_insider_context_failed:{symbol}",
            symbol=str(symbol),
        )
        context = {}
        return context


def _congressional_context(con, symbol: str) -> Dict[str, Any]:
    try:
        look_days = int(os.environ.get("CONGRESSIONAL_CONTEXT_LOOKBACK_DAYS", "90"))
    except Exception:
        look_days = 90

    cutoff_ms = int(time.time() * 1000) - (int(look_days) * 24 * 3600 * 1000)
    try:
        row = con.execute(
            """
            SELECT politician_name, chamber, office, transaction_type, direction,
                   amount_mid, transaction_date, disclosure_date, source
            FROM congressional_trades
            WHERE symbol=?
              AND COALESCE(transaction_ts_ms, disclosure_ts_ms, ingested_ts_ms, 0) >= ?
            ORDER BY COALESCE(transaction_ts_ms, disclosure_ts_ms, ingested_ts_ms) DESC
            LIMIT 1
            """,
            (str(symbol), int(cutoff_ms)),
        ).fetchone()
        if not row:
            return {}
        return {
            "politician_name": str(row[0]) if row[0] is not None else None,
            "chamber": str(row[1]) if row[1] is not None else None,
            "office": str(row[2]) if row[2] is not None else None,
            "transaction_type": str(row[3]) if row[3] is not None else None,
            "direction": str(row[4]) if row[4] is not None else None,
            "amount_mid": float(row[5]) if row[5] is not None else None,
            "transaction_date": str(row[6]) if row[6] is not None else None,
            "disclosure_date": str(row[7]) if row[7] is not None else None,
            "source": str(row[8]) if row[8] is not None else None,
        }
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_CONGRESSIONAL_CONTEXT_FAILED",
            e,
            once_key=f"process_events_congressional_context_failed:{symbol}",
            symbol=str(symbol),
        )
        context = {}
        return context

def _tradability_from_pred(expected_z: float, horizon_s: int, novelty: float) -> Dict[str, float]:
    """
    Convert model output (impact z) into tradability proxies.
    Explain-only, intentionally conservative.
    """
    try:
        z = float(expected_z)
    except Exception:
        z = 0.0
    try:
        h = max(1, int(horizon_s))
    except Exception:
        h = 3600

    # Adjust horizon by sqrt(time) relative to 1h baseline
    h_scale = (h / 3600.0) ** 0.5

    expected_ret = z * float(RET_SCALE_PER_Z) * float(h_scale)
    expected_cost = float(COST_BPS) / 10000.0

    # Win-prob proxy: monotonic in z + modest novelty boost
    p_win = 0.5 + 0.15 * max(-3.0, min(3.0, z))
    p_win += 0.05 * max(0.0, min(1.0, float(novelty)))
    p_win = max(0.0, min(1.0, p_win))

    expected_dd = abs(z) * 0.001 * float(h_scale)

    return {
        "p_win": float(p_win),
        "expected_ret": float(expected_ret),
        "expected_cost": float(expected_cost),
        "expected_ret_net": float(expected_ret - expected_cost),
        "expected_dd": float(expected_dd),
    }

# ------            -- ------------------------------------------------------
# Heuristic relevance rules (title-only)
# ------            -- ------------------------------------------------------

_RELEVANCE_RULES = {
    "BTC": [
        ("crypto keywords", r"\b(bitcoin|btc|crypto|cryptocurrency|ethereum|eth|defi|blockchain)\b"),
        ("major exchanges", r"\b(coinbase|binance|kraken)\b"),
        ("regulation / ETF", r"\b(sec|etf|spot etf|crypto etf|regulat(ion|ory))\b"),
        ("stablecoins", r"\b(stablecoin|usdt|tether|usdc)\b"),
    ],
    "OIL": [
        ("oil keywords", r"\b(oil|crude|wti|brent)\b"),
        ("OPEC / supply", r"\b(opec|opec\+|production cut|output)\b"),
        ("energy / gasoline", r"\b(energy|gasoline|diesel|refiner(y|ies)|pipeline)\b"),
        ("geopolitics", r"\b(iran|iraq|saudi|russia|ukraine|middle east|red sea)\b"),
    ],
    "SPY": [
        ("macro policy", r"\b(fed|fomc|rates?|hike|cut|qt|qe|powell)\b"),
        ("inflation / jobs", r"\b(cpi|pce|inflation|nfp|payrolls?|unemployment|jobs report)\b"),
        ("bonds / yields", r"\b(yield(s)?|treasur(y|ies)|bond(s)?|curve)\b"),
        ("equities broad", r"\b(sp\s*500|s&p|nasdaq|dow|equities?|stocks?)\b"),
        ("growth / recession", r"\b(recession|gdp|soft landing|hard landing)\b"),
        ("earnings broad", r"\b(earnings|guidance)\b"),
    ],
}

_COMPILED = {
    sym: [(label, re.compile(pat, flags=re.IGNORECASE)) for (label, pat) in rules]
    for sym, rules in _RELEVANCE_RULES.items()
}


def _score_from_hit_count(n: int) -> float:
    if n <= 0:
        return 0.0
    if n == 1:
        return 0.35
    if n == 2:
        return 0.60
    return 0.85


def relevance_for_title(title: str, symbol: str) -> Tuple[float, List[str]]:
    t = (title or "").strip()
    if not t:
        return 0.0, []
    reasons: List[str] = []
    for label, rx in _COMPILED.get(symbol, []):
        try:
            if rx.search(t):
                reasons.append(label)
        except re.error as e:
            _warn_nonfatal(
                "PROCESS_EVENTS_RELEVANCE_REGEX_FAILED",
                e,
                once_key=f"process_events_relevance_regex_failed:{symbol}:{label}",
                symbol=str(symbol),
                label=str(label),
            )
            continue
    return _score_from_hit_count(len(reasons)), reasons

# ------            -- ------------------------------------------------------
# Symbol discovery (WATCH-only)
# ------            -- ------------------------------------------------------

_SYMBOL_PATTERNS = [
    # Equities / ETFs
    r"\b([A-Z]{2,5})\b",
    # Crypto tickers
    r"\b(BTC|ETH|SOL|BNB|XRP|ADA|AVAX|DOT|LINK)\b",
]

def discover_symbols_from_text(text: str):
    if not text:
        return set()
    out = set()
    for pat in _SYMBOL_PATTERNS:
        try:
            for m in re.findall(pat, text):
                sym = m if isinstance(m, str) else m[0]
                if 2 <= len(sym) <= 6:
                    out.add(sym.upper())
        except Exception as e:
            _warn_nonfatal(
                "PROCESS_EVENTS_SYMBOL_PATTERN_PARSE_FAILED",
                e,
                once_key=f"process_events_symbol_pattern_parse_failed:{pat}",
                pattern=str(pat),
            )
            continue
    return out

def upsert_watch_symbols(con, symbols, ts_ms: int):
    for sym in symbols:
        try:
            con.execute(
                """
                INSERT INTO symbol_universe(symbol, status, first_seen_ms, last_seen_ms, seen_n)
                VALUES (?, 'WATCH', ?, ?, 1)
                ON CONFLICT(symbol) DO UPDATE SET
                  last_seen_ms=excluded.last_seen_ms,
                  seen_n=seen_n+1
                """,
                (str(sym), int(ts_ms), int(ts_ms)),
            )
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_WATCH_SYMBOL_UPSERT_FAILED", e, once_key=f"watch_symbol_upsert:{sym}", symbol=str(sym), ts_ms=int(ts_ms))

def relevance_map(title: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for sym in symbols:
        score, reasons = relevance_for_title(title, sym)
        out[sym] = {"score": float(score), "reasons": list(reasons)}
    return out

# ------            -- ------------------------------------------------------
# Embedding model (lazy init)
# ------            -- ------------------------------------------------------

_model: Any = None


def _get_model() -> Any:
    global _model
    if _model is None:
        resolution = resolve_torch_device(torch, env_var="EMBED_DEVICE", fallback_envs=("TORCH_DEVICE",))
        dev = resolution.resolved

        # Performance flags. CUDA-specific flags are gated by the resolved device.
        try:
            torch.set_float32_matmul_precision(os.environ.get("TORCH_MATMUL_PRECISION", "high"))
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_TORCH_MATMUL_PRECISION_FAILED", e, once_key="torch_matmul_precision")
        if _CUDA_RUNTIME_ENABLED:
            try:
                torch.backends.cuda.matmul.allow_tf32 = os.environ.get("TORCH_ALLOW_TF32", "1") == "1"
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_TORCH_CUDA_TF32_FAILED", e, once_key="torch_cuda_tf32")
            try:
                torch.backends.cudnn.allow_tf32 = os.environ.get("CUDNN_ALLOW_TF32", "1") == "1"
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_TORCH_CUDNN_TF32_FAILED", e, once_key="torch_cudnn_tf32")
            try:
                torch.backends.cudnn.benchmark = os.environ.get("CUDNN_BENCHMARK", "1") == "1"
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_TORCH_CUDNN_BENCHMARK_FAILED", e, once_key="torch_cudnn_benchmark")

        # Allow separate disks via env (avoid OS drive fallback)
        for _k in ("HF_HOME", "TRANSFORMERS_CACHE", "SENTENCE_TRANSFORMERS_HOME"):
            if _k in os.environ:
                try:
                    Path(os.environ[_k]).mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    _warn_nonfatal("PROCESS_EVENTS_MODEL_CACHE_DIR_MKDIR_FAILED", e, once_key=f"model_cache_dir:{_k}", env_key=str(_k), path=str(os.environ.get(_k) or ""))

        _model = _SENTENCE_TRANSFORMER_CLS("all-MiniLM-L6-v2", device=dev)

    return _model

def _require_fresh_prices_or_exit(job_name: str) -> None:
    stale_after_ms = int(float(os.environ.get("PROCESS_EVENTS_PRICE_MAX_AGE_S", "180")) * 1000.0)
    min_rows = int(os.environ.get("PROCESS_EVENTS_MIN_FRESH_PRICE_ROWS", "3"))
    min_symbol_coverage = float(os.environ.get("PROCESS_EVENTS_MIN_FRESH_SYMBOL_COVERAGE", "0.60"))
    wait_timeout_s = float(os.environ.get("PROCESS_EVENTS_WAIT_FOR_PRICES_S", "120"))
    poll_every_s = max(1.0, float(os.environ.get("PROCESS_EVENTS_WAIT_FOR_PRICES_POLL_S", "2")))
    deadline = time.time() + max(0.0, wait_timeout_s)
    warned_wait = False

    while True:
        now_ms = int(time.time() * 1000)
        con = connect_ro()
        try:
            active_row = con.execute(
                """
                SELECT COUNT(*)
                FROM symbols
                WHERE status IN ('ACTIVE','WATCH')
                """
            ).fetchone() or (0,)

            row = con.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT symbol), MAX(ts_ms)
                FROM prices
                WHERE COALESCE(price, px) IS NOT NULL
                  AND ts_ms >= ?
                """,
                (int(now_ms - stale_after_ms),),
            ).fetchone() or (0, 0, None)
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_CONNECTION_CLOSE_FAILED", e, once_key="require_fresh_prices_close", scope="require_fresh_prices")

        active_symbols = int(active_row[0] or 0)
        required_symbols = int(min_rows)
        if active_symbols > 0:
            required_symbols = max(required_symbols, int(max(1, active_symbols * min_symbol_coverage)))

        ipc_state = market_data_status(max_age_ms=stale_after_ms)
        if ipc_state.get("ok"):
            ipc_fresh_n = int(ipc_state.get("fresh_rows") or 0)
            ipc_fresh_symbols = int(ipc_state.get("fresh_symbols") or 0)
            ipc_last_ts_ms = int(ipc_state.get("last_price_ts_ms") or 0)
            ipc_running = bool(ipc_state.get("running"))
            ipc_child = str(ipc_state.get("active_child") or "")

            if (
                ipc_running
                and ipc_last_ts_ms >= int(now_ms - stale_after_ms)
                and ipc_fresh_n >= min_rows
                and ipc_fresh_symbols >= required_symbols
            ):
                return

            if not warned_wait and wait_timeout_s > 0:
                _warn_nonfatal(
                    "PROCESS_EVENTS_WAITING_FOR_MARKET_DATA",
                    RuntimeError("waiting for ipc market_data"),
                    once_key=f"market_data_wait:{job_name}",
                    job_name=str(job_name),
                    ipc_running=bool(ipc_running),
                    ipc_child=bool(ipc_child),
                    ipc_fresh_n=int(ipc_fresh_n),
                    ipc_fresh_symbols=int(ipc_fresh_symbols),
                    required_symbols=int(required_symbols),
                    ipc_last_ts_ms=int(ipc_last_ts_ms),
                    stale_after_ms=int(stale_after_ms),
                    timeout_s=float(wait_timeout_s),
                )

        fresh_n = int(row[0] or 0)
        fresh_symbols = int(row[1] or 0)
        last_ts_ms = int(row[2] or 0)

        if fresh_n >= min_rows and fresh_symbols >= required_symbols:
            return

        if time.time() >= deadline or wait_timeout_s <= 0:
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

        if not warned_wait:
            _warn_nonfatal(
                "PROCESS_EVENTS_WAITING_FOR_FRESH_PRICES",
                RuntimeError("waiting for fresh prices"),
                once_key=f"fresh_prices_wait:{job_name}",
                job_name=str(job_name),
                fresh_n=int(fresh_n),
                fresh_symbols=int(fresh_symbols),
                required_symbols=int(required_symbols),
                active_symbols=int(active_symbols),
                last_ts_ms=int(last_ts_ms),
                stale_after_ms=int(stale_after_ms),
                timeout_s=float(wait_timeout_s),
            )
            warned_wait = True

        time.sleep(poll_every_s)

# ------            -- ------------------------------------------------------
# Main loop
# ------            -- ------------------------------------------------------

def main() -> None:
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)
    _trace_main("lock_acquired", job=JOB_NAME, pid=PID)

    last_hb_s = 0.0
    started_ms = int(time.time() * 1000)

    # Crash-safe resume checkpoint (best-effort, idempotent)
    ck = {"last_event_id": 0, "last_event_ts_ms": 0}
    try:
        ck = get_job_checkpoint(JOB_NAME)
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_CHECKPOINT_READ_FAILED", e, once_key="job_checkpoint_read", job_name=str(JOB_NAME))

    # Commit cadence (avoid losing whole batch if crash)
    COMMIT_EVERY_EVENTS = int(os.environ.get("COMMIT_EVERY_EVENTS", "1"))  # 1 = safest
    _since_commit = 0

    try:
        # Phase 3: Global rules engine (auto kill-switch)
        try:
            evaluate_rules()
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_RULES_EVALUATION_FAILED", e, once_key="evaluate_rules")
        _trace_main("rules_evaluated")

        allow0, _, _ = execution_allowed(symbol=None, regime=None)
        _trace_main("execution_allowed_checked", allow=int(bool(allow0)))
        if not allow0:
            _warn_nonfatal(
                "PROCESS_EVENTS_KILL_SWITCH_BLOCKED",
                RuntimeError("execution blocked by kill switch"),
                once_key="kill_switch_blocked",
            )

        _require_fresh_prices_or_exit(JOB_NAME)
        _trace_main("fresh_prices_ready")

        # Load dynamic universe (ACTIVE + WATCH). Fallback if empty.
        conu = connect_ro()

        try:
            try:
                symbols = get_active_symbols(conu, limit=int(os.environ.get("PROCESS_SYMBOL_LIMIT", "2000")))
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_SYMBOL_LOAD_FAILED", e, once_key="load_active_symbols")
                symbols = []

            if not symbols:
                symbols = list(DEFAULT_SYMBOLS)

            symbols = list(dict.fromkeys(symbols))  # de-dup, preserve order
            _trace_main("symbols_loaded", n=len(symbols))

            # Feature 4: Kill-switch on execution cost spikes (spread)
            try:
                spike_info = _detect_exec_cost_spike(conu)
                if spike_info and spike_info.get("spike"):
                    logging.error("EXEC_COST_SPIKE spike_info=%s", spike_info)

                    try:
                        emit_alert(
                            event_title="Execution cost spike — trading halted",
                            symbol="SPY",
                            horizon_s=0,
                            expected_z=0.0,
                            confidence=1.0,
                            explain={
                                "type": "exec_cost_spike",
                                "spike_info": spike_info,
                                "threshold_bps": float(EXEC_COST_SPIKE_BPS),
                            },
                        )
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_EXEC_COST_SPIKE_ALERT_FAILED", e, once_key="exec_cost_spike_alert")

                    return

            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_EXEC_COST_SPIKE_CHECK_FAILED", e, once_key="exec_cost_spike_check_startup")

            # Preload symbol status map (avoid per-symbol DB queries)
            symbol_status = {}
            try:
                rows = conu.execute("SELECT symbol, status FROM symbol_universe").fetchall()
                for s, st in rows or []:
                    symbol_status[str(s)] = str(st)
            except Exception:
                symbol_status = {}
        finally:
            try:
                conu.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_CONNECTION_CLOSE_FAILED", e, once_key="symbols_connection_close", scope="load_symbols")

        # Read candidate events (no write txn)
        con = connect_ro()
        _trace_main("event_scan_begin")

        try:
            rows = con.execute(
                """
                SELECT e.id, e.ts_ms, e.source, e.title, e.body, e.url, e.meta_json
                FROM events e
                LEFT JOIN event_embeddings emb ON emb.event_id = e.id
                WHERE emb.event_id IS NULL
                  AND (e.id > ? OR e.ts_ms > ?)
                ORDER BY e.ts_ms ASC, e.id ASC
                LIMIT 50
                """,
                (int(ck.get("last_event_id", 0)), int(ck.get("last_event_ts_ms", 0))),
            ).fetchall()
            _trace_main("event_scan_done", n=len(rows))

        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_CONNECTION_CLOSE_FAILED", e, once_key="event_scan_connection_close", scope="scan_events")

        if not rows:
            logging.info("no new events to process")
            return

        # Embed outside write transaction
        titles = [(r[3] or "") for r in rows]
        _trace_main("embedding_begin", n=len(titles))
        if _LIVE_STREAM is not None and _CUDA_RUNTIME_ENABLED:
            with torch.cuda.stream(cast(Any, _LIVE_STREAM)):
                embeddings = _get_model().encode(

                    titles,
                    batch_size=int(os.environ.get("EMBED_BATCH_SIZE", "64")),
                    show_progress_bar=False,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                )
                torch.cuda.synchronize(cast(Any, _LIVE_STREAM))

                embeddings = embeddings.astype(np.float32, copy=False)
                embeddings.setflags(write=False)

        else:
            embeddings = _get_model().encode(
                titles,
                batch_size=int(os.environ.get("EMBED_BATCH_SIZE", "64")),
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,
            ).astype(np.float32, copy=False)
        _trace_main("embedding_done", n=len(titles))

        conw = _new_connection(readonly=False)

        try:
            cur = conw.cursor()
            pending_signal_publications: List[Dict[str, Any]] = []
            ingestion_to_db_latencies: List[int] = []
            db_to_prediction_latencies: List[int] = []
            prediction_to_decision_latencies: List[int] = []
            predictions_written = 0

            for (eid, ts_ms, source, title, body, url, meta_json), vec in zip(rows, embeddings):
                _gpu_throttle_if_needed()
                pending_watch_symbols = set()
                pending_predictions: List[Dict[str, Any]] = []
                pending_decisions: List[Dict[str, Any]] = []
                pending_alerts: List[Dict[str, Any]] = []

                now_s = time.time()

                if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                    try:
                        touch_job_lock(JOB_NAME, OWNER, PID)
                        put_job_heartbeat(
                            JOB_NAME,
                            OWNER,
                            PID,
                            extra_json=json.dumps(
                                {"event_id": int(eid), "event_ts_ms": int(ts_ms)},
                                separators=(",", ":"),
                                sort_keys=True,
                            ),
                        )
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_HEARTBEAT_UPDATE_FAILED", e, once_key="heartbeat_update", job_name=str(JOB_NAME), event_id=int(eid), event_ts_ms=int(ts_ms))
                    last_hb_s = now_s

                title = title or ""
                body = body or ""
                source = source or ""
                url = url or ""
                ts_ms = int(ts_ms or 0)

                try:
                    event_meta = json.loads(meta_json) if meta_json else {}
                except Exception:
                    event_meta = {}
                if not isinstance(event_meta, dict):
                    event_meta = {}
                pipeline_timing = dict(event_meta.get("pipeline_timing") or {})
                db_observed_ts_ms = _safe_int(
                    pipeline_timing.get("db_observed_ts_ms")
                    or pipeline_timing.get("db_write_ts_ms")
                    or ts_ms
                    or int(time.time() * 1000),
                    ts_ms or int(time.time() * 1000),
                )
                ingestion_to_db_latency_ms = max(
                    0,
                    _safe_int(
                        pipeline_timing.get("ingestion_to_db_latency_ms"),
                        max(0, db_observed_ts_ms - ts_ms),
                    ),
                )

                domain = extract_domain(url, meta_json)
                if domain:
                    event_meta["domain"] = domain

                if TRACE_STEPS:
                    logging.info("EVENT id=%s ts_ms=%s title=%s", eid, ts_ms, title)
                _trace_step(int(eid), "event_start", symbols=len(symbols))

                # WATCH-only symbol discovery from text (never trades by itself)
                if DISCOVER_SYMBOLS:
                    try:
                        syms = set()
                        syms |= discover_symbols_from_text(title or "")
                        syms |= discover_symbols_from_text(body or "")
                        if syms:
                            pending_watch_symbols |= set(syms)
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_SYMBOL_DISCOVERY_FAILED", e, once_key="symbol_discovery", event_id=int(eid))

                rel_map = relevance_map(title, symbols)
                event_ctx = {
                    "event_id": int(eid),
                    "ts_ms": int(ts_ms),
                    "source": source,
                    "title": title,
                    "body": body,
                    "url": url,
                    "meta": event_meta,
                }

                # Batched prediction across all horizons (single forward path inside predictor)
                _gpu_throttle_if_needed()

                # Optional pinned prefetch (helps if predictor accepts torch.Tensor)
                if PINNED_PREFETCH:
                    _pinned_prefetch_to_device(vec)

                preds = predict_runtime_event(
                    vec,
                    symbols,
                    HORIZONS,
                    top_k=8,
                    event=event_ctx,
                )
                _trace_step(int(eid), "predict_event_done", mode="canonical_realtime")

                # Temporal predictor (shadow-mode only)
                temporal_shadow = None
                if ENABLE_EXPERIMENTAL_MODELS and predict_temporal_shadow_for_event:
                    try:
                        if _SHADOW_STREAM is not None and _CUDA_RUNTIME_ENABLED:
                            with torch.cuda.stream(cast(Any, _SHADOW_STREAM)):
                                temporal_shadow = predict_temporal_shadow_for_event(
                                    conw,
                                    event_id=int(eid),
                                    ts_ms=int(ts_ms),
                                    symbols=symbols,
                                    horizons=HORIZONS,
                                )
                        else:
                            temporal_shadow = predict_temporal_shadow_for_event(
                                conw,
                                event_id=int(eid),
                                ts_ms=int(ts_ms),
                                symbols=symbols,
                                horizons=HORIZONS,
                            )
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_TEMPORAL_SHADOW_FAILED", e, once_key="temporal_shadow", event_id=int(eid))
                        temporal_shadow = None
                _trace_step(int(eid), "temporal_shadow_done", present=bool(temporal_shadow))

                if ENABLE_SHADOW_PREDICTIONS and shadow_predict:
                    for sym in symbols:
                        try:
                            shadow_predict(
                                event_id=int(eid),
                                symbol=str(sym),
                                horizon_s=max(int(h) for h in HORIZONS),
                                features=vec,
                                temporal_predictions=(
                                    {
                                        k: v
                                        for k, v in (temporal_shadow or {}).items()
                                        if str(k[0]).upper().strip() == str(sym).upper().strip()
                                    }
                                    if isinstance(temporal_shadow, dict)
                                    else None
                                ),
                            )
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_SHADOW_PREDICT_FAILED", e, once_key="shadow_predict", event_id=int(eid), symbol=str(sym))

                # Update in-memory novelty cache (best-effort)
                _RECENT_EMB_CACHE.append(vec)
                if len(_RECENT_EMB_CACHE) > _RECENT_EMB_CACHE_MAX:
                    _RECENT_EMB_CACHE.pop(0)

                # Novelty scoring — compute + persist
                novelty = 0.0
                try:
                    novelty = _compute_novelty(conw, event_id=int(eid), vec=vec, lookback=int(NOVELTY_LOOKBACK))
                except Exception as e:
                    _warn_nonfatal("PROCESS_EVENTS_NOVELTY_COMPUTE_FAILED", e, once_key="novelty_compute", event_id=int(eid))
                    novelty = 0.0
                _trace_step(int(eid), "novelty_done", novelty=round(float(novelty), 4))

                event_meta["novelty"] = float(novelty)

                # Optional clustering
                cluster_info = None
                if assign_cluster:
                    try:
                        cluster_info = assign_cluster(event_id=int(eid), ts_ms=int(ts_ms), title=title, vec=vec)
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_CLUSTER_ASSIGN_FAILED", e, once_key="cluster_assign", event_id=int(eid))
                        cluster_info = None
                _trace_step(int(eid), "cluster_done", clustered=bool(cluster_info))

                # Store predictions + decisions + alerts
                conw, cur = _ensure_live_write_connection(conw, cur)
                for sym in symbols:
                    st = symbol_status.get(sym)
                    if st in ("DISABLED", "COOLDOWN"):
                        continue

                    for h in HORIZONS:
                        expected_z, conf, explain = preds[(sym, int(h))]
                        base_conf = float(conf)
                        adj_conf = float(base_conf)
                        adj_explain = {}
                        explain = dict(explain or {})

                        if event_meta:
                            explain["event_meta"] = event_meta

                        r = rel_map.get(sym) or {}
                        explain["relevance"] = float(r.get("score", 0.0))
                        explain["relevance_reasons"] = list(r.get("reasons", []))

                        explain["tradability"] = _tradability_from_pred(
                            expected_z=float(expected_z),
                            horizon_s=int(h),
                            novelty=float(novelty),
                        )

                        opt_ctx = _options_context(conw, sym)
                        if opt_ctx:
                            explain["options"] = opt_ctx

                        opt_anom = _options_anomaly(conw, sym)
                        if opt_anom:
                            explain["options_anomaly"] = opt_anom

                        reg = get_current_regime(sym)
                        explain["regime"] = str(reg)

                        if domain and is_domain_blocked(domain, sym):
                            continue
                        if domain:
                            explain["domain"] = domain

                        earn_ctx = _earnings_context(conw, sym)
                        if earn_ctx:
                            explain["earnings"] = earn_ctx

                        filing_ctx = _sec_filing_context(conw, sym)
                        if filing_ctx:
                            explain["sec_filing"] = filing_ctx

                        insider_ctx = _insider_context(conw, sym)
                        if insider_ctx:
                            explain["insider_transaction"] = insider_ctx

                        congressional_ctx = _congressional_context(conw, sym)
                        if congressional_ctx:
                            explain["congressional_trade"] = congressional_ctx

                        explain.setdefault("event", {"event_id": int(eid), "title": title})
                        explain.setdefault("relevance_map", rel_map)
                        if cluster_info:
                            explain["cluster"] = cluster_info

                        try:
                            cost_ctx = _exec_cost_context(conw, sym)
                            if cost_ctx:
                                explain["exec_cost"] = cost_ctx

                                sbps = cost_ctx.get("spread_bps")
                                if sbps is not None:
                                    sbps = float(sbps)

                                    hard_bps = float(os.environ.get("SPREAD_HARD_BLOCK_BPS", "80"))
                                    soft_bps = float(os.environ.get("SPREAD_SOFT_START_BPS", "15"))
                                    max_decay = float(os.environ.get("SPREAD_MAX_DECAY", "0.60"))
                                    slope = float(os.environ.get("SPREAD_DECAY_SLOPE", "0.01"))

                                    if sbps >= hard_bps:
                                        continue

                                    if sbps > soft_bps:
                                        excess = sbps - soft_bps
                                        mult = 1.0 - slope * excess
                                        if mult < max_decay:
                                            mult = max_decay
                                        adj_conf = adj_conf * mult
                                        explain["spread_conf_mult"] = float(mult)
                                    else:
                                        explain["spread_conf_mult"] = 1.0
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_SPREAD_CONFIDENCE_ADJUST_FAILED", e, once_key="spread_confidence_adjust", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        if ENABLE_EXPERIMENTAL_MODELS and isinstance(temporal_shadow, dict):
                            try:
                                k = (str(sym).upper().strip(), int(h))
                                if k in temporal_shadow:
                                    tz, tconf, texplain = temporal_shadow[k]
                                    explain["temporal_shadow"] = {
                                        "predicted_z": float(tz),
                                        "confidence": float(tconf),
                                        "explain": (texplain or {}),
                                    }
                            except Exception as e:
                                _warn_nonfatal("PROCESS_EVENTS_TEMPORAL_SHADOW_EXPLAIN_FAILED", e, once_key="temporal_shadow_explain", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        try:
                            adj_conf, adj_explain = get_adjusted_confidence(
                                conw,
                                symbol=sym,
                                horizon_s=int(h),
                                base_conf=float(adj_conf),
                            )
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_CONFIDENCE_ADJUST_FAILED", e, once_key="confidence_adjust", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        explain["confidence_adjust"] = adj_explain

                        try:
                            micro_ctx = load_latest_microstructure_context(
                                conw,
                                symbol=sym,
                                ts_ref_ms=int(time.time() * 1000),
                            )
                            adj_conf, micro_explain = apply_microstructure_confidence(
                                expected_z=float(expected_z),
                                base_conf=float(adj_conf),
                                micro_ctx=micro_ctx,
                            )
                            explain["microstructure"] = micro_explain
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_MICROSTRUCTURE_ADJUST_FAILED", e, once_key="microstructure_adjust", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        avg_iv = opt_ctx.get("avg_iv") if opt_ctx else None
                        if avg_iv is not None:
                            iv = float(avg_iv)
                            if iv > 1.0:
                                adj_conf = adj_conf * 0.85

                        if earn_ctx and earn_ctx.get("earnings_date"):
                            try:
                                adj_conf = adj_conf * float(os.environ.get("EARNINGS_CONF_DOWNWEIGHT", "0.85"))
                            except Exception as e:
                                _warn_nonfatal("PROCESS_EVENTS_EARNINGS_CONF_DOWNWEIGHT_FAILED", e, once_key="earnings_conf_downweight")
                                adj_conf = adj_conf * 0.85

                        if filing_ctx and filing_ctx.get("form"):
                            form = str(filing_ctx.get("form") or "").upper()
                            if form == "8-K":
                                try:
                                    adj_conf = adj_conf * float(os.environ.get("SEC_8K_CONF_DOWNWEIGHT", "0.90"))
                                except Exception as e:
                                    _warn_nonfatal("PROCESS_EVENTS_SEC_8K_CONF_DOWNWEIGHT_FAILED", e, once_key="sec_8k_conf_downweight")
                                    adj_conf = adj_conf * 0.90

                        try:
                            if opt_anom and opt_anom.get("iv_ratio_1h_24h") is not None:
                                ivr = float(opt_anom["iv_ratio_1h_24h"])
                                if ivr >= float(os.environ.get("IV_SPIKE_RATIO", "1.6")):
                                    adj_conf = adj_conf * float(os.environ.get("IV_SPIKE_CONF_DOWNWEIGHT", "0.88"))
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_IV_SPIKE_CONF_DOWNWEIGHT_FAILED", e, once_key="iv_spike_conf_downweight", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        try:
                            if domain:
                                mult = domain_conf_multiplier(domain, sym, reg, int(h))
                                adj_conf = adj_conf * float(mult)
                                explain["domain_conf_mult"] = float(mult)
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_DOMAIN_CONFIDENCE_FAILED", e, once_key="domain_confidence", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        signal_payload = describe_signal_confidence(
                            expected_z=float(expected_z),
                            confidence=float(adj_conf),
                            raw_confidence=((explain.get("confidence_engine") or {}).get("raw_confidence") if isinstance(explain.get("confidence_engine"), dict) else conf),
                            horizon_s=int(h),
                            calibration=((explain.get("confidence_engine") or {}).get("calibration") if isinstance(explain.get("confidence_engine"), dict) else None),
                            signal_ts_ms=int(ts_ms),
                        )
                        prediction_ts_ms = int(time.time() * 1000)
                        adj_conf = float(signal_payload["confidence"])
                        explain = apply_confidence_payload(explain, signal_payload)
                        explain["adjusted_confidence"] = float(adj_conf)
                        explain["regime"] = str(reg)
                        explain["pipeline_timing"] = {
                            "source_event_ts_ms": int(ts_ms),
                            "db_observed_ts_ms": int(db_observed_ts_ms),
                            "ingestion_to_db_latency_ms": int(ingestion_to_db_latency_ms),
                            "db_to_prediction_latency_ms": int(max(0, prediction_ts_ms - db_observed_ts_ms)),
                            "prediction_ts_ms": int(prediction_ts_ms),
                        }
                        explain["model_intent"] = build_model_intent(
                            symbol=str(sym),
                            horizon_s=int(h),
                            expected_z=float(expected_z),
                            confidence=float(adj_conf),
                            explain=explain,
                            regime=str(reg),
                            size_mult=float(signal_payload["size_mult"]),
                            prediction_strength=float(signal_payload["prediction_strength"]),
                        )

                        pending_predictions.append(
                            {
                                "event_id": int(eid),
                                "symbol": str(sym),
                                "horizon_s": int(h),
                                "predicted_z": float(expected_z),
                                "confidence": float(adj_conf),
                                "confidence_raw": float(signal_payload["raw_confidence"]),
                                "prediction_strength": float(signal_payload["prediction_strength"]),
                                "model_name": str(explain.get("model_name") or ""),
                                "model_id": str(explain.get("model_id") or ""),
                                "model_version": str(explain.get("model_version") or ""),
                                "features_version": str(explain.get("feature_set_tag") or "unknown"),
                                "prediction_ts_ms": int(prediction_ts_ms),
                            }
                        )

                        pending_decisions.append(
                            {
                                "event_id": int(eid),
                                "symbol": str(sym),
                                "horizon_s": int(h),
                                "predicted_z": float(expected_z),
                                "confidence": float(adj_conf),
                                "model_name": str(explain.get("model_name") or explain.get("model") or "knn"),
                                "model_kind": explain.get("model_kind"),
                                "model_ts_ms": explain.get("model_ts_ms"),
                                "features_hash": _features_hash(vec),
                                "feature_set_tag": str(explain.get("feature_set_tag") or ""),
                                "features_json": dict(explain.get("feature_snapshot") or {}),
                                "explain_json": explain,
                                "prediction_ts_ms": int(prediction_ts_ms),
                                "extra_json": {
                                    "event_title": title,
                                    "source": source,
                                    "url": url,
                                    "domain": domain,
                                    "regime": str(reg),
                                    "pipeline_timing": dict(explain.get("pipeline_timing") or {}),
                                    "model_intent": dict(explain.get("model_intent") or {}),
                                },
                            }
                        )

                        alert_conf = float(adj_conf)
                        if ALERT_DOWNWEIGHT_NEG_NET:
                            try:
                                tr = explain.get("tradability") or {}
                                net = float(tr.get("expected_ret_net") or 0.0)
                                if net < 0.0:
                                    alert_conf = alert_conf * float(ALERT_DOWNWEIGHT_MULT)
                            except Exception as e:
                                _warn_nonfatal("PROCESS_EVENTS_ALERT_DOWNWEIGHT_FAILED", e, once_key="alert_downweight_neg_net", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        try:
                            spike_info2 = _detect_exec_cost_spike(conw)
                            if spike_info2 and spike_info2.get("spike"):
                                logging.error("EXEC_COST_SPIKE mid_pass spike_info=%s", spike_info2)
                                continue
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_EXEC_COST_SPIKE_CHECK_FAILED", e, once_key="exec_cost_spike_check_midpass", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        allow_global, _, _ = execution_allowed(symbol=None, regime=None)
                        allow_sym, _, _ = execution_allowed(symbol=sym, regime=None)
                        explain["execution_gate"] = {
                            "global_allowed": bool(allow_global),
                            "symbol_allowed": bool(allow_sym),
                        }
                        pending_alerts.append(
                            {
                                "event_id": int(eid),
                                "event_title": title,
                                "symbol": str(sym),
                                "horizon_s": int(h),
                                "expected_z": float(expected_z),
                                "confidence": float(alert_conf),
                                "explain": explain,
                            }
                        )
                _trace_step(
                    int(eid),
                    "pending_built",
                    watch=len(pending_watch_symbols),
                    predictions=len(pending_predictions),
                    decisions=len(pending_decisions),
                    alerts=len(pending_alerts),
                )

                # Per-event transactional safety: take the write transaction only
                # after the expensive model work is finished.
                conw, cur = _ensure_live_write_connection(conw, cur)
                try:
                    _trace_step(int(eid), "savepoint_begin")
                    conw.execute("SAVEPOINT ev;")
                except Exception as e:
                    _warn_nonfatal("PROCESS_EVENTS_SAVEPOINT_BEGIN_FAILED", e, once_key="savepoint_begin", event_id=int(eid))

                try:
                    event_signal_publications: List[Dict[str, Any]] = []
                    if pending_watch_symbols:
                        _trace_step(int(eid), "watch_upsert_begin", n=len(pending_watch_symbols))
                        upsert_watch_symbols(conw, pending_watch_symbols, int(ts_ms))
                        _trace_step(int(eid), "watch_upsert_done", n=len(pending_watch_symbols))

                    _trace_step(int(eid), "embedding_write_begin")
                    cur.execute(
                        "INSERT OR REPLACE INTO event_embeddings(event_id, dim, vec) VALUES (?,?,?)",
                        (int(eid), int(len(vec)), vec.tobytes()),
                    )
                    _trace_step(int(eid), "embedding_write_done")
                    _trace_step(int(eid), "event_meta_update_begin")
                    _update_event_meta_json(conw, event_id=int(eid), meta=event_meta)
                    _trace_step(int(eid), "event_meta_update_done")

                    _trace_step(int(eid), "predictions_write_begin", n=len(pending_predictions))
                    for item in pending_predictions:
                        prediction_ts_ms = _safe_int(item.get("prediction_ts_ms"), int(time.time() * 1000))
                        store_prediction(
                            event_id=item["event_id"],
                            symbol=item["symbol"],
                            horizon_s=item["horizon_s"],
                            predicted_z=item["predicted_z"],
                            confidence=item["confidence"],
                            confidence_raw=item.get("confidence_raw"),
                            prediction_strength=item.get("prediction_strength"),
                            model_name=item.get("model_name"),
                            model_id=item.get("model_id"),
                            model_version=item.get("model_version"),
                            features_version=item.get("features_version"),
                            tracking_source="process_events",
                            con=conw,
                        )
                        ingestion_to_db_latencies.append(int(ingestion_to_db_latency_ms))
                        db_to_prediction_latencies.append(int(max(0, prediction_ts_ms - db_observed_ts_ms)))
                        predictions_written += 1
                    _trace_step(int(eid), "predictions_write_done", n=len(pending_predictions))

                    _trace_step(int(eid), "decisions_write_begin", n=len(pending_decisions))
                    for item in pending_decisions:
                        decision_ts_ms = int(time.time() * 1000)
                        prediction_ts_ms = _safe_int(item.get("prediction_ts_ms"), decision_ts_ms)
                        explain_json = item.get("explain_json")
                        if not isinstance(explain_json, dict):
                            explain_json = dict(explain_json or {})
                        pipeline_timing = dict(explain_json.get("pipeline_timing") or {})
                        pipeline_timing["decision_ts_ms"] = int(decision_ts_ms)
                        pipeline_timing["prediction_to_decision_latency_ms"] = int(
                            max(0, decision_ts_ms - prediction_ts_ms)
                        )
                        explain_json["pipeline_timing"] = pipeline_timing
                        extra_json = item.get("extra_json")
                        if not isinstance(extra_json, dict):
                            extra_json = dict(extra_json or {})
                        extra_json["pipeline_timing"] = dict(pipeline_timing)
                        log_decision(
                            event_id=item["event_id"],
                            symbol=item["symbol"],
                            horizon_s=item["horizon_s"],
                            predicted_z=item["predicted_z"],
                            confidence=item["confidence"],
                            model_name=item["model_name"],
                            model_kind=item["model_kind"],
                            model_ts_ms=item["model_ts_ms"],
                            features_hash=item["features_hash"],
                            feature_set_tag=item.get("feature_set_tag"),
                            features_json=item["features_json"],
                            explain_json=explain_json,
                            extra_json=extra_json,
                            ts_ms=int(decision_ts_ms),
                            con=conw,
                        )
                        prediction_to_decision_latencies.append(
                            int(max(0, decision_ts_ms - prediction_ts_ms))
                        )
                    _trace_step(int(eid), "decisions_write_done", n=len(pending_decisions))

                    _trace_step(int(eid), "alerts_write_begin", n=len(pending_alerts))
                    for item in pending_alerts:
                        details = _alert_details_dict(emit_alert(
                            event_id=item["event_id"],
                            event_title=item["event_title"],
                            symbol=item["symbol"],
                            horizon_s=item["horizon_s"],
                            expected_z=item["expected_z"],
                            confidence=item["confidence"],
                            explain=item["explain"],
                            con=conw,
                            return_details=True,
                        ))
                        payload = details.get("payload")
                        if details.get("inserted") and isinstance(payload, dict):
                            event_signal_publications.append(dict(payload))
                    _trace_step(int(eid), "alerts_write_done", n=len(pending_alerts))

                    _trace_step(int(eid), "checkpoint_begin")
                    put_job_checkpoint(JOB_NAME, int(eid), int(ts_ms), con=conw)
                    _trace_step(int(eid), "checkpoint_done")
                except Exception:
                    try:
                        conw.execute("ROLLBACK TO SAVEPOINT ev;")
                    except Exception as rollback_err:
                        _warn_nonfatal("PROCESS_EVENTS_SAVEPOINT_ROLLBACK_FAILED", rollback_err, once_key="savepoint_rollback", event_id=int(eid))
                    raise

                try:
                    conw.execute("RELEASE SAVEPOINT ev;")
                    _trace_step(int(eid), "savepoint_release_done")
                    if event_signal_publications:
                        pending_signal_publications.extend(event_signal_publications)
                except Exception:
                    try:
                        conw.execute("ROLLBACK TO SAVEPOINT ev;")
                        conw.execute("RELEASE SAVEPOINT ev;")
                    except Exception as rollback_err:
                        _warn_nonfatal("PROCESS_EVENTS_SAVEPOINT_RELEASE_FAILED", rollback_err, once_key="savepoint_release", event_id=int(eid))

                _since_commit += 1
                if _since_commit >= COMMIT_EVERY_EVENTS:
                    try:
                        _trace_step(int(eid), "commit_begin")
                        conw.commit()
                        _trace_step(int(eid), "commit_done")
                        for payload in pending_signal_publications:
                            try:
                                publish_event("strategy_signal", payload)
                            except Exception as e:
                                _warn_nonfatal("PROCESS_EVENTS_SIGNAL_PUBLISH_FAILED", e, once_key="signal_publish", event_id=int(eid))
                        pending_signal_publications.clear()
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_COMMIT_FAILED", e, once_key="commit_batch", event_id=int(eid))
                    _since_commit = 0

            conw.commit()
            _emit_latency_snapshot(
                {
                    "ingestion_to_db": ingestion_to_db_latencies,
                    "db_to_prediction": db_to_prediction_latencies,
                    "prediction_to_decision": prediction_to_decision_latencies,
                },
                event_count=len(rows),
                prediction_count=predictions_written,
            )
            for payload in pending_signal_publications:
                try:
                    publish_event("strategy_signal", payload)
                except Exception as e:
                    _warn_nonfatal("PROCESS_EVENTS_SIGNAL_PUBLISH_FAILED", e, once_key="signal_publish_final")
            pending_signal_publications.clear()

        finally:
            try:
                conw.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_CONNECTION_CLOSE_FAILED", e, once_key="write_connection_close", scope="main_write_connection")

        dur_ms = int(time.time() * 1000) - started_ms
        logging.info("PROCESS COMPLETE dur_ms=%s", dur_ms)

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_JOB_LOCK_RELEASE_FAILED", e, once_key="job_lock_release", job_name=str(JOB_NAME), pid=int(PID))


if __name__ == "__main__":
    main()
