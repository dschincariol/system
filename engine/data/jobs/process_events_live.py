# FILE: process_events_live.py
"""
LIVE inference worker (minimal + production-safe)

Responsibilities:
- Ensure DB schema exists
- Read unembedded events
- Embed titles (GPU if available)
- Predict expected impact
- Store predictions
- Emit alerts
- Job lock + heartbeats
- CUDA stream separation (live vs shadow stream reserved but unused here)
- Async pinned H→D pipeline (best-effort, interface-safe)
- GPU utilization feedback loop (best-effort; adjusts embed batch + shadow throttles)

IMPORTANT: This file is intentionally minimal.
All “lost” enrichment/risk/explainability helpers from the 1228-line original
are preserved in:
  - process_events_enriched.py
  - process_events_shadow.py

Function inventory note:
- This file keeps only the core loop + minimal helpers.
- See process_events_enriched.py for novelty/options/earnings/sec/exec-cost/relevance/discovery.
"""

import os
import time
import json
import random
import logging
from typing import Dict, Any, List, Optional, cast

import numpy as np
import torch
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
        component="engine.data.jobs.process_events_live",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


def _is_expected_nvml_unavailable(error: Exception) -> bool:
    text = str(error or "").strip().lower()
    err_type = type(error).__name__.lower()
    return (
        "nvml shared library not found" in text
        or "nvmllibrarynotfound" in err_type
        or ("could not find module" in text and "nvml" in text)
    )

# -----------------------------------------------------------------------------
# ENV defaults (safe)
# -----------------------------------------------------------------------------
apply_cpu_first_runtime_defaults()
_TORCH_DEVICE_RESOLUTION = resolve_torch_device(torch, env_var="TORCH_DEVICE")
_CUDA_RUNTIME_ENABLED = torch_device_is_cuda(torch, _TORCH_DEVICE_RESOLUTION)
_NVIDIA_TELEMETRY_ENABLED = nvidia_telemetry_enabled(torch)

_thread_config = configure_torch_thread_pools(torch)
if _thread_config.get("reason") == "failed":
    _warn_nonfatal(
        "PROCESS_EVENTS_LIVE_TORCH_THREAD_CONFIG_FAILED",
        _thread_config["error"],
        cpu_threads=int(_thread_config.get("cpu_threads") or 0),
        interop_threads=int(_thread_config.get("interop_threads") or 0),
    )
log_runtime_hardware_diagnostics(LOGGER, torch_module=torch, component="engine.data.jobs.process_events_live")

# -----------------------------------------------------------------------------
# CUDA streams (live vs shadow reserved)
# -----------------------------------------------------------------------------
_LIVE_STREAM = None
_SHADOW_STREAM = None

if _CUDA_RUNTIME_ENABLED:
    try:
        _LIVE_STREAM = torch.cuda.default_stream()
        # lower priority stream for optional background work
        _SHADOW_STREAM = torch.cuda.Stream(priority=1)
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_LIVE_CUDA_STREAM_INIT_FAILED", e)
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
from engine.runtime.metrics import emit_snapshot

from engine.strategy.predictor import predict_runtime_event
from engine.runtime.alerts import emit_alert, init_alerts_db
from engine.runtime.event_bus import publish_event
from engine.strategy.validation import store_prediction, init_validation_db
from engine.strategy.decision_log import log_decision, hash_feature_vector
from engine.strategy.confidence_adjust import get_adjusted_confidence
from engine.strategy.confidence_engine import apply_confidence_payload, describe_signal_confidence
from engine.strategy.model_intent import build_model_intent
from engine.strategy.microstructure_signals import (
    load_latest_microstructure_context,
    apply_microstructure_confidence,
)
from engine.data.universe import get_active_symbols
from engine.strategy.model_v2 import get_current_regime
from engine.strategy.model_config import configured_model_horizons, experimental_models_enabled
from engine.strategy.news_domain import extract_domain, is_domain_blocked
from engine.execution.kill_switch import execution_allowed
from engine.strategy.rules_engine import evaluate_rules

# -----------------------------------------------------------------------------
# Runtime config
# -----------------------------------------------------------------------------
# process_events_live is the stripped-down production-safe path: it handles the
# core embed/predict/store loop without the heavier enrichment helpers.
DEFAULT_SYMBOLS = load_default_symbols(extra=["OIL"])
HORIZONS = configured_model_horizons(default=[300, 3600])
ENABLE_EXPERIMENTAL_MODELS = experimental_models_enabled()

JOB_NAME = "process_events_live"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [process_events_live] %(message)s",
)

# GPU feedback loop knobs
GPU_FEEDBACK_EVERY_S = float(os.environ.get("GPU_FEEDBACK_EVERY_S", "10.0"))
GPU_UTIL_HIGH = float(os.environ.get("GPU_UTIL_HIGH", "92.0"))
GPU_UTIL_LOW = float(os.environ.get("GPU_UTIL_LOW", "35.0"))
EMBED_BATCH_MIN = int(os.environ.get("EMBED_BATCH_MIN", "16"))
EMBED_BATCH_MAX = int(os.environ.get("EMBED_BATCH_MAX", "256"))
EMBED_BATCH_DEFAULT = int(os.environ.get("EMBED_BATCH_SIZE", "64"))

# -----------------------------------------------------------------------------
# Lazy model
# -----------------------------------------------------------------------------
_model: Any = None


def _get_model() -> Any:
    global _model
    if _model is None:
        resolution = resolve_torch_device(torch, env_var="EMBED_DEVICE", fallback_envs=("TORCH_DEVICE",))
        dev = resolution.resolved

        # Safe perf flags
        try:
            torch.set_float32_matmul_precision(os.environ.get("TORCH_MATMUL_PRECISION", "high"))
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_LIVE_TORCH_MATMUL_PRECISION_FAILED", e, once_key="torch_matmul_precision")
        if _CUDA_RUNTIME_ENABLED:
            try:
                torch.backends.cuda.matmul.allow_tf32 = os.environ.get("TORCH_ALLOW_TF32", "1") == "1"
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_LIVE_TORCH_CUDA_TF32_FAILED", e, once_key="torch_cuda_tf32")
            try:
                torch.backends.cudnn.allow_tf32 = os.environ.get("CUDNN_ALLOW_TF32", "1") == "1"
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_LIVE_TORCH_CUDNN_TF32_FAILED", e, once_key="torch_cudnn_tf32")
            try:
                torch.backends.cudnn.benchmark = os.environ.get("CUDNN_BENCHMARK", "1") == "1"
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_LIVE_TORCH_CUDNN_BENCHMARK_FAILED", e, once_key="torch_cudnn_benchmark")

        for _k in ("HF_HOME", "TRANSFORMERS_CACHE", "SENTENCE_TRANSFORMERS_HOME"):
            if _k in os.environ:
                try:
                    Path(os.environ[_k]).mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    _warn_nonfatal("PROCESS_EVENTS_LIVE_MODEL_CACHE_DIR_MKDIR_FAILED", e, once_key=f"model_cache_dir:{_k}", env_key=str(_k), path=str(os.environ.get(_k) or ""))

        # Keep model init lazy so import-time startup stays cheap and failures
        # only occur if this job actually runs.
        _model = _SENTENCE_TRANSFORMER_CLS("all-MiniLM-L6-v2", device=dev)

    return _model


def _features_hash(vec: np.ndarray) -> str:
    return str(hash_feature_vector(vec.tolist()) or "")


def _alert_details_dict(details: Any) -> Dict[str, Any]:
    return dict(details) if isinstance(details, dict) else {}


def _sleep_with_jitter(seconds: float) -> None:
    if seconds <= 0:
        return
    j = seconds * 0.2
    time.sleep(max(0.05, seconds + random.uniform(-j, j)))


def _safe_json_obj(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        loaded = json.loads(str(raw))
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_LIVE_ENV_JSON_PARSE_FAILED", e, once_key="env_json_parse", raw=str(raw)[:200])
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_LIVE_SAFE_INT_FAILED", e, once_key="safe_int", value=repr(value)[:120])
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
            "component": "engine.data.jobs.process_events_live",
            "job": JOB_NAME,
        },
    )


# -----------------------------------------------------------------------------
# GPU utilization feedback (best-effort)
# -----------------------------------------------------------------------------
class _GpuUtilProbe:
    def __init__(self) -> None:
        self._mode = "none"
        self._nvml = None
        self._handle = None
        if not _NVIDIA_TELEMETRY_ENABLED:
            self._mode = "disabled"
            return
        try:
            import pynvml  # type: ignore

            pynvml.nvmlInit()
            self._nvml = pynvml
            idx = int(os.environ.get("GPU_INDEX", "0"))
            self._handle = pynvml.nvmlDeviceGetHandleByIndex(idx)
            self._mode = "nvml"
        except Exception as e:
            if not _is_expected_nvml_unavailable(e):
                _warn_nonfatal("PROCESS_EVENTS_LIVE_GPU_PROBE_INIT_FAILED", e, once_key="gpu_probe_init")
            self._mode = "fallback"

    def utilization(self) -> Optional[float]:
        if not _NVIDIA_TELEMETRY_ENABLED:
            return None
        if self._mode == "nvml" and self._nvml and self._handle:
            try:
                u = self._nvml.nvmlDeviceGetUtilizationRates(self._handle)
                return float(u.gpu)
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_LIVE_GPU_UTIL_READ_FAILED", e, once_key="gpu_util_read")
                return None
        # Fallback: no true utilization available; return None
        return None


class _GpuFeedbackController:
    def __init__(self) -> None:
        self.probe = _GpuUtilProbe()
        self.embed_batch = int(EMBED_BATCH_DEFAULT)
        self._last_ts = 0.0

    def maybe_update(self) -> None:
        now = time.time()
        if (now - self._last_ts) < GPU_FEEDBACK_EVERY_S:
            return
        self._last_ts = now

        util = self.probe.utilization()
        if util is None:
            return

        # Simple proportional “keep util between LOW and HIGH”
        if util > GPU_UTIL_HIGH:
            self.embed_batch = max(EMBED_BATCH_MIN, int(self.embed_batch * 0.8))
        elif util < GPU_UTIL_LOW:
            self.embed_batch = min(EMBED_BATCH_MAX, int(self.embed_batch * 1.2))

        # Clamp
        self.embed_batch = int(max(EMBED_BATCH_MIN, min(EMBED_BATCH_MAX, self.embed_batch)))


_GPU_CTRL = _GpuFeedbackController()

# -----------------------------------------------------------------------------
# Async pinned H→D pipeline (best-effort, interface-safe)
# -----------------------------------------------------------------------------
def _maybe_pin_embeddings(emb: np.ndarray) -> np.ndarray:
    """
    SentenceTransformer returns numpy. Some downstream code may convert to torch.
    Pinned memory is only meaningful for torch tensors; we keep interface-safe.

    If you later add a torch-based predictor API, you can use:
      torch.from_numpy(emb).pin_memory().to('cuda', non_blocking=True)
    """
    # Numpy arrays can’t be “pinned”; return as-is.
    try:
        emb.setflags(write=False)
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_LIVE_EMBEDDING_FLAGS_FAILED", e, once_key="embedding_flags")
    return emb


# -----------------------------------------------------------------------------
# Ensure schemas exist (must be import-time safe)
# -----------------------------------------------------------------------------
init_db()
init_alerts_db()
init_validation_db()


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
            _warn_nonfatal("PROCESS_EVENTS_LIVE_CONNECTION_CLOSE_FAILED", e, once_key="require_fresh_prices_close", scope="require_fresh_prices")

    fresh_n = int(row[0] or 0)
    fresh_symbols = int(row[1] or 0)
    last_ts_ms = int(row[2] or 0)
    active_symbols = int(active_row[0] or 0)

    required_symbols = int(min_rows)
    if active_symbols > 0:
        required_symbols = max(required_symbols, int(max(1, active_symbols * min_symbol_coverage)))

    try:
        from engine.runtime.ipc import market_data_status

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
        _warn_nonfatal("PROCESS_EVENTS_LIVE_MARKET_DATA_STATUS_FAILED", e, once_key="market_data_status")

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
    started_ms = int(time.time() * 1000)

    try:
        # rules engine (best-effort)
        try:
            evaluate_rules()
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_LIVE_RULES_EVALUATION_FAILED", e, once_key="evaluate_rules")

        allow0, _, _ = execution_allowed(symbol=None, regime=None)
        if not allow0:
            _warn_nonfatal(
                "PROCESS_EVENTS_LIVE_KILL_SWITCH_BLOCKED",
                RuntimeError("execution blocked by kill switch"),
                once_key="kill_switch_blocked",
            )
            return

        _require_fresh_prices_or_exit(JOB_NAME)

        # Load universe
        conu = connect_ro()
        try:
            try:
                symbols = get_active_symbols(conu, limit=int(os.environ.get("PROCESS_SYMBOL_LIMIT", "2000")))
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_LIVE_SYMBOL_LOAD_FAILED", e, once_key="load_active_symbols")
                symbols = []
            if not symbols:
                symbols = list(DEFAULT_SYMBOLS)
            symbols = list(dict.fromkeys(symbols))
        finally:
            try:
                conu.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_LIVE_CONNECTION_CLOSE_FAILED", e, once_key="symbols_connection_close", scope="load_symbols")

        # Read candidate events (unembedded)
        con = connect_ro()
        try:
            rows = con.execute(
                """
                SELECT e.id, e.ts_ms, e.source, e.title, e.body, e.url, e.meta_json
                FROM events e
                LEFT JOIN event_embeddings emb ON emb.event_id = e.id
                WHERE emb.event_id IS NULL
                ORDER BY e.ts_ms DESC
                LIMIT 50
                """
            ).fetchall()
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_LIVE_CONNECTION_CLOSE_FAILED", e, once_key="event_scan_connection_close", scope="scan_events")

        if not rows:
            logging.info("no new events")
            return

        # GPU feedback loop adjusts embed batch
        _GPU_CTRL.maybe_update()
        embed_bs = int(_GPU_CTRL.embed_batch)

        titles = [(r[3] or "") for r in rows]

        # Embed on live stream
        if _LIVE_STREAM is not None and _CUDA_RUNTIME_ENABLED:
            with torch.cuda.stream(cast(Any, _LIVE_STREAM)):
                embeddings = _get_model().encode(
                    titles,
                    batch_size=int(embed_bs),
                    show_progress_bar=False,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                )
                torch.cuda.synchronize(cast(Any, _LIVE_STREAM))
            embeddings = embeddings.astype(np.float32, copy=False)
        else:
            embeddings = _get_model().encode(
                titles,
                batch_size=int(embed_bs),
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,
            ).astype(np.float32, copy=False)

        embeddings = _maybe_pin_embeddings(embeddings)

        # Write connection
        conw = connect(readonly=False)
        try:
            cur = conw.cursor()
            pending_signal_publications: List[Dict[str, Any]] = []
            ingestion_to_db_latencies: List[int] = []
            db_to_prediction_latencies: List[int] = []
            prediction_to_decision_latencies: List[int] = []
            predictions_written = 0

            for (eid, ts_ms, source, title, body, url, meta_json), vec in zip(rows, embeddings):
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
                        _warn_nonfatal("PROCESS_EVENTS_LIVE_HEARTBEAT_UPDATE_FAILED", e, once_key="heartbeat_update", job_name=str(JOB_NAME), event_id=int(eid), event_ts_ms=int(ts_ms or 0))
                    last_hb_s = now_s

                if not execution_allowed():
                    continue

                title = title or ""
                body = body or ""
                source = source or ""
                url = url or ""
                ts_ms = int(ts_ms or 0)
                event_meta = _safe_json_obj(meta_json)
                pipeline_timing = dict(event_meta.get("pipeline_timing") or {})
                db_observed_ts_ms = _safe_int(
                    pipeline_timing.get("db_observed_ts_ms")
                    or pipeline_timing.get("db_write_ts_ms")
                    or ts_ms
                    or int(time.time() * 1000),
                    ts_ms or int(time.time() * 1000),
                )
                ingestion_to_db_latency_ms = _safe_int(
                    pipeline_timing.get("ingestion_to_db_latency_ms"),
                    max(0, db_observed_ts_ms - ts_ms),
                )
                ingestion_to_db_latency_ms = max(0, int(ingestion_to_db_latency_ms))

                # Domain gate
                domain = extract_domain(url, meta_json)
                if domain and is_domain_blocked(domain, "*"):
                    continue

                event_ctx = {
                    "event_id": int(eid),
                    "ts_ms": int(ts_ms),
                    "source": source,
                    "title": title,
                    "body": body,
                    "url": url,
                    "meta": {},  # enriched version fills this
                }

                # Champion/live predictions come from the active competition winner.
                preds = predict_runtime_event(
                    vec,
                    symbols,
                    HORIZONS,
                    top_k=8,
                    event=event_ctx,
                )

                # Persist embedding
                cur.execute(
                    "INSERT OR REPLACE INTO event_embeddings(event_id, dim, vec) VALUES (?,?,?)",
                    (int(eid), int(len(vec)), vec.tobytes()),
                )

                # Persist preds + alerts
                for sym in symbols:
                    reg = get_current_regime(sym)

                    allow_sym, _, _ = execution_allowed(symbol=sym, regime=None)
                    if not allow_sym:
                        continue

                    for h in HORIZONS:
                        expected_z, conf, explain = preds[(sym, int(h))]
                        prediction_ts_ms = int(time.time() * 1000)
                        adj_conf = float(conf)
                        adj_explain = {}
                        explain = dict(explain or {})

                        try:
                            adj_conf, adj_explain = get_adjusted_confidence(
                                conw, symbol=sym, horizon_s=int(h), base_conf=float(adj_conf)
                            )
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_LIVE_CONFIDENCE_ADJUST_FAILED", e, once_key="confidence_adjust", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

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
                            _warn_nonfatal("PROCESS_EVENTS_LIVE_MICROSTRUCTURE_ADJUST_FAILED", e, once_key="microstructure_adjust", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        signal_payload = describe_signal_confidence(
                            expected_z=float(expected_z),
                            confidence=float(adj_conf),
                            raw_confidence=((explain.get("confidence_engine") or {}).get("raw_confidence") if isinstance(explain.get("confidence_engine"), dict) else conf),
                            horizon_s=int(h),
                            calibration=((explain.get("confidence_engine") or {}).get("calibration") if isinstance(explain.get("confidence_engine"), dict) else None),
                            signal_ts_ms=int(ts_ms),
                        )
                        adj_conf = float(signal_payload["confidence"])
                        explain = apply_confidence_payload(explain, signal_payload)
                        explain["adjusted_confidence"] = float(adj_conf)
                        explain["regime"] = str(reg)
                        explain["model_name"] = str(explain.get("model_name") or "")
                        explain["model_id"] = str(explain.get("model_id") or explain.get("model_name") or "")
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

                        if ENABLE_EXPERIMENTAL_MODELS:
                            explain["experimental_models_enabled"] = True

                        store_prediction(
                            event_id=eid,
                            symbol=sym,
                            horizon_s=int(h),
                            predicted_z=float(expected_z),
                            confidence=float(adj_conf),
                            confidence_raw=float(signal_payload["raw_confidence"]),
                            prediction_strength=float(signal_payload["prediction_strength"]),
                            model_name=str(explain.get("model_name") or ""),
                            model_id=str(explain.get("model_id") or ""),
                            model_version=str((explain or {}).get("model_version") or ""),
                            features_version=str((explain or {}).get("feature_set_tag") or "unknown"),
                            tracking_source="process_events_live",
                            con=conw,
                        )
                        decision_ts_ms = int(time.time() * 1000)
                        explain["pipeline_timing"]["decision_ts_ms"] = int(decision_ts_ms)
                        explain["pipeline_timing"]["prediction_to_decision_latency_ms"] = int(
                            max(0, decision_ts_ms - prediction_ts_ms)
                        )
                        ingestion_to_db_latencies.append(int(ingestion_to_db_latency_ms))
                        db_to_prediction_latencies.append(int(max(0, prediction_ts_ms - db_observed_ts_ms)))
                        prediction_to_decision_latencies.append(
                            int(max(0, decision_ts_ms - prediction_ts_ms))
                        )
                        predictions_written += 1

                        log_decision(
                            event_id=eid,
                            symbol=sym,
                            horizon_s=int(h),
                            predicted_z=float(expected_z),
                            confidence=float(adj_conf),
                            model_name=str(explain.get("model_name") or explain.get("model") or "knn"),
                            model_kind=explain.get("model_kind"),
                            model_ts_ms=explain.get("model_ts_ms"),
                            features_hash=_features_hash(vec),
                            feature_set_tag=str(explain.get("feature_set_tag") or ""),
                            features_json=dict(explain.get("feature_snapshot") or {}),
                            explain_json=explain,
                            extra_json={
                                "event_title": title,
                                "source": source,
                                "url": url,
                                "domain": domain,
                                "regime": str(reg),
                                "pipeline_timing": dict(explain.get("pipeline_timing") or {}),
                                "model_intent": dict(explain.get("model_intent") or {}),
                                "competition_role": "champion",
                            },
                            ts_ms=int(decision_ts_ms),
                            con=conw,
                        )

                        try:
                            details = _alert_details_dict(emit_alert(
                                event_id=int(eid),
                                event_title=title,
                                symbol=sym,
                                horizon_s=int(h),
                                expected_z=float(expected_z),
                                confidence=float(adj_conf),
                                explain=explain,
                                con=conw,
                                return_details=True,
                            ))
                            payload = details.get("payload")
                            if details.get("inserted") and isinstance(payload, dict):
                                pending_signal_publications.append(dict(payload))
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_LIVE_ALERT_EMIT_FAILED", e, once_key="alert_emit", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

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
                    _warn_nonfatal("PROCESS_EVENTS_LIVE_SIGNAL_PUBLISH_FAILED", e, once_key="signal_publish")

        finally:
            try:
                conw.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_LIVE_CONNECTION_CLOSE_FAILED", e, once_key="write_connection_close", scope="main_write_connection")

        dur_ms = int(time.time() * 1000) - started_ms
        logging.info("LIVE COMPLETE dur_ms=%s embed_bs=%s", dur_ms, embed_bs)

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_LIVE_JOB_LOCK_RELEASE_FAILED", e, once_key="job_lock_release", job_name=str(JOB_NAME), pid=int(PID))


if __name__ == "__main__":
    main()
