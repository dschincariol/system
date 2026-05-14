# FILE: process_events_enriched.py
"""
ENRICHED worker (risk + explainability)

This file preserves the “missing” 1228-line original subsystems:
- novelty cache + novelty scoring
- tradability proxy
- heuristic relevance scoring (regex rules)
- symbol discovery + WATCH universe growth
- exec-cost context + spread-based confidence decay
- exec-cost spike kill-switch (global halt)
- options context + options anomaly (IV/OI)
- earnings calendar context + downweight
- SEC filings context + downweight
- domain confidence multiplier (if you enable it here)
- clustering hook (optional)
- temporal shadow hook (optional, but shadow-heavy work belongs in shadow worker)

It runs slower than LIVE and is intended for:
- investor UI explainability richness
- risk gating
- research-grade annotations
"""

import re
import time
import os
import json
import random
import logging
from typing import Dict, Any, List, Tuple, cast

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
from engine.runtime.torch_threads import configure_torch_thread_pools
from engine.strategy.model_config import configured_model_horizons, experimental_models_enabled

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
        component="engine.data.jobs.process_events_enriched",
        extra=extra or None,
        include_health=False,
        persist=False,
    )

# -----------------------------------------------------------------------------
# ENV defaults
# -----------------------------------------------------------------------------
os.environ.setdefault("CUDA_VISIBLE_DEVICES", "0")
os.environ.setdefault("TORCH_DEVICE", "cuda")

os.environ.setdefault("OMP_NUM_THREADS", "8")
os.environ.setdefault("MKL_NUM_THREADS", "8")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "8")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "8")

_thread_config = configure_torch_thread_pools(torch)
if _thread_config.get("reason") == "failed":
    _warn_nonfatal(
        "PROCESS_EVENTS_ENRICHED_TORCH_THREAD_CONFIG_FAILED",
        _thread_config["error"],
        once_key="torch_thread_config",
        cpu_threads=int(_thread_config.get("cpu_threads") or 0),
        interop_threads=int(_thread_config.get("interop_threads") or 0),
    )

# -----------------------------------------------------------------------------
# CUDA streams
# -----------------------------------------------------------------------------
_LIVE_STREAM = None
_SHADOW_STREAM = None

if torch.cuda.is_available():
    try:
        _LIVE_STREAM = torch.cuda.default_stream()
        _SHADOW_STREAM = torch.cuda.Stream(priority=1)
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_CUDA_STREAM_INIT_FAILED", e, once_key="cuda_stream_init")
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
    put_finbert_sentiment_enrichment,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
    put_job_heartbeat,
)

from engine.data.finbert_sentiment import FINBERT_MODEL_NAME, USE_FINBERT_SENTIMENT, score_event_rows
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
from engine.strategy.news_domain import extract_domain, is_domain_blocked, domain_conf_multiplier
from engine.execution.kill_switch import execution_allowed
from engine.strategy.rules_engine import evaluate_rules

# Optional subsystems
try:
    from engine.strategy.temporal_predictor import predict_temporal_shadow_for_event
except Exception:
    predict_temporal_shadow_for_event = None

try:
    from engine.strategy.clustering import assign_cluster
except Exception:
    assign_cluster = None

# -----------------------------------------------------------------------------
# Runtime config (preserved)
# -----------------------------------------------------------------------------
DEFAULT_SYMBOLS = load_default_symbols(extra=["OIL"])
HORIZONS = configured_model_horizons(default=[300, 3600])
ENABLE_EXPERIMENTAL_MODELS = experimental_models_enabled()

JOB_NAME = "process_events_enriched"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

# Novelty scoring
_RECENT_EMB_CACHE: List[np.ndarray] = []
_RECENT_EMB_CACHE_MAX = int(os.environ.get("NOVELTY_CACHE_MAX", "500"))
NOVELTY_LOOKBACK = int(os.environ.get("NOVELTY_LOOKBACK", "200"))
NOVELTY_MIN_EVENTS = int(os.environ.get("NOVELTY_MIN_EVENTS", "8"))
NOVELTY_MIN_SCORE = float(os.environ.get("NOVELTY_MIN_SCORE", "0.20"))

# Tradability proxy parameters
RET_SCALE_PER_Z = float(os.environ.get("RET_SCALE_PER_Z", "0.0025"))
COST_BPS = float(os.environ.get("COST_BPS", "6.0"))

# Exec-cost spike kill-switch
EXEC_COST_SPIKE_BPS = float(os.environ.get("EXEC_COST_SPIKE_BPS", "45.0"))
EXEC_COST_SPIKE_WINDOW_S = int(os.environ.get("EXEC_COST_SPIKE_WINDOW_S", "120"))
EXEC_COST_SPIKE_MIN_N = int(os.environ.get("EXEC_COST_SPIKE_MIN_N", "8"))
EXEC_COST_SPIKE_SYMBOL_LIMIT = int(os.environ.get("EXEC_COST_SPIKE_SYMBOL_LIMIT", "50"))

# Legacy alert behavior: downweight if expected_ret_net < 0
ALERT_DOWNWEIGHT_NEG_NET = os.environ.get("ALERT_DOWNWEIGHT_NEG_NET", "1") == "1"
ALERT_DOWNWEIGHT_MULT = float(os.environ.get("ALERT_DOWNWEIGHT_MULT", "0.75"))

# Symbol discovery (WATCH-only)
DISCOVER_SYMBOLS = os.environ.get("DISCOVER_SYMBOLS", "1") == "1"

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [process_events_enriched] %(message)s",
)
# -----------------------------------------------------------------------------
# Lazy embedding model
# -----------------------------------------------------------------------------
_model: Any = None


def _get_model() -> Any:
    global _model
    if _model is None:
        # Delay model allocation until the enriched worker actually needs it.
        # That keeps health checks/import smoke lightweight and avoids paying
        # GPU initialization cost in processes that only inspect the module.
        dev = os.environ.get("EMBED_DEVICE", "").strip().lower()
        if not dev:
            dev = "cuda" if torch.cuda.is_available() else "cpu"

        try:
            torch.set_float32_matmul_precision(os.environ.get("TORCH_MATMUL_PRECISION", "high"))
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_TORCH_MATMUL_PRECISION_FAILED", e, once_key="torch_matmul_precision")
        try:
            torch.backends.cuda.matmul.allow_tf32 = os.environ.get("TORCH_ALLOW_TF32", "1") == "1"
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_TORCH_CUDA_TF32_FAILED", e, once_key="torch_cuda_tf32")
        try:
            torch.backends.cudnn.allow_tf32 = os.environ.get("CUDNN_ALLOW_TF32", "1") == "1"
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_TORCH_CUDNN_TF32_FAILED", e, once_key="torch_cudnn_tf32")
        try:
            torch.backends.cudnn.benchmark = os.environ.get("CUDNN_BENCHMARK", "1") == "1"
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_TORCH_CUDNN_BENCHMARK_FAILED", e, once_key="torch_cudnn_benchmark")

        for _k in ("HF_HOME", "TRANSFORMERS_CACHE", "SENTENCE_TRANSFORMERS_HOME"):
            if _k in os.environ:
                try:
                    Path(os.environ[_k]).mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    _warn_nonfatal("PROCESS_EVENTS_ENRICHED_MODEL_CACHE_DIR_MKDIR_FAILED", e, once_key=f"model_cache_dir:{_k}", env_key=str(_k), path=str(os.environ.get(_k) or ""))

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


# -----------------------------------------------------------------------------
# Novelty (preserved)
# -----------------------------------------------------------------------------
def _cosine_max_sim(vec: np.ndarray, mat: np.ndarray) -> float:
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
    # Fast path: in-memory cache
    try:
        if len(_RECENT_EMB_CACHE) >= max(1, int(NOVELTY_MIN_EVENTS)):
            mat = np.vstack(_RECENT_EMB_CACHE[-lookback:]).astype(np.float32, copy=False)
            max_sim = _cosine_max_sim(vec, mat)
            novelty = 1.0 - max_sim
            if novelty == novelty:
                return float(max(0.0, min(1.0, novelty)))
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_NOVELTY_CACHE_FAILED", e, once_key="novelty_cache_fast_path", event_id=int(event_id))

    # Fallback: DB lookup
    # This preserves novelty continuity across worker restarts; otherwise a fresh
    # process would temporarily overstate novelty until the RAM cache refills.
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
            "PROCESS_EVENTS_ENRICHED_NOVELTY_ROWS_FAILED",
            e,
            once_key="novelty_rows_failed",
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
            "PROCESS_EVENTS_ENRICHED_NOVELTY_COMPUTE_FAILED",
            e,
            once_key="novelty_compute_failed",
            event_id=int(event_id),
        )
        novelty = 0.0
        return novelty


def _update_event_meta_json(con, event_id: int, meta: Dict[str, Any]) -> None:
    try:
        con.execute(
            "UPDATE events SET meta_json=? WHERE id=?",
            (json.dumps(meta or {}, separators=(",", ":"), sort_keys=True), int(event_id)),
        )
    except Exception as e:
        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_META_UPDATE_FAILED", e, once_key="event_meta_update", event_id=int(event_id))


# -----------------------------------------------------------------------------
# Exec-cost + spike kill-switch (preserved)
# -----------------------------------------------------------------------------
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
        if last and spread and float(last) > 1e-9:
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
            "PROCESS_EVENTS_ENRICHED_EXEC_COST_CONTEXT_FAILED",
            e,
            once_key=f"exec_cost_context:{symbol}",
            symbol=str(symbol),
        )
        context: Dict[str, Any] = {}
        return context


def _detect_exec_cost_spike(con) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    cutoff = int(now_ms - int(EXEC_COST_SPIKE_WINDOW_S) * 1000)

    syms: List[str] = []
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

    spreads: List[float] = []
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
                lastf = float(last) if last is not None else None
                if lastf is None or lastf <= 1e-9:
                    continue
                if spr is None and bid is not None and ask is not None:
                    spr = float(ask) - float(bid)
                if spr is None:
                    continue
                sbps = 10000.0 * float(spr) / float(lastf)
                if sbps == sbps and sbps >= 0.0:
                    spreads.append(float(sbps))
            except Exception as e:
                _warn_nonfatal(
                    "PROCESS_EVENTS_ENRICHED_EXEC_COST_SPREAD_ROW_FAILED",
                    e,
                    once_key="exec_cost_spread_row_failed",
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


# -----------------------------------------------------------------------------
# Options / earnings / SEC contexts (preserved)
# -----------------------------------------------------------------------------
def _options_context(con, symbol: str) -> Dict[str, Any]:
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
            "PROCESS_EVENTS_ENRICHED_OPTIONS_CONTEXT_FAILED",
            e,
            once_key=f"options_context:{symbol}",
            symbol=str(symbol),
        )
        context: Dict[str, Any] = {}
        return context


def _options_anomaly(con, symbol: str) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    h1 = now_ms - 3600_000
    h24 = now_ms - 24 * 3600_000

    try:
        r1 = con.execute(
            "SELECT AVG(iv), AVG(open_interest) FROM options_chain WHERE symbol=? AND ts_ms >= ?",
            (str(symbol), int(h1)),
        ).fetchone()
        r24 = con.execute(
            "SELECT AVG(iv), AVG(open_interest) FROM options_chain WHERE symbol=? AND ts_ms >= ?",
            (str(symbol), int(h24)),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_ENRICHED_OPTIONS_ANOMALY_QUERY_FAILED",
            e,
            once_key=f"options_anomaly_query:{symbol}",
            symbol=str(symbol),
        )
        anomaly: Dict[str, Any] = {}
        return anomaly

    try:
        iv1 = float(r1[0]) if r1 and r1[0] is not None else None
        iv24 = float(r24[0]) if r24 and r24[0] is not None else None
        oi1 = float(r1[1]) if r1 and r1[1] is not None else None
        oi24 = float(r24[1]) if r24 and r24[1] is not None else None
    except Exception as e:
        _warn_nonfatal(
            "PROCESS_EVENTS_ENRICHED_OPTIONS_ANOMALY_PARSE_FAILED",
            e,
            once_key=f"options_anomaly_parse:{symbol}",
            symbol=str(symbol),
        )
        anomaly = {}
        return anomaly

    out: Dict[str, Any] = {}
    if iv1 is not None and iv24 is not None and iv24 > 1e-12:
        out["iv_ratio_1h_24h"] = float(iv1 / iv24)
    if oi1 is not None and oi24 is not None:
        out["oi_delta_1h_24h"] = float(oi1 - oi24)
    return out


def _earnings_context(con, symbol: str) -> Dict[str, Any]:
    try:
        look_days = int(os.environ.get("EARNINGS_EVENT_LOOKAHEAD_DAYS", "10"))
    except Exception:
        look_days = 10

    try:
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
            "PROCESS_EVENTS_ENRICHED_EARNINGS_CONTEXT_FAILED",
            e,
            once_key=f"earnings_context:{symbol}",
            symbol=str(symbol),
        )
        context: Dict[str, Any] = {}
        return context


def _sec_filing_context(con, symbol: str) -> Dict[str, Any]:
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
            "PROCESS_EVENTS_ENRICHED_SEC_FILING_CONTEXT_FAILED",
            e,
            once_key=f"sec_filing_context:{symbol}",
            symbol=str(symbol),
        )
        context: Dict[str, Any] = {}
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
            "PROCESS_EVENTS_ENRICHED_INSIDER_CONTEXT_FAILED",
            e,
            once_key=f"insider_context:{symbol}",
            symbol=str(symbol),
        )
        context: Dict[str, Any] = {}
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
            "PROCESS_EVENTS_ENRICHED_CONGRESSIONAL_CONTEXT_FAILED",
            e,
            once_key=f"congressional_context:{symbol}",
            symbol=str(symbol),
        )
        context: Dict[str, Any] = {}
        return context


# -----------------------------------------------------------------------------
# Tradability proxy (preserved)
# -----------------------------------------------------------------------------
def _tradability_from_pred(expected_z: float, horizon_s: int, novelty: float) -> Dict[str, float]:
    try:
        z = float(expected_z)
    except Exception:
        z = 0.0
    try:
        h = max(1, int(horizon_s))
    except Exception:
        h = 3600

    h_scale = (h / 3600.0) ** 0.5
    expected_ret = z * float(RET_SCALE_PER_Z) * float(h_scale)
    expected_cost = float(COST_BPS) / 10000.0

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


# -----------------------------------------------------------------------------
# Heuristic relevance (preserved)
# -----------------------------------------------------------------------------
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
                "PROCESS_EVENTS_ENRICHED_RELEVANCE_REGEX_FAILED",
                e,
                once_key=f"relevance_regex:{symbol}:{label}",
                symbol=str(symbol),
                rule=str(label),
            )
            continue
    return _score_from_hit_count(len(reasons)), reasons


def relevance_map(title: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for sym in symbols:
        score, reasons = relevance_for_title(title, sym)
        out[sym] = {"score": float(score), "reasons": list(reasons)}
    return out


# -----------------------------------------------------------------------------
# Symbol discovery (WATCH-only) (preserved)
# -----------------------------------------------------------------------------
_SYMBOL_PATTERNS = [
    r"\b([A-Z]{2,5})\b",
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
                "PROCESS_EVENTS_ENRICHED_DISCOVER_SYMBOLS_PATTERN_FAILED",
                e,
                once_key=f"discover_symbols_pattern:{pat}",
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
            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_WATCH_SYMBOL_UPSERT_FAILED", e, once_key=f"watch_symbol_upsert:{sym}", symbol=str(sym), ts_ms=int(ts_ms))


# -----------------------------------------------------------------------------
# Ensure schemas exist
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
            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_CONNECTION_CLOSE_FAILED", e, once_key="require_fresh_prices_close", scope="require_fresh_prices")

    fresh_n = int(row[0] or 0)
    fresh_symbols = int(row[1] or 0)
    last_ts_ms = int(row[2] or 0)
    active_symbols = int(active_row[0] or 0)

    required_symbols = int(min_rows)
    if active_symbols > 0:
        required_symbols = max(required_symbols, int(max(1, active_symbols * min_symbol_coverage)))

    try:
        from engine.runtime.ipc import market_data_status

        # If the supervised market-data snapshot says prices are fresh, trust it.
        # This keeps the enriched worker aligned with the ingestion supervisor
        # instead of inventing a separate freshness authority.
        snap = market_data_status(max_age_ms=stale_after_ms)
        if (
            snap.get("ok")
            and snap.get("running")
            and int(snap.get("last_price_ts_ms") or 0) >= int(now_ms - stale_after_ms)
            and int(snap.get("fresh_rows") or 0) >= min_rows
            and int(snap.get("fresh_symbols") or 0) >= required_symbols
        ):
            return
    except Exception:
        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_MARKET_DATA_STATUS_FAILED", Exception("market_data_status lookup failed"), once_key="market_data_status")

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
        try:
            evaluate_rules()
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_RULES_EVALUATION_FAILED", e, once_key="evaluate_rules")

        # Enriched processing participates in the same global execution gates as
        # the rest of the stack because some of its outputs feed downstream risk
        # and alerting paths, not just passive analytics.
        allow0, _, _ = execution_allowed(symbol=None, regime=None)
        if not allow0:
            _warn_nonfatal(
                "PROCESS_EVENTS_ENRICHED_KILL_SWITCH_BLOCKED",
                RuntimeError("execution blocked by kill switch"),
                once_key="kill_switch_blocked",
            )
            return

        _require_fresh_prices_or_exit(JOB_NAME)

        # The enriched path uses the active universe as the anchor for all extra
        # context layers so explainability/risk annotations stay consistent with
        # what the rest of the system considers tradable or worth watching.
        # Universe + symbol status map + spike kill-switch (read-only conn)
        conu = connect_ro()
        try:
            try:
                symbols = get_active_symbols(conu, limit=int(os.environ.get("PROCESS_SYMBOL_LIMIT", "2000")))
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_ENRICHED_SYMBOL_LOAD_FAILED", e, once_key="load_active_symbols")
                symbols = []
            if not symbols:
                symbols = list(DEFAULT_SYMBOLS)
            symbols = list(dict.fromkeys(symbols))

            # Exec-cost spike kill-switch
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
                        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_EXEC_COST_SPIKE_ALERT_FAILED", e, once_key="exec_cost_spike_alert")
                    return
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_ENRICHED_EXEC_COST_SPIKE_CHECK_FAILED", e, once_key="exec_cost_spike_check_startup")

            symbol_status: Dict[str, str] = {}
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
                _warn_nonfatal("PROCESS_EVENTS_ENRICHED_CONNECTION_CLOSE_FAILED", e, once_key="symbols_connection_close", scope="load_symbols")

        # LIVE keeps the minimal event path moving. Enriched work only picks up
        # events that still need embeddings and heavier annotations.
        # Read unembedded events
        con = connect_ro()
        try:
            rows = con.execute(
                """
                SELECT
                  e.id,
                  e.ts_ms,
                  e.source,
                  e.title,
                  e.body,
                  e.url,
                  e.meta_json,
                  e.event_type,
                  e.symbol,
                  e.source_id,
                  nef.finbert_model_name
                FROM events e
                LEFT JOIN event_embeddings emb ON emb.event_id = e.id
                LEFT JOIN news_event_features nef
                  ON nef.event_id = e.id
                WHERE emb.event_id IS NULL
                ORDER BY e.ts_ms DESC
                LIMIT 50
                """
            ).fetchall()
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_ENRICHED_CONNECTION_CLOSE_FAILED", e, once_key="event_scan_connection_close", scope="scan_events")

        if not rows:
            logging.info("no new events")
            return

        finbert_by_event_id: Dict[int, Dict[str, Any]] = {}
        if USE_FINBERT_SENTIMENT:
            pending_finbert_inputs = [
                {
                    "body": body,
                    "event_id": int(eid),
                    "event_key": None,
                    "event_type": event_type,
                    "source": source,
                    "source_id": source_id,
                    "symbol": symbol,
                    "title": title,
                    "ts_ms": int(ts_ms or 0),
                }
                for (eid, ts_ms, source, title, body, _url, _meta_json, event_type, symbol, source_id, finbert_model_name) in rows
                if str(event_type or "").lower() == "news" and str(finbert_model_name or "").strip() != str(FINBERT_MODEL_NAME)
            ]
            if pending_finbert_inputs:
                try:
                    scored_finbert_rows = score_event_rows(pending_finbert_inputs)
                except Exception as e:
                    _warn_nonfatal(
                        "PROCESS_EVENTS_ENRICHED_FINBERT_SCORE_FAILED",
                        e,
                        once_key="finbert_score",
                        batch=int(len(pending_finbert_inputs)),
                    )
                    scored_finbert_rows = []
                if scored_finbert_rows:
                    try:
                        def _write_finbert(conw) -> None:
                            for sentiment_row in scored_finbert_rows:
                                put_finbert_sentiment_enrichment(sentiment_row, con=conw)

                        run_write_txn(
                            _write_finbert,
                            table="news_event_features",
                            operation="process_events_enriched_finbert_batch",
                            context={"job": JOB_NAME, "rows": int(len(scored_finbert_rows))},
                        )
                        finbert_by_event_id = {
                            int(row["event_id"]): dict(row)
                            for row in scored_finbert_rows
                            if int(row.get("event_id") or 0) > 0
                        }
                    except Exception as e:
                        _warn_nonfatal(
                            "PROCESS_EVENTS_ENRICHED_FINBERT_PERSIST_FAILED",
                            e,
                            once_key="finbert_persist",
                            batch=int(len(scored_finbert_rows)),
                        )

        titles = [(r[3] or "") for r in rows]

        # Embed (live stream)
        if _LIVE_STREAM is not None and torch.cuda.is_available():
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
        else:
            embeddings = _get_model().encode(
                titles,
                batch_size=int(os.environ.get("EMBED_BATCH_SIZE", "64")),
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,
            ).astype(np.float32, copy=False)

        # Write connection
        conw = connect(readonly=False)
        try:
            cur = conw.cursor()
            pending_signal_publications: List[Dict[str, Any]] = []

            for (
                eid,
                ts_ms,
                source,
                title,
                body,
                url,
                meta_json,
                _event_type,
                _event_symbol,
                _source_id,
                _finbert_model_name,
            ), vec in zip(rows, embeddings):
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
                        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_HEARTBEAT_UPDATE_FAILED", e, once_key="heartbeat_update", job_name=str(JOB_NAME), event_id=int(eid), event_ts_ms=int(ts_ms or 0))
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
                finbert_summary = finbert_by_event_id.get(int(eid))
                if finbert_summary:
                    event_meta["finbert_sentiment"] = {
                        "confidence": float(finbert_summary.get("confidence") or 0.0),
                        "label": str(finbert_summary.get("label") or ""),
                        "model_name": str(finbert_summary.get("model_name") or ""),
                        "model_version": str(finbert_summary.get("model_version") or ""),
                        "neg": float(finbert_summary.get("neg") or 0.0),
                        "neu": float(finbert_summary.get("neu") or 0.0),
                        "pos": float(finbert_summary.get("pos") or 0.0),
                        "score": float(finbert_summary.get("score") or 0.0),
                    }

                domain = extract_domain(url, meta_json)
                if domain:
                    event_meta["domain"] = domain

                # WATCH discovery
                if DISCOVER_SYMBOLS:
                    try:
                        syms = set()
                        syms |= discover_symbols_from_text(title)
                        syms |= discover_symbols_from_text(body)
                        if syms:
                            upsert_watch_symbols(conw, syms, int(ts_ms))
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_SYMBOL_DISCOVERY_FAILED", e, once_key="symbol_discovery", event_id=int(eid))

                # Enrichment maps
                rel_map = relevance_map(title, symbols)

                # Persist embedding
                cur.execute(
                    "INSERT OR REPLACE INTO event_embeddings(event_id, dim, vec) VALUES (?,?,?)",
                    (int(eid), int(len(vec)), vec.tobytes()),
                )

                # Novelty cache + novelty value
                _RECENT_EMB_CACHE.append(vec)
                if len(_RECENT_EMB_CACHE) > _RECENT_EMB_CACHE_MAX:
                    _RECENT_EMB_CACHE.pop(0)

                novelty = 0.0
                try:
                    novelty = _compute_novelty(conw, event_id=int(eid), vec=vec, lookback=int(NOVELTY_LOOKBACK))
                except Exception:
                    novelty = 0.0
                event_meta["novelty"] = float(novelty)
                _update_event_meta_json(conw, event_id=int(eid), meta=event_meta)

                # Optional clustering (enrichment only)
                cluster_info = None
                if assign_cluster:
                    try:
                        cluster_info = assign_cluster(event_id=int(eid), ts_ms=int(ts_ms), title=title, vec=vec)
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_CLUSTER_ASSIGN_FAILED", e, once_key="cluster_assign", event_id=int(eid))
                        cluster_info = None

                event_ctx = {
                    "event_id": int(eid),
                    "ts_ms": int(ts_ms),
                    "source": source,
                    "title": title,
                    "body": body,
                    "url": url,
                    "meta": event_meta,
                }

                # Predict (batched by horizon)
                preds = predict_runtime_event(
                    vec,
                    symbols,
                    HORIZONS,
                    top_k=8,
                    event=event_ctx,
                )

                # Shadow temporal (kept here but should usually be in shadow worker)
                temporal_shadow = None
                if ENABLE_EXPERIMENTAL_MODELS and predict_temporal_shadow_for_event:
                    try:
                        if _SHADOW_STREAM is not None and torch.cuda.is_available():
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
                        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_TEMPORAL_SHADOW_FAILED", e, once_key="temporal_shadow", event_id=int(eid))
                        temporal_shadow = None

                # Per-symbol processing
                for sym in symbols:
                    st = symbol_status.get(sym)
                    if st in ("DISABLED", "COOLDOWN"):
                        continue

                    reg = get_current_regime(sym)

                    if domain and is_domain_blocked(domain, sym):
                        continue

                    micro_ctx = None
                    try:
                        micro_ctx = load_latest_microstructure_context(conw, sym)
                    except Exception as e:
                        _warn_nonfatal("PROCESS_EVENTS_ENRICHED_MICROSTRUCTURE_CONTEXT_FAILED", e, once_key="microstructure_context_initial", event_id=int(eid), symbol=str(sym))
                        micro_ctx = None

                    for h in HORIZONS:
                        expected_z, conf, explain = preds[(sym, int(h))]
                        adj_conf = float(conf)
                        adj_explain: Dict[str, Any] = {}

                        # microstructure confidence adjustment
                        try:
                            if micro_ctx:
                                adj_conf, micro_explain = apply_microstructure_confidence(
                                    expected_z=float(expected_z),
                                    base_conf=float(adj_conf),
                                    micro_ctx=micro_ctx,
                                )
                                if micro_explain:
                                    adj_explain["microstructure"] = micro_explain
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_MICROSTRUCTURE_ADJUST_FAILED", e, once_key="microstructure_adjust", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        explain = dict(explain or {})
                        explain["event_meta"] = event_meta

                        r = rel_map.get(sym) or {}
                        explain["relevance"] = float(r.get("score", 0.0))
                        explain["relevance_reasons"] = list(r.get("reasons", []))
                        explain["tradability"] = _tradability_from_pred(
                            expected_z=float(expected_z),
                            horizon_s=int(h),
                            novelty=float(novelty),
                        )

                        # Options / earnings / filings
                        opt_ctx = _options_context(conw, sym)
                        if opt_ctx:
                            explain["options"] = opt_ctx
                        opt_anom = _options_anomaly(conw, sym)
                        if opt_anom:
                            explain["options_anomaly"] = opt_anom

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

                        explain["regime"] = str(reg)

                        if micro_ctx:
                            explain["microstructure"] = micro_ctx
                        if domain:
                            explain["domain"] = domain

                        if cluster_info:
                            explain["cluster"] = cluster_info

                        # Exec cost confidence decay + hard block
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
                            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_SPREAD_CONFIDENCE_ADJUST_FAILED", e, once_key="spread_confidence_adjust", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        # Temporal shadow explain
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
                                _warn_nonfatal("PROCESS_EVENTS_ENRICHED_TEMPORAL_SHADOW_EXPLAIN_FAILED", e, once_key="temporal_shadow_explain", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        # Calibrated confidence
                        try:
                            adj_conf, adj_explain = get_adjusted_confidence(
                                conw, symbol=sym, horizon_s=int(h), base_conf=float(adj_conf)
                            )
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_CONFIDENCE_ADJUST_FAILED", e, once_key="confidence_adjust", event_id=int(eid), symbol=str(sym), horizon_s=int(h))
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
                            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_MICROSTRUCTURE_ADJUST_FAILED", e, once_key="microstructure_adjust_secondary", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        # IV / earnings / SEC downweights
                        if opt_ctx and opt_ctx.get("avg_iv"):
                            try:
                                iv = float(opt_ctx["avg_iv"])
                                if iv > 1.0:
                                    adj_conf = adj_conf * 0.85
                            except Exception as e:
                                _warn_nonfatal("PROCESS_EVENTS_ENRICHED_OPTIONS_IV_ADJUST_FAILED", e, once_key="options_iv_adjust", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        if earn_ctx and earn_ctx.get("earnings_date"):
                            try:
                                adj_conf = adj_conf * float(os.environ.get("EARNINGS_CONF_DOWNWEIGHT", "0.85"))
                            except Exception as e:
                                _warn_nonfatal("PROCESS_EVENTS_ENRICHED_EARNINGS_CONF_DOWNWEIGHT_FAILED", e, once_key="earnings_conf_downweight")
                                adj_conf = adj_conf * 0.85

                        if filing_ctx and filing_ctx.get("form"):
                            form = str(filing_ctx.get("form") or "").upper()
                            if form == "8-K":
                                try:
                                    adj_conf = adj_conf * float(os.environ.get("SEC_8K_CONF_DOWNWEIGHT", "0.90"))
                                except Exception as e:
                                    _warn_nonfatal("PROCESS_EVENTS_ENRICHED_SEC_8K_CONF_DOWNWEIGHT_FAILED", e, once_key="sec_8k_conf_downweight")
                                    adj_conf = adj_conf * 0.90

                        # Options anomaly IV spike downweight
                        try:
                            if opt_anom and opt_anom.get("iv_ratio_1h_24h") is not None:
                                ivr = float(opt_anom["iv_ratio_1h_24h"])
                                if ivr >= float(os.environ.get("IV_SPIKE_RATIO", "1.6")):
                                    adj_conf = adj_conf * float(os.environ.get("IV_SPIKE_CONF_DOWNWEIGHT", "0.88"))
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_IV_SPIKE_CONF_DOWNWEIGHT_FAILED", e, once_key="iv_spike_conf_downweight", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        # Domain confidence multiplier (regime-aware)
                        try:
                            if domain:
                                mult = domain_conf_multiplier(domain, sym, reg, int(h))
                                adj_conf = adj_conf * float(mult)
                                explain["domain_conf_mult"] = float(mult)
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_DOMAIN_CONFIDENCE_FAILED", e, once_key="domain_confidence", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

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

                        # Store prediction
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
                            model_version=str(explain.get("model_version") or ""),
                            features_version=str(explain.get("feature_set_tag") or "unknown"),
                            tracking_source="process_events_enriched",
                            con=conw,
                        )

                        # Log decision
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
                                "model_intent": dict(explain.get("model_intent") or {}),
                            },
                            con=conw,
                        )

                        # Alert confidence legacy downweight
                        alert_conf = float(adj_conf)
                        if ALERT_DOWNWEIGHT_NEG_NET:
                            try:
                                tr = explain.get("tradability") or {}
                                net = float(tr.get("expected_ret_net") or 0.0)
                                if net < 0.0:
                                    alert_conf = alert_conf * float(ALERT_DOWNWEIGHT_MULT)
                            except Exception as e:
                                _warn_nonfatal("PROCESS_EVENTS_ENRICHED_ALERT_DOWNWEIGHT_FAILED", e, once_key="alert_downweight_neg_net", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        # Mid-pass spike recheck
                        try:
                            spike_info2 = _detect_exec_cost_spike(conw)
                            if spike_info2 and spike_info2.get("spike"):
                                logging.error("EXEC_COST_SPIKE mid_pass spike_info=%s", spike_info2)
                                continue
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_EXEC_COST_SPIKE_CHECK_FAILED", e, once_key="exec_cost_spike_check_midpass", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

                        if not execution_allowed():
                            continue

                        allow_sym, _, _ = execution_allowed(symbol=sym, regime=None)
                        if not allow_sym:
                            continue

                        try:
                            details = _alert_details_dict(emit_alert(
                                event_id=int(eid),
                                event_title=title,
                                symbol=sym,
                                horizon_s=int(h),
                                expected_z=float(expected_z),
                                confidence=float(alert_conf),
                                explain=explain,
                                con=conw,
                                return_details=True,
                            ))
                            payload = details.get("payload")
                            if details.get("inserted") and isinstance(payload, dict):
                                pending_signal_publications.append(dict(payload))
                        except Exception as e:
                            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_ALERT_EMIT_FAILED", e, once_key="alert_emit", event_id=int(eid), symbol=str(sym), horizon_s=int(h))

            conw.commit()
            for payload in pending_signal_publications:
                try:
                    publish_event("strategy_signal", payload)
                except Exception as e:
                    _warn_nonfatal("PROCESS_EVENTS_ENRICHED_SIGNAL_PUBLISH_FAILED", e, once_key="signal_publish")

        finally:
            try:
                conw.close()
            except Exception as e:
                _warn_nonfatal("PROCESS_EVENTS_ENRICHED_CONNECTION_CLOSE_FAILED", e, once_key="write_connection_close", scope="main_write_connection")

        dur_ms = int(time.time() * 1000) - started_ms
        logging.info("ENRICHED COMPLETE dur_ms=%s", dur_ms)

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("PROCESS_EVENTS_ENRICHED_JOB_LOCK_RELEASE_FAILED", e, once_key="job_lock_release", job_name=str(JOB_NAME), pid=int(PID))


if __name__ == "__main__":
    main()
