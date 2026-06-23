"""
FILE: options_poll.py

Data subsystem module for `options_poll`.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError as exc:
    if getattr(exc, "name", "") != "dotenv" and "No module named 'dotenv'" not in str(exc):
        raise
except Exception:
    logging.getLogger(__name__).debug("dotenv load skipped", exc_info=True)

from engine.data._credentials import get_data_credential
from engine.data.default_symbols import parse_symbol_limit
from engine.data.options.options_polygon import fetch_options_chain_snapshot
from engine.data.options.tradier_live import TradierFetchError, fetch_options_chain
from engine.data.provider_registry import list_provider_definitions
from engine.data.universe import get_active_symbols
from engine.runtime import dbapi_compat as dbapi
from engine.runtime.alerts import emit_alert
from engine.runtime.ingestion_shards import (
    current_ingestion_shard,
    filter_symbols_for_shard,
    ingestion_shard_job_name,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.metrics_store import write_runtime_metric
from engine.runtime.non_price_ingestion_spool import (
    NonPriceIngestionSpoolFullError,
    NonPriceIngestionSpoolUnavailableError,
    SQLiteNonPriceIngestionSpool,
)
from engine.runtime.platform import default_local_db_dir
from engine.runtime.telemetry_append_buffer import append_price_provider_health
from engine.runtime.storage import (
    acquire_job_lock,
    checkpoint_if_due,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = "options_poll"
OWNER = os.environ.get("JOB_OWNER", "system")
PID = os.getpid()
INGESTION_SHARD = current_ingestion_shard()
JOB_LIVENESS_NAME = ingestion_shard_job_name(JOB_NAME, INGESTION_SHARD)


def _parse_options_commit_batch_symbols(raw: Any) -> int:
    text = str(raw or "").strip().lower()
    try:
        value = int(text or "50")
    except Exception:
        value = 50
    return min(50, max(25, int(value)))


def _parse_options_fetch_concurrency(raw: Any) -> int:
    text = str(raw or "").strip().lower()
    try:
        value = int(text or "4")
    except Exception:
        value = 4
    return min(32, max(1, int(value)))


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
OPTIONS_POLL_SECONDS = max(1, int(os.environ.get("OPTIONS_POLL_SECONDS", "300")))
HEARTBEAT_EVERY_S = float(os.environ.get("OPTIONS_POLL_HEARTBEAT_S", "15"))
PROVIDER_HEALTH_EVERY_S = float(os.environ.get("OPTIONS_PROVIDER_HEALTH_EVERY_S", "30"))
OPTIONS_CACHE_MAX_AGE_S = max(60, int(os.environ.get("OPTIONS_CACHE_MAX_AGE_S", str(max(OPTIONS_POLL_SECONDS * 6, 1800)))))
OPTIONS_SYMBOL_FAILURE_THRESHOLD = max(1, int(os.environ.get("OPTIONS_SYMBOL_FAILURE_THRESHOLD", "3")))
OPTIONS_SYMBOL_DISABLE_S = max(60, int(os.environ.get("OPTIONS_SYMBOL_DISABLE_S", "900")))
OPTIONS_POLL_COMMIT_BATCH_SYMBOLS = _parse_options_commit_batch_symbols(
    os.environ.get("OPTIONS_POLL_COMMIT_BATCH_SYMBOLS", os.environ.get("OPTIONS_POLL_COMMIT_EVERY_SYMBOLS", "50"))
)
OPTIONS_POLL_COMMIT_EVERY_SYMBOLS = OPTIONS_POLL_COMMIT_BATCH_SYMBOLS
OPTIONS_POLL_FETCH_CONCURRENCY = _parse_options_fetch_concurrency(os.environ.get("OPTIONS_POLL_FETCH_CONCURRENCY", "4"))
OPTIONS_POLL_COPY_STAGING_ENABLED = _parse_bool_env("OPTIONS_POLL_COPY_STAGING_ENABLED", True)
OPTIONS_POLL_COPY_STAGING_FALLBACK_ENABLED = _parse_bool_env("OPTIONS_POLL_COPY_STAGING_FALLBACK_ENABLED", True)
OPTIONS_POLL_DURABLE_BUFFER_ENABLED = _parse_bool_env("OPTIONS_POLL_DURABLE_BUFFER_ENABLED", True)
OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS = max(1, int(os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS", "250000")))
OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES = max(
    1,
    int(os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES", str(128 * 1024 * 1024))),
)
OPTIONS_POLL_DURABLE_BUFFER_BUSY_TIMEOUT_MS = max(
    1,
    int(os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_BUSY_TIMEOUT_MS", "5000")),
)
OPTIONS_POLL_DURABLE_BUFFER_SYNCHRONOUS = str(
    os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_SYNCHRONOUS", "NORMAL")
).strip().upper()
OPTIONS_POLL_DURABLE_REPLAY_MAX_ROWS = max(
    1,
    int(os.environ.get("OPTIONS_POLL_DURABLE_REPLAY_MAX_ROWS", "50000")),
)
OPTIONS_SYMBOL_STATE_LOAD_CHUNK_SIZE = 900
OPTIONS_PROVIDER_RATE_LIMIT_BASE_COOLDOWN_S = max(
    1,
    int(os.environ.get("OPTIONS_PROVIDER_RATE_LIMIT_BASE_COOLDOWN_S", "60")),
)
OPTIONS_PROVIDER_RATE_LIMIT_MAX_COOLDOWN_S = max(
    OPTIONS_PROVIDER_RATE_LIMIT_BASE_COOLDOWN_S,
    int(os.environ.get("OPTIONS_PROVIDER_RATE_LIMIT_MAX_COOLDOWN_S", "1800")),
)
OPTIONS_SYMBOL_LIMIT = parse_symbol_limit(
    os.environ.get("OPTIONS_SYMBOL_LIMIT", os.environ.get("OPTIONS_UNDERLYING_LIMIT")),
    600,
)
OPTIONS_CRITICAL_SYMBOLS = tuple(
    sorted(
        {
            str(sym or "").upper().strip()
            for sym in os.environ.get("OPTIONS_CRITICAL_SYMBOLS", "DIA,SPY,QQQ,IWM").split(",")
            if str(sym or "").strip()
        }
    )
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [poll_options] %(message)s",
)
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()

provider_cooldowns: Dict[str, float] = {}
provider_rate_limit_counts: Dict[str, int] = {}
provider_cooldown_reasons: Dict[str, str] = {}

_OPTIONS_DURABLE_SPOOL_TABLE = "options_poll_batch"
_OPTIONS_SPOOL_KIND_POLYGON = "options_chain_v2"
_OPTIONS_SPOOL_KIND_TRADIER = "options_chain"
_OPTIONS_SPOOL_KIND_STATE = "options_symbol_ingestion_state"
_OPTIONS_SPOOL_KIND_EVENT = "events"


def _options_durable_spool_path() -> Path:
    configured = str(os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser()
    db_path = str(os.environ.get("DB_PATH") or "").strip()
    if db_path and "://" not in db_path:
        db = Path(db_path).expanduser()
        root = db if db.suffix == "" else db.parent
    else:
        root = Path(os.environ.get("TS_DATA_ROOT") or default_local_db_dir()).expanduser()
    return root / "options_poll_durable_buffer.sqlite"


def _new_options_durable_spool() -> SQLiteNonPriceIngestionSpool:
    return SQLiteNonPriceIngestionSpool(
        path=_options_durable_spool_path(),
        max_rows=int(OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS),
        max_bytes=int(OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES),
        busy_timeout_ms=int(OPTIONS_POLL_DURABLE_BUFFER_BUSY_TIMEOUT_MS),
        synchronous=str(OPTIONS_POLL_DURABLE_BUFFER_SYNCHRONOUS),
    )


def _require_storage_owned_table(con, table_name: str) -> None:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name=?
        LIMIT 1
        """,
        (str(table_name),),
    ).fetchone()
    if row:
        return
    raise dbapi.OperationalError(
        f"{table_name} missing; call engine.runtime.storage.init_db() before options_poll"
    )


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _warn_state(code: str, message: str, **extra: Any) -> None:
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(message),
        error=None,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )


def _clip_error(value: Any) -> str:
    return str(value or "").strip()[:400]


def _provider_name(provider: Any) -> str:
    return str(provider or "").strip().lower()


def _provider_status_code(error: Any) -> Optional[int]:
    raw = getattr(error, "status_code", None)
    if raw is None:
        response = getattr(error, "response", None)
        raw = getattr(response, "status_code", None)
    if raw is not None:
        try:
            status_code = int(raw)
            if 100 <= status_code <= 599:
                return status_code
        except Exception as e:
            _warn_nonfatal(
                "OPTIONS_POLL_STATUS_CODE_PARSE_FAILED",
                e,
                once_key="options_poll_status_code_parse",
                status_code=repr(raw)[:80],
            )

    text = str(error or "")
    match = re.search(r"\b(429|503|401|403)\b", text)
    if match:
        return int(match.group(1))
    return None


def _provider_retry_after_s(error: Any) -> Optional[float]:
    raw = getattr(error, "retry_after_s", None)
    if raw is None:
        response = getattr(error, "response", None)
        headers = getattr(response, "headers", None)
        if headers is None:
            headers = getattr(error, "headers", None)
        if headers is not None:
            try:
                raw = headers.get("Retry-After")
            except Exception:
                raw = None
    if raw is None:
        match = re.search(r"retry-after[=:]\s*([0-9]+(?:\.[0-9]+)?)", str(error or ""), flags=re.IGNORECASE)
        if match:
            raw = match.group(1)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        value = float(raw)
    except Exception as e:
        _warn_nonfatal(
            "OPTIONS_POLL_RETRY_AFTER_PARSE_FAILED",
            e,
            once_key="options_poll_retry_after_parse",
            retry_after=repr(raw)[:120],
        )
        return None
    if not math.isfinite(value):
        return None
    return max(0.0, float(value))


def _provider_error_is_rate_limited(error: Any) -> bool:
    status_code = _provider_status_code(error)
    if status_code in {429, 503}:
        return True
    kind = str(getattr(error, "kind", "") or "").strip().lower()
    if kind == "rate_limit":
        return True
    text = str(error or "").strip().lower()
    return "rate limit" in text or "rate_limited" in text or "too many requests" in text


def _provider_error_disables_run(error: Any) -> bool:
    kind = str(getattr(error, "kind", "") or "").strip().lower()
    if kind in {"config_error", "auth_error"}:
        return True
    status_code = _provider_status_code(error)
    if status_code in {401, 403}:
        return True
    text = str(error or "").strip().lower()
    return any(
        marker in text
        for marker in (
            "api_key not set",
            "api token missing",
            "token_missing",
            "unauthorized",
            "forbidden",
        )
    )


def _provider_cooldown_remaining_s(provider: str, now_s: float) -> float:
    cooldown_until_s = float(provider_cooldowns.get(_provider_name(provider)) or 0.0)
    return max(0.0, cooldown_until_s - float(now_s))


def _provider_cooldown_snapshot(providers: List[str], *, now_s: float) -> Dict[str, Dict[str, Any]]:
    snapshot: Dict[str, Dict[str, Any]] = {}
    for provider in providers:
        name = _provider_name(provider)
        if not name:
            continue
        cooldown_until_s = float(provider_cooldowns.get(name) or 0.0)
        remaining_s = max(0.0, cooldown_until_s - float(now_s))
        if remaining_s <= 0.0:
            continue
        snapshot[name] = {
            "cooldown_until_ts_ms": int(cooldown_until_s * 1000.0),
            "remaining_s": float(remaining_s),
            "consecutive_rate_limits": int(provider_rate_limit_counts.get(name) or 0),
            "reason": str(provider_cooldown_reasons.get(name) or "rate_limit"),
        }
    return snapshot


def _write_provider_cooldown_metric(provider: str, *, now_s: float, remaining_s: float) -> None:
    name = _provider_name(provider)
    if not name:
        return
    cooldown_until_s = float(provider_cooldowns.get(name) or 0.0)
    try:
        write_runtime_metric(
            "options.provider.cooldown_remaining_s",
            value_num=max(0.0, float(remaining_s)),
            tags={
                "job": JOB_NAME,
                "provider": name,
                "reason": str(provider_cooldown_reasons.get(name) or ""),
                "cooldown_until_ts_ms": str(int(cooldown_until_s * 1000.0)) if cooldown_until_s > 0.0 else "0",
                "consecutive_rate_limits": str(int(provider_rate_limit_counts.get(name) or 0)),
            },
            ts_ms=int(float(now_s) * 1000.0),
        )
    except Exception as e:
        _warn_nonfatal(
            "OPTIONS_POLL_PROVIDER_COOLDOWN_METRIC_FAILED",
            e,
            once_key=f"provider_cooldown_metric:{name}",
            provider=name,
        )


def _write_options_poll_metric(
    metric: str,
    value: Any,
    *,
    ts_ms: int,
    tags: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        metric_tags = {
            "job": JOB_NAME,
            "liveness_job": JOB_LIVENESS_NAME,
            "shard_index": str(int(INGESTION_SHARD.index)),
            "shard_count": str(int(INGESTION_SHARD.count)),
        }
        for key, tag_value in dict(tags or {}).items():
            if tag_value is not None:
                metric_tags[str(key)] = str(tag_value)
        write_runtime_metric(
            str(metric),
            value_num=value,
            tags=metric_tags,
            ts_ms=int(ts_ms),
        )
    except Exception as e:
        _warn_nonfatal(
            "OPTIONS_POLL_RUNTIME_METRIC_FAILED",
            e,
            once_key=f"options_poll_runtime_metric:{metric}",
            metric=str(metric),
        )


def _emit_options_poll_run_metrics(meta: Dict[str, Any], provider_status: Dict[str, Dict[str, Any]], *, ts_ms: int) -> None:
    scalar_metrics = {
        "options.poll.state_load_queries": int(meta.get("state_load_queries") or 0),
        "options.poll.state_load_symbols": int(meta.get("state_load_symbols") or 0),
        "options.poll.fetch_batches": int(meta.get("provider_fetch_batches") or 0),
        "options.poll.fetch_symbols": int(meta.get("provider_fetch_symbols") or 0),
        "options.poll.fetch_max_workers": int(meta.get("provider_fetch_max_workers") or 0),
        "options.poll.commit_batches": int(meta.get("commit_batches") or 0),
        "options.poll.max_symbols_per_commit": int(meta.get("max_symbols_per_commit") or 0),
        "options.poll.rows_written": int(meta.get("rows_written") or 0),
        "options.poll.polygon_rows_written": int(meta.get("polygon_rows_written") or 0),
        "options.poll.tradier_rows_written": int(meta.get("tradier_rows_written") or 0),
        "options.poll.event_rows_written": int(meta.get("event_rows_written") or 0),
        "options.poll.state_rows_written": int(meta.get("state_rows_written") or 0),
        "options.poll.copy_staging_batches": int(meta.get("copy_staging_batches") or 0),
        "options.poll.executemany_batches": int(meta.get("executemany_batches") or 0),
        "options.poll.copy_fallbacks": int(meta.get("copy_fallbacks") or 0),
        "options.poll.cached_fallback_symbols": int(meta.get("cached_fallback_symbols") or 0),
        "options.poll.fetch_failures": int(meta.get("provider_fetch_failures") or 0),
        "options.poll.bulk_write_failures": int(meta.get("bulk_write_failures") or 0),
        "options.poll.event_write_failures": int(meta.get("event_write_failures") or 0),
        "options.poll.state_write_failures": int(meta.get("state_write_failures") or 0),
        "options.poll.durable_buffer.pending_rows": int(meta.get("durable_buffer_pending_rows") or 0),
        "options.poll.durable_buffer.pending_bytes": int(meta.get("durable_buffer_pending_bytes") or 0),
        "options.poll.durable_buffer.oldest_age_ms": int(meta.get("durable_buffer_oldest_age_ms") or 0),
        "options.poll.durable_buffer.rows_fill_ratio": float(meta.get("durable_buffer_rows_fill_ratio") or 0.0),
        "options.poll.durable_buffer.bytes_fill_ratio": float(meta.get("durable_buffer_bytes_fill_ratio") or 0.0),
        "options.poll.durable_buffer.spooled_rows": int(meta.get("durable_buffer_spooled_rows") or 0),
        "options.poll.durable_buffer.replayed_rows": int(meta.get("durable_buffer_replayed_rows") or 0),
        "options.poll.durable_buffer.deleted_rows": int(meta.get("durable_buffer_deleted_rows") or 0),
        "options.poll.durable_buffer.dropped_rows": int(meta.get("durable_buffer_dropped_rows") or 0),
        "options.poll.durable_buffer.rejected_rows": int(meta.get("durable_buffer_rejected_rows") or 0),
        "options.poll.durable_buffer.enqueue_failures": int(meta.get("durable_buffer_enqueue_failures") or 0),
        "options.poll.durable_buffer.replay_failures": int(meta.get("durable_buffer_replay_failures") or 0),
        "options.poll.durable_buffer.delete_failures": int(meta.get("durable_buffer_delete_failures") or 0),
        "options.poll.durable_buffer.corrupt_payload_rows": int(
            meta.get("durable_buffer_corrupt_payload_rows") or 0
        ),
        "options.poll.durable_buffer.backpressure_active": 1
        if bool(meta.get("durable_buffer_backpressure_active"))
        else 0,
        "options.poll.durable_buffer.backpressure_events": int(meta.get("durable_buffer_backpressure_events") or 0),
        "options.poll.durable_buffer.backpressure_recoveries": int(
            meta.get("durable_buffer_backpressure_recoveries") or 0
        ),
    }
    for metric, value in scalar_metrics.items():
        _write_options_poll_metric(metric, value, ts_ms=ts_ms)
    for provider, status in dict(provider_status or {}).items():
        provider_name = _provider_name(provider)
        if not provider_name:
            continue
        _write_options_poll_metric(
            "options.poll.provider.rows",
            int((status or {}).get("rows") or 0),
            ts_ms=ts_ms,
            tags={"provider": provider_name},
        )
        _write_options_poll_metric(
            "options.poll.provider.failed_symbols",
            int((status or {}).get("failed_symbols") or 0),
            ts_ms=ts_ms,
            tags={"provider": provider_name},
        )


def _clear_provider_cooldown(provider: str, *, now_s: float) -> None:
    name = _provider_name(provider)
    if not name:
        return
    had_cooldown = name in provider_cooldowns
    provider_cooldowns.pop(name, None)
    provider_cooldown_reasons.pop(name, None)
    if had_cooldown:
        _write_provider_cooldown_metric(name, now_s=now_s, remaining_s=0.0)


def _refresh_provider_cooldowns(providers: List[str], *, now_s: float) -> None:
    active = {_provider_name(provider) for provider in providers if _provider_name(provider)}
    for provider in list(provider_cooldowns.keys()):
        if provider not in active:
            continue
        remaining_s = _provider_cooldown_remaining_s(provider, now_s)
        if remaining_s <= 0.0:
            _clear_provider_cooldown(provider, now_s=now_s)
        else:
            _write_provider_cooldown_metric(provider, now_s=now_s, remaining_s=remaining_s)


def _mark_provider_rate_limited(provider: str, error: Any, *, now_s: float) -> Dict[str, Any]:
    name = _provider_name(provider)
    retry_after_s = _provider_retry_after_s(error)
    consecutive = int(provider_rate_limit_counts.get(name) or 0) + 1
    provider_rate_limit_counts[name] = consecutive

    if retry_after_s is not None and retry_after_s > 0.0:
        cooldown_s = float(retry_after_s)
    else:
        cooldown_s = float(OPTIONS_PROVIDER_RATE_LIMIT_BASE_COOLDOWN_S) * (2 ** max(0, consecutive - 1))
    cooldown_s = min(float(OPTIONS_PROVIDER_RATE_LIMIT_MAX_COOLDOWN_S), max(1.0, cooldown_s))

    status_code = _provider_status_code(error)
    reason = f"http_{status_code}" if status_code is not None else "rate_limit"
    provider_cooldowns[name] = float(now_s) + float(cooldown_s)
    provider_cooldown_reasons[name] = reason
    _write_provider_cooldown_metric(name, now_s=now_s, remaining_s=cooldown_s)
    return {
        "cooldown_s": float(cooldown_s),
        "cooldown_until_ts_ms": int(provider_cooldowns[name] * 1000.0),
        "consecutive_rate_limits": int(consecutive),
        "retry_after_s": retry_after_s,
        "status_code": status_code,
        "reason": reason,
    }


def _provider_cooldown_error(provider: str, *, now_s: float) -> str:
    remaining_s = _provider_cooldown_remaining_s(provider, now_s)
    return f"provider_rate_limit_cooldown:{int(math.ceil(remaining_s))}s"


def _record_provider_rate_limit(
    provider_status: Dict[str, Dict[str, Any]],
    provider_errors: Dict[str, str],
    provider: str,
    error: Any,
    *,
    now_s: float,
    symbol: str,
) -> str:
    cooldown = _mark_provider_rate_limited(provider, error, now_s=now_s)
    retry_after_s = cooldown.get("retry_after_s")
    status_code = cooldown.get("status_code")
    error_text = (
        f"{cooldown.get('reason')}:provider_rate_limited:"
        f"cooldown_s={int(math.ceil(float(cooldown.get('cooldown_s') or 0.0)))}"
    )
    provider_errors[provider] = error_text
    status = provider_status.get(provider)
    if status is not None:
        status["error"] = _clip_error(error_text)
        status["failed_symbols"] = int(status.get("failed_symbols") or 0) + 1
        status["cooldown_until_ts_ms"] = int(cooldown.get("cooldown_until_ts_ms") or 0)
        status["cooldown_remaining_s"] = float(cooldown.get("cooldown_s") or 0.0)
        status["consecutive_rate_limits"] = int(cooldown.get("consecutive_rate_limits") or 0)
    _warn_state(
        "OPTIONS_POLL_PROVIDER_RATE_LIMITED",
        "Options provider entered cooldown after rate-limit response.",
        provider=provider,
        symbol=symbol,
        status_code=status_code,
        retry_after_s=retry_after_s,
        cooldown_s=float(cooldown.get("cooldown_s") or 0.0),
        cooldown_until_ts_ms=int(cooldown.get("cooldown_until_ts_ms") or 0),
    )
    return error_text


def _safe_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal("OPTIONS_POLL_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(value)[:120])
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _safe_int(value: Any) -> Optional[int]:
    out = _safe_float(value)
    if out is None:
        return None
    return int(out)


def _ensure_options_symbol_state_table(con) -> None:
    _require_storage_owned_table(con, "options_symbol_ingestion_state")


def _put_provider_health(provider: str, ok: bool, n_symbols: int, error: str | None = None) -> None:
    now_ms = int(time.time() * 1000)
    append_price_provider_health(
        provider=str(provider),
        ok=bool(ok),
        latency_ms=None,
        n_symbols=int(n_symbols),
        error=(_clip_error(error) if error else None),
        ts_ms=int(now_ms),
    )
    get_manager().record_source_status(
        str(provider),
        ok=bool(ok),
        message="options provider health update",
        error=str(error or ""),
        meta={"job_name": JOB_NAME, "n_symbols": int(n_symbols)},
        ts_ms=int(now_ms),
        best_effort=True,
    )


def _get_enabled_options_providers() -> List[str]:
    providers = []

    polygon_key = get_data_credential("POLYGON_API_KEY")
    tradier_token = get_data_credential("TRADIER_API_TOKEN")

    for provider in list_provider_definitions():
        if not provider.enabled:
            continue

        supports = provider.supports or {}
        asset_classes = supports.get("asset_classes") or []
        provider_name = str(provider.provider_name or "").strip().lower()

        if "options" not in asset_classes:
            continue

        if provider_name == "polygon":
            if polygon_key:
                providers.append("polygon")
            continue

        if provider_name == "tradier":
            if tradier_token:
                providers.append("tradier")
            continue

    if tradier_token and "tradier" not in providers:
        providers.append("tradier")

    return providers


def _resolve_providers() -> List[str]:
    providers = [
        provider.strip().lower()
        for provider in os.environ.get("OPTIONS_PROVIDER_CHAIN", "").split(",")
        if provider.strip()
    ]

    supported_providers = {"polygon", "tradier"}
    providers = [provider for provider in providers if provider in supported_providers]

    if not providers:
        providers = _get_enabled_options_providers()

    if not providers:
        polygon_key = get_data_credential("POLYGON_API_KEY")
        tradier_token = get_data_credential("TRADIER_API_TOKEN")

        if polygon_key:
            providers = ["polygon"]
        elif tradier_token:
            providers = ["tradier"]
        else:
            raise RuntimeError("options_poll_no_enabled_provider")

    return providers


def _normal_symbol(symbol: Any) -> str:
    return str(symbol or "").upper().strip()


def _load_active_symbols_for_shard(con) -> List[str]:
    symbols = list(
        dict.fromkeys(
            _normal_symbol(symbol)
            for symbol in get_active_symbols(con, limit=OPTIONS_SYMBOL_LIMIT)
            if _normal_symbol(symbol)
        )
    )
    return filter_symbols_for_shard(symbols, INGESTION_SHARD)


def _default_symbol_state(symbol: str) -> Dict[str, Any]:
    return {
        "symbol": str(symbol),
        "provider": "",
        "consecutive_failures": 0,
        "total_failures": 0,
        "last_failure_ts_ms": None,
        "last_failure_error": "",
        "last_success_ts_ms": None,
        "last_fresh_snapshot_ts_ms": None,
        "last_cached_snapshot_ts_ms": None,
        "last_fallback_ts_ms": None,
        "last_row_count": 0,
        "disabled_until_ts_ms": 0,
        "updated_ts_ms": 0,
    }


def _coerce_symbol_state(symbol: str, row: Any) -> Dict[str, Any]:
    if not row:
        return _default_symbol_state(symbol)
    return {
        "symbol": str(symbol),
        "provider": str(row[0] or ""),
        "consecutive_failures": int(row[1] or 0),
        "total_failures": int(row[2] or 0),
        "last_failure_ts_ms": (int(row[3]) if row[3] is not None else None),
        "last_failure_error": str(row[4] or ""),
        "last_success_ts_ms": (int(row[5]) if row[5] is not None else None),
        "last_fresh_snapshot_ts_ms": (int(row[6]) if row[6] is not None else None),
        "last_cached_snapshot_ts_ms": (int(row[7]) if row[7] is not None else None),
        "last_fallback_ts_ms": (int(row[8]) if row[8] is not None else None),
        "last_row_count": int(row[9] or 0),
        "disabled_until_ts_ms": int(row[10] or 0),
        "updated_ts_ms": int(row[11] or 0),
    }


def _load_symbol_states(con, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    ordered_symbols = list(dict.fromkeys(_normal_symbol(symbol) for symbol in symbols if _normal_symbol(symbol)))
    states = {symbol: _default_symbol_state(symbol) for symbol in ordered_symbols}
    if not ordered_symbols:
        return states

    for idx in range(0, len(ordered_symbols), OPTIONS_SYMBOL_STATE_LOAD_CHUNK_SIZE):
        chunk = ordered_symbols[idx : idx + OPTIONS_SYMBOL_STATE_LOAD_CHUNK_SIZE]
        placeholders = ",".join("?" for _ in chunk)
        rows = con.execute(
            f"""
            SELECT
              symbol,
              provider,
              consecutive_failures,
              total_failures,
              last_failure_ts_ms,
              last_failure_error,
              last_success_ts_ms,
              last_fresh_snapshot_ts_ms,
              last_cached_snapshot_ts_ms,
              last_fallback_ts_ms,
              last_row_count,
              disabled_until_ts_ms,
              updated_ts_ms
            FROM options_symbol_ingestion_state
            WHERE symbol IN ({placeholders})
            """,
            tuple(chunk),
        ).fetchall() or []
        for row in rows:
            symbol = _normal_symbol(row[0])
            if symbol:
                states[symbol] = _coerce_symbol_state(symbol, row[1:])
    return states


def _load_symbol_state(con, symbol: str) -> Dict[str, Any]:
    symbol = _normal_symbol(symbol)
    row = con.execute(
        """
        SELECT
          provider,
          consecutive_failures,
          total_failures,
          last_failure_ts_ms,
          last_failure_error,
          last_success_ts_ms,
          last_fresh_snapshot_ts_ms,
          last_cached_snapshot_ts_ms,
          last_fallback_ts_ms,
          last_row_count,
          disabled_until_ts_ms,
          updated_ts_ms
        FROM options_symbol_ingestion_state
        WHERE symbol = ?
        LIMIT 1
        """,
        (str(symbol),),
    ).fetchone()
    return _coerce_symbol_state(symbol, row)


def _cached_symbol_state(
    state_cache: Optional[Dict[str, Dict[str, Any]]],
    symbol: str,
) -> Optional[Dict[str, Any]]:
    if state_cache is None:
        return None
    symbol = _normal_symbol(symbol)
    if symbol not in state_cache:
        state_cache[symbol] = _default_symbol_state(symbol)
    return dict(state_cache[symbol])


def _update_cached_symbol_state(
    state_cache: Optional[Dict[str, Dict[str, Any]]],
    state: Dict[str, Any],
) -> None:
    if state_cache is None:
        return
    symbol = _normal_symbol(state.get("symbol"))
    if symbol:
        state_cache[symbol] = dict(state)


_SYMBOL_STATE_UPSERT_SQL = """
INSERT INTO options_symbol_ingestion_state(
  symbol, provider, consecutive_failures, total_failures,
  last_failure_ts_ms, last_failure_error, last_success_ts_ms,
  last_fresh_snapshot_ts_ms, last_cached_snapshot_ts_ms, last_fallback_ts_ms,
  last_row_count, disabled_until_ts_ms, updated_ts_ms
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol) DO UPDATE SET
  provider=excluded.provider,
  consecutive_failures=excluded.consecutive_failures,
  total_failures=excluded.total_failures,
  last_failure_ts_ms=excluded.last_failure_ts_ms,
  last_failure_error=excluded.last_failure_error,
  last_success_ts_ms=excluded.last_success_ts_ms,
  last_fresh_snapshot_ts_ms=excluded.last_fresh_snapshot_ts_ms,
  last_cached_snapshot_ts_ms=excluded.last_cached_snapshot_ts_ms,
  last_fallback_ts_ms=excluded.last_fallback_ts_ms,
  last_row_count=excluded.last_row_count,
  disabled_until_ts_ms=excluded.disabled_until_ts_ms,
  updated_ts_ms=excluded.updated_ts_ms
"""


def _symbol_state_params(state: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        state["symbol"],
        state["provider"],
        state["consecutive_failures"],
        state["total_failures"],
        state["last_failure_ts_ms"],
        state["last_failure_error"],
        state["last_success_ts_ms"],
        state["last_fresh_snapshot_ts_ms"],
        state["last_cached_snapshot_ts_ms"],
        state["last_fallback_ts_ms"],
        state["last_row_count"],
        state["disabled_until_ts_ms"],
        state["updated_ts_ms"],
    )


def _write_symbol_state_rows(con, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    con.executemany(_SYMBOL_STATE_UPSERT_SQL, rows)
    return len(rows)


def _upsert_symbol_state(con, state: Dict[str, Any]) -> None:
    con.execute(_SYMBOL_STATE_UPSERT_SQL, _symbol_state_params(state))


def _build_symbol_success_state(
    prev: Dict[str, Any],
    symbol: str,
    *,
    provider: str,
    now_ms: int,
    snapshot_ts_ms: int,
    row_count: int,
) -> Dict[str, Any]:
    symbol = _normal_symbol(symbol)
    return {
        "symbol": str(symbol),
        "provider": str(provider),
        "consecutive_failures": 0,
        "total_failures": int(prev.get("total_failures") or 0),
        "last_failure_ts_ms": prev.get("last_failure_ts_ms"),
        "last_failure_error": "",
        "last_success_ts_ms": int(now_ms),
        "last_fresh_snapshot_ts_ms": int(snapshot_ts_ms),
        "last_cached_snapshot_ts_ms": prev.get("last_cached_snapshot_ts_ms"),
        "last_fallback_ts_ms": prev.get("last_fallback_ts_ms"),
        "last_row_count": int(row_count),
        "disabled_until_ts_ms": 0,
        "updated_ts_ms": int(now_ms),
    }


def _record_symbol_success(
    con,
    symbol: str,
    *,
    provider: str,
    now_ms: int,
    snapshot_ts_ms: int,
    row_count: int,
    state_cache: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    symbol = _normal_symbol(symbol)
    prev = _cached_symbol_state(state_cache, symbol) or _load_symbol_state(con, symbol)
    state = _build_symbol_success_state(
        prev,
        symbol,
        provider=provider,
        now_ms=now_ms,
        snapshot_ts_ms=snapshot_ts_ms,
        row_count=row_count,
    )
    _upsert_symbol_state(con, state)
    _update_cached_symbol_state(state_cache, state)
    return state


def _build_symbol_failure_state(
    prev: Dict[str, Any],
    symbol: str,
    *,
    provider: str,
    error: str,
    now_ms: int,
    fallback_snapshot_ts_ms: Optional[int] = None,
    row_count: int = 0,
) -> Dict[str, Any]:
    symbol = _normal_symbol(symbol)
    consecutive_failures = int(prev.get("consecutive_failures") or 0) + 1
    total_failures = int(prev.get("total_failures") or 0) + 1
    disabled_until_ts_ms = int(prev.get("disabled_until_ts_ms") or 0)
    if consecutive_failures >= OPTIONS_SYMBOL_FAILURE_THRESHOLD:
        disabled_until_ts_ms = max(disabled_until_ts_ms, int(now_ms + (OPTIONS_SYMBOL_DISABLE_S * 1000)))
    elif disabled_until_ts_ms <= now_ms:
        disabled_until_ts_ms = 0
    return {
        "symbol": str(symbol),
        "provider": str(provider),
        "consecutive_failures": int(consecutive_failures),
        "total_failures": int(total_failures),
        "last_failure_ts_ms": int(now_ms),
        "last_failure_error": _clip_error(error),
        "last_success_ts_ms": prev.get("last_success_ts_ms"),
        "last_fresh_snapshot_ts_ms": prev.get("last_fresh_snapshot_ts_ms"),
        "last_cached_snapshot_ts_ms": (
            int(fallback_snapshot_ts_ms) if fallback_snapshot_ts_ms is not None else prev.get("last_cached_snapshot_ts_ms")
        ),
        "last_fallback_ts_ms": (int(now_ms) if fallback_snapshot_ts_ms is not None else prev.get("last_fallback_ts_ms")),
        "last_row_count": int(row_count if row_count > 0 else (prev.get("last_row_count") or 0)),
        "disabled_until_ts_ms": int(disabled_until_ts_ms),
        "updated_ts_ms": int(now_ms),
    }


def _record_symbol_failure(
    con,
    symbol: str,
    *,
    provider: str,
    error: str,
    now_ms: int,
    fallback_snapshot_ts_ms: Optional[int] = None,
    row_count: int = 0,
    state_cache: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    symbol = _normal_symbol(symbol)
    prev = _cached_symbol_state(state_cache, symbol) or _load_symbol_state(con, symbol)
    state = _build_symbol_failure_state(
        prev,
        symbol,
        provider=provider,
        error=error,
        now_ms=now_ms,
        fallback_snapshot_ts_ms=fallback_snapshot_ts_ms,
        row_count=row_count,
    )
    _upsert_symbol_state(con, state)
    _update_cached_symbol_state(state_cache, state)
    return state


def _rollback_if_active(con) -> None:
    try:
        if bool(getattr(con, "in_transaction", False)):
            con.rollback()
    except Exception as e:
        _warn_nonfatal(
            "OPTIONS_POLL_ROLLBACK_FAILED",
            e,
            once_key="options_poll_rollback_failed",
        )


def _build_symbol_cache_use_state(
    prev: Dict[str, Any],
    symbol: str,
    *,
    provider: str,
    now_ms: int,
    snapshot_ts_ms: int,
    row_count: int,
) -> Dict[str, Any]:
    symbol = _normal_symbol(symbol)
    return {
        "symbol": str(symbol),
        "provider": str(provider),
        "consecutive_failures": int(prev.get("consecutive_failures") or 0),
        "total_failures": int(prev.get("total_failures") or 0),
        "last_failure_ts_ms": prev.get("last_failure_ts_ms"),
        "last_failure_error": prev.get("last_failure_error"),
        "last_success_ts_ms": prev.get("last_success_ts_ms"),
        "last_fresh_snapshot_ts_ms": prev.get("last_fresh_snapshot_ts_ms"),
        "last_cached_snapshot_ts_ms": int(snapshot_ts_ms),
        "last_fallback_ts_ms": int(now_ms),
        "last_row_count": int(row_count),
        "disabled_until_ts_ms": int(prev.get("disabled_until_ts_ms") or 0),
        "updated_ts_ms": int(now_ms),
    }


def _record_symbol_cache_use(
    con,
    symbol: str,
    *,
    provider: str,
    now_ms: int,
    snapshot_ts_ms: int,
    row_count: int,
    state_cache: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    symbol = _normal_symbol(symbol)
    prev = _cached_symbol_state(state_cache, symbol) or _load_symbol_state(con, symbol)
    state = _build_symbol_cache_use_state(
        prev,
        symbol,
        provider=provider,
        now_ms=now_ms,
        snapshot_ts_ms=snapshot_ts_ms,
        row_count=row_count,
    )
    _upsert_symbol_state(con, state)
    _update_cached_symbol_state(state_cache, state)
    return state


def _validate_tradier_rows(rows: List[Dict[str, Any]], symbol: str) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        expiry = str(row.get("expiry") or "").strip()
        call_put = str(row.get("call_put") or "").strip().upper()
        strike = _safe_float(row.get("strike"))
        if not expiry or call_put not in {"C", "P"} or strike is None:
            continue
        cleaned.append(
            {
                "expiry": expiry,
                "strike": float(strike),
                "call_put": call_put,
                "iv": _safe_float(row.get("iv")),
                "open_interest": _safe_int(row.get("open_interest")),
                "volume": _safe_int(row.get("volume")),
            }
        )
    if rows and not cleaned:
        raise ValueError(f"tradier_payload_rejected:{symbol}")
    return cleaned


def _validate_polygon_contracts(contracts: List[Dict[str, Any]], symbol: str, ts_ms: int) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    for contract in contracts or []:
        if not isinstance(contract, dict):
            continue
        contract_id = str(contract.get("contract") or "").strip()
        underlying = str(contract.get("underlying") or symbol).strip().upper()
        if not contract_id or not underlying:
            continue
        cleaned.append(
            {
                "ts_ms": int(contract.get("ts_ms") or ts_ms),
                "underlying": underlying,
                "contract": contract_id,
                "expiration": (str(contract.get("expiration")) if contract.get("expiration") is not None else None),
                "contract_type": (str(contract.get("contract_type")) if contract.get("contract_type") is not None else None),
                "strike": _safe_float(contract.get("strike")),
                "iv": _safe_float(contract.get("iv")),
                "open_interest": _safe_float(contract.get("open_interest")),
                "volume": _safe_float(contract.get("volume")),
                "bid": _safe_float(contract.get("bid")),
                "ask": _safe_float(contract.get("ask")),
                "delta": _safe_float(contract.get("delta")),
                "gamma": _safe_float(contract.get("gamma")),
                "theta": _safe_float(contract.get("theta")),
                "vega": _safe_float(contract.get("vega")),
                "source": str(contract.get("source") or "polygon"),
            }
        )
    if contracts and not cleaned:
        raise ValueError(f"polygon_payload_rejected:{symbol}")
    return cleaned


def _polygon_contract_value_rows(contracts: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    return [
        (
            int(contract["ts_ms"]),
            str(contract["underlying"]),
            str(contract["contract"]),
            contract.get("expiration"),
            contract.get("contract_type"),
            contract.get("strike"),
            contract.get("iv"),
            contract.get("open_interest"),
            contract.get("volume"),
            contract.get("bid"),
            contract.get("ask"),
            contract.get("delta"),
            contract.get("gamma"),
            contract.get("theta"),
            contract.get("vega"),
            str(contract.get("source") or "polygon"),
        )
        for contract in contracts
    ]


def _tradier_value_rows(symbol: str, rows: List[Dict[str, Any]], *, ts_ms: int, source: str) -> List[Tuple[Any, ...]]:
    return [
        (
            int(ts_ms),
            str(symbol),
            str(row["expiry"]),
            float(row["strike"]),
            str(row["call_put"]),
            row.get("iv"),
            row.get("open_interest"),
            row.get("volume"),
            str(source),
        )
        for row in rows
    ]


def _write_polygon_contract_rows(con, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO options_chain_v2(
          ts_ms,
          underlying, contract, expiration, contract_type, strike,
          iv, open_interest, volume,
          bid, ask,
          delta, gamma, theta, vega,
          source
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(contract, ts_ms) DO UPDATE SET
          underlying=excluded.underlying,
          expiration=excluded.expiration,
          contract_type=excluded.contract_type,
          strike=excluded.strike,
          iv=excluded.iv,
          open_interest=excluded.open_interest,
          volume=excluded.volume,
          bid=excluded.bid,
          ask=excluded.ask,
          delta=excluded.delta,
          gamma=excluded.gamma,
          theta=excluded.theta,
          vega=excluded.vega,
          source=excluded.source
        """,
        rows,
    )
    return len(rows)


def _write_tradier_value_rows(con, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO options_chain(
          ts_ms, symbol, expiry, strike, call_put,
          iv, open_interest, volume, source
        )
        VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, expiry, strike, call_put, ts_ms) DO UPDATE SET
          iv=excluded.iv,
          open_interest=excluded.open_interest,
          volume=excluded.volume,
          source=excluded.source
        """,
        rows,
    )
    return len(rows)


class _OptionsCopyUnavailable(RuntimeError):
    pass


def _copy_staging_session() -> str:
    return f"{PID}:{time.time_ns()}"


def _raw_copy_cursor(con):
    raw = getattr(con, "raw", None)
    if raw is None or dbapi.is_sqlite_connection(raw) or dbapi.is_sqlite_connection(con):
        raise _OptionsCopyUnavailable("options_copy_staging_requires_postgres_raw_connection")
    cursor_factory = getattr(raw, "cursor", None)
    if not callable(cursor_factory):
        raise _OptionsCopyUnavailable("options_copy_staging_cursor_unavailable")
    cur = raw.cursor()
    if not callable(getattr(cur, "copy", None)):
        try:
            cur.close()
        except Exception:
            pass
        raise _OptionsCopyUnavailable("options_copy_staging_copy_api_unavailable")
    return cur


def _write_copy_rows(cur, sql: str, rows: List[Tuple[Any, ...]]) -> None:
    copy_cm = cur.copy(sql)
    with copy_cm as copy:
        for row in rows:
            copy.write_row(tuple(row))


def _write_polygon_copy_staging(cur, rows: List[Tuple[Any, ...]], *, session: str) -> int:
    if not rows:
        return 0
    cur.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS options_chain_v2_write_staging (
          staging_session TEXT NOT NULL,
          staging_ordinal BIGINT NOT NULL,
          ts_ms BIGINT NOT NULL,
          underlying TEXT NOT NULL,
          contract TEXT NOT NULL,
          expiration TEXT,
          contract_type TEXT,
          strike DOUBLE PRECISION,
          iv DOUBLE PRECISION,
          open_interest DOUBLE PRECISION,
          volume DOUBLE PRECISION,
          bid DOUBLE PRECISION,
          ask DOUBLE PRECISION,
          delta DOUBLE PRECISION,
          gamma DOUBLE PRECISION,
          theta DOUBLE PRECISION,
          vega DOUBLE PRECISION,
          source TEXT
        ) ON COMMIT DELETE ROWS
        """
    )
    copy_rows = [(str(session), int(idx), *tuple(row)) for idx, row in enumerate(rows)]
    _write_copy_rows(
        cur,
        """
        COPY options_chain_v2_write_staging (
          staging_session, staging_ordinal, ts_ms, underlying, contract,
          expiration, contract_type, strike, iv, open_interest, volume,
          bid, ask, delta, gamma, theta, vega, source
        ) FROM STDIN
        """,
        copy_rows,
    )
    cur.execute(
        """
        INSERT INTO options_chain_v2(
          ts_ms,
          underlying, contract, expiration, contract_type, strike,
          iv, open_interest, volume,
          bid, ask,
          delta, gamma, theta, vega,
          source
        )
        SELECT
          ts_ms,
          underlying, contract, expiration, contract_type, strike,
          iv, open_interest, volume,
          bid, ask,
          delta, gamma, theta, vega,
          source
        FROM (
          SELECT DISTINCT ON (contract, ts_ms)
            ts_ms,
            underlying, contract, expiration, contract_type, strike,
            iv, open_interest, volume,
            bid, ask,
            delta, gamma, theta, vega,
            source,
            staging_ordinal
          FROM options_chain_v2_write_staging
          WHERE staging_session = %s
          ORDER BY contract, ts_ms, staging_ordinal DESC
        ) staged
        ON CONFLICT(contract, ts_ms) DO UPDATE SET
          underlying=excluded.underlying,
          expiration=excluded.expiration,
          contract_type=excluded.contract_type,
          strike=excluded.strike,
          iv=excluded.iv,
          open_interest=excluded.open_interest,
          volume=excluded.volume,
          bid=excluded.bid,
          ask=excluded.ask,
          delta=excluded.delta,
          gamma=excluded.gamma,
          theta=excluded.theta,
          vega=excluded.vega,
          source=excluded.source
        """,
        (str(session),),
    )
    cur.execute("DELETE FROM options_chain_v2_write_staging WHERE staging_session = %s", (str(session),))
    return len(rows)


def _write_tradier_copy_staging(cur, rows: List[Tuple[Any, ...]], *, session: str) -> int:
    if not rows:
        return 0
    cur.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS options_chain_write_staging (
          staging_session TEXT NOT NULL,
          staging_ordinal BIGINT NOT NULL,
          ts_ms BIGINT NOT NULL,
          symbol TEXT NOT NULL,
          expiry TEXT NOT NULL,
          strike DOUBLE PRECISION NOT NULL,
          call_put TEXT NOT NULL,
          iv DOUBLE PRECISION,
          open_interest BIGINT,
          volume BIGINT,
          source TEXT
        ) ON COMMIT DELETE ROWS
        """
    )
    copy_rows = [(str(session), int(idx), *tuple(row)) for idx, row in enumerate(rows)]
    _write_copy_rows(
        cur,
        """
        COPY options_chain_write_staging (
          staging_session, staging_ordinal, ts_ms, symbol, expiry, strike,
          call_put, iv, open_interest, volume, source
        ) FROM STDIN
        """,
        copy_rows,
    )
    cur.execute(
        """
        INSERT INTO options_chain(
          ts_ms, symbol, expiry, strike, call_put,
          iv, open_interest, volume, source
        )
        SELECT
          ts_ms, symbol, expiry, strike, call_put,
          iv, open_interest, volume, source
        FROM (
          SELECT DISTINCT ON (symbol, expiry, strike, call_put, ts_ms)
            ts_ms, symbol, expiry, strike, call_put,
            iv, open_interest, volume, source,
            staging_ordinal
          FROM options_chain_write_staging
          WHERE staging_session = %s
          ORDER BY symbol, expiry, strike, call_put, ts_ms, staging_ordinal DESC
        ) staged
        ON CONFLICT(symbol, expiry, strike, call_put, ts_ms) DO UPDATE SET
          iv=excluded.iv,
          open_interest=excluded.open_interest,
          volume=excluded.volume,
          source=excluded.source
        """,
        (str(session),),
    )
    cur.execute("DELETE FROM options_chain_write_staging WHERE staging_session = %s", (str(session),))
    return len(rows)


def _write_options_bulk_rows_copy_staging(
    con,
    *,
    polygon_rows: List[Tuple[Any, ...]],
    tradier_rows: List[Tuple[Any, ...]],
) -> Dict[str, Any]:
    session = _copy_staging_session()
    cur = _raw_copy_cursor(con)
    try:
        polygon_count = _write_polygon_copy_staging(cur, list(polygon_rows or []), session=session)
        tradier_count = _write_tradier_copy_staging(cur, list(tradier_rows or []), session=session)
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return {
        "polygon_rows": int(polygon_count),
        "tradier_rows": int(tradier_count),
        "raw_rows": int(polygon_count + tradier_count),
        "write_path": "copy_staging",
    }


def _write_options_bulk_rows(
    con,
    *,
    polygon_rows: List[Tuple[Any, ...]],
    tradier_rows: List[Tuple[Any, ...]],
) -> Dict[str, Any]:
    polygon_values = list(polygon_rows or [])
    tradier_values = list(tradier_rows or [])
    fallback_reason = ""
    if (polygon_values or tradier_values) and bool(OPTIONS_POLL_COPY_STAGING_ENABLED):
        try:
            return _write_options_bulk_rows_copy_staging(
                con,
                polygon_rows=polygon_values,
                tradier_rows=tradier_values,
            )
        except _OptionsCopyUnavailable as exc:
            fallback_reason = str(exc)
        except Exception as exc:
            if not bool(OPTIONS_POLL_COPY_STAGING_FALLBACK_ENABLED):
                raise
            fallback_reason = f"{type(exc).__name__}:{exc}"
            _warn_nonfatal(
                "OPTIONS_POLL_COPY_STAGING_FALLBACK",
                exc,
                polygon_rows=len(polygon_values),
                tradier_rows=len(tradier_values),
            )
    polygon_count = _write_polygon_contract_rows(con, polygon_values)
    tradier_count = _write_tradier_value_rows(con, tradier_values)
    write_path = "executemany"
    if fallback_reason:
        write_path = "executemany_copy_fallback"
    return {
        "polygon_rows": int(polygon_count),
        "tradier_rows": int(tradier_count),
        "raw_rows": int(polygon_count + tradier_count),
        "write_path": write_path,
        "copy_fallback_reason": fallback_reason,
    }


def _write_polygon_contracts(con, contracts: List[Dict[str, Any]]) -> int:
    rows = _polygon_contract_value_rows(list(contracts or []))
    return int(_write_options_bulk_rows(con, polygon_rows=rows, tradier_rows=[]).get("raw_rows") or 0)


def _write_tradier_rows(con, symbol: str, rows: List[Dict[str, Any]], *, ts_ms: int, source: str) -> int:
    value_rows = _tradier_value_rows(symbol, list(rows or []), ts_ms=ts_ms, source=source)
    return int(_write_options_bulk_rows(con, polygon_rows=[], tradier_rows=value_rows).get("raw_rows") or 0)


def _load_cached_tradier_rows(con, symbol: str, *, now_ms: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    cutoff_ts_ms = int(now_ms - (OPTIONS_CACHE_MAX_AGE_S * 1000))
    row = con.execute(
        """
        SELECT MAX(ts_ms)
        FROM options_chain
        WHERE symbol = ?
          AND source = 'tradier'
          AND ts_ms >= ?
        """,
        (str(symbol), int(cutoff_ts_ms)),
    ).fetchone()
    snapshot_ts_ms = int(row[0]) if row and row[0] is not None else None
    if snapshot_ts_ms is None:
        return [], None
    rows = con.execute(
        """
        SELECT expiry, strike, call_put, iv, open_interest, volume
        FROM options_chain
        WHERE symbol = ?
          AND ts_ms = ?
          AND source = 'tradier'
        ORDER BY expiry ASC, strike ASC, call_put ASC
        """,
        (str(symbol), int(snapshot_ts_ms)),
    ).fetchall() or []
    return (
        [
            {
                "expiry": str(row[0]),
                "strike": float(row[1]),
                "call_put": str(row[2]),
                "iv": (_safe_float(row[3]) if row[3] is not None else None),
                "open_interest": (_safe_int(row[4]) if row[4] is not None else None),
                "volume": (_safe_int(row[5]) if row[5] is not None else None),
            }
            for row in rows
        ],
        int(snapshot_ts_ms),
    )


_OPTIONS_SNAPSHOT_EVENT_UPSERT_SQL = """
INSERT INTO events(
  ts_ms, timestamp, event_type, symbol, source, title, body, url, event_key, meta_json
)
VALUES (?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(event_key, ts_ms) DO UPDATE SET
  timestamp=excluded.timestamp,
  event_type=excluded.event_type,
  symbol=excluded.symbol,
  source=excluded.source,
  title=excluded.title,
  body=excluded.body,
  url=excluded.url,
  meta_json=excluded.meta_json
"""


def _options_snapshot_event_params(
    *,
    ts_ms: int,
    source: str,
    symbol: str,
    provider: str,
    row_count: int,
) -> Tuple[Any, ...]:
    return (
        int(ts_ms),
        int(ts_ms),
        "options_snapshot",
        str(symbol),
        str(source),
        f"{symbol} options snapshot",
        f"{int(row_count)} contracts",
        None,
        f"options:{provider}:{symbol}:{ts_ms}",
        {
            "symbol": str(symbol),
            "provider": str(provider),
            "contracts": int(row_count),
        },
    )


def _write_options_snapshot_event_rows(con, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    con.executemany(_OPTIONS_SNAPSHOT_EVENT_UPSERT_SQL, rows)
    return len(rows)


def _write_options_snapshot_event(
    con,
    *,
    ts_ms: int,
    source: str,
    symbol: str,
    provider: str,
    row_count: int,
) -> None:
    _write_options_snapshot_event_rows(
        con,
        [
            _options_snapshot_event_params(
                ts_ms=ts_ms,
                source=source,
                symbol=symbol,
                provider=provider,
                row_count=row_count,
            )
        ],
    )


class _OptionsWriteBuffer:
    def __init__(self, con, *, batch_symbols: int) -> None:
        self._con = con
        self._batch_symbols = max(0, int(batch_symbols))
        self._pending_symbols = 0
        self._polygon_rows: List[Tuple[Any, ...]] = []
        self._tradier_rows: List[Tuple[Any, ...]] = []
        self._event_rows: List[Tuple[Any, ...]] = []
        self._state_rows: Dict[str, Tuple[Any, ...]] = {}
        self._durable_spool = _new_options_durable_spool() if bool(OPTIONS_POLL_DURABLE_BUFFER_ENABLED) else None
        self._last_spool_stats: Dict[str, Any] = {}
        self.commit_batches = 0
        self.committed_symbols = 0
        self.max_symbols_per_commit = 0
        self.polygon_rows_written = 0
        self.tradier_rows_written = 0
        self.event_rows_written = 0
        self.state_rows_written = 0
        self.raw_rows_written = 0
        self.bulk_write_failures = 0
        self.event_write_failures = 0
        self.state_write_failures = 0
        self.copy_fallbacks = 0
        self.write_path_counts: Dict[str, int] = {}
        self.last_write_path = ""
        self.durable_spooled_rows = 0
        self.durable_replayed_rows = 0
        self.durable_deleted_rows = 0
        self.durable_dropped_rows = 0
        self.durable_rejected_rows = 0
        self.durable_enqueue_failures = 0
        self.durable_replay_failures = 0
        self.durable_delete_failures = 0
        self.durable_corrupt_batches = 0
        self.durable_corrupt_payload_rows = 0
        self.durable_unavailable_count = 0
        self.durable_backpressure_active = False
        self.durable_backpressure_events = 0
        self.durable_backpressure_recoveries = 0
        self.durable_last_error = ""

    def add_polygon_contracts(self, contracts: List[Dict[str, Any]]) -> int:
        rows = _polygon_contract_value_rows(list(contracts or []))
        self._polygon_rows.extend(rows)
        return len(rows)

    def add_tradier_rows(self, symbol: str, rows: List[Dict[str, Any]], *, ts_ms: int, source: str) -> int:
        value_rows = _tradier_value_rows(symbol, list(rows or []), ts_ms=ts_ms, source=source)
        self._tradier_rows.extend(value_rows)
        return len(value_rows)

    def stage_snapshot_event(
        self,
        *,
        ts_ms: int,
        source: str,
        symbol: str,
        provider: str,
        row_count: int,
    ) -> None:
        self._event_rows.append(
            _options_snapshot_event_params(
                ts_ms=ts_ms,
                source=source,
                symbol=symbol,
                provider=provider,
                row_count=row_count,
            )
        )

    def stage_symbol_success(
        self,
        symbol: str,
        *,
        provider: str,
        now_ms: int,
        snapshot_ts_ms: int,
        row_count: int,
        state_cache: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        prev = _cached_symbol_state(state_cache, symbol) or _default_symbol_state(symbol)
        state = _build_symbol_success_state(
            prev,
            symbol,
            provider=provider,
            now_ms=now_ms,
            snapshot_ts_ms=snapshot_ts_ms,
            row_count=row_count,
        )
        _update_cached_symbol_state(state_cache, state)
        self._state_rows[_normal_symbol(symbol)] = _symbol_state_params(state)
        return state

    def stage_symbol_failure(
        self,
        symbol: str,
        *,
        provider: str,
        error: str,
        now_ms: int,
        fallback_snapshot_ts_ms: Optional[int] = None,
        row_count: int = 0,
        state_cache: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        prev = _cached_symbol_state(state_cache, symbol) or _default_symbol_state(symbol)
        state = _build_symbol_failure_state(
            prev,
            symbol,
            provider=provider,
            error=error,
            now_ms=now_ms,
            fallback_snapshot_ts_ms=fallback_snapshot_ts_ms,
            row_count=row_count,
        )
        _update_cached_symbol_state(state_cache, state)
        self._state_rows[_normal_symbol(symbol)] = _symbol_state_params(state)
        return state

    def stage_symbol_cache_use(
        self,
        symbol: str,
        *,
        provider: str,
        now_ms: int,
        snapshot_ts_ms: int,
        row_count: int,
        state_cache: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        prev = _cached_symbol_state(state_cache, symbol) or _default_symbol_state(symbol)
        state = _build_symbol_cache_use_state(
            prev,
            symbol,
            provider=provider,
            now_ms=now_ms,
            snapshot_ts_ms=snapshot_ts_ms,
            row_count=row_count,
        )
        _update_cached_symbol_state(state_cache, state)
        self._state_rows[_normal_symbol(symbol)] = _symbol_state_params(state)
        return state

    def _batch_row_count(
        self,
        polygon_rows: List[Tuple[Any, ...]],
        tradier_rows: List[Tuple[Any, ...]],
        event_rows: List[Tuple[Any, ...]],
        state_rows: List[Tuple[Any, ...]],
    ) -> int:
        return int(len(polygon_rows) + len(tradier_rows) + len(event_rows) + len(state_rows))

    def _set_durable_backpressure(self, active: bool, reason: str = "") -> None:
        if active:
            if not bool(self.durable_backpressure_active):
                self.durable_backpressure_events += 1
            self.durable_backpressure_active = True
            if reason:
                self.durable_last_error = str(reason)
            return
        if bool(self.durable_backpressure_active):
            self.durable_backpressure_recoveries += 1
        self.durable_backpressure_active = False
        if reason:
            self.durable_last_error = str(reason)

    def _refresh_durable_spool_stats(self) -> Dict[str, Any]:
        if self._durable_spool is None:
            self._last_spool_stats = {}
            return {}
        try:
            stats = self._durable_spool.stats(table=_OPTIONS_DURABLE_SPOOL_TABLE)
        except NonPriceIngestionSpoolUnavailableError as exc:
            self.durable_unavailable_count += 1
            self.durable_last_error = f"{type(exc).__name__}:{exc}"
            self._set_durable_backpressure(True, self.durable_last_error)
            _warn_nonfatal(
                "OPTIONS_POLL_DURABLE_BUFFER_STATS_FAILED",
                exc,
                pending_symbols=int(self._pending_symbols),
            )
            return dict(self._last_spool_stats)
        self._last_spool_stats = dict(stats)
        fill_ratio = max(float(stats.get("rows_fill_ratio") or 0.0), float(stats.get("bytes_fill_ratio") or 0.0))
        if fill_ratio >= 1.0:
            self._set_durable_backpressure(True, "durable_spool_full")
        return dict(stats)

    def _recover_durable_backpressure_if_room(self) -> None:
        stats = dict(self._last_spool_stats)
        fill_ratio = max(float(stats.get("rows_fill_ratio") or 0.0), float(stats.get("bytes_fill_ratio") or 0.0))
        if bool(self.durable_backpressure_active) and fill_ratio < 0.80:
            self._set_durable_backpressure(False, "durable_spool_recovered")

    def _encode_durable_rows(
        self,
        *,
        polygon_rows: List[Tuple[Any, ...]],
        tradier_rows: List[Tuple[Any, ...]],
        event_rows: List[Tuple[Any, ...]],
        state_rows: List[Tuple[Any, ...]],
    ) -> List[Tuple[Any, ...]]:
        encoded: List[Tuple[Any, ...]] = []
        encoded.extend((_OPTIONS_SPOOL_KIND_POLYGON, *tuple(row)) for row in polygon_rows)
        encoded.extend((_OPTIONS_SPOOL_KIND_TRADIER, *tuple(row)) for row in tradier_rows)
        encoded.extend((_OPTIONS_SPOOL_KIND_EVENT, *tuple(row)) for row in event_rows)
        encoded.extend((_OPTIONS_SPOOL_KIND_STATE, *tuple(row)) for row in state_rows)
        return encoded

    def _enqueue_durable_batch(
        self,
        *,
        polygon_rows: List[Tuple[Any, ...]],
        tradier_rows: List[Tuple[Any, ...]],
        event_rows: List[Tuple[Any, ...]],
        state_rows: List[Tuple[Any, ...]],
    ) -> Tuple[int, int]:
        if self._durable_spool is None:
            return 0, 0
        encoded_rows = self._encode_durable_rows(
            polygon_rows=polygon_rows,
            tradier_rows=tradier_rows,
            event_rows=event_rows,
            state_rows=state_rows,
        )
        row_count = int(len(encoded_rows))
        if row_count <= 0:
            self._refresh_durable_spool_stats()
            return 0, 0
        try:
            stats = self._durable_spool.enqueue(
                table=_OPTIONS_DURABLE_SPOOL_TABLE,
                rows=encoded_rows,
                created_ts_ms=int(time.time() * 1000),
            )
        except (NonPriceIngestionSpoolFullError, NonPriceIngestionSpoolUnavailableError) as exc:
            self.durable_enqueue_failures += 1
            self.durable_rejected_rows += int(row_count)
            self.durable_last_error = f"{type(exc).__name__}:{exc}"
            if isinstance(exc, NonPriceIngestionSpoolUnavailableError):
                self.durable_unavailable_count += 1
            self._set_durable_backpressure(True, self.durable_last_error)
            _warn_nonfatal(
                "OPTIONS_POLL_DURABLE_BUFFER_ENQUEUE_FAILED",
                exc,
                rows=int(row_count),
                pending_symbols=int(self._pending_symbols),
            )
            raise
        self._last_spool_stats = dict(stats)
        self.durable_spooled_rows += int(row_count)
        if bool(self.durable_backpressure_active):
            self._refresh_durable_spool_stats()
            self._recover_durable_backpressure_if_room()
        return int(stats.get("inserted_id") or 0), int(row_count)

    def _delete_durable_records(self, ids: List[int], *, row_count: int) -> int:
        if self._durable_spool is None or not ids:
            return 0
        try:
            deleted = int(self._durable_spool.delete(ids))
        except NonPriceIngestionSpoolUnavailableError as exc:
            self.durable_delete_failures += 1
            self.durable_last_error = f"{type(exc).__name__}:{exc}"
            self._set_durable_backpressure(True, self.durable_last_error)
            _warn_nonfatal(
                "OPTIONS_POLL_DURABLE_BUFFER_DELETE_FAILED",
                exc,
                rows=int(row_count),
                spool_ids=list(ids),
            )
            return 0
        self.durable_deleted_rows += int(row_count)
        self._refresh_durable_spool_stats()
        self._recover_durable_backpressure_if_room()
        return int(deleted)

    def _decode_durable_rows(
        self,
        rows: List[Tuple[Any, ...]],
    ) -> Tuple[List[Tuple[Any, ...]], List[Tuple[Any, ...]], List[Tuple[Any, ...]], List[Tuple[Any, ...]], int]:
        polygon_rows: List[Tuple[Any, ...]] = []
        tradier_rows: List[Tuple[Any, ...]] = []
        event_rows: List[Tuple[Any, ...]] = []
        state_rows: List[Tuple[Any, ...]] = []
        unknown_rows = 0
        for row in rows:
            if not row:
                unknown_rows += 1
                continue
            kind = str(row[0] or "")
            values = tuple(row[1:])
            if kind == _OPTIONS_SPOOL_KIND_POLYGON:
                polygon_rows.append(values)
            elif kind == _OPTIONS_SPOOL_KIND_TRADIER:
                tradier_rows.append(values)
            elif kind == _OPTIONS_SPOOL_KIND_EVENT:
                event_rows.append(values)
            elif kind == _OPTIONS_SPOOL_KIND_STATE:
                state_rows.append(values)
            else:
                unknown_rows += 1
        return polygon_rows, tradier_rows, event_rows, state_rows, int(unknown_rows)

    def _write_decoded_durable_batch(
        self,
        *,
        polygon_rows: List[Tuple[Any, ...]],
        tradier_rows: List[Tuple[Any, ...]],
        event_rows: List[Tuple[Any, ...]],
        state_rows: List[Tuple[Any, ...]],
    ) -> Dict[str, Any]:
        counts = {"polygon_rows": 0, "tradier_rows": 0, "raw_rows": 0, "write_path": "none"}
        if polygon_rows or tradier_rows:
            counts = _write_options_bulk_rows(
                self._con,
                polygon_rows=polygon_rows,
                tradier_rows=tradier_rows,
            )
        if event_rows:
            _write_options_snapshot_event_rows(self._con, event_rows)
        if state_rows:
            _write_symbol_state_rows(self._con, state_rows)
        self._con.commit()
        return counts

    def replay_spooled(self, *, max_rows: Optional[int] = None) -> None:
        if self._durable_spool is None:
            return
        remaining = max(1, int(max_rows or OPTIONS_POLL_DURABLE_REPLAY_MAX_ROWS))
        while remaining > 0:
            try:
                records, corrupt = self._durable_spool.select_batch(
                    limit_rows=int(remaining),
                    tables=[_OPTIONS_DURABLE_SPOOL_TABLE],
                )
            except NonPriceIngestionSpoolUnavailableError as exc:
                self.durable_unavailable_count += 1
                self.durable_replay_failures += 1
                self.durable_last_error = f"{type(exc).__name__}:{exc}"
                self._set_durable_backpressure(True, self.durable_last_error)
                _warn_nonfatal(
                    "OPTIONS_POLL_DURABLE_BUFFER_SELECT_FAILED",
                    exc,
                    remaining_rows=int(remaining),
                )
                return
            if corrupt:
                corrupt_ids = [int(record.id) for record in corrupt]
                corrupt_rows = int(sum(int(record.total_rows) for record in corrupt))
                self.durable_corrupt_batches += int(len(corrupt))
                self.durable_corrupt_payload_rows += int(corrupt_rows)
                self.durable_dropped_rows += int(corrupt_rows)
                self._delete_durable_records(corrupt_ids, row_count=corrupt_rows)
                _warn_nonfatal(
                    "OPTIONS_POLL_DURABLE_BUFFER_CORRUPT_ROWS_DROPPED",
                    RuntimeError("options_poll_durable_payload_corrupt"),
                    batches=int(len(corrupt)),
                    rows=int(corrupt_rows),
                )
            if not records:
                self._refresh_durable_spool_stats()
                return
            ids = [int(record.id) for record in records]
            rows = [tuple(row) for record in records for row in record.rows]
            row_count = int(len(rows))
            polygon_rows, tradier_rows, event_rows, state_rows, unknown_rows = self._decode_durable_rows(rows)
            try:
                counts = self._write_decoded_durable_batch(
                    polygon_rows=polygon_rows,
                    tradier_rows=tradier_rows,
                    event_rows=event_rows,
                    state_rows=state_rows,
                )
            except Exception as exc:
                self.durable_replay_failures += 1
                if polygon_rows or tradier_rows:
                    self.bulk_write_failures += 1
                if event_rows:
                    self.event_write_failures += 1
                if state_rows:
                    self.state_write_failures += 1
                _rollback_if_active(self._con)
                self.durable_last_error = f"{type(exc).__name__}:{exc}"
                self._set_durable_backpressure(True, self.durable_last_error)
                _warn_nonfatal(
                    "OPTIONS_POLL_DURABLE_BUFFER_REPLAY_FAILED",
                    exc,
                    rows=int(row_count),
                    spool_ids=list(ids),
                )
                return
            self.commit_batches += 1
            self.polygon_rows_written += int(counts.get("polygon_rows") or 0)
            self.tradier_rows_written += int(counts.get("tradier_rows") or 0)
            self.event_rows_written += int(len(event_rows))
            self.state_rows_written += int(len(state_rows))
            self.raw_rows_written += int(counts.get("raw_rows") or 0)
            if unknown_rows:
                self.durable_dropped_rows += int(unknown_rows)
            write_path = str(counts.get("write_path") or "none")
            self.last_write_path = write_path
            self.write_path_counts[write_path] = int(self.write_path_counts.get(write_path) or 0) + 1
            self.durable_replayed_rows += int(row_count)
            self._delete_durable_records(ids, row_count=row_count)
            remaining -= max(1, int(row_count))

    def mark_symbol(self) -> None:
        self._pending_symbols += 1
        if self._batch_symbols > 0 and self._pending_symbols >= self._batch_symbols:
            self.flush()

    def flush(self) -> None:
        if (
            self._pending_symbols <= 0
            and not self._polygon_rows
            and not self._tradier_rows
            and not self._event_rows
            and not self._state_rows
        ):
            return
        polygon_rows = list(self._polygon_rows)
        tradier_rows = list(self._tradier_rows)
        event_rows = list(self._event_rows)
        state_rows = list(self._state_rows.values())
        pending_symbols = int(self._pending_symbols)
        counts = {"polygon_rows": 0, "tradier_rows": 0, "raw_rows": 0}
        spool_id = 0
        spooled_row_count = 0
        spool_id, spooled_row_count = self._enqueue_durable_batch(
            polygon_rows=polygon_rows,
            tradier_rows=tradier_rows,
            event_rows=event_rows,
            state_rows=state_rows,
        )
        try:
            if polygon_rows or tradier_rows:
                counts = _write_options_bulk_rows(
                    self._con,
                    polygon_rows=polygon_rows,
                    tradier_rows=tradier_rows,
                )
            if event_rows:
                _write_options_snapshot_event_rows(self._con, event_rows)
            if state_rows:
                _write_symbol_state_rows(self._con, state_rows)
            self._con.commit()
        except Exception as exc:
            if polygon_rows or tradier_rows:
                self.bulk_write_failures += 1
            if event_rows:
                self.event_write_failures += 1
            if state_rows:
                self.state_write_failures += 1
            _rollback_if_active(self._con)
            if state_rows and not polygon_rows and not tradier_rows and not event_rows and dbapi.is_transient_write_error(exc):
                _warn_nonfatal(
                    "OPTIONS_POLL_SYMBOL_STATE_BATCH_RECORD_FAILED",
                    exc,
                    committed_batches=int(self.commit_batches),
                    pending_symbols=int(pending_symbols),
                    state_rows=len(state_rows),
                )
                self._state_rows.clear()
                self._pending_symbols = 0
                return
            _warn_nonfatal(
                "OPTIONS_POLL_BULK_WRITE_FAILED",
                exc,
                committed_batches=int(self.commit_batches),
                pending_symbols=int(pending_symbols),
                polygon_rows=len(polygon_rows),
                tradier_rows=len(tradier_rows),
                event_rows=len(event_rows),
                state_rows=len(state_rows),
            )
            raise
        if spool_id:
            self._delete_durable_records([int(spool_id)], row_count=int(spooled_row_count))
        self.commit_batches += 1
        self.committed_symbols += int(pending_symbols)
        self.max_symbols_per_commit = max(int(self.max_symbols_per_commit), int(pending_symbols))
        self.polygon_rows_written += int(counts.get("polygon_rows") or 0)
        self.tradier_rows_written += int(counts.get("tradier_rows") or 0)
        self.event_rows_written += int(len(event_rows))
        self.state_rows_written += int(len(state_rows))
        self.raw_rows_written += int(counts.get("raw_rows") or 0)
        write_path = str(counts.get("write_path") or ("none" if not polygon_rows and not tradier_rows else "executemany"))
        self.last_write_path = write_path
        self.write_path_counts[write_path] = int(self.write_path_counts.get(write_path) or 0) + 1
        if write_path == "executemany_copy_fallback":
            self.copy_fallbacks += 1
        self._polygon_rows.clear()
        self._tradier_rows.clear()
        self._event_rows.clear()
        self._state_rows.clear()
        self._pending_symbols = 0

    def snapshot(self) -> Dict[str, Any]:
        spool_stats = self._refresh_durable_spool_stats()
        return {
            "commit_batches": int(self.commit_batches),
            "committed_symbols": int(self.committed_symbols),
            "max_symbols_per_commit": int(self.max_symbols_per_commit),
            "polygon_rows_written": int(self.polygon_rows_written),
            "tradier_rows_written": int(self.tradier_rows_written),
            "event_rows_written": int(self.event_rows_written),
            "state_rows_written": int(self.state_rows_written),
            "rows_written": int(self.raw_rows_written),
            "bulk_write_failures": int(self.bulk_write_failures),
            "event_write_failures": int(self.event_write_failures),
            "state_write_failures": int(self.state_write_failures),
            "copy_fallbacks": int(self.copy_fallbacks),
            "copy_staging_batches": int(self.write_path_counts.get("copy_staging") or 0),
            "executemany_batches": int(
                int(self.write_path_counts.get("executemany") or 0)
                + int(self.write_path_counts.get("executemany_copy_fallback") or 0)
            ),
            "state_only_batches": int(self.write_path_counts.get("none") or 0),
            "last_write_path": str(self.last_write_path),
            "write_paths": dict(self.write_path_counts),
            "pending_symbols": int(self._pending_symbols),
            "pending_polygon_rows": int(len(self._polygon_rows)),
            "pending_tradier_rows": int(len(self._tradier_rows)),
            "pending_event_rows": int(len(self._event_rows)),
            "pending_state_rows": int(len(self._state_rows)),
            "durable_buffer_enabled": bool(self._durable_spool is not None),
            "durable_buffer_path": str(spool_stats.get("path") or _options_durable_spool_path()),
            "durable_buffer_pending_batches": int(spool_stats.get("pending_batches") or 0),
            "durable_buffer_pending_rows": int(spool_stats.get("pending_rows") or 0),
            "durable_buffer_pending_bytes": int(spool_stats.get("pending_bytes") or 0),
            "durable_buffer_file_bytes": int(spool_stats.get("file_bytes") or 0),
            "durable_buffer_max_rows": int(spool_stats.get("max_rows") or OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS),
            "durable_buffer_max_bytes": int(spool_stats.get("max_bytes") or OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES),
            "durable_buffer_rows_fill_ratio": float(spool_stats.get("rows_fill_ratio") or 0.0),
            "durable_buffer_bytes_fill_ratio": float(spool_stats.get("bytes_fill_ratio") or 0.0),
            "durable_buffer_oldest_age_ms": int(spool_stats.get("oldest_age_ms") or 0),
            "durable_buffer_oldest_created_ts_ms": int(spool_stats.get("oldest_created_ts_ms") or 0),
            "durable_buffer_spooled_rows": int(self.durable_spooled_rows),
            "durable_buffer_replayed_rows": int(self.durable_replayed_rows),
            "durable_buffer_deleted_rows": int(self.durable_deleted_rows),
            "durable_buffer_dropped_rows": int(self.durable_dropped_rows),
            "durable_buffer_rejected_rows": int(self.durable_rejected_rows),
            "durable_buffer_enqueue_failures": int(self.durable_enqueue_failures),
            "durable_buffer_replay_failures": int(self.durable_replay_failures),
            "durable_buffer_delete_failures": int(self.durable_delete_failures),
            "durable_buffer_unavailable_count": int(self.durable_unavailable_count),
            "durable_buffer_corrupt_batches": int(self.durable_corrupt_batches),
            "durable_buffer_corrupt_payload_rows": int(self.durable_corrupt_payload_rows),
            "durable_buffer_corruption_events": int(spool_stats.get("corruption_events") or 0),
            "durable_buffer_backpressure_active": bool(self.durable_backpressure_active),
            "durable_buffer_backpressure_events": int(self.durable_backpressure_events),
            "durable_buffer_backpressure_recoveries": int(self.durable_backpressure_recoveries),
            "durable_buffer_last_error": str(self.durable_last_error),
            "durable_buffer_synchronous": str(spool_stats.get("synchronous") or OPTIONS_POLL_DURABLE_BUFFER_SYNCHRONOUS),
        }


def _fetch_provider_symbol(provider: str, symbol: str, ts_ms: int) -> Dict[str, Any]:
    provider = _provider_name(provider)
    symbol = _normal_symbol(symbol)
    result: Dict[str, Any]
    try:
        if provider == "polygon":
            contracts, err = fetch_options_chain_snapshot(symbol, limit=250, max_pages=4)
            if err:
                return {
                    "ok": False,
                    "provider": provider,
                    "symbol": symbol,
                    "error": err,
                    "error_text": str(err),
                    "kind": "provider_error",
                }
            contracts = _validate_polygon_contracts(contracts or [], symbol, ts_ms)
            if not contracts:
                return {
                    "ok": False,
                    "provider": provider,
                    "symbol": symbol,
                    "error": "polygon_empty_chain",
                    "error_text": "polygon_empty_chain",
                    "kind": "empty",
                }
            return {
                "ok": True,
                "provider": provider,
                "symbol": symbol,
                "contracts": contracts,
                "row_count": int(len(contracts)),
                "snapshot_ts_ms": int(ts_ms),
            }

        if provider == "tradier":
            result = fetch_options_chain(symbol)
            rows = _validate_tradier_rows(list((result or {}).get("rows") or []), symbol)
            if not rows:
                return {
                    "ok": False,
                    "provider": provider,
                    "symbol": symbol,
                    "error": "tradier_empty_chain",
                    "error_text": "tradier_empty_chain",
                    "kind": "empty",
                }
            return {
                "ok": True,
                "provider": provider,
                "symbol": symbol,
                "rows": rows,
                "row_count": int(len(rows)),
                "snapshot_ts_ms": int(ts_ms),
            }

        return {
            "ok": False,
            "provider": provider,
            "symbol": symbol,
            "error": f"unsupported_provider:{provider}",
            "error_text": f"unsupported_provider:{provider}",
            "kind": "provider_error",
        }
    except TradierFetchError as exc:
        result = {
            "ok": False,
            "provider": provider,
            "symbol": symbol,
            "exception": exc,
            "error_text": f"{exc.kind}:{exc}",
            "kind": "tradier_exception",
        }
    except Exception as exc:
        result = {
            "ok": False,
            "provider": provider,
            "symbol": symbol,
            "exception": exc,
            "error_text": f"{type(exc).__name__}:{exc}",
            "kind": "exception",
        }
    return result


def _fetch_provider_symbols_parallel(provider: str, symbols: List[str], ts_ms: int) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    max_workers = _provider_fetch_max_workers(symbols)
    if max_workers <= 1:
        return {symbol: _fetch_provider_symbol(provider, symbol, ts_ms) for symbol in symbols}
    results: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"options-{provider}") as executor:
        future_to_symbol = {
            executor.submit(_fetch_provider_symbol, provider, symbol, ts_ms): symbol
            for symbol in symbols
        }
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                results[symbol] = future.result()
            except Exception as exc:
                results[symbol] = {
                    "ok": False,
                    "provider": provider,
                    "symbol": symbol,
                    "exception": exc,
                    "error_text": f"{type(exc).__name__}:{exc}",
                    "kind": "exception",
                }
    return results


def _provider_fetch_max_workers(symbols: List[str]) -> int:
    if not symbols:
        return 0
    return min(int(OPTIONS_POLL_FETCH_CONCURRENCY), len(symbols))


def _format_symbol_error(provider_errors: Dict[str, str]) -> str:
    parts = [
        f"{provider}:{_clip_error(error)}"
        for provider, error in provider_errors.items()
        if str(error or "").strip()
    ]
    return _clip_error("; ".join(parts))


def _critical_symbols_for_run(symbols: List[str]) -> List[str]:
    universe = {str(symbol or "").upper().strip() for symbol in symbols if str(symbol or "").strip()}
    return sorted(symbol for symbol in OPTIONS_CRITICAL_SYMBOLS if symbol in universe)


def _build_run_meta(
    symbols: List[str],
    symbol_results: Dict[str, Dict[str, Any]],
    providers: List[str],
    last_ingested_ts_ms: Optional[int],
) -> Dict[str, Any]:
    fresh_symbols = sorted(symbol for symbol, row in symbol_results.items() if str((row or {}).get("status") or "") == "fresh")
    cached_symbols = sorted(
        symbol
        for symbol, row in symbol_results.items()
        if str((row or {}).get("status") or "") in {"cached", "disabled_cached"}
    )
    failed_symbols = sorted(symbol for symbol, row in symbol_results.items() if str((row or {}).get("status") or "") == "failed")
    disabled_symbols = sorted(
        symbol
        for symbol, row in symbol_results.items()
        if str((row or {}).get("status") or "") in {"disabled", "disabled_cached"}
    )
    critical_symbols = _critical_symbols_for_run(symbols)
    critical_unavailable_symbols = sorted(
        symbol
        for symbol in critical_symbols
        if symbol not in fresh_symbols and symbol not in cached_symbols
    )
    symbol_status = {}
    for symbol, row in sorted(symbol_results.items()):
        row_dict = dict(row) if isinstance(row, dict) else {}
        snapshot_ts_ms = row_dict.get("snapshot_ts_ms")
        disabled_until_ts_ms = row_dict.get("disabled_until_ts_ms")
        symbol_status[str(symbol)] = {
            "status": str(row_dict.get("status") or ""),
            "provider": str(row_dict.get("provider") or ""),
            "rows": int(row_dict.get("rows") or 0),
            "snapshot_ts_ms": (int(snapshot_ts_ms) if snapshot_ts_ms is not None else None),
            "disabled_until_ts_ms": (int(disabled_until_ts_ms) if disabled_until_ts_ms is not None else None),
            "error": _clip_error(row_dict.get("error")),
        }
    return {
        "providers": list(providers),
        "symbols_n": int(len(symbols)),
        "symbols_attempted": int(len(symbols)),
        "symbols_succeeded": int(len(fresh_symbols)),
        "symbols_failed": int(len(failed_symbols)),
        "fresh_symbols": fresh_symbols,
        "cached_symbols": cached_symbols,
        "failed_symbols": failed_symbols,
        "disabled_symbols": disabled_symbols,
        "critical_symbols": critical_symbols,
        "critical_unavailable_symbols": critical_unavailable_symbols,
        "fresh_symbol_count": int(len(fresh_symbols)),
        "cached_symbol_count": int(len(cached_symbols)),
        "failed_symbol_count": int(len(failed_symbols)),
        "disabled_symbol_count": int(len(disabled_symbols)),
        "critical": bool(critical_unavailable_symbols),
        "degraded": bool(critical_unavailable_symbols),
        "last_available_snapshot_ts_ms": (int(last_ingested_ts_ms) if last_ingested_ts_ms is not None else None),
        "symbol_status": symbol_status,
    }


def _run_once(providers: List[str]) -> Dict[str, Any]:
    ts_ms = int(time.time() * 1000)
    now_s = float(ts_ms) / 1000.0
    providers = [_provider_name(provider) for provider in providers if _provider_name(provider)]
    _refresh_provider_cooldowns(providers, now_s=now_s)

    con = connect(readonly=True)
    try:
        symbols = _load_active_symbols_for_shard(con)
    finally:
        con.close()

    conw = connect()
    provider_status = {
        str(provider): {
            "ok": False,
            "error": None,
            "rows": 0,
            "fresh_symbols": 0,
            "cached_symbols": 0,
            "failed_symbols": 0,
            "skipped_symbols": 0,
            "cooldown_until_ts_ms": (
                int(float(provider_cooldowns.get(str(provider)) or 0.0) * 1000.0)
                if _provider_cooldown_remaining_s(str(provider), now_s) > 0.0
                else 0
            ),
            "cooldown_remaining_s": float(_provider_cooldown_remaining_s(str(provider), now_s)),
            "consecutive_rate_limits": int(provider_rate_limit_counts.get(str(provider)) or 0),
        }
        for provider in providers
    }
    if INGESTION_SHARD.enabled and not symbols:
        run_meta = _build_run_meta([], {}, providers, None)
        run_meta["provider_cooldowns"] = _provider_cooldown_snapshot(providers, now_s=now_s)
        run_meta["shard"] = INGESTION_SHARD.as_dict()
        run_meta["shard_empty"] = True
        for provider_row in provider_status.values():
            provider_row["ok"] = True
        conw.close()
        return {
            "symbols": [],
            "provider_status": provider_status,
            "event_rows": 0,
            "raw_rows": 0,
            "last_ingested_ts_ms": None,
            "pipeline_ok": True,
            "last_error": None,
            "meta": run_meta,
        }
    symbol_results: Dict[str, Dict[str, Any]] = {}
    event_rows = 0
    raw_rows = 0
    last_ingested_ts_ms: Optional[int] = None
    provider_disabled_errors: Dict[str, str] = {}
    symbol_states: Dict[str, Dict[str, Any]] = {}
    state_load_queries = int(math.ceil(len(symbols) / float(OPTIONS_SYMBOL_STATE_LOAD_CHUNK_SIZE))) if symbols else 0
    provider_fetch_batches = 0
    provider_fetch_symbols = 0
    provider_fetch_max_workers = 0
    provider_fetch_failures = 0
    cached_fallback_symbols = 0
    symbol_context: Dict[str, Dict[str, Any]] = {
        symbol: {"provider_errors": {}}
        for symbol in symbols
    }
    completed_symbols: set[str] = set()
    write_buffer = _OptionsWriteBuffer(conw, batch_symbols=OPTIONS_POLL_COMMIT_BATCH_SYMBOLS)
    write_buffer.replay_spooled(max_rows=OPTIONS_POLL_DURABLE_REPLAY_MAX_ROWS)

    def _symbol_provider_errors(symbol: str) -> Dict[str, str]:
        context = symbol_context.setdefault(symbol, {"provider_errors": {}})
        errors = context.setdefault("provider_errors", {})
        return errors  # type: ignore[return-value]

    def _mark_provider_error(symbol: str, provider: str, error_text: str, *, skipped: bool = False) -> None:
        provider_errors = _symbol_provider_errors(symbol)
        provider_errors[provider] = str(error_text)
        status = provider_status[provider]
        status["error"] = _clip_error(error_text)
        status["failed_symbols"] += 1
        if skipped:
            status["skipped_symbols"] += 1

    def _mark_provider_disabled(symbol: str, provider: str) -> None:
        error_text = str(provider_disabled_errors.get(provider) or "provider_disabled")
        _mark_provider_error(symbol, provider, error_text)

    def _mark_provider_cooldown_skip(symbol: str, provider: str) -> None:
        cooldown_remaining_s = _provider_cooldown_remaining_s(provider, now_s)
        error_text = _provider_cooldown_error(provider, now_s=now_s)
        _mark_provider_error(symbol, provider, error_text, skipped=True)
        status = provider_status[provider]
        status["cooldown_until_ts_ms"] = int(float(provider_cooldowns.get(provider) or 0.0) * 1000.0)
        status["cooldown_remaining_s"] = float(cooldown_remaining_s)
        status["consecutive_rate_limits"] = int(provider_rate_limit_counts.get(provider) or 0)

    def _provider_blocked(provider: str) -> bool:
        return provider in provider_disabled_errors or _provider_cooldown_remaining_s(provider, now_s) > 0.0

    def _apply_fetch_result(symbol: str, provider: str, result: Dict[str, Any]) -> None:
        nonlocal event_rows, last_ingested_ts_ms, raw_rows, provider_fetch_failures

        provider_errors = _symbol_provider_errors(symbol)
        if bool(result.get("ok")):
            if provider == "polygon":
                written = write_buffer.add_polygon_contracts(list(result.get("contracts") or []))
                event_source = "options_polygon"
            else:
                rows = list(result.get("rows") or [])
                written = write_buffer.add_tradier_rows(symbol, rows, ts_ms=ts_ms, source="tradier")
                event_source = "options_tradier"

            raw_rows += int(written)
            provider_status[provider]["ok"] = True
            provider_status[provider]["error"] = None
            provider_status[provider]["rows"] += int(written)
            provider_status[provider]["fresh_symbols"] += 1
            provider_rate_limit_counts[provider] = 0
            _clear_provider_cooldown(provider, now_s=now_s)
            last_ingested_ts_ms = max(int(last_ingested_ts_ms or 0), int(ts_ms))
            write_buffer.stage_symbol_success(
                symbol,
                provider=provider,
                now_ms=ts_ms,
                snapshot_ts_ms=ts_ms,
                row_count=int(written),
                state_cache=symbol_states,
            )
            write_buffer.stage_snapshot_event(
                ts_ms=ts_ms,
                source=event_source,
                symbol=symbol,
                provider=provider,
                row_count=int(written),
            )
            event_rows += 1
            symbol_results[symbol] = {
                "status": "fresh",
                "provider": provider,
                "rows": int(written),
                "snapshot_ts_ms": int(ts_ms),
                "disabled_until_ts_ms": 0,
                "error": "",
            }
            completed_symbols.add(symbol)
            write_buffer.mark_symbol()
            logging.info("options fetch ok provider=%s symbol=%s rows=%s", provider, symbol, written)
            return

        provider_fetch_failures += 1
        error_obj = result.get("exception") or result.get("error") or result.get("error_text")
        error_text = str(result.get("error_text") or error_obj or "options_fetch_failed")
        if _provider_error_is_rate_limited(error_obj):
            _record_provider_rate_limit(
                provider_status,
                provider_errors,
                provider,
                error_obj,
                now_s=now_s,
                symbol=symbol,
            )
            return

        _mark_provider_error(symbol, provider, error_text)
        if _provider_error_disables_run(error_obj):
            provider_disabled_errors[provider] = error_text
            _warn_state(
                "OPTIONS_POLL_PROVIDER_DISABLED",
                "Options provider disabled for the remainder of the poll due to configuration error.",
                provider=provider,
                error=error_text,
            )

        kind = str(result.get("kind") or "")
        exc = result.get("exception")
        if provider == "polygon" and kind == "provider_error":
            _warn_state(
                "OPTIONS_POLL_POLYGON_PROVIDER_ERROR",
                "Polygon options provider returned an error.",
                symbol=symbol,
                error=error_text,
            )
        elif provider == "tradier" and kind == "empty":
            _warn_state(
                "OPTIONS_POLL_TRADIER_EMPTY_CHAIN",
                "Tradier returned an empty options chain.",
                symbol=symbol,
            )
        if isinstance(exc, TradierFetchError):
            _warn_nonfatal(
                "OPTIONS_POLL_TRADIER_PROVIDER_ERROR",
                exc,
                once_key=f"tradier_provider_error:{symbol}",
                symbol=symbol,
                error_text=error_text,
            )
        elif isinstance(exc, Exception):
            _warn_nonfatal(
                "OPTIONS_POLL_PROVIDER_ERROR",
                exc,
                once_key=f"provider_error:{provider}:{symbol}",
                provider=provider,
                symbol=symbol,
                error_text=error_text,
            )

    try:
        logging.info("options poll start providers=%s symbols=%s", providers, len(symbols))
        symbol_states = _load_symbol_states(conw, symbols)

        for symbol in symbols:
            symbol_state = dict(symbol_states.get(symbol) or _default_symbol_state(symbol))
            if int(symbol_state.get("disabled_until_ts_ms") or 0) > ts_ms:
                cached_rows, cached_snapshot_ts_ms = _load_cached_tradier_rows(conw, symbol, now_ms=ts_ms)
                if cached_rows and cached_snapshot_ts_ms is not None:
                    cached_rows = _validate_tradier_rows(cached_rows, symbol)
                    written = write_buffer.add_tradier_rows(symbol, cached_rows, ts_ms=ts_ms, source="tradier_cache")
                    raw_rows += written
                    last_ingested_ts_ms = max(int(last_ingested_ts_ms or 0), int(cached_snapshot_ts_ms))
                    write_buffer.stage_symbol_cache_use(
                        symbol,
                        provider="tradier",
                        now_ms=ts_ms,
                        snapshot_ts_ms=int(cached_snapshot_ts_ms),
                        row_count=written,
                        state_cache=symbol_states,
                    )
                    cached_fallback_symbols += 1
                    if "tradier" in provider_status:
                        provider_status["tradier"]["cached_symbols"] += 1
                    symbol_results[symbol] = {
                        "status": "disabled_cached",
                        "provider": "tradier",
                        "rows": int(written),
                        "snapshot_ts_ms": int(cached_snapshot_ts_ms),
                        "disabled_until_ts_ms": int(symbol_state.get("disabled_until_ts_ms") or 0),
                        "error": str(symbol_state.get("last_failure_error") or "temporarily_disabled"),
                    }
                    write_buffer.mark_symbol()
                else:
                    symbol_results[symbol] = {
                        "status": "disabled",
                        "provider": "tradier",
                        "rows": 0,
                        "snapshot_ts_ms": None,
                        "disabled_until_ts_ms": int(symbol_state.get("disabled_until_ts_ms") or 0),
                        "error": str(symbol_state.get("last_failure_error") or "temporarily_disabled"),
                    }
                completed_symbols.add(symbol)

        for provider in providers:
            pending_symbols = [symbol for symbol in symbols if symbol not in completed_symbols]
            if not pending_symbols:
                continue

            if provider in provider_disabled_errors:
                for symbol in pending_symbols:
                    _mark_provider_disabled(symbol, provider)
                continue

            if _provider_cooldown_remaining_s(provider, now_s) > 0.0:
                for symbol in pending_symbols:
                    _mark_provider_cooldown_skip(symbol, provider)
                continue

            first_symbol = pending_symbols[0]
            provider_fetch_symbols += 1
            provider_fetch_max_workers = max(provider_fetch_max_workers, 1)
            _apply_fetch_result(first_symbol, provider, _fetch_provider_symbol(provider, first_symbol, ts_ms))
            remaining_symbols = pending_symbols[1:]
            if _provider_blocked(provider):
                for symbol in remaining_symbols:
                    if provider in provider_disabled_errors:
                        _mark_provider_disabled(symbol, provider)
                    else:
                        _mark_provider_cooldown_skip(symbol, provider)
                continue

            if remaining_symbols:
                provider_fetch_batches += 1
                provider_fetch_symbols += int(len(remaining_symbols))
                provider_fetch_max_workers = max(
                    int(provider_fetch_max_workers),
                    int(_provider_fetch_max_workers(remaining_symbols)),
                )
            fetch_results = _fetch_provider_symbols_parallel(provider, remaining_symbols, ts_ms)
            for symbol in remaining_symbols:
                if symbol in completed_symbols:
                    continue
                if provider in provider_disabled_errors:
                    _mark_provider_disabled(symbol, provider)
                    continue
                if _provider_cooldown_remaining_s(provider, now_s) > 0.0:
                    _mark_provider_cooldown_skip(symbol, provider)
                    continue
                _apply_fetch_result(
                    symbol,
                    provider,
                    fetch_results.get(symbol)
                    or {
                        "ok": False,
                        "provider": provider,
                        "symbol": symbol,
                        "error_text": "options_fetch_missing_result",
                        "kind": "provider_error",
                    },
                )

        for symbol in symbols:
            if symbol in completed_symbols:
                continue

            cached_rows, cached_snapshot_ts_ms = _load_cached_tradier_rows(conw, symbol, now_ms=ts_ms)
            provider_errors = _symbol_provider_errors(symbol)
            error_text = _format_symbol_error(provider_errors) or "options_fetch_failed"
            failure_provider = "tradier" if "tradier" in providers else str(providers[-1] if providers else "unknown")

            if cached_rows and cached_snapshot_ts_ms is not None:
                cached_rows = _validate_tradier_rows(cached_rows, symbol)
                written = write_buffer.add_tradier_rows(symbol, cached_rows, ts_ms=ts_ms, source="tradier_cache")
                raw_rows += written
                last_ingested_ts_ms = max(int(last_ingested_ts_ms or 0), int(cached_snapshot_ts_ms))
                state = write_buffer.stage_symbol_failure(
                    symbol,
                    provider=failure_provider,
                    error=error_text,
                    now_ms=ts_ms,
                    fallback_snapshot_ts_ms=int(cached_snapshot_ts_ms),
                    row_count=written,
                    state_cache=symbol_states,
                )
                cached_fallback_symbols += 1
                if "tradier" in provider_status:
                    provider_status["tradier"]["cached_symbols"] += 1
                symbol_results[symbol] = {
                    "status": "cached",
                    "provider": "tradier",
                    "rows": int(written),
                    "snapshot_ts_ms": int(cached_snapshot_ts_ms),
                    "disabled_until_ts_ms": int(state.get("disabled_until_ts_ms") or 0),
                    "error": error_text,
                }
                _warn_state(
                    "OPTIONS_POLL_FETCH_FALLBACK",
                    "Options poll fell back to cached Tradier data.",
                    provider="tradier",
                    symbol=symbol,
                    cached_snapshot_ts_ms=int(cached_snapshot_ts_ms),
                    error=error_text,
                )
            else:
                state = write_buffer.stage_symbol_failure(
                    symbol,
                    provider=failure_provider,
                    error=error_text,
                    now_ms=ts_ms,
                    state_cache=symbol_states,
                )
                symbol_results[symbol] = {
                    "status": "failed",
                    "provider": failure_provider,
                    "rows": 0,
                    "snapshot_ts_ms": None,
                    "disabled_until_ts_ms": int(state.get("disabled_until_ts_ms") or 0),
                    "error": error_text,
                }
                disabled_provider_failures = {
                    str(provider_name): str(provider_error or "")
                    for provider_name, provider_error in provider_errors.items()
                    if str(provider_name or "").strip().lower() in provider_disabled_errors
                }
                if not disabled_provider_failures or len(disabled_provider_failures) != len(provider_errors):
                    _warn_state(
                        "OPTIONS_POLL_FETCH_FAILED",
                        "Options fetch failed across all providers.",
                        symbol=symbol,
                        providers=list(providers),
                        error=error_text,
                    )

            completed_symbols.add(symbol)
            write_buffer.mark_symbol()

        write_buffer.flush()
        checkpoint_if_due(writes=max(1, raw_rows))

        run_meta = _build_run_meta(symbols, symbol_results, providers, last_ingested_ts_ms)
        run_meta["provider_cooldowns"] = _provider_cooldown_snapshot(providers, now_s=now_s)
        run_meta["write_buffer"] = write_buffer.snapshot()
        run_meta["shard"] = INGESTION_SHARD.as_dict()
        run_meta["state_load_queries"] = int(state_load_queries)
        run_meta["state_load_symbols"] = int(len(symbols))
        run_meta["provider_fetch_batches"] = int(provider_fetch_batches)
        run_meta["provider_fetch_symbols"] = int(provider_fetch_symbols)
        run_meta["provider_fetch_max_workers"] = int(provider_fetch_max_workers)
        run_meta["provider_fetch_concurrency_limit"] = int(OPTIONS_POLL_FETCH_CONCURRENCY)
        run_meta["commit_batch_symbols"] = int(OPTIONS_POLL_COMMIT_BATCH_SYMBOLS)
        run_meta["cached_fallback_symbols"] = int(cached_fallback_symbols)
        run_meta["provider_fetch_failures"] = int(provider_fetch_failures)
        run_meta.update(write_buffer.snapshot())
        pipeline_ok = bool(raw_rows > 0 and not bool(run_meta.get("critical")))
        last_error = None
        failed_symbols = list(run_meta.get("failed_symbols") or [])
        critical_unavailable = list(run_meta.get("critical_unavailable_symbols") or [])
        if failed_symbols or critical_unavailable:
            last_error = _clip_error(
                f"failed={','.join(failed_symbols[:8])};critical={','.join(critical_unavailable[:8])}"
            )

        if int(run_meta.get("symbols_attempted") or 0) > 0 and int(run_meta.get("symbols_succeeded") or 0) == 0:
            try:
                emit_alert(
                    event_title="Options poll fresh ingestion unavailable",
                    symbol=str((critical_unavailable[:1] or ["OPTIONS"])[0]),
                    horizon_s=0,
                    expected_z=0.0,
                    confidence=0.0,
                    explain={
                        "job": JOB_NAME,
                        "providers": list(providers),
                        "symbols_attempted": int(run_meta.get("symbols_attempted") or 0),
                        "symbols_succeeded": int(run_meta.get("symbols_succeeded") or 0),
                        "symbols_failed": int(run_meta.get("symbols_failed") or 0),
                        "cached_symbol_count": int(run_meta.get("cached_symbol_count") or 0),
                        "critical_unavailable_symbols": list(critical_unavailable),
                    },
                )
            except Exception as e:
                _warn_nonfatal(
                    "OPTIONS_POLL_ZERO_FRESH_ALERT_FAILED",
                    e,
                    once_key="options_poll_zero_fresh_alert",
                    providers=list(providers),
                )

        logging.info("options poll complete providers=%s rows=%s", providers, raw_rows)
        _emit_options_poll_run_metrics(run_meta, provider_status, ts_ms=ts_ms)
        return {
            "symbols": symbols,
            "provider_status": provider_status,
            "event_rows": int(event_rows),
            "raw_rows": int(raw_rows),
            "last_ingested_ts_ms": (int(last_ingested_ts_ms) if last_ingested_ts_ms is not None else None),
            "pipeline_ok": pipeline_ok,
            "last_error": last_error,
            "meta": run_meta,
        }
    except Exception as exc:
        failure_meta = {
            "providers": list(providers),
            "symbols_n": int(len(symbols)),
            "state_load_queries": int(state_load_queries),
            "state_load_symbols": int(len(symbols)),
            "provider_fetch_batches": int(provider_fetch_batches),
            "provider_fetch_symbols": int(provider_fetch_symbols),
            "provider_fetch_max_workers": int(provider_fetch_max_workers),
            "provider_fetch_concurrency_limit": int(OPTIONS_POLL_FETCH_CONCURRENCY),
            "provider_fetch_failures": int(provider_fetch_failures),
            "cached_fallback_symbols": int(cached_fallback_symbols),
            "commit_batch_symbols": int(OPTIONS_POLL_COMMIT_BATCH_SYMBOLS),
            "shard": INGESTION_SHARD.as_dict(),
            **write_buffer.snapshot(),
        }
        _write_options_poll_metric(
            "options.poll.failures",
            1,
            ts_ms=ts_ms,
            tags={"error": type(exc).__name__},
        )
        _emit_options_poll_run_metrics(failure_meta, provider_status, ts_ms=ts_ms)
        _warn_nonfatal(
            "OPTIONS_POLL_RUN_FAILED",
            exc,
            committed_batches=int(failure_meta.get("commit_batches") or 0),
            committed_symbols=int(failure_meta.get("committed_symbols") or 0),
            pending_symbols=int(failure_meta.get("pending_symbols") or 0),
            provider_fetch_symbols=int(failure_meta.get("provider_fetch_symbols") or 0),
        )
        try:
            setattr(exc, "options_poll_failure_meta", dict(failure_meta))
        except Exception:
            pass
        raise
    finally:
        conw.close()


def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(
            JOB_NAME,
            ok=True,
            message="options_poll disabled by data source control plane",
            meta={"liveness_job_name": JOB_LIVENESS_NAME, "shard": INGESTION_SHARD.as_dict()},
            best_effort=True,
        )
        return

    init_db()

    if not acquire_job_lock(JOB_LIVENESS_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    last_provider_health_s = 0.0
    last_status: Dict[str, Dict[str, Any]] = {}

    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                manager.record_job_status(
                    JOB_NAME,
                    ok=True,
                    message="options_poll disabled by data source control plane",
                    meta={"liveness_job_name": JOB_LIVENESS_NAME, "shard": INGESTION_SHARD.as_dict()},
                    best_effort=True,
                )
                break

            now_s = time.time()
            providers = _resolve_providers()

            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_LIVENESS_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_LIVENESS_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {
                            "job_name": JOB_NAME,
                            "liveness_job_name": JOB_LIVENESS_NAME,
                            "shard": INGESTION_SHARD.as_dict(),
                            "providers": providers,
                            "status": last_status,
                            "poll_seconds": int(OPTIONS_POLL_SECONDS),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
                last_hb_s = now_s

            symbols: List[str] = []
            try:
                result = _run_once(providers)
                symbols = list(result.get("symbols") or [])
                last_status = dict(result.get("provider_status") or {})
                raw_rows = int(result.get("raw_rows") or 0)
                event_rows = int(result.get("event_rows") or 0)
                pipeline_ok = bool(result.get("pipeline_ok"))
                last_error = str(result.get("last_error") or "").strip()
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=pipeline_ok,
                    raw_rows=raw_rows,
                    event_rows=event_rows,
                    last_ingested_ts_ms=result.get("last_ingested_ts_ms"),
                    error=(last_error or None),
                    meta=dict(result.get("meta") or {}),
                    best_effort=True,
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=pipeline_ok,
                    message="options_poll cycle complete",
                    error=(last_error or ""),
                    meta={
                        "providers": providers,
                        "liveness_job_name": JOB_LIVENESS_NAME,
                        "shard": INGESTION_SHARD.as_dict(),
                        "symbols_n": len(symbols),
                        "raw_rows": raw_rows,
                        "event_rows": event_rows,
                        "critical": bool((result.get("meta") or {}).get("critical")),
                    },
                    best_effort=True,
                )
                put_job_heartbeat(JOB_LIVENESS_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            except Exception as exc:
                last_status = {str(provider): {"ok": False, "error": str(exc), "rows": 0} for provider in providers}
                logging.exception("options poll failure")
                failure_meta = {
                    "providers": providers,
                    "liveness_job_name": JOB_LIVENESS_NAME,
                    "shard": INGESTION_SHARD.as_dict(),
                }
                attached_meta = getattr(exc, "options_poll_failure_meta", None)
                if isinstance(attached_meta, dict):
                    failure_meta.update(attached_meta)
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=None,
                    error=str(exc),
                    meta=failure_meta,
                    best_effort=True,
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=False,
                    message="options_poll cycle failed",
                    error=str(exc),
                    meta=failure_meta,
                    best_effort=True,
                )
                put_job_heartbeat(JOB_LIVENESS_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))

            now_s = time.time()
            if (now_s - last_provider_health_s) >= PROVIDER_HEALTH_EVERY_S:
                for provider in providers:
                    status = dict(last_status.get(str(provider)) or {})
                    try:
                        _put_provider_health(
                            str(provider),
                            ok=bool(status.get("ok")),
                            n_symbols=len(symbols),
                            error=(str(status.get("error")) if status.get("error") else None),
                        )
                    except Exception:
                        logging.exception("options provider health update failed provider=%s", provider)
                last_provider_health_s = now_s

            time.sleep(float(OPTIONS_POLL_SECONDS))
    finally:
        release_job_lock(JOB_LIVENESS_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
