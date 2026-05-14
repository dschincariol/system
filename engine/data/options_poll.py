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
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

from engine.data._credentials import get_data_credential
from engine.data.default_symbols import parse_symbol_limit
from engine.data.options.options_polygon import fetch_options_chain_snapshot
from engine.data.options.tradier_live import TradierFetchError, fetch_options_chain
from engine.data.provider_registry import list_provider_definitions
from engine.data.universe import get_active_symbols
from engine.runtime import dbapi_compat as dbapi
from engine.runtime.alerts import emit_alert
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.metrics_store import write_runtime_metric
from engine.runtime.telemetry_append_buffer import append_price_provider_health
from engine.runtime.storage import (
    acquire_job_lock,
    checkpoint_if_due,
    connect,
    init_db,
    put_event,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = "options_poll"
OWNER = os.environ.get("JOB_OWNER", "system")
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
OPTIONS_POLL_SECONDS = max(1, int(os.environ.get("OPTIONS_POLL_SECONDS", "300")))
HEARTBEAT_EVERY_S = float(os.environ.get("OPTIONS_POLL_HEARTBEAT_S", "15"))
PROVIDER_HEALTH_EVERY_S = float(os.environ.get("OPTIONS_PROVIDER_HEALTH_EVERY_S", "30"))
OPTIONS_CACHE_MAX_AGE_S = max(60, int(os.environ.get("OPTIONS_CACHE_MAX_AGE_S", str(max(OPTIONS_POLL_SECONDS * 6, 1800)))))
OPTIONS_SYMBOL_FAILURE_THRESHOLD = max(1, int(os.environ.get("OPTIONS_SYMBOL_FAILURE_THRESHOLD", "3")))
OPTIONS_SYMBOL_DISABLE_S = max(60, int(os.environ.get("OPTIONS_SYMBOL_DISABLE_S", "900")))
OPTIONS_POLL_COMMIT_EVERY_SYMBOLS = max(1, int(os.environ.get("OPTIONS_POLL_COMMIT_EVERY_SYMBOLS", "1")))
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


def _load_symbol_state(con, symbol: str) -> Dict[str, Any]:
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
    if not row:
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


def _record_symbol_success(
    con,
    symbol: str,
    *,
    provider: str,
    now_ms: int,
    snapshot_ts_ms: int,
    row_count: int,
) -> Dict[str, Any]:
    prev = _load_symbol_state(con, symbol)
    con.execute(
        """
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
        """,
        (
            str(symbol),
            str(provider),
            0,
            int(prev.get("total_failures") or 0),
            prev.get("last_failure_ts_ms"),
            "",
            int(now_ms),
            int(snapshot_ts_ms),
            prev.get("last_cached_snapshot_ts_ms"),
            prev.get("last_fallback_ts_ms"),
            int(row_count),
            0,
            int(now_ms),
        ),
    )
    return _load_symbol_state(con, symbol)


def _record_symbol_failure(
    con,
    symbol: str,
    *,
    provider: str,
    error: str,
    now_ms: int,
    fallback_snapshot_ts_ms: Optional[int] = None,
    row_count: int = 0,
) -> Dict[str, Any]:
    prev = _load_symbol_state(con, symbol)
    consecutive_failures = int(prev.get("consecutive_failures") or 0) + 1
    total_failures = int(prev.get("total_failures") or 0) + 1
    disabled_until_ts_ms = int(prev.get("disabled_until_ts_ms") or 0)
    if consecutive_failures >= OPTIONS_SYMBOL_FAILURE_THRESHOLD:
        disabled_until_ts_ms = max(disabled_until_ts_ms, int(now_ms + (OPTIONS_SYMBOL_DISABLE_S * 1000)))
    elif disabled_until_ts_ms <= now_ms:
        disabled_until_ts_ms = 0

    con.execute(
        """
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
        """,
        (
            str(symbol),
            str(provider),
            int(consecutive_failures),
            int(total_failures),
            int(now_ms),
            _clip_error(error),
            prev.get("last_success_ts_ms"),
            prev.get("last_fresh_snapshot_ts_ms"),
            (int(fallback_snapshot_ts_ms) if fallback_snapshot_ts_ms is not None else prev.get("last_cached_snapshot_ts_ms")),
            (int(now_ms) if fallback_snapshot_ts_ms is not None else prev.get("last_fallback_ts_ms")),
            int(row_count if row_count > 0 else (prev.get("last_row_count") or 0)),
            int(disabled_until_ts_ms),
            int(now_ms),
        ),
    )
    return _load_symbol_state(con, symbol)


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


def _record_symbol_cache_use(
    con,
    symbol: str,
    *,
    provider: str,
    now_ms: int,
    snapshot_ts_ms: int,
    row_count: int,
) -> Dict[str, Any]:
    prev = _load_symbol_state(con, symbol)
    con.execute(
        """
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
        """,
        (
            str(symbol),
            str(provider),
            int(prev.get("consecutive_failures") or 0),
            int(prev.get("total_failures") or 0),
            prev.get("last_failure_ts_ms"),
            prev.get("last_failure_error"),
            prev.get("last_success_ts_ms"),
            prev.get("last_fresh_snapshot_ts_ms"),
            int(snapshot_ts_ms),
            int(now_ms),
            int(row_count),
            int(prev.get("disabled_until_ts_ms") or 0),
            int(now_ms),
        ),
    )
    return _load_symbol_state(con, symbol)


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


def _write_polygon_contracts(con, contracts: List[Dict[str, Any]]) -> int:
    if not contracts:
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
        [
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
        ],
    )
    return len(contracts)


def _write_tradier_rows(con, symbol: str, rows: List[Dict[str, Any]], *, ts_ms: int, source: str) -> int:
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
        [
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
        ],
    )
    return len(rows)


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
        symbols = get_active_symbols(con, limit=OPTIONS_SYMBOL_LIMIT)
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
    symbol_results: Dict[str, Dict[str, Any]] = {}
    event_rows = 0
    raw_rows = 0
    last_ingested_ts_ms: Optional[int] = None
    committed_symbols = 0
    provider_disabled_errors: Dict[str, str] = {}

    try:
        logging.info("options poll start providers=%s symbols=%s", providers, len(symbols))

        for symbol in symbols:
            symbol = str(symbol or "").upper().strip()
            if not symbol:
                continue

            symbol_state = _load_symbol_state(conw, symbol)
            provider_errors: Dict[str, str] = {}
            success = False

            if int(symbol_state.get("disabled_until_ts_ms") or 0) > ts_ms:
                cached_rows, cached_snapshot_ts_ms = _load_cached_tradier_rows(conw, symbol, now_ms=ts_ms)
                if cached_rows and cached_snapshot_ts_ms is not None:
                    cached_rows = _validate_tradier_rows(cached_rows, symbol)
                    written = _write_tradier_rows(conw, symbol, cached_rows, ts_ms=ts_ms, source="tradier_cache")
                    raw_rows += written
                    last_ingested_ts_ms = max(int(last_ingested_ts_ms or 0), int(cached_snapshot_ts_ms))
                    _record_symbol_cache_use(
                        conw,
                        symbol,
                        provider="tradier",
                        now_ms=ts_ms,
                        snapshot_ts_ms=int(cached_snapshot_ts_ms),
                        row_count=written,
                    )
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
                else:
                    symbol_results[symbol] = {
                        "status": "disabled",
                        "provider": "tradier",
                        "rows": 0,
                        "snapshot_ts_ms": None,
                        "disabled_until_ts_ms": int(symbol_state.get("disabled_until_ts_ms") or 0),
                        "error": str(symbol_state.get("last_failure_error") or "temporarily_disabled"),
                    }
                committed_symbols += 1
                if committed_symbols >= OPTIONS_POLL_COMMIT_EVERY_SYMBOLS:
                    conw.commit()
                    committed_symbols = 0
                continue

            for provider in providers:
                provider = _provider_name(provider)
                if provider in provider_disabled_errors:
                    error_text = str(provider_disabled_errors.get(provider) or "provider_disabled")
                    provider_errors[provider] = error_text
                    provider_status[provider]["error"] = _clip_error(error_text)
                    provider_status[provider]["failed_symbols"] += 1
                    continue
                cooldown_remaining_s = _provider_cooldown_remaining_s(provider, now_s)
                if cooldown_remaining_s > 0.0:
                    error_text = _provider_cooldown_error(provider, now_s=now_s)
                    provider_errors[provider] = error_text
                    provider_status[provider]["error"] = _clip_error(error_text)
                    provider_status[provider]["failed_symbols"] += 1
                    provider_status[provider]["skipped_symbols"] += 1
                    provider_status[provider]["cooldown_until_ts_ms"] = int(float(provider_cooldowns.get(provider) or 0.0) * 1000.0)
                    provider_status[provider]["cooldown_remaining_s"] = float(cooldown_remaining_s)
                    provider_status[provider]["consecutive_rate_limits"] = int(provider_rate_limit_counts.get(provider) or 0)
                    continue
                try:
                    if provider == "polygon":
                        contracts, err = fetch_options_chain_snapshot(symbol, limit=250, max_pages=4)
                        if err:
                            if _provider_error_is_rate_limited(err):
                                _record_provider_rate_limit(
                                    provider_status,
                                    provider_errors,
                                    provider,
                                    err,
                                    now_s=now_s,
                                    symbol=symbol,
                                )
                                continue
                            error_text = str(err)
                            provider_errors[provider] = error_text
                            provider_status[provider]["error"] = _clip_error(error_text)
                            provider_status[provider]["failed_symbols"] += 1
                            if _provider_error_disables_run(err):
                                provider_disabled_errors[provider] = error_text
                                _warn_state(
                                    "OPTIONS_POLL_PROVIDER_DISABLED",
                                    "Options provider disabled for the remainder of the poll due to configuration error.",
                                    provider=provider,
                                    error=error_text,
                                )
                            _warn_state("OPTIONS_POLL_POLYGON_PROVIDER_ERROR", "Polygon options provider returned an error.", symbol=symbol, error=str(err))
                            continue

                        contracts = _validate_polygon_contracts(contracts or [], symbol, ts_ms)
                        if not contracts:
                            provider_errors[provider] = "polygon_empty_chain"
                            provider_status[provider]["error"] = "polygon_empty_chain"
                            provider_status[provider]["failed_symbols"] += 1
                            continue

                        written = _write_polygon_contracts(conw, contracts)
                        raw_rows += written
                        provider_status[provider]["ok"] = True
                        provider_status[provider]["error"] = None
                        provider_status[provider]["rows"] += written
                        provider_status[provider]["fresh_symbols"] += 1
                        provider_rate_limit_counts[provider] = 0
                        _clear_provider_cooldown(provider, now_s=now_s)
                        last_ingested_ts_ms = max(int(last_ingested_ts_ms or 0), int(ts_ms))
                        _record_symbol_success(
                            conw,
                            symbol,
                            provider=provider,
                            now_ms=ts_ms,
                            snapshot_ts_ms=ts_ms,
                            row_count=written,
                        )
                        put_event(
                            ts_ms=ts_ms,
                            source="options_polygon",
                            title=f"{symbol} options snapshot",
                            body=f"{written} contracts",
                            url=None,
                            event_key=f"options:{provider}:{symbol}:{ts_ms}",
                            meta_json=json.dumps(
                                {"symbol": symbol, "provider": provider, "contracts": written},
                                separators=(",", ":"),
                                sort_keys=True,
                            ),
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
                        logging.info("options fetch ok provider=%s symbol=%s rows=%s", provider, symbol, written)
                        success = True
                        break

                    if provider == "tradier":
                        result = fetch_options_chain(symbol)
                        rows = _validate_tradier_rows(list((result or {}).get("rows") or []), symbol)
                        if not rows:
                            provider_errors[provider] = "tradier_empty_chain"
                            provider_status[provider]["error"] = "tradier_empty_chain"
                            provider_status[provider]["failed_symbols"] += 1
                            _warn_state("OPTIONS_POLL_TRADIER_EMPTY_CHAIN", "Tradier returned an empty options chain.", symbol=symbol)
                            continue

                        written = _write_tradier_rows(conw, symbol, rows, ts_ms=ts_ms, source="tradier")
                        raw_rows += written
                        provider_status[provider]["ok"] = True
                        provider_status[provider]["error"] = None
                        provider_status[provider]["rows"] += written
                        provider_status[provider]["fresh_symbols"] += 1
                        provider_rate_limit_counts[provider] = 0
                        _clear_provider_cooldown(provider, now_s=now_s)
                        last_ingested_ts_ms = max(int(last_ingested_ts_ms or 0), int(ts_ms))
                        _record_symbol_success(
                            conw,
                            symbol,
                            provider=provider,
                            now_ms=ts_ms,
                            snapshot_ts_ms=ts_ms,
                            row_count=written,
                        )
                        put_event(
                            ts_ms=ts_ms,
                            source="options_tradier",
                            title=f"{symbol} options snapshot",
                            body=f"{written} contracts",
                            url=None,
                            event_key=f"options:{provider}:{symbol}:{ts_ms}",
                            meta_json=json.dumps(
                                {"symbol": symbol, "provider": provider, "contracts": written},
                                separators=(",", ":"),
                                sort_keys=True,
                            ),
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
                        logging.info("options fetch ok provider=%s symbol=%s rows=%s", provider, symbol, written)
                        success = True
                        break
                except TradierFetchError as exc:
                    error_text = f"{exc.kind}:{exc}"
                    if _provider_error_is_rate_limited(exc):
                        _record_provider_rate_limit(
                            provider_status,
                            provider_errors,
                            provider,
                            exc,
                            now_s=now_s,
                            symbol=symbol,
                        )
                    else:
                        provider_errors[provider] = error_text
                        provider_status[provider]["error"] = _clip_error(error_text)
                        provider_status[provider]["failed_symbols"] += 1
                    if not _provider_error_is_rate_limited(exc) and _provider_error_disables_run(exc):
                        provider_disabled_errors[provider] = error_text
                        _warn_state(
                            "OPTIONS_POLL_PROVIDER_DISABLED",
                            "Options provider disabled for the remainder of the poll due to configuration error.",
                            provider=provider,
                            error=error_text,
                        )
                    _warn_nonfatal("OPTIONS_POLL_TRADIER_PROVIDER_ERROR", exc, once_key=f"tradier_provider_error:{symbol}", symbol=symbol, error_text=error_text)
                except Exception as exc:
                    error_text = f"{type(exc).__name__}:{exc}"
                    if _provider_error_is_rate_limited(exc):
                        _record_provider_rate_limit(
                            provider_status,
                            provider_errors,
                            provider,
                            exc,
                            now_s=now_s,
                            symbol=symbol,
                        )
                    else:
                        provider_errors[provider] = error_text
                        provider_status[provider]["error"] = _clip_error(error_text)
                        provider_status[provider]["failed_symbols"] += 1
                    _warn_nonfatal("OPTIONS_POLL_PROVIDER_ERROR", exc, once_key=f"provider_error:{provider}:{symbol}", provider=provider, symbol=symbol, error_text=error_text)

            if success:
                committed_symbols += 1
                if committed_symbols >= OPTIONS_POLL_COMMIT_EVERY_SYMBOLS:
                    conw.commit()
                    committed_symbols = 0
                continue

            cached_rows, cached_snapshot_ts_ms = _load_cached_tradier_rows(conw, symbol, now_ms=ts_ms)
            error_text = _format_symbol_error(provider_errors) or "options_fetch_failed"
            failure_provider = "tradier" if "tradier" in providers else str(providers[-1] if providers else "unknown")

            if cached_rows and cached_snapshot_ts_ms is not None:
                cached_rows = _validate_tradier_rows(cached_rows, symbol)
                written = _write_tradier_rows(conw, symbol, cached_rows, ts_ms=ts_ms, source="tradier_cache")
                raw_rows += written
                last_ingested_ts_ms = max(int(last_ingested_ts_ms or 0), int(cached_snapshot_ts_ms))
                state = _record_symbol_failure(
                    conw,
                    symbol,
                    provider=failure_provider,
                    error=error_text,
                    now_ms=ts_ms,
                    fallback_snapshot_ts_ms=int(cached_snapshot_ts_ms),
                    row_count=written,
                )
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
                try:
                    state = _record_symbol_failure(
                        conw,
                        symbol,
                        provider=failure_provider,
                        error=error_text,
                        now_ms=ts_ms,
                    )
                except dbapi.OperationalError as exc:
                    _rollback_if_active(conw)
                    _warn_nonfatal(
                        "OPTIONS_POLL_SYMBOL_FAILURE_RECORD_FAILED",
                        exc,
                        once_key=f"symbol_failure_record:{symbol}",
                        symbol=symbol,
                        provider=failure_provider,
                        provider_error=error_text,
                    )
                    state = {"disabled_until_ts_ms": 0}
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

            committed_symbols += 1
            if committed_symbols >= OPTIONS_POLL_COMMIT_EVERY_SYMBOLS:
                conw.commit()
                committed_symbols = 0

        conw.commit()
        checkpoint_if_due(writes=max(1, raw_rows))

        run_meta = _build_run_meta(symbols, symbol_results, providers, last_ingested_ts_ms)
        run_meta["provider_cooldowns"] = _provider_cooldown_snapshot(providers, now_s=now_s)
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
            best_effort=True,
        )
        return

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
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
                    best_effort=True,
                )
                break

            now_s = time.time()
            providers = _resolve_providers()

            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {
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
                        "symbols_n": len(symbols),
                        "raw_rows": raw_rows,
                        "event_rows": event_rows,
                        "critical": bool((result.get("meta") or {}).get("critical")),
                    },
                    best_effort=True,
                )
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            except Exception as exc:
                last_status = {str(provider): {"ok": False, "error": str(exc), "rows": 0} for provider in providers}
                logging.exception("options poll failure")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=None,
                    error=str(exc),
                    meta={"providers": providers},
                    best_effort=True,
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=False,
                    message="options_poll cycle failed",
                    error=str(exc),
                    meta={"providers": providers},
                    best_effort=True,
                )
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))

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
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
