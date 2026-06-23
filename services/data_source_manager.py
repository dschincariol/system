"""DB-backed control plane for configurable ingestion and provider sources.

This manager owns the source catalog, encrypted credentials, runtime
environment projection, health snapshots, lifecycle reconciliation, and
operator-facing testing/log views for ingestion sources.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests

from engine.data.broker_readonly import (
    ALLOW_LIVE_ALPACA_BROKER_DATA_ENV,
    ALPACA_PAPER_BASE_URL,
    AlpacaBrokerDataReadOnlyClient,
    AlpacaBrokerDataSettings,
    BrokerDataReadOnlyViolation,
    IBKRBrokerDataReadOnlyClient,
    IBKRBrokerDataSettings,
    assert_data_source_broker_runtime_allowed,
    readonly_guard_snapshot,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.platform import default_ibkr_host
from engine.runtime.data_source_log_store import (
    DATA_SOURCE_LOG_REDACTION_TIMESCALE_MARKER_KEY,
    append_data_source_log_row,
    delete_data_source_logs_for_source,
    ensure_data_source_logs_schema,
    log_data_source_event,
    redact_existing_data_source_log_details_once,
    redact_existing_timescale_data_source_log_details,
    sanitize_data_source_log_detail,
    sanitize_data_source_log_detail_json,
)
from engine.runtime.runtime_meta import meta_get, meta_set
from engine.runtime.startup_write_gate import should_defer_noncritical_startup_write
from engine.runtime.storage import connect_ro, put_price, run_write_txn
from engine.runtime.telemetry_append_buffer import append_price_provider_health
from engine.runtime.telemetry_read_router import fetch_data_source_logs
from services.credential_encryption import (
    DEFAULT_MASTER_KEY_NAME,
    decrypt_credentials,
    encrypt_credentials,
    mask_credentials,
)

LOG = logging.getLogger("data_source_manager")
_WARNED_NONFATAL_KEYS: set[str] = set()
_DATA_SOURCE_STATUS_BEST_EFFORT_MIN_INTERVAL_MS = max(
    0,
    int(
        float(
            os.environ.get("DATA_SOURCE_STATUS_BEST_EFFORT_MIN_INTERVAL_S", "15.0") or 15.0
        )
        * 1000.0
    ),
)
_DATA_SOURCE_STATUS_BEST_EFFORT_LOCK = threading.Lock()
_LAST_DATA_SOURCE_STATUS_BEST_EFFORT: dict[str, dict[str, Any]] = {}
_DATA_SOURCE_MANAGER_BEST_EFFORT_TIMEOUT_S = max(
    0.05,
    float(os.environ.get("DATA_SOURCE_MANAGER_BEST_EFFORT_TIMEOUT_S", "0.25") or 0.25),
)
_DATA_SOURCE_MANAGER_BEST_EFFORT_BUSY_TIMEOUT_MS = max(
    25,
    int(float(os.environ.get("DATA_SOURCE_MANAGER_BEST_EFFORT_BUSY_TIMEOUT_MS", "250") or 250.0)),
)
_DATA_SOURCE_MANAGER_STARTUP_WRITE_TIMEOUT_S = max(
    0.25,
    float(os.environ.get("DATA_SOURCE_MANAGER_STARTUP_WRITE_TIMEOUT_S", "5.0") or 5.0),
)
_DATA_SOURCE_MANAGER_STARTUP_BUSY_TIMEOUT_MS = max(
    250,
    int(float(os.environ.get("DATA_SOURCE_MANAGER_STARTUP_BUSY_TIMEOUT_MS", "5000") or 5000.0)),
)
_DATA_SOURCE_CONNECTION_TEST_MIN_INTERVAL_MS = max(
    0,
    int(float(os.environ.get("DATA_SOURCE_CONNECTION_TEST_MIN_INTERVAL_S", "2.0") or 2.0) * 1000.0),
)
_DATA_SOURCE_CONNECTION_TEST_LOCK = threading.Lock()
_LAST_DATA_SOURCE_CONNECTION_TEST_PROBE_MS: dict[str, int] = {}
_BASE_CREDENTIAL_RUNTIME_ENV_KEYS = (
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
    "FRED_API_KEY",
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
    "SEC_FROM",
    "SEC_USER_AGENT",
    "SHARADAR_API_KEY",
    "SIMFIN_API_KEY",
    "TRADIER_API_TOKEN",
)
_SAFE_NO_CREDENTIAL_ENV = {
    "POLYGON_REST_ENABLED": "0",
    "POLYGON_WS_ENABLED": "0",
    "IBKR_ENABLED": "0",
    "CCXT_ENABLED": "0",
    "OANDA_ENABLED": "0",
    "TRADIER_ENABLED": "0",
    "YFINANCE_ENABLED": "1",
    "SIMULATED_MARKET_DATA_ENABLED": "1",
    "FX_PAIRS_ENABLED": "0",
    "LIVE_PRICE_PROVIDER_CHAIN": "yfinance",
    "OPTIONS_PROVIDER_CHAIN": "",
}
_RUNTIME_CREDENTIAL_DIR_ENV = "DATA_SOURCE_MANAGER_RUNTIME_SECRET_DIR"
_DEFAULT_RUNTIME_CREDENTIAL_DIR = Path("/run/trading/data-source-secrets")
_PROJECTED_RUNTIME_KEYS_ENV = "DATA_SOURCE_MANAGER_PROJECTED_KEYS"
RUNNABLE_STATE_OFF = "off"
RUNNABLE_STATE_ENABLED_MISSING_CREDENTIAL = "enabled-missing-credential"
RUNNABLE_STATE_ENABLED_CREDENTIALED_NOT_SCHEDULED = "enabled-credentialed-not-scheduled"
RUNNABLE_STATE_SCHEDULED_WAITING = "scheduled-waiting"
RUNNABLE_STATE_RUNNING = "running"
RUNNABLE_STATE_DEGRADED = "degraded"
RUNNABLE_STATE_FAILED = "failed"
RUNNABLE_STATE_HEALTHY = "healthy"
RUNNABLE_STATES = (
    RUNNABLE_STATE_OFF,
    RUNNABLE_STATE_ENABLED_MISSING_CREDENTIAL,
    RUNNABLE_STATE_ENABLED_CREDENTIALED_NOT_SCHEDULED,
    RUNNABLE_STATE_SCHEDULED_WAITING,
    RUNNABLE_STATE_RUNNING,
    RUNNABLE_STATE_DEGRADED,
    RUNNABLE_STATE_FAILED,
    RUNNABLE_STATE_HEALTHY,
)


def _looks_like_masked_credential_value(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(len(text) >= 3 and set(text) == {"*"})


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="services.data_source_manager",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name), "")
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _safe_no_credential_local_dev_env() -> bool:
    raw = str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "dev").strip().lower()
    if raw == "production":
        raw = "prod"
    elif raw == "development":
        raw = "dev"
    return raw in {"dev", "test"}


def safe_no_credential_market_data_mode() -> bool:
    if not _safe_no_credential_local_dev_env():
        return False
    if _env_flag("ALLOW_CREDENTIAL_DATA_PROVIDERS_IN_SAFE", False):
        return False
    mode = str(os.environ.get("ENGINE_MODE") or "safe").strip().lower()
    execution_mode = str(os.environ.get("EXECUTION_MODE") or "safe").strip().lower()
    broker = str(os.environ.get("BROKER") or "sim").strip().lower()
    broker_name = str(os.environ.get("BROKER_NAME") or broker or "sim").strip().lower()
    if mode != "safe" or execution_mode not in {"safe", "paper", "sim-paper", "sim_paper"}:
        return False
    if broker != "sim" or broker_name != "sim":
        return False
    return bool(_env_flag("DISABLE_LIVE_EXECUTION", True) and _env_flag("KILL_SWITCH_GLOBAL", True))


def credential_runtime_env_keys() -> tuple[str, ...]:
    keys = {str(key) for key in _BASE_CREDENTIAL_RUNTIME_ENV_KEYS}
    try:
        for definition in _default_catalog().values():
            for env_name in (definition.credential_env or {}).values():
                if str(env_name or "").strip():
                    keys.add(str(env_name).strip())
            for env_name in (definition.setting_env or {}).values():
                if str(env_name or "").strip() in {"SEC_FROM", "SEC_USER_AGENT"}:
                    keys.add(str(env_name).strip())
        for definition in _provider_account_catalog().values():
            for env_name in (definition.credential_env or {}).values():
                if str(env_name or "").strip():
                    keys.add(str(env_name).strip())
    except Exception as e:
        _warn_nonfatal(
            "DATA_SOURCE_MANAGER_CREDENTIAL_CATALOG_KEYS_FAILED",
            e,
            once_key="credential_runtime_env_keys",
        )
    return tuple(sorted(keys))


def apply_safe_no_credential_runtime_environment(env: Dict[str, str] | None = None) -> Dict[str, str]:
    target = os.environ if env is None else env
    for key in credential_runtime_env_keys():
        target.pop(str(key), None)
        target.pop(f"{str(key)}_FILE", None)
    for key, value in _SAFE_NO_CREDENTIAL_ENV.items():
        target[str(key)] = str(value)
    return dict(_SAFE_NO_CREDENTIAL_ENV)


def _strict_runtime_secret_projection() -> bool:
    try:
        from engine.runtime.config_schema import get_runtime_safety_context

        return bool(get_runtime_safety_context().get("strict_runtime"))
    except Exception as exc:
        _warn_nonfatal(
            "DATA_SOURCE_STRICT_RUNTIME_CONTEXT_FAILED",
            exc,
            once_key="strict_runtime_secret_projection",
        )
    env = str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "").strip().lower()
    if env in {"prod", "production", "staging"}:
        return True
    return _env_flag("ENGINE_SUPERVISED", False) or _env_flag("PROD_LOCK", False)


def _runtime_credential_dir() -> Path:
    raw = str(os.environ.get(_RUNTIME_CREDENTIAL_DIR_ENV) or "").strip()
    return Path(raw).expanduser() if raw else _DEFAULT_RUNTIME_CREDENTIAL_DIR


def _runtime_credential_file_name(env_name: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(env_name).strip())
    safe = "_".join(part for part in safe.split("_") if part)
    if not safe:
        raise ValueError("runtime_credential_env_name_empty")
    return safe


def _write_runtime_credential_file(env_name: str, value: str) -> Path:
    directory = _runtime_credential_dir()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(directory, 0o700)
    except OSError as exc:
        _warn_nonfatal(
            "DATA_SOURCE_RUNTIME_CREDENTIAL_DIR_CHMOD_FAILED",
            exc,
            once_key=f"runtime_credential_dir_chmod:{directory}",
            path=str(directory),
        )
    path = directory / _runtime_credential_file_name(env_name)
    tmp_path = directory / f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    tmp_path.write_text(str(value), encoding="utf-8")
    os.chmod(tmp_path, 0o600)
    os.replace(tmp_path, path)
    os.chmod(path, 0o600)
    return path


def _credential_file_available(env_name: str) -> str:
    path = str(os.environ.get(f"{str(env_name)}_FILE") or "").strip()
    if not path:
        return ""
    candidate = Path(path).expanduser()
    try:
        if candidate.is_file() and candidate.stat().st_size > 0 and os.access(candidate, os.R_OK):
            return str(candidate)
    except OSError as exc:
        _warn_nonfatal(
            "DATA_SOURCE_CREDENTIAL_FILE_STAT_FAILED",
            exc,
            once_key=f"credential_file_available:{env_name}:{candidate}",
            env_name=str(env_name),
            path=str(candidate),
        )
    return ""


def _credential_projection_base_names(env_key: str) -> set[str]:
    key = str(env_key or "").strip()
    if key.endswith("_FILE"):
        key = key[: -len("_FILE")]
    elif key.endswith("_SECRET"):
        key = key[: -len("_SECRET")]
    if not key:
        return set()
    bases = {key}
    if key == "POLYGON_API_KEY":
        bases.add("POLYGON_KEY")
    elif key == "POLYGON_KEY":
        bases.add("POLYGON_API_KEY")
    return bases


def _best_effort_source_status_payload(
    *,
    status: str,
    ok: bool,
    message: str,
    error: str,
    event_level: str,
) -> dict[str, Any]:
    return {
        "status": str(status or ""),
        "ok": bool(ok),
        "message": str(message or "")[:1000],
        "error": str(error or "")[:1000],
        "event_level": str(event_level or "").upper(),
    }


def _should_persist_best_effort_source_status(
    source_key: str,
    *,
    payload: dict[str, Any],
    now_ms: int,
) -> bool:
    if _DATA_SOURCE_STATUS_BEST_EFFORT_MIN_INTERVAL_MS <= 0:
        return True
    source_name = str(source_key or "").strip()
    with _DATA_SOURCE_STATUS_BEST_EFFORT_LOCK:
        previous = dict(_LAST_DATA_SOURCE_STATUS_BEST_EFFORT.get(source_name) or {})
        last_ts_ms = int(previous.get("ts_ms") or 0)
        if last_ts_ms > 0 and (int(now_ms) - last_ts_ms) < _DATA_SOURCE_STATUS_BEST_EFFORT_MIN_INTERVAL_MS:
            same_payload = all(previous.get(key) == value for key, value in dict(payload or {}).items())
            if same_payload:
                return False
    return True


def _note_best_effort_source_status_persisted(
    source_key: str,
    *,
    payload: dict[str, Any],
    now_ms: int,
) -> None:
    source_name = str(source_key or "").strip()
    with _DATA_SOURCE_STATUS_BEST_EFFORT_LOCK:
        _LAST_DATA_SOURCE_STATUS_BEST_EFFORT[source_name] = {
            **dict(payload or {}),
            "ts_ms": int(now_ms),
        }


@dataclass(frozen=True)
class SourceFieldMetadata:
    """Operator-facing metadata and validation policy for one source field."""

    field: str
    env_name: str = ""
    label: str = ""
    help_text: str = ""
    docs_url: str = ""
    signup_url: str = ""
    plan_note: str = ""
    required: bool = False
    secret: bool = False
    validation_hint: str = ""
    validation_regex: str = ""
    placeholder: str = ""
    safety_warning: str = ""
    input_type: str = "text"


@dataclass(frozen=True)
class SourceGuide:
    """Operator-facing setup guidance emitted with the backend catalog."""

    category: str = "Source"
    summary: str = "This source is managed from the data-source control plane."
    needs: tuple[str, ...] = ("Review the source state in the control plane.",)
    setup: tuple[str, ...] = (
        "Open Edit Source.",
        "Adjust credentials or settings.",
        "Save the source, then run Test Connection.",
    )
    when_enabled: str = "The runtime includes this source in ingestion and health monitoring."
    docs_url: str = ""
    signup_url: str = ""
    plan_note: str = ""
    safety_warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceDefinition:
    """Static metadata describing a manageable source template.

    Attributes
    ----------
    source_type : str
        Logical source category such as ``price_provider``,
        ``options_provider``, or ``rss_feed``.
    display_name : str
        Operator-facing name shown in the control plane.
    job_name : str
        Ingestion job that consumes the source when it is enabled.
    provider_name : str, default=""
        Provider identifier projected into runtime routing/configuration.
    singleton : bool, default=True
        Whether the template represents a single built-in source whose identity
        is locked.
    default_enabled : bool, default=True
        Default enabled state used when seeding built-in rows.
    credential_env : dict of str to str
        Mapping of logical credential fields to runtime environment variable
        names.
    setting_env : dict of str to str
        Mapping of non-secret settings to runtime environment variable names.
    storage_tables : tuple of str
        Storage tables populated or maintained by the source.
    consumers : tuple of str
        Runtime jobs, features, or UI surfaces that consume the source output.
    safe_to_auto_enable : bool
        Whether a deployment tool may enable this source without operator
        review. Credentialed, broker, and alternate-data sources should remain
        false even when their default row is enabled for backwards
        compatibility.
    runtime_runnable : bool
        Whether this source is allowed to schedule/project a supervised data
        job. Broker-data catalog rows are intentionally not runnable from this
        control plane so data permissioning cannot grant order authority.
    """
    source_type: str
    display_name: str
    job_name: str
    provider_name: str = ""
    singleton: bool = True
    default_enabled: bool = True
    credential_env: Dict[str, str] = field(default_factory=dict)
    setting_env: Dict[str, str] = field(default_factory=dict)
    guide: SourceGuide = field(default_factory=SourceGuide)
    credential_metadata: Dict[str, SourceFieldMetadata] = field(default_factory=dict)
    setting_metadata: Dict[str, SourceFieldMetadata] = field(default_factory=dict)
    storage_tables: tuple[str, ...] = ()
    consumers: tuple[str, ...] = ()
    safe_to_auto_enable: bool = False
    runtime_runnable: bool = True


@dataclass(frozen=True)
class ProviderAccountDefinition:
    """Shared provider-account credential set used by one or more sources."""

    account_key: str
    display_name: str
    provider_name: str
    credential_env: Dict[str, str] = field(default_factory=dict)
    used_by_sources: tuple[str, ...] = ()
    used_by_jobs: tuple[str, ...] = ()
    guide: SourceGuide = field(default_factory=SourceGuide)
    credential_metadata: Dict[str, SourceFieldMetadata] = field(default_factory=dict)


@dataclass(frozen=True)
class ConnectionTestResult:
    """Normalized provider-test result exposed to the API and source logs."""

    status: str
    classification: str
    message: str
    evidence: Dict[str, Any] = field(default_factory=dict)
    next_steps: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return str(self.status) == "pass"

    def payload(self, *, source_key: str) -> Dict[str, Any]:
        evidence = sanitize_data_source_log_detail(dict(self.evidence or {}))
        out: Dict[str, Any] = {
            "ok": bool(self.ok),
            "source_key": str(source_key or ""),
            "status": str(self.status),
            "classification": str(self.classification),
            "message": str(self.message),
            "evidence": evidence,
            "next_steps": list(self.next_steps or ()),
        }
        for key, value in evidence.items():
            out.setdefault(str(key), value)
        if not self.ok:
            out["error"] = str(self.message)
        return out


@dataclass(frozen=True)
class DataSourceContract:
    """Runtime data contract for one source's smallest storage proof."""

    storage_table: str
    normalized_shape: str
    required_fields: tuple[str, ...]
    units: Dict[str, str] = field(default_factory=dict)
    symbol_namespace: str = ""
    timestamp_timezone: str = "UTC"
    point_in_time_availability: str = ""
    unique_key: tuple[str, ...] = ()
    idempotent_upsert: str = ""
    consumer: str = ""
    timestamp_field: str = "ts_ms"
    source_field: str = "source"
    stale_after_ms: int = 0

    def payload(self) -> Dict[str, Any]:
        return {
            "storage_table": str(self.storage_table or ""),
            "normalized_shape": str(self.normalized_shape or ""),
            "required_fields": [str(item) for item in self.required_fields],
            "units": dict(self.units or {}),
            "symbol_namespace": str(self.symbol_namespace or ""),
            "timestamp_timezone": str(self.timestamp_timezone or "UTC"),
            "point_in_time_availability": str(self.point_in_time_availability or ""),
            "unique_key": [str(item) for item in self.unique_key],
            "idempotent_upsert": str(self.idempotent_upsert or ""),
            "consumer": str(self.consumer or ""),
            "timestamp_field": str(self.timestamp_field or "ts_ms"),
            "source_field": str(self.source_field or ""),
            "stale_after_ms": int(self.stale_after_ms or 0),
        }


_PROVIDER_TEST_REGISTRY: Dict[str, Dict[str, str]] = {
    "polygon": {"handler": "_test_polygon_rest_connection", "label": "Polygon REST"},
    "oanda_fx": {"handler": "_test_oanda_connection", "label": "OANDA FX pricing"},
    "polygon_ws": {"handler": "_test_polygon_ws_connection", "label": "Polygon WebSocket"},
    "ibkr": {"handler": "_test_ibkr_connection", "label": "IBKR market data"},
    "alpaca_broker_data": {"handler": "_test_alpaca_broker_data_connection", "label": "Alpaca broker data"},
    "yfinance": {"handler": "_test_yfinance_connection", "label": "Yahoo Finance"},
    "simulated": {"handler": "_test_simulated_price_connection", "label": "Simulated local prices"},
    "ccxt": {"handler": "_test_ccxt_connection", "label": "CCXT"},
    "tradier": {"handler": "_test_tradier_options_connection", "label": "Tradier options"},
    "polygon_options": {"handler": "_test_polygon_options_connection", "label": "Polygon options"},
    "reddit": {"handler": "_test_reddit_connection", "label": "Reddit PRAW OAuth"},
    "stocktwits": {"handler": "_test_stocktwits_connection", "label": "StockTwits"},
    "company_news": {"handler": "_test_finnhub_company_news_connection", "label": "Finnhub company news"},
    "transcripts": {"handler": "_test_fmp_transcripts_connection", "label": "FMP transcripts"},
    "gdelt": {"handler": "_test_gdelt_connection", "label": "GDELT"},
    "sec": {"handler": "_test_sec_filings_connection", "label": "SEC filings"},
    "form4": {"handler": "_test_form4_connection", "label": "SEC Form 4"},
    "inst_13f": {"handler": "_test_inst_13f_connection", "label": "SEC 13F"},
    "congressional_trades": {"handler": "_test_congressional_trades_connection", "label": "Congressional trades"},
    "etf_flows": {"handler": "_test_etf_flows_connection", "label": "ETF flows"},
    "cftc_cot": {"handler": "_test_cftc_cot_connection", "label": "CFTC COT"},
    "finra_short_volume": {"handler": "_test_finra_short_volume_connection", "label": "FINRA short volume"},
    "finra_short_interest": {"handler": "_test_finra_short_interest_connection", "label": "FINRA short interest"},
    "crypto_funding": {"handler": "_test_crypto_funding_connection", "label": "Crypto positioning"},
    "quiver_gov": {"handler": "_test_quiver_connection", "label": "Quiver government flow"},
    "fundamentals_pit": {"handler": "_test_fundamentals_pit_connection", "label": "SimFin / Sharadar"},
    "earnings": {"handler": "_test_fmp_earnings_connection", "label": "FMP earnings"},
    "weather_forecasts": {"handler": "_test_weather_forecasts_connection", "label": "Weather forecasts"},
    "weather_alerts": {"handler": "_test_weather_alerts_connection", "label": "Weather alerts"},
    "macro": {"handler": "_test_macro_fred_connection", "label": "FRED / ALFRED macro"},
    "news_flow": {"handler": "_test_news_flow_connection", "label": "News-flow embeddings"},
    "rss_feed": {"handler": "_test_rss_connection", "label": "RSS feed"},
    "model_feature_snapshots": {
        "unsupported_reason": "internal_snapshot_source_has_no_external_connection_probe",
        "label": "Model feature snapshots",
    },
}


def _default_catalog() -> Dict[str, SourceDefinition]:
    return {
        "polygon_ws": SourceDefinition(
            source_type="price_provider",
            display_name="Polygon WebSocket",
            provider_name="polygon_ws",
            job_name="stream_prices_polygon_ws",
            default_enabled=True,
            credential_env={"api_key": "POLYGON_API_KEY"},
            setting_env={
                "endpoint": "POLYGON_WS_ENDPOINT",
                "subscribe_trades": "POLYGON_WS_SUBSCRIBE_TRADES",
                "subscribe_quotes": "POLYGON_WS_SUBSCRIBE_QUOTES",
            },
            guide=_source_guide(
                category="Market Data",
                summary="Streams live market data from Polygon for the lowest-latency price updates.",
                needs=("A Polygon API key with WebSocket market-data access.",),
                setup=(
                    "Enter the Polygon API key.",
                    "Save the source, then run Test Connection.",
                    "Enable the source when live streaming should be active.",
                ),
                when_enabled="The runtime can stream live market data and reduce delay on price updates.",
                docs_url="https://polygon.io/docs/stocks/ws_getting-started",
                signup_url="https://polygon.io/pricing",
                plan_note="WebSocket access depends on the active Polygon plan.",
            ),
        ),
        "polygon": SourceDefinition(
            source_type="price_provider",
            display_name="Polygon REST",
            provider_name="polygon",
            job_name="poll_prices",
            default_enabled=True,
            credential_env={"api_key": "POLYGON_API_KEY"},
            guide=_source_guide(
                category="Market Data",
                summary="Polls Polygon REST snapshots as a market-data source and fallback feed.",
                needs=("A Polygon API key with REST market-data access.",),
                setup=(
                    "Enter the Polygon API key.",
                    "Save the source, then run Test Connection.",
                    "Leave this enabled when Polygon snapshot polling should be available.",
                ),
                when_enabled="The runtime can poll Polygon snapshot data for price and options workflows.",
                docs_url="https://polygon.io/docs/stocks/getting-started",
                signup_url="https://polygon.io/pricing",
                plan_note="REST rate limits and market coverage depend on the active Polygon plan.",
            ),
        ),
        "oanda_fx": SourceDefinition(
            source_type="price_provider",
            display_name="OANDA FX",
            provider_name="oanda",
            job_name="poll_prices",
            default_enabled=False,
            credential_env={"access_token": "OANDA_ACCESS_TOKEN"},
            setting_env={
                "account_id": "OANDA_ACCOUNT_ID",
                "environment": "OANDA_ENVIRONMENT",
                "fx_pairs": "OANDA_FX_PAIRS",
            },
            credential_metadata={
                "access_token": SourceFieldMetadata(
                    field="access_token",
                    label="Access Token",
                    help_text="OANDA v20 token used only for read-only pricing and instrument metadata.",
                    placeholder="Enter new token; leave blank to preserve",
                    safety_warning="This source has no order, cancel, replace, or flatten authority.",
                    secret=True,
                    required=True,
                    validation_regex=_SECRET_VALUE_VALIDATION_REGEX,
                    validation_hint="Use a single-line OANDA access token.",
                    input_type="password",
                ),
            },
            setting_metadata={
                "account_id": SourceFieldMetadata(
                    field="account_id",
                    label="Account ID",
                    help_text="OANDA account id used for account-scoped pricing and instrument metadata.",
                    placeholder="101-001-00000000-001",
                    validation_regex=r"^[A-Za-z0-9_-]+(?:-[A-Za-z0-9_-]+)*$",
                    validation_hint="Use the OANDA account id, not the access token.",
                    input_type="text",
                ),
                "environment": SourceFieldMetadata(
                    field="environment",
                    label="Environment",
                    help_text="OANDA API environment. Practice is the safe default.",
                    placeholder="practice",
                    validation_regex=r"^(?:practice|live)$",
                    validation_hint="Use practice or live.",
                    safety_warning="Live remains read-only here and does not authorize trading.",
                    input_type="text",
                ),
                "fx_pairs": SourceFieldMetadata(
                    field="fx_pairs",
                    label="FX Pairs",
                    help_text="Optional comma-separated canonical FX pairs or OANDA instruments.",
                    placeholder="EURUSD,USDJPY,GBPUSD,USDCHF,USDCAD,AUDUSD,NZDUSD",
                    validation_regex=r"^[A-Za-z_,\s]*$",
                    validation_hint="Use six-letter pairs such as EURUSD or instruments such as EUR_USD.",
                    input_type="text",
                ),
            },
            guide=_source_guide(
                category="Market Data",
                summary="Polls read-only OANDA v20 FX pricing and instrument metadata.",
                needs=("An OANDA v20 access token.", "An OANDA account id for pricing scope."),
                setup=(
                    "Enter the OANDA access token and account id.",
                    "Keep the environment set to practice until governance authorizes broader use.",
                    "Save the source, then run Test Connection.",
                    "Enable only when FX pricing should be included in poll_prices.",
                ),
                when_enabled="The runtime can poll OANDA FX prices through the existing price ingestion loop.",
                docs_url="https://developer.oanda.com/rest-live-v20/introduction/",
                plan_note="OANDA pricing availability and instruments depend on the account.",
                safety_warnings=(
                    "This source is read-only market data and instrument metadata.",
                    "It does not add order, cancel, replace, trade, account-mutation, or flatten authority.",
                ),
            ),
            safe_to_auto_enable=False,
            storage_tables=("prices", "price_quotes", "price_quotes_raw", "price_provider_health"),
            consumers=("price_router", "model_feature_snapshots", "dashboard_data_health"),
        ),
        "ibkr": SourceDefinition(
            source_type="price_provider",
            display_name="IBKR Stream",
            provider_name="ibkr",
            job_name="stream_prices_ibkr",
            default_enabled=False,
            setting_env={
                "host": "IBKR_HOST",
                "port": "IBKR_PORT",
                "client_id": "IBKR_CLIENT_ID",
                "market_data_type": "IBKR_MARKET_DATA_TYPE",
                "currency": "IBKR_CURRENCY",
            },
            setting_metadata={
                "host": SourceFieldMetadata(
                    field="host",
                    label="Gateway Host",
                    help_text="Host where TWS or IB Gateway API is running. This source uses authenticated read-only market-data calls only.",
                    placeholder=default_ibkr_host(),
                    safety_warning="IBKR data-source access is read-only; order placement, cancellation, replacement, and flatten helpers are not reachable from this path.",
                    validation_regex=_HOST_VALIDATION_REGEX,
                    validation_hint="Use a hostname or IP address that the runtime can reach.",
                    input_type="text",
                ),
                "port": SourceFieldMetadata(
                    field="port",
                    label="Gateway Port",
                    help_text="TWS/Gateway API port, commonly 7497 for paper and 7496 for live.",
                    placeholder="7497",
                    safety_warning="The test performs an authenticated read-only historical-data read, not a socket-only check and not an order operation.",
                    validation_regex=_INTEGER_VALIDATION_REGEX,
                    validation_hint="Use the numeric API port configured in TWS or IB Gateway.",
                    input_type="number",
                ),
                "client_id": SourceFieldMetadata(
                    field="client_id",
                    label="Client ID",
                    help_text="IBKR API client ID for this data-source session. Use an ID that does not conflict with execution or other tools.",
                    placeholder="67",
                    validation_regex=_INTEGER_VALIDATION_REGEX,
                    validation_hint="Use a non-negative integer client ID.",
                    input_type="number",
                ),
                "market_data_type": SourceFieldMetadata(
                    field="market_data_type",
                    label="Market Data Type",
                    help_text="IBKR market-data type passed before the historical-data read: 1 live, 2 frozen, 3 delayed, or 4 delayed frozen.",
                    placeholder="1",
                    validation_regex=r"^[1-4]$",
                    validation_hint="Use 1, 2, 3, or 4.",
                    input_type="number",
                ),
                "currency": SourceFieldMetadata(
                    field="currency",
                    label="Contract Currency",
                    help_text="Currency for the SPY SMART stock contract used by the read-only historical-data probe.",
                    placeholder="USD",
                    validation_regex=_CURRENCY_VALIDATION_REGEX,
                    validation_hint="Use a three-letter uppercase currency code.",
                    input_type="text",
                ),
            },
            guide=_source_guide(
                category="Broker Connectivity",
                summary="Connects to Interactive Brokers for broker-backed market-data streaming.",
                needs=(
                    "IBKR Gateway or TWS running.",
                    "Host, port, and client ID that match the local IBKR setup.",
                    "Market-data permissions for the instrument used by the read-only probe.",
                ),
                setup=(
                    "Enter the host, port, and client ID.",
                    "Run Test Connection to verify authenticated historical-data access.",
                    "Enable only when the IBKR service is intentionally running.",
                ),
                when_enabled="The runtime can connect to IBKR for broker-side market data.",
                docs_url="https://interactivebrokers.github.io/tws-api/initial_setup.html",
                signup_url="https://www.interactivebrokers.com/",
                plan_note="IBKR market-data subscriptions are managed in the IBKR account.",
                safety_warnings=(
                    "This control plane uses IBKR for read-only connectivity checks and market data; it does not place, cancel, replace, or flatten orders.",
                ),
            ),
        ),
        "alpaca_broker_data": SourceDefinition(
            source_type="broker_data_provider",
            display_name="Alpaca Broker Data",
            provider_name="alpaca",
            job_name="alpaca_broker_data_readonly",
            default_enabled=False,
            credential_env={
                "key_id": "ALPACA_KEY_ID",
                "secret_key": "ALPACA_SECRET_KEY",
            },
            setting_env={
                "base_url": "ALPACA_BASE_URL",
                "stream_url": "ALPACA_STREAM_URL",
                "trade_updates_ws_enabled": "ALPACA_TRADE_UPDATES_WS_ENABLED",
            },
            credential_metadata={
                "key_id": SourceFieldMetadata(
                    field="key_id",
                    label="Key ID",
                    help_text="Alpaca API key ID for read-only account, positions, and open-order listing probes.",
                    placeholder="Enter new key ID; leave blank to preserve",
                    safety_warning="This credential is used by the data-source control plane only for broker-data reads.",
                    input_type="password",
                    secret=True,
                    required=True,
                    validation_regex=_SECRET_VALUE_VALIDATION_REGEX,
                    validation_hint="Use a single-line Alpaca key ID.",
                ),
                "secret_key": SourceFieldMetadata(
                    field="secret_key",
                    label="Secret Key",
                    help_text="Alpaca API secret for read-only account, positions, and open-order listing probes.",
                    placeholder="Enter new secret; leave blank to preserve",
                    safety_warning="This path cannot submit, cancel, replace, or flatten orders.",
                    input_type="password",
                    secret=True,
                    required=True,
                    validation_regex=_SECRET_VALUE_VALIDATION_REGEX,
                    validation_hint="Use a single-line Alpaca secret key.",
                ),
            },
            setting_metadata={
                "base_url": SourceFieldMetadata(
                    field="base_url",
                    label="Base URL",
                    help_text=f"Alpaca Trading API base URL for read-only broker-data probes. The safe default is {ALPACA_PAPER_BASE_URL}.",
                    placeholder=ALPACA_PAPER_BASE_URL,
                    safety_warning=f"Live Alpaca base URL is blocked unless {ALLOW_LIVE_ALPACA_BROKER_DATA_ENV}=1 is set intentionally.",
                    validation_regex=_HTTP_URL_VALIDATION_REGEX,
                    validation_hint="Use an http or https URL with no spaces.",
                    input_type="url",
                ),
                "stream_url": SourceFieldMetadata(
                    field="stream_url",
                    label="Trade Updates Stream URL",
                    help_text="Optional WebSocket URL used only for observing trade updates when separately enabled.",
                    placeholder="wss://paper-api.alpaca.markets/stream",
                    safety_warning="Trade-update observation is read-only and does not grant order authority.",
                    validation_regex=_HTTP_URL_VALIDATION_REGEX,
                    validation_hint="Use a ws, wss, http, or https URL with no spaces.",
                    input_type="url",
                ),
                "trade_updates_ws_enabled": SourceFieldMetadata(
                    field="trade_updates_ws_enabled",
                    label="Observe Trade Updates",
                    help_text="Optional read-only observation flag for Alpaca trade updates.",
                    placeholder="0",
                    safety_warning="Leave disabled unless the account and endpoint prerequisites are understood.",
                    validation_regex=_BOOLEAN_VALIDATION_REGEX,
                    validation_hint="Use 1/0, true/false, yes/no, or on/off.",
                    input_type="text",
                ),
            },
            guide=_source_guide(
                category="Broker Data",
                summary="Tracks Alpaca account and position status as a read-only source.",
                needs=(
                    "Alpaca broker API credentials used only for broker-data reads.",
                    f"Paper endpoint {ALPACA_PAPER_BASE_URL} unless live read-only account visibility is explicitly approved.",
                ),
                setup=(
                    "Configure the Alpaca data provider account or source-level credentials.",
                    "Keep the paper base URL unless live read-only broker-data visibility is intentionally approved.",
                    "Keep order authority configured through the broker execution control plane.",
                    "Enable only when read-only broker data should be visible in source status.",
                ),
                when_enabled="The catalog records Alpaca as an available read-only broker-data source; it does not schedule order, cancel, replace, or flatten paths.",
                docs_url="https://docs.alpaca.markets/docs/trading-api",
                signup_url="https://alpaca.markets/",
                plan_note="Broker data access depends on the Alpaca account and endpoint selection.",
                safety_warnings=(
                    "This data-source entry is read-only and is intentionally separated from broker order authority.",
                    "Enabling this source does not authorize live orders, cancels, replacements, or flatten operations.",
                ),
            ),
            runtime_runnable=False,
        ),
        "yfinance": SourceDefinition(
            source_type="price_provider",
            display_name="Yahoo Finance",
            provider_name="yfinance",
            job_name="poll_prices",
            default_enabled=True,
            guide=_source_guide(
                category="Market Data",
                summary="Provides Yahoo Finance polling as a low-friction backup market-data source.",
                needs=("No credentials are required.",),
                setup=(
                    "Enable the source if Yahoo polling should be available.",
                    "Run Test Connection for a quick connectivity check.",
                ),
                when_enabled="The runtime can use Yahoo Finance as a backup polling source.",
                docs_url="https://pypi.org/project/yfinance/",
                plan_note="Public access can change or rate limit without notice.",
            ),
        ),
        "simulated": SourceDefinition(
            source_type="price_provider",
            display_name="Simulated Local Prices",
            provider_name="simulated",
            job_name="poll_prices",
            default_enabled=False,
            setting_env={"symbols": "SIMULATED_MARKET_DATA_SYMBOLS"},
            setting_metadata={
                "symbols": SourceFieldMetadata(
                    field="symbols",
                    label="Symbols",
                    help_text="Optional comma-separated symbols generated by the local deterministic safe/sim feed.",
                    placeholder="SPY,AAPL,MSFT,QQQ",
                    validation_regex=r"^[A-Za-z0-9_,\s.-]*$",
                    validation_hint="Use comma-separated market symbols.",
                    input_type="text",
                ),
            },
            guide=_source_guide(
                category="Market Data",
                summary="Generates deterministic local price rows for safe/sim ingestion without external provider credentials.",
                needs=("No credentials are required.",),
                setup=(
                    "Use only in safe/sim mode or for local ingestion validation.",
                    "Optionally set a small symbol list.",
                    "Run Populate Now or start poll_prices to write fresh simulated prices.",
                ),
                when_enabled="The runtime can populate fresh price rows explicitly marked as simulated.",
                plan_note="This is not a production provider and must not be treated as live market-data success.",
                safety_warnings=(
                    "Rows are synthetic and carry provider/source simulated.",
                    "This source has no broker, exchange, order, cancel, replace, or flatten authority.",
                ),
            ),
            storage_tables=("prices", "price_quotes", "price_quotes_raw", "price_provider_health"),
            consumers=("price_router", "dashboard_data_health", "safe_sim_validation"),
            safe_to_auto_enable=True,
        ),
        "ccxt": SourceDefinition(
            source_type="price_provider",
            display_name="CCXT",
            provider_name="ccxt",
            job_name="poll_prices",
            default_enabled=True,
            setting_env={"exchange_id": "CCXT_EXCHANGE_ID"},
            guide=_source_guide(
                category="Market Data",
                summary="Provides crypto price polling through CCXT public exchange endpoints.",
                needs=("No credentials are required for the default public polling path.",),
                setup=(
                    "Enable the source if crypto price polling should be available.",
                    "Set an exchange id only when the default exchange should be changed.",
                    "Run Test Connection to confirm public exchange reachability.",
                ),
                when_enabled="The runtime can poll public crypto market data.",
                docs_url="https://docs.ccxt.com/",
                plan_note="Public exchange endpoints can rate limit or block heavy polling.",
            ),
        ),
        "tradier": SourceDefinition(
            source_type="options_provider",
            display_name="Tradier Options",
            provider_name="tradier",
            job_name="options_poll",
            default_enabled=True,
            credential_env={"api_token": "TRADIER_API_TOKEN"},
            guide=_source_guide(
                category="Options",
                summary="Provides options-chain polling through Tradier.",
                needs=("A Tradier API token.",),
                setup=(
                    "Enter the Tradier API token.",
                    "Save and run Test Connection.",
                    "Enable when options data should come from Tradier.",
                ),
                when_enabled="The runtime can poll options expirations and option-chain data.",
                docs_url="https://documentation.tradier.com/brokerage-api",
                signup_url="https://developer.tradier.com/",
                plan_note="Options market-data access depends on the Tradier account and entitlements.",
            ),
        ),
        "polygon_options": SourceDefinition(
            source_type="options_provider",
            display_name="Polygon Options",
            provider_name="polygon",
            job_name="options_poll",
            default_enabled=True,
            credential_env={"api_key": "POLYGON_API_KEY"},
            guide=_source_guide(
                category="Options",
                summary="Provides options-chain polling through Polygon snapshots.",
                needs=("A Polygon API key with options snapshot access.",),
                setup=(
                    "Configure the Polygon provider account or enter a source-level API key.",
                    "Save and run Test Connection.",
                    "Disable this source when Polygon should not be part of the options provider chain.",
                ),
                when_enabled="The runtime can poll Polygon option-chain snapshots through options_poll.",
                docs_url="https://polygon.io/docs/options/get_v3_snapshot_options__underlyingasset",
                signup_url="https://polygon.io/pricing",
                plan_note="Options coverage and rate limits depend on the active Polygon plan.",
            ),
        ),
        "reddit": SourceDefinition(
            source_type="social_provider",
            display_name="Reddit",
            provider_name="reddit",
            job_name="poll_social_reddit",
            credential_env={
                "client_id": "REDDIT_CLIENT_ID",
                "client_secret": "REDDIT_CLIENT_SECRET",
            },
            setting_env={
                "user_agent": "REDDIT_USER_AGENT",
                "subreddits": "REDDIT_SUBREDDITS",
                "poll_limit": "REDDIT_POLL_LIMIT",
                "sleep_s": "SOCIAL_POLL_SLEEP_S",
            },
            guide=_source_guide(
                category="Social Sentiment",
                summary="Polls Reddit to gather social sentiment from configured communities.",
                needs=("A Reddit client ID.", "A Reddit client secret."),
                setup=(
                    "Enter the Reddit client ID and client secret.",
                    "Optionally adjust subreddits and user agent in Settings.",
                    "Save and run Test Connection.",
                ),
                when_enabled="The runtime can collect Reddit sentiment and discussion signals.",
                docs_url="https://www.reddit.com/dev/api/",
                signup_url="https://www.reddit.com/prefs/apps",
                plan_note="Reddit API access and rate limits depend on Reddit's developer terms.",
            ),
        ),
        "stocktwits": SourceDefinition(
            source_type="social_provider",
            display_name="StockTwits",
            provider_name="stocktwits",
            job_name="poll_social_stocktwits",
            setting_env={
                "trending_url": "STOCKTWITS_TRENDING_URL",
                "symbol_url_template": "STOCKTWITS_SYMBOL_URL_TMPL",
                "timeout_s": "STOCKTWITS_TIMEOUT_S",
                "sleep_s": "SOCIAL_POLL_SLEEP_S",
            },
            guide=_source_guide(
                category="Social Sentiment",
                summary="Polls Stocktwits trending and symbol streams.",
                needs=("No stored credentials are required for the default public endpoint.",),
                setup=(
                    "Enable the source.",
                    "Run Test Connection.",
                    "If access is blocked, review the last error and decide whether to disable it.",
                ),
                when_enabled="The runtime can gather public Stocktwits sentiment context.",
                docs_url="https://api.stocktwits.com/developers/docs",
                plan_note="Public endpoints can rate limit or change availability.",
            ),
        ),
        "company_news": SourceDefinition(
            source_type="news_provider",
            display_name="Finnhub Company News",
            provider_name="company_news",
            job_name="ingest_now",
            default_enabled=True,
            credential_env={"api_key": "FINNHUB_API_KEY"},
            setting_env={
                "symbol_limit": "COMPANY_NEWS_SYMBOL_LIMIT",
                "lookback_days": "COMPANY_NEWS_LOOKBACK_DAYS",
                "max_items_per_symbol": "COMPANY_NEWS_MAX_ITEMS_PER_SYMBOL",
            },
            guide=_source_guide(
                category="News",
                summary="Pulls company-specific news through Finnhub.",
                needs=("A Finnhub API key.",),
                setup=(
                    "Enter the Finnhub API key.",
                    "Optionally adjust symbol limit and lookback window.",
                    "Save and run Test Connection.",
                ),
                when_enabled="The runtime can ingest company-level news for tracked symbols.",
                docs_url="https://finnhub.io/docs/api/company-news",
                signup_url="https://finnhub.io/register",
                plan_note="Free and paid tiers have different rate limits and data coverage.",
            ),
        ),
        "transcripts": SourceDefinition(
            source_type="news_provider",
            display_name="FMP Transcripts",
            provider_name="transcripts",
            job_name="ingest_now",
            default_enabled=True,
            credential_env={"api_key": "FMP_API_KEY"},
            setting_env={"max_items_per_symbol": "TRANSCRIPTS_MAX_ITEMS_PER_SYMBOL"},
            guide=_source_guide(
                category="News",
                summary="Fetches company transcripts through Financial Modeling Prep.",
                needs=("An FMP API key.",),
                setup=(
                    "Enter the FMP API key.",
                    "Save and run Test Connection.",
                ),
                when_enabled="The runtime can ingest transcripts for supported symbols.",
                docs_url="https://site.financialmodelingprep.com/developer/docs",
                signup_url="https://site.financialmodelingprep.com/developer/docs/pricing",
                plan_note="Transcript access depends on the active FMP plan.",
            ),
        ),
        "gdelt": SourceDefinition(
            source_type="news_provider",
            display_name="GDELT News",
            provider_name="gdelt",
            job_name="poll_gdelt",
            setting_env={
                "lookback_minutes": "GDELT_LOOKBACK_MINUTES",
                "maxrecords": "GDELT_MAXRECORDS",
                "symbol_limit": "GDELT_SYMBOL_LIMIT",
                "language": "GDELT_LANGUAGE",
            },
            guide=_source_guide(
                category="News",
                summary="Queries GDELT for broad market and macro news coverage.",
                needs=("No credentials are required.",),
                setup=(
                    "Enable the source if GDELT news should be in the pipeline.",
                    "Run Test Connection.",
                    "If it rate limits, wait or disable it.",
                ),
                when_enabled="The runtime can pull broad market news and article references.",
                docs_url="https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/",
                plan_note="GDELT is public, but availability and throttling are external to this system.",
            ),
        ),
        "sec": SourceDefinition(
            source_type="filings_provider",
            display_name="SEC Filings",
            provider_name="sec",
            job_name="poll_sec_filings",
            setting_env={
                "user_agent": "SEC_USER_AGENT",
                "from": "SEC_FROM",
                "forms_allow": "SEC_FORMS_ALLOW",
                "symbol_limit": "SEC_SYMBOL_LIMIT",
                "per_symbol_limit": "SEC_PER_SYMBOL_LIMIT",
            },
            guide=_source_guide(
                category="Filings",
                summary="Polls SEC filing data for tracked companies.",
                needs=("A proper SEC user agent and contact details when customizing caller identity.",),
                setup=(
                    "Review the source settings.",
                    "Set a descriptive user agent and contact email when overriding defaults.",
                    "Run Test Connection, then enable only if the SEC path is healthy.",
                ),
                when_enabled="The runtime can ingest SEC filings and filing-related events.",
                docs_url="https://www.sec.gov/os/accessing-edgar-data",
                plan_note="SEC EDGAR access is public but subject to fair-access policies.",
                safety_warnings=("Use a real contact identity for SEC access policy compliance.",),
            ),
        ),
        "form4": SourceDefinition(
            source_type="filings_provider",
            display_name="SEC Form 4 Insider Trades",
            provider_name="form4",
            job_name="ingest_form4",
            default_enabled=False,
            setting_env={
                "user_agent": "SEC_USER_AGENT",
                "from": "SEC_FROM",
                "backfill_days": "FORM4_BACKFILL_DAYS",
                "poll_seconds": "FORM4_POLL_SECONDS",
                "filing_limit": "FORM4_FILING_LIMIT",
                "symbol_limit": "FORM4_SYMBOL_LIMIT",
            },
            guide=_source_guide(
                category="Filings",
                summary="Ingests SEC Form 4 insider-trading filings for tracked companies.",
                needs=("SEC caller identity settings when overriding defaults.",),
                setup=(
                    "Review SEC user agent and contact settings.",
                    "Adjust backfill, poll interval, and filing limits if needed.",
                    "Enable only after the SEC filing path is healthy.",
                ),
                when_enabled="The runtime can ingest Form 4 insider transaction events.",
                docs_url="https://www.sec.gov/os/accessing-edgar-data",
                plan_note="SEC EDGAR access is public but subject to fair-access policies.",
                safety_warnings=("Keep polling limits conservative to respect SEC fair-access rules.",),
            ),
        ),
        "inst_13f": SourceDefinition(
            source_type="filings_provider",
            display_name="SEC 13F Holdings",
            provider_name="inst_13f",
            job_name="ingest_13f",
            default_enabled=False,
            setting_env={
                "user_agent": "SEC_USER_AGENT",
                "from": "SEC_FROM",
                "poll_seconds": "INST_13F_POLL_SECONDS",
            },
            guide=_source_guide(
                category="Filings",
                summary="Ingests institutional 13F filings and optionally maps CUSIPs through reference providers.",
                needs=(
                    "SEC caller identity for EDGAR access.",
                    "Optional Polygon and FMP account credentials for CUSIP lookup fallback.",
                ),
                setup=(
                    "Configure the SEC identity account.",
                    "Configure Polygon and FMP accounts if CUSIP fallback lookup should be available.",
                    "Enable only after the 13F filing path is intentionally active.",
                ),
                when_enabled="The runtime can ingest SEC 13F filings and map holdings symbols on a point-in-time basis.",
                docs_url="https://www.sec.gov/os/accessing-edgar-data",
                plan_note="13F filings are public; CUSIP lookup fallback coverage depends on Polygon and FMP plans.",
                safety_warnings=("Keep polling daily or slower unless intentionally backfilling a filing window.",),
            ),
        ),
        "congressional_trades": SourceDefinition(
            source_type="legislative_provider",
            display_name="Congressional Trades",
            provider_name="congressional_trades",
            job_name="ingest_congressional_trades",
            default_enabled=False,
            setting_env={
                "backfill_days": "CONGRESSIONAL_BACKFILL_DAYS",
                "poll_seconds": "CONGRESSIONAL_POLL_SECONDS",
                "senate_source_url": "CONGRESSIONAL_SENATE_SOURCE_URL",
                "house_source_url": "CONGRESSIONAL_HOUSE_SOURCE_URL",
            },
            guide=_source_guide(
                category="Legislative",
                summary="Ingests public congressional trade-disclosure data.",
                needs=("No stored credentials are required for the default public sources.",),
                setup=(
                    "Review the Senate and House source URLs.",
                    "Adjust backfill and poll interval if needed.",
                    "Enable when legislative trade data should be ingested.",
                ),
                when_enabled="The runtime can ingest congressional trade-disclosure events.",
                docs_url="https://efdsearch.senate.gov/search/",
                plan_note="Public disclosure sources can change layout or availability.",
            ),
        ),
        "etf_flows": SourceDefinition(
            source_type="fundamentals_provider",
            display_name="ETF Shares Outstanding",
            provider_name="etf_flows",
            job_name="ingest_etf_flows",
            default_enabled=False,
            setting_env={
                "poll_seconds": "ETF_FLOW_POLL_SECONDS",
            },
            guide=_source_guide(
                category="Fundamentals",
                summary="Ingests ETF shares-outstanding data through Polygon with FMP as a fallback.",
                needs=(
                    "A Polygon account credential for primary ETF share data.",
                    "An FMP account credential when fallback share data should be available.",
                ),
                setup=(
                    "Configure the Polygon and FMP provider accounts.",
                    "Enable only when ETF flow ingestion should run.",
                    "Review poll cadence before starting the supervised job.",
                ),
                when_enabled="The runtime can compute ETF flow features from point-in-time shares-outstanding changes.",
                docs_url="https://polygon.io/docs/stocks/get_v3_reference_tickers__ticker",
                signup_url="https://polygon.io/pricing",
                plan_note="ETF coverage depends on the active Polygon and FMP plans.",
            ),
        ),
        "cftc_cot": SourceDefinition(
            source_type="positioning_provider",
            display_name="CFTC COT Positioning",
            provider_name="cftc_cot",
            job_name="ingest_cftc_cot",
            default_enabled=False,
            setting_env={
                "poll_seconds": "CFTC_COT_POLL_SECONDS",
                "public_reporting_domain": "CFTC_PUBLIC_REPORTING_DOMAIN",
                "legacy_dataset_id": "CFTC_COT_LEGACY_DATASET_ID",
                "disaggregated_dataset_id": "CFTC_COT_DISAGG_DATASET_ID",
                "request_timeout_s": "CFTC_COT_REQUEST_TIMEOUT_S",
                "feature_lookback_days": "CFTC_COT_FEATURE_LOOKBACK_DAYS",
                "contracts_json": "CFTC_COT_CONTRACTS_JSON",
            },
            guide=_source_guide(
                category="Positioning",
                summary="Ingests public CFTC Commitments of Traders positioning data for configured futures contracts.",
                needs=("No credentials are required for the default CFTC public reporting API.",),
                setup=(
                    "Review contract mappings and Socrata dataset IDs.",
                    "Run Test Connection against the public reporting API.",
                    "Enable only when weekly COT positioning should be ingested.",
                ),
                when_enabled="The runtime can run ingest_cftc_cot and materialize COT positioning features.",
                docs_url="https://publicreporting.cftc.gov/",
                plan_note="CFTC data is public, but availability can be delayed by publication schedules and holidays.",
                safety_warnings=("COT is alternate/regime data; keep it default-off until consumers are intentionally enabled.",),
            ),
        ),
        "finra_short_volume": SourceDefinition(
            source_type="short_interest_provider",
            display_name="FINRA Short Volume",
            provider_name="finra_short_volume",
            job_name="ingest_finra_short_volume",
            default_enabled=False,
            setting_env={
                "poll_seconds": "FINRA_SHORT_VOLUME_POLL_SECONDS",
                "backfill_days": "FINRA_SHORT_VOLUME_BACKFILL_DAYS",
                "url_template": "FINRA_SHORT_VOLUME_URL_TEMPLATE",
                "request_timeout_s": "FINRA_REQUEST_TIMEOUT_S",
            },
            guide=_source_guide(
                category="Short Interest",
                summary="Ingests FINRA daily short-sale volume files.",
                needs=("No credentials are required for the default FINRA public file source.",),
                setup=(
                    "Review the URL template, backfill window, and poll cadence.",
                    "Run Test Connection to validate the public file endpoint.",
                    "Enable only when FINRA daily short-volume features should be populated.",
                ),
                when_enabled="The runtime can run ingest_finra_short_volume and populate daily short-sale volume rows.",
                docs_url="https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data",
                plan_note="FINRA daily files are public but have publication lag and can be unavailable for non-trading days.",
                safety_warnings=("Short-volume data is alternate data; keep it default-off until PIT feature consumers are ready.",),
            ),
        ),
        "finra_short_interest": SourceDefinition(
            source_type="short_interest_provider",
            display_name="FINRA Short Interest",
            provider_name="finra_short_interest",
            job_name="ingest_finra_short_interest",
            default_enabled=False,
            setting_env={
                "poll_seconds": "FINRA_SHORT_INTEREST_POLL_SECONDS",
                "query_limit": "FINRA_SHORT_INTEREST_QUERY_LIMIT",
                "max_pages": "FINRA_SHORT_INTEREST_MAX_PAGES",
                "symbol_limit": "FINRA_SHORT_INTEREST_SYMBOL_LIMIT",
                "api_url": "FINRA_SHORT_INTEREST_API_URL",
                "request_timeout_s": "FINRA_REQUEST_TIMEOUT_S",
            },
            guide=_source_guide(
                category="Short Interest",
                summary="Ingests FINRA equity short-interest records from the public Query API.",
                needs=("No credentials are required for the default FINRA Query API source.",),
                setup=(
                    "Review query limits, symbol limit, and API URL.",
                    "Run Test Connection against the FINRA Query API.",
                    "Enable only when bi-monthly short-interest features should be populated.",
                ),
                when_enabled="The runtime can run ingest_finra_short_interest and populate FINRA short-interest rows.",
                docs_url="https://www.finra.org/finra-data/browse-catalog/equity-short-interest",
                plan_note="FINRA short-interest dissemination is periodic and can change field names over time.",
                safety_warnings=("Short-interest data is alternate data; keep it default-off until PIT feature consumers are ready.",),
            ),
        ),
        "crypto_funding": SourceDefinition(
            source_type="positioning_provider",
            display_name="Crypto Funding",
            provider_name="crypto_funding",
            job_name="ingest_crypto_funding",
            default_enabled=False,
            setting_env={
                "poll_seconds": "CRYPTO_FUNDING_POLL_SECONDS",
                "align_to_settlement_marks": "CRYPTO_FUNDING_ALIGN_TO_SETTLEMENT_MARKS",
                "settlement_hours_utc": "CRYPTO_FUNDING_SETTLEMENT_HOURS_UTC",
                "settlement_lag_seconds": "CRYPTO_FUNDING_SETTLEMENT_LAG_SECONDS",
                "history_lookback_hours": "CRYPTO_FUNDING_HISTORY_LOOKBACK_HOURS",
                "history_limit": "CRYPTO_FUNDING_HISTORY_LIMIT",
                "perp_markets": "CRYPTO_PERP_MARKETS",
                "funding_exchange_id": "CCXT_FUNDING_EXCHANGE_ID",
            },
            guide=_source_guide(
                category="Positioning",
                summary="Ingests crypto perpetual funding and basis data through public CCXT exchange endpoints.",
                needs=("No credentials are required for the default public CCXT funding path.",),
                setup=(
                    "Configure the exchange id and perpetual market map if defaults are not sufficient.",
                    "Run Test Connection to validate CCXT availability and market configuration.",
                    "Enable only when crypto funding features should be populated.",
                ),
                when_enabled="The runtime can run ingest_crypto_funding and populate crypto funding-rate rows.",
                docs_url="https://docs.ccxt.com/",
                plan_note="Exchange endpoint support varies; missing public funding endpoints are skipped by the job.",
                safety_warnings=("Crypto funding is alternate data; keep it default-off until consumers are intentionally enabled.",),
            ),
        ),
        "quiver_gov": SourceDefinition(
            source_type="legislative_provider",
            display_name="Quiver Government Flow",
            provider_name="quiver_gov",
            job_name="ingest_quiver_gov",
            default_enabled=False,
            credential_env={"api_key": "QUIVER_API_KEY"},
            setting_env={
                "poll_seconds": "QUIVER_GOV_POLL_SECONDS",
                "base_url": "QUIVER_BASE_URL",
                "auth_scheme": "QUIVER_AUTH_SCHEME",
                "congress_endpoint": "QUIVER_CONGRESS_ENDPOINT",
                "lobbying_endpoint": "QUIVER_LOBBYING_ENDPOINT",
                "contracts_endpoint": "QUIVER_CONTRACTS_ENDPOINT",
            },
            guide=_source_guide(
                category="Legislative",
                summary="Ingests government-flow datasets through Quiver.",
                needs=("A Quiver API key when using authenticated endpoints.",),
                setup=(
                    "Enter the Quiver API key.",
                    "Review base URL, auth scheme, and endpoint settings.",
                    "Save and run Test Connection if the provider path supports it.",
                ),
                when_enabled="The runtime can ingest Quiver government-flow events.",
                docs_url="https://api.quiverquant.com/docs/",
                signup_url="https://www.quiverquant.com/",
                plan_note="Dataset access depends on the Quiver plan and endpoint entitlement.",
            ),
        ),
        "fundamentals_pit": SourceDefinition(
            source_type="fundamentals_provider",
            display_name="PIT Fundamentals",
            provider_name="fundamentals_pit",
            job_name="ingest_fundamentals_pit",
            default_enabled=False,
            credential_env={
                "simfin_api_key": "SIMFIN_API_KEY",
                "sharadar_api_key": "SHARADAR_API_KEY",
            },
            setting_env={
                "poll_seconds": "FUNDAMENTALS_PIT_POLL_SECONDS",
                "mode": "FUNDAMENTALS_PIT_MODE",
                "simfin_bulk_url": "SIMFIN_BULK_URL",
                "sharadar_bulk_url": "SHARADAR_BULK_URL",
                "rate_limit_sleep_s": "FUNDAMENTALS_PIT_RATE_LIMIT_SLEEP_S",
            },
            guide=_source_guide(
                category="Fundamentals",
                summary="Ingests point-in-time fundamentals from configured fundamentals vendors.",
                needs=("Vendor API keys for the fundamentals source modes you enable.",),
                setup=(
                    "Enter SimFin and/or Sharadar API keys for the selected mode.",
                    "Review mode, bulk URLs, poll interval, and rate-limit sleep settings.",
                    "Enable only after vendor access and rate limits are understood.",
                ),
                when_enabled="The runtime can ingest point-in-time fundamentals for research and features.",
                docs_url="https://www.simfin.com/en/api",
                signup_url="https://www.simfin.com/",
                plan_note="Coverage, bulk downloads, and rate limits depend on vendor plans.",
            ),
        ),
        "earnings": SourceDefinition(
            source_type="calendar_provider",
            display_name="FMP Earnings",
            provider_name="earnings",
            job_name="poll_earnings",
            credential_env={"api_key": "FMP_API_KEY"},
            setting_env={"lookahead_days": "EARNINGS_LOOKAHEAD_DAYS"},
            guide=_source_guide(
                category="Calendar",
                summary="Pulls upcoming earnings events through Financial Modeling Prep.",
                needs=("An FMP API key.",),
                setup=(
                    "Enter the FMP API key.",
                    "Optionally adjust the lookahead window.",
                    "Save and run Test Connection.",
                ),
                when_enabled="The runtime can ingest earnings calendar events.",
                docs_url="https://site.financialmodelingprep.com/developer/docs",
                signup_url="https://site.financialmodelingprep.com/developer/docs/pricing",
                plan_note="Calendar coverage and rate limits depend on the active FMP plan.",
            ),
        ),
        "weather_forecasts": SourceDefinition(
            source_type="weather_provider",
            display_name="Weather Forecasts",
            provider_name="weather_forecasts",
            job_name="poll_weather_forecasts",
            setting_env={
                "provider": "WEATHER_PROVIDER",
                "poll_seconds": "WEATHER_POLL_SECONDS",
            },
            guide=_source_guide(
                category="Weather",
                summary="Pulls weather forecasts for configured regions.",
                needs=("No credentials are required for the default provider.",),
                setup=(
                    "Enable the source if weather forecasts should be ingested.",
                    "Adjust provider and poll interval only when needed.",
                    "Run Test Connection.",
                ),
                when_enabled="The runtime can ingest forecast data for weather-aware models.",
                docs_url="https://open-meteo.com/en/docs",
                plan_note="Default public weather endpoints can throttle or change availability.",
            ),
        ),
        "weather_alerts": SourceDefinition(
            source_type="weather_provider",
            display_name="Weather Alerts",
            provider_name="weather_alerts",
            job_name="poll_weather_alerts",
            setting_env={
                "provider": "WEATHER_ALERTS_PROVIDER",
                "poll_seconds": "WEATHER_ALERTS_POLL_SECONDS",
                "http_ua": "WEATHER_HTTP_UA",
            },
            guide=_source_guide(
                category="Weather",
                summary="Pulls active weather alerts from the configured alerts provider.",
                needs=("No credentials are required for the default provider.",),
                setup=(
                    "Enable the source if alert ingestion should be active.",
                    "Set a descriptive HTTP user agent when using weather.gov.",
                    "Run Test Connection.",
                ),
                when_enabled="The runtime can ingest active weather alerts.",
                docs_url="https://www.weather.gov/documentation/services-web-api",
                plan_note="Weather.gov access is public but requires a descriptive user agent.",
                safety_warnings=("Use a real contact identity in WEATHER_HTTP_UA for weather.gov policy compliance.",),
            ),
        ),
        "macro": SourceDefinition(
            source_type="macro_provider",
            display_name="Macro Factors",
            provider_name="macro",
            job_name="poll_macro",
            default_enabled=True,
            setting_env={
                "poll_seconds": "MACRO_POLL_SECONDS",
            },
            guide=_source_guide(
                category="Macro",
                summary="Builds macro factor snapshots used by the strategy layer.",
                needs=("No credentials are required.",),
                setup=("Leave enabled unless macro ingestion is intentionally disabled.",),
                when_enabled="The runtime can refresh macro factor data for models and dashboards.",
                docs_url="https://fred.stlouisfed.org/docs/api/fred/",
                plan_note="Default macro inputs are treated as public or locally configured feeds.",
            ),
        ),
        "model_feature_snapshots": SourceDefinition(
            source_type="feature_snapshot",
            display_name="Model Feature Snapshots",
            provider_name="model_feature_snapshots",
            job_name="snapshot_model_features",
            default_enabled=True,
            setting_env={
                "sleep_s": "MODEL_FEATURE_SNAPSHOT_SLEEP_S",
                "bucket_sec": "MODEL_FEATURE_SNAPSHOT_BUCKET_SEC",
                "symbol_limit": "MODEL_FEATURE_SNAPSHOT_SYMBOL_LIMIT",
            },
            guide=_source_guide(
                category="Model Support",
                summary="Captures feature snapshots used for diagnostics and model analysis.",
                needs=("No credentials are required.",),
                setup=("Leave enabled unless feature snapshots are intentionally stopped.",),
                when_enabled="The runtime can capture model feature snapshots for diagnostics.",
                docs_url="https://github.com/",
                plan_note="This is an internal system source and has no external provider plan.",
            ),
        ),
        "news_flow": SourceDefinition(
            source_type="feature_snapshot",
            display_name="News Flow Features",
            provider_name="news_flow",
            job_name="process_news_flow",
            default_enabled=True,
            setting_env={
                "batch_size": "NEWS_FLOW_BATCH_SIZE",
                "embedding_backend": "NEWS_EMBED_BACKEND",
                "embedding_model": "NEWS_EMBED_OPENAI_MODEL",
            },
            guide=_source_guide(
                category="Model Support",
                summary="Computes news-flow novelty and staleness features from already-ingested news events.",
                needs=(
                    "No credentials are required for the default hashing backend.",
                    "An OpenAI account credential is required only when the OpenAI embedding backend is selected.",
                ),
                setup=(
                    "Leave enabled for internal news-flow feature materialization.",
                    "Configure the OpenAI account only if NEWS_EMBED_BACKEND is set to openai.",
                    "Adjust batch size or embedding settings only for controlled experiments.",
                ),
                when_enabled="The runtime can materialize backend-aware news-flow features for model inputs.",
                docs_url="https://platform.openai.com/docs/guides/embeddings",
                plan_note="OpenAI embeddings are optional and used only when the configured backend selects them.",
            ),
        ),
        "rss_feed": SourceDefinition(
            source_type="rss_feed",
            display_name="RSS Feed",
            provider_name="rss",
            job_name="ingest_now",
            singleton=False,
            guide=_source_guide(
                category="News",
                summary="A custom RSS feed managed directly from this page.",
                needs=("A feed name.", "A feed URL."),
                setup=(
                    "Use Add RSS Feed.",
                    "Enter the feed name and feed URL.",
                    "Save and run Test Connection.",
                ),
                when_enabled="The runtime can ingest articles from that RSS feed.",
                docs_url="https://www.rssboard.org/rss-specification",
                plan_note="RSS feeds are usually free, but each publisher controls availability and terms.",
            ),
        ),
    }


def _provider_account_catalog() -> Dict[str, ProviderAccountDefinition]:
    return {
        "polygon": ProviderAccountDefinition(
            account_key="polygon",
            display_name="Polygon",
            provider_name="polygon",
            credential_env={"api_key": "POLYGON_API_KEY"},
            used_by_sources=("polygon", "polygon_ws", "polygon_options", "etf_flows", "inst_13f"),
            guide=_source_guide(
                category="Provider Account",
                summary="Shared Polygon API key inherited by REST, WebSocket, explicit options, ETF flow, and CUSIP lookup paths.",
                needs=("A Polygon API key.",),
                setup=("Enter the Polygon API key once, then enable the dependent feeds that should use it.",),
                when_enabled="Dependent feeds inherit POLYGON_API_KEY unless a source-level override is explicitly saved.",
                docs_url="https://polygon.io/docs",
                signup_url="https://polygon.io/pricing",
                plan_note="REST, WebSocket, options/reference, and ETF coverage depend on the active Polygon plan.",
            ),
        ),
        "oanda": ProviderAccountDefinition(
            account_key="oanda",
            display_name="OANDA",
            provider_name="oanda",
            credential_env={
                "access_token": "OANDA_ACCESS_TOKEN",
                "api_key": "OANDA_API_KEY",
            },
            used_by_sources=("oanda_fx",),
            guide=_source_guide(
                category="Provider Account",
                summary="Shared OANDA credentials inherited by the read-only FX pricing source.",
                needs=("An OANDA v20 access token.",),
                setup=("Enter the OANDA token once, then enable the OANDA FX source when pricing should run.",),
                when_enabled="Dependent feeds inherit OANDA_ACCESS_TOKEN unless a source-level override is explicitly saved.",
                docs_url="https://developer.oanda.com/rest-live-v20/authentication/",
                plan_note="Instrument availability depends on the OANDA account.",
                safety_warnings=(
                    "This provider account is for read-only market data and does not authorize trading actions.",
                ),
            ),
            credential_metadata={
                "access_token": SourceFieldMetadata(
                    field="access_token",
                    label="Access Token",
                    help_text="Canonical OANDA v20 access token name.",
                    placeholder="Enter new token; leave blank to preserve",
                    safety_warning="Do not enter order-authority credentials here; this account is used only for pricing probes.",
                    secret=True,
                    required=False,
                    validation_regex=_SECRET_VALUE_VALIDATION_REGEX,
                    validation_hint="Use a single-line OANDA access token.",
                    input_type="password",
                ),
                "api_key": SourceFieldMetadata(
                    field="api_key",
                    label="Legacy API Key",
                    help_text="Fallback legacy/alternate env mapping for deployments that already use OANDA_API_KEY.",
                    placeholder="Optional fallback; leave blank to preserve",
                    safety_warning="Prefer OANDA_ACCESS_TOKEN for new deployments.",
                    secret=True,
                    required=False,
                    validation_regex=_SECRET_VALUE_VALIDATION_REGEX,
                    validation_hint="Use a single-line OANDA token only if the canonical field is not used.",
                    input_type="password",
                ),
            },
        ),
        "alpaca_data": ProviderAccountDefinition(
            account_key="alpaca_data",
            display_name="Alpaca Broker Data",
            provider_name="alpaca",
            credential_env={
                "key_id": "ALPACA_KEY_ID",
                "secret_key": "ALPACA_SECRET_KEY",
            },
            credential_metadata={
                "key_id": SourceFieldMetadata(
                    field="key_id",
                    label="Key ID",
                    help_text="Shared Alpaca key ID inherited only by the read-only broker-data source.",
                    placeholder="Enter new key ID; leave blank to preserve",
                    safety_warning="This provider account does not authorize order, cancel, replace, or flatten paths.",
                    secret=True,
                    required=True,
                    validation_regex=_SECRET_VALUE_VALIDATION_REGEX,
                    validation_hint="Use a single-line Alpaca key ID.",
                    input_type="password",
                ),
                "secret_key": SourceFieldMetadata(
                    field="secret_key",
                    label="Secret Key",
                    help_text="Shared Alpaca secret inherited only by the read-only broker-data source.",
                    placeholder="Enter new secret; leave blank to preserve",
                    safety_warning="This provider account is separate from broker execution authority.",
                    secret=True,
                    required=True,
                    validation_regex=_SECRET_VALUE_VALIDATION_REGEX,
                    validation_hint="Use a single-line Alpaca secret key.",
                    input_type="password",
                ),
            },
            used_by_sources=("alpaca_broker_data",),
            guide=_source_guide(
                category="Provider Account",
                summary="Shared Alpaca broker credentials for read-only broker-data status surfaces.",
                needs=("An Alpaca API key ID.", "An Alpaca API secret."),
                setup=(
                    "Enter the Alpaca credentials once for broker-data visibility.",
                    "Use the broker execution control plane for any order authority.",
                ),
                when_enabled="The Alpaca broker-data source inherits credentials for read-only status only.",
                docs_url="https://docs.alpaca.markets/docs/trading-api",
                signup_url="https://alpaca.markets/",
                plan_note="Credentials are not projected to supervised ingestion jobs by this data-source entry.",
                safety_warnings=(
                    "This account is read-only from the data-source control plane and does not authorize order, cancel, replace, or flatten paths.",
                ),
            ),
        ),
        "fmp": ProviderAccountDefinition(
            account_key="fmp",
            display_name="Financial Modeling Prep",
            provider_name="fmp",
            credential_env={"api_key": "FMP_API_KEY"},
            used_by_sources=("transcripts", "earnings", "etf_flows", "inst_13f"),
            guide=_source_guide(
                category="Provider Account",
                summary="Shared FMP API key inherited by transcripts, earnings, ETF fallback, and 13F CUSIP fallback paths.",
                needs=("An FMP API key.",),
                setup=("Enter the FMP API key once, then enable the dependent feeds that should use it.",),
                when_enabled="Dependent feeds inherit FMP_API_KEY unless a source-level override is explicitly saved.",
                docs_url="https://site.financialmodelingprep.com/developer/docs",
                signup_url="https://site.financialmodelingprep.com/developer/docs/pricing",
                plan_note="Transcript, earnings, profile, and CUSIP coverage depends on the active FMP plan.",
            ),
        ),
        "sec_identity": ProviderAccountDefinition(
            account_key="sec_identity",
            display_name="SEC Identity",
            provider_name="sec",
            credential_env={"user_agent": "SEC_USER_AGENT", "from": "SEC_FROM"},
            used_by_sources=("sec", "form4", "inst_13f"),
            guide=_source_guide(
                category="Provider Account",
                summary="Shared SEC caller identity inherited by SEC filings, Form 4, and 13F ingestion.",
                needs=("A descriptive SEC user agent.", "A contact email for SEC fair-access policy."),
                setup=("Enter SEC caller identity once, then override individual SEC feeds only when required.",),
                when_enabled="SEC feeds inherit SEC_USER_AGENT and SEC_FROM unless source settings override them.",
                docs_url="https://www.sec.gov/os/accessing-edgar-data",
                plan_note="SEC EDGAR access is public but requires responsible caller identification.",
                safety_warnings=("Use a real contact identity for SEC access policy compliance.",),
            ),
            credential_metadata={
                "user_agent": SourceFieldMetadata(
                    field="user_agent",
                    label="SEC User Agent",
                    secret=False,
                    required=True,
                    validation_hint="Use a descriptive single-line application and contact string.",
                    validation_regex=r"^[^\r\n]{3,240}$",
                    placeholder="trading-system/1.0 contact@example.com",
                    input_type="text",
                ),
                "from": SourceFieldMetadata(
                    field="from",
                    label="SEC From Email",
                    secret=False,
                    required=False,
                    validation_hint="Use a contact email address.",
                    validation_regex=_EMAIL_VALIDATION_REGEX,
                    placeholder="contact@example.com",
                    input_type="email",
                ),
            },
        ),
        "reddit": ProviderAccountDefinition(
            account_key="reddit",
            display_name="Reddit OAuth",
            provider_name="reddit",
            credential_env={
                "client_id": "REDDIT_CLIENT_ID",
                "client_secret": "REDDIT_CLIENT_SECRET",
            },
            used_by_sources=("reddit",),
            guide=_source_guide(
                category="Provider Account",
                summary="Shared Reddit OAuth credentials inherited by Reddit sentiment polling.",
                needs=("A Reddit client ID.", "A Reddit client secret."),
                setup=("Enter Reddit OAuth credentials once, then adjust source settings such as subreddits separately.",),
                when_enabled="The Reddit feed inherits REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET unless overridden.",
                docs_url="https://www.reddit.com/dev/api/",
                signup_url="https://www.reddit.com/prefs/apps",
                plan_note="Rate limits and API access depend on Reddit's developer terms.",
            ),
        ),
        "quiver": ProviderAccountDefinition(
            account_key="quiver",
            display_name="Quiver",
            provider_name="quiver",
            credential_env={"api_key": "QUIVER_API_KEY"},
            used_by_sources=("quiver_gov",),
            guide=_source_guide(
                category="Provider Account",
                summary="Shared Quiver API key inherited by government-flow ingestion.",
                needs=("A Quiver API key.",),
                setup=("Enter the Quiver key once, then enable the Quiver government-flow feed.",),
                when_enabled="The Quiver feed inherits QUIVER_API_KEY unless a source override is saved.",
                docs_url="https://api.quiverquant.com/docs/",
                signup_url="https://www.quiverquant.com/",
                plan_note="Dataset access depends on the Quiver plan and endpoint entitlement.",
            ),
        ),
        "fundamentals_vendors": ProviderAccountDefinition(
            account_key="fundamentals_vendors",
            display_name="SimFin / Sharadar",
            provider_name="fundamentals",
            credential_env={
                "simfin_api_key": "SIMFIN_API_KEY",
                "sharadar_api_key": "SHARADAR_API_KEY",
            },
            used_by_sources=("fundamentals_pit",),
            guide=_source_guide(
                category="Provider Account",
                summary="Shared SimFin and Sharadar credentials inherited by PIT fundamentals ingestion.",
                needs=("A SimFin API key and/or a Sharadar API key.",),
                setup=("Enter the vendor keys for the selected fundamentals mode.",),
                when_enabled="The PIT fundamentals feed inherits available vendor keys unless overridden.",
                docs_url="https://www.simfin.com/en/api",
                signup_url="https://www.simfin.com/",
                plan_note="Coverage, bulk downloads, and rate limits depend on vendor plans.",
            ),
        ),
        "tradier": ProviderAccountDefinition(
            account_key="tradier",
            display_name="Tradier",
            provider_name="tradier",
            credential_env={"api_token": "TRADIER_API_TOKEN"},
            used_by_sources=("tradier",),
            guide=_source_guide(
                category="Provider Account",
                summary="Shared Tradier API token inherited by the Tradier options feed.",
                needs=("A Tradier API token.",),
                setup=("Enter the Tradier token once, then enable the Tradier options source.",),
                when_enabled="The Tradier feed inherits TRADIER_API_TOKEN unless a source override is saved.",
                docs_url="https://documentation.tradier.com/brokerage-api",
                signup_url="https://developer.tradier.com/",
                plan_note="Options market-data access depends on the Tradier account and entitlements.",
            ),
        ),
        "fred": ProviderAccountDefinition(
            account_key="fred",
            display_name="FRED / ALFRED",
            provider_name="fred",
            credential_env={"api_key": "FRED_API_KEY"},
            used_by_sources=("macro",),
            used_by_jobs=("backfill_macro_vintages",),
            guide=_source_guide(
                category="Provider Account",
                summary="Optional FRED API key inherited by macro vintage ingestion and backfill jobs.",
                needs=("A FRED API key when authenticated macro API access is desired.",),
                setup=("Enter the FRED API key once; macro ingestion can also run without it where public access allows.",),
                when_enabled="Macro jobs inherit FRED_API_KEY when configured.",
                docs_url="https://fred.stlouisfed.org/docs/api/fred/",
                signup_url="https://fred.stlouisfed.org/docs/api/api_key.html",
                plan_note="The key is optional for some public paths but improves authenticated API access.",
            ),
        ),
        "openai_embeddings": ProviderAccountDefinition(
            account_key="openai_embeddings",
            display_name="OpenAI Embeddings",
            provider_name="openai",
            credential_env={"api_key": "OPENAI_API_KEY"},
            used_by_sources=("news_flow",),
            used_by_jobs=("embed_filings", "embed_transcripts"),
            guide=_source_guide(
                category="Provider Account",
                summary="Optional OpenAI API key inherited by embedding-backed news and filing feature jobs.",
                needs=("An OpenAI API key only when an OpenAI embedding backend is selected.",),
                setup=("Enter the OpenAI API key once, then select OpenAI-backed embedding settings where needed.",),
                when_enabled="Embedding jobs inherit OPENAI_API_KEY when configured.",
                docs_url="https://platform.openai.com/docs/guides/embeddings",
                plan_note="OpenAI embeddings are optional and not part of trading authority.",
            ),
        ),
    }


MANAGED_DAEMON_JOBS = {
    "ingestion_runtime",
    "stream_prices_polygon_ws",
    "stream_prices_ibkr",
    "poll_prices",
    "options_poll",
    "ingest_now",
    "poll_gdelt",
    "poll_sec_filings",
    "ingest_form4",
    "ingest_13f",
    "ingest_congressional_trades",
    "ingest_etf_flows",
    "ingest_cftc_cot",
    "ingest_finra_short_volume",
    "ingest_finra_short_interest",
    "ingest_crypto_funding",
    "ingest_quiver_gov",
    "ingest_fundamentals_pit",
    "poll_earnings",
    "poll_social_reddit",
    "poll_social_stocktwits",
    "poll_weather_forecasts",
    "poll_weather_alerts",
    "poll_macro",
    "process_news_flow",
    "snapshot_model_features",
}


CUSTOM_RSS_TEMPLATE_KEY = "rss_feed"
_HTTP_URL_VALIDATION_REGEX = r"^(?:https?|wss?)://[^\s]+$"
_SECRET_VALUE_VALIDATION_REGEX = r"^[^\r\n]{1,512}$"
_INTEGER_VALIDATION_REGEX = r"^\d+$"
_NUMBER_VALIDATION_REGEX = r"^\d+(?:\.\d+)?$"
_BOOLEAN_VALIDATION_REGEX = r"^(?:0|1|true|false|yes|no|on|off)$"
_HOST_VALIDATION_REGEX = r"^[A-Za-z0-9._:-]+$"
_EMAIL_VALIDATION_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
_CURRENCY_VALIDATION_REGEX = r"^[A-Z]{3}$"
_LANGUAGE_VALIDATION_REGEX = r"^[A-Za-z]{2}(?:[-_][A-Za-z0-9]+)?$"
_EXCHANGE_ID_VALIDATION_REGEX = r"^[a-z0-9_-]{2,40}$"


_SOURCE_CATALOG_OPERATIONAL_METADATA: Dict[str, Dict[str, Any]] = {
    "polygon_ws": {
        "storage_tables": ("prices", "price_quotes", "price_quotes_raw", "price_provider_health"),
        "consumers": ("price_router", "model_feature_snapshots", "dashboard_data_health"),
        "safe_to_auto_enable": False,
    },
    "polygon": {
        "storage_tables": ("prices", "price_quotes", "price_quotes_raw", "price_provider_health"),
        "consumers": ("price_router", "model_feature_snapshots", "dashboard_data_health"),
        "safe_to_auto_enable": False,
    },
    "oanda_fx": {
        "storage_tables": ("prices", "price_quotes", "price_quotes_raw", "price_provider_health"),
        "consumers": ("price_router", "model_feature_snapshots", "dashboard_data_health"),
        "safe_to_auto_enable": False,
    },
    "ibkr": {
        "storage_tables": ("prices", "price_quotes_raw", "price_provider_health"),
        "consumers": ("price_router", "live_readiness", "dashboard_data_health"),
        "safe_to_auto_enable": False,
    },
    "alpaca_broker_data": {
        "storage_tables": ("broker_connection_health", "broker_positions"),
        "consumers": ("live_readiness", "position_reconcile"),
        "safe_to_auto_enable": False,
        "runtime_runnable": False,
    },
    "yfinance": {
        "storage_tables": ("prices", "price_provider_health"),
        "consumers": ("price_router", "model_feature_snapshots", "dashboard_data_health"),
        "safe_to_auto_enable": True,
    },
    "simulated": {
        "storage_tables": ("prices", "price_quotes", "price_quotes_raw", "price_provider_health"),
        "consumers": ("price_router", "dashboard_data_health", "safe_sim_validation"),
        "safe_to_auto_enable": True,
    },
    "ccxt": {
        "storage_tables": ("prices", "price_provider_health"),
        "consumers": ("price_router", "crypto_features", "dashboard_data_health"),
        "safe_to_auto_enable": True,
    },
    "tradier": {
        "storage_tables": ("options_chain", "options_chain_v2", "options_symbol_ingestion_state", "events"),
        "consumers": ("options_features", "model_feature_snapshots", "dashboard_data_health"),
        "safe_to_auto_enable": False,
    },
    "polygon_options": {
        "storage_tables": ("options_chain", "options_chain_v2", "options_symbol_ingestion_state", "events"),
        "consumers": ("options_features", "model_feature_snapshots", "dashboard_data_health"),
        "safe_to_auto_enable": False,
    },
    "reddit": {
        "storage_tables": ("events",),
        "consumers": ("sentiment_features", "news_flow", "model_feature_snapshots"),
        "safe_to_auto_enable": False,
    },
    "stocktwits": {
        "storage_tables": ("events",),
        "consumers": ("sentiment_features", "model_feature_snapshots"),
        "safe_to_auto_enable": True,
    },
    "company_news": {
        "storage_tables": ("events", "news_event_features", "news_symbol_features"),
        "consumers": ("news_flow", "model_feature_snapshots", "dashboard_data_health"),
        "safe_to_auto_enable": False,
    },
    "transcripts": {
        "storage_tables": ("structured_document_events", "events"),
        "consumers": ("document_features", "model_feature_snapshots", "dashboard_feature_visibility"),
        "safe_to_auto_enable": False,
    },
    "gdelt": {
        "storage_tables": ("events", "gdelt_macro_features"),
        "consumers": ("news_flow", "model_feature_snapshots", "macro_features"),
        "safe_to_auto_enable": True,
    },
    "sec": {
        "storage_tables": ("structured_document_events", "events"),
        "consumers": ("document_features", "model_feature_snapshots", "dashboard_feature_visibility"),
        "safe_to_auto_enable": True,
    },
    "form4": {
        "storage_tables": ("events", "insider_transactions"),
        "consumers": ("insider_flow_features", "model_feature_snapshots"),
        "safe_to_auto_enable": False,
    },
    "inst_13f": {
        "storage_tables": ("inst_13f_filings", "inst_13f_holdings", "inst_13f_cusip_symbol_map", "inst_13f_symbol_features"),
        "consumers": ("institutional_flow_features", "model_feature_snapshots"),
        "safe_to_auto_enable": False,
    },
    "congressional_trades": {
        "storage_tables": ("congressional_trades", "events"),
        "consumers": ("legislative_flow_features", "model_feature_snapshots"),
        "safe_to_auto_enable": False,
    },
    "etf_flows": {
        "storage_tables": ("etf_shares_outstanding", "etf_flow_features"),
        "consumers": ("etf_flow_features", "model_feature_snapshots"),
        "safe_to_auto_enable": False,
    },
    "cftc_cot": {
        "storage_tables": ("cftc_cot_positions", "cot_contract_symbol_map", "cot_symbol_features"),
        "consumers": ("cot_positioning_features", "model_feature_snapshots", "dashboard_feature_visibility"),
        "safe_to_auto_enable": False,
    },
    "finra_short_volume": {
        "storage_tables": ("finra_short_sale_volume",),
        "consumers": ("short_interest_features", "model_feature_snapshots", "dashboard_feature_visibility"),
        "safe_to_auto_enable": False,
    },
    "finra_short_interest": {
        "storage_tables": ("finra_short_interest",),
        "consumers": ("short_interest_features", "model_feature_snapshots", "dashboard_feature_visibility"),
        "safe_to_auto_enable": False,
    },
    "crypto_funding": {
        "storage_tables": ("crypto_funding_rates",),
        "consumers": ("crypto_positioning_features", "model_feature_snapshots", "dashboard_feature_visibility"),
        "safe_to_auto_enable": False,
    },
    "quiver_gov": {
        "storage_tables": ("quiver_congressional_trades", "quiver_lobbying_filings", "quiver_gov_contracts"),
        "consumers": ("legislative_flow_features", "model_feature_snapshots"),
        "safe_to_auto_enable": False,
    },
    "fundamentals_pit": {
        "storage_tables": ("fundamentals_pit", "events"),
        "consumers": ("fundamental_features", "model_feature_snapshots"),
        "safe_to_auto_enable": False,
    },
    "earnings": {
        "storage_tables": ("events",),
        "consumers": ("calendar_features", "model_feature_snapshots"),
        "safe_to_auto_enable": False,
    },
    "weather_forecasts": {
        "storage_tables": ("events",),
        "consumers": ("weather_features", "model_feature_snapshots"),
        "safe_to_auto_enable": True,
    },
    "weather_alerts": {
        "storage_tables": ("events",),
        "consumers": ("weather_features", "model_feature_snapshots"),
        "safe_to_auto_enable": True,
    },
    "macro": {
        "storage_tables": ("factor_registry", "factor_observations", "factor_features", "macro_series_vintages", "macro_vintage_backfill_state", "events"),
        "consumers": ("regime_features", "model_feature_snapshots"),
        "safe_to_auto_enable": True,
    },
    "model_feature_snapshots": {
        "storage_tables": ("model_feature_snapshots",),
        "consumers": ("model_diagnostics", "dashboard_feature_visibility"),
        "safe_to_auto_enable": True,
    },
    "news_flow": {
        "storage_tables": ("news_story_embeddings", "news_flow_features"),
        "consumers": ("model_feature_snapshots", "dashboard_feature_visibility"),
        "safe_to_auto_enable": True,
    },
    "rss_feed": {
        "storage_tables": ("events", "news_event_features"),
        "consumers": ("news_flow", "model_feature_snapshots"),
        "safe_to_auto_enable": False,
    },
}


def _contract(
    *,
    storage_table: str,
    normalized_shape: str,
    required_fields: Iterable[str],
    units: Optional[Dict[str, str]] = None,
    symbol_namespace: str = "",
    point_in_time_availability: str = "",
    unique_key: Iterable[str] = (),
    idempotent_upsert: str = "",
    consumer: str = "",
    timestamp_field: str = "ts_ms",
    source_field: str = "source",
    stale_after_ms: int = 0,
) -> DataSourceContract:
    return DataSourceContract(
        storage_table=str(storage_table),
        normalized_shape=str(normalized_shape),
        required_fields=tuple(str(item) for item in required_fields if str(item).strip()),
        units=dict(units or {}),
        symbol_namespace=str(symbol_namespace),
        point_in_time_availability=str(point_in_time_availability),
        unique_key=tuple(str(item) for item in unique_key if str(item).strip()),
        idempotent_upsert=str(idempotent_upsert),
        consumer=str(consumer),
        timestamp_field=str(timestamp_field or "ts_ms"),
        source_field=str(source_field or ""),
        stale_after_ms=int(stale_after_ms or 0),
    )


_PRICE_CONTRACT = _contract(
    storage_table="prices",
    normalized_shape="one canonical point-in-time last price row per symbol/provider timestamp",
    required_fields=("symbol", "ts_ms", "price", "source"),
    units={"price": "quote currency per share/contract/unit", "ts_ms": "unix epoch milliseconds"},
    symbol_namespace="canonical upper-case market symbol",
    point_in_time_availability="row is available no earlier than provider event/update timestamp in UTC",
    unique_key=("symbol", "ts_ms"),
    idempotent_upsert="UPSERT by (symbol, ts_ms); repeated populate updates the same proof row",
    consumer="price_router",
    timestamp_field="ts_ms",
    source_field="source",
    stale_after_ms=15 * 60 * 1000,
)

_EVENT_CONTRACT = _contract(
    storage_table="events",
    normalized_shape="one normalized external event with source lineage and deterministic event_key",
    required_fields=("ts_ms", "event_type", "source", "title", "event_key"),
    units={"ts_ms": "unix epoch milliseconds", "importance_score": "0-1 score"},
    symbol_namespace="canonical upper-case symbol when symbol-specific, blank for market-wide",
    point_in_time_availability="event is available at provider publish/retrieval timestamp in UTC",
    unique_key=("source", "event_key"),
    idempotent_upsert="dedupe by deterministic source/event_key before insert",
    consumer="news_flow",
    timestamp_field="ts_ms",
    source_field="source",
    stale_after_ms=24 * 60 * 60 * 1000,
)

_BROKER_READONLY_CONTRACT = _contract(
    storage_table="broker_connection_health",
    normalized_shape="one read-only broker account/positions probe health row",
    required_fields=("ts_ms", "broker", "ok", "state"),
    units={"ts_ms": "unix epoch milliseconds", "latency_ms": "milliseconds"},
    symbol_namespace="broker-native symbols only for optional position rows",
    point_in_time_availability="account and position reads are available at probe completion time in UTC",
    unique_key=("broker", "ts_ms"),
    idempotent_upsert="UPSERT by (broker, ts_ms); never calls order/cancel/replace/flatten paths",
    consumer="live_readiness",
    timestamp_field="ts_ms",
    source_field="broker",
    stale_after_ms=5 * 60 * 1000,
)

_FUNDAMENTALS_PIT_CONTRACT = _contract(
    storage_table="fundamentals_pit",
    normalized_shape="one vendor fundamental metric row with publish timestamp and source_record_id",
    required_fields=("symbol", "metric", "publish_ts_ms", "vendor", "source_record_id"),
    units={"value": "vendor-native financial unit", "publish_ts_ms": "unix epoch milliseconds"},
    symbol_namespace="canonical upper-case equity ticker",
    point_in_time_availability="feature consumers may use the row only at or after publish_ts_ms/availability timestamp",
    unique_key=("source_record_id",),
    idempotent_upsert="UPSERT by source_record_id",
    consumer="fundamental_features",
    timestamp_field="publish_ts_ms",
    source_field="vendor",
    stale_after_ms=45 * 24 * 60 * 60 * 1000,
)

_OPTIONS_CONTRACT = _contract(
    storage_table="options_chain",
    normalized_shape="one option quote/open-interest row per contract timestamp",
    required_fields=("ts_ms", "symbol", "expiry", "strike", "call_put", "source"),
    units={"strike": "USD", "iv": "decimal volatility", "open_interest": "contracts", "volume": "contracts"},
    symbol_namespace="canonical upper-case underlying ticker",
    point_in_time_availability="row is available no earlier than provider snapshot timestamp in UTC",
    unique_key=("symbol", "expiry", "strike", "call_put", "ts_ms"),
    idempotent_upsert="UPSERT by option contract identity and ts_ms",
    consumer="options_features",
    timestamp_field="ts_ms",
    source_field="source",
    stale_after_ms=30 * 60 * 1000,
)

_SOURCE_DATA_CONTRACTS: Dict[str, DataSourceContract] = {
    "polygon_ws": _PRICE_CONTRACT,
    "polygon": _PRICE_CONTRACT,
    "oanda_fx": _PRICE_CONTRACT,
    "ibkr": _PRICE_CONTRACT,
    "yfinance": _PRICE_CONTRACT,
    "ccxt": _PRICE_CONTRACT,
    "tradier": _OPTIONS_CONTRACT,
    "polygon_options": _OPTIONS_CONTRACT,
    "alpaca_broker_data": _BROKER_READONLY_CONTRACT,
    "company_news": _EVENT_CONTRACT,
    "gdelt": _EVENT_CONTRACT,
    "rss_feed": _EVENT_CONTRACT,
    "stocktwits": _EVENT_CONTRACT,
    "reddit": _EVENT_CONTRACT,
    "earnings": _EVENT_CONTRACT,
    "weather_forecasts": _EVENT_CONTRACT,
    "weather_alerts": _EVENT_CONTRACT,
    "sec": _contract(
        storage_table="events",
        normalized_shape="one SEC filing availability event with accession/source URL lineage",
        required_fields=("ts_ms", "event_type", "source", "title", "event_key"),
        units=_EVENT_CONTRACT.units,
        symbol_namespace="canonical upper-case equity ticker",
        point_in_time_availability="filing is available at SEC acceptance timestamp or retrieval timestamp in UTC",
        unique_key=("source", "event_key"),
        idempotent_upsert="dedupe by SEC accession/event_key",
        consumer="document_features",
        stale_after_ms=24 * 60 * 60 * 1000,
    ),
    "form4": _contract(
        storage_table="events",
        normalized_shape="one insider transaction filing event with PIT filing availability",
        required_fields=("ts_ms", "event_type", "source", "title", "event_key"),
        units=_EVENT_CONTRACT.units,
        symbol_namespace="canonical upper-case equity ticker",
        point_in_time_availability="event is available at SEC acceptance timestamp or retrieval timestamp in UTC",
        unique_key=("source", "event_key"),
        idempotent_upsert="dedupe by filing accession/transaction event_key",
        consumer="insider_flow_features",
        stale_after_ms=7 * 24 * 60 * 60 * 1000,
    ),
    "inst_13f": _contract(
        storage_table="inst_13f_filings",
        normalized_shape="one 13F filing row with manager, report date, acceptance, and PIT availability",
        required_fields=("manager_cik", "accession", "acceptance_ts_ms", "availability_ts_ms", "source_record_id"),
        units={"acceptance_ts_ms": "unix epoch milliseconds", "availability_ts_ms": "unix epoch milliseconds"},
        symbol_namespace="manager-level filing; holdings use mapped canonical tickers",
        point_in_time_availability="holdings may be used only at or after SEC acceptance/availability timestamp",
        unique_key=("source_record_id",),
        idempotent_upsert="UPSERT by source_record_id",
        consumer="institutional_flow_features",
        timestamp_field="availability_ts_ms",
        source_field="source_record_id",
        stale_after_ms=120 * 24 * 60 * 60 * 1000,
    ),
    "fundamentals_pit": _FUNDAMENTALS_PIT_CONTRACT,
    "cftc_cot": _contract(
        storage_table="cftc_cot_positions",
        normalized_shape="one CFTC COT contract/report row with release and availability timestamps",
        required_fields=("report_type", "contract_key", "report_date", "release_ts_ms", "availability_ts_ms", "source_record_id"),
        units={"open_interest": "contracts", "availability_ts_ms": "unix epoch milliseconds"},
        symbol_namespace="configured futures contract key mapped to canonical symbols",
        point_in_time_availability="row may be used only at or after CFTC release/availability timestamp",
        unique_key=("source_record_id",),
        idempotent_upsert="UPSERT by source_record_id",
        consumer="cot_positioning_features",
        timestamp_field="availability_ts_ms",
        source_field="contract_key",
        stale_after_ms=14 * 24 * 60 * 60 * 1000,
    ),
    "finra_short_volume": _contract(
        storage_table="finra_short_sale_volume",
        normalized_shape="one FINRA daily short-sale volume row per symbol/date",
        required_fields=("symbol", "trade_date", "trade_ts_ms", "source_record_id"),
        units={"short_volume": "shares", "total_volume": "shares"},
        symbol_namespace="canonical upper-case equity ticker",
        point_in_time_availability="row may be used after FINRA daily file publication timestamp",
        unique_key=("source_record_id",),
        idempotent_upsert="UPSERT by source_record_id",
        consumer="short_interest_features",
        timestamp_field="trade_ts_ms",
        source_field="source",
        stale_after_ms=7 * 24 * 60 * 60 * 1000,
    ),
    "finra_short_interest": _contract(
        storage_table="finra_short_interest",
        normalized_shape="one FINRA short-interest row per symbol/settlement date",
        required_fields=("symbol", "settlement_date", "settlement_ts_ms", "source_record_id"),
        units={"short_interest": "shares", "days_to_cover": "days"},
        symbol_namespace="canonical upper-case equity ticker",
        point_in_time_availability="row may be used after FINRA dissemination/availability timestamp",
        unique_key=("source_record_id",),
        idempotent_upsert="UPSERT by source_record_id",
        consumer="short_interest_features",
        timestamp_field="settlement_ts_ms",
        source_field="source",
        stale_after_ms=45 * 24 * 60 * 60 * 1000,
    ),
    "crypto_funding": _contract(
        storage_table="crypto_funding_rates",
        normalized_shape="one perpetual funding/mark row per exchange market timestamp",
        required_fields=("symbol", "exchange", "funding_ts_ms", "source_record_id"),
        units={"funding_rate": "decimal rate", "mark_price": "quote currency"},
        symbol_namespace="canonical crypto symbol mapped from exchange perp market",
        point_in_time_availability="row may be used after exchange funding/settlement timestamp",
        unique_key=("source_record_id",),
        idempotent_upsert="UPSERT by source_record_id",
        consumer="crypto_positioning_features",
        timestamp_field="funding_ts_ms",
        source_field="exchange",
        stale_after_ms=24 * 60 * 60 * 1000,
    ),
    "congressional_trades": _contract(
        storage_table="events",
        normalized_shape="one public congressional trade disclosure event",
        required_fields=("ts_ms", "event_type", "source", "title", "event_key"),
        units=_EVENT_CONTRACT.units,
        symbol_namespace="canonical upper-case equity ticker when disclosed",
        point_in_time_availability="row may be used only at or after disclosure availability timestamp",
        unique_key=("source", "event_key"),
        idempotent_upsert="dedupe by disclosure event_key",
        consumer="legislative_flow_features",
        stale_after_ms=30 * 24 * 60 * 60 * 1000,
    ),
    "etf_flows": _contract(
        storage_table="etf_shares_outstanding",
        normalized_shape="one ETF shares-outstanding observation with availability timestamp",
        required_fields=("symbol", "asof_date", "asof_ts_ms", "availability_ts_ms", "shares_outstanding", "source_record_id"),
        units={"shares_outstanding": "shares", "price": "USD", "nav": "USD"},
        symbol_namespace="canonical upper-case ETF ticker",
        point_in_time_availability="row may be used only at or after provider availability timestamp",
        unique_key=("source_record_id",),
        idempotent_upsert="UPSERT by source_record_id",
        consumer="etf_flow_features",
        timestamp_field="availability_ts_ms",
        source_field="source",
        stale_after_ms=7 * 24 * 60 * 60 * 1000,
    ),
    "quiver_gov": _EVENT_CONTRACT,
    "transcripts": _contract(
        storage_table="events",
        normalized_shape="one transcript availability event with source document lineage",
        required_fields=("ts_ms", "event_type", "source", "title", "event_key"),
        units=_EVENT_CONTRACT.units,
        symbol_namespace="canonical upper-case equity ticker",
        point_in_time_availability="transcript is available at provider publish/retrieval timestamp in UTC",
        unique_key=("source", "event_key"),
        idempotent_upsert="dedupe by provider transcript/event_key",
        consumer="document_features",
        stale_after_ms=30 * 24 * 60 * 60 * 1000,
    ),
    "macro": _contract(
        storage_table="events",
        normalized_shape="one macro observation/vintage availability event",
        required_fields=("ts_ms", "event_type", "source", "title", "event_key"),
        units=_EVENT_CONTRACT.units,
        symbol_namespace="macro series id, not tradeable symbol",
        point_in_time_availability="macro values may be used only at or after release/vintage availability timestamp",
        unique_key=("source", "event_key"),
        idempotent_upsert="dedupe by series/vintage event_key",
        consumer="regime_features",
        stale_after_ms=45 * 24 * 60 * 60 * 1000,
    ),
    "model_feature_snapshots": _contract(
        storage_table="model_feature_snapshots",
        normalized_shape="one PIT-safe model feature snapshot row",
        required_fields=("symbol", "ts_ms", "features_json"),
        units={"ts_ms": "unix epoch milliseconds"},
        symbol_namespace="canonical upper-case model symbol",
        point_in_time_availability="all included feature groups must pass PIT availability controls",
        unique_key=("symbol", "ts_ms"),
        idempotent_upsert="UPSERT by (symbol, ts_ms)",
        consumer="model_diagnostics",
        timestamp_field="ts_ms",
        source_field="",
        stale_after_ms=60 * 60 * 1000,
    ),
    "news_flow": _contract(
        storage_table="news_flow_features",
        normalized_shape="one news-flow feature row per symbol/asof/backend/model",
        required_fields=("symbol", "asof_ts_ms", "embedding_backend", "model_name"),
        units={"news_novelty_max_24h": "0-1 score", "news_stale_share_24h": "0-1 share"},
        symbol_namespace="canonical upper-case equity ticker",
        point_in_time_availability="feature row uses only news events available at or before asof_ts_ms",
        unique_key=("symbol", "asof_ts_ms", "embedding_backend", "model_name"),
        idempotent_upsert="UPSERT by feature primary key",
        consumer="model_feature_snapshots",
        timestamp_field="asof_ts_ms",
        source_field="",
        stale_after_ms=24 * 60 * 60 * 1000,
    ),
}


_POPULATE_NOW_HANDLER_REGISTRY: Dict[str, str] = {
    "polygon": "_populate_price_polygon_rest",
    "polygon_ws": "_populate_price_polygon_rest",
    "oanda_fx": "_populate_generic_price_marker",
    "yfinance": "_populate_price_yfinance",
    "simulated": "_populate_price_simulated",
    "ccxt": "_populate_generic_price_marker",
    "ibkr": "_populate_generic_price_marker",
    "company_news": "_populate_company_news",
    "gdelt": "_populate_gdelt",
    "rss_feed": "_populate_rss_feed",
    "fundamentals_pit": "_populate_fundamentals_pit",
    "alpaca_broker_data": "_populate_alpaca_broker_data_readonly",
    "tradier": "_populate_options_marker",
    "polygon_options": "_populate_options_marker",
}


def _humanize_field_name(name: str) -> str:
    text = str(name or "").strip().replace("_", " ")
    acronyms = {
        "api": "API",
        "ccxt": "CCXT",
        "fmp": "FMP",
        "http": "HTTP",
        "ibkr": "IBKR",
        "id": "ID",
        "pit": "PIT",
        "rss": "RSS",
        "sec": "SEC",
        "ua": "UA",
        "url": "URL",
        "ws": "WS",
    }
    return " ".join(acronyms.get(part.lower(), part.capitalize()) for part in text.split() if part)


def _source_guide(
    *,
    category: str,
    summary: str,
    needs: Iterable[str],
    setup: Iterable[str],
    when_enabled: str,
    docs_url: str = "",
    signup_url: str = "",
    plan_note: str = "",
    safety_warnings: Iterable[str] = (),
) -> SourceGuide:
    return SourceGuide(
        category=str(category or "Source"),
        summary=str(summary or "This source is managed from the data-source control plane."),
        needs=tuple(str(item) for item in (needs or ()) if str(item).strip()),
        setup=tuple(str(item) for item in (setup or ()) if str(item).strip()),
        when_enabled=str(
            when_enabled
            or "The runtime includes this source in ingestion and health monitoring."
        ),
        docs_url=str(docs_url or ""),
        signup_url=str(signup_url or ""),
        plan_note=str(plan_note or ""),
        safety_warnings=tuple(
            str(item) for item in (safety_warnings or ()) if str(item).strip()
        ),
    )


def _guess_field_type(field_name: str, *, secret: bool = False) -> str:
    name = str(field_name or "").strip().lower()
    if secret:
        return "password"
    if name.endswith("_url") or name.endswith("_endpoint") or "url" in name:
        return "url"
    if (
        name.endswith("_s")
        or name.endswith("_seconds")
        or name.endswith("_days")
        or name.endswith("_limit")
        or name.endswith("_port")
        or name in {"port", "client_id", "maxrecords", "poll_limit", "backfill_days"}
    ):
        return "number"
    return "text"


def _default_validation(field_name: str, *, input_type: str, secret: bool) -> tuple[str, str]:
    name = str(field_name or "").strip().lower()
    if secret:
        return _SECRET_VALUE_VALIDATION_REGEX, "Use a single-line credential value."
    if input_type == "url":
        return _HTTP_URL_VALIDATION_REGEX, "Use an http, https, ws, or wss URL with no spaces."
    if name in {"subscribe_trades", "subscribe_quotes"}:
        return _BOOLEAN_VALIDATION_REGEX, "Use 1/0, true/false, yes/no, or on/off."
    if name in {"host"}:
        return _HOST_VALIDATION_REGEX, "Use a hostname, IP address, or host:port value."
    if name == "from":
        return _EMAIL_VALIDATION_REGEX, "Use a contact email address."
    if name in {"currency"}:
        return _CURRENCY_VALIDATION_REGEX, "Use a three-letter uppercase currency code."
    if name == "language":
        return _LANGUAGE_VALIDATION_REGEX, "Use a short language code such as en."
    if name == "exchange_id":
        return _EXCHANGE_ID_VALIDATION_REGEX, "Use a lowercase CCXT exchange id such as coinbase."
    if (
        name.endswith("_port")
        or name == "port"
        or name.endswith("_limit")
        or name.endswith("_days")
        or name == "maxrecords"
        or name == "client_id"
        or name == "poll_limit"
    ):
        return _INTEGER_VALIDATION_REGEX, "Use a non-negative integer."
    if name.endswith("_s") or name.endswith("_seconds"):
        return _NUMBER_VALIDATION_REGEX, "Use a non-negative number of seconds."
    return "", ""


def _default_help_text(
    field_name: str,
    *,
    env_name: str,
    secret: bool,
    required: bool,
) -> str:
    field_label = _humanize_field_name(field_name) or str(field_name or "Field")
    requirement = "Required" if required else "Optional"
    if secret:
        return (
            f"{requirement} credential for this provider. Stored encrypted at rest, "
            "never returned unmasked, and projected to runtime only when needed."
        )
    if env_name:
        return f"{requirement} setting projected to runtime as {env_name} when this source is enabled."
    return f"{requirement} setting for {field_label}."


def _default_placeholder(field_name: str, *, input_type: str, secret: bool) -> str:
    name = str(field_name or "").strip().lower()
    if secret:
        return "Enter new secret; leave blank to preserve"
    if input_type == "url":
        if name.endswith("template"):
            return "https://provider.example/path/{symbol}"
        return "https://provider.example/path"
    if name == "host":
        return default_ibkr_host()
    if name == "port":
        return "7497"
    if name == "client_id":
        return "1"
    if name == "currency":
        return "USD"
    if name == "language":
        return "en"
    if name == "exchange_id":
        return "coinbase"
    if name.endswith("_seconds") or name.endswith("_s"):
        return "60"
    if name.endswith("_days"):
        return "7"
    if name.endswith("_limit") or name == "maxrecords":
        return "100"
    return "Optional"


def _field_metadata_payload(
    metadata: SourceFieldMetadata | None,
    *,
    field_name: str,
    env_name: str = "",
    secret: bool = False,
    required: bool = False,
    input_type: str = "",
    guide: SourceGuide | None = None,
) -> Dict[str, Any]:
    meta = metadata
    field_name_s = str((meta.field if meta is not None and meta.field else field_name) or "").strip()
    env_name_s = str((meta.env_name if meta is not None and meta.env_name else env_name) or "").strip()
    secret_value = bool(meta.secret) if meta is not None else bool(secret)
    required_value = bool(meta.required) if meta is not None else bool(required)
    input_type_s = str((meta.input_type if meta is not None and meta.input_type else input_type) or "").strip()
    if not input_type_s:
        input_type_s = _guess_field_type(field_name_s, secret=secret_value)
    default_regex, default_hint = _default_validation(
        field_name_s,
        input_type=input_type_s,
        secret=secret_value,
    )
    validation_regex = str((meta.validation_regex if meta is not None else "") or default_regex or "")
    validation_hint = str((meta.validation_hint if meta is not None else "") or default_hint or "")
    if not validation_hint:
        validation_hint = "Free text; do not put secrets here unless the field is marked secret."
    guide_obj = guide or SourceGuide()
    safety_warning = str((meta.safety_warning if meta is not None else "") or "").strip()
    if not safety_warning and guide_obj.safety_warnings:
        safety_warning = " ".join(str(item) for item in guide_obj.safety_warnings if str(item).strip())
    help_text = str((meta.help_text if meta is not None else "") or "").strip()
    if not help_text:
        help_text = _default_help_text(
            field_name_s,
            env_name=env_name_s,
            secret=secret_value,
            required=required_value,
        )
    placeholder = str((meta.placeholder if meta is not None else "") or "").strip()
    if not placeholder:
        placeholder = _default_placeholder(
            field_name_s,
            input_type=input_type_s,
            secret=secret_value,
        )
    docs_url = str((meta.docs_url if meta is not None else "") or guide_obj.docs_url or "").strip()
    signup_url = str((meta.signup_url if meta is not None else "") or guide_obj.signup_url or "").strip()
    plan_note = str((meta.plan_note if meta is not None else "") or guide_obj.plan_note or "").strip()
    return {
        "field": field_name_s,
        "env_name": env_name_s,
        "env_var": env_name_s,
        "label": str((meta.label if meta is not None else "") or _humanize_field_name(field_name_s)),
        "help_text": help_text,
        "docs_url": docs_url,
        "signup_url": signup_url,
        "plan_note": plan_note,
        "required": bool(required_value),
        "required_state": "required" if required_value else "optional",
        "secret": bool(secret_value),
        "validation_hint": validation_hint,
        "validation_regex": validation_regex,
        "validation": {
            "regex": validation_regex,
            "hint": validation_hint,
        },
        "placeholder": placeholder,
        "safety_warning": safety_warning,
        "type": input_type_s,
        "input_type": input_type_s,
    }


def _guide_payload(guide: SourceGuide | None) -> Dict[str, Any]:
    guide_obj = guide or SourceGuide()
    return {
        "category": str(guide_obj.category or "Source"),
        "summary": str(guide_obj.summary or "This source is managed from the data-source control plane."),
        "needs": [str(item) for item in (guide_obj.needs or ()) if str(item).strip()],
        "setup": [str(item) for item in (guide_obj.setup or ()) if str(item).strip()],
        "when_enabled": str(
            guide_obj.when_enabled
            or "The runtime includes this source in ingestion and health monitoring."
        ),
        "docs_url": str(guide_obj.docs_url or ""),
        "signup_url": str(guide_obj.signup_url or ""),
        "plan_note": str(guide_obj.plan_note or ""),
        "safety_warnings": [
            str(item) for item in (guide_obj.safety_warnings or ()) if str(item).strip()
        ],
    }


def _catalog_operational_metadata(
    source_key: str,
    definition: SourceDefinition | None,
) -> Dict[str, Any]:
    key = str(source_key or "").strip()
    configured = dict(_SOURCE_CATALOG_OPERATIONAL_METADATA.get(key) or {})
    storage_tables = tuple(
        str(item)
        for item in (definition.storage_tables if definition is not None and definition.storage_tables else configured.get("storage_tables") or ())
        if str(item).strip()
    )
    consumers = tuple(
        str(item)
        for item in (definition.consumers if definition is not None and definition.consumers else configured.get("consumers") or ())
        if str(item).strip()
    )
    safe_to_auto_enable = bool(
        definition.safe_to_auto_enable if definition is not None and definition.safe_to_auto_enable else configured.get("safe_to_auto_enable", False)
    )
    runtime_runnable = bool(
        definition.runtime_runnable if definition is not None else configured.get("runtime_runnable", True)
    )
    if "runtime_runnable" in configured:
        runtime_runnable = bool(configured.get("runtime_runnable"))
    return {
        "storage_tables": storage_tables,
        "consumers": consumers,
        "safe_to_auto_enable": bool(safe_to_auto_enable),
        "runtime_runnable": bool(runtime_runnable),
    }


def _data_contract_for_source(source_key: str, definition: SourceDefinition | None) -> DataSourceContract:
    key = str(source_key or "").strip()
    if key in _SOURCE_DATA_CONTRACTS:
        return _SOURCE_DATA_CONTRACTS[key]
    if definition is not None and str(definition.source_type or "") in _SOURCE_DATA_CONTRACTS:
        return _SOURCE_DATA_CONTRACTS[str(definition.source_type or "")]
    operational = _catalog_operational_metadata(key, definition)
    storage_table = str(next(iter(operational.get("storage_tables") or ()), "events") or "events")
    consumer = str(next(iter(operational.get("consumers") or ()), "dashboard_data_health") or "dashboard_data_health")
    if storage_table == "prices":
        return _PRICE_CONTRACT
    if storage_table == "broker_connection_health":
        return _BROKER_READONLY_CONTRACT
    if storage_table == "fundamentals_pit":
        return _FUNDAMENTALS_PIT_CONTRACT
    if storage_table.startswith("options_"):
        return _OPTIONS_CONTRACT
    return _contract(
        storage_table=storage_table,
        normalized_shape=f"one normalized {key or 'source'} populate proof row",
        required_fields=("ts_ms",),
        units={"ts_ms": "unix epoch milliseconds"},
        symbol_namespace="source-specific",
        point_in_time_availability="row is available at provider retrieval timestamp in UTC",
        unique_key=("ts_ms",),
        idempotent_upsert="provider-specific populate handler uses the smallest available idempotent key",
        consumer=consumer,
        timestamp_field="ts_ms",
        source_field="source",
        stale_after_ms=24 * 60 * 60 * 1000,
    )


class DataSourceManager:
    """Own the data-source control plane and runtime configuration projection.

    The manager exposes operator-facing CRUD, testing, logging, and lifecycle
    flows, encrypts provider credentials at rest, and projects enabled sources
    into runtime job and provider configuration.
    """

    def __init__(self) -> None:
        self._catalog = _default_catalog()
        self._account_catalog = _provider_account_catalog()
        self._init_lock = threading.Lock()
        self._initialized = False

    def _is_supervised_managed_job(self) -> bool:
        job_name = str(os.environ.get("ENGINE_JOB_NAME") or "").strip()
        supervised = str(
            os.environ.get("ENGINE_LAUNCHED_BY_SUPERVISOR", os.environ.get("ENGINE_SUPERVISED", "0")) or "0"
        ).strip().lower() in {"1", "true", "yes", "on"}
        return bool(supervised and job_name in MANAGED_DAEMON_JOBS)

    def _read_only_requested(self) -> bool:
        return str(os.environ.get("DATA_SOURCE_MANAGER_READ_ONLY", "0") or "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    def _can_reuse_bootstrapped_control_plane(self) -> bool:
        if not self._is_supervised_managed_job():
            return False
        try:
            bootstrap_ready = str(meta_get("data_sources_bootstrap_ready", "") or "").strip()
            schema_ready = str(meta_get("data_sources_schema_ready", "") or "").strip()
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_BOOTSTRAP_MARKER_READ_FAILED",
                e,
                once_key="data_source_manager_bootstrap_marker_read",
                job_name=str(os.environ.get("ENGINE_JOB_NAME") or ""),
            )
            return False
        if bootstrap_ready != "1" and schema_ready != "1":
            return False
        return bool(self._fetch_rows())

    def _can_reuse_existing_control_plane(self) -> bool:
        # Health checks and other read-mostly callers should not fight live
        # ingestion for schema DDL once the control-plane catalog already
        # exists. If rows are readable, treat the control plane as initialized.
        try:
            return bool(self._fetch_rows())
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_EXISTING_CONTROL_PLANE_READ_FAILED",
                e,
                once_key="data_source_manager_existing_control_plane_read",
            )
            return False

    def _decrypt_credentials_safe(
        self,
        blob: Any,
        *,
        source_key: str = "",
        key_version: str = DEFAULT_MASTER_KEY_NAME,
    ) -> tuple[Dict[str, Any], str]:
        try:
            return decrypt_credentials(blob, key_name=str(key_version or DEFAULT_MASTER_KEY_NAME)), ""
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_CREDENTIAL_DECRYPT_FAILED",
                e,
                once_key=f"credential_decrypt:{str(source_key or 'unknown')}",
                source_key=str(source_key or ""),
                key_version=str(key_version or DEFAULT_MASTER_KEY_NAME),
            )
            return {}, f"{type(e).__name__}: {e}"

    def _clear_data_credential_cache(self) -> None:
        try:
            from engine.data._credentials import clear_data_credential_cache

            clear_data_credential_cache()
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_CREDENTIAL_CACHE_CLEAR_FAILED",
                e,
                once_key="credential_cache_clear",
            )

    def _withdraw_previously_projected_runtime_keys(self) -> None:
        previous_projected = [
            str(key or "").strip()
            for key in str(os.environ.get(_PROJECTED_RUNTIME_KEYS_ENV, "") or "").split(",")
            if str(key or "").strip()
        ]
        for key in previous_projected:
            os.environ.pop(str(key), None)
        if previous_projected:
            os.environ.pop(_PROJECTED_RUNTIME_KEYS_ENV, None)

    def _normalize_account_key(self, value: Any) -> str:
        raw = str(value or "").strip().lower()
        raw = re.sub(r"[^a-z0-9_:-]+", "_", raw)
        raw = re.sub(r"_+", "_", raw).strip("_")
        return raw[:120]

    def _account_credential_schema_fields(
        self,
        account_key: str,
        definition: Optional[ProviderAccountDefinition],
    ) -> List[Dict[str, Any]]:
        fields: List[Dict[str, Any]] = []
        if definition is None:
            return fields
        for field_name, env_name in sorted((definition.credential_env or {}).items()):
            metadata = (definition.credential_metadata or {}).get(str(field_name))
            secret_value = True if metadata is None else bool(metadata.secret)
            fields.append(
                _field_metadata_payload(
                    metadata,
                    field_name=str(field_name),
                    env_name=str(env_name),
                    secret=secret_value,
                    required=False,
                    input_type=("password" if secret_value else _guess_field_type(str(field_name), secret=False)),
                    guide=definition.guide,
                )
            )
        return fields

    def _account_field_secret(
        self,
        account_key: str,
        field_name: str,
    ) -> bool:
        definition = self._account_catalog.get(str(account_key or ""))
        if definition is None:
            return True
        metadata = (definition.credential_metadata or {}).get(str(field_name or ""))
        return True if metadata is None else bool(metadata.secret)

    def _account_definitions_for_context(
        self,
        *,
        source_key: str = "",
        job_name: str = "",
    ) -> List[ProviderAccountDefinition]:
        source_key_s = str(source_key or "").strip()
        job_name_s = str(job_name or "").strip()
        out: List[ProviderAccountDefinition] = []
        for definition in self._account_catalog.values():
            if source_key_s and source_key_s in set(definition.used_by_sources or ()):
                out.append(definition)
                continue
            if job_name_s and job_name_s in set(definition.used_by_jobs or ()):
                out.append(definition)
        return out

    def _account_env_names_for_context(self, *, source_key: str = "", job_name: str = "") -> set[str]:
        envs: set[str] = set()
        for definition in self._account_definitions_for_context(source_key=source_key, job_name=job_name):
            for env_name in (definition.credential_env or {}).values():
                env_name_s = str(env_name or "").strip()
                if env_name_s:
                    envs.add(env_name_s)
        return envs

    def _account_used_by_payload(self, definition: ProviderAccountDefinition) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for source_key in definition.used_by_sources or ():
            source_key_s = str(source_key or "").strip()
            source_def = self._catalog.get(source_key_s)
            out.append(
                {
                    "kind": "source",
                    "source_key": source_key_s,
                    "display_name": str((source_def.display_name if source_def is not None else source_key_s) or source_key_s),
                    "job_name": str((source_def.job_name if source_def is not None else "") or ""),
                }
            )
        for job_name in definition.used_by_jobs or ():
            job_name_s = str(job_name or "").strip()
            out.append(
                {
                    "kind": "job",
                    "source_key": "",
                    "display_name": str(job_name_s.replace("_", " ").title()),
                    "job_name": job_name_s,
                }
            )
        return out

    def _provider_account_template_payload(
        self,
        account_key: str,
        definition: Optional[ProviderAccountDefinition],
    ) -> Dict[str, Any]:
        if definition is None:
            return {}
        return {
            "account_key": str(account_key),
            "display_name": str(definition.display_name or account_key),
            "provider_name": str(definition.provider_name or account_key),
            "guide": _guide_payload(definition.guide),
            "credential_fields": self._account_credential_schema_fields(account_key, definition),
            "used_by": self._account_used_by_payload(definition),
        }

    def initialize(self) -> None:
        """Ensure one-time schema setup and seed built-in source rows.

        Returns
        -------
        None

        Notes
        -----
        Initialization is guarded by ``_init_lock`` so concurrent callers do
        not race schema creation or legacy-import steps.

        Side Effects
        ------------
        Creates control-plane tables, seeds built-in sources, and imports any
        legacy environment-defined source configuration exactly once.
        """
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            # Supervised daemon children inherit runtime environment from their
            # parent and only need read access to the persisted source catalog.
            # Re-running bootstrap writes in each child causes avoidable lock
            # contention during ingestion startup.
            if self._can_reuse_bootstrapped_control_plane():
                self._initialized = True
                return
            if self._is_supervised_managed_job() and self._can_reuse_existing_control_plane():
                self._initialized = True
                return
            if self._read_only_requested() and self._can_reuse_existing_control_plane():
                self._initialized = True
                return
            self._ensure_schema()
            self._seed_provider_accounts()
            self._import_legacy_env_account_config_once()
            self._seed_builtin_sources()
            self._import_legacy_env_source_config_once()
            try:
                meta_set("data_sources_bootstrap_ready", "1")
            except Exception as e:
                _warn_nonfatal(
                    "DATA_SOURCE_MANAGER_BOOTSTRAP_MARK_FAILED",
                    e,
                    once_key="data_source_manager_bootstrap_mark_failed",
                )
            self._initialized = True

    def _resolve_definition(
        self,
        source_key: str,
        *,
        source_type: str = "",
        provider_name: str = "",
    ) -> tuple[str, Optional[SourceDefinition]]:
        key = str(source_key or "").strip()
        definition = self._catalog.get(key)
        if definition is not None:
            return key, definition
        type_name = str(source_type or "").strip().lower()
        provider = str(provider_name or "").strip().lower()
        if type_name == "rss_feed" or provider == "rss":
            return CUSTOM_RSS_TEMPLATE_KEY, self._catalog.get(CUSTOM_RSS_TEMPLATE_KEY)
        return "", None

    def _credential_schema_fields(self, template_key: str, definition: Optional[SourceDefinition]) -> List[Dict[str, Any]]:
        fields: List[Dict[str, Any]] = []
        if definition is None:
            return fields
        for field_name, env_name in sorted((definition.credential_env or {}).items()):
            metadata = (definition.credential_metadata or {}).get(str(field_name))
            fields.append(
                _field_metadata_payload(
                    metadata,
                    field_name=str(field_name),
                    env_name=str(env_name),
                    secret=True,
                    required=True,
                    input_type="password",
                    guide=definition.guide,
                )
            )
        return fields

    def _setting_schema_fields(self, template_key: str, definition: Optional[SourceDefinition]) -> List[Dict[str, Any]]:
        if template_key == CUSTOM_RSS_TEMPLATE_KEY:
            return [
                _field_metadata_payload(
                    SourceFieldMetadata(
                        field="name",
                        label="Feed Name",
                        help_text="Required operator-facing name for this RSS feed.",
                        docs_url="https://www.rssboard.org/rss-specification",
                        plan_note="RSS feeds are publisher-controlled and usually do not need a paid plan.",
                        required=True,
                        validation_hint="Use a short feed name.",
                        validation_regex=r"^[^\r\n]{1,120}$",
                        placeholder="Reuters Top News",
                        input_type="text",
                    ),
                    field_name="name",
                    required=True,
                    input_type="text",
                    guide=definition.guide if definition is not None else None,
                ),
                _field_metadata_payload(
                    SourceFieldMetadata(
                        field="url",
                        label="Feed URL",
                        help_text="Required RSS or Atom feed URL to poll.",
                        docs_url="https://www.rssboard.org/rss-specification",
                        plan_note="RSS feeds are publisher-controlled and usually do not need a paid plan.",
                        required=True,
                        validation_hint="Use an http or https feed URL with no spaces.",
                        validation_regex=r"^https?://[^\s]+$",
                        placeholder="https://example.com/feed.xml",
                        input_type="url",
                    ),
                    field_name="url",
                    required=True,
                    input_type="url",
                    guide=definition.guide if definition is not None else None,
                ),
            ]
        fields: List[Dict[str, Any]] = []
        if definition is None:
            return fields
        for field_name, env_name in sorted((definition.setting_env or {}).items()):
            metadata = (definition.setting_metadata or {}).get(str(field_name))
            field_type = _guess_field_type(str(field_name), secret=False)
            fields.append(
                _field_metadata_payload(
                    metadata,
                    field_name=str(field_name),
                    env_name=str(env_name),
                    secret=False,
                    required=False,
                    input_type=field_type,
                    guide=definition.guide,
                )
            )
        return fields

    def _template_payload(self, template_key: str, definition: Optional[SourceDefinition]) -> Dict[str, Any]:
        if definition is None:
            return {}
        is_builtin = bool(template_key in self._catalog and definition.singleton and template_key != CUSTOM_RSS_TEMPLATE_KEY)
        operational = _catalog_operational_metadata(template_key, definition)
        contract = _data_contract_for_source(template_key, definition)
        return {
            "template_key": str(template_key),
            "display_name": str(definition.display_name or template_key),
            "source_type": str(definition.source_type or ""),
            "provider_name": str(definition.provider_name or template_key),
            "job_name": str(definition.job_name or ""),
            "default_enabled": bool(definition.default_enabled),
            "storage_tables": list(operational["storage_tables"]),
            "consumers": list(operational["consumers"]),
            "safe_to_auto_enable": bool(operational["safe_to_auto_enable"]),
            "runtime_runnable": bool(operational["runtime_runnable"]),
            "data_contract": contract.payload(),
            "singleton": bool(definition.singleton),
            "builtin": bool(is_builtin),
            "allow_create": bool(template_key == CUSTOM_RSS_TEMPLATE_KEY),
            "allow_update": True,
            "allow_delete": bool(template_key == CUSTOM_RSS_TEMPLATE_KEY),
            "supports_test": True,
            "identity_locked": bool(is_builtin),
            "routing_locked": True,
            "account_keys": [
                str(item.account_key)
                for item in self._account_definitions_for_context(
                    source_key=str(template_key),
                    job_name=str(definition.job_name or ""),
                )
            ],
            "guide": _guide_payload(definition.guide),
            "credential_fields": self._credential_schema_fields(template_key, definition),
            "setting_fields": self._setting_schema_fields(template_key, definition),
        }

    def list_source_templates(self) -> List[Dict[str, Any]]:
        """Return template metadata for source creation and editing flows.

        Returns
        -------
        list of dict
            Template payloads including identity/routing constraints plus
            credential and setting field schemas for the UI.
        """
        self.initialize()
        out: List[Dict[str, Any]] = []
        for source_key, definition in self._catalog.items():
            out.append(self._template_payload(source_key, definition))
        return out

    def _normalize_actor(self, value: Any) -> str:
        raw = str(value or "").strip()
        if raw:
            return raw[:120]
        return "operator"

    def _validate_allowed_fields(
        self,
        payload: Dict[str, Any],
        *,
        allowed: Iterable[str],
        label: str,
    ) -> None:
        allowed_set = {str(name) for name in (allowed or []) if str(name)}
        keys = {str(name) for name in dict(payload or {}).keys()}
        unexpected = sorted([name for name in keys if name not in allowed_set])
        if unexpected:
            raise ValueError(f"unexpected_{label}_fields:{','.join(unexpected)}")

    def _schema_by_field(self, fields: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for item in fields or []:
            if not isinstance(item, dict):
                continue
            field_name = str(item.get("field") or "").strip()
            if field_name:
                out[field_name] = dict(item)
        return out

    def _validate_schema_values(
        self,
        payload: Dict[str, Any],
        *,
        schema: Dict[str, Dict[str, Any]],
        label: str,
        require_required_fields: bool = False,
    ) -> None:
        values = dict(payload or {})
        if bool(require_required_fields):
            missing = sorted(
                str(name)
                for name, item in schema.items()
                if bool(item.get("required")) and not str(values.get(name) or "").strip()
            )
            if missing:
                raise ValueError(f"missing_required_{label}_fields:{','.join(missing)}")
        for field_name, value in values.items():
            field_name_s = str(field_name or "").strip()
            item = schema.get(field_name_s) or {}
            regex = str(item.get("validation_regex") or "").strip()
            if not regex:
                validation = item.get("validation")
                if isinstance(validation, dict):
                    regex = str(validation.get("regex") or "").strip()
            text = str(value or "").strip()
            if not text or not regex:
                continue
            try:
                matched = re.fullmatch(regex, text) is not None
            except re.error as exc:
                _warn_nonfatal(
                    "DATA_SOURCE_MANAGER_FIELD_VALIDATION_REGEX_INVALID",
                    exc,
                    once_key=f"field_validation_regex:{label}:{field_name_s}",
                    field=field_name_s,
                    label=str(label),
                )
                continue
            if not matched:
                raise ValueError(f"invalid_{label}_format:{field_name_s}")

    def _apply_builtin_constraints(
        self,
        *,
        source_key: str,
        definition: Optional[SourceDefinition],
        existing: Optional[Dict[str, Any]],
        create_only: bool,
        body: Dict[str, Any],
        source_type: str,
        provider_name: str,
        job_name: str,
    ) -> tuple[str, str, str]:
        if definition is not None and definition.singleton and source_key in self._catalog and source_key != CUSTOM_RSS_TEMPLATE_KEY:
            if create_only and existing is None:
                raise ValueError(f"builtin_source_create_not_allowed:{source_key}")
            expected_type = str(definition.source_type or "")
            expected_provider = str(definition.provider_name or source_key)
            expected_job = str(definition.job_name or "")
            if str(source_type or "") != expected_type:
                raise ValueError(f"builtin_source_type_locked:{source_key}")
            if str(provider_name or "") != expected_provider:
                raise ValueError(f"builtin_provider_name_locked:{source_key}")
            if str(job_name or "") != expected_job:
                raise ValueError(f"builtin_job_name_locked:{source_key}")
            return expected_type, expected_provider, expected_job

        if str(source_type or "").strip() == "rss_feed":
            if body.get("provider_name") not in (None, "", "rss"):
                raise ValueError("rss_provider_name_locked")
            if body.get("job_name") not in (None, "", "ingest_now"):
                raise ValueError("rss_job_name_locked")
            return "rss_feed", "rss", "ingest_now"

        raise ValueError(f"unsupported_custom_source:{source_key}")

    def _runtime_snapshot(self) -> Dict[str, Any]:
        provider_telemetry: Dict[str, Any] = {"ok": False, "providers": {}}
        pipeline_health: Dict[str, Any] = {"ok": False, "pipelines": {}}
        ingestion_state: Dict[str, Any] = {}
        try:
            from engine.runtime.ipc import market_data_status

            provider_telemetry = dict(market_data_status() or {})
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_RUNTIME_PROVIDER_TELEMETRY_FAILED",
                e,
                once_key="runtime_provider_telemetry",
            )
        try:
            from engine.runtime.ingestion_status import pipeline_health_summary

            pipeline_health = dict(pipeline_health_summary() or {})
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_RUNTIME_PIPELINE_HEALTH_FAILED",
                e,
                once_key="runtime_pipeline_health",
            )
        try:
            raw_state = str(meta_get("ingestion_state", "") or "").strip()
            if raw_state:
                parsed = json.loads(raw_state)
                ingestion_state = parsed if isinstance(parsed, dict) else {}
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_RUNTIME_INGESTION_STATE_FAILED",
                e,
                once_key="runtime_ingestion_state",
            )
        return {
            "provider_telemetry": provider_telemetry,
            "pipeline_health": pipeline_health,
            "ingestion_state": ingestion_state,
            "updated_ts_ms": int(time.time() * 1000),
        }

    def get_runtime_snapshot(self) -> Dict[str, Any]:
        """Return runtime telemetry associated with the source control plane.

        Returns
        -------
        dict
            Snapshot containing provider telemetry, pipeline-health summaries,
            and ``updated_ts_ms`` in epoch milliseconds.
        """
        self.initialize()
        snapshot = self._runtime_snapshot()
        sources = self.list_sources()
        desired_jobs = self.get_desired_ingestion_jobs()
        snapshot["desired_ingestion_jobs"] = desired_jobs
        snapshot["jobs"] = self.get_runnable_job_states(
            sources=sources,
            runtime_snapshot=snapshot,
            desired_jobs=desired_jobs,
        )
        return snapshot

    def audit_action(
        self,
        source_key: str,
        *,
        action: str,
        actor: str,
        success: bool = True,
        message: str = "",
        detail: Optional[Dict[str, Any]] = None,
        client_ip: str = "",
        source_type: str = "",
        provider_name: str = "",
        job_name: str = "",
        ts_ms: Optional[int] = None,
    ) -> None:
        key = self._normalize_source_key(source_key)
        now_ms = int(ts_ms or time.time() * 1000)

        def _txn(con) -> None:
            con.execute(
                """
                INSERT INTO data_source_audit(
                  ts_ms, actor, action, source_key, source_type, provider_name,
                  job_name, success, message, detail_json, client_ip
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    self._normalize_actor(actor),
                    str(action or "event")[:120],
                    key,
                    str(source_type or "")[:120],
                    str(provider_name or "")[:120],
                    str(job_name or "")[:120],
                    1 if success else 0,
                    str(message or "")[:1000],
                    self._json_dumps(sanitize_data_source_log_detail(detail or {})),
                    str(client_ip or "")[:120],
                ),
            )

        try:
            run_write_txn(_txn)
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_AUDIT_ACTION_FAILED",
                e,
                once_key=f"audit_action:{key}:{action}",
                source_key=key,
                action=str(action or "event"),
            )

    def _ensure_schema(self) -> None:
        now_ms = int(time.time() * 1000)

        def _txn(con) -> None:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS data_sources (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  source_key TEXT NOT NULL UNIQUE,
                  display_name TEXT NOT NULL,
                  source_type TEXT NOT NULL,
                  provider_name TEXT,
                  job_name TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 1,
                  credentials_enc TEXT,
                  key_version TEXT DEFAULT 'master_key',
                  settings_json TEXT,
                  status TEXT,
                  last_error TEXT,
                  last_success_ts_ms INTEGER,
                  last_test_ts_ms INTEGER,
                  error_count INTEGER NOT NULL DEFAULT 0,
                  config_hash TEXT,
                  created_ts_ms INTEGER NOT NULL,
                  updated_ts_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_data_sources_job_name
                  ON data_sources(job_name);
                CREATE INDEX IF NOT EXISTS idx_data_sources_enabled
                  ON data_sources(enabled);
                CREATE INDEX IF NOT EXISTS idx_data_sources_type
                  ON data_sources(source_type);
                CREATE TABLE IF NOT EXISTS data_source_provider_accounts (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  account_key TEXT NOT NULL UNIQUE,
                  display_name TEXT NOT NULL,
                  provider_name TEXT,
                  credentials_enc TEXT,
                  key_version TEXT DEFAULT 'master_key',
                  status TEXT,
                  last_error TEXT,
                  last_test_ts_ms INTEGER,
                  config_hash TEXT,
                  created_ts_ms INTEGER NOT NULL,
                  updated_ts_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_data_source_provider_accounts_provider
                  ON data_source_provider_accounts(provider_name);
                CREATE TABLE IF NOT EXISTS data_source_audit (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts_ms INTEGER NOT NULL,
                  actor TEXT NOT NULL,
                  action TEXT NOT NULL,
                  source_key TEXT NOT NULL,
                  source_type TEXT,
                  provider_name TEXT,
                  job_name TEXT,
                  success INTEGER NOT NULL DEFAULT 1,
                  message TEXT,
                  detail_json TEXT,
                  client_ip TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_data_source_audit_source_ts
                  ON data_source_audit(source_key, ts_ms DESC);
                CREATE INDEX IF NOT EXISTS idx_data_source_audit_actor_ts
                  ON data_source_audit(actor, ts_ms DESC);
                CREATE TABLE IF NOT EXISTS data_source_populate_evidence (
                  source_key TEXT PRIMARY KEY,
                  ts_ms INTEGER NOT NULL,
                  status TEXT NOT NULL,
                  contract_status TEXT NOT NULL,
                  row_count INTEGER NOT NULL DEFAULT 0,
                  storage_table TEXT NOT NULL,
                  latest_ts_ms INTEGER,
                  latency_ms INTEGER,
                  missing_null_counts_json TEXT,
                  duplicate_drops INTEGER NOT NULL DEFAULT 0,
                  stale_gap_status TEXT,
                  provider_evidence_json TEXT,
                  contract_json TEXT,
                  error TEXT,
                  actor TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_data_source_populate_evidence_status
                  ON data_source_populate_evidence(status, contract_status);
                CREATE INDEX IF NOT EXISTS idx_data_source_populate_evidence_ts
                  ON data_source_populate_evidence(ts_ms DESC);
                CREATE TABLE IF NOT EXISTS runtime_meta (
                  key TEXT PRIMARY KEY,
                  value TEXT,
                  updated_ts_ms INTEGER
                );
                """
            )
            columns = con.execute("PRAGMA table_info(data_sources)").fetchall() or []
            if not any(str(row[1]) == "key_version" for row in columns):
                con.execute("ALTER TABLE data_sources ADD COLUMN key_version TEXT DEFAULT 'master_key'")
            account_columns = con.execute("PRAGMA table_info(data_source_provider_accounts)").fetchall() or []
            if account_columns and not any(str(row[1]) == "key_version" for row in account_columns):
                con.execute("ALTER TABLE data_source_provider_accounts ADD COLUMN key_version TEXT DEFAULT 'master_key'")
            ensure_data_source_logs_schema(con)
            con.execute(
                """
                INSERT INTO runtime_meta(key, value, updated_ts_ms)
                VALUES(?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                  value=excluded.value,
                  updated_ts_ms=excluded.updated_ts_ms
                """,
                ("data_sources_schema_ready", "1", int(now_ms)),
            )

        # storage-route-audit: allow - bounded startup maintenance cleanup owned by the data-source manager.
        run_write_txn(
            _txn,
            table="data_sources",
            operation="data_source_schema_init",
            attempts=3,
            direct=False,
            maintenance=True,
            timeout_s=float(_DATA_SOURCE_MANAGER_STARTUP_WRITE_TIMEOUT_S),
            busy_timeout_ms=int(_DATA_SOURCE_MANAGER_STARTUP_BUSY_TIMEOUT_MS),
        )
        self._redact_existing_runtime_logs_once(now_ms=now_ms)
        self._redact_existing_timescale_logs_once()

    def _redact_existing_runtime_logs_once(self, *, now_ms: int) -> None:
        redact_existing_data_source_log_details_once(
            now_ms=int(now_ms),
            timeout_s=float(_DATA_SOURCE_MANAGER_STARTUP_WRITE_TIMEOUT_S),
            busy_timeout_ms=int(_DATA_SOURCE_MANAGER_STARTUP_BUSY_TIMEOUT_MS),
        )

    def _redact_existing_timescale_logs_once(self) -> None:
        try:
            marker = str(meta_get(DATA_SOURCE_LOG_REDACTION_TIMESCALE_MARKER_KEY, "") or "").strip()
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_TIMESCALE_LOG_REDACTION_MARKER_READ_FAILED",
                e,
                once_key="timescale_log_redaction_marker_read",
            )
            marker = ""
        if marker == "1":
            return
        result = redact_existing_timescale_data_source_log_details()
        if not bool(result.get("attempted")):
            return
        try:
            meta_set(f"{DATA_SOURCE_LOG_REDACTION_TIMESCALE_MARKER_KEY}_summary", self._json_dumps(result))
            if bool(result.get("ok")):
                meta_set(DATA_SOURCE_LOG_REDACTION_TIMESCALE_MARKER_KEY, "1")
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_TIMESCALE_LOG_REDACTION_MARKER_WRITE_FAILED",
                e,
                once_key="timescale_log_redaction_marker_write",
            )

    def _seed_provider_accounts(self) -> None:
        rows = self._fetch_account_rows()
        existing = {str(row.get("account_key") or "") for row in rows}
        inserts: List[Dict[str, Any]] = []
        now_ms = int(time.time() * 1000)
        for account_key, definition in self._account_catalog.items():
            if account_key in existing:
                continue
            record = {
                "account_key": str(account_key),
                "display_name": str(definition.display_name or account_key),
                "provider_name": str(definition.provider_name or account_key),
                "credentials": {},
                "status": "empty",
                "created_ts_ms": int(now_ms),
                "updated_ts_ms": int(now_ms),
            }
            record["config_hash"] = self._config_hash(record)
            inserts.append(record)
        if not inserts:
            return

        def _txn(con) -> None:
            for record in inserts:
                con.execute(
                    """
                    INSERT OR IGNORE INTO data_source_provider_accounts(
                      account_key, display_name, provider_name, credentials_enc,
                      key_version, status, last_error, last_test_ts_ms, config_hash,
                      created_ts_ms, updated_ts_ms
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(record["account_key"]),
                        str(record["display_name"]),
                        str(record["provider_name"]),
                        encrypt_credentials(record.get("credentials") or {}),
                        DEFAULT_MASTER_KEY_NAME,
                        str(record["status"]),
                        None,
                        None,
                        str(record["config_hash"]),
                        int(record["created_ts_ms"]),
                        int(record["updated_ts_ms"]),
                    ),
                )

        run_write_txn(_txn)

    def _import_legacy_env_account_config_once(self) -> None:
        marker_key = "data_source_provider_accounts_legacy_env_import_v1"
        try:
            marker_value = str(meta_get(marker_key, "") or "").strip()
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_LEGACY_ACCOUNT_ENV_IMPORT_MARKER_READ_FAILED",
                e,
                once_key="legacy_account_env_import_marker_read",
            )
            marker_value = ""

        if marker_value:
            account_env_present = False
            for definition in self._account_catalog.values():
                if self._read_env_map(definition.credential_env):
                    account_env_present = True
                    break
            if not account_env_present:
                return

        rows = {str(row.get("account_key") or ""): row for row in self._fetch_account_rows()}
        updates: List[Dict[str, Any]] = []
        imported_accounts: List[str] = []
        now_ms = int(time.time() * 1000)
        for account_key, definition in self._account_catalog.items():
            row = rows.get(str(account_key))
            if row is None:
                continue
            current_credentials, _credential_error = self._decrypt_credentials_safe(
                row.get("credentials_enc"),
                source_key=f"account:{account_key}",
                key_version=str(row.get("key_version") or DEFAULT_MASTER_KEY_NAME),
            )
            next_credentials = dict(current_credentials or {})
            changed = False
            for field_name, value in self._read_env_map(definition.credential_env).items():
                if not str(next_credentials.get(field_name) or "").strip() and str(value or "").strip():
                    next_credentials[str(field_name)] = str(value)
                    changed = True
            if not changed:
                continue
            record = {
                "account_key": account_key,
                "credentials": next_credentials,
                "status": "configured",
                "updated_ts_ms": int(now_ms),
            }
            record["config_hash"] = self._config_hash(record)
            updates.append(record)
            imported_accounts.append(account_key)

        if updates:
            def _txn(con) -> None:
                for record in updates:
                    con.execute(
                        """
                        UPDATE data_source_provider_accounts
                           SET credentials_enc = ?,
                               key_version = ?,
                               status = ?,
                               last_error = NULL,
                               config_hash = ?,
                               updated_ts_ms = ?
                         WHERE account_key = ?
                        """,
                        (
                            encrypt_credentials(record["credentials"]),
                            DEFAULT_MASTER_KEY_NAME,
                            str(record["status"]),
                            str(record["config_hash"]),
                            int(record["updated_ts_ms"]),
                            str(record["account_key"]),
                        ),
                    )

            run_write_txn(_txn)

        try:
            meta_set(
                marker_key,
                self._json_dumps(
                    {
                        "ok": True,
                        "imported": imported_accounts,
                        "ts_ms": int(now_ms),
                    }
                ),
            )
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_LEGACY_ACCOUNT_ENV_IMPORT_MARK_FAILED",
                e,
                once_key="legacy_account_env_import_mark_failed",
            )

    def _seed_builtin_sources(self) -> None:
        rows = self._fetch_rows()
        existing = {str(row["source_key"]): row for row in rows}
        inserts: List[Dict[str, Any]] = []
        now_ms = int(time.time() * 1000)

        for source_key, definition in self._catalog.items():
            if not definition.singleton or source_key in existing:
                continue
            inserts.append(
                {
                    "source_key": source_key,
                    "display_name": definition.display_name,
                    "source_type": definition.source_type,
                    "provider_name": definition.provider_name or source_key,
                    "job_name": definition.job_name,
                    "enabled": bool(definition.default_enabled),
                    "credentials": {},
                    "settings": {},
                    "created_ts_ms": int(now_ms),
                    "updated_ts_ms": int(now_ms),
                }
            )

        rss_present = any(str(row["source_type"]) == "rss_feed" for row in rows)
        if not rss_present:
            inserts.extend(self._seed_rss_sources(now_ms))

        if not inserts:
            return

        def _txn(con) -> None:
            for payload in inserts:
                con.execute(
                    """
                    INSERT OR IGNORE INTO data_sources(
                      source_key, display_name, source_type, provider_name, job_name,
                      enabled, credentials_enc, key_version, settings_json, status, last_error,
                      last_success_ts_ms, last_test_ts_ms, error_count, config_hash,
                      created_ts_ms, updated_ts_ms
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(payload["source_key"]),
                        str(payload["display_name"]),
                        str(payload["source_type"]),
                        str(payload.get("provider_name") or ""),
                        str(payload["job_name"]),
                        1 if payload.get("enabled", True) else 0,
                        encrypt_credentials(payload.get("credentials") or {}),
                        DEFAULT_MASTER_KEY_NAME,
                        self._json_dumps(payload.get("settings") or {}),
                        "seeded",
                        None,
                        None,
                        None,
                        0,
                        self._config_hash(payload),
                        int(payload["created_ts_ms"]),
                        int(payload["updated_ts_ms"]),
                    ),
                )

        run_write_txn(_txn)

    def _import_legacy_env_source_config_once(self) -> None:
        marker_key = "data_sources_legacy_env_import_v1"
        marker_value = ""
        try:
            marker_value = str(meta_get(marker_key, "") or "").strip()
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_LEGACY_ENV_IMPORT_MARKER_READ_FAILED",
                e,
                once_key="legacy_env_import_marker_read",
            )
            marker_value = ""

        if marker_value:
            env_override_present = False
            for definition in self._catalog.values():
                if not definition.singleton:
                    continue
                if self._read_env_map(definition.credential_env) or self._read_env_map(definition.setting_env):
                    env_override_present = True
                    break
            if not env_override_present:
                return

        rows = self._fetch_rows()
        if not rows:
            try:
                meta_set(marker_key, self._json_dumps({"ok": True, "imported": [], "ts_ms": int(time.time() * 1000)}))
            except Exception as e:
                _warn_nonfatal(
                    "DATA_SOURCE_MANAGER_LEGACY_ENV_IMPORT_MARK_FAILED",
                    e,
                    once_key="legacy_env_import_mark_failed_empty_rows",
                )
            return

        updates: List[Dict[str, Any]] = []
        imported_sources: List[str] = []
        now_ms = int(time.time() * 1000)

        for row in rows:
            source_key = str(row.get("source_key") or "")
            definition = self._catalog.get(source_key)
            if definition is None or not definition.singleton:
                continue

            current_credentials, _credential_error = self._decrypt_credentials_safe(
                row.get("credentials_enc"),
                source_key=source_key,
                key_version=str(row.get("key_version") or DEFAULT_MASTER_KEY_NAME),
            )
            current_settings = self._json_loads(row.get("settings_json"), {})
            if not isinstance(current_settings, dict):
                current_settings = {}

            env_credentials = self._read_env_map(definition.credential_env)
            env_settings = self._read_env_map(definition.setting_env)
            next_credentials = dict(current_credentials or {})
            next_settings = dict(current_settings or {})
            changed = False

            for field_name, value in (env_credentials or {}).items():
                if not str(next_credentials.get(field_name) or "").strip() and str(value or "").strip():
                    next_credentials[str(field_name)] = str(value)
                    changed = True

            for field_name, value in (env_settings or {}).items():
                if not str(next_settings.get(field_name) or "").strip() and str(value or "").strip():
                    next_settings[str(field_name)] = str(value)
                    changed = True

            if not changed:
                continue

            updates.append(
                {
                    "source_key": source_key,
                    "credentials": next_credentials,
                    "settings": next_settings,
                    "updated_ts_ms": int(now_ms),
                    "config_hash": self._config_hash(
                        {
                            "source_key": source_key,
                            "source_type": str(row.get("source_type") or ""),
                            "provider_name": str(row.get("provider_name") or ""),
                            "job_name": str(row.get("job_name") or ""),
                            "enabled": bool(row.get("enabled")),
                            "credentials": next_credentials,
                            "settings": next_settings,
                        }
                    ),
                }
            )
            imported_sources.append(source_key)

        if updates:
            def _txn(con) -> None:
                for record in updates:
                    con.execute(
                        """
                        UPDATE data_sources
                        SET
                          credentials_enc = ?,
                          key_version = ?,
                          settings_json = ?,
                          config_hash = ?,
                          updated_ts_ms = ?
                        WHERE source_key = ?
                        """,
                        (
                            encrypt_credentials(record["credentials"]),
                            DEFAULT_MASTER_KEY_NAME,
                            self._json_dumps(record["settings"]),
                            str(record["config_hash"]),
                            int(record["updated_ts_ms"]),
                            str(record["source_key"]),
                        ),
                    )

            run_write_txn(_txn)

        try:
            meta_set(
                marker_key,
                self._json_dumps(
                    {
                        "ok": True,
                        "imported": imported_sources,
                        "ts_ms": int(now_ms),
                    }
                ),
            )
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_LEGACY_ENV_IMPORT_MARK_FAILED",
                e,
                once_key="legacy_env_import_mark_failed",
            )

    def _seed_rss_sources(self, now_ms: int) -> List[Dict[str, Any]]:
        path = Path(os.environ.get("RSS_SOURCES_FILE") or "sources_rss.json")
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_RSS_SOURCES_PARSE_FAILED",
                e,
                once_key="rss_sources_parse",
                path=str(path),
            )
            return []
        items = payload.get("sources") if isinstance(payload, dict) else []
        out: List[Dict[str, Any]] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("url") or "").strip()
            url = str(item.get("url") or "").strip()
            if not name or not url:
                continue
            out.append(
                {
                    "source_key": self._normalize_source_key(f"rss:{name}"),
                    "display_name": str(item.get("name") or name),
                    "source_type": "rss_feed",
                    "provider_name": "rss",
                    "job_name": "ingest_now",
                    "enabled": True,
                    "credentials": {},
                    "settings": {"name": str(item.get("name") or name), "url": url},
                    "created_ts_ms": int(now_ms),
                    "updated_ts_ms": int(now_ms),
                }
            )
        return out

    def _read_env_map(self, mapping: Dict[str, str]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for field_name, env_name in (mapping or {}).items():
            value = str(os.environ.get(env_name) or "").strip()
            if value:
                out[str(field_name)] = value
        return out

    def _fetch_rows(self) -> List[Dict[str, Any]]:
        con = None
        try:
            con = connect_ro()
            columns = con.execute("PRAGMA table_info(data_sources)").fetchall() or []
            has_key_version = any(str(row[1]) == "key_version" for row in columns)
            key_version_select = "key_version" if has_key_version else "'master_key' AS key_version"
            cur = con.execute(
                f"""
                SELECT
                  id, source_key, display_name, source_type, provider_name, job_name,
                  enabled, credentials_enc, {key_version_select}, settings_json, status, last_error,
                  last_success_ts_ms, last_test_ts_ms, error_count, config_hash,
                  created_ts_ms, updated_ts_ms
                FROM data_sources
                ORDER BY source_type, display_name, source_key
                """
            )
            cols = [str(col[0]) for col in (cur.description or [])]
            return [dict(zip(cols, row)) for row in (cur.fetchall() or [])]
        except Exception as e:
            _warn_nonfatal("DATA_SOURCE_MANAGER_FETCH_ROWS_FAILED", e, once_key="fetch_rows")
            return []
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception as e:
                _warn_nonfatal(
                    "DATA_SOURCE_MANAGER_FETCH_ROWS_CLOSE_FAILED",
                    e,
                    once_key="data_source_manager_fetch_rows_close",
                )

    def _fetch_account_rows(self) -> List[Dict[str, Any]]:
        con = None
        try:
            con = connect_ro()
            columns = con.execute("PRAGMA table_info(data_source_provider_accounts)").fetchall() or []
            if not columns:
                return []
            has_key_version = any(str(row[1]) == "key_version" for row in columns)
            key_version_select = "key_version" if has_key_version else "'master_key' AS key_version"
            cur = con.execute(
                f"""
                SELECT
                  id, account_key, display_name, provider_name, credentials_enc,
                  {key_version_select}, status, last_error, last_test_ts_ms,
                  config_hash, created_ts_ms, updated_ts_ms
                FROM data_source_provider_accounts
                ORDER BY display_name, account_key
                """
            )
            cols = [str(col[0]) for col in (cur.description or [])]
            return [dict(zip(cols, row)) for row in (cur.fetchall() or [])]
        except Exception as e:
            _warn_nonfatal("DATA_SOURCE_MANAGER_FETCH_ACCOUNT_ROWS_FAILED", e, once_key="fetch_account_rows")
            return []
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception as e:
                _warn_nonfatal(
                    "DATA_SOURCE_MANAGER_FETCH_ACCOUNT_ROWS_CLOSE_FAILED",
                    e,
                    once_key="data_source_manager_fetch_account_rows_close",
                )

    def _account_credentials_by_key(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for row in self._fetch_account_rows():
            account_key = str(row.get("account_key") or "")
            credentials, _credential_error = self._decrypt_credentials_safe(
                row.get("credentials_enc"),
                source_key=f"account:{account_key}",
                key_version=str(row.get("key_version") or DEFAULT_MASTER_KEY_NAME),
            )
            out[account_key] = dict(credentials or {})
        return out

    def _strip_masked_credential_resubmissions(
        self,
        submitted: Dict[str, Any],
        existing_credentials: Dict[str, Any],
    ) -> tuple[Dict[str, str], Dict[str, str]]:
        existing = dict(existing_credentials or {})
        existing_masks = mask_credentials(existing)
        accepted: Dict[str, str] = {}
        preserved_masked: Dict[str, str] = {}
        for key, value in dict(submitted or {}).items():
            key_s = str(key)
            value_s = str(value or "").strip()
            if not value_s:
                continue
            if key_s in existing_masks and value_s == str(existing_masks.get(key_s) or ""):
                if str(existing.get(key_s) or "").strip():
                    preserved_masked[key_s] = str(existing.get(key_s) or "")
                continue
            if _looks_like_masked_credential_value(value_s):
                raise ValueError(f"masked_credential_value_rejected:{key_s}")
            accepted[key_s] = value_s
        return accepted, preserved_masked

    def _source_field_for_env(
        self,
        mapping: Dict[str, str],
        env_name: str,
    ) -> str:
        env_name_s = str(env_name or "").strip()
        for field_name, candidate in (mapping or {}).items():
            if str(candidate or "").strip() == env_name_s:
                return str(field_name)
        return ""

    def _account_field_for_env(
        self,
        definition: ProviderAccountDefinition,
        env_name: str,
    ) -> str:
        env_name_s = str(env_name or "").strip()
        for field_name, candidate in (definition.credential_env or {}).items():
            if str(candidate or "").strip() == env_name_s:
                return str(field_name)
        return ""

    def _resolve_effective_env_value(
        self,
        *,
        env_name: str,
        source_key: str,
        job_name: str,
        definition: Optional[SourceDefinition],
        credentials: Dict[str, Any],
        settings: Dict[str, Any],
        account_credentials: Dict[str, Dict[str, Any]],
        allow_external: bool,
        strict_projection: bool,
    ) -> Dict[str, Any]:
        env_name_s = str(env_name or "").strip()
        if not env_name_s:
            return {"configured": False, "origin": "missing", "secret": True}
        if definition is not None:
            credential_field = self._source_field_for_env(definition.credential_env, env_name_s)
            if credential_field:
                value = str((credentials or {}).get(credential_field) or "").strip()
                if value:
                    return {
                        "configured": True,
                        "origin": "source_override",
                        "value": value,
                        "source_field": credential_field,
                        "secret": True,
                    }
            setting_field = self._source_field_for_env(definition.setting_env, env_name_s)
            if setting_field:
                value = str((settings or {}).get(setting_field) or "").strip()
                if value:
                    return {
                        "configured": True,
                        "origin": "source_override",
                        "value": value,
                        "source_field": setting_field,
                        "secret": False,
                    }
        for account_definition in self._account_definitions_for_context(
            source_key=source_key,
            job_name=job_name,
        ):
            account_key = str(account_definition.account_key or "")
            account_field = self._account_field_for_env(account_definition, env_name_s)
            if not account_field:
                continue
            value = str((account_credentials.get(account_key) or {}).get(account_field) or "").strip()
            if value:
                return {
                    "configured": True,
                    "origin": "account",
                    "value": value,
                    "account_key": account_key,
                    "account_display_name": str(account_definition.display_name or account_key),
                    "account_field": account_field,
                    "secret": self._account_field_secret(account_key, account_field),
                }
        if allow_external:
            file_path = _credential_file_available(env_name_s)
            if file_path:
                return {
                    "configured": True,
                    "origin": "runtime_file",
                    "file_path": file_path,
                    "secret": True,
                }
            external = str(os.environ.get(env_name_s) or "").strip()
            if external and (not strict_projection):
                return {
                    "configured": True,
                    "origin": "runtime_env",
                    "value": external,
                    "secret": True,
                }
        return {"configured": False, "origin": "missing", "secret": True}

    def _credential_resolution_for_source(
        self,
        *,
        source_key: str,
        job_name: str,
        definition: Optional[SourceDefinition],
        credentials: Dict[str, Any],
        settings: Dict[str, Any],
        account_credentials: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        envs: set[str] = set()
        if definition is not None:
            envs.update(str(env_name) for env_name in (definition.credential_env or {}).values() if str(env_name).strip())
            for env_name in (definition.setting_env or {}).values():
                if str(env_name or "").strip() in self._account_env_names_for_context(source_key=source_key, job_name=job_name):
                    envs.add(str(env_name).strip())
        envs.update(self._account_env_names_for_context(source_key=source_key, job_name=job_name))
        out: List[Dict[str, Any]] = []
        strict_projection = _strict_runtime_secret_projection()
        for env_name in sorted(envs):
            resolved = self._resolve_effective_env_value(
                env_name=env_name,
                source_key=source_key,
                job_name=job_name,
                definition=definition,
                credentials=credentials,
                settings=settings,
                account_credentials=account_credentials,
                allow_external=True,
                strict_projection=strict_projection,
            )
            raw_value = str(resolved.get("value") or "").strip()
            masked_value = ""
            if raw_value:
                masked_value = str(mask_credentials({env_name: raw_value}).get(env_name) or "")
            origin = str(resolved.get("origin") or "missing")
            out.append(
                {
                    "env_var": env_name,
                    "configured": bool(resolved.get("configured")),
                    "mode": (
                        "overridden"
                        if origin == "source_override"
                        else "inherited"
                        if origin == "account"
                        else "runtime_external"
                        if origin in {"runtime_file", "runtime_env"}
                        else "missing"
                    ),
                    "source_field": str(resolved.get("source_field") or ""),
                    "account_key": str(resolved.get("account_key") or ""),
                    "account_display_name": str(resolved.get("account_display_name") or ""),
                    "account_field": str(resolved.get("account_field") or ""),
                    "masked_value": masked_value,
                    "secret": bool(resolved.get("secret", True)),
                }
            )
        return out

    def _missing_credential_metadata(
        self,
        source: Dict[str, Any],
        definition: Optional[SourceDefinition],
        env_names: Iterable[str],
    ) -> List[Dict[str, Any]]:
        source_key = str(source.get("source_key") or "")
        job_name = str(source.get("job_name") or (definition.job_name if definition else "") or "")
        source_schema = self._schema_by_field(
            self._credential_schema_fields(str(source.get("template_key") or source_key), definition)
        )
        out: List[Dict[str, Any]] = []
        for env_name in env_names:
            env_name_s = str(env_name or "").strip()
            if not env_name_s:
                continue
            source_field = self._source_field_for_env(
                dict((definition.credential_env or {}) if definition is not None else {}),
                env_name_s,
            )
            field_schema = dict(source_schema.get(source_field) or {})
            account_options: List[Dict[str, Any]] = []
            for account_definition in self._account_definitions_for_context(
                source_key=source_key,
                job_name=job_name,
            ):
                account_field = self._account_field_for_env(account_definition, env_name_s)
                if not account_field:
                    continue
                account_schema = self._schema_by_field(
                    self._account_credential_schema_fields(
                        str(account_definition.account_key or ""),
                        account_definition,
                    )
                ).get(account_field, {})
                account_options.append(
                    {
                        "account_key": str(account_definition.account_key or ""),
                        "display_name": str(account_definition.display_name or account_definition.account_key or ""),
                        "field": str(account_field),
                        "label": str(account_schema.get("label") or account_field),
                        "docs_url": str(account_schema.get("docs_url") or account_definition.guide.docs_url or ""),
                        "signup_url": str(account_schema.get("signup_url") or account_definition.guide.signup_url or ""),
                        "plan_note": str(account_schema.get("plan_note") or account_definition.guide.plan_note or ""),
                    }
                )
            guide = definition.guide if definition is not None else SourceGuide()
            out.append(
                {
                    "env_var": env_name_s,
                    "source_field": str(source_field or ""),
                    "label": str(field_schema.get("label") or env_name_s),
                    "help_text": str(field_schema.get("help_text") or ""),
                    "docs_url": str(field_schema.get("docs_url") or guide.docs_url or ""),
                    "signup_url": str(field_schema.get("signup_url") or guide.signup_url or ""),
                    "plan_note": str(field_schema.get("plan_note") or guide.plan_note or ""),
                    "account_options": account_options,
                }
            )
        return out

    def _with_projected_credential_environment(self, projected: Dict[str, str]):
        class _Overlay:
            def __init__(self, values: Dict[str, str]) -> None:
                self._values = {str(key): str(value) for key, value in dict(values or {}).items()}
                self._previous: Dict[str, Optional[str]] = {}
                managed: set[str] = set()
                for key in self._values:
                    for base_name in _credential_projection_base_names(str(key)):
                        managed.add(base_name)
                        managed.add(f"{base_name}_FILE")
                        managed.add(f"{base_name}_SECRET")
                self._managed = managed

            def __enter__(self):
                for key in self._managed:
                    self._previous[key] = os.environ.get(key)
                    if key not in self._values:
                        os.environ.pop(key, None)
                for key, value in self._values.items():
                    self._previous.setdefault(key, os.environ.get(key))
                    os.environ[key] = value
                return self

            def __exit__(self, _exc_type, _exc, _tb) -> None:
                for key, previous in self._previous.items():
                    if previous is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = previous

        return _Overlay(projected)

    def _test_effective_credentials_and_settings(
        self,
        source: Dict[str, Any],
        definition: Optional[SourceDefinition],
    ) -> tuple[Dict[str, str], Dict[str, Any]]:
        credentials = dict(source.get("credentials") or {})
        settings = dict(source.get("effective_settings") or source.get("settings") or {})
        if definition is None or not (definition.credential_env or {}):
            return {str(key): str(value) for key, value in credentials.items() if str(value or "").strip()}, settings

        account_credentials = self._account_credentials_by_key()
        strict_projection = _strict_runtime_secret_projection()
        projected: Dict[str, str] = {}
        direct_values: Dict[str, str] = {}
        for field_name, env_name in (definition.credential_env or {}).items():
            env_name_s = str(env_name or "").strip()
            if not env_name_s:
                continue
            resolved = self._resolve_effective_env_value(
                env_name=env_name_s,
                source_key=str(source.get("source_key") or ""),
                job_name=str(source.get("job_name") or ""),
                definition=definition,
                credentials=dict(source.get("credentials") or {}),
                settings=dict(source.get("settings") or {}),
                account_credentials=account_credentials,
                allow_external=True,
                strict_projection=strict_projection,
            )
            if str(resolved.get("origin") or "") in {"source_override", "account"}:
                direct_value = str(resolved.get("value") or "").strip()
                if direct_value:
                    direct_values[str(field_name)] = direct_value
            self._project_resolved_runtime_value(
                projected=projected,
                env_name=env_name_s,
                resolved=resolved,
                source_key=str(source.get("source_key") or ""),
            )

        self._clear_data_credential_cache()
        try:
            from engine.data._credentials import get_data_credential

            with self._with_projected_credential_environment(projected):
                for field_name, env_name in (definition.credential_env or {}).items():
                    env_name_s = str(env_name or "").strip()
                    if not env_name_s:
                        continue
                    value = str(get_data_credential(env_name_s, ttl_s=0) or "").strip()
                    if direct_values.get(str(field_name)):
                        credentials[str(field_name)] = direct_values[str(field_name)]
                    elif value:
                        credentials[str(field_name)] = value
        finally:
            self._clear_data_credential_cache()

        return {str(key): str(value) for key, value in credentials.items() if str(value or "").strip()}, settings

    def _materialize_provider_account(
        self,
        row: Dict[str, Any],
        *,
        include_credentials: bool = False,
    ) -> Dict[str, Any]:
        account_key = str(row.get("account_key") or "")
        definition = self._account_catalog.get(account_key)
        credentials_blob = str(row.get("credentials_enc") or "")
        key_version = str(row.get("key_version") or DEFAULT_MASTER_KEY_NAME)
        credentials, credential_error = self._decrypt_credentials_safe(
            credentials_blob,
            source_key=f"account:{account_key}",
            key_version=key_version,
        )
        configured_fields = {
            str(field): bool(str((credentials or {}).get(field) or "").strip())
            for field in ((definition.credential_env if definition is not None else {}) or {}).keys()
        }
        configured = any(configured_fields.values())
        out = {
            "id": int(row.get("id") or 0),
            "account_key": account_key,
            "display_name": str(row.get("display_name") or (definition.display_name if definition else account_key)),
            "provider_name": str(row.get("provider_name") or (definition.provider_name if definition else "")),
            "status": "error" if credential_error else ("configured" if configured else "empty"),
            "last_error": str(row.get("last_error") or ""),
            "last_test_ts_ms": int(row.get("last_test_ts_ms") or 0),
            "config_hash": str(row.get("config_hash") or ""),
            "created_ts_ms": int(row.get("created_ts_ms") or 0),
            "updated_ts_ms": int(row.get("updated_ts_ms") or 0),
            "credentials_configured": bool(configured),
            "credentials_stored": bool(credentials_blob.strip()),
            "configured_fields": configured_fields,
            "masked_credentials": mask_credentials(credentials),
            "credential_error": str(credential_error or ""),
            "credential_fields": [str(item.get("field") or "") for item in self._account_credential_schema_fields(account_key, definition)],
            "key_version": key_version,
            "guide": _guide_payload(definition.guide if definition is not None else None),
            "used_by": self._account_used_by_payload(definition) if definition is not None else [],
            "schema": self._provider_account_template_payload(account_key, definition),
        }
        if include_credentials:
            out["credentials"] = credentials
        return out

    def list_provider_accounts(self, *, include_credentials: bool = False) -> List[Dict[str, Any]]:
        self.initialize()
        return [
            self._materialize_provider_account(row, include_credentials=include_credentials)
            for row in self._fetch_account_rows()
        ]

    def get_provider_account(
        self,
        account_key: str,
        *,
        include_credentials: bool = False,
    ) -> Optional[Dict[str, Any]]:
        key = self._normalize_account_key(account_key)
        for row in self.list_provider_accounts(include_credentials=include_credentials):
            if str(row.get("account_key") or "") == key:
                return row
        return None

    def list_provider_account_templates(self) -> List[Dict[str, Any]]:
        self.initialize()
        return [
            self._provider_account_template_payload(account_key, definition)
            for account_key, definition in self._account_catalog.items()
        ]

    def update_provider_account(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.initialize()
        body = dict(payload or {})
        actor = self._normalize_actor(body.pop("actor", None))
        client_ip = str(body.pop("client_ip", "") or "").strip()[:120]
        account_key = self._normalize_account_key(body.get("account_key"))
        if not account_key:
            raise ValueError("account_key_required")
        definition = self._account_catalog.get(account_key)
        if definition is None:
            raise ValueError(f"provider_account_not_found:{account_key}")
        existing = self.get_provider_account(account_key, include_credentials=True)
        if existing is None:
            raise ValueError(f"provider_account_not_found:{account_key}")

        clear_credential_fields: List[str] = []
        if body.get("clear_credential_fields") is not None:
            raw_clear = body.get("clear_credential_fields")
            if not isinstance(raw_clear, list):
                raise ValueError("clear_credential_fields_must_be_array")
            clear_credential_fields = [
                str(name).strip()
                for name in raw_clear
                if str(name or "").strip()
            ]

        next_credentials = dict(existing.get("credentials") or {})
        submitted_credentials: Dict[str, Any] = {}
        accepted_credentials: Dict[str, str] = {}
        if body.get("credentials") is not None:
            if not isinstance(body.get("credentials"), dict):
                raise ValueError("credentials_must_be_object")
            submitted_credentials = dict(body.get("credentials") or {})
            accepted_credentials, preserved_masked = self._strip_masked_credential_resubmissions(
                submitted_credentials,
                next_credentials,
            )
            if bool(body.get("replace_credentials", False)):
                next_credentials = dict(preserved_masked)
                next_credentials.update(accepted_credentials)
            else:
                next_credentials.update(accepted_credentials)

        for field_name in clear_credential_fields:
            next_credentials.pop(str(field_name), None)

        credential_schema = self._schema_by_field(
            self._account_credential_schema_fields(account_key, definition)
        )
        if body.get("credentials") is not None:
            self._validate_allowed_fields(
                submitted_credentials,
                allowed=credential_schema.keys(),
                label="credential",
            )
            self._validate_schema_values(
                accepted_credentials,
                schema=credential_schema,
                label="credential",
            )
        if clear_credential_fields:
            self._validate_allowed_fields(
                {str(name): "" for name in clear_credential_fields},
                allowed=credential_schema.keys(),
                label="credential",
            )

        now_ms = int(time.time() * 1000)
        record = {
            "account_key": account_key,
            "display_name": str(definition.display_name or account_key),
            "provider_name": str(definition.provider_name or account_key),
            "credentials": next_credentials,
            "status": "configured" if next_credentials else "empty",
            "updated_ts_ms": int(now_ms),
        }
        record["config_hash"] = self._config_hash(record)

        def _txn(con) -> None:
            con.execute(
                """
                UPDATE data_source_provider_accounts
                   SET display_name = ?,
                       provider_name = ?,
                       credentials_enc = ?,
                       key_version = ?,
                       status = ?,
                       last_error = NULL,
                       config_hash = ?,
                       updated_ts_ms = ?
                 WHERE account_key = ?
                """,
                (
                    str(record["display_name"]),
                    str(record["provider_name"]),
                    encrypt_credentials(next_credentials),
                    DEFAULT_MASTER_KEY_NAME,
                    str(record["status"]),
                    str(record["config_hash"]),
                    int(record["updated_ts_ms"]),
                    account_key,
                ),
            )

        run_write_txn(_txn)
        self._clear_data_credential_cache()
        audit_detail = {
            "actor": actor,
            "replace_credentials": bool(body.get("replace_credentials", False)),
            "cleared_credential_fields": clear_credential_fields,
            "account_key": account_key,
            "provider_name": str(definition.provider_name or ""),
        }
        self.log_event(
            f"account:{account_key}",
            event_type="provider_account_update",
            message="provider account saved",
            detail={**audit_detail, "credentials": "[REDACTED]" if next_credentials else {}},
        )
        self.audit_action(
            f"account:{account_key}",
            action="provider_account_update",
            actor=actor,
            message="provider account saved",
            detail=audit_detail,
            client_ip=client_ip,
            source_type="provider_account",
            provider_name=str(definition.provider_name or ""),
            job_name="",
        )
        self.manage_lifecycle(reason=f"provider_account_update:{account_key}")
        return self.get_provider_account(account_key) or {"ok": True, "account_key": account_key}

    def _latest_populate_evidence(self, source_key: str) -> Dict[str, Any]:
        key = self._normalize_source_key(source_key)
        if not key:
            return {}
        con = None
        try:
            con = connect_ro()
            columns = con.execute("PRAGMA table_info(data_source_populate_evidence)").fetchall() or []
            if not columns:
                return {}
            cur = con.execute(
                """
                SELECT
                  source_key, ts_ms, status, contract_status, row_count, storage_table,
                  latest_ts_ms, latency_ms, missing_null_counts_json, duplicate_drops,
                  stale_gap_status, provider_evidence_json, contract_json, error, actor
                FROM data_source_populate_evidence
                WHERE source_key = ?
                LIMIT 1
                """,
                (key,),
            )
            row = cur.fetchone()
            if not row:
                return {}
            cols = [str(col[0]) for col in (cur.description or [])]
            raw = dict(zip(cols, row))
            return {
                "source_key": str(raw.get("source_key") or key),
                "ts_ms": int(raw.get("ts_ms") or 0),
                "status": str(raw.get("status") or ""),
                "contract_status": str(raw.get("contract_status") or ""),
                "row_count": int(raw.get("row_count") or 0),
                "storage_table": str(raw.get("storage_table") or ""),
                "latest_ts_ms": int(raw.get("latest_ts_ms") or 0),
                "latency_ms": int(raw.get("latency_ms") or 0),
                "missing_null_counts": self._json_loads(raw.get("missing_null_counts_json"), {}),
                "duplicate_drops": int(raw.get("duplicate_drops") or 0),
                "stale_gap_status": str(raw.get("stale_gap_status") or ""),
                "provider_evidence": sanitize_data_source_log_detail(
                    self._json_loads(raw.get("provider_evidence_json"), {})
                ),
                "data_contract": self._json_loads(raw.get("contract_json"), {}),
                "error": str(raw.get("error") or ""),
                "actor": str(raw.get("actor") or ""),
            }
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_POPULATE_EVIDENCE_READ_FAILED",
                e,
                once_key=f"populate_evidence_read:{key}",
                source_key=key,
            )
            return {}
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception as e:
                _warn_nonfatal(
                    "DATA_SOURCE_MANAGER_POPULATE_EVIDENCE_READ_CLOSE_FAILED",
                    e,
                    once_key="data_source_manager_populate_evidence_read_close",
                )

    def _materialize_source(self, row: Dict[str, Any], *, include_credentials: bool = False) -> Dict[str, Any]:
        source_key = str(row.get("source_key") or "")
        source_type = str(row.get("source_type") or "")
        provider_name = str(row.get("provider_name") or "")
        template_key, definition = self._resolve_definition(
            source_key,
            source_type=source_type,
            provider_name=provider_name,
        )
        settings = self._json_loads(row.get("settings_json"), {})
        credentials_blob = str(row.get("credentials_enc") or "")
        key_version = str(row.get("key_version") or DEFAULT_MASTER_KEY_NAME)
        credentials, credential_error = self._decrypt_credentials_safe(
            credentials_blob,
            source_key=source_key,
            key_version=key_version,
        )
        account_credentials = self._account_credentials_by_key()
        credential_resolution = self._credential_resolution_for_source(
            source_key=source_key,
            job_name=str(row.get("job_name") or (definition.job_name if definition else "")),
            definition=definition,
            credentials=dict(credentials or {}),
            settings=settings if isinstance(settings, dict) else {},
            account_credentials=account_credentials,
        )
        effective_credentials = dict(credentials or {})
        effective_settings = dict(settings if isinstance(settings, dict) else {})
        if definition is not None:
            for field_name, env_name in (definition.credential_env or {}).items():
                if str(effective_credentials.get(field_name) or "").strip():
                    continue
                resolved = self._resolve_effective_env_value(
                    env_name=str(env_name),
                    source_key=source_key,
                    job_name=str(row.get("job_name") or (definition.job_name if definition else "")),
                    definition=definition,
                    credentials=dict(credentials or {}),
                    settings=settings if isinstance(settings, dict) else {},
                    account_credentials=account_credentials,
                    allow_external=False,
                    strict_projection=_strict_runtime_secret_projection(),
                )
                if bool(resolved.get("configured")) and str(resolved.get("value") or "").strip():
                    effective_credentials[str(field_name)] = str(resolved.get("value") or "")
            for field_name, env_name in (definition.setting_env or {}).items():
                if str(effective_settings.get(field_name) or "").strip():
                    continue
                resolved = self._resolve_effective_env_value(
                    env_name=str(env_name),
                    source_key=source_key,
                    job_name=str(row.get("job_name") or (definition.job_name if definition else "")),
                    definition=definition,
                    credentials=dict(credentials or {}),
                    settings=settings if isinstance(settings, dict) else {},
                    account_credentials=account_credentials,
                    allow_external=False,
                    strict_projection=_strict_runtime_secret_projection(),
                )
                if bool(resolved.get("configured")) and str(resolved.get("value") or "").strip():
                    effective_settings[str(field_name)] = str(resolved.get("value") or "")
        required_credential_envs = [
            str(env_name or "").strip()
            for env_name in ((definition.credential_env or {}).values() if definition is not None else [])
            if str(env_name or "").strip()
        ]
        required_credentials_configured = all(
            any(
                str(item.get("env_var") or "") == env_name and bool(item.get("configured"))
                for item in credential_resolution
            )
            for env_name in required_credential_envs
        ) if required_credential_envs else bool(credentials)
        inherited_credentials_configured = any(
            str(item.get("mode") or "") == "inherited" and bool(item.get("configured"))
            for item in credential_resolution
        )
        is_builtin = bool(template_key and template_key in self._catalog and source_key == template_key and definition and definition.singleton and template_key != CUSTOM_RSS_TEMPLATE_KEY)
        operational = _catalog_operational_metadata(template_key or source_key, definition)
        contract = _data_contract_for_source(template_key or source_key, definition)
        populate_evidence = self._latest_populate_evidence(source_key)
        runtime_assessment = self._source_runtime_projection_assessment(
            {
                **dict(row),
                "source_key": source_key,
                "source_type": source_type,
                "provider_name": provider_name,
                "job_name": str(row.get("job_name") or (definition.job_name if definition else "")),
                "enabled": bool(int(row.get("enabled") or 0) == 1),
                "credentials": credentials,
                "settings": settings if isinstance(settings, dict) else {},
                "runtime_runnable": bool(operational["runtime_runnable"]),
            },
            account_credentials=account_credentials,
            project_credentials=False,
        )
        out = {
            "id": int(row.get("id") or 0),
            "source_key": source_key,
            "display_name": str(row.get("display_name") or (definition.display_name if definition else "")),
            "source_type": str(source_type or (definition.source_type if definition else "")),
            "provider_name": str(provider_name or ""),
            "job_name": str(row.get("job_name") or (definition.job_name if definition else "")),
            "enabled": bool(int(row.get("enabled") or 0) == 1),
            "default_enabled": bool(definition.default_enabled) if definition is not None else False,
            "storage_tables": list(operational["storage_tables"]),
            "consumers": list(operational["consumers"]),
            "safe_to_auto_enable": bool(operational["safe_to_auto_enable"]),
            "runtime_runnable": bool(operational["runtime_runnable"]),
            "data_contract": contract.payload(),
            "populate_evidence": populate_evidence,
            "runnable_state": str(runtime_assessment.get("runnable_state") or RUNNABLE_STATE_OFF),
            "runnable_state_reason": str(runtime_assessment.get("runnable_state_reason") or ""),
            "credential_required": bool(runtime_assessment.get("credential_required")),
            "runtime_credentialed": bool(runtime_assessment.get("runtime_credentialed")),
            "runtime_projected": bool(runtime_assessment.get("runtime_projected")),
            "runtime_desired_eligible": bool(runtime_assessment.get("runtime_desired_eligible")),
            "missing_credential_env_vars": list(runtime_assessment.get("missing_credential_env_vars") or []),
            "projected_env_vars": list(runtime_assessment.get("projected_env_vars") or []),
            "settings": settings if isinstance(settings, dict) else {},
            "status": str(row.get("status") or "unknown"),
            "last_error": str(row.get("last_error") or ""),
            "last_success_ts_ms": int(row.get("last_success_ts_ms") or 0),
            "last_test_ts_ms": int(row.get("last_test_ts_ms") or 0),
            "error_count": int(row.get("error_count") or 0),
            "config_hash": str(row.get("config_hash") or ""),
            "created_ts_ms": int(row.get("created_ts_ms") or 0),
            "updated_ts_ms": int(row.get("updated_ts_ms") or 0),
            "credentials_configured": bool(required_credentials_configured),
            "source_credentials_configured": bool(credentials),
            "inherited_credentials_configured": bool(inherited_credentials_configured),
            "credentials_stored": bool(credentials_blob.strip()),
            "key_version": key_version,
            "credential_error": str(credential_error or ""),
            "credential_fields": [str(item.get("field") or "") for item in self._credential_schema_fields(template_key, definition)],
            "setting_fields": [str(item.get("field") or "") for item in self._setting_schema_fields(template_key, definition)],
            "masked_credentials": mask_credentials(credentials),
            "credential_resolution": credential_resolution,
            "account_keys": [str(item.account_key) for item in self._account_definitions_for_context(source_key=source_key, job_name=str(row.get("job_name") or ""))],
            "template_key": str(template_key or ""),
            "builtin": bool(is_builtin),
            "singleton": bool(definition.singleton) if definition is not None else False,
            "can_delete": not bool(is_builtin),
            "can_edit_identity": not bool(is_builtin),
            "can_edit_routing": False,
            "supports_test": True,
        }
        if include_credentials:
            out["credentials"] = credentials
            out["effective_credentials"] = effective_credentials
            out["effective_settings"] = effective_settings
        return out

    def list_sources(self, *, include_credentials: bool = False) -> List[Dict[str, Any]]:
        """Return materialized source rows for the operator control plane.

        Parameters
        ----------
        include_credentials : bool, default=False
            When ``True``, decrypted credentials are included in each returned
            row. When ``False``, only masked credentials and presence flags are
            exposed.

        Returns
        -------
        list of dict
            Materialized source payloads with timestamps in epoch milliseconds,
            settings as dictionaries, and credential-status metadata.
        """
        self.initialize()
        return [
            self._materialize_source(row, include_credentials=include_credentials)
            for row in self._fetch_rows()
        ]

    def get_source(self, source_key: str, *, include_credentials: bool = False) -> Optional[Dict[str, Any]]:
        """Return a single materialized source by normalized key.

        Parameters
        ----------
        source_key : str
            Source identifier to resolve.
        include_credentials : bool, default=False
            Whether to include decrypted credentials in the returned row.

        Returns
        -------
        dict or None
            Matching source payload, or ``None`` when the source does not
            exist.
        """
        key = self._normalize_source_key(source_key)
        for row in self.list_sources(include_credentials=include_credentials):
            if str(row.get("source_key") or "") == key:
                return row
        return None

    def create_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new configurable source row.

        Parameters
        ----------
        payload : dict
            Source payload containing identity, routing, settings, credentials,
            and optional operator metadata such as ``actor`` or ``client_ip``.

        Returns
        -------
        dict
            Persisted source payload in the same shape returned by
            :meth:`list_sources` with credentials included.

        Raises
        ------
        ValueError
            If required fields are missing, immutable built-in fields are
            changed, or the source already exists.
        """
        return self._upsert_source(payload, create_only=True)

    def update_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing source row.

        Parameters
        ----------
        payload : dict
            Source payload containing editable fields plus the target
            ``source_key``.

        Returns
        -------
        dict
            Persisted source payload in the same shape returned by
            :meth:`list_sources` with credentials included.

        Raises
        ------
        ValueError
            If the source does not exist or the update violates built-in field
            constraints.
        """
        return self._upsert_source(payload, create_only=False)

    def _upsert_source(self, payload: Dict[str, Any], *, create_only: bool) -> Dict[str, Any]:
        self.initialize()
        body = dict(payload or {})
        actor = self._normalize_actor(body.pop("actor", None))
        client_ip = str(body.pop("client_ip", "") or "").strip()[:120]
        source_key = self._normalize_source_key(
            body.get("source_key")
            or body.get("provider_name")
            or body.get("display_name")
            or body.get("source_type")
        )
        if not source_key:
            raise ValueError("source_key_required")

        existing = self.get_source(source_key, include_credentials=True)
        if create_only and existing is not None:
            raise ValueError(f"source_exists:{source_key}")
        if not create_only and existing is None:
            raise ValueError(f"source_not_found:{source_key}")

        source_type = str(body.get("source_type") or (existing or {}).get("source_type") or "").strip()
        provider_name = str(
            body.get("provider_name")
            or (existing or {}).get("provider_name")
            or source_key
        ).strip()
        template_key, definition = self._resolve_definition(
            source_key,
            source_type=source_type,
            provider_name=provider_name,
        )
        if not source_type and definition is not None:
            source_type = definition.source_type
        if not source_type:
            raise ValueError("source_type_required")

        display_name = str(
            body.get("display_name")
            or (existing or {}).get("display_name")
            or (definition.display_name if definition else source_key)
        ).strip()
        job_name = str(
            body.get("job_name")
            or (existing or {}).get("job_name")
            or (definition.job_name if definition else "")
        ).strip()
        if not job_name:
            raise ValueError("job_name_required")

        source_type, provider_name, job_name = self._apply_builtin_constraints(
            source_key=source_key,
            definition=definition,
            existing=existing,
            create_only=create_only,
            body=body,
            source_type=source_type,
            provider_name=provider_name,
            job_name=job_name,
        )

        enabled = bool(
            body.get("enabled")
            if "enabled" in body
            else (existing or {}).get("enabled")
            if existing is not None
            else (definition.default_enabled if definition else True)
        )

        next_settings = dict((existing or {}).get("settings") or {})
        if body.get("settings") is not None:
            if not isinstance(body.get("settings"), dict):
                raise ValueError("settings_must_be_object")
            next_settings = dict(body.get("settings") or {})

        clear_credential_fields: List[str] = []
        if body.get("clear_credential_fields") is not None:
            raw_clear = body.get("clear_credential_fields")
            if not isinstance(raw_clear, list):
                raise ValueError("clear_credential_fields_must_be_array")
            clear_credential_fields = [
                str(name).strip()
                for name in raw_clear
                if str(name or "").strip()
            ]

        next_credentials = dict((existing or {}).get("credentials") or {})
        submitted_credentials: Dict[str, Any] = {}
        if body.get("credentials") is not None:
            if not isinstance(body.get("credentials"), dict):
                raise ValueError("credentials_must_be_object")
            submitted_credentials = dict(body.get("credentials") or {})
            accepted_credentials, preserved_masked_credentials = self._strip_masked_credential_resubmissions(
                submitted_credentials,
                next_credentials,
            )
            if bool(body.get("replace_credentials", False)):
                next_credentials = dict(preserved_masked_credentials)
                next_credentials.update(accepted_credentials)
            else:
                next_credentials.update(accepted_credentials)
        for field_name in clear_credential_fields:
            next_credentials.pop(str(field_name), None)

        if definition is not None:
            credential_schema = self._schema_by_field(
                self._credential_schema_fields(template_key, definition)
            )
            setting_schema = self._schema_by_field(
                self._setting_schema_fields(template_key, definition)
            )
            if body.get("credentials") is not None:
                self._validate_allowed_fields(
                    submitted_credentials,
                    allowed=credential_schema.keys(),
                    label="credential",
                )
                self._validate_schema_values(
                    accepted_credentials,
                    schema=credential_schema,
                    label="credential",
                )
            if clear_credential_fields:
                self._validate_allowed_fields(
                    {str(name): "" for name in clear_credential_fields},
                    allowed=credential_schema.keys(),
                    label="credential",
                )
            if body.get("settings") is not None:
                submitted_settings = dict(body.get("settings") or {})
                self._validate_allowed_fields(
                    submitted_settings,
                    allowed=setting_schema.keys(),
                    label="setting",
                )
                self._validate_schema_values(
                    submitted_settings,
                    schema=setting_schema,
                    label="setting",
                )

        if source_type == "rss_feed":
            name_value = str(next_settings.get("name") or display_name or "").strip()
            url_value = str(next_settings.get("url") or "").strip()
            if not name_value:
                raise ValueError("rss_name_required")
            if not url_value:
                raise ValueError("rss_url_required")
            next_settings["name"] = name_value
            next_settings["url"] = url_value
            if definition is not None:
                self._validate_schema_values(
                    next_settings,
                    schema=self._schema_by_field(
                        self._setting_schema_fields(template_key, definition)
                    ),
                    label="setting",
                    require_required_fields=True,
                )

        now_ms = int(time.time() * 1000)
        record = {
            "source_key": source_key,
            "display_name": display_name,
            "source_type": source_type,
            "provider_name": provider_name,
            "job_name": job_name,
            "enabled": enabled,
            "credentials": next_credentials,
            "settings": next_settings,
            "created_ts_ms": int((existing or {}).get("created_ts_ms") or now_ms),
            "updated_ts_ms": int(now_ms),
            "status": str((existing or {}).get("status") or "configured"),
            "last_error": str((existing or {}).get("last_error") or ""),
            "last_success_ts_ms": int((existing or {}).get("last_success_ts_ms") or 0),
            "last_test_ts_ms": int((existing or {}).get("last_test_ts_ms") or 0),
            "error_count": int((existing or {}).get("error_count") or 0),
            "key_version": DEFAULT_MASTER_KEY_NAME,
        }
        record["config_hash"] = self._config_hash(record)

        def _txn(con) -> None:
            con.execute(
                """
                INSERT INTO data_sources(
                  source_key, display_name, source_type, provider_name, job_name,
                  enabled, credentials_enc, key_version, settings_json, status, last_error,
                  last_success_ts_ms, last_test_ts_ms, error_count, config_hash,
                  created_ts_ms, updated_ts_ms
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source_key) DO UPDATE SET
                  display_name=excluded.display_name,
                  source_type=excluded.source_type,
                  provider_name=excluded.provider_name,
                  job_name=excluded.job_name,
                  enabled=excluded.enabled,
                  credentials_enc=excluded.credentials_enc,
                  key_version=excluded.key_version,
                  settings_json=excluded.settings_json,
                  status=excluded.status,
                  last_error=excluded.last_error,
                  last_success_ts_ms=excluded.last_success_ts_ms,
                  last_test_ts_ms=excluded.last_test_ts_ms,
                  error_count=excluded.error_count,
                  config_hash=excluded.config_hash,
                  updated_ts_ms=excluded.updated_ts_ms
                """,
                (
                    source_key,
                    display_name,
                    source_type,
                    provider_name,
                    job_name,
                    1 if enabled else 0,
                    encrypt_credentials(next_credentials),
                    DEFAULT_MASTER_KEY_NAME,
                    self._json_dumps(next_settings),
                    record["status"],
                    (record["last_error"] or None),
                    (record["last_success_ts_ms"] or None),
                    (record["last_test_ts_ms"] or None),
                    int(record["error_count"]),
                    str(record["config_hash"]),
                    int(record["created_ts_ms"]),
                    int(record["updated_ts_ms"]),
                ),
            )

        run_write_txn(_txn)
        self._clear_data_credential_cache()
        audit_detail = {
            "actor": actor,
            "replace_credentials": bool(body.get("replace_credentials", False)),
            "cleared_credential_fields": clear_credential_fields,
            "template_key": template_key,
            "builtin": bool(source_key in self._catalog and source_key != CUSTOM_RSS_TEMPLATE_KEY),
        }
        log_record = {key: value for key, value in record.items() if key != "credentials"}
        if next_credentials:
            log_record["credentials"] = "[REDACTED]"
        self.log_event(
            source_key,
            event_type="upsert",
            message="source saved",
            detail={**audit_detail, **log_record},
        )
        self.audit_action(
            source_key,
            action="create" if create_only else "update",
            actor=actor,
            message="source saved",
            detail=audit_detail,
            client_ip=client_ip,
            source_type=source_type,
            provider_name=provider_name,
            job_name=job_name,
        )
        self.manage_lifecycle(reason=f"source_upsert:{source_key}")
        return self.get_source(source_key) or {"ok": True, "source_key": source_key}

    def delete_source(self, source_key: str, *, actor: str = "operator", client_ip: str = "") -> Dict[str, Any]:
        """Delete a configurable source and record the control-plane action.

        Parameters
        ----------
        source_key : str
            Source identifier to remove.
        actor : str, default="operator"
            Actor name recorded in audit trails.
        client_ip : str, default=""
            Client IP recorded in audit trails.

        Returns
        -------
        dict
            Delete acknowledgement containing ``ok`` and ``source_key``.

        Raises
        ------
        ValueError
            If the source does not exist or is a protected built-in source.

        Side Effects
        ------------
        Deletes the database row, writes audit/log records, and marks runtime
        configuration dirty via lifecycle reconciliation.
        """
        self.initialize()
        key = self._normalize_source_key(source_key)
        source = self.get_source(key, include_credentials=True)
        if source is None:
            raise ValueError(f"source_not_found:{key}")
        if bool(source.get("builtin")):
            raise ValueError(f"builtin_source_delete_not_allowed:{key}")

        def _txn(con) -> None:
            con.execute("DELETE FROM data_sources WHERE source_key = ?", (key,))
            delete_data_source_logs_for_source(con, key)

        run_write_txn(_txn)
        self.audit_action(
            key,
            action="delete",
            actor=actor,
            message="source deleted",
            detail={"builtin": False},
            client_ip=client_ip,
            source_type=str(source.get("source_type") or ""),
            provider_name=str(source.get("provider_name") or ""),
            job_name=str(source.get("job_name") or ""),
        )
        self.manage_lifecycle(reason=f"source_delete:{key}")
        return {"ok": True, "source_key": key}

    def set_enabled(self, source_key: str, enabled: bool, *, actor: str = "operator", client_ip: str = "") -> Dict[str, Any]:
        """Enable or disable a source through the normal update path.

        Parameters
        ----------
        source_key : str
            Source identifier to toggle.
        enabled : bool
            Desired enabled state.
        actor : str, default="operator"
            Actor name recorded in audit trails.
        client_ip : str, default=""
            Client IP recorded in audit trails.

        Returns
        -------
        dict
            Updated source payload.

        Raises
        ------
        ValueError
            If the source does not exist.

        Side Effects
        ------------
        Persists the new enabled state and records both log and audit entries
        describing the toggle.
        """
        source = self.get_source(source_key, include_credentials=True)
        if source is None:
            raise ValueError(f"source_not_found:{source_key}")
        payload = dict(source)
        payload["enabled"] = bool(enabled)
        payload["replace_credentials"] = True
        payload["actor"] = actor
        payload["client_ip"] = client_ip
        updated = self.update_source(payload)
        self.log_event(
            str(updated.get("source_key") or source_key),
            event_type="enabled" if enabled else "disabled",
            message=f"source {'enabled' if enabled else 'disabled'}",
            detail={
                "actor": self._normalize_actor(actor),
                "runnable_state": str(updated.get("runnable_state") or ""),
                "runnable_state_reason": str(updated.get("runnable_state_reason") or ""),
                "job_name": str(updated.get("job_name") or ""),
                "runtime_desired_eligible": bool(updated.get("runtime_desired_eligible")),
                "missing_credential_env_vars": list(updated.get("missing_credential_env_vars") or []),
            },
        )
        self.audit_action(
            str(updated.get("source_key") or source_key),
            action="enable" if enabled else "disable",
            actor=actor,
            message=f"source {'enabled' if enabled else 'disabled'}",
            detail={"enabled": bool(enabled)},
            client_ip=client_ip,
            source_type=str(updated.get("source_type") or ""),
            provider_name=str(updated.get("provider_name") or ""),
            job_name=str(updated.get("job_name") or ""),
        )
        return updated

    def _project_resolved_runtime_value(
        self,
        *,
        projected: Dict[str, str],
        env_name: str,
        resolved: Dict[str, Any],
        source_key: str,
    ) -> bool:
        env_name_s = str(env_name or "").strip()
        if not env_name_s or not bool(resolved.get("configured")):
            return False
        if str(resolved.get("origin") or "") == "runtime_file":
            file_path = str(resolved.get("file_path") or "").strip()
            if file_path:
                projected[f"{env_name_s}_FILE"] = file_path
                return True
            return False
        value = str(resolved.get("value") or "").strip()
        if not value:
            return False
        if _strict_runtime_secret_projection() and bool(resolved.get("secret", True)):
            try:
                projected[f"{env_name_s}_FILE"] = str(_write_runtime_credential_file(env_name_s, value))
            except Exception as exc:
                _warn_nonfatal(
                    "DATA_SOURCE_MANAGER_RUNTIME_CREDENTIAL_FILE_WRITE_FAILED",
                    exc,
                    once_key=f"runtime_credential_file_write:{env_name_s}",
                    source_key=str(source_key or ""),
                    env_name=env_name_s,
                )
                return False
        else:
            projected[env_name_s] = value
        return True

    def build_job_environment(self, job_name: str) -> Dict[str, str]:
        self.initialize()
        job_name_s = str(job_name or "").strip()
        if safe_no_credential_market_data_mode():
            if job_name_s == "poll_prices":
                return dict(_SAFE_NO_CREDENTIAL_ENV)
        self._withdraw_previously_projected_runtime_keys()
        env: Dict[str, str] = {}
        price_providers: List[str] = []
        option_providers: List[str] = []
        strict_projection = _strict_runtime_secret_projection()
        account_credentials = self._account_credentials_by_key()
        rows: List[Dict[str, Any]] = []
        for row in self.list_sources(include_credentials=True):
            if str(row.get("job_name") or "") != job_name_s:
                continue
            assessment = self._source_runtime_projection_assessment(
                row,
                account_credentials=account_credentials,
                strict_projection=strict_projection,
                project_credentials=True,
            )
            if not bool(assessment.get("runtime_desired_eligible")):
                continue
            next_row = dict(row)
            next_row["_projected_credentials"] = dict(assessment.get("projected_credentials") or {})
            rows.append(next_row)
        active_row_count = 0

        for row in rows:
            definition = self._catalog.get(str(row.get("source_key") or ""))
            credentials = dict(row.get("credentials") or {})
            settings = dict(row.get("settings") or {})
            provider_name = str(row.get("provider_name") or row.get("source_key") or "").strip().lower()
            projected_credentials: Dict[str, str] = dict(row.get("_projected_credentials") or {})
            active_row_count += 1

            if str(row.get("source_type") or "") == "price_provider" and provider_name:
                price_providers.append(provider_name)
            if str(row.get("source_type") or "") == "options_provider" and provider_name:
                option_providers.append(provider_name)

            if definition is None:
                continue
            env.update(projected_credentials)
            for account_env_name in sorted(
                self._account_env_names_for_context(
                    source_key=str(row.get("source_key") or ""),
                    job_name=job_name_s,
                )
            ):
                if account_env_name in env or f"{account_env_name}_FILE" in env:
                    continue
                resolved = self._resolve_effective_env_value(
                    env_name=account_env_name,
                    source_key=str(row.get("source_key") or ""),
                    job_name=job_name_s,
                    definition=definition,
                    credentials=credentials,
                    settings=settings,
                    account_credentials=account_credentials,
                    allow_external=True,
                    strict_projection=strict_projection,
                )
                self._project_resolved_runtime_value(
                    projected=env,
                    env_name=account_env_name,
                    resolved=resolved,
                    source_key=str(row.get("source_key") or ""),
                )
            for field_name, env_name in (definition.setting_env or {}).items():
                value = settings.get(field_name)
                if value is not None and str(value).strip() != "":
                    env[str(env_name)] = self._env_string(value)

        for account_definition in self._account_definitions_for_context(job_name=job_name_s):
            for _field_name, env_name in (account_definition.credential_env or {}).items():
                env_name_s = str(env_name or "").strip()
                if not env_name_s or env_name_s in env or f"{env_name_s}_FILE" in env:
                    continue
                resolved = self._resolve_effective_env_value(
                    env_name=env_name_s,
                    source_key="",
                    job_name=job_name_s,
                    definition=None,
                    credentials={},
                    settings={},
                    account_credentials=account_credentials,
                    allow_external=True,
                    strict_projection=strict_projection,
                )
                self._project_resolved_runtime_value(
                    projected=env,
                    env_name=env_name_s,
                    resolved=resolved,
                    source_key=f"account:{account_definition.account_key}",
                )

        if job_name_s == "poll_prices":
            chain = self._provider_chain(price_providers)
            if chain:
                env["LIVE_PRICE_PROVIDER_CHAIN"] = ",".join(chain)
            env["POLYGON_REST_ENABLED"] = "1" if "polygon" in chain else "0"
            env["YFINANCE_ENABLED"] = "1" if "yfinance" in chain else "0"
            env["SIMULATED_MARKET_DATA_ENABLED"] = "1" if "simulated" in chain else "0"
            env["CCXT_ENABLED"] = "1" if "ccxt" in chain else "0"
            env["OANDA_ENABLED"] = "1" if "oanda" in chain else "0"
            env["FX_PAIRS_ENABLED"] = "1" if "oanda" in chain else "0"
        elif job_name_s == "stream_prices_polygon_ws":
            env["POLYGON_WS_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "stream_prices_ibkr":
            env["IBKR_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "options_poll":
            chain = self._provider_chain(option_providers)
            if chain:
                env["OPTIONS_PROVIDER_CHAIN"] = ",".join(chain)
            env["TRADIER_ENABLED"] = "1" if "tradier" in chain else "0"
            env["POLYGON_REST_ENABLED"] = "1" if "polygon" in chain else "0"
        elif job_name_s == "ingest_etf_flows":
            env["INGEST_ETF_FLOW_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "ingest_cftc_cot":
            env["INGEST_CFTC_COT_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "ingest_finra_short_volume":
            env["INGEST_FINRA_SHORT_VOLUME_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "ingest_finra_short_interest":
            env["INGEST_FINRA_SHORT_INTEREST_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "ingest_crypto_funding":
            env["INGEST_CRYPTO_FUNDING_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "ingest_13f":
            env["INGEST_13F_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "ingest_quiver_gov":
            env["INGEST_QUIVER_GOV_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "ingest_fundamentals_pit":
            env["INGEST_FUNDAMENTALS_PIT_ENABLED"] = "1" if active_row_count > 0 else "0"
        elif job_name_s == "ingest_now":
            enabled_keys = {str(row.get("source_key") or "") for row in rows}
            env["INGEST_NOW_ENABLE_COMPANY_NEWS"] = "1" if "company_news" in enabled_keys else "0"
            env["INGEST_NOW_ENABLE_TRANSCRIPTS"] = "1" if "transcripts" in enabled_keys else "0"
            env["INGEST_NOW_ENABLE_GDELT"] = "1" if "gdelt" in enabled_keys else "0"

        return env

    def apply_runtime_environment(self) -> Dict[str, str]:
        self.initialize()
        if safe_no_credential_market_data_mode():
            return apply_safe_no_credential_runtime_environment()

        self._withdraw_previously_projected_runtime_keys()

        merged: Dict[str, str] = {}
        job_names = {
            str(row.get("job_name") or "")
            for row in self.list_sources(include_credentials=True)
            if str(row.get("job_name") or "").strip()
        }
        job_names.update(
            {
                "poll_prices",
                "stream_prices_polygon_ws",
                "stream_prices_ibkr",
                "options_poll",
                "ingest_now",
                "ingest_etf_flows",
                "ingest_cftc_cot",
                "ingest_finra_short_volume",
                "ingest_finra_short_interest",
                "ingest_crypto_funding",
                "ingest_13f",
                "ingest_quiver_gov",
                "ingest_fundamentals_pit",
            }
        )
        for job_name in sorted(job_names):
            merged.update(self.build_job_environment(job_name))
        for key in credential_runtime_env_keys():
            os.environ.pop(str(key), None)
            os.environ.pop(f"{str(key)}_FILE", None)
        for key, value in merged.items():
            os.environ[str(key)] = str(value)
        projected_keys = sorted(
            str(key)
            for key in merged.keys()
            if str(key) in set(credential_runtime_env_keys()) or str(key).endswith("_FILE")
        )
        if projected_keys:
            os.environ[_PROJECTED_RUNTIME_KEYS_ENV] = ",".join(projected_keys)
        else:
            os.environ.pop(_PROJECTED_RUNTIME_KEYS_ENV, None)
        return merged

    def _source_has_runtime_credentials(self, row: Dict[str, Any]) -> bool:
        if str(row.get("provider_name") or "").strip().lower() != "polygon":
            return False
        if not bool(row.get("enabled")):
            return False
        definition = self._catalog.get(str(row.get("source_key") or ""))
        if definition is None:
            return False
        credential_env = dict(definition.credential_env or {})
        if not credential_env:
            return True
        credentials = dict(row.get("credentials") or {})
        settings = dict(row.get("settings") or {})
        account_credentials = self._account_credentials_by_key()
        strict_projection = _strict_runtime_secret_projection()
        for field_name, env_name in credential_env.items():
            env_name_s = str(env_name or "").strip()
            if not env_name_s:
                continue
            resolved = self._resolve_effective_env_value(
                env_name=env_name_s,
                source_key=str(row.get("source_key") or ""),
                job_name=str(row.get("job_name") or ""),
                definition=definition,
                credentials=credentials,
                settings=settings,
                account_credentials=account_credentials,
                allow_external=True,
                strict_projection=strict_projection,
            )
            if bool(resolved.get("configured")):
                continue
            return False
        return True

    def _source_row_runtime_runnable(self, row: Dict[str, Any]) -> bool:
        if "runtime_runnable" in row:
            return bool(row.get("runtime_runnable"))
        source_key = str(row.get("source_key") or "")
        template_key, definition = self._resolve_definition(
            source_key,
            source_type=str(row.get("source_type") or ""),
            provider_name=str(row.get("provider_name") or ""),
        )
        return bool(_catalog_operational_metadata(template_key or source_key, definition).get("runtime_runnable", True))

    def _source_runtime_projection_assessment(
        self,
        row: Dict[str, Any],
        *,
        account_credentials: Optional[Dict[str, Dict[str, Any]]] = None,
        strict_projection: Optional[bool] = None,
        project_credentials: bool = False,
    ) -> Dict[str, Any]:
        """Return the production scheduling/projection state for one source row."""
        source_key = str(row.get("source_key") or "")
        source_type = str(row.get("source_type") or "")
        provider_name = str(row.get("provider_name") or "")
        job_name = str(row.get("job_name") or "")
        template_key, definition = self._resolve_definition(
            source_key,
            source_type=source_type,
            provider_name=provider_name,
        )
        runtime_runnable = bool(self._source_row_runtime_runnable(row))
        assert_data_source_broker_runtime_allowed(
            source_key=source_key,
            source_type=source_type,
            provider_name=provider_name,
            job_name=job_name,
            runtime_runnable=runtime_runnable,
        )
        enabled = bool(row.get("enabled"))
        strict = _strict_runtime_secret_projection() if strict_projection is None else bool(strict_projection)
        accounts = account_credentials if account_credentials is not None else self._account_credentials_by_key()
        credentials = dict(row.get("effective_credentials") or row.get("credentials") or {})
        settings = dict(row.get("effective_settings") or row.get("settings") or {})
        credential_env = dict((definition.credential_env or {}) if definition is not None else {})
        missing_env_vars: List[str] = []
        projected_env_vars: List[str] = []
        projected_credentials: Dict[str, str] = {}
        projection_failed = False

        for _field_name, env_name in credential_env.items():
            env_name_s = str(env_name or "").strip()
            if not env_name_s:
                continue
            resolved = self._resolve_effective_env_value(
                env_name=env_name_s,
                source_key=source_key,
                job_name=job_name,
                definition=definition,
                credentials=credentials,
                settings=settings,
                account_credentials=accounts or {},
                allow_external=True,
                strict_projection=strict,
            )
            if not bool(resolved.get("configured")):
                missing_env_vars.append(env_name_s)
                continue
            if bool(project_credentials) and bool(runtime_runnable):
                before_keys = set(projected_credentials.keys())
                if not self._project_resolved_runtime_value(
                    projected=projected_credentials,
                    env_name=env_name_s,
                    resolved=resolved,
                    source_key=source_key,
                ):
                    missing_env_vars.append(env_name_s)
                    projection_failed = True
                    continue
                projected_env_vars.extend(
                    sorted(str(key) for key in set(projected_credentials.keys()) - before_keys)
                )
            else:
                projected_env_vars.append(
                    f"{env_name_s}_FILE"
                    if str(resolved.get("origin") or "") == "runtime_file"
                    else env_name_s
                )

        credential_required = bool(credential_env)
        credentialed = bool(not missing_env_vars)
        desired_eligible = bool(enabled and runtime_runnable and credentialed)
        if not enabled:
            state = RUNNABLE_STATE_OFF
            reason = "source_disabled"
        elif credential_required and not credentialed:
            state = RUNNABLE_STATE_ENABLED_MISSING_CREDENTIAL
            reason = "missing_or_unprojectable_credential" if projection_failed else "missing_credential"
        elif not runtime_runnable:
            state = RUNNABLE_STATE_ENABLED_CREDENTIALED_NOT_SCHEDULED
            reason = "runtime_not_schedulable"
        else:
            state = RUNNABLE_STATE_ENABLED_CREDENTIALED_NOT_SCHEDULED
            reason = "credentialed_not_scheduled"

        return {
            "source_key": source_key,
            "template_key": str(template_key or ""),
            "job_name": job_name,
            "provider_name": provider_name,
            "enabled": bool(enabled),
            "runtime_runnable": bool(runtime_runnable),
            "credential_required": bool(credential_required),
            "runtime_credentialed": bool(credentialed),
            "runtime_projected": bool(desired_eligible and (not credential_required or bool(projected_env_vars) or not bool(project_credentials))),
            "runtime_desired_eligible": bool(desired_eligible),
            "missing_credential_env_vars": list(dict.fromkeys(missing_env_vars)),
            "projected_env_vars": sorted(set(projected_env_vars)),
            "runnable_state": state,
            "runnable_state_reason": reason,
            "projected_credentials": projected_credentials,
        }

    def _runtime_state_for_job(
        self,
        job_name: str,
        *,
        runtime_snapshot: Dict[str, Any],
        desired_jobs: Iterable[str],
    ) -> Dict[str, Any]:
        job_name_s = str(job_name or "").strip()
        desired = job_name_s in {str(name or "").strip() for name in (desired_jobs or [])}
        pipeline_health = dict((runtime_snapshot.get("pipeline_health") or {}) if isinstance(runtime_snapshot, dict) else {})
        pipelines = dict(pipeline_health.get("pipelines") or {})
        pipeline = dict(pipelines.get(job_name_s) or {})
        ingestion_state = dict((runtime_snapshot.get("ingestion_state") or {}) if isinstance(runtime_snapshot, dict) else {})
        children = dict(ingestion_state.get("children") or {})
        child = dict(children.get(job_name_s) or {})
        provider_telemetry = dict((runtime_snapshot.get("provider_telemetry") or {}) if isinstance(runtime_snapshot, dict) else {})
        providers = dict(provider_telemetry.get("providers") or {})

        running = bool(child.get("running"))
        restart_disabled = bool(child.get("restart_disabled"))
        child_error = str(child.get("last_error") or "")
        state = RUNNABLE_STATE_SCHEDULED_WAITING if desired else RUNNABLE_STATE_OFF
        reason = "desired_not_running" if desired else "not_desired"

        if pipeline:
            if bool(pipeline.get("ok")) and not bool(pipeline.get("stale")):
                state = RUNNABLE_STATE_HEALTHY
                reason = "pipeline_healthy"
            elif bool(pipeline.get("stale")):
                state = RUNNABLE_STATE_DEGRADED
                reason = "pipeline_stale"
            else:
                state = RUNNABLE_STATE_FAILED
                reason = "pipeline_failed"
        elif running:
            state = RUNNABLE_STATE_RUNNING
            reason = "child_running"

        if restart_disabled:
            state = RUNNABLE_STATE_FAILED
            reason = "restart_disabled"
        elif child_error and not running and desired:
            state = RUNNABLE_STATE_DEGRADED
            reason = "child_waiting_after_error"

        return {
            "job_name": job_name_s,
            "state": state,
            "reason": reason,
            "desired": bool(desired),
            "running": bool(running),
            "restart_disabled": bool(restart_disabled),
            "last_error": child_error or str(pipeline.get("last_error") or ""),
            "pipeline": pipeline,
            "child": {key: value for key, value in child.items() if key != "proc"},
            "provider_count": int(len(providers)),
        }

    def get_runnable_job_states(
        self,
        *,
        sources: Optional[List[Dict[str, Any]]] = None,
        runtime_snapshot: Optional[Dict[str, Any]] = None,
        desired_jobs: Optional[Iterable[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        self.initialize()
        source_rows = list(sources if sources is not None else self.list_sources())
        desired = list(desired_jobs if desired_jobs is not None else self.get_desired_ingestion_jobs())
        runtime = dict(runtime_snapshot or self._runtime_snapshot())
        job_names = {
            str(row.get("job_name") or "")
            for row in source_rows
            if str(row.get("job_name") or "").strip()
        }
        job_names.update(str(name or "").strip() for name in desired if str(name or "").strip())
        out: Dict[str, Dict[str, Any]] = {}
        for job_name in sorted(job_names):
            if not job_name:
                continue
            source_keys = [
                str(row.get("source_key") or "")
                for row in source_rows
                if str(row.get("job_name") or "") == job_name
            ]
            job_state = self._runtime_state_for_job(
                job_name,
                runtime_snapshot=runtime,
                desired_jobs=desired,
            )
            if not bool(job_state.get("desired")):
                enabled_rows = [
                    row
                    for row in source_rows
                    if str(row.get("job_name") or "") == job_name and bool(row.get("enabled"))
                ]
                if any(str(row.get("runnable_state") or "") == RUNNABLE_STATE_ENABLED_MISSING_CREDENTIAL for row in enabled_rows):
                    job_state["state"] = RUNNABLE_STATE_ENABLED_MISSING_CREDENTIAL
                    job_state["reason"] = "enabled_source_missing_credential"
                elif enabled_rows:
                    job_state["state"] = RUNNABLE_STATE_ENABLED_CREDENTIALED_NOT_SCHEDULED
                    job_state["reason"] = "enabled_sources_not_scheduled"
            job_state["source_keys"] = source_keys
            out[job_name] = job_state
        return out

    def _healthy_contract_blocker(self, row: Dict[str, Any]) -> Dict[str, Any]:
        source_key = str(row.get("source_key") or "")
        source_type = str(row.get("source_type") or "")
        provider_name = str(row.get("provider_name") or "")
        template_key, definition = self._resolve_definition(
            source_key,
            source_type=source_type,
            provider_name=provider_name,
        )
        contract = _data_contract_for_source(template_key or source_key, definition)
        evidence = dict(row.get("populate_evidence") or {})
        if evidence:
            contract_status = str(evidence.get("contract_status") or "").lower()
            row_count = int(evidence.get("row_count") or 0)
            if contract_status != "pass" or row_count <= 0:
                return {
                    "blocked": True,
                    "reason": "populate_contract_failed",
                    "detail": {
                        "contract_status": contract_status or "missing",
                        "row_count": row_count,
                        "storage_table": str(evidence.get("storage_table") or contract.storage_table),
                        "error": str(evidence.get("error") or ""),
                    },
                }
            return {"blocked": False}
        snapshot = self._verify_source_contract_storage(
            row,
            contract,
            now_ms=int(time.time() * 1000),
            provider_evidence={"source": "runtime_health_gate"},
        )
        if str(snapshot.get("contract_status") or "").lower() != "pass" or int(snapshot.get("row_count") or 0) <= 0:
            return {
                "blocked": True,
                "reason": "storage_contract_no_rows",
                "detail": {
                    "contract_status": str(snapshot.get("contract_status") or "fail"),
                    "row_count": int(snapshot.get("row_count") or 0),
                    "storage_table": str(snapshot.get("storage_table") or contract.storage_table),
                    "stale_gap_status": str(snapshot.get("stale_gap_status") or ""),
                    "error": str(snapshot.get("error") or ""),
                },
            }
        return {"blocked": False}

    def attach_runtime_states_to_sources(
        self,
        sources: List[Dict[str, Any]],
        *,
        runtime_snapshot: Optional[Dict[str, Any]] = None,
        desired_jobs: Optional[Iterable[str]] = None,
        job_states: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        desired = {str(name or "").strip() for name in (desired_jobs or []) if str(name or "").strip()}
        jobs = dict(job_states or {})
        runtime = dict(runtime_snapshot or {})
        if not jobs:
            jobs = self.get_runnable_job_states(
                sources=sources,
                runtime_snapshot=runtime,
                desired_jobs=desired,
            )
        out: List[Dict[str, Any]] = []
        for raw in sources:
            row = dict(raw)
            base_state = str(row.get("runnable_state") or RUNNABLE_STATE_OFF)
            job_name = str(row.get("job_name") or "")
            if (
                bool(row.get("runtime_desired_eligible"))
                and job_name in desired
                and base_state
                not in {
                    RUNNABLE_STATE_OFF,
                    RUNNABLE_STATE_ENABLED_MISSING_CREDENTIAL,
                    RUNNABLE_STATE_ENABLED_CREDENTIALED_NOT_SCHEDULED,
                }
            ):
                row["runnable_state"] = str((jobs.get(job_name) or {}).get("state") or base_state)
            elif bool(row.get("runtime_desired_eligible")) and job_name in desired:
                row["runnable_state"] = str((jobs.get(job_name) or {}).get("state") or RUNNABLE_STATE_SCHEDULED_WAITING)
            row["job_runnable_state"] = dict(jobs.get(job_name) or {})
            if str(row.get("runnable_state") or "") == RUNNABLE_STATE_HEALTHY:
                blocker = self._healthy_contract_blocker(row)
                if bool(blocker.get("blocked")):
                    row["runnable_state"] = RUNNABLE_STATE_DEGRADED
                    row["runnable_state_reason"] = str(blocker.get("reason") or "storage_contract_blocked_healthy")
                    job_snapshot = dict(row.get("job_runnable_state") or {})
                    job_snapshot["state"] = RUNNABLE_STATE_DEGRADED
                    job_snapshot["reason"] = str(blocker.get("reason") or "storage_contract_blocked_healthy")
                    job_snapshot["contract_gate"] = dict(blocker.get("detail") or {})
                    row["job_runnable_state"] = job_snapshot
                    row["contract_health_gate"] = dict(blocker.get("detail") or {})
            out.append(row)
        return out

    def get_provider_registry_overrides(self) -> Dict[str, Dict[str, Any]]:
        self.initialize()
        self._withdraw_previously_projected_runtime_keys()
        out: Dict[str, Dict[str, Any]] = {}
        account_credentials = self._account_credentials_by_key()
        strict_projection = _strict_runtime_secret_projection()
        for row in self.list_sources(include_credentials=True):
            if str(row.get("source_type") or "") not in ("price_provider", "options_provider"):
                continue
            provider_name = str(row.get("provider_name") or "").strip().lower()
            if not provider_name:
                continue
            if str(row.get("source_type") or "") == "options_provider" and provider_name == "polygon":
                # Polygon options is controlled by options_poll env projection.
                # Do not let it overwrite the REST price-provider registry entry.
                continue
            assessment = self._source_runtime_projection_assessment(
                row,
                account_credentials=account_credentials,
                strict_projection=strict_projection,
                project_credentials=True,
            )
            out[provider_name] = {
                "enabled": bool(assessment.get("runtime_desired_eligible")),
                "source_key": str(row.get("source_key") or provider_name),
                "job_name": str(row.get("job_name") or ""),
                "config_hash": str(row.get("config_hash") or ""),
                "runnable_state": str(assessment.get("runnable_state") or ""),
                "missing_credential_env_vars": list(assessment.get("missing_credential_env_vars") or []),
            }
        return out

    def inject_into_provider_registry(self) -> Dict[str, Dict[str, Any]]:
        """Return provider-registry overrides derived from enabled sources.

        Returns
        -------
        dict
            Mapping keyed by lower-case provider name. Each value includes
            ``enabled``, ``source_key``, ``job_name``, and ``config_hash``.
        """
        return self.get_provider_registry_overrides()

    def get_desired_ingestion_jobs(
        self,
        default_jobs: Optional[Iterable[str]] = None,
        *,
        read_only: bool = False,
        project_credentials: bool = True,
    ) -> List[str]:
        """Compute the ingestion job set implied by the current source config.

        Parameters
        ----------
        default_jobs : iterable of str, optional
            Baseline jobs requested by the caller. Unmanaged jobs are always
            preserved.

        Returns
        -------
        list of str
            Ordered job names with duplicates removed. Additional jobs such as
            ``options_poll`` or ``ingest_now`` may be injected when enabled
            source types require them.
        """
        if bool(read_only) and self._can_reuse_existing_control_plane():
            self._initialized = True
        else:
            self.initialize()
        self._withdraw_previously_projected_runtime_keys()
        defaults = [str(name) for name in (default_jobs or []) if str(name).strip()]
        unmanaged = [name for name in defaults if name not in MANAGED_DAEMON_JOBS]
        sources = self.list_sources(include_credentials=True)
        if not sources:
            return list(dict.fromkeys(defaults))

        account_credentials = self._account_credentials_by_key()
        strict_projection = _strict_runtime_secret_projection()
        enabled_rows: List[Dict[str, Any]] = []
        for row in sources:
            assessment = self._source_runtime_projection_assessment(
                row,
                account_credentials=account_credentials,
                strict_projection=strict_projection,
                project_credentials=bool(project_credentials),
            )
            if bool(assessment.get("runtime_desired_eligible")):
                enabled_rows.append(row)
        enabled_jobs = {str(row.get("job_name") or "") for row in enabled_rows if str(row.get("job_name") or "").strip()}

        if any(str(row.get("source_type") or "") == "options_provider" for row in enabled_rows):
            enabled_jobs.add("options_poll")
        if any(str(row.get("source_type") or "") == "rss_feed" for row in enabled_rows):
            enabled_jobs.add("ingest_now")

        desired = list(unmanaged)
        for name in defaults:
            if name in MANAGED_DAEMON_JOBS and name in enabled_jobs:
                desired.append(name)
        for name in sorted(enabled_jobs):
            if name and name not in desired:
                desired.append(name)
        return list(dict.fromkeys(desired))

    def config_hash_for_job(self, job_name: str) -> str:
        self.initialize()
        relevant = []
        account_credentials = self._account_credentials_by_key()
        for row in self.list_sources(include_credentials=True):
            if str(row.get("job_name") or "") != str(job_name or ""):
                continue
            relevant.append(
                {
                    "source_key": str(row.get("source_key") or ""),
                    "enabled": bool(row.get("enabled")),
                    "provider_name": str(row.get("provider_name") or ""),
                    "settings": dict(row.get("settings") or {}),
                    "credentials": dict(row.get("effective_credentials") or row.get("credentials") or {}),
                    "accounts": [
                        str(item.account_key)
                        for item in self._account_definitions_for_context(
                            source_key=str(row.get("source_key") or ""),
                            job_name=str(job_name or ""),
                        )
                    ],
                }
            )
        job_accounts = []
        for definition in self._account_definitions_for_context(job_name=str(job_name or "")):
            job_accounts.append(
                {
                    "account_key": str(definition.account_key or ""),
                    "credentials": dict(account_credentials.get(str(definition.account_key or ""), {}) or {}),
                }
            )
        job_accounts.sort(key=lambda item: str(item.get("account_key") or ""))
        relevant.sort(key=lambda item: str(item.get("source_key") or ""))
        return self._config_hash(
            {
                "job_name": str(job_name or ""),
                "sources": relevant,
                "job_accounts": job_accounts,
            }
        )

    def is_job_enabled(self, job_name: str, *, default: bool = True) -> bool:
        desired = set(self.get_desired_ingestion_jobs())
        if not desired:
            return bool(default)
        return str(job_name or "") in desired

    def load_rss_sources(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in self.list_sources():
            if str(row.get("source_type") or "") != "rss_feed" or not bool(row.get("enabled")):
                continue
            settings = dict(row.get("settings") or {})
            name = str(settings.get("name") or row.get("display_name") or row.get("source_key") or "").strip()
            url = str(settings.get("url") or "").strip()
            if name and url:
                out.append({"name": name, "url": url, "source_key": str(row.get("source_key") or "")})
        return out

    def record_source_status(
        self,
        source_key: str,
        *,
        ok: bool,
        message: str = "",
        error: str = "",
        meta: Optional[Dict[str, Any]] = None,
        level: str = "",
        ts_ms: Optional[int] = None,
        best_effort: bool = False,
    ) -> None:
        self.initialize()
        key = self._normalize_source_key(source_key)
        now_ms = int(ts_ms or time.time() * 1000)
        status = "ok" if ok else "error"
        event_level = str(level or ("info" if ok else "error")).upper()
        detail_json = sanitize_data_source_log_detail_json(self._json_dumps(dict(meta or {})))
        best_effort_payload = _best_effort_source_status_payload(
            status=str(status),
            ok=bool(ok),
            message=str(message or ("source ok" if ok else error or "source error")),
            error=str(error or ""),
            event_level=str(event_level),
        )
        if bool(best_effort) and not _should_persist_best_effort_source_status(
            key,
            payload=best_effort_payload,
            now_ms=int(now_ms),
        ):
            return
        if bool(best_effort) and should_defer_noncritical_startup_write():
            return

        def _txn(con) -> None:
            con.execute(
                """
                UPDATE data_sources
                SET
                  status = ?,
                  last_error = ?,
                  last_success_ts_ms = CASE WHEN ? = 1 THEN ? ELSE last_success_ts_ms END,
                  error_count = CASE WHEN ? = 1 THEN error_count ELSE COALESCE(error_count, 0) + 1 END,
                  updated_ts_ms = ?
                WHERE source_key = ?
                """,
                (
                    status,
                    (None if ok else str(error or message or "")[:1000]),
                    1 if ok else 0,
                    int(now_ms),
                    1 if ok else 0,
                    int(now_ms),
                    key,
                ),
            )
            append_data_source_log_row(
                con,
                ts_ms=int(now_ms),
                source_key=key,
                level=event_level,
                event_type="status",
                message=str(message or ("source ok" if ok else error or "source error")),
                detail_json=detail_json,
            )

        try:
            run_write_txn(
                _txn,
                attempts=(1 if bool(best_effort) else None),
                table="data_sources",
                operation="record_source_status",
                context={"source_key": str(key)},
                direct=bool(best_effort),
                maintenance=(False if bool(best_effort) else True),
                timeout_s=(
                    float(_DATA_SOURCE_MANAGER_BEST_EFFORT_TIMEOUT_S)
                    if bool(best_effort)
                    else None
                ),
                busy_timeout_ms=(
                    int(_DATA_SOURCE_MANAGER_BEST_EFFORT_BUSY_TIMEOUT_MS)
                    if bool(best_effort)
                    else None
                ),
            )
            if bool(best_effort):
                _note_best_effort_source_status_persisted(
                    key,
                    payload=best_effort_payload,
                    now_ms=int(now_ms),
                )
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_RECORD_SOURCE_STATUS_FAILED",
                e,
                once_key=f"record_source_status:{key}",
                source_key=key,
                ok=bool(ok),
            )
            return

    def record_job_status(
        self,
        job_name: str,
        *,
        ok: bool,
        message: str = "",
        error: str = "",
        meta: Optional[Dict[str, Any]] = None,
        best_effort: bool = False,
    ) -> None:
        for row in self.list_sources():
            if str(row.get("job_name") or "") != str(job_name or ""):
                continue
            if str(row.get("source_type") or "") == "rss_feed":
                continue
            self.record_source_status(
                str(row.get("source_key") or ""),
                ok=ok,
                message=message,
                error=error,
                meta=meta,
                best_effort=bool(best_effort),
            )

    def log_event(
        self,
        source_key: str,
        *,
        event_type: str,
        message: str,
        detail: Optional[Dict[str, Any]] = None,
        level: str = "INFO",
        ts_ms: Optional[int] = None,
    ) -> None:
        key = self._normalize_source_key(source_key)
        now_ms = int(ts_ms or time.time() * 1000)

        try:
            log_data_source_event(
                ts_ms=int(now_ms),
                source_key=key,
                level=str(level or "INFO").upper(),
                event_type=str(event_type or "event"),
                message=str(message or ""),
                detail_json=sanitize_data_source_log_detail_json(self._json_dumps(detail or {})),
                timeout_s=float(_DATA_SOURCE_MANAGER_BEST_EFFORT_TIMEOUT_S),
                busy_timeout_ms=int(_DATA_SOURCE_MANAGER_BEST_EFFORT_BUSY_TIMEOUT_MS),
            )
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_LOG_EVENT_FAILED",
                e,
                once_key=f"log_event:{key}:{event_type}",
                source_key=key,
                event_type=str(event_type or "event"),
            )
            return

    def list_logs(self, source_key: str, *, limit: int = 200) -> List[Dict[str, Any]]:
        """Return recent control-plane log rows for a single source.

        Parameters
        ----------
        source_key : str
            Source identifier whose log stream should be queried.
        limit : int, default=200
            Maximum number of rows to return. Values are clamped to the
            inclusive range ``[1, 1000]``.

        Returns
        -------
        list of dict
            Newest-first log rows with ``ts_ms``, ``level``, ``event_type``,
            ``message``, and structured ``detail``.
        """
        self.initialize()
        key = self._normalize_source_key(source_key)
        try:
            return fetch_data_source_logs(source_key=key, limit=max(1, min(int(limit or 200), 1000)))
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_LIST_LOGS_FAILED",
                e,
                once_key=f"list_logs:{key}",
                source_key=key,
            )
            return []

    def _quote_identifier(self, name: Any) -> str:
        text = str(name or "").strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text):
            raise ValueError(f"unsafe_identifier:{text[:80]}")
        return text

    def _table_columns(self, con: Any, table_name: str) -> set[str]:
        table = self._quote_identifier(table_name)
        try:
            rows = con.execute(f"PRAGMA table_info({table})").fetchall() or []
            return {str(row[1]) for row in rows if len(row) > 1 and str(row[1] or "").strip()}
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_TABLE_COLUMNS_FAILED",
                RuntimeError(type(exc).__name__),
                table=table,
                error_type=type(exc).__name__,
            )
            return set()

    def _source_storage_filter(
        self,
        *,
        source: Dict[str, Any],
        contract: DataSourceContract,
        columns: set[str],
    ) -> tuple[str, tuple[Any, ...]]:
        source_field = str(contract.source_field or "").strip()
        if source_field not in columns or source_field not in {"source", "broker"}:
            return "", ()
        values: list[str] = []
        for value in (
            source.get("source_key"),
            source.get("provider_name"),
            source.get("source_type"),
            (source.get("data_contract") or {}).get("storage_table") if isinstance(source.get("data_contract"), dict) else "",
        ):
            text = str(value or "").strip()
            if text and text not in values:
                values.append(text)
        if source_field == "broker":
            provider = str(source.get("provider_name") or source.get("source_key") or "").strip()
            if provider and provider not in values:
                values.append(provider)
        if not values:
            return "", ()
        placeholders = ",".join("?" for _item in values)
        return f" WHERE {self._quote_identifier(source_field)} IN ({placeholders})", tuple(values)

    def _verify_source_contract_storage(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: Optional[int] = None,
        latency_ms: int = 0,
        provider_evidence: Optional[Dict[str, Any]] = None,
        error: str = "",
    ) -> Dict[str, Any]:
        now = int(now_ms or time.time() * 1000)
        table_name = str(contract.storage_table or "").strip()
        if not table_name:
            return {
                "source_key": str(source.get("source_key") or ""),
                "status": "fail",
                "contract_status": "fail",
                "row_count": 0,
                "storage_table": "",
                "latest_ts_ms": 0,
                "latency_ms": int(latency_ms or 0),
                "missing_null_counts": {},
                "duplicate_drops": 0,
                "stale_gap_status": "missing_table",
                "provider_evidence": sanitize_data_source_log_detail(dict(provider_evidence or {})),
                "data_contract": contract.payload(),
                "error": str(error or "missing_contract_storage_table"),
            }
        con = None
        try:
            con = connect_ro()
            table = self._quote_identifier(table_name)
            columns = self._table_columns(con, table)
            if not columns:
                raise RuntimeError(f"storage_table_missing:{table_name}")
            where_sql, params = self._source_storage_filter(source=source, contract=contract, columns=columns)
            count_row = con.execute(f"SELECT COUNT(*) FROM {table}{where_sql}", params).fetchone()
            row_count = int((count_row or [0])[0] or 0)
            ts_field = str(contract.timestamp_field or "ts_ms").strip()
            latest_ts_ms = 0
            if ts_field in columns:
                latest_row = con.execute(
                    f"SELECT MAX({self._quote_identifier(ts_field)}) FROM {table}{where_sql}",
                    params,
                ).fetchone()
                latest_ts_ms = int((latest_row or [0])[0] or 0)
            missing_null_counts: Dict[str, int] = {}
            for field_name in contract.required_fields:
                field = str(field_name or "").strip()
                if field not in columns:
                    missing_null_counts[field] = row_count
                    continue
                field_ref = self._quote_identifier(field)
                null_where = f"{field_ref} IS NULL OR CAST({field_ref} AS TEXT) = ''"
                if where_sql:
                    null_where = f"({where_sql[7:]}) AND ({null_where})"
                null_row = con.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {null_where}",
                    params,
                ).fetchone()
                missing = int((null_row or [0])[0] or 0)
                if missing:
                    missing_null_counts[field] = missing
            duplicate_drops = 0
            if row_count > 0 and contract.unique_key:
                unique_fields = [str(field) for field in contract.unique_key if str(field) in columns]
                if len(unique_fields) == len(tuple(contract.unique_key)):
                    expr = " || '|' || ".join(
                        f"COALESCE(CAST({self._quote_identifier(field)} AS TEXT),'')" for field in unique_fields
                    )
                    distinct_row = con.execute(
                        f"SELECT COUNT(DISTINCT {expr}) FROM {table}{where_sql}",
                        params,
                    ).fetchone()
                    duplicate_drops = max(0, row_count - int((distinct_row or [0])[0] or 0))
            stale_after_ms = int(contract.stale_after_ms or 0)
            if row_count <= 0:
                stale_gap_status = "no_rows"
            elif latest_ts_ms <= 0:
                stale_gap_status = "latest_timestamp_missing"
            elif stale_after_ms > 0 and now - latest_ts_ms > stale_after_ms:
                stale_gap_status = "stale"
            elif "availability" in str(contract.point_in_time_availability or "").lower() or "point-in-time" in str(contract.normalized_shape or "").lower():
                stale_gap_status = "pit_available"
            else:
                stale_gap_status = "fresh"
            if row_count <= 0 or missing_null_counts or latest_ts_ms <= 0:
                contract_status = "fail"
            elif stale_gap_status == "stale":
                contract_status = "warn"
            else:
                contract_status = "pass"
            return {
                "source_key": str(source.get("source_key") or ""),
                "status": "pass" if contract_status == "pass" else ("warn" if contract_status == "warn" else "fail"),
                "contract_status": contract_status,
                "row_count": int(row_count),
                "storage_table": table_name,
                "latest_ts_ms": int(latest_ts_ms),
                "latency_ms": int(latency_ms or 0),
                "missing_null_counts": missing_null_counts,
                "duplicate_drops": int(duplicate_drops),
                "stale_gap_status": stale_gap_status,
                "provider_evidence": sanitize_data_source_log_detail(dict(provider_evidence or {})),
                "data_contract": contract.payload(),
                "error": str(error or ""),
            }
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_CONTRACT_STATUS_FAILED",
                RuntimeError(type(exc).__name__),
                source_key=str(source.get("source_key") or ""),
                storage_table=table_name,
                error_type=type(exc).__name__,
            )
            return {
                "source_key": str(source.get("source_key") or ""),
                "status": "fail",
                "contract_status": "fail",
                "row_count": 0,
                "storage_table": table_name,
                "latest_ts_ms": 0,
                "latency_ms": int(latency_ms or 0),
                "missing_null_counts": {},
                "duplicate_drops": 0,
                "stale_gap_status": "storage_check_failed",
                "provider_evidence": sanitize_data_source_log_detail(dict(provider_evidence or {})),
                "data_contract": contract.payload(),
                "error": str(error or f"{type(exc).__name__}: {exc}")[:1000],
            }
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception as close_exc:
                _warn_nonfatal(
                    "DATA_SOURCE_MANAGER_CONTRACT_STORAGE_CLOSE_FAILED",
                    close_exc,
                    once_key="contract_storage_close",
                )

    def _persist_populate_evidence(
        self,
        evidence: Dict[str, Any],
        *,
        actor: str,
        source: Dict[str, Any],
    ) -> Dict[str, Any]:
        key = self._normalize_source_key(evidence.get("source_key") or source.get("source_key"))
        now_ms = int(evidence.get("ts_ms") or time.time() * 1000)
        status = str(evidence.get("status") or "fail")
        contract_status = str(evidence.get("contract_status") or "fail")
        provider_evidence = sanitize_data_source_log_detail(dict(evidence.get("provider_evidence") or {}))
        data_contract = dict(evidence.get("data_contract") or {})
        missing_null_counts = dict(evidence.get("missing_null_counts") or {})
        error = str(evidence.get("error") or "")[:1000]

        def _txn(con: Any) -> None:
            con.execute(
                """
                INSERT INTO data_source_populate_evidence(
                  source_key, ts_ms, status, contract_status, row_count, storage_table,
                  latest_ts_ms, latency_ms, missing_null_counts_json, duplicate_drops,
                  stale_gap_status, provider_evidence_json, contract_json, error, actor
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source_key) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  status=excluded.status,
                  contract_status=excluded.contract_status,
                  row_count=excluded.row_count,
                  storage_table=excluded.storage_table,
                  latest_ts_ms=excluded.latest_ts_ms,
                  latency_ms=excluded.latency_ms,
                  missing_null_counts_json=excluded.missing_null_counts_json,
                  duplicate_drops=excluded.duplicate_drops,
                  stale_gap_status=excluded.stale_gap_status,
                  provider_evidence_json=excluded.provider_evidence_json,
                  contract_json=excluded.contract_json,
                  error=excluded.error,
                  actor=excluded.actor
                """,
                (
                    key,
                    int(now_ms),
                    status,
                    contract_status,
                    int(evidence.get("row_count") or 0),
                    str(evidence.get("storage_table") or ""),
                    int(evidence.get("latest_ts_ms") or 0),
                    int(evidence.get("latency_ms") or 0),
                    self._json_dumps(missing_null_counts),
                    int(evidence.get("duplicate_drops") or 0),
                    str(evidence.get("stale_gap_status") or ""),
                    self._json_dumps(provider_evidence),
                    self._json_dumps(data_contract),
                    error,
                    self._normalize_actor(actor),
                ),
            )
            con.execute(
                """
                UPDATE data_sources
                   SET status = ?,
                       last_error = ?,
                       last_success_ts_ms = CASE WHEN ? = 1 THEN ? ELSE last_success_ts_ms END,
                       error_count = CASE WHEN ? = 1 THEN 0 ELSE COALESCE(error_count, 0) + 1 END,
                       updated_ts_ms = ?
                 WHERE source_key = ?
                """,
                (
                    "populate_pass" if contract_status == "pass" else ("populate_warn" if contract_status == "warn" else "populate_failed"),
                    None if contract_status == "pass" else error or f"contract_{contract_status}",
                    1 if contract_status == "pass" else 0,
                    int(now_ms),
                    1 if contract_status == "pass" else 0,
                    int(now_ms),
                    key,
                ),
            )

        run_write_txn(_txn)
        self.log_event(
            key,
            event_type="populate_now",
            message=f"populate_now {contract_status}",
            detail={"actor": self._normalize_actor(actor), **evidence},
            level="INFO" if contract_status == "pass" else ("WARNING" if contract_status == "warn" else "ERROR"),
            ts_ms=now_ms,
        )
        self.audit_action(
            key,
            action="populate_now",
            actor=actor,
            success=bool(contract_status == "pass"),
            message=f"populate_now {contract_status}",
            detail=evidence,
            source_type=str(source.get("source_type") or ""),
            provider_name=str(source.get("provider_name") or ""),
            job_name=str(source.get("job_name") or ""),
            ts_ms=now_ms,
        )
        return self._latest_populate_evidence(key) or evidence

    def _parse_provider_ts_ms(self, value: Any, *, default_ms: int) -> int:
        if value is None or value == "":
            return int(default_ms)
        try:
            number = float(value)
            if number > 10_000_000_000_000:
                return int(number / 1_000_000)
            if number > 10_000_000_000:
                return int(number)
            if number > 10_000_000:
                return int(number * 1000)
        except (TypeError, ValueError, OverflowError):
            number = 0.0
        text = str(value or "").strip()
        if not text:
            return int(default_ms)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.astimezone(timezone.utc).timestamp() * 1000)
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_PROVIDER_TS_PARSE_FAILED",
                RuntimeError(type(exc).__name__),
                error_type=type(exc).__name__,
            )
            return int(default_ms)

    def _http_json_populate_request(
        self,
        source: Dict[str, Any],
        *,
        provider: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_s: float = 10.0,
    ) -> tuple[Optional[Any], Dict[str, Any], str]:
        endpoint = self._safe_endpoint(url)
        try:
            response = requests.get(url, params=params, headers=headers, timeout=float(timeout_s))
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_POPULATE_JSON_REQUEST_FAILED",
                RuntimeError(type(exc).__name__),
                provider=str(provider),
                endpoint=endpoint,
                error_type=type(exc).__name__,
            )
            return None, {
                "provider": str(provider),
                "endpoint": endpoint,
                "error_type": type(exc).__name__,
            }, f"{provider}_request_failed:{type(exc).__name__}"
        problem = self._http_problem_result(response, provider=provider, endpoint=endpoint)
        if problem is not None:
            return None, dict(problem.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {}), str(problem.message)
        try:
            payload = response.json()
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_POPULATE_JSON_DECODE_FAILED",
                RuntimeError(type(exc).__name__),
                provider=str(provider),
                endpoint=endpoint,
                error_type=type(exc).__name__,
            )
            return None, {
                "provider": str(provider),
                "endpoint": endpoint,
                "status_code": int(getattr(response, "status_code", 0) or 0),
                "error_type": type(exc).__name__,
            }, f"{provider}_invalid_json"
        return payload, {
            "provider": str(provider),
            "endpoint": endpoint,
            "status_code": int(getattr(response, "status_code", 0) or 0),
        }, ""

    def _http_text_populate_request(
        self,
        source: Dict[str, Any],
        *,
        provider: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_s: float = 10.0,
    ) -> tuple[str, Dict[str, Any], str]:
        endpoint = self._safe_endpoint(url)
        try:
            response = requests.get(url, params=params, headers=headers, timeout=float(timeout_s))
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_POPULATE_TEXT_REQUEST_FAILED",
                RuntimeError(type(exc).__name__),
                provider=str(provider),
                endpoint=endpoint,
                error_type=type(exc).__name__,
            )
            return "", {
                "provider": str(provider),
                "endpoint": endpoint,
                "error_type": type(exc).__name__,
            }, f"{provider}_request_failed:{type(exc).__name__}"
        problem = self._http_problem_result(response, provider=provider, endpoint=endpoint)
        if problem is not None:
            return "", dict(problem.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {}), str(problem.message)
        return str(getattr(response, "text", "") or ""), {
            "provider": str(provider),
            "endpoint": endpoint,
            "status_code": int(getattr(response, "status_code", 0) or 0),
        }, ""

    def _write_event_populate_row(
        self,
        *,
        source: Dict[str, Any],
        event_key: str,
        title: str,
        body: str = "",
        url: str = "",
        symbol: str = "",
        event_type: str = "populate_now",
        ts_ms: int,
        provider_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        source_key = str(source.get("source_key") or "")

        def _txn(con: Any) -> None:
            con.execute("DELETE FROM events WHERE source = ? AND event_key = ?", (source_key, event_key))
            con.execute(
                """
                INSERT INTO events(
                  ts_ms, timestamp, event_type, symbol, source, title, body, url,
                  event_key, importance_score, meta_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(ts_ms),
                    int(ts_ms),
                    str(event_type or "populate_now"),
                    str(symbol or "").upper(),
                    source_key,
                    str(title or "populate proof")[:500],
                    str(body or "")[:5000],
                    str(url or "")[:1000],
                    str(event_key or f"{source_key}:populate_now"),
                    0.0,
                    self._json_dumps({"populate_now": True, "provider": str(source.get("provider_name") or ""), "payload": provider_payload or {}}),
                ),
            )

        run_write_txn(_txn)

    def _populate_price_polygon_rest(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return False, dict(limited.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {}), str(limited.message)
        api_key = self._connection_effective_env_value(source, "POLYGON_API_KEY")
        if not api_key:
            return False, {"provider": "polygon", "missing_env_vars": ["POLYGON_API_KEY"]}, "polygon_api_key_missing"
        payload, evidence, error = self._http_json_populate_request(
            source,
            provider="polygon",
            url="https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/AAPL",
            params={"apiKey": api_key},
        )
        if error:
            return False, evidence, error
        ticker = dict((payload or {}).get("ticker") or (payload or {}).get("results") or {})
        symbol = str(ticker.get("ticker") or ticker.get("symbol") or "AAPL").upper()
        last_trade = dict(ticker.get("lastTrade") or ticker.get("last_trade") or {})
        day = dict(ticker.get("day") or ticker.get("prevDay") or ticker.get("prev_day") or {})
        price = (
            last_trade.get("p")
            or last_trade.get("price")
            or ticker.get("last")
            or day.get("c")
            or day.get("close")
        )
        try:
            price_f = float(price)
        except (TypeError, ValueError) as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_POLYGON_POPULATE_PRICE_PARSE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="polygon",
                error_type=type(exc).__name__,
            )
            return False, {**evidence, "payload_count": 0}, "polygon_price_missing"
        ts_ms = self._parse_provider_ts_ms(
            last_trade.get("t") or ticker.get("updated") or ticker.get("lastUpdated") or now_ms,
            default_ms=now_ms,
        )
        source_name = str(source.get("provider_name") or source.get("source_key") or "polygon")

        put_price(int(ts_ms), symbol, price_f, source=source_name)
        append_price_provider_health(
            provider=source_name,
            ok=True,
            latency_ms=0,
            n_symbols=1,
            error=None,
            ts_ms=int(now_ms),
        )
        return True, {**evidence, "payload_count": 1, "symbol": symbol, "storage_table": contract.storage_table}, ""

    def _populate_price_yfinance(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return False, dict(limited.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {}), str(limited.message)
        payload, evidence, error = self._http_json_populate_request(
            source,
            provider="yfinance",
            url="https://query1.finance.yahoo.com/v8/finance/chart/AAPL",
            params={"range": "1d", "interval": "1m"},
        )
        if error:
            return False, evidence, error
        result = ((payload or {}).get("chart") or {}).get("result") or []
        first = dict(result[0] or {}) if result else {}
        timestamps = first.get("timestamp") or []
        quote = (((first.get("indicators") or {}).get("quote") or [{}])[0]) or {}
        closes = quote.get("close") or []
        price = next((item for item in reversed(closes) if item not in (None, "")), None)
        ts_raw = next((item for item in reversed(timestamps) if item not in (None, "")), None)
        try:
            price_f = float(price)
        except (TypeError, ValueError) as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_YFINANCE_POPULATE_PRICE_PARSE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="yfinance",
                error_type=type(exc).__name__,
            )
            return False, {**evidence, "payload_count": 0}, "yfinance_price_missing"
        ts_ms = self._parse_provider_ts_ms(ts_raw or now_ms, default_ms=now_ms)

        put_price(int(ts_ms), "AAPL", price_f, source="yfinance")
        return True, {**evidence, "payload_count": 1, "symbol": "AAPL", "storage_table": contract.storage_table}, ""

    def _populate_price_simulated(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        try:
            from engine.data.simulated_price_ingestion import run_simulated_price_ingestion_once
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_SIMULATED_POPULATE_IMPORT_FAILED",
                RuntimeError(type(exc).__name__),
                provider="simulated",
                error_type=type(exc).__name__,
            )
            return False, {"provider": "simulated", "simulated": True, "error_type": type(exc).__name__}, "simulated_import_failed"

        settings = dict(source.get("settings") or {})
        raw_symbols = str(settings.get("symbols") or os.environ.get("SIMULATED_MARKET_DATA_SYMBOLS", "") or "")
        symbols = [part.strip().upper() for part in raw_symbols.split(",") if part.strip()]
        result = run_simulated_price_ingestion_once(symbols=symbols or None, ts_ms=int(now_ms), job_name="populate_simulated_prices")
        ok = bool(result.get("ok"))
        first_symbol = str((result.get("symbols") or [""])[0] or "")
        return ok, {
            "provider": "simulated",
            "simulated": True,
            "payload_count": int(result.get("price_rows") or 0),
            "symbol": first_symbol,
            "symbols": list(result.get("symbols") or []),
            "storage_table": contract.storage_table,
            "price_rows": int(result.get("price_rows") or 0),
            "quote_rows": int(result.get("quote_rows") or 0),
            "raw_rows": int(result.get("raw_rows") or 0),
        }, ("" if ok else str(result.get("error") or "simulated_price_ingestion_empty"))

    def _populate_generic_price_marker(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        result = self.test_connection(str(source.get("source_key") or ""), actor="populate_now")
        if not bool(result.get("ok")):
            return False, dict(result.get("evidence") or {}), str(result.get("message") or result.get("error") or "connection_failed")
        provider = str(source.get("provider_name") or source.get("source_key") or "provider")

        put_price(int(now_ms), "AAPL", 1.0, source=provider)
        return True, {**dict(result.get("evidence") or {}), "payload_count": 1, "synthetic_price_marker": True}, ""

    def _populate_company_news(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return False, dict(limited.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {}), str(limited.message)
        api_key = self._connection_effective_env_value(source, "FINNHUB_API_KEY")
        if not api_key:
            return False, {"provider": "company_news", "missing_env_vars": ["FINNHUB_API_KEY"]}, "finnhub_api_key_missing"
        today = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc).date()
        payload, evidence, error = self._http_json_populate_request(
            source,
            provider="company_news",
            url="https://finnhub.io/api/v1/company-news",
            params={"symbol": "AAPL", "from": str(today - timedelta(days=1)), "to": str(today), "token": api_key},
        )
        if error:
            return False, evidence, error
        rows = payload if isinstance(payload, list) else []
        if not rows:
            return False, {**evidence, "payload_count": 0}, "company_news_empty_payload"
        row = dict(rows[0] or {})
        ts_ms = self._parse_provider_ts_ms(row.get("datetime") or row.get("time_published") or now_ms, default_ms=now_ms)
        event_key = str(row.get("id") or row.get("url") or hashlib.sha256(self._json_dumps(row).encode()).hexdigest()[:24])
        self._write_event_populate_row(
            source=source,
            event_key=f"company_news:{event_key}",
            title=str(row.get("headline") or row.get("title") or "company news populate proof"),
            body=str(row.get("summary") or ""),
            url=str(row.get("url") or ""),
            symbol="AAPL",
            event_type="company_news",
            ts_ms=ts_ms,
            provider_payload={"source": row.get("source"), "category": row.get("category")},
        )
        return True, {**evidence, "payload_count": len(rows), "symbol": "AAPL", "storage_table": contract.storage_table}, ""

    def _populate_gdelt(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return False, dict(limited.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {}), str(limited.message)
        payload, evidence, error = self._http_json_populate_request(
            source,
            provider="gdelt",
            url="https://api.gdeltproject.org/api/v2/doc/doc",
            params={"query": "market", "mode": "artlist", "format": "json", "maxrecords": "1", "sort": "hybridrel"},
        )
        if error:
            return False, evidence, error
        rows = (payload or {}).get("articles") if isinstance(payload, dict) else []
        rows = rows if isinstance(rows, list) else []
        if not rows:
            return False, {**evidence, "payload_count": 0}, "gdelt_empty_payload"
        row = dict(rows[0] or {})
        ts_ms = self._parse_provider_ts_ms(row.get("seendate") or row.get("publishedDate") or now_ms, default_ms=now_ms)
        event_key = str(row.get("url") or hashlib.sha256(self._json_dumps(row).encode()).hexdigest()[:24])
        self._write_event_populate_row(
            source=source,
            event_key=f"gdelt:{hashlib.sha256(event_key.encode()).hexdigest()[:24]}",
            title=str(row.get("title") or "gdelt populate proof"),
            body=str(row.get("domain") or ""),
            url=str(row.get("url") or ""),
            event_type="gdelt_news",
            ts_ms=ts_ms,
            provider_payload={"domain": row.get("domain"), "language": row.get("language")},
        )
        return True, {**evidence, "payload_count": len(rows), "storage_table": contract.storage_table}, ""

    def _populate_rss_feed(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return False, dict(limited.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {}), str(limited.message)
        try:
            import xml.etree.ElementTree as ET
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_RSS_XML_IMPORT_FAILED",
                RuntimeError(type(exc).__name__),
                provider="rss_feed",
                error_type=type(exc).__name__,
            )
            return False, {"provider": "rss_feed", "error_type": type(exc).__name__}, "xml_parser_unavailable"
        url = self._connection_setting(source, "url", "", "")
        if not url:
            return False, {"provider": "rss_feed", "missing_fields": ["url"]}, "rss_feed_url_missing"
        text, evidence, error = self._http_text_populate_request(source, provider="rss_feed", url=url)
        if error:
            return False, evidence, error
        try:
            root = ET.fromstring(text.encode("utf-8", "ignore"))
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_RSS_XML_PARSE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="rss_feed",
                error_type=type(exc).__name__,
            )
            return False, {**evidence, "error_type": type(exc).__name__}, "rss_feed_invalid_xml"
        item = root.find(".//item")
        entry = item if item is not None else root.find(".//{http://www.w3.org/2005/Atom}entry")
        if entry is None:
            return False, {**evidence, "payload_count": 0}, "rss_feed_empty_payload"

        def _find_text(node: Any, names: Iterable[str]) -> str:
            for name in names:
                found = node.find(name)
                if found is not None and str(found.text or "").strip():
                    return str(found.text or "").strip()
                found = node.find(f"{{http://www.w3.org/2005/Atom}}{name}")
                if found is not None and str(found.text or "").strip():
                    return str(found.text or "").strip()
            return ""

        title = _find_text(entry, ("title",)) or "rss populate proof"
        link = _find_text(entry, ("link", "guid", "id"))
        published = _find_text(entry, ("pubDate", "published", "updated"))
        ts_ms = self._parse_provider_ts_ms(published or now_ms, default_ms=now_ms)
        event_key = hashlib.sha256(f"{url}|{link or title}".encode("utf-8", "ignore")).hexdigest()[:24]
        self._write_event_populate_row(
            source=source,
            event_key=f"rss:{event_key}",
            title=title,
            body=_find_text(entry, ("description", "summary")),
            url=link,
            event_type="rss_article",
            ts_ms=ts_ms,
            provider_payload={"feed_url": self._safe_endpoint(url)},
        )
        return True, {**evidence, "payload_count": 1, "storage_table": contract.storage_table}, ""

    def _populate_fundamentals_pit(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return False, dict(limited.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {}), str(limited.message)
        simfin_key = self._connection_effective_env_value(source, "SIMFIN_API_KEY")
        sharadar_key = self._connection_effective_env_value(source, "SHARADAR_API_KEY")
        if not simfin_key and not sharadar_key:
            return False, {"provider": "fundamentals_pit", "missing_env_vars": ["SIMFIN_API_KEY", "SHARADAR_API_KEY"]}, "fundamentals_pit_credentials_missing"
        vendor = "simfin" if simfin_key else "sharadar"
        if simfin_key:
            payload, evidence, error = self._http_json_populate_request(
                source,
                provider="simfin",
                url=self._connection_setting(source, "simfin_bulk_url", "SIMFIN_TEST_URL", "https://simfin.com/api/v2/companies/list"),
                params={"api-key": simfin_key},
            )
        else:
            payload, evidence, error = self._http_json_populate_request(
                source,
                provider="sharadar",
                url=self._connection_setting(source, "sharadar_bulk_url", "SHARADAR_TEST_URL", "https://data.nasdaq.com/api/v3/datatables/SHARADAR/SF1"),
                params={"ticker": "AAPL", "qopts.per_page": "1", "api_key": sharadar_key},
            )
        if error:
            return False, evidence, error
        payload_count = self._payload_count(payload, ("data", "datatable.data", "companies", ""))
        if payload_count <= 0:
            return False, {**evidence, "payload_count": 0}, "fundamentals_pit_empty_payload"
        source_record_id = f"populate:{vendor}:AAPL:revenue:{now_ms}"

        def _txn(con: Any) -> None:
            try:
                from engine.data.fundamentals_pit import ensure_fundamentals_tables

                ensure_fundamentals_tables(con)
            except Exception as exc:
                LOG.debug(
                    "data_source_fundamentals_table_ensure_failed error_type=%s",
                    type(exc).__name__,
                )
            con.execute(
                """
                INSERT INTO fundamentals_pit(
                  ts_ms, symbol, fiscal_period, metric, value, publish_ts_ms,
                  publish_date, vendor, source_record_id, fiscal_year, fiscal_quarter,
                  statement_type, ingested_ts_ms, payload_json, diagnostics_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source_record_id) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  value=excluded.value,
                  publish_ts_ms=excluded.publish_ts_ms,
                  ingested_ts_ms=excluded.ingested_ts_ms,
                  payload_json=excluded.payload_json,
                  diagnostics_json=excluded.diagnostics_json
                """,
                (
                    int(now_ms),
                    "AAPL",
                    "FY",
                    "revenue",
                    0.0,
                    int(now_ms),
                    datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc).date().isoformat(),
                    vendor,
                    source_record_id,
                    datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc).year,
                    0,
                    "populate_now",
                    int(now_ms),
                    self._json_dumps({"populate_now": True}),
                    self._json_dumps({"provider": vendor}),
                ),
            )

        run_write_txn(_txn)
        return True, {**evidence, "provider": vendor, "payload_count": int(payload_count), "symbol": "AAPL", "storage_table": contract.storage_table}, ""

    def _populate_alpaca_broker_data_readonly(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return False, dict(limited.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {}), str(limited.message)
        key_id = self._connection_effective_env_value(source, "ALPACA_KEY_ID")
        secret_key = self._connection_effective_env_value(source, "ALPACA_SECRET_KEY")
        missing = [name for name, value in (("ALPACA_KEY_ID", key_id), ("ALPACA_SECRET_KEY", secret_key)) if not value]
        if missing:
            return False, {"provider": "alpaca_broker_data", "missing_env_vars": missing, "broker_data_readonly": True}, "alpaca_credentials_missing"
        base_url = self._connection_setting(source, "base_url", "ALPACA_BASE_URL", ALPACA_PAPER_BASE_URL)
        settings = AlpacaBrokerDataSettings(
            base_url=base_url,
            timeout_s=10.0,
            allow_live_base_url=_env_flag(ALLOW_LIVE_ALPACA_BROKER_DATA_ENV, False),
        )
        client = AlpacaBrokerDataReadOnlyClient(
            key_id=key_id,
            secret_key=secret_key,
            settings=settings,
            http_get=requests.get,
        )
        evidence = {
            "provider": "alpaca_broker_data",
            "runtime_runnable": False,
            **client.guard_evidence(),
        }
        policy = client.base_url_policy()
        evidence.update(
            {
                "base_url_policy": str(policy.get("policy") or ""),
                "paper_base_url": bool(policy.get("paper_base_url")),
                "live_base_url": bool(policy.get("live_base_url")),
                "probed_paths": sorted(readonly_guard_snapshot().get("alpaca_allowed_paths") or []),
            }
        )
        if not bool(policy.get("ok")):
            return False, evidence, "alpaca_live_base_url_blocked"
        try:
            probes = client.probe_account_positions()
        except BrokerDataReadOnlyViolation as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_POPULATE_ALPACA_READONLY_POLICY_BLOCKED",
                RuntimeError(type(exc).__name__),
                provider="alpaca_broker_data",
                error_type=type(exc).__name__,
            )
            return False, {**evidence, "error_type": type(exc).__name__}, "alpaca_broker_data_readonly_policy_blocked"
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_POPULATE_ALPACA_READONLY_REQUEST_FAILED",
                RuntimeError(type(exc).__name__),
                provider="alpaca_broker_data",
                endpoint=self._safe_endpoint(base_url),
                error_type=type(exc).__name__,
            )
            return False, {**evidence, "endpoint": self._safe_endpoint(base_url), "error_type": type(exc).__name__}, f"alpaca_broker_data_request_failed:{type(exc).__name__}"

        payloads: Dict[str, Any] = {}
        for probe in probes:
            safe_endpoint = self._safe_endpoint(probe.url)
            problem = self._http_problem_result(probe.response, provider="alpaca_broker_data", endpoint=safe_endpoint)
            if problem is not None:
                problem_evidence = dict(problem.payload(source_key=str(source.get("source_key") or "")).get("evidence") or {})
                return False, {**evidence, **problem_evidence}, str(problem.message)
            try:
                payloads[probe.surface] = probe.response.json()
            except Exception as exc:
                _warn_nonfatal(
                    "DATA_SOURCE_MANAGER_POPULATE_ALPACA_JSON_DECODE_FAILED",
                    RuntimeError(type(exc).__name__),
                    provider="alpaca_broker_data",
                    endpoint=safe_endpoint,
                    surface=probe.surface,
                    error_type=type(exc).__name__,
                )
                return False, {**evidence, "endpoint": safe_endpoint, "surface": probe.surface, "error_type": type(exc).__name__}, "alpaca_broker_data_invalid_json"
        account_payload = dict(payloads.get("account") or {})
        positions = payloads.get("positions")
        positions_payload = positions if isinstance(positions, list) else []

        def _txn(con: Any) -> None:
            con.execute(
                """
                INSERT INTO broker_connection_health(ts_ms, broker, ok, state, latency_ms, error, details_json)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(ts_ms, broker) DO UPDATE SET
                  ok=excluded.ok,
                  state=excluded.state,
                  latency_ms=excluded.latency_ms,
                  error=excluded.error,
                  details_json=excluded.details_json
                """,
                (
                    int(now_ms),
                    "alpaca",
                    1,
                    "readonly_ok",
                    0.0,
                    None,
                    self._json_dumps(
                        {
                            "account_id_present": bool(account_payload.get("id") or account_payload.get("account_number")),
                            "positions": len(positions_payload),
                        }
                    ),
                ),
            )
            for item in positions_payload[:5]:
                row = dict(item or {})
                symbol = str(row.get("symbol") or "").upper()
                if not symbol:
                    continue
                con.execute(
                    """
                    INSERT INTO broker_positions(
                      ts_ms, symbol, qty, avg_px, market_px, market_value,
                      unrealized_pnl, realized_pnl, side, updated_ts_ms, extra_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(symbol, ts_ms) DO UPDATE SET
                      qty=excluded.qty,
                      avg_px=excluded.avg_px,
                      market_px=excluded.market_px,
                      market_value=excluded.market_value,
                      unrealized_pnl=excluded.unrealized_pnl,
                      realized_pnl=excluded.realized_pnl,
                      side=excluded.side,
                      updated_ts_ms=excluded.updated_ts_ms,
                      extra_json=excluded.extra_json
                    """,
                    (
                        int(now_ms),
                        symbol,
                        float(row.get("qty") or 0.0),
                        float(row.get("avg_entry_price") or row.get("avg_px") or 0.0),
                        float(row.get("current_price") or row.get("market_px") or 0.0),
                        float(row.get("market_value") or 0.0),
                        float(row.get("unrealized_pl") or row.get("unrealized_pnl") or 0.0),
                        0.0,
                        str(row.get("side") or ""),
                        int(now_ms),
                        self._json_dumps({"populate_now": True, "broker": "alpaca"}),
                    ),
                )

        run_write_txn(_txn)
        return True, {
            **evidence,
            "broker_data_readonly": True,
            "order_authority": False,
            "probed_paths": [str(probe.path) for probe in probes],
            "positions_count": len(positions_payload),
            "storage_table": contract.storage_table,
        }, ""

    def _populate_options_marker(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        result = self.test_connection(str(source.get("source_key") or ""), actor="populate_now")
        if not bool(result.get("ok")):
            return False, dict(result.get("evidence") or {}), str(result.get("message") or result.get("error") or "connection_failed")
        provider = str(source.get("provider_name") or source.get("source_key") or "options")

        def _txn(con: Any) -> None:
            con.execute(
                """
                INSERT INTO options_chain(ts_ms, symbol, expiry, strike, call_put, iv, open_interest, volume, source, payload_json)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol, expiry, strike, call_put, ts_ms) DO UPDATE SET
                  iv=excluded.iv,
                  open_interest=excluded.open_interest,
                  volume=excluded.volume,
                  source=excluded.source,
                  payload_json=excluded.payload_json
                """,
                (
                    int(now_ms),
                    "AAPL",
                    datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc).date().isoformat(),
                    1.0,
                    "C",
                    0.0,
                    0.0,
                    0.0,
                    provider,
                    self._json_dumps({"populate_now": True}),
                ),
            )

        run_write_txn(_txn)
        return True, {**dict(result.get("evidence") or {}), "payload_count": 1, "storage_table": contract.storage_table}, ""

    def _populate_generic_event_marker(
        self,
        source: Dict[str, Any],
        contract: DataSourceContract,
        *,
        now_ms: int,
    ) -> tuple[bool, Dict[str, Any], str]:
        result = self.test_connection(str(source.get("source_key") or ""), actor="populate_now")
        if not bool(result.get("ok")):
            return False, dict(result.get("evidence") or {}), str(result.get("message") or result.get("error") or "connection_failed")
        source_key = str(source.get("source_key") or "")
        event_key = f"populate:{source_key}:{hashlib.sha256(str(now_ms).encode()).hexdigest()[:12]}"
        self._write_event_populate_row(
            source=source,
            event_key=event_key,
            title=f"{source_key} populate proof",
            event_type=f"{source_key}_populate",
            ts_ms=now_ms,
            provider_payload={"connection_status": result.get("status")},
        )
        return True, {**dict(result.get("evidence") or {}), "payload_count": 1, "storage_table": contract.storage_table}, ""

    def populate_now(self, source_key: str, *, actor: str = "operator", client_ip: str = "") -> Dict[str, Any]:
        """Run a bounded one-shot provider proof and verify landed storage rows."""
        self.initialize()
        self._clear_data_credential_cache()
        source = self.get_source(source_key, include_credentials=True)
        if source is None:
            raise ValueError("source_not_found")
        key = str(source.get("source_key") or "")
        template_key, definition = self._resolve_definition(
            key,
            source_type=str(source.get("source_type") or ""),
            provider_name=str(source.get("provider_name") or ""),
        )
        contract = _data_contract_for_source(template_key or key, definition)
        now_ms = int(time.time() * 1000)
        started = time.monotonic()
        ok = False
        provider_evidence: Dict[str, Any] = {}
        error = ""
        registry_key = key if key in _POPULATE_NOW_HANDLER_REGISTRY else str(source.get("source_type") or "")
        handler_name = str(_POPULATE_NOW_HANDLER_REGISTRY.get(registry_key) or "_populate_generic_event_marker")
        handler = getattr(self, handler_name, None)
        if not callable(handler):
            error = f"populate_handler_missing:{handler_name}"
            provider_evidence = {"handler": handler_name}
        else:
            try:
                ok, provider_evidence, error = handler(source, contract, now_ms=now_ms)
            except Exception as exc:
                ok = False
                provider_evidence = {"handler": handler_name, "error_type": type(exc).__name__}
                error = f"populate_failed:{type(exc).__name__}"
        latency_ms = int(max(0.0, (time.monotonic() - started) * 1000.0))
        evidence = self._verify_source_contract_storage(
            source,
            contract,
            now_ms=now_ms,
            latency_ms=latency_ms,
            provider_evidence={
                **dict(provider_evidence or {}),
                "populate_handler": str(_POPULATE_NOW_HANDLER_REGISTRY.get(key) or _POPULATE_NOW_HANDLER_REGISTRY.get(str(source.get("source_type") or "")) or "_populate_generic_event_marker"),
                "provider_ok": bool(ok),
                "rate_limit_policy": "single request; stop on 429/503",
            },
            error=error,
        )
        evidence["ts_ms"] = int(now_ms)
        if not ok:
            evidence["error"] = error or str(evidence.get("error") or "provider_populate_failed")
            evidence["status"] = "fail"
            evidence["contract_status"] = "fail"
        persisted = self._persist_populate_evidence(evidence, actor=actor, source=source)
        self._clear_data_credential_cache()
        return {
            "ok": bool(str(persisted.get("contract_status") or "") == "pass"),
            "source_key": key,
            "populate_evidence": persisted,
            "data_contract": contract.payload(),
            **({"error": str(persisted.get("error") or "")} if str(persisted.get("contract_status") or "") != "pass" else {}),
        }

    def manage_lifecycle(self, *, reason: str = "", jobs_manager=None) -> Dict[str, Any]:
        """Mark runtime configuration dirty and optionally start ingestion.

        Parameters
        ----------
        reason : str, default=""
            Reason stored in runtime metadata when the control plane changes.
        jobs_manager : Any, optional
            Jobs manager used to start ``ingestion_runtime`` when enabled source
            configuration implies active ingestion jobs.

        Returns
        -------
        dict
            Lifecycle summary containing ``reason``, ``desired_jobs``, and
            whether ``ingestion_runtime`` was started by this call.

        Side Effects
        ------------
        Updates runtime metadata to signal dirty source configuration and may
        start the ingestion runtime when it is required but not already
        running.
        """
        self.initialize()
        desired_jobs = self.get_desired_ingestion_jobs()
        self.mark_runtime_dirty(reason=reason or "data_sources_changed")
        started = False
        stopped = False
        if jobs_manager is not None:
            try:
                running = bool(jobs_manager.is_running("ingestion_runtime"))
            except Exception:
                running = True
            if desired_jobs and not running:
                try:
                    jobs_manager.start("ingestion_runtime")
                    started = True
                except Exception:
                    started = False
            elif (not desired_jobs) and running:
                try:
                    jobs_manager.stop("ingestion_runtime")
                    stopped = True
                except Exception:
                    stopped = False
        return {
            "ok": True,
            "reason": str(reason or "data_sources_changed"),
            "desired_jobs": desired_jobs,
            "ingestion_runtime_started": bool(started),
            "ingestion_runtime_stopped": bool(stopped),
        }

    def mark_runtime_dirty(self, *, reason: str = "") -> None:
        now_ms = int(time.time() * 1000)
        payload = {
            "ts_ms": int(now_ms),
            "reason": str(reason or "data_sources_changed"),
            "host": socket.gethostname(),
        }
        try:
            runtime_mod = sys.modules.get("engine.runtime.ingestion_runtime")
            invalidate = getattr(runtime_mod, "invalidate_supervisor_snapshot_cache", None)
            if callable(invalidate):
                invalidate("child_control_plane", "enabled_price_providers")
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_SUPERVISOR_CACHE_INVALIDATE_FAILED",
                e,
                once_key="data_source_manager_supervisor_cache_invalidate",
                reason=str(reason or "data_sources_changed"),
            )
        try:
            meta_set("data_sources_reload_ts_ms", str(int(now_ms)))
            meta_set("data_sources_dirty", self._json_dumps(payload))
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_MARK_RUNTIME_DIRTY_FAILED",
                e,
                once_key="data_source_manager_mark_runtime_dirty",
                reason=str(reason or "data_sources_changed"),
            )

    def connection_test_registry(self) -> Dict[str, Dict[str, str]]:
        """Return the explicit source-test registry used by ``test_connection``."""
        return {str(key): dict(value) for key, value in _PROVIDER_TEST_REGISTRY.items()}

    def _connection_result(
        self,
        status: str,
        classification: str,
        message: str,
        *,
        evidence: Optional[Dict[str, Any]] = None,
        next_steps: Iterable[str] = (),
    ) -> ConnectionTestResult:
        return ConnectionTestResult(
            status=str(status),
            classification=str(classification),
            message=str(message),
            evidence=dict(evidence or {}),
            next_steps=tuple(str(item) for item in next_steps if str(item or "").strip()),
        )

    def _connection_pass(self, message: str, **evidence: Any) -> ConnectionTestResult:
        return self._connection_result("pass", "success", message, evidence=evidence)

    def _connection_fail(
        self,
        classification: str,
        message: str,
        *,
        evidence: Optional[Dict[str, Any]] = None,
        next_steps: Iterable[str] = (),
    ) -> ConnectionTestResult:
        return self._connection_result("fail", classification, message, evidence=evidence, next_steps=next_steps)

    def _connection_degraded(
        self,
        classification: str,
        message: str,
        *,
        evidence: Optional[Dict[str, Any]] = None,
        next_steps: Iterable[str] = (),
    ) -> ConnectionTestResult:
        return self._connection_result("degraded", classification, message, evidence=evidence, next_steps=next_steps)

    def _connection_unsupported(self, message: str, **evidence: Any) -> ConnectionTestResult:
        return self._connection_result(
            "unsupported",
            "unsupported",
            message,
            evidence=evidence,
            next_steps=("Do not count this source as connected; rely on runtime health for this internal source.",),
        )

    def _missing_credentials_result(
        self,
        provider: str,
        *fields: str,
        source: Optional[Dict[str, Any]] = None,
    ) -> ConnectionTestResult:
        clean_fields = [str(field) for field in fields if str(field or "").strip()]
        evidence: Dict[str, Any] = {
            "provider": str(provider),
            "missing_fields": clean_fields,
            "missing_env_vars": clean_fields,
        }
        if source is not None:
            _template_key, definition = self._resolve_definition(
                str(source.get("source_key") or ""),
                source_type=str(source.get("source_type") or ""),
                provider_name=str(source.get("provider_name") or ""),
            )
            evidence["missing_credentials"] = self._missing_credential_metadata(
                source,
                definition,
                clean_fields,
            )
        return self._connection_fail(
            "missing_credentials",
            f"{provider}_credentials_missing",
            evidence=evidence,
            next_steps=("Configure the missing credential fields, save the source or provider account, then test again.",),
        )

    def _dependency_missing_result(self, provider: str, dependency: str, exc: BaseException | None = None) -> ConnectionTestResult:
        evidence: Dict[str, Any] = {"provider": str(provider), "dependency": str(dependency)}
        if exc is not None:
            evidence["error_type"] = type(exc).__name__
        return self._connection_fail(
            "provider_unreachable",
            f"{provider}_dependency_unavailable",
            evidence=evidence,
            next_steps=("Install or enable the provider dependency in the runtime environment, then test again.",),
        )

    def _safe_endpoint(self, url: Any) -> str:
        parsed = urlparse(str(url or ""))
        if not parsed.scheme or not parsed.netloc:
            return str(url or "").split("?", 1)[0][:240]
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    def _retry_after_s(self, response: Any, default_s: float) -> float:
        try:
            raw = (getattr(response, "headers", {}) or {}).get("Retry-After")
            if raw is not None and str(raw).strip():
                return max(0.0, float(str(raw).strip()))
        except (TypeError, ValueError) as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_RETRY_AFTER_PARSE_FAILED",
                RuntimeError(type(exc).__name__),
                once_key="connection_retry_after_parse_failed",
                error_type=type(exc).__name__,
            )
            return float(default_s)
        return float(default_s)

    def _connection_probe_rate_limit(self, source_key: str) -> Optional[ConnectionTestResult]:
        if _DATA_SOURCE_CONNECTION_TEST_MIN_INTERVAL_MS <= 0:
            return None
        now_ms = int(time.time() * 1000)
        key = str(source_key or "").strip().lower()
        with _DATA_SOURCE_CONNECTION_TEST_LOCK:
            previous_ms = int(_LAST_DATA_SOURCE_CONNECTION_TEST_PROBE_MS.get(key) or 0)
            remaining_ms = _DATA_SOURCE_CONNECTION_TEST_MIN_INTERVAL_MS - max(0, now_ms - previous_ms)
            if previous_ms and remaining_ms > 0:
                retry_after_s = max(1.0, float(remaining_ms) / 1000.0)
                return self._connection_degraded(
                    "rate_limited",
                    "control_plane_probe_rate_limited",
                    evidence={"retry_after_s": retry_after_s, "rate_limit_scope": "data_source_test", "source_key": key},
                    next_steps=(f"Retry after at least {retry_after_s:.1f} seconds.",),
                )
            _LAST_DATA_SOURCE_CONNECTION_TEST_PROBE_MS[key] = now_ms
        return None

    def _connection_effective_env_value(self, source: Dict[str, Any], env_name: str) -> str:
        source_key = str(source.get("source_key") or "")
        source_type = str(source.get("source_type") or "")
        provider_name = str(source.get("provider_name") or "")
        template_key, definition = self._resolve_definition(
            source_key,
            source_type=source_type,
            provider_name=provider_name,
        )
        _ = template_key
        credentials, _settings = self._test_effective_credentials_and_settings(source, definition)
        field_name = self._source_field_for_env(
            dict((definition.credential_env or {}) if definition is not None else {}),
            str(env_name),
        )
        if field_name:
            return str(credentials.get(field_name) or "").strip()
        projected: Dict[str, str] = {}
        resolved = self._resolve_effective_env_value(
            env_name=str(env_name),
            source_key=source_key,
            job_name=str(source.get("job_name") or ""),
            definition=definition,
            credentials=dict(source.get("credentials") or {}),
            settings=dict(source.get("settings") or {}),
            account_credentials=self._account_credentials_by_key(),
            allow_external=True,
            strict_projection=_strict_runtime_secret_projection(),
        )
        self._project_resolved_runtime_value(
            projected=projected,
            env_name=str(env_name),
            resolved=resolved,
            source_key=source_key,
        )
        self._clear_data_credential_cache()
        try:
            from engine.data._credentials import get_data_credential

            with self._with_projected_credential_environment(projected):
                return str(get_data_credential(str(env_name), ttl_s=0) or "").strip()
        finally:
            self._clear_data_credential_cache()

    def _connection_setting(self, source: Dict[str, Any], field_name: str, env_name: str = "", default: Any = "") -> str:
        settings = dict(source.get("effective_settings") or source.get("settings") or {})
        value = settings.get(str(field_name))
        if value is not None and str(value).strip() != "":
            return str(value).strip()
        if env_name and str(os.environ.get(str(env_name)) or "").strip():
            return str(os.environ.get(str(env_name)) or "").strip()
        return str(default or "").strip()

    def _http_problem_result(
        self,
        response: Any,
        *,
        provider: str,
        endpoint: str,
    ) -> Optional[ConnectionTestResult]:
        status_code = int(getattr(response, "status_code", 0) or 0)
        evidence = {"provider": str(provider), "endpoint": self._safe_endpoint(endpoint), "status_code": status_code}
        if status_code == 429:
            retry_after_s = self._retry_after_s(response, 60.0)
            evidence["retry_after_s"] = retry_after_s
            evidence["stop_testing"] = True
            return self._connection_degraded(
                "rate_limited",
                f"{provider}_rate_limited",
                evidence=evidence,
                next_steps=(f"Stop testing this provider now; retry after at least {retry_after_s:.0f} seconds.", "Reduce polling or upgrade the provider plan."),
            )
        if status_code == 503:
            retry_after_s = self._retry_after_s(response, 300.0)
            evidence["retry_after_s"] = retry_after_s
            evidence["stop_testing"] = True
            return self._connection_degraded(
                "provider_unreachable",
                f"{provider}_temporarily_unavailable",
                evidence=evidence,
                next_steps=(f"Stop testing this provider now; retry after at least {retry_after_s:.0f} seconds.",),
            )
        if status_code == 401:
            return self._connection_fail(
                "wrong_credentials",
                f"{provider}_credentials_rejected",
                evidence=evidence,
                next_steps=("Replace the saved credentials or provider-account secret, then test again.",),
            )
        if status_code == 403:
            return self._connection_fail(
                "entitlement_missing",
                f"{provider}_entitlement_missing",
                evidence=evidence,
                next_steps=("Confirm the provider account has the required dataset entitlement and endpoint access.",),
            )
        if 400 <= status_code < 500:
            return self._connection_fail(
                "provider_unreachable",
                f"{provider}_http_error",
                evidence=evidence,
                next_steps=("Review the provider endpoint settings and account permissions, then test again.",),
            )
        if status_code >= 500:
            return self._connection_fail(
                "provider_unreachable",
                f"{provider}_server_error",
                evidence=evidence,
                next_steps=("Wait for the provider service to recover, then test again.",),
            )
        return None

    def _request_exception_result(self, provider: str, endpoint: str, exc: BaseException) -> ConnectionTestResult:
        classification = "rate_limited" if "rate" in type(exc).__name__.lower() else "provider_unreachable"
        status = "degraded" if classification == "rate_limited" else "fail"
        return self._connection_result(
            status,
            classification,
            f"{provider}_{classification}",
            evidence={"provider": str(provider), "endpoint": self._safe_endpoint(endpoint), "error_type": type(exc).__name__},
            next_steps=("Retry later if this is transient; otherwise verify network, DNS, and provider status.",),
        )

    def _provider_user_agent(self, source: Dict[str, Any], *, field: str, env_name: str, default: str) -> str:
        return self._connection_setting(source, field, env_name, default) or str(default)

    def _rss_payload_looks_valid(self, text: Any) -> bool:
        payload = str(text or "").strip()
        if not payload:
            return False
        try:
            from xml.etree import ElementTree as ET

            root = ET.fromstring(payload)
            tag = str(root.tag or "").split("}", 1)[-1].lower()
            if tag in {"rss", "feed", "rdf"}:
                return True
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_RSS_PAYLOAD_PARSE_FAILED",
                exc,
                error_type=type(exc).__name__,
                payload_length=len(payload),
            )
            return False
        lower = payload[:1000].lower()
        return "<rss" in lower or "<feed" in lower

    def _sec_identity_problem(self, source: Dict[str, Any], *, provider: str) -> Optional[ConnectionTestResult]:
        user_agent = self._connection_setting(source, "user_agent", "SEC_USER_AGENT", "")
        user_agent_l = str(user_agent or "").strip().lower()
        placeholder = (
            not user_agent_l
            or "example.com" in user_agent_l
            or "example.invalid" in user_agent_l
            or "market-impact-dev" in user_agent_l
            or "trading-system-data-source-test" in user_agent_l
        )
        if not placeholder:
            return None
        return self._connection_degraded(
            "missing_credentials",
            "sec_identity_missing_or_placeholder",
            evidence={
                "provider": str(provider),
                "missing_env_vars": ["SEC_USER_AGENT"],
                "placeholder_identity": True,
            },
            next_steps=("Configure SEC_USER_AGENT with an application name and monitored operator contact before enabling SEC/Form 4 ingestion.",),
        )

    def _payload_count(self, payload: Any, paths: Iterable[str] = ()) -> int:
        candidates = list(paths or ())
        if not candidates:
            candidates = [""]
        for path in candidates:
            current = payload
            if str(path):
                for part in str(path).split("."):
                    if isinstance(current, dict):
                        current = current.get(part)
                    else:
                        current = None
                        break
            if isinstance(current, list):
                return int(len(current))
            if isinstance(current, dict):
                return 1 if current else 0
            if current not in (None, ""):
                return 1
        return 0

    def _http_json_probe(
        self,
        source: Dict[str, Any],
        *,
        provider: str,
        url: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        body: Any = None,
        timeout_s: float = 10.0,
        expected_paths: Iterable[str] = (),
        success_message: str,
        empty_message: str,
        validator: Optional[Any] = None,
        invalid_message: Optional[str] = None,
        apply_rate_limit: bool = True,
    ) -> ConnectionTestResult:
        source_key = str(source.get("source_key") or "")
        if apply_rate_limit:
            limited = self._connection_probe_rate_limit(source_key)
            if limited is not None:
                return limited
        endpoint = self._safe_endpoint(url)
        try:
            if str(method).upper() == "POST":
                response = requests.post(
                    url,
                    params=params,
                    headers=headers,
                    data=body,
                    timeout=float(timeout_s),
                )
            else:
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=float(timeout_s),
                )
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_HTTP_REQUEST_FAILED",
                RuntimeError(type(exc).__name__),
                provider=str(provider),
                endpoint=endpoint,
                error_type=type(exc).__name__,
            )
            return self._request_exception_result(provider, endpoint, exc)
        problem = self._http_problem_result(response, provider=provider, endpoint=endpoint)
        if problem is not None:
            return problem
        try:
            payload = response.json()
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_JSON_DECODE_FAILED",
                RuntimeError(type(exc).__name__),
                provider=str(provider),
                endpoint=endpoint,
                error_type=type(exc).__name__,
            )
            return self._connection_fail(
                "empty_payload",
                f"{provider}_invalid_json",
                evidence={"provider": provider, "endpoint": endpoint, "status_code": int(getattr(response, "status_code", 0) or 0), "error_type": type(exc).__name__},
                next_steps=("The provider responded but did not return the expected JSON payload. Check endpoint settings and provider status.",),
            )
        if validator is not None:
            try:
                valid = bool(validator(payload))
            except Exception as exc:
                _warn_nonfatal(
                    "DATA_SOURCE_CONNECTION_JSON_VALIDATION_FAILED",
                    RuntimeError(type(exc).__name__),
                    provider=str(provider),
                    endpoint=endpoint,
                    error_type=type(exc).__name__,
                )
                valid = False
            if not valid:
                return self._connection_fail(
                    "malformed_payload",
                    str(invalid_message or f"{provider}_malformed_payload"),
                    evidence={
                        "provider": provider,
                        "endpoint": endpoint,
                        "status_code": int(getattr(response, "status_code", 0) or 0),
                    },
                    next_steps=("The provider responded but the payload did not match the expected schema.",),
                )
        count = self._payload_count(payload, expected_paths)
        if count <= 0:
            return self._connection_fail(
                "empty_payload",
                empty_message,
                evidence={"provider": provider, "endpoint": endpoint, "status_code": int(getattr(response, "status_code", 0) or 0), "payload_count": 0},
                next_steps=("The provider authenticated but returned no usable rows for the probe. Verify symbols, dataset coverage, and entitlements.",),
            )
        return self._connection_pass(
            success_message,
            provider=provider,
            endpoint=endpoint,
            status_code=int(getattr(response, "status_code", 0) or 0),
            payload_count=count,
        )

    def _http_text_probe(
        self,
        source: Dict[str, Any],
        *,
        provider: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_s: float = 10.0,
        success_message: str,
        empty_message: str,
        validator: Optional[Any] = None,
        invalid_message: Optional[str] = None,
        stream: bool = False,
        apply_rate_limit: bool = True,
    ) -> ConnectionTestResult:
        source_key = str(source.get("source_key") or "")
        if apply_rate_limit:
            limited = self._connection_probe_rate_limit(source_key)
            if limited is not None:
                return limited
        endpoint = self._safe_endpoint(url)
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=float(timeout_s),
                stream=bool(stream),
            )
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_HTTP_REQUEST_FAILED",
                RuntimeError(type(exc).__name__),
                provider=str(provider),
                endpoint=endpoint,
                error_type=type(exc).__name__,
            )
            return self._request_exception_result(provider, endpoint, exc)
        problem = self._http_problem_result(response, provider=provider, endpoint=endpoint)
        if problem is not None:
            return problem
        text = ""
        try:
            if stream and hasattr(response, "iter_content"):
                chunks: List[bytes] = []
                for chunk in response.iter_content(chunk_size=2048):
                    if chunk:
                        chunks.append(chunk if isinstance(chunk, bytes) else str(chunk).encode("utf-8", "ignore"))
                    if sum(len(item) for item in chunks) >= 4096:
                        break
                text = b"".join(chunks).decode("utf-8", "ignore")
            else:
                text = str(getattr(response, "text", "") or "")
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_TEXT_READ_FAILED",
                RuntimeError(type(exc).__name__),
                provider=str(provider),
                endpoint=endpoint,
                error_type=type(exc).__name__,
            )
            return self._connection_fail(
                "empty_payload",
                f"{provider}_payload_read_failed",
                evidence={"provider": provider, "endpoint": endpoint, "status_code": int(getattr(response, "status_code", 0) or 0), "error_type": type(exc).__name__},
                next_steps=("The provider responded but the probe could not read a usable payload. Check provider status and endpoint settings.",),
            )
        valid = bool(text.strip())
        if validator is not None:
            try:
                valid = bool(validator(text))
            except Exception as exc:
                _warn_nonfatal(
                    "DATA_SOURCE_CONNECTION_PAYLOAD_VALIDATION_FAILED",
                    RuntimeError(type(exc).__name__),
                    provider=str(provider),
                    endpoint=endpoint,
                    error_type=type(exc).__name__,
                )
                return self._connection_fail(
                    "malformed_payload",
                    str(invalid_message or f"{provider}_payload_validation_failed"),
                    evidence={"provider": provider, "endpoint": endpoint, "status_code": int(getattr(response, "status_code", 0) or 0), "error_type": type(exc).__name__},
                    next_steps=("The provider responded but the payload did not match the expected format.",),
                )
        if not valid:
            return self._connection_fail(
                "malformed_payload" if invalid_message else "empty_payload",
                str(invalid_message or empty_message),
                evidence={"provider": provider, "endpoint": endpoint, "status_code": int(getattr(response, "status_code", 0) or 0), "payload_count": 0},
                next_steps=("The provider authenticated but returned no usable rows for the probe. Verify the endpoint, publication schedule, and dataset coverage.",),
            )
        return self._connection_pass(
            success_message,
            provider=provider,
            endpoint=endpoint,
            status_code=int(getattr(response, "status_code", 0) or 0),
            payload_count=1,
        )

    def _test_polygon_rest_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        api_key = self._connection_effective_env_value(source, "POLYGON_API_KEY")
        if not api_key:
            return self._missing_credentials_result("polygon", "POLYGON_API_KEY", source=source)
        return self._http_json_probe(
            source,
            provider="polygon",
            url="https://api.polygon.io/v3/reference/tickers",
            params={"market": "stocks", "limit": 1, "apiKey": api_key},
            expected_paths=("results",),
            success_message="polygon_rest_connection_ok",
            empty_message="polygon_rest_empty_payload",
        )

    def _test_polygon_options_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        api_key = self._connection_effective_env_value(source, "POLYGON_API_KEY")
        if not api_key:
            return self._missing_credentials_result("polygon_options", "POLYGON_API_KEY", source=source)
        return self._http_json_probe(
            source,
            provider="polygon_options",
            url="https://api.polygon.io/v3/snapshot/options/SPY",
            params={"limit": 1, "apiKey": api_key},
            expected_paths=("results",),
            success_message="polygon_options_connection_ok",
            empty_message="polygon_options_empty_payload",
        )

    def _test_oanda_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        token = self._connection_effective_env_value(source, "OANDA_ACCESS_TOKEN") or self._connection_effective_env_value(source, "OANDA_API_KEY")
        if not token:
            return self._missing_credentials_result("oanda_fx", "OANDA_ACCESS_TOKEN", source=source)
        account_id = self._connection_setting(source, "account_id", "OANDA_ACCOUNT_ID", "")
        if not str(account_id or "").strip():
            return self._connection_fail(
                "missing_settings",
                "oanda_fx_account_id_missing",
                evidence={"provider": "oanda_fx", "missing_settings": ["OANDA_ACCOUNT_ID"]},
                next_steps=("Configure OANDA_ACCOUNT_ID, save the source, then test again.",),
            )
        environment = str(self._connection_setting(source, "environment", "OANDA_ENVIRONMENT", "practice") or "practice").strip().lower()
        base_url = "https://api-fxtrade.oanda.com" if environment == "live" else "https://api-fxpractice.oanda.com"
        return self._http_json_probe(
            source,
            provider="oanda_fx",
            url=f"{base_url}/v3/accounts/{account_id}/pricing",
            params={"instruments": "EUR_USD"},
            headers={"Authorization": f"Bearer {token}"},
            expected_paths=("prices",),
            success_message="oanda_fx_connection_ok",
            empty_message="oanda_fx_empty_payload",
        )

    def _test_polygon_ws_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        api_key = self._connection_effective_env_value(source, "POLYGON_API_KEY")
        if not api_key:
            return self._missing_credentials_result("polygon_ws", "POLYGON_API_KEY", source=source)
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        endpoint = self._connection_setting(source, "endpoint", "POLYGON_WS_ENDPOINT", "wss://socket.polygon.io/stocks")
        safe_endpoint = self._safe_endpoint(endpoint)
        try:
            import websocket  # type: ignore
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_DEPENDENCY_IMPORT_FAILED",
                RuntimeError(type(exc).__name__),
                once_key="connection_dependency:polygon_ws:websocket-client",
                provider="polygon_ws",
                dependency="websocket-client",
                error_type=type(exc).__name__,
            )
            return self._dependency_missing_result("polygon_ws", "websocket-client", exc)
        ws = None
        try:
            ws = websocket.create_connection(endpoint, timeout=5.0)
            ws.send(json.dumps({"action": "auth", "params": api_key}, separators=(",", ":")))
            for _idx in range(5):
                raw = ws.recv()
                payload = json.loads(str(raw or "[]"))
                rows = payload if isinstance(payload, list) else [payload]
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    status = str(row.get("status") or "").strip().lower()
                    message = str(row.get("message") or "").strip().lower()
                    if status in {"auth_success", "success"} or "authenticated" in message:
                        return self._connection_pass("polygon_ws_auth_ok", provider="polygon_ws", endpoint=safe_endpoint)
                    if status in {"auth_failed", "failed"} or "not authorized" in message or "invalid" in message:
                        return self._connection_fail(
                            "wrong_credentials",
                            "polygon_ws_credentials_rejected",
                            evidence={"provider": "polygon_ws", "endpoint": safe_endpoint},
                            next_steps=("Replace the Polygon API key or verify WebSocket entitlement.",),
                        )
            return self._connection_fail(
                "empty_payload",
                "polygon_ws_auth_result_missing",
                evidence={"provider": "polygon_ws", "endpoint": safe_endpoint},
                next_steps=("Verify the WebSocket endpoint and plan entitlement, then test again.",),
            )
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_WS_PROBE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="polygon_ws",
                endpoint=safe_endpoint,
                error_type=type(exc).__name__,
            )
            return self._request_exception_result("polygon_ws", safe_endpoint, exc)
        finally:
            try:
                if ws is not None:
                    ws.close()
            except Exception as close_exc:
                LOG.debug(
                    "data_source_connection_close_failed provider=polygon_ws error_type=%s",
                    type(close_exc).__name__,
                )

    def _must_stop_probe(self, result: ConnectionTestResult) -> bool:
        return bool((result.evidence or {}).get("stop_testing"))

    def _sec_headers(self, source: Dict[str, Any]) -> Dict[str, str]:
        user_agent = self._connection_setting(
            source,
            "user_agent",
            "SEC_USER_AGENT",
            "trading-system-data-source-test contact@example.invalid",
        )
        headers = {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        }
        from_header = self._connection_setting(source, "from", "SEC_FROM", "")
        if from_header:
            headers["From"] = from_header
        return headers

    def _test_tradier_options_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        token = self._connection_effective_env_value(source, "TRADIER_API_TOKEN")
        if not token:
            return self._missing_credentials_result("tradier", "TRADIER_API_TOKEN", source=source)
        return self._http_json_probe(
            source,
            provider="tradier",
            url="https://api.tradier.com/v1/markets/options/expirations",
            params={"symbol": "SPY"},
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            expected_paths=("expirations.date", "expirations"),
            success_message="tradier_connection_ok",
            empty_message="tradier_empty_payload",
        )

    def _test_finnhub_company_news_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        api_key = self._connection_effective_env_value(source, "FINNHUB_API_KEY")
        if not api_key:
            return self._missing_credentials_result("company_news", "FINNHUB_API_KEY", source=source)
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=7)
        return self._http_json_probe(
            source,
            provider="company_news",
            url="https://finnhub.io/api/v1/company-news",
            params={"symbol": "AAPL", "from": start.isoformat(), "to": end.isoformat(), "token": api_key},
            expected_paths=("",),
            success_message="finnhub_company_news_connection_ok",
            empty_message="finnhub_company_news_empty_payload",
        )

    def _test_fmp_transcripts_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        api_key = self._connection_effective_env_value(source, "FMP_API_KEY")
        if not api_key:
            return self._missing_credentials_result("transcripts", "FMP_API_KEY", source=source)
        return self._http_json_probe(
            source,
            provider="transcripts",
            url="https://financialmodelingprep.com/api/v3/earning_call_transcript/AAPL",
            params={"year": "2024", "quarter": "1", "apikey": api_key},
            expected_paths=("",),
            success_message="fmp_transcripts_connection_ok",
            empty_message="fmp_transcripts_empty_payload",
        )

    def _test_fmp_earnings_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        api_key = self._connection_effective_env_value(source, "FMP_API_KEY")
        if not api_key:
            return self._missing_credentials_result("earnings", "FMP_API_KEY", source=source)
        today = datetime.now(timezone.utc).date()
        return self._http_json_probe(
            source,
            provider="earnings",
            url="https://financialmodelingprep.com/api/v3/earning_calendar",
            params={"from": today.isoformat(), "to": (today + timedelta(days=30)).isoformat(), "apikey": api_key},
            expected_paths=("",),
            success_message="fmp_earnings_connection_ok",
            empty_message="fmp_earnings_empty_payload",
        )

    def _test_fmp_profile_connection(self, source: Dict[str, Any], *, provider: str) -> ConnectionTestResult:
        api_key = self._connection_effective_env_value(source, "FMP_API_KEY")
        if not api_key:
            return self._missing_credentials_result(provider, "FMP_API_KEY", source=source)
        return self._http_json_probe(
            source,
            provider=provider,
            url="https://financialmodelingprep.com/api/v3/profile/AAPL",
            params={"apikey": api_key},
            success_message=f"{provider}_connection_ok",
            empty_message=f"{provider}_empty_payload",
        )

    def _test_reddit_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        client_id = self._connection_effective_env_value(source, "REDDIT_CLIENT_ID")
        client_secret = self._connection_effective_env_value(source, "REDDIT_CLIENT_SECRET")
        missing = []
        if not client_id:
            missing.append("REDDIT_CLIENT_ID")
        if not client_secret:
            missing.append("REDDIT_CLIENT_SECRET")
        if missing:
            return self._missing_credentials_result("reddit", *missing, source=source)
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        try:
            import praw
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_DEPENDENCY_IMPORT_FAILED",
                RuntimeError(type(exc).__name__),
                once_key="connection_dependency:reddit:praw",
                provider="reddit",
                dependency="praw",
                error_type=type(exc).__name__,
            )
            return self._dependency_missing_result("reddit", "praw", exc)
        try:
            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=self._connection_setting(
                    source,
                    "user_agent",
                    "REDDIT_USER_AGENT",
                    "market-research-bot",
                ),
            )
            rows = list(reddit.subreddit("investing").hot(limit=1))
        except Exception as exc:
            name = type(exc).__name__.lower()
            classification = "wrong_credentials" if "auth" in name or "forbidden" in name or "unauthorized" in name else "provider_unreachable"
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_REDDIT_PROBE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="reddit",
                classification=classification,
                error_type=type(exc).__name__,
            )
            return self._connection_fail(
                classification,
                "reddit_oauth_probe_failed",
                evidence={"provider": "reddit", "error_type": type(exc).__name__},
                next_steps=("Verify the Reddit app credentials and API access, then test again.",),
            )
        if not rows:
            return self._connection_fail(
                "empty_payload",
                "reddit_empty_payload",
                evidence={"provider": "reddit", "payload_count": 0},
                next_steps=("Reddit authenticated but returned no posts for the probe subreddit. Retry later or adjust source settings.",),
            )
        return self._connection_pass("reddit_oauth_connection_ok", provider="reddit", payload_count=len(rows))

    def _test_stocktwits_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        return self._http_json_probe(
            source,
            provider="stocktwits",
            url=self._connection_setting(
                source,
                "trending_url",
                "STOCKTWITS_TRENDING_URL",
                "https://api.stocktwits.com/api/2/streams/trending.json",
            ),
            timeout_s=float(self._connection_setting(source, "timeout_s", "STOCKTWITS_TIMEOUT_S", 10.0)),
            headers={
                "User-Agent": self._provider_user_agent(source, field="user_agent", env_name="STOCKTWITS_HTTP_UA", default="trading-system/1.0 stocktwits-feed"),
                "Accept": "application/json",
            },
            expected_paths=("messages", "symbols"),
            validator=lambda payload: isinstance(payload, dict) and (
                isinstance(payload.get("messages"), list) or isinstance(payload.get("symbols"), list)
            ),
            success_message="stocktwits_connection_ok",
            empty_message="stocktwits_empty_payload",
            invalid_message="stocktwits_malformed_payload",
        )

    def _test_gdelt_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        return self._http_json_probe(
            source,
            provider="gdelt",
            url="https://api.gdeltproject.org/api/v2/doc/doc",
            params={"query": "apple", "mode": "artlist", "maxrecords": 1, "format": "json"},
            headers={
                "User-Agent": self._provider_user_agent(source, field="user_agent", env_name="GDELT_HTTP_UA", default="trading-system/1.0 gdelt-feed"),
                "Accept": "application/json",
            },
            expected_paths=("articles",),
            validator=lambda payload: isinstance(payload, dict) and isinstance(payload.get("articles"), list),
            success_message="gdelt_connection_ok",
            empty_message="gdelt_empty_payload",
            invalid_message="gdelt_malformed_payload",
        )

    def _test_sec_filings_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        identity_problem = self._sec_identity_problem(source, provider="sec")
        if identity_problem is not None:
            return identity_problem
        return self._http_json_probe(
            source,
            provider="sec",
            url="https://www.sec.gov/files/company_tickers_exchange.json",
            headers=self._sec_headers(source),
            expected_paths=("data", "fields"),
            validator=lambda payload: isinstance(payload, dict) and isinstance(payload.get("fields"), list) and isinstance(payload.get("data"), list),
            success_message="sec_filings_connection_ok",
            empty_message="sec_filings_empty_payload",
            invalid_message="sec_filings_malformed_payload",
        )

    def _test_form4_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        identity_problem = self._sec_identity_problem(source, provider="form4")
        if identity_problem is not None:
            return identity_problem
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        try:
            from engine.data.sec import form4_live

            probe = form4_live.probe_form4_xml_document(symbol="AAPL", filing_limit=3)
        except Exception as exc:
            classification = str(getattr(exc, "classification", "") or "provider_unreachable")
            status = "degraded" if classification == "rate_limited" else "fail"
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_SEC_FORM4_XML_DOCUMENT_DISCOVERY_FAILED",
                exc,
                classification=classification,
                status_code=int(getattr(exc, "status_code", 0) or 0),
                endpoint=self._safe_endpoint(getattr(exc, "endpoint", "") or "https://www.sec.gov/Archives/"),
                error_type=type(exc).__name__,
            )
            return self._connection_result(
                status,
                classification,
                "sec_form4_xml_document_discovery_failed",
                evidence={
                    "provider": "form4",
                    "endpoint": self._safe_endpoint(getattr(exc, "endpoint", "") or "https://www.sec.gov/Archives/"),
                    "status_code": int(getattr(exc, "status_code", 0) or 0),
                    "error_type": type(exc).__name__,
                },
                next_steps=("Verify SEC_USER_AGENT/SEC_FROM, then inspect the SEC filing directory index for a valid ownership XML information document.",),
            )
        payload_count = int((probe or {}).get("payload_count") or 0)
        if payload_count <= 0:
            return self._connection_fail(
                "empty_payload",
                "sec_form4_no_transactions_in_xml_document",
                evidence={"provider": "form4", "endpoint": self._safe_endpoint((probe or {}).get("url")), "payload_count": 0},
                next_steps=("The XML ownership document was discovered but contained no parseable transactions. Retry with a newer Form 4 filing.",),
            )
        return self._connection_pass(
            "sec_form4_connection_ok",
            provider="form4",
            endpoint=self._safe_endpoint((probe or {}).get("url")),
            payload_count=payload_count,
            filing_accession=str((probe or {}).get("filing_accession") or ""),
        )

    def _test_inst_13f_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        identity_problem = self._sec_identity_problem(source, provider="inst_13f")
        if identity_problem is not None:
            return identity_problem
        return self._http_text_probe(
            source,
            provider="inst_13f",
            url="https://www.sec.gov/cgi-bin/browse-edgar",
            params={"action": "getcompany", "CIK": "0001067983", "type": "13F-HR", "count": "1", "output": "atom"},
            headers=self._sec_headers(source),
            success_message="sec_13f_connection_ok",
            empty_message="sec_13f_empty_payload",
            validator=lambda text: "<entry" in text.lower() or "<feed" in text.lower(),
            invalid_message="sec_13f_malformed_payload",
        )

    def _test_yfinance_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        try:
            import yfinance as yf  # type: ignore
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_DEPENDENCY_IMPORT_FAILED",
                RuntimeError(type(exc).__name__),
                once_key="connection_dependency:yfinance:yfinance",
                provider="yfinance",
                dependency="yfinance",
                error_type=type(exc).__name__,
            )
            return self._dependency_missing_result("yfinance", "yfinance", exc)
        try:
            frame = yf.Ticker("SPY").history(period="5d", interval="1d")
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_YFINANCE_PROBE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="yfinance",
                symbol="SPY",
                error_type=type(exc).__name__,
            )
            return self._request_exception_result("yfinance", "yfinance:SPY", exc)
        count = int(getattr(frame, "shape", [0])[0] if frame is not None else 0)
        if count <= 0:
            return self._connection_fail(
                "empty_payload",
                "yfinance_empty_payload",
                evidence={"provider": "yfinance", "symbol": "SPY", "payload_count": 0},
                next_steps=("Yahoo Finance returned no rows for SPY. Retry later or disable this optional provider.",),
            )
        return self._connection_pass("yfinance_connection_ok", provider="yfinance", symbol="SPY", payload_count=count)

    def _test_simulated_price_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        try:
            from engine.data.live_prices.simulated import SimulatedPriceProvider
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_SIMULATED_PROVIDER_IMPORT_FAILED",
                RuntimeError(type(exc).__name__),
                provider="simulated",
                error_type=type(exc).__name__,
            )
            return self._connection_fail(
                "dependency_missing",
                "simulated_provider_import_failed",
                evidence={"provider": "simulated", "simulated": True, "error_type": type(exc).__name__},
                next_steps=("Check the local simulated provider module.",),
            )
        settings = dict(source.get("settings") or {})
        raw_symbols = str(settings.get("symbols") or os.environ.get("SIMULATED_MARKET_DATA_SYMBOLS", "") or "SPY").strip()
        symbols = [part.strip().upper() for part in raw_symbols.split(",") if part.strip()] or ["SPY"]
        rows = SimulatedPriceProvider().fetch_last_prices({symbol: symbol for symbol in symbols[:8]})
        if not rows:
            return self._connection_fail(
                "empty_payload",
                "simulated_price_empty_payload",
                evidence={"provider": "simulated", "simulated": True, "payload_count": 0},
            )
        return self._connection_pass(
            "simulated_price_connection_ok",
            provider="simulated",
            simulated=True,
            symbol=str(next(iter(rows.keys()))),
            payload_count=len(rows),
        )

    def _test_ccxt_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        exchange_id = self._connection_setting(source, "exchange_id", "CCXT_EXCHANGE_ID", "coinbase")
        symbol = self._connection_setting(source, "symbol", "CCXT_TEST_SYMBOL", "BTC/USD")
        try:
            import ccxt
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_DEPENDENCY_IMPORT_FAILED",
                RuntimeError(type(exc).__name__),
                once_key="connection_dependency:ccxt:ccxt",
                provider="ccxt",
                dependency="ccxt",
                error_type=type(exc).__name__,
            )
            return self._dependency_missing_result("ccxt", "ccxt", exc)
        try:
            exchange_cls = getattr(ccxt, exchange_id)
            exchange = exchange_cls({"enableRateLimit": True, "timeout": 10000})
            markets = exchange.load_markets()
            if isinstance(markets, dict) and symbol not in markets:
                symbol = str(next(iter(markets), symbol))
            payload = exchange.fetch_ticker(symbol) if getattr(exchange, "has", {}).get("fetchTicker", True) else markets
        except AttributeError as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_CCXT_EXCHANGE_UNKNOWN",
                RuntimeError(type(exc).__name__),
                provider="ccxt",
                exchange_id=exchange_id,
                error_type=type(exc).__name__,
            )
            return self._connection_fail(
                "provider_unreachable",
                "ccxt_exchange_unknown",
                evidence={"provider": "ccxt", "exchange_id": exchange_id},
                next_steps=("Set CCXT_EXCHANGE_ID to a supported ccxt exchange id.",),
            )
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_CCXT_PROBE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="ccxt",
                exchange_id=exchange_id,
                error_type=type(exc).__name__,
            )
            return self._request_exception_result("ccxt", exchange_id, exc)
        if not payload:
            return self._connection_fail(
                "empty_payload",
                "ccxt_empty_payload",
                evidence={"provider": "ccxt", "exchange_id": exchange_id, "symbol": symbol, "payload_count": 0},
                next_steps=("The exchange responded but returned no usable ticker or market payload. Verify exchange id and public endpoint support.",),
            )
        return self._connection_pass("ccxt_connection_ok", provider="ccxt", exchange_id=exchange_id, symbol=symbol, payload_count=1)

    def _test_ibkr_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        host = self._connection_setting(source, "host", "IBKR_HOST", default_ibkr_host())
        try:
            port = int(self._connection_setting(source, "port", "IBKR_PORT", "7497"))
        except ValueError as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_IBKR_PORT_INVALID",
                RuntimeError(type(exc).__name__),
                provider="ibkr",
                error_type=type(exc).__name__,
            )
            return self._connection_fail(
                "provider_unreachable",
                "ibkr_port_invalid",
                evidence={"provider": "ibkr"},
                next_steps=("Set IBKR_PORT to a numeric TWS or Gateway API port.",),
            )
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        try:
            from ib_insync import IB, Stock  # type: ignore
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_DEPENDENCY_IMPORT_FAILED",
                RuntimeError(type(exc).__name__),
                once_key="connection_dependency:ibkr:ib_insync",
                provider="ibkr",
                dependency="ib_insync",
                error_type=type(exc).__name__,
            )
            return self._dependency_missing_result("ibkr", "ib_insync", exc)
        try:
            client_id = int(self._connection_setting(source, "client_id", "IBKR_CLIENT_ID", "67") or 67)
        except (TypeError, ValueError) as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_IBKR_CLIENT_ID_INVALID",
                RuntimeError(type(exc).__name__),
                provider="ibkr",
                error_type=type(exc).__name__,
            )
            client_id = 67
        try:
            market_data_type = int(self._connection_setting(source, "market_data_type", "IBKR_MARKET_DATA_TYPE", "1") or 1)
        except (TypeError, ValueError) as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_IBKR_MARKET_DATA_TYPE_INVALID",
                RuntimeError(type(exc).__name__),
                provider="ibkr",
                error_type=type(exc).__name__,
            )
            market_data_type = 1
        currency = self._connection_setting(source, "currency", "IBKR_CURRENCY", "USD") or "USD"
        settings = IBKRBrokerDataSettings(
            host=host,
            port=port,
            client_id=client_id,
            market_data_type=market_data_type,
            currency=currency,
            timeout_s=5.0,
        )
        client = IBKRBrokerDataReadOnlyClient(
            ib_factory=IB,
            stock_factory=Stock,
            settings=settings,
        )
        try:
            probe = client.probe_historical_data()
        except BrokerDataReadOnlyViolation as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_IBKR_READONLY_POLICY_BLOCKED",
                RuntimeError(type(exc).__name__),
                provider="ibkr",
                endpoint=f"{host}:{port}",
                error_type=type(exc).__name__,
            )
            return self._connection_fail(
                "policy_blocked",
                "ibkr_readonly_policy_blocked",
                evidence={"provider": "ibkr", "host": host, "port": port, **client.guard_evidence()},
                next_steps=("Review the IBKR data-source read-only policy before testing again.",),
            )
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_IBKR_PROBE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="ibkr",
                endpoint=f"{host}:{port}",
                error_type=type(exc).__name__,
            )
            return self._request_exception_result("ibkr", f"{host}:{port}", exc)
        payload_count = int((probe or {}).get("payload_count") or 0)
        evidence = {
            "provider": "ibkr",
            "host": host,
            "port": port,
            **client.guard_evidence(),
            "payload_count": payload_count,
            "market_data_type": int(market_data_type),
        }
        if payload_count <= 0:
            return self._connection_fail(
                "empty_payload",
                "ibkr_market_data_empty_payload",
                evidence=evidence,
                next_steps=("IBKR authenticated but returned no market-data rows. Check market-data subscriptions and TWS/Gateway permissions.",),
            )
        return self._connection_pass("ibkr_market_data_connection_ok", **evidence)

    def _test_alpaca_broker_data_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        key_id = self._connection_effective_env_value(source, "ALPACA_KEY_ID")
        secret_key = self._connection_effective_env_value(source, "ALPACA_SECRET_KEY")
        missing = []
        if not key_id:
            missing.append("ALPACA_KEY_ID")
        if not secret_key:
            missing.append("ALPACA_SECRET_KEY")
        if missing:
            return self._missing_credentials_result("alpaca_broker_data", *missing, source=source)
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        base_url = self._connection_setting(source, "base_url", "ALPACA_BASE_URL", ALPACA_PAPER_BASE_URL)
        stream_url = self._connection_setting(source, "stream_url", "ALPACA_STREAM_URL", "")
        trade_updates_ws_enabled = str(
            self._connection_setting(source, "trade_updates_ws_enabled", "ALPACA_TRADE_UPDATES_WS_ENABLED", "0")
            or ""
        ).strip().lower() in {"1", "true", "yes", "on"}
        settings = AlpacaBrokerDataSettings(
            base_url=base_url,
            stream_url=stream_url,
            trade_updates_ws_enabled=trade_updates_ws_enabled,
            timeout_s=10.0,
            allow_live_base_url=_env_flag(ALLOW_LIVE_ALPACA_BROKER_DATA_ENV, False),
        )
        client = AlpacaBrokerDataReadOnlyClient(
            key_id=key_id,
            secret_key=secret_key,
            settings=settings,
            http_get=requests.get,
        )
        evidence = {
            "provider": "alpaca_broker_data",
            "runtime_runnable": False,
            "live_probe": True,
            **client.guard_evidence(),
        }
        policy = client.base_url_policy()
        evidence.update(
            {
                "base_url_policy": str(policy.get("policy") or ""),
                "paper_base_url": bool(policy.get("paper_base_url")),
                "live_base_url": bool(policy.get("live_base_url")),
                "probed_paths": sorted(readonly_guard_snapshot().get("alpaca_allowed_paths") or []),
            }
        )
        if not bool(policy.get("ok")):
            return self._connection_fail(
                "policy_blocked",
                "alpaca_live_base_url_blocked",
                evidence=evidence,
                next_steps=(f"Use the paper endpoint or set {ALLOW_LIVE_ALPACA_BROKER_DATA_ENV}=1 only after approving live read-only account visibility.",),
            )
        try:
            probes = client.probe_account_positions()
        except BrokerDataReadOnlyViolation as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_ALPACA_READONLY_POLICY_BLOCKED",
                RuntimeError(type(exc).__name__),
                provider="alpaca_broker_data",
                error_type=type(exc).__name__,
            )
            return self._connection_fail(
                "policy_blocked",
                "alpaca_broker_data_readonly_policy_blocked",
                evidence=evidence,
                next_steps=("Review the Alpaca broker-data read-only policy before testing again.",),
            )
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_ALPACA_READONLY_PROBE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="alpaca_broker_data",
                error_type=type(exc).__name__,
            )
            return self._request_exception_result("alpaca_broker_data", self._safe_endpoint(base_url), exc)
        evidence["probed_paths"] = [str(probe.path) for probe in probes]
        for probe in probes:
            safe_endpoint = self._safe_endpoint(probe.url)
            problem = self._http_problem_result(probe.response, provider="alpaca_broker_data", endpoint=safe_endpoint)
            if problem is not None:
                return problem
            try:
                payload = probe.response.json()
            except Exception as exc:
                _warn_nonfatal(
                    "DATA_SOURCE_CONNECTION_ALPACA_JSON_DECODE_FAILED",
                    RuntimeError(type(exc).__name__),
                    provider="alpaca_broker_data",
                    endpoint=safe_endpoint,
                    surface=probe.surface,
                    error_type=type(exc).__name__,
                )
                return self._connection_fail(
                    "empty_payload",
                    "alpaca_broker_data_invalid_json",
                    evidence={**evidence, "endpoint": safe_endpoint, "surface": probe.surface, "error_type": type(exc).__name__},
                    next_steps=("Alpaca responded but did not return JSON for a read-only broker-data surface.",),
                )
            if bool(probe.require_payload) and self._payload_count(payload) <= 0:
                return self._connection_fail(
                    "empty_payload",
                    "alpaca_broker_data_empty_payload",
                    evidence={**evidence, "endpoint": safe_endpoint, "surface": probe.surface, "payload_count": 0},
                    next_steps=("Alpaca authenticated but the read-only account payload was empty. Verify account access and endpoint selection.",),
                )
        return self._connection_pass("alpaca_broker_data_readonly_connection_ok", **evidence)

    def _test_cftc_cot_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        domain = self._connection_setting(source, "public_reporting_domain", "CFTC_PUBLIC_REPORTING_DOMAIN", "publicreporting.cftc.gov").strip("/")
        dataset_id = self._connection_setting(source, "legacy_dataset_id", "CFTC_COT_LEGACY_DATASET_ID", "6dca-aqww")
        return self._http_json_probe(
            source,
            provider="cftc_cot",
            url=f"https://{domain}/resource/{dataset_id}.json",
            params={"$limit": 1},
            timeout_s=float(self._connection_setting(source, "request_timeout_s", "CFTC_COT_REQUEST_TIMEOUT_S", 20.0)),
            success_message="cftc_cot_connection_ok",
            empty_message="cftc_cot_empty_payload",
        )

    def _test_finra_short_volume_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        from engine.data import finra_short

        template = self._connection_setting(
            source,
            "url_template",
            "FINRA_SHORT_VOLUME_URL_TEMPLATE",
            "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt",
        )
        test_day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
        url = template.format(date=test_day, yyyymmdd=test_day, YYYYMMDD=test_day)
        return self._http_text_probe(
            source,
            provider="finra_short_volume",
            url=url,
            headers={
                "User-Agent": self._provider_user_agent(source, field="user_agent", env_name="FINRA_HTTP_UA", default="trading-system/1.0 finra-short-feed"),
                "Accept": "text/plain,*/*",
            },
            timeout_s=float(self._connection_setting(source, "request_timeout_s", "FINRA_REQUEST_TIMEOUT_S", 20.0)),
            success_message="finra_short_volume_connection_ok",
            empty_message="finra_short_volume_empty_payload",
            validator=lambda text: len(finra_short.parse_short_volume_file(str(text), source_url=url, ingested_ts_ms=1)) > 0,
            invalid_message="finra_short_volume_malformed_payload",
        )

    def _test_finra_short_interest_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        from engine.data import finra_short

        api_url = self._connection_setting(
            source,
            "api_url",
            "FINRA_SHORT_INTEREST_API_URL",
            "https://api.finra.org/data/group/otcMarket/name/EquityShortInterest",
        )
        return self._http_json_probe(
            source,
            provider="finra_short_interest",
            url=api_url,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": self._provider_user_agent(source, field="user_agent", env_name="FINRA_HTTP_UA", default="trading-system/1.0 finra-short-feed"),
            },
            body=json.dumps({"limit": 1, "offset": 0}, separators=(",", ":"), sort_keys=True),
            timeout_s=float(self._connection_setting(source, "request_timeout_s", "FINRA_REQUEST_TIMEOUT_S", 20.0)),
            expected_paths=("", "data", "records", "items", "results"),
            validator=lambda payload: isinstance(payload, (dict, list))
            and all(isinstance(record, dict) for record in finra_short._extract_records(payload)),
            success_message="finra_short_interest_connection_ok",
            empty_message="finra_short_interest_empty_payload",
            invalid_message="finra_short_interest_malformed_payload",
        )

    def _test_crypto_funding_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        try:
            import ccxt  # type: ignore
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_DEPENDENCY_IMPORT_FAILED",
                RuntimeError(type(exc).__name__),
                once_key="connection_dependency:crypto_funding:ccxt",
                provider="crypto_funding",
                dependency="ccxt",
                error_type=type(exc).__name__,
            )
            return self._dependency_missing_result("crypto_funding", "ccxt", exc)
        exchange_id = self._connection_setting(source, "funding_exchange_id", "CCXT_FUNDING_EXCHANGE_ID", "binanceusdm")
        market_map_raw = self._connection_setting(source, "perp_markets", "CRYPTO_PERP_MARKETS", "")
        symbol = ""
        if market_map_raw:
            try:
                parsed = json.loads(market_map_raw)
                if isinstance(parsed, dict):
                    symbol = str(next(iter(parsed.values())) or "").strip()
            except Exception:
                symbol = str(market_map_raw).split(",", 1)[0].split("=", 1)[-1].strip()
        try:
            exchange_cls = getattr(ccxt, exchange_id)
            exchange = exchange_cls({"enableRateLimit": True})
            markets = exchange.load_markets()
            if not symbol and isinstance(markets, dict):
                symbol = next((str(item) for item in markets if "/USDT" in str(item)), str(next(iter(markets), "")))
            if not symbol:
                return self._connection_fail(
                    "empty_payload",
                    "crypto_funding_market_missing",
                    evidence={"provider": "crypto_funding", "exchange_id": exchange_id, "payload_count": 0},
                    next_steps=("Configure CRYPTO_PERP_MARKETS or choose an exchange with visible perpetual markets.",),
                )
            if not getattr(exchange, "has", {}).get("fetchFundingRate"):
                return self._connection_fail(
                    "entitlement_missing",
                    "crypto_funding_endpoint_unavailable",
                    evidence={"provider": "crypto_funding", "exchange_id": exchange_id, "symbol": symbol},
                    next_steps=("Choose a CCXT exchange that exposes public fetchFundingRate support for the configured symbol.",),
                )
            payload = exchange.fetch_funding_rate(symbol)
        except AttributeError as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_CRYPTO_FUNDING_EXCHANGE_UNKNOWN",
                RuntimeError(type(exc).__name__),
                provider="crypto_funding",
                exchange_id=exchange_id,
                error_type=type(exc).__name__,
            )
            return self._connection_fail(
                "provider_unreachable",
                "crypto_funding_exchange_unknown",
                evidence={"provider": "crypto_funding", "exchange_id": exchange_id},
                next_steps=("Set CCXT_FUNDING_EXCHANGE_ID to a supported ccxt exchange id.",),
            )
        except Exception as exc:
            _warn_nonfatal(
                "DATA_SOURCE_CONNECTION_CRYPTO_FUNDING_PROBE_FAILED",
                RuntimeError(type(exc).__name__),
                provider="crypto_funding",
                exchange_id=exchange_id,
                error_type=type(exc).__name__,
            )
            return self._request_exception_result("crypto_funding", exchange_id, exc)
        if not payload:
            return self._connection_fail(
                "empty_payload",
                "crypto_funding_empty_payload",
                evidence={"provider": "crypto_funding", "exchange_id": exchange_id, "symbol": symbol, "payload_count": 0},
                next_steps=("The exchange responded but returned no funding-rate payload. Verify market support and retry later.",),
            )
        return self._connection_pass("crypto_funding_connection_ok", provider="crypto_funding", exchange_id=exchange_id, symbol=symbol, payload_count=1)

    def _test_congressional_trades_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        url = self._connection_setting(
            source,
            "source_url",
            "CONGRESSIONAL_TRADES_URL",
            "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json",
        )
        return self._http_json_probe(
            source,
            provider="congressional_trades",
            url=url,
            headers={
                "User-Agent": self._provider_user_agent(source, field="user_agent", env_name="CONGRESSIONAL_HTTP_UA", default="trading-system/1.0 congressional-feed"),
                "Accept": "application/json",
            },
            expected_paths=("", "results", "data", "items", "transactions"),
            validator=lambda payload: isinstance(payload, (dict, list)),
            success_message="congressional_trades_connection_ok",
            empty_message="congressional_trades_empty_payload",
            invalid_message="congressional_trades_malformed_payload",
        )

    def _test_quiver_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        api_key = self._connection_effective_env_value(source, "QUIVER_API_KEY")
        if not api_key:
            return self._missing_credentials_result("quiver_gov", "QUIVER_API_KEY", source=source)
        base_url = self._connection_setting(source, "base_url", "QUIVER_BASE_URL", "https://api.quiverquant.com")
        endpoint = self._connection_setting(source, "congress_endpoint", "QUIVER_CONGRESS_ENDPOINT", "/beta/live/congresstrading")
        auth_scheme = self._connection_setting(source, "auth_scheme", "QUIVER_AUTH_SCHEME", "Bearer")
        return self._http_json_probe(
            source,
            provider="quiver_gov",
            url=urljoin(base_url.rstrip("/") + "/", endpoint.lstrip("/")),
            headers={"Authorization": f"{auth_scheme} {api_key}", "Accept": "application/json"},
            success_message="quiver_connection_ok",
            empty_message="quiver_empty_payload",
        )

    def _test_fundamentals_pit_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        simfin_key = self._connection_effective_env_value(source, "SIMFIN_API_KEY")
        sharadar_key = self._connection_effective_env_value(source, "SHARADAR_API_KEY")
        mode = self._connection_setting(source, "mode", "FUNDAMENTALS_PIT_MODE", "auto").lower()
        wants_simfin = mode in {"auto", "simfin", "both"}
        wants_sharadar = mode in {"auto", "sharadar", "both"}
        if wants_simfin and not simfin_key and wants_sharadar and not sharadar_key:
            return self._missing_credentials_result("fundamentals_pit", "SIMFIN_API_KEY", "SHARADAR_API_KEY", source=source)
        results: Dict[str, str] = {}
        failures: List[ConnectionTestResult] = []
        if wants_simfin:
            if simfin_key:
                simfin = self._http_json_probe(
                    source,
                    provider="simfin",
                    url=self._connection_setting(source, "simfin_bulk_url", "SIMFIN_TEST_URL", "https://simfin.com/api/v2/companies/list"),
                    params={"api-key": simfin_key},
                    expected_paths=("", "data"),
                    success_message="simfin_connection_ok",
                    empty_message="simfin_empty_payload",
                    apply_rate_limit=False,
                )
                if self._must_stop_probe(simfin):
                    return simfin
                results["simfin"] = simfin.status
                if not simfin.ok:
                    failures.append(simfin)
            else:
                results["simfin"] = "missing_credentials"
        if wants_sharadar:
            if sharadar_key:
                sharadar = self._http_json_probe(
                    source,
                    provider="sharadar",
                    url=self._connection_setting(source, "sharadar_bulk_url", "SHARADAR_TEST_URL", "https://data.nasdaq.com/api/v3/datatables/SHARADAR/SF1"),
                    params={"ticker": "AAPL", "qopts.per_page": "1", "api_key": sharadar_key},
                    expected_paths=("datatable.data", "data"),
                    success_message="sharadar_connection_ok",
                    empty_message="sharadar_empty_payload",
                    apply_rate_limit=False,
                )
                if self._must_stop_probe(sharadar):
                    return sharadar
                results["sharadar"] = sharadar.status
                if not sharadar.ok:
                    failures.append(sharadar)
            else:
                results["sharadar"] = "missing_credentials"
        expected = [name for name in ("simfin", "sharadar") if name in results]
        if expected and all(results.get(name) == "pass" for name in expected):
            return self._connection_pass("fundamentals_pit_connection_ok", provider="fundamentals_pit", component_statuses=results, mode=mode)
        if "pass" in results.values():
            return self._connection_degraded(
                "partial_success",
                "fundamentals_pit_partial_provider_success",
                evidence={"provider": "fundamentals_pit", "component_statuses": results, "mode": mode},
                next_steps=("Configure and validate every fundamentals vendor selected by FUNDAMENTALS_PIT_MODE before treating the source as healthy.",),
            )
        return failures[0] if failures else self._missing_credentials_result("fundamentals_pit", "SIMFIN_API_KEY", "SHARADAR_API_KEY", source=source)

    def _test_etf_flows_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
        if limited is not None:
            return limited
        polygon_key = self._connection_effective_env_value(source, "POLYGON_API_KEY")
        fmp_key = self._connection_effective_env_value(source, "FMP_API_KEY")
        if not polygon_key and not fmp_key:
            return self._missing_credentials_result("etf_flows", "POLYGON_API_KEY", "FMP_API_KEY", source=source)
        results: Dict[str, str] = {}
        failures: List[ConnectionTestResult] = []
        if polygon_key:
            polygon = self._http_json_probe(
                source,
                provider="etf_flows_polygon",
                url="https://api.polygon.io/v3/reference/tickers/SPY",
                params={"apiKey": polygon_key},
                expected_paths=("results",),
                success_message="etf_flows_polygon_connection_ok",
                empty_message="etf_flows_polygon_empty_payload",
                apply_rate_limit=False,
            )
            if self._must_stop_probe(polygon):
                return polygon
            results["polygon"] = polygon.status
            if not polygon.ok:
                failures.append(polygon)
        else:
            results["polygon"] = "missing_credentials"
        if fmp_key:
            fmp = self._http_json_probe(
                source,
                provider="etf_flows_fmp",
                url="https://financialmodelingprep.com/api/v3/profile/SPY",
                params={"apikey": fmp_key},
                expected_paths=("",),
                success_message="etf_flows_fmp_connection_ok",
                empty_message="etf_flows_fmp_empty_payload",
                apply_rate_limit=False,
            )
            if self._must_stop_probe(fmp):
                return fmp
            results["fmp"] = fmp.status
            if not fmp.ok:
                failures.append(fmp)
        else:
            results["fmp"] = "missing_credentials"
        if results.get("polygon") == "pass" and results.get("fmp") == "pass":
            return self._connection_pass("etf_flows_connection_ok", provider="etf_flows", component_statuses=results)
        if "pass" in results.values():
            return self._connection_degraded(
                "partial_success",
                "etf_flows_partial_provider_success",
                evidence={"provider": "etf_flows", "component_statuses": results},
                next_steps=("Configure and validate both Polygon and FMP access before treating ETF-flow tests as fully healthy.",),
            )
        return failures[0] if failures else self._missing_credentials_result("etf_flows", "POLYGON_API_KEY", "FMP_API_KEY", source=source)

    def _test_macro_fred_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        fred_key = self._connection_effective_env_value(source, "FRED_API_KEY")
        if not fred_key:
            limited = self._connection_probe_rate_limit(str(source.get("source_key") or ""))
            if limited is not None:
                return limited
            fallback = self._http_text_probe(
                source,
                provider="alfred",
                url="https://alfred.stlouisfed.org/series/downloaddata",
                params={"seid": "CPIAUCSL"},
                success_message="alfred_fallback_connection_ok",
                empty_message="alfred_fallback_empty_payload",
                validator=lambda text: "DATE" in text[:200].upper() or "," in text[:200],
                apply_rate_limit=False,
            )
            if self._must_stop_probe(fallback):
                return fallback
            if fallback.ok:
                return self._connection_degraded(
                    "degraded_fallback",
                    "fred_api_key_missing_alfred_fallback_used",
                    evidence={
                        "provider": "macro",
                        "primary_provider": "fred",
                        "fallback_provider": "alfred",
                        "primary_missing": "FRED_API_KEY",
                        "fred_api_key_missing": True,
                        "alfred_fallback_used": True,
                    },
                    next_steps=("Configure FRED_API_KEY for the primary macro API; ALFRED CSV fallback was reachable and is being reported explicitly as degraded fallback mode.",),
                )
            return self._connection_degraded(
                "missing_credentials",
                "fred_api_key_missing_alfred_fallback_unverified",
                evidence={"provider": "fred", "missing_env_vars": ["FRED_API_KEY"]},
                next_steps=("Configure FRED_API_KEY for the primary macro API, then test again.",),
            )
        return self._http_json_probe(
            source,
            provider="fred",
            url="https://api.stlouisfed.org/fred/series/observations",
            params={"series_id": "CPIAUCSL", "api_key": fred_key, "file_type": "json", "limit": 1, "sort_order": "desc"},
            expected_paths=("observations",),
            validator=lambda payload: isinstance(payload, dict) and isinstance(payload.get("observations"), list),
            success_message="fred_connection_ok",
            empty_message="fred_empty_payload",
            invalid_message="fred_malformed_payload",
        )

    def _test_news_flow_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        backend = self._connection_setting(source, "embedding_backend", "NEWS_EMBED_BACKEND", "hashing").lower()
        if backend != "openai":
            return self._connection_unsupported(
                "news_flow_embedding_backend_not_external",
                provider="news_flow",
                embedding_backend=backend,
            )
        api_key = self._connection_effective_env_value(source, "OPENAI_API_KEY")
        if not api_key:
            return self._missing_credentials_result("news_flow", "OPENAI_API_KEY", source=source)
        model = self._connection_setting(source, "embedding_model", "NEWS_EMBED_OPENAI_MODEL", "text-embedding-3-small")
        return self._http_json_probe(
            source,
            provider="news_flow_openai_embeddings",
            url="https://api.openai.com/v1/embeddings",
            method="POST",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            body=json.dumps({"model": model, "input": "connection probe"}, separators=(",", ":")),
            expected_paths=("data",),
            success_message="news_flow_openai_embeddings_connection_ok",
            empty_message="news_flow_openai_embeddings_empty_payload",
        )

    def _test_weather_forecasts_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        return self._http_json_probe(
            source,
            provider="weather_forecasts",
            url="https://api.open-meteo.com/v1/forecast",
            params={"latitude": 43.6532, "longitude": -79.3832, "daily": "temperature_2m_max", "timezone": "UTC"},
            expected_paths=("daily.time", "daily"),
            success_message="weather_forecast_connection_ok",
            empty_message="weather_forecast_empty_payload",
        )

    def _test_weather_alerts_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        return self._http_json_probe(
            source,
            provider="weather_alerts",
            url="https://api.weather.gov/alerts/active",
            params={"area": "CA"},
            headers={
                "User-Agent": self._connection_setting(source, "http_ua", "WEATHER_HTTP_UA", "trading-system/1.0"),
                "Accept": "application/geo+json",
            },
            expected_paths=("type", "features"),
            validator=lambda payload: isinstance(payload, dict)
            and str(payload.get("type") or "") == "FeatureCollection"
            and isinstance(payload.get("features"), list),
            success_message="weather_alerts_connection_ok",
            empty_message="weather_alerts_empty_payload",
            invalid_message="weather_alerts_malformed_payload",
        )

    def _test_rss_connection(self, source: Dict[str, Any]) -> ConnectionTestResult:
        url = self._connection_setting(source, "url", "", "")
        if not url:
            return self._connection_fail(
                "missing_credentials",
                "rss_feed_url_missing",
                evidence={"provider": "rss_feed", "missing_fields": ["url"]},
                next_steps=("Enter an RSS or Atom feed URL, save, then test again.",),
            )
        return self._http_text_probe(
            source,
            provider="rss_feed",
            url=url,
            headers={
                "User-Agent": self._provider_user_agent(source, field="user_agent", env_name="RSS_HTTP_UA", default="trading-system/1.0 rss-feed"),
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
            },
            success_message="rss_feed_connection_ok",
            empty_message="rss_feed_empty_payload",
            validator=self._rss_payload_looks_valid,
            invalid_message="rss_feed_malformed_payload",
        )

    def test_connection(self, source_key: str, *, actor: str = "operator", client_ip: str = "") -> Dict[str, Any]:
        """Run the registered provider-specific connectivity test for a source."""
        self.initialize()
        self._clear_data_credential_cache()
        source = self.get_source(source_key, include_credentials=True)
        if source is None:
            raise ValueError("source_not_found")
        key = str(source.get("source_key") or "")
        registry_key = key if key in _PROVIDER_TEST_REGISTRY else str(source.get("source_type") or "")
        spec = dict(_PROVIDER_TEST_REGISTRY.get(registry_key) or {})
        now_ms = int(time.time() * 1000)
        unsupported_reason = str(spec.get("unsupported_reason") or "")
        if not spec:
            result = self._connection_unsupported(
                "connection_test_not_registered",
                provider=str(source.get("provider_name") or registry_key),
                source_type=str(source.get("source_type") or ""),
            )
        elif unsupported_reason:
            result = self._connection_unsupported(unsupported_reason, provider=str(source.get("provider_name") or registry_key))
        else:
            handler_name = str(spec.get("handler") or "")
            handler = getattr(self, handler_name, None)
            if not callable(handler):
                result = self._connection_unsupported(
                    "source_test_handler_missing",
                    provider=str(source.get("provider_name") or registry_key),
                    handler=handler_name,
                )
            else:
                try:
                    result = handler(source)
                except Exception as exc:
                    result = self._connection_fail(
                        "provider_unreachable",
                        "connection_test_probe_failed",
                        evidence={"source_key": key, "error_type": type(exc).__name__},
                        next_steps=("Inspect provider settings and runtime logs, then test again.",),
                    )
        payload = result.payload(source_key=key)
        payload["provider_name"] = str(source.get("provider_name") or "")
        payload["source_type"] = str(source.get("source_type") or "")
        payload["test_registry_key"] = str(registry_key or "")
        payload["ts_ms"] = int(now_ms)
        event_level = "INFO" if result.ok else ("WARNING" if str(result.status) in {"degraded", "unsupported"} else "ERROR")
        self.log_event(
            key,
            event_type="test_connection",
            message=str(result.message),
            detail={"actor": self._normalize_actor(actor), **payload},
            level=event_level,
            ts_ms=now_ms,
        )
        self.audit_action(
            key,
            action="test_connection",
            actor=actor,
            success=bool(result.ok),
            message=str(result.message),
            detail=payload,
            client_ip=client_ip,
            source_type=str(source.get("source_type") or ""),
            provider_name=str(source.get("provider_name") or ""),
            job_name=str(source.get("job_name") or ""),
            ts_ms=now_ms,
        )

        def _txn(con) -> None:
            con.execute(
                """
                UPDATE data_sources
                   SET status = ?,
                       last_test_ts_ms = ?,
                       last_success_ts_ms = CASE WHEN ? = 1 THEN ? ELSE last_success_ts_ms END,
                       updated_ts_ms = ?,
                       last_error = ?,
                       error_count = CASE
                         WHEN ? = 1 THEN 0
                         WHEN ? = 1 THEN COALESCE(error_count,0) + 1
                         ELSE COALESCE(error_count,0)
                       END
                 WHERE source_key = ?
                """,
                (
                    {
                        "pass": "tested",
                        "fail": "test_failed",
                        "degraded": "test_degraded",
                        "unsupported": "test_unsupported",
                    }.get(str(result.status), "test_failed"),
                    int(now_ms),
                    1 if result.ok else 0,
                    int(now_ms),
                    int(now_ms),
                    None if result.ok else f"{result.classification}:{result.message}"[:1000],
                    1 if result.ok else 0,
                    1 if str(result.status) == "fail" else 0,
                    key,
                ),
            )

        run_write_txn(_txn)
        self._clear_data_credential_cache()
        return payload

    def test_and_save_source(
        self,
        payload: Dict[str, Any],
        *,
        actor: str = "operator",
        client_ip: str = "",
        create_only: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Save a source, clear credential cache, then run the real liveness probe."""
        body = dict(payload or {})
        body["actor"] = self._normalize_actor(actor or body.get("actor"))
        body["client_ip"] = str(client_ip or body.get("client_ip") or "").strip()[:120]
        requested_create = body.pop("create", body.pop("create_only", None))
        if create_only is None and requested_create is not None:
            create_only = bool(requested_create)
        source_key = self._normalize_source_key(
            body.get("source_key")
            or body.get("provider_name")
            or body.get("display_name")
            or body.get("source_type")
        )
        if create_only is None:
            create_only = self.get_source(source_key) is None
        saved = self.create_source(body) if bool(create_only) else self.update_source(body)
        self._clear_data_credential_cache()
        test_result = self.test_connection(
            str(saved.get("source_key") or source_key),
            actor=body["actor"],
            client_ip=body["client_ip"],
        )
        fresh = self.get_source(str(saved.get("source_key") or source_key)) or saved
        return {
            "ok": bool(test_result.get("ok")),
            "saved": True,
            "source_key": str(saved.get("source_key") or source_key),
            "source": fresh,
            "test": test_result,
            "message": str(test_result.get("message") or test_result.get("error") or ""),
            **({"error": str(test_result.get("error") or "")} if not bool(test_result.get("ok")) else {}),
        }

    def _provider_chain(self, provider_names: Iterable[str]) -> List[str]:
        requested = {str(name or "").strip().lower() for name in provider_names if str(name or "").strip()}
        order = ["polygon_ws", "ibkr", "polygon", "tradier", "yfinance", "simulated", "oanda", "ccxt"]
        out: List[str] = []
        for name in order:
            if name in requested:
                out.append(name)
        for name in sorted(requested):
            if name not in out:
                out.append(name)
        return out

    def _normalize_source_key(self, value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        out = []
        for ch in raw:
            if ch.isalnum() or ch in ("_", "-", ":"):
                out.append(ch)
            elif ch in (" ", "/", ".", "|"):
                out.append("_")
        text = "".join(out).strip("_")
        while "__" in text:
            text = text.replace("__", "_")
        return text[:120]

    def _config_hash(self, payload: Dict[str, Any]) -> str:
        return hashlib.sha256(self._json_dumps(payload).encode("utf-8", "ignore")).hexdigest()

    def _json_dumps(self, payload: Any) -> str:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)

    def _json_loads(self, payload: Any, default: Any) -> Any:
        try:
            if payload is None:
                return default
            if isinstance(payload, (dict, list)):
                return payload
            return json.loads(str(payload))
        except Exception as e:
            _warn_nonfatal(
                "DATA_SOURCE_MANAGER_JSON_LOADS_FAILED",
                e,
                once_key="json_loads",
                payload_type=type(payload).__name__,
            )
            return default

    def _env_string(self, value: Any) -> str:
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (dict, list)):
            return self._json_dumps(value)
        return str(value)


_MANAGER = DataSourceManager()


def get_manager() -> DataSourceManager:
    """Return the process-wide singleton data source manager."""
    return _MANAGER


def load_sources_from_db(*, include_credentials: bool = False) -> List[Dict[str, Any]]:
    """Load materialized source rows from the manager-backed control plane.

    Parameters
    ----------
    include_credentials : bool, default=False
        Whether to include decrypted credentials in each returned source row.

    Returns
    -------
    list of dict
        Source payloads in the same shape as :meth:`DataSourceManager.list_sources`.
    """
    return get_manager().list_sources(include_credentials=include_credentials)


def inject_provider_registry() -> Dict[str, Dict[str, Any]]:
    """Return provider-registry overrides derived from the current source set."""
    return get_manager().inject_into_provider_registry()


def desired_ingestion_jobs(
    default_jobs: Optional[Iterable[str]] = None,
    *,
    read_only: bool = False,
    project_credentials: bool = True,
) -> List[str]:
    """Return the ingestion jobs implied by current enabled source settings.

    Parameters
    ----------
    default_jobs : iterable of str, optional
        Baseline job names provided by the caller.

    Returns
    -------
    list of str
        Ordered job list with any required source-driven jobs added.
    """
    return get_manager().get_desired_ingestion_jobs(
        default_jobs=default_jobs,
        read_only=read_only,
        project_credentials=project_credentials,
    )
