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
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.data_source_log_store import (
    append_data_source_log_row,
    delete_data_source_logs_for_source,
    ensure_data_source_logs_schema,
    log_data_source_event,
)
from engine.runtime.runtime_meta import meta_get, meta_set
from engine.runtime.startup_write_gate import should_defer_noncritical_startup_write
from engine.runtime.storage import connect_ro, run_write_txn
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
    """
    source_type: str
    display_name: str
    job_name: str
    provider_name: str = ""
    singleton: bool = True
    default_enabled: bool = True
    credential_env: Dict[str, str] = field(default_factory=dict)
    setting_env: Dict[str, str] = field(default_factory=dict)


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
        ),
        "polygon": SourceDefinition(
            source_type="price_provider",
            display_name="Polygon REST",
            provider_name="polygon",
            job_name="poll_prices",
            default_enabled=True,
            credential_env={"api_key": "POLYGON_API_KEY"},
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
        ),
        "yfinance": SourceDefinition(
            source_type="price_provider",
            display_name="Yahoo Finance",
            provider_name="yfinance",
            job_name="poll_prices",
            default_enabled=True,
        ),
        "ccxt": SourceDefinition(
            source_type="price_provider",
            display_name="CCXT",
            provider_name="ccxt",
            job_name="poll_prices",
            default_enabled=True,
            setting_env={"exchange_id": "CCXT_EXCHANGE_ID"},
        ),
        "tradier": SourceDefinition(
            source_type="options_provider",
            display_name="Tradier Options",
            provider_name="tradier",
            job_name="options_poll",
            default_enabled=True,
            credential_env={"api_token": "TRADIER_API_TOKEN"},
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
        ),
        "transcripts": SourceDefinition(
            source_type="news_provider",
            display_name="FMP Transcripts",
            provider_name="transcripts",
            job_name="ingest_now",
            default_enabled=True,
            credential_env={"api_key": "FMP_API_KEY"},
            setting_env={"max_items_per_symbol": "TRANSCRIPTS_MAX_ITEMS_PER_SYMBOL"},
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
        ),
        "earnings": SourceDefinition(
            source_type="calendar_provider",
            display_name="FMP Earnings",
            provider_name="earnings",
            job_name="poll_earnings",
            credential_env={"api_key": "FMP_API_KEY"},
            setting_env={"lookahead_days": "EARNINGS_LOOKAHEAD_DAYS"},
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
        ),
        "rss_feed": SourceDefinition(
            source_type="rss_feed",
            display_name="RSS Feed",
            provider_name="rss",
            job_name="ingest_now",
            singleton=False,
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
    "ingest_congressional_trades",
    "poll_earnings",
    "poll_social_reddit",
    "poll_social_stocktwits",
    "poll_weather_forecasts",
    "poll_weather_alerts",
    "poll_macro",
    "snapshot_model_features",
}


CUSTOM_RSS_TEMPLATE_KEY = "rss_feed"


def _humanize_field_name(name: str) -> str:
    text = str(name or "").strip().replace("_", " ")
    return " ".join(part.capitalize() for part in text.split() if part)


class DataSourceManager:
    """Own the data-source control plane and runtime configuration projection.

    The manager exposes operator-facing CRUD, testing, logging, and lifecycle
    flows, encrypts provider credentials at rest, and projects enabled sources
    into runtime job and provider configuration.
    """

    def __init__(self) -> None:
        self._catalog = _default_catalog()
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
            fields.append(
                {
                    "field": str(field_name),
                    "label": _humanize_field_name(field_name),
                    "env_name": str(env_name),
                    "secret": True,
                }
            )
        return fields

    def _setting_schema_fields(self, template_key: str, definition: Optional[SourceDefinition]) -> List[Dict[str, Any]]:
        if template_key == CUSTOM_RSS_TEMPLATE_KEY:
            return [
                {"field": "name", "label": "Feed Name", "required": True, "type": "text"},
                {"field": "url", "label": "Feed URL", "required": True, "type": "url"},
            ]
        fields: List[Dict[str, Any]] = []
        if definition is None:
            return fields
        for field_name, env_name in sorted((definition.setting_env or {}).items()):
            field_type = "number" if field_name.endswith("_s") or field_name.endswith("_seconds") or field_name.endswith("_days") or field_name.endswith("_limit") or field_name.endswith("_port") else "text"
            fields.append(
                {
                    "field": str(field_name),
                    "label": _humanize_field_name(field_name),
                    "env_name": str(env_name),
                    "required": False,
                    "type": field_type,
                }
            )
        return fields

    def _template_payload(self, template_key: str, definition: Optional[SourceDefinition]) -> Dict[str, Any]:
        if definition is None:
            return {}
        is_builtin = bool(template_key in self._catalog and definition.singleton and template_key != CUSTOM_RSS_TEMPLATE_KEY)
        return {
            "template_key": str(template_key),
            "display_name": str(definition.display_name or template_key),
            "source_type": str(definition.source_type or ""),
            "provider_name": str(definition.provider_name or template_key),
            "job_name": str(definition.job_name or ""),
            "singleton": bool(definition.singleton),
            "builtin": bool(is_builtin),
            "allow_create": bool(template_key == CUSTOM_RSS_TEMPLATE_KEY),
            "allow_update": True,
            "allow_delete": bool(template_key == CUSTOM_RSS_TEMPLATE_KEY),
            "supports_test": True,
            "identity_locked": bool(is_builtin),
            "routing_locked": True,
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
        return {
            "provider_telemetry": provider_telemetry,
            "pipeline_health": pipeline_health,
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
        return self._runtime_snapshot()

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
                    self._json_dumps(detail or {}),
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

        run_write_txn(_txn)

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
        is_builtin = bool(template_key and template_key in self._catalog and source_key == template_key and definition and definition.singleton and template_key != CUSTOM_RSS_TEMPLATE_KEY)
        out = {
            "id": int(row.get("id") or 0),
            "source_key": source_key,
            "display_name": str(row.get("display_name") or (definition.display_name if definition else "")),
            "source_type": str(source_type or (definition.source_type if definition else "")),
            "provider_name": str(provider_name or ""),
            "job_name": str(row.get("job_name") or (definition.job_name if definition else "")),
            "enabled": bool(int(row.get("enabled") or 0) == 1),
            "settings": settings if isinstance(settings, dict) else {},
            "status": str(row.get("status") or "unknown"),
            "last_error": str(row.get("last_error") or ""),
            "last_success_ts_ms": int(row.get("last_success_ts_ms") or 0),
            "last_test_ts_ms": int(row.get("last_test_ts_ms") or 0),
            "error_count": int(row.get("error_count") or 0),
            "config_hash": str(row.get("config_hash") or ""),
            "created_ts_ms": int(row.get("created_ts_ms") or 0),
            "updated_ts_ms": int(row.get("updated_ts_ms") or 0),
            "credentials_configured": bool(credentials),
            "credentials_stored": bool(credentials_blob.strip()),
            "key_version": key_version,
            "credential_error": str(credential_error or ""),
            "credential_fields": [str(item.get("field") or "") for item in self._credential_schema_fields(template_key, definition)],
            "setting_fields": [str(item.get("field") or "") for item in self._setting_schema_fields(template_key, definition)],
            "masked_credentials": mask_credentials(credentials),
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
        if body.get("credentials") is not None:
            if not isinstance(body.get("credentials"), dict):
                raise ValueError("credentials_must_be_object")
            if bool(body.get("replace_credentials", False)):
                next_credentials = dict(body.get("credentials") or {})
            else:
                for key, value in dict(body.get("credentials") or {}).items():
                    if str(value or "").strip():
                        next_credentials[str(key)] = str(value)
        for field_name in clear_credential_fields:
            next_credentials.pop(str(field_name), None)

        if definition is not None:
            self._validate_allowed_fields(
                next_credentials,
                allowed=(definition.credential_env or {}).keys(),
                label="credential",
            )
            self._validate_allowed_fields(
                next_settings,
                allowed=["name", "url"] if template_key == CUSTOM_RSS_TEMPLATE_KEY else (definition.setting_env or {}).keys(),
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
        audit_detail = {
            "actor": actor,
            "replace_credentials": bool(body.get("replace_credentials", False)),
            "cleared_credential_fields": clear_credential_fields,
            "template_key": template_key,
            "builtin": bool(source_key in self._catalog and source_key != CUSTOM_RSS_TEMPLATE_KEY),
        }
        self.log_event(
            source_key,
            event_type="upsert",
            message="source saved",
            detail={**audit_detail, **record},
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
            detail={"actor": self._normalize_actor(actor)},
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

    def build_job_environment(self, job_name: str) -> Dict[str, str]:
        self.initialize()
        rows = [
            row
            for row in self.list_sources(include_credentials=True)
            if str(row.get("job_name") or "") == str(job_name or "")
            and bool(row.get("enabled"))
        ]
        env: Dict[str, str] = {}
        price_providers: List[str] = []
        option_providers: List[str] = []

        for row in rows:
            definition = self._catalog.get(str(row.get("source_key") or ""))
            credentials = dict(row.get("credentials") or {})
            settings = dict(row.get("settings") or {})
            provider_name = str(row.get("provider_name") or row.get("source_key") or "").strip().lower()

            if str(row.get("source_type") or "") == "price_provider" and provider_name:
                price_providers.append(provider_name)
            if str(row.get("source_type") or "") == "options_provider" and provider_name:
                option_providers.append(provider_name)

            if definition is None:
                continue
            for field_name, env_name in (definition.credential_env or {}).items():
                value = credentials.get(field_name)
                if value is not None and str(value).strip() != "":
                    env[str(env_name)] = str(value)
            for field_name, env_name in (definition.setting_env or {}).items():
                value = settings.get(field_name)
                if value is not None and str(value).strip() != "":
                    env[str(env_name)] = self._env_string(value)

        if job_name == "options_poll" and any(
            str(row.get("provider_name") or "").strip().lower() == "polygon"
            and bool(row.get("enabled"))
            and str((row.get("credentials") or {}).get("api_key") or "").strip()
            for row in self.list_sources(include_credentials=True)
        ):
            option_providers.append("polygon")

        if job_name == "poll_prices":
            chain = self._provider_chain(price_providers)
            if chain:
                env["LIVE_PRICE_PROVIDER_CHAIN"] = ",".join(chain)
            env["POLYGON_REST_ENABLED"] = "1" if "polygon" in chain else "0"
            env["YFINANCE_ENABLED"] = "1" if "yfinance" in chain else "0"
            env["CCXT_ENABLED"] = "1" if "ccxt" in chain else "0"
        elif job_name == "stream_prices_polygon_ws":
            env["POLYGON_WS_ENABLED"] = "1"
        elif job_name == "stream_prices_ibkr":
            env["IBKR_ENABLED"] = "1"
        elif job_name == "options_poll":
            chain = self._provider_chain(option_providers)
            if chain:
                env["OPTIONS_PROVIDER_CHAIN"] = ",".join(chain)
            env["TRADIER_ENABLED"] = "1" if "tradier" in chain else "0"
        elif job_name == "ingest_now":
            enabled_keys = {str(row.get("source_key") or "") for row in rows}
            env["INGEST_NOW_ENABLE_COMPANY_NEWS"] = "1" if "company_news" in enabled_keys else "0"
            env["INGEST_NOW_ENABLE_TRANSCRIPTS"] = "1" if "transcripts" in enabled_keys else "0"
            env["INGEST_NOW_ENABLE_GDELT"] = "1" if "gdelt" in enabled_keys else "0"

        return env

    def apply_runtime_environment(self) -> Dict[str, str]:
        self.initialize()
        merged: Dict[str, str] = {}
        for row in self.list_sources():
            if not bool(row.get("enabled")):
                continue
            merged.update(self.build_job_environment(str(row.get("job_name") or "")))
        for key, value in merged.items():
            os.environ[str(key)] = str(value)
        return merged

    def get_provider_registry_overrides(self) -> Dict[str, Dict[str, Any]]:
        self.initialize()
        out: Dict[str, Dict[str, Any]] = {}
        for row in self.list_sources():
            if str(row.get("source_type") or "") not in ("price_provider", "options_provider"):
                continue
            provider_name = str(row.get("provider_name") or "").strip().lower()
            if not provider_name:
                continue
            out[provider_name] = {
                "enabled": bool(row.get("enabled")),
                "source_key": str(row.get("source_key") or provider_name),
                "job_name": str(row.get("job_name") or ""),
                "config_hash": str(row.get("config_hash") or ""),
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
        defaults = [str(name) for name in (default_jobs or []) if str(name).strip()]
        unmanaged = [name for name in defaults if name not in MANAGED_DAEMON_JOBS]
        sources = self.list_sources(include_credentials=True)
        if not sources:
            return list(dict.fromkeys(defaults))

        enabled_rows = [row for row in sources if bool(row.get("enabled"))]
        enabled_jobs = {str(row.get("job_name") or "") for row in enabled_rows if str(row.get("job_name") or "").strip()}

        if any(str(row.get("source_type") or "") == "options_provider" for row in enabled_rows):
            enabled_jobs.add("options_poll")
        elif any(
            str(row.get("provider_name") or "").strip().lower() == "polygon"
            and str((row.get("credentials") or {}).get("api_key") or "").strip()
            for row in enabled_rows
        ):
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
        for row in self.list_sources(include_credentials=True):
            if str(row.get("job_name") or "") != str(job_name or ""):
                continue
            relevant.append(
                {
                    "source_key": str(row.get("source_key") or ""),
                    "enabled": bool(row.get("enabled")),
                    "provider_name": str(row.get("provider_name") or ""),
                    "settings": dict(row.get("settings") or {}),
                    "credentials": dict(row.get("credentials") or {}),
                }
            )
        relevant.sort(key=lambda item: str(item.get("source_key") or ""))
        return self._config_hash({"job_name": str(job_name or ""), "sources": relevant})

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
        detail_json = self._json_dumps(dict(meta or {}))
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
                detail_json=self._json_dumps(detail or {}),
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
        return {
            "ok": True,
            "reason": str(reason or "data_sources_changed"),
            "desired_jobs": desired_jobs,
            "ingestion_runtime_started": bool(started),
        }

    def mark_runtime_dirty(self, *, reason: str = "") -> None:
        now_ms = int(time.time() * 1000)
        payload = {
            "ts_ms": int(now_ms),
            "reason": str(reason or "data_sources_changed"),
            "host": socket.gethostname(),
        }
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

    def test_connection(self, source_key: str, *, actor: str = "operator", client_ip: str = "") -> Dict[str, Any]:
        """Run the provider-specific connectivity test for a source.

        Parameters
        ----------
        source_key : str
            Source identifier to test.
        actor : str, default="operator"
            Actor name recorded in audit/log rows.
        client_ip : str, default=""
            Client IP recorded in audit trails.

        Returns
        -------
        dict
            Provider-specific success or failure payload. Successful results
            include ``ok=True`` and update the stored source status.

        Raises
        ------
        ValueError
            If the source does not exist.

        Side Effects
        ------------
        Writes source log/audit records and updates ``status``,
        ``last_test_ts_ms``, and ``last_error`` in the source row.
        """
        source = self.get_source(source_key, include_credentials=True)
        if source is None:
            raise ValueError(f"source_not_found:{source_key}")

        provider_name = str(source.get("provider_name") or source.get("source_key") or "").strip().lower()
        credentials = dict(source.get("credentials") or {})
        settings = dict(source.get("settings") or {})
        now_ms = int(time.time() * 1000)

        def _ok(message: str, **extra) -> Dict[str, Any]:
            self.log_event(
                str(source.get("source_key") or ""),
                event_type="test_connection",
                message=message,
                detail={"ok": True, "actor": self._normalize_actor(actor), **extra},
                level="INFO",
            )
            self.audit_action(
                str(source.get("source_key") or ""),
                action="test_connection",
                actor=actor,
                success=True,
                message=message,
                detail=extra,
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
                    SET status = ?, last_test_ts_ms = ?, updated_ts_ms = ?, last_error = NULL
                    WHERE source_key = ?
                    """,
                    ("tested", int(now_ms), int(now_ms), str(source.get("source_key") or "")),
                )

            run_write_txn(_txn)
            return {"ok": True, "source_key": str(source.get("source_key") or ""), "message": message, **extra}

        def _fail(message: str, **extra) -> Dict[str, Any]:
            self.log_event(
                str(source.get("source_key") or ""),
                event_type="test_connection",
                message=message,
                detail={"ok": False, "actor": self._normalize_actor(actor), **extra},
                level="ERROR",
            )
            self.audit_action(
                str(source.get("source_key") or ""),
                action="test_connection",
                actor=actor,
                success=False,
                message=message,
                detail=extra,
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
                    SET status = ?, last_test_ts_ms = ?, updated_ts_ms = ?, last_error = ?
                    WHERE source_key = ?
                    """,
                    ("test_failed", int(now_ms), int(now_ms), str(message)[:1000], str(source.get("source_key") or "")),
                )

            run_write_txn(_txn)
            return {"ok": False, "source_key": str(source.get("source_key") or ""), "error": message, **extra}

        try:
            if provider_name in ("polygon", "polygon_ws"):
                api_key = str(credentials.get("api_key") or "").strip()
                if not api_key:
                    return _fail("polygon_api_key_missing")
                response = requests.get(
                    "https://api.polygon.io/v3/reference/tickers",
                    params={"market": "stocks", "limit": 1, "apiKey": api_key},
                    timeout=10,
                )
                response.raise_for_status()
                return _ok("polygon_connection_ok", status_code=int(response.status_code))

            if provider_name == "tradier":
                token = str(credentials.get("api_token") or "").strip()
                if not token:
                    return _fail("tradier_api_token_missing")
                response = requests.get(
                    "https://api.tradier.com/v1/markets/options/expirations",
                    params={"symbol": "SPY"},
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=10,
                )
                response.raise_for_status()
                return _ok("tradier_connection_ok", status_code=int(response.status_code))

            if provider_name == "company_news":
                api_key = str(credentials.get("api_key") or "").strip()
                if not api_key:
                    return _fail("finnhub_api_key_missing")
                response = requests.get(
                    "https://finnhub.io/api/v1/quote",
                    params={"symbol": "AAPL", "token": api_key},
                    timeout=10,
                )
                response.raise_for_status()
                return _ok("finnhub_connection_ok", status_code=int(response.status_code))

            if provider_name in ("transcripts", "earnings"):
                api_key = str(credentials.get("api_key") or "").strip()
                if not api_key:
                    return _fail("fmp_api_key_missing")
                response = requests.get(
                    "https://financialmodelingprep.com/api/v3/profile/AAPL",
                    params={"apikey": api_key},
                    timeout=10,
                )
                response.raise_for_status()
                return _ok("fmp_connection_ok", status_code=int(response.status_code))

            if provider_name == "reddit":
                client_id = str(credentials.get("client_id") or "").strip()
                client_secret = str(credentials.get("client_secret") or "").strip()
                if not client_id or not client_secret:
                    return _fail("reddit_credentials_missing")
                try:
                    import praw
                except Exception as exc:
                    return _fail(f"praw_unavailable:{type(exc).__name__}:{exc}")
                reddit = praw.Reddit(
                    client_id=client_id,
                    client_secret=client_secret,
                    user_agent=str(settings.get("user_agent") or os.environ.get("REDDIT_USER_AGENT") or "market-research-bot"),
                )
                list(reddit.subreddit("investing").hot(limit=1))
                return _ok("reddit_connection_ok")

            if provider_name == "stocktwits":
                response = requests.get(
                    str(settings.get("trending_url") or os.environ.get("STOCKTWITS_TRENDING_URL") or "https://api.stocktwits.com/api/2/streams/trending.json"),
                    timeout=float(settings.get("timeout_s") or os.environ.get("STOCKTWITS_TIMEOUT_S") or 10.0),
                )
                response.raise_for_status()
                return _ok("stocktwits_connection_ok", status_code=int(response.status_code))

            if provider_name == "gdelt":
                response = requests.get(
                    "https://api.gdeltproject.org/api/v2/doc/doc",
                    params={"query": "apple", "mode": "artlist", "maxrecords": 1, "format": "json"},
                    timeout=10,
                )
                response.raise_for_status()
                return _ok("gdelt_connection_ok", status_code=int(response.status_code))

            if provider_name == "sec":
                headers = {
                    "User-Agent": str(settings.get("user_agent") or os.environ.get("SEC_USER_AGENT") or "trading-system/1.0"),
                }
                response = requests.get(
                    "https://www.sec.gov/files/company_tickers_exchange.json",
                    headers=headers,
                    timeout=10,
                )
                response.raise_for_status()
                return _ok("sec_connection_ok", status_code=int(response.status_code))

            if provider_name == "weather_forecasts":
                response = requests.get(
                    "https://api.open-meteo.com/v1/forecast",
                    params={"latitude": 43.6532, "longitude": -79.3832, "daily": "temperature_2m_max", "timezone": "UTC"},
                    timeout=10,
                )
                response.raise_for_status()
                return _ok("weather_forecast_connection_ok", status_code=int(response.status_code))

            if provider_name == "weather_alerts":
                response = requests.get(
                    "https://api.weather.gov/alerts/active",
                    params={"area": "CA"},
                    headers={
                        "User-Agent": str(settings.get("http_ua") or os.environ.get("WEATHER_HTTP_UA") or "trading-system/1.0"),
                        "Accept": "application/geo+json",
                    },
                    timeout=10,
                )
                response.raise_for_status()
                return _ok("weather_alerts_connection_ok", status_code=int(response.status_code))

            if provider_name == "ibkr":
                host = str(settings.get("host") or os.environ.get("IBKR_HOST") or "127.0.0.1").strip()
                port = int(str(settings.get("port") or os.environ.get("IBKR_PORT") or "7497").strip())
                with socket.create_connection((host, port), timeout=5.0):
                    pass
                return _ok("ibkr_socket_ok", host=host, port=port)

            if source.get("source_type") == "rss_feed":
                url = str(settings.get("url") or "").strip()
                if not url:
                    return _fail("rss_url_missing")
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return _ok("rss_connection_ok", status_code=int(response.status_code))

            return _ok("connection_test_not_required", provider_name=provider_name)
        except Exception as exc:
            return _fail(f"{type(exc).__name__}:{exc}")

    def _provider_chain(self, provider_names: Iterable[str]) -> List[str]:
        requested = {str(name or "").strip().lower() for name in provider_names if str(name or "").strip()}
        order = ["polygon_ws", "ibkr", "polygon", "tradier", "yfinance", "ccxt"]
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
    return get_manager().get_desired_ingestion_jobs(default_jobs=default_jobs, read_only=read_only)
