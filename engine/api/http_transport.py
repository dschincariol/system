"""
FILE: http_transport.py

Low-level HTTP transport and response helpers.
"""

"""
HTTP transport layer only.

Does NOT contain business logic.
Delegates to injected:
- ROUTE_SPECS
- API_HANDLERS
- auth configuration
"""

import gzip
import json
import hashlib
import hmac
import inspect
import ipaddress
import logging
import os
import re
import time
from contextlib import nullcontext
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

from engine.api.auth_config import (
    dashboard_api_token_issue,
    env_flag as _auth_env_flag,
    format_mutation_auth_config_error,
    safe_dev_localhost_fallback_enabled,
    strict_mutation_auth_reasons,
    validate_mutation_auth_config,
)
from engine.api.http_parsing import deny_if_shutdown
from engine.api.rate_limit import build_default_rate_limiter
from engine.api.redaction import redact_api_payload
from engine.runtime.failure_diagnostics import log_failure, normalize_root_cause_code
from engine.runtime.metrics import emit_counter, emit_timing
from engine.runtime.platform import (
    LOCALHOST_NAME,
    LOOPBACK_HOSTS,
    default_dashboard_dev_port,
    default_dashboard_host,
)

log = logging.getLogger(__name__)
_DEV_DASHBOARD_ORIGINS = (
    f"http://{default_dashboard_host()}:{default_dashboard_dev_port()}",
    f"http://{LOCALHOST_NAME}:{default_dashboard_dev_port()}",
)


def _log_nonfatal(scope: str, err: BaseException | None = None, **extra):
    log_failure(
        log,
        event=f"http_transport_{scope}",
        code=normalize_root_cause_code(f"http_transport_{scope}"),
        message=str(err or scope),
        error=err,
        level=logging.WARNING,
        component="engine.api.http_transport",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


class InsecureConfiguration(RuntimeError):
    """Raised when the HTTP API would expose unsafe production defaults."""


def _env_flag(name: str) -> str:
    return _auth_env_flag(name)


_IMPORT_AUTH_CONFIG = validate_mutation_auth_config()
if not bool(_IMPORT_AUTH_CONFIG.get("ok")):
    raise InsecureConfiguration(format_mutation_auth_config_error(_IMPORT_AUTH_CONFIG))


_STATE_PAYLOAD_HINT_KEYS = frozenset(
    {
        "health",
        "ingestion",
        "services",
        "readiness",
        "timestamps",
        "critical_blockers",
        "root_cause_candidates",
        "system_stage",
        "prices",
        "events",
        "providers",
        "job_summary",
        "lifecycle",
        "alive",
        "db",
    }
)


def _coerce_http_status(*values, default=200):
    for value in values:
        if isinstance(value, bool):
            status = int(value)
        elif isinstance(value, int):
            status = int(value)
        elif isinstance(value, float):
            status = int(value)
        else:
            text = str(value or "").strip()
            signless = text[1:] if text[:1] in {"+", "-"} else text
            if not text or not signless.isdigit():
                continue
            status = int(text)
        if 100 <= status <= 599:
            return status
    if default is None:
        return None
    return int(default)


def _looks_like_state_payload(payload):
    if not isinstance(payload, dict):
        return False

    if any(key in payload for key in _STATE_PAYLOAD_HINT_KEYS):
        return True

    canonical_keys = (
        "status",
        "state",
        "mode",
        "execution_mode",
        "execution_allowed",
        "reasons",
    )
    return sum(1 for key in canonical_keys if key in payload) >= 3


def _map_error_to_status(error_code: str) -> int:
    code = str(error_code or "").strip().lower()
    if not code:
        return 500

    if "timeout" in code:
        return 504
    if "body_too_large" in code:
        return 413
    if code == "unauthorized" or code.endswith("_unauthorized"):
        return 401
    if code in {"wrong_credentials", "credentials_rejected"} or code.endswith("_credentials_rejected"):
        return 401
    if code == "unauthorized_table" or code.startswith("unauthorized_table:"):
        return 400
    if code in {"entitlement_missing"} or code.endswith("_entitlement_missing"):
        return 403
    if "forbidden" in code:
        return 403
    if code in {
        "execution_blocked",
        "order_blocked",
        "safety_gate_blocked",
        "live_execution_blocked",
        "prelive_reconcile_blocked",
        "operator_dashboard_auth_required",
        "operator_sidecar_token_unconfigured",
    }:
        return 403
    if code in {
        "pre_trade_rejected",
        "duplicate_recent_order",
        "max_qty_exceeded",
        "max_notional_exceeded",
        "stale_price",
        "missing_price",
    }:
        return 409
    if code == "unknown_endpoint" or "not_found" in code or "not_registered" in code:
        return 404
    if code.startswith("deprecated") or code.startswith("gone"):
        return 410
    if "rate_limit" in code or "cooldown" in code or "too_many_requests" in code:
        return 429
    if (
        code == "missing_credentials"
        or code.endswith("_credentials_missing")
        or code.endswith("_account_id_missing")
    ):
        return 422
    if (
        code.startswith("missing_")
        or code.startswith("invalid_")
        or code.startswith("unsupported_")
        or code.startswith("bad_")
        or code.startswith("malformed_")
        or code.endswith("_required")
        or "validation" in code
        or "schema_invalid" in code
    ):
        return 400
    if "unavailable" in code or "unreachable" in code or "restart_blocked" in code:
        return 503
    return 500


def _request_storage_timeout_s(ctx=None) -> float:
    raw = None
    if isinstance(ctx, dict):
        raw = ctx.get("STORAGE_REQUEST_TIMEOUT_S")
    if raw is None:
        raw = os.environ.get("DASHBOARD_STORAGE_REQUEST_TIMEOUT_S") or os.environ.get("TS_API_STORAGE_TIMEOUT_S")
    try:
        return max(0.05, float(raw if raw is not None else 0.5))
    except Exception as e:
        _log_nonfatal("request_storage_timeout_parse_failed", e, raw=str(raw))
        return 0.5


def _storage_readiness_cache_s(ctx=None) -> float:
    raw = None
    if isinstance(ctx, dict):
        raw = ctx.get("STORAGE_READINESS_CACHE_S")
    if raw is None:
        raw = os.environ.get("DASHBOARD_STORAGE_READINESS_CACHE_S")
    try:
        return max(0.0, float(raw if raw is not None else 2.0))
    except Exception as e:
        _log_nonfatal("storage_readiness_cache_parse_failed", e, raw=str(raw))
        return 2.0


def _storage_required_paths(ctx=None) -> frozenset[str]:
    raw = ()
    if isinstance(ctx, dict):
        raw = ctx.get("STORAGE_REQUIRED_PATHS") or ()
    if isinstance(raw, str):
        return frozenset(part.strip() for part in raw.split(",") if part.strip())
    try:
        return frozenset(str(part or "").strip() for part in raw if str(part or "").strip())
    except Exception as e:
        _log_nonfatal("storage_required_paths_parse_failed", e, raw_type=type(raw).__name__)
        return frozenset()


def _storage_unavailable_response(*, endpoint: str, error: BaseException | None = None, readiness=None) -> dict:
    try:
        from engine.runtime.storage_pool import storage_unavailable_payload

        return storage_unavailable_payload(endpoint=endpoint, error=error, readiness=readiness)
    except Exception as e:
        _log_nonfatal("storage_unavailable_response_failed", e, endpoint=str(endpoint or ""))
        detail = f"{type(error).__name__}: {error}" if error is not None else "runtime_storage_unavailable"
        return {
            "ok": False,
            "error": "storage_unavailable",
            "detail": detail,
            "endpoint": str(endpoint or ""),
            "meta": {"status": 503, "retryable": True, "ts_ms": int(time.time() * 1000)},
        }


def _is_storage_acquisition_exception(error: BaseException) -> bool:
    try:
        from engine.runtime.storage_pool import is_storage_acquisition_error

        return bool(is_storage_acquisition_error(error))
    except Exception as e:
        _log_nonfatal("storage_acquisition_classify_failed", e)
        text = str(error or "").lower()
        return "couldn't get a connection" in text or "storagepooltimeout" in type(error).__name__.lower()


def _storage_known_unavailable() -> bool:
    try:
        from engine.runtime.storage_pool import storage_readiness_snapshot

        snapshot = storage_readiness_snapshot()
        return bool(snapshot.get("checked") and snapshot.get("ok") is False)
    except Exception as e:
        _log_nonfatal("storage_known_unavailable_failed", e)
        return False


def _derive_response_status(payload, default_status=200):
    default_status = _coerce_http_status(default_status, default=200)

    if not isinstance(payload, dict):
        return default_status

    meta = payload.get("meta")
    explicit_status = _coerce_http_status(
        (meta or {}).get("status") if isinstance(meta, dict) else None,
        (meta or {}).get("http_status") if isinstance(meta, dict) else None,
        payload.get("status_code"),
        payload.get("http_status"),
        default=None,
    )
    if explicit_status is not None:
        return explicit_status

    if payload.get("ok", True):
        return default_status

    if _looks_like_state_payload(payload):
        return default_status

    return _map_error_to_status(
        str(payload.get("error") or payload.get("root_cause_code") or "")
    )


class StreamingResponse:
    def __init__(self, *, status=200, headers=None, stream_fn=None):
        self.status = int(status)
        self.headers = dict(headers or {})
        self.stream_fn = stream_fn


ROUTE_SENSITIVITY_PUBLIC = "public"
ROUTE_SENSITIVITY_SENSITIVE = "sensitive"

_PUBLIC_GET_ENDPOINT_PATHS = frozenset(
    {
        "/api/health",
        "/api/operator/health",
        "/api/system/health",
        "/api/liveness",
        "/api/system/liveness",
        "/api/readiness",
        "/api/operator/ping",
    }
)

_SENSITIVE_GET_ENDPOINT_PATHS = frozenset(
    {
        "/api/system/config",
        "/api/operator/logs",
        "/api/operator/stderr_tail",
        "/api/operator/support_snapshot",
        "/api/operator/snapshot",
        "/api/terminal/positions",
    }
)

_DESTRUCTIVE_ENDPOINT_PATHS = frozenset(
    {
        "/api/operator/emergency_stop",
        "/api/operator/broker_risk",
        "/api/operator/clear_manual_halt",
        "/api/operator/stop",
        "/api/operator/restart",
        "/api/operator/restart_engine",
        "/api/operator/autofix",
        "/api/operator/restart_feeds",
        "/api/operator/self_repair",
        "/api/operator/bootstrap_pipeline",
        "/api/system/repair_schema",
        "/api/repair_schema",
        "/api/jobs/start",
        "/api/jobs/stop",
        "/api/pipeline/run",
        "/api/terminal/order",
        "/api/terminal/flatten",
        "/api/data_sources/delete",
        "/api/data_sources/update",
        "/api/data_sources/test_save",
        "/api/data_sources/accounts/update",
    }
)

_CONFIRMATION_REGISTRY = {
    "/api/operator/emergency_stop": {
        "action_id": "operator.emergency_stop",
        "required_token": "KILL",
        "severity": "emergency",
        "consequence": "Immediately stops operator jobs, trips the global kill switch, and disarms execution.",
        "hold_ms": 3000,
        "require_ack": True,
        "require_actor": True,
        "require_source": True,
    },
    "/api/operator/broker_risk": {
        "action_id": "operator.broker_risk",
        "required_token": "BROKER_RISK",
        "severity": "emergency",
        "consequence": "Cancels live broker orders and may submit flattening orders under configured shutdown-risk limits.",
        "hold_ms": 3000,
        "require_ack": True,
        "require_actor": True,
        "require_source": True,
    },
    "/api/operator/clear_manual_halt": {
        "action_id": "operator.clear_manual_halt",
        "required_token": "CLEAR_MANUAL_HALT",
        "severity": "high",
        "consequence": "Clears an operator/manual kill-switch hold after confirming the current active row is not rules-owned.",
        "require_ack": True,
        "require_actor": True,
        "require_source": True,
    },
    "/api/operator/stop": {
        "action_id": "operator.stop",
        "required_token": "STOP_OPERATOR",
        "severity": "high",
        "consequence": "Stops operator-controlled runtime processes.",
        "require_ack": True,
    },
    "/api/operator/restart": {
        "action_id": "operator.restart",
        "required_token": "RESTART_OPERATOR",
        "severity": "high",
        "consequence": "Restarts operator-controlled runtime processes.",
        "require_ack": True,
    },
    "/api/operator/restart_engine": {
        "action_id": "operator.restart",
        "required_token": "RESTART_OPERATOR",
        "severity": "high",
        "consequence": "Restarts operator-controlled runtime processes.",
        "require_ack": True,
    },
    "/api/operator/autofix": {
        "action_id": "operator.autofix",
        "required_token": "SYSTEM_FIX",
        "severity": "high",
        "consequence": "Runs automatic repair steps against startup/runtime issues.",
        "require_ack": True,
    },
    "/api/operator/restart_feeds": {
        "action_id": "operator.restart_feeds",
        "required_token": "RESTART_FEEDS",
        "severity": "high",
        "consequence": "Restarts market-data feed jobs and may run a pipeline refresh.",
        "require_ack": True,
    },
    "/api/operator/self_repair": {
        "action_id": "operator.self_repair",
        "required_token": "SYSTEM_FIX",
        "severity": "high",
        "consequence": "Runs automatic runtime repair actions.",
        "require_ack": True,
    },
    "/api/operator/bootstrap_pipeline": {
        "action_id": "operator.guided_bootstrap",
        "required_token": "GUIDED_BOOTSTRAP",
        "severity": "high",
        "consequence": "Runs guided bootstrap pipeline work.",
        "require_ack": True,
    },
    "/api/system/repair_schema": {
        "action_id": "operator.repair_schema",
        "required_token": "REPAIR_SCHEMA",
        "severity": "high",
        "consequence": "Runs schema repair against runtime storage.",
        "require_ack": True,
    },
    "/api/repair_schema": {
        "action_id": "operator.repair_schema",
        "required_token": "REPAIR_SCHEMA",
        "severity": "high",
        "consequence": "Runs schema repair against runtime storage.",
        "require_ack": True,
    },
    "/api/jobs/start": {
        "action_id": "jobs.start",
        "required_token": "JOB_ACTION",
        "severity": "high",
        "consequence": "Starts a runtime job and may change live data or system state.",
        "require_ack": True,
    },
    "/api/jobs/stop": {
        "action_id": "jobs.stop",
        "required_token": "JOB_ACTION",
        "severity": "high",
        "consequence": "Stops a runtime job and may reduce data freshness or protection coverage.",
        "require_ack": True,
    },
    "/api/pipeline/run": {
        "action_id": "pipeline.run",
        "required_token": "RUN_PIPELINE",
        "severity": "high",
        "consequence": "Runs the training/evaluation pipeline and may update model evidence.",
        "require_ack": True,
    },
    "/api/terminal/order": {
        "action_id": "terminal.order",
        "required_token": "TRADE",
        "severity": "high",
        "consequence": "Records a terminal order intent for the execution pipeline.",
        "require_ack": True,
        "threshold_policy": {"policy": "always"},
    },
    "/api/terminal/flatten": {
        "action_id": "terminal.flatten",
        "required_token": "FLATTEN",
        "severity": "high",
        "consequence": "Records a flatten intent for the selected symbol.",
        "hold_ms": 1500,
        "require_ack": True,
        "threshold_policy": {"policy": "always"},
    },
    "/api/data_sources/delete": {
        "action_id": "data_sources.delete",
        "required_token": "DELETE_SOURCE",
        "severity": "high",
        "consequence": "Deletes a configured data source and reconciles ingestion jobs.",
        "require_ack": True,
    },
    "/api/data_sources/update": {
        "action_id": "data_sources.reset_credentials",
        "required_token": "RESET_CREDENTIALS",
        "severity": "high",
        "consequence": "Clears stored credentials for a configured data source.",
        "require_ack": True,
        "body_policy": {"requires_any": ("clear_credential_fields",)},
    },
    "/api/data_sources/test_save": {
        "action_id": "data_sources.test_save_reset_credentials",
        "required_token": "RESET_CREDENTIALS",
        "severity": "high",
        "consequence": "Clears stored credentials and immediately tests the data source.",
        "require_ack": True,
        "body_policy": {"requires_any": ("clear_credential_fields",)},
    },
    "/api/data_sources/accounts/update": {
        "action_id": "data_sources.reset_provider_account",
        "required_token": "RESET_CREDENTIALS",
        "severity": "high",
        "consequence": "Clears stored credentials for a shared provider account.",
        "require_ack": True,
        "body_policy": {"requires_any": ("clear_credential_fields",)},
    },
    "/api/broker/config": {
        "action_id": "broker.activate",
        "required_token": "ACTIVATE_BROKER",
        "severity": "high",
        "consequence": "Changes active broker configuration or live activation state.",
        "require_ack": True,
        "body_policy": {"truthy_any": ("active", "activate", "enabled")},
    },
    "/api/promotion/enable": {
        "action_id": "promotion.enable",
        "required_token": "PROMOTION",
        "severity": "high",
        "consequence": "Changes model promotion automation state.",
    },
    "/api/models/promote": {
        "action_id": "models.promote",
        "required_token": "PROMOTION",
        "severity": "high",
        "consequence": "Promotes a model candidate.",
    },
    "/api/system/fix": {
        "action_id": "system.fix",
        "required_token": "SYSTEM_FIX",
        "severity": "high",
        "consequence": "Runs automatic system repair actions.",
    },
    "/api/size_policy/train": {
        "action_id": "size_policy.train",
        "required_token": "TRAIN_SIZE_POLICY",
        "severity": "high",
        "consequence": "Trains and writes size-policy calibration.",
    },
    "/api/strategy/size_policy/train": {
        "action_id": "size_policy.train",
        "required_token": "TRAIN_SIZE_POLICY",
        "severity": "high",
        "consequence": "Trains and writes size-policy calibration.",
    },
    "/api/promotion/rollback": {
        "action_id": "promotion.rollback",
        "required_token": "ROLLBACK_CHAMPION",
        "severity": "high",
        "consequence": "Rolls the champion model back to a retired candidate.",
    },
    "/api/champion/rollback": {
        "action_id": "promotion.rollback",
        "required_token": "ROLLBACK_CHAMPION",
        "severity": "high",
        "consequence": "Rolls the champion model back to a retired candidate.",
    },
}
_CONFIRMATION_ENDPOINTS = {
    path: str(spec.get("required_token") or "")
    for path, spec in _CONFIRMATION_REGISTRY.items()
}


def _truthy_confirmation_value(value) -> bool:
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "ack", "confirmed"}


def _route_confirmation_spec(path: str, body) -> dict | None:
    spec = _CONFIRMATION_REGISTRY.get(str(path or ""))
    if not spec:
        return None
    payload = body if isinstance(body, dict) else {}
    body_policy = spec.get("body_policy") if isinstance(spec.get("body_policy"), dict) else {}
    requires_any = tuple(body_policy.get("requires_any") or ())
    if requires_any and not any(payload.get(key) for key in requires_any):
        return None
    truthy_any = tuple(body_policy.get("truthy_any") or ())
    if truthy_any and not any(_truthy_confirmation_value(payload.get(key)) for key in truthy_any):
        return None
    return dict(spec)


def _confirmation_hash(spec: dict | None) -> str:
    text = str((spec or {}).get("consequence") or "")
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _append_mutation_audit_event(payload: dict) -> None:
    from engine.runtime.event_log import append_event

    route_id = f"{payload.get('method') or ''} {payload.get('path') or ''}".strip()
    append_event(
        event_type="api_mutation",
        event_source="engine.api.http_transport",
        entity_type="api_route",
        entity_id=route_id,
        correlation_id=str(payload.get("request_id") or ""),
        payload=dict(payload or {}),
        best_effort=True,
    )


def _route_specs_include_mutation(route_specs) -> bool:
    for route in route_specs or []:
        method = ""
        if isinstance(route, dict):
            method = str(route.get("method", "") or "").upper().strip()
        elif isinstance(route, tuple) and len(route) >= 1:
            method = str(route[0] or "").upper().strip()
        if method and method != "GET":
            return True
    return False


def _normalize_route_sensitivity(method: str, path: str, route=None) -> str:
    raw = ""
    if isinstance(route, dict):
        raw = str(
            route.get("sensitivity")
            or route.get("route_sensitivity")
            or route.get("auth")
            or ""
        ).strip().lower()
    if raw in {"public", "unauthenticated", "anonymous", "health", "readiness"}:
        return ROUTE_SENSITIVITY_PUBLIC
    if raw in {"sensitive", "protected", "private", "operator", "authenticated"}:
        return ROUTE_SENSITIVITY_SENSITIVE

    method = str(method or "").upper().strip()
    path = str(path or "").strip()
    if method and method != "GET" and path.startswith("/api/"):
        return ROUTE_SENSITIVITY_SENSITIVE
    if method == "GET" and path in _PUBLIC_GET_ENDPOINT_PATHS:
        return ROUTE_SENSITIVITY_PUBLIC
    if method == "GET" and (path in _SENSITIVE_GET_ENDPOINT_PATHS or path.startswith("/api/")):
        return ROUTE_SENSITIVITY_SENSITIVE
    return ROUTE_SENSITIVITY_PUBLIC


def _route_specs_include_sensitive_get(route_specs) -> bool:
    for route in route_specs or []:
        method = ""
        path = ""
        if isinstance(route, dict):
            method = str(route.get("method", "") or "").upper().strip()
            path = str(route.get("path", "") or "").strip()
        elif isinstance(route, tuple) and len(route) >= 2:
            method = str(route[0] or "").upper().strip()
            path = str(route[1] or "").strip()
        if method == "GET" and _normalize_route_sensitivity(method, path, route) == ROUTE_SENSITIVITY_SENSITIVE:
            return True
    return False


def _parse_trusted_proxy_networks(raw: str | None):
    networks = []
    for item in str(raw or "").split(","):
        text = item.strip()
        if not text:
            continue
        try:
            networks.append(ipaddress.ip_network(text, strict=False))
        except ValueError:
            _log_nonfatal(
                "invalid_trusted_proxy",
                ValueError("invalid trusted proxy entry"),
                entry=text,
            )
            continue
    return tuple(networks)


def _ip_in_networks(ip_text: str, networks) -> bool:
    try:
        addr = ipaddress.ip_address(str(ip_text or "").strip())
    except Exception as e:
        _log_nonfatal("ip_address_parse_failed", e, ip_text=str(ip_text))
        return False
    return any(addr in network for network in networks)


def _is_loopback_ip(ip_text: str) -> bool:
    try:
        return bool(ipaddress.ip_address(str(ip_text or "").strip()).is_loopback)
    except Exception as e:
        _log_nonfatal("loopback_ip_parse_failed", e, ip_text=str(ip_text))
        return str(ip_text or "").strip() in LOOPBACK_HOSTS


def build_handler(ROUTE_SPECS, API_HANDLERS, dashboard_api_token, ctx=None, static_dir=None):
    """
    Builds and returns a configured HTTP request handler class.
    """

    _MAX_JSON_BODY_BYTES = int(
        os.environ.get("DASHBOARD_MAX_JSON_BODY_BYTES", "1048576")
    )

    # Responses are gzip-compressed when the client advertises gzip and the
    # payload is large enough to be worth it. This is the single biggest LAN
    # win for large snapshot/history/log JSON payloads. Static assets get a
    # short revalidatable cache window so repeat page loads are cheap without
    # serving stale UI after a deploy. Both are env-tunable; set the gzip
    # minimum very high to effectively disable compression.
    try:
        _GZIP_MIN_BYTES = max(0, int(os.environ.get("DASHBOARD_GZIP_MIN_BYTES", "1024")))
    except Exception:
        _GZIP_MIN_BYTES = 1024
    try:
        _STATIC_CACHE_MAX_AGE_S = max(0, int(os.environ.get("DASHBOARD_STATIC_CACHE_MAX_AGE_S", "60")))
    except Exception:
        _STATIC_CACHE_MAX_AGE_S = 60

    dashboard_token = (dashboard_api_token or "").strip()
    if _route_specs_include_mutation(ROUTE_SPECS) or _route_specs_include_sensitive_get(ROUTE_SPECS):
        auth_config = validate_mutation_auth_config(dashboard_token)
        if not bool(auth_config.get("ok")):
            raise InsecureConfiguration(format_mutation_auth_config_error(auth_config))

    trusted_proxy_networks = _parse_trusted_proxy_networks(
        os.environ.get("TS_DASHBOARD_TRUSTED_PROXIES", "")
    )
    injected_limiter = (ctx or {}).get("API_RATE_LIMITER") if isinstance(ctx, dict) else None
    rate_limiter = (
        injected_limiter
        if injected_limiter is not None and hasattr(injected_limiter, "check")
        else build_default_rate_limiter()
    )

    def _warn(scope: str, err: Exception, **extra):
        log_failure(
            log,
            event=f"http_transport_{scope}",
            code=normalize_root_cause_code(f"http_transport_{scope}"),
            message=str(err),
            error=err,
            level=logging.WARNING,
            component="engine.api.http_transport",
            extra=extra or None,
            include_health=False,
            persist=True,
        )

    # ------------------------------------------------------------
    # Normalize ROUTE_SPECS (supports dict and tuple formats)
    # ------------------------------------------------------------
    # Route normalization happens once at handler construction so request
    # dispatch stays cheap and business handlers receive a stable map.
    routes = {}
    route_meta = {}
    template_routes = {}

    def _compile_route_template(path):
        names = []
        parts = []
        for token in re.split(r"(\{[^/{}]+\})", str(path or "")):
            if token.startswith("{") and token.endswith("}"):
                name = token[1:-1].strip()
                if not name:
                    parts.append(re.escape(token))
                    continue
                names.append(name)
                parts.append(f"(?P<{name}>[^/]+)")
            else:
                parts.append(re.escape(token))
        return re.compile("^" + "".join(parts) + "$"), tuple(names)

    for r in ROUTE_SPECS:

        if isinstance(r, dict):
            method = str(r.get("method", "")).upper()
            path = str(r.get("path", ""))
            handler = r.get("handler")
            if method and path and handler:
                meta = {"sensitivity": _normalize_route_sensitivity(method, path, r)}
                if "{" in path and "}" in path:
                    regex, names = _compile_route_template(path)
                    template_routes.setdefault(method, []).append((path, regex, names, handler, meta))
                else:
                    routes[(method, path)] = handler
                    route_meta[(method, path)] = meta
            continue

        if isinstance(r, tuple) and len(r) >= 3:
            method = str(r[0]).upper()
            path = str(r[1])
            handler = r[2]
            meta = {"sensitivity": _normalize_route_sensitivity(method, path, r)}
            if "{" in path and "}" in path:
                regex, names = _compile_route_template(path)
                template_routes.setdefault(method, []).append((path, regex, names, handler, meta))
            else:
                routes[(method, path)] = handler
                route_meta[(method, path)] = meta
            continue

    _STATIC_DIR = os.path.abspath(static_dir or os.getcwd())

    def _call_handler(fn, *, method, parsed, body, handler_ctx):
        try:
            sig = inspect.signature(fn)
            params = [
                p
                for p in sig.parameters.values()
                if p.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
            ]
            has_varargs = any(
                p.kind == inspect.Parameter.VAR_POSITIONAL
                for p in sig.parameters.values()
            )
        except Exception as e:
            _warn("handler_signature", e, fn=getattr(fn, "__name__", "unknown"))
            params = []
            has_varargs = True

        argc = len(params)

        if method == "GET":
            if has_varargs or argc >= 3:
                return fn(parsed, None, handler_ctx)
            if argc == 2:
                return fn(parsed, handler_ctx)
            if argc == 1:
                return fn(parsed)
            return fn()

        if has_varargs or argc >= 3:
            return fn(parsed, body, handler_ctx)
        if argc == 2:
            return fn(parsed, body)
        if argc == 1:
            return fn(parsed)
        return fn()

    # ------------------------------------------------------------
    # Handler Class
    # ------------------------------------------------------------
    class Handler(SimpleHTTPRequestHandler):

        ROUTES = routes
        ROUTE_META = route_meta
        TEMPLATE_ROUTES = template_routes

        def __init__(self, *args, **kwargs):
            self._ctx = ctx or {}
            self._response_status = None
            self._response_ok = None
            self._response_output_valid = None
            self._response_streaming = False
            self._request_body_valid = True
            self._mutation_auth_kind = ""
            self._route_sensitivity = ROUTE_SENSITIVITY_PUBLIC
            self._response_redaction_enabled = False
            try:
                super().__init__(*args, directory=_STATIC_DIR, **kwargs)
            except TypeError:
                super().__init__(*args, **kwargs)

            try:
                self.connection.settimeout(15.0)
            except Exception as e:
                _warn("socket_timeout_set", e)

        # --------------------------------------------------------
        # Helpers
        # --------------------------------------------------------

        def log_message(self, format, *args):
            # system-audit: ignore[stub] BaseHTTPRequestHandler hook override
            # intentionally disables per-request stderr logging.
            # disable default noisy logging
            return

        def _normalize_ui_legacy_path(self):
            try:
                parsed = urlparse(self.path)
                path = parsed.path or "/"
                static_root_name = os.path.basename(os.path.normpath(_STATIC_DIR))
                dashboard_path = "/dashboard.html" if static_root_name == "ui" else "/ui/dashboard.html"
                terminal_path = "/terminal/terminal.html" if static_root_name == "ui" else "/ui/terminal/terminal.html"

                if path in ("/", "/dashboard", "/dashboard.html"):
                    self.path = dashboard_path
                    return

                if path in ("/ui", "/ui/"):
                    self.path = dashboard_path
                    return

                if path in ("/terminal", "/terminal.html"):
                    self.path = terminal_path
                    return

                # allow /ui/... when static root is project root
                if path.startswith("/ui/"):
                    candidate = os.path.join(_STATIC_DIR, path.lstrip("/"))

                    if os.path.exists(candidate):
                        return

                    # when static root == ui directory
                    if static_root_name == "ui":
                        trimmed = path[3:] or "/"
                        if parsed.query:
                            trimmed = f"{trimmed}?{parsed.query}"
                        self.path = trimmed
                        return
            except Exception as e:
                _warn("normalize_ui_legacy_path", e, path=getattr(self, "path", ""))

        def _read_json_body(self):

            try:
                n = int(self.headers.get("Content-Length") or "0")
            except Exception as e:
                _warn("body_content_length_parse", e)
                n = 0

            if n <= 0:
                return None

            if n > _MAX_JSON_BODY_BYTES:
                return {
                    "__body_error__": "body_too_large",
                    "__body_bytes__": n,
                }

            try:
                raw = self.rfile.read(n)
            except Exception as e:
                _warn("body_read", e)
                return {"__body_error__": "body_read_failed"}

            try:
                return json.loads(
                    raw.decode("utf-8", errors="strict") or "{}"
                )
            except Exception as e:
                _warn("body_json_decode", e)
                return {"__body_error__": "invalid_json"}

        def _record_response_observation(
            self,
            *,
            status,
            ok,
            output_valid,
            streaming=False,
        ):
            try:
                self._response_status = int(status)
            except Exception:
                self._response_status = None
            self._response_ok = None if ok is None else bool(ok)
            self._response_output_valid = bool(output_valid)
            self._response_streaming = bool(streaming)

        def _client_accepts_gzip(self) -> bool:
            accept = ""
            try:
                accept = str(self.headers.get("Accept-Encoding") or "").lower()
            except Exception as e:
                _warn("gzip_accept_encoding_header", e)
            return "gzip" in accept

        def _maybe_gzip(self, data: bytes):
            """Return ``(body, content_encoding)`` for a response body.

            Compresses only when the client advertised gzip and the payload
            clears ``_GZIP_MIN_BYTES``; otherwise returns the data unchanged
            with ``None`` encoding. Never raises -- compression failures fall
            back to the original bytes.
            """
            try:
                if (
                    not data
                    or _GZIP_MIN_BYTES <= 0
                    or len(data) < _GZIP_MIN_BYTES
                    or not self._client_accepts_gzip()
                ):
                    return data, None
                return gzip.compress(data, compresslevel=6), "gzip"
            except Exception as e:
                _warn("gzip_compress", e)
                return data, None

        def respond_json(self, obj, status=200, headers=None):
            if isinstance(obj, dict):
                obj = dict(obj)
            elif obj is None:
                obj = {"ok": False, "error": "empty_response"}
            else:
                obj = {"ok": bool(int(status) < 400), "data": obj}

            status = _derive_response_status(obj, default_status=status)

            # Normalize envelope shape centrally so endpoint handlers can
            # focus on business payloads instead of response boilerplate.
            ok = obj.get("ok")
            if ok is None:
                obj["ok"] = bool(int(status) < 400)
            if obj.get("ok", True):
                obj.setdefault("error", None)
            else:
                obj["error"] = str(obj.get("error") or "request_failed")
            meta = obj.get("meta")
            if not isinstance(meta, dict):
                meta = {}
            meta.setdefault("status", int(status))
            obj["meta"] = meta

            if bool(getattr(self, "_response_redaction_enabled", False)):
                try:
                    obj = redact_api_payload(obj)
                except Exception as e:
                    _warn("response_redaction", e, status=status)

            try:
                data = json.dumps(
                    obj,
                    separators=(",", ":"),
                    sort_keys=True,
                    default=str,
                ).encode("utf-8")
            except Exception as e:
                _warn("response_json_encode", e, status=status)
                data = b'{"ok":false,"error":"json_encode_failed","meta":{"status":500}}'
                status = 500
                obj = {"ok": False, "error": "json_encode_failed", "meta": {"status": 500}}

            self._record_response_observation(
                status=status,
                ok=obj.get("ok") if isinstance(obj, dict) else None,
                output_valid=isinstance(obj, dict) and "meta" in obj and "ok" in obj,
            )

            try:
                req_origin = (self.headers.get("Origin") or "").strip()
            except Exception as e:
                _warn("origin_header_read", e)
                req_origin = ""

            allow_origin = _DEV_DASHBOARD_ORIGINS[0]
            if req_origin in _DEV_DASHBOARD_ORIGINS:
                allow_origin = req_origin

            body, content_encoding = self._maybe_gzip(data)

            try:
                self.send_response(int(status))
                self.send_header(
                    "Content-Type", "application/json; charset=utf-8"
                )
                self.send_header("Cache-Control", "no-store")
                self.send_header("Access-Control-Allow-Origin", allow_origin)
                if content_encoding:
                    self.send_header("Content-Encoding", content_encoding)
                    self.send_header("Vary", "Origin, Accept-Encoding")
                else:
                    self.send_header("Vary", "Origin")
                self.send_header(
                    "Access-Control-Allow-Headers",
                    "Content-Type, X-API-Token",
                )
                self.send_header(
                    "Access-Control-Allow-Methods",
                    "GET, POST, OPTIONS",
                )
                for header_name, header_value in dict(headers or {}).items():
                    self.send_header(str(header_name), str(header_value))
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError) as e:
                _warn("response_write_disconnected", e, status=status)
                return
            except Exception as e:
                _warn("response_write", e, status=status)
                return

        def _direct_client_ip(self):
            try:
                return str(self.client_address[0] or "").strip()
            except Exception as e:
                _warn("direct_client_ip", e)
                return ""

        def _client_ip(self):
            direct_ip = self._direct_client_ip()
            if not trusted_proxy_networks or not _ip_in_networks(direct_ip, trusted_proxy_networks):
                return direct_ip

            try:
                raw_values = self.headers.get_all("X-Forwarded-For") or []
            except Exception as e:
                _warn("xff_header_read", e)
                raw_values = []

            chain = []
            for raw in raw_values:
                for part in str(raw or "").split(","):
                    value = part.strip()
                    if value:
                        chain.append(value)

            if not chain:
                return direct_ip

            for candidate in reversed(chain):
                if not _ip_in_networks(candidate, trusted_proxy_networks):
                    return candidate

            return direct_ip

        def _is_localhost_client(self):
            try:
                return _is_loopback_ip(self._client_ip())
            except Exception as e:
                _warn("localhost_client_check", e)
                return False

        def _request_api_token_parts(self):
            try:
                hdr = (self.headers.get("X-API-Token") or "").strip()
            except Exception as e:
                _warn("auth_header_read", e)
                hdr = ""
            if hdr:
                return hdr, "header"

            try:
                parsed = urlparse(self.path)
                q = parse_qs(parsed.query)
                qtok = (q.get("token") or [""])[0]
            except Exception as e:
                _warn("auth_query_parse", e, path=getattr(self, "path", ""))
                qtok = ""
            qtok = str(qtok or "").strip()
            if qtok:
                return qtok, "query"
            return "", ""

        def _request_api_token(self):
            token_value, _source = self._request_api_token_parts()
            return str(token_value or "")

        def _server_bind_host_candidates(self):
            candidates = []
            try:
                ctx_host = str((self._ctx or {}).get("DASHBOARD_HOST") or "").strip()
                if ctx_host:
                    candidates.append(("ctx.dashboard_host", ctx_host))
            except Exception as e:
                _warn("ctx_dashboard_host_read", e)
            env_host = str(os.environ.get("DASHBOARD_HOST", "") or "").strip()
            if env_host:
                candidates.append(("env.dashboard_host", env_host))
            try:
                server_address = getattr(self.server, "server_address", None) or ()
                server_host = str(server_address[0] or "").strip()
                if server_host:
                    candidates.append(("server.bind_host", server_host))
            except Exception as e:
                _warn("server_bind_host_read", e)
            return candidates

        def _is_loopback_bind_host(self, host):
            text = str(host or "").strip()
            if not text:
                return True
            if text in LOOPBACK_HOSTS or text.lower() in {"localhost", "[::1]"}:
                return True
            try:
                return bool(ipaddress.ip_address(text.strip("[]")).is_loopback)
            except Exception as e:
                _log_nonfatal("loopback_bind_host_parse", e, host=text)
                return False

        def _remote_bind_reasons(self):
            reasons = []
            for source, host in self._server_bind_host_candidates():
                if not self._is_loopback_bind_host(host):
                    reasons.append(f"{source}={host}")
            return tuple(reasons)

        def _require_mutation_auth(self):
            return self._require_protected_route_auth(
                allow_safe_dev_localhost_fallback=True,
                protection_reasons=(),
            )

        def _require_protected_route_auth(
            self,
            *,
            allow_safe_dev_localhost_fallback,
            protection_reasons,
        ):
            self._mutation_auth_kind = ""
            strict_reasons = strict_mutation_auth_reasons()
            all_reasons = tuple(str(item) for item in list(strict_reasons) + list(protection_reasons or ()))

            if dashboard_token:
                token_issue = dashboard_api_token_issue(
                    dashboard_token,
                    strict=bool(strict_reasons),
                ) if all_reasons else ""
                if token_issue:
                    return {
                        "ok": False,
                        "error": "forbidden_insecure_dashboard_api_token",
                        "reason": token_issue,
                        "meta": {"status": 403},
                    }

                supplied, source = self._request_api_token_parts()
                if source == "query" and strict_reasons:
                    return {
                        "ok": False,
                        "error": "query_token_forbidden",
                        "reason": "query_string_token_authentication_disabled_in_production_live",
                        "meta": {"status": 401},
                    }
                if hmac.compare_digest(supplied, dashboard_token):
                    self._mutation_auth_kind = "dashboard_api_token"
                    return None

                return {"ok": False, "error": "unauthorized", "meta": {"status": 401}}

            if all_reasons:
                return {
                    "ok": False,
                    "error": "forbidden_dashboard_api_token_required",
                    "strict_reasons": list(strict_reasons),
                    "protection_reasons": list(protection_reasons or ()),
                    "meta": {"status": 403},
                }

            if (
                allow_safe_dev_localhost_fallback
                and safe_dev_localhost_fallback_enabled()
                and self._is_localhost_client()
            ):
                self._mutation_auth_kind = "safe_dev_localhost_fallback"
                return None

            if self._is_localhost_client():
                return {
                    "ok": False,
                    "error": "forbidden_localhost_fallback_disabled",
                    "meta": {"status": 403},
                }

            return {"ok": False, "error": "forbidden_localhost_only", "meta": {"status": 403}}

        def _warn_if_token_unset(self, method, path):
            if dashboard_token:
                return
            try:
                log_failure(
                    log,
                    event="http_transport_dashboard_api_token_unset",
                    code="HTTP_TRANSPORT_DASHBOARD_API_TOKEN_UNSET",
                    message="dashboard api token is unset",
                    level=logging.WARNING,
                    component="engine.api.http_transport",
                    extra={
                        "method": str(method or ""),
                        "path": str(path or ""),
                        "client_ip": self._client_ip(),
                    },
                    include_health=False,
                    persist=False,
                )
            except Exception as e:
                _warn("token_unset_warning", e, method=method, path=path)

        def _rate_limit_protected_request(self, parsed_path, *, token_for_bucket=None):
            if token_for_bucket is None:
                token_for_bucket = self._request_api_token() if dashboard_token else ""
            destructive = str(parsed_path or "") in _DESTRUCTIVE_ENDPOINT_PATHS
            try:
                decision = rate_limiter.check(
                    token=str(token_for_bucket or ""),
                    ip=self._client_ip(),
                    destructive=destructive,
                )
            except Exception as e:
                _warn("rate_limit_check", e, path=str(parsed_path or ""))
                return None

            if decision.allowed:
                return None

            retry_after = max(1, int(decision.retry_after_s or 1))
            return (
                {
                    "ok": False,
                    "error": "rate_limit_exceeded",
                    "retry_after_s": retry_after,
                    "limit_per_min": int(decision.limit_per_min or 0),
                    "meta": {"status": 429},
                },
                {"Retry-After": str(retry_after)},
            )

        def _rate_limit_mutation(self, parsed_path, *, token_for_bucket=None):
            return self._rate_limit_protected_request(
                parsed_path,
                token_for_bucket=token_for_bucket,
            )

        def _require_mutation_confirmation(self, parsed_path, body):
            spec = _route_confirmation_spec(str(parsed_path or ""), body)
            self._mutation_confirmation = None
            if not spec:
                return None
            expected = str(spec.get("required_token") or "").strip()
            payload = dict(body or {}) if isinstance(body, dict) else {}
            actual = str(payload.get("confirmation") or payload.get("confirm") or "").strip()
            method = "typed_phrase" if payload.get("confirmation") is not None else "legacy_confirm"
            actor = str(payload.get("actor") or payload.get("who") or "").strip()
            source = str(payload.get("source") or payload.get("source_surface") or "").strip()
            reason = str(payload.get("reason") or payload.get("justification") or payload.get("note") or "").strip()
            target = str(payload.get("target") or payload.get("target_id") or payload.get("name") or payload.get("source_key") or "").strip()
            hold_ms = 0
            try:
                hold_ms = int(float(payload.get("confirmation_hold_ms") or payload.get("hold_ms") or 0))
            except Exception:
                hold_ms = 0
            required_hold_ms = int(float(spec.get("hold_ms") or 0))
            missing = []
            if not hmac.compare_digest(actual, str(expected)):
                missing.append("confirmation")
            if bool(spec.get("require_ack")) and not _truthy_confirmation_value(payload.get("consequence_ack")):
                missing.append("consequence_ack")
            if bool(spec.get("require_actor")) and not actor:
                missing.append("actor")
            if bool(spec.get("require_source")) and not source:
                missing.append("source")
            if required_hold_ms > 0 and hold_ms < required_hold_ms:
                missing.append("confirmation_hold_ms")
            if not missing:
                self._mutation_confirmation = {
                    "action_id": str(spec.get("action_id") or ""),
                    "severity": str(spec.get("severity") or ""),
                    "required_confirm": expected,
                    "confirmation_method": method,
                    "actor": actor,
                    "source_surface": source,
                    "reason": reason,
                    "target": target,
                    "confirmation_hold_ms": int(hold_ms),
                    "consequence_hash": _confirmation_hash(spec),
                    "threshold_policy": dict(spec.get("threshold_policy") or {}),
                }
                return None
            return {
                "ok": False,
                "error": "confirmation_required",
                "required_confirm": str(expected),
                "required_token": str(expected),
                "required_fields": missing,
                "action_id": str(spec.get("action_id") or ""),
                "severity": str(spec.get("severity") or ""),
                "consequence": str(spec.get("consequence") or ""),
                "min_hold_ms": required_hold_ms,
                "meta": {"status": 422},
            }

        def _audit_mutation(
            self,
            *,
            method,
            path,
            handler_name="",
            outcome,
            status=0,
            error="",
            body_valid=True,
            confirmed=None,
            rate_limited=False,
        ):
            try:
                supplied_token = self._request_api_token()
                confirmation = (
                    dict(self._mutation_confirmation or {})
                    if isinstance(getattr(self, "_mutation_confirmation", None), dict)
                    else {}
                )
                request_id = (
                    self.headers.get("X-Request-ID")
                    or self.headers.get("X-Correlation-ID")
                    or f"{int(time.time() * 1000)}-{id(self)}"
                )
                payload = {
                    "ts_ms": int(time.time() * 1000),
                    "request_id": str(request_id),
                    "method": str(method or ""),
                    "path": str(path or ""),
                    "handler": str(handler_name or ""),
                    "route_sensitivity": str(getattr(self, "_route_sensitivity", "") or ""),
                    "outcome": str(outcome or ""),
                    "status": int(status or 0),
                    "error": str(error or ""),
                    "client_ip": self._client_ip(),
                    "localhost_client": bool(self._is_localhost_client()),
                    "token_present": bool(supplied_token),
                    "dashboard_token_configured": bool(dashboard_token),
                    "auth_kind": str(self._mutation_auth_kind or ""),
                    "strict_reasons": list(strict_mutation_auth_reasons()),
                    "safe_dev_localhost_fallback_enabled": bool(
                        safe_dev_localhost_fallback_enabled()
                    ),
                    "destructive": str(path or "") in _DESTRUCTIVE_ENDPOINT_PATHS,
                    "body_valid": bool(body_valid),
                    "rate_limited": bool(rate_limited),
                }
                if confirmed is not None:
                    payload["confirmed"] = bool(confirmed)
                spec = _route_confirmation_spec(str(path or ""), {})
                if confirmation:
                    payload.update({
                        "action_id": confirmation.get("action_id", ""),
                        "confirmation_method": confirmation.get("confirmation_method", ""),
                        "actor": confirmation.get("actor", ""),
                        "source_surface": confirmation.get("source_surface", ""),
                        "reason": confirmation.get("reason", ""),
                        "target": confirmation.get("target", ""),
                        "confirmation_severity": confirmation.get("severity", ""),
                        "consequence_hash": confirmation.get("consequence_hash", ""),
                        "threshold_policy": confirmation.get("threshold_policy", {}),
                    })
                elif spec:
                    payload.update({
                        "action_id": str(spec.get("action_id") or ""),
                        "confirmation_severity": str(spec.get("severity") or ""),
                        "consequence_hash": _confirmation_hash(spec),
                    })
                _append_mutation_audit_event(payload)
            except Exception as e:
                _warn(
                    "mutation_audit",
                    e,
                    method=str(method or ""),
                    path=str(path or ""),
                    handler=str(handler_name or ""),
                    outcome=str(outcome or ""),
                )

        # --------------------------------------------------------
        # Core Dispatch
        # --------------------------------------------------------

        def _dispatch(self):
            started = time.time()

            method = str(self.command or "").upper().strip()
            self._response_status = None
            self._response_ok = None
            self._response_output_valid = None
            self._response_streaming = False
            self._request_body_valid = True
            self._mutation_confirmation = None
            self._route_sensitivity = ROUTE_SENSITIVITY_PUBLIC
            self._response_redaction_enabled = False
            self._serving_static = False

            self._normalize_ui_legacy_path()

            parsed = urlparse(self.path)
            key = (method, parsed.path)
            handler_name = self.ROUTES.get(key)
            parsed_for_handler = parsed
            route_meta = dict(self.ROUTE_META.get(key) or {})

            # tolerate trailing slash mismatches
            if not handler_name and parsed.path.endswith("/"):
                key = (method, parsed.path.rstrip("/"))
                handler_name = self.ROUTES.get(key)
                if handler_name:
                    parsed = urlparse(parsed._replace(path=parsed.path.rstrip("/")).geturl())
                    parsed_for_handler = parsed
                    route_meta = dict(self.ROUTE_META.get(key) or {})

            if not handler_name:
                for route_path, route_regex, route_names, route_handler, template_meta in self.TEMPLATE_ROUTES.get(method, []):
                    match = route_regex.match(parsed.path)
                    if not match:
                        continue
                    try:
                        raw_qs = parse_qs(parsed.query, keep_blank_values=True)
                        parsed_for_handler = {
                            str(k): ("" if not v else str(v[0]))
                            for k, v in raw_qs.items()
                        }
                    except Exception as e:
                        _warn("template_query_parse", e, path=parsed.path)
                        parsed_for_handler = {}
                    for name in route_names:
                        parsed_for_handler[str(name)] = unquote(str(match.group(name) or ""))
                    parsed_for_handler["_route_path"] = str(route_path)
                    parsed_for_handler["_path"] = str(parsed.path or "")
                    handler_name = route_handler
                    route_meta = dict(template_meta or {})
                    break

            # no route → static or 404
            if not handler_name:

                if method == "GET":
                    # Mark this as a static-file response so end_headers() can
                    # attach a revalidatable Cache-Control window for assets.
                    self._serving_static = True
                    return super().do_GET()

                self._audit_mutation(
                    method=method,
                    path=parsed.path,
                    outcome="unknown_endpoint",
                    status=404,
                    error="unknown_endpoint",
                )
                return self.respond_json(
                    {"ok": False, "error": "unknown_endpoint"},
                    404,
                )

            self._route_sensitivity = str(
                route_meta.get("sensitivity")
                or _normalize_route_sensitivity(method, parsed.path)
            )
            self._response_redaction_enabled = bool(
                method == "GET"
                and self._route_sensitivity == ROUTE_SENSITIVITY_SENSITIVE
            )
            protected_get_reasons = (
                self._remote_bind_reasons()
                if method == "GET" and self._route_sensitivity == ROUTE_SENSITIVITY_SENSITIVE
                else ()
            )
            protected_access = bool(
                method != "GET"
                or (method == "GET" and self._route_sensitivity == ROUTE_SENSITIVITY_SENSITIVE and (strict_mutation_auth_reasons() or protected_get_reasons))
            )

            fn = API_HANDLERS.get(handler_name)

            if not fn:
                if protected_access:
                    self._audit_mutation(
                        method=method,
                        path=parsed.path,
                        handler_name=handler_name,
                        outcome="handler_missing",
                        status=500,
                        error=f"handler_missing:{handler_name}",
                    )
                return self.respond_json(
                    {
                        "ok": False,
                        "error": f"handler_missing:{handler_name}",
                    },
                    500,
                )

            self._warn_if_token_unset(method, parsed.path)

            # ----------------------------------------------------
            # protected route auth/rate-limit/audit gate
            # ----------------------------------------------------

            if protected_access:

                if method != "GET":
                    denied = deny_if_shutdown()
                    if denied:
                        self._audit_mutation(
                            method=method,
                            path=parsed.path,
                            handler_name=handler_name,
                            outcome="shutdown_denied",
                            status=503,
                            error=str((denied or {}).get("error") or "shutdown"),
                        )
                        return self.respond_json(denied, 503)

                auth = self._require_protected_route_auth(
                    allow_safe_dev_localhost_fallback=method != "GET",
                    protection_reasons=protected_get_reasons,
                )
                if auth:
                    auth_status = _derive_response_status(auth, default_status=403)
                    limited = self._rate_limit_protected_request(parsed.path, token_for_bucket="")
                    if limited:
                        payload, headers = limited
                        self._audit_mutation(
                            method=method,
                            path=parsed.path,
                            handler_name=handler_name,
                            outcome="rate_limited_auth_denied",
                            status=429,
                            error=str(payload.get("error") or "rate_limit_exceeded"),
                            rate_limited=True,
                        )
                        return self.respond_json(payload, 429, headers=headers)
                    self._audit_mutation(
                        method=method,
                        path=parsed.path,
                        handler_name=handler_name,
                        outcome="auth_denied",
                        status=auth_status,
                        error=str(auth.get("error") or "auth_denied"),
                    )
                    return self.respond_json(auth, auth_status)

                limited = self._rate_limit_protected_request(parsed.path)
                if limited:
                    payload, headers = limited
                    self._audit_mutation(
                        method=method,
                        path=parsed.path,
                        handler_name=handler_name,
                        outcome="rate_limited",
                        status=429,
                        error=str(payload.get("error") or "rate_limit_exceeded"),
                        rate_limited=True,
                    )
                    return self.respond_json(payload, 429, headers=headers)

            if method == "GET" and parsed.path in _storage_required_paths(self._ctx):
                try:
                    from engine.runtime.storage_pool import probe_storage_readiness

                    readiness = probe_storage_readiness(
                        timeout_s=_request_storage_timeout_s(self._ctx),
                        max_age_s=_storage_readiness_cache_s(self._ctx),
                    )
                except Exception as e:
                    _warn("storage_readiness_probe_failed", e, path=str(parsed.path))
                    return self.respond_json(
                        _storage_unavailable_response(endpoint=parsed.path, error=e),
                        503,
                    )
                if not bool((readiness or {}).get("ok")):
                    return self.respond_json(
                        _storage_unavailable_response(endpoint=parsed.path, readiness=readiness),
                        503,
                    )

            try:

                body = None

                if method != "GET":
                    body = self._read_json_body() or {}

                    if isinstance(body, dict):

                        if body.get("__body_error__") == "body_too_large":
                            self._request_body_valid = False
                            self._audit_mutation(
                                method=method,
                                path=parsed.path,
                                handler_name=handler_name,
                                outcome="invalid_body",
                                status=413,
                                error="body_too_large",
                                body_valid=False,
                            )
                            return self.respond_json(
                                {
                                    "ok": False,
                                    "error": "body_too_large",
                                    "max_bytes": _MAX_JSON_BODY_BYTES,
                                    "content_length": body.get(
                                        "__body_bytes__"
                                    ),
                                },
                                413,
                            )

                        if body.get("__body_error__") == "invalid_json":
                            self._request_body_valid = False
                            self._audit_mutation(
                                method=method,
                                path=parsed.path,
                                handler_name=handler_name,
                                outcome="invalid_body",
                                status=400,
                                error="invalid_json",
                                body_valid=False,
                            )
                            return self.respond_json(
                                {"ok": False, "error": "invalid_json"},
                                400,
                            )

                        if body.get("__body_error__") == "body_read_failed":
                            self._request_body_valid = False
                            self._audit_mutation(
                                method=method,
                                path=parsed.path,
                                handler_name=handler_name,
                                outcome="invalid_body",
                                status=400,
                                error="body_read_failed",
                                body_valid=False,
                            )
                            return self.respond_json(
                                {"ok": False, "error": "body_read_failed"},
                                400,
                            )

                    confirmation = self._require_mutation_confirmation(parsed.path, body)
                    if confirmation:
                        self._audit_mutation(
                            method=method,
                            path=parsed.path,
                            handler_name=handler_name,
                            outcome="confirmation_denied",
                            status=_derive_response_status(confirmation, default_status=422),
                            error=str(confirmation.get("error") or "confirmation_required"),
                            confirmed=False,
                        )
                        return self.respond_json(confirmation, 422)

                try:
                    from engine.runtime.storage_pool import storage_acquire_timeout_override

                    storage_timeout_ctx = storage_acquire_timeout_override(
                        _request_storage_timeout_s(self._ctx)
                    )
                except Exception:
                    storage_timeout_ctx = nullcontext()

                with storage_timeout_ctx:
                    result = _call_handler(
                        fn,
                        method=method,
                        parsed=parsed_for_handler,
                        body=body,
                        handler_ctx=self._ctx,
                    )

                if result is None:
                    result = {
                        "ok": False,
                        "error": "empty_response",
                        "meta": {"handler": str(handler_name)},
                    }

                if isinstance(result, StreamingResponse):
                    if protected_access:
                        self._audit_mutation(
                            method=method,
                            path=parsed.path,
                            handler_name=handler_name,
                            outcome="completed",
                            status=int(result.status),
                            error="",
                            confirmed=(
                                True
                                if self._mutation_confirmation is not None
                                else None
                            ),
                        )
                    self._record_response_observation(
                        status=int(result.status),
                        ok=True,
                        output_valid=callable(result.stream_fn),
                        streaming=True,
                    )
                    self.send_response(int(result.status))

                    headers = dict(result.headers or {})
                    if "Content-Type" not in headers:
                        headers["Content-Type"] = "application/octet-stream"

                    for k, v in headers.items():
                        self.send_header(str(k), str(v))

                    self.end_headers()
                    try:
                        if callable(result.stream_fn):
                            return result.stream_fn(self)
                    except Exception as e:
                        _warn("streaming_response", e, handler=handler_name or "")
                        return
                    return

                if protected_access:
                    result_status = _derive_response_status(result, default_status=200)
                    result_dict = result if isinstance(result, dict) else {}
                    self._audit_mutation(
                        method=method,
                        path=parsed.path,
                        handler_name=handler_name,
                        outcome="completed" if _looks_like_state_payload(result) or bool(result_dict.get("ok", True)) else "handler_rejected",
                        status=result_status,
                        error=str(result_dict.get("error") or ""),
                        confirmed=(
                            True
                            if self._mutation_confirmation is not None
                            else None
                        ),
                    )
                return self.respond_json(result)

            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError) as e:
                if protected_access:
                    self._audit_mutation(
                        method=method,
                        path=parsed.path,
                        handler_name=handler_name,
                        outcome="client_disconnected",
                        status=0,
                        error=type(e).__name__,
                    )
                _warn("handler_disconnected", e, handler=handler_name or "")
                return

            except Exception as e:
                if _is_storage_acquisition_exception(e):
                    log_failure(
                        log,
                        event="http_transport_handler_storage_unavailable",
                        code="HTTP_TRANSPORT_HANDLER_STORAGE_UNAVAILABLE",
                        message=str(e),
                        error=e,
                        level=logging.WARNING,
                        component="engine.api.http_transport",
                        extra={
                            "handler": handler_name or "",
                            "method": str(method),
                            "path": str(getattr(self, "path", "")),
                        },
                        include_health=False,
                        persist=False,
                    )
                    if protected_access:
                        self._audit_mutation(
                            method=method,
                            path=parsed.path,
                            handler_name=handler_name,
                            outcome="storage_unavailable",
                            status=503,
                            error=f"{type(e).__name__}: {e}",
                        )
                    return self.respond_json(
                        _storage_unavailable_response(endpoint=parsed.path, error=e),
                        503,
                    )

                _warn(
                    "handler_exception",
                    e,
                    handler=handler_name or "",
                    method=method,
                    path=getattr(self, "path", ""),
                )

                if protected_access:
                    self._audit_mutation(
                        method=method,
                        path=parsed.path,
                        handler_name=handler_name,
                        outcome="handler_exception",
                        status=500,
                        error=f"{type(e).__name__}: {e}",
                    )
                return self.respond_json(
                    {
                        "ok": False,
                        "error": "internal_server_error",
                        "reason_code": "handler_exception",
                        "message": "Request handler failed unexpectedly.",
                        "detail": type(e).__name__,
                    },
                    500,
                )
            finally:
                elapsed_ms = int((time.time() - started) * 1000)
                if elapsed_ms >= 2000:
                    try:
                        log.log(
                            logging.WARNING,
                            "HTTP_SLOW_REQUEST method=%s path=%s handler=%s elapsed_ms=%s",
                            method,
                            parsed.path if "parsed" in locals() else self.path,
                            handler_name or "",
                            elapsed_ms,
                        )
                    except Exception as e:
                        _warn(
                            "slow_request_log",
                            e,
                            method=method,
                            path=parsed.path if 'parsed' in locals() else getattr(self, "path", ""),
                            handler=handler_name or "",
                            elapsed_ms=elapsed_ms,
                        )
                try:
                    if not _storage_known_unavailable():
                        status_tag = (
                            int(self._response_status)
                            if self._response_status is not None
                            else 0
                        )
                        route_path = parsed.path if "parsed" in locals() else getattr(self, "path", "")
                        common_tags = {
                            "method": method,
                            "path": str(route_path or ""),
                            "handler": str(handler_name or ""),
                            "status": status_tag,
                            "body_valid": int(bool(self._request_body_valid)),
                            "output_valid": int(bool(self._response_output_valid)),
                            "streaming": int(bool(self._response_streaming)),
                        }
                        emit_counter(
                            "http_request_total",
                            1,
                            component="engine.api.http_transport",
                            extra_tags=common_tags,
                        )
                        if self._response_ok is False or not bool(self._request_body_valid):
                            emit_counter(
                                "http_request_failure_total",
                                1,
                                component="engine.api.http_transport",
                                extra_tags=dict(
                                    common_tags,
                                    ok=(
                                        ""
                                        if self._response_ok is None
                                        else int(bool(self._response_ok))
                                    ),
                                ),
                            )
                        if (self._response_output_valid is False) or (not bool(self._request_body_valid)):
                            emit_counter(
                                "http_request_validation_total",
                                1,
                                component="engine.api.http_transport",
                                extra_tags=dict(common_tags, validation="failed"),
                            )
                        emit_timing(
                            "http_request_latency_ms",
                            elapsed_ms,
                            component="engine.api.http_transport",
                            extra_tags=common_tags,
                        )
                except Exception as e:
                    _warn(
                        "request_metrics_emit",
                        e,
                        method=method,
                        path=parsed.path if 'parsed' in locals() else getattr(self, "path", ""),
                        handler=handler_name or "",
                    )

        # --------------------------------------------------------
        # HTTP verbs
        # --------------------------------------------------------

        def end_headers(self):
            # Attach a short, revalidatable cache window + nosniff to static
            # asset responses only. API/JSON responses set their own
            # Cache-Control (no-store) and never reach this branch.
            if getattr(self, "_serving_static", False):
                try:
                    path = urlparse(self.path).path.lower()
                    cacheable = path.endswith((
                        ".js", ".mjs", ".css", ".svg", ".png", ".jpg",
                        ".jpeg", ".gif", ".ico", ".woff", ".woff2", ".ttf",
                        ".map",
                    ))
                    if cacheable and _STATIC_CACHE_MAX_AGE_S > 0:
                        self.send_header(
                            "Cache-Control",
                            f"public, max-age={_STATIC_CACHE_MAX_AGE_S}, must-revalidate",
                        )
                    else:
                        # HTML shells reference versioned assets -> revalidate.
                        self.send_header("Cache-Control", "no-cache")
                    self.send_header("X-Content-Type-Options", "nosniff")
                except Exception as e:
                    _warn("static_cache_headers", e)
            super().end_headers()

        def do_GET(self):
            self._dispatch()

        def do_POST(self):
            self._dispatch()

        def do_OPTIONS(self):
            try:
                req_origin = (self.headers.get("Origin") or "").strip()
            except Exception as e:
                _warn("options_origin_read", e)
                req_origin = ""

            allow_origin = _DEV_DASHBOARD_ORIGINS[0]
            if req_origin in _DEV_DASHBOARD_ORIGINS:
                allow_origin = req_origin

            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", allow_origin)
            self.send_header("Vary", "Origin")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type, X-API-Token",
            )
            self.send_header(
                "Access-Control-Allow-Methods",
                "GET, POST, OPTIONS",
            )
            self.send_header("Cache-Control", "no-store")
            self.end_headers()

    return Handler



def run_http_server(host, port, handler_cls):

    class _ReusableThreadingHTTPServer(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    try:
        httpd = _ReusableThreadingHTTPServer((host, int(port)), handler_cls)
    except Exception as e:
        log_failure(
            log,
            event="http_transport_bind_failed",
            code="HTTP_TRANSPORT_BIND_FAILED",
            message=f"run_http_server_bind_failed host={host} port={int(port)}",
            error=e,
            level=logging.ERROR,
            component="engine.api.http_transport",
            extra={"host": str(host), "port": int(port)},
            include_health=False,
            persist=True,
        )
        raise RuntimeError(
            f"run_http_server_bind_failed host={host} port={int(port)} error={e}"
        ) from e

    return httpd
