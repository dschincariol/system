from __future__ import annotations

"""Central fail-closed checks required before live trading can be enabled."""

import os
import ipaddress
from typing import Any, Dict, Optional

from engine.api.auth_config import dashboard_api_token_from_env, dashboard_api_token_issue
from engine.execution.broker_failover_policy import (
    broker_startup_preflight,
    live_broker_environment_contract,
)
from engine.execution.broker_shutdown_risk import broker_shutdown_policy_snapshot
from engine.execution.options_readiness import live_options_readiness_snapshot
from engine.execution.position_reconcile import position_reconcile_evidence_snapshot
from engine.runtime.backup_evidence import backup_restore_evidence_snapshot, wal_archiver_runtime_snapshot
from engine.runtime.clock_health import clock_health_snapshot
from engine.runtime.live_execution_control import (
    DISABLE_LIVE_EXECUTION_REASON,
    env_flag_truthy,
    live_execution_disabled,
    prelive_reconcile_policy_snapshot,
)
from engine.runtime.live_ai_safety import live_ai_safety_snapshot
from engine.runtime.platform import LOOPBACK_HOSTS, default_dashboard_host


_TRUTHY_VALUES = {"1", "true", "yes", "on"}
_FALSEY_VALUES = {"0", "false", "no", "off"}
DEFAULT_LIVE_CONFIRM_PHRASE = "I_UNDERSTAND_LIVE_TRADING"
DEFAULT_OPERATOR_TOKEN_MIN_LENGTH = 16
DEFAULT_PUBLIC_NETWORK_EXPOSURE_ACK_PHRASE = "I_UNDERSTAND_THIS_EXPOSES_TRADING_SERVICES"
_CONTAINER_INTERNAL_BIND_CONTEXTS = {"container", "container_internal", "docker", "docker_internal", "compose"}
_PLACEHOLDER_AUDIT_VALUES = {"", "unknown", "none", "n/a", "na", "todo", "tbd", "test", "dev"}
PLACEHOLDER_OPERATOR_API_TOKENS = {
    "change-me",
    "changeme",
    "change_me",
    "replace-me",
    "replace_me",
    "default",
    "operator",
    "operator-token",
    "token",
    "secret",
    "password",
    "test-token",
    "dev-token",
}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in _TRUTHY_VALUES


def _env_bool_from(environ: dict[str, Any], name: str, default: bool = False) -> bool:
    raw = str(environ.get(name, "") or "").strip().lower()
    if raw == "":
        return bool(default)
    return raw in _TRUTHY_VALUES


def _env_text_from(environ: dict[str, Any], name: str, default: str = "") -> str:
    return str(environ.get(name, default) or "").strip()


def _env_bool_snapshot(name: str, default: bool = False) -> tuple[bool, bool, str]:
    raw = str(os.environ.get(name, "")).strip()
    lowered = raw.lower()
    if lowered == "":
        return bool(default), False, raw
    if lowered in _TRUTHY_VALUES:
        return True, False, raw
    if lowered in _FALSEY_VALUES:
        return False, False, raw
    return bool(default), True, raw


def _normalize_mode(value: Any, default: str = "safe") -> str:
    mode = str(value or default).strip().lower() or default
    return mode if mode in {"safe", "paper", "shadow", "live", "dev", "development"} else default


def _confirmation_phrase() -> str:
    return DEFAULT_LIVE_CONFIRM_PHRASE


def operator_api_token_from_env() -> str:
    token = str(os.environ.get("OPERATOR_API_TOKEN", "") or "").strip()
    if token:
        return token
    from engine.runtime.secret_sources import read_secret_text_from_env

    return read_secret_text_from_env("OPERATOR_API_TOKEN")


def operator_api_token_issue(token: str | None) -> str:
    value = str(token or "").strip()
    if not value:
        return "missing_operator_api_token"
    if value.lower() in PLACEHOLDER_OPERATOR_API_TOKENS:
        return "default_operator_api_token"
    try:
        min_length = max(
            8,
            int(str(os.environ.get("OPERATOR_API_TOKEN_MIN_LENGTH", DEFAULT_OPERATOR_TOKEN_MIN_LENGTH)).strip()),
        )
    except Exception:
        min_length = DEFAULT_OPERATOR_TOKEN_MIN_LENGTH
    if len(value) < int(min_length):
        return "weak_operator_api_token"
    return ""


def _operator_bind_host_is_unsafe(host: str) -> bool:
    value = str(host or "").strip().lower()
    if value in {"", "127.0.0.1", "localhost", "::1", "[::1]"}:
        return False
    if value in {"0.0.0.0", "::", "[::]"}:
        return True
    return value not in {str(item).lower() for item in LOOPBACK_HOSTS}


def _is_loopback_bind_host(host: str) -> bool:
    value = str(host or "").strip()
    if not value:
        return False
    lowered = value.lower()
    if lowered in {"localhost", "127.0.0.1", "::1", "[::1]"}:
        return True
    if value in LOOPBACK_HOSTS:
        return True
    try:
        return bool(ipaddress.ip_address(value.strip("[]")).is_loopback)
    except ValueError:
        return False


def _is_wildcard_bind_host(host: str) -> bool:
    value = str(host or "").strip().lower()
    return value in {"", "*", "0.0.0.0", "::", "[::]"}


def _audit_value_ok(value: str, *, min_len: int = 3) -> bool:
    text = str(value or "").strip()
    if len(text) < int(min_len):
        return False
    if text.lower() in _PLACEHOLDER_AUDIT_VALUES:
        return False
    return True


def public_network_exposure_ack_snapshot(
    *, environ: Optional[dict[str, Any]] = None
) -> Dict[str, Any]:
    env = dict(os.environ if environ is None else environ)
    phrase = DEFAULT_PUBLIC_NETWORK_EXPOSURE_ACK_PHRASE
    ack = _env_text_from(env, "TRADING_PUBLIC_NETWORK_EXPOSURE_ACK")
    owner = _env_text_from(env, "TRADING_PUBLIC_NETWORK_EXPOSURE_OWNER")
    reason = _env_text_from(env, "TRADING_PUBLIC_NETWORK_EXPOSURE_REASON")
    blockers: list[str] = []
    if ack != phrase:
        blockers.append("TRADING_PUBLIC_NETWORK_EXPOSURE_ACK_required")
    if not _audit_value_ok(owner, min_len=3):
        blockers.append("TRADING_PUBLIC_NETWORK_EXPOSURE_OWNER_required")
    if not _audit_value_ok(reason, min_len=16):
        blockers.append("TRADING_PUBLIC_NETWORK_EXPOSURE_REASON_required")
    return {
        "ok": not blockers,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "ack_configured": bool(ack),
        "expected_ack": phrase,
        "owner": owner,
        "exposure_reason": reason,
    }


def _production_network_exposure_required(environ: dict[str, Any], mode: str) -> bool:
    if str(mode or "").strip().lower() == "live":
        return True
    for key in ("ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE", "MODE"):
        if str(environ.get(key, "") or "").strip().lower() == "live":
            return True
    for key in ("ENV", "APP_ENV", "TS_ENV", "NODE_ENV"):
        if str(environ.get(key, "") or "").strip().lower() in {"prod", "production"}:
            return True
    return _env_bool_from(environ, "PROD_LOCK", False)


def _service_bind_host(environ: dict[str, Any], service: str) -> tuple[str, str, bool]:
    candidates = {
        "timescale": (("TIMESCALE_DANGEROUS_PUBLIC_BIND_HOST", "TIMESCALE_HOST_BIND"), "127.0.0.1"),
        "redis": (("REDIS_DANGEROUS_PUBLIC_BIND_HOST", "REDIS_HOST_BIND"), "127.0.0.1"),
        "minio": (("MINIO_DANGEROUS_PUBLIC_BIND_HOST", "MINIO_HOST_BIND"), "127.0.0.1"),
        "minio_console": (("MINIO_CONSOLE_DANGEROUS_PUBLIC_BIND_HOST", "MINIO_CONSOLE_HOST_BIND"), "127.0.0.1"),
        "dashboard": (("DASHBOARD_DANGEROUS_PUBLIC_BIND_HOST", "DASHBOARD_HOST_BIND"), "127.0.0.1"),
        "operator": (("OPERATOR_DANGEROUS_PUBLIC_BIND_HOST", "OPERATOR_HOST_BIND"), "127.0.0.1"),
    }
    names, default = candidates[service]
    for name in names:
        if name in environ:
            return str(environ.get(name, "") or "").strip(), name, True
    if service == "operator" and str(environ.get("OPERATOR_PUBLIC_PORT", "") or "").strip():
        bind_host = str(environ.get("OPERATOR_BIND_HOST", "") or "").strip()
        if bind_host:
            return bind_host, "OPERATOR_BIND_HOST", True
    return default, names[0], False


def _service_port_configured(environ: dict[str, Any], service: str) -> tuple[bool, str, str]:
    specs = {
        "timescale": ("TIMESCALE_PORT", "5432", True),
        "redis": ("REDIS_PORT", "6379", True),
        "minio": ("MINIO_PORT", "9000", True),
        "minio_console": ("MINIO_CONSOLE_PORT", "9001", True),
        "dashboard": ("DASHBOARD_PUBLIC_PORT", "8000", True),
        "operator": ("OPERATOR_PUBLIC_PORT", "", False),
    }
    env_name, default, default_enabled = specs[service]
    raw = str(environ.get(env_name, default if default_enabled else "") or "").strip()
    return bool(raw), env_name, raw


def _service_allow_env(service: str) -> str:
    return {
        "timescale": "TIMESCALE_ALLOW_DANGEROUS_PUBLIC_BIND",
        "redis": "REDIS_ALLOW_DANGEROUS_PUBLIC_BIND",
        "minio": "MINIO_ALLOW_DANGEROUS_PUBLIC_BIND",
        "minio_console": "MINIO_CONSOLE_ALLOW_DANGEROUS_PUBLIC_BIND",
        "dashboard": "DASHBOARD_ALLOW_DANGEROUS_PUBLIC_BIND",
        "operator": "OPERATOR_ALLOW_DANGEROUS_PUBLIC_BIND",
    }[service]


def _dashboard_process_bind_state(environ: dict[str, Any]) -> dict[str, Any] | None:
    host = _env_text_from(environ, "DASHBOARD_HOST")
    if not host:
        return None
    context = _env_text_from(environ, "DASHBOARD_BIND_CONTEXT").lower()
    container_internal = context in _CONTAINER_INTERNAL_BIND_CONTEXTS
    return {
        "source": "DASHBOARD_HOST",
        "bind_host": host,
        "bind_context": context or "host_process",
        "container_internal": bool(container_internal),
        "non_loopback": not _is_loopback_bind_host(host),
        "wildcard": _is_wildcard_bind_host(host),
    }


def public_network_exposure_snapshot(
    *,
    engine_mode: Optional[str] = None,
    environ: Optional[dict[str, Any]] = None,
) -> Dict[str, Any]:
    env = dict(os.environ if environ is None else environ)
    mode = _normalize_mode(engine_mode if engine_mode is not None else env.get("ENGINE_MODE"), "safe")
    required = _production_network_exposure_required(env, mode)
    ack = public_network_exposure_ack_snapshot(environ=env)
    services: list[dict[str, Any]] = []
    blockers: list[str] = []

    for service in ("timescale", "redis", "minio", "minio_console", "dashboard", "operator"):
        host, host_source, host_explicit = _service_bind_host(env, service)
        port_configured, port_env, port = _service_port_configured(env, service)
        allow_env = _service_allow_env(service)
        service_allow = _env_bool_from(env, allow_env, False)
        host_is_loopback = _is_loopback_bind_host(host)
        host_is_wildcard = _is_wildcard_bind_host(host)
        public_host_bind = bool(port_configured and not host_is_loopback)
        service_blockers: list[str] = []
        if required and public_host_bind:
            if not service_allow:
                service_blockers.append(f"{allow_env}_required")
            if not bool(ack.get("ok")):
                service_blockers.extend(str(item) for item in list(ack.get("blockers") or []))
            if service_blockers:
                if host_is_wildcard:
                    blockers.append(f"{service}_0_0_0_0_without_approved_exposure")
                else:
                    blockers.append(f"{service}_lan_bind_without_approved_exposure")
                blockers.extend(service_blockers)

        services.append(
            {
                "service": service,
                "bind_host": host,
                "bind_host_source": host_source,
                "bind_host_explicit": bool(host_explicit),
                "port_env": port_env,
                "port": port,
                "port_configured": bool(port_configured),
                "loopback": bool(host_is_loopback),
                "wildcard": bool(host_is_wildcard),
                "public_host_bind": bool(public_host_bind),
                "allow_env": allow_env,
                "allow_dangerous_public_bind": bool(service_allow),
                "approved": bool((not public_host_bind) or (service_allow and ack.get("ok"))),
                "blockers": service_blockers,
            }
        )

    dashboard_process = _dashboard_process_bind_state(env)
    if dashboard_process is not None:
        if (
            required
            and bool(dashboard_process.get("non_loopback"))
            and not bool(dashboard_process.get("container_internal"))
        ):
            allow_env = _service_allow_env("dashboard")
            service_allow = _env_bool_from(env, allow_env, False)
            if not service_allow or not bool(ack.get("ok")):
                blockers.append("dashboard_process_bind_without_approved_exposure")
                if bool(dashboard_process.get("wildcard")):
                    blockers.append("dashboard_0_0_0_0_without_approved_exposure")
                else:
                    blockers.append("dashboard_lan_bind_without_approved_exposure")
                if not service_allow:
                    blockers.append(f"{allow_env}_required")
                if not bool(ack.get("ok")):
                    blockers.extend(str(item) for item in list(ack.get("blockers") or []))

    blockers = list(dict.fromkeys(blockers))
    public_services = [
        str(item.get("service"))
        for item in services
        if bool(item.get("public_host_bind"))
    ]
    return {
        "ok": not blockers,
        "required": bool(required),
        "mode": mode,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "ack": dict(ack or {}),
        "services": services,
        "public_services": public_services,
        "dashboard_process_bind": dashboard_process or {},
    }


def operator_sidecar_security_snapshot(
    *,
    engine_mode: Optional[str] = None,
    operator_bind_host: Optional[str] = None,
    operator_api_token: Optional[str] = None,
    operator_public_port: Optional[str] = None,
    internal_only: Optional[bool] = None,
) -> Dict[str, Any]:
    mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")
    bind_host = str(
        operator_bind_host
        if operator_bind_host is not None
        else os.environ.get("OPERATOR_BIND_HOST", "")
    ).strip()
    public_port = str(
        operator_public_port
        if operator_public_port is not None
        else os.environ.get("OPERATOR_PUBLIC_PORT", "")
    ).strip()
    token = str(
        operator_api_token
        if operator_api_token is not None
        else operator_api_token_from_env()
    ).strip()
    if internal_only is None:
        internal_only = _env_bool("OPERATOR_SIDECAR_INTERNAL_ONLY", False)
    allow_public_bind = _env_bool("OPERATOR_ALLOW_DANGEROUS_PUBLIC_BIND", False)

    blockers: list[str] = []
    exposure: Dict[str, Any] = {}
    operator_state: Dict[str, Any] = {}
    operator_public_approved = False
    if public_port:
        exposure = public_network_exposure_snapshot(engine_mode=mode)
        operator_state = next(
            (
                dict(item)
                for item in list(exposure.get("services") or [])
                if str(dict(item).get("service") or "") == "operator"
            ),
            {},
        )
        operator_public_approved = bool(
            operator_state.get("public_host_bind") and operator_state.get("approved")
        )
    token_issue = operator_api_token_issue(token)
    if token_issue:
        blockers.append(token_issue)
    if bool(internal_only):
        if public_port:
            blockers.append("operator_public_port_ignored_internal_only")
        if allow_public_bind:
            blockers.append("operator_public_bind_flag_ignored_internal_only")
    if bind_host and _operator_bind_host_is_unsafe(bind_host) and not bool(internal_only) and not operator_public_approved:
        blockers.append("operator_bind_host_public_without_internal_only")
    if public_port:
        if not bool(internal_only):
            blockers.append("operator_sidecar_public_port_forbidden")
        if bool(operator_state.get("public_host_bind")) and not bool(operator_state.get("approved")):
            blockers.append("operator_sidecar_public_port_forbidden_without_approved_exposure")

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required": mode == "live",
        "mode": mode,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "operator_bind_host": bind_host,
        "operator_public_port_configured": bool(public_port),
        "operator_api_token_configured": bool(token),
        "operator_api_token_issue": token_issue,
        "internal_only": bool(internal_only),
        "operator_allow_dangerous_public_bind": bool(allow_public_bind),
    }


def lob_deeplob_shadow_readiness_snapshot(
    *,
    engine_mode: Optional[str] = None,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")
    try:
        from engine.execution.lob_simulation import deeplob_shadow_enabled, lob_deeplob_readiness_snapshot
    except Exception as exc:
        required = _env_bool("EXEC_LOB_DEEPLOB_SHADOW_ENABLED", False)
        blockers = [f"lob_deeplob_readiness_unavailable:{type(exc).__name__}"] if required else []
        return {
            "ok": not blockers,
            "required": bool(required),
            "enabled": bool(required),
            "mode": mode,
            "shadow_only": True,
            "reason": "ok" if not blockers else blockers[0],
            "blockers": blockers,
        }

    enabled = bool(deeplob_shadow_enabled())
    if not enabled:
        return {
            "ok": True,
            "required": False,
            "enabled": False,
            "mode": mode,
            "shadow_only": True,
            "reason": "disabled",
            "blockers": [],
        }

    con = None
    try:
        from engine.runtime.storage import connect

        con = connect(readonly=True)
        snapshot = dict(
            lob_deeplob_readiness_snapshot(
                con,
                symbol=symbol or os.environ.get("EXEC_LOB_PREFLIGHT_SYMBOL") or None,
            )
            or {}
        )
    except Exception as exc:
        snapshot = {
            "ok": False,
            "required": True,
            "enabled": True,
            "shadow_only": True,
            "reason": f"lob_deeplob_readiness_failed:{type(exc).__name__}",
            "blockers": [f"lob_deeplob_readiness_failed:{type(exc).__name__}"],
        }
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                # no-op-guard: allow best-effort close after readiness snapshot construction.
                pass

    snapshot["mode"] = mode
    snapshot["required"] = True
    snapshot["enabled"] = True
    snapshot["shadow_only"] = True
    return snapshot


def production_secret_sources_snapshot() -> Dict[str, Any]:
    """Return production secret-source policy state without exposing values."""

    try:
        from engine.runtime.secret_sources import secret_source_policy_snapshot

        return dict(secret_source_policy_snapshot(validate_files=True) or {})
    except Exception as exc:
        return {
            "ok": False,
            "required": True,
            "reason": f"secret_source_policy_unavailable:{type(exc).__name__}",
            "blockers": [f"secret_source_policy_unavailable:{type(exc).__name__}"],
            "error": str(exc),
        }


def dsn_context_preflight_snapshot() -> Dict[str, Any]:
    """Return redacted host/container DSN context validation state."""

    try:
        from engine.runtime.dsn_context import dsn_context_snapshot

        return dict(dsn_context_snapshot() or {})
    except Exception as exc:
        return {
            "ok": False,
            "required": True,
            "context": "unknown",
            "reason": f"dsn_context_preflight_unavailable:{type(exc).__name__}",
            "blockers": [f"dsn_context_preflight_unavailable:{type(exc).__name__}"],
            "warnings": [],
            "entries": [],
        }


def live_confirmation_snapshot(
    *,
    engine_mode: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return the canonical live-confirmation contract.

    Live confirmation is intentionally not runtime-configurable. Operators arm
    it by setting LIVE_TRADING_CONFIRM to the built-in phrase in deployment
    configuration for the target host.
    """

    mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")
    confirm = str(
        live_confirm
        if live_confirm is not None
        else os.environ.get("LIVE_TRADING_CONFIRM", "")
    ).strip()
    phrase = _confirmation_phrase()
    phrase_override = str(os.environ.get("LIVE_TRADING_CONFIRM_PHRASE") or "").strip()
    if require_confirmation is None:
        requested_required, invalid_required, raw_required = _env_bool_snapshot(
            "LIVE_TRADING_REQUIRE_CONFIRMATION",
            True,
        )
    else:
        requested_required = bool(require_confirmation)
        invalid_required = False
        raw_required = "1" if requested_required else "0"

    blockers: list[str] = []
    required = bool(mode == "live")
    if required:
        if invalid_required:
            blockers.append("live_trading_confirmation_requirement_invalid")
        if phrase_override:
            blockers.append("live_trading_confirmation_phrase_override_forbidden")
        if not requested_required:
            blockers.append("live_trading_confirmation_cannot_be_disabled")
        if confirm != phrase:
            blockers.append("live_trading_confirmation_required")

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required": required,
        "mode": mode,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "expected_phrase": phrase,
        "configured": bool(confirm),
        "require_confirmation_raw": raw_required,
        "require_confirmation_requested": bool(requested_required),
        "require_confirmation_invalid": bool(invalid_required),
        "phrase_override_configured": bool(phrase_override),
    }


def assert_live_trading_confirmation(
    *,
    engine_mode: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    state = live_confirmation_snapshot(
        engine_mode=engine_mode,
        live_confirm=live_confirm,
        require_confirmation=require_confirmation,
    )
    if not bool(state.get("ok")):
        blockers = ",".join(str(item) for item in list(state.get("blockers") or []))
        raise RuntimeError(f"live_trading_confirmation_failed:{blockers}")
    return state


def assert_live_execution_arming_preflight(
    *,
    engine_mode: Optional[str] = "live",
    execution_mode: Optional[str] = None,
    dashboard_host: Optional[str] = None,
    dashboard_api_token: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_dashboard_api_token: Optional[bool] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    """Require the full live-capital preflight before writing `armed=1`.

    This guard intentionally runs before the execution-mode row is updated. The
    audit-chain check inside `live_trading_preflight()` is still enforced after
    arming by the barrier; before arming, it is normally not required because the
    DB row has not yet been signed off.
    """

    state = live_trading_preflight(
        engine_mode=engine_mode,
        execution_mode=execution_mode,
        dashboard_host=dashboard_host,
        dashboard_api_token=dashboard_api_token,
        live_confirm=live_confirm,
        require_dashboard_api_token=require_dashboard_api_token,
        require_confirmation=require_confirmation,
    )
    if not bool(state.get("ok")):
        blockers = ",".join(str(item) for item in list(state.get("blockers") or []))
        raise RuntimeError(f"live_execution_arming_preflight_failed:{blockers}")
    return state


def _execution_mode_state() -> Dict[str, Any]:
    try:
        from engine.cache.wrappers.execution_mode import read_execution_mode

        state = dict(read_execution_mode() or {})
        state.setdefault("source", "engine.cache.wrappers.execution_mode")
        return state
    except Exception as exc:
        return {
            "mode": "",
            "armed": None,
            "source": "unavailable",
            "error": str(exc),
        }


def _initial_kill_switch_hold_snapshot(*, engine_mode: str) -> Dict[str, Any]:
    raw = str(os.environ.get("KILL_SWITCH_GLOBAL", "") or "").strip()
    armed = bool(env_flag_truthy("KILL_SWITCH_GLOBAL", False))
    db_state = _execution_mode_state()
    db_mode = str(db_state.get("mode") or "").strip().lower()
    try:
        db_armed = int(db_state.get("armed") or 0) == 1
    except Exception:
        db_armed = False
    signed_off = bool(db_mode == "live" and db_armed)
    required = bool(str(engine_mode or "").strip().lower() == "live" and not signed_off)
    ok = bool((not required) or armed)
    reason = "ok" if ok else "kill_switch_global_initial_hold_required"
    return {
        "ok": ok,
        "required": required,
        "reason": reason,
        "armed": armed,
        "signed_off": signed_off,
        "db_execution_mode": db_state,
        "env": {"KILL_SWITCH_GLOBAL": raw},
    }


def _live_armed(state: Dict[str, Any]) -> bool:
    mode = str(state.get("mode") or "").strip().lower()
    try:
        armed = int(state.get("armed") or 0) == 1
    except Exception:
        armed = False
    return bool(mode == "live" and armed)


def _audit_bytes_or_none(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return bytes(value)
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, str):
        try:
            return bytes.fromhex(value)
        except ValueError:
            return value.encode("utf-8")
    return bytes(value)


def _audit_hash_hex(value: Any) -> str | None:
    raw = _audit_bytes_or_none(value)
    return raw.hex() if raw else None


def _audit_row_dict(row: Any, columns: list[str]) -> Dict[str, Any]:
    if hasattr(row, "keys"):
        try:
            return {str(key): row[key] for key in row.keys()}
        except (AttributeError, TypeError, KeyError, IndexError):
            pass
    return {str(columns[idx]): row[idx] for idx in range(min(len(columns), len(row)))}


def _execution_mode_audit_public_row(row: Dict[str, Any], *, chain_index: int) -> Dict[str, Any]:
    return {
        "chain_index": int(chain_index),
        "ts_ms": int(row.get("ts_ms") or 0),
        "prev_mode": str(row.get("prev_mode") or ""),
        "new_mode": str(row.get("new_mode") or ""),
        "actor": str(row.get("actor") or ""),
        "reason": str(row.get("reason") or ""),
        "prev_armed": int(row.get("prev_armed") or 0),
        "new_armed": int(row.get("new_armed") or 0),
        "prev_hash": _audit_hash_hex(row.get("prev_hash")),
        "row_hash": _audit_hash_hex(row.get("row_hash")),
        "prev_hash_present": row.get("prev_hash") is not None,
        "row_hash_present": row.get("row_hash") is not None,
    }


def _execution_mode_audit_chain_snapshot(con) -> Dict[str, Any]:
    """Recompute the execution-mode audit hash chain in canonical row order."""

    from engine.audit.chain import coerce_row_for_hash, order_by_clause, row_identifier, table_columns
    from engine.audit.hashing import compute_row_hash

    required_columns = {
        "ts_ms",
        "prev_mode",
        "new_mode",
        "actor",
        "reason",
        "prev_armed",
        "new_armed",
        "prev_hash",
        "row_hash",
    }
    columns = table_columns(con, "execution_mode_audit")
    column_names = [col.name for col in columns]
    missing_columns = sorted(required_columns.difference(column_names))
    blockers: list[str] = []
    findings: list[Dict[str, Any]] = []
    if missing_columns:
        blockers.append("execution_mode_audit_schema_missing_hash_columns")
        return {
            "ok": False,
            "reason": blockers[0],
            "blockers": blockers,
            "rows_verified": 0,
            "missing_columns": missing_columns,
            "findings": findings,
            "latest_row": {},
            "latest_live_armed_row": {},
        }

    order_sql = order_by_clause(con, "execution_mode_audit")
    raw_rows = con.execute(f"SELECT * FROM execution_mode_audit {order_sql}").fetchall() or []
    rows = [
        coerce_row_for_hash(_audit_row_dict(raw, column_names), columns)
        for raw in raw_rows
    ]
    hash_positions: dict[bytes, list[int]] = {}
    for idx, row in enumerate(rows):
        actual_hash = _audit_bytes_or_none(row.get("row_hash"))
        if actual_hash is not None:
            hash_positions.setdefault(actual_hash, []).append(idx)

    def _add_finding(row: Dict[str, Any], idx: int, finding: str, expected: bytes | None, actual: bytes | None) -> None:
        findings.append(
            {
                "row_id": row_identifier(row, idx + 1),
                "chain_index": int(idx + 1),
                "finding": str(finding),
                "expected_hash": expected.hex() if expected else None,
                "actual_hash": actual.hex() if actual else None,
                "ts_ms": int(row.get("ts_ms") or 0),
                "new_mode": str(row.get("new_mode") or ""),
                "new_armed": int(row.get("new_armed") or 0),
            }
        )

    prev_actual: bytes | None = None
    for idx, row in enumerate(rows):
        stored_prev = _audit_bytes_or_none(row.get("prev_hash"))
        actual = _audit_bytes_or_none(row.get("row_hash"))
        expected_prev = prev_actual
        if actual is None:
            _add_finding(row, idx, "row_hash_missing", None, actual)
        if stored_prev != expected_prev:
            if stored_prev is None and expected_prev is not None:
                _add_finding(row, idx, "prev_hash_missing", expected_prev, stored_prev)
            elif stored_prev in hash_positions and not any(pos == idx - 1 for pos in hash_positions[stored_prev]):
                _add_finding(row, idx, "chain_order_broken", expected_prev, stored_prev)
            else:
                _add_finding(row, idx, "prev_hash_mismatch", expected_prev, stored_prev)
        expected_hash = compute_row_hash(expected_prev, row)
        if actual != expected_hash:
            _add_finding(row, idx, "row_hash_mismatch", expected_hash, actual)
        prev_actual = actual

    finding_blockers = {
        "row_hash_missing": "execution_mode_audit_row_hash_missing",
        "row_hash_mismatch": "execution_mode_audit_row_hash_mismatch",
        "prev_hash_missing": "execution_mode_audit_prev_hash_missing",
        "prev_hash_mismatch": "execution_mode_audit_prev_hash_mismatch",
        "chain_order_broken": "execution_mode_audit_chain_order_broken",
    }
    blockers.extend(
        finding_blockers.get(str(finding.get("finding")), "execution_mode_audit_chain_invalid")
        for finding in findings
    )
    blockers = list(dict.fromkeys(blockers))

    latest_row = _execution_mode_audit_public_row(rows[-1], chain_index=len(rows)) if rows else {}
    latest_live_armed_row: Dict[str, Any] = {}
    for idx in range(len(rows) - 1, -1, -1):
        row = rows[idx]
        if str(row.get("new_mode") or "").strip().lower() == "live" and int(row.get("new_armed") or 0) == 1:
            latest_live_armed_row = _execution_mode_audit_public_row(row, chain_index=idx + 1)
            break

    return {
        "ok": not blockers,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "rows_verified": len(rows),
        "missing_columns": missing_columns,
        "findings": findings,
        "latest_row": latest_row,
        "latest_live_armed_row": latest_live_armed_row,
    }


def _execution_arming_audit_snapshot(*, engine_mode: str) -> Dict[str, Any]:
    """Verify live arming came from the persisted audit-chain DB path."""

    mode = _normalize_mode(engine_mode, "safe")
    cached_state = _execution_mode_state()
    cached_live_armed = _live_armed(cached_state)
    required = bool(mode == "live" and cached_live_armed)
    chain_required = bool(required)
    blockers: list[str] = []
    db_state: Dict[str, Any] = {}
    audit_state: Dict[str, Any] = {}
    audit_chain_state: Dict[str, Any] = {}

    try:
        from engine.runtime.storage import connect

        con = connect(readonly=True)
        try:
            row = con.execute(
                "SELECT mode, armed, updated_ts_ms, actor, reason FROM execution_mode WHERE id=1"
            ).fetchone()
            if row:
                db_state = {
                    "mode": str(row[0] or ""),
                    "armed": int(row[1] or 0),
                    "updated_ts_ms": int(row[2] or 0),
                    "actor": str(row[3] or ""),
                    "reason": str(row[4] or ""),
                    "source": "engine.runtime.storage",
                }
            else:
                db_state = {
                    "mode": "",
                    "armed": 0,
                    "updated_ts_ms": 0,
                    "actor": "",
                    "reason": "missing",
                    "source": "engine.runtime.storage",
                }

            db_live_armed = _live_armed(db_state)
            required = bool(mode == "live" and (cached_live_armed or db_live_armed))
            db_live_mode = str(db_state.get("mode") or "").strip().lower() == "live"
            chain_required = bool(mode == "live" and (required or db_live_mode))
            if required and not db_live_armed:
                blockers.append("execution_mode_live_armed_not_from_db")

            if chain_required:
                audit_chain_state = _execution_mode_audit_chain_snapshot(con)
                if not bool(audit_chain_state.get("ok")):
                    blockers.extend(str(item) for item in list(audit_chain_state.get("blockers") or []))

            if db_live_armed:
                latest_row = dict(audit_chain_state.get("latest_row") or {})
                latest_live_armed_row = dict(audit_chain_state.get("latest_live_armed_row") or {})
                if not latest_live_armed_row:
                    blockers.append("execution_mode_live_armed_audit_missing")
                else:
                    audit_state = latest_live_armed_row
                    if latest_row and int(latest_row.get("chain_index") or 0) != int(audit_state.get("chain_index") or 0):
                        blockers.append("execution_mode_live_armed_latest_audit_missing")
                    mismatches = []
                    if int(audit_state["ts_ms"]) != int(db_state.get("updated_ts_ms") or 0):
                        mismatches.append("ts_ms")
                    if str(audit_state["new_mode"]) != str(db_state.get("mode") or ""):
                        mismatches.append("mode")
                    if int(audit_state["new_armed"]) != int(db_state.get("armed") or 0):
                        mismatches.append("armed")
                    if str(audit_state["actor"]) != str(db_state.get("actor") or ""):
                        mismatches.append("actor")
                    if str(audit_state["reason"]) != str(db_state.get("reason") or ""):
                        mismatches.append("reason")
                    if not bool(audit_state["row_hash_present"]):
                        blockers.append("execution_mode_live_armed_audit_hash_missing")
                    if mismatches:
                        audit_state["mismatches"] = list(mismatches)
                        blockers.append("execution_mode_live_armed_audit_mismatch")
        finally:
            con.close()
    except Exception as exc:
        if required:
            blockers.append("execution_mode_live_armed_audit_unavailable")
        db_state.setdefault("source", "engine.runtime.storage")
        db_state["error"] = str(exc)

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required": bool(required),
        "mode": mode,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "cached_execution_mode": dict(cached_state or {}),
        "db_execution_mode": dict(db_state or {}),
        "audit": dict(audit_state or {}),
        "audit_chain": dict(audit_chain_state or {}),
    }


def live_environment_contract_snapshot(
    *,
    engine_mode: Optional[str] = None,
    execution_mode: Optional[str] = None,
    dashboard_host: Optional[str] = None,
    dashboard_api_token: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_dashboard_api_token: Optional[bool] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return the live deployment env contract without backup/reconcile gates."""

    mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")
    exec_mode = _normalize_mode(
        execution_mode if execution_mode is not None else os.environ.get("EXECUTION_MODE"),
        "safe",
    )
    token = str(
        dashboard_api_token
        if dashboard_api_token is not None
        else dashboard_api_token_from_env()
    ).strip()
    confirm = str(
        live_confirm
        if live_confirm is not None
        else os.environ.get("LIVE_TRADING_CONFIRM", "")
    ).strip()
    host = str(
        dashboard_host
        if dashboard_host is not None
        else os.environ.get("DASHBOARD_HOST", default_dashboard_host())
    ).strip() or default_dashboard_host()
    require_token = (
        bool(require_dashboard_api_token)
        if require_dashboard_api_token is not None
        else _env_bool("LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN", True)
    )

    blockers: list[str] = []
    if mode == "live" and exec_mode != "live":
        blockers.append("execution_mode_live_required")

    broker_contract = live_broker_environment_contract(engine_mode=mode)
    if mode == "live" and not bool(broker_contract.get("ok")):
        blockers.extend(str(item) for item in list(broker_contract.get("blockers") or []))

    broker_preflight = broker_startup_preflight(engine_mode=mode)
    if mode == "live" and not bool(broker_preflight.get("ok")):
        blockers.extend(str(item) for item in list(broker_preflight.get("blockers") or []))

    initial_kill_switch_hold = _initial_kill_switch_hold_snapshot(engine_mode=mode)
    if mode == "live" and not bool(initial_kill_switch_hold.get("ok")):
        blockers.append(str(initial_kill_switch_hold.get("reason") or "kill_switch_global_initial_hold_required"))

    token_issue = dashboard_api_token_issue(token, strict=(mode == "live"))
    if host not in LOOPBACK_HOSTS and not token:
        blockers.append("dashboard_api_token_required_for_remote_bind")
    elif host not in LOOPBACK_HOSTS and token_issue:
        blockers.append(f"dashboard_api_token_invalid_for_remote_bind:{token_issue}")
    if mode == "live" and require_token:
        if not token:
            blockers.append("dashboard_api_token_required_for_live")
        elif token_issue:
            blockers.append(f"dashboard_api_token_invalid_for_live:{token_issue}")
    confirmation = live_confirmation_snapshot(
        engine_mode=mode,
        live_confirm=confirm,
        require_confirmation=require_confirmation,
    )
    if mode == "live" and not bool(confirmation.get("ok")):
        blockers.extend(str(item) for item in list(confirmation.get("blockers") or []))
    operator_sidecar_security = operator_sidecar_security_snapshot(engine_mode=mode)
    if mode == "live" and not bool(operator_sidecar_security.get("ok")):
        blockers.extend(str(item) for item in list(operator_sidecar_security.get("blockers") or []))
    public_network_exposure = public_network_exposure_snapshot(engine_mode=mode)
    if bool(public_network_exposure.get("required")) and not bool(public_network_exposure.get("ok")):
        blockers.extend(str(item) for item in list(public_network_exposure.get("blockers") or []))
    secret_sources = production_secret_sources_snapshot()
    if bool(secret_sources.get("required")) and not bool(secret_sources.get("ok")):
        blockers.extend(str(item) for item in list(secret_sources.get("blockers") or []))
    dsn_context = dsn_context_preflight_snapshot()
    if bool(dsn_context.get("required")) and not bool(dsn_context.get("ok")):
        blockers.extend(str(item) for item in list(dsn_context.get("blockers") or []))
    phrase = _confirmation_phrase()

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "mode": mode,
        "execution_mode": exec_mode,
        "required": mode == "live",
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "dashboard_host": host,
        "dashboard_api_token_configured": bool(token),
        "dashboard_api_token_issue": token_issue,
        "confirmation_required": bool(confirmation.get("required")),
        "confirmation_phrase": phrase if bool(confirmation.get("required")) else "",
        "confirmation": dict(confirmation or {}),
        "broker_contract": dict(broker_contract or {}),
        "broker_preflight": dict(broker_preflight or {}),
        "initial_kill_switch_hold": dict(initial_kill_switch_hold or {}),
        "operator_sidecar_security": dict(operator_sidecar_security or {}),
        "public_network_exposure": dict(public_network_exposure or {}),
        "secret_sources": dict(secret_sources or {}),
        "dsn_context": dict(dsn_context or {}),
    }


def live_trading_preflight(
    *,
    engine_mode: Optional[str] = None,
    execution_mode: Optional[str] = None,
    dashboard_host: Optional[str] = None,
    dashboard_api_token: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_dashboard_api_token: Optional[bool] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return the live-trading preflight state without mutating runtime state."""

    mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")
    exec_mode = _normalize_mode(
        execution_mode if execution_mode is not None else os.environ.get("EXECUTION_MODE"),
        "safe",
    )
    token = str(
        dashboard_api_token
        if dashboard_api_token is not None
        else dashboard_api_token_from_env()
    ).strip()
    confirm = str(
        live_confirm
        if live_confirm is not None
        else os.environ.get("LIVE_TRADING_CONFIRM", "")
    ).strip()
    host = str(
        dashboard_host
        if dashboard_host is not None
        else os.environ.get("DASHBOARD_HOST", default_dashboard_host())
    ).strip() or default_dashboard_host()
    require_token = (
        bool(require_dashboard_api_token)
        if require_dashboard_api_token is not None
        else _env_bool("LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN", True)
    )
    contract = live_environment_contract_snapshot(
        engine_mode=mode,
        execution_mode=exec_mode,
        dashboard_host=host,
        dashboard_api_token=token,
        live_confirm=confirm,
        require_dashboard_api_token=require_token,
        require_confirmation=require_confirmation,
    )
    blockers = list(contract.get("blockers") or [])
    if mode == "live" and live_execution_disabled():
        blockers.append(DISABLE_LIVE_EXECUTION_REASON)

    prelive_reconcile = prelive_reconcile_policy_snapshot(
        engine_mode=mode,
        source="engine.runtime.live_trading_preflight",
    )
    if mode == "live" and not bool(prelive_reconcile.get("ok")):
        blockers.extend(str(item) for item in list(prelive_reconcile.get("blockers") or []))

    position_reconcile_evidence = position_reconcile_evidence_snapshot(engine_mode=mode)
    if mode == "live" and not bool(position_reconcile_evidence.get("ok")):
        blockers.extend(str(item) for item in list(position_reconcile_evidence.get("blockers") or []))

    broker_shutdown_policy = broker_shutdown_policy_snapshot(engine_mode=mode)
    if mode == "live" and not bool(broker_shutdown_policy.get("ok")):
        blockers.extend(str(item) for item in list(broker_shutdown_policy.get("blockers") or []))

    backup_restore_evidence = backup_restore_evidence_snapshot(engine_mode=mode)
    if mode == "live" and not bool(backup_restore_evidence.get("ok")):
        blockers.extend(str(item) for item in list(backup_restore_evidence.get("blockers") or []))

    wal_archiver_runtime = wal_archiver_runtime_snapshot(engine_mode=mode)
    if mode == "live" and not bool(wal_archiver_runtime.get("ok")):
        blockers.extend(str(item) for item in list(wal_archiver_runtime.get("blockers") or []))

    clock_health = clock_health_snapshot(engine_mode=mode)
    if mode == "live" and not bool(clock_health.get("ok")):
        blockers.extend(str(item) for item in list(clock_health.get("blockers") or []))

    execution_arming_audit = _execution_arming_audit_snapshot(engine_mode=mode)
    if mode == "live" and not bool(execution_arming_audit.get("ok")):
        blockers.extend(str(item) for item in list(execution_arming_audit.get("blockers") or []))

    live_ai_safety = live_ai_safety_snapshot(
        engine_mode=mode,
        execution_mode=exec_mode,
        broker=os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or os.environ.get("LIVE_BROKER") or "",
    )
    if mode == "live" and not bool(live_ai_safety.get("ok")):
        blockers.extend(str(item) for item in list(live_ai_safety.get("blockers") or []))

    lob_deeplob_shadow = lob_deeplob_shadow_readiness_snapshot(engine_mode=mode)
    if mode == "live" and bool(lob_deeplob_shadow.get("enabled")) and not bool(lob_deeplob_shadow.get("ok")):
        blockers.extend(str(item) for item in list(lob_deeplob_shadow.get("blockers") or []))

    options_instruments = live_options_readiness_snapshot(
        engine_mode=mode,
        execution_mode=exec_mode,
        broker=os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or os.environ.get("LIVE_BROKER") or "",
    )
    if mode == "live" and bool(options_instruments.get("required")) and not bool(options_instruments.get("ok")):
        blockers.extend(str(item) for item in list(options_instruments.get("blockers") or []))

    phrase = _confirmation_phrase()
    blockers = list(dict.fromkeys(blockers))

    return {
        "ok": not blockers,
        "mode": mode,
        "execution_mode": exec_mode,
        "required": mode == "live",
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "dashboard_host": host,
        "dashboard_api_token_configured": bool(token),
        "confirmation_required": bool((contract.get("confirmation") or {}).get("required")),
        "confirmation_phrase": phrase if bool((contract.get("confirmation") or {}).get("required")) else "",
        "confirmation": dict(contract.get("confirmation") or {}),
        "deployment_contract": dict(contract or {}),
        "prelive_reconcile": dict(prelive_reconcile or {}),
        "position_reconcile_evidence": dict(position_reconcile_evidence or {}),
        "broker_shutdown_policy": dict(broker_shutdown_policy or {}),
        "broker_contract": dict(contract.get("broker_contract") or {}),
        "broker_preflight": dict(contract.get("broker_preflight") or {}),
        "initial_kill_switch_hold": dict(contract.get("initial_kill_switch_hold") or {}),
        "operator_sidecar_security": dict(contract.get("operator_sidecar_security") or {}),
        "public_network_exposure": dict(contract.get("public_network_exposure") or {}),
        "secret_sources": dict(contract.get("secret_sources") or {}),
        "dsn_context": dict(contract.get("dsn_context") or {}),
        "backup_restore_evidence": dict(backup_restore_evidence or {}),
        "wal_archiver_runtime": dict(wal_archiver_runtime or {}),
        "clock_health": dict(clock_health or {}),
        "execution_arming_audit": dict(execution_arming_audit or {}),
        "live_ai_safety": dict(live_ai_safety or {}),
        "lob_deeplob_shadow": dict(lob_deeplob_shadow or {}),
        "options_instruments": dict(options_instruments or {}),
    }


def assert_dashboard_security_config(
    *,
    engine_mode: Optional[str] = None,
    dashboard_host: Optional[str] = None,
    dashboard_api_token: Optional[str] = None,
    live_confirm: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate dashboard security settings and raise RuntimeError on failure."""

    state = live_trading_preflight(
        engine_mode=engine_mode,
        dashboard_host=dashboard_host,
        dashboard_api_token=dashboard_api_token,
        live_confirm=live_confirm,
    )
    if not bool(state.get("ok")):
        blockers = ",".join(str(x) for x in state.get("blockers") or [])
        raise RuntimeError(f"dashboard_security_preflight_failed:{blockers}")
    return state


__all__ = [
    "DEFAULT_LIVE_CONFIRM_PHRASE",
    "DEFAULT_PUBLIC_NETWORK_EXPOSURE_ACK_PHRASE",
    "assert_live_execution_arming_preflight",
    "assert_live_trading_confirmation",
    "assert_dashboard_security_config",
    "live_confirmation_snapshot",
    "live_environment_contract_snapshot",
    "live_trading_preflight",
    "lob_deeplob_shadow_readiness_snapshot",
    "operator_api_token_issue",
    "dsn_context_preflight_snapshot",
    "operator_sidecar_security_snapshot",
    "production_secret_sources_snapshot",
    "public_network_exposure_ack_snapshot",
    "public_network_exposure_snapshot",
]
