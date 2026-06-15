from __future__ import annotations

"""Central fail-closed checks required before live trading can be enabled."""

import os
from typing import Any, Dict, Optional

from engine.api.auth_config import dashboard_api_token_issue
from engine.runtime.platform import LOOPBACK_HOSTS, default_dashboard_host


_TRUTHY_VALUES = {"1", "true", "yes", "on"}
DEFAULT_LIVE_CONFIRM_PHRASE = "I_UNDERSTAND_LIVE_TRADING"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in _TRUTHY_VALUES


def _normalize_mode(value: Any, default: str = "safe") -> str:
    mode = str(value or default).strip().lower() or default
    return mode if mode in {"safe", "paper", "shadow", "live", "dev", "development"} else default


def _confirmation_phrase() -> str:
    return str(os.environ.get("LIVE_TRADING_CONFIRM_PHRASE") or DEFAULT_LIVE_CONFIRM_PHRASE).strip()


def live_trading_preflight(
    *,
    engine_mode: Optional[str] = None,
    dashboard_host: Optional[str] = None,
    dashboard_api_token: Optional[str] = None,
    live_confirm: Optional[str] = None,
    require_dashboard_api_token: Optional[bool] = None,
    require_confirmation: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return the live-trading preflight state without mutating runtime state."""

    mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")
    token = str(
        dashboard_api_token
        if dashboard_api_token is not None
        else os.environ.get("DASHBOARD_API_TOKEN", "")
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
    require_confirm = (
        bool(require_confirmation)
        if require_confirmation is not None
        else _env_bool("LIVE_TRADING_REQUIRE_CONFIRMATION", True)
    )

    blockers = []
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
    phrase = _confirmation_phrase()
    if mode == "live" and require_confirm and confirm != phrase:
        blockers.append("live_trading_confirmation_required")

    return {
        "ok": not blockers,
        "mode": mode,
        "required": mode == "live",
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "dashboard_host": host,
        "dashboard_api_token_configured": bool(token),
        "confirmation_required": bool(mode == "live" and require_confirm),
        "confirmation_phrase": phrase if mode == "live" and require_confirm else "",
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
    "assert_dashboard_security_config",
    "live_trading_preflight",
]
