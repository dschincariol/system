"""Shared execution-mode parsing and live broker boundary checks."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

CANONICAL_EXECUTION_MODES = ("safe", "paper", "shadow", "live")
PERSISTED_EXECUTION_MODES = CANONICAL_EXECUTION_MODES
EXECUTION_MODE_ENV_KEYS = ("EXECUTION_MODE", "ENGINE_MODE", "OPERATOR_MODE", "MODE")
PRIMARY_EXECUTION_MODE_ENV_KEYS = ("EXECUTION_MODE", "ENGINE_MODE")
SECONDARY_EXECUTION_MODE_ENV_KEYS = ("OPERATOR_MODE", "MODE")

_MODE_ALIASES = {
    "safe": "safe",
    "dev": "safe",
    "development": "safe",
    "local": "safe",
    "test": "safe",
    "paper": "paper",
    "sim": "paper",
    "simulation": "paper",
    "sim-paper": "paper",
    "sim_paper": "paper",
    "sandbox": "paper",
    "shadow": "shadow",
    "live": "live",
}
_MODE_RANK = {
    "safe": 4,
    "paper": 3,
    "shadow": 2,
    "live": 1,
}


@dataclass(frozen=True)
class ExecutionModeParse:
    raw: str
    mode: str
    valid: bool
    missing: bool
    source: str
    reason: str

    def diagnostic(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "raw": _safe_mode_text(self.raw),
            "mode": self.mode,
            "valid": bool(self.valid),
            "missing": bool(self.missing),
            "reason": self.reason,
        }


def _safe_mode_text(value: Any) -> str:
    text = str(value or "").strip()
    cleaned = "".join(ch if ch.isprintable() else "?" for ch in text)
    return cleaned[:80]


def parse_execution_mode(
    value: Any,
    *,
    default: Optional[str] = "safe",
    source: str = "mode",
) -> ExecutionModeParse:
    raw = "" if value is None else str(value)
    text = raw.strip().lower()
    if not text:
        if default is None:
            return ExecutionModeParse(
                raw=raw,
                mode="safe",
                valid=False,
                missing=True,
                source=str(source or "mode"),
                reason="missing_execution_mode",
            )
        default_parsed = parse_execution_mode(default, default="safe", source=f"{source}:default")
        mode = default_parsed.mode if default_parsed.valid else "safe"
        return ExecutionModeParse(
            raw=raw,
            mode=mode,
            valid=True,
            missing=True,
            source=str(source or "mode"),
            reason="missing_defaulted",
        )

    mode = _MODE_ALIASES.get(text)
    if mode is None:
        return ExecutionModeParse(
            raw=raw,
            mode="safe",
            valid=False,
            missing=False,
            source=str(source or "mode"),
            reason="invalid_execution_mode",
        )

    return ExecutionModeParse(
        raw=raw,
        mode=mode,
        valid=True,
        missing=False,
        source=str(source or "mode"),
        reason="ok",
    )


def coerce_execution_mode(value: Any, *, source: str = "mode") -> str:
    parsed = parse_execution_mode(value, default=None, source=source)
    if not parsed.valid:
        diagnostic = parsed.diagnostic()
        raise ValueError(
            f"invalid_execution_mode:{diagnostic['source']}:{diagnostic['raw'] or '<missing>'}"
        )
    return parsed.mode


def mode_rank(mode: Any) -> int:
    parsed = parse_execution_mode(mode, default="safe")
    return int(_MODE_RANK.get(parsed.mode, _MODE_RANK["safe"]))


def most_restrictive_mode(modes: list[ExecutionModeParse]) -> ExecutionModeParse:
    valid_modes = [item for item in modes if item.valid]
    if not valid_modes:
        return parse_execution_mode("safe", source="default")
    return max(valid_modes, key=lambda item: mode_rank(item.mode))


def env_execution_mode_snapshot(
    environ: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    source = environ if environ is not None else os.environ
    parsed_by_name: dict[str, ExecutionModeParse] = {}
    for name in EXECUTION_MODE_ENV_KEYS:
        raw = source.get(name)
        if raw is None or str(raw).strip() == "":
            continue
        parsed = parse_execution_mode(raw, default=None, source=name)
        parsed_by_name[name] = parsed
        if not parsed.valid:
            return {
                "mode": "safe",
                "explicit": True,
                "source": name,
                "invalid": parsed.diagnostic(),
                "inputs": {key: value.diagnostic() for key, value in parsed_by_name.items()},
            }

    primary = [
        parsed_by_name[name]
        for name in PRIMARY_EXECUTION_MODE_ENV_KEYS
        if name in parsed_by_name
    ]
    secondary = [
        parsed_by_name[name]
        for name in SECONDARY_EXECUTION_MODE_ENV_KEYS
        if name in parsed_by_name
    ]
    selected = most_restrictive_mode(primary or secondary)
    explicit = bool(primary or secondary)
    return {
        "mode": selected.mode,
        "explicit": explicit,
        "source": selected.source if explicit else "default",
        "invalid": None,
        "inputs": {key: value.diagnostic() for key, value in parsed_by_name.items()},
    }


def resolve_effective_execution_mode(
    mode_state: Any = None,
    *,
    environ: Optional[Mapping[str, str]] = None,
    default: str = "safe",
) -> dict[str, Any]:
    env_snapshot = env_execution_mode_snapshot(environ)
    invalid = env_snapshot.get("invalid")
    if invalid:
        return {
            "ok": False,
            "mode": "safe",
            "armed": None,
            "reason": "invalid_execution_mode",
            "invalid_mode": dict(invalid),
            "env_mode": dict(env_snapshot),
            "source": str(invalid.get("source") or "env"),
        }

    env_mode = str(env_snapshot.get("mode") or default)
    env_explicit = bool(env_snapshot.get("explicit"))
    env_source = str(env_snapshot.get("source") or "default")

    db_mode: Optional[str] = None
    db_source = ""
    armed: Optional[int] = None
    if isinstance(mode_state, dict):
        raw_mode = mode_state.get("mode", mode_state.get("execution_mode"))
        if raw_mode not in (None, ""):
            parsed = parse_execution_mode(raw_mode, default=None, source=str(mode_state.get("source") or "mode_state"))
            if not parsed.valid:
                return {
                    "ok": False,
                    "mode": "safe",
                    "armed": None,
                    "reason": "invalid_execution_mode",
                    "invalid_mode": parsed.diagnostic(),
                    "env_mode": dict(env_snapshot),
                    "source": parsed.source,
                }
            db_mode = parsed.mode
            db_source = parsed.source
        if "armed" in mode_state:
            try:
                armed = int(mode_state.get("armed") or 0)
            except Exception:
                armed = None
    elif isinstance(mode_state, str):
        parsed = parse_execution_mode(mode_state, default=None, source="mode_state")
        if not parsed.valid:
            return {
                "ok": False,
                "mode": "safe",
                "armed": None,
                "reason": "invalid_execution_mode",
                "invalid_mode": parsed.diagnostic(),
                "env_mode": dict(env_snapshot),
                "source": parsed.source,
            }
        db_mode = parsed.mode
        db_source = parsed.source

    mode = db_mode or env_mode
    source_name = db_source or env_source
    if db_mode and env_explicit and mode_rank(env_mode) >= mode_rank(db_mode):
        mode = env_mode
        source_name = f"{db_source}+env_restrictive" if db_source else env_source
    elif not db_mode and not env_explicit:
        parsed_default = parse_execution_mode(default, default="safe", source="default")
        mode = parsed_default.mode
        source_name = "default"

    return {
        "ok": True,
        "mode": mode,
        "armed": armed,
        "reason": "ok",
        "source": source_name,
        "db_mode": db_mode,
        "env_mode": dict(env_snapshot),
    }


def live_broker_mode_boundary_block(
    *,
    broker: str,
    get_execution_mode_fn: Optional[Callable[[], Any]],
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[dict[str, Any]]:
    if not callable(get_execution_mode_fn):
        return {
            "ok": False,
            "status": "execution_mode_provider_missing",
            "reason": "execution_mode_provider_missing",
            "broker": str(broker),
            "stop_failover": True,
            "retryable": False,
        }
    try:
        mode_state = get_execution_mode_fn()
    except Exception as exc:
        return {
            "ok": False,
            "status": "execution_mode_unavailable",
            "reason": "execution_mode_unavailable",
            "broker": str(broker),
            "error_type": type(exc).__name__,
            "stop_failover": True,
            "retryable": False,
        }

    resolved = resolve_effective_execution_mode(mode_state, environ=environ)
    if not bool(resolved.get("ok")):
        return {
            "ok": False,
            "status": "execution_mode_invalid",
            "reason": "invalid_execution_mode",
            "broker": str(broker),
            "mode": str(resolved.get("mode") or "safe"),
            "invalid_mode": dict(resolved.get("invalid_mode") or {}),
            "stop_failover": True,
            "retryable": False,
        }

    mode = str(resolved.get("mode") or "safe")
    armed = resolved.get("armed")
    if mode != "live":
        return {
            "ok": False,
            "status": "execution_mode_blocked",
            "reason": "mode_not_live",
            "broker": str(broker),
            "mode": mode,
            "armed": armed,
            "stop_failover": True,
            "retryable": False,
        }
    if int(armed or 0) != 1:
        return {
            "ok": False,
            "status": "execution_mode_blocked",
            "reason": "live_not_armed",
            "broker": str(broker),
            "mode": mode,
            "armed": armed,
            "stop_failover": True,
            "retryable": False,
        }
    return None
