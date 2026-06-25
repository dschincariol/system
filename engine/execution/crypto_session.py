"""Pure crypto execution-session state.

Crypto spot trades continuously by default. This module models that 24/7
behavior and only reports a closed session when an operator explicitly configures
a daily maintenance window through ``CRYPTO_MAINTENANCE_*`` environment knobs.

No broker, database, network, order, cost, or schema operation is performed here.
"""

from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta, timezone
import json
import os
from typing import Any, Dict

_UTC = timezone.utc
_KNOWN_CRYPTO_BASES = {"BTC", "ETH", "SOL", "BNB", "XRP"}
_KNOWN_CRYPTO_QUOTES = {"USD", "USDT", "USDC"}
_CRYPTO_BASE_ALIASES = {"XBT": "BTC"}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.environ.get(name, str(default)) or str(default)).strip())
    except Exception:
        value = int(default)
    return max(int(minimum), min(int(maximum), int(value)))


def _dt_from_ms(ts_ms: int) -> datetime:
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=_UTC)
    except Exception:
        return datetime.fromtimestamp(0, tz=_UTC)


def _ms_from_dt(value: datetime) -> int:
    return int(value.astimezone(_UTC).timestamp() * 1000)


def normalize_crypto_symbol(symbol: Any) -> str:
    """Return the local bare-root crypto symbol used by asset_map/storage.

    Local fallback only: the canonical ``crypto_instrument.py`` owner is still
    absent, so keep this behavior aligned with the other crypto fallbacks.
    """

    try:
        text = str("" if symbol is None else symbol).upper().strip()
    except Exception:
        return ""
    if not text:
        return ""
    for separator in ("/", "-", "_", ":"):
        if separator in text:
            text = text.split(separator, 1)[0]
            break
    for quote in sorted(_KNOWN_CRYPTO_QUOTES, key=len, reverse=True):
        if text.endswith(quote) and len(text) > len(quote):
            text = text[: -len(quote)]
            break
    return _CRYPTO_BASE_ALIASES.get(text, text)


def _override_crypto_symbols() -> set[str]:
    raw = os.environ.get("ASSET_CLASS_MAP_JSON", "").strip()
    if not raw:
        return set()
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            return set()
        return {
            str(symbol).upper().strip()
            for symbol, asset_class in parsed.items()
            if str(asset_class or "").upper().strip() == "CRYPTO" and str(symbol or "").strip()
        }
    except Exception:
        return set()


def _is_crypto_symbol(symbol: Any) -> bool:
    base = normalize_crypto_symbol(symbol)
    if not base:
        return False
    return base in _KNOWN_CRYPTO_BASES or base in _override_crypto_symbols()


def _configured_symbols_allow(symbol: Any) -> bool:
    raw = os.environ.get("CRYPTO_MAINTENANCE_SYMBOLS", "").strip()
    if not raw:
        return True
    allowed = {
        normalize_crypto_symbol(part)
        for part in raw.replace(";", ",").split(",")
        if normalize_crypto_symbol(part)
    }
    return normalize_crypto_symbol(symbol) in allowed


def _maintenance_start_time() -> dt_time | None:
    raw = os.environ.get("CRYPTO_MAINTENANCE_START_UTC", "").strip()
    if raw:
        try:
            hour_text, minute_text = raw.split(":", 1)
            hour = max(0, min(23, int(hour_text.strip())))
            minute = max(0, min(59, int(minute_text.strip())))
            return dt_time(hour=hour, minute=minute, tzinfo=_UTC)
        except Exception:
            return None
    if "CRYPTO_MAINTENANCE_START_HOUR_UTC" not in os.environ and not _env_bool("CRYPTO_MAINTENANCE_ENABLED", False):
        return None
    hour = _env_int("CRYPTO_MAINTENANCE_START_HOUR_UTC", 0, 0, 23)
    minute = _env_int("CRYPTO_MAINTENANCE_START_MINUTE_UTC", 0, 0, 59)
    return dt_time(hour=hour, minute=minute, tzinfo=_UTC)


def _maintenance_window(ts_ms: int, symbol: Any) -> tuple[bool, int | None, int | None]:
    if not _configured_symbols_allow(symbol):
        return False, None, None

    start_time = _maintenance_start_time()
    if start_time is None:
        return False, None, None

    duration_min = _env_int("CRYPTO_MAINTENANCE_DURATION_MINUTES", 0, 0, 24 * 60)
    if duration_min <= 0:
        return False, None, None

    dt = _dt_from_ms(int(ts_ms))
    start = datetime.combine(dt.date(), start_time)
    end = start + timedelta(minutes=int(duration_min))
    if dt < start:
        previous_start = start - timedelta(days=1)
        previous_end = previous_start + timedelta(minutes=int(duration_min))
        if previous_start <= dt < previous_end:
            return True, _ms_from_dt(previous_start), _ms_from_dt(previous_end)
    if start <= dt < end:
        return True, _ms_from_dt(start), _ms_from_dt(end)
    return False, _ms_from_dt(start), _ms_from_dt(end)


def crypto_session_state(symbol: Any, now_ms: int) -> Dict[str, Any]:
    """Return deterministic crypto execution-session state for ``symbol``."""

    is_crypto = _is_crypto_symbol(symbol)
    if not is_crypto:
        return {
            "is_crypto": False,
            "session": "open",
            "is_open": True,
            "in_maintenance_window": False,
            "next_open_ms": None,
        }

    try:
        in_maintenance, _start_ms, end_ms = _maintenance_window(int(now_ms), symbol)
    except Exception:
        in_maintenance, end_ms = False, None

    if in_maintenance:
        return {
            "is_crypto": True,
            "session": "maintenance",
            "is_open": False,
            "in_maintenance_window": True,
            "next_open_ms": end_ms,
        }
    return {
        "is_crypto": True,
        "session": "open",
        "is_open": True,
        "in_maintenance_window": False,
        "next_open_ms": None,
    }


def crypto_timing_adjustment(symbol: Any, now_ms: int, base_decision: Dict[str, Any]) -> Dict[str, Any]:
    """Return a crypto-session-adjusted execution decision.

    Non-crypto decisions are returned unchanged. Crypto decisions remain
    executable 24/7 by default and are only marked for policy-layer suppression
    inside an explicitly configured maintenance window.
    """

    try:
        out = dict(base_decision or {})
    except Exception:
        out = {}

    state = crypto_session_state(symbol, now_ms)
    if not bool(state.get("is_crypto")):
        return out

    out["crypto_session"] = dict(state)
    out["crypto_session_blocked"] = False
    if not bool(state.get("is_open")):
        out["crypto_session_blocked"] = True
        out["crypto_session_reason"] = "maintenance"
    return out


__all__ = ["normalize_crypto_symbol", "crypto_session_state", "crypto_timing_adjustment"]
