"""
FILE: provider.py

Live price feed integration for `provider`.
"""

import time

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.data.provider_registry import build_price_provider

LOG = get_logger("engine.data.live_prices.provider")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.data.live_prices.provider",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _provider_health_key(name: str) -> str:
    return f"price_provider_health::{name}"


def _record_provider_failure(name: str):
    # Provider availability is mirrored into risk_state as a lightweight
    # runtime-visible breadcrumb for dashboards and guards.
    con = connect()
    try:
        con.execute(
            """
            INSERT INTO risk_state(key, value, updated_ts_ms)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (_provider_health_key(name), "fail", int(time.time() * 1000)),
        )
        try:
            con.commit()
        except Exception as e:
            _warn_nonfatal("LIVE_PRICE_PROVIDER_COMMIT_FAILED", e, once_key="record_provider_failure_commit", provider=str(name))
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("LIVE_PRICE_PROVIDER_CLOSE_FAILED", e, once_key="record_provider_failure_close", provider=str(name))


def _record_provider_success(name: str):
    con = connect()
    try:
        con.execute(
            """
            INSERT INTO risk_state(key, value, updated_ts_ms)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (_provider_health_key(name), "ok", int(time.time() * 1000)),
        )
        try:
            con.commit()
        except Exception as e:
            _warn_nonfatal("LIVE_PRICE_PROVIDER_COMMIT_FAILED", e, once_key="record_provider_success_commit", provider=str(name))
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("LIVE_PRICE_PROVIDER_CLOSE_FAILED", e, once_key="record_provider_success_close", provider=str(name))

def get_price_provider_by_name(provider: str):
    provider = str(provider or "").strip().lower()
    return build_price_provider(provider)

def get_price_provider():
    import os

    provider = os.environ.get("LIVE_PRICE_PROVIDER", "yfinance").lower()

    try:
        # First honor the explicitly requested provider.
        p = get_price_provider_by_name(provider)
        _record_provider_success(provider)
        return p
    except Exception:
        _record_provider_failure(provider)

    chain = os.environ.get("LIVE_PRICE_PROVIDER_CHAIN", "").strip()
    if chain:
        for name in [x.strip().lower() for x in chain.split(",") if x.strip()]:
            try:
                p = get_price_provider_by_name(name)
                _record_provider_success(name)
                return p
            except Exception:
                _record_provider_failure(name)

    # Final fallback order is intentionally conservative and stable.
    for name in ("ibkr", "polygon", "yfinance", "ccxt"):
        try:
            p = get_price_provider_by_name(name)
            _record_provider_success(name)
            return p
        except Exception:
            _record_provider_failure(name)

    raise RuntimeError("No live price provider available")
