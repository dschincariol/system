"""Deterministic local market-data provider for safe/sim mode.

This provider is intentionally explicit: rows carry provider/source
``simulated`` and never imply that an external market-data provider succeeded.
"""

from __future__ import annotations

import hashlib
import math
import os
import time
from typing import Any, Dict, Mapping


PROVIDER_NAME = "simulated"
DEFAULT_SYMBOLS = ("SPY", "AAPL", "MSFT", "QQQ")


def simulated_market_data_enabled() -> bool:
    raw = os.environ.get("SIMULATED_MARKET_DATA_ENABLED")
    if raw is not None and str(raw).strip() != "":
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}
    engine_mode = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower()
    execution_mode = str(os.environ.get("EXECUTION_MODE", "safe") or "safe").strip().lower()
    broker = str(os.environ.get("BROKER", "sim") or "sim").strip().lower()
    broker_name = str(os.environ.get("BROKER_NAME", broker) or broker).strip().lower()
    return bool(
        engine_mode in {"safe", "sim", "simulation", "test"}
        and execution_mode in {"safe", "sim", "simulation", "sim-paper", "sim_paper", "paper"}
        and broker == "sim"
        and broker_name == "sim"
    )


def configured_simulated_symbols() -> list[str]:
    raw = str(os.environ.get("SIMULATED_MARKET_DATA_SYMBOLS", "") or "").strip()
    values = raw.split(",") if raw else DEFAULT_SYMBOLS
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out or list(DEFAULT_SYMBOLS)


def _stable_unit_interval(symbol: str, *, salt: str) -> float:
    digest = hashlib.sha256(f"{salt}:{symbol}".encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(0xFFFFFFFFFFFF)


def simulated_quote_for_symbol(symbol: str, *, ts_ms: int | None = None) -> Dict[str, Any]:
    symbol_s = str(symbol or "").strip().upper()
    now_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
    bucket = int(now_ms // 60_000)
    base = 20.0 + (_stable_unit_interval(symbol_s, salt="base") * 480.0)
    phase = (_stable_unit_interval(symbol_s, salt="phase") * math.tau)
    drift = math.sin((bucket / 17.0) + phase) * max(0.01, base * 0.0015)
    price = max(0.01, base + drift)
    spread = max(0.01, price * 0.0002)
    bid = max(0.01, price - (spread / 2.0))
    ask = price + (spread / 2.0)
    volume = 1000.0 + int(_stable_unit_interval(symbol_s, salt=f"vol:{bucket}") * 9000.0)
    return {
        "symbol": symbol_s,
        "ts_ms": now_ms,
        "timestamp": now_ms,
        "price": float(round(price, 6)),
        "last": float(round(price, 6)),
        "bid": float(round(bid, 6)),
        "ask": float(round(ask, 6)),
        "spread": float(round(ask - bid, 6)),
        "volume": float(volume),
        "provider": PROVIDER_NAME,
        "source": PROVIDER_NAME,
        "simulated": True,
        "event_type": "Q",
    }


class SimulatedPriceProvider:
    """Local deterministic provider with the same fetch contract as REST feeds."""

    provider_name = PROVIDER_NAME

    def fetch_last_prices(self, ticker_map: Mapping[str, str] | None) -> Dict[str, Dict[str, Any]]:
        symbols = list((ticker_map or {}).keys()) or configured_simulated_symbols()
        now_ms = int(time.time() * 1000)
        out: Dict[str, Dict[str, Any]] = {}
        for symbol in symbols:
            symbol_s = str(symbol or "").strip().upper()
            if not symbol_s:
                continue
            out[symbol_s] = simulated_quote_for_symbol(symbol_s, ts_ms=now_ms)
        return out

